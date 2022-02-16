//
//! The Page Service listens for client connections and serves their GetPage@LSN
//! requests.
//
//   It is possible to connect here using usual psql/pgbench/libpq. Following
// commands are supported now:
//     *status* -- show actual info about this pageserver,
//     *pagestream* -- enter mode where smgr and pageserver talk with their
//  custom protocol.
//     *callmemaybe <zenith timelineid> $url* -- ask pageserver to start walreceiver on $url
//

use anyhow::{bail, ensure, Context, Result};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use lazy_static::lazy_static;
use regex::Regex;
use std::io;
use std::net::TcpListener;
use std::str;
use std::str::FromStr;
use std::sync::{Arc, RwLockReadGuard};
use tracing::*;
use zenith_metrics::{register_histogram_vec, HistogramVec};
use zenith_utils::auth::{self, JwtAuth};
use zenith_utils::auth::{Claims, Scope};
use zenith_utils::lsn::Lsn;
use zenith_utils::postgres_backend::is_socket_read_timed_out;
use zenith_utils::postgres_backend::PostgresBackend;
use zenith_utils::postgres_backend::{self, AuthType};
use zenith_utils::pq_proto::{
    BeMessage, FeMessage, RowDescriptor, HELLO_WORLD_ROW, SINGLE_COL_ROWDESC,
};
use zenith_utils::zid::{ZTenantId, ZTimelineId};

use crate::basebackup;
use crate::config::PageServerConf;
use crate::relish::*;
use crate::repository::Timeline;
use crate::tenant_mgr;
use crate::thread_mgr;
use crate::thread_mgr::ThreadKind;
use crate::walreceiver;
use crate::CheckpointConfig;

// Wrapped in libpq CopyData
enum PagestreamFeMessage {
    Exists(PagestreamExistsRequest),
    Nblocks(PagestreamNblocksRequest),
    GetPage(PagestreamGetPageRequest),
}

// Wrapped in libpq CopyData
enum PagestreamBeMessage {
    Exists(PagestreamExistsResponse),
    Nblocks(PagestreamNblocksResponse),
    GetPage(PagestreamGetPageResponse),
    Error(PagestreamErrorResponse),
}

#[derive(Debug)]
struct PagestreamExistsRequest {
    latest: bool,
    lsn: Lsn,
    rel: RelTag,
}

#[derive(Debug)]
struct PagestreamNblocksRequest {
    latest: bool,
    lsn: Lsn,
    rel: RelTag,
}

#[derive(Debug)]
struct PagestreamGetPageRequest {
    latest: bool,
    lsn: Lsn,
    rel: RelTag,
    blkno: u32,
}

#[derive(Debug)]
struct PagestreamExistsResponse {
    exists: bool,
}

#[derive(Debug)]
struct PagestreamNblocksResponse {
    n_blocks: u32,
}

#[derive(Debug)]
struct PagestreamGetPageResponse {
    page: Bytes,
}

#[derive(Debug)]
struct PagestreamErrorResponse {
    message: String,
}

impl PagestreamFeMessage {
    fn parse(mut body: Bytes) -> anyhow::Result<PagestreamFeMessage> {
        // TODO these gets can fail

        // these correspond to the ZenithMessageTag enum in pagestore_client.h
        //
        // TODO: consider using protobuf or serde bincode for less error prone
        // serialization.
        let msg_tag = body.get_u8();
        match msg_tag {
            0 => Ok(PagestreamFeMessage::Exists(PagestreamExistsRequest {
                latest: body.get_u8() != 0,
                lsn: Lsn::from(body.get_u64()),
                rel: RelTag {
                    spcnode: body.get_u32(),
                    dbnode: body.get_u32(),
                    relnode: body.get_u32(),
                    forknum: body.get_u8(),
                },
            })),
            1 => Ok(PagestreamFeMessage::Nblocks(PagestreamNblocksRequest {
                latest: body.get_u8() != 0,
                lsn: Lsn::from(body.get_u64()),
                rel: RelTag {
                    spcnode: body.get_u32(),
                    dbnode: body.get_u32(),
                    relnode: body.get_u32(),
                    forknum: body.get_u8(),
                },
            })),
            2 => Ok(PagestreamFeMessage::GetPage(PagestreamGetPageRequest {
                latest: body.get_u8() != 0,
                lsn: Lsn::from(body.get_u64()),
                rel: RelTag {
                    spcnode: body.get_u32(),
                    dbnode: body.get_u32(),
                    relnode: body.get_u32(),
                    forknum: body.get_u8(),
                },
                blkno: body.get_u32(),
            })),
            _ => bail!("unknown smgr message tag: {},'{:?}'", msg_tag, body),
        }
    }
}

impl PagestreamBeMessage {
    fn serialize(&self) -> Bytes {
        let mut bytes = BytesMut::new();

        match self {
            Self::Exists(resp) => {
                bytes.put_u8(100); /* tag from pagestore_client.h */
                bytes.put_u8(resp.exists as u8);
            }

            Self::Nblocks(resp) => {
                bytes.put_u8(101); /* tag from pagestore_client.h */
                bytes.put_u32(resp.n_blocks);
            }

            Self::GetPage(resp) => {
                bytes.put_u8(102); /* tag from pagestore_client.h */
                bytes.put(&resp.page[..]);
            }

            Self::Error(resp) => {
                bytes.put_u8(103); /* tag from pagestore_client.h */
                bytes.put(resp.message.as_bytes());
                bytes.put_u8(0); // null terminator
            }
        }

        bytes.into()
    }
}

///////////////////////////////////////////////////////////////////////////////

///
/// Main loop of the page service.
///
/// Listens for connections, and launches a new handler thread for each.
///
pub fn thread_main(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    listener: TcpListener,
    auth_type: AuthType,
) -> anyhow::Result<()> {
    listener.set_nonblocking(true)?;
    let basic_rt = tokio::runtime::Builder::new_current_thread()
        .enable_io()
        .build()?;

    let tokio_listener = {
        let _guard = basic_rt.enter();
        tokio::net::TcpListener::from_std(listener)
    }?;

    // Wait for a new connection to arrive, or for server shutdown.
    while let Some(res) = basic_rt.block_on(async {
        let shutdown_watcher = thread_mgr::shutdown_watcher();
        tokio::select! {
            biased;

            _ = shutdown_watcher => {
                // We were requested to shut down.
                None
            }

            res = tokio_listener.accept() => {
                Some(res)
            }
        }
    }) {
        match res {
            Ok((socket, peer_addr)) => {
                // Connection established. Spawn a new thread to handle it.
                debug!("accepted connection from {}", peer_addr);
                let local_auth = auth.clone();

                // PageRequestHandler threads are not associated with any particular
                // timeline in the thread manager. In practice most connections will
                // only deal with a particular timeline, but we don't know which one
                // yet.
                if let Err(err) = thread_mgr::spawn(
                    ThreadKind::PageRequestHandler,
                    None,
                    None,
                    "serving Page Service thread",
                    move || page_service_conn_main(conf, local_auth, socket, auth_type),
                ) {
                    // Thread creation failed. Log the error and continue.
                    error!("could not spawn page service thread: {:?}", err);
                }
            }
            Err(err) => {
                // accept() failed. Log the error, and loop back to retry on next connection.
                error!("accept() failed: {:?}", err);
            }
        }
    }

    debug!("page_service loop terminated");

    Ok(())
}

fn page_service_conn_main(
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    socket: tokio::net::TcpStream,
    auth_type: AuthType,
) -> anyhow::Result<()> {
    // Immediately increment the gauge, then create a job to decrement it on thread exit.
    // One of the pros of `defer!` is that this will *most probably*
    // get called, even in presence of panics.
    let gauge = crate::LIVE_CONNECTIONS_COUNT.with_label_values(&["page_service"]);
    gauge.inc();
    scopeguard::defer! {
        gauge.dec();
    }

    // We use Tokio to accept the connection, but the rest of the code works with a
    // regular socket. Convert.
    let socket = socket
        .into_std()
        .context("could not convert tokio::net:TcpStream to std::net::TcpStream")?;
    socket
        .set_nonblocking(false)
        .context("could not put socket to blocking mode")?;

    socket
        .set_nodelay(true)
        .context("could not set TCP_NODELAY")?;

    let mut conn_handler = PageServerHandler::new(conf, auth);
    let pgbackend = PostgresBackend::new(socket, auth_type, None, true)?;
    pgbackend.run(&mut conn_handler)
}

#[derive(Debug)]
struct PageServerHandler {
    conf: &'static PageServerConf,
    auth: Option<Arc<JwtAuth>>,
    claims: Option<Claims>,
}

const TIME_BUCKETS: &[f64] = &[
    0.00001, // 1/100000 s
    0.0001, 0.00015, 0.0002, 0.00025, 0.0003, 0.00035, 0.0005, 0.00075, // 1/10000 s
    0.001, 0.0025, 0.005, 0.0075, // 1/1000 s
    0.01, 0.0125, 0.015, 0.025, 0.05, // 1/100 s
    0.1,  // 1/10 s
];

lazy_static! {
    static ref SMGR_QUERY_TIME: HistogramVec = register_histogram_vec!(
        "pageserver_smgr_query_time",
        "Time spent on smgr query handling",
        &["smgr_query_type"],
        TIME_BUCKETS.into()
    )
    .expect("failed to define a metric");
}

impl PageServerHandler {
    pub fn new(conf: &'static PageServerConf, auth: Option<Arc<JwtAuth>>) -> Self {
        PageServerHandler {
            conf,
            auth,
            claims: None,
        }
    }

    fn handle_pagerequests(
        &self,
        pgb: &mut PostgresBackend,
        timelineid: ZTimelineId,
        tenantid: ZTenantId,
    ) -> anyhow::Result<()> {
        let _enter = info_span!("pagestream", timeline = %timelineid, tenant = %tenantid).entered();

        // Check that the timeline exists
        let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)
            .context("Cannot handle pagerequests for a remote timeline")?;

        /* switch client to COPYBOTH */
        pgb.write_message(&BeMessage::CopyBothResponse)?;

        while !thread_mgr::is_shutdown_requested() {
            match pgb.read_message() {
                Ok(message) => {
                    if let Some(message) = message {
                        trace!("query: {:?}", message);

                        let copy_data_bytes = match message {
                            FeMessage::CopyData(bytes) => bytes,
                            _ => continue,
                        };

                        let zenith_fe_msg = PagestreamFeMessage::parse(copy_data_bytes)?;

                        let response = match zenith_fe_msg {
                            PagestreamFeMessage::Exists(req) => SMGR_QUERY_TIME
                                .with_label_values(&["get_rel_exists"])
                                .observe_closure_duration(|| {
                                    self.handle_get_rel_exists_request(timeline.as_ref(), &req)
                                }),
                            PagestreamFeMessage::Nblocks(req) => SMGR_QUERY_TIME
                                .with_label_values(&["get_rel_size"])
                                .observe_closure_duration(|| {
                                    self.handle_get_nblocks_request(timeline.as_ref(), &req)
                                }),
                            PagestreamFeMessage::GetPage(req) => SMGR_QUERY_TIME
                                .with_label_values(&["get_page_at_lsn"])
                                .observe_closure_duration(|| {
                                    self.handle_get_page_at_lsn_request(timeline.as_ref(), &req)
                                }),
                        };

                        let response = response.unwrap_or_else(|e| {
                            // print the all details to the log with {:#}, but for the client the
                            // error message is enough
                            error!("error reading relation or page version: {:?}", e);
                            PagestreamBeMessage::Error(PagestreamErrorResponse {
                                message: e.to_string(),
                            })
                        });

                        pgb.write_message(&BeMessage::CopyData(&response.serialize()))?;
                    } else {
                        break;
                    }
                }
                Err(e) => {
                    if !is_socket_read_timed_out(&e) {
                        return Err(e);
                    }
                }
            }
        }
        Ok(())
    }

    /// Helper function to handle the LSN from client request.
    ///
    /// Each GetPage (and Exists and Nblocks) request includes information about
    /// which version of the page is being requested. The client can request the
    /// latest version of the page, or the version that's valid at a particular
    /// LSN. The primary compute node will always request the latest page
    /// version, while a standby will request a version at the LSN that it's
    /// currently caught up to.
    ///
    /// In either case, if the page server hasn't received the WAL up to the
    /// requested LSN yet, we will wait for it to arrive. The return value is
    /// the LSN that should be used to look up the page versions.
    fn wait_or_get_last_lsn(
        timeline: &dyn Timeline,
        mut lsn: Lsn,
        latest: bool,
        latest_gc_cutoff_lsn: &RwLockReadGuard<Lsn>,
    ) -> Result<Lsn> {
        if latest {
            // Latest page version was requested. If LSN is given, it is a hint
            // to the page server that there have been no modifications to the
            // page after that LSN. If we haven't received WAL up to that point,
            // wait until it arrives.
            let last_record_lsn = timeline.get_last_record_lsn();

            // Note: this covers the special case that lsn == Lsn(0). That
            // special case means "return the latest version whatever it is",
            // and it's used for bootstrapping purposes, when the page server is
            // connected directly to the compute node. That is needed because
            // when you connect to the compute node, to receive the WAL, the
            // walsender process will do a look up in the pg_authid catalog
            // table for authentication. That poses a deadlock problem: the
            // catalog table lookup will send a GetPage request, but the GetPage
            // request will block in the page server because the recent WAL
            // hasn't been received yet, and it cannot be received until the
            // walsender completes the authentication and starts streaming the
            // WAL.
            if lsn <= last_record_lsn {
                lsn = last_record_lsn;
            } else {
                timeline.wait_lsn(lsn)?;
                // Since we waited for 'lsn' to arrive, that is now the last
                // record LSN. (Or close enough for our purposes; the
                // last-record LSN can advance immediately after we return
                // anyway)
            }
        } else {
            if lsn == Lsn(0) {
                bail!("invalid LSN(0) in request");
            }
            timeline.wait_lsn(lsn)?;
        }
        ensure!(
            lsn >= **latest_gc_cutoff_lsn,
            "tried to request a page version that was garbage collected. requested at {} gc cutoff {}",
            lsn, **latest_gc_cutoff_lsn
        );
        Ok(lsn)
    }

    fn handle_get_rel_exists_request(
        &self,
        timeline: &dyn Timeline,
        req: &PagestreamExistsRequest,
    ) -> Result<PagestreamBeMessage> {
        let _enter = info_span!("get_rel_exists", rel = %req.rel, req_lsn = %req.lsn).entered();

        let tag = RelishTag::Relation(req.rel);
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn)?;

        let exists = timeline.get_rel_exists(tag, lsn)?;

        Ok(PagestreamBeMessage::Exists(PagestreamExistsResponse {
            exists,
        }))
    }

    fn handle_get_nblocks_request(
        &self,
        timeline: &dyn Timeline,
        req: &PagestreamNblocksRequest,
    ) -> Result<PagestreamBeMessage> {
        let _enter = info_span!("get_nblocks", rel = %req.rel, req_lsn = %req.lsn).entered();
        let tag = RelishTag::Relation(req.rel);
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn)?;

        let n_blocks = timeline.get_relish_size(tag, lsn)?;

        // Return 0 if relation is not found.
        // This is what postgres smgr expects.
        let n_blocks = n_blocks.unwrap_or(0);

        Ok(PagestreamBeMessage::Nblocks(PagestreamNblocksResponse {
            n_blocks,
        }))
    }

    fn handle_get_page_at_lsn_request(
        &self,
        timeline: &dyn Timeline,
        req: &PagestreamGetPageRequest,
    ) -> Result<PagestreamBeMessage> {
        let _enter = info_span!("get_page", rel = %req.rel, blkno = &req.blkno, req_lsn = %req.lsn)
            .entered();
        let tag = RelishTag::Relation(req.rel);
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        let lsn = Self::wait_or_get_last_lsn(timeline, req.lsn, req.latest, &latest_gc_cutoff_lsn)?;
        /*
        // Add a 1s delay to some requests. The delayed causes the requests to
        // hit the race condition from github issue #1047 more easily.
        use rand::Rng;
        if rand::thread_rng().gen::<u8>() < 5 {
            std::thread::sleep(std::time::Duration::from_millis(1000));
        }
        */
        let page = timeline.get_page_at_lsn(tag, req.blkno, lsn)?;

        Ok(PagestreamBeMessage::GetPage(PagestreamGetPageResponse {
            page,
        }))
    }

    fn handle_basebackup_request(
        &self,
        pgb: &mut PostgresBackend,
        timelineid: ZTimelineId,
        lsn: Option<Lsn>,
        tenantid: ZTenantId,
    ) -> anyhow::Result<()> {
        let span = info_span!("basebackup", timeline = %timelineid, tenant = %tenantid, lsn = field::Empty);
        let _enter = span.enter();

        // check that the timeline exists
        let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)
            .context("Cannot handle basebackup request for a remote timeline")?;
        let latest_gc_cutoff_lsn = timeline.get_latest_gc_cutoff_lsn();
        if let Some(lsn) = lsn {
            timeline
                .check_lsn_is_in_scope(lsn, &latest_gc_cutoff_lsn)
                .context("invalid basebackup lsn")?;
        }

        // switch client to COPYOUT
        pgb.write_message(&BeMessage::CopyOutResponse)?;

        /* Send a tarball of the latest layer on the timeline */
        {
            let mut writer = CopyDataSink { pgb };
            let mut basebackup = basebackup::Basebackup::new(&mut writer, &timeline, lsn)?;
            span.record("lsn", &basebackup.lsn.to_string().as_str());
            basebackup.send_tarball()?;
        }
        pgb.write_message(&BeMessage::CopyDone)?;
        debug!("CopyDone sent!");

        Ok(())
    }

    // when accessing management api supply None as an argument
    // when using to authorize tenant pass corresponding tenant id
    fn check_permission(&self, tenantid: Option<ZTenantId>) -> Result<()> {
        if self.auth.is_none() {
            // auth is set to Trust, nothing to check so just return ok
            return Ok(());
        }
        // auth is some, just checked above, when auth is some
        // then claims are always present because of checks during connetion init
        // so this expect won't trigger
        let claims = self
            .claims
            .as_ref()
            .expect("claims presence already checked");
        auth::check_permission(claims, tenantid)
    }
}

impl postgres_backend::Handler for PageServerHandler {
    fn check_auth_jwt(
        &mut self,
        _pgb: &mut PostgresBackend,
        jwt_response: &[u8],
    ) -> anyhow::Result<()> {
        // this unwrap is never triggered, because check_auth_jwt only called when auth_type is ZenithJWT
        // which requires auth to be present
        let data = self
            .auth
            .as_ref()
            .as_ref()
            .unwrap()
            .decode(str::from_utf8(jwt_response)?)?;

        if matches!(data.claims.scope, Scope::Tenant) {
            ensure!(
                data.claims.tenant_id.is_some(),
                "jwt token scope is Tenant, but tenant id is missing"
            )
        }

        info!(
            "jwt auth succeeded for scope: {:#?} by tenantid: {:?}",
            data.claims.scope, data.claims.tenant_id,
        );

        self.claims = Some(data.claims);
        Ok(())
    }

    fn process_query(
        &mut self,
        pgb: &mut PostgresBackend,
        query_string: &str,
    ) -> anyhow::Result<()> {
        debug!("process query {:?}", query_string);

        if query_string.starts_with("pagestream ") {
            let (_, params_raw) = query_string.split_at("pagestream ".len());
            let params = params_raw.split(' ').collect::<Vec<_>>();
            ensure!(
                params.len() == 2,
                "invalid param number for pagestream command"
            );
            let tenantid = ZTenantId::from_str(params[0])?;
            let timelineid = ZTimelineId::from_str(params[1])?;

            self.check_permission(Some(tenantid))?;

            self.handle_pagerequests(pgb, timelineid, tenantid)?;
        } else if query_string.starts_with("basebackup ") {
            let (_, params_raw) = query_string.split_at("basebackup ".len());
            let params = params_raw.split_whitespace().collect::<Vec<_>>();

            ensure!(
                params.len() >= 2,
                "invalid param number for basebackup command"
            );

            let tenantid = ZTenantId::from_str(params[0])?;
            let timelineid = ZTimelineId::from_str(params[1])?;

            self.check_permission(Some(tenantid))?;

            let lsn = if params.len() == 3 {
                Some(Lsn::from_str(params[2])?)
            } else {
                None
            };

            // Check that the timeline exists
            self.handle_basebackup_request(pgb, timelineid, lsn, tenantid)?;
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("callmemaybe ") {
            // callmemaybe <zenith tenantid as hex string> <zenith timelineid as hex string> <connstr>
            // TODO lazy static
            let re = Regex::new(r"^callmemaybe ([[:xdigit:]]+) ([[:xdigit:]]+) (.*)$").unwrap();
            let caps = re
                .captures(query_string)
                .with_context(|| format!("invalid callmemaybe: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
            let connstr = caps.get(3).unwrap().as_str().to_owned();

            self.check_permission(Some(tenantid))?;

            let _enter =
                info_span!("callmemaybe", timeline = %timelineid, tenant = %tenantid).entered();

            // Check that the timeline exists
            tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)
                .context("Failed to fetch local timeline for callmemaybe requests")?;

            walreceiver::launch_wal_receiver(self.conf, tenantid, timelineid, &connstr)?;

            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("tenant_list") {
            let tenants = crate::tenant_mgr::list_tenants()?;
            let tenants_buf = serde_json::to_vec(&tenants)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::DataRow(&[Some(&tenants_buf)]))?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        // TODO kb for test_auth.py and similar, I have to add timeline manipulation commands too?
        } else if query_string.starts_with("tenant_create") {
            let err = || format!("invalid tenant_create: '{}'", query_string);

            // tenant_create <tenantid>
            let re = Regex::new(r"^tenant_create ([[:xdigit:]]+)$").unwrap();
            let caps = re.captures(query_string).with_context(err)?;

            self.check_permission(None)?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;

            tenant_mgr::create_repository_for_tenant(self.conf, tenantid)?;

            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("status") {
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&HELLO_WORLD_ROW)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.to_ascii_lowercase().starts_with("set ") {
            // important because psycopg2 executes "SET datestyle TO 'ISO'"
            // on connect
            pgb.write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("do_gc ") {
            // Run GC immediately on given timeline.
            // FIXME: This is just for tests. See test_runner/batch_others/test_gc.py.
            // This probably should require special authentication or a global flag to
            // enable, I don't think we want to or need to allow regular clients to invoke
            // GC.

            // do_gc <tenant_id> <timeline_id> <gc_horizon>
            let re = Regex::new(r"^do_gc ([[:xdigit:]]+)\s([[:xdigit:]]+)($|\s)([[:digit:]]+)?")
                .unwrap();

            let caps = re
                .captures(query_string)
                .with_context(|| format!("invalid do_gc: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;
            let gc_horizon: u64 = caps
                .get(4)
                .map(|h| h.as_str().parse())
                .unwrap_or(Ok(self.conf.gc_horizon))?;

            let repo = tenant_mgr::get_repository_for_tenant(tenantid)?;
            let result = repo.gc_iteration(Some(timelineid), gc_horizon, true)?;
            pgb.write_message_noflush(&BeMessage::RowDescription(&[
                RowDescriptor::int8_col(b"layer_relfiles_total"),
                RowDescriptor::int8_col(b"layer_relfiles_needed_by_cutoff"),
                RowDescriptor::int8_col(b"layer_relfiles_needed_by_branches"),
                RowDescriptor::int8_col(b"layer_relfiles_not_updated"),
                RowDescriptor::int8_col(b"layer_relfiles_needed_as_tombstone"),
                RowDescriptor::int8_col(b"layer_relfiles_removed"),
                RowDescriptor::int8_col(b"layer_relfiles_dropped"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_total"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_needed_by_cutoff"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_needed_by_branches"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_not_updated"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_needed_as_tombstone"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_removed"),
                RowDescriptor::int8_col(b"layer_nonrelfiles_dropped"),
                RowDescriptor::int8_col(b"elapsed"),
            ]))?
            .write_message_noflush(&BeMessage::DataRow(&[
                Some(result.ondisk_relfiles_total.to_string().as_bytes()),
                Some(
                    result
                        .ondisk_relfiles_needed_by_cutoff
                        .to_string()
                        .as_bytes(),
                ),
                Some(
                    result
                        .ondisk_relfiles_needed_by_branches
                        .to_string()
                        .as_bytes(),
                ),
                Some(result.ondisk_relfiles_not_updated.to_string().as_bytes()),
                Some(
                    result
                        .ondisk_relfiles_needed_as_tombstone
                        .to_string()
                        .as_bytes(),
                ),
                Some(result.ondisk_relfiles_removed.to_string().as_bytes()),
                Some(result.ondisk_relfiles_dropped.to_string().as_bytes()),
                Some(result.ondisk_nonrelfiles_total.to_string().as_bytes()),
                Some(
                    result
                        .ondisk_nonrelfiles_needed_by_cutoff
                        .to_string()
                        .as_bytes(),
                ),
                Some(
                    result
                        .ondisk_nonrelfiles_needed_by_branches
                        .to_string()
                        .as_bytes(),
                ),
                Some(result.ondisk_nonrelfiles_not_updated.to_string().as_bytes()),
                Some(
                    result
                        .ondisk_nonrelfiles_needed_as_tombstone
                        .to_string()
                        .as_bytes(),
                ),
                Some(result.ondisk_nonrelfiles_removed.to_string().as_bytes()),
                Some(result.ondisk_nonrelfiles_dropped.to_string().as_bytes()),
                Some(result.elapsed.as_millis().to_string().as_bytes()),
            ]))?
            .write_message(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else if query_string.starts_with("checkpoint ") {
            // Run checkpoint immediately on given timeline.

            // checkpoint <tenant_id> <timeline_id>
            let re = Regex::new(r"^checkpoint ([[:xdigit:]]+)\s([[:xdigit:]]+)($|\s)?").unwrap();

            let caps = re
                .captures(query_string)
                .with_context(|| format!("invalid checkpoint command: '{}'", query_string))?;

            let tenantid = ZTenantId::from_str(caps.get(1).unwrap().as_str())?;
            let timelineid = ZTimelineId::from_str(caps.get(2).unwrap().as_str())?;

            let timeline = tenant_mgr::get_timeline_for_tenant(tenantid, timelineid)
                .context("Failed to fetch local timeline for checkpoint request")?;

            timeline.checkpoint(CheckpointConfig::Forced)?;
            pgb.write_message_noflush(&SINGLE_COL_ROWDESC)?
                .write_message_noflush(&BeMessage::CommandComplete(b"SELECT 1"))?;
        } else {
            bail!("unknown command");
        }

        pgb.flush()?;

        Ok(())
    }
}

///
/// A std::io::Write implementation that wraps all data written to it in CopyData
/// messages.
///
struct CopyDataSink<'a> {
    pgb: &'a mut PostgresBackend,
}

impl<'a> io::Write for CopyDataSink<'a> {
    fn write(&mut self, data: &[u8]) -> io::Result<usize> {
        // CopyData
        // FIXME: if the input is large, we should split it into multiple messages.
        // Not sure what the threshold should be, but the ultimate hard limit is that
        // the length cannot exceed u32.
        // FIXME: flush isn't really required, but makes it easier
        // to view in wireshark
        self.pgb.write_message(&BeMessage::CopyData(data))?;
        trace!("CopyData sent for {} bytes!", data.len());

        Ok(data.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        // no-op
        Ok(())
    }
}
