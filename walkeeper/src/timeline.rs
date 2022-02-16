//! This module contains timeline id -> safekeeper state map with file-backed
//! persistence and support for interaction between sending and receiving wal.

use anyhow::{Context, Result};

use lazy_static::lazy_static;

use std::cmp::{max, min};
use std::collections::HashMap;
use std::fs::{self};

use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use tokio::sync::mpsc::UnboundedSender;
use tracing::*;

use zenith_utils::lsn::Lsn;
use zenith_utils::zid::ZTenantTimelineId;

use crate::callmemaybe::{CallmeEvent, SubscriptionStateKey};
use crate::control_file::{self, CreateControlFile};

use crate::safekeeper::{
    AcceptorProposerMessage, ProposerAcceptorMessage, SafeKeeper, SafeKeeperState,
};
use crate::send_wal::HotStandbyFeedback;
use crate::wal_storage;
use crate::SafeKeeperConf;

use zenith_utils::pq_proto::ZenithFeedback;

const POLL_STATE_TIMEOUT: Duration = Duration::from_secs(1);

/// Replica status update + hot standby feedback
#[derive(Debug, Clone, Copy)]
pub struct ReplicaState {
    /// last known lsn received by replica
    pub last_received_lsn: Lsn, // None means we don't know
    /// combined remote consistent lsn of pageservers
    pub remote_consistent_lsn: Lsn,
    /// combined hot standby feedback from all replicas
    pub hs_feedback: HotStandbyFeedback,
    /// Zenith specific feedback received from pageserver, if any
    pub zenith_feedback: Option<ZenithFeedback>,
}

impl Default for ReplicaState {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplicaState {
    pub fn new() -> ReplicaState {
        ReplicaState {
            last_received_lsn: Lsn::MAX,
            remote_consistent_lsn: Lsn(0),
            hs_feedback: HotStandbyFeedback {
                ts: 0,
                xmin: u64::MAX,
                catalog_xmin: u64::MAX,
            },
            zenith_feedback: None,
        }
    }
}

/// Shared state associated with database instance
struct SharedState {
    /// Safekeeper object
    sk: SafeKeeper<control_file::FileStorage>,
    /// For receiving-sending wal cooperation
    /// quorum commit LSN we've notified walsenders about
    notified_commit_lsn: Lsn,
    /// State of replicas
    replicas: Vec<Option<ReplicaState>>,
    /// Inactive clusters shouldn't occupy any resources, so timeline is
    /// activated whenever there is a compute connection or pageserver is not
    /// caughtup (it must have latest WAL for new compute start) and suspended
    /// otherwise.
    ///
    /// TODO: it might be better to remove tli completely from GlobalTimelines
    /// when tli is inactive instead of having this flag.
    active: bool,
    num_computes: u32,
    pageserver_connstr: Option<String>,
}

impl SharedState {
    /// Restore SharedState from control file.
    /// If create=false and file doesn't exist, bails out.
    fn create_restore(
        conf: &SafeKeeperConf,
        zttid: &ZTenantTimelineId,
        create: CreateControlFile,
    ) -> Result<Self> {
        let state = control_file::FileStorage::load_control_file_conf(conf, zttid, create)
            .context("failed to load from control file")?;

        let control_store = control_file::FileStorage::new(zttid, conf);

        let wal_store = wal_storage::PhysicalStorage::new(zttid, conf);

        info!("timeline {} created or restored", zttid.timeline_id);

        Ok(Self {
            notified_commit_lsn: Lsn(0),
            sk: SafeKeeper::new(zttid.timeline_id, control_store, wal_store, state),
            replicas: Vec::new(),
            active: false,
            num_computes: 0,
            pageserver_connstr: None,
        })
    }

    /// Activate the timeline: start/change walsender (via callmemaybe).
    fn activate(
        &mut self,
        zttid: &ZTenantTimelineId,
        pageserver_connstr: Option<&String>,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        if let Some(ref pageserver_connstr) = self.pageserver_connstr {
            // unsub old sub. xxx: callmemaybe is going out
            let old_subscription_key = SubscriptionStateKey::new(
                zttid.tenant_id,
                zttid.timeline_id,
                pageserver_connstr.to_owned(),
            );
            callmemaybe_tx
                .send(CallmeEvent::Unsubscribe(old_subscription_key))
                .unwrap_or_else(|e| {
                    error!("failed to send Pause request to callmemaybe thread {}", e);
                });
        }
        if let Some(pageserver_connstr) = pageserver_connstr {
            let subscription_key = SubscriptionStateKey::new(
                zttid.tenant_id,
                zttid.timeline_id,
                pageserver_connstr.to_owned(),
            );
            // xx: sending to channel under lock is not very cool, but
            // shouldn't be a problem here. If it is, we can grab a counter
            // here and later augment channel messages with it.
            callmemaybe_tx
                .send(CallmeEvent::Subscribe(subscription_key))
                .unwrap_or_else(|e| {
                    error!(
                        "failed to send Subscribe request to callmemaybe thread {}",
                        e
                    );
                });
            info!(
                "timeline {} is subscribed to callmemaybe to {}",
                zttid.timeline_id, pageserver_connstr
            );
        }
        self.pageserver_connstr = pageserver_connstr.map(|c| c.to_owned());
        self.active = true;
        Ok(())
    }

    /// Deactivate the timeline: stop callmemaybe.
    fn deactivate(
        &mut self,
        zttid: &ZTenantTimelineId,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        if self.active {
            if let Some(ref pageserver_connstr) = self.pageserver_connstr {
                let subscription_key = SubscriptionStateKey::new(
                    zttid.tenant_id,
                    zttid.timeline_id,
                    pageserver_connstr.to_owned(),
                );
                callmemaybe_tx
                    .send(CallmeEvent::Unsubscribe(subscription_key))
                    .unwrap_or_else(|e| {
                        error!(
                            "failed to send Unsubscribe request to callmemaybe thread {}",
                            e
                        );
                    });
                info!(
                    "timeline {} is unsubscribed from callmemaybe to {}",
                    zttid.timeline_id,
                    self.pageserver_connstr.as_ref().unwrap()
                );
            }
            self.active = false;
        }
        Ok(())
    }

    /// Get combined state of all alive replicas
    pub fn get_replicas_state(&self) -> ReplicaState {
        let mut acc = ReplicaState::new();
        for state in self.replicas.iter().flatten() {
            acc.hs_feedback.ts = max(acc.hs_feedback.ts, state.hs_feedback.ts);
            acc.hs_feedback.xmin = min(acc.hs_feedback.xmin, state.hs_feedback.xmin);
            acc.hs_feedback.catalog_xmin =
                min(acc.hs_feedback.catalog_xmin, state.hs_feedback.catalog_xmin);

            // FIXME
            // If multiple pageservers are streaming WAL and send feedback for the same timeline simultaneously,
            // this code is not correct.
            // Now the most advanced feedback is used.
            // If one pageserver lags when another doesn't, the backpressure won't be activated on compute and lagging
            // pageserver is prone to timeout errors.
            //
            // To choose what feedback to use and resend to compute node,
            // we need to know which pageserver compute node considers to be main.
            // See https://github.com/zenithdb/zenith/issues/1171
            //
            if let Some(zenith_feedback) = state.zenith_feedback {
                if let Some(acc_feedback) = acc.zenith_feedback {
                    if acc_feedback.ps_writelsn < zenith_feedback.ps_writelsn {
                        warn!("More than one pageserver is streaming WAL for the timeline. Feedback resolving is not fully supported yet.");
                        acc.zenith_feedback = Some(zenith_feedback);
                    }
                } else {
                    acc.zenith_feedback = Some(zenith_feedback);
                }

                // last lsn received by pageserver
                // FIXME if multiple pageservers are streaming WAL, last_received_lsn must be tracked per pageserver.
                // See https://github.com/zenithdb/zenith/issues/1171
                acc.last_received_lsn = Lsn::from(zenith_feedback.ps_writelsn);

                // When at least one pageserver has preserved data up to remote_consistent_lsn,
                // safekeeper is free to delete it, so choose max of all pageservers.
                acc.remote_consistent_lsn = max(
                    Lsn::from(zenith_feedback.ps_applylsn),
                    acc.remote_consistent_lsn,
                );
            }
        }
        acc
    }

    /// Assign new replica ID. We choose first empty cell in the replicas vector
    /// or extend the vector if there are no free slots.
    pub fn add_replica(&mut self, state: ReplicaState) -> usize {
        if let Some(pos) = self.replicas.iter().position(|r| r.is_none()) {
            self.replicas[pos] = Some(state);
            return pos;
        }
        let pos = self.replicas.len();
        self.replicas.push(Some(state));
        pos
    }
}

/// Database instance (tenant)
pub struct Timeline {
    pub zttid: ZTenantTimelineId,
    mutex: Mutex<SharedState>,
    /// conditional variable used to notify wal senders
    cond: Condvar,
}

impl Timeline {
    fn new(zttid: ZTenantTimelineId, shared_state: SharedState) -> Timeline {
        Timeline {
            zttid,
            mutex: Mutex::new(shared_state),
            cond: Condvar::new(),
        }
    }

    /// Register compute connection, starting timeline-related activity if it is
    /// not running yet.
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_connect(
        &self,
        pageserver_connstr: Option<&String>,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.num_computes += 1;
        // FIXME: currently we always adopt latest pageserver connstr, but we
        // should have kind of generations assigned by compute to distinguish
        // the latest one or even pass it through consensus to reliably deliver
        // to all safekeepers.
        shared_state.activate(&self.zttid, pageserver_connstr, callmemaybe_tx)?;
        Ok(())
    }

    /// De-register compute connection, shutting down timeline activity if
    /// pageserver doesn't need catchup.
    /// Can fail only if channel to a static thread got closed, which is not normal at all.
    pub fn on_compute_disconnect(
        &self,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<()> {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.num_computes -= 1;
        // If there is no pageserver, can suspend right away; otherwise let
        // walsender do that.
        if shared_state.num_computes == 0 && shared_state.pageserver_connstr.is_none() {
            shared_state.deactivate(&self.zttid, callmemaybe_tx)?;
        }
        Ok(())
    }

    /// Deactivate tenant if there is no computes and pageserver is caughtup,
    /// assuming the pageserver status is in replica_id.
    /// Returns true if deactivated.
    pub fn check_deactivate(
        &self,
        replica_id: usize,
        callmemaybe_tx: &UnboundedSender<CallmeEvent>,
    ) -> Result<bool> {
        let mut shared_state = self.mutex.lock().unwrap();
        if !shared_state.active {
            // already suspended
            return Ok(true);
        }
        if shared_state.num_computes == 0 {
            let replica_state = shared_state.replicas[replica_id].unwrap();
            let deactivate = shared_state.notified_commit_lsn == Lsn(0) || // no data at all yet
            (replica_state.last_received_lsn != Lsn::MAX && // Lsn::MAX means that we don't know the latest LSN yet.
             replica_state.last_received_lsn >= shared_state.sk.commit_lsn);
            if deactivate {
                shared_state.deactivate(&self.zttid, callmemaybe_tx)?;
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Timed wait for an LSN to be committed.
    ///
    /// Returns the last committed LSN, which will be at least
    /// as high as the LSN waited for, or None if timeout expired.
    ///
    pub fn wait_for_lsn(&self, lsn: Lsn) -> Option<Lsn> {
        let mut shared_state = self.mutex.lock().unwrap();
        loop {
            let commit_lsn = shared_state.notified_commit_lsn;
            // This must be `>`, not `>=`.
            if commit_lsn > lsn {
                return Some(commit_lsn);
            }
            let result = self
                .cond
                .wait_timeout(shared_state, POLL_STATE_TIMEOUT)
                .unwrap();
            if result.1.timed_out() {
                return None;
            }
            shared_state = result.0
        }
    }

    // Notify caught-up WAL senders about new WAL data received
    pub fn notify_wal_senders(&self, commit_lsn: Lsn) {
        let mut shared_state = self.mutex.lock().unwrap();
        if shared_state.notified_commit_lsn < commit_lsn {
            shared_state.notified_commit_lsn = commit_lsn;
            self.cond.notify_all();
        }
    }

    /// Pass arrived message to the safekeeper.
    pub fn process_msg(
        &self,
        msg: &ProposerAcceptorMessage,
    ) -> Result<Option<AcceptorProposerMessage>> {
        let mut rmsg: Option<AcceptorProposerMessage>;
        let commit_lsn: Lsn;
        {
            let mut shared_state = self.mutex.lock().unwrap();
            rmsg = shared_state.sk.process_msg(msg)?;
            // locally available commit lsn. flush_lsn can be smaller than
            // commit_lsn if we are catching up safekeeper.
            commit_lsn = shared_state.sk.commit_lsn;

            // if this is AppendResponse, fill in proper hot standby feedback and disk consistent lsn
            if let Some(AcceptorProposerMessage::AppendResponse(ref mut resp)) = rmsg {
                let state = shared_state.get_replicas_state();
                resp.hs_feedback = state.hs_feedback;
                if let Some(zenith_feedback) = state.zenith_feedback {
                    resp.zenith_feedback = zenith_feedback;
                }
            }
        }
        // Ping wal sender that new data might be available.
        self.notify_wal_senders(commit_lsn);
        Ok(rmsg)
    }

    pub fn get_info(&self) -> SafeKeeperState {
        self.mutex.lock().unwrap().sk.s.clone()
    }

    pub fn add_replica(&self, state: ReplicaState) -> usize {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.add_replica(state)
    }

    pub fn update_replica_state(&self, id: usize, state: ReplicaState) {
        let mut shared_state = self.mutex.lock().unwrap();
        shared_state.replicas[id] = Some(state);
    }

    pub fn remove_replica(&self, id: usize) {
        let mut shared_state = self.mutex.lock().unwrap();
        assert!(shared_state.replicas[id].is_some());
        shared_state.replicas[id] = None;
    }

    pub fn get_end_of_wal(&self) -> Lsn {
        let shared_state = self.mutex.lock().unwrap();
        shared_state.sk.wal_store.flush_lsn()
    }
}

// Utilities needed by various Connection-like objects
pub trait TimelineTools {
    fn set(
        &mut self,
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        create: CreateControlFile,
    ) -> Result<()>;

    fn get(&self) -> &Arc<Timeline>;
}

impl TimelineTools for Option<Arc<Timeline>> {
    fn set(
        &mut self,
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        create: CreateControlFile,
    ) -> Result<()> {
        // We will only set the timeline once. If it were to ever change,
        // anyone who cloned the Arc would be out of date.
        assert!(self.is_none());
        *self = Some(GlobalTimelines::get(conf, zttid, create)?);
        Ok(())
    }

    fn get(&self) -> &Arc<Timeline> {
        self.as_ref().unwrap()
    }
}

lazy_static! {
    pub static ref TIMELINES: Mutex<HashMap<ZTenantTimelineId, Arc<Timeline>>> =
        Mutex::new(HashMap::new());
}

/// A zero-sized struct used to manage access to the global timelines map.
pub struct GlobalTimelines;

impl GlobalTimelines {
    /// Get a timeline with control file loaded from the global TIMELINES map.
    /// If control file doesn't exist and create=false, bails out.
    pub fn get(
        conf: &SafeKeeperConf,
        zttid: ZTenantTimelineId,
        create: CreateControlFile,
    ) -> Result<Arc<Timeline>> {
        let mut timelines = TIMELINES.lock().unwrap();

        match timelines.get(&zttid) {
            Some(result) => Ok(Arc::clone(result)),
            None => {
                if let CreateControlFile::True = create {
                    let dir = conf.timeline_dir(&zttid);
                    info!(
                        "creating timeline dir {}, create is {:?}",
                        dir.display(),
                        create
                    );
                    fs::create_dir_all(dir)?;
                }

                let shared_state = SharedState::create_restore(conf, &zttid, create)
                    .context("failed to restore shared state")?;

                let new_tli = Arc::new(Timeline::new(zttid, shared_state));
                timelines.insert(zttid, Arc::clone(&new_tli));
                Ok(new_tli)
            }
        }
    }
}
