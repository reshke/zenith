#[cfg(test)]
mod tests;

use crate::{
    auth::{self, backend::AuthSuccess},
    cancellation::{self, CancelMap},
    compute::{self, PostgresConnection},
    config::{AuthenticationConfig, ProxyConfig, TlsConfig},
    console::{self, errors::WakeComputeError, messages::MetricsAuxInfo, Api},
    http::StatusCode,
    protocol2::WithClientIp,
    stream::{PqStream, Stream},
    usage_metrics::{Ids, USAGE_METRICS},
};
use anyhow::{bail, Context};
use async_trait::async_trait;
use futures::TryFutureExt;
use itertools::Itertools;
use metrics::{exponential_buckets, register_int_counter_vec, IntCounterVec};
use once_cell::sync::{Lazy, OnceCell};
use pq_proto::{BeMessage as Be, FeStartupPacket, StartupMessageParams};
use prometheus::{register_histogram_vec, HistogramVec};
use regex::Regex;
use std::{error::Error, io, ops::ControlFlow, sync::Arc, time::Instant};
use tokio::{
    io::{AsyncRead, AsyncWrite, AsyncWriteExt},
    time,
};
use tokio_util::sync::CancellationToken;
use tracing::{error, info, info_span, warn, Instrument};
use utils::measured_stream::MeasuredStream;

/// Number of times we should retry the `/proxy_wake_compute` http request.
/// Retry duration is BASE_RETRY_WAIT_DURATION * RETRY_WAIT_EXPONENT_BASE ^ n, where n starts at 0
pub const NUM_RETRIES_CONNECT: u32 = 16;
const CONNECT_TIMEOUT: time::Duration = time::Duration::from_secs(2);
const BASE_RETRY_WAIT_DURATION: time::Duration = time::Duration::from_millis(25);
const RETRY_WAIT_EXPONENT_BASE: f64 = std::f64::consts::SQRT_2;

const ERR_INSECURE_CONNECTION: &str = "connection is insecure (try using `sslmode=require`)";
const ERR_PROTO_VIOLATION: &str = "protocol violation";

pub static NUM_DB_CONNECTIONS_OPENED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_opened_db_connections_total",
        "Number of opened connections to a database.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_DB_CONNECTIONS_CLOSED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_closed_db_connections_total",
        "Number of closed connections to a database.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CLIENT_CONNECTION_OPENED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_opened_client_connections_total",
        "Number of opened connections from a client.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CLIENT_CONNECTION_CLOSED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_closed_client_connections_total",
        "Number of closed connections from a client.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CONNECTIONS_ACCEPTED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_accepted_connections_total",
        "Number of client connections accepted.",
        &["protocol"],
    )
    .unwrap()
});

pub static NUM_CONNECTIONS_CLOSED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_closed_connections_total",
        "Number of client connections closed.",
        &["protocol"],
    )
    .unwrap()
});

static COMPUTE_CONNECTION_LATENCY: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec!(
        "proxy_compute_connection_latency_seconds",
        "Time it took for proxy to establish a connection to the compute endpoint",
        // http/ws/tcp, true/false, true/false, success/failure
        // 3 * 2 * 2 * 2 = 24 counters
        &["protocol", "cache_miss", "pool_miss", "outcome"],
        // largest bucket = 2^16 * 0.5ms = 32s
        exponential_buckets(0.0005, 2.0, 16).unwrap(),
    )
    .unwrap()
});

pub struct LatencyTimer {
    // time since the stopwatch was started
    start: Option<Instant>,
    // accumulated time on the stopwatch
    accumulated: std::time::Duration,
    // label data
    protocol: &'static str,
    cache_miss: bool,
    pool_miss: bool,
    outcome: &'static str,
}

pub struct LatencyTimerPause<'a> {
    timer: &'a mut LatencyTimer,
}

impl LatencyTimer {
    pub fn new(protocol: &'static str) -> Self {
        Self {
            start: Some(Instant::now()),
            accumulated: std::time::Duration::ZERO,
            protocol,
            cache_miss: false,
            // by default we don't do pooling
            pool_miss: true,
            // assume failed unless otherwise specified
            outcome: "failed",
        }
    }

    pub fn pause(&mut self) -> LatencyTimerPause<'_> {
        // stop the stopwatch and record the time that we have accumulated
        let start = self.start.take().expect("latency timer should be started");
        self.accumulated += start.elapsed();
        LatencyTimerPause { timer: self }
    }

    pub fn cache_miss(&mut self) {
        self.cache_miss = true;
    }

    pub fn pool_hit(&mut self) {
        self.pool_miss = false;
    }

    pub fn success(mut self) {
        self.outcome = "success";
    }
}

impl Drop for LatencyTimerPause<'_> {
    fn drop(&mut self) {
        // start the stopwatch again
        self.timer.start = Some(Instant::now());
    }
}

impl Drop for LatencyTimer {
    fn drop(&mut self) {
        let duration =
            self.start.map(|start| start.elapsed()).unwrap_or_default() + self.accumulated;
        COMPUTE_CONNECTION_LATENCY
            .with_label_values(&[
                self.protocol,
                bool_to_str(self.cache_miss),
                bool_to_str(self.pool_miss),
                self.outcome,
            ])
            .observe(duration.as_secs_f64())
    }
}

static NUM_CONNECTION_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_connection_failures_total",
        "Number of connection failures (per kind).",
        &["kind"],
    )
    .unwrap()
});

static NUM_WAKEUP_FAILURES: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_connection_failures_breakdown",
        "Number of wake-up failures (per kind).",
        &["retry", "kind"],
    )
    .unwrap()
});

static NUM_BYTES_PROXIED_PER_CLIENT_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_io_bytes_per_client",
        "Number of bytes sent/received between client and backend.",
        crate::console::messages::MetricsAuxInfo::TRAFFIC_LABELS,
    )
    .unwrap()
});

static NUM_BYTES_PROXIED_COUNTER: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec!(
        "proxy_io_bytes",
        "Number of bytes sent/received between all clients and backends.",
        &["direction"],
    )
    .unwrap()
});

pub async fn task_main(
    config: &'static ProxyConfig,
    listener: tokio::net::TcpListener,
    cancellation_token: CancellationToken,
) -> anyhow::Result<()> {
    scopeguard::defer! {
        info!("proxy has shut down");
    }

    // When set for the server socket, the keepalive setting
    // will be inherited by all accepted client sockets.
    socket2::SockRef::from(&listener).set_keepalive(true)?;

    let mut connections = tokio::task::JoinSet::new();
    let cancel_map = Arc::new(CancelMap::default());

    loop {
        tokio::select! {
            accept_result = listener.accept() => {
                let (socket, _) = accept_result?;

                let session_id = uuid::Uuid::new_v4();
                let cancel_map = Arc::clone(&cancel_map);
                connections.spawn(
                    async move {
                        info!("accepted postgres client connection");

                        let mut socket = WithClientIp::new(socket);
                        if let Some(ip) = socket.wait_for_addr().await? {
                            tracing::Span::current().record("peer_addr", &tracing::field::display(ip));
                        } else if config.require_client_ip {
                            bail!("missing required client IP");
                        }

                        socket
                            .inner
                            .set_nodelay(true)
                            .context("failed to set socket option")?;

                        handle_client(config, &cancel_map, session_id, socket, ClientMode::Tcp).await
                    }
                    .instrument(info_span!("handle_client", ?session_id, peer_addr = tracing::field::Empty))
                    .unwrap_or_else(move |e| {
                        // Acknowledge that the task has finished with an error.
                        error!(?session_id, "per-client task finished with an error: {e:#}");
                    }),
                );
            }
            Some(Err(e)) = connections.join_next(), if !connections.is_empty() => {
                if !e.is_panic() && !e.is_cancelled() {
                    warn!("unexpected error from joined connection task: {e:?}");
                }
            }
            _ = cancellation_token.cancelled() => {
                drop(listener);
                break;
            }
        }
    }
    // Drain connections
    while let Some(res) = connections.join_next().await {
        if let Err(e) = res {
            if !e.is_panic() && !e.is_cancelled() {
                warn!("unexpected error from joined connection task: {e:?}");
            }
        }
    }
    Ok(())
}

pub enum ClientMode {
    Tcp,
    Websockets { hostname: Option<String> },
}

/// Abstracts the logic of handling TCP vs WS clients
impl ClientMode {
    fn protocol_label(&self) -> &'static str {
        match self {
            ClientMode::Tcp => "tcp",
            ClientMode::Websockets { .. } => "ws",
        }
    }

    fn allow_cleartext(&self) -> bool {
        match self {
            ClientMode::Tcp => false,
            ClientMode::Websockets { .. } => true,
        }
    }

    fn allow_self_signed_compute(&self, config: &ProxyConfig) -> bool {
        match self {
            ClientMode::Tcp => config.allow_self_signed_compute,
            ClientMode::Websockets { .. } => false,
        }
    }

    fn hostname<'a, S>(&'a self, s: &'a Stream<S>) -> Option<&'a str> {
        match self {
            ClientMode::Tcp => s.sni_hostname(),
            ClientMode::Websockets { hostname } => hostname.as_deref(),
        }
    }

    fn handshake_tls<'a>(&self, tls: Option<&'a TlsConfig>) -> Option<&'a TlsConfig> {
        match self {
            ClientMode::Tcp => tls,
            // TLS is None here if using websockets, because the connection is already encrypted.
            ClientMode::Websockets { .. } => None,
        }
    }
}

pub async fn handle_client<S: AsyncRead + AsyncWrite + Unpin>(
    config: &'static ProxyConfig,
    cancel_map: &CancelMap,
    session_id: uuid::Uuid,
    stream: S,
    mode: ClientMode,
) -> anyhow::Result<()> {
    info!(
        protocol = mode.protocol_label(),
        "handling interactive connection from client"
    );

    let proto = mode.protocol_label();
    NUM_CLIENT_CONNECTION_OPENED_COUNTER
        .with_label_values(&[proto])
        .inc();
    NUM_CONNECTIONS_ACCEPTED_COUNTER
        .with_label_values(&[proto])
        .inc();
    scopeguard::defer! {
        NUM_CLIENT_CONNECTION_CLOSED_COUNTER.with_label_values(&[proto]).inc();
        NUM_CONNECTIONS_CLOSED_COUNTER.with_label_values(&[proto]).inc();
    }

    let tls = config.tls_config.as_ref();

    let do_handshake = handshake(stream, mode.handshake_tls(tls), cancel_map);
    let (mut stream, params) = match do_handshake.await? {
        Some(x) => x,
        None => return Ok(()), // it's a cancellation request
    };

    // Extract credentials which we're going to use for auth.
    let creds = {
        let hostname = mode.hostname(stream.get_ref());
        let common_names = tls.and_then(|tls| tls.common_names.clone());
        let result = config
            .auth_backend
            .as_ref()
            .map(|_| auth::ClientCredentials::parse(&params, hostname, common_names))
            .transpose();

        match result {
            Ok(creds) => creds,
            Err(e) => stream.throw_error(e).await?,
        }
    };

    let client = Client::new(
        stream,
        creds,
        &params,
        session_id,
        mode.allow_self_signed_compute(config),
    );
    cancel_map
        .with_session(|session| client.connect_to_db(session, mode, &config.authentication_config))
        .await
}

/// Establish a (most probably, secure) connection with the client.
/// For better testing experience, `stream` can be any object satisfying the traits.
/// It's easier to work with owned `stream` here as we need to upgrade it to TLS;
/// we also take an extra care of propagating only the select handshake errors to client.
#[tracing::instrument(skip_all)]
async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
    stream: S,
    mut tls: Option<&TlsConfig>,
    cancel_map: &CancelMap,
) -> anyhow::Result<Option<(PqStream<Stream<S>>, StartupMessageParams)>> {
    // Client may try upgrading to each protocol only once
    let (mut tried_ssl, mut tried_gss) = (false, false);

    let mut stream = PqStream::new(Stream::from_raw(stream));
    loop {
        let msg = stream.read_startup_packet().await?;
        info!("received {msg:?}");

        use FeStartupPacket::*;
        match msg {
            SslRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_ssl => {
                    tried_ssl = true;

                    // We can't perform TLS handshake without a config
                    let enc = tls.is_some();
                    stream.write_message(&Be::EncryptionResponse(enc)).await?;
                    if let Some(tls) = tls.take() {
                        // Upgrade raw stream into a secure TLS-backed stream.
                        // NOTE: We've consumed `tls`; this fact will be used later.

                        let (raw, read_buf) = stream.into_inner();
                        // TODO: Normally, client doesn't send any data before
                        // server says TLS handshake is ok and read_buf is empy.
                        // However, you could imagine pipelining of postgres
                        // SSLRequest + TLS ClientHello in one hunk similar to
                        // pipelining in our node js driver. We should probably
                        // support that by chaining read_buf with the stream.
                        if !read_buf.is_empty() {
                            bail!("data is sent before server replied with EncryptionResponse");
                        }
                        stream = PqStream::new(raw.upgrade(tls.to_server_config()).await?);
                    }
                }
                _ => bail!(ERR_PROTO_VIOLATION),
            },
            GssEncRequest => match stream.get_ref() {
                Stream::Raw { .. } if !tried_gss => {
                    tried_gss = true;

                    // Currently, we don't support GSSAPI
                    stream.write_message(&Be::EncryptionResponse(false)).await?;
                }
                _ => bail!(ERR_PROTO_VIOLATION),
            },
            StartupMessage { params, .. } => {
                // Check that the config has been consumed during upgrade
                // OR we didn't provide it at all (for dev purposes).
                if tls.is_some() {
                    stream.throw_error_str(ERR_INSECURE_CONNECTION).await?;
                }

                info!(session_type = "normal", "successful handshake");
                break Ok(Some((stream, params)));
            }
            CancelRequest(cancel_key_data) => {
                cancel_map.cancel_session(cancel_key_data).await?;

                info!(session_type = "cancellation", "successful handshake");
                break Ok(None);
            }
        }
    }
}

/// If we couldn't connect, a cached connection info might be to blame
/// (e.g. the compute node's address might've changed at the wrong time).
/// Invalidate the cache entry (if any) to prevent subsequent errors.
#[tracing::instrument(name = "invalidate_cache", skip_all)]
pub fn invalidate_cache(node_info: console::CachedNodeInfo) -> compute::ConnCfg {
    let is_cached = node_info.cached();
    if is_cached {
        warn!("invalidating stalled compute node info cache entry");
    }
    let label = match is_cached {
        true => "compute_cached",
        false => "compute_uncached",
    };
    NUM_CONNECTION_FAILURES.with_label_values(&[label]).inc();

    node_info.invalidate().config
}

/// Try to connect to the compute node once.
#[tracing::instrument(name = "connect_once", skip_all)]
async fn connect_to_compute_once(
    node_info: &console::CachedNodeInfo,
    timeout: time::Duration,
) -> Result<PostgresConnection, compute::ConnectionError> {
    let allow_self_signed_compute = node_info.allow_self_signed_compute;

    node_info
        .config
        .connect(allow_self_signed_compute, timeout)
        .await
}

#[async_trait]
pub trait ConnectMechanism {
    type Connection;
    type ConnectError;
    type Error: From<Self::ConnectError>;
    async fn connect_once(
        &self,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<Self::Connection, Self::ConnectError>;

    fn update_connect_config(&self, conf: &mut compute::ConnCfg);
}

pub struct TcpMechanism<'a> {
    /// KV-dictionary with PostgreSQL connection params.
    pub params: &'a StartupMessageParams,
}

#[async_trait]
impl ConnectMechanism for TcpMechanism<'_> {
    type Connection = PostgresConnection;
    type ConnectError = compute::ConnectionError;
    type Error = compute::ConnectionError;

    async fn connect_once(
        &self,
        node_info: &console::CachedNodeInfo,
        timeout: time::Duration,
    ) -> Result<PostgresConnection, Self::Error> {
        connect_to_compute_once(node_info, timeout).await
    }

    fn update_connect_config(&self, config: &mut compute::ConnCfg) {
        config.set_startup_params(self.params);
    }
}

const fn bool_to_str(x: bool) -> &'static str {
    if x {
        "true"
    } else {
        "false"
    }
}

fn report_error(e: &WakeComputeError, retry: bool) {
    use crate::console::errors::ApiError;
    let retry = bool_to_str(retry);
    let kind = match e {
        WakeComputeError::BadComputeAddress(_) => "bad_compute_address",
        WakeComputeError::ApiError(ApiError::Transport(_)) => "api_transport_error",
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ref text,
        }) if text.contains("written data quota exceeded")
            || text.contains("the limit for current plan reached") =>
        {
            "quota_exceeded"
        }
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::LOCKED,
            ..
        }) => "api_console_locked",
        WakeComputeError::ApiError(ApiError::Console {
            status: StatusCode::BAD_REQUEST,
            ..
        }) => "api_console_bad_request",
        WakeComputeError::ApiError(ApiError::Console { status, .. })
            if status.is_server_error() =>
        {
            "api_console_other_server_error"
        }
        WakeComputeError::ApiError(ApiError::Console { .. }) => "api_console_other_error",
        WakeComputeError::TimeoutError => "timeout_error",
    };
    NUM_WAKEUP_FAILURES.with_label_values(&[retry, kind]).inc();
}

/// Try to connect to the compute node, retrying if necessary.
/// This function might update `node_info`, so we take it by `&mut`.
#[tracing::instrument(skip_all)]
pub async fn connect_to_compute<M: ConnectMechanism>(
    mechanism: &M,
    mut node_info: console::CachedNodeInfo,
    extra: &console::ConsoleReqExtra<'_>,
    creds: &auth::BackendType<'_, auth::ClientCredentials<'_>>,
    mut latency_timer: LatencyTimer,
) -> Result<M::Connection, M::Error>
where
    M::ConnectError: ShouldRetry + std::fmt::Debug,
    M::Error: From<WakeComputeError>,
{
    mechanism.update_connect_config(&mut node_info.config);

    // try once
    let (config, err) = match mechanism.connect_once(&node_info, CONNECT_TIMEOUT).await {
        Ok(res) => {
            latency_timer.success();
            return Ok(res);
        }
        Err(e) => {
            error!(error = ?e, "could not connect to compute node");
            (invalidate_cache(node_info), e)
        }
    };

    latency_timer.cache_miss();

    let mut num_retries = 1;

    // if we failed to connect, it's likely that the compute node was suspended, wake a new compute node
    info!("compute node's state has likely changed; requesting a wake-up");
    let node_info = loop {
        let wake_res = match creds {
            auth::BackendType::Console(api, creds) => api.wake_compute(extra, creds).await,
            auth::BackendType::Postgres(api, creds) => api.wake_compute(extra, creds).await,
            // nothing to do?
            auth::BackendType::Link(_) => return Err(err.into()),
            // test backend
            auth::BackendType::Test(x) => x.wake_compute(),
        };

        match handle_try_wake(wake_res, num_retries) {
            Err(e) => {
                error!(error = ?e, num_retries, retriable = false, "couldn't wake compute node");
                report_error(&e, false);
                return Err(e.into());
            }
            // failed to wake up but we can continue to retry
            Ok(ControlFlow::Continue(e)) => {
                report_error(&e, true);
                warn!(error = ?e, num_retries, retriable = true, "couldn't wake compute node");
            }
            // successfully woke up a compute node and can break the wakeup loop
            Ok(ControlFlow::Break(mut node_info)) => {
                node_info.config.reuse_password(&config);
                mechanism.update_connect_config(&mut node_info.config);
                break node_info;
            }
        }

        let wait_duration = retry_after(num_retries);
        num_retries += 1;

        time::sleep(wait_duration).await;
    };

    // now that we have a new node, try connect to it repeatedly.
    // this can error for a few reasons, for instance:
    // * DNS connection settings haven't quite propagated yet
    info!("wake_compute success. attempting to connect");
    loop {
        match mechanism.connect_once(&node_info, CONNECT_TIMEOUT).await {
            Ok(res) => {
                latency_timer.success();
                return Ok(res);
            }
            Err(e) => {
                let retriable = e.should_retry(num_retries);
                if !retriable {
                    error!(error = ?e, num_retries, retriable, "couldn't connect to compute node");
                    return Err(e.into());
                }
                warn!(error = ?e, num_retries, retriable, "couldn't connect to compute node");
            }
        }

        let wait_duration = retry_after(num_retries);
        num_retries += 1;

        time::sleep(wait_duration).await;
    }
}

/// Attempts to wake up the compute node.
/// * Returns Ok(Continue(e)) if there was an error waking but retries are acceptable
/// * Returns Ok(Break(node)) if the wakeup succeeded
/// * Returns Err(e) if there was an error
pub fn handle_try_wake(
    result: Result<console::CachedNodeInfo, WakeComputeError>,
    num_retries: u32,
) -> Result<ControlFlow<console::CachedNodeInfo, WakeComputeError>, WakeComputeError> {
    match result {
        Err(err) => match &err {
            WakeComputeError::ApiError(api) if api.should_retry(num_retries) => {
                Ok(ControlFlow::Continue(err))
            }
            _ => Err(err),
        },
        // Ready to try again.
        Ok(new) => Ok(ControlFlow::Break(new)),
    }
}

pub trait ShouldRetry {
    fn could_retry(&self) -> bool;
    fn should_retry(&self, num_retries: u32) -> bool {
        match self {
            _ if num_retries >= NUM_RETRIES_CONNECT => false,
            err => err.could_retry(),
        }
    }
}

impl ShouldRetry for io::Error {
    fn could_retry(&self) -> bool {
        use std::io::ErrorKind;
        matches!(
            self.kind(),
            ErrorKind::ConnectionRefused | ErrorKind::AddrNotAvailable | ErrorKind::TimedOut
        )
    }
}

impl ShouldRetry for tokio_postgres::error::DbError {
    fn could_retry(&self) -> bool {
        use tokio_postgres::error::SqlState;
        matches!(
            self.code(),
            &SqlState::CONNECTION_FAILURE
                | &SqlState::CONNECTION_EXCEPTION
                | &SqlState::CONNECTION_DOES_NOT_EXIST
                | &SqlState::SQLCLIENT_UNABLE_TO_ESTABLISH_SQLCONNECTION,
        )
    }
}

impl ShouldRetry for tokio_postgres::Error {
    fn could_retry(&self) -> bool {
        if let Some(io_err) = self.source().and_then(|x| x.downcast_ref()) {
            io::Error::could_retry(io_err)
        } else if let Some(db_err) = self.source().and_then(|x| x.downcast_ref()) {
            tokio_postgres::error::DbError::could_retry(db_err)
        } else {
            false
        }
    }
}

impl ShouldRetry for compute::ConnectionError {
    fn could_retry(&self) -> bool {
        match self {
            compute::ConnectionError::Postgres(err) => err.could_retry(),
            compute::ConnectionError::CouldNotConnect(err) => err.could_retry(),
            _ => false,
        }
    }
}

pub fn retry_after(num_retries: u32) -> time::Duration {
    BASE_RETRY_WAIT_DURATION.mul_f64(RETRY_WAIT_EXPONENT_BASE.powi((num_retries as i32) - 1))
}

/// Finish client connection initialization: confirm auth success, send params, etc.
#[tracing::instrument(skip_all)]
async fn prepare_client_connection(
    node: &compute::PostgresConnection,
    reported_auth_ok: bool,
    session: cancellation::Session<'_>,
    stream: &mut PqStream<impl AsyncRead + AsyncWrite + Unpin>,
) -> anyhow::Result<()> {
    // Register compute's query cancellation token and produce a new, unique one.
    // The new token (cancel_key_data) will be sent to the client.
    let cancel_key_data = session.enable_query_cancellation(node.cancel_closure.clone());

    // Report authentication success if we haven't done this already.
    // Note that we do this only (for the most part) after we've connected
    // to a compute (see above) which performs its own authentication.
    if !reported_auth_ok {
        stream.write_message_noflush(&Be::AuthenticationOk)?;
    }

    // Forward all postgres connection params to the client.
    // Right now the implementation is very hacky and inefficent (ideally,
    // we don't need an intermediate hashmap), but at least it should be correct.
    for (name, value) in &node.params {
        // TODO: Theoretically, this could result in a big pile of params...
        stream.write_message_noflush(&Be::ParameterStatus {
            name: name.as_bytes(),
            value: value.as_bytes(),
        })?;
    }

    stream
        .write_message_noflush(&Be::BackendKeyData(cancel_key_data))?
        .write_message(&Be::ReadyForQuery)
        .await?;

    Ok(())
}

/// Forward bytes in both directions (client <-> compute).
#[tracing::instrument(skip_all)]
pub async fn proxy_pass(
    client: impl AsyncRead + AsyncWrite + Unpin,
    compute: impl AsyncRead + AsyncWrite + Unpin,
    aux: &MetricsAuxInfo,
) -> anyhow::Result<()> {
    let usage = USAGE_METRICS.register(Ids {
        endpoint_id: aux.endpoint_id.to_string(),
        branch_id: aux.branch_id.to_string(),
    });

    let m_sent = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["tx"]);
    let m_sent2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("tx"));
    let mut client = MeasuredStream::new(
        client,
        |_| {},
        |cnt| {
            // Number of bytes we sent to the client (outbound).
            m_sent.inc_by(cnt as u64);
            m_sent2.inc_by(cnt as u64);
            usage.record_egress(cnt as u64);
        },
    );

    let m_recv = NUM_BYTES_PROXIED_COUNTER.with_label_values(&["rx"]);
    let m_recv2 = NUM_BYTES_PROXIED_PER_CLIENT_COUNTER.with_label_values(&aux.traffic_labels("rx"));
    let mut compute = MeasuredStream::new(
        compute,
        |_| {},
        |cnt| {
            // Number of bytes the client sent to the compute node (inbound).
            m_recv.inc_by(cnt as u64);
            m_recv2.inc_by(cnt as u64);
        },
    );

    // Starting from here we only proxy the client's traffic.
    info!("performing the proxy pass...");
    let _ = tokio::io::copy_bidirectional(&mut client, &mut compute).await?;

    Ok(())
}

/// Thin connection context.
struct Client<'a, S> {
    /// The underlying libpq protocol stream.
    stream: PqStream<S>,
    /// Client credentials that we care about.
    creds: auth::BackendType<'a, auth::ClientCredentials<'a>>,
    /// KV-dictionary with PostgreSQL connection params.
    params: &'a StartupMessageParams,
    /// Unique connection ID.
    session_id: uuid::Uuid,
    /// Allow self-signed certificates (for testing).
    allow_self_signed_compute: bool,
}

impl<'a, S> Client<'a, S> {
    /// Construct a new connection context.
    fn new(
        stream: PqStream<S>,
        creds: auth::BackendType<'a, auth::ClientCredentials<'a>>,
        params: &'a StartupMessageParams,
        session_id: uuid::Uuid,
        allow_self_signed_compute: bool,
    ) -> Self {
        Self {
            stream,
            creds,
            params,
            session_id,
            allow_self_signed_compute,
        }
    }
}

impl<S: AsyncRead + AsyncWrite + Unpin> Client<'_, S> {
    /// Let the client authenticate and connect to the designated compute node.
    // Instrumentation logs endpoint name everywhere. Doesn't work for link
    // auth; strictly speaking we don't know endpoint name in its case.
    #[tracing::instrument(name = "", fields(ep = self.creds.get_endpoint().unwrap_or("".to_owned())), skip_all)]
    async fn connect_to_db(
        self,
        session: cancellation::Session<'_>,
        mode: ClientMode,
        config: &'static AuthenticationConfig,
    ) -> anyhow::Result<()> {
        let Self {
            mut stream,
            mut creds,
            params,
            session_id,
            allow_self_signed_compute,
        } = self;

        let console_options = neon_options(params);

        let extra = console::ConsoleReqExtra {
            session_id, // aka this connection's id
            application_name: params.get("application_name"),
            options: console_options.as_deref(),
        };

        let mut latency_timer = LatencyTimer::new(mode.protocol_label());

        let auth_result = match creds
            .authenticate(
                &extra,
                &mut stream,
                mode.allow_cleartext(),
                config,
                &mut latency_timer,
            )
            .await
        {
            Ok(auth_result) => auth_result,
            Err(e) => {
                let user = creds.get_user();
                let db = params.get("database");
                let app = params.get("application_name");
                let params_span = tracing::info_span!("", ?user, ?db, ?app);

                return stream.throw_error(e).instrument(params_span).await;
            }
        };

        let AuthSuccess {
            reported_auth_ok,
            value: mut node_info,
        } = auth_result;

        node_info.allow_self_signed_compute = allow_self_signed_compute;

        let aux = node_info.aux.clone();
        let mut node = connect_to_compute(
            &TcpMechanism { params },
            node_info,
            &extra,
            &creds,
            latency_timer,
        )
        .or_else(|e| stream.throw_error(e))
        .await?;

        let proto = mode.protocol_label();
        NUM_DB_CONNECTIONS_OPENED_COUNTER
            .with_label_values(&[proto])
            .inc();
        scopeguard::defer! {
            NUM_DB_CONNECTIONS_CLOSED_COUNTER.with_label_values(&[proto]).inc();
        }

        prepare_client_connection(&node, reported_auth_ok, session, &mut stream).await?;
        // Before proxy passing, forward to compute whatever data is left in the
        // PqStream input buffer. Normally there is none, but our serverless npm
        // driver in pipeline mode sends startup, password and first query
        // immediately after opening the connection.
        let (stream, read_buf) = stream.into_inner();
        node.stream.write_all(&read_buf).await?;
        proxy_pass(stream, node.stream, &aux).await
    }
}

pub fn neon_options(params: &StartupMessageParams) -> Option<String> {
    #[allow(unstable_name_collisions)]
    let options: String = params
        .options_raw()?
        .filter(|opt| is_neon_param(opt))
        .sorted() // we sort it to use as cache key
        .intersperse(" ") // TODO: use impl from std once it's stabilized
        .collect();

    // Don't even bother with empty options.
    if options.is_empty() {
        return None;
    }

    Some(options)
}

pub fn is_neon_param(bytes: &str) -> bool {
    static RE: OnceCell<Regex> = OnceCell::new();
    RE.get_or_init(|| Regex::new(r"^neon_\w+:").unwrap());

    RE.get().unwrap().is_match(bytes)
}
