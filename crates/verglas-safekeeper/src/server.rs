//! PostgreSQL wire listener for Neon walproposer and pageserver clients. One
//! listener lives inside each cache-node process and maps timeline connections
//! onto [`crate::EcAppendLog`] instances backed by the shared EC ring.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::Mutex;
use verglas_core::read::ObjectRead;
use verglas_core::write::ObjectWrite;

use crate::broker::proto::{
    SafekeeperTimelineInfo, TenantTimelineId, broker_service_client::BrokerServiceClient,
};
use crate::log::SEGMENT_TARGET;
use crate::protocol::{
    AcceptorGreeting, AcceptorMessage, AppendResponse, Membership, ProposerMessage, ProtocolError,
    SafekeeperCommand, TermSwitch, VoteResponse, parse_command, parse_proposer, serialize_acceptor,
};
use crate::{
    AppendError, AppendGeometry, AppendLog, EcAppendLog, Epoch, FragmentTransport, LiveMembership,
    Lsn,
};

/// PostgreSQL protocol version 3.0.
const PG_PROTOCOL_V3: u32 = 196_608;
/// PostgreSQL SSLRequest code. The VXLAN listener answers `N` and continues
/// with the startup packet; tenant network isolation is the transport boundary.
const PG_SSL_REQUEST: u32 = 80_877_103;
/// Maximum bytes returned in one physical replication `XLogData` frame.
// Match the append-log's immutable segment size. Flushed WAL reads fetch one
// complete origin object, so a smaller replication frame would redownload the
// same object for every slice during pageserver catch-up.
const REPLICATION_CHUNK: u64 = SEGMENT_TARGET;
/// Delay between attempts to drain committed EC WAL into object storage.
const WAL_DRAIN_INTERVAL: Duration = Duration::from_secs(1);
const BROKER_PUBLISH_INTERVAL: Duration = Duration::from_secs(1);
const BROKER_RETRY_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Clone)]
struct BrokerConfig {
    endpoint: String,
    advertise_pg_addr: String,
}

/// One Neon tenant/timeline identity.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct TimelineKey {
    /// Neon tenant id.
    tenant_id: String,
    /// Neon timeline id.
    timeline_id: String,
}

impl TimelineKey {
    /// Builds a validated key from two Neon hexadecimal ids.
    fn new(tenant_id: String, timeline_id: String) -> Result<Self, ServerError> {
        validate_id("tenant", &tenant_id)?;
        validate_id("timeline", &timeline_id)?;
        Ok(Self {
            tenant_id,
            timeline_id,
        })
    }
}

/// Failure serving a Neon safekeeper connection.
#[derive(Debug, thiserror::Error)]
pub enum ServerError {
    /// Socket or PostgreSQL framing I/O failed.
    #[error("safekeeper I/O: {0}")]
    Io(#[from] std::io::Error),
    /// The Neon proposer/acceptor payload was invalid.
    #[error(transparent)]
    Protocol(#[from] ProtocolError),
    /// The EC WAL store rejected an operation.
    #[error(transparent)]
    Storage(#[from] AppendError),
    /// Startup/query state was incomplete or inconsistent.
    #[error("safekeeper connection: {0}")]
    Connection(String),
}

/// One cache-node safekeeper service, shared by every accepted connection.
pub struct SafekeeperServer<S> {
    /// Numeric identity returned in acceptor greetings.
    node_id: u64,
    /// Origin store used after WAL segments drain from EC fragments.
    origin: Arc<S>,
    /// Origin bucket containing WAL objects.
    bucket: String,
    /// Tenant-scoped prefix before tenant/timeline ids.
    prefix: String,
    /// Shared cache-node fragment transport.
    transport: Arc<dyn FragmentTransport>,
    /// Shared cache-node live ring membership.
    membership: Arc<dyn LiveMembership>,
    /// Local fast-restart state root.
    state_dir: PathBuf,
    /// WAL erasure geometry.
    geometry: AppendGeometry,
    /// Open timelines, created lazily on first connection.
    timelines: Mutex<HashMap<TimelineKey, Arc<EcAppendLog<S>>>>,
    /// Optional Neon storage-broker publication required for pageserver WAL
    /// receiver discovery. Connections retry forever because the broker lives
    /// in the dependent Postgres VM and may start after the cache.
    broker: Option<BrokerConfig>,
}

impl<S> SafekeeperServer<S>
where
    S: ObjectRead + ObjectWrite + 'static,
{
    /// Builds a safekeeper over the cache node's existing origin and ring plane.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u64,
        origin: Arc<S>,
        bucket: impl Into<String>,
        prefix: impl Into<String>,
        transport: Arc<dyn FragmentTransport>,
        membership: Arc<dyn LiveMembership>,
        state_dir: impl AsRef<Path>,
        geometry: AppendGeometry,
    ) -> Arc<Self> {
        Arc::new(Self {
            node_id,
            origin,
            bucket: bucket.into(),
            prefix: prefix.into(),
            transport,
            membership,
            state_dir: state_dir.as_ref().to_path_buf(),
            geometry,
            timelines: Mutex::new(HashMap::new()),
            broker: None,
        })
    }

    /// Publishes every opened timeline to Neon's storage broker. The advertised
    /// address must be reachable from the Postgres microVM over the tenant VXLAN.
    #[must_use]
    pub fn with_broker(
        mut self: Arc<Self>,
        endpoint: impl Into<String>,
        advertise_pg_addr: impl Into<String>,
    ) -> Arc<Self> {
        Arc::get_mut(&mut self)
            .expect("with_broker must be called before cloning the server")
            .broker = Some(BrokerConfig {
            endpoint: endpoint.into(),
            advertise_pg_addr: advertise_pg_addr.into(),
        });
        self
    }

    /// Accepts connections until the listener fails. Each connection is
    /// independent; a malformed client is logged and closed without stopping
    /// the cache-node process.
    pub async fn serve(self: Arc<Self>, listener: TcpListener) -> Result<(), ServerError> {
        if let Some(config) = self.broker.clone() {
            let server = Arc::clone(&self);
            tokio::spawn(async move { server.publish_broker(config).await });
        }
        loop {
            let (stream, peer) = listener.accept().await?;
            let server = Arc::clone(&self);
            tokio::spawn(async move {
                if let Err(error) = server.serve_connection(stream).await {
                    tracing::warn!(%peer, %error, "safekeeper connection closed");
                }
            });
        }
    }

    async fn publish_broker(self: Arc<Self>, config: BrokerConfig) {
        loop {
            let server = Arc::clone(&self);
            let advertise_pg_addr = config.advertise_pg_addr.clone();
            let outbound = async_stream::stream! {
                loop {
                    let timelines = server.timelines.lock().await.clone();
                    for (key, timeline) in timelines {
                        let state = timeline.safekeeper_state().await;
                        yield SafekeeperTimelineInfo {
                            safekeeper_id: server.node_id,
                            tenant_timeline_id: Some(TenantTimelineId {
                                tenant_id: decode_hex_id(&key.tenant_id),
                                timeline_id: decode_hex_id(&key.timeline_id),
                            }),
                            term: state.term,
                            last_log_term: state.term_history.last().map_or(0, |entry| entry.0),
                            flush_lsn: state.flush_lsn.0,
                            commit_lsn: state.commit_lsn.0,
                            backup_lsn: state.backup_lsn.0,
                            remote_consistent_lsn: state.remote_consistent_lsn.0,
                            peer_horizon_lsn: state.truncate_lsn.0,
                            local_start_lsn: state.local_start_lsn.0,
                            standby_horizon: 0,
                            safekeeper_connstr: advertise_pg_addr.clone(),
                            http_connstr: String::new(),
                            https_connstr: None,
                            availability_zone: Some("verglas".to_owned()),
                        };
                    }
                    tokio::time::sleep(BROKER_PUBLISH_INTERVAL).await;
                }
            };
            match BrokerServiceClient::connect(config.endpoint.clone()).await {
                Ok(mut client) => {
                    tracing::info!(endpoint = %config.endpoint, "publishing safekeeper timelines to storage broker");
                    if let Err(error) = client
                        .publish_safekeeper_info(tonic::Request::new(outbound))
                        .await
                    {
                        tracing::warn!(endpoint = %config.endpoint, %error, "storage broker publisher disconnected");
                    }
                }
                Err(error) => {
                    tracing::debug!(endpoint = %config.endpoint, %error, "storage broker unavailable; retrying");
                }
            }
            tokio::time::sleep(BROKER_RETRY_INTERVAL).await;
        }
    }

    /// Opens a timeline and recovers a newer coordinator state from the ring.
    async fn timeline(&self, key: &TimelineKey) -> Result<Arc<EcAppendLog<S>>, ServerError> {
        if let Some(existing) = self.timelines.lock().await.get(key).cloned() {
            return Ok(existing);
        }
        let dir = self.state_dir.join(&key.tenant_id).join(&key.timeline_id);
        let prefix = format!(
            "{}/{}/{}",
            self.prefix.trim_matches('/'),
            key.tenant_id,
            key.timeline_id
        );
        let log = Arc::new(EcAppendLog::open(
            self.node_id,
            Arc::clone(&self.origin),
            self.bucket.clone(),
            prefix,
            Arc::clone(&self.transport),
            Arc::clone(&self.membership),
            dir,
            self.geometry,
        )?);
        log.recover_from_ring().await?;
        let mut timelines = self.timelines.lock().await;
        let (timeline, inserted) = match timelines.entry(key.clone()) {
            std::collections::hash_map::Entry::Occupied(entry) => (entry.get().clone(), false),
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert(Arc::clone(&log));
                (log, true)
            }
        };
        drop(timelines);
        if inserted {
            spawn_wal_drain(key.clone(), Arc::clone(&timeline));
        }
        Ok(timeline)
    }

    /// Handles one PostgreSQL connection from startup through termination.
    async fn serve_connection(&self, mut stream: TcpStream) -> Result<(), ServerError> {
        let startup = read_startup(&mut stream).await?;
        write_startup_ok(&mut stream).await?;
        let startup_key = timeline_from_startup(&startup)?;

        loop {
            let Some((tag, payload)) = read_frontend(&mut stream).await? else {
                return Ok(());
            };
            match tag {
                b'Q' => {
                    let query = payload_cstr(&payload, "query")?;
                    let command = parse_command(query)?;
                    match command {
                        SafekeeperCommand::TimelineCreate { start_lsn } => {
                            let key = startup_key.clone().ok_or_else(|| {
                                ServerError::Connection(
                                    "TIMELINE_CREATE needs tenant_id and timeline_id startup options"
                                        .to_owned(),
                                )
                            })?;
                            let timeline = self.timeline(&key).await?;
                            timeline.initialize_timeline(start_lsn).await?;
                            write_command_complete(&mut stream, "TIMELINE_CREATE").await?;
                            write_backend(&mut stream, b'Z', b"I").await?;
                        }
                        SafekeeperCommand::StartWalPush {
                            protocol_version,
                            allow_timeline_creation: _,
                        } => {
                            write_copy_both(&mut stream).await?;
                            return self
                                .receive_wal(&mut stream, startup_key, protocol_version)
                                .await;
                        }
                        SafekeeperCommand::StartReplication { start_lsn, term } => {
                            let key = startup_key.clone().ok_or_else(|| {
                                ServerError::Connection(
                                    "START_REPLICATION needs tenant_id and timeline_id startup options"
                                        .to_owned(),
                                )
                            })?;
                            let timeline = self.timeline(&key).await?;
                            if let Some(term) = term
                                && term < timeline.epoch().0
                            {
                                return Err(ServerError::Connection(format!(
                                    "requested stale term {term}; current is {}",
                                    timeline.epoch().0
                                )));
                            }
                            write_copy_both(&mut stream).await?;
                            return send_wal(&mut stream, timeline, start_lsn).await;
                        }
                        SafekeeperCommand::IdentifySystem => {
                            let key = startup_key.clone().ok_or_else(|| {
                                ServerError::Connection(
                                    "IDENTIFY_SYSTEM needs timeline startup options".to_owned(),
                                )
                            })?;
                            let timeline = self.timeline(&key).await?;
                            write_identify_system(&mut stream, &timeline).await?;
                        }
                        SafekeeperCommand::TimelineStatus => {
                            let key = startup_key.clone().ok_or_else(|| {
                                ServerError::Connection(
                                    "TIMELINE_STATUS needs timeline startup options".to_owned(),
                                )
                            })?;
                            let timeline = self.timeline(&key).await?;
                            write_timeline_status(&mut stream, &timeline).await?;
                        }
                    }
                }
                b'X' => return Ok(()),
                _ => {
                    return Err(ServerError::Connection(format!(
                        "unexpected PostgreSQL frontend tag {tag:#x}"
                    )));
                }
            }
        }
    }

    /// Runs Neon's proposer/acceptor exchange inside PostgreSQL `COPY BOTH`.
    async fn receive_wal(
        &self,
        stream: &mut TcpStream,
        startup_key: Option<TimelineKey>,
        protocol_version: u32,
    ) -> Result<(), ServerError> {
        let mut timeline: Option<Arc<EcAppendLog<S>>> = None;
        let mut membership: Option<Membership> = None;
        loop {
            let Some((tag, payload)) = read_frontend(stream).await? else {
                return Ok(());
            };
            match tag {
                b'd' => match parse_proposer(payload, protocol_version)? {
                    ProposerMessage::Greeting(greeting) => {
                        let key = TimelineKey::new(
                            greeting.tenant_id.clone(),
                            greeting.timeline_id.clone(),
                        )?;
                        if let Some(startup_key) = &startup_key
                            && startup_key != &key
                        {
                            return Err(ServerError::Connection(
                                "startup and greeting name different timelines".to_owned(),
                            ));
                        }
                        let opened = self.timeline(&key).await?;
                        opened
                            .configure_timeline(
                                greeting.membership.generation,
                                greeting.system_id,
                                greeting.pg_version,
                                greeting.wal_segment_size,
                            )
                            .await?;
                        let state = opened.safekeeper_state().await;
                        write_copy_data(
                            stream,
                            serialize_acceptor(
                                &AcceptorMessage::Greeting(AcceptorGreeting {
                                    node_id: self.node_id,
                                    membership: greeting.membership.clone(),
                                    term: state.term,
                                }),
                                protocol_version,
                            )?,
                        )
                        .await?;
                        membership = Some(greeting.membership);
                        timeline = Some(opened);
                    }
                    ProposerMessage::Vote(vote) => {
                        let timeline = required_timeline(&timeline)?;
                        let vote_given = timeline.accept_vote(vote.generation, vote.term).await?;
                        let state = timeline.safekeeper_state().await;
                        write_copy_data(
                            stream,
                            serialize_acceptor(
                                &AcceptorMessage::Vote(VoteResponse {
                                    generation: state.generation,
                                    term: state.term,
                                    vote_given,
                                    flush_lsn: state.flush_lsn,
                                    truncate_lsn: state.truncate_lsn,
                                    term_history: state
                                        .term_history
                                        .into_iter()
                                        .map(|(term, lsn)| TermSwitch { term, lsn })
                                        .collect(),
                                }),
                                protocol_version,
                            )?,
                        )
                        .await?;
                    }
                    ProposerMessage::Elected(elected) => {
                        let timeline = required_timeline(&timeline)?;
                        timeline
                            .announce_elected(
                                elected.generation,
                                elected.term,
                                elected.start_streaming_at,
                                elected
                                    .term_history
                                    .into_iter()
                                    .map(|entry| (entry.term, entry.lsn))
                                    .collect(),
                            )
                            .await?;
                    }
                    ProposerMessage::Append(append) => {
                        let timeline = required_timeline(&timeline)?;
                        if membership.as_ref().map(|set| set.generation) != Some(append.generation)
                        {
                            return Err(ServerError::Connection(format!(
                                "append generation {} differs from greeting",
                                append.generation
                            )));
                        }
                        let state = timeline
                            .append_with_watermarks(
                                Epoch(append.term),
                                append.begin_lsn,
                                append.wal,
                                append.commit_lsn,
                                append.truncate_lsn,
                            )
                            .await?;
                        write_copy_data(
                            stream,
                            serialize_acceptor(
                                &AcceptorMessage::Append(AppendResponse {
                                    generation: state.generation,
                                    term: state.term,
                                    flush_lsn: state.flush_lsn,
                                    commit_lsn: state.commit_lsn,
                                }),
                                protocol_version,
                            )?,
                        )
                        .await?;
                    }
                },
                b'c' | b'X' => return Ok(()),
                other => {
                    return Err(ServerError::Connection(format!(
                        "unexpected COPY frontend tag {other:#x}"
                    )));
                }
            }
        }
    }
}

/// Runs the asynchronous EC-to-object-storage drain for one open timeline.
/// Failed origin writes retain the EC fragments and are retried; they never
/// alter the already-issued durability acknowledgement.
fn spawn_wal_drain<S>(key: TimelineKey, timeline: Arc<EcAppendLog<S>>)
where
    S: ObjectRead + ObjectWrite + 'static,
{
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(WAL_DRAIN_INTERVAL);
        interval.tick().await;
        loop {
            interval.tick().await;
            match timeline.flush().await {
                Ok(flushed) => {
                    let state = timeline.safekeeper_state().await;
                    // A proposer peer horizon only says other safekeepers no
                    // longer need this WAL. Retain it until the pageserver also
                    // reports the prefix durable remotely and the local backup
                    // has completed. Truncating on peer_horizon alone races the
                    // pageserver's first replication stream and loses WAL.
                    // (#47 stopped that race; this uses pageserver feedback.)
                    let retention_lsn = retention_lsn(&state, flushed);
                    if retention_lsn.0 > state.local_start_lsn.0
                        && let Err(error) = timeline.truncate(retention_lsn).await
                    {
                        tracing::warn!(
                            tenant_id = %key.tenant_id,
                            timeline_id = %key.timeline_id,
                            %error,
                            "failed to truncate drained safekeeper WAL"
                        );
                    }
                }
                Err(error) => tracing::warn!(
                    tenant_id = %key.tenant_id,
                    timeline_id = %key.timeline_id,
                    %error,
                    "failed to drain safekeeper WAL to object storage"
                ),
            }
        }
    });
}

fn retention_lsn(state: &crate::SafekeeperState, flushed: Lsn) -> Lsn {
    Lsn(state
        .truncate_lsn
        .0
        .min(state.remote_consistent_lsn.0)
        .min(state.commit_lsn.0)
        .min(flushed.0))
}

/// Returns the active timeline or an ordering error.
fn required_timeline<S>(
    timeline: &Option<Arc<EcAppendLog<S>>>,
) -> Result<&Arc<EcAppendLog<S>>, ServerError> {
    timeline
        .as_ref()
        .ok_or_else(|| ServerError::Connection("message arrived before greeting".to_owned()))
}

/// Validates Neon's fixed-width hexadecimal identity.
fn validate_id(kind: &str, value: &str) -> Result<(), ServerError> {
    if value.len() == 32 && value.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(ServerError::Connection(format!(
            "{kind} id must be 32 hexadecimal characters"
        )))
    }
}

fn decode_hex_id(value: &str) -> Vec<u8> {
    value
        .as_bytes()
        .chunks_exact(2)
        .map(|pair| (hex_nibble(pair[0]) << 4) | hex_nibble(pair[1]))
        .collect()
}

fn hex_nibble(byte: u8) -> u8 {
    match byte {
        b'0'..=b'9' => byte - b'0',
        b'a'..=b'f' => byte - b'a' + 10,
        b'A'..=b'F' => byte - b'A' + 10,
        _ => unreachable!("timeline ids are validated before broker publication"),
    }
}

/// Reads startup packets, declining SSL when libpq probes for it.
async fn read_startup(stream: &mut TcpStream) -> Result<HashMap<String, String>, ServerError> {
    loop {
        let len = stream.read_u32().await? as usize;
        if !(8..=1024 * 1024).contains(&len) {
            return Err(ServerError::Connection(format!(
                "invalid startup length {len}"
            )));
        }
        let mut payload = vec![0_u8; len - 4];
        stream.read_exact(&mut payload).await?;
        let mut payload = Bytes::from(payload);
        let code = payload.get_u32();
        if code == PG_SSL_REQUEST {
            stream.write_all(b"N").await?;
            continue;
        }
        if code != PG_PROTOCOL_V3 {
            return Err(ServerError::Connection(format!(
                "unsupported PostgreSQL protocol {code}"
            )));
        }
        let mut params = HashMap::new();
        while !payload.is_empty() && payload[0] != 0 {
            let key = take_cstr(&mut payload, "startup key")?;
            let value = take_cstr(&mut payload, "startup value")?;
            params.insert(key, value);
        }
        return Ok(params);
    }
}

/// Extracts timeline ids from libpq startup options when present.
fn timeline_from_startup(
    params: &HashMap<String, String>,
) -> Result<Option<TimelineKey>, ServerError> {
    let mut tenant = params
        .get("tenant_id")
        .or_else(|| params.get("ztenantid"))
        .cloned();
    let mut timeline = params
        .get("timeline_id")
        .or_else(|| params.get("ztimelineid"))
        .cloned();
    if let Some(options) = params.get("options") {
        let fields: Vec<&str> = options.split_whitespace().collect();
        for field in fields {
            let field = field.trim_start_matches("-c");
            let Some((key, value)) = field.split_once('=').or_else(|| field.split_once(':')) else {
                continue;
            };
            match key {
                "tenant_id" | "ztenantid" => tenant = Some(value.to_owned()),
                "timeline_id" | "ztimelineid" => timeline = Some(value.to_owned()),
                _ => {}
            }
        }
    }
    match (tenant, timeline) {
        (None, None) => Ok(None),
        (Some(tenant), Some(timeline)) => TimelineKey::new(tenant, timeline).map(Some),
        _ => Err(ServerError::Connection(
            "startup supplied only one of tenant_id/timeline_id".to_owned(),
        )),
    }
}

/// Sends trust authentication success and the minimum PostgreSQL startup state.
async fn write_startup_ok(stream: &mut TcpStream) -> Result<(), ServerError> {
    write_backend(stream, b'R', &0_u32.to_be_bytes()).await?;
    write_parameter(stream, "server_version", "16.0").await?;
    write_parameter(stream, "client_encoding", "UTF8").await?;
    let mut backend_key = BytesMut::new();
    backend_key.put_u32(std::process::id());
    backend_key.put_u32(0);
    write_backend(stream, b'K', &backend_key).await?;
    write_backend(stream, b'Z', b"I").await
}

/// Sends one PostgreSQL ParameterStatus message.
async fn write_parameter(
    stream: &mut TcpStream,
    key: &str,
    value: &str,
) -> Result<(), ServerError> {
    let mut payload = BytesMut::new();
    payload.put_slice(key.as_bytes());
    payload.put_u8(0);
    payload.put_slice(value.as_bytes());
    payload.put_u8(0);
    write_backend(stream, b'S', &payload).await
}

/// Reads one tagged frontend message, or `None` at a clean EOF.
async fn read_frontend(stream: &mut TcpStream) -> Result<Option<(u8, Bytes)>, ServerError> {
    let tag = match stream.read_u8().await {
        Ok(tag) => tag,
        Err(error) if error.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let len = stream.read_u32().await? as usize;
    if !(4..=16 * 1024 * 1024).contains(&len) {
        return Err(ServerError::Connection(format!(
            "invalid frontend message length {len}"
        )));
    }
    let mut payload = vec![0_u8; len - 4];
    stream.read_exact(&mut payload).await?;
    Ok(Some((tag, Bytes::from(payload))))
}

/// Sends one tagged PostgreSQL backend message.
async fn write_backend(stream: &mut TcpStream, tag: u8, payload: &[u8]) -> Result<(), ServerError> {
    stream.write_u8(tag).await?;
    stream.write_u32((payload.len() + 4) as u32).await?;
    stream.write_all(payload).await?;
    stream.flush().await?;
    Ok(())
}

/// Enters PostgreSQL copy-both mode with no column descriptors.
async fn write_copy_both(stream: &mut TcpStream) -> Result<(), ServerError> {
    write_backend(stream, b'W', &[0, 0, 0]).await
}

/// Sends one proposer/acceptor or replication CopyData payload.
async fn write_copy_data(stream: &mut TcpStream, payload: Bytes) -> Result<(), ServerError> {
    write_backend(stream, b'd', &payload).await
}

/// Streams WAL from the EC/origin log using PostgreSQL physical replication
/// `XLogData` frames, then stays attached for future appends and feedback.
async fn send_wal<S>(
    stream: &mut TcpStream,
    timeline: Arc<EcAppendLog<S>>,
    mut position: Lsn,
) -> Result<(), ServerError>
where
    S: ObjectRead + ObjectWrite + 'static,
{
    loop {
        let tail = timeline.tail();
        if position.0 < tail.0 {
            let end = Lsn((position.0 + REPLICATION_CHUNK).min(tail.0));
            let wal = timeline.read(position, end).await?;
            let mut payload = BytesMut::with_capacity(25 + wal.len());
            payload.put_u8(b'w');
            payload.put_u64(position.0);
            payload.put_u64(tail.0);
            payload.put_i64(0);
            payload.put_slice(&wal);
            write_copy_data(stream, payload.freeze()).await?;
            position = end;
            continue;
        }

        match tokio::time::timeout(Duration::from_secs(1), read_frontend(stream)).await {
            Ok(Ok(Some((b'X' | b'c', _)))) | Ok(Ok(None)) => return Ok(()),
            Ok(Ok(Some((b'd', feedback)))) => {
                if let Some(remote_consistent_lsn) = parse_pageserver_feedback(&feedback)? {
                    timeline
                        .record_remote_consistent_lsn(remote_consistent_lsn)
                        .await?;
                }
            }
            Ok(Ok(Some((_tag, _feedback)))) => {}
            Ok(Err(error)) => return Err(error),
            Err(_) => {
                let mut keepalive = BytesMut::with_capacity(18);
                keepalive.put_u8(b'k');
                keepalive.put_u64(timeline.tail().0);
                keepalive.put_i64(0);
                keepalive.put_u8(0);
                write_copy_data(stream, keepalive.freeze()).await?;
            }
        }
    }
}

/// Parses the pageserver's extensible `z` feedback frame and returns its
/// remotely durable apply LSN. Unknown fields remain forward-compatible.
fn parse_pageserver_feedback(payload: &Bytes) -> Result<Option<Lsn>, ServerError> {
    if payload.first() != Some(&b'z') {
        return Ok(None);
    }
    if payload.len() < 10 {
        return Err(ServerError::Connection(
            "truncated pageserver feedback".to_owned(),
        ));
    }
    let mut fields = payload.slice(9..);
    if !fields.has_remaining() {
        return Err(ServerError::Connection(
            "pageserver feedback has no fields".to_owned(),
        ));
    }
    let count = fields.get_u8();
    let mut remote_consistent_lsn = None;
    for _ in 0..count {
        let key = take_cstr(&mut fields, "pageserver feedback key")?;
        if fields.remaining() < 4 {
            return Err(ServerError::Connection(
                "truncated pageserver feedback length".to_owned(),
            ));
        }
        let len = fields.get_u32() as usize;
        if fields.remaining() < len {
            return Err(ServerError::Connection(
                "truncated pageserver feedback value".to_owned(),
            ));
        }
        let mut value = fields.split_to(len);
        if key == "ps_applylsn" {
            if len != 8 {
                return Err(ServerError::Connection(
                    "invalid ps_applylsn length".to_owned(),
                ));
            }
            remote_consistent_lsn = Some(Lsn(value.get_u64()));
        }
    }
    Ok(remote_consistent_lsn.filter(|lsn| *lsn != Lsn(0)))
}

/// Sends Neon's `IDENTIFY_SYSTEM` row followed by ReadyForQuery.
async fn write_identify_system<S>(
    stream: &mut TcpStream,
    timeline: &EcAppendLog<S>,
) -> Result<(), ServerError>
where
    S: ObjectRead + ObjectWrite,
{
    let state = timeline.safekeeper_state().await;
    write_row_description(
        stream,
        &[
            ("systemid", 25, -1),
            ("timeline", 23, 4),
            ("xlogpos", 25, -1),
            ("dbname", 25, -1),
        ],
    )
    .await?;
    write_data_row(
        stream,
        &[
            Some(state.system_id.to_string()),
            Some("1".to_owned()),
            Some(format_lsn(state.commit_lsn)),
            None,
        ],
    )
    .await?;
    write_command_complete(stream, "IDENTIFY_SYSTEM").await?;
    write_backend(stream, b'Z', b"I").await
}

/// Sends Neon's `TIMELINE_STATUS` row followed by ReadyForQuery.
async fn write_timeline_status<S>(
    stream: &mut TcpStream,
    timeline: &EcAppendLog<S>,
) -> Result<(), ServerError>
where
    S: ObjectRead + ObjectWrite,
{
    let state = timeline.safekeeper_state().await;
    write_row_description(stream, &[("flush_lsn", 25, -1), ("commit_lsn", 25, -1)]).await?;
    write_data_row(
        stream,
        &[
            Some(format_lsn(state.flush_lsn)),
            Some(format_lsn(state.commit_lsn)),
        ],
    )
    .await?;
    write_command_complete(stream, "TIMELINE_STATUS").await?;
    write_backend(stream, b'Z', b"I").await
}

/// Sends a PostgreSQL RowDescription for text/int fields.
async fn write_row_description(
    stream: &mut TcpStream,
    fields: &[(&str, u32, i16)],
) -> Result<(), ServerError> {
    let mut payload = BytesMut::new();
    payload.put_u16(fields.len() as u16);
    for (name, oid, len) in fields {
        payload.put_slice(name.as_bytes());
        payload.put_u8(0);
        payload.put_u32(0);
        payload.put_i16(0);
        payload.put_u32(*oid);
        payload.put_i16(*len);
        payload.put_i32(-1);
        payload.put_i16(0);
    }
    write_backend(stream, b'T', &payload).await
}

/// Sends one PostgreSQL DataRow.
async fn write_data_row(
    stream: &mut TcpStream,
    fields: &[Option<String>],
) -> Result<(), ServerError> {
    let mut payload = BytesMut::new();
    payload.put_u16(fields.len() as u16);
    for field in fields {
        match field {
            Some(value) => {
                payload.put_i32(value.len() as i32);
                payload.put_slice(value.as_bytes());
            }
            None => payload.put_i32(-1),
        }
    }
    write_backend(stream, b'D', &payload).await
}

/// Sends PostgreSQL CommandComplete.
async fn write_command_complete(stream: &mut TcpStream, command: &str) -> Result<(), ServerError> {
    let mut payload = BytesMut::new();
    payload.put_slice(command.as_bytes());
    payload.put_u8(0);
    write_backend(stream, b'C', &payload).await
}

/// Reads a NUL-terminated UTF-8 string from a protocol payload.
fn take_cstr(payload: &mut Bytes, field: &str) -> Result<String, ServerError> {
    let end = payload
        .iter()
        .position(|byte| *byte == 0)
        .ok_or_else(|| ServerError::Connection(format!("{field} lacks NUL terminator")))?;
    let bytes = payload.split_to(end);
    payload.advance(1);
    std::str::from_utf8(&bytes)
        .map(str::to_owned)
        .map_err(|error| ServerError::Connection(format!("{field} is not UTF-8: {error}")))
}

/// Borrows one NUL-terminated query without its terminator.
fn payload_cstr<'a>(payload: &'a Bytes, field: &str) -> Result<&'a str, ServerError> {
    let bytes = payload
        .strip_suffix(&[0])
        .ok_or_else(|| ServerError::Connection(format!("{field} lacks NUL terminator")))?;
    std::str::from_utf8(bytes)
        .map_err(|error| ServerError::Connection(format!("{field} is not UTF-8: {error}")))
}

/// Renders an LSN using PostgreSQL's `HIGH/LOW` hexadecimal convention.
fn format_lsn(lsn: Lsn) -> String {
    format!("{:X}/{:08X}", lsn.0 >> 32, lsn.0 & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::SafekeeperState;

    fn state(peer: u64, remote: u64, commit: u64, backup: u64) -> SafekeeperState {
        SafekeeperState {
            system_id: 1,
            pg_version: 17,
            wal_segment_size: 16 * 1024 * 1024,
            generation: 0,
            term: 1,
            flush_lsn: Lsn(0x9000),
            commit_lsn: Lsn(commit),
            truncate_lsn: Lsn(peer),
            backup_lsn: Lsn(backup),
            remote_consistent_lsn: Lsn(remote),
            local_start_lsn: Lsn(0x1000),
            term_history: vec![(1, Lsn(0x1000))],
        }
    }

    #[test]
    fn proposer_horizon_cannot_truncate_before_pageserver_feedback() {
        assert_eq!(
            retention_lsn(&state(0x8000, 0, 0x8000, 0x8000), Lsn(0x8000)),
            Lsn(0)
        );
    }

    #[test]
    fn retention_uses_the_slowest_durability_watermark() {
        assert_eq!(
            retention_lsn(&state(0x8000, 0x5000, 0x7000, 0x6000), Lsn(0x6000)),
            Lsn(0x5000),
        );
    }

    #[test]
    fn parses_remote_consistent_lsn_from_neon_feedback() {
        let mut fields = BytesMut::new();
        fields.put_u8(2);
        fields.put_slice(b"ps_writelsn\0");
        fields.put_u32(8);
        fields.put_u64(0x7000);
        fields.put_slice(b"ps_applylsn\0");
        fields.put_u32(8);
        fields.put_u64(0x5000);

        let mut frame = BytesMut::new();
        frame.put_u8(b'z');
        frame.put_u64(fields.len() as u64);
        frame.extend_from_slice(&fields);
        assert_eq!(
            parse_pageserver_feedback(&frame.freeze()).expect("valid pageserver feedback"),
            Some(Lsn(0x5000))
        );
    }
}
