//! Supervised single-replica Durable Object child process.

use std::error::Error;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use futures::StreamExt;
use object_store::aws::{AmazonS3Builder, S3ConditionalPut};
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, ObjectStoreExt};
use uuid::Uuid;
use verglas_do_engine::{
    CasCommitAuthority, CheckpointReceipt, LeaseGrant, LeaseIdentity,
    ObjectStoreCheckpointPublisher, ObjectStoreOffloadBatchArchive, OffloadBatchArchive,
    ReplicaCommitAuthority, ReplicaEndpoint, ReplicaEndpointRole, SqliteReplicaStore,
    TransactionEnvelope, UnixReplicaSink,
};

struct Config {
    do_id: String,
    replica_id: u64,
    role: ReplicaEndpointRole,
    socket: PathBuf,
    data_dir: PathBuf,
    replica_socket: Option<PathBuf>,
    lease_token: Option<String>,
    lease_generation: Option<u64>,
    start_sequence: Option<u64>,
    lease_etag: Option<String>,
    lease_version: Option<String>,
    cas_endpoint: Option<String>,
    cas_bucket: Option<String>,
    cas_prefix: Option<String>,
    cas_region: Option<String>,
    cas_access_key_id: Option<String>,
    cas_secret_access_key: Option<String>,
    offload_dir: Option<PathBuf>,
}

impl Config {
    /// Parses the exact argument set supplied by `celld-host`.
    fn parse() -> Result<Self, String> {
        let mut arguments = std::env::args().skip(1);
        let mut do_id = None;
        let mut replica_id = None;
        let mut role = None;
        let mut socket = None;
        let mut data_dir = None;
        let mut replica_socket = None;
        let mut lease_token = None;
        let mut lease_generation = None;
        let mut start_sequence = None;
        let mut lease_etag = None;
        let mut lease_version = None;
        let mut cas_endpoint = None;
        let mut cas_bucket = None;
        let mut cas_prefix = None;
        let mut cas_region = None;
        let mut cas_access_key_id = None;
        let mut cas_secret_access_key = None;
        let mut offload_dir = None;
        while let Some(argument) = arguments.next() {
            match argument.as_str() {
                "--do-id" => do_id = Some(next_value(&mut arguments, "--do-id")?),
                "--replica-id" => {
                    let value = next_value(&mut arguments, "--replica-id")?;
                    replica_id = Some(
                        value
                            .parse::<u64>()
                            .map_err(|_| "--replica-id must be an unsigned integer".to_owned())?,
                    );
                }
                "--role" => {
                    role = Some(match next_value(&mut arguments, "--role")?.as_str() {
                        "worker" => ReplicaEndpointRole::Worker,
                        "replica" => ReplicaEndpointRole::Replica,
                        _ => return Err("--role must be worker or replica".to_owned()),
                    });
                }
                "--socket" => {
                    socket = Some(PathBuf::from(next_value(&mut arguments, "--socket")?));
                }
                "--data-dir" => {
                    data_dir = Some(PathBuf::from(next_value(&mut arguments, "--data-dir")?));
                }
                "--replica-socket" => {
                    replica_socket = Some(PathBuf::from(next_value(
                        &mut arguments,
                        "--replica-socket",
                    )?));
                }
                "--lease-token" => {
                    lease_token = Some(next_value(&mut arguments, "--lease-token")?);
                }
                "--lease-generation" => {
                    lease_generation = Some(parse_u64_option(
                        next_value(&mut arguments, "--lease-generation")?,
                        "--lease-generation",
                    )?);
                }
                "--start-sequence" => {
                    start_sequence = Some(parse_u64_option(
                        next_value(&mut arguments, "--start-sequence")?,
                        "--start-sequence",
                    )?);
                }
                "--lease-etag" => {
                    lease_etag = Some(next_value(&mut arguments, "--lease-etag")?);
                }
                "--lease-version" => {
                    lease_version = Some(next_value(&mut arguments, "--lease-version")?);
                }
                "--cas-endpoint" => {
                    cas_endpoint = Some(next_value(&mut arguments, "--cas-endpoint")?);
                }
                "--cas-bucket" => {
                    cas_bucket = Some(next_value(&mut arguments, "--cas-bucket")?);
                }
                "--cas-prefix" => {
                    cas_prefix = Some(next_value(&mut arguments, "--cas-prefix")?);
                }
                "--cas-region" => {
                    cas_region = Some(next_value(&mut arguments, "--cas-region")?);
                }
                "--cas-access-key-id" => {
                    cas_access_key_id = Some(next_value(&mut arguments, "--cas-access-key-id")?);
                }
                "--cas-secret-access-key" => {
                    cas_secret_access_key =
                        Some(next_value(&mut arguments, "--cas-secret-access-key")?);
                }
                "--offload-dir" => {
                    offload_dir = Some(PathBuf::from(next_value(&mut arguments, "--offload-dir")?));
                }
                "--help" => return Err(usage().to_owned()),
                other => return Err(format!("unknown argument {other}\n{}", usage())),
            }
        }
        let config = Self {
            do_id: do_id.ok_or_else(|| "missing --do-id".to_owned())?,
            replica_id: replica_id.ok_or_else(|| "missing --replica-id".to_owned())?,
            role: role.ok_or_else(|| "missing --role".to_owned())?,
            socket: socket.ok_or_else(|| "missing --socket".to_owned())?,
            data_dir: data_dir.ok_or_else(|| "missing --data-dir".to_owned())?,
            replica_socket,
            lease_token,
            lease_generation,
            start_sequence,
            lease_etag,
            lease_version,
            cas_endpoint,
            cas_bucket,
            cas_prefix,
            cas_region,
            cas_access_key_id,
            cas_secret_access_key,
            offload_dir,
        };
        let replica_complete = config.replica_socket.is_some();
        let cas_any = config.cas_endpoint.is_some()
            || config.cas_bucket.is_some()
            || config.cas_prefix.is_some()
            || config.cas_region.is_some()
            || config.cas_access_key_id.is_some()
            || config.cas_secret_access_key.is_some()
            || config.lease_etag.is_some()
            || config.lease_version.is_some();
        let cas_complete = config.cas_endpoint.is_some()
            && config.cas_bucket.is_some()
            && config.cas_prefix.is_some()
            && config.cas_region.is_some()
            && config.cas_access_key_id.is_some()
            && config.cas_secret_access_key.is_some()
            && (config.lease_etag.is_some() || config.lease_version.is_some());
        if config.role == ReplicaEndpointRole::Worker
            && (config.lease_token.is_none()
                || config.lease_generation.is_none()
                || config.start_sequence.is_none()
                || (!replica_complete && !cas_complete)
                || (replica_complete && cas_any))
        {
            return Err(
                "worker requires either replica durability or complete managed CAS configuration"
                    .to_owned(),
            );
        }
        if config.role != ReplicaEndpointRole::Worker && cas_any {
            return Err("managed CAS configuration requires worker role".to_owned());
        }
        Ok(config)
    }

    /// Returns whether this worker carries the managed-CAS launch variant.
    fn is_managed_cas(&self) -> bool {
        self.cas_endpoint.is_some()
    }
}

/// Reads one required option value.
fn next_value(
    arguments: &mut impl Iterator<Item = String>,
    option: &str,
) -> Result<String, String> {
    arguments
        .next()
        .ok_or_else(|| format!("missing value for {option}"))
}

/// Parses one unsigned worker durability option.
fn parse_u64_option(value: String, option: &str) -> Result<u64, String> {
    value
        .parse::<u64>()
        .map_err(|_| format!("{option} must be an unsigned integer"))
}

/// Returns the child command line supplied by the host supervisor.
fn usage() -> &'static str {
    "usage: verglasd --do-id ID --replica-id N --role worker|replica --socket PATH --data-dir PATH [replica durability or --cas-endpoint URL --cas-bucket BUCKET --cas-prefix PREFIX --cas-region REGION --cas-access-key-id KEY --cas-secret-access-key SECRET --lease-token TOKEN --lease-generation N --start-sequence N (--lease-etag ETAG | --lease-version VERSION)]"
}

/// One immutable transaction object discovered below a managed DO prefix.
struct ManagedTransactionObject {
    sequence: u64,
    transaction_id: Uuid,
    path: ObjectPath,
}

/// One immutable checkpoint object discovered below a managed DO prefix.
struct ManagedCheckpointObject {
    sequence: u64,
    sha256: String,
    path: ObjectPath,
}

/// Builds the configured conditional S3 client without selecting a fallback store.
fn build_managed_store(config: &Config) -> Result<Arc<dyn ObjectStore>, Box<dyn Error>> {
    let endpoint = config
        .cas_endpoint
        .as_deref()
        .ok_or_else(|| std::io::Error::other("missing managed CAS endpoint"))?;
    let bucket = config
        .cas_bucket
        .as_deref()
        .ok_or_else(|| std::io::Error::other("missing managed CAS bucket"))?;
    let region = config
        .cas_region
        .as_deref()
        .ok_or_else(|| std::io::Error::other("missing managed CAS region"))?;
    let access_key_id = config
        .cas_access_key_id
        .as_deref()
        .ok_or_else(|| std::io::Error::other("missing managed CAS access key"))?;
    let secret_access_key = config
        .cas_secret_access_key
        .as_deref()
        .ok_or_else(|| std::io::Error::other("missing managed CAS secret key"))?;
    let store = AmazonS3Builder::new()
        .with_bucket_name(bucket)
        .with_region(region)
        .with_endpoint(endpoint)
        .with_allow_http(true)
        .with_virtual_hosted_style_request(false)
        .with_access_key_id(access_key_id)
        .with_secret_access_key(secret_access_key)
        .with_conditional_put(S3ConditionalPut::ETagMatch)
        .build()?;
    Ok(Arc::new(store))
}

/// Parses one immutable transaction object name and rejects malformed CAS paths.
fn parse_transaction_object(
    path: &ObjectPath,
) -> Result<Option<ManagedTransactionObject>, Box<dyn Error>> {
    let value = path.to_string();
    let name = value
        .rsplit('/')
        .next()
        .ok_or_else(|| std::io::Error::other("managed transaction path has no object name"))?;
    let Some(stem) = name.strip_suffix(".arrow") else {
        return Ok(None);
    };
    let (sequence, transaction_id) = stem.split_once('-').ok_or_else(|| {
        std::io::Error::other("managed transaction path has no sequence separator")
    })?;
    let sequence = sequence.parse::<u64>()?;
    let transaction_id = Uuid::parse_str(transaction_id)?;
    Ok(Some(ManagedTransactionObject {
        sequence,
        transaction_id,
        path: path.clone(),
    }))
}

/// Parses one immutable checkpoint object name and retains its hash identity.
fn parse_checkpoint_object(
    path: &ObjectPath,
) -> Result<Option<ManagedCheckpointObject>, Box<dyn Error>> {
    let value = path.to_string();
    let name = value
        .rsplit('/')
        .next()
        .ok_or_else(|| std::io::Error::other("managed checkpoint path has no object name"))?;
    let Some(stem) = name.strip_suffix(".sqlite") else {
        return Ok(None);
    };
    let (sequence, sha256) = stem.split_once('-').ok_or_else(|| {
        std::io::Error::other("managed checkpoint path has no sequence separator")
    })?;
    let sequence = sequence.parse::<u64>()?;
    if sha256.is_empty() {
        return Err("managed checkpoint path has an empty hash".into());
    }
    Ok(Some(ManagedCheckpointObject {
        sequence,
        sha256: sha256.to_owned(),
        path: path.clone(),
    }))
}

/// Discovers the newest checkpoint not ahead of the held replica sequence.
async fn latest_checkpoint(
    store: &dyn ObjectStore,
    prefix: &ObjectPath,
    held_sequence: u64,
) -> Result<Option<ManagedCheckpointObject>, Box<dyn Error>> {
    let mut checkpoints = store.list(Some(prefix));
    let mut latest = None;
    while let Some(metadata) = checkpoints.next().await {
        let metadata = metadata?;
        if let Some(candidate) = parse_checkpoint_object(&metadata.location)? {
            if candidate.sequence > held_sequence {
                return Err(format!(
                    "replica checkpoint sequence {} is ahead of held sequence {}",
                    candidate.sequence, held_sequence
                )
                .into());
            }
            if latest
                .as_ref()
                .is_none_or(|current: &ManagedCheckpointObject| {
                    candidate.sequence > current.sequence
                })
            {
                latest = Some(candidate);
            }
        }
    }
    Ok(latest)
}

/// Removes a stale SQLite image and its WAL sidecars before verified restore.
async fn remove_sqlite_image(destination: &Path) -> Result<(), Box<dyn Error>> {
    let mut wal = destination.as_os_str().to_os_string();
    wal.push("-wal");
    let mut shm = destination.as_os_str().to_os_string();
    shm.push("-shm");
    let sidecars = [
        destination.to_path_buf(),
        PathBuf::from(wal),
        PathBuf::from(shm),
    ];
    for sidecar in sidecars {
        match tokio::fs::remove_file(sidecar).await {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

/// Restores a fresh replica pager from a verified checkpoint and replays its service tail.
async fn recover_replica(
    checkpoint_store: Option<Arc<dyn ObjectStore>>,
    do_id: &str,
    destination: &Path,
    replica_socket: PathBuf,
    start_sequence: u64,
) -> Result<(Arc<SqliteReplicaStore>, Arc<UnixReplicaSink>), Box<dyn Error>> {
    let checkpoint = if let Some(store) = &checkpoint_store {
        latest_checkpoint(
            store.as_ref(),
            &ObjectPath::from("checkpoints")
                .join(do_id)
                .join("checkpoints"),
            start_sequence,
        )
        .await?
    } else {
        None
    };
    let checkpoint_available = checkpoint.is_some();
    let existing = if tokio::fs::try_exists(destination).await? {
        Some(Arc::new(SqliteReplicaStore::open(destination, do_id)?))
    } else {
        None
    };
    let restore_checkpoint = match (&existing, checkpoint.as_ref()) {
        (Some(current), Some(checkpoint)) => {
            current.state()?.applied_sequence() < checkpoint.sequence
        }
        (None, Some(_)) => true,
        _ => false,
    };
    let replica = if restore_checkpoint {
        drop(existing);
        remove_sqlite_image(destination).await?;
        let store = checkpoint_store.as_ref().ok_or_else(|| {
            std::io::Error::other("replica checkpoint store disappeared during recovery")
        })?;
        let checkpoint = checkpoint.ok_or_else(|| {
            std::io::Error::other("replica checkpoint disappeared during recovery")
        })?;
        let publisher = ObjectStoreCheckpointPublisher::new(store.clone(), "checkpoints");
        let receipt = CheckpointReceipt::new(
            checkpoint.sequence,
            checkpoint.path.to_string(),
            checkpoint.sha256,
        )?;
        let restored = publisher.restore(do_id, &receipt, destination).await?;
        restored.mark_checkpointed(receipt.through_sequence(), receipt.object_path())?;
        Arc::new(restored)
    } else if let Some(existing) = existing {
        existing
    } else {
        Arc::new(SqliteReplicaStore::open(destination, do_id)?)
    };
    let sink = Arc::new(UnixReplicaSink::new(replica_socket));
    let mut after = replica.state()?.applied_sequence();
    if after > start_sequence {
        return Err(format!(
            "replayed sequence {after} is ahead of held sequence {start_sequence}"
        )
        .into());
    }
    loop {
        let entries = sink.replay(after, 1_024).await?;
        if entries.is_empty() {
            break;
        }
        for entry in entries {
            let expected = after.saturating_add(1);
            if entry.sequence() != expected {
                return Err(format!(
                    "replica replay sequence {} does not follow {after}",
                    entry.sequence()
                )
                .into());
            }
            if entry.sequence() > start_sequence {
                return Err(format!(
                    "replica replay sequence {} is ahead of held sequence {start_sequence}",
                    entry.sequence()
                )
                .into());
            }
            replica.apply_committed(
                entry.sequence(),
                entry.transaction_id(),
                entry.canonical_envelope(),
            )?;
            if checkpoint_available {
                replica.mark_archived(
                    entry.sequence(),
                    &format!(
                        "replica-replay/{}-{}",
                        entry.sequence(),
                        entry.transaction_id()
                    ),
                )?;
            }
            after = entry.sequence();
        }
    }
    if after != start_sequence {
        return Err(format!(
            "replayed sequence {after} does not match held start sequence {start_sequence}"
        )
        .into());
    }
    Ok((replica, sink))
}

/// Reconstructs a fresh SQLite pager from the latest checkpoint and CAS tail.
async fn recover_managed(
    store: Arc<dyn ObjectStore>,
    prefix: &str,
    do_id: &str,
    destination: &Path,
    grant: &LeaseGrant,
) -> Result<Arc<SqliteReplicaStore>, Box<dyn Error>> {
    let publisher = ObjectStoreCheckpointPublisher::new(store.clone(), prefix);
    let checkpoint_prefix = ObjectPath::from(prefix).join(do_id).join("checkpoints");
    let latest = latest_checkpoint(store.as_ref(), &checkpoint_prefix, grant.sequence()).await?;
    let replica = if tokio::fs::try_exists(destination).await? {
        Arc::new(SqliteReplicaStore::open(destination, do_id)?)
    } else if let Some(checkpoint) = latest {
        let receipt = CheckpointReceipt::new(
            checkpoint.sequence,
            checkpoint.path.to_string(),
            checkpoint.sha256,
        )?;
        let restored = publisher.restore(do_id, &receipt, destination).await?;
        restored.mark_checkpointed(receipt.through_sequence(), receipt.object_path())?;
        Arc::new(restored)
    } else {
        Arc::new(SqliteReplicaStore::open(destination, do_id)?)
    };
    let state = replica.state()?;
    if state.applied_sequence() > grant.sequence() {
        return Err(format!(
            "local sequence {} is ahead of held managed sequence {}",
            state.applied_sequence(),
            grant.sequence()
        )
        .into());
    }

    let transaction_prefix = ObjectPath::from(prefix).join(do_id).join("transactions");
    let mut transactions = store.list(Some(&transaction_prefix));
    let mut objects = Vec::new();
    while let Some(metadata) = transactions.next().await {
        let metadata = metadata?;
        if let Some(candidate) = parse_transaction_object(&metadata.location)?
            && candidate.sequence <= grant.sequence()
        {
            objects.push(candidate);
        }
    }
    objects.sort_by_key(|object| object.sequence);
    let mut after = state.applied_sequence();
    let mut cursor = 0_usize;
    while after < grant.sequence() {
        let expected = after.saturating_add(1);
        while cursor < objects.len() && objects[cursor].sequence < expected {
            cursor += 1;
        }
        let Some(object) = objects.get(cursor) else {
            return Err(format!("managed transaction tail is missing sequence {expected}").into());
        };
        if object.sequence != expected {
            return Err(format!(
                "managed transaction sequence {} does not follow {after}",
                object.sequence
            )
            .into());
        }
        if objects
            .get(cursor + 1)
            .is_some_and(|next| next.sequence == expected)
        {
            return Err(
                format!("managed transaction sequence {expected} has multiple objects").into(),
            );
        }
        let canonical = store.get(&object.path).await?.bytes().await?;
        let envelope = TransactionEnvelope::from_canonical_bytes(&canonical)?;
        if envelope.do_id() != do_id
            || envelope.transaction_id() != object.transaction_id
            || envelope.base_commit_sequence() != after
        {
            return Err(
                format!("managed transaction identity mismatch at sequence {expected}").into(),
            );
        }
        replica.apply_committed(expected, object.transaction_id, &canonical)?;
        replica.mark_archived(expected, object.path.as_ref())?;
        after = expected;
        cursor += 1;
    }
    Ok(replica)
}

/// Opens the durable pager and serves the child-exclusive Unix endpoint.
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = match Config::parse() {
        Ok(config) => config,
        Err(message) => {
            eprintln!("{message}");
            return Err("invalid verglasd arguments".into());
        }
    };
    tokio::fs::create_dir_all(&config.data_dir).await?;
    let declaration_path = (config.role == ReplicaEndpointRole::Worker)
        .then(|| {
            config.offload_dir.as_ref().map(|root| {
                root.join("indexes")
                    .join(&config.do_id)
                    .join("declarations")
            })
        })
        .flatten();
    let mut endpoint = if config.role == ReplicaEndpointRole::Worker {
        let lease_token = config
            .lease_token
            .clone()
            .ok_or_else(|| std::io::Error::other("missing validated lease token"))?;
        let lease_generation = config
            .lease_generation
            .ok_or_else(|| std::io::Error::other("missing validated lease generation"))?;
        let start_sequence = config
            .start_sequence
            .ok_or_else(|| std::io::Error::other("missing validated start sequence"))?;
        if config.is_managed_cas() {
            let prefix = config
                .cas_prefix
                .as_deref()
                .ok_or_else(|| std::io::Error::other("missing managed CAS prefix"))?;
            let managed_store = build_managed_store(&config)?;
            let grant = LeaseGrant::new(
                LeaseIdentity::new(lease_token, lease_generation),
                start_sequence,
                config.lease_etag.clone(),
                config.lease_version.clone(),
            )?;
            let authority = Arc::new(CasCommitAuthority::from_grant(
                managed_store.clone(),
                prefix,
                config.do_id.clone(),
                grant.clone(),
            )?);
            authority.validate_grant().await?;
            let store = recover_managed(
                managed_store.clone(),
                prefix,
                &config.do_id,
                &config.data_dir.join("replica.sqlite"),
                &grant,
            )
            .await?;
            let publisher = Arc::new(ObjectStoreCheckpointPublisher::new(managed_store, prefix));
            ReplicaEndpoint::bind_worker_with_cas(
                &config.socket,
                config.do_id,
                config.replica_id,
                store,
                authority,
                publisher,
                prefix,
            )
            .await?
        } else {
            let replica_socket = config
                .replica_socket
                .clone()
                .ok_or_else(|| std::io::Error::other("missing validated replica socket"))?;
            let checkpoint_store = match config.offload_dir.as_ref() {
                Some(root) => {
                    Some(Arc::new(LocalFileSystem::new_with_prefix(root)?) as Arc<dyn ObjectStore>)
                }
                None => None,
            };
            let (store, replica) = recover_replica(
                checkpoint_store,
                &config.do_id,
                &config.data_dir.join("replica.sqlite"),
                replica_socket,
                start_sequence,
            )
            .await?;
            let lease = LeaseIdentity::new(lease_token, lease_generation);
            let authority = Arc::new(ReplicaCommitAuthority::new(
                config.do_id.clone(),
                lease.clone(),
                start_sequence,
                replica.clone(),
            ));
            let archive = config
                .offload_dir
                .clone()
                .map(
                    |root| -> Result<Arc<dyn OffloadBatchArchive>, Box<dyn Error>> {
                        let store = Arc::new(LocalFileSystem::new_with_prefix(root)?);
                        Ok(Arc::new(ObjectStoreOffloadBatchArchive::new(
                            store,
                            ObjectPath::from("transactions"),
                        )))
                    },
                )
                .transpose()?;
            if let Some(offload_dir) = config.offload_dir.clone() {
                let archive = archive.ok_or_else(|| {
                    std::io::Error::other("replica offload archive was not initialized")
                })?;
                let local_store = Arc::new(LocalFileSystem::new_with_prefix(offload_dir)?);
                let publisher = Arc::new(ObjectStoreCheckpointPublisher::new(
                    local_store,
                    "checkpoints",
                ));
                ReplicaEndpoint::bind_worker_with_replica_checkpoint(
                    &config.socket,
                    config.do_id,
                    config.replica_id,
                    store,
                    authority,
                    archive,
                    publisher,
                    replica,
                    lease,
                )
                .await?
            } else {
                ReplicaEndpoint::bind_worker_with_archive(
                    &config.socket,
                    config.do_id,
                    config.replica_id,
                    store,
                    authority,
                    archive,
                )
                .await?
            }
        }
    } else {
        let store = Arc::new(SqliteReplicaStore::open(
            config.data_dir.join("replica.sqlite"),
            config.do_id.clone(),
        )?);
        ReplicaEndpoint::bind(
            &config.socket,
            config.do_id,
            config.replica_id,
            config.role,
            store,
        )
        .await?
    };
    if let Some(path) = declaration_path {
        endpoint.configure_index_declarations(path).await?;
    }
    tokio::select! {
        result = endpoint.run() => result?,
        signal = tokio::signal::ctrl_c() => signal?,
    }
    Ok(())
}
