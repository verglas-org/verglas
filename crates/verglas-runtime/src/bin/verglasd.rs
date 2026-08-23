//! Supervised single-replica Durable Object child process.

use std::error::Error;
use std::path::PathBuf;
use std::sync::Arc;

use object_store::local::LocalFileSystem;
use object_store::path::Path;
use verglas_do_engine::{
    LeaseIdentity, ObjectStoreOffloadBatchArchive, OffloadBatchArchive, ReplicaCommitAuthority,
    ReplicaEndpoint, ReplicaEndpointRole, SqliteReplicaStore, UnixReplicaSink,
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
            offload_dir,
        };
        if config.role == ReplicaEndpointRole::Worker
            && (config.replica_socket.is_none()
                || config.lease_token.is_none()
                || config.lease_generation.is_none()
                || config.start_sequence.is_none())
        {
            return Err(
                "worker role requires replica socket, lease token, generation, and start sequence"
                    .to_owned(),
            );
        }
        Ok(config)
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
    "usage: verglasd --do-id ID --replica-id N --role worker|replica --socket PATH --data-dir PATH [--replica-socket PATH --lease-token TOKEN --lease-generation N --start-sequence N]"
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
    let store = Arc::new(SqliteReplicaStore::open(
        config.data_dir.join("replica.sqlite"),
        config.do_id.clone(),
    )?);
    let mut endpoint = if config.role == ReplicaEndpointRole::Worker {
        let (Some(replica_socket), Some(lease_token), Some(lease_generation), Some(start_sequence)) = (
            config.replica_socket,
            config.lease_token,
            config.lease_generation,
            config.start_sequence,
        ) else {
            return Err("missing validated worker durability configuration".into());
        };
        let replica = Arc::new(UnixReplicaSink::new(replica_socket));
        let mut after = store.state()?.applied_sequence();
        loop {
            let entries = replica.replay(after, 1_024).await?;
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
                store.apply_committed(
                    entry.sequence(),
                    entry.transaction_id(),
                    entry.canonical_envelope(),
                )?;
                after = entry.sequence();
            }
        }
        if after != start_sequence {
            return Err(format!(
                "replayed sequence {after} does not match held start sequence {start_sequence}"
            )
            .into());
        }
        let authority = Arc::new(ReplicaCommitAuthority::new(
            config.do_id.clone(),
            LeaseIdentity::new(lease_token, lease_generation),
            start_sequence,
            replica,
        ));
        let archive = config
            .offload_dir
            .map(
                |root| -> Result<Arc<dyn OffloadBatchArchive>, Box<dyn Error>> {
                    let store = Arc::new(LocalFileSystem::new_with_prefix(root)?);
                    Ok(Arc::new(ObjectStoreOffloadBatchArchive::new(
                        store,
                        Path::from("transactions"),
                    )))
                },
            )
            .transpose()?;
        ReplicaEndpoint::bind_worker_with_archive(
            &config.socket,
            config.do_id,
            config.replica_id,
            store,
            authority,
            archive,
        )
        .await?
    } else {
        ReplicaEndpoint::bind(
            &config.socket,
            config.do_id,
            config.replica_id,
            config.role,
            store,
        )
        .await?
    };
    tokio::select! {
        result = endpoint.run() => result?,
        signal = tokio::signal::ctrl_c() => signal?,
    }
    Ok(())
}
