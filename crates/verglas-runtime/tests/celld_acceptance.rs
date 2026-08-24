//! Full local acceptance: real celld-host, multiple workers, commits, failure, and replay.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::{Int64Array, RecordBatch};
use arrow_ipc::reader::StreamReader;
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;
use verglas_do_engine::{IsolationLevel, MutationDomain, TableId, TransactionEnvelope};

struct ManagedChild(Child);

impl std::ops::Deref for ManagedChild {
    type Target = Child;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for ManagedChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for ManagedChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn celld_runs_multiple_dos_and_restores_fresh_workers_from_replica() {
    let directory = tempfile::tempdir().expect("acceptance root");
    let cell_root = directory.path().join("cell");
    let control = directory.path().join("celld.sock");
    let offload_root = directory.path().join("managed-offload");
    std::fs::create_dir_all(&offload_root).expect("offload root");
    let mut replicas = Vec::new();
    for (name, id) in [("agent-a", 1_u64), ("catalog", 2_u64)] {
        let socket = directory.path().join(format!("{name}-replica.sock"));
        let mut replica = spawn_verglasd(
            name,
            "replica",
            &socket,
            &directory.path().join(format!("{name}-replica")),
            &[],
        );
        wait_for_child_path(&mut replica, &socket).await;
        replicas.push((name, id, socket, replica));
    }
    let mut host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;

    let mut worker_sockets = Vec::new();
    for (name, id, replica_socket, _) in &replicas {
        let response = control_request(
            &control,
            &format!(
                "SPAWN_WORKER {name} {id} 0 {} {} {} 0 {}",
                replica_socket.display(),
                hex::encode(format!("lease-{name}")),
                id + 10,
                if *name == "agent-a" {
                    offload_root.display().to_string()
                } else {
                    "-".to_owned()
                }
            ),
        )
        .await;
        assert!(response.starts_with("OK "), "spawn failed: {response}");
        let worker_socket = PathBuf::from(response.trim().strip_prefix("OK ").expect("socket"));
        worker_sockets.push((*name, *id, worker_socket));
    }
    let mut worker_pids = Vec::new();
    for (name, _, _) in &worker_sockets {
        worker_pids.push(control_request(&control, &format!("PID {name}")).await);
    }
    assert_eq!(worker_pids.len(), 2);
    assert_ne!(worker_pids[0], worker_pids[1]);
    assert!(
        worker_pids
            .iter()
            .all(|response| response.starts_with("OK "))
    );

    for (name, id, worker_socket) in &worker_sockets {
        let mut envelope = TransactionEnvelope::new(
            *name,
            Uuid::from_u128(100 + u128::from(*id)),
            0,
            IsolationLevel::Snapshot,
        );
        let schema = Arc::new(Schema::new(vec![Field::new(
            "value",
            DataType::Int64,
            false,
        )]));
        let mut schema_ipc = Vec::new();
        StreamWriter::try_new(&mut schema_ipc, &schema)
            .expect("schema writer")
            .finish()
            .expect("schema finish");
        assert_eq!(
            endpoint_request(
                worker_socket,
                &format!("REGISTER items {}", hex::encode(schema_ipc)),
            )
            .await,
            "OK\n"
        );
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![
                i64::try_from(*id).expect("id"),
            ]))],
        )
        .expect("row batch");
        envelope.append(MutationDomain::Relational, TableId::new("items"), batch);
        assert_eq!(
            endpoint_request(
                worker_socket,
                &format!(
                    "COMMIT {}",
                    hex::encode(envelope.canonical_bytes().expect("canonical"))
                ),
            )
            .await,
            "OK 1\n"
        );
        assert_query_value(worker_socket, *id).await;
    }

    let agent_socket = worker_sockets
        .iter()
        .find_map(|(name, _, socket)| (*name == "agent-a").then_some(socket))
        .expect("agent socket");
    assert_eq!(endpoint_request(agent_socket, "DRAIN").await, "OK 1\n");
    assert!(
        offload_root
            .join("transactions/agent-a/00000000000000000001-00000000000000000001.batch")
            .is_file()
    );
    let catalog_socket = worker_sockets
        .iter()
        .find_map(|(name, _, socket)| (*name == "catalog").then_some(socket))
        .expect("catalog socket");
    assert_eq!(
        endpoint_request(catalog_socket, "DRAIN").await,
        "ERR transaction archive failed: managed offload is disabled\n"
    );
    assert!(!offload_root.join("transactions/catalog").exists());
    for (name, id, replica_socket, _) in &replicas {
        let token = hex::encode(format!("lease-{name}"));
        assert!(
            endpoint_request(
                replica_socket,
                &format!("REPLICA_CLEAN {} {token} 1", id + 9),
            )
            .await
            .starts_with("ERR ")
        );
        assert!(
            endpoint_request(
                replica_socket,
                &format!("REPLICA_CLEAN {} {token} 1", id + 10),
            )
            .await
            .starts_with("ERR ")
        );
        assert!(
            endpoint_request(replica_socket, "REPLICA_REPLAY 0 10")
                .await
                .starts_with("OK 1:")
        );
    }

    stop_host(&mut host);
    for (name, id, _) in &worker_sockets {
        std::fs::remove_dir_all(cell_root.join(name).join(id.to_string()))
            .expect("remove failed worker pager");
    }
    let mut replacement_host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;

    for (name, id, replica_socket, _) in &replicas {
        let response = control_request(
            &control,
            &format!(
                "SPAWN_WORKER {name} {id} 1 {} {} {} 1 {}",
                replica_socket.display(),
                hex::encode(format!("lease-{name}")),
                id + 10,
                if *name == "agent-a" {
                    offload_root.display().to_string()
                } else {
                    "-".to_owned()
                }
            ),
        )
        .await;
        assert!(response.starts_with("OK "), "restore failed: {response}");
        let socket = PathBuf::from(response.trim().strip_prefix("OK ").expect("socket"));
        assert_eq!(
            endpoint_request(&socket, "STATUS").await,
            "OK worker 1 0 0\n"
        );
        assert_query_value(&socket, *id).await;
    }

    stop_host(&mut replacement_host);
    for (_, _, _, replica) in &mut replicas {
        replica.kill().expect("kill replica");
        replica.wait().expect("wait replica");
    }
}

async fn assert_query_value(socket: &Path, expected: u64) {
    let response = endpoint_request(
        socket,
        &format!(
            "QUERY items {}",
            hex::encode("SELECT sum(value) AS total FROM items")
        ),
    )
    .await;
    let encoded = response
        .trim()
        .strip_prefix("OK ")
        .unwrap_or_else(|| panic!("query failed: {response}"));
    let bytes = hex::decode(encoded).expect("query IPC");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).expect("IPC reader");
    let batch = reader
        .next()
        .expect("query batch")
        .expect("valid query batch");
    let values = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum int64");
    assert_eq!(values.value(0), i64::try_from(expected).expect("expected"));
}

fn spawn_verglasd(
    do_id: &str,
    role: &str,
    socket: &Path,
    data_dir: &Path,
    extra: &[String],
) -> ManagedChild {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglasd"));
    ManagedChild(
        command
            .arg("--do-id")
            .arg(do_id)
            .arg("--replica-id")
            .arg("1")
            .arg("--role")
            .arg(role)
            .arg("--socket")
            .arg(socket)
            .arg("--data-dir")
            .arg(data_dir)
            .args(extra)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn verglasd"),
    )
}

fn spawn_host(root: &Path, control: &Path) -> ManagedChild {
    ManagedChild(
        Command::new(env!("CARGO_BIN_EXE_celld-host"))
            .arg("--host-id")
            .arg("cell-local")
            .arg("--root")
            .arg(root)
            .arg("--child")
            .arg(env!("CARGO_BIN_EXE_verglasd"))
            .arg("--control")
            .arg(control)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn celld-host"),
    )
}

fn stop_host(host: &mut Child) {
    let pid = host.id().to_string();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid)
        .status()
        .expect("signal celld-host");
    assert!(status.success());
    assert!(host.wait().expect("wait celld-host").success());
}

async fn wait_for_child_path(child: &mut Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        if let Some(status) = child.try_wait().expect("inspect child") {
            let mut stderr = String::new();
            if let Some(mut stream) = child.stderr.take() {
                stream.read_to_string(&mut stderr).expect("child stderr");
            }
            panic!("{} exited with {status}: {stderr}", path.display());
        }
        assert!(Instant::now() < deadline, "{} not ready", path.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(3);
    while !path.exists() {
        assert!(Instant::now() < deadline, "{} not ready", path.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn control_request(socket: &Path, command: &str) -> String {
    endpoint_request(socket, command).await
}

async fn endpoint_request(socket: &Path, command: &str) -> String {
    let mut stream = UnixStream::connect(socket).await.expect("connect socket");
    stream
        .write_all(format!("{command}\n").as_bytes())
        .await
        .expect("write command");
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .await
        .expect("read response");
    response
}
