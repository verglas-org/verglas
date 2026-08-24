//! Real-process replica CLEAN recovery with a verified checkpoint and SQL tail replay.

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
    /// Kills a child left behind by a failed acceptance assertion.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn clean_replica_restores_checkpoint_then_replays_sql_tail() {
    let directory = tempfile::tempdir().expect("acceptance root");
    let cell_root = directory.path().join("cell");
    let control = directory.path().join("celld.sock");
    let replica_socket = directory.path().join("replica.sock");
    let replica_dir = directory.path().join("replica");
    let offload_dir = directory.path().join("managed-offload");
    std::fs::create_dir_all(&offload_dir).expect("offload root");

    let mut replica = spawn_verglasd("clean-agent", "replica", &replica_socket, &replica_dir, &[]);
    wait_for_child_path(&mut replica, &replica_socket).await;
    let mut host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;

    let worker = spawn_worker(&control, &replica_socket, &offload_dir, "clean-agent", 0, 0).await;
    register_items(&worker).await;
    commit_value(&worker, 301, 0, 11).await;
    assert_eq!(endpoint_request(&worker, "DRAIN").await, "OK 1\n");
    assert_eq!(endpoint_request(&worker, "CHECKPOINT").await, "OK 1\n");
    assert_eq!(
        endpoint_request(&replica_socket, "STATUS").await,
        "OK replica 1 1 1\n"
    );
    assert_eq!(
        endpoint_request(&replica_socket, "REPLICA_CLEAN 10 636c65616e2d746f6b656e 1").await,
        "OK\n"
    );

    commit_value(&worker, 302, 1, 7).await;
    assert_eq!(
        endpoint_request(&replica_socket, "STATUS").await,
        "OK replica 2 1 1\n"
    );
    assert_query_value(&worker, 18).await;

    stop_host(&mut host);
    std::fs::remove_dir_all(cell_root.join("clean-agent").join("1")).expect("remove worker pager");

    let mut replacement_host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;
    let replacement =
        spawn_worker(&control, &replica_socket, &offload_dir, "clean-agent", 2, 2).await;
    assert_eq!(
        endpoint_request(&replacement, "STATUS").await,
        "OK worker 2 2 1\n"
    );
    assert_query_value(&replacement, 18).await;

    stop_host(&mut replacement_host);
    replica.kill().expect("kill replica");
    replica.wait().expect("wait replica");
}

async fn spawn_worker(
    control: &Path,
    replica_socket: &Path,
    offload_dir: &Path,
    do_id: &str,
    applied: u64,
    start_sequence: u64,
) -> PathBuf {
    let response = endpoint_request(
        control,
        &format!(
            "SPAWN_WORKER {do_id} 1 {applied} {} {} 10 {start_sequence} {}",
            replica_socket.display(),
            hex::encode("clean-token"),
            offload_dir.display(),
        ),
    )
    .await;
    assert!(response.starts_with("OK "), "spawn failed: {response}");
    PathBuf::from(response.trim().strip_prefix("OK ").expect("worker socket"))
}

async fn register_items(socket: &Path) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let mut ipc = Vec::new();
    StreamWriter::try_new(&mut ipc, &schema)
        .expect("schema writer")
        .finish()
        .expect("schema finish");
    assert_eq!(
        endpoint_request(socket, &format!("REGISTER items {}", hex::encode(ipc))).await,
        "OK\n"
    );
}

async fn commit_value(socket: &Path, id: u128, base: u64, value: i64) {
    let schema = Arc::new(Schema::new(vec![Field::new(
        "value",
        DataType::Int64,
        false,
    )]));
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(Int64Array::from(vec![value]))],
    )
    .expect("row batch");
    let mut envelope = TransactionEnvelope::new(
        "clean-agent",
        Uuid::from_u128(id),
        base,
        IsolationLevel::Snapshot,
    );
    envelope.append(MutationDomain::Relational, TableId::new("items"), batch);
    assert_eq!(
        endpoint_request(
            socket,
            &format!(
                "COMMIT {}",
                hex::encode(envelope.canonical_bytes().expect("canonical"))
            ),
        )
        .await,
        format!("OK {}\n", base + 1)
    );
}

async fn assert_query_value(socket: &Path, expected: i64) {
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
    assert_eq!(values.value(0), expected);
}

fn spawn_verglasd(
    do_id: &str,
    role: &str,
    socket: &Path,
    data_dir: &Path,
    extra: &[String],
) -> ManagedChild {
    ManagedChild(
        Command::new(env!("CARGO_BIN_EXE_verglasd"))
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
            .stderr(Stdio::null())
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
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn celld-host"),
    )
}

fn stop_host(host: &mut Child) {
    let status = Command::new("kill")
        .arg("-INT")
        .arg(host.id().to_string())
        .status()
        .expect("signal celld-host");
    assert!(status.success());
    assert!(host.wait().expect("wait celld-host").success());
}

async fn wait_for_child_path(child: &mut Child, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        if let Some(status) = child.try_wait().expect("inspect child") {
            panic!("{} exited with {status}", path.display());
        }
        assert!(Instant::now() < deadline, "{} not ready", path.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn wait_for_path(path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !path.exists() {
        assert!(Instant::now() < deadline, "{} not ready", path.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
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
