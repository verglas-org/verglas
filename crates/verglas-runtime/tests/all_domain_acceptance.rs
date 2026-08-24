//! Real-process all-domain transaction, projection recovery, and retry acceptance.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow_array::types::Float32Type;
use arrow_array::{Array, FixedSizeListArray, Float64Array, Int64Array, RecordBatch, StringArray};
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
    /// Kills a process left behind by a failed assertion.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn all_domains_commit_atomically_recover_indexes_and_retry_exactly() {
    let directory = tempfile::tempdir().expect("acceptance root");
    let cell_root = directory.path().join("cell");
    let control = directory.path().join("celld.sock");
    let replica_socket = directory.path().join("replica.sock");
    let replica_dir = directory.path().join("replica");
    let offload_dir = directory.path().join("managed-offload");
    std::fs::create_dir_all(&offload_dir).expect("offload root");

    let mut replica = spawn_verglasd(
        "all-domain-agent",
        "replica",
        &replica_socket,
        &replica_dir,
        &[],
    );
    wait_for_child_path(&mut replica, &replica_socket).await;
    let mut host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;
    let (worker, worker_pid) = spawn_worker(&control, &replica_socket, &offload_dir, 0, 0).await;

    let table = TableId::new("items");
    let batch = all_domain_batch(42, [1.0, 0.0], "edge-1");
    register_table(&worker, &table, &batch.schema()).await;
    assert_eq!(
        request(&worker, "REGISTER_VECTOR items id embedding 2 cosine").await,
        "OK\n"
    );
    assert_eq!(request(&worker, "REGISTER_GRAPH items").await, "OK\n");

    let transaction_id = Uuid::from_u128(0x1710_0000_0000_0000_0000_0000_0000_0001);
    let canonical = canonical_all_domain(transaction_id, 0, &table, &batch);
    assert_eq!(commit(&worker, &canonical).await, "OK 1\n");
    assert_eq!(request(&worker, "DOMAINS").await, "OK 1 1 1 1\n");
    assert_eq!(query_sum(&worker).await, 42);
    assert!(
        request(&worker, &vector_search_command([1.0, 0.0]))
            .await
            .starts_with("OK 1:")
    );
    assert_eq!(
        request(&worker, &graph_neighbors_command("A", "knows")).await,
        "OK B:edge-1:knows\n"
    );

    assert_eq!(commit(&worker, &canonical).await, "OK 1\n");
    let conflicting = all_domain_batch(43, [1.0, 0.0], "edge-1");
    let conflicting_canonical = canonical_all_domain(transaction_id, 0, &table, &conflicting);
    assert!(
        commit(&worker, &conflicting_canonical)
            .await
            .starts_with("ERR ")
    );

    kill_pid(worker_pid);
    stop_host(&mut host);
    let mut replacement_host = spawn_host(&cell_root, &control);
    wait_for_path(&control).await;
    let (replacement, replacement_pid) =
        spawn_worker(&control, &replica_socket, &offload_dir, 1, 1).await;
    assert_eq!(request(&replacement, "STATUS").await, "OK worker 1 0 0\n");
    assert_eq!(request(&replacement, "DOMAINS").await, "OK 1 1 1 1\n");
    assert_eq!(query_sum(&replacement).await, 42);
    assert!(
        request(&replacement, &vector_search_command([1.0, 0.0]))
            .await
            .starts_with("OK 1:")
    );
    assert_eq!(
        request(&replacement, &graph_neighbors_command("A", "knows")).await,
        "OK B:edge-1:knows\n"
    );
    assert_eq!(
        request(&replacement, "INDEX_STATUS items").await,
        "OK vector=1 graph=1\n"
    );

    kill_pid(replacement_pid);
    stop_host(&mut replacement_host);
    replica.kill().expect("kill replica");
    replica.wait().expect("wait replica");
}

fn all_domain_batch(value: i64, vector: [f32; 2], edge_id: &str) -> RecordBatch {
    let embedding = FixedSizeListArray::from_iter_primitive::<Float32Type, _, _>(
        vec![Some(vec![Some(vector[0]), Some(vector[1])])],
        2,
    );
    let schema = Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("value", DataType::Int64, false),
        Field::new("embedding", embedding.data_type().clone(), false),
        Field::new("edge_id", DataType::Utf8, false),
        Field::new("src_id", DataType::Utf8, false),
        Field::new("predicate", DataType::Utf8, false),
        Field::new("dst_id", DataType::Utf8, false),
        Field::new("provenance", DataType::Utf8, false),
        Field::new("confidence", DataType::Float64, false),
        Field::new("supersedes", DataType::Utf8, true),
    ]));
    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![value])),
            Arc::new(embedding),
            Arc::new(StringArray::from(vec![edge_id])),
            Arc::new(StringArray::from(vec!["A"])),
            Arc::new(StringArray::from(vec!["knows"])),
            Arc::new(StringArray::from(vec!["B"])),
            Arc::new(StringArray::from(vec!["acceptance"])),
            Arc::new(Float64Array::from(vec![0.9])),
            Arc::new(StringArray::from(vec![None::<&str>])),
        ],
    )
    .expect("all-domain batch")
}

fn canonical_all_domain(
    transaction_id: Uuid,
    base: u64,
    table: &TableId,
    batch: &RecordBatch,
) -> Vec<u8> {
    let mut envelope = TransactionEnvelope::new(
        "all-domain-agent",
        transaction_id,
        base,
        IsolationLevel::Snapshot,
    );
    envelope.append_schema_change(table.clone(), batch.schema());
    envelope.append(MutationDomain::Relational, table.clone(), batch.clone());
    envelope.append(MutationDomain::Vector, table.clone(), batch.clone());
    envelope.append(MutationDomain::Graph, table.clone(), batch.clone());
    envelope.canonical_bytes().expect("canonical envelope")
}

async fn register_table(socket: &Path, table: &TableId, schema: &arrow_schema::SchemaRef) {
    let mut ipc = Vec::new();
    StreamWriter::try_new(&mut ipc, schema)
        .expect("schema writer")
        .finish()
        .expect("schema finish");
    assert_eq!(
        request(
            socket,
            &format!("REGISTER {} {}", table.as_str(), hex::encode(ipc))
        )
        .await,
        "OK\n"
    );
}

async fn commit(socket: &Path, canonical: &[u8]) -> String {
    request(socket, &format!("COMMIT {}", hex::encode(canonical))).await
}

async fn query_sum(socket: &Path) -> i64 {
    let response = request(
        socket,
        &format!(
            "QUERY items {}",
            hex::encode("SELECT sum(value) AS total FROM items")
        ),
    )
    .await;
    let bytes = hex::decode(response.trim().strip_prefix("OK ").expect("query response"))
        .expect("query IPC");
    let mut reader = StreamReader::try_new(std::io::Cursor::new(bytes), None).expect("IPC reader");
    let batch = reader
        .next()
        .expect("query batch")
        .expect("valid query batch");
    batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .expect("sum int64")
        .value(0)
}

fn vector_search_command(query: [f32; 2]) -> String {
    let bytes = query
        .into_iter()
        .flat_map(f32::to_le_bytes)
        .collect::<Vec<_>>();
    format!("VECTOR_SEARCH items 1 {}", hex::encode(bytes))
}

fn graph_neighbors_command(node: &str, predicate: &str) -> String {
    format!(
        "GRAPH_NEIGHBORS items {} out {}",
        hex::encode(node),
        hex::encode(predicate)
    )
}

async fn spawn_worker(
    control: &Path,
    replica_socket: &Path,
    offload_dir: &Path,
    applied: u64,
    start_sequence: u64,
) -> (PathBuf, u32) {
    let response = request(
        control,
        &format!(
            "SPAWN_WORKER all-domain-agent 1 {applied} {} {} 7 {start_sequence} {}",
            replica_socket.display(),
            hex::encode("all-domain-token"),
            offload_dir.display(),
        ),
    )
    .await;
    assert!(response.starts_with("OK "), "spawn failed: {response}");
    let socket = PathBuf::from(response.trim().strip_prefix("OK ").expect("worker socket"));
    let pid = request(control, "PID all-domain-agent")
        .await
        .trim()
        .strip_prefix("OK ")
        .expect("worker pid")
        .parse::<u32>()
        .expect("worker pid integer");
    (socket, pid)
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
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn celld-host"),
    )
}

async fn wait_for_child_path(child: &mut ManagedChild, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
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
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        assert!(Instant::now() < deadline, "{} not ready", path.display());
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn request(socket: &Path, command: &str) -> String {
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

fn kill_pid(pid: u32) {
    let status = Command::new("kill")
        .arg(pid.to_string())
        .status()
        .expect("kill worker");
    assert!(status.success());
}

fn stop_host(host: &mut Child) {
    let pid = host.id().to_string();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid)
        .status()
        .expect("signal host");
    assert!(status.success());
    assert!(host.wait().expect("wait host").success());
}
