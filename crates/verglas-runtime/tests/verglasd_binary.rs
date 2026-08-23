//! End-to-end launch contract between `celld-host` and `verglasd`.

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;
use verglas_do_engine::{IsolationLevel, TransactionEnvelope};

#[tokio::test]
async fn supervised_binary_binds_socket_and_opens_sqlite_pager() {
    let directory = tempfile::tempdir().expect("replica directory");
    let socket = directory.path().join("worker.sock");
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--do-id")
        .arg("agent-1")
        .arg("--replica-id")
        .arg("2")
        .arg("--role")
        .arg("replica")
        .arg("--socket")
        .arg(&socket)
        .arg("--data-dir")
        .arg(directory.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglasd");
    let deadline = Instant::now() + Duration::from_secs(2);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "child did not bind socket");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }

    let mut stream = UnixStream::connect(&socket).await.expect("connect child");
    stream.write_all(b"STATUS\n").await.expect("write status");
    let mut response = String::new();
    BufReader::new(stream)
        .read_line(&mut response)
        .await
        .expect("read status");
    assert_eq!(response, "OK replica 0 0 0\n");
    assert!(directory.path().join("replica.sqlite").exists());

    child.kill().expect("kill child");
    child.wait().expect("wait child");
}

#[tokio::test]
async fn worker_commits_to_replica_and_replacement_replays_after_failover() {
    let directory = tempfile::tempdir().expect("runtime directory");
    let replica_dir = directory.path().join("replica");
    let worker_dir = directory.path().join("worker");
    let replacement_dir = directory.path().join("replacement");
    let replica_socket = directory.path().join("replica.sock");
    let worker_socket = directory.path().join("worker.sock");
    let replacement_socket = directory.path().join("replacement.sock");
    let mut replica = spawn("replica", &replica_socket, &replica_dir, &[]);
    wait_for_socket(&replica_socket).await;
    let worker_options = [
        "--replica-socket".to_owned(),
        replica_socket.display().to_string(),
        "--lease-token".to_owned(),
        "held-token".to_owned(),
        "--lease-generation".to_owned(),
        "5".to_owned(),
        "--start-sequence".to_owned(),
        "0".to_owned(),
    ];
    let mut worker = spawn("worker", &worker_socket, &worker_dir, &worker_options);
    wait_for_socket(&worker_socket).await;
    let transaction = TransactionEnvelope::new(
        "agent-e2e",
        Uuid::from_u128(71),
        0,
        IsolationLevel::Snapshot,
    );
    let response = request(
        &worker_socket,
        &format!(
            "COMMIT {}",
            hex::encode(transaction.canonical_bytes().expect("canonical"))
        ),
    )
    .await;
    assert_eq!(response, "OK 1\n");
    assert_eq!(
        request(&replica_socket, "STATUS").await,
        "OK replica 1 0 0\n"
    );
    worker.kill().expect("kill first worker");
    worker.wait().expect("wait first worker");

    let replacement_options = [
        "--replica-socket".to_owned(),
        replica_socket.display().to_string(),
        "--lease-token".to_owned(),
        "held-token".to_owned(),
        "--lease-generation".to_owned(),
        "5".to_owned(),
        "--start-sequence".to_owned(),
        "1".to_owned(),
    ];
    let mut replacement = spawn(
        "worker",
        &replacement_socket,
        &replacement_dir,
        &replacement_options,
    );
    wait_for_socket(&replacement_socket).await;
    assert_eq!(
        request(&replacement_socket, "STATUS").await,
        "OK worker 1 0 0\n"
    );

    replacement.kill().expect("kill replacement");
    replacement.wait().expect("wait replacement");
    replica.kill().expect("kill replica");
    replica.wait().expect("wait replica");
}

fn spawn(
    role: &str,
    socket: &std::path::Path,
    data_dir: &std::path::Path,
    extra: &[String],
) -> std::process::Child {
    let mut command = Command::new(env!("CARGO_BIN_EXE_verglasd"));
    command
        .arg("--do-id")
        .arg("agent-e2e")
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
        .expect("spawn verglasd")
}

async fn wait_for_socket(socket: &std::path::Path) {
    let deadline = Instant::now() + Duration::from_secs(2);
    while !socket.exists() {
        assert!(Instant::now() < deadline, "child did not bind socket");
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

async fn request(socket: &std::path::Path, command: &str) -> String {
    let mut stream = UnixStream::connect(socket).await.expect("connect child");
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
