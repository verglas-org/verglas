//! Process-level proof that three cache-node binaries form the fragment ring
//! used by the embedded Neon safekeeper.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use bytes::{BufMut, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

const TENANT: &str = "0123456789abcdef0123456789abcdef";
const TIMELINE: &str = "fedcba9876543210fedcba9876543210";

/// Kills child cache nodes even when an assertion fails.
struct Fleet {
    /// Spawned cache-node processes.
    children: Vec<Child>,
    /// Captured stderr lines from every child, for failure diagnostics.
    stderr: Arc<Mutex<Vec<String>>>,
}

impl Drop for Fleet {
    /// Stops and reaps every spawned cache node.
    fn drop(&mut self) {
        for child in &mut self.children {
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

impl Fleet {
    /// Returns a copy of every captured child stderr line.
    fn stderr_snapshot(&self) -> String {
        self.stderr
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .join("\n")
    }

    /// Waits until `count` children have logged that the embedded safekeeper is listening.
    fn wait_for_safekeepers(&mut self, count: usize, deadline: Duration) {
        let start = Instant::now();
        while start.elapsed() < deadline {
            let ready = self
                .stderr
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .iter()
                .filter(|line| line.contains("embedded safekeeper listening on"))
                .count();
            if ready >= count {
                return;
            }
            for (index, child) in self.children.iter_mut().enumerate() {
                if let Ok(Some(status)) = child.try_wait() {
                    panic!(
                        "cache-node {index} exited early with {status}; stderr:\n{}",
                        self.stderr_snapshot()
                    );
                }
            }
            thread::sleep(Duration::from_millis(50));
        }
        panic!(
            "timed out waiting for {count} embedded safekeepers; stderr:\n{}",
            self.stderr_snapshot()
        );
    }
}

/// Reserves and releases a loopback port for a child listener.
fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("reserve port")
        .local_addr()
        .expect("reserved address")
        .port()
}

/// Writes the minimal cache-node config used by one child.
fn write_config(root: &std::path::Path, index: usize, s3: u16, admin: u16) -> std::path::PathBuf {
    let node = root.join(format!("node-{index}"));
    std::fs::create_dir_all(&node).expect("node cache dir");
    let credentials = node.join("credentials");
    std::fs::write(
        &credentials,
        "[default]\naws_access_key_id = test\naws_secret_access_key = testsecret\n",
    )
    .expect("credentials");
    let config = node.join("config.toml");
    let body = format!(
        "[listen]\ns3_port = {s3}\nadmin_port = {admin}\n\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n[backend]\nbucket = \"wal-test\"\nendpoint = \"http://127.0.0.1:9\"\nallow_http = true\nregion = \"us-east-1\"\ncredentials_file = \"{}\"\n\n[auth]\ncredentials_file = \"{}\"\n",
        node.display(),
        credentials.display(),
        credentials.display(),
    );
    std::fs::write(&config, body).expect("config");
    config
}

/// Connects once the child has bound its safekeeper listener.
async fn connect_ready(addr: SocketAddr) -> TcpStream {
    for _ in 0..100 {
        if let Ok(stream) = TcpStream::connect(addr).await {
            return stream;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("cache-node safekeeper did not listen on {addr}");
}

/// Sends a PostgreSQL startup packet.
async fn startup(stream: &mut TcpStream, params: &[(&str, &str)]) {
    let mut payload = BytesMut::new();
    payload.put_u32(196_608);
    for (key, value) in params {
        payload.put_slice(key.as_bytes());
        payload.put_u8(0);
        payload.put_slice(value.as_bytes());
        payload.put_u8(0);
    }
    payload.put_u8(0);
    stream
        .write_u32((payload.len() + 4) as u32)
        .await
        .expect("startup length");
    stream.write_all(&payload).await.expect("startup body");
}

/// Sends one PostgreSQL frontend frame.
async fn send(stream: &mut TcpStream, tag: u8, payload: &[u8]) {
    stream.write_u8(tag).await.expect("frontend tag");
    stream
        .write_u32((payload.len() + 4) as u32)
        .await
        .expect("frontend length");
    stream.write_all(payload).await.expect("frontend body");
}

/// Reads one PostgreSQL backend frame.
async fn receive(stream: &mut TcpStream) -> Result<(u8, Vec<u8>), std::io::Error> {
    let tag = stream.read_u8().await?;
    let length = stream.read_u32().await? as usize;
    let mut payload = vec![0; length - 4];
    stream.read_exact(&mut payload).await?;
    Ok((tag, payload))
}

/// Reads startup responses through ReadyForQuery.
async fn ready(stream: &mut TcpStream) -> Result<(), std::io::Error> {
    while receive(stream).await?.0 != b'Z' {}
    Ok(())
}

/// Writes one NUL-terminated Neon string.
fn cstr(frame: &mut BytesMut, value: &str) {
    frame.put_slice(value.as_bytes());
    frame.put_u8(0);
}

/// Completes the PostgreSQL startup handshake, retrying if a child closes early.
async fn connect_and_ready(addr: SocketAddr, params: &[(&str, &str)]) -> TcpStream {
    let mut last = None;
    for _ in 0..20 {
        let mut stream = connect_ready(addr).await;
        startup(&mut stream, params).await;
        match ready(&mut stream).await {
            Ok(()) => return stream,
            Err(error) => {
                last = Some(error);
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        }
    }
    panic!(
        "safekeeper at {addr} closed during startup handshake: {}",
        last.map(|error| error.to_string()).unwrap_or_default()
    );
}

/// Three real cache-node processes quorum-ack WAL and serve it back unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_node_embeds_the_ring_backed_safekeeper() {
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ring_ports = [free_port(), free_port(), free_port()];
    let safekeeper_ports = [free_port(), free_port(), free_port()];
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let mut children = Vec::new();
    for index in 0..3 {
        let config = write_config(root.path(), index, free_port(), free_port());
        let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-cache-node"))
            .arg("--config")
            .arg(config)
            .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
            .env("VERGLAS_NODE_ID", format!("node-{index}"))
            .env("VERGLAS_RING_PEERS", &peers)
            .env(
                "VERGLAS_RING_ADDR",
                format!("127.0.0.1:{}", ring_ports[index]),
            )
            .env("VERGLAS_BLOCK_ADDR", format!("127.0.0.1:{}", free_port()))
            .env(
                "VERGLAS_SAFEKEEPER_ADDR",
                format!("127.0.0.1:{}", safekeeper_ports[index]),
            )
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn cache node");
        let pipe = child.stderr.take().expect("piped stderr");
        let sink = Arc::clone(&stderr);
        thread::spawn(move || {
            for line in BufReader::new(pipe).lines() {
                let Ok(line) = line else {
                    return;
                };
                if let Ok(mut lines) = sink.lock() {
                    lines.push(line);
                }
            }
        });
        children.push(child);
    }
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(3, Duration::from_secs(30));

    let address = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[0]));
    let mut proposer = connect_and_ready(address, &[("user", "cloud_admin")]).await;
    send(
        &mut proposer,
        b'Q',
        b"START_WAL_PUSH (proto_version '3', allow_timeline_creation 'true')\0",
    )
    .await;
    let copy_both = receive(&mut proposer).await.unwrap_or_else(|error| {
        panic!("START_WAL_PUSH reply: {error}\n{}", fleet.stderr_snapshot())
    });
    assert_eq!(copy_both.0, b'W');

    let mut greeting = BytesMut::new();
    greeting.put_u8(b'g');
    cstr(&mut greeting, TENANT);
    cstr(&mut greeting, TIMELINE);
    greeting.put_u32(1);
    greeting.put_u32(1);
    greeting.put_u64(1);
    cstr(&mut greeting, "127.0.0.1");
    greeting.put_u16(safekeeper_ports[0]);
    greeting.put_u32(0);
    greeting.put_u32(160_000);
    greeting.put_u64(0x1122_3344_5566_7788);
    greeting.put_u32(16 * 1024 * 1024);
    send(&mut proposer, b'd', &greeting).await;
    assert_eq!(
        receive(&mut proposer).await.expect("greeting reply").1[0],
        b'g'
    );

    let mut vote = BytesMut::new();
    vote.put_u8(b'v');
    vote.put_u32(1);
    vote.put_u64(1);
    send(&mut proposer, b'd', &vote).await;
    assert_eq!(receive(&mut proposer).await.expect("vote reply").1[0], b'v');

    let start = 0x1000_u64;
    let mut elected = BytesMut::new();
    elected.put_u8(b'e');
    elected.put_u32(1);
    elected.put_u64(1);
    elected.put_u64(start);
    elected.put_u32(1);
    elected.put_u64(1);
    elected.put_u64(start);
    send(&mut proposer, b'd', &elected).await;

    let wal = b"wal-through-three-real-cache-nodes";
    let end = start + wal.len() as u64;
    let mut append = BytesMut::new();
    append.put_u8(b'a');
    append.put_u32(1);
    append.put_u64(1);
    append.put_u64(start);
    append.put_u64(end);
    append.put_u64(end);
    append.put_u64(start);
    append.put_slice(wal);
    send(&mut proposer, b'd', &append).await;
    let (_, ack) = receive(&mut proposer)
        .await
        .unwrap_or_else(|error| panic!("append ack: {error}\n{}", fleet.stderr_snapshot()));
    assert_eq!(ack[0], b'a');
    assert_eq!(
        u64::from_be_bytes(ack[13..21].try_into().expect("flush LSN bytes")),
        end
    );

    let options = format!("-c tenant_id={TENANT} -c timeline_id={TIMELINE}");
    let mut replica =
        connect_and_ready(address, &[("user", "cloud_admin"), ("options", &options)]).await;
    send(
        &mut replica,
        b'Q',
        b"START_REPLICATION PHYSICAL 0/00001000 (term='1')\0",
    )
    .await;
    assert_eq!(
        receive(&mut replica)
            .await
            .expect("replication copy-both")
            .0,
        b'W'
    );
    let (tag, xlog) = receive(&mut replica)
        .await
        .unwrap_or_else(|error| panic!("xlog data: {error}\n{}", fleet.stderr_snapshot()));
    assert_eq!(tag, b'd');
    assert_eq!(xlog[0], b'w');
    assert_eq!(&xlog[25..], wal);
}
