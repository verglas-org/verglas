//! Process-level proof that cache-node binaries form the fragment ring used by
//! the embedded Neon safekeeper and retain one-node-failure availability.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use verglas_safekeeper::{WalRequest, WalResponse};

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
                .filter(|line| line.contains("Verglas Neon WAL ingress listening on"))
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
        "[listen]\ns3_port = {s3}\nadmin_port = {admin}\n\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n[backend]\nbucket = \"wal-test\"\nendpoint = \"http://127.0.0.1:9\"\nallow_http = true\nregion = \"us-east-1\"\ncredentials_file = \"{}\"\n\n[wal_archive]\nbucket = \"wal-test\"\nprefix = \"_verglas/test-wal\"\n\n[auth]\ncredentials_file = \"{}\"\n",
        node.display(),
        credentials.display(),
        credentials.display(),
    );
    std::fs::write(&config, body).expect("config");
    config
}

/// Submits one canonical WAL operation and decodes its complete response.
async fn submit(addr: SocketAddr, request: WalRequest) -> Result<WalResponse, String> {
    let body = request.encode().expect("encode WAL request");
    let response = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .map_err(|error| error.to_string())?
        .post(format!("http://{addr}/wal/v1/{TENANT}/{TIMELINE}"))
        .header("content-type", "application/octet-stream")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!("WAL operation failed with {}", response.status()));
    }
    WalResponse::decode(&response.bytes().await.expect("WAL response body"))
        .map_err(|error| error.to_string())
}

/// Four real cache nodes preserve writes with one voter down and refuse two failures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_node_embeds_the_ring_backed_safekeeper() {
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ring_ports = [free_port(), free_port(), free_port(), free_port()];
    let safekeeper_ports = [free_port(), free_port(), free_port(), free_port()];
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let mut children = Vec::new();
    for index in 0..4 {
        let config = write_config(root.path(), index, free_port(), free_port());
        let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-cache-node"))
            .arg("--config")
            .arg(config)
            .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
            .env("VERGLAS_NODE_ID", format!("node-{index}"))
            .env("VERGLAS_RING_PEERS", &peers)
            .env("VERGLAS_SAFEKEEPER_EC_K", "2")
            .env("VERGLAS_SAFEKEEPER_EC_M", "2")
            .env("VERGLAS_SAFEKEEPER_EC_W", "3")
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
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));

    let address = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[0]));
    let opened = submit(
        address,
        WalRequest::OpenTimeline {
            request_id: 100,
            start_lsn: 0x16_b6ff_f000,
        },
    )
    .await
    .expect("open timeline through four-voter quorum");
    let WalResponse::Applied {
        wal_end: Some(opened_lsn),
        ..
    } = opened
    else {
        panic!("unexpected timeline-open response: {opened:?}");
    };
    assert_eq!(opened_lsn, 0x16_b6ff_f000);
    let acquired = submit(
        address,
        WalRequest::AcquireWriter {
            request_id: 1,
            writer: "integration-compute".to_owned(),
        },
    )
    .await
    .expect("acquire writer through four-voter quorum");
    let WalResponse::Applied {
        index: acquire_index,
        writer_epoch: Some(writer_epoch),
        wal_end: Some(start),
        ..
    } = acquired
    else {
        panic!("unexpected acquire response: {acquired:?}");
    };
    let wal = b"wal-through-three-real-cache-nodes";
    let end = start + wal.len() as u64;
    let appended = submit(
        address,
        WalRequest::Append {
            request_id: (u128::from(writer_epoch) << 64) | u128::from(start),
            writer_epoch,
            start_lsn: start,
            payload: wal.to_vec(),
        },
    )
    .await
    .expect("append through four-voter quorum");
    let WalResponse::Applied {
        index: append_index,
        wal_end: Some(committed_end),
        ..
    } = appended
    else {
        panic!("unexpected append response: {appended:?}");
    };
    assert_eq!(committed_end, end);
    assert!(append_index > acquire_index);

    fleet.children[3].kill().expect("stop fourth voter");
    fleet.children[3].wait().expect("reap fourth voter");
    let second = b"-while-one-voter-is-down";
    let second_end = end + second.len() as u64;
    let appended = submit(
        address,
        WalRequest::Append {
            request_id: (u128::from(writer_epoch) << 64) | u128::from(end),
            writer_epoch,
            start_lsn: end,
            payload: second.to_vec(),
        },
    )
    .await
    .expect("three live voters must commit without waiting for the fourth");
    let WalResponse::Applied {
        index: second_index,
        wal_end: Some(committed_end),
        ..
    } = appended
    else {
        panic!("unexpected one-down append response: {appended:?}");
    };
    assert_eq!(committed_end, second_end);
    assert!(second_index > append_index);

    let read = submit(
        SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[1])),
        WalRequest::ReadWal {
            from: start,
            to: second_end,
            minimum_index: second_index,
        },
    )
    .await
    .expect("read committed WAL through another live ingress");
    let mut expected = wal.to_vec();
    expected.extend_from_slice(second);
    assert_eq!(
        read,
        WalResponse::WalBytes {
            payload: expected
        }
    );

    fleet.children[2].kill().expect("stop third voter");
    fleet.children[2].wait().expect("reap third voter");
    let minority = submit(
        address,
        WalRequest::Append {
            request_id: (u128::from(writer_epoch) << 64) | u128::from(second_end),
            writer_epoch,
            start_lsn: second_end,
            payload: b"must-not-commit".to_vec(),
        },
    )
    .await;
    assert!(minority.is_err(), "two live voters must fail closed");
}
