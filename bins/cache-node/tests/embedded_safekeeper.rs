//! Process-level proof that cache-node binaries form the fragment ring used by
//! the embedded Neon safekeeper and retain one-node-failure availability.

use std::io::{BufRead, BufReader};
use std::net::{SocketAddr, TcpListener};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use verglas_catalog::{ManagedCatalogRequest, ManagedCatalogResponse};
use verglas_consensus::{CatalogAction, CatalogBatch, CatalogEntity, CatalogRequirement};
use verglas_safekeeper::{WalRequest, WalResponse};

const TENANT: &str = "0123456789abcdef0123456789abcdef";
const TIMELINE: &str = "fedcba9876543210fedcba9876543210";
const CATALOG_TENANT: &str = "tenant-tpch";
const WAREHOUSE: &str = "tpch";
const CANONICAL_WAL_APPEND_BYTES: usize = 8 * 1024 * 1024;
const WAL_BODY_LIMIT_BYTES: usize = 17 * 1024 * 1024;

/// Process-level fleets release reserved ports before their children bind them.
/// Keep the four fleet tests from racing each other for those ports.
static FLEET_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
                let deadline = Instant::now() + Duration::from_secs(5);
                loop {
                    let diagnostics = self.stderr_snapshot();
                    if diagnostics
                        .matches("hosted Iceberg catalog disabled: VERGLAS_CATALOG=off")
                        .count()
                        >= self.children.len()
                    {
                        return;
                    }
                    assert!(
                        Instant::now() < deadline,
                        "every Neon node must keep the hosted catalog closed: {diagnostics}"
                    );
                    thread::sleep(Duration::from_millis(10));
                }
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

/// Unique loopback ports consumed by one four-node process fleet.
struct FleetPorts {
    ring: [u16; 4],
    safekeeper: [u16; 4],
    block: [u16; 4],
    s3: [u16; 4],
    admin: [u16; 4],
}

/// Reserves every fleet port at once so the OS cannot return one port twice.
fn reserve_fleet_ports() -> FleetPorts {
    let reservations: [TcpListener; 20] =
        std::array::from_fn(|_| TcpListener::bind("127.0.0.1:0").expect("reserve fleet port"));
    let ports: [u16; 20] = std::array::from_fn(|index| {
        reservations[index]
            .local_addr()
            .expect("reserved fleet address")
            .port()
    });
    FleetPorts {
        ring: ports[0..4].try_into().expect("four ring ports"),
        safekeeper: ports[4..8].try_into().expect("four safekeeper ports"),
        block: ports[8..12].try_into().expect("four block ports"),
        s3: ports[12..16].try_into().expect("four S3 ports"),
        admin: ports[16..20].try_into().expect("four admin ports"),
    }
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
        "[listen]\ns3_port = {s3}\nadmin_port = {admin}\n\n[cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n[backend]\nbucket = \"wal-test\"\nbucket_globs = [\"catalog-test\"]\nendpoint = \"http://127.0.0.1:9\"\nallow_http = true\nregion = \"us-east-1\"\ncredentials_file = \"{}\"\n\n[catalog_archive]\nbucket = \"catalog-test\"\nprefix = \"_verglas/test-catalog\"\n\n[auth]\ncredentials_file = \"{}\"\n",
        node.display(),
        credentials.display(),
        credentials.display(),
    );
    std::fs::write(&config, body).expect("config");
    config
}

/// Starts one cache node against its retained directory and captures diagnostics.
fn spawn_node(
    index: usize,
    config: &std::path::Path,
    peers: &str,
    ring_port: u16,
    safekeeper_port: u16,
    block_port: u16,
    stderr: &Arc<Mutex<Vec<String>>>,
) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_verglas-cache-node"))
        .arg("--config")
        .arg(config)
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .env("VERGLAS_CATALOG", "off")
        .env("VERGLAS_NODE_ID", format!("node-{index}"))
        .env("VERGLAS_CATALOG_EVENT_TOKEN", "embedded-control-token")
        .env("VERGLAS_RING_PEERS", peers)
        .env("VERGLAS_SAFEKEEPER_EC_K", "2")
        .env("VERGLAS_SAFEKEEPER_EC_M", "2")
        .env("VERGLAS_SAFEKEEPER_EC_W", "3")
        .env("VERGLAS_RING_ADDR", format!("127.0.0.1:{ring_port}"))
        .env("VERGLAS_BLOCK_ADDR", format!("127.0.0.1:{block_port}"))
        .env(
            "VERGLAS_SAFEKEEPER_ADDR",
            format!("127.0.0.1:{safekeeper_port}"),
        )
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn cache node");
    let pipe = child.stderr.take().expect("piped stderr");
    let sink = Arc::clone(stderr);
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
    child
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

/// Reads the admin port rendered into one cache-node test config.
fn admin_port(config: &std::path::Path) -> u16 {
    std::fs::read_to_string(config)
        .expect("read node config")
        .lines()
        .find_map(|line| line.strip_prefix("admin_port = "))
        .and_then(|raw| raw.parse().ok())
        .expect("admin port in node config")
}

/// Commits the test timeline's required per-database WAL archive binding.
async fn bind_timeline(addr: SocketAddr) -> Result<(), String> {
    let response = reqwest::Client::new()
        .post(format!("http://{addr}/admin/neon/timeline-bindings"))
        .bearer_auth("embedded-control-token")
        .json(&serde_json::json!({
            "tenant_id": TENANT,
            "timeline_id": TIMELINE,
            "bucket": "wal-test",
        }))
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if response.status() == reqwest::StatusCode::NO_CONTENT {
        Ok(())
    } else {
        Err(format!(
            "bind timeline failed with {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ))
    }
}

/// Sends one complete canonical WAL request and leaves its HTTP status visible.
async fn submit_wal_http(
    addr: SocketAddr,
    request: WalRequest,
) -> Result<reqwest::Response, String> {
    let body = request.encode().expect("encode WAL request");
    reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| error.to_string())?
        .post(format!("http://{addr}/wal/v1/{TENANT}/{TIMELINE}"))
        .header("content-type", "application/octet-stream")
        .body(body)
        .send()
        .await
        .map_err(|error| error.to_string())
}

/// Submits one typed native-catalog request and decodes its linearizable response.
async fn submit_catalog(
    addr: SocketAddr,
    request: ManagedCatalogRequest,
) -> Result<ManagedCatalogResponse, String> {
    let response = reqwest::Client::builder()
        // Leader recovery verifies the retained catalog prefix before serving
        // authority. Match the process-level 30-second consensus bound rather
        // than imposing the old single-election five-second client deadline.
        .timeout(Duration::from_secs(30))
        .build()
        .map_err(|error| error.to_string())?
        .post(format!(
            "http://{addr}/catalog/v1/{CATALOG_TENANT}/{WAREHOUSE}"
        ))
        .json(&request)
        .send()
        .await
        .map_err(|error| error.to_string())?;
    if !response.status().is_success() {
        return Err(format!(
            "catalog operation failed with {}: {}",
            response.status(),
            response.text().await.unwrap_or_default()
        ));
    }
    response.json().await.map_err(|error| error.to_string())
}

/// Four real cache nodes preserve writes with one voter down and refuse two failures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cache_node_embeds_the_ring_backed_safekeeper() {
    let _fleet_guard = FLEET_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ports = reserve_fleet_ports();
    let ring_ports = ports.ring;
    let safekeeper_ports = ports.safekeeper;
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let configs = (0..4)
        .map(|index| write_config(root.path(), index, ports.s3[index], ports.admin[index]))
        .collect::<Vec<_>>();
    let mut children = Vec::new();
    for index in 0..4 {
        children.push(spawn_node(
            index,
            &configs[index],
            &peers,
            ring_ports[index],
            safekeeper_ports[index],
            ports.block[index],
            &stderr,
        ));
    }
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));
    bind_timeline(SocketAddr::from(([127, 0, 0, 1], admin_port(&configs[0]))))
        .await
        .expect("bind timeline archive bucket");

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

    fleet.children[0].kill().expect("stop elected leader");
    fleet.children[0].wait().expect("reap elected leader");
    let immediate = submit(
        SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[1])),
        WalRequest::ReadWal {
            from: start,
            to: end,
            minimum_index: append_index,
        },
    )
    .await
    .expect("a live ingress must ride out election and serve immediately after leader loss");
    assert_eq!(
        immediate,
        WalResponse::WalBytes {
            payload: wal.to_vec(),
        }
    );
    fleet.children[0] = spawn_node(
        0,
        &configs[0],
        &peers,
        ring_ports[0],
        safekeeper_ports[0],
        ports.block[0],
        &fleet.stderr,
    );
    fleet.wait_for_safekeepers(5, Duration::from_secs(30));

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
    assert_eq!(read, WalResponse::WalBytes { payload: expected });

    fleet.children[3] = spawn_node(
        3,
        &configs[3],
        &peers,
        ring_ports[3],
        safekeeper_ports[3],
        ports.block[3],
        &fleet.stderr,
    );
    fleet.wait_for_safekeepers(5, Duration::from_secs(30));

    fleet.children[2].kill().expect("stop third voter");
    fleet.children[2].wait().expect("reap third voter");
    let recovered_payload = b"through-restarted-follower";
    let recovered = submit(
        address,
        WalRequest::Append {
            request_id: (u128::from(writer_epoch) << 64) | u128::from(second_end),
            writer_epoch,
            start_lsn: second_end,
            payload: recovered_payload.to_vec(),
        },
    )
    .await
    .expect("a restarted durable follower must reopen its group from Raft traffic");
    assert!(matches!(recovered, WalResponse::Applied { .. }));

    fleet.children[3]
        .kill()
        .expect("stop restarted fourth voter");
    fleet.children[3]
        .wait()
        .expect("reap restarted fourth voter");
    let minority = submit(
        address,
        WalRequest::Append {
            request_id: (u128::from(writer_epoch) << 64) | u128::from(second_end + 1),
            writer_epoch,
            start_lsn: second_end + recovered_payload.len() as u64,
            payload: b"must-not-commit".to_vec(),
        },
    )
    .await;
    assert!(minority.is_err(), "two live voters must fail closed");
}

/// The public WAL listener accepts benchmark-sized appends but retains a hard body ceiling.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_listener_accepts_canonical_eight_mib_append_and_rejects_larger_bodies() {
    let _fleet_guard = FLEET_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ports = reserve_fleet_ports();
    let ring_ports = ports.ring;
    let safekeeper_ports = ports.safekeeper;
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let configs = (0..4)
        .map(|index| write_config(root.path(), index, ports.s3[index], ports.admin[index]))
        .collect::<Vec<_>>();
    let children = (0..4)
        .map(|index| {
            spawn_node(
                index,
                &configs[index],
                &peers,
                ring_ports[index],
                safekeeper_ports[index],
                ports.block[index],
                &stderr,
            )
        })
        .collect();
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));
    bind_timeline(SocketAddr::from(([127, 0, 0, 1], admin_port(&configs[0]))))
        .await
        .expect("bind timeline archive bucket");

    let address = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[0]));
    let opened = submit(
        address,
        WalRequest::OpenTimeline {
            request_id: 200,
            start_lsn: 0,
        },
    )
    .await
    .expect("open timeline");
    let start = match opened {
        WalResponse::Applied {
            wal_end: Some(start),
            ..
        } => start,
        other => panic!("unexpected timeline-open response: {other:?}"),
    };
    let acquired = submit(
        address,
        WalRequest::AcquireWriter {
            request_id: 201,
            writer: "body-limit-compute".to_owned(),
        },
    )
    .await
    .expect("acquire writer");
    let writer_epoch = match acquired {
        WalResponse::Applied {
            writer_epoch: Some(writer_epoch),
            ..
        } => writer_epoch,
        other => panic!("unexpected writer-acquire response: {other:?}"),
    };
    let payload = vec![0xa5; CANONICAL_WAL_APPEND_BYTES];
    let appended = submit_wal_http(
        address,
        WalRequest::Append {
            request_id: 202,
            writer_epoch,
            start_lsn: start,
            payload: payload.clone(),
        },
    )
    .await
    .expect("submit benchmark-sized WAL append");
    assert!(
        appended.status().is_success(),
        "canonical append reaches WAL handler"
    );
    let response = WalResponse::decode(&appended.bytes().await.expect("append response body"))
        .expect("decode append response");
    assert!(
        matches!(response, WalResponse::Applied { wal_end: Some(end), .. } if end == payload.len() as u64)
    );

    let rejected = submit_wal_http(
        address,
        WalRequest::Append {
            request_id: 203,
            writer_epoch,
            start_lsn: start + payload.len() as u64,
            payload: vec![0; WAL_BODY_LIMIT_BYTES],
        },
    )
    .await
    .expect("submit over-limit WAL append");
    assert_eq!(rejected.status(), reqwest::StatusCode::PAYLOAD_TOO_LARGE);
}

/// A large WAL tail continues through every surviving ingress after its first leader dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn large_wal_append_continues_after_exact_leader_death() {
    let _fleet_guard = FLEET_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ports = reserve_fleet_ports();
    let ring_ports = ports.ring;
    let safekeeper_ports = ports.safekeeper;
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let configs = (0..4)
        .map(|index| write_config(root.path(), index, ports.s3[index], ports.admin[index]))
        .collect::<Vec<_>>();
    let children = (0..4)
        .map(|index| {
            spawn_node(
                index,
                &configs[index],
                &peers,
                ring_ports[index],
                safekeeper_ports[index],
                ports.block[index],
                &stderr,
            )
        })
        .collect();
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));
    bind_timeline(SocketAddr::from(([127, 0, 0, 1], admin_port(&configs[0]))))
        .await
        .expect("bind timeline archive bucket");

    let leader = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[0]));
    let start = match submit(
        leader,
        WalRequest::OpenTimeline {
            request_id: 300,
            start_lsn: 0,
        },
    )
    .await
    .expect("open timeline through node zero")
    {
        WalResponse::Applied {
            wal_end: Some(start),
            ..
        } => start,
        other => panic!("unexpected timeline-open response: {other:?}"),
    };
    let writer_epoch = match submit(
        leader,
        WalRequest::AcquireWriter {
            request_id: 301,
            writer: "large-failover-compute".to_owned(),
        },
    )
    .await
    .expect("acquire writer through node zero")
    {
        WalResponse::Applied {
            writer_epoch: Some(writer_epoch),
            ..
        } => writer_epoch,
        other => panic!("unexpected writer-acquire response: {other:?}"),
    };

    let mut end = start;
    for sequence in 0..4_u128 {
        let payload = vec![sequence as u8; CANONICAL_WAL_APPEND_BYTES];
        let response = submit_wal_http(
            leader,
            WalRequest::Append {
                request_id: 0x1350_3000 + sequence,
                writer_epoch,
                start_lsn: end,
                payload,
            },
        )
        .await
        .expect("submit retained large WAL frame through node zero");
        assert!(
            response.status().is_success(),
            "retained large WAL frame {sequence} must commit; status={}; stderr:\n{}",
            response.status(),
            fleet.stderr_snapshot(),
        );
        let response = WalResponse::decode(&response.bytes().await.expect("append response body"))
            .expect("decode retained large WAL append response");
        end = match response {
            WalResponse::Applied {
                wal_end: Some(committed_end),
                ..
            } => committed_end,
            other => panic!("unexpected retained large WAL response: {other:?}"),
        };
    }

    fleet.children[0]
        .kill()
        .expect("stop exact node-zero leader after retained large tail");
    fleet.children[0]
        .wait()
        .expect("reap exact node-zero leader");

    let continuation = vec![4_u8; CANONICAL_WAL_APPEND_BYTES];
    for (index, port) in safekeeper_ports.iter().enumerate().skip(1) {
        let response = submit_wal_http(
            SocketAddr::from(([127, 0, 0, 1], *port)),
            WalRequest::Append {
                request_id: 0x1350_3004,
                writer_epoch,
                start_lsn: end,
                payload: continuation.clone(),
            },
        )
        .await
        .expect("submit large WAL continuation through surviving ingress");
        let status = response.status();
        let body = response.bytes().await.expect("continuation response body");
        assert!(
            status.is_success(),
            "surviving ingress node {index} rejected the large WAL continuation with {status}: {}; stderr:\n{}",
            String::from_utf8_lossy(&body),
            fleet.stderr_snapshot(),
        );
        let response = WalResponse::decode(&body).expect("decode continuation response");
        assert!(
            matches!(response, WalResponse::Applied { wal_end: Some(committed_end), .. } if committed_end == end + CANONICAL_WAL_APPEND_BYTES as u64),
            "surviving ingress node {index} returned an unexpected continuation response: {response:?}",
        );
    }
}

/// Four native-catalog ingresses retain linearizable availability after their elected leader dies.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_catalog_survives_leader_loss_with_a_retained_prefix() {
    let _fleet_guard = FLEET_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ports = reserve_fleet_ports();
    let ring_ports = ports.ring;
    let safekeeper_ports = ports.safekeeper;
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let configs = (0..4)
        .map(|index| write_config(root.path(), index, ports.s3[index], ports.admin[index]))
        .collect::<Vec<_>>();
    let mut children = Vec::new();
    for index in 0..4 {
        children.push(spawn_node(
            index,
            &configs[index],
            &peers,
            ring_ports[index],
            safekeeper_ports[index],
            ports.block[index],
            &stderr,
        ));
    }
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));

    let leader = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[0]));
    let namespace = CatalogBatch::new(
        vec![],
        vec![CatalogAction::CreateNamespace {
            namespace: "analytics".to_owned(),
        }],
    )
    .expect("namespace batch");
    assert!(matches!(
        submit_catalog(
            leader,
            ManagedCatalogRequest::Commit {
                request_id: 1,
                batch: namespace,
            },
        )
        .await
        .expect("commit catalog namespace through four voters"),
        ManagedCatalogResponse::Applied(_)
    ));

    // The dead-leader case must validate a realistic retained catalog prefix,
    // rather than only the one registration command in the tenant-root group.
    for revision in 0..106_u128 {
        let batch = CatalogBatch::new(
            vec![],
            vec![CatalogAction::PutTable {
                table: "analytics.lineitem".to_owned(),
                metadata_location: format!("s3://warehouse/lineitem/v{revision}.metadata.json"),
            }],
        )
        .expect("table batch");
        assert!(matches!(
            submit_catalog(
                leader,
                ManagedCatalogRequest::Commit {
                    request_id: revision + 2,
                    batch,
                },
            )
            .await
            .expect("commit retained catalog prefix"),
            ManagedCatalogResponse::Applied(_)
        ));
    }

    fleet.children[0]
        .kill()
        .expect("stop elected catalog leader");
    fleet.children[0]
        .wait()
        .expect("reap elected catalog leader");
    let survivor = SocketAddr::from(([127, 0, 0, 1], safekeeper_ports[1]));
    assert_eq!(
        submit_catalog(survivor, ManagedCatalogRequest::Namespaces)
            .await
            .expect("survivor serves an immediate linearizable catalog read"),
        ManagedCatalogResponse::Namespaces(vec!["analytics".to_owned()])
    );
    let mutation = CatalogBatch::new(
        vec![],
        vec![CatalogAction::PutTable {
            table: "analytics.lineitem".to_owned(),
            metadata_location: "s3://warehouse/lineitem/after-leader-loss.metadata.json".to_owned(),
        }],
    )
    .expect("post-loss table batch");
    assert!(matches!(
        submit_catalog(
            survivor,
            ManagedCatalogRequest::Commit {
                request_id: 10_000,
                batch: mutation,
            },
        )
        .await
        .expect("three live voters commit after catalog-leader loss"),
        ManagedCatalogResponse::Applied(_)
    ));

    fleet.children[3]
        .kill()
        .expect("stop a second catalog voter");
    fleet.children[3].wait().expect("reap second catalog voter");
    let minority = CatalogBatch::new(
        vec![],
        vec![CatalogAction::PutTable {
            table: "analytics.lineitem".to_owned(),
            metadata_location: "s3://warehouse/lineitem/must-not-commit.metadata.json".to_owned(),
        }],
    )
    .expect("minority table batch");
    assert!(
        submit_catalog(
            survivor,
            ManagedCatalogRequest::Commit {
                request_id: 10_001,
                batch: minority,
            },
        )
        .await
        .is_err(),
        "two live voters must reject a catalog mutation"
    );
}

/// Exact wire repro: create a hosted-domain record, point-read it back, then
/// enumerate its collection. `Records` must return exactly what `Record`
/// already proved durable, on every node — the live production bug had
/// enumeration return nothing forever while point reads kept succeeding.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn native_catalog_records_enumeration_matches_point_read() {
    let _fleet_guard = FLEET_TEST_LOCK.lock().await;
    let root = tempfile::tempdir().expect("fleet tempdir");
    let ports = reserve_fleet_ports();
    let ring_ports = ports.ring;
    let safekeeper_ports = ports.safekeeper;
    let peers = ring_ports
        .iter()
        .enumerate()
        .map(|(index, port)| format!("node-{index}=127.0.0.1:{port}"))
        .collect::<Vec<_>>()
        .join(",");
    let stderr = Arc::new(Mutex::new(Vec::new()));
    let configs = (0..4)
        .map(|index| write_config(root.path(), index, ports.s3[index], ports.admin[index]))
        .collect::<Vec<_>>();
    let mut children = Vec::new();
    for index in 0..4 {
        children.push(spawn_node(
            index,
            &configs[index],
            &peers,
            ring_ports[index],
            safekeeper_ports[index],
            ports.block[index],
            &stderr,
        ));
    }
    let mut fleet = Fleet { children, stderr };
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));

    let record_id = "namespace/default/analytics";
    let record_document = r#"{"namespace":["analytics"],"properties":{}}"#;
    let create_record = CatalogBatch::new(
        vec![CatalogRequirement::RecordAbsent {
            entity: CatalogEntity::Namespace,
            id: record_id.to_owned(),
        }],
        vec![CatalogAction::PutRecord {
            entity: CatalogEntity::Namespace,
            id: record_id.to_owned(),
            document: record_document.to_owned(),
        }],
    )
    .expect("hosted namespace record batch");

    for (index, &port) in safekeeper_ports.iter().enumerate() {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        if index == 0 {
            assert!(
                matches!(
                    submit_catalog(
                        addr,
                        ManagedCatalogRequest::Commit {
                            request_id: 1,
                            batch: create_record.clone(),
                        },
                    )
                    .await
                    .expect("commit hosted namespace record through four voters"),
                    ManagedCatalogResponse::Applied(_)
                ),
                "record commit must apply"
            );
        }

        assert_eq!(
            submit_catalog(
                addr,
                ManagedCatalogRequest::Record {
                    entity: CatalogEntity::Namespace,
                    id: record_id.to_owned(),
                },
            )
            .await
            .expect("point read must succeed on every node"),
            ManagedCatalogResponse::Record(Some(record_document.to_owned())),
            "node {index} point read must see the committed record"
        );

        assert_eq!(
            submit_catalog(
                addr,
                ManagedCatalogRequest::Records {
                    entity: CatalogEntity::Namespace,
                },
            )
            .await
            .expect("enumeration must succeed on every node"),
            ManagedCatalogResponse::Records(vec![(
                record_id.to_owned(),
                record_document.to_owned()
            )]),
            "node {index} enumeration must include the record its own point read sees"
        );
    }

    // Restart the whole fleet against its retained data directories. A
    // "forever empty" enumeration bug would plausibly show only after the
    // state machine reloads its persisted image from disk, so re-verify
    // Record vs Records after every node reopens its durable state.
    for child in &mut fleet.children {
        let _ = child.kill();
        let _ = child.wait();
    }
    let mut restarted = Vec::new();
    for index in 0..4 {
        restarted.push(spawn_node(
            index,
            &configs[index],
            &peers,
            ring_ports[index],
            safekeeper_ports[index],
            ports.block[index],
            &fleet.stderr,
        ));
    }
    fleet.children = restarted;
    fleet.wait_for_safekeepers(4, Duration::from_secs(30));

    for (index, &port) in safekeeper_ports.iter().enumerate() {
        let addr = SocketAddr::from(([127, 0, 0, 1], port));
        assert_eq!(
            submit_catalog_retrying(
                addr,
                ManagedCatalogRequest::Record {
                    entity: CatalogEntity::Namespace,
                    id: record_id.to_owned(),
                },
                Duration::from_secs(30),
            )
            .await
            .expect("point read must survive a full-fleet restart"),
            ManagedCatalogResponse::Record(Some(record_document.to_owned())),
            "node {index} point read must see the record after restart"
        );

        assert_eq!(
            submit_catalog_retrying(
                addr,
                ManagedCatalogRequest::Records {
                    entity: CatalogEntity::Namespace,
                },
                Duration::from_secs(30),
            )
            .await
            .expect("enumeration must survive a full-fleet restart"),
            ManagedCatalogResponse::Records(vec![(
                record_id.to_owned(),
                record_document.to_owned()
            )]),
            "node {index} enumeration must include the record after restart"
        );
    }
}

/// Retries `submit_catalog` while the just-restarted listener is not yet
/// accepting connections or its local group has not finished re-electing.
async fn submit_catalog_retrying(
    addr: SocketAddr,
    request: ManagedCatalogRequest,
    deadline: Duration,
) -> Result<ManagedCatalogResponse, String> {
    let start = Instant::now();
    loop {
        match submit_catalog(addr, request.clone()).await {
            Ok(response) => return Ok(response),
            Err(error) if start.elapsed() < deadline => {
                tokio::time::sleep(Duration::from_millis(100)).await;
                let _ = error;
            }
            Err(error) => return Err(error),
        }
    }
}
