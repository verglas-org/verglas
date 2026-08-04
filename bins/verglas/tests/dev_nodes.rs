//! Live acceptance tests for `verglas dev --nodes N` (issues #160, #194).
//!
//! These boot real `verglasd` children through the `verglas dev` command and
//! assert the pod-shaped behavior the issues' acceptance criteria demand:
//! - `--nodes 3` boots three daemons, each `/admin/stats` reporting its OWN
//!   budgets and each `/admin/members` converging on the 3-member view;
//! - a graceful Ctrl-C (SIGINT) tears every child down and removes the
//!   ephemeral per-node cache dirs (no leaked processes, no leaked temp dirs);
//! - the two-phase death policy: a node dying AFTER a healthy boot leaves the
//!   survivors serving — gossip drops it from the surviving members' view within
//!   the suspicion window — and the final exit after Ctrl-C is non-zero so
//!   scripts still detect the degraded run;
//! - port selection is race-free by construction (#194): every node binds
//!   `127.0.0.1:0` and reports the kernel-assigned ports, so the tests read the
//!   ports back rather than probing them free, and two pods boot concurrently
//!   with no coordination and neither loses a port race.
//!
//! They locate `verglasd` next to the `verglas` test binary (the same install
//! layout `verglas dev` uses at runtime); `cargo test --workspace` builds both.

use std::collections::BTreeSet;
use std::io::Read as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use verglas_core::admin::{MembersInfo, StatsInfo};

/// Path to the `verglas` CLI binary under test.
fn verglas_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_verglas"))
}

/// The origin backend flags every `verglas dev` invocation now requires
/// (`--endpoint`/`--region`/`--credentials-file`). These tests exercise pod
/// lifecycle, not serving — the origin is never reached — so a placeholder
/// endpoint and a dummy AWS-format credentials file are enough. The creds file
/// is written under a per-process temp dir so its path outlives the call.
fn backend_args() -> Vec<String> {
    let dir = std::env::temp_dir().join(format!("verglas-devtest-creds-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("creds dir");
    let creds = dir.join("backend-credentials");
    std::fs::write(
        &creds,
        "[default]\naws_access_key_id = dev\naws_secret_access_key = dev\n",
    )
    .expect("write dummy backend creds");
    vec![
        "--endpoint".into(),
        "https://backend.invalid".into(),
        "--region".into(),
        "us-east-1".into(),
        "--credentials-file".into(),
        creds.display().to_string(),
    ]
}

/// Binds an ephemeral port, reads it, and releases it — a free port for the
/// test that exercises the EXPLICIT `--port` path (the one path that still pins a
/// fixed port and keeps the loud collision error, #194). The default port path
/// never probes; it binds `:0` and reports, which is what every other test uses.
fn a_free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .expect("bind ephemeral")
        .local_addr()
        .expect("addr")
        .port()
}

/// Names of the entries in a directory — used to prove teardown removed every
/// temp artifact from the pod's private `TMPDIR`.
fn dir_entries(dir: &Path) -> BTreeSet<String> {
    let mut out = BTreeSet::new();
    if let Ok(entries) = std::fs::read_dir(dir) {
        for e in entries.flatten() {
            if let Some(name) = e.file_name().to_str() {
                out.insert(name.to_owned());
            }
        }
    }
    out
}

/// The resolved `(s3, admin)` `host:port` endpoints `verglas dev --ports-file`
/// reports for one node.
#[derive(Clone)]
struct NodePorts {
    s3: String,
    admin: String,
}

impl NodePorts {
    /// This node's admin API base URL.
    fn admin_url(&self) -> String {
        format!("http://{}", self.admin)
    }

    /// This node's admin port, for `pid_listening_on`.
    fn admin_port(&self) -> u16 {
        self.admin
            .rsplit(':')
            .next()
            .and_then(|p| p.parse().ok())
            .expect("admin port parses")
    }
}

/// Opens a stderr capture file for a `verglas dev` child in `dir`: the spawn
/// redirects the parent's (and, inherited, the daemons') stderr here instead of
/// discarding it, so a boot failure panics WITH the daemon's own error text.
/// Startup output must never be swallowed — a bare `exit status: 1` gives
/// nothing to act on.
fn stderr_capture(dir: &Path) -> (Stdio, PathBuf) {
    let path = dir.join("dev-stderr.log");
    let file = std::fs::File::create(&path).expect("create stderr capture");
    (Stdio::from(file), path)
}

/// The captured stderr so far, trimmed, for inclusion in a panic message.
fn captured(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_default()
        .trim()
        .to_owned()
}

/// Polls the `verglas dev --ports-file` output until every node's line is
/// present, returning each node's resolved endpoints (issue #194). `verglas dev`
/// writes the file once the whole pod is up, so a full read means every node is
/// ready. Panics if the parent exits early or the file never fills — either is a
/// real failure of the boot the test is asserting — and the panic carries the
/// child's stderr (via `stderr_text`) so the daemon's own error is in the
/// report, not just an exit status.
fn read_dev_ports(
    path: &Path,
    nodes: usize,
    child: &mut Child,
    stderr_text: impl Fn() -> String,
) -> Vec<NodePorts> {
    let deadline = Instant::now() + Duration::from_secs(45);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!(
                "verglas dev exited before reporting its ports: {status}; child stderr:\n{}",
                stderr_text()
            );
        }
        if let Ok(text) = std::fs::read_to_string(path) {
            let mut out: Vec<NodePorts> = Vec::new();
            for line in text.lines() {
                // Each line is `node <i> s3=<addr> admin=<addr>`.
                let mut s3 = None;
                let mut admin = None;
                for tok in line.split_whitespace() {
                    if let Some(v) = tok.strip_prefix("s3=") {
                        s3 = Some(v.to_owned());
                    }
                    if let Some(v) = tok.strip_prefix("admin=") {
                        admin = Some(v.to_owned());
                    }
                }
                if let (Some(s3), Some(admin)) = (s3, admin) {
                    out.push(NodePorts { s3, admin });
                }
            }
            if out.len() == nodes {
                return out;
            }
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "verglas dev did not report {nodes} node port line(s) in 45s; child stderr:\n{}",
        stderr_text()
    );
}

/// Fetches and decodes `GET /admin/stats`, `None` until the daemon serves it.
fn fetch_stats(admin_url: &str) -> Option<StatsInfo> {
    reqwest::blocking::get(format!("{admin_url}/admin/stats"))
        .ok()
        .filter(|r| r.status().is_success())
        .and_then(|r| r.json::<StatsInfo>().ok())
}

/// Fetches and decodes `GET /admin/members`, `None` until gossip has a view.
fn fetch_members(admin_url: &str) -> Option<MembersInfo> {
    reqwest::blocking::get(format!("{admin_url}/admin/members"))
        .ok()
        .filter(|r| r.status().is_success())
        .and_then(|r| r.json::<MembersInfo>().ok())
}

/// Blocks until `admin_url` answers `/admin/stats`, or the deadline passes.
fn wait_for_stats(admin_url: &str) -> bool {
    let deadline = Instant::now() + Duration::from_secs(30);
    while Instant::now() < deadline {
        if fetch_stats(admin_url).is_some() {
            return true;
        }
        std::thread::sleep(Duration::from_millis(150));
    }
    false
}

/// Sends SIGINT to a child so its Ctrl-C teardown path runs (SIGKILL would skip
/// the graceful shutdown and the temp-dir cleanup it triggers).
#[cfg(unix)]
fn interrupt(child: &Child) {
    let _ = Command::new("kill")
        .args(["-INT", &child.id().to_string()])
        .status();
}

#[cfg(not(unix))]
fn interrupt(child: &Child) {
    // No portable graceful signal off unix; the tests here run on unix CI.
    let _ = child;
}

/// Sends SIGTERM to a child — the signal `kill`/`pkill` and process supervisors
/// send by default. `verglas dev` must treat it exactly like SIGINT (issue
/// #170): the same reverse-order teardown and ephemeral-cache cleanup.
#[cfg(unix)]
fn terminate(child: &Child) {
    let _ = Command::new("kill")
        .args(["-TERM", &child.id().to_string()])
        .status();
}

/// SIGKILLs a process by pid (an uncatchable kill), used to simulate the
/// `verglas dev` parent dying with no chance to run any handler (issue #170).
#[cfg(unix)]
fn hard_kill(pid: u32) {
    let _ = Command::new("kill")
        .args(["-KILL", &pid.to_string()])
        .status();
}

/// PID of the process listening on a loopback TCP port, via `lsof` (present on
/// macOS and the Linux CI runners). Used to kill one pod node from outside.
#[cfg(unix)]
fn pid_listening_on(port: u16) -> Option<u32> {
    let out = Command::new("lsof")
        .args(["-ti", &format!("tcp:{port}"), "-sTCP:LISTEN"])
        .output()
        .ok()?;
    String::from_utf8(out.stdout)
        .ok()?
        .lines()
        .next()?
        .trim()
        .parse()
        .ok()
}

// The child's lifecycle is driven by hand (poll `try_wait`, SIGINT, then a
// final `wait`/`kill`) so the process is always reaped; clippy's simple
// all-paths heuristic cannot see that.
#[allow(clippy::zombie_processes)]
#[test]
fn pod_of_three_boots_independent_budgets_converges_and_tears_down() {
    // A private TMPDIR isolates this pod's ephemeral cache/config artifacts so
    // the leak check below cannot race with concurrently running pod tests. The
    // dev ports file lives elsewhere so it is not counted as a leftover.
    let scratch = tempfile::tempdir().expect("scratch tmpdir");
    let ports_dir = tempfile::tempdir().expect("ports dir");
    let ports_file = ports_dir.path().join("ports");
    let (stderr, stderr_log) = stderr_capture(ports_dir.path());

    let mut child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--nodes",
            "3",
            "--dram",
            "80MB",
            "--cache-size",
            "122MB",
            "--ports-file",
        ])
        .arg(&ports_file)
        .args(backend_args())
        .env("TMPDIR", scratch.path())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("spawn verglas dev");

    // Learn the kernel-assigned ports the pod reports (issue #194) — no probing.
    let nodes = read_dev_ports(&ports_file, 3, &mut child, || captured(&stderr_log));
    let admin = |i: usize| nodes[i].admin_url();

    // Each node reports ITS OWN configured budgets over /admin/stats.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut all_up = false;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("verglas dev exited early with {status}");
        }
        if (0..3).all(|i| {
            fetch_stats(&admin(i)).is_some_and(|s| {
                s.cache.dram_bytes == 80 * 1024 * 1024
                    && s.cache.capacity_bytes == 122 * 1024 * 1024
            })
        }) {
            all_up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(
        all_up,
        "three nodes did not boot with their own budgets in 30s"
    );

    // Gossip converges: every node's /admin/members reports all three members.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut converged = false;
    while Instant::now() < deadline {
        if (0..3).all(|i| fetch_members(&admin(i)).is_some_and(|m| m.members.len() == 3)) {
            converged = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        converged,
        "the 3-node pod did not converge on 3 members in 30s"
    );

    // Graceful teardown: SIGINT, then the command exits cleanly.
    interrupt(&child);
    let mut exited = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(status.success(), "verglas dev exited with {status}");
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
        panic!("verglas dev did not exit within 15s of SIGINT");
    }

    // No leaked daemons: every admin port is unreachable after teardown.
    for i in 0..3 {
        assert!(
            fetch_stats(&admin(i)).is_none(),
            "node {i} still answering after teardown"
        );
    }

    // No leaked temp artifacts: the pod's private TMPDIR is empty again (the
    // ephemeral cache root and every node's config tempfile were removed).
    let leftovers = dir_entries(scratch.path());
    assert!(
        leftovers.is_empty(),
        "ephemeral pod temp artifacts leaked: {leftovers:?}"
    );
}

// The children are reaped by hand; clippy's all-paths heuristic cannot see it.
#[allow(clippy::zombie_processes)]
#[test]
fn two_pods_boot_concurrently_without_port_coordination() {
    // Regression for #194 (TDD): two independent `verglas dev` pods, launched at
    // once with no --port and no shared coordination, must both come up. Under
    // the old probe-then-bind default (a fixed base port) they raced for the same
    // ports and one lost — deterministically, since both defaulted to the same
    // base. Binding `:0` and reporting the resolved ports removes the race by
    // construction: the kernel hands out distinct ports and both pods boot.
    let dir_a = tempfile::tempdir().expect("dir a");
    let dir_b = tempfile::tempdir().expect("dir b");
    let ports_a = dir_a.path().join("ports");
    let ports_b = dir_b.path().join("ports");
    let (stderr_a, stderr_log_a) = stderr_capture(dir_a.path());
    let (stderr_b, stderr_log_b) = stderr_capture(dir_b.path());

    let spawn_pod = |ports: &Path, stderr: Stdio| {
        Command::new(verglas_bin())
            .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
            .args([
                "dev",
                "--bucket",
                "dev-bucket",
                "--nodes",
                "2",
                "--dram",
                "80MB",
                "--cache-size",
                "122MB",
                "--ports-file",
            ])
            .arg(ports)
            .args(backend_args())
            .stdout(Stdio::null())
            .stderr(stderr)
            .spawn()
            .expect("spawn verglas dev")
    };

    // Launch both before reading either, so their boots genuinely overlap.
    let mut pod_a = spawn_pod(&ports_a, stderr_a);
    let mut pod_b = spawn_pod(&ports_b, stderr_b);

    let nodes_a = read_dev_ports(&ports_a, 2, &mut pod_a, || captured(&stderr_log_a));
    let nodes_b = read_dev_ports(&ports_b, 2, &mut pod_b, || captured(&stderr_log_b));

    // Every node of both pods answers — all eight listeners bound distinct ports.
    for node in nodes_a.iter().chain(nodes_b.iter()) {
        assert!(
            wait_for_stats(&node.admin_url()),
            "a node did not answer after a concurrent boot: {}",
            node.admin
        );
    }

    // The two pods took entirely distinct ports (no overlap between the eight).
    let mut all: Vec<String> = nodes_a
        .iter()
        .chain(nodes_b.iter())
        .flat_map(|n| [n.s3.clone(), n.admin.clone()])
        .collect();
    let count = all.len();
    all.sort();
    all.dedup();
    assert_eq!(
        all.len(),
        count,
        "two concurrent pods reused a port: {all:?}"
    );

    for pod in [&pod_a, &pod_b] {
        interrupt(pod);
    }
    for pod in [&mut pod_a, &mut pod_b] {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if pod.try_wait().expect("try_wait").is_some() {
                break;
            }
            if Instant::now() > deadline {
                let _ = pod.kill();
                let _ = pod.wait();
                break;
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    }
}

#[cfg(unix)]
#[test]
fn pod_boot_failure_of_one_node_tears_down_and_exits_nonzero() {
    // Phase 1 of the two-phase death policy on the EXPLICIT `--port` path: a
    // node whose fixed port is already taken must not start the pod. Occupy node
    // 1's S3 port (base + 1*4) so the pre-flight refuses, tears down, and exits
    // non-zero naming the node whose port is held. Explicit ports keep this loud
    // pre-flight (issue #194); the default ephemeral path cannot collide.
    let (base, squatter) = {
        let mut acquired = None;
        for _ in 0..50 {
            let base = a_free_port();
            if let Ok(sq) = TcpListener::bind(("127.0.0.1", base + 4)) {
                acquired = Some((base, sq));
                break;
            }
        }
        acquired.expect("could not reserve and hold node 1's S3 port")
    };

    let child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--nodes",
            "2",
            "--dram",
            "80MB",
            "--cache-size",
            "122MB",
            "--port",
            &base.to_string(),
        ])
        .args(backend_args())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglas dev");

    let output = child.wait_with_output().expect("wait");
    drop(squatter);

    assert!(
        !output.status.success(),
        "pod with an unbootable node must exit non-zero"
    );
    let mut stderr = String::new();
    let _ = output.stderr.as_slice().read_to_string(&mut stderr);
    // The pre-flight refuses loudly and names a colliding node's port. (Which
    // node depends on which fixed port is contended — under concurrent tests an
    // ephemeral daemon may also hold node 0's port — so the assertion pins the
    // loud collision shape, not a specific node.)
    assert!(
        stderr.contains("is already in use") && stderr.contains("lsof -i :"),
        "error must name a colliding port and hand over the lsof line; stderr was: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("node "),
        "error must name the node whose port is held; stderr was: {stderr}"
    );
}

// The child's lifecycle is driven by hand (poll `try_wait`, SIGINT, then a
// final `wait`/`kill`); clippy's all-paths heuristic cannot see the reap.
#[allow(clippy::zombie_processes)]
#[cfg(unix)]
#[test]
fn post_boot_death_of_one_node_keeps_survivors_serving_and_exits_nonzero() {
    // Phase 2 of the two-phase death policy: after a healthy boot, a dying
    // node must NOT kill the pod — the survivors keep serving, the surviving
    // members' /admin/members drops to 2 within the suspicion window, a loud
    // notice names the dead node, and the final exit after Ctrl-C is non-zero
    // so scripts detect the degraded run.
    let ports_dir = tempfile::tempdir().expect("ports dir");
    let ports_file = ports_dir.path().join("ports");

    let mut child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--nodes",
            "3",
            "--dram",
            "80MB",
            "--cache-size",
            "122MB",
            "--ports-file",
        ])
        .arg(&ports_file)
        .args(backend_args())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglas dev");

    // Drain stderr on a thread (children inherit it; an unread pipe would
    // eventually block the daemons' logging).
    let stderr_pipe = child.stderr.take().expect("stderr piped");
    let stderr_buf = std::sync::Arc::new(std::sync::Mutex::new(String::new()));
    let stderr_sink = std::sync::Arc::clone(&stderr_buf);
    let reader = std::thread::spawn(move || {
        let mut pipe = stderr_pipe;
        let mut chunk = [0u8; 4096];
        loop {
            match pipe.read(&mut chunk) {
                Ok(0) | Err(_) => break,
                Ok(n) => {
                    if let Ok(mut buf) = stderr_sink.lock() {
                        buf.push_str(&String::from_utf8_lossy(&chunk[..n]));
                    }
                }
            }
        }
    });

    let nodes = read_dev_ports(&ports_file, 3, &mut child, || {
        stderr_buf.lock().map(|b| b.clone()).unwrap_or_default()
    });
    let admin = |i: usize| nodes[i].admin_url();

    // Healthy boot: all three nodes serve stats and converge on 3 members.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut booted = false;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("verglas dev exited during boot with {status}");
        }
        if (0..3).all(|i| fetch_stats(&admin(i)).is_some())
            && (0..3).all(|i| fetch_members(&admin(i)).is_some_and(|m| m.members.len() == 3))
        {
            booted = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(booted, "the 3-node pod did not boot and converge in 30s");

    // Kill node 2's daemon out from under the pod (its admin port listener).
    let pid = pid_listening_on(nodes[2].admin_port()).expect("node 2 daemon pid");
    assert!(
        Command::new("kill")
            .args(["-KILL", &pid.to_string()])
            .status()
            .expect("kill")
            .success(),
        "kill -KILL node 2"
    );

    // The pod does NOT exit: survivors keep serving, and their membership view
    // drops to 2 within the suspicion window (default 5s; generous deadline).
    let deadline = Instant::now() + Duration::from_secs(45);
    let mut dropped = false;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("verglas dev must survive a post-boot node death; exited with {status}");
        }
        if (0..2).all(|i| fetch_stats(&admin(i)).is_some())
            && (0..2).all(|i| fetch_members(&admin(i)).is_some_and(|m| m.members.len() == 2))
        {
            dropped = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    assert!(
        dropped,
        "survivors did not keep serving / drop to 2 members within the window"
    );

    // The loud notice named the dead node.
    {
        let buf = stderr_buf.lock().expect("stderr buf");
        assert!(
            buf.contains("node 2"),
            "the death notice must name node 2; stderr so far: {buf}"
        );
    }

    // Ctrl-C teardown still works — and the exit is NON-zero (degraded run).
    interrupt(&child);
    let mut status = None;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Some(s) = child.try_wait().expect("try_wait") {
            status = Some(s);
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    let Some(status) = status else {
        let _ = child.kill();
        let _ = child.wait();
        panic!("verglas dev did not exit within 15s of SIGINT");
    };
    assert!(
        !status.success(),
        "a pod that lost a node mid-run must exit non-zero"
    );
    let _ = reader.join();

    // No leaked daemons after teardown.
    for i in 0..3 {
        assert!(
            fetch_stats(&admin(i)).is_none(),
            "node {i} still answering after teardown"
        );
    }
}

// The child's lifecycle is driven by hand (poll `try_wait`, SIGTERM, then a
// final `wait`/`kill`) so the process is always reaped; clippy's simple
// all-paths heuristic cannot see that.
#[allow(clippy::zombie_processes)]
#[cfg(unix)]
#[test]
fn sigterm_to_the_parent_tears_down_the_pod_like_ctrl_c() {
    // Acceptance (issue #170): a SIGTERM to the `verglas dev` parent must tear
    // every child down, free the ports, and remove the ephemeral per-node
    // caches — byte-for-byte the SIGINT teardown. This mirrors the SIGINT pod
    // test above but delivers SIGTERM instead of SIGINT.
    let scratch = tempfile::tempdir().expect("scratch tmpdir");
    let ports_dir = tempfile::tempdir().expect("ports dir");
    let ports_file = ports_dir.path().join("ports");
    let (stderr, stderr_log) = stderr_capture(ports_dir.path());

    let mut child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--nodes",
            "2",
            "--dram",
            "80MB",
            "--cache-size",
            "122MB",
            "--ports-file",
        ])
        .arg(&ports_file)
        .args(backend_args())
        .env("TMPDIR", scratch.path())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("spawn verglas dev");

    let nodes = read_dev_ports(&ports_file, 2, &mut child, || captured(&stderr_log));
    let admin = |i: usize| nodes[i].admin_url();

    // Both nodes boot and answer stats.
    let deadline = Instant::now() + Duration::from_secs(30);
    let mut up = false;
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            panic!("verglas dev exited early with {status}");
        }
        if (0..2).all(|i| fetch_stats(&admin(i)).is_some()) {
            up = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }
    assert!(up, "the 2-node pod did not boot in 30s");

    // SIGTERM (not SIGINT): the same graceful teardown must run.
    terminate(&child);
    let mut exited = false;
    let deadline = Instant::now() + Duration::from_secs(15);
    while Instant::now() < deadline {
        if let Some(status) = child.try_wait().expect("try_wait") {
            assert!(
                status.success(),
                "verglas dev exited with {status} on SIGTERM"
            );
            exited = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(100));
    }
    if !exited {
        let _ = child.kill();
        let _ = child.wait();
        panic!("verglas dev did not exit within 15s of SIGTERM");
    }

    // Ports freed: neither node still answers.
    for i in 0..2 {
        assert!(
            fetch_stats(&admin(i)).is_none(),
            "node {i} still answering after SIGTERM teardown"
        );
    }

    // Ephemeral caches removed: the pod's private TMPDIR is empty again.
    let leftovers = dir_entries(scratch.path());
    assert!(
        leftovers.is_empty(),
        "ephemeral pod temp artifacts leaked after SIGTERM: {leftovers:?}"
    );
}

// The parent is SIGKILLed and reaped by hand; clippy's all-paths heuristic
// cannot see the reap.
#[allow(clippy::zombie_processes)]
#[cfg(unix)]
#[test]
fn sigkill_to_the_parent_still_stops_the_child_daemon() {
    // Acceptance (issue #170): SIGKILL to the parent leaves no chance to run any
    // handler, yet the orphaned `verglasd` child must still exit within a
    // bounded window via its parent-death watch, freeing the port instead of
    // squatting it with stale keys. With ephemeral ports (#194) the test reads
    // the daemon's reported admin port rather than a probed-free one.
    let ports_dir = tempfile::tempdir().expect("ports dir");
    let ports_file = ports_dir.path().join("ports");
    let (stderr, stderr_log) = stderr_capture(ports_dir.path());

    let mut child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--nodes",
            "1",
            // Explicit small budgets like every other test here: a lifecycle
            // test needs no real cache, and the daemon validates
            // cache.capacity_bytes against the free disk backing its cache
            // dir — the 20GB default fails on a nearly-full machine or a CI
            // runner whose target/ tree has eaten the headroom.
            "--dram",
            "80MB",
            "--cache-size",
            "122MB",
            "--ports-file",
        ])
        .arg(&ports_file)
        .args(backend_args())
        .stdout(Stdio::null())
        .stderr(stderr)
        .spawn()
        .expect("spawn verglas dev");

    let nodes = read_dev_ports(&ports_file, 1, &mut child, || captured(&stderr_log));
    let admin_url = nodes[0].admin_url();

    // Wait until the single daemon is serving.
    assert!(
        wait_for_stats(&admin_url),
        "the single-node daemon did not boot in 30s"
    );

    // Record the orphan's pid so the test can guarantee cleanup even on failure.
    let daemon_pid = pid_listening_on(nodes[0].admin_port());

    // SIGKILL the parent (uncatchable — no teardown handler can run) and reap it.
    hard_kill(child.id());
    let _ = child.wait();

    // The orphaned child must exit on its own within a bounded window: the watch
    // polls its parent pid and shuts down when it changes (reparented to pid 1).
    let deadline = Instant::now() + Duration::from_secs(15);
    let mut freed = false;
    while Instant::now() < deadline {
        if fetch_stats(&admin_url).is_none() {
            freed = true;
            break;
        }
        std::thread::sleep(Duration::from_millis(200));
    }

    // Belt-and-braces cleanup: if the watch failed, do not leak the daemon.
    if !freed && let Some(pid) = daemon_pid {
        hard_kill(pid);
    }
    assert!(
        freed,
        "orphaned verglasd kept holding its port after the parent was SIGKILLed"
    );
}

#[cfg(unix)]
#[test]
fn port_collision_at_startup_names_the_port_and_hints_at_orphans() {
    // Acceptance (issue #170): an EXPLICIT `--port` that is already taken must be
    // loud — the error names the port and hands the operator the `lsof -i :<port>`
    // line to find the orphan daemon squatting it, blaming a prior `verglas dev`
    // that did not shut down cleanly. This is the one path that still pins a fixed
    // port (issue #194 keeps the collision error only for explicit `--port`).
    let base = a_free_port();
    // Squat the S3 port before launching so the pre-flight bind check trips.
    let squatter = TcpListener::bind(("127.0.0.1", base)).expect("hold the S3 port");

    let child = Command::new(verglas_bin())
        // Pod-lifecycle tests point at a placeholder origin; skip the #233
        // startup probe so the daemons boot without a reachable backend.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .args([
            "dev",
            "--bucket",
            "dev-bucket",
            "--port",
            &base.to_string(),
            "--nodes",
            "1",
        ])
        .args(backend_args())
        .stdout(Stdio::null())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn verglas dev");

    let output = child.wait_with_output().expect("wait");
    drop(squatter);

    assert!(
        !output.status.success(),
        "a port collision at startup must exit non-zero"
    );
    let mut stderr = String::new();
    let _ = output.stderr.as_slice().read_to_string(&mut stderr);
    assert!(
        stderr.contains(&base.to_string()),
        "the error must name the colliding port; stderr was: {stderr}"
    );
    assert!(
        stderr.contains(&format!("lsof -i :{base}")),
        "the error must hand over the lsof line; stderr was: {stderr}"
    );
    assert!(
        stderr.to_lowercase().contains("verglas dev"),
        "the error must blame a prior verglas dev orphan; stderr was: {stderr}"
    );
}
