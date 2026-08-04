//! End-to-end subprocess proof of the graceful-drain lifecycle (#31): a real
//! localhost gossip pod, drained through the same `POST /admin/drain` control
//! `verglas drain` performs against its local daemon, then observed to
//! rebalance.
//!
//! The daemon drain glue exercised here — the cluster agent gossiping itself
//! `draining`, the `/admin/drain` handler, and the timed clean exit — is not
//! reachable from an in-process unit test (it ends in `process::exit`). This
//! test drives it against real `verglasd` processes and asserts the acceptance
//! criteria: the drain is acked `draining`, and after the drained node exits the
//! survivors' ring rebalances to exclude it.

use std::io::Write;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use verglas_core::admin::{DrainAck, DrainRequest, MembersInfo};

/// Reserves a free localhost port (binds then drops); a small race window
/// remains, acceptable for tests.
fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
    let port = listener.local_addr().expect("addr").port();
    drop(listener);
    port
}

/// A spawned daemon plus its admin endpoint and scratch dir (kept alive so it is
/// not deleted while the daemon runs).
struct Daemon {
    child: Child,
    node_id: String,
    endpoint: String,
    _dir: tempfile::TempDir,
}

impl Drop for Daemon {
    /// Kills and reaps the daemon when the handle drops (a no-op if it already
    /// exited on its own via the drain timer).
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Writes a `[cluster]` config (with the admin port *in the config*, so the
/// gossiped admin address matches the real one and membership resolution works)
/// and spawns `verglasd` for one pod node.
fn spawn_node(node_id: &str, gossip_port: u16, seed_port: u16, admin_port: u16) -> Daemon {
    let admin_addr = format!("127.0.0.1:{admin_port}");
    let dir = tempfile::tempdir().expect("temp dir");
    let toml = format!(
        "[listen]\nadmin_port = {admin_port}\n\n\
         [cache]\ndir = \"{}\"\ncapacity_bytes = \"64MB\"\ndram_bytes = \"80MB\"\n\n\
         [backend]\nbucket = \"test-lake\"\n\n\
         [cluster]\npod_id = \"drain-itest\"\nnode_id = \"{node_id}\"\n\
         gossip_addr = \"127.0.0.1:{gossip_port}\"\nseeds = [\"127.0.0.1:{seed_port}\"]\n\
         suspicion_secs = 2\n",
        dir.path().display()
    );
    let cfg = dir.path().join("verglas.toml");
    std::fs::File::create(&cfg)
        .expect("create config")
        .write_all(toml.as_bytes())
        .expect("write config");

    let child = Command::new(env!("CARGO_BIN_EXE_verglasd"))
        .arg("--config")
        .arg(&cfg)
        .env("VERGLAS_ADMIN_ADDR", &admin_addr)
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        // The test bucket has no reachable origin; skip the #233 startup probe.
        .env("VERGLAS_DEV_ALLOW_MISSING_ORIGIN", "1")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn verglasd");

    Daemon {
        child,
        node_id: node_id.to_owned(),
        endpoint: format!("http://{admin_addr}"),
        _dir: dir,
    }
}

/// Fetches `GET /admin/members`, `None` until the daemon is up.
fn fetch_members(endpoint: &str) -> Option<MembersInfo> {
    reqwest::blocking::get(format!("{endpoint}/admin/members"))
        .ok()
        .filter(|r| r.status().is_success())
        .and_then(|r| r.json::<MembersInfo>().ok())
}

/// Waits until every endpoint reports exactly `n` members, or the deadline.
fn wait_for_member_count(endpoints: &[&str], n: usize, within: Duration) -> bool {
    let deadline = Instant::now() + within;
    while Instant::now() < deadline {
        if endpoints
            .iter()
            .all(|e| fetch_members(e).is_some_and(|m| m.members.len() == n))
        {
            return true;
        }
        std::thread::sleep(Duration::from_millis(250));
    }
    false
}

/// The full #31 acceptance path: a 3-node pod converges, one node is drained via
/// the resolve-then-drain flow the CLI uses, and after it exits the survivors'
/// ring rebalances to two members.
#[test]
fn draining_a_pod_node_acks_then_rebalances_after_exit() {
    let seed_port = free_port();
    let gb = free_port();
    let gc = free_port();
    let (aa, ab, ac) = (free_port(), free_port(), free_port());

    let a = spawn_node("node-a", seed_port, seed_port, aa);
    let b = spawn_node("node-b", gb, seed_port, ab);
    let c = spawn_node("node-c", gc, seed_port, ac);

    // Converge on all three members first.
    assert!(
        wait_for_member_count(
            &[&a.endpoint, &b.endpoint, &c.endpoint],
            3,
            Duration::from_secs(30)
        ),
        "the pod did not converge on 3 members"
    );

    // Resolve node-c's admin address from node-a's membership — exactly what
    // `verglas node drain node-c` does before targeting the node.
    let members = fetch_members(&a.endpoint).expect("a members");
    let target = members
        .members
        .iter()
        .find(|m| m.node_id == c.node_id)
        .expect("node-c is a member");
    let target_admin = target
        .admin_addr
        .clone()
        .expect("node-c advertises an admin address");

    // Drain node-c with a short donor window so it exits within the test.
    let http = reqwest::blocking::Client::new();
    let ack: DrainAck = http
        .post(format!("http://{target_admin}/admin/drain"))
        .json(&DrainRequest {
            timeout_secs: Some(3),
        })
        .send()
        .expect("drain request")
        .json()
        .expect("drain ack");
    assert_eq!(ack.node_id, "node-c");
    assert_eq!(ack.state, "draining", "the node acks that it is draining");
    assert_eq!(ack.timeout_secs, 3);

    // node-c serves as a donor for its 3s window then exits cleanly; the
    // survivors' failure detector drops it and the ring rebalances to two.
    assert!(
        wait_for_member_count(&[&a.endpoint, &b.endpoint], 2, Duration::from_secs(30)),
        "the pod did not rebalance to 2 members after the drained node exited"
    );

    // The drained node's process is gone (it exited on its own).
    let mut c = c;
    let exited = {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            match c.child.try_wait() {
                Ok(Some(_)) => break true,
                Ok(None) if Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(100));
                }
                _ => break false,
            }
        }
    };
    assert!(exited, "the drained node did not exit on its own");

    // node-c is no longer an owner in the survivors' view (it drained out).
    let view_a = fetch_members(&a.endpoint).expect("a members");
    assert!(
        !view_a.members.iter().any(|m| m.node_id == "node-c"),
        "node-c must be gone from the ring after draining"
    );

    let _ = (a, b);
}
