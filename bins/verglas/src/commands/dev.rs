//! `verglas dev`: the local cluster-of-one, or a local pseudo-cluster
//! (issues #25, #160).
//!
//! One command puts a local Verglas endpoint in front of the one bucket named
//! by `--bucket`: it generates a dev keypair, spins up
//! `verglasd` child processes bound to loopback with temp-dir (or
//! `--cache-dir`) caches, and prints copy-pasteable DuckDB / Polars / aws-CLI
//! snippets. One endpoint serves both reads and writes (write passthrough,
//! #9). The origin backend is required and explicit — `--endpoint`, `--region`,
//! and `--credentials-file` — and the daemon reads its keys from that file,
//! never inline. Ctrl-C tears the daemon(s) down and removes the temp cache(s).
//!
//! `--nodes N` (default 1) boots N daemons as one gossip pod: each node gets
//! its own cache dir and its own `--dram`/`--cache-size` budgets (per node),
//! wired into one pod whose seed is node 0. The dev keys are shared across the
//! pod so a client can talk to any node; the printed engine snippets point at
//! node 0. `--nodes 1` is the single-node path, byte-for-byte unchanged.
//! Node death is two-phase: fail-fast during startup, survive-and-warn after a
//! healthy boot (the pod keeps serving on the survivors; the final exit is
//! non-zero) — so single-node failures can be exercised, not just avoided.

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::Duration;

use tokio::process::{Child, Command};

use crate::cli::DevArgs;

/// The neutral region stamped into the printed snippets. The Verglas endpoint
/// validates SigV4 against whatever region the client signs with, so any value
/// works as long as the client and its snippet agree; a fixed default keeps the
/// copy-paste story simple.
const DEV_REGION: &str = "us-east-1";

/// The gossip cluster id every dev-pod node joins. Fixed so all nodes converge
/// on one pod; distinct from any real deployment's pod id.
const DEV_POD_ID: &str = "verglas-dev";

/// Env var carrying the `verglas dev` parent pid into every spawned `verglasd`,
/// arming that daemon's parent-death watch. The name must match the
/// one `verglasd` reads; it is set to the parent's own pid at spawn so the child
/// can detect the parent vanishing (SIGKILL included) and free its port instead
/// of squatting it as a stale orphan.
const PARENT_PID_ENV: &str = "VERGLAS_PARENT_PID";

/// A shutdown-signal watcher whose OS handlers are installed the moment it is
/// constructed, not when it is first awaited.
///
/// This distinction is the whole point. Registration must happen *before* the
/// parent starts waiting for its children to come up, because a supervisor (or
/// a test) that sees a child answering can deliver SIGTERM immediately — and if
/// the parent has not yet installed its handler, the kernel's default action
/// kills it outright (exit-by-signal 15) instead of running the graceful pod
/// teardown. On Unix the watcher covers SIGINT (Ctrl-C), SIGTERM (`kill` /
/// `pkill` and most supervisors), and SIGHUP (terminal close); all three route
/// through the same reverse-order teardown and ephemeral-cache cleanup so a
/// `verglasd` child never outlives the parent's graceful exit.
#[cfg(unix)]
struct ShutdownSignal {
    interrupt: tokio::signal::unix::Signal,
    terminate: tokio::signal::unix::Signal,
    hangup: tokio::signal::unix::Signal,
}

#[cfg(unix)]
impl ShutdownSignal {
    /// Installs the SIGINT/SIGTERM/SIGHUP handlers now, so a signal delivered
    /// any time after this returns is caught and later drains through `recv`.
    /// If the kernel refuses a handler (never, in practice, for these three)
    /// startup aborts rather than run a `verglas dev` that cannot catch its own
    /// shutdown and would leak the whole pod.
    fn install() -> Self {
        use tokio::signal::unix::{SignalKind, signal};
        /// Installs one handler, or aborts startup if the kernel refuses it —
        /// a `verglas dev` that cannot catch SIGTERM must not run, since it
        /// would leak its whole pod on shutdown.
        fn arm(kind: SignalKind) -> tokio::signal::unix::Signal {
            #[allow(clippy::expect_used)]
            signal(kind).expect("install shutdown signal handler")
        }
        Self {
            interrupt: arm(SignalKind::interrupt()),
            terminate: arm(SignalKind::terminate()),
            hangup: arm(SignalKind::hangup()),
        }
    }

    /// Resolves once the first of SIGINT, SIGTERM, or SIGHUP is delivered.
    async fn recv(&mut self) {
        tokio::select! {
            _ = self.interrupt.recv() => {}
            _ = self.terminate.recv() => {}
            _ = self.hangup.recv() => {}
        }
    }
}

/// Off Unix the only portable operator interrupt is Ctrl-C. Ctrl-C handler
/// registration on those platforms happens on first poll; there is no separate
/// install step to hoist, so the watcher is a thin marker.
#[cfg(not(unix))]
struct ShutdownSignal;

#[cfg(not(unix))]
impl ShutdownSignal {
    /// No eager registration is available off Unix; construction is a no-op.
    fn install() -> Self {
        Self
    }

    /// Resolves once Ctrl-C is delivered.
    async fn recv(&mut self) {
        let _ = tokio::signal::ctrl_c().await;
    }
}

/// Renders the loud startup error for a port already in use: it
/// names the port and its role, blames a possibly-orphaned prior `verglas dev`,
/// and hands the operator the exact `lsof` line to find the daemon squatting it.
/// Pure so it is asserted on directly.
fn render_port_collision(port: u16, role: &str) -> String {
    format!(
        "{role} port {port} is already in use.\n  \
         A previous `verglas dev` may not have shut down cleanly, leaving an \
         orphaned verglasd holding it.\n  \
         Find the process squatting the port with:\n      \
         lsof -i :{port}\n  \
         then stop it (or pick another --port) and retry."
    )
}

/// Verifies every `(port, role)` is bindable on loopback before any daemon is
/// spawned, so a collision surfaces as the loud orphan-hint error
/// naming the port — not as an opaque mid-startup daemon exit. A benign TOCTOU
/// gap remains between this probe and the daemon binding; if lost, the daemon's
/// own bind error still fails startup, just without the hint.
fn ensure_ports_free(ports: &[(u16, String)]) -> Result<(), String> {
    for (port, role) in ports {
        match std::net::TcpListener::bind(("127.0.0.1", *port)) {
            Ok(listener) => drop(listener),
            Err(e) if e.kind() == std::io::ErrorKind::AddrInUse => {
                return Err(render_port_collision(*port, role));
            }
            Err(e) => return Err(format!("cannot probe {role} port {port}: {e}")),
        }
    }
    Ok(())
}

/// Runs `verglas dev`: dispatch to the single-node path (default, unchanged) or
/// the multi-node pod path when `--nodes > 1`. The dev keys are generated once
/// and shared so a client authenticates against any node.
pub async fn run(args: DevArgs) -> Result<(), Box<dyn std::error::Error>> {
    if args.nodes == 0 {
        return Err("--nodes must be at least 1".into());
    }
    let (access_key_id, secret_access_key) = generate_dev_keys();
    if args.nodes == 1 {
        run_single(&args, &access_key_id, &secret_access_key).await
    } else if args.port == 0 {
        // Default: every node binds kernel-assigned ports and reports them, so
        // the pod boots sequentially with no port coordination (issue #194).
        run_pod_ephemeral(&args, &access_key_id, &secret_access_key).await
    } else {
        // Explicit `--port`: fixed consecutive port blocks with the loud
        // collision pre-flight (issue #170).
        run_pod(&args, &access_key_id, &secret_access_key).await
    }
}

/// Runs the single-node cluster-of-one: resolve the cache dir, write a daemon
/// config, spawn `verglasd` on loopback, print the banner, and block until the
/// child exits or Ctrl-C — cleaning up the temp cache on the way out. This path
/// is byte-for-byte the pre- behavior.
async fn run_single(
    args: &DevArgs,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve the cache directory: an explicit --cache-dir is persistent; the
    // default is an owned temp dir removed when this function returns.
    let (cache_dir, _temp_guard): (PathBuf, Option<tempfile::TempDir>) = match &args.cache_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            (dir.clone(), None)
        }
        None => {
            let temp = tempfile::Builder::new().prefix("verglas-dev-").tempdir()?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let cache_dir = cache_dir.canonicalize()?;

    // Ephemeral ports by default (issue #194): the child binds `127.0.0.1:0` for
    // both listeners and reports the kernel-assigned ports back, so nothing races
    // to grab a probed-free port between probe and bind. An explicit `--port`
    // keeps the fixed pair and the loud pre-flight collision check (#170).
    let ephemeral = args.port == 0;
    let (s3_bind_port, admin_bind_port) = if ephemeral {
        (0, 0)
    } else {
        let admin = args
            .port
            .checked_add(1)
            .ok_or("port too high (admin port would overflow)")?;
        (args.port, admin)
    };
    if !ephemeral {
        // A leftover orphan daemon holding an explicitly requested port is the
        // exact failure this names and hints at (#170); the ephemeral default
        // cannot collide, so it needs no pre-flight.
        ensure_ports_free(&[
            (s3_bind_port, "S3 endpoint".to_owned()),
            (admin_bind_port, "admin API".to_owned()),
        ])?;
    }

    // Write the shared dev keypair into an ephemeral creds file in the cache dir
    // and point [auth] at it, so no secret is ever written into the config.
    let auth_creds = cache_dir.join("endpoint-credentials");
    write_endpoint_credentials(&auth_creds, access_key_id, secret_access_key)?;

    // The config's [listen] ports must be valid (non-zero, distinct), but the
    // daemon binds the `VERGLAS_*_ADDR` env addresses below and reports the ports
    // it actually resolved — so for the ephemeral default these are placeholders
    // the daemon never binds.
    let (cfg_s3_port, cfg_admin_port) = if ephemeral {
        (8333, 8334)
    } else {
        (s3_bind_port, admin_bind_port)
    };
    let config_toml = build_config_toml(
        &cache_dir,
        &args.cache_size,
        &args.dram,
        args.admit_probability,
        cfg_s3_port,
        cfg_admin_port,
        &auth_creds,
        &writeback_section(args),
        &catalog_section(args, &cache_dir)?,
        &args.bucket,
        &BackendOrigin::from_args(args),
    );
    // The config names the served bucket but no secrets: the endpoint keypair
    // lives in the creds file [auth] points at, and the backend keys live in the
    // AWS-format file [backend].credentials_file names (from --credentials-file).
    let mut config_file = tempfile::Builder::new()
        .prefix("verglas-dev-")
        .suffix(".toml")
        .tempfile()?;
    use std::io::Write as _;
    config_file.write_all(config_toml.as_bytes())?;
    config_file.flush()?;

    let medium = verglas_core::medium::CacheMedium::detect(&cache_dir);

    // The child reports its resolved listener ports here (issue #194).
    let ports_report = cache_dir.join("verglasd-ports");
    let _ = std::fs::remove_file(&ports_report);

    let verglasd = locate_verglasd()?;
    let mut child = Command::new(&verglasd)
        .arg("--config")
        .arg(config_file.path())
        .arg("--ports-file")
        .arg(&ports_report)
        // Bind loopback explicitly (the config default is 0.0.0.0); dev is a
        // local tool, not a fleet node. Port 0 asks the kernel to assign a free
        // port, reported back through the ports file.
        .env("VERGLAS_S3_ADDR", format!("127.0.0.1:{s3_bind_port}"))
        .env("VERGLAS_ADMIN_ADDR", format!("127.0.0.1:{admin_bind_port}"))
        // Human-readable logs for local dev, regardless of the generated
        // config's `[log] format` (#61).
        .env("VERGLAS_LOG_FORMAT", "pretty")
        // Arm the child's parent-death watch so it exits if we die uncatchably
        // (SIGKILL) instead of squatting this port as a stale orphan (#170).
        .env(PARENT_PID_ENV, std::process::id().to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| format!("failed to spawn verglasd at {}: {e}", verglasd.display()))?;

    // Install the shutdown handler before waiting for the child to answer.
    // A SIGTERM arriving in the ready-wait window must reach our teardown, not
    // the kernel's default terminate (#170).
    let mut shutdown = ShutdownSignal::install();

    // Learn the kernel-assigned ports the child bound before waiting on it.
    let ports = read_reported_ports(&ports_report, &["s3", "admin"], &mut child).await?;
    let s3_addr = ports
        .get("s3")
        .cloned()
        .ok_or("child did not report its S3 port")?;
    let admin_addr = ports
        .get("admin")
        .cloned()
        .ok_or("child did not report its admin port")?;
    let endpoint = format!("http://{s3_addr}");
    wait_until_ready(&admin_addr, &mut child).await?;

    write_resolved_ports_file(args.ports_file.as_deref(), &[(0, &s3_addr, &admin_addr)])?;

    print_banner(&Banner {
        endpoint: &endpoint,
        bucket: &args.bucket,
        cache_dir: &cache_dir,
        ephemeral: args.cache_dir.is_none(),
        medium_note: &medium.note(),
        access_key_id,
        secret_access_key,
    });

    // Block until the daemon exits on its own or the operator interrupts us.
    tokio::select! {
        status = child.wait() => {
            let status = status?;
            if !status.success() {
                return Err(format!("verglasd exited with {status}").into());
            }
        }
        // SIGINT, SIGTERM, and SIGHUP all route through this one teardown path
        // so a child never outlives the parent's graceful exit (#170).
        _ = shutdown.recv() => {
            eprintln!("\nverglas dev: shutting down…");
            let _ = child.start_kill();
            let _ = child.wait().await;
        }
    }
    // `_temp_guard` drops here, removing the temp cache dir when ephemeral.
    Ok(())
}

/// Polls the ports file `verglasd --ports-file` writes until every requested
/// role appears, returning `role -> "ip:port"`. The daemon appends
/// one `role ip:port` line as each listener binds, so a role's presence means
/// its port is final — the parent learns the kernel-assigned ports without ever
/// probing them free first. Fails if the child exits during startup, or if the
/// ports do not appear within the readiness window.
async fn read_reported_ports(
    path: &Path,
    roles: &[&str],
    child: &mut Child,
) -> Result<std::collections::HashMap<String, String>, String> {
    for _ in 0..300 {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("verglasd exited during startup with {status}"));
        }
        if let Ok(text) = std::fs::read_to_string(path) {
            let mut map = std::collections::HashMap::new();
            for line in text.lines() {
                if let Some((role, addr)) = line.split_once(' ') {
                    map.insert(role.to_owned(), addr.trim().to_owned());
                }
            }
            if roles.iter().all(|role| map.contains_key(*role)) {
                return Ok(map);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    Err("verglasd did not report its resolved ports within the startup window".to_owned())
}

/// Writes the resolved per-node endpoints to `verglas dev --ports-file` when the
/// operator asked for one: one `node <i> s3=<addr> admin=<addr>`
/// line per node, so a script or test learns the kernel-assigned ports without
/// scraping the banner. A no-op when no path was given.
fn write_resolved_ports_file(
    path: Option<&Path>,
    nodes: &[(usize, &str, &str)],
) -> std::io::Result<()> {
    let Some(path) = path else {
        return Ok(());
    };
    let mut out = String::new();
    for (index, s3, admin) in nodes {
        out.push_str(&format!("node {index} s3={s3} admin={admin}\n"));
    }
    std::fs::write(path, out)
}

/// Per-node port and cache-dir assignment within a dev pod. Each node owns a
/// contiguous 4-port block off the base port so nodes never collide: S3, admin,
/// gossip, and peer (advertise) ports, in that order.
struct NodeSpec {
    /// Zero-based node index; node 0 is the pod's gossip seed.
    index: usize,
    /// The S3 endpoint port engines point at.
    s3_port: u16,
    /// The loopback admin API port the CLI and tests query.
    admin_port: u16,
    /// The gossip port this node binds and advertises (`[cluster].gossip_addr`).
    gossip_port: u16,
    /// The peer-fetch port advertised for's transport (`advertise_addr`).
    /// The transport that consumes it is's scope; dev advertises it now so
    /// the ring/peer layer has it (extension point).
    peer_port: u16,
    /// This node's own cache directory (a subdir of the pod cache root).
    cache_dir: PathBuf,
}

/// Ports each node reserves off its base: S3, admin, gossip, peer.
const PORTS_PER_NODE: u16 = 4;

/// Lays out `count` nodes' port blocks and cache dirs off `base_port`, rooted at
/// `cache_root`. Node `i` takes ports `base + i*4 .. base + i*4 + 4` and cache
/// dir `cache_root/node-i`. Node 0 keeps the base S3/admin pair so the engine
/// snippets point at the same ports the single-node path uses. Fails if a
/// node's block would run past `u16::MAX` rather than wrapping onto another
/// node's ports.
fn plan_nodes(base_port: u16, count: usize, cache_root: &Path) -> Result<Vec<NodeSpec>, String> {
    let mut specs = Vec::with_capacity(count);
    for index in 0..count {
        let offset = u16::try_from(index)
            .ok()
            .and_then(|i| i.checked_mul(PORTS_PER_NODE))
            .and_then(|o| base_port.checked_add(o))
            .ok_or_else(|| format!("--port {base_port} too high for a {count}-node pod"))?;
        let port = |n: u16| {
            offset
                .checked_add(n)
                .ok_or_else(|| format!("--port {base_port} too high for a {count}-node pod"))
        };
        specs.push(NodeSpec {
            index,
            s3_port: port(0)?,
            admin_port: port(1)?,
            gossip_port: port(2)?,
            peer_port: port(3)?,
            cache_dir: cache_root.join(format!("node-{index}")),
        });
    }
    Ok(specs)
}

/// A spawned pod node: its child handle, admin address for readiness/teardown
/// checks, and the config-file guard kept alive until the daemon has read it.
struct PodChild {
    /// Zero-based node index — the node's name in every operator message.
    index: usize,
    /// The running `verglasd` process.
    child: Child,
    /// This node's `127.0.0.1:admin_port`, polled for readiness.
    admin_addr: String,
    /// Kept alive so the on-disk config outlives the daemon's startup read.
    _config_file: tempfile::NamedTempFile,
}

/// Runs the multi-node pod: plan the nodes, spawn each `verglasd` wired into one
/// gossip pod seeded at node 0, wait for readiness, print the pod banner, then
/// block until Ctrl-C.
///
/// Node death follows a two-phase policy. DURING STARTUP (until every node's
/// `/admin/healthz` has answered once) a dying node kills the pod and returns
/// non-zero naming it — a partially-booted pod is a config error, not a
/// resilience scenario. AFTER a healthy boot a dying node does NOT kill the
/// pod: a loud notice names it and the survivors keep serving (gossip drops it
/// from the ring within the suspicion window) — that degraded-pod shape is
/// exactly what the dev harness exists to exercise. The final exit after
/// Ctrl-C is non-zero if any node died mid-run, so scripts still detect it.
/// Ctrl-C tears every child down in reverse order; ephemeral cache dirs are
/// removed when the temp guard drops.
async fn run_pod(
    args: &DevArgs,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    // Resolve the pod cache root: an explicit --cache-dir is persistent; the
    // default is an owned temp dir removed when this function returns. Every
    // node lives in a subdir of this root.
    let (cache_root, _temp_guard): (PathBuf, Option<tempfile::TempDir>) = match &args.cache_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            (dir.clone(), None)
        }
        None => {
            let temp = tempfile::Builder::new()
                .prefix("verglas-dev-pod-")
                .tempdir()?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let cache_root = cache_root.canonicalize()?;

    let specs = plan_nodes(args.port, args.nodes, &cache_root)?;

    // Fail loudly on any squatted S3/admin port before spawning a single daemon,
    // naming the node whose port a leftover orphan is holding (#170).
    let mut ports = Vec::with_capacity(specs.len() * 2);
    for spec in &specs {
        ports.push((spec.s3_port, format!("node {} S3", spec.index)));
        ports.push((spec.admin_port, format!("node {} admin", spec.index)));
    }
    ensure_ports_free(&ports)?;

    let seed_addr = format!("127.0.0.1:{}", specs[0].gossip_port);
    let verglasd = locate_verglasd()?;
    let writeback = writeback_section(args);
    let origin = BackendOrigin::from_args(args);

    // Install the shutdown handler before any node is spawned. Once a child
    // starts answering its admin port a supervisor may deliver SIGTERM at once;
    // if the parent has not yet armed its handler the kernel's default action
    // kills it (exit-by-signal 15) and the pod is never torn down (#170).
    let mut shutdown = ShutdownSignal::install();

    // Spawn every node. On any failure, tear down whatever booted so a partial
    // pod never leaks daemons.
    let mut running: Vec<PodChild> = Vec::with_capacity(specs.len());
    for spec in &specs {
        match spawn_pod_node(
            &verglasd,
            spec,
            &seed_addr,
            &args.cache_size,
            &args.dram,
            args.admit_probability,
            access_key_id,
            secret_access_key,
            &writeback,
            &args.bucket,
            &origin,
        ) {
            Ok(node) => running.push(node),
            Err(e) => {
                teardown_pod(&mut running).await;
                return Err(e.into());
            }
        }
    }

    // STARTUP phase: wait for each node's admin API to answer once. A node
    // dying here aborts the whole pod, naming which one — fail-fast ends the
    // moment every node has answered healthz.
    for node in &mut running {
        let admin_addr = node.admin_addr.clone();
        if let Err(e) = wait_until_ready(&admin_addr, &mut node.child).await {
            let msg = format!("node {} (verglasd) failed to start: {e}", node.index);
            teardown_pod(&mut running).await;
            return Err(msg.into());
        }
    }

    let node0_endpoint = format!("http://127.0.0.1:{}", specs[0].s3_port);
    let lines: Vec<PodNodeLine> = specs
        .iter()
        .map(|s| PodNodeLine {
            index: s.index,
            s3_endpoint: format!("http://127.0.0.1:{}", s.s3_port),
            admin_endpoint: format!("http://127.0.0.1:{}", s.admin_port),
            gossip_addr: format!("127.0.0.1:{}", s.gossip_port),
            cache_dir: s.cache_dir.clone(),
        })
        .collect();
    print!(
        "{}",
        render_pod_banner(&PodBanner {
            nodes: &lines,
            pod_id: DEV_POD_ID,
            bucket: &args.bucket,
            seed_addr: &seed_addr,
            ephemeral: args.cache_dir.is_none(),
            dram: &args.dram,
            cache_size: &args.cache_size,
            access_key_id,
            secret_access_key,
            node0_endpoint: &node0_endpoint,
        })
    );

    // SERVING phase: block until the operator interrupts us. A node dying now
    // does NOT kill the pod — it gets a loud notice and the survivors keep
    // serving; gossip drops it from the ring within the suspicion window.
    // The handler installed above is polled across loop iterations so a signal
    // arriving between reaps is never missed. SIGINT, SIGTERM, and SIGHUP all
    // reach it, tearing the whole pod down in reverse order (#170).
    let mut dead_nodes: Vec<String> = Vec::new();
    let result = loop {
        reap_dead_nodes(&mut running, &mut dead_nodes);
        if running.is_empty() {
            break Err("every node in the pod has died".to_owned());
        }
        tokio::select! {
            _ = shutdown.recv() => {
                eprintln!("\nverglas dev: shutting down pod…");
                break Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    };

    teardown_pod(&mut running).await;
    // `_temp_guard` drops here, removing the pod cache root when ephemeral.
    match result {
        Err(msg) => Err(msg.into()),
        // A degraded run still exits non-zero after teardown, so scripts
        // detect the mid-run death even though the pod kept serving.
        Ok(()) if !dead_nodes.is_empty() => {
            Err(format!("pod ran degraded: {} died mid-run", dead_nodes.join(", ")).into())
        }
        Ok(()) => Ok(()),
    }
}

/// Runs the multi-node pod with kernel-assigned ports — the default path (issue
///). Boots nodes one at a time: each binds `127.0.0.1:0` for every listener
/// (S3, admin, gossip, peer) and reports the resolved ports, so node 0's real
/// gossip address is known before node 1 starts and becomes the seed for the
/// rest. Nothing is probed for freedom, so two pods can boot concurrently with
/// no port coordination and neither loses a port race. After a healthy boot the
/// death policy matches [`run_pod`]: a node dying mid-run is loud but not fatal,
/// and the final exit is non-zero so scripts still detect the degraded run.
async fn run_pod_ephemeral(
    args: &DevArgs,
    access_key_id: &str,
    secret_access_key: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let (cache_root, _temp_guard): (PathBuf, Option<tempfile::TempDir>) = match &args.cache_dir {
        Some(dir) => {
            std::fs::create_dir_all(dir)?;
            (dir.clone(), None)
        }
        None => {
            let temp = tempfile::Builder::new()
                .prefix("verglas-dev-pod-")
                .tempdir()?;
            (temp.path().to_path_buf(), Some(temp))
        }
    };
    let cache_root = cache_root.canonicalize()?;

    let verglasd = locate_verglasd()?;
    let writeback = writeback_section(args);
    let origin = BackendOrigin::from_args(args);

    // Install the shutdown handler before any node is spawned, so a SIGTERM in
    // the boot window reaches the pod teardown, not the kernel's default (#170).
    let mut shutdown = ShutdownSignal::install();

    let mut running: Vec<PodChild> = Vec::with_capacity(args.nodes);
    let mut lines: Vec<PodNodeLine> = Vec::with_capacity(args.nodes);
    let mut resolved: Vec<(usize, String, String)> = Vec::with_capacity(args.nodes);
    // Node 0's reported gossip address; the seed every later node joins through.
    let mut seed_addr: Option<String> = None;

    for index in 0..args.nodes {
        let node_cache = cache_root.join(format!("node-{index}"));
        let spawned = spawn_pod_node_ephemeral(
            &verglasd,
            index,
            &node_cache,
            seed_addr.as_deref(),
            &args.cache_size,
            &args.dram,
            args.admit_probability,
            access_key_id,
            secret_access_key,
            &writeback,
            &args.bucket,
            &origin,
        );
        let (mut child, config_file, ports_report) = match spawned {
            Ok(parts) => parts,
            Err(e) => {
                teardown_pod(&mut running).await;
                return Err(e.into());
            }
        };

        // Learn this node's resolved ports; gossip is what seeds the next node.
        let ports = match read_reported_ports(&ports_report, &["s3", "admin", "gossip"], &mut child)
            .await
        {
            Ok(ports) => ports,
            Err(e) => {
                let _ = child.start_kill();
                let _ = child.wait().await;
                teardown_pod(&mut running).await;
                return Err(format!("node {index} (verglasd) failed to start: {e}").into());
            }
        };
        let s3_addr = ports.get("s3").cloned().unwrap_or_default();
        let admin_addr = ports.get("admin").cloned().unwrap_or_default();
        let gossip_addr = ports.get("gossip").cloned().unwrap_or_default();
        if index == 0 {
            seed_addr = Some(gossip_addr.clone());
        }

        let mut node = PodChild {
            index,
            child,
            admin_addr: admin_addr.clone(),
            _config_file: config_file,
        };
        // STARTUP phase: a node dying before its healthz answers aborts the pod
        // and names it — a partially-booted pod is a config error (#160).
        if let Err(e) = wait_until_ready(&node.admin_addr, &mut node.child).await {
            let msg = format!("node {index} (verglasd) failed to start: {e}");
            let _ = node.child.start_kill();
            let _ = node.child.wait().await;
            teardown_pod(&mut running).await;
            return Err(msg.into());
        }

        lines.push(PodNodeLine {
            index,
            s3_endpoint: format!("http://{s3_addr}"),
            admin_endpoint: format!("http://{admin_addr}"),
            gossip_addr,
            cache_dir: node_cache,
        });
        resolved.push((index, s3_addr, admin_addr));
        running.push(node);
    }

    let node0_endpoint = lines[0].s3_endpoint.clone();
    let seed = seed_addr.unwrap_or_default();
    print!(
        "{}",
        render_pod_banner(&PodBanner {
            nodes: &lines,
            pod_id: DEV_POD_ID,
            bucket: &args.bucket,
            seed_addr: &seed,
            ephemeral: args.cache_dir.is_none(),
            dram: &args.dram,
            cache_size: &args.cache_size,
            access_key_id,
            secret_access_key,
            node0_endpoint: &node0_endpoint,
        })
    );

    let refs: Vec<(usize, &str, &str)> = resolved
        .iter()
        .map(|(index, s3, admin)| (*index, s3.as_str(), admin.as_str()))
        .collect();
    write_resolved_ports_file(args.ports_file.as_deref(), &refs)?;

    // SERVING phase: identical policy to `run_pod` — a post-boot death is loud
    // but not fatal, the survivors keep serving, and the final exit is non-zero
    // if any node died mid-run.
    let mut dead_nodes: Vec<String> = Vec::new();
    let result = loop {
        reap_dead_nodes(&mut running, &mut dead_nodes);
        if running.is_empty() {
            break Err("every node in the pod has died".to_owned());
        }
        tokio::select! {
            _ = shutdown.recv() => {
                eprintln!("\nverglas dev: shutting down pod…");
                break Ok(());
            }
            _ = tokio::time::sleep(Duration::from_millis(200)) => {}
        }
    };

    teardown_pod(&mut running).await;
    match result {
        Err(msg) => Err(msg.into()),
        Ok(()) if !dead_nodes.is_empty() => {
            Err(format!("pod ran degraded: {} died mid-run", dead_nodes.join(", ")).into())
        }
        Ok(()) => Ok(()),
    }
}

/// Spawns one pod node with kernel-assigned ports: a `:0` bind for
/// every listener plus a `--ports-file` the node reports its resolved ports to.
/// Node 0 (an empty `seed_addr`) starts as the pod founder; later nodes seed off
/// node 0's reported gossip address. Returns the child, its config-file guard
/// (kept alive until the daemon reads it), and the ports file to poll.
#[allow(clippy::too_many_arguments)]
fn spawn_pod_node_ephemeral(
    verglasd: &Path,
    index: usize,
    node_cache: &Path,
    seed_addr: Option<&str>,
    cache_size: &str,
    dram: &str,
    admit_probability: Option<f64>,
    access_key_id: &str,
    secret_access_key: &str,
    writeback: &str,
    bucket: &str,
    origin: &BackendOrigin,
) -> Result<(Child, tempfile::NamedTempFile, PathBuf), String> {
    std::fs::create_dir_all(node_cache)
        .map_err(|e| format!("cannot create cache dir for node {index}: {e}"))?;
    let cache_dir = node_cache
        .canonicalize()
        .map_err(|e| format!("cannot resolve cache dir for node {index}: {e}"))?;

    let auth_creds = cache_dir.join("endpoint-credentials");
    write_endpoint_credentials(&auth_creds, access_key_id, secret_access_key)
        .map_err(|e| format!("cannot write endpoint credentials for node {index}: {e}"))?;

    // Placeholder [listen] ports: config requires them non-zero, but the daemon
    // binds the `:0` env addresses below and reports the ports it resolved.
    let base = build_config_toml(
        &cache_dir,
        cache_size,
        dram,
        admit_probability,
        8333,
        8334,
        &auth_creds,
        writeback,
        // The multi-node dev pod does not proxy a catalog (#287).
        "",
        bucket,
        origin,
    );
    let section = cluster_section(
        DEV_POD_ID,
        &format!("node-{index}"),
        "127.0.0.1:0",
        "127.0.0.1:0",
        seed_addr.unwrap_or(""),
    );
    let config_toml = format!("{base}{section}");

    let mut config_file = tempfile::Builder::new()
        .prefix("verglas-dev-pod-")
        .suffix(".toml")
        .tempfile()
        .map_err(|e| format!("cannot write config for node {index}: {e}"))?;
    use std::io::Write as _;
    config_file
        .write_all(config_toml.as_bytes())
        .and_then(|()| config_file.flush())
        .map_err(|e| format!("cannot write config for node {index}: {e}"))?;

    let ports_report = cache_dir.join("verglasd-ports");
    let _ = std::fs::remove_file(&ports_report);

    let child = Command::new(verglasd)
        .arg("--config")
        .arg(config_file.path())
        .arg("--ports-file")
        .arg(&ports_report)
        .env("VERGLAS_S3_ADDR", "127.0.0.1:0")
        .env("VERGLAS_ADMIN_ADDR", "127.0.0.1:0")
        .env("VERGLAS_LOG_FORMAT", "pretty")
        .env(PARENT_PID_ENV, std::process::id().to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| {
            format!(
                "node {index} failed to spawn verglasd at {}: {e}",
                verglasd.display()
            )
        })?;

    Ok((child, config_file, ports_report))
}

/// Reaps any nodes that died after the healthy boot: each is removed from the
/// running set, gets a loud stderr notice, and is recorded (name + status) for
/// the final non-zero exit. The pod itself keeps running on the survivors.
fn reap_dead_nodes(running: &mut Vec<PodChild>, dead: &mut Vec<String>) {
    let mut i = 0;
    while i < running.len() {
        if let Ok(Some(status)) = running[i].child.try_wait() {
            let node = running.remove(i);
            eprint!(
                "{}",
                render_node_death_notice(node.index, &status.to_string(), running.len())
            );
            dead.push(format!("node {} ({status})", node.index));
        } else {
            i += 1;
        }
    }
}

/// Renders the unmissable notice printed when a node dies after a healthy
/// boot: the node's name and exit status, the surviving count the pod keeps
/// serving on, and the promise that gossip drops the dead node from the ring
/// within the suspicion window. Pure so it is asserted on without capturing
/// stderr.
fn render_node_death_notice(index: usize, status: &str, survivors: usize) -> String {
    format!(
        "\n\
         ================================================================\n\
         verglas dev: node {index} DIED ({status})\n\
         pod continues serving on {survivors} node(s); gossip will drop\n\
         node {index} from the ring within the suspicion window.\n\
         The final exit code will be non-zero.\n\
         ================================================================\n"
    )
}

/// Kills and reaps every pod child in reverse spawn order (node 0 last, since
/// it is the gossip seed), leaving no orphaned daemons behind.
async fn teardown_pod(running: &mut Vec<PodChild>) {
    while let Some(mut node) = running.pop() {
        let _ = node.child.start_kill();
        let _ = node.child.wait().await;
    }
}

/// Writes one node's config (cache dir, per-node budgets, shared dev keys, and
/// the `[cluster]` block seeded at node 0) and spawns its `verglasd` on the
/// node's loopback ports.
// The dev-node parameters (budgets, admission fraction, ports, keys) are all
// independent scalars threaded straight into the child config; bundling them
// into a struct would only rename the same fields at every call site.
#[allow(clippy::too_many_arguments)]
fn spawn_pod_node(
    verglasd: &Path,
    spec: &NodeSpec,
    seed_addr: &str,
    cache_size: &str,
    dram: &str,
    admit_probability: Option<f64>,
    access_key_id: &str,
    secret_access_key: &str,
    writeback: &str,
    bucket: &str,
    origin: &BackendOrigin,
) -> Result<PodChild, String> {
    std::fs::create_dir_all(&spec.cache_dir)
        .map_err(|e| format!("cannot create cache dir for node {}: {e}", spec.index))?;
    let cache_dir = spec
        .cache_dir
        .canonicalize()
        .map_err(|e| format!("cannot resolve cache dir for node {}: {e}", spec.index))?;

    // Write this node's ephemeral endpoint creds file into its cache dir and
    // point [auth] at it, so no secret is written into the node config.
    let auth_creds = cache_dir.join("endpoint-credentials");
    write_endpoint_credentials(&auth_creds, access_key_id, secret_access_key).map_err(|e| {
        format!(
            "cannot write endpoint credentials for node {}: {e}",
            spec.index
        )
    })?;

    let base = build_config_toml(
        &cache_dir,
        cache_size,
        dram,
        admit_probability,
        spec.s3_port,
        spec.admin_port,
        &auth_creds,
        writeback,
        // The multi-node dev pod does not proxy a catalog; the loopback catalog
        // gateway is a single-node dev / real-deployment path (#287).
        "",
        bucket,
        origin,
    );
    let section = cluster_section(
        DEV_POD_ID,
        &format!("node-{}", spec.index),
        &format!("127.0.0.1:{}", spec.gossip_port),
        &format!("127.0.0.1:{}", spec.peer_port),
        seed_addr,
    );
    let config_toml = format!("{base}{section}");

    let mut config_file = tempfile::Builder::new()
        .prefix("verglas-dev-pod-")
        .suffix(".toml")
        .tempfile()
        .map_err(|e| format!("cannot write config for node {}: {e}", spec.index))?;
    use std::io::Write as _;
    config_file
        .write_all(config_toml.as_bytes())
        .and_then(|()| config_file.flush())
        .map_err(|e| format!("cannot write config for node {}: {e}", spec.index))?;

    let s3_addr = format!("127.0.0.1:{}", spec.s3_port);
    let admin_addr = format!("127.0.0.1:{}", spec.admin_port);
    let child = Command::new(verglasd)
        .arg("--config")
        .arg(config_file.path())
        .env("VERGLAS_S3_ADDR", &s3_addr)
        .env("VERGLAS_ADMIN_ADDR", &admin_addr)
        // Human-readable logs for local dev, regardless of the generated
        // config's `[log] format` (#61).
        .env("VERGLAS_LOG_FORMAT", "pretty")
        // Arm this node's parent-death watch: if the pod parent dies uncatchably
        // (SIGKILL) the daemon exits rather than squatting its port (#170).
        .env(PARENT_PID_ENV, std::process::id().to_string())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .spawn()
        .map_err(|e| {
            format!(
                "node {} failed to spawn verglasd at {}: {e}",
                spec.index,
                verglasd.display()
            )
        })?;

    Ok(PodChild {
        index: spec.index,
        child,
        admin_addr,
        _config_file: config_file,
    })
}

/// Renders the `[cluster]` TOML block appended to a pod node's config: the pod
/// id, this node's id and gossip/peer addresses, and a single seed pointing at
/// node 0's gossip address (the pod's join point). Gossip exists now, so
/// there is no static-ring fallback — the seed is a live gossip address.
fn cluster_section(
    pod_id: &str,
    node_id: &str,
    gossip_addr: &str,
    peer_addr: &str,
    seed_addr: &str,
) -> String {
    // Node 0 in the ephemeral pod does not know its own gossip address before it
    // binds (issue #194), so it starts as the pod founder with no seed; every
    // later node seeds off node 0's reported address.
    let seeds = if seed_addr.is_empty() {
        "[]".to_owned()
    } else {
        format!("[\"{seed_addr}\"]")
    };
    format!(
        "\n[cluster]\n\
         pod_id = \"{pod_id}\"\n\
         node_id = \"{node_id}\"\n\
         gossip_addr = \"{gossip_addr}\"\n\
         advertise_addr = \"{peer_addr}\"\n\
         seeds = {seeds}\n"
    )
}

/// One line of the pod banner: a node's endpoints and cache dir.
struct PodNodeLine {
    /// Zero-based node index.
    index: usize,
    /// The node's S3 endpoint URL.
    s3_endpoint: String,
    /// The node's admin API URL.
    admin_endpoint: String,
    /// The node's gossip address.
    gossip_addr: String,
    /// The node's on-disk cache directory.
    cache_dir: PathBuf,
}

/// Fields the pod banner renders. Grouped so the printer stays a pure function
/// of its inputs (golden-testable).
struct PodBanner<'a> {
    /// One entry per node, in index order.
    nodes: &'a [PodNodeLine],
    /// The gossip pod id every node joined.
    pod_id: &'a str,
    /// The one bucket every node in the pod serves.
    bucket: &'a str,
    /// Node 0's gossip address, the pod's shared seed.
    seed_addr: &'a str,
    /// Whether the per-node caches are temp dirs removed on Ctrl-C.
    ephemeral: bool,
    /// The per-node DRAM ceiling (applies to every node).
    dram: &'a str,
    /// The per-node disk ceiling (applies to every node).
    cache_size: &'a str,
    /// The shared dev access key id.
    access_key_id: &'a str,
    /// The shared dev secret access key.
    secret_access_key: &'a str,
    /// Node 0's endpoint — where the engine snippets point.
    node0_endpoint: &'a str,
}

/// Renders the pod startup banner: a line per node (endpoint, admin, gossip,
/// cache), a pod summary (id, seed, shared per-node budgets), the shared dev
/// keys, and the engine snippets pointed at node 0. Pure so it is asserted on
/// without capturing stdout.
fn render_pod_banner(b: &PodBanner<'_>) -> String {
    let cache_kind = if b.ephemeral {
        "temp dirs, removed on Ctrl-C"
    } else {
        "persistent"
    };
    let mut out = String::new();
    out.push_str(&format!(
        "\n\
         \x20 Verglas dev — local pod ({n} nodes)\n\
         \x20 pod:        {pod} (seed {seed})\n\
         \x20 bucket:     {bucket}\n\
         \x20 budgets:    per node — dram={dram}, disk={disk}\n\
         \x20 cache:      one dir per node ({cache_kind})\n",
        n = b.nodes.len(),
        pod = b.pod_id,
        seed = b.seed_addr,
        bucket = b.bucket,
        dram = b.dram,
        disk = b.cache_size,
    ));
    for node in b.nodes {
        out.push_str(&format!(
            "\x20 node {i}:     endpoint {ep}   admin {admin}   gossip {gossip}\n\
             \x20             cache {cache}\n",
            i = node.index,
            ep = node.s3_endpoint,
            admin = node.admin_endpoint,
            gossip = node.gossip_addr,
            cache = node.cache_dir.display(),
        ));
    }
    out.push_str(&format!(
        "\x20 dev keys:   access_key_id={access}\n\
         \x20             secret_access_key={secret}\n\
         \n\
         \x20 Shared dev keys: one client authenticates against any node.\n\
         \x20 Backend origin from --endpoint / --region / --credentials-file.\n\
         \n\
         \x20 Engines point at node 0 (any node serves any key via the ring):\n\
         \n\
         {snippets}\
         \x20 Press Ctrl-C to stop (tears down all {n} nodes).\n\n",
        access = b.access_key_id,
        secret = b.secret_access_key,
        snippets = engine_snippets(
            b.node0_endpoint,
            b.access_key_id,
            b.secret_access_key,
            b.bucket
        ),
        n = b.nodes.len(),
    ));
    out
}

/// Fields the startup banner renders. Grouped so the printer stays a pure
/// function of its inputs (golden-testable).
struct Banner<'a> {
    /// The local endpoint URL engines point at.
    endpoint: &'a str,
    /// The one bucket the daemon serves.
    bucket: &'a str,
    /// The resolved on-disk cache directory.
    cache_dir: &'a Path,
    /// Whether the cache dir is a temp dir removed on shutdown.
    ephemeral: bool,
    /// The cache-medium disclosure line (path, filesystem, IO mode).
    medium_note: &'a str,
    /// The generated dev access key ID.
    access_key_id: &'a str,
    /// The generated dev secret access key.
    secret_access_key: &'a str,
}

/// Prints the human startup banner to stdout.
fn print_banner(b: &Banner<'_>) {
    print!("{}", render_banner(b));
}

/// Renders the human startup banner: what got resolved, the dev keys, and the
/// copy-pasteable engine snippets for reads *and* writes through one endpoint.
/// Pure so it can be asserted on without capturing stdout.
fn render_banner(b: &Banner<'_>) -> String {
    let cache_kind = if b.ephemeral {
        "temp dir, removed on Ctrl-C"
    } else {
        "persistent"
    };
    format!(
        "\n\
         \x20 Verglas dev — local cluster-of-one\n\
         \x20 endpoint:   {endpoint}\n\
         \x20 bucket:     {bucket}\n\
         \x20 cache:      {cache} ({cache_kind})\n\
         \x20 medium:     {medium}\n\
         \x20 dev keys:   access_key_id={access}\n\
         \x20             secret_access_key={secret}\n\
         \n\
         \x20 One endpoint serves both reads and writes (write passthrough).\n\
         \x20 Backend origin from --endpoint / --region / --credentials-file.\n\
         \n\
         {snippets}\
         \x20 Press Ctrl-C to stop.\n\n",
        endpoint = b.endpoint,
        bucket = b.bucket,
        cache = b.cache_dir.display(),
        medium = b.medium_note,
        access = b.access_key_id,
        secret = b.secret_access_key,
        snippets = engine_snippets(b.endpoint, b.access_key_id, b.secret_access_key, b.bucket),
    )
}

/// Generates a dev access keypair for the local endpoint. Not customer
/// credentials: these only authenticate engines to this laptop's daemon.
fn generate_dev_keys() -> (String, String) {
    use std::hash::{BuildHasher, RandomState};
    // RandomState is seeded from OS entropy; good enough for dev keys.
    let r = || RandomState::new().hash_one(0u64);
    (
        format!("VG{:016X}", r()),
        format!("{:016x}{:016x}", r(), r()),
    )
}

/// Builds the `verglasd` config TOML for the dev daemon: the resolved cache dir,
/// the disk (`cache_size`) and DRAM (`dram`) ceilings, the optional
/// resident-biased admission fraction (`admit_probability`, — omitted when
/// `None` so the daemon takes the default), the loopback ports, and the
/// generated static keys. `bucket` names the one bucket the daemon serves,
/// written into `[backend]` (required — the daemon refuses to start without it);
/// backend credentials come from the child's inherited AWS environment, never
/// the config. The `dram` value is passed through verbatim; the engine validates
/// it against its floor at startup, so a value below the floor surfaces as a
/// clear daemon-startup error rather than being clamped here.
// The config inputs are independent scalars rendered straight into TOML;
// bundling them into a struct would only rename the same fields at each caller.
#[allow(clippy::too_many_arguments)]
/// The origin backend the dev pod caches, from the required `--endpoint`/
/// `--region`/`--credentials-file` flags (plus optional `--credentials-profile`/
/// `--allow-http`). Dev serves nothing without a real backend, so these are not
/// optional — there is no ambient-AWS fallback.
struct BackendOrigin {
    endpoint: String,
    region: String,
    credentials_file: String,
    credentials_profile: Option<String>,
    allow_http: bool,
}

impl BackendOrigin {
    /// Collects the origin flags from `DevArgs`.
    fn from_args(args: &crate::cli::DevArgs) -> Self {
        BackendOrigin {
            endpoint: args.endpoint.clone(),
            region: args.region.clone(),
            credentials_file: args.credentials_file.display().to_string(),
            credentials_profile: args.credentials_profile.clone(),
            allow_http: args.allow_http,
        }
    }

    /// The `[backend]` origin lines (endpoint/region/allow_http/credentials).
    fn backend_lines(&self) -> String {
        let mut s = format!(
            "endpoint = \"{}\"\nregion = \"{}\"\n",
            toml_escape(&self.endpoint),
            toml_escape(&self.region),
        );
        if self.allow_http {
            s.push_str("allow_http = true\n");
        }
        s.push_str(&format!(
            "credentials_file = \"{}\"\n",
            toml_escape(&self.credentials_file)
        ));
        if let Some(p) = &self.credentials_profile {
            s.push_str(&format!("credentials_profile = \"{}\"\n", toml_escape(p)));
        }
        s
    }
}

#[allow(clippy::too_many_arguments)]
fn build_config_toml(
    cache_dir: &Path,
    cache_size: &str,
    dram: &str,
    admit_probability: Option<f64>,
    s3_port: u16,
    admin_port: u16,
    auth_credentials_file: &Path,
    writeback: &str,
    catalog: &str,
    bucket: &str,
    origin: &BackendOrigin,
) -> String {
    let dir = toml_escape(&cache_dir.display().to_string());
    let creds = toml_escape(&auth_credentials_file.display().to_string());
    // Accept `s3://name` or a bare `name` for the served bucket, matching
    // `verglas init --bucket`, so the natural `s3://…` form is not served
    // literally (which would answer every real request with NoSuchBucket).
    let bucket = toml_escape(bucket.strip_prefix("s3://").unwrap_or(bucket));
    let backend_origin = origin.backend_lines();
    // Only emit the admission override when `--admit-probability` is set; absent
    // it, the daemon takes the config default (1.0, no resident-biased thinning)
    // and the `[cache.admission]` table is omitted entirely (issue #164).
    let admission = match admit_probability {
        Some(p) => format!("\n[cache.admission]\nchurn_admit_probability = {p}\n"),
        None => String::new(),
    };
    format!(
        "[listen]\n\
         s3_port = {s3_port}\n\
         admin_port = {admin_port}\n\
         \n\
         [cache]\n\
         dir = \"{dir}\"\n\
         capacity_bytes = \"{cache_size}\"\n\
         dram_bytes = \"{dram}\"\n\
         {admission}\
         {writeback}\
         \n\
         [backend]\n\
         bucket = \"{bucket}\"\n\
         {backend_origin}\
         \n\
         [auth]\n\
         credentials_file = \"{creds}\"\n\
         {catalog}"
    )
}

/// Renders the `[catalog]` TOML block for `verglas dev --catalog-uri`, or an
/// empty string when no catalog is proxied. With `--catalog-token`, the
/// token is written to a mode-0600 file in the cache dir and named by
/// `credentials_file`, so no secret is written into the config. Turning on the
/// catalog makes the daemon expose its loopback Iceberg REST gateway, which is
/// what the zero-config `verglas table` / `verglas query` verbs use.
fn catalog_section(args: &DevArgs, cache_dir: &Path) -> std::io::Result<String> {
    let Some(uri) = &args.catalog_uri else {
        return Ok(String::new());
    };
    let mut section = format!("\n[catalog]\nuri = \"{}\"\n", toml_escape(uri));
    if let Some(token) = &args.catalog_token {
        let token_file = cache_dir.join("catalog-token");
        write_secret_file(&token_file, token)?;
        section.push_str(&format!(
            "credentials_file = \"{}\"\n",
            toml_escape(&token_file.display().to_string())
        ));
    }
    Ok(section)
}

/// Writes `contents` to `path` at mode 0600 (owner read/write only). Used for
/// the ephemeral catalog bearer token so no secret lands in any config.toml.
fn write_secret_file(path: &Path, contents: &str) -> std::io::Result<()> {
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents.as_bytes())?;
    }
    Ok(())
}

/// Writes the shared dev keypair into an ephemeral endpoint credentials file at
/// `path` in AWS format, mode 0600. The dev config's `[auth]` names this file so
/// the daemon reads the keypair from disk — no secret is ever written into any
/// config.toml. Lives in the node's cache/temp dir, removed with it on shutdown.
fn write_endpoint_credentials(
    path: &Path,
    access_key_id: &str,
    secret_access_key: &str,
) -> std::io::Result<()> {
    let contents = format!(
        "[default]\naws_access_key_id = {access_key_id}\naws_secret_access_key = {secret_access_key}\n"
    );
    #[cfg(unix)]
    {
        use std::io::Write as _;
        use std::os::unix::fs::OpenOptionsExt as _;
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(path)?;
        file.write_all(contents.as_bytes())?;
    }
    #[cfg(not(unix))]
    {
        std::fs::write(path, contents.as_bytes())?;
    }
    Ok(())
}

/// Renders the `[cache.writeback]` section for `verglas dev --writeback`, or an
/// empty string when the tier is off. Prefixes are omitted, so every key
/// opts in at the chosen geometry.
fn writeback_section(args: &DevArgs) -> String {
    if !args.writeback {
        return String::new();
    }
    format!(
        "\n[cache.writeback]\n\
         enabled = true\n\
         k = {}\n\
         m = {}\n\
         w = {}\n",
        args.writeback_k, args.writeback_m, args.writeback_w
    )
}

/// Escapes a string for a TOML basic (double-quoted) value: backslashes and
/// double quotes only, which covers filesystem paths on the platforms dev runs.
fn toml_escape(s: &str) -> String {
    s.replace('\\', "\\\\").replace('"', "\\\"")
}

/// Renders the copy-pasteable engine config snippets: DuckDB `SET`s, a
/// Polars/PyArrow `storage_options` dict, and an aws-CLI invocation — all
/// pointed at the one endpoint for reads and writes. Object paths use `bucket`,
/// the one bucket the daemon serves.
fn engine_snippets(endpoint: &str, access_key_id: &str, secret: &str, bucket: &str) -> String {
    // DuckDB wants host:port with no scheme; the aws CLI and object_store
    // clients want the full URL.
    let host_port = endpoint.strip_prefix("http://").unwrap_or(endpoint);
    format!(
        "  DuckDB:\n\
         \x20   INSTALL httpfs; LOAD httpfs;\n\
         \x20   SET s3_endpoint='{host_port}';\n\
         \x20   SET s3_url_style='path';\n\
         \x20   SET s3_use_ssl=false;\n\
         \x20   SET s3_region='{DEV_REGION}';\n\
         \x20   SET s3_access_key_id='{access_key_id}';\n\
         \x20   SET s3_secret_access_key='{secret}';\n\
         \x20   -- SELECT count(*) FROM read_parquet('s3://{bucket}/path/to/file.parquet');\n\
         \n\
         \x20 Polars / PyArrow (storage_options):\n\
         \x20   storage_options = {{\n\
         \x20       \"aws_access_key_id\": \"{access_key_id}\",\n\
         \x20       \"aws_secret_access_key\": \"{secret}\",\n\
         \x20       \"aws_region\": \"{DEV_REGION}\",\n\
         \x20       \"aws_endpoint_url\": \"{endpoint}\",\n\
         \x20       \"aws_virtual_hosted_style_request\": \"false\",\n\
         \x20       \"aws_allow_http\": \"true\",\n\
         \x20   }}\n\
         \n\
         \x20 aws CLI (reads and writes):\n\
         \x20   AWS_ACCESS_KEY_ID={access_key_id} \\\n\
         \x20   AWS_SECRET_ACCESS_KEY={secret} \\\n\
         \x20   aws --endpoint-url {endpoint} s3 ls s3://{bucket}/\n\
         \n"
    )
}

/// Locates the `verglasd` binary next to this executable — the same install
/// directory `cargo install` and the release tarball place both binaries in.
fn locate_verglasd() -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| format!("cannot resolve current exe: {e}"))?;
    let dir = exe.parent().ok_or("current exe has no parent directory")?;
    let candidate = dir.join(format!("verglasd{}", std::env::consts::EXE_SUFFIX));
    if candidate.exists() {
        Ok(candidate)
    } else {
        Err(format!(
            "verglasd not found next to verglas at {}",
            candidate.display()
        ))
    }
}

/// Polls the child's admin health endpoint until it answers, failing fast if
/// the daemon dies during startup (a bad bucket, unreachable origin, or a
/// port already in use).
async fn wait_until_ready(admin_addr: &str, child: &mut Child) -> Result<(), String> {
    let url = format!("http://{admin_addr}/admin/healthz");
    for _ in 0..100 {
        if let Ok(Some(status)) = child.try_wait() {
            return Err(format!("verglasd exited during startup with {status}"));
        }
        if let Ok(resp) = reqwest::get(&url).await
            && resp.status().is_success()
        {
            return Ok(());
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    Err("verglasd did not become ready within 10s".to_owned())
}

#[cfg(test)]
mod tests {
    /// A filled origin for build_config_toml tests.
    fn test_origin() -> BackendOrigin {
        BackendOrigin {
            endpoint: "https://origin.example.com".to_owned(),
            region: "us-east-1".to_owned(),
            credentials_file: "/tmp/verglas-cache/backend-credentials".to_owned(),
            credentials_profile: None,
            allow_http: false,
        }
    }
    use super::*;

    #[test]
    fn dev_keys_have_distinct_prefixed_shape() {
        let (id, secret) = generate_dev_keys();
        assert!(id.starts_with("VG"), "id: {id}");
        assert_eq!(id.len(), 18);
        assert_eq!(secret.len(), 32);
        assert_ne!(id, secret);
    }

    #[test]
    fn config_toml_names_the_bucket_and_auth_creds_file_and_carries_no_inline_secret() {
        let toml = build_config_toml(
            Path::new("/tmp/verglas-cache"),
            "20GB",
            "1GB",
            None,
            8333,
            8334,
            Path::new("/tmp/verglas-cache/endpoint-credentials"),
            "",
            "",
            "my-bucket",
            &test_origin(),
        );
        // It is a valid daemon config and round-trips through the loader.
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        assert_eq!(config.listen.s3_port, 8333);
        assert_eq!(config.listen.admin_port, 8334);
        // [auth] names a credentials file; the keypair is never inline.
        let auth = config.auth.expect("auth present");
        assert_eq!(
            auth.credentials_file,
            "/tmp/verglas-cache/endpoint-credentials"
        );
        assert!(
            !toml.contains("access_key_id"),
            "no inline access key: {toml}"
        );
        assert!(
            !toml.contains("secret_access_key"),
            "no inline secret: {toml}"
        );
        // The config names the one bucket to serve in a [backend] section, but
        // embeds no AWS_* credentials; those come from the ambient env chain.
        assert!(
            toml.contains("[backend]"),
            "names a backend section: {toml}"
        );
        assert!(toml.contains("my-bucket"), "names the bucket: {toml}");
        assert!(!toml.contains("AWS_"));
    }

    #[test]
    fn endpoint_credentials_file_is_aws_format_and_readable_by_the_daemon_parser() {
        // The dev keypair is written to a mode-0600 AWS-format file the daemon
        // reads with the same parser the backend uses — proving the file-based
        // wiring, with no secret in any config.toml.
        let dir = tempfile::Builder::new()
            .prefix("verglas-dev-auth-")
            .tempdir()
            .expect("tempdir");
        let path = dir.path().join("endpoint-credentials");
        write_endpoint_credentials(&path, "VGABC", "supersecret").expect("write creds");
        let text = std::fs::read_to_string(&path).expect("read creds");
        let (id, secret) =
            verglas_backend::read_aws_keypair(&text, "default").expect("keypair present");
        assert_eq!(id, "VGABC");
        assert_eq!(secret, "supersecret");
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&path).expect("stat").permissions().mode();
            assert_eq!(mode & 0o777, 0o600, "creds file must be mode 0600");
        }
    }

    #[test]
    fn dram_flag_flows_into_the_daemon_config() {
        // The DRAM ceiling is no longer hardcoded: whatever `verglas dev --dram`
        // resolved lands in the daemon config verbatim (issue #141's
        // nvme-resident profile lowers it to the engine floor).
        let toml = build_config_toml(
            Path::new("/tmp/verglas-cache"),
            "20GB",
            "80MB",
            None,
            8333,
            8334,
            Path::new("/tmp/verglas-cache/endpoint-credentials"),
            "",
            "",
            "my-bucket",
            &test_origin(),
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        assert_eq!(config.cache.dram_bytes.0, 80 * 1024 * 1024);
        // The disk ceiling is independent of the DRAM ceiling.
        assert_eq!(config.cache.capacity_bytes.0, 20 * 1024 * 1024 * 1024);
        // Absent the flag, no admission override is emitted: the daemon takes
        // the default (churn thinning off, p = 1.0).
        assert!(!toml.contains("[cache.admission]"));
        assert_eq!(config.cache.admission.churn_admit_probability, 1.0);
    }

    #[test]
    fn writeback_flag_emits_enabled_tier_config() {
        // `--writeback` emits a [cache.writeback] section the daemon config
        // loader accepts and that enables the tier at the chosen geometry (#180).
        let section = "\n[cache.writeback]\nenabled = true\nk = 2\nm = 1\nw = 3\n";
        let toml = build_config_toml(
            Path::new("/tmp/verglas-cache"),
            "20GB",
            "1GB",
            None,
            8333,
            8334,
            Path::new("/tmp/verglas-cache/endpoint-credentials"),
            section,
            "",
            "my-bucket",
            &test_origin(),
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        assert!(config.cache.writeback.enabled);
        assert_eq!(config.cache.writeback.k, 2);
        assert_eq!(config.cache.writeback.w, 3);
    }

    #[test]
    fn admit_probability_flag_flows_into_the_daemon_config() {
        // `--admit-probability` (issue #164) lands as a `[cache.admission]`
        // override so the disk-constrained TPC-H profile can engage the
        // resident-biased thinning that halves warm-leg backend fills.
        let toml = build_config_toml(
            Path::new("/tmp/verglas-cache"),
            "122MB",
            "80MB",
            Some(0.1),
            8333,
            8334,
            Path::new("/tmp/verglas-cache/endpoint-credentials"),
            "",
            "",
            "my-bucket",
            &test_origin(),
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        assert_eq!(config.cache.admission.churn_admit_probability, 0.1);
        assert!(toml.contains("[cache.admission]"));
    }

    #[test]
    fn catalog_flag_emits_a_catalog_section_that_turns_on_the_loopback_gateway() {
        // `--catalog-uri` (issue #287) makes the dev daemon proxy an Iceberg REST
        // catalog on its loopback gateway, which is what the zero-config
        // `verglas table` / `verglas query` verbs use. The token is written to a
        // mode-0600 file, never inline in the config.
        let dir = tempfile::Builder::new()
            .prefix("verglas-dev-catalog-")
            .tempdir()
            .expect("tempdir");
        let args = DevArgs {
            bucket: "warehouse".to_owned(),
            endpoint: "http://127.0.0.1:9000".to_owned(),
            region: "us-east-1".to_owned(),
            credentials_file: std::path::PathBuf::from("/tmp/creds"),
            credentials_profile: None,
            allow_http: true,
            catalog_uri: Some("http://127.0.0.1:8181".to_owned()),
            catalog_token: Some("s3cr3t-token".to_owned()),
            cache_dir: None,
            cache_size: "20GB".to_owned(),
            dram: "1GB".to_owned(),
            admit_probability: None,
            port: 8333,
            nodes: 1,
            writeback: false,
            writeback_k: 2,
            writeback_m: 1,
            writeback_w: 3,
            ports_file: None,
        };
        let section = catalog_section(&args, dir.path()).expect("catalog section");
        assert!(section.contains("[catalog]"), "section: {section}");
        assert!(
            section.contains("http://127.0.0.1:8181"),
            "section: {section}"
        );
        // The token is in a file, not inline.
        assert!(!section.contains("s3cr3t-token"), "section: {section}");
        assert!(section.contains("credentials_file"), "section: {section}");
        let token_file = dir.path().join("catalog-token");
        assert_eq!(
            std::fs::read_to_string(&token_file).expect("token file"),
            "s3cr3t-token"
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            let mode = std::fs::metadata(&token_file)
                .expect("stat")
                .permissions()
                .mode();
            assert_eq!(mode & 0o777, 0o600, "token file must be mode 0600");
        }

        // The whole config, with the catalog section, round-trips through the
        // daemon config loader and turns Iceberg awareness on.
        let toml = build_config_toml(
            dir.path(),
            "20GB",
            "1GB",
            None,
            8333,
            8334,
            &dir.path().join("endpoint-credentials"),
            "",
            &section,
            "warehouse",
            &test_origin(),
        );
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        let catalog = config.catalog.expect("catalog present");
        assert_eq!(catalog.uri, "http://127.0.0.1:8181");
    }

    #[test]
    fn banner_discloses_the_served_bucket_cache_medium_and_keys() {
        let banner = render_banner(&Banner {
            endpoint: "http://127.0.0.1:8333",
            bucket: "my-bucket",
            cache_dir: Path::new("/tmp/verglas-dev-cache"),
            ephemeral: false,
            medium_note: "/tmp/verglas-dev-cache on apfs — IO mode: buffered",
            access_key_id: "VGDEADBEEF",
            secret_access_key: "cafef00d",
        });
        assert!(banner.contains("cluster-of-one"));
        assert!(banner.contains("http://127.0.0.1:8333"));
        // The banner names the one bucket the daemon serves.
        assert!(banner.contains("my-bucket"));
        assert!(banner.contains("IO mode: buffered"));
        assert!(banner.contains("VGDEADBEEF") && banner.contains("cafef00d"));
        assert!(banner.contains("(persistent)"));
        assert!(banner.contains("reads and writes"));
        // The snippets are embedded.
        assert!(banner.contains("SET s3_endpoint='127.0.0.1:8333';"));
    }

    /// Fixed inputs for the single-node golden banner, so the exact shape of
    /// the `--nodes 1` output is locked (issue: it must stay byte-for-byte
    /// what it was before the pod path existed).
    fn golden_single_banner() -> String {
        render_banner(&Banner {
            endpoint: "http://127.0.0.1:8333",
            bucket: "my-bucket",
            cache_dir: Path::new("/tmp/verglas-dev-cache"),
            ephemeral: true,
            medium_note: "/tmp/verglas-dev-cache on apfs — IO mode: buffered",
            access_key_id: "VGDEADBEEF00000000",
            secret_access_key: "cafef00dcafef00dcafef00dcafef00d",
        })
    }

    #[test]
    fn single_node_banner_matches_the_golden_snapshot() {
        // Any change to this literal is a change to the single-node UX and must
        // be deliberate; --nodes 1 is the unchanged cluster-of-one.
        let expected = "\n  Verglas dev — local cluster-of-one\n  \
endpoint:   http://127.0.0.1:8333\n  \
bucket:     my-bucket\n  \
cache:      /tmp/verglas-dev-cache (temp dir, removed on Ctrl-C)\n  \
medium:     /tmp/verglas-dev-cache on apfs — IO mode: buffered\n  \
dev keys:   access_key_id=VGDEADBEEF00000000\n              \
secret_access_key=cafef00dcafef00dcafef00dcafef00d\n\n  \
One endpoint serves both reads and writes (write passthrough).\n  \
Backend origin from --endpoint / --region / --credentials-file.\n\n  \
DuckDB:\n    INSTALL httpfs; LOAD httpfs;\n    \
SET s3_endpoint='127.0.0.1:8333';\n    \
SET s3_url_style='path';\n    \
SET s3_use_ssl=false;\n    \
SET s3_region='us-east-1';\n    \
SET s3_access_key_id='VGDEADBEEF00000000';\n    \
SET s3_secret_access_key='cafef00dcafef00dcafef00dcafef00d';\n    \
-- SELECT count(*) FROM read_parquet('s3://my-bucket/path/to/file.parquet');\n\n  \
Polars / PyArrow (storage_options):\n    storage_options = {\n        \
\"aws_access_key_id\": \"VGDEADBEEF00000000\",\n        \
\"aws_secret_access_key\": \"cafef00dcafef00dcafef00dcafef00d\",\n        \
\"aws_region\": \"us-east-1\",\n        \
\"aws_endpoint_url\": \"http://127.0.0.1:8333\",\n        \
\"aws_virtual_hosted_style_request\": \"false\",\n        \
\"aws_allow_http\": \"true\",\n    }\n\n  \
aws CLI (reads and writes):\n    \
AWS_ACCESS_KEY_ID=VGDEADBEEF00000000 \\\n    \
AWS_SECRET_ACCESS_KEY=cafef00dcafef00dcafef00dcafef00d \\\n    \
aws --endpoint-url http://127.0.0.1:8333 s3 ls s3://my-bucket/\n\n  \
Press Ctrl-C to stop.\n\n";
        assert_eq!(golden_single_banner(), expected);
    }

    #[test]
    fn banner_marks_an_ephemeral_cache() {
        let banner = render_banner(&Banner {
            endpoint: "http://127.0.0.1:8333",
            bucket: "my-bucket",
            cache_dir: Path::new("/tmp/x"),
            ephemeral: true,
            medium_note: "note",
            access_key_id: "k",
            secret_access_key: "s",
        });
        assert!(banner.contains("removed on Ctrl-C"));
    }

    #[test]
    fn snippets_use_one_endpoint_and_the_served_bucket() {
        let out = engine_snippets("http://127.0.0.1:8333", "VGKEY", "SECRET", "my-bucket");
        // DuckDB gets host:port without scheme; url-style path; ssl off.
        assert!(out.contains("SET s3_endpoint='127.0.0.1:8333';"));
        assert!(out.contains("SET s3_url_style='path';"));
        assert!(out.contains("SET s3_use_ssl=false;"));
        // Polars/PyArrow and aws CLI get the full URL — the same one.
        assert!(out.contains("\"aws_endpoint_url\": \"http://127.0.0.1:8333\""));
        assert!(out.contains("aws --endpoint-url http://127.0.0.1:8333"));
        // The dev keys appear so the paste actually authenticates.
        assert!(out.contains("VGKEY"));
        assert!(out.contains("SECRET"));
        // The one served bucket appears in the object-path examples.
        assert!(out.contains("my-bucket"));
        // No "point writers at the origin" caveat exists — one endpoint.
        assert!(!out.to_lowercase().contains("origin bucket"));
    }

    // --- pod (multi-node) planning, config, and banner (issue #160) -------- //

    #[test]
    fn plan_nodes_lays_out_consecutive_per_node_port_blocks() {
        // Each node owns a 4-port block off the base port: S3, admin, gossip,
        // and peer, in that order. Node 0 keeps the base S3/admin pair so the
        // engine snippets point at the same ports the single-node path uses.
        let root = Path::new("/tmp/verglas-dev-pod");
        let specs = plan_nodes(8333, 3, root).expect("plan");
        assert_eq!(specs.len(), 3);

        assert_eq!(specs[0].s3_port, 8333);
        assert_eq!(specs[0].admin_port, 8334);
        assert_eq!(specs[0].gossip_port, 8335);
        assert_eq!(specs[0].peer_port, 8336);

        assert_eq!(specs[1].s3_port, 8337);
        assert_eq!(specs[1].admin_port, 8338);
        assert_eq!(specs[1].gossip_port, 8339);
        assert_eq!(specs[1].peer_port, 8340);

        assert_eq!(specs[2].s3_port, 8341);
        assert_eq!(specs[2].admin_port, 8342);

        // Every node gets its own cache subdir under the pod root.
        assert_eq!(specs[0].cache_dir, root.join("node-0"));
        assert_eq!(specs[2].cache_dir, root.join("node-2"));

        // All ports across all nodes are distinct.
        let mut ports: Vec<u16> = specs
            .iter()
            .flat_map(|s| [s.s3_port, s.admin_port, s.gossip_port, s.peer_port])
            .collect();
        let count = ports.len();
        ports.sort_unstable();
        ports.dedup();
        assert_eq!(ports.len(), count, "ports must be unique across the pod");
    }

    #[test]
    fn plan_nodes_rejects_a_port_block_that_overflows() {
        // A base port whose per-node block would run past u16 must fail fast,
        // not wrap around onto another node's ports.
        let root = Path::new("/tmp/verglas-dev-pod");
        assert!(plan_nodes(65535, 3, root).is_err());
    }

    #[test]
    fn cluster_section_seeds_every_node_at_node_zero_and_advertises_a_peer_addr() {
        // The pod config renders a `[cluster]` block that parses, names the pod
        // and this node, binds this node's gossip/peer addrs, and points its
        // single seed at node 0's gossip address — the join point for the pod.
        let base = build_config_toml(
            Path::new("/tmp/verglas-cache/node-1"),
            "122MB",
            "80MB",
            None,
            8337,
            8338,
            Path::new("/tmp/verglas-cache/node-1/endpoint-credentials"),
            "",
            "",
            "my-bucket",
            &test_origin(),
        );
        let section = cluster_section(
            "verglas-dev",
            "node-1",
            "127.0.0.1:8339",
            "127.0.0.1:8340",
            "127.0.0.1:8335",
        );
        let toml = format!("{base}{section}");
        let config = verglas_core::config::Config::from_toml_str(&toml).expect("valid config");
        let cluster = config.cluster.expect("cluster present");
        assert_eq!(cluster.pod_id, "verglas-dev");
        assert_eq!(cluster.node_id.as_deref(), Some("node-1"));
        assert_eq!(cluster.gossip_addr, "127.0.0.1:8339");
        assert_eq!(cluster.advertise_addr.as_deref(), Some("127.0.0.1:8340"));
        assert_eq!(cluster.seeds, vec!["127.0.0.1:8335".to_owned()]);
        // Per-node budgets survive alongside the cluster wiring.
        assert_eq!(config.cache.dram_bytes.0, 80 * 1024 * 1024);
        assert_eq!(config.cache.capacity_bytes.0, 122 * 1024 * 1024);
    }

    #[test]
    fn pod_banner_lists_every_node_summarizes_the_pod_and_points_engines_at_node_zero() {
        let nodes = vec![
            PodNodeLine {
                index: 0,
                s3_endpoint: "http://127.0.0.1:8333".to_owned(),
                admin_endpoint: "http://127.0.0.1:8334".to_owned(),
                gossip_addr: "127.0.0.1:8335".to_owned(),
                cache_dir: PathBuf::from("/tmp/pod/node-0"),
            },
            PodNodeLine {
                index: 1,
                s3_endpoint: "http://127.0.0.1:8337".to_owned(),
                admin_endpoint: "http://127.0.0.1:8338".to_owned(),
                gossip_addr: "127.0.0.1:8339".to_owned(),
                cache_dir: PathBuf::from("/tmp/pod/node-1"),
            },
            PodNodeLine {
                index: 2,
                s3_endpoint: "http://127.0.0.1:8341".to_owned(),
                admin_endpoint: "http://127.0.0.1:8342".to_owned(),
                gossip_addr: "127.0.0.1:8343".to_owned(),
                cache_dir: PathBuf::from("/tmp/pod/node-2"),
            },
        ];
        let banner = render_pod_banner(&PodBanner {
            nodes: &nodes,
            pod_id: "verglas-dev",
            bucket: "my-bucket",
            seed_addr: "127.0.0.1:8335",
            ephemeral: true,
            dram: "80MB",
            cache_size: "122MB",
            access_key_id: "VGKEY",
            secret_access_key: "SECRET",
            node0_endpoint: "http://127.0.0.1:8333",
        });

        // The header states this is a pod of three nodes.
        assert!(banner.contains("local pod"));
        assert!(banner.contains("3 nodes"));
        // Every node's endpoint and admin URL appears.
        assert!(banner.contains("http://127.0.0.1:8333"));
        assert!(banner.contains("http://127.0.0.1:8338"));
        assert!(banner.contains("http://127.0.0.1:8341"));
        // The pod summary names the pod, the shared seed, and the served bucket.
        assert!(banner.contains("verglas-dev"));
        assert!(banner.contains("127.0.0.1:8335"));
        assert!(banner.contains("my-bucket"));
        // Per-node budgets are disclosed (they apply per node).
        assert!(banner.contains("80MB") && banner.contains("122MB"));
        // Shared dev keys: one client can talk to any node.
        assert!(banner.contains("VGKEY") && banner.contains("SECRET"));
        // Engines point at node 0 — the snippets carry node 0's endpoint.
        assert!(banner.contains("SET s3_endpoint='127.0.0.1:8333';"));
        // Ephemeral caches are called out and Ctrl-C tears the whole pod down.
        assert!(banner.contains("Ctrl-C"));
        assert!(banner.to_lowercase().contains("node 0"));
    }

    #[test]
    fn node_death_notice_is_loud_names_the_node_and_promises_ring_repair() {
        // After a healthy boot a dying node must NOT kill the pod (#160 design
        // change): the operator gets an unmissable notice naming the node and
        // its exit status, stating the pod keeps serving on the survivors, and
        // that gossip drops the node from the ring within the suspicion window.
        let notice = render_node_death_notice(2, "exit status: 9", 2);
        assert!(notice.contains("node 2"), "names the node: {notice}");
        assert!(
            notice.contains("exit status: 9"),
            "carries status: {notice}"
        );
        assert!(
            notice.contains("2 node"),
            "states the surviving count: {notice}"
        );
        assert!(
            notice.to_lowercase().contains("suspicion window"),
            "promises ring repair: {notice}"
        );
        // Loud: a full-width separator makes it unmissable among daemon logs.
        assert!(notice.contains("======"), "must be visually loud: {notice}");
    }

    #[tokio::test]
    async fn run_rejects_zero_nodes() {
        // `--nodes 0` is meaningless (a pod needs at least one daemon); it must
        // fail fast rather than spawn nothing and hang.
        let args = DevArgs {
            bucket: "my-bucket".to_owned(),
            endpoint: "https://origin.example.com".to_owned(),
            region: "us-east-1".to_owned(),
            credentials_file: std::path::PathBuf::from("/tmp/creds"),
            credentials_profile: None,
            allow_http: false,
            catalog_uri: None,
            catalog_token: None,
            cache_dir: None,
            cache_size: "20GB".to_owned(),
            dram: "1GB".to_owned(),
            admit_probability: None,
            port: 8333,
            nodes: 0,
            writeback: false,
            writeback_k: 2,
            writeback_m: 1,
            writeback_w: 3,
            ports_file: None,
        };
        let err = run(args).await.expect_err("zero nodes must be rejected");
        assert!(err.to_string().contains("--nodes"));
    }

    #[test]
    fn port_collision_error_names_the_port_and_hints_at_orphans() {
        // Issue #170: a port already in use at startup must produce a loud error
        // that names the port and its role, blames a possibly-orphaned prior
        // `verglas dev`, and hands over the exact `lsof` line to find it.
        let msg = render_port_collision(8333, "node 0 S3");
        assert!(msg.contains("8333"), "names the port: {msg}");
        assert!(msg.contains("node 0 S3"), "names the role: {msg}");
        assert!(
            msg.contains("lsof -i :8333"),
            "hands over the lsof line: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("verglas dev"),
            "blames a prior verglas dev: {msg}"
        );
        assert!(
            msg.to_lowercase().contains("orphan"),
            "names the orphan daemon: {msg}"
        );
    }

    #[test]
    fn ensure_ports_free_flags_a_held_port_with_the_loud_message() {
        // A bound port must fail the pre-flight with the orphan-hint message
        // naming that port; a free port passes.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind");
        let held = listener.local_addr().expect("addr").port();
        let err = ensure_ports_free(&[(held, "S3 endpoint".to_owned())])
            .expect_err("a held port must be rejected");
        assert!(err.contains(&held.to_string()), "names the port: {err}");
        assert!(err.contains("lsof -i :"), "gives the lsof hint: {err}");
        drop(listener);
        // Once free, the same port passes the check.
        ensure_ports_free(&[(held, "S3 endpoint".to_owned())]).expect("a free port passes");
    }

    #[test]
    fn pod_banner_marks_persistent_caches() {
        let nodes = vec![PodNodeLine {
            index: 0,
            s3_endpoint: "http://127.0.0.1:8333".to_owned(),
            admin_endpoint: "http://127.0.0.1:8334".to_owned(),
            gossip_addr: "127.0.0.1:8335".to_owned(),
            cache_dir: PathBuf::from("/data/pod/node-0"),
        }];
        let banner = render_pod_banner(&PodBanner {
            nodes: &nodes,
            pod_id: "verglas-dev",
            bucket: "my-bucket",
            seed_addr: "127.0.0.1:8335",
            ephemeral: false,
            dram: "1GB",
            cache_size: "20GB",
            access_key_id: "k",
            secret_access_key: "s",
            node0_endpoint: "http://127.0.0.1:8333",
        });
        assert!(banner.contains("persistent"));
    }
}
