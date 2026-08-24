//! Real-process managed-CAS acceptance covering recovery, handoff, and fencing.

mod support;

use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use object_store::ObjectStoreExt;
use support::store_endpoint::StoreEndpoint;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;
use verglas_do_engine::{
    CasCommitAuthority, IsolationLevel, LeaseGrant, LeaseIdentity, TransactionEnvelope,
};

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
    /// Stops a child left behind by a failed acceptance assertion.
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

#[tokio::test]
async fn managed_cas_worker_recovers_checkpoint_tail_handoffs_and_fences_stale_process() {
    let endpoint = StoreEndpoint::start().await.expect("strict S3 endpoint");
    endpoint
        .capability_probe()
        .await
        .expect("conditional object-store capability");
    let descriptor = endpoint.descriptor().clone();
    let store = endpoint.inspector();
    let do_id = "cas-agent";
    let prefix = "managed";
    let old_identity = LeaseIdentity::new("old-process-token", 1);
    let old_authority =
        CasCommitAuthority::acquire(store.clone(), prefix, do_id, old_identity.clone())
            .await
            .expect("initial managed head");
    let old_grant = old_authority.lease_grant().expect("initial grant");
    let first_root = tempfile::tempdir().expect("first cell root");
    let first_control = first_root.path().join("celld.sock");
    let mut first_host = spawn_host(first_root.path(), &first_control);
    wait_for_host(&mut first_host, &first_control).await;
    let first_worker =
        spawn_cas_worker(&first_control, &descriptor, prefix, do_id, 0, &old_grant).await;
    assert_eq!(
        endpoint_request(&first_worker, "STATUS").await,
        "OK worker 0 0 0\n"
    );

    let first = TransactionEnvelope::new(do_id, Uuid::from_u128(101), 0, IsolationLevel::Snapshot);
    assert_eq!(commit(&first_worker, &first).await, "OK 1\n");
    assert_eq!(
        endpoint_request(&first_worker, "CHECKPOINT").await,
        "OK 1\n"
    );
    assert_eq!(
        endpoint_request(&first_worker, "STATUS").await,
        "OK worker 1 1 1\n"
    );

    let second = TransactionEnvelope::new(do_id, Uuid::from_u128(102), 1, IsolationLevel::Snapshot);
    assert_eq!(commit(&first_worker, &second).await, "OK 2\n");
    assert_eq!(
        endpoint_request(&first_worker, "STATUS").await,
        "OK worker 2 2 1\n"
    );

    let head = store
        .head(&object_store::path::Path::from("managed/cas-agent/head"))
        .await
        .expect("current managed head");
    let handoff_grant = LeaseGrant::new(old_identity, 2, head.e_tag.clone(), head.version.clone())
        .expect("old held head grant");
    let successor = CasCommitAuthority::handoff(
        store.clone(),
        prefix,
        do_id,
        handoff_grant,
        LeaseIdentity::new("replacement-process-token", 2),
    )
    .await
    .expect("atomic generation handoff");
    let successor_grant = successor.lease_grant().expect("successor grant");

    let replacement_root = tempfile::tempdir().expect("replacement cell root");
    let replacement_control = replacement_root.path().join("celld.sock");
    let mut replacement_host = spawn_host(replacement_root.path(), &replacement_control);
    wait_for_host(&mut replacement_host, &replacement_control).await;
    let replacement_worker = spawn_cas_worker(
        &replacement_control,
        &descriptor,
        prefix,
        do_id,
        2,
        &successor_grant,
    )
    .await;
    assert_eq!(
        endpoint_request(&replacement_worker, "STATUS").await,
        "OK worker 2 2 1\n"
    );

    let third = TransactionEnvelope::new(do_id, Uuid::from_u128(103), 2, IsolationLevel::Snapshot);
    assert_eq!(commit(&replacement_worker, &third).await, "OK 3\n");
    let stale = TransactionEnvelope::new(do_id, Uuid::from_u128(104), 2, IsolationLevel::Snapshot);
    assert!(commit(&first_worker, &stale).await.starts_with("ERR "));

    stop_host(&mut replacement_host);
    stop_host(&mut first_host);
    endpoint.stop().await;
}

async fn commit(socket: &Path, envelope: &TransactionEnvelope) -> String {
    endpoint_request(
        socket,
        &format!(
            "COMMIT {}",
            hex::encode(envelope.canonical_bytes().expect("canonical envelope"))
        ),
    )
    .await
}

async fn spawn_cas_worker(
    control: &Path,
    descriptor: &support::store_endpoint::StoreDescriptor,
    prefix: &str,
    do_id: &str,
    applied: u64,
    grant: &LeaseGrant,
) -> PathBuf {
    let response = control_request(
        control,
        &format!(
            "SPAWN_CAS_WORKER {do_id} 1 {applied} {} {} {prefix} {} {} {} {} {} {} {} {}",
            descriptor.endpoint,
            descriptor.bucket,
            descriptor.region,
            descriptor.access_key_id,
            descriptor.secret_access_key,
            hex::encode(grant.token()),
            grant.generation(),
            grant.sequence(),
            grant
                .e_tag()
                .map(hex::encode)
                .unwrap_or_else(|| "-".to_owned()),
            grant
                .version()
                .map(hex::encode)
                .unwrap_or_else(|| "-".to_owned()),
        ),
    )
    .await;
    assert!(
        response.starts_with("OK "),
        "CAS worker launch failed: {response}"
    );
    PathBuf::from(response.trim().strip_prefix("OK ").expect("worker socket"))
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
    let pid = host.id().to_string();
    let status = Command::new("kill")
        .arg("-INT")
        .arg(pid)
        .status()
        .expect("signal celld-host");
    assert!(status.success());
    assert!(host.wait().expect("wait celld-host").success());
}

async fn wait_for_host(host: &mut ManagedChild, path: &Path) {
    let deadline = Instant::now() + Duration::from_secs(30);
    while !path.exists() {
        if let Some(status) = host.try_wait().expect("inspect celld-host") {
            let mut stderr = String::new();
            if let Some(mut stream) = host.stderr.take() {
                stream.read_to_string(&mut stderr).expect("host stderr");
            }
            panic!("{} exited with {status}: {stderr}", path.display());
        }
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
