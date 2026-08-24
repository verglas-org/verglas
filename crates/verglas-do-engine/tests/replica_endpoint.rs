//! Unix-socket endpoint for one supervised `verglas-do` replica process.

use std::sync::Arc;

use object_store::memory::InMemory;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::UnixStream;
use uuid::Uuid;
use verglas_do_engine::{
    CasCommitAuthority, IsolationLevel, LeaseIdentity, ReplicaEndpoint, ReplicaEndpointRole,
    ReplicaSink, SqliteReplicaStore, TransactionEnvelope, UnixReplicaSink,
};

async fn request(endpoint: &mut ReplicaEndpoint, command: &str) -> String {
    let path = endpoint.path().to_path_buf();
    let client = async {
        let mut stream = UnixStream::connect(path).await.expect("connect replica");
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
    };
    let (served, response) = tokio::join!(endpoint.serve_once(), client);
    served.expect("serve request");
    response
}

#[tokio::test]
async fn worker_commit_uses_configured_authority_before_sqlite_visibility() {
    let directory = tempfile::tempdir().expect("replica directory");
    let store = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
            .expect("open pager"),
    );
    let authority = Arc::new(
        CasCommitAuthority::acquire(
            Arc::new(InMemory::new()),
            "tenant-a",
            "agent-1",
            LeaseIdentity::new("held-worker-token", 1),
        )
        .await
        .expect("authority"),
    );
    let mut endpoint = ReplicaEndpoint::bind_worker(
        directory.path().join("worker.sock"),
        "agent-1",
        1,
        store.clone(),
        authority,
    )
    .await
    .expect("worker endpoint");
    let envelope =
        TransactionEnvelope::new("agent-1", Uuid::from_u128(33), 0, IsolationLevel::Snapshot);
    let command = format!(
        "COMMIT {}",
        hex::encode(envelope.canonical_bytes().expect("canonical"))
    );

    assert_eq!(request(&mut endpoint, &command).await, "OK 1\n");
    assert_eq!(request(&mut endpoint, &command).await, "OK 1\n");
    assert_eq!(store.state().expect("state").applied_sequence(), 1);
}

#[tokio::test]
async fn replica_applies_commits_serves_fenced_snapshots_and_rejects_events() {
    let directory = tempfile::tempdir().expect("replica directory");
    let store = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
            .expect("open pager"),
    );
    let socket = directory.path().join("worker.sock");
    let mut endpoint =
        ReplicaEndpoint::bind(&socket, "agent-1", 2, ReplicaEndpointRole::Replica, store)
            .await
            .expect("bind endpoint");

    assert_eq!(request(&mut endpoint, "STATUS").await, "OK replica 0 0 0\n");
    assert!(request(&mut endpoint, "STATEFUL").await.starts_with("ERR "));
    let transaction_id = Uuid::from_u128(44);
    let envelope = TransactionEnvelope::new("agent-1", transaction_id, 0, IsolationLevel::Snapshot);
    let canonical = envelope.canonical_bytes().expect("canonical envelope");
    let unfenced = format!("APPLY 1 {transaction_id} {}", hex::encode(&canonical));
    assert!(request(&mut endpoint, &unfenced).await.starts_with("ERR "));
    let apply = format!(
        "REPLICA_APPLY 7 {} 1 {transaction_id} {}",
        hex::encode("held-token"),
        hex::encode(canonical)
    );
    assert_eq!(request(&mut endpoint, &apply).await, "OK\n");
    assert_eq!(request(&mut endpoint, "SNAPSHOT 1").await, "OK 1\n");
    assert!(
        request(&mut endpoint, "SNAPSHOT 2")
            .await
            .starts_with("ERR ")
    );
    assert_eq!(request(&mut endpoint, "STATUS").await, "OK replica 1 0 0\n");
}

#[tokio::test]
async fn endpoint_rejects_wrong_do_and_conflicting_retry_before_pager_visibility() {
    let directory = tempfile::tempdir().expect("replica directory");
    let store = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
            .expect("open pager"),
    );
    let mut endpoint = ReplicaEndpoint::bind(
        directory.path().join("worker.sock"),
        "agent-1",
        1,
        ReplicaEndpointRole::Worker,
        store,
    )
    .await
    .expect("bind endpoint");
    let wrong =
        TransactionEnvelope::new("agent-2", Uuid::from_u128(9), 0, IsolationLevel::Snapshot);
    let command = format!(
        "APPLY 1 {} {}",
        wrong.transaction_id(),
        hex::encode(wrong.canonical_bytes().expect("canonical"))
    );
    assert!(request(&mut endpoint, &command).await.starts_with("ERR "));
    assert_eq!(request(&mut endpoint, "STATUS").await, "OK worker 0 0 0\n");
    assert_eq!(request(&mut endpoint, "STATEFUL").await, "OK\n");
}

#[tokio::test]
async fn unix_replica_sink_carries_the_lease_fence_to_sqlite() {
    let directory = tempfile::tempdir().expect("replica directory");
    let store = Arc::new(
        SqliteReplicaStore::open(directory.path().join("replica.sqlite"), "agent-1")
            .expect("open pager"),
    );
    let socket = directory.path().join("worker.sock");
    let mut endpoint = ReplicaEndpoint::bind(
        &socket,
        "agent-1",
        5,
        ReplicaEndpointRole::Replica,
        store.clone(),
    )
    .await
    .expect("bind endpoint");
    let sink = UnixReplicaSink::new(socket);
    let transaction_id = Uuid::from_u128(55);
    let envelope = TransactionEnvelope::new("agent-1", transaction_id, 0, IsolationLevel::Snapshot);
    let canonical = envelope.canonical_bytes().expect("canonical");
    let lease = LeaseIdentity::new("opaque replica token", 12);

    let (served, persisted) = tokio::join!(
        endpoint.serve_once(),
        sink.persist(&lease, 1, transaction_id, &canonical,)
    );
    served.expect("serve replica write");
    persisted.expect("persist replica write");
    assert_eq!(store.state().expect("state").applied_sequence(), 1);
    let stale = format!(
        "REPLICA_APPLY 11 {} 2 {} {}",
        hex::encode("old-token"),
        Uuid::from_u128(56),
        hex::encode(
            TransactionEnvelope::new("agent-1", Uuid::from_u128(56), 1, IsolationLevel::Snapshot,)
                .canonical_bytes()
                .expect("stale canonical")
        )
    );
    assert!(request(&mut endpoint, &stale).await.starts_with("ERR "));
    assert_eq!(store.state().expect("state").applied_sequence(), 1);

    let (served, replay) = tokio::join!(endpoint.serve_once(), sink.replay(0, 10));
    served.expect("serve replay");
    let replay = replay.expect("replay tail");
    assert_eq!(replay.len(), 1);
    assert_eq!(replay[0].transaction_id(), transaction_id);
    assert_eq!(replay[0].canonical_envelope(), canonical);
    let stale_lease = LeaseIdentity::new("old-token", 11);
    let (served, stale_clean) = tokio::join!(endpoint.serve_once(), sink.clean(&stale_lease, 1));
    served.expect("serve stale clean");
    assert!(stale_clean.is_err());
    let (served, uncovered_clean) = tokio::join!(endpoint.serve_once(), sink.clean(&lease, 1));
    served.expect("serve uncovered clean");
    assert!(uncovered_clean.is_err());
    assert_eq!(
        store
            .state()
            .expect("state after rejected clean")
            .applied_sequence(),
        1
    );
    store
        .mark_archived(1, "transaction-one")
        .expect("archive transaction");
    store
        .mark_checkpointed(1, "checkpoint-one")
        .expect("checkpoint transaction");
    let (served, cleaned) = tokio::join!(endpoint.serve_once(), sink.clean(&lease, 1));
    served.expect("serve clean");
    cleaned.expect("clean replica tail");
    assert!(
        request(&mut endpoint, "REPLICA_REPLAY 0 10")
            .await
            .starts_with("ERR ")
    );
}
