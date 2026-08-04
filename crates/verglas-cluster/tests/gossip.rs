//! Integration tests for chitchat gossip membership (#27), mapped to the
//! issue's acceptance criteria: a multi-node pod converges on one identical
//! member list within seconds, a killed node is detected and dropped within
//! the suspicion window, and the ring follows membership. All nodes run
//! in-process over chitchat's `ChannelTransport`, so no UDP sockets are bound.

use std::net::SocketAddr;
use std::time::Duration;

use verglas_cluster::member::NodeState;
use verglas_cluster::transport::ChannelTransport;
use verglas_cluster::{AgentConfig, ClusterAgent};
use verglas_core::ring::Ring;

/// Builds an agent config for a node addressed at `127.0.0.1:<port>` with the
/// given capacity (its ring weight) and seed ports, tuned for fast test
/// convergence (100 ms gossip, 1 s suspicion window).
fn config(port: u16, capacity: u64, seeds: &[u16]) -> AgentConfig {
    AgentConfig {
        node_id: format!("node-{port}"),
        pod_id: "test-pod".to_owned(),
        generation: 1,
        gossip_addr: format!("127.0.0.1:{port}")
            .parse::<SocketAddr>()
            .expect("gossip addr"),
        advertise_addr: Some(
            format!("127.0.0.1:{}", port + 1000)
                .parse()
                .expect("advertise addr"),
        ),
        admin_addr: Some(
            format!("127.0.0.1:{}", port + 2000)
                .parse()
                .expect("admin addr"),
        ),
        capacity_bytes: capacity,
        seeds: seeds.iter().map(|p| format!("127.0.0.1:{p}")).collect(),
        gossip_interval: Duration::from_millis(100),
        suspicion: Duration::from_secs(1),
    }
}

/// Polls `agent.members()` until it reports exactly `want` members or the
/// deadline lapses, returning whether it converged.
async fn wait_for_member_count(agent: &ClusterAgent, want: usize, deadline: Duration) -> bool {
    let start = std::time::Instant::now();
    while start.elapsed() < deadline {
        if agent.members().len() == want {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    agent.members().len() == want
}

#[tokio::test]
async fn three_nodes_converge_on_identical_membership() {
    let transport = ChannelTransport::default();
    // Node A is the bootstrap seed; B and C seed off A.
    let a = ClusterAgent::spawn(config(7010, 1000, &[7010]), &transport)
        .await
        .expect("spawn a");
    let b = ClusterAgent::spawn(config(7011, 2000, &[7010]), &transport)
        .await
        .expect("spawn b");
    let c = ClusterAgent::spawn(config(7012, 1000, &[7010]), &transport)
        .await
        .expect("spawn c");

    for agent in [&a, &b, &c] {
        assert!(
            wait_for_member_count(agent, 3, Duration::from_secs(10)).await,
            "agent did not converge on 3 members"
        );
    }

    // Every node sees the same three node ids.
    let ids = |m: &[verglas_cluster::member::NodeMeta]| {
        let mut v: Vec<String> = m.iter().map(|n| n.node_id.as_str().to_owned()).collect();
        v.sort();
        v
    };
    let ma = a.members();
    let mb = b.members();
    let mc = c.members();
    assert_eq!(ids(&ma), vec!["node-7010", "node-7011", "node-7012"]);
    assert_eq!(ids(&ma), ids(&mb));
    assert_eq!(ids(&mb), ids(&mc));

    // Published metadata propagates: B advertised 2000 capacity and its peer
    // and admin addresses.
    let b_seen = ma
        .iter()
        .find(|n| n.node_id.as_str() == "node-7011")
        .expect("node-7011 in A's view");
    assert_eq!(b_seen.capacity_bytes, 2000);
    assert_eq!(b_seen.state, NodeState::Active);
    assert_eq!(
        b_seen.advertise_addr,
        Some("127.0.0.1:8011".parse().expect("adv"))
    );
    assert_eq!(
        b_seen.admin_addr,
        Some("127.0.0.1:9011".parse().expect("admin"))
    );

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

#[tokio::test]
async fn ring_reflects_live_membership() {
    let transport = ChannelTransport::default();
    let a = ClusterAgent::spawn(config(7020, 1000, &[7020]), &transport)
        .await
        .expect("spawn a");
    let b = ClusterAgent::spawn(config(7021, 1000, &[7020]), &transport)
        .await
        .expect("spawn b");
    let c = ClusterAgent::spawn(config(7022, 1000, &[7020]), &transport)
        .await
        .expect("spawn c");

    for agent in [&a, &b, &c] {
        assert!(wait_for_member_count(agent, 3, Duration::from_secs(10)).await);
    }

    // The ring each node computes has all three members and agrees on owners.
    let ring_a = a.ring();
    let ring_b = b.ring();
    assert_eq!(ring_a.members().len(), 3);
    for i in 0..500u32 {
        let key = verglas_core::CacheKey {
            bucket: "lake".to_owned(),
            key: format!("obj-{i}"),
        };
        assert_eq!(ring_a.owner(&key), ring_b.owner(&key));
    }

    a.shutdown().await;
    b.shutdown().await;
    c.shutdown().await;
}

/// Draining a node (#31) sheds its ownership without removing it from the pod:
/// once `set_state(Draining)` propagates, peers drop it from the *owning* ring
/// (their `ring.members()` shrinks) but keep it in the membership snapshot as a
/// `Draining` donor — the state a successor warms from while the node exits.
#[tokio::test]
async fn draining_node_sheds_ownership_but_stays_a_member() {
    let transport = ChannelTransport::default();
    let a = ClusterAgent::spawn(config(7040, 1000, &[7040]), &transport)
        .await
        .expect("spawn a");
    let b = ClusterAgent::spawn(config(7041, 1000, &[7040]), &transport)
        .await
        .expect("spawn b");

    for agent in [&a, &b] {
        assert!(wait_for_member_count(agent, 2, Duration::from_secs(10)).await);
    }
    assert_eq!(b.ring().members().len(), 2, "both nodes own a share");

    // Node A announces it is draining.
    a.set_state(NodeState::Draining).await;

    // B drops A from ownership (one owner left) but still sees A as a member.
    let start = std::time::Instant::now();
    let mut shed = false;
    while start.elapsed() < Duration::from_secs(10) {
        if b.ring().members().len() == 1 {
            shed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(shed, "the draining node was not shed from ownership");
    assert_eq!(
        b.ring().members(),
        vec![verglas_core::node::NodeId::new("node-7041")],
        "only the surviving node owns keyspace"
    );
    // A is still a member of the pod, marked draining — a donor, not gone.
    let a_seen = b
        .members()
        .into_iter()
        .find(|n| n.node_id.as_str() == "node-7040")
        .expect("draining node stays in the membership view");
    assert_eq!(a_seen.state, NodeState::Draining);

    // The draining node also stops counting itself an owner.
    assert!(!a.ring().members().contains(&a_seen.node_id));

    a.shutdown().await;
    b.shutdown().await;
}

#[tokio::test]
async fn killed_node_is_detected_and_dropped_from_ring() {
    let transport = ChannelTransport::default();
    let a = ClusterAgent::spawn(config(7030, 1000, &[7030]), &transport)
        .await
        .expect("spawn a");
    let b = ClusterAgent::spawn(config(7031, 1000, &[7030]), &transport)
        .await
        .expect("spawn b");
    let c = ClusterAgent::spawn(config(7032, 1000, &[7030]), &transport)
        .await
        .expect("spawn c");

    for agent in [&a, &b, &c] {
        assert!(wait_for_member_count(agent, 3, Duration::from_secs(10)).await);
    }

    let ring_a = a.ring();
    let epoch_before = ring_a.epoch();

    // Kill C. Survivors must detect it and drop it within the suspicion window
    // (generous ceiling for CI scheduling jitter).
    c.shutdown().await;

    assert!(
        wait_for_member_count(&a, 2, Duration::from_secs(20)).await,
        "surviving node A did not drop the killed node"
    );
    assert!(wait_for_member_count(&b, 2, Duration::from_secs(20)).await);

    // The ring followed membership: two members now, and the epoch advanced.
    assert_eq!(ring_a.members().len(), 2);
    assert!(ring_a.epoch() > epoch_before, "ring epoch must advance");
    let remaining: Vec<String> = a
        .members()
        .iter()
        .map(|n| n.node_id.as_str().to_owned())
        .collect();
    assert!(!remaining.contains(&"node-7032".to_owned()));

    a.shutdown().await;
    b.shutdown().await;
}
