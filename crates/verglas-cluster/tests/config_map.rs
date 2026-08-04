//! Unit tests for the pure daemon-facing mapping in the cluster crate:
//! deriving an [`AgentConfig`] from the `[cluster]` config (#27), the node-state
//! wire vocabulary (§3.5), and the spawn error path. These run in-process (the
//! subprocess daemon tests exercise the same code but do not contribute
//! coverage), and pin the defaulting rules the daemon depends on.

use std::time::Duration;

use verglas_cluster::member::NodeState;
use verglas_cluster::{AgentConfig, ClusterError};
use verglas_core::config::Config;

/// Parses a config with a `[cluster]` table appended to a minimal valid doc.
fn config_with_cluster(cluster_body: &str) -> Config {
    let dir = std::env::temp_dir().join(format!("verglas-cluster-cfg-{}", std::process::id()));
    std::fs::create_dir_all(&dir).expect("scratch dir");
    let toml = format!(
        "[cache]\ndir = \"{}\"\ncapacity_bytes = \"32MB\"\n\n[cluster]\n{cluster_body}",
        dir.display()
    );
    Config::from_toml_str(&toml).expect("parses")
}

#[test]
fn from_config_applies_documented_defaults() {
    let config = config_with_cluster("gossip_addr = \"127.0.0.1:7100\"\n");
    let cluster = config.cluster.as_ref().expect("cluster present");
    let agent = AgentConfig::from_config(cluster, &config.cache, 9099).expect("derive");

    // node_id defaults to the gossip address string; pod_id to "default".
    assert_eq!(agent.node_id, "127.0.0.1:7100");
    assert_eq!(agent.pod_id, "default");
    // Weight defaults to the configured NVMe budget — capacity is the weight.
    assert_eq!(agent.capacity_bytes, 32 * 1024 * 1024);
    // The admin address is derived from the daemon's admin port (loopback).
    assert_eq!(
        agent.admin_addr,
        Some("127.0.0.1:9099".parse().expect("addr"))
    );
    assert!(agent.advertise_addr.is_none());
    assert!(agent.seeds.is_empty());
    assert_eq!(agent.suspicion, Duration::from_secs(5));
    assert_eq!(
        agent.gossip_addr,
        "127.0.0.1:7100".parse().expect("gossip addr")
    );
}

#[test]
fn from_config_honours_explicit_fields() {
    let config = config_with_cluster(
        "pod_id = \"az-1a\"\nnode_id = \"node-a\"\ngossip_addr = \"10.0.0.1:7946\"\n\
         advertise_addr = \"10.0.0.1:8333\"\nseeds = [\"10.0.0.2:7946\"]\n\
         weight = 6000\nsuspicion_secs = 3\n",
    );
    let cluster = config.cluster.as_ref().expect("cluster present");
    let agent = AgentConfig::from_config(cluster, &config.cache, 9099).expect("derive");

    assert_eq!(agent.node_id, "node-a");
    assert_eq!(agent.pod_id, "az-1a");
    // The explicit weight overrides the cache budget.
    assert_eq!(agent.capacity_bytes, 6000);
    assert_eq!(
        agent.advertise_addr,
        Some("10.0.0.1:8333".parse().expect("addr"))
    );
    assert_eq!(agent.seeds, vec!["10.0.0.2:7946".to_owned()]);
    assert_eq!(agent.suspicion, Duration::from_secs(3));
}

#[test]
fn from_config_rejects_an_unparseable_gossip_addr() {
    // Parsing does not validate the address (that is `Config::validate`'s job),
    // so `from_config` re-checks it and surfaces a named error.
    let config = config_with_cluster("gossip_addr = \"not-an-addr\"\n");
    let cluster = config.cluster.as_ref().expect("cluster present");
    let err = AgentConfig::from_config(cluster, &config.cache, 9099).expect_err("must fail");
    let ClusterError::Gossip { reason, .. } = err;
    assert!(reason.contains("invalid gossip_addr"), "got: {reason}");
}

#[test]
fn node_state_wire_vocabulary_round_trips() {
    for (state, wire) in [
        (NodeState::Active, "active"),
        (NodeState::Donor, "donor"),
        (NodeState::Takeover, "takeover"),
        (NodeState::Draining, "draining"),
        (NodeState::Dead, "dead"),
    ] {
        assert_eq!(state.as_str(), wire);
        assert_eq!(NodeState::from_wire(Some(wire)), state);
    }
    // Absent or unrecognized state defaults to active — a node that gossips no
    // state is assumed to be serving (never black-holed).
    assert_eq!(NodeState::from_wire(None), NodeState::Active);
    assert_eq!(NodeState::from_wire(Some("bogus")), NodeState::Active);
}
