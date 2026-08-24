//! Three-host replica and resource-aware leader placement acceptance tests.

use verglas_celld::{
    DoLoad, HostCapacity, HostId, HostLoad, PlacementError, PlacementPlanner, ReplicaRole,
};

fn host(name: &str, cpu_used: u32, memory_used: u64, leaders: u32, tx_load: u64) -> HostLoad {
    HostLoad::new(
        HostId::new(name),
        HostCapacity::new(8_000, 32 * 1024),
        cpu_used,
        memory_used,
        leaders,
        tx_load,
    )
}

#[test]
fn active_runtime_host_leads_when_it_has_capacity() {
    let hosts = vec![
        host("a", 2_000, 8_000, 1, 100),
        host("b", 2_000, 8_000, 1, 100),
        host("c", 2_000, 8_000, 1, 100),
    ];
    let placement =
        PlacementPlanner::place(&hosts, &DoLoad::new(500, 512, 50), Some(&HostId::new("b")))
            .expect("place three replicas");

    assert_eq!(placement.replicas().len(), 3);
    assert_eq!(placement.leader().host(), &HostId::new("b"));
    assert_eq!(
        placement
            .replicas()
            .iter()
            .filter(|replica| replica.role() == ReplicaRole::Leader)
            .count(),
        1
    );
}

#[test]
fn overloaded_runtime_host_is_not_selected_as_leader() {
    let hosts = vec![
        host("a", 1_000, 4_000, 0, 10),
        host("b", 7_450, 31_000, 0, 0),
        host("c", 4_000, 12_000, 3, 1_000),
    ];
    let placement =
        PlacementPlanner::place(&hosts, &DoLoad::new(500, 512, 100), Some(&HostId::new("b")))
            .expect("place on eligible hosts");

    assert_eq!(placement.leader().host(), &HostId::new("a"));
}

#[test]
fn fewer_than_three_eligible_hosts_fails_closed() {
    let hosts = vec![
        host("a", 1_000, 4_000, 0, 10),
        host("b", 7_900, 31_900, 0, 0),
        host("c", 7_900, 31_900, 0, 0),
    ];
    let result = PlacementPlanner::place(&hosts, &DoLoad::new(500, 512, 100), None);
    assert!(matches!(
        result,
        Err(PlacementError::InsufficientEligibleHosts { eligible: 1 })
    ));
}
