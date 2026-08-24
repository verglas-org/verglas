//! External placement-owner identity tests.

use verglas_celld::HostId;

#[test]
fn host_identity_is_stable_without_local_placement_logic() {
    let host = HostId::new("tenant-cell-a");
    assert_eq!(host.as_str(), "tenant-cell-a");
}
