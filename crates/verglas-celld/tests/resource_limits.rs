//! Process ceiling configuration tests.

use verglas_celld::WorkerResourceLimits;

#[test]
fn resource_limits_reject_zero_and_preserve_values() {
    assert!(WorkerResourceLimits::new(0, 10).is_err());
    assert!(WorkerResourceLimits::new(10, 0).is_err());
    let limits = WorkerResourceLimits::new(1024, 321).expect("limits");
    assert_eq!(limits.memory_bytes(), 1024);
    assert_eq!(limits.open_files(), 321);
}
