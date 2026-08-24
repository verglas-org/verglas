//! Dependency-free process helper used by celld lifecycle acceptance tests.

#[path = "../../tests/support/orchestration_worker.rs"]
mod worker;

/// Runs the shared orchestration and resource-limit test endpoint.
fn main() -> std::io::Result<()> {
    worker::main()
}
