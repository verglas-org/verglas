# Development tasks for the Verglas repository.
#
# One cargo workspace covers the Worker/Durable Object runtime and catalog
# libraries under `crates/`.

# Build everything.
build:
    cargo build --workspace

# Run the test suite.
test:
    cargo test --workspace

# Formatting + clippy, exactly as CI will run them.
lint:
    cargo fmt --all --check
    cargo clippy --workspace --all-targets -- -D warnings
