# Development tasks for the Verglas repository.
#
# One cargo workspace covers everything: the engine and the Lakekeeper-derived
# catalog under `crates/`, plus `bins/`.

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

# Ceph s3-tests conformance: full suite against verglas-cache-node over MinIO.
# Pass extra flags through, e.g. `just s3-tests --smoke` or `just s3-tests --debug`.
s3-tests *ARGS:
    ./tests/s3-conformance/run.sh {{ARGS}}

# `cargo install` compiles in release by default; --force replaces an earlier
# install. Installs the public server into ~/.cargo/bin.
install:
    cargo install --path bins/cache-node --locked --force
