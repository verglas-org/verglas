# Development tasks for the Verglas workspace.

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

# Run the cache daemon locally.
run-dev:
    cargo run --bin verglasd

# Ceph s3-tests conformance: full suite against verglasd over MinIO (issue #22).
# Pass extra flags through, e.g. `just s3-tests --smoke` or `just s3-tests --debug`.
s3-tests *ARGS:
    ./tests/s3-conformance/run.sh {{ARGS}}

# `cargo install` compiles in release by default; --force replaces an earlier
# install. Installs into ~/.cargo/bin, which is on PATH.
#
# Build release and install the verglas CLI + verglasd daemon onto your PATH.
install:
    cargo install --path bins/verglas --locked --force
    cargo install --path bins/verglasd --locked --force
