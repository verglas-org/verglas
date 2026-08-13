# Build the exact Linux/DuckDB artifact that the client process loads.  This
# stage is intentionally separate from the measured Python image.
FROM rust:1.96-bookworm AS extension-builder
RUN apt-get update && apt-get install -y --no-install-recommends build-essential cmake git make python3 python3-venv && rm -rf /var/lib/apt/lists/*
WORKDIR /source
COPY extensions/verglas /source/extensions/verglas
RUN rm -rf /source/extensions/verglas/configure /source/extensions/verglas/.venv && \
    test -f /source/extensions/verglas/extension-ci-tools/makefiles/c_api_extensions/base.Makefile && \
    make -C /source/extensions/verglas EXTENSION_VERSION=benchmark-1.5.5 configure release

FROM python:3.12-slim
RUN pip install --no-cache-dir duckdb==1.5.5 pyarrow==19.0.1 numpy==2.2.3 pyiceberg==0.11.1 s3fs==2026.2.0
RUN python -c "import duckdb; connection = duckdb.connect(); connection.execute('INSTALL quack')"
WORKDIR /bench
COPY benchmarks/duckdb-extension/benchmark.py /bench/benchmark.py
COPY benchmarks/duckdb-extension/managed_runner.py /bench/managed_runner.py
COPY --from=extension-builder /source/extensions/verglas/build/release/extension/verglas/verglas.duckdb_extension /artifacts/verglas.duckdb_extension
