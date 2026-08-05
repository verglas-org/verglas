# Verglas cache server (`verglas-server`) — self-host via Docker.
#
# Build:  docker build -t verglas/verglas-server .
# Run:    see docker-compose.yml

FROM rust:bookworm AS build
WORKDIR /src
# Install the workspace-pinned toolchain before copying sources so layer
# caching survives source edits.
COPY rust-toolchain.toml ./
RUN rustup show
COPY . .
RUN cargo build --release \
    -p verglas-server \
    -p verglas-scheduler-bin \
    -p verglas-query \
    -p verglas-write-node

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/verglas --shell /usr/sbin/nologin verglas \
    && mkdir -p /var/lib/verglas /etc/verglas \
    && chown -R verglas:verglas /var/lib/verglas

FROM runtime AS verglas-scheduler
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/verglas-scheduler /usr/local/bin/verglas-scheduler
USER verglas
EXPOSE 8340
ENTRYPOINT ["verglas-scheduler"]

FROM runtime AS verglas-server
COPY --from=build /src/target/release/verglas-server /usr/local/bin/verglas-server
COPY --from=build /src/target/release/verglas-query /usr/local/bin/verglas-query
COPY --from=build /src/target/release/verglas-write /usr/local/bin/verglas-write
COPY deploy/docker/verglas.toml /etc/verglas/config.toml
USER verglas
EXPOSE 8333 8334
ENTRYPOINT ["verglas-server", "--config", "/etc/verglas/config.toml"]
