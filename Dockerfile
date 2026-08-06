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
    -p verglas-gadget-runtime \
    -p verglas-container-runtime \
    -p verglas-query \
    -p verglas-write-node

FROM oven/bun:1.3.8 AS gadget-host
WORKDIR /opt/verglas-gadget-runtime
COPY crates/verglas-gadget-runtime/runtime/package.json \
    crates/verglas-gadget-runtime/runtime/bun.lock ./
RUN bun install --frozen-lockfile --production
COPY crates/verglas-gadget-runtime/runtime/host.mjs ./host.mjs

FROM oven/bun:1.3.8 AS verglas-integration-runtime
WORKDIR /opt/verglas-integration-runtime
COPY crates/verglas-integration-runtime/runtime.mjs ./runtime.mjs
COPY crates/verglas-integration-runtime/contract.mjs ./contract.mjs
COPY sdks/typescript/src ./sdk
USER bun
EXPOSE 8370
ENTRYPOINT ["bun", "/opt/verglas-integration-runtime/runtime.mjs"]

FROM oven/bun:1.3.8 AS verglas-application-runtime
WORKDIR /opt/verglas-application-runtime
COPY crates/verglas-application-runtime/runtime.mjs ./runtime.mjs
COPY crates/verglas-application-runtime/contract.mjs ./contract.mjs
COPY sdks/typescript/src ./sdk
USER bun
EXPOSE 8380
ENTRYPOINT ["bun", "/opt/verglas-application-runtime/runtime.mjs"]

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

FROM runtime AS verglas-gadget-runtime
COPY --from=build /src/target/release/verglas-gadget-runtime /usr/local/bin/verglas-gadget-runtime
COPY --from=gadget-host /usr/local/bin/bun /usr/local/bin/bun
COPY --from=gadget-host /opt/verglas-gadget-runtime /opt/verglas-gadget-runtime
USER verglas
EXPOSE 8350
ENTRYPOINT ["verglas-gadget-runtime"]

FROM runtime AS verglas-container-runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/verglas-container-runtime
COPY --from=build /src/target/release/verglas-container-runtime /usr/local/bin/verglas-container-runtime
COPY --from=build /src/target/release/verglas-scheduler /usr/local/bin/verglas-scheduler
COPY --from=gadget-host /usr/local/bin/bun /usr/local/bin/bun
COPY crates/verglas-integration-runtime/runtime.mjs /opt/verglas-integration-runtime/runtime.mjs
COPY crates/verglas-integration-runtime/contract.mjs /opt/verglas-integration-runtime/contract.mjs
COPY sdks/typescript/src /opt/verglas-integration-runtime/sdk
EXPOSE 8360
ENTRYPOINT ["verglas-container-runtime"]

FROM runtime AS verglas-server
COPY --from=build /src/target/release/verglas-server /usr/local/bin/verglas-server
COPY --from=build /src/target/release/verglas-query /usr/local/bin/verglas-query
COPY --from=build /src/target/release/verglas-write /usr/local/bin/verglas-write
USER verglas
EXPOSE 8333 8334
ENTRYPOINT ["verglas-server", "--environment"]
