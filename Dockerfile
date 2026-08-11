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
RUN --mount=type=cache,id=verglas-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=verglas-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=verglas-build-target,target=/src/target,sharing=locked \
    cargo build --release \
    -p verglas-server \
    -p verglas-access-bin \
    -p verglas-queue-service \
    -p verglas-scheduler-bin \
    -p verglas-container-runtime \
    -p verglas-cache-node \
    -p verglas-query \
    -p verglas-write-node \
    && mkdir -p /tmp/verglas-build \
    && cp /src/target/release/verglas-server /tmp/verglas-build/ \
    && cp /src/target/release/verglas-access /tmp/verglas-build/ \
    && cp /src/target/release/verglas-queue-service /tmp/verglas-build/ \
    && cp /src/target/release/verglas-scheduler /tmp/verglas-build/ \
    && cp /src/target/release/verglas-container-runtime /tmp/verglas-build/ \
    && cp /src/target/release/verglas-cache-node /tmp/verglas-build/ \
    && cp /src/target/release/verglas-neon-bootstrap /tmp/verglas-build/ \
    && cp /src/target/release/verglas-query /tmp/verglas-build/ \
    && cp /src/target/release/verglas-write /tmp/verglas-build/

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

FROM node:22-bookworm-slim AS verglas-agent-runtime-build
RUN npm install --global pnpm@11.9.0
WORKDIR /build/apps/os
COPY apps/os/package.json apps/os/pnpm-lock.yaml apps/os/pnpm-workspace.yaml ./
COPY apps/os/packages/agent-runtime ./packages/agent-runtime
RUN pnpm --config.inject-workspace-packages=true \
    --filter @verglas/agent-runtime deploy --prod /opt/verglas-agent-runtime

FROM oven/bun:1.3.8 AS verglas-agent-runtime
WORKDIR /opt/verglas-agent-runtime
COPY --from=verglas-agent-runtime-build /opt/verglas-agent-runtime /opt/verglas-agent-runtime
COPY sdks/typescript /opt/verglas-sdk
RUN cd /opt/verglas-sdk \
    && bun install --production --frozen-lockfile \
    && mkdir -p /workspace/node_modules/@verglas \
    && ln -s /opt/verglas-sdk /workspace/node_modules/@verglas/sdk \
    && chown -R bun:bun /workspace
USER bun
EXPOSE 8390
ENTRYPOINT ["bun", "/opt/verglas-agent-runtime/src/server.mjs"]
CMD ["serve"]

FROM node:22-bookworm-slim AS verglas-os
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && npm install --global pnpm@11.9.0
WORKDIR /workspace/apps/os
COPY sdks/typescript /workspace/sdks/typescript
COPY apps/os /workspace/apps/os
RUN pnpm install --frozen-lockfile \
    && pnpm --filter @verglas/typed-storage build \
    && pnpm --filter @verglas/workshop-frontend exec vite build
EXPOSE 8787
CMD ["node", "run-dev-server.js", "--serve-frontend-assets"]

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
COPY --from=build /tmp/verglas-build/verglas-scheduler /usr/local/bin/verglas-scheduler
USER verglas
EXPOSE 8340
ENTRYPOINT ["verglas-scheduler"]

FROM runtime AS verglas-access
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/run/verglas/access /var/run/verglas/server /var/run/verglas/lakekeeper /var/run/verglas/neon \
    && chown -R verglas:verglas /var/run/verglas
COPY --from=build /tmp/verglas-build/verglas-access /usr/local/bin/verglas-access
USER verglas
EXPOSE 8345
ENTRYPOINT ["verglas-access"]

FROM runtime AS verglas-neon-bootstrap
COPY --from=build /tmp/verglas-build/verglas-neon-bootstrap /usr/local/bin/verglas-neon-bootstrap
ENTRYPOINT ["verglas-neon-bootstrap"]

FROM runtime AS verglas-queue-service
COPY --from=build /tmp/verglas-build/verglas-queue-service /usr/local/bin/verglas-queue-service
USER verglas
EXPOSE 8370
ENTRYPOINT ["verglas-queue-service"]

FROM runtime AS verglas-container-runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl python3 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/verglas-container-runtime /var/run/verglas/neon \
    && chown -R verglas:verglas /var/run/verglas
COPY --from=build /tmp/verglas-build/verglas-container-runtime /usr/local/bin/verglas-container-runtime
COPY --from=build /tmp/verglas-build/verglas-scheduler /usr/local/bin/verglas-scheduler
COPY --from=oven/bun:1.3.8 /usr/local/bin/bun /usr/local/bin/bun
COPY crates/verglas-integration-runtime/runtime.mjs /opt/verglas-integration-runtime/runtime.mjs
COPY crates/verglas-integration-runtime/contract.mjs /opt/verglas-integration-runtime/contract.mjs
COPY sdks/typescript/src /opt/verglas-integration-runtime/sdk
EXPOSE 8360
ENTRYPOINT ["verglas-container-runtime"]

FROM runtime AS verglas-cache-node
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*
COPY --from=build /tmp/verglas-build/verglas-cache-node /usr/local/bin/verglas-cache-node
COPY deploy/cache-node/start.sh /usr/local/bin/verglas-cache-node-start
RUN chmod 0755 /usr/local/bin/verglas-cache-node-start
USER verglas
EXPOSE 5454 8333 8334 8335 8336
ENTRYPOINT ["verglas-cache-node-start"]

FROM runtime AS verglas-server
COPY --from=build /tmp/verglas-build/verglas-server /usr/local/bin/verglas-server
COPY --from=build /tmp/verglas-build/verglas-query /usr/local/bin/verglas-query
COPY --from=build /tmp/verglas-build/verglas-write /usr/local/bin/verglas-write
USER verglas
EXPOSE 8333 8334
ENTRYPOINT ["verglas-server", "--environment"]
