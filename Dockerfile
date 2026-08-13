# Independently deployable Verglas engine roles.

FROM rust:bookworm AS build
WORKDIR /src
COPY rust-toolchain.toml ./
RUN rustup show
COPY . .
RUN --mount=type=cache,id=verglas-cargo-registry,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,id=verglas-cargo-git,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,id=verglas-engine-target,target=/src/target,sharing=locked \
    cargo build --release \
      -p verglas-cache-node \
      -p verglas-query \
      -p verglas-write-node \
    && mkdir -p /tmp/verglas-build \
    && cp /src/target/release/verglas-cache-node /tmp/verglas-build/ \
    && cp /src/target/release/verglas-query /tmp/verglas-build/ \
    && cp /src/target/release/verglas-write /tmp/verglas-build/

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --home /var/lib/verglas --shell /usr/sbin/nologin verglas \
    && mkdir -p /var/lib/verglas /etc/verglas \
    && chown -R verglas:verglas /var/lib/verglas

FROM runtime AS verglas-cache-node
COPY --from=build /tmp/verglas-build/verglas-cache-node /usr/local/bin/verglas-cache-node
COPY deploy/cache-node/start.sh /usr/local/bin/verglas-cache-node-start
RUN chmod 0755 /usr/local/bin/verglas-cache-node-start
USER verglas
EXPOSE 5454 8333 8334 8335 8336
ENTRYPOINT ["verglas-cache-node-start"]

FROM runtime AS verglas-query
COPY --from=build /tmp/verglas-build/verglas-query /usr/local/bin/verglas-query
USER verglas
EXPOSE 8350
ENTRYPOINT ["verglas-query"]

FROM runtime AS verglas-write
COPY --from=build /tmp/verglas-build/verglas-write /usr/local/bin/verglas-write
USER verglas
EXPOSE 8355
ENTRYPOINT ["verglas-write"]
