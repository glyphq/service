# syntax=docker/dockerfile:1.7
#
# Glyph runtime image. Builds the `glyph` operator/indexer binary and the
# `glyph-api` HTTP server from the workspace, then copies only the binaries into
# a small non-root Debian runtime.
#
#   docker build -t glyph:local .
#   docker run --rm -e GLYPH_ROLE=api -p 8080:8080 glyph:local
#
# The image intentionally depends on the workspace Cargo.lock. Rebuild with
# `--no-cache` after changing dependencies if the registry cache looks stale.

# Pinned for reproducible builds. The workspace advertises 1.85, but
# object_store 0.13 uses let-chains, which require Rust 1.88. Do not lower this
# without re-resolving dependencies or patching object_store.
ARG RUST_VERSION=1.88
FROM rust:${RUST_VERSION}-bookworm AS builder

WORKDIR /build

ENV CARGO_INCREMENTAL=0 \
    CARGO_NET_RETRY=5 \
    CARGO_TERM_COLOR=always

# Copy manifests and sources. Tests, fixtures, and docs are excluded by
# .dockerignore; cargo only needs the workspace members that cli/api depend on.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# Cache the registry and target directory between builds. Cargo's target
# directory lives on the cache mount, so copy the two binaries into the image
# layer at the end of the same RUN step.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/build/target \
    cargo build --release --locked -p cli -p api --bins \
 && install -D -m 0755 /build/target/release/glyph /out/glyph \
 && install -D -m 0755 /build/target/release/glyph-api /out/glyph-api

FROM debian:bookworm-slim AS runtime

# ca-certificates is required for outbound TLS (peers and object storage).
# curl is used by the Compose healthcheck for the API. bash runs the entrypoint.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl bash \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 glyph \
 && useradd --system --uid 10001 --gid glyph --no-create-home \
        --home-dir /var/lib/glyph --shell /usr/sbin/nologin glyph

COPY --from=builder /out/glyph /out/glyph-api /usr/local/bin/
COPY --chmod=0755 deploy/docker/glyph-entrypoint.sh /usr/local/bin/glyph-entrypoint

ENV BIND=0.0.0.0:8080 \
    CLICKHOUSE_URL=http://clickhouse:8123 \
    CLICKHOUSE_DB=glyph

USER glyph:glyph
EXPOSE 8080
# The API handles SIGINT and SIGTERM; the indexer currently handles only SIGINT
# (tokio::signal::ctrl_c). Docker stop therefore sends SIGINT so both processes
# shut down gracefully.
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/glyph-entrypoint"]
CMD ["api"]
