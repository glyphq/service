# Glyph API

Glyph API is the from-scratch Rust indexer and read API for the Glyph ecosystem. It talks
directly to Qubic peers over TCP, verifies ticks and votes, stores normalized history in
ClickHouse, and serves a resource-oriented HTTP API.

The index starts at the epoch that is current on first run and moves forward. There is no
backfill before that epoch and no compatibility layer for core-bob or the official RPC.

## The no-hosted-RPC promise

**Glyph API never calls a hosted RPC at runtime.** This is a design constraint, not a
configuration default.

- Ingest connects only to configured seed peers and addresses learned through
  `ExchangePublicPeers`. Qubic core peers use TCP `21841`, Bob peers use `21842`.
- Verification uses the computor keys and signatures carried by the protocol. There is no
  "trusted upstream" mode.
- The API reads only the local ClickHouse database. A missing resource is a `404` problem
  document, never a proxy request.
- Falling back to a hosted API is not a supported recovery path. If every peer is down,
  the indexer stops advancing the head and the API keeps serving the indexed history.

This keeps the trust boundary at the set of peers and the computor quorum, which is the
same boundary a Qubic node already has.

## Status

The core loop is implemented and has been exercised against live peers:

- Direct TCP transport, `ExchangePublicPeers` handshake, current head, computors, tick
  data, quorum votes, transactions, log ranges, and log bodies.
- In-repo K12 and SchnorrQ. Live tick data and 451+ vote signatures verify.
- Quorum and data verification tiers. Every stored tick records the tier it reached.
- ClickHouse persistence with idempotent replay, cursor resume, and per-tick rows for
  transactions, typed events, and identity flow.
- Resource API with typed event bodies, offset pagination, problem-details errors,
  OpenAPI, recent activity feeds, and identity transaction history.

Current limits are deliberate:

- Ingestion starts at the current epoch on first run. Earlier history is not fetched.
- Identity `balance` is net QU flow over the indexed range, not a spectrum snapshot.
- The `state` verification tier is reserved; ticks reach `quorum` or `data`.
- The peer pool reconnects across configured seeds. `glyph peers` exposes addresses
  learned through `ExchangePublicPeers`, but the indexer does not merge them into its
  candidate set yet.
- Pagination is offset-based. Asset, epoch, and computor endpoints, batch lookup, SSE,
  and WebSocket streaming are not implemented.

Run `scripts/smoke.sh` to exercise the stack on your host.

## System requirements

| Component | Minimum | Notes |
| --- | --- | --- |
| OS | Linux x86_64/aarch64 or macOS | Dev scripts assume Bash. Windows is supported through WSL. |
| Rust | 1.85 | The exact channel is pinned in `rust-toolchain.toml`. |
| ClickHouse | 26.x | Tested with 26.11. A local binary is used, no Docker required. |
| Memory | 2 GB RAM | 4 GB or more recommended when ClickHouse and the indexer share a host. |
| Disk | 10 GB free | Roughly 10 GB per epoch at current activity, so size for the epochs you retain. |
| Network | Outbound TCP | Qubic core peers on `21841`, Bob peers on `21842`. |
| Local ports | `8080`, `8123`, `9000` | API, ClickHouse HTTP, ClickHouse native. All are configurable. |
| Tools | `cargo`, `bash`, `curl` | `jq` is optional and improves the smoke output. |

An epoch is 604,800 ticks. At current Qubic activity the compressed ClickHouse index
grows by approximately **10 GB per epoch**. Budget about 12 GB per retained epoch for
headroom; actual growth scales with transaction, event, and identity volume.

## Quick start

```bash
# 1. Install a local ClickHouse binary (skip if ~/.local/bin/clickhouse exists).
scripts/install-clickhouse.sh

# 2. Start ClickHouse, apply the schema, build the binaries, start the API and indexer.
scripts/dev-up.sh

# 3. Check the stack: ClickHouse, API status, peer head, and a tick fetch.
scripts/smoke.sh

# 4. Stop everything. Add --wipe to delete local ClickHouse data.
scripts/dev-down.sh
```

Useful variants:

```bash
scripts/dev-up.sh --no-indexer
scripts/dev-up.sh --peer 157.180.10.49:21841 --bind 127.0.0.1:8080
scripts/dev-down.sh --wipe
```

Logs and pid files live under `.data/`:

```text
.data/logs/clickhouse.log
.data/logs/api.log
.data/logs/indexer.log
.data/run/api.pid
.data/run/indexer.pid
.data/clickhouse/
```

### Manual start

```bash
scripts/clickhouse.sh start

cargo run -p cli --bin glyph -- db-init

cargo run -p api --bin glyph-api
# in another shell
cargo run -p cli --bin glyph -- sync --peer 157.180.10.49:21841
```

The API listens on `127.0.0.1:8080` by default.

## Peer configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `GLYPH_PEER` | `157.180.10.49:21841` | Seed peer used by `dev-up.sh` and `smoke.sh` |
| `--peer ADDR` | command default | Peer for `glyph head`, `peers`, `tick`, and `sync` |
| Core port | `21841` | Qubic core peer TCP |
| Bob port | `21842` | Bob peer TCP |

Peer selection rules:

- Start with one or more configured seeds. `NodePool::connect_best` chooses the peer with
  the fastest handshake; the indexer reconnects across the configured seeds on failure.
- `glyph peers` prints the addresses a seed advertises through `ExchangePublicPeers`.
- Never request a tick above a peer's advertised head. The peer rejects it and the
  indexer retries after refreshing the head.
- A peer that returns malformed packets, wrong fixed sizes, or invalid signatures fails
  bundle verification. The indexer does not persist that tick and reconnects.
- Do not put a hosted RPC URL in the peer list. The CLI expects a socket address.

## Workspace map

| Crate | Binary | Responsibility |
| --- | --- | --- |
| [`crates/qubic`](crates/qubic) | none | Wire framing, message payloads, identity base-26, log decoding, K12, SchnorrQ |
| [`crates/node`](crates/node) | none | Direct TCP peer connections, handshake, request/response correlation, peer pool, bundle assembly |
| [`crates/indexer`](crates/indexer) | none | Sync loop, quorum and data verification, checkpointing, batching |
| [`crates/store`](crates/store) | none | ClickHouse schema, row types, batch writer, queries |
| [`crates/api`](crates/api) | `glyph-api` | Axum router, response models, RFC 9457 errors, OpenAPI, offset pagination, search |
| [`crates/cli`](crates/cli) | `glyph` | Operator commands: `head`, `peers`, `tick`, `sync`, `db-init`, `serve` |

The dependency direction is acyclic: `cli` depends on `api`, `indexer`, `node`, `store`,
and `qubic`. `api` depends on `store` and `qubic`. `indexer` depends on `node`, `store`,
and `qubic`. `node` and `store` depend on `qubic`. `qubic` has no I/O.

## Commands

```bash
# Current head from a peer
cargo run -p cli --bin glyph -- head --peer 157.180.10.49:21841

# Peers discovered through ExchangePublicPeers
cargo run -p cli --bin glyph -- peers --peer 157.180.10.49:21841

# Fetch and summarize one tick bundle
cargo run -p cli --bin glyph -- tick 91915000 --peer 157.180.10.49:21841

# Run the indexer from the current epoch initial tick
cargo run -p cli --bin glyph -- sync --peer 157.180.10.49:21841

# Apply ClickHouse schema
cargo run -p cli --bin glyph -- db-init

# Serve the API (equivalently: cargo run -p api --bin glyph-api)
cargo run -p cli --bin glyph -- serve
```

## Environment

| Variable | Default | Used by |
| --- | --- | --- |
| `BIND` | `127.0.0.1:8080` | API listen address |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | API, CLI, store |
| `CLICKHOUSE_DB` | `glyph` | API, CLI, store |
| `CLICKHOUSE_TEST_URL` | `http://127.0.0.1:8123` | Store integration tests |
| `RUST_LOG` | `info,tower_http=info` for API, `info` for CLI | Tracing filter |
| `GLYPH_PEER` | `157.180.10.49:21841` | Dev and smoke scripts |
| `GLYPH_BIN` | `target/debug/glyph` | Smoke script |
| `API_URL` | `http://127.0.0.1:8080` | Smoke script |
| `CH_BIN` | `~/.local/bin/clickhouse` | ClickHouse helper |
| `CH_DATA_DIR` | `.data/clickhouse` | ClickHouse helper |

## API

The v1 contract is in [docs/API.md](docs/API.md). Implemented today:

```bash
curl -sS localhost:8080/healthz
curl -sS localhost:8080/v1/status
curl -sS localhost:8080/v1/ticks/91915000
curl -sS 'localhost:8080/v1/ticks/91915000/transactions?limit=25'
curl -sS 'localhost:8080/v1/ticks/91915000/events?limit=25'
curl -sS 'localhost:8080/v1/transactions?limit=25'
curl -sS 'localhost:8080/v1/events?limit=25'
curl -sS localhost:8080/v1/transactions/<hash>
curl -sS localhost:8080/v1/identities/<identity>
curl -sS 'localhost:8080/v1/identities/<identity>/transactions?limit=25'
curl -sS 'localhost:8080/v1/search?q=91915000'
curl -sS localhost:8080/openapi.json
```

Asset, epoch, and computor endpoints, batch lookup, SSE, and WebSocket are not
implemented. The list handlers use `limit` and `offset`.

Response conventions:

- `snake_case` JSON fields.
- Amounts and balances as JSON integers. Use a 64-bit-safe parser.
- RFC 3339 UTC timestamps with milliseconds.
- Canonical identities in uppercase base-26 and transaction hashes in lowercase base-26.
- RFC 9457 problem documents with a stable `code`.
- Offset pagination with `limit` (default 50, maximum 200) and `offset`.
- Event `body` is a JSON object; `type_name` is a typed event name.
- Tick and transaction reads are immutable-cacheable.

## Documentation

| Document | Contents |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Goals, non-goals, direct-peer rationale, options, split ingest and API, diagrams, verification tiers, epoch handling, failure modes, capacity, operations |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | Framing, message types, struct offsets, identity encoding, ingestion sequence, quorum rules, logs, end-epoch virtual tick, gotchas |
| [docs/API.md](docs/API.md) | Implemented endpoints, offset pagination, RFC 9457 errors, caching, search, examples |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | Entities, invariants, ClickHouse keys and projections, capacity math, retention, replay |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Phased milestones with exit criteria |

## Development

```bash
# Format, lint, test
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets

# Scripts must parse
for script in scripts/*.sh; do bash -n "$script"; done
```

CI runs format, clippy, and tests without services. ClickHouse integration tests skip when
`CLICKHOUSE_TEST_URL` is unreachable, so a clean runner does not need a database.

### Scripts

| Script | Purpose |
| --- | --- |
| [`scripts/install-clickhouse.sh`](scripts/install-clickhouse.sh) | Download a ClickHouse binary to `CH_BIN` |
| [`scripts/clickhouse.sh`](scripts/clickhouse.sh) | `start`, `stop`, `status`, `run`, `client`, `wipe` |
| [`scripts/dev-up.sh`](scripts/dev-up.sh) | Start ClickHouse, apply schema, build and start API and indexer |
| [`scripts/dev-down.sh`](scripts/dev-down.sh) | Stop API, indexer, and ClickHouse. `--wipe` deletes data |
| [`scripts/smoke.sh`](scripts/smoke.sh) | Check ClickHouse, API status, peer head, and a tick fetch |

`deploy/clickhouse/config.xml` and `deploy/clickhouse/users.xml` define the local
ClickHouse instance. The data directory is `.data/clickhouse`, the HTTP port is `8123`,
and the native port is `9000`. The configuration is development-only and binds to
`127.0.0.1`.

## Data and retention

- ClickHouse is the only store today. Epoch partitions make retention cheap.
- A raw archive on local disk or S3 is planned as the cold store and replay source. It is
  not implemented.
- Log IDs are epoch-scoped. The canonical event identifier is `{epoch}:{logId}`.
- Epochs below 240 are not indexed. Tick lookups outside the indexed range return
  `404 not_found`.
- Amounts fit in `i64` and are returned as JSON numbers. Use a 64-bit-safe parser.

See [docs/DATA_MODEL.md](docs/DATA_MODEL.md) for capacity math and replay procedures.

## License

MIT OR Apache-2.0. See the workspace manifest.
