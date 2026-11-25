# Glyph API

Glyph API is a self-hosted indexer and read API for Qubic. It connects to Qubic peers
over TCP, verifies what they return, stores normalized history in ClickHouse, and serves
a resource-oriented HTTP API. There is no hosted RPC dependency and no compatibility
layer for core-bob or other Qubic API services.

The index starts at the current epoch on first run and moves forward. It can catch up
within that epoch, follow the live network, and serve reads while ingestion continues.

## Why this exists

Most Qubic tooling treats a node or an RPC service as a black box. Glyph treats the
protocol as the source of truth:

- **Direct peers.** Tick data, votes, transactions, computors, logs, entities, and
  assets are requested from Qubic peers over the native TCP protocol.
- **Verification at ingest.** Quorum vote groups are checked before a tick is written.
  K12 and SchnorrQ are implemented in this repository, so verification does not depend
  on an external RPC or a pre-verified database.
- **Normalized history.** Transactions, typed events, identity activity, assets, and
  prices live in ClickHouse with stable keys and projections for the read paths.
- **Reads that match the data.** Cursor pagination, RFC 9457 errors, immutable point
  reads, live streams, and an OpenAPI 3.1 description are part of the service.

## What it provides

- Live indexing from direct Qubic peers, with bounded concurrent fetch and ordered
  writes.
- Quorum and data verification tiers. Every stored tick records the tier it reached.
- Observed epoch boundaries persisted in `glyph.epochs`, with optional partition
  retention via `PRUNE_KEEP_EPOCHS` or `glyph prune`.
- Raw tick archive as zstd-compressed QBB1 bundles on local disk or any S3-compatible
  store. Vote headers are opt-in, and the computor set is stored once per epoch.
- Internal transaction tracking with retries, expiry, and signed webhook delivery.
- REST resources for ticks, transactions, events, identities, wallets, assets, epochs,
  the on-chain QUBIC/USDT price, analytics, search, and network status.
- Explorer-shaped reads that answer a whole page in one request: transaction effects,
  identity counterparties, bucketed QU flow, and asset transfer history.
- External QUBIC/USDT market consensus from MEXC and Gate.io, with CoinGecko
  reference data, cached in memory and exposed beside the verified on-chain series.
- SSE and WebSocket streams that follow the verified local head, with per-identity
  and per-asset filters.
- Optional direct-peer routes for entity lookup, transaction status, contract queries,
  and signed transaction broadcast.
- A server-rendered status dashboard at `/status` and a branded Scalar API reference
  at `/docs`, with local logo and favicon assets.
- Docker Compose deployment with ClickHouse, SeaweedFS S3, schema migration, API, and
  indexer, plus a no-Compose runner for Container-Optimized hosts and local scripts for
  development without Docker.

## Quick start

Requirements: Rust 1.85 or newer, Bash, and `curl`. ClickHouse is installed by the
first script if it is missing.

```bash
# Start ClickHouse, apply the schema, build, and start the API and indexer.
scripts/dev-up.sh

# Check ClickHouse, API status, peer head, and a sample tick.
scripts/smoke.sh

# Open the API reference and status page.
open http://127.0.0.1:8080/docs
open http://127.0.0.1:8080/status

# Stop everything. Add --wipe to delete local ClickHouse data.
scripts/dev-down.sh
```

Useful variants:

```bash
scripts/dev-up.sh --no-indexer
scripts/dev-up.sh --peer peer.example:21841 --bind 127.0.0.1:8080
scripts/dev-down.sh --wipe
```

Logs and runtime files are written under `.data/`:

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

# In another shell:
cargo run -p cli --bin glyph -- sync --peer peer.example:21841
```

The API listens on `127.0.0.1:8080` by default.

## System requirements

| Component | Minimum | Recommended | Notes |
| --- | --- | --- | --- |
| OS | Linux x86_64/aarch64 or macOS | Linux x86_64 | Windows works through WSL. Dev scripts assume Bash. |
| Rust | 1.85 | Current stable | The Docker image pins 1.88. |
| ClickHouse | 25.8 LTS or newer | Current stable | Local binary or Compose service. |
| Memory | 2 GB | 16 GB | 8 GB is workable for the full stack. |
| CPU | 2 vCPU | 4 vCPU | Live ingestion is I/O-bound. Signature verification and ClickHouse merges use the rest. |
| Disk | 20 GB free | Sized per retained epoch | About 16 GB of indexed data per epoch. Budget 20 GB per epoch. |
| Network | Outbound TCP | Low-latency route to peers | Qubic core peers use port `21841`; Bob peers use `21842`. |
| Local ports | `8080`, `8123`, `8333` | Same | API, ClickHouse HTTP, S3-compatible API. All are configurable. |

Epoch length is observed, not fixed. The current network runs roughly 330k ticks per
epoch, and Glyph records the boundary in `glyph.epochs` rather than assuming 604,800.
At current activity, a sizing run of 1,000 ticks measured about 26 MB with the schema
and its projections, or roughly **16 GB per epoch**. Raw archiving is much larger:
expect about **213 GiB per epoch uncompressed** before zstd compression. Size archive
storage separately and enable it only when replay matters.

## How ingestion works

The indexer separates fetching from writing:

1. A coordinator polls the shared head and keeps the current epoch's computor set.
2. Fetch workers pull tick bundles in parallel over independent peer connections.
   Each worker reuses the cached head and computors, and caches whether a peer supports
   all-ranges log requests.
3. Results may arrive out of order. A bounded ordered buffer holds them until the next
   tick is available, so verification and identity snapshots stay sequential.
4. The writer verifies quorum, builds one batch, and persists many ticks per ClickHouse
   insert. Cursors advance only after the batch is durable.

Relevant settings:

| Variable | Default | Meaning |
| --- | --- | --- |
| `INDEXER_FETCH_CONCURRENCY` | `32` | Fetch workers, balanced across healthy peers |
| `INDEXER_FETCH_WINDOW` | `512` | Maximum tick bundles in flight or buffered |
| `INDEXER_WRITE_BATCH` | `128` | Maximum ticks per ClickHouse insert batch |
| `INDEXER_WRITE_FLUSH_MS` | `250` | Maximum delay before a partial batch is flushed |
| `INDEXER_VERIFY_CONCURRENCY` | CPU count minus one | Concurrent signature-verification tasks |
| `INDEXER_POLL_MS` | `750` | Head poll interval when caught up |
| `GLYPH_VERIFY_SIGNATURES` | `false` | Verify SchnorrQ signatures in addition to quorum digests |

The indexer pipelines tick data, quorum votes, and tick transactions on one
connection, then fetches logs separately. Fetch workers lease the least-loaded
healthy peer, fail over immediately on resets or timeouts, and quarantine a
peer after repeated failures. Verification runs inside the fetch workers, so
multiple ticks verify in parallel while the ordered writer only builds and
persists rows. This keeps live ingestion ahead of the chain during normal 4-5
tick/s operation and short bursts.

By default the indexer does not verify SchnorrQ signatures locally. It still requires
a distinct supermajority of computor indices to agree on one transaction digest, checks
the tick-data digest, verifies every log digest, and resolves every non-zero transaction
digest. Local FourQ verification costs roughly 5 ms per vote on a small host, so checking
all ~451 votes caps catch-up near one tick per second. Pass `--verify-signatures` (or set
`GLYPH_VERIFY_SIGNATURES=true`) when you want cryptographic evidence from the votes,
tick data, and transactions in addition to the quorum agreement.

## Configuration

The most common variables are below. The full list is in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md).

| Variable | Default | Used by |
| --- | --- | --- |
| `BIND` | `127.0.0.1:8080` | API listen address |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | API, CLI, store |
| `CLICKHOUSE_DB` | `glyph` | API, CLI, store |
| `QUBIC_PEERS` | unset | Indexer peers, live API routes, and broadcast |
| `ALLOW_BROADCAST` | `false` | Enables `POST /v1/transactions` |
| `BROADCAST_PEERS` | `8` | Peers per broadcast fan-out, capped at `8` |
| `STREAM_POLL_MS` | `1000` | Stream hub index-head poll interval |
| `PRICE_POLL_MS` | `60000` | Live price feed refresh interval |
| `MARKET_ENABLED` | `true` | Set `false` to disable the external market poller |
| `MARKET_POLL_MS` | `5000` | Market provider round-robin interval |
| `COINGECKO_API_KEY`, `COINGECKO_URL`, `MEXC_URL`, `GATE_URL` | unset | Market data endpoints and optional key |
| `ARCHIVE_DIR` | unset | Filesystem raw archive when S3 is unset |
| `S3_BUCKET`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_PREFIX` | unset | Raw archive to any S3-compatible store |
| `ARCHIVE_FAIL_CLOSED` | `false` | Fail a tick instead of indexing when archiving fails |
| `ARCHIVE_VOTES` | `false` | Store full vote headers in tick bundles |
| `MEMPOOL_ENABLED` | `true` | Internal transaction tracker and webhook events |
| `MEMPOOL_POLL_MS` | `500` | Tracker status reconciliation interval |
| `MEMPOOL_RETRY_MS` | `1000` | Minimum delay between rebroadcasts |
| `MEMPOOL_MAX_ATTEMPTS` | `10` | Maximum broadcast attempts per entry |
| `MEMPOOL_EXPIRY_TICKS` | `10` | Expiry window after the target tick |
| `MEMPOOL_MAX_ENTRIES` | `10000` | Maximum open tracked transactions |
| `MEMPOOL_MAX_PER_SOURCE` | `100` | Maximum open tracked transactions per source |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Webhook delivery timeout. Each event is attempted once. |
| `WEBHOOK_ALLOW_PRIVATE` | `false` | Allow localhost and private webhook targets |
| `PRUNE_KEEP_EPOCHS` | unset | Drop partitions older than the newest N epochs |

Peer selection starts from the configured seeds. `NodePool::connect_best` picks the
peer with the fastest handshake, and the indexer reconnects across the seed list on
failure. `glyph peers` prints addresses advertised through `ExchangePublicPeers`;
those learned addresses are informational until they are added to the configuration.
Do not put a hosted RPC URL in the peer list. The CLI expects a socket address.

## API

The v1 contract is in [docs/API.md](docs/API.md). A short tour:

```bash
# Service and index state
curl -sS localhost:8080/healthz
curl -sS localhost:8080/v1/status
curl -sS localhost:8080/v1/network
curl -sS localhost:8080/v1/network/index
curl -sS localhost:8080/v1/network/system
curl -sS localhost:8080/v1/network/peers

# Ticks and transactions
curl -sS 'localhost:8080/v1/ticks?epoch=240&limit=25'
curl -sS localhost:8080/v1/ticks/91915000
curl -sS localhost:8080/v1/ticks/91915000/capacity
curl -sS 'localhost:8080/v1/ticks/91915000/events?limit=25'
curl -sS localhost:8080/v1/transactions/<hash>
curl -sS localhost:8080/v1/transactions/<hash>/effects
curl -sS localhost:8080/v1/transactions/<hash>/lifecycle
curl -sS 'localhost:8080/v1/transactions/inclusion-risk?source=<identity>&target_tick=91915100'

# Live computor revenue and oracle state
curl -sS 'localhost:8080/v1/network/revenue?limit=25&sort=reward'
curl -sS localhost:8080/v1/network/oracles
curl -sS 'localhost:8080/v1/network/oracles/queries?status=pending&limit=10'

# Identity history, including contract-originated effects
curl -sS 'localhost:8080/v1/identities/<identity>/activity?limit=25'
curl -sS 'localhost:8080/v1/identities/<identity>/events?limit=25'
curl -sS 'localhost:8080/v1/identities/<identity>/counterparties?limit=10'
curl -sS 'localhost:8080/v1/identities/<identity>/flow?interval=3600&limit=24'

# Wallets and assets
curl -sS localhost:8080/v1/wallets/<identity>
curl -sS 'localhost:8080/v1/assets/<issuer>/<name>/transfers?limit=25'
curl -sS 'localhost:8080/v1/assets/<issuer>/<name>/holders?limit=25'

# Prices, market, analytics, and streams
curl -sS 'localhost:8080/v1/prices/history?hours=24'
curl -sS localhost:8080/v1/market
curl -sS 'localhost:8080/v1/analytics/activity?interval=3600&limit=24'
curl -sS -N 'localhost:8080/v1/stream?types=tick,event'
curl -sS -N 'localhost:8080/v1/stream?identity=<identity>'

# Validation, broadcast, webhooks, archive, docs
curl -sS -X POST localhost:8080/v1/transactions/validate \
  -H 'content-type: application/json' -d '{"raw":"0x..."}'
curl -sS -X POST localhost:8080/v1/webhooks \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com/glyph","secret":"at-least-16-chars","events":["transaction.confirmed"]}'
curl -sS -X POST localhost:8080/v1/transactions \
  -H 'content-type: application/json' -d '{"raw":"0x..."}'
curl -sS -X POST localhost:8080/v1/transactions \
  -H 'content-type: application/json' -d '{"raw":"0x...","direct":true}'
curl -sS 'localhost:8080/v1/ticks/91915000/raw?section=transactions' -o tick.bin
curl -sS localhost:8080/docs
```

Response conventions:

- `snake_case` JSON fields and RFC 3339 UTC timestamps.
- Amounts and balances are JSON integers. Use a 64-bit-safe parser.
- Identities are uppercase base-26; transaction hashes are lowercase base-26.
- Errors use RFC 9457 problem documents with a stable `code`.
- Collections use cursor pagination with `limit` (default 50, maximum 200) and
  `meta.next_cursor`. `offset` remains available as a deprecated fallback.
- Tick and transaction point reads are immutable and cacheable. Collection feeds are
  short-lived.

Peer-backed routes and broadcast use direct TCP peers. Streaming, batch lookup,
analytics, prices, and raw bundle reads use only local state. `/docs` is the Scalar API
reference backed by an embedded bundle, so it works offline.

The price routes report QUBIC/USDT from the Qubic Price oracle. They return
`meta.available: false` until an active QUBIC/USDT oracle subscription has a revealed
reply, or QPayhub's `GetQuUsdPrice` has a usable value. The mining price series is not
served as QUBIC.

## Workspace

| Crate | Binary | Responsibility |
| --- | --- | --- |
| [`crates/qubic`](crates/qubic) | none | Wire framing, message payloads, identity base-26, log decoding, K12, SchnorrQ |
| [`crates/node`](crates/node) | none | Direct TCP connections, handshake, request correlation, peer pool, bundle assembly |
| [`crates/indexer`](crates/indexer) | none | Fetch pipeline, verification, epoch boundaries, archive policy, ordered buffering, cursor checkpoints |
| [`crates/store`](crates/store) | none | ClickHouse schema, gateway tables, row types, batched writer, queries |
| [`crates/archive`](crates/archive) | none | QBB1 bundle encoding, epoch computor sidecar, local/S3 archive backends |
| [`crates/api`](crates/api) | `glyph-api` | Axum router, response models, OpenAPI, pagination, streaming, search, mempool, webhooks |
| [`crates/cli`](crates/cli) | `glyph` | Operator commands: `head`, `peers`, `tick`, `sync`, `db-init`, `prune`, `serve` |

The dependency direction is acyclic. `cli` depends on `api`, `archive`, `indexer`,
`node`, `store`, and `qubic`. `api` depends on `archive`, `node`, `store`, and `qubic`.
`indexer` depends on `archive`, `node`, `store`, and `qubic`. `node` and `store` depend
on `qubic`. `qubic` has no I/O.

## Commands

```bash
# Current head from a peer
cargo run -p cli --bin glyph -- head --peer peer.example:21841

# Peers advertised through ExchangePublicPeers
cargo run -p cli --bin glyph -- peers --peer peer.example:21841

# Fetch and summarize one tick bundle
cargo run -p cli --bin glyph -- tick 91915000 --peer peer.example:21841

# Read the on-chain QUBIC/USDT price, or report why it is unavailable
cargo run -p cli --bin glyph -- price --peer peer.example:21841

# Index from the current epoch initial tick
cargo run -p cli --bin glyph -- sync --peer peer.example:21841

# Backfill with local SchnorrQ checks enabled
cargo run -p cli --bin glyph -- sync --peer peer.example:21841 --verify-signatures

# Apply the ClickHouse schema
cargo run -p cli --bin glyph -- db-init

# Drop chain partitions older than the newest 54 epochs. Add --dry-run first.
cargo run -p cli --bin glyph -- prune --keep 54 --dry-run

# Serve the API
cargo run -p cli --bin glyph -- serve
```

## Operations

- ClickHouse is the only hot store. Epoch partitions make retention and replay cheap.
- Epoch boundaries are observed from peer heads and persisted in `glyph.epochs`. One
  `glyph` database holds every epoch partition; per-epoch databases are deprecated.
- Retention is opt-in. `PRUNE_KEEP_EPOCHS=N` drops chain partitions older than the
  newest N epochs after a rollover, and `glyph prune --keep N [--dry-run]` does the
  same on demand. Neither deletes archive objects, `identities`, or `price_points`.
- The raw archive is the cold store and replay source. Each verified tick can be written
  as a zstd-compressed QBB1 bundle before it is persisted. Vote headers are omitted by
  default (`ARCHIVE_VOTES=false`); the computor set is written once per epoch to
  `raw/epoch=NNN/computors.bin`.
- Log IDs are epoch-scoped. The canonical event identifier is `{epoch}:{logId}`.
- Epochs below 240 are not indexed. Tick lookups outside the indexed range return
  `404 not_found`.
- Backfills resume from the stored cursor. Replaying a range is idempotent because every
  row is versioned.
- Compose is optional. `deploy/docker/env.example` lists every setting, and
  [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) covers health, backups, upgrades, and
  security.

## Development

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace --all-targets

for script in scripts/*.sh; do bash -n "$script"; done
```

CI runs format, clippy, and tests without services. ClickHouse integration tests skip
when `CLICKHOUSE_TEST_URL` is unreachable, so a clean runner does not need a database.

### Scripts

| Script | Purpose |
| --- | --- |
| [`scripts/install-clickhouse.sh`](scripts/install-clickhouse.sh) | Download a ClickHouse binary to `CH_BIN` |
| [`scripts/clickhouse.sh`](scripts/clickhouse.sh) | `start`, `stop`, `status`, `run`, `client`, `wipe` |
| [`scripts/dev-up.sh`](scripts/dev-up.sh) | Start ClickHouse, apply schema, build and start API and indexer |
| [`scripts/dev-down.sh`](scripts/dev-down.sh) | Stop API, indexer, and ClickHouse. `--wipe` deletes data |
| [`scripts/smoke.sh`](scripts/smoke.sh) | Check ClickHouse, API status, peer head, and a tick fetch |
| [`scripts/bench-api.sh`](scripts/bench-api.sh) | Measure warm p50/p95/p99 for each read route |
| [`scripts/bench-load.py`](scripts/bench-load.py) | Run a timed concurrency benchmark with error counts |
| [`scripts/docker-up.sh`](scripts/docker-up.sh) | Build and start the Compose stack, then wait for health |
| [`scripts/docker-down.sh`](scripts/docker-down.sh) | Stop the Compose stack. `--wipe` deletes volumes |

`deploy/clickhouse/config.xml` and `deploy/clickhouse/users.xml` define the local
ClickHouse instance. The data directory is `.data/clickhouse`, the HTTP port is `8123`,
and the native port is `9000`. The configuration is development-only and binds to
`127.0.0.1`.

## Documentation

| Document | Contents |
| --- | --- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | Goals, non-goals, direct-peer rationale, verification tiers, epoch handling, failure modes, capacity |
| [docs/PROTOCOL.md](docs/PROTOCOL.md) | Framing, message types, struct offsets, identity encoding, ingestion sequence, quorum rules, logs |
| [docs/API.md](docs/API.md) | Endpoints, cursor pagination, RFC 9457 errors, caching, search, examples |
| [docs/DATA_MODEL.md](docs/DATA_MODEL.md) | Entities, invariants, ClickHouse keys and projections, capacity math, retention, replay |
| [docs/DEPLOYMENT.md](docs/DEPLOYMENT.md) | Docker image, Compose stack, configuration, health, backups, upgrades, security |
| [docs/ROADMAP.md](docs/ROADMAP.md) | Phased milestones with exit criteria |

## License

MIT OR Apache-2.0. See the workspace manifest.
