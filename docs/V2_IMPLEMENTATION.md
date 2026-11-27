# Glyph v2 implementation contract

This file is the working contract for the v2 change set. It records the
breaking changes, the assumptions behind them, and which lane owns which files.
It is temporary design documentation and can be folded into the permanent docs
after the change lands.

## Scope

This change set implements four things:

1. Correct epoch lifecycle handling with observed epoch boundaries and retention.
2. A much smaller raw archive by default: no full vote headers per tick, and
   computors stored once per epoch in a sidecar object.
3. An API mempool for broadcast transactions, with status polling, retries,
   expiry, and signed webhook delivery.
4. Documentation and OpenAPI updates for the new behavior.

## Explicit non-goals for this change set

These are deferred. Do not implement them in this pass:

- Replacing ClickHouse with Iceberg, StarRocks, or DuckDB.
- The full binary-encoding schema migration.
- Removing every full-row ClickHouse projection.
- A production webhook scheduler with a separate queue.
- A protocol-level Qubic mempool. Qubic peers do not expose a mempool.

## Assumptions

- Epoch boundaries are observed from peer `Head` values and persisted. Do not
  assume 604,800 ticks per epoch. The current network has roughly 330k ticks per
  epoch, and the tick rate can change.
- Previous-epoch backfill is only attempted when the epoch boundary and computor
  set are known. Never silently assign a tick to the wrong epoch.
- A transaction's `tick` field is its target tick. Qubic does not acknowledge a
  broadcast. A mempool transaction is considered expired when the indexed head
  is greater than `target_tick + MEMPOOL_EXPIRY_TICKS` and the transaction has
  not appeared in the index. The default expiry window is 10 ticks and is
  configurable.
- Mempool status is best-effort. `broadcast` means at least one peer accepted
  the packet at the socket. It does not mean the transaction will be included.
- Full vote headers are not part of the default long-term archive. Operators can
  opt back in with `ARCHIVE_VOTES=true`.
- The computor set is written once per epoch to a sidecar object. It is not
  repeated in every tick bundle by default.

## Epoch design

Add an `epochs` table:

```sql
CREATE TABLE IF NOT EXISTS glyph.epochs
(
    epoch UInt16,
    initial_tick UInt32,
    first_tick UInt32,
    last_tick UInt32,
    closed UInt8,
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY epoch
```

Store methods:

- `write_epoch_start(epoch, initial_tick)` upserts a boundary.
- `close_epoch(epoch, first_tick, last_tick)` marks an epoch closed.
- `epoch_boundaries() -> Vec<EpochBoundary>` returns `(epoch, initial_tick)`.
- `prune_epochs(keep: u16) -> Vec<u16>` drops ClickHouse partitions older than
  the newest `keep` epochs and deletes their metadata. It must not delete
  `identities` or `price_points`; those are global or time-keyed tables.
- `epoch_metadata()` and `epoch_metadata_for(epoch)` may be used by the API.

Indexer behavior:

- On startup and on every head refresh, record the current epoch and initial
  tick.
- On an epoch change, close the previous epoch at its last observed tick. Do not use
  `new_initial_tick - 1`; restart-based transitions skip tick numbers.
- Replace the hardcoded `TICKS_PER_EPOCH` lookup with the persisted boundary
  map. If a tick's epoch cannot be determined, skip it with a warning rather
  than guessing.
- Keep the existing previous-epoch catch-up behavior, but only for epochs whose
  computors and boundaries are known.
- Optional retention: after an epoch rollover, prune epochs older than
  `PRUNE_KEEP_EPOCHS` when it is set.

API behavior:

- `GET /v1/epochs` rows gain `initial_tick`.
- The existing `closed` field remains.
- If the `epochs` table has no row for an epoch, fall back to the derived values
  so existing databases still work.

CLI:

```text
glyph prune --keep 54 [--dry-run]
```

The command drops ClickHouse partitions only. It does not delete archive
objects, because archive deletion is irreversible and should be a separate,
explicit operation.

## Archive design

Default policy:

- `ARCHIVE_VOTES=false`: do not store full vote headers in tick bundles.
- Computors: do not store the 21.7 KB computor set in every tick bundle. Write
  one sidecar object per epoch at `raw/epoch=NNN/computors.bin`.
- Tick bundles still contain: tick header, `TickData`, transactions, log ranges,
  and logs.

Archive API:

- `archive::computor_key(epoch)`.
- `archive::put_computors(archive, epoch, bytes)`.
- `archive::get_computors(archive, epoch)`.
- `RawBundle` keeps the optional `Votes` and `Computors` sections for backward
  compatibility with existing objects.
- `GET /v1/ticks/{tick}/raw?section=votes` returns `404` with a clear message
  when votes were not archived.
- `GET /v1/ticks/{tick}/raw?section=computors` reads the epoch sidecar if the
  bundle has no computors section.

## Mempool and webhook API

Environment:

| Variable | Default | Meaning |
| --- | --- | --- |
| `MEMPOOL_ENABLED` | `true` | Enable mempool persistence and background tracking. |
| `MEMPOOL_POLL_MS` | `500` | Status poll interval. |
| `MEMPOOL_RETRY_MS` | `1000` | Minimum delay between rebroadcasts. |
| `MEMPOOL_MAX_ATTEMPTS` | `10` | Maximum broadcast attempts. |
| `MEMPOOL_EXPIRY_TICKS` | `10` | Expiry window after the target tick. |
| `MEMPOOL_MAX_ENTRIES` | `10000` | Maximum open mempool entries. |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Delivery timeout. |
| `WEBHOOK_ALLOW_PRIVATE` | `false` | Allow localhost/private webhook targets. |
| `GLYPH_LABELS_FILE` | unset | Operator label registry for identities, assets, and contracts. |
| `GLYPH_WHALE_MIN_AMOUNT` | `1000000000` | Whale threshold in qu. |

Statuses:

`queued`, `broadcast`, `confirmed`, `expired`, `failed`, `cancelled`.

Endpoints:

- `POST /v1/mempool`: body `{ "raw": "hex", "peers": 3, "webhook_url": "...", "webhook_secret": "..." }`.
  Returns `202` with the entry and a link to `GET /v1/mempool/{hash}`.
- `GET /v1/mempool`: list entries, optional `status` and `limit`.
- `GET /v1/mempool/{hash}`: one entry with merged indexed status.
- `DELETE /v1/mempool/{hash}`: mark `cancelled` when not confirmed.
- `POST /v1/transactions`: now submits to the mempool by default. Add
  `"direct": true` to use the old direct fan-out behavior.
- `POST /v1/webhooks`: register a webhook.
- `GET /v1/webhooks`: list active webhooks.
- `DELETE /v1/webhooks/{id}`: deactivate.
- `GET /v1/webhooks/{id}/deliveries`: recent delivery attempts.

Webhook registration body:

```json
{
  "url": "https://example.com/glyph",
  "secret": "at-least-16-characters",
  "events": ["transaction.confirmed", "transaction.expired"],
  "transaction_hashes": ["..."],
  "identity": "..."
}
```

Empty `transaction_hashes` means all hashes. Empty `identity` means all
identities. Events support `transaction.queued`, `transaction.broadcast`,
`transaction.confirmed`, `transaction.expired`, `transaction.failed`, and
`transaction.cancelled`.

Delivery headers:

- `X-Glyph-Event`
- `X-Glyph-Delivery`
- `X-Glyph-Timestamp`
- `X-Glyph-Signature: sha256=<hex hmac of "{timestamp}.{body}">`

Retry with exponential backoff. Persist every attempt in
`webhook_deliveries`.

## ClickHouse gateway tables

Mempool and webhooks are operational state, not indexed chain data. Put their
DDL and client code in `crates/store/src/gateway.rs`, not in the canonical
schema module.

Tables:

- `mempool`
- `webhooks`
- `webhook_deliveries`

Use `ReplacingMergeTree(version)` with a monotonic millisecond version.

## File ownership

Lane `epoch`:

- `crates/store/src/schema.rs`
- `crates/store/src/lib.rs`
- `crates/store/src/rows.rs`
- `crates/indexer/src/lib.rs`
- `crates/cli/src/main.rs`

Lane `archive`:

- `crates/archive/src/key.rs`
- `crates/archive/src/lib.rs`
- `crates/archive/src/bundle.rs`
- `crates/indexer/src/archive_policy.rs` (new file)
- archive tests

Lane `mempool`:

- `crates/store/src/gateway.rs` (new file)
- `crates/store/src/lib.rs` only to export the gateway module
- `crates/api/src/mempool.rs` (new file)
- `crates/api/src/webhook.rs` (new file)
- `crates/api/src/lib.rs`
- `crates/api/src/advanced.rs` for validation helper visibility
- `crates/api/Cargo.toml`
- root `Cargo.toml` for `hmac`, `sha2`
- mempool and webhook tests

Lane `docs`:

- `docs/API.md`
- `docs/DEPLOYMENT.md`
- `docs/ARCHITECTURE.md`
- `docs/DATA_MODEL.md`
- `README.md`
- `deploy/docker/env.example`
- `crates/api/src/openapi.rs`

Integration (root):

- wire the archive policy into the indexer
- wire the gateway/mempool/webhook services into `AppState`
- route updates not owned by a lane
- full workspace verification
- commits

Do not edit a file owned by another lane. If a change is required, report it in
the lane result and let the root integrate it.

## Verification

Every lane must run the narrowest relevant tests and report the exact commands.

Root verification:

```bash
cargo fmt --all --check
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
cargo build --release --workspace
```

If a ClickHouse integration test requires a live server, report that it was
skipped rather than claiming it ran.
