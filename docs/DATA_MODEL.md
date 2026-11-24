# Data model

The index lives in ClickHouse. This document defines the entities, the invariants the
indexer must preserve, the physical keys and projections, retention, capacity, and replay.
The canonical DDL is [`crates/store/src/schema.rs`](../crates/store/src/schema.rs). Tables
described as derived are computed from the canonical tables at query time and have no
physical DDL of their own.

## Design rules

- **Append and replace.** Every table uses `ReplacingMergeTree(version)`. Re-ingesting a
  tick or retrying a batch reuses the tick version, while rewriting a changed tick uses a
  higher `version`. ClickHouse converges to the newest row per key.
- **One partition per epoch.** `PARTITION BY epoch` makes epoch pruning cheap and matches
  the Qubic lifecycle. Tables that span epochs are the exception and are documented.
- **Keys match the API.** `ticks` is ordered by `(epoch, tick)`, `transactions` by
  `(tick, tx_index)`, and `events` by `(epoch, log_id)`. Hot lookups that do not match the
  sort key get a projection.
- **Payloads are stored, not executed.** Transaction input is lowercase hex. Log bodies are
  JSON documents produced at decode time. Both use `ZSTD(3)` and are never interpreted as
  executable content or parsed at query time.
- **Canonical encodings.** Identities are uppercase base-26. Transaction hashes are
  lowercase base-26. Digests and signatures are raw fixed-width bytes. Decimal values are
  `Int64` in storage and JSON numbers on the wire.
- **Only quorum-verified data is published.** The write path inserts a tick only after the
  matching vote group reaches the quorum threshold. It stores `data` when the complete
  transaction, log, and signature checks pass, and `quorum` when they do not. The `state`
  tier is reserved and never emitted.

## Entities

```text
epoch 1---* tick 1---* transaction 1---* event
              |              |
              |              +---* event (via log range)
              +---* event (special slots 4096..4101)
              +---1 quorumEvidence
identity 1---* transaction (source or destination)
identity 1---* event (participant)
asset 1---* event (issuance, ownership, possession)
epoch 1---* computor
epoch 1---* tick
```

| Entity | Identity | Lifetime | Source |
| --- | --- | --- | --- |
| Epoch | `epoch` number | Immutable once closed | `CurrentTickInfo`, `Computors` |
| Tick | `(epoch, tick)`, `tick` globally monotonic | Immutable once quorum reached | `Tick`, `TickData`, votes |
| Transaction | `hash`, unique, and `(tick, txIndex)` | Immutable once quorum final | `RespondLog` transaction stream |
| Event | `(epoch, logId)` | Immutable once digest verified | `RespondLog` |
| Quorum evidence | `(epoch, tick)` | Summary row in `quorum_evidence`; full vote sets are written to the raw archive when it is enabled | `Tick` vote set |
| Identity | 60-character identity | Derived, updated as activity arrives | Transfers and burns |
| Asset | `(issuer, name)` | Derived from `events`; no dedicated table | Asset events |
| Computor | `(epoch, index)` | Cached in memory, persisted in `computors`, and archived once per epoch in `raw/epoch=NNN/computors.bin` | `Computors` |
| Ingest checkpoint | key | Updated per batch | Indexer |
| Price point | `hour` | Latest row per hour wins | QUBIC/USDT oracle poller |
| Mempool entry | `hash` | Operational; open until confirmed, expired, failed, or cancelled | API submission |
| Webhook | `id` | Operational until deactivated | Webhook registration |
| Webhook delivery | `id` | Retried in place until delivered or dead-lettered | Webhook dispatcher |

## `glyph.ticks`

One canonical row per tick.

| Column | Type | Meaning |
| --- | --- | --- |
| `epoch` | `UInt16` | Epoch number |
| `tick` | `UInt32` | Global tick number. Monotonic across epochs |
| `timestamp` | `DateTime64(3, 'UTC')` | Time from the tick header, not the ingest clock |
| `computor_index` | `UInt16` | Index of the computor that proposed the winning tick |
| `transaction_count` | `UInt32` | Number of non-zero transaction slots |
| `event_count` | `UInt32` | Number of events assigned to the tick |
| `signature` | `FixedString(64)` | Winning tick signature |
| `tick_digest` | `FixedString(32)` | Canonical tick digest used for fork detection |
| `verification` | `LowCardinality(String)` | `quorum` or `data`. `state` is reserved |
| `version` | `UInt64` | Logical write sequence |

```sql
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, tick)
```

Invariants:

- `(epoch, tick)` is unique after merge, and `tick` is unique globally. The same tick
  number must not appear in two epochs.
- `transaction_count` equals the number of non-zero entries in the tick's
  `TickData.transaction_digests`.
- `event_count` equals the number of rows in `glyph.events` for the tick.
- `tick_digest` is stable for the winning tick and is the value used to detect a divergent
  peer.
- `verification = 'data'` implies the quorum threshold and the complete data checks passed.
  `quorum` means only the vote threshold and digest agreement passed.
- `verification` never decreases for the same `(epoch, tick)` unless a higher `version`
  explicitly quarantines the tick. A quarantined tick is removed from default listings.

`kind` (`normal` or `epochEnd`) is derived: a tick is `epochEnd` when a row exists with
`tx_index = -1` and `log_type = 255` whose custom op code is `END_EPOCH`, or when the
epoch table marks it as the final tick. It is not stored as a separate string in the
current schema. If the API needs a direct filter, add a `kind Enum8` column in an additive
migration.

## `glyph.transactions`

One row per non-empty transaction slot.

| Column | Type | Meaning |
| --- | --- | --- |
| `epoch` | `UInt16` | Epoch number |
| `tick` | `UInt32` | Tick containing the transaction |
| `tx_index` | `UInt16` | Slot index, `0..4095` |
| `hash` | `String` | Lowercase base-26 K12 hash of the signed transaction |
| `source` | `String` | Uppercase base-26 source identity |
| `destination` | `String` | Uppercase base-26 destination identity |
| `amount` | `Int64` | Amount in qu. API renders it as a string |
| `input_type` | `UInt16` | Contract or procedure selector |
| `input_data` | `String` | Lowercase hex of the raw input, `ZSTD(3)`, no `0x` prefix |
| `status` | `LowCardinality(String)` | `success`, `failed`, or `unknown` |
| `timestamp` | `DateTime64(3, 'UTC')` | Tick time |
| `version` | `UInt64` | Logical write sequence |

```sql
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (tick, tx_index)

ALTER TABLE glyph.transactions
ADD PROJECTION IF NOT EXISTS by_hash
(
    SELECT * ORDER BY hash
)
```

Invariants:

- `(tick, tx_index)` is unique after merge.
- `hash` is unique across all indexed history. A duplicate hash with different content is
  a data-tier failure, not a replacement.
- `hash` is `K12(source || destination || amount || tick || input_type || input_size ||
  input || signature)`, encoded with the lowercase base-26 function.
- The row set for a tick exactly matches the non-zero digests in that tick's `TickData`.
  No missing slots, no extra slots.
- `input_data` is the exact input bytes, hex encoded. It is never decoded into JSON at
  storage time.
- `timestamp` equals the parent tick timestamp. It is denormalized for query speed and is
  not independently authoritative.

Projections for the API filters (created by `schema.rs`):

```sql
ALTER TABLE glyph.transactions
ADD PROJECTION IF NOT EXISTS by_source (SELECT * ORDER BY (source, tick, tx_index));

ALTER TABLE glyph.transactions
ADD PROJECTION IF NOT EXISTS by_destination (SELECT * ORDER BY (destination, tick, tx_index));
```

Identity history is `source` or `destination`, so a single identity query may need both
projections. If the query planner does not combine them well, add a normalized
`glyph.transaction_participants` table instead of more projections.

## `glyph.events`

One row per decoded log event.

| Column | Type | Meaning |
| --- | --- | --- |
| `epoch` | `UInt16` | Epoch number |
| `tick` | `UInt32` | Tick that emitted the event |
| `log_id` | `UInt64` | Epoch-scoped log ID from the network |
| `log_type` | `UInt16` | Numeric type, see [PROTOCOL.md](./PROTOCOL.md#log-bodies) |
| `type_name` | `LowCardinality(String)` | Stable name derived from `log_type` |
| `tx_hash` | `String` | Originating transaction hash, empty for special events |
| `tx_index` | `Int16` | Slot index, `-1` for special events |
| `body` | `String` | JSON-encoded decoded body (or raw representation), `ZSTD(3)` |
| `log_digest` | `UInt64` | `K12_8(received body)` read as little-endian `u64` |
| `timestamp` | `DateTime64(3, 'UTC')` | Tick time |
| `version` | `UInt64` | Logical write sequence |

```sql
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, log_id)

ALTER TABLE glyph.events
ADD PROJECTION IF NOT EXISTS by_tick
(
    SELECT * ORDER BY (epoch, tick, log_id)
)
```

Invariants:

- `(epoch, log_id)` is unique after merge. `log_id` is not globally unique across epochs.
- `log_digest` was checked against `K12_8(received body)` before insert. A mismatch is
  rejected and the tick is not promoted to `data`.
- `type_name` is derived from `log_type`. Renaming a type in code does not change
  `log_type`, and old rows keep the old name until re-ingested. The numeric field is
  authoritative for clients that need stability across a rename.
- For `tx_index >= 0`, `tx_hash` must match the transaction at that slot. For
  `tx_index = -1`, `tx_hash` is empty and the event is a special event from slots
  4096..4101.
- The number of events for a tick equals `ticks.event_count` and the sum of the valid
  lengths in the tick's `LogRanges`.
- `body` is a JSON document. The indexer decodes the raw log body into typed fields and
  stores the JSON string. Unknown types keep a raw representation. The original raw bytes
  are not retained in the hot table.

Projection for event type scans:

```sql
ALTER TABLE glyph.events
ADD PROJECTION IF NOT EXISTS by_type
(
    SELECT * ORDER BY (type_name, epoch, log_id)
);
```

Identity-filtered event queries cannot use the body blob efficiently. The normalized
`glyph.event_participants` table stores `(epoch, log_id, identity, role)` where role is
`from`, `to`, `issuer`, `owner`, `possessor`, `burner`, `querier`, or `solver`. The
indexer populates it from the typed decoder in the same batch as the event. Its order key
is `(identity, tick, log_id)`, so a wallet history query reads only that identity's rows
and joins back to `events` for the decoded body.

## `glyph.identities`

Derived aggregates per identity. This is a convenience table, not consensus state.

| Column | Type | Meaning |
| --- | --- | --- |
| `identity` | `String` | Uppercase base-26 identity |
| `public_key` | `String` | `0x` plus 64 lowercase hex characters |
| `balance` | `Int64` | Net qu from indexed transfers and burns |
| `incoming_amount` | `Int64` | Sum of incoming transfers |
| `outgoing_amount` | `Int64` | Sum of outgoing transfers and burns |
| `transfer_count` | `UInt64` | Number of transfer events touching the identity |
| `first_seen_tick` | `UInt32` | Earliest indexed tick |
| `last_seen_tick` | `UInt32` | Latest indexed tick |
| `version` | `UInt64` | Logical write sequence |

```sql
ENGINE = ReplacingMergeTree(version)
ORDER BY identity

ALTER TABLE glyph.identities
ADD PROJECTION IF NOT EXISTS by_activity
(
    SELECT * ORDER BY (last_seen_tick, identity)
)
```

Invariants:

- `identity` is canonical uppercase base-26 with a valid checksum.
- `public_key` is the decoded 32-byte key in hex.
- `balance = incoming_amount - outgoing_amount` for the indexed transfer and burn set.
- `first_seen_tick <= last_seen_tick`.
- The row is a materialized aggregate, not a source of truth. It can be recomputed from
  `glyph.events` and `glyph.transactions` at any time.
- On a fork rewrite, affected identities are recomputed from the last checkpoint before the
  new rows are published.

The `identities` table is not partitioned by epoch because identities span epochs. It is
rebuilt on retention changes that remove the events it was derived from.

## `glyph.ingest_state`

Small key/value table for indexer coordination and replay.

| Column | Type | Meaning |
| --- | --- | --- |
| `key` | `String` | State key |
| `value` | `String` | JSON or decimal string |
| `updated_at` | `DateTime64(3, 'UTC')` | Last update time |
| `version` | `UInt64` | Logical write sequence |

```sql
ENGINE = ReplacingMergeTree(version)
ORDER BY key
```

The store defines two cursor keys:

| Key | Value | Notes |
| --- | --- | --- |
| `head` | JSON `HeadCursor` | `{"epoch":240,"tick":91915000,"timestamp":"..."}` |
| `ingest` | JSON `IngestCursor` | `{"last_indexed_tick":91915000,"epoch":240,"verification":"data","updated_at":"..."}` |

`/v1/status` reports the head from the maximum row in `glyph.ticks`, and exposes the
`ingest` cursor as progress. Both cursor keys are written after the tick row. Neither moves
backwards when an older tick is replayed. Additional operator keys can share the table, but
they must use the same `version` convention.

The writer order is deliberate: transactions, events, and identities first, then the tick
row, then the cursors. The API never reports a head whose children are missing. A crash
between inserts leaves orphan rows with the same `version` as the tick. Replaying the
same batch replaces them, and the head only advances after the tick row exists.

Only one writer should run against a database. There is no lease enforcement yet; this is
an operational rule.

## `glyph.quorum_evidence`

One row per stored tick when the producer supplies quorum evidence. The hot store keeps
the winning digests, vote counts, and verification tier. It does not keep 676 full vote
headers, because that would cost about 134 GiB per epoch; the full vote set is preserved
in the raw archive when archiving is enabled.

```sql
CREATE TABLE IF NOT EXISTS glyph.quorum_evidence
(
    epoch UInt16,
    tick UInt32,
    quorum_digest FixedString(32),
    tick_data_digest FixedString(32),
    required_votes UInt32,
    aligned_votes UInt32,
    fetched_votes UInt32,
    unique_votes UInt32,
    verified_votes UInt32,
    verification LowCardinality(String),
    warning_count UInt32,
    warnings String,
    timestamp DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
PARTITION BY epoch
ORDER BY (epoch, tick)
```

The store and writer support this table. A `TickBatch` without evidence simply omits the
row. Evidence is inserted before the tick row, so a visible tick never has a half-written
audit row. `GET /v1/ticks/{tick}/evidence` returns this row and reports `404 not_found`
when no evidence is stored.

## `glyph.epochs`, `glyph.computors`, and epoch summaries

Epoch boundaries are observed from peer `Head` values and persisted. The indexer does not
assume a fixed number of ticks per epoch.

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

The indexer upserts a boundary on startup and on every head refresh, and closes the
previous epoch at its last observed tick on rollover. Restart-based transitions can skip
tick numbers, so the next epoch's initial tick is not used to infer the previous last
tick. `GET /v1/epochs` reads the persisted
row and includes `initial_tick`; when no row exists for an epoch, the API falls back to
values derived from `glyph.ticks` so older databases still work.

The elected computor set is stored per epoch:

```sql
CREATE TABLE IF NOT EXISTS glyph.computors
(
    epoch UInt16,
    computor_index UInt16,
    public_key String,
    identity String,
    version UInt64,
    timestamp DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(version)
ORDER BY (epoch, computor_index)
```

The set is written once per epoch. The raw archive also stores one sidecar object per
epoch at `raw/epoch=NNN/computors.bin` instead of repeating the 21.7 KB set in every tick
bundle. `GET /v1/epochs/{epoch}/computors` returns the persisted set ordered by
`computor_index`.

## Assets (derived)

There is no `glyph.assets` table. The assets API derives issuance, ownership, and
possession rows from the JSON bodies in `glyph.events` with
`ORDER BY version DESC LIMIT 1 BY` per asset key. A dedicated holdings table would only be
needed if the API had to answer "who holds asset X at tick N" without scanning events; it
does not exist.

## `glyph.price_points`

The API price poller stores hourly QUBIC/USDT oracle readings. The primary source is an
active `OI::Price` oracle subscription (interface index 0); the QPayhub
`GetQuUsdPrice` read function (contract 29, input type 4) is a fallback. This is not the
core node's BTC/USDT `RequestPriceSeries` feed. `hour` is the logical key; a later
`version` replaces an earlier revision of the same hour.

```sql
CREATE TABLE IF NOT EXISTS glyph.price_points
(
    hour UInt32,
    append_tick UInt32,
    source_tick UInt32,
    epoch UInt16,
    flags UInt32,
    price_e8 Int64,
    source UInt32,
    synced UInt8,
    bound UInt8,
    series_digest FixedString(32) DEFAULT '',
    observed_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY hour
```

`price_e8` is the QUBIC price in USDT scaled by `1e8`. `source` is `0` for the price
oracle and `29` for the QPayhub fallback. Flag bit 0 marks a filled hour and bit 1 marks a
stale reading. The API price routes read the newest row per hour. Price ingestion runs in
the API process, not in the tick indexer.

## Gateway tables (operational)

Mempool and webhook state is operational, not indexed chain data. Its DDL and client code
live in `crates/store/src/gateway.rs`, separate from the canonical schema module. Every
table uses `ReplacingMergeTree(version)` with a monotonic millisecond version, and reads
resolve the newest row per key with `ORDER BY version DESC LIMIT 1 BY key`.

```sql
CREATE TABLE IF NOT EXISTS glyph.mempool
(
    hash String,
    raw String CODEC(ZSTD(3)),
    source String,
    destination String,
    amount Int64,
    tick UInt32,
    peers UInt8,
    status LowCardinality(String),
    attempts UInt8,
    broadcast_peers UInt8,
    broadcast_at UInt64,
    confirmed_at UInt64,
    confirmed_tick UInt32,
    error String,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY hash
TTL updated_at + INTERVAL 1 DAY DELETE
    WHERE status IN ('confirmed', 'expired', 'cancelled', 'failed')
```

`raw` is retained so a retry can rebroadcast without asking the client again. `tick` is the
transaction's target tick and `peers` is the requested fan-out. `status` is one of
`queued`, `broadcast`, `confirmed`, `expired`, `failed`, or `cancelled`. `queued`,
`broadcast`, and `failed` with attempts remaining are open and still tracked; the others
are terminal. Optional timestamps and counters use `0` as a sentinel rather than ClickHouse
`Nullable` columns, so `broadcast_at`, `confirmed_at`, and `confirmed_tick` are `0` when
unset. An entry expires when the indexed head is greater than
`tick + MEMPOOL_EXPIRY_TICKS` and the hash is still absent from the index.

```sql
CREATE TABLE IF NOT EXISTS glyph.webhooks
(
    id String,
    url String,
    secret String,
    events String,
    transaction_hashes String,
    identity String,
    active UInt8,
    created_at DateTime64(3, 'UTC'),
    updated_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
TTL updated_at + INTERVAL 7 DAY DELETE WHERE active = 0
```

`events` and `transaction_hashes` are JSON-encoded arrays in a `String` column. An empty
`events` array matches every transaction event, an empty `transaction_hashes` array matches
every hash, and an empty `identity` matches every source identity. `secret` must be at
least 16 characters and is used to sign deliveries. It is never serialized into API
responses. When `WEBHOOK_ENCRYPTION_KEY` is set, `url` is stored as
`enc:v1:<base64 nonce and ChaCha20-Poly1305 ciphertext>` and only the dispatcher decrypts
it. API responses expose a scheme-and-host `url_hint` instead of the full URL.

```sql
CREATE TABLE IF NOT EXISTS glyph.webhook_deliveries
(
    id String,
    webhook_id String,
    event String,
    transaction_hash String,
    status LowCardinality(String),
    attempt UInt32,
    next_attempt_at UInt64,
    response_status UInt16,
    error String,
    payload String CODEC(ZSTD(3)),
    created_at DateTime64(3, 'UTC'),
    version UInt64
)
ENGINE = ReplacingMergeTree(version)
ORDER BY id
TTL created_at + INTERVAL 1 DAY DELETE
    WHERE status IN ('delivered', 'failed')
```

A delivery row is updated in place (with a new version) across retries. `status` is
`pending`, `delivered`, or `failed`; `next_attempt_at` is `0` once no retry is scheduled;
`payload` retains the exact JSON body so retries are byte-identical. Retries use a
1 s, 2 s, 4 s, ... backoff capped at 60 s and stop after `WEBHOOK_MAX_ATTEMPTS`.

These tables are created by `Store::init`/`glyph db-init` alongside the canonical schema,
not by a separate gateway migration. Startup also applies `ALTER TABLE ... MODIFY TTL` so
existing deployments pick up retention without a manual migration.

Retention is intentionally short because Qubic broadcast has no fee: terminal mempool
entries and delivered or dead-lettered deliveries are removed after 1 day, and inactive
webhooks after 7 days.
Open mempool entries and pending deliveries are never removed by TTL. ClickHouse evaluates
TTL during background merges, so deletion is eventual rather than immediate.

## Invariants

The indexer and the store must preserve all of these. A violation is a bug, not a warning.

1. **No gaps.** Within an indexed epoch, every tick from `firstIndexedTick` to
   `lastIndexedTick` has a row. A gap stops the head.
2. **No forks in the canonical set.** At most one tick digest per `(epoch, tick)` is
   published. A second candidate that reaches threshold is quarantined and escalated.
3. **Quorum before publish.** Every published tick has at least 451 aligned valid votes
   for a non-empty tick, or 226 for an empty tick.
4. **Data completeness.** The transaction set and event set exactly match the tick's
   commitments and log ranges.
5. **Digest validity.** Every stored event has `K12_8(body) == log_digest`.
6. **Hash validity.** Every stored transaction hash matches the full signed transaction.
7. **Index uniqueness.** `(tick, tx_index)`, `(epoch, log_id)`, and transaction `hash` are
   unique after merge.
8. **Referential integrity.** Every transaction's tick exists. Every event's tick exists
   and, when `tx_index >= 0`, the transaction at that slot exists.
9. **Counts agree.** `ticks.transaction_count` and `ticks.event_count` equal the row
   counts for the tick.
10. **Monotonic head.** The committed head never moves backwards except during an explicit
    replay that writes a higher `version` for the same tick.
11. **Epoch consistency.** All ticks in an epoch share the epoch computor set and are at or
    above the epoch `initial_tick`.
12. **Canonical identifiers.** Stored identities are uppercase base-26 with valid
    checksums. Stored hashes are lowercase base-26 with valid checksums. Invalid encodings
    never reach storage.
13. **Amount range.** Amounts fit in `Int64`. API responses render them as decimal strings.
14. **Verification monotonicity.** A tick's `verification` tier only increases. A
    quarantined tick is removed from default listings and has a higher `version`.

## Versioning and replacement

`version` is a logical write sequence, not a wall-clock timestamp. `TickBatch.version`
defaults to the tick number, and every row derived from that tick shares it. A reorg
replacement must set a version higher than the previous copy of the same tick. The
writer uses the same value for all transactions, events, identities, and the tick row:

```text
version = batch.version   (default: tick number)
```

The version must be monotonically increasing for a logical key. Retries may reuse the
same value because `ReplacingMergeTree` treats equal versions as duplicates. A committed
replacement always moves forward.

Consequences:

- Re-ingesting the same tick with an equal version is a duplicate and converges to one row.
- A fork rewrite uses a still higher version.
- Clock skew cannot reorder rows.
- `FINAL` returns the winning row, but it is expensive on large scans.

Queries use `FINAL` for point reads and checkpoint checks. Aggregations that tolerate
in-flight duplicates omit `FINAL` and deduplicate with `argMax` or a subquery. Never use
`FINAL` on an unbounded scan.

## Physical keys and projections

| Table | Partition | Order key | Projections | Serves |
| --- | --- | --- | --- | --- |
| `ticks` | `epoch` | `(epoch, tick)` | `by_tick` | Tick by number, head, epoch ranges |
| `transactions` | `epoch` | `(tick, tx_index)` | `by_hash`, `by_source`, `by_destination` | Hash lookup, tick list, identity history |
| `events` | `epoch` | `(epoch, log_id)` | `by_tick`, `by_recent`, `by_type` | Event by ID, tick list, type scans, activity buckets |
| `event_participants` | `epoch` | `(identity, tick, log_id)` | none | Identity activity and contract effects |
| `identities` | none | `identity` | `by_activity` | Identity lookup, top identities |
| `ingest_state` | none | `key` | none | Internal |
| `quorum_evidence` | `epoch` | `(epoch, tick)` | none | Verification details |
| `epochs` | none | `epoch` | none | Observed epoch boundaries and lifecycle |
| `computors` | none | `(epoch, computor_index)` | none | Elected computor set per epoch |
| `price_points` | none | `hour` | none | Hourly QUBIC/USDT prices |
| `mempool` | none | `hash` | none | API broadcast tracking (gateway) |
| `webhooks` | none | `id` | none | Webhook registrations (gateway) |
| `webhook_deliveries` | none | `(webhook_id, id)` | none | Persisted delivery attempts (gateway) |

`quorum_evidence` is partitioned by epoch, while `price_points` is not partitioned. Both
are small relative to the tick tables and are read by a single key or by hour. The
gateway tables hold API operational state, not chain history.

`ticks` has a `by_tick` projection because the order key starts with `epoch`. Without it,
`WHERE tick = ?` scans every epoch partition. It is created by
`CREATE_TICKS_BY_TICK` in the schema:

```sql
ALTER TABLE glyph.ticks
ADD PROJECTION IF NOT EXISTS by_tick (SELECT * ORDER BY tick);
```

`ReplacingMergeTree` projections require `deduplicate_merge_projection_mode = 'rebuild'`.
That setting is applied to `ticks`, `transactions`, and `events` in the schema. If a table
predates the projection, run the matching `ALTER ... MODIFY SETTING` statement before
adding the projection.

## Retention and tiering

The default policy is deliberately simple and configurable.

| Tier | Store | Default window | Contents | Cost |
| --- | --- | --- | --- | --- |
| Hot | ClickHouse | Current epoch forward | Canonical rows, current indexes | Fast queries |
| Warm | ClickHouse with older partitions | Configurable | Same schema, older epochs | Disk |
| Cold | Raw archive on local disk or S3 | Optional, unbounded | Raw tick bundles, transactions, logs, vote headers | Object storage |

Rules:

- The current implementation keeps indexed epochs until an operator prunes them. `PRUNE_KEEP_EPOCHS` enables optional retention after a rollover, and `glyph prune --keep N [--dry-run]` drops ClickHouse partitions older than the newest N epochs. Pruning deletes only epoch-partitioned chain tables; it never deletes `glyph.identities` or `glyph.price_points`, and it never deletes archive objects.
- The raw archive is optional and off by default. When enabled, it preserves the verified
  bundle for each tick before the ClickHouse write, so a dropped partition can be rebuilt.
  Full vote headers are omitted by default (`ARCHIVE_VOTES=false`); the epoch computor set
  is stored once per epoch at `raw/epoch=NNN/computors.bin`. Without the archive, dropping
  a partition loses that history unless it can be re-fetched from a peer that still
  retains it.
- `identities` is derived from indexed QU transfer events and spans epochs. Dropping an
  epoch without rebuilding the aggregate makes the balance partial; the API only labels the
  current limitation in its `meta.notice`.
- ClickHouse backups are per partition. They are an availability optimization, because the
  index can also be rebuilt from peers within their retention window.

## Capacity math

Constants:

- 86,400 ticks per day
- 604,800 ticks per epoch (planning upper bound; the current network epoch is roughly
  330k ticks, so scale per-epoch totals by about 0.55)
- `TickData` 139,376 B
- Tick header 352 B
- Max transactions 4,096 per tick
- Max input 1,024 B
- Max packet 16,777,215 B including the header

| Item | Raw size | Per epoch | Notes |
| --- | --- | --- | --- |
| Winning tick header | 352 B | 203 MiB | Indexed |
| Full vote set, 676 headers | 237,952 B per tick | 134 GiB | Not stored |
| Quorum evidence row | about 110 B plus warnings per tick | about 63 MiB | Indexed when provided |
| `TickData` | 139,376 B | 78.5 GiB | Not stored |
| Transaction, average 128 B input | 272 B | Depends on count | Indexed |
| Transaction, worst case | 1,168 B | Depends on count | Max input |
| Max transaction payload per tick | 4,784,128 B | 2.63 TiB | Absolute worst case |
| Event, 26 B header plus 300 B body | 326 B | Depends on count | Indexed |

Indexed store planning, assuming 128 B average input and 300 B average event body, before
compression:

| Transactions per tick | Transactions per epoch | Transaction bytes per epoch | Events per tick | Event bytes per epoch | Total indexed per epoch |
| --- | --- | --- | --- | --- | --- |
| 50 | 30.2 M | 8.2 GB | 50 | 9.9 M | About 18 GB |
| 250 | 151.2 M | 41.1 GB | 250 | 49.3 M | About 91 GB |
| 1,000 | 604.8 M | 164.5 GB | 1,000 | 197.2 M | About 363 GB |

ZSTD on hex bodies and repetitive identity strings typically yields 3x to 10x. Plan for the
uncompressed number for ingest capacity and the compressed number for disk.

Network per tick at 250 transactions with 128 B inputs:

- `TickData`: 136 KiB
- 451 vote headers: about 155 KiB
- Transactions: about 68 KiB
- Logs: variable, plan 1 to 2 KiB per event

That is roughly 0.4 MB per tick, or 0.4 MB/s at one tick per second. The worst case with
every transaction slot full is about 4.8 MB per tick, or 4.8 MB/s. Provision at least
100 Mbit/s and two independent peers.

ClickHouse development limits in [`deploy/clickhouse/config.xml`](../deploy/clickhouse/config.xml):
512 MiB mark cache, 4 GiB server memory ceiling, 2 GiB per-query memory limit. Production
needs separate tuning. The indexer insert batch should stay below 100k rows per statement
to keep memory bounded.

## Replay

There are three replay paths.

### 1. Retry after a crash

- Read `head.epoch`, `head.tick`, and `head.version`.
- Start at `head.tick + 1`. Do not re-fetch the committed head unless the digest check
  fails.
- Any rows written by the failed attempt but not checkpointed are replaced by the retry
  with the same `version` for an unchanged tick, or a higher one for a changed tick. This
  is safe because the writes are idempotent.

### 2. Rebuild a partition from peers

There is no automated archive-to-ClickHouse replay command, so re-fetching from peers is
the rebuild path today. The raw archive itself is implemented: when enabled it stores one
QBB1 bundle per verified tick before the ClickHouse write, and
`GET /v1/ticks/{tick}/raw?section=` serves those bundles on demand. Keeping the archive
lets an operator copy evidence out before it leaves peer retention; it does not yet
re-ingest automatically.

1. Stop the indexer.
2. Drop the affected partitions from `ticks`, `transactions`, `events`, and `identities`.
3. Set `ingest` to the last tick before the dropped range, or clear it to start at the
   current epoch `initialTick`.
4. Start the indexer. It re-fetches and re-verifies the range from peers, subject to their
   retention window.
5. Identities are rebuilt from the events seen during the re-fetch.

### 3. Recover from a lost checkpoint

1. Query the maximum `(epoch, tick)` per table.
2. Choose the minimum common tick across ticks, transactions, and events. This is the last
   tick that is complete in all tables.
3. Set `head.*` to that tick.
4. Re-fetch from peers if the tick is still within their retention window. An archive replay
   path does not exist yet.

Replay never goes below the epoch present at first start. A request to replay a lower epoch returns a configuration
error. The data model has no rows before that epoch and no compatibility path for older
layouts.

## Migrations

- DDL lives in `crates/store/src/schema.rs` for canonical chain tables and in
  `crates/store/src/gateway.rs` for operational gateway tables (mempool, webhooks,
  webhook deliveries). All statements are idempotent
  (`CREATE ... IF NOT EXISTS`, `ADD PROJECTION IF NOT EXISTS`).
- `glyph db-init` and `Store::init` apply the full statement list from both modules.
  Applying it twice is a no-op.
- Additive changes (new column with a default, new projection, new table) are safe on a live
  database. Destructive changes require a new table and a backfill. They are never applied
  implicitly.
- Projections are built asynchronously by ClickHouse. Query performance may lag until the
  projection is materialized. Use `SYSTEM WAIT PROJECTIONS` in tests when timing matters.
- Schema changes that affect row semantics, such as changing the meaning of `status`, bump
  `schema.version` and require a reindex of affected partitions.
