# API

Glyph API exposes an HTTP API over the local ClickHouse index. Read routes never
call a hosted RPC. Peer-backed operations live under `/v1/live` and connect
directly to a configured Qubic peer per request. Transaction broadcast is
disabled unless `ALLOW_BROADCAST=true`. Signed transactions are queued in the
API mempool by default, or fanned out immediately with `direct: true`. If a
resource is not indexed, the API returns a problem document instead of proxying
upstream.

- Base URL in development: `http://127.0.0.1:8080`
- Indexed data routes are under `/v1`. Peer-backed routes are under `/v1/live`.
  `/healthz`, `/openapi.json`, and `/docs` are unversioned.
- The canonical market route is `/v1/market`. Aliases are not published.
- Requests and responses are UTF-8 JSON. Errors use `application/problem+json`.
- The server is axum with gzip and zstd response compression, CORS (permissive
  by default or restricted with `CORS_ALLOWED_ORIGINS`), an `x-request-id`
  header, optional `RATE_LIMIT_PER_MINUTE` throttling, and request tracing.
- The OpenAPI 3.1 document is at `GET /openapi.json`. An interactive Scalar
  reference with grouped sections and generated request snippets is at
  `GET /docs`.

Route families:

| Prefix | Reads from | Cacheable |
| --- | --- | --- |
| `/v1/status`, `/v1/network` | Index and sampled peer state | No |
| `/v1/epochs`, `/v1/ticks`, `/v1/transactions`, `/v1/events` | ClickHouse index | Point reads yes, lists briefly |
| `/v1/identities`, `/v1/wallets`, `/v1/assets` | ClickHouse index plus peer joins | Briefly |
| `/v1/prices`, `/v1/market`, `/v1/analytics` | Stored prices and aggregates | Briefly |
| `/v1/stream`, `/v1/ws` | Local index head | No |
| `/v1/mempool` | Local mempool state plus index status | No |
| `/v1/webhooks` | Local webhook registrations and deliveries | No |
| `/v1/live` | Direct peer call | At most 2 s, some no-store |

This page documents the implemented surface. Planned endpoints and tiers are in
[ROADMAP.md](./ROADMAP.md) and are not part of the current contract.

### Migration from 0.1

The 0.2 spec moves direct-peer reads under `/v1/live`, removes duplicate market
aliases, and standardizes operation IDs. Regenerate generated clients.

| Old path | New path |
| --- | --- |
| `/v1/network/stats` | `/v1/network` |
| `/v1/identities/{identity}/entity` | `/v1/live/identities/{identity}` |
| `/v1/transactions/{hash}/network` | `/v1/live/transactions/{hash}` |
| `/v1/transactions/{hash}/network/status` | `/v1/live/transactions/{hash}/status` |
| `/v1/ticks/{tick}/tx-status` | `/v1/live/ticks/{tick}/transactions` |
| `/v1/contracts/{contract_index}/query` | `/v1/live/contracts/{contract_index}/query` |
| `/v1/wallets/{identity}/snapshot` | `/v1/live/wallets/{identity}` |
| `/v1/market/qubic` | `/v1/market` |
| `/v1/prices/market` | `/v1/market` |

## Response envelope

Single resources use a `data` object:

```json
{
  "data": {
    "epoch": 240
  }
}
```

Collections use `data` plus `meta`:

```json
{
  "data": [],
  "meta": {
    "limit": 50,
    "offset": 0,
    "count": 0,
    "next_offset": null,
    "next_cursor": null,
    "has_more": false
  }
}
```

`/v1/status` and `/v1/identities/{identity}` add their own `meta` fields,
documented below. Clients must ignore unknown fields.

## Field conventions

- JSON field names are `snake_case`, matching the response structs. They are
  not camelCase.
- `amount`, `balance`, `log_digest`, and other 64-bit values are JSON numbers.
  Use a 64-bit-safe JSON parser; values can exceed `2^53 - 1`.
- Timestamps are RFC 3339 UTC strings, for example
  `2026-11-18T14:50:33.043Z`.
- Identities are uppercase 60-character base-26 strings. Transaction hashes are
  lowercase 60-character base-26 strings.
- `verification` is one of `quorum`, `data`, or `state`. `state` is defined in
  the enum and reserved; the current indexer emits `quorum` or `data`.
- Transaction `input_data` is a lowercase hex string without a `0x` prefix.
  Event `body` is a JSON object. It contains the decoded body when a typed
  decoder applies, or a raw representation otherwise. Body keys are
  `snake_case` and `amount`/`shares` are JSON integers.
- `tx_index` is the transaction slot. Special events use `-1`. `tx_hash` is
  empty or a synthetic `special:` marker for special events.

## Pagination

Every collection uses opaque cursor pagination.

| Parameter | Default | Bounds |
| --- | --- | --- |
| `limit` | `50` | `1..200` |
| `cursor` | none | Opaque token from `meta.next_cursor`. |
| `offset` | `0` | Legacy, deprecated. Ignored when `cursor` is present. |

The response `meta` contains:

| Field | Meaning |
| --- | --- |
| `limit` | Effective limit after clamping. |
| `offset` | Effective offset used for this page. |
| `count` | Rows returned in this page. |
| `next_offset` | Legacy next offset when `has_more`, otherwise `null`. |
| `next_cursor` | Cursor for the next page when `has_more`, otherwise `null`. |
| `has_more` | Whether another page exists. |

A cursor is bound to the route and its normalized filters. Reusing a cursor
with a different filter set, or sending a malformed token, returns
`400 invalid_cursor`. Clients must treat cursors as opaque and follow
`next_cursor` until it is `null`.

Collections are ordered newest first unless the endpoint says otherwise. Tick
and transaction point reads are immutable and cacheable; list feeds are
short-lived.

## Endpoints

### `GET /healthz`

Liveness only. Does not touch ClickHouse.

```json
{ "ok": true }
```

### `GET /status`

Server-rendered operations dashboard, intended for humans and for uptime checks
that prefer HTML over JSON. It shows API version, process uptime, indexed head,
data lag, counts, ingest cursor, peer count, and whether streaming, broadcast,
and the raw archive are enabled. The page refreshes every five seconds and does
not require JavaScript or external assets. Storage failures are rendered on the
page while `/healthz` and the JSON API keep their normal status codes.

### `GET /v1/status`

Indexed head, live ingest and peer progress, current-epoch backfill coverage,
and counts. This response is never cached.

```json
{
  "data": {
    "head": {
      "epoch": 240,
      "tick": 91916480,
      "timestamp": "2026-11-18T15:10:00.000Z",
      "computor_index": 7,
      "transaction_count": 7,
      "event_count": 16,
      "verification": "data"
    },
    "ingest": {
      "last_indexed_tick": 91916480,
      "epoch": 240,
      "verification": "data",
      "updated_at": "2026-11-18T15:10:03.000Z"
    },
    "ingest_status": {
      "last_indexed_tick": 91916480,
      "indexed_ticks": 6481,
      "ticks_per_second": 34.2,
      "lag_ticks": 3,
      "eta_seconds": 0
    },
    "network": {
      "reachable": true,
      "head": 91916483,
      "epoch": 240,
      "initial_tick": 91910000,
      "tick_duration": 0,
      "ticks_per_second": 0.7,
      "error": null
    },
    "backfill": {
      "available": true,
      "complete": true,
      "initial_tick": 91910000,
      "first_indexed_tick": 91910000,
      "last_indexed_tick": 91916480,
      "missing_before": 0,
      "indexed_ticks": 6481,
      "epoch_ticks": 6484,
      "remaining_ticks": 3,
      "coverage": 0.9995373238741517,
      "coverage_percent": 99.95
    },
    "stats": {
      "head": { "...": "..." },
      "counts": {
        "ticks": 6481,
        "transactions": 45120,
        "events": 103696,
        "identities": 3210
      },
      "cursor": {
        "epoch": 240,
        "tick": 91916480,
        "timestamp": "2026-11-18T15:10:00.000Z"
      },
      "ingest": { "...": "..." }
    }
  },
  "meta": {
    "source": "direct-qubic-peers"
  }
}
```

`head` is `null` before the first tick is written. `data.ingest` is the typed
ingest cursor. `data.stats` contains `head`, `counts`, `cursor`, and `ingest`.

`ingest_status.ticks_per_second` is measured between status requests at least
one second apart, so the first request after startup returns `null`. A
zero-growth interval returns `0.0`. `lag_ticks` is the peer head minus the last
indexed tick, and `eta_seconds` divides that lag by the ingest rate.

`network` comes from a peer head sampled with a two-second timeout. `head`,
`epoch`, `initial_tick`, and `tick_duration` are peer values. `ticks_per_second`
is the observed peer head advance since the previous status request, which
varies with network conditions. When the peer cannot be reached, `reachable` is
`false`, `error` explains why, and the other network values are `null`.

`backfill` describes current-epoch coverage. `complete` is true when the first
indexed tick is at or before `initial_tick`. `indexed_ticks` counts only ticks
inside the current epoch (bounded by the peer head), `epoch_ticks` counts ticks
elapsed so far, and `coverage` is their ratio. When the peer is unavailable,
`available` is `false` and a `reason` field is included.

### `GET /v1/network`

Typed index-wide counts and progress. This is the stable shape for dashboards
and does not require parsing the free-form `stats` object in `/v1/status`.

```json
{
  "data": {
    "head": { "epoch": 240, "tick": 91916480, "verification": "data" },
    "epochs": 1,
    "ticks": 6481,
    "transactions": 45120,
    "events": 103696,
    "identities": 3210,
    "latest_timestamp": "2026-11-18T15:10:00.000Z",
    "ingest": { "last_indexed_tick": 91916480, "epoch": 240 }
  },
  "meta": { "source": "direct-qubic-peers" }
}
```

### `GET /v1/epochs`

Epoch summaries, newest first. Uses cursor pagination. `initial_tick` is the
observed first tick of the epoch recorded in `glyph.epochs`; `first_tick` and
`last_tick` are the indexed range. When the `epochs` table has no row for an
epoch, the API falls back to values derived from `glyph.ticks`, so databases
created before the epoch table still work.

```json
{
  "data": [
    {
      "epoch": 240,
      "initial_tick": 91880000,
      "first_tick": 91880000,
      "last_tick": 91916480,
      "first_timestamp": "2026-11-18T02:00:00.000Z",
      "last_timestamp": "2026-11-18T15:10:00.000Z",
      "tick_count": 6481,
      "transaction_count": 45120,
      "event_count": 103696,
      "closed": false
    }
  ],
  "meta": {
    "limit": 50,
    "offset": 0,
    "count": 1,
    "next_offset": null,
    "next_cursor": null,
    "has_more": false
  }
}
```

`closed` comes from the persisted `glyph.epochs` row when one exists. Without
one, it falls back to true when a later epoch is already indexed. The newest
epoch can still receive ticks.

#### Epoch lifecycle and retention

Epoch boundaries are observed from peer `Head` values, not assumed from a fixed
tick count. The network has used different epoch lengths, so Glyph never
derives a boundary from 604,800 ticks.

- On startup and on every head refresh the indexer upserts `(epoch,
  initial_tick)` into the `glyph.epochs` table.
- On an epoch change the previous epoch is closed at `new_initial_tick - 1`.
  `closed` is therefore a persisted fact, not a guess from the newest indexed
  tick.
- Tick-to-epoch assignment uses the persisted boundary map. A tick whose epoch
  cannot be resolved is skipped with a warning instead of being guessed.
- Previous-epoch catch-up runs only when that epoch's boundary and computor set
  are known.
- All chain tables remain in one `glyph` ClickHouse database and are partitioned
  by `epoch`. Per-epoch databases are deprecated: they duplicate DDL, break
  cross-epoch queries (identities, price points, search), and make checkpoint
  handling ambiguous. One database with epoch partitions gives the same cheap
  partition drop without splitting the schema.

Retention is optional and explicit:

- `PRUNE_KEEP_EPOCHS`, when set, drops ClickHouse partitions older than the
  newest N epochs after a rollover.
- `glyph prune --keep N [--dry-run]` performs the same operation on demand.
- Pruning drops chain partitions only. It does not delete raw archive objects,
  because archive deletion is irreversible. It never deletes `identities` (which
  span epochs) or `price_points` (which are keyed by hour).
- Run `glyph prune --keep N --dry-run` first and confirm the reported epochs.

### `GET /v1/epochs/{epoch}`

One epoch summary. An epoch with no indexed ticks returns `404 not_found`.

### `GET /v1/epochs/{epoch}/computors`

The elected computor set for one epoch, ordered by `computor_index`. The list
has one row per computor and is not paginated. `public_key` is the 0x-prefixed
hex form of the 32-byte key; `identity` is its canonical uppercase base-26
form.

```json
{
  "data": [
    {
      "epoch": 240,
      "computor_index": 0,
      "public_key": "0x0100000000000000000000000000000000000000000000000000000000000000",
      "identity": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK"
    }
  ],
  "meta": { "epoch": 240, "count": 676 }
}
```

An epoch with no stored computor set returns `404 not_found`.

### `GET /v1/ticks`

Ticks newest first, optionally filtered by `epoch`. Uses cursor pagination.

| Query | Meaning |
| --- | --- |
| `epoch` | Restrict to one epoch. |
| `limit` | Page size. |
| `cursor` | Cursor from `meta.next_cursor`. |

### `GET /v1/ticks/{tick}`

One tick by number.

```json
{
  "data": {
    "epoch": 240,
    "tick": 91916480,
    "timestamp": "2026-11-18T15:10:00.000Z",
    "computor_index": 7,
    "transaction_count": 7,
    "event_count": 16,
    "verification": "data"
  }
}
```

- Success responses are cacheable with
  `Cache-Control: public, max-age=31536000, immutable`.
- An unindexed tick is `404 not_found`.
- The tick number is globally unique; the response does not require an epoch.

### `GET /v1/ticks/{tick}/evidence`

The quorum evidence recorded when a tick was verified. `quorum_digest` is the
hex K12 digest the winning vote group agreed on, and `tick_data_digest` is the
hex digest of the persisted tick data. Vote counts describe how the quorum was
assembled. `warnings` is the producer's serialized warning list and
`warning_count` is its length.

```json
{
  "data": {
    "epoch": 240,
    "tick": 91916480,
    "quorum_digest": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    "tick_data_digest": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    "required_votes": 451,
    "aligned_votes": 500,
    "fetched_votes": 600,
    "unique_votes": 550,
    "verified_votes": 480,
    "verification": "data",
    "warning_count": 1,
    "warnings": "[\"lagging peer\"]",
    "timestamp": "2026-11-18T15:10:00.000Z"
  }
}
```

A tick or evidence row that is not indexed returns `404 not_found`.

### `GET /v1/ticks/{tick}/transactions`

Transactions in slot order.

| Query | Default | Bounds |
| --- | --- | --- |
| `limit` | `50` | `1..200` |
| `offset` | `0` | `>= 0` |

```json
{
  "data": [
    {
      "epoch": 240,
      "tick": 91916480,
      "tx_index": 3,
      "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "source": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
      "destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      "amount": 1000000,
      "input_type": 0,
      "input_data": "",
      "status": "success",
      "timestamp": "2026-11-18T15:10:00.000Z"
    }
  ],
  "meta": {
    "limit": 50,
    "offset": 0,
    "count": 1,
    "next_offset": null,
    "has_more": false
  }
}
```

An unknown tick returns `404 not_found`, consistent with the single-tick route.

`status` is `success` by default. A transaction whose slot emitted a
`contract_error_message` event is marked `failed`.

### `GET /v1/ticks/{tick}/events`

Events in log id order.

| Query | Default | Bounds |
| --- | --- | --- |
| `limit` | `50` | `1..200` |
| `offset` | `0` | `>= 0` |

```json
{
  "data": [
    {
      "epoch": 240,
      "tick": 91916480,
      "log_id": 1234567,
      "log_type": 0,
      "type_name": "qu_transfer",
      "tx_hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "tx_index": 3,
      "body": {
        "from": "BZBQ...",
        "to": "AAAA...",
        "amount": "1000000"
      },
      "log_digest": 123456789,
      "timestamp": "2026-11-18T15:10:00.000Z"
    }
  ],
  "meta": {
    "limit": 50,
    "offset": 0,
    "count": 1,
    "next_offset": null,
    "has_more": false
  }
}
```

`body` is a JSON object, not a string. `type_name` comes from the log decoder,
for example `qu_transfer`, `asset_issuance`, `burning`,
`contract_error_message`, or `custom_message`. Unknown types are stored with a
raw representation.

An unknown tick returns `404 not_found`.

### `GET /v1/transactions`

Transactions across indexed ticks, newest tick first. Uses cursor pagination
and the same `meta` shape.

| Query | Meaning |
| --- | --- |
| `epoch` | Restrict to one epoch. |
| `tick` | Restrict to one tick. |
| `identity` | Match either source or destination (case-insensitive). |
| `limit`, `cursor` | Pagination. |

```json
{
  "data": [
    {
      "epoch": 240,
      "tick": 91916480,
      "tx_index": 3,
      "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "source": "BZBQ...EXK",
      "destination": "AAAA...AAA",
      "amount": 1000000,
      "input_type": 0,
      "input_data": "",
      "status": "success",
      "timestamp": "2026-11-18T15:10:00.000Z"
    }
  ],
  "meta": {
    "limit": 50,
    "offset": 0,
    "count": 1,
    "next_offset": null,
    "has_more": false
  }
}
```

Recent feeds return `200` even when the index is empty.

### `GET /v1/events`

Typed events across indexed ticks, newest tick first. Uses cursor pagination
and the same `meta` shape. Event `body` is a JSON object, as on the per-tick
events route.

| Query | Meaning |
| --- | --- |
| `epoch` | Restrict to one epoch. |
| `tick` | Restrict to one tick. |
| `type` | Match `type_name`, for example `qu_transfer`. |
| `limit`, `cursor` | Pagination. |

Recent feeds return `200` even when the index is empty.

### `GET /v1/identities/{identity}/transactions`

Transactions where the identity is the source or destination, newest tick
first. Uses cursor pagination and the same `meta` shape.
A malformed identity returns `400 invalid_identity`. A valid identity with no
history returns an empty list.

### `GET /v1/identities/{identity}/activity`

Linear history that merges direct transactions with contract-originated effects.
A contract payout or asset movement is an event, not a transaction submitted by
the identity, so `/transactions` alone hides it. `activity` joins the normalized
event participant index and returns both kinds in one timeline:

- `kind: "transaction"` for transactions where the identity is source or
  destination.
- `kind: "event"` for decoded events where the identity is a participant
  (`from`, `to`, `issuer`, `owner`, `possessor`, `burner`, `querier`, or
  `solver`).

Rows are ordered by tick, log id, and transaction slot, newest first. Event rows
carry `type_name`, `body`, `role`, `log_id`, and the originating `tx_hash`.
Fields that do not apply to a row are omitted rather than returned as `null` or
`""`. A transaction row has no `log_id`, `type_name`, `body`, or `role`. An
event row has no transaction `status` or `input_data`, and `source`,
`destination`, and `input_type` appear only when that row has them.

### `GET /v1/identities/{identity}/events`

The event-only projection of the same participant index, newest first. Use this
when a client wants contract effects without direct transaction rows.

### `GET /v1/identities/{identity}/counterparties`

Top counterparties derived from the local transaction index. Self-transfers are
excluded so a row always names a distinct identity.

| Query | Meaning |
| --- | --- |
| `direction` | `all` (default), `incoming`, or `outgoing`. |
| `limit` | Maximum rows, default 20, maximum 100. |

Rows are ranked by total incoming plus outgoing amount, then transaction count,
then identity. `net_amount` is incoming minus outgoing. Amounts stay in the
network's base unit.

```json
{
  "data": [
    {
      "identity": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
      "incoming_amount": 120,
      "outgoing_amount": 80,
      "net_amount": 40,
      "transaction_count": 7,
      "first_tick": 91910000,
      "last_tick": 91933164
    }
  ],
  "meta": { "identity": "BZBQ...", "direction": "all", "limit": 20, "count": 1 }
}
```

### `GET /v1/identities/{identity}/flow`

Incoming and outgoing QU flow in fixed buckets ending at the indexed head.
Empty buckets are zero-filled so a chart can render the series directly.

| Query | Meaning |
| --- | --- |
| `interval` | Bucket width in seconds, default 3600, range 60 to 86400. |
| `limit` | Bucket count, default 168, maximum 1000. Newest first. |

`meta.totals` sums the requested window. `counterparty_count` is the number of
distinct counterparties inside one bucket.

### `GET /v1/identities`

Indexed identity summaries ordered by most recent activity, then canonical
identity. Uses cursor pagination.

```json
{
  "data": [
    {
      "identity": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
      "public_key": "0x0000000000000000000000000000000000000000000000000000000000000000",
      "balance": 42000000,
      "incoming_amount": 90000000,
      "outgoing_amount": 48000000,
      "transfer_count": 37,
      "first_seen_tick": 91800000,
      "last_seen_tick": 91916480
    }
  ],
  "meta": { "limit": 50, "offset": 0, "count": 1, "next_cursor": null, "has_more": false }
}
```

### `GET /v1/assets`

Assets derived from indexed `asset_issuance` events, ordered by issuer and
name. The last issuance for each `(issuer, name)` pair supplies the metadata.
Uses cursor pagination. `issuer` optionally filters by canonical identity.

```json
{
  "data": [
    {
      "issuer": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
      "name": "QX",
      "decimals": 0,
      "unit": "",
      "managing_contract": 1,
      "shares": 1000,
      "issued_tick": 91916480,
      "log_id": 1234567,
      "timestamp": "2026-11-18T15:10:00.000Z"
    }
  ],
  "meta": { "limit": 50, "offset": 0, "count": 1, "next_cursor": null, "has_more": false }
}
```

### `GET /v1/assets/{issuer}/{name}`

One asset by canonical issuer and name. A malformed issuer returns
`400 invalid_identity`; an unindexed asset returns `404 not_found`. The asset
tables are planned; this read currently aggregates issuance events.

### `GET /v1/assets/{issuer}/{name}/transfers`

Local ownership and possession transfer history for one asset, newest first.
This route reads decoded `asset_ownership_change` and `asset_possession_change`
events and works without a peer.

| Query | Meaning |
| --- | --- |
| `kind` | `all` (default), `ownership`, or `possession`. |
| `limit`, `cursor` | Cursor pagination. |

Rows carry `kind`, `hash`, `tx_index`, `from`, `to`, `shares`, and
`managing_contract`. `/holders` remains the peer snapshot of current ownership;
`/transfers` is the indexed history that produced it.

### `GET /v1/transactions/{hash}`

One transaction by lowercase base-26 hash.

```json
{
  "data": {
    "epoch": 240,
    "tick": 91916480,
    "tx_index": 3,
    "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
    "source": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
    "destination": "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    "amount": 1000000,
    "input_type": 0,
    "input_data": "",
    "status": "success",
    "timestamp": "2026-11-18T15:10:00.000Z"
  }
}
```

- Input is lowercased before validation.
- A malformed hash returns `400 invalid_transaction_hash`.
- An unindexed hash returns `404 not_found`.
- Success responses are cacheable with
  `Cache-Control: public, max-age=31536000, immutable`.

### `GET /v1/transactions/{hash}/effects`

One request for an explorer transaction page. It returns the indexed
transaction, its tick header, every decoded event, a typed effect list, and an
aggregate summary. It never opens a peer connection.

The effect list covers `qu_transfer`, `asset_ownership_change`,
`asset_possession_change`, `burning`, contract message types, and
`custom_message`. The summary counts each kind and sums QU transfer amounts.
The response is immutable and cached for one year.

### `GET /v1/identities/{identity}`

Identity summary derived from indexed QU transfer events.

```json
{
  "data": {
    "identity": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK",
    "public_key": "0x0000000000000000000000000000000000000000000000000000000000000000",
    "balance": 42000000,
    "incoming_amount": 90000000,
    "outgoing_amount": 48000000,
    "transfer_count": 37,
    "first_seen_tick": 91800000,
    "last_seen_tick": 91916480
  },
  "meta": {
    "notice": "balance is the net QU flow observed in the indexed epoch range"
  }
}
```

- Input is uppercased before validation.
- A malformed identity returns `400 invalid_identity`.
- An unindexed identity returns `404 not_found`.
- `balance` is the net QU flow over the indexed epoch range. It is not a
  spectrum snapshot and does not include balances from unindexed history.
- `first_seen_tick` and `last_seen_tick` are bounded by the indexed range.

### `GET /v1/search?q=`

Resolves a value against the index. `q` is required and trimmed. Results are
accumulated rather than stopping at the first match:

1. All ASCII digits: try both a tick and an epoch.
2. Lowercase value that decodes as a transaction hash: return the transaction.
3. Uppercase value that decodes as an identity: return the identity.
4. `{issuer}/{name}`: return the asset when the issuer and name are valid.
5. Otherwise return an empty list.

```json
{
  "data": [
    { "type": "tick", "tick": 91916480, "value": { "...": "..." } }
  ]
}
```

`type` is `tick`, `epoch`, `transaction`, `identity`, or `asset`. A missing or
empty `q` returns `400 bad_request`.

### `POST /v1/transactions/batch`

Looks up up to 100 unique transaction hashes in one request. Duplicate hashes
are collapsed. Unknown hashes do not fail the request; they are listed in
`meta.missing`.

```json
{
  "hashes": [
    "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh"
  ]
}
```

```json
{
  "data": [ { "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh", "...": "..." } ],
  "meta": {
    "requested": 1,
    "found": 1,
    "missing": []
  }
}
```

More than 100 unique hashes, or a malformed hash, returns `400 bad_request`.

### Analytics

Analytics routes read pre-aggregated projections in ClickHouse. They are
eventually consistent with the index head and are cached for 30 seconds.

| Route | Meaning |
| --- | --- |
| `GET /v1/analytics/activity?interval=3600&limit=24` | Transaction, event, and tick counts per `interval`-second bucket, newest first. `interval` is clamped to `1..86400`; `limit` is clamped to `1..500`. |
| `GET /v1/analytics/top-identities?limit=25` | Identities ranked by observed transfer count, then amount. `limit` is clamped to `1..200`. |
| `GET /v1/analytics/event-types?limit=50` | Decoded event type counts, highest first. |

Each returns `data` plus a `meta` object describing the effective `interval` and
`limit`. Analytics routes do not use cursors.

### Streaming

Streaming follows the local index head. It never opens a peer connection, so a
stream stays available during peer outages and only emits ticks that have been
verified and persisted.

| Route | Meaning |
| --- | --- |
| `GET /v1/stream` | Server-sent events for all event kinds. |
| `GET /v1/stream/ticks` | Tick events only. |
| `GET /v1/stream/transactions` | Transaction events only. |
| `GET /v1/stream/events` | Log events only. |
| `GET /v1/stream/prices` | Price-point updates only. |
| `GET /v1/stream/market` | Market-snapshot updates only. |
| `GET /v1/ws` | WebSocket carrying the same JSON events. |

`GET /v1/stream` accepts `?types=tick,event` to select a subset. Event names are
`tick`, `transaction`, `event`, `price`, and `market`; each SSE frame has an `id`
and a JSON `data` payload. The type-specific routes always filter to their own
event regardless of `types`. Add `identity=<identity>` to receive only
transactions and events that mention one identity, and
`asset=<issuer>/<name>` to receive only events for one asset. A filtered stream
suppresses tick and price events. Both filters are applied per subscriber, so
one broadcast still serves every client.

The server sends a keep-alive comment every 15 seconds. If a client falls behind
the internal 4096-event buffer, it receives a `lag` event with the number of
dropped events and the stream continues from the current head. When the hub
itself is catching up after an index jump, it emits a `catchup` event with
`last_published_tick`, `head_tick`, and `pending` before continuing. A failed
store read does not advance the stream cursor, so the tick is retried instead of
being skipped.

The polling cadence is controlled by `STREAM_POLL_MS`. A hub that is not running
returns `503 unavailable`.

### Live routes (`/v1/live`)

These routes connect directly to a configured peer for each request and are
separate from the indexed read path. They may carry a short two-second cache.
Configure peers with `QUBIC_PEERS` (comma-separated `host:port`) or
`QUBIC_PEER`.

| Route | Meaning |
| --- | --- |
| `GET /v1/live/identities/{identity}` | Entity record (balance, incoming/outgoing amount, number of transfers) as reported by a peer. |
| `GET /v1/live/transactions/{hash}` | Peer view of a transaction, including whether the peer knows it. |
| `GET /v1/live/transactions/{hash}/status` | Peer confirmation state for one transaction, including the money-flew flag when the peer runs the tx-status add-on. |
| `GET /v1/wallets/{identity}` | Indexed identity and recent transactions joined with the peer spectrum entity and asset lists. |
| `GET /v1/wallets/{identity}/assets?kind=` | Issued, owned, and possessed assets for a wallet from the peer asset tree. |
| `GET /v1/live/wallets/{identity}` | Point-in-time peer entity, assets, counts, and head for a wallet. |
| `GET /v1/assets/{issuer}/{name}/holders?limit=` | Ownership holders for an asset, ranked by shares. |
| `GET /v1/live/ticks/{tick}/transactions` | Peer transaction confirmation status with money-flew flags. |
| `POST /v1/live/contracts/{contract_index}/query` | Read-only contract function call. Body is `{"input_type": 0, "input": "hex"}`. Output is hex. |

An unavailable peer returns `503 unavailable`. Invalid input returns
`400 bad_request`. Peer responses are not persisted or merged into the index.

### Prices

The price routes serve QUBIC/USDT. Hour `0` is `2022-04-13T12:00:00Z`; each point
carries `price_e8` (1e-8 USDT per QUBIC) and a rounded `price_usdt`. A background
poller reads the latest verified reply of an active `OI::Price` oracle
subscription (interface index 0) discovered through `RequestOracleData`. The
QPayhub `GetQuUsdPrice` read function (contract index 29, input type 4) is a
secondary source. The poller stores one point per hour in ClickHouse and
publishes a `price` stream event when a stored hour changes. It never substitutes
the mining price series.

When neither on-chain source has produced a point, the routes fall back to the
external exchange consensus so a chart still has data. Those rows use
`source: 1000`, `source_name: "external_market"`, and `onchain: false`. They omit
on-chain-only fields: `epoch`, `append_tick`, `source_tick`, `flags`, `filled`,
`synced`, `bound`, and `series_digest`. An external price has no oracle
observation tick, so the API does not fabricate one.

| Route | Meaning |
| --- | --- |
| `GET /v1/prices?from_hour=&limit=` | Ascending hourly points. Default limit 168, maximum 1000. |
| `GET /v1/prices/history?hours=24` | Convenience window ending at the latest stored hour. |
| `GET /v1/prices/latest` | Latest stored point, including `filled`, `stale`, `source`, `synced`, `bound`, and the reply digest. |
| `GET /v1/prices/snapshot` | Latest point plus total point count and source metadata. Returns `200` with `data: null`, `meta.available: false`, and a `meta.warning` before a usable on-chain price exists. |

The oracle reply is verified by the Qubic network; the poller selects the
QUBIC/USDT pair and scales `numerator / denominator` to 1e-8 units. `meta.pair`
is always `QUBIC/USDT`. `meta.source` is `0` for the Price oracle interface and
`29` for the QPayhub fallback; `meta.source_name` gives the matching name.
`meta.stale` is true when the source marks the value stale. `PRICE_POLL_MS`
controls the refresh cadence (default 60 seconds). `meta.onchain` is false for
the external fallback series.

Points use snake_case fields. All points carry `hour`, `timestamp`, `price_e8`,
`price_usdt`, `stale`, `source`, `source_name`, `onchain`, and `observed_at`.
On-chain points additionally carry `append_tick`, `source_tick`, `epoch`,
`flags`, `filled`, `synced`, `bound`, and `series_digest`. `append_tick` is the
reveal tick of the oracle reply or the QPayhub update tick; `source_tick` is the
query tick or update tick. `series_digest` is the K12 digest of the reply payload
that produced the point.

### Market data

The market routes expose a cached external QUBIC/USDT reference. MEXC and
Gate.io spot tickers provide the price consensus; CoinGecko supplies market cap,
24-hour volume, and 24-hour change. The poller runs every five seconds and
contacts one provider per tick in round robin, so each venue is queried every
fifteen seconds instead of all three at once. The API never calls an exchange
per request. Set `MARKET_ENABLED=false` to disable the poller.

| Route | Meaning |
| --- | --- |
| `GET /v1/market` | Current market snapshot with per-exchange quotes. |

These are reference values, not on-chain oracle points. `/v1/prices` uses
verified on-chain points when they exist and marks external fallback rows with
`onchain: false`. A cold process returns `503 unavailable` until the first poll
succeeds; a failed refresh keeps the previous snapshot and reports `stale` from
its age.

### Wallets and snapshots

Wallet routes combine the local index with a direct peer snapshot. They are
read-only and never sign or submit anything.

| Route | Meaning |
| --- | --- |
| `GET /v1/wallets/{identity}` | Peer spectrum entity plus issued, owned, and possessed assets, plus the last 10 indexed transactions. |
| `GET /v1/wallets/{identity}/assets?kind=issued\|owned\|possessed` | Typed asset lists from one peer snapshot. |
| `GET /v1/live/wallets/{identity}` | Point-in-time entity and asset snapshot with peer tick and epoch. |
| `GET /v1/assets/{issuer}/{name}/holders?limit=100` | Ownership snapshot for one asset, ranked by shares and deduplicated by identity. |

The wallet overview returns `404` only when the identity is absent from both the
index and the peer spectrum. A peer that does not expose a spectrum entry
returns the indexed view with a warning. Snapshot and holder responses use
`Cache-Control: no-store` because the peer head moves.

### Contract queries

`POST /v1/live/contracts/{contract_index}/query` is deliberately generic:

```json
{ "input_type": 2, "input": "0x..." }
```

It sends the exact function selector and input bytes to a direct peer, enforces
`MAX_INPUT_SIZE`, and returns both the hex output and byte count. The API does
not hardcode a contract ABI or contract list, because deployed contracts and
function tables change over time. Use the contract's own published interface to
build `input_type` and `input`.

### `GET /v1/live/transactions/{hash}/status`

Confirms one transaction through a direct peer and reports whether it moved
value or assets.

```json
{
  "data": {
    "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
    "tick": 91916480,
    "source": "BZBQ...EXK",
    "destination": "AAAA...AAA",
    "amount": 1000000,
    "input_type": 0,
    "input_data": "",
    "confirmed": true,
    "money_flew": true,
    "execution": "transferred",
    "current_tick": 91916490,
    "confirmations": 10
  },
  "meta": { "source": "peer", "status_source": "tx-status" }
}
```

`execution` is `transferred` when `money_flew` is `true`, `no_effect` when it is
`false`, and `unknown` when the peer does not expose the tx-status add-on. In
the `unknown` case the route still returns `200`, with `money_flew`, `current_tick`,
and `confirmations` set to `null` and `meta.status_source` set to `unavailable`.

- A malformed hash returns `400 invalid_transaction_hash`.
- A hash the peer does not retain as a confirmed transaction returns
  `404 not_found`; it may be unknown or still pending.
- No reachable peer returns `503 unavailable`.

### `GET /v1/wallets/{identity}`

Joins the local index with one direct peer connection. The index supplies the
identity summary and its ten newest transactions; the peer supplies the live
spectrum entity and the issued, owned, and possessed asset lists.

```json
{
  "data": {
    "identity": "BZBQ...EXK",
    "public_key": "0x0000...0000",
    "entity": {
      "spectrum_index": 1204,
      "tick": 91916480,
      "public_key": "0x0000...0000",
      "balance": 42000000,
      "incoming_amount": 90000000,
      "outgoing_amount": 48000000,
      "incoming_transfers": 20,
      "outgoing_transfers": 17,
      "latest_incoming_tick": 91916000,
      "latest_outgoing_tick": 91915000
    },
    "assets": {
      "issued": [],
      "owned": [],
      "possessed": []
    },
    "recent": []
  },
  "meta": {
    "source": "peer+index",
    "peer_tick": 91916480,
    "warning": null
  }
}
```

- One peer connection serves the whole request.
- `meta.warning` is set when the peer has no spectrum entry; in that case the
  peer entity amounts are zero.
- A malformed identity returns `400 invalid_identity`.
- `404 not_found` is returned only when the identity is absent from both the
  index and the peer spectrum.
- No reachable peer returns `503 unavailable`. Responses are `no-store`.

### `GET /v1/wallets/{identity}/assets?kind=issued|owned|possessed`

Returns the peer asset lists for one wallet. `kind` defaults to `all` and may
be `issued`, `owned`, `possessed`, or `all`.

Issued entries carry `issuer`, `name`, `decimals`, `unit`, `universe_index`, and
`tick`. Owned entries add `owner`, `shares`, `managing_contract_index`,
`ownership_index`, and an `issuance` object. Possessed entries add `possessor`,
the ownership `owner` and `ownership` object, `managing_contract_index`, and
`possession_index`. Records the peer returns with an unexpected union kind are
skipped rather than failing the request.

A malformed identity or unknown `kind` returns `400`. No peer returns `503`.

### `GET /v1/live/wallets/{identity}`

A point-in-time peer snapshot for one wallet: identity, public key, peer tick,
epoch when the peer head is available, entity, all three asset lists, and
counts. `meta.source` is `peer`. This route never reads the index and is always
`Cache-Control: no-store`. A missing spectrum entry sets `meta.warning` and
returns a zeroed entity rather than failing.

### `GET /v1/assets/{issuer}/{name}/holders?limit=`

Queries the peer asset tree for ownership records of one `(issuer, name)` pair.
`name` is 1 to 7 uppercase ASCII letters or digits; input is uppercased first.
The name is encoded into the 64-bit asset name exactly like core: each
character stores `char - '0'` in a little-endian byte, zero-padded.

Holders are deduplicated by identity, summed, sorted by shares descending then
identity, and truncated to `limit` (default 100, clamped to `1..=1000`).

```json
{
  "data": [
    { "identity": "BZBQ...EXK", "shares": 1000 },
    { "identity": "AAAA...AAA", "shares": 250 }
  ],
  "meta": {
    "issuer": "BZBQ...EXK",
    "name": "QX",
    "count": 2,
    "limit": 100,
    "source": "peer",
    "truncated": false
  }
}
```

An empty list is a valid `200` snapshot. A malformed issuer or an invalid
asset name returns `400`. No reachable peer returns `503`.

### `POST /v1/transactions/validate`

Validates a signed transaction locally. The route never opens a peer
connection, never broadcasts, and does not require `ALLOW_BROADCAST`, so it is
safe for preflight checks in wallets and tooling.

```json
{ "raw": "0x..." }
```

`raw` is the full signed transaction as hex, with or without a `0x` prefix. The
API decodes it and returns a report:

```json
{
  "data": {
    "valid": false,
    "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
    "source": "BZBQ...EXK",
    "destination": "AAAA...AAA",
    "amount": 1000000,
    "tick": 91916480,
    "input_type": 0,
    "input_data": "",
    "input_bytes": 0,
    "total_bytes": 144,
    "checks": [
      { "name": "amount", "passed": true },
      { "name": "input_size", "passed": true },
      { "name": "encoding", "passed": true },
      { "name": "signature", "passed": false, "detail": "signature does not verify against the source public key" }
    ]
  },
  "meta": { "source": "local", "broadcast": false }
}
```

`valid` is `true` only when every check passes. Failed checks carry a `detail`
string; passing checks omit it. Malformed hex or bytes that do not decode as a
transaction return `400 bad_request`, because no report can be produced.
Responses are `Cache-Control: no-store`.

### `POST /v1/transactions`

Submits a signed transaction. Disabled unless `ALLOW_BROADCAST=true`. The
default path queues the transaction in the mempool and returns `202` with a
tracked entry. Send `"direct": true` to use the previous behavior: one
concurrent fan-out to direct peers, with the per-peer report.

```json
{
  "raw": "0x...",
  "dry_run": false,
  "peers": 3,
  "direct": false,
  "webhook_url": "https://example.com/glyph",
  "webhook_secret": "at-least-16-characters"
}
```

`raw` is the full signed transaction as hex, with or without a `0x` prefix.
`transaction`, `raw_transaction`, and `signed_transaction` are accepted aliases
for `raw`; `max_peers` is an alias for `peers`. The API decodes the transaction
and runs the same checks as `/v1/transactions/validate` before opening any
socket. A transaction that fails validation is rejected with `400` and does not
reach a peer.

`webhook_url` and `webhook_secret` apply to the mempool path. They register a
transaction-scoped webhook without a separate `POST /v1/webhooks` call. The
secret must be at least 16 characters.

The mempool path validates, persists, and performs the first broadcast attempt
before returning, so the response usually reports `broadcast` or `failed`, not
`queued`. The response contains the same `MempoolEntry` shape as
`GET /v1/mempool/{hash}`:

```json
{
  "data": {
    "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
    "raw": "00...",
    "source": "BZBQ...EXK",
    "destination": "AAAA...AAA",
    "amount": 1000000,
    "tick": 91916480,
    "peers": 3,
    "status": "broadcast",
    "attempts": 1,
    "broadcast_peers": 2,
    "broadcast_at": "2026-11-18T15:10:00.000Z",
    "confirmed_at": null,
    "confirmed_tick": null,
    "error": null,
    "created_at": "2026-11-18T15:10:00.000Z",
    "updated_at": "2026-11-18T15:10:00.050Z"
  },
  "meta": {
    "source": "mempool",
    "status": "broadcast",
    "broadcast": true,
    "broadcast_peers": 2,
    "confirmed": false,
    "links": {
      "status": "/v1/mempool/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "transaction": "/v1/transactions/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh"
    },
    "hint": "A broadcast status means at least one peer accepted the packet at the socket. Qubic does not acknowledge broadcasts; poll the status link or the indexed transaction."
  }
}
```

The direct path sends valid transactions with `dejavu=0` to up to `peers` peers
concurrently. The default and ceiling come from `BROADCAST_PEERS` (default `3`,
maximum `8`). Fan-out avoids a single slow peer dropping the transaction before
it propagates.

Set `"dry_run": true` to get the validation report, transaction summary, and
target peer count without broadcasting or queueing. A dry run returns `200` and
never opens a peer connection. It is also the easiest way for a wallet to show a
preview before asking the user to sign.

```json
{
  "data": {
    "sent": 2,
    "targeted": 3,
    "transaction": {
      "hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "source": "BZBQ...EXK",
      "destination": "AAAA...AAA",
      "amount": 1000000,
      "tick": 91916480,
      "input_type": 0,
      "input_data": "",
      "input_bytes": 0,
      "total_bytes": 144
    },
    "peers": [
      { "peer": "peer-a.example:21841", "sent": true, "latency_ms": 84, "error": null },
      { "peer": "peer-b.example:21841", "sent": true, "latency_ms": 121, "error": null },
      { "peer": "peer-c.example:21841", "sent": false, "latency_ms": 2000, "error": "connection timed out" }
    ],
    "warnings": [
      "transaction reached 2 of 3 peers; propagation may be slower"
    ]
  },
  "meta": {
    "source": "direct-qubic-peers",
    "broadcast": true,
    "confirmed": false,
    "links": {
      "transaction": "/v1/transactions/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "tick": "/v1/ticks/91916480"
    },
    "hint": "A sent peer accepted the packet at the socket. Qubic does not acknowledge broadcasts; poll the transaction link or stream new ticks to observe inclusion."
  }
}
```

A direct send returns `202 Accepted`. Qubic does not acknowledge broadcasts, so
`sent` means the packet reached the peer socket, not that the transaction will
be included. Use `meta.links.transaction`, `GET /v1/mempool/{hash}`,
`GET /v1/transactions/{hash}/status`, or the tick stream to observe inclusion.

- `403 forbidden` when `ALLOW_BROADCAST` is not `true`.
- `400 bad_request` for malformed hex, a transaction that fails local checks,
  or a `webhook_url` without a matching `webhook_secret`.
- `429 mempool_full` when `MEMPOOL_MAX_ENTRIES` is reached and the mempool path
  is used. A duplicate hash returns the existing entry with `202` instead.
- `503 unavailable` when no configured peer accepts a direct packet, or when
  the mempool service is disabled and the direct path is unavailable.

When `MEMPOOL_ENABLED=false`, or with `direct: true`, the request uses the
direct fan-out path instead.

### Mempool

The mempool is API-local operational state, not a Qubic protocol mempool. Qubic
peers do not expose a mempool. Each entry records a broadcast attempt so a
wallet can poll one resource instead of retrying blindly.

Status lifecycle:

| Status | Meaning |
| --- | --- |
| `queued` | Persisted before the first broadcast attempt. It is normally visible only to an in-flight request and to webhook deliveries. |
| `broadcast` | At least one peer accepted the packet at the socket in a recent attempt. Not a confirmation. |
| `confirmed` | The transaction hash is present in the local index. `confirmed_tick` and `confirmed_at` are set. |
| `expired` | The indexed head passed `tick + MEMPOOL_EXPIRY_TICKS` and the hash never appeared. Default expiry is 10 ticks. |
| `failed` | The most recent attempt reached no peer. Failed entries stay open and are retried until `MEMPOOL_MAX_ATTEMPTS`, then remain failed until they expire. |
| `cancelled` | An unconfirmed entry was cancelled through the API. A packet a peer already accepted is not recalled. |

A transaction's `tick` field is its target tick. Qubic has no broadcast
acknowledgement, so the tracker polls the index. Expiry uses the indexed head,
not wall time. Rebroadcasts are at least `MEMPOOL_RETRY_MS` apart and stop after
`MEMPOOL_MAX_ATTEMPTS`. `MEMPOOL_POLL_MS` controls how often statuses are
reconciled.

Every entry retains `raw`, the signed transaction hex, so a retry does not need
the client to resubmit. `error` carries the last broadcast error when set.

#### `POST /v1/mempool`

```json
{
  "raw": "0x...",
  "peers": 3,
  "webhook_url": "https://example.com/glyph",
  "webhook_secret": "at-least-16-characters"
}
```

Returns `202` with the entry and `meta.links.status`. Requires
`ALLOW_BROADCAST=true`. A duplicate tracked hash returns `202` with the existing
entry, so submissions are idempotent. A full mempool (`MEMPOOL_MAX_ENTRIES`)
returns `429 mempool_full`. `webhook_url` and `webhook_secret` must be supplied
together.

#### `GET /v1/mempool`

Lists entries, newest first. Optional `status` filters one lifecycle value;
an unknown value returns `400 bad_request`. `limit` is clamped to `1..500`
(default `100`). Returns `data` plus a `meta` object with the effective `limit`
and `status`.

#### `GET /v1/mempool/{hash}`

One tracked entry, with indexed confirmation merged in before the next
background poll persists it. A hash that is not tracked returns `404 not_found`
even when the transaction is already indexed; use
`GET /v1/transactions/{hash}` for that case. A malformed hash returns
`400 invalid_transaction_hash`.

#### `DELETE /v1/mempool/{hash}`

Marks an entry `cancelled` and returns it. A confirmed entry returns
`409 already_confirmed`. `expired`, `failed`, and already `cancelled` entries
can still be marked cancelled. An unknown hash returns `404 not_found`.

### Webhooks

Webhooks push mempool status changes to an HTTP or HTTPS endpoint. They are
optional; polling `/v1/mempool/{hash}` is enough for a single client.

#### `POST /v1/webhooks`

```json
{
  "url": "https://example.com/glyph",
  "secret": "at-least-16-characters",
  "events": ["transaction.confirmed", "transaction.expired"],
  "transaction_hashes": ["abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh"],
  "identity": "BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK"
}
```

`secret` is required and must be at least 16 characters. Events are
`transaction.queued`, `transaction.broadcast`, `transaction.confirmed`,
`transaction.expired`, `transaction.failed`, and `transaction.cancelled`. An
empty or omitted `events` list subscribes to every transaction event. Empty
`transaction_hashes` means all hashes. Empty `identity` means all source
identities; a non-empty value matches only the transaction source.

Returns `201` with the registration. The secret is never returned. Localhost and
private address targets are rejected with `400 bad_request` unless
`WEBHOOK_ALLOW_PRIVATE=true`.

#### `GET /v1/webhooks`

Active registrations only, without secrets. `meta.count` reports the number
returned.

#### `DELETE /v1/webhooks/{id}`

Deactivates a registration and returns the updated record without its secret.
Pending deliveries are not retried. The record and its delivery history remain.

#### `GET /v1/webhooks/{id}/deliveries`

Recent deliveries, newest first. Each row tracks one delivery across retries. Optional
`limit` is clamped to `1..500` (default `50`).

```json
{
  "data": [
    {
      "id": "2b6f0b8e-7f4a-4c2e-9f1d-3d5f7a9c1e02",
      "webhook_id": "9f0e8d7c-6b5a-4321-8fed-cba987654321",
      "event": "transaction.confirmed",
      "transaction_hash": "abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh",
      "status": "delivered",
      "attempt": 1,
      "next_attempt_at": null,
      "response_status": 200,
      "error": null,
      "payload": "{\"event\":\"transaction.confirmed\",...}",
      "created_at": "2026-11-18T15:10:02.000Z"
    }
  ],
  "meta": { "limit": 50 }
}
```

`status` is `pending`, `delivered`, or `failed`. `attempt` counts completed
attempts and `next_attempt_at` is set while a retry is scheduled. `payload` is
the exact JSON body sent to the receiver, retained so retries are
byte-identical. Delivery is at-least-once: retries use a 1 s, 2 s, 4 s, ...
backoff capped at 60 s and stop after `WEBHOOK_MAX_ATTEMPTS`, at which point the
row becomes `failed`. A non-2xx response or a timeout counts as a failed
attempt. `WEBHOOK_TIMEOUT_MS` bounds each request.

Delivery headers:

| Header | Meaning |
| --- | --- |
| `X-Glyph-Event` | Event name, for example `transaction.confirmed`. |
| `X-Glyph-Delivery` | Unique delivery id, stable across retries. Use it for receiver-side deduplication. |
| `X-Glyph-Timestamp` | Unix seconds when the request was signed. |
| `X-Glyph-Signature` | `sha256=<hex HMAC-SHA256 of "{timestamp}.{body}">`. |

The body is a JSON object with `event` and a `transaction` object carrying the
mempool entry fields. It is stored on the delivery row and replayed
byte-for-byte on retries.

Verify the signature over the exact bytes received, compare in constant time,
and reject stale timestamps. Example in Node:

```js
import crypto from "node:crypto";

export function verifyGlyphWebhook(rawBody, headers, secret) {
  const timestamp = headers["x-glyph-timestamp"];
  const received = headers["x-glyph-signature"] ?? "";
  const expected =
    "sha256=" +
    crypto
      .createHmac("sha256", secret)
      .update(`${timestamp}.${rawBody}`)
      .digest("hex");

  const age = Math.abs(Date.now() / 1000 - Number(timestamp));
  if (!Number.isFinite(age) || age > 300) return false;
  if (received.length !== expected.length) return false;
  return crypto.timingSafeEqual(Buffer.from(received), Buffer.from(expected));
}
```

Security caveats:

- The URL is supplied by an API client. The dispatcher rejects non-HTTP(S)
  schemes and private or loopback addresses by default, but
  `WEBHOOK_ALLOW_PRIVATE=true` removes that protection. Do not enable it on a
  public deployment unless clients are trusted.
- The secret is a credential. Never log it, return it, or reuse it across
  receivers. It is stored in ClickHouse so the dispatcher can sign retries;
  protect database credentials and backups accordingly.
- Delivery is at-least-once and retries can repeat an event. Deduplicate with
  `X-Glyph-Delivery` or by `(transaction_hash, event)`.
- Reject timestamps outside a short window to limit replay of a captured
  request.
- Running more than one API replica with webhooks enabled can duplicate
  deliveries because each replica runs its own dispatcher. Use one dispatcher
  or accept at-least-once delivery.

### `GET /v1/ticks/{tick}/raw`

Returns the archived raw bundle for an indexed tick. Raw archiving is optional
and is enabled on the indexer with `S3_BUCKET`/MinIO or `ARCHIVE_DIR`. The API
must be pointed at the same archive to serve this route.

Without `section`, the response is the zstd-compressed QBB1 container. With
`?section=` it returns one uncompressed section:

| `section` | Contents |
| --- | --- |
| `tick` | Tick header record. |
| `tick_data` | Tick data record. |
| `votes` | Concatenated 352-byte computor votes. Only present when the indexer ran with `ARCHIVE_VOTES=true`; otherwise `404 not_found` with a message explaining that votes were not archived. |
| `transactions` | Concatenated 144-byte transaction records. |
| `log_ranges` | Log range record. |
| `logs` | Concatenated encoded log events. |
| `computors` | Computor list for the tick's epoch. Read from the bundle when present, otherwise from the per-epoch sidecar at `raw/epoch=NNN/computors.bin`. |

Responses are `application/octet-stream` with an immutable cache policy and a
filename such as `tick-91916480-transactions.bin`. An unindexed tick or a
missing bundle returns `404 not_found`. A missing archive configuration returns
`503 unavailable`.

#### Archive policy

The default archive is smaller than the wire data:

- `ARCHIVE_VOTES=false` is the default. Full 352-byte vote headers are not part
  of the long-term archive. Operators who need forensic vote sets can set
  `ARCHIVE_VOTES=true`.
- The computor set is identical for every tick in an epoch. It is written once
  per epoch to `raw/epoch=NNN/computors.bin` instead of being repeated in every
  tick bundle. The set is about 21.7 KB, so repeating it 330k times per epoch
  costs roughly 7 GB of redundant object data.
- Tick bundles still contain the tick header, `TickData`, transactions, log
  ranges, and logs.
- Existing objects that already contain `Votes` or `Computors` sections remain
  readable. The bundle decoder treats both as optional.

This policy keeps the evidence needed to verify and replay indexed state while
removing two large redundant sections. A vote set that is not archived can be
re-fetched from a peer only while that peer retains the tick. If votes are
required for audit, enable `ARCHIVE_VOTES=true` before the epoch is ingested;
the setting does not backfill existing bundles.

### `GET /openapi.json`

OpenAPI 3.1 document covering the implemented routes. The document is generated
from a static spec in `crates/api/src/openapi.rs`.

### `GET /docs`

Interactive Scalar API reference backed by `/openapi.json`. Sections are grouped
into Operations, Chain Data, Accounts & Assets, Markets & Analytics, and Live &
Lookup. Every operation has a stable `operationId`, so Scalar's generated
snippets use readable method names.

### Unknown routes

Any unmatched path returns the normal problem document with
`404 not_found` and `detail` `route not found`. It is not an HTML error page.

## Errors

Errors are problem documents with exactly these fields:

```json
{
  "type": "about:blank",
  "title": "not_found",
  "status": 404,
  "code": "not_found",
  "detail": "tick 91916480 is not indexed"
}
```

| `code` | HTTP | Meaning |
| --- | --- | --- |
| `bad_request` | 400 | Missing or invalid request input, including `q` on search. |
| `invalid_identity` | 400 | Identity is not a valid base-26 identity. |
| `invalid_transaction_hash` | 400 | Hash is not a valid base-26 transaction hash. |
| `invalid_cursor` | 400 | Cursor is malformed or belongs to another route/filter set. |
| `not_found` | 404 | The resource is valid but absent from the index. |
| `conflict` | 409 | A generic state conflict. |
| `already_confirmed` | 409 | A confirmed mempool entry cannot be cancelled. |
| `mempool_full` | 429 | `MEMPOOL_MAX_ENTRIES` is reached. |
| `rate_limited` | 429 | `RATE_LIMIT_PER_MINUTE` was exceeded. Includes `Retry-After`. |
| `forbidden` | 403 | The operation is disabled, for example broadcast without `ALLOW_BROADCAST=true`. |
| `unavailable` | 503 | A peer, the stream hub, or the raw archive is not available. |
| `internal_error` | 500 | Storage or server failure. |

`type` is currently always `about:blank`. `title` equals `code`. `detail` is for
humans and is not stable.

## Caching

Successful tick and transaction reads set an explicit cache header:

```text
Cache-Control: public, max-age=31536000, immutable
```

Recent transaction and event feeds set:

```text
Cache-Control: public, max-age=2, stale-while-revalidate=5
```

Per-tick transaction and event lists are immutable and use the immutable
policy. Epoch, identity, identity history, and asset reads use:

```text
Cache-Control: public, max-age=30, stale-while-revalidate=60
```

Status and error responses use `no-store`. Health, search, and OpenAPI do not
set an explicit policy. There is no ETag or conditional request handling in the
current surface.

## Middleware

- Response compression: gzip and zstd when requested through
  `Accept-Encoding`.
- CORS: permissive by default. `CORS_ALLOWED_ORIGINS` accepts a
  comma-separated allowlist.
- Request id: `x-request-id` is set when absent and propagated.
- Rate limit hook: `RATE_LIMIT_PER_MINUTE` enables a fixed-window,
  per-client-IP limit and returns `429 rate_limited` with `Retry-After`.
- Tracing: tower-http trace layer; `RUST_LOG` controls verbosity.
- Body and concurrency limits are not configured in the current surface.

## Configuration

| Variable | Default | Meaning |
| --- | --- | --- |
| `BIND` | `127.0.0.1:8080` | Listen address. |
| `CLICKHOUSE_URL` | `http://127.0.0.1:8123` | ClickHouse HTTP endpoint. |
| `CLICKHOUSE_DB` | `glyph` | Database name. |
| `CORS_ALLOWED_ORIGINS` | unset (permissive) | Comma-separated browser origins. |
| `RATE_LIMIT_PER_MINUTE` | unset (disabled) | Per-client-IP request budget. |
| `QUBIC_PEERS` | unset | Comma-separated `host:port` peers for live routes and broadcast. |
| `QUBIC_PEER` | unset | Single fallback peer when `QUBIC_PEERS` is unset. |
| `ALLOW_BROADCAST` | `false` | Enable `POST /v1/transactions` and `POST /v1/mempool`. |
| `BROADCAST_PEERS` | `3` | Default and maximum peer fan-out per broadcast attempt (maximum `8`). |
| `MEMPOOL_ENABLED` | `true` | Enable mempool persistence and background tracking. |
| `MEMPOOL_POLL_MS` | `1000` | Mempool status reconciliation interval. |
| `MEMPOOL_RETRY_MS` | `5000` | Minimum delay between rebroadcasts. |
| `MEMPOOL_MAX_ATTEMPTS` | `5` | Maximum broadcast attempts per entry. |
| `MEMPOOL_EXPIRY_TICKS` | `10` | Expiry window after the target tick. |
| `MEMPOOL_MAX_ENTRIES` | `10000` | Maximum open mempool entries. |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Webhook delivery timeout. |
| `WEBHOOK_MAX_ATTEMPTS` | `6` | Delivery attempts before dead-lettering. |
| `WEBHOOK_ALLOW_PRIVATE` | `false` | Allow localhost and private webhook targets. |
| `STREAM_POLL_MS` | `1000` | Index-head poll interval for the stream hub. |
| `S3_BUCKET`, `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_PREFIX` | unset | Raw archive object storage. Presence of `S3_BUCKET` enables it; the `_ID`/`_SECRET_ACCESS_KEY` aliases are also accepted. |
| `ARCHIVE_DIR` | unset | Filesystem raw archive used when S3 is not configured. |
| `ARCHIVE_VOTES` | `false` | Include full vote headers in raw tick bundles. |
| `RUST_LOG` | `info,tower_http=info` | Tracing filter. |

The API never writes ticks. It applies schema initialization on startup and can
run as multiple replicas against the same database.

The indexer reads `PRUNE_KEEP_EPOCHS` for optional epoch retention. It is not an
API process variable.

## Examples

```bash
curl -sS localhost:8080/healthz
curl -sS localhost:8080/v1/status | jq .data.head
curl -sS localhost:8080/v1/network | jq .data
curl -sS 'localhost:8080/v1/epochs?limit=10' | jq .data
curl -sS 'localhost:8080/v1/ticks?epoch=240&limit=10' | jq .data
curl -sS localhost:8080/v1/ticks/91916480 | jq .data
curl -sS 'localhost:8080/v1/ticks/91916480/transactions?limit=10' | jq .
curl -sS 'localhost:8080/v1/ticks/91916480/events?limit=10' | jq .
curl -sS 'localhost:8080/v1/transactions?limit=10' | jq .
curl -sS 'localhost:8080/v1/events?type=asset_issuance&limit=10' | jq .
curl -sS localhost:8080/v1/transactions/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh
curl -sS 'localhost:8080/v1/identities?limit=10' | jq .
curl -sS localhost:8080/v1/identities/BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK
curl -sS 'localhost:8080/v1/identities/BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK/transactions?limit=10' | jq .
curl -sS 'localhost:8080/v1/assets?limit=10' | jq .
curl -sS 'localhost:8080/v1/search?q=91916480' | jq .
curl -sS -X POST localhost:8080/v1/transactions/batch \
  -H 'content-type: application/json' \
  -d '{"hashes":["abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh"]}' | jq .
curl -sS 'localhost:8080/v1/analytics/activity?interval=60&limit=10' | jq .
curl -sS 'localhost:8080/v1/analytics/top-identities?limit=10' | jq .
curl -sS -N 'localhost:8080/v1/stream?types=tick,event'
curl -sS 'localhost:8080/v1/live/identities/BZBQFLLBNCXEMGLOBHUVFTLUPLVCPQUASSILFABOFFBCADQSSUPNWLZBQEXK' | jq .
curl -sS 'localhost:8080/v1/live/ticks/91916480/transactions' | jq .
curl -sS 'localhost:8080/v1/live/transactions/<hash>/status' | jq .
curl -sS -X POST localhost:8080/v1/transactions/validate \
  -H 'content-type: application/json' \
  -d '{"raw":"0x..."}' | jq .
curl -sS -X POST localhost:8080/v1/transactions \
  -H 'content-type: application/json' -d '{"raw":"0x..."}' | jq .
curl -sS -X POST localhost:8080/v1/transactions \
  -H 'content-type: application/json' -d '{"raw":"0x...","direct":true}' | jq .
curl -sS -X POST localhost:8080/v1/mempool \
  -H 'content-type: application/json' -d '{"raw":"0x..."}' | jq .
curl -sS 'localhost:8080/v1/mempool?status=broadcast&limit=20' | jq .
curl -sS localhost:8080/v1/mempool/abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyzabcdefgh | jq .
curl -sS -X POST localhost:8080/v1/webhooks \
  -H 'content-type: application/json' \
  -d '{"url":"https://example.com/glyph","secret":"at-least-16-characters","events":["transaction.confirmed"]}' | jq .
curl -sS -o tick.bin 'localhost:8080/v1/ticks/91916480/raw?section=transactions'
```
