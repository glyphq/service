# API

Glyph API exposes a read-only HTTP API over the local ClickHouse index. It never
calls a hosted RPC at request time. If a resource is not indexed, the API
returns a problem document instead of proxying upstream.

- Base URL in development: `http://127.0.0.1:8080`
- Data routes are under `/v1`. `/healthz`, `/openapi.json`, and `/docs` are
  unversioned.
- Requests and responses are UTF-8 JSON. Errors use `application/problem+json`.
- The server is axum with gzip and zstd response compression, CORS (permissive
  by default or restricted with `CORS_ALLOWED_ORIGINS`), an `x-request-id`
  header, optional `RATE_LIMIT_PER_MINUTE` throttling, and request tracing.
- The OpenAPI 3.1 document is at `GET /openapi.json`. A small HTML index is at
  `GET /docs`.

This page documents the implemented surface. Planned endpoints and tiers are in
[ROADMAP.md](./ROADMAP.md) and are not part of the current contract.

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
    "nextOffset": null,
    "nextCursor": null,
    "hasMore": false
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
  decoder applies, or a raw representation otherwise.
- `tx_index` is the transaction slot. Special events use `-1`. `tx_hash` is
  empty or a synthetic `special:` marker for special events.

## Pagination

Every collection uses opaque cursor pagination.

| Parameter | Default | Bounds |
| --- | --- | --- |
| `limit` | `50` | `1..200` |
| `cursor` | none | Opaque token from `meta.nextCursor`. |
| `offset` | `0` | Legacy, deprecated. Ignored when `cursor` is present. |

The response `meta` contains:

| Field | Meaning |
| --- | --- |
| `limit` | Effective limit after clamping. |
| `offset` | Effective offset used for this page. |
| `count` | Rows returned in this page. |
| `nextOffset` | Legacy next offset when `hasMore`, otherwise `null`. |
| `nextCursor` | Cursor for the next page when `hasMore`, otherwise `null`. |
| `hasMore` | Whether another page exists. |

A cursor is bound to the route and its normalized filters. Reusing a cursor
with a different filter set, or sending a malformed token, returns
`400 invalid_cursor`. Clients must treat cursors as opaque and follow
`nextCursor` until it is `null`.

Collections are ordered newest first unless the endpoint says otherwise. Tick
and transaction point reads are immutable and cacheable; list feeds are
short-lived.

## Endpoints

### `GET /healthz`

Liveness only. Does not touch ClickHouse.

```json
{ "ok": true }
```

### `GET /v1/status`

Indexed head, progress, and counts.

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

### `GET /v1/network/stats`

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

Epoch summaries derived from indexed ticks, newest first. Uses cursor
pagination.

```json
{
  "data": [
    {
      "epoch": 240,
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
    "nextOffset": null,
    "nextCursor": null,
    "hasMore": false
  }
}
```

`closed` is true when a later epoch is already indexed. The newest epoch can
still receive ticks.

### `GET /v1/epochs/{epoch}`

One epoch summary. An epoch with no indexed ticks returns `404 not_found`.

### `GET /v1/ticks`

Ticks newest first, optionally filtered by `epoch`. Uses cursor pagination.

| Query | Meaning |
| --- | --- |
| `epoch` | Restrict to one epoch. |
| `limit` | Page size. |
| `cursor` | Cursor from `meta.nextCursor`. |

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
    "nextOffset": null,
    "hasMore": false
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
    "nextOffset": null,
    "hasMore": false
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
    "nextOffset": null,
    "hasMore": false
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
  "meta": { "limit": 50, "offset": 0, "count": 1, "nextCursor": null, "hasMore": false }
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
  "meta": { "limit": 50, "offset": 0, "count": 1, "nextCursor": null, "hasMore": false }
}
```

### `GET /v1/assets/{issuer}/{name}`

One asset by canonical issuer and name. A malformed issuer returns
`400 invalid_identity`; an unindexed asset returns `404 not_found`. The asset
tables are planned; this read currently aggregates issuance events.

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

Resolves a single value against the index. `q` is required and trimmed.

Resolution order:

1. All ASCII digits: parse as a tick and return it if indexed.
2. Lowercase value that decodes as a transaction hash: return the transaction
   if indexed.
3. Uppercase value that decodes as an identity: return the identity if indexed.
4. Otherwise return an empty list.

```json
{
  "data": [
    { "type": "tick", "tick": 91916480, "value": { "...": "..." } }
  ]
}
```

`type` is `tick`, `transaction`, or `identity`. A missing or empty `q` returns
`400 bad_request`.

### `GET /openapi.json`

OpenAPI 3.1 document covering the implemented routes. The document is generated
from a static spec in `crates/api/src/openapi.rs`.

### `GET /docs`

Small HTML index linking to `/openapi.json` and listing the implemented routes.

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
| `rate_limited` | 429 | `RATE_LIMIT_PER_MINUTE` was exceeded. Includes `Retry-After`. |
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
| `RUST_LOG` | `info,tower_http=info` | Tracing filter. |

The API never writes ticks. It applies schema initialization on startup and can
run as multiple replicas against the same database.

## Examples

```bash
curl -sS localhost:8080/healthz
curl -sS localhost:8080/v1/status | jq .data.head
curl -sS localhost:8080/v1/network/stats | jq .data
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
```
