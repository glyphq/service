# Roadmap

This roadmap is ordered by dependency, not by user-visible excitement. A phase is done
only when its exit criteria pass on a live peer connection. "Code exists" is not done.

## Current status

Phases 0 through 5 are implemented and exercised against live peers, with the API and
store also covered by ClickHouse integration tests. The remaining gaps are narrower than
the original phase boundaries:

- Implemented: direct peer transport, K12 and SchnorrQ, quorum and data verification,
  ClickHouse persistence and checkpointing, the v1 resource API, typed events, cursor
  pagination, asset and epoch reads derived from indexed rows, search, OpenAPI, batch
  transaction lookup, analytics, SSE and WebSocket streaming, peer-backed entity,
  transaction, status, and contract routes, opt-in broadcast, the raw bundle archive, and
  the Docker Compose stack.
- Partial: streaming has no `Last-Event-ID` replay, only a lag event and current-head
  continuation. The batch route looks up transaction hashes, not arbitrary GET requests.
  The archive writes and reads bundles but has no spool, manifest, or replay command yet.
  Operations have health probes, request IDs, tracing, and graceful shutdown, but no
  Prometheus metrics or fuzz targets.
- Not started: dedicated quorum evidence, epoch, computor, asset, and participant tables;
  a generic batch endpoint; Prometheus metrics; multi-peer quorum cross-checking; a
  seven-day soak.

## Phase 0: Foundations

**Scope**

- Workspace crates: `qubic`, `node`, `indexer`, `store`, `archive`, `api`, `cli`.
- Wire framing, message types, identity encoding, log decoding, K12, SchnorrQ.
- ClickHouse schema and local development configuration.
- Architecture, protocol, API, data model, and roadmap documents.
- CI for formatting, clippy, and tests.

**Exit criteria**

- `cargo fmt --all -- --check` passes.
- `cargo clippy --workspace --all-targets -- -D warnings` passes.
- `cargo test --workspace` passes without requiring a live peer or ClickHouse. Crypto and
  wire tests use fixtures and property tests.
- `scripts/dev-up.sh`, `scripts/dev-down.sh`, and `scripts/smoke.sh` pass `bash -n` and run
  on a host with no Docker.
- Every relative markdown link in `docs/` and `README.md` resolves.
- No crate depends on a hosted RPC client.

## Phase 1: Peer transport

**Scope**

- TCP connect with connect timeout and read timeout.
- `ExchangePublicPeers` handshake, peer discovery, deduplication, bounded candidate set.
- `RequestCurrentTickInfo` and `RespondCurrentTickInfo`.
- `RequestComputors` and the epoch computor cache.
- Packet reader that handles partial reads, interleaved broadcasts, `EndResponse`, and
  `TryAgain`.
- `dejavu` correlation and one in-flight request per connection.
- Peer scoring by latency and failure rate. Failover on the first error.
- `glyph head` and `glyph peers`.

**Exit criteria**

- `glyph head --peer <addr>` returns the current epoch, head, initial tick, and tick
  duration against two independent Qubic peers.
- `glyph peers --peer <addr>` prints at least one discovered peer that is not the seed.
- A forced disconnect mid-request is detected, the peer is demoted, and the next peer
  serves the request.
- A one-hour soak with one request every 10 seconds has zero unhandled errors and no
  unbounded memory growth.
- A malformed packet closes only that connection, never the process.
- Unit tests cover header decode, 24-bit size, unknown type, truncated payload, and
  interleaved broadcast handling.

## Phase 2: Tick verification and ingest

**Scope**

- `RequestTickData`, `RequestQuorumTick`, `RequestTickTransactions`,
  `RequestAllLogIdRangesFromTx`, `RequestLog`.
- Vote decoding and SchnorrQ verification against the epoch computor set.
- Quorum counting: 451 for non-empty, 226 for empty.
- Transaction hash verification against `TickData` digests.
- Log digest verification and log ID range mapping.
- State digest chain checks (future; the `state` tier is currently reserved).
- `VerifiedTick` with a tier and evidence.
- `glyph tick <n>` bundle summary.

**Exit criteria**

- 1,000 consecutive ticks are fetched and verified from the live network with zero gaps.
- Every complete bundle reaches the `data` tier. A bundle with missing transactions or logs
  is stored at `quorum` and reported in the warnings.
- A tampered transaction digest, a tampered log body, and a vote from an unknown key are
  each rejected by an integration test with a fixture.
- A tick with fewer than threshold votes is not persisted. The indexer retries and records
  the failure.
- An empty tick is accepted at 226 votes and rejected at 225.
- The verifier is a pure function over a bundle and has no network or database calls.
- `glyph tick` reports transaction, vote, and log counts that match a second peer's view
  for the same tick.

## Phase 3: Durable store and checkpoint

**Scope**

- ClickHouse schema application from `crates/store/src/schema.rs`.
- Tick, transaction, and event insert batches with a logical `version`.
- Checkpoint keys in `ingest_state`.
- Single-writer lease.
- Idempotent retry after a crash.
- `glyph sync` follow-head loop.
- `/v1/status` backed by real queries.

**Exit criteria**

- Killing the indexer mid-epoch and restarting resumes at the next uncommitted tick with no
  gaps and no duplicate logical rows after merge.
- Inserting the same tick twice with a higher version converges to one row under `FINAL`.
- The checkpoint and the row batch are consistent: a crash between them causes a refetch,
  not a gap.
- A ClickHouse restart does not lose the checkpoint. The indexer reconnects and resumes.
- `/v1/status` reports the indexed head, network head, lag, and verification counts.
- A schema applied twice is a no-op.
- The indexer runs for one hour at one tick per second with bounded memory and a shrinking
  lag after a deliberate 10-minute pause.

## Phase 4: API v1 core

**Scope**

- Response envelope with `{data, meta}`.
- Implemented routes: `/healthz`, `/v1/status`, `/v1/ticks/{tick}`,
  `/v1/ticks/{tick}/transactions`, `/v1/ticks/{tick}/events`,
  `/v1/transactions`, `/v1/transactions/{hash}`, `/v1/events`,
  `/v1/identities/{identity}`, `/v1/identities/{identity}/transactions`, `/v1/search`,
  `/openapi.json`, and `/docs`.
- `snake_case` response fields, JSON integer amounts, RFC 3339 timestamps.
- Pagination with `limit` (default 50, maximum 200). The implemented surface uses opaque
  cursors in `meta.nextCursor`; `offset` remains a deprecated fallback.
- RFC 9457 problem documents with `bad_request`, `invalid_identity`,
  `invalid_transaction_hash`, `not_found`, and `internal_error`.
- Immutable `Cache-Control` on tick and transaction reads.
- Identity aggregate maintenance from indexed QU transfers.

**Exit criteria**

- Every implemented route has contract tests for success, not found, invalid identifier,
  and storage failure.
- A page scan over 10,000 ticks returns every row exactly once in ascending order.
- `limit` above 200 is clamped and `offset` is honored.
- Amounts round-trip as signed 64-bit integers through a 64-bit-safe client.
- `/v1/ticks/{tick}` for an unindexed tick returns `404 not_found`, not an empty object.
- p95 latency for a point read is under 50 ms and for a 100-row page under 100 ms on the
  development host with 10 million indexed transactions.
- The API can run two replicas against the same ClickHouse with identical results.

## Phase 5: Events, logs, and typed decoding

**Scope**

- Typed bodies for all documented log types.
- Unknown bodies preserved as raw hex.
- Event endpoints and filters.
- Asset tables and endpoints.
- Contract message indexes.
- Quorum evidence endpoint and signer bitmap.

**Exit criteria**

- Every log type in [PROTOCOL.md](./PROTOCOL.md#log-bodies) has a fixture and a decode
  test.
- An unknown log type is stored and served with `type = "unknown"` and a raw body. It does
  not drop the event or fail the tick.
- Event counts match the sum of valid log range lengths for 1,000 consecutive ticks.
- An event reported under the wrong transaction slot fails the data tier.
- Asset issuance, ownership change, and possession change round-trip from log body to API
  response.
- `GET /v1/ticks/{tick}/votes` returns a signer bitmap that passes a count check against
  `quorumVotes`.

## Phase 6: Streaming, batch, search, OpenAPI

**Scope**

- SSE `/v1/stream` with `Last-Event-ID` replay.
- WebSocket `/v1/ws` with subscribe, unsubscribe, ping.
- Batch transaction lookup `/v1/transactions/batch` for up to 100 hashes.
- Search `/v1/search` with typed classification.
- OpenAPI 3.1 generation and `/docs`.
- Rate limiting.

**Exit criteria**

- A client that disconnects and reconnects with `Last-Event-ID` receives every tick exactly
  once or receives an explicit `reset` with the current head. No silent gaps.
- SSE and WebSocket deliver the same payload for the same tick.
- A slow stream consumer does not block ingest or other clients. Tick events are never
  dropped. The lag event is observed.
- A batch returns found rows plus `meta.missing`, including a mix of present and absent
  hashes, with a 200 envelope.
- More than 100 unique hashes returns `400 bad_request`.
- Search classifies a tick, an identity, a transaction hash, an asset, and an event ID
  correctly, and rejects garbage with `invalid_query`.
- The generated OpenAPI document validates against the OpenAPI 3.1 schema and covers every
  implemented route.
- Rate limits return `429` with `Retry-After` and do not affect a different client.

## Phase 7: Archive and replay

**Scope**

- Raw bundle writer for local filesystem and S3. (Implemented.)
- Manifest with content hashes.
- Local spool and retry on archive failure. (Not implemented; failures are logged or fail
  the tick under `ARCHIVE_FAIL_CLOSED`.)
- Partition rebuild from the archive.
- Checkpoint recovery.
- Retention policy and partition drops.

**Exit criteria**

- A full epoch of archived bundles can rebuild the ClickHouse partitions from an empty
  database.
- Replayed data is byte-identical at the row level to the original for ticks,
  transactions, and events.
- An archive outage during ingest does not stop the head. The spool drains after recovery.
- A dropped partition returns `404 not_found` for missing ticks, not a partial result.
- A lost checkpoint is recovered from the maximum common tick across tables and replay
  resumes without duplication.
- A replay that encounters a bad digest stops at that tick and reports it.

## Phase 8: Operations and hardening

**Scope**

- Prometheus metrics and a starter dashboard.
- Structured JSON logs with request IDs and peer IDs.
- Health and readiness probes.
- Graceful shutdown with checkpoint flush.
- Multi-peer quorum cross-checking.
- Fuzz targets for packet and log decoders.
- Security review of SQL parameters, size limits, and rate limits.
- Load test of the read path.

**Exit criteria**

- A seven-day soak covers at least one epoch rollover and one peer restart with no data
  gaps and no manual intervention.
- Metrics expose head, lag, peer count, fetch errors, verification failures, insert
  latency, and API latency.
- Fuzz targets run for 10 million iterations without a panic or unbounded allocation.
- A 10x read traffic spike does not increase indexer lag beyond one tick.
- Graceful shutdown never leaves an uncommitted batch without a checkpointed predecessor.
- Runbooks exist for peer loss, ClickHouse loss, disk full, archive failure, and epoch
  rollover.
- The project has a tagged release, a reproducible build, and a documented upgrade path.

## Cross-cutting work

These items are tracked in every phase, not deferred to the end.

- **Documentation.** A change to the wire layout, API shape, or schema updates the relevant
  document in the same change.
- **Tests.** Every bug fix gets a regression test. Every decoder gets a malformed-input
  test. Crypto gets known-answer and property tests.
- **No hosted RPC.** A pull request that adds a hosted RPC dependency is rejected. Peer
  addresses are configuration, not data sources.
- **No pre-240 backfill.** A feature that requires older data is out of scope until there
  is a separate archive import path with its own compatibility guarantees.
- **No `glyph` folder prefix.** New crates and directories use their functional name.

## Explicit non-goals

- Wallet features. Transaction broadcast exists as an opt-in API route, not as a wallet.
- Consensus participation or voting.
- Hosted multi-tenant service with billing.
- Full historical backfill before the epoch present at first start.
- Compatibility endpoints for core-bob or the official RPC.
- Smart contract execution or state simulation.
- Price or oracle services beyond indexed log data.

## Release gates

| Gate | Required |
| --- | --- |
| `main` merge | fmt, clippy with `-D warnings`, tests, docs links |
| Preview deploy | Phase 4 exit criteria plus a live peer smoke |
| Public v1 | Phases 0 to 6 exit criteria, OpenAPI validation, rate limits, 7-day soak |
| Production v1 | Phase 7 and Phase 8 exit criteria, backup and replay drill, on-call runbook |
