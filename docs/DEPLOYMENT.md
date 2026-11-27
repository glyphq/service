# Deployment with Docker

This document describes the single-host Docker Compose deployment. It runs the
same two long-running processes as the development stack, with ClickHouse and
SeaweedFS S3 as dependencies:

| Process | Binary | Role |
| --- | --- | --- |
| API | `glyph-api` | HTTP API. Reads ClickHouse; optional live routes and broadcast connect directly to Qubic peers. |
| Indexer | `glyph sync` | Fetches and verifies peer ticks, writes ClickHouse. |
| ClickHouse | `clickhouse-server` | Shared analytical store and ingest checkpoint. |
| SeaweedFS | `seaweedfs` | S3-compatible storage, provisioned for the raw archive. |

The deployment is intentionally a single host with one indexer. ClickHouse is a
single node and the indexer owns the write path. Scale API replicas only behind
a load balancer; see [Scaling the API](#scaling-the-api).

## Prerequisites

- Docker Engine 24 or newer with the Compose v2 plugin (`docker compose`).
- 8 GB of RAM is the practical minimum for ClickHouse, SeaweedFS, the API, and the
  indexer together; 16 GB is recommended for production. Two vCPUs are workable
  for testing, but 4 vCPUs are recommended for full signature verification,
  ClickHouse merges, and API traffic at the same time.
- Disk for ClickHouse data. Budget roughly **20 GB per retained epoch** at
  current activity. See [Capacity estimates](ARCHITECTURE.md#capacity-estimates)
  and the sizing notes in the [README](../README.md#system-requirements).
- Outbound TCP to Qubic peers (`21841`, or `21842` for Bob peers). No inbound
  peer port is required.
- Optional: `curl` on the host for the smoke script and manual checks.

The image builds the workspace with the pinned `rust:1.88-bookworm` toolchain
and honours `Cargo.lock`. The workspace MSRV is 1.85, but the image pins 1.88
because `object_store 0.13` uses language features that need a newer compiler.
The first build compiles all dependencies and can take several minutes; later
builds reuse the BuildKit registry and target caches. The runtime image is
`debian:bookworm-slim` with `ca-certificates`, `curl`, and `bash`, and runs as
the unprivileged `glyph` user (`uid 10001`).

## Quick start

```bash
# Build the image, start ClickHouse, SeaweedFS, the schema migration, the API, and
# the indexer, then wait for their healthchecks.
scripts/docker-up.sh

# Check ClickHouse, the API, and a live peer tick.
scripts/smoke.sh
```

Without the helper:

```bash
docker compose up -d --build
curl -fsS http://127.0.0.1:8080/healthz
```

Stop the stack and keep its data:

```bash
scripts/docker-down.sh
# or: docker compose down
```

Stop and delete ClickHouse, SeaweedFS, and log volumes. This is irreversible:

```bash
scripts/docker-down.sh --wipe
```

`scripts/docker-down.sh --images` also removes the locally built image.

## Configuration

Compose reads a `.env` file from the repository root, or variables from the
environment of the invoking shell. `scripts/docker-up.sh` and
`scripts/docker-down.sh` pass `./.env` explicitly when it exists; override that
with `GLYPH_ENV_FILE=/path/to/file`.

A documented template lives at
[`deploy/docker/env.example`](../deploy/docker/env.example). Copy it to `.env`
and edit the peers and secrets:

```bash
cp deploy/docker/env.example .env
$EDITOR .env
```

The defaults are safe for a local host: published ports bind to `127.0.0.1`
and the stack is reachable only from the same machine.

### Application variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `CLICKHOUSE_DB` | `glyph` | ClickHouse database name. |
| `CLICKHOUSE_URL` | `http://clickhouse:8123` | ClickHouse HTTP endpoint used by the API and indexer. |
| `CLICKHOUSE_HTTP_PORT` | `8123` | Host loopback port for the ClickHouse HTTP endpoint. |
| `CLICKHOUSE_MAX_EXECUTION_TIME` | `10` | Per-query ClickHouse time limit in seconds. |
| `CLICKHOUSE_MAX_MEMORY_USAGE` | `2000000000` | Per-query ClickHouse memory limit in bytes. |
| `CLICKHOUSE_MAX_THREADS` | `2` | Per-query ClickHouse thread limit. Keep this below the core count on hosts shared with ingestion. |
| `BIND` | `0.0.0.0:8080` | Listen address for `glyph-api` inside the container. Keep the port at `8080` unless you also change the `api` port mapping. |
| `API_HOST` | `127.0.0.1` | Host interface for the published API port. Set `0.0.0.0` to expose it. |
| `API_PORT` | `8080` | Host port for the API. |
| `QUBIC_PEERS` | `peer.example:21841` | Comma-separated peer addresses used by the indexer and by live API routes. |
| `ALLOW_BROADCAST` | `false` | Set `true` to enable `POST /v1/transactions`. Keep disabled unless the API is trusted. |
| `BROADCAST_PEERS` | `8` | Peers contacted concurrently per broadcast. Capped at `8`; each connection is a direct peer socket. |
| `STREAM_POLL_MS` | `1000` | Stream hub index-head poll interval. |
| `PRICE_POLL_MS` | `60000` | Live QUBIC/USDT oracle price refresh interval in the API. |
| `MARKET_ENABLED` | `true` | Enable the external exchange consensus poller. Set `false` to disable outbound HTTP market reads. |
| `MARKET_POLL_MS` | `5000` | Market poll interval. One provider is queried per tick in round robin. |
| `COINGECKO_API_KEY`, `COINGECKO_URL`, `MEXC_URL`, `GATE_URL` | unset | Optional market endpoints and CoinGecko key. |
| `GLYPH_FROM_TICK` | unset | Optional first tick when there is no stored cursor. |
| `INDEXER_POLL_MS` | `750` | Head poll interval once caught up. |
| `INDEXER_PROGRESS_EVERY` | `100` | Log progress every N indexed ticks. |
| `INDEXER_FETCH_CONCURRENCY` | `32` | Concurrent peer connections used by fetch workers. Raise for backfill, lower on a small host. |
| `INDEXER_FETCH_WINDOW` | `512` | Maximum ticks in flight or buffered before the ordered writer. Memory scales with this value. |
| `INDEXER_FETCH_BATCH` | `32` | Maximum ticks per peer fetch batch. Starts at the maximum and halves on batch-level failures, with a floor of 4. |
| `INDEXER_PEER_CONNECTIONS` | `4` | Concurrent TCP connections to one peer. Qubic peers close excess connections from one address. |
| `INDEXER_WRITE_BATCH` | `128` | Maximum ticks per ClickHouse insert batch. |
| `INDEXER_WRITE_FLUSH_MS` | `250` | Maximum delay before an idle partial batch is flushed. |
| `INDEXER_DISCOVER_PEERS` | `false` | Append IPv4 peers advertised through `ExchangePublicPeers`, using port `21841`. |
| `INDEXER_MAX_TICK_ATTEMPTS` | `4` | Skip a tick after this many attempts when every peer reports its tick data does not exist. Transient errors keep retrying. |
| `GLYPH_VERIFY_SIGNATURES` | `false` | Set `true` to verify SchnorrQ signatures locally. Quorum agreement is always checked. |
| `S3_ENDPOINT` | `http://seaweedfs:8333` | S3 endpoint inside the Compose network, passed to the archive client and `s3-init`. |
| `S3_BUCKET` | `glyph-archive` | Bucket created by `s3-init` and used by the archive client. |
| `S3_ACCESS_KEY` | `glyph` | SeaweedFS identity access key. |
| `S3_SECRET_KEY` | `glyph-dev-secret` | SeaweedFS identity secret key. Must be at least 8 characters. |
| `S3_REGION` | `us-east-1` | S3 region. SeaweedFS accepts the default. |
| `S3_PORT` | `8333` | Host loopback port for the SeaweedFS S3 API. |
| `CLICKHOUSE_USER` | `glyph` | ClickHouse user created by the image entrypoint. |
| `CLICKHOUSE_PASSWORD` | `glyph-dev-secret` | ClickHouse password. `CLICKHOUSE_URL` must embed the same credentials. |
| `ARCHIVE_FAIL_CLOSED` | `false` | Set `true` to stop ingestion when a bundle cannot be archived. |
| `ARCHIVE_VOTES` | `false` | Store full vote headers in every tick bundle. Off by default; the computor set is always written once per epoch at `raw/epoch=NNN/computors.bin`. |
| `MEMPOOL_ENABLED` | `true` | Enable the internal transaction tracker and webhook events. Requires `ALLOW_BROADCAST=true` to reach peers. |
| `MEMPOOL_POLL_MS` | `500` | Tracker status poll interval. |
| `MEMPOOL_RETRY_MS` | `1000` | Minimum delay between rebroadcasts of an unconfirmed transaction. |
| `MEMPOOL_MAX_ATTEMPTS` | `10` | Maximum broadcast attempts before an entry is marked `failed`. |
| `MEMPOOL_EXPIRY_TICKS` | `10` | Ticks after the target tick before an unindexed transaction expires. |
| `MEMPOOL_MAX_ENTRIES` | `10000` | Maximum open tracked transactions. New submissions are rejected when full. |
| `MEMPOOL_MAX_PER_SOURCE` | `100` | Maximum open tracked transactions per source identity. |
| `WEBHOOK_TIMEOUT_MS` | `5000` | Webhook delivery timeout. |
| `WEBHOOK_ALLOW_PRIVATE` | `false` | Allow localhost and private-range webhook targets. Keep `false` unless the receiver is internal and trusted. |
| `WEBHOOK_ENCRYPTION_KEY` | unset | 32-byte hex or base64 key for webhook URL encryption at rest. `deploy/docker/run-stack.sh` generates and persists one automatically. |
| `GLYPH_LABELS_FILE` | unset | Operator JSON label registry for identities, assets, and contract indices. |
| `GLYPH_WHALE_MIN_AMOUNT` | `1000000000` | Whale threshold in qu used by analytics, streams, and webhook events. |
| `PRUNE_KEEP_EPOCHS` | unset | Optional retention after an epoch rollover. Drops ClickHouse partitions older than the newest N epochs. |
| `GLYPH_IMAGE` | `glyph:local` | Image tag built and used by all glyph services. |
| `GLYPH_API_LOG` | `info,tower_http=info` | `RUST_LOG` for the API. |
| `GLYPH_INDEXER_LOG` | `info` | `RUST_LOG` for the indexer. |
| `GLYPH_MIGRATE_LOG` | `info` | `RUST_LOG` for the one-shot migration. |

`CLICKHOUSE_URL` and `S3_ENDPOINT` are resolved inside the glyph containers.
Keep the Compose service names (`clickhouse`, `seaweedfs`); `127.0.0.1` would
point at the container itself. The default URL embeds `CLICKHOUSE_USER` and
`CLICKHOUSE_PASSWORD`; change all three together. Use `API_HOST`/`API_PORT` and
the published ClickHouse and S3 ports to reach the stack from the host.

The `migrate` service runs `glyph db-init` on every start. The DDL is
`CREATE ... IF NOT EXISTS`, so it is idempotent and safe to rerun.

On a fresh database the indexer starts from the current epoch initial tick. Peers
prune old tick data after a few thousand ticks, so set `GLYPH_FROM_TICK` to a
recent retained tick when bootstrapping after that point. Find one with
`glyph tick <tick> --peer <addr>`; the indexer logs `peer returned end response`
and retries forever if the requested tick is gone. Once the first batch is
stored, the cursor takes over and `GLYPH_FROM_TICK` is ignored.

`s3-init` creates the archive bucket. It is idempotent. The indexer writes one
zstd-compressed QBB1 bundle per verified tick before persisting that tick, using
the S3 variables from the shared application environment. By default a bundle
contains the tick header, tick data, transactions, log ranges, and logs. Full
vote headers are opt-in with `ARCHIVE_VOTES=true`, and the epoch computor set is
written once to `raw/epoch=NNN/computors.bin` rather than repeated in every
bundle. The API exposes the same objects through `GET /v1/ticks/{tick}/raw`.

The API owns the mempool and webhook loop. `POST /v1/transactions` submits to the
mempool by default when `MEMPOOL_ENABLED=true`; add `"direct": true` to use the
old synchronous peer fan-out. Mempool and webhook state lives in the `mempool`,
`tracked_transactions`, `webhooks`, and `webhook_deliveries` ClickHouse tables,
so a restart does not lose queued entries and public transaction pages survive
operational mempool cleanup. Keep `ALLOW_BROADCAST=false` unless the deployment
is trusted: without it, queued entries never reach a peer and expire.

### Epoch lifecycle and retention

Epoch boundaries are observed from peer `Head` values and stored in
`glyph.epochs`. Do not size or configure around a fixed 604,800-tick epoch; the
current network uses roughly 330k ticks and the rate can change.

Epochs are not tick-number contiguous. Qubic has started epochs from a
published `TICK` after a coordinated restart, so the next epoch's
`initial_tick` can be tens of thousands above the previous epoch's last tick.
The indexer closes the previous epoch at its last observed tick and skips the
nonexistent gap instead of retrying those tick numbers.

- All chain tables live in the single `CLICKHOUSE_DB` database and are
  partitioned by `epoch`. Per-epoch databases are deprecated. They duplicate
  DDL, break cross-epoch tables (`identities`, `price_points`), and complicate
  the checkpoint.
- `PRUNE_KEEP_EPOCHS` drops chain partitions older than the newest N epochs
  after a rollover. Leave it unset to retain everything.
- `docker compose run --rm indexer prune --keep N --dry-run` lists what would be
  dropped. Without `--dry-run` it drops ClickHouse partitions only. Archive
  objects are never deleted, and `identities` and `price_points` are never
  pruned by this command.
- Pruning is irreversible for the hot store. Confirm that the affected epochs
  are backed up or that the raw archive is sufficient before running it.

### Script variables

| Variable | Default | Purpose |
| --- | --- | --- |
| `GLYPH_ENV_FILE` | `./.env` when present | Explicit Compose env file. |
| `GLYPH_WAIT_TIMEOUT` | `300` | Seconds to wait for API and indexer health in `docker-up.sh`. |

The helper accepts repeatable `--peer` flags. They are joined into
`QUBIC_PEERS`, overriding the value from the environment or `.env`:

```bash
scripts/docker-up.sh --peer 1.2.3.4:21841 --peer 5.6.7.8:21841
```

## Services, ports, and health

| Service | Image | Published | Healthcheck |
| --- | --- | --- | --- |
| `clickhouse` | `clickhouse/clickhouse-server:25.8` | `127.0.0.1:${CLICKHOUSE_HTTP_PORT}` | `clickhouse-client --user ... --query "SELECT 1"` |
| `seaweedfs` | `chrislusf/seaweedfs:latest` | `127.0.0.1:${S3_PORT}` | S3 bucket bootstrap |
| `s3-init` | `amazon/aws-cli:latest` | none, one-shot | exit status |
| `migrate` | local `glyph:local` image | none, one-shot | exit status |
| `api` | local `glyph:local` image | `${API_HOST}:${API_PORT}` | `GET /healthz` |
| `indexer` | local `glyph:local` image | none | process liveness (`kill -0 1`) |

Startup order:

1. ClickHouse becomes healthy.
2. `migrate` runs `glyph db-init` and exits successfully.
3. SeaweedFS starts; `s3-init` creates the archive bucket.
4. API and indexer start. The API becomes healthy on `/healthz`; the indexer
   healthcheck starts after a 30 second start period.

The indexer has no HTTP listener, so its Compose healthcheck only proves that
the process is alive. Use `docker compose logs -f indexer` and `/v1/status` for
progress. `/healthz` proves only that the API process is alive; use `/v1/status`
for readiness because it queries ClickHouse. See
[Health and readiness](ARCHITECTURE.md#health-and-readiness).

Both long-running processes receive `SIGINT` on `docker compose stop` so the
indexer can finish its current batch. The grace period is 30 seconds.

The image has one entrypoint that selects a role from the first command word:
`api` (default), `indexer`, or `glyph` for an arbitrary CLI command. The
Compose file uses `api`, `indexer`, and `glyph db-init` respectively.

## Volumes and data

| Volume | Mount | Contents |
| --- | --- | --- |
| `glyph_clickhouse-data` | `/var/lib/clickhouse` | Canonical tables, projections, and the ingest checkpoint. |
| `glyph_clickhouse-logs` | `/var/log/clickhouse-server` | ClickHouse server logs. |
| `glyph_seaweedfs-data` | `/data` | Raw archive bucket objects. |

ClickHouse is the required online state. Deleting its volume loses the index and
the checkpoint. The raw archive preserves the verified bundle for every tick
that was archived; it can be replayed into a fresh ClickHouse volume. If the
archive is disabled, restarting from an empty volume starts at the current epoch
initial tick and earlier history is not recovered.

ClickHouse data is partitioned by epoch, so retention can drop old partitions
cheaply. Do not delete individual rows. Back up at the volume level or with
ClickHouse's backup tooling while the indexer is paused, and test a restore on a
copy before relying on it.

### Backups and recovery

The helper scripts preserve named volumes on `docker compose down`; only
`--wipe` deletes them. For a cold backup:

```bash
docker compose stop indexer api
docker run --rm \
  -v glyph_clickhouse-data:/data:ro \
  -v "$PWD/backup:/backup" \
  alpine tar -C /data -czf /backup/clickhouse-data.tgz .
docker compose start api indexer
```

A filesystem copy of a running ClickHouse instance is not guaranteed to be
consistent. Stop writes first, or use ClickHouse's `BACKUP` command with a
configured backup disk. This Compose file does not configure one.

Recovery options, in order of preference:

1. Restore the ClickHouse volume from a cold backup.
2. Restore an epoch partition if you have a per-partition backup.
3. Rebuild from the raw archive when it is enabled.
   Manifest the range first, then dry-run and replay:

   ```bash
   glyph archive-manifest --epoch "$EPOCH" --from "$FROM" --to "$TO" --verify
   glyph archive-replay --epoch "$EPOCH" --from "$FROM" --to "$TO" --dry-run
   glyph archive-replay --epoch "$EPOCH" --from "$FROM" --to "$TO"
   ```

   Replay requires bundles archived with `ARCHIVE_VOTES=true` and the epoch
   computor sidecar. The command reconstructs each `TickBundle`, verifies quorum
   and data digests, and writes through the normal indexer path.
4. Re-index from peers. The indexer resumes from the checkpoint or, if the
   checkpoint is gone, from the maximum indexed tick. History before the
   current epoch is not recoverable without the archive.

## Day-to-day operations

```bash
# Start or reconcile the stack. Rebuilds the image unless --no-build is set.
scripts/docker-up.sh

# Start only the storage, migration, and API, without the indexer.
scripts/docker-up.sh --no-indexer

# Reuse an existing image and skip compilation.
scripts/docker-up.sh --no-build

# Skip the health wait and only start the containers.
scripts/docker-up.sh --no-wait

# Use two peers and a longer health timeout.
scripts/docker-up.sh --peer 1.2.3.4:21841 --peer 5.6.7.8:21841 --timeout 300

# Pass extra arguments after `--` to `docker compose up`.
scripts/docker-up.sh -- --force-recreate

# Status and logs.
docker compose ps
docker compose logs -f api
docker compose logs -f indexer
docker compose logs -f clickhouse

# ClickHouse client inside the network.
docker compose exec clickhouse clickhouse-client --database glyph

# Confirm bucket creation and credentials.
docker compose run --rm minio-init

# Restart one process without touching the others.
docker compose restart api

# Re-run the idempotent schema migration.
docker compose run --rm migrate

# Drop ClickHouse partitions older than the newest 54 epochs. Archive objects
# are never deleted by this command. Add --dry-run to list what would be dropped.
docker compose run --rm indexer prune --keep 54

# Stop. --wipe deletes volumes; --images removes the local image.
scripts/docker-down.sh
scripts/docker-down.sh --wipe
```

The API listens on `http://127.0.0.1:8080` by default. The OpenAPI document is
at `/openapi.json`. Examples:

```bash
curl -sS localhost:8080/healthz
curl -sS localhost:8080/v1/status
curl -sS 'localhost:8080/v1/ticks?limit=5'
```

### Smoke test

With the stack up:

```bash
scripts/smoke.sh
```

This checks ClickHouse, API health, `/v1/status`, a tick lookup when a head
exists, and a live peer head and tick fetch. To check only the containerized
API and store:

```bash
API_URL=http://127.0.0.1:8080 scripts/smoke.sh --skip-peer
```

To use a different host port:

```bash
API_PORT=9090 scripts/docker-up.sh
API_URL=http://127.0.0.1:9090 scripts/smoke.sh --skip-peer
```

## Upgrades

The schema is additive and idempotent. `migrate` runs on every start before the
API or indexer, so a plain rebuild is usually enough:

```bash
git pull
scripts/docker-up.sh
```

The helper runs `docker compose up -d --build`, which recreates changed
containers. Compose stops the old indexer before starting the new one; the
checkpoint makes that safe. For a controlled rollout:

1. Build and tag the new image without restarting services.
2. Run `docker compose run --rm migrate`.
3. Recreate the indexer, then the API. Do not restart the indexer for API-only
   changes.
4. Keep the previous image tag available for rollback. Additive schema changes
   let an older binary read the current schema.

For a non-additive schema migration, stop the indexer first, run the migration,
then start the new indexer and API. See
[Deployment](ARCHITECTURE.md#deployment).

## Scaling the API

The API is stateless and reads ClickHouse. This Compose file publishes a fixed
host port, so `docker compose up -d --scale api=3` fails with a port collision.
For more than one replica on one host, remove or parameterize the `ports` block
for `api` and place a reverse proxy on the `glyph` network in front of the
replicas. Then:

```bash
docker compose up -d --scale api=3
```

The indexer must remain a single replica. Multiple indexers would race on the
checkpoint and duplicate writes, though writes are idempotent.

## Sizing and tuning

- ClickHouse memory: the container has no explicit memory limit here, so
  ClickHouse uses the server defaults. On a small host, cap the service with
  `mem_limit` and tune `max_server_memory_usage` through a mounted config. The
  development config in [`deploy/clickhouse/config.xml`](../deploy/clickhouse/config.xml)
  is for the non-Docker stack, not this image.
- Disk: keep 20 GB free per retained epoch plus headroom for merges. ClickHouse
  merges temporarily need extra space.
- Raw archive: budget separately from ClickHouse. One epoch of `TickData` and the full
  vote set is about 213 GiB uncompressed before Zstandard. Object storage is usually the
  better target than the ClickHouse volume.
- CPU: local SchnorrQ verification is CPU-bound. It is off by default because
  checking every vote caps catch-up near one tick per second on a small host.
  Enable it only when you need cryptographic evidence in addition to quorum
  agreement.
- Network: one peer is a single point of failure. Supply at least two addresses
  in `QUBIC_PEERS`; the indexer fails over across configured peers.
- Log rotation: service logs use the `json-file` driver with a 10 MB limit and
  three files per container. ClickHouse's own logs are in a volume.
- Image tags: `clickhouse/clickhouse-server:25.8` is pinned. `chrislusf/seaweedfs`
  and `amazon/aws-cli` use `latest`; pin release tags for reproducible production
  builds.

## Security

This Compose file is a single-host deployment, not a hardened multi-tenant
platform.

- The API is read-only by default. `POST /v1/transactions` and
  `POST /v1/mempool` are disabled unless `ALLOW_BROADCAST=true`; live routes
  only read peer state. It has permissive CORS, no API keys, and no rate limit
  by default. Put it behind an authenticating reverse proxy if it is not on a
  trusted network.
- Webhook targets are client-supplied URLs. The dispatcher rejects non-HTTP(S)
  schemes and private or loopback addresses unless `WEBHOOK_ALLOW_PRIVATE=true`.
  Leave that setting `false` on any public deployment. Webhook secrets sign
  deliveries and must be treated as credentials; they are never returned by the
  API.
- ClickHouse is published only on `127.0.0.1` and uses the credentials embedded
  in `CLICKHOUSE_URL`. The HTTP connection is not TLS inside the Docker network.
  Never expose port `8123` publicly.
- S3 ports are published only on `127.0.0.1`. Change `S3_SECRET_KEY` and
  `CLICKHOUSE_PASSWORD` before putting the host on an untrusted network.
- Peer traffic is outbound only. Peer data is untrusted and is verified before
  it is stored.
- The container runs as a non-root user (`uid 10001`).
- Compose secrets are environment variables. Prefer your platform's secret
  store or a Docker secret for real credentials. Do not commit `.env`.
- The stack does not terminate TLS. Put a TLS reverse proxy in front of the API
  if it is reachable from an untrusted network.

## Troubleshooting

| Symptom | Check |
| --- | --- |
| Image build fails on a missing entrypoint | `deploy/docker/glyph-entrypoint.sh` must be in the build context. `.dockerignore` excludes `deploy/clickhouse` but not `deploy/docker`. |
| Build fails with `let expressions in this position are unstable` | The builder toolchain is older than Rust 1.88. Keep `ARG RUST_VERSION=1.88` or newer; `object_store 0.13` uses let-chains. |
| `api` restarts or never becomes healthy | `docker compose logs api`; check ClickHouse health and that `migrate` exited `0`. |
| `migrate` fails | `docker compose logs migrate`; ClickHouse may not be ready or the database name may be invalid. |
| Indexer restart loops | `docker compose logs indexer`; confirm `QUBIC_PEERS` addresses are reachable and use port `21841` (or `21842` for Bob peers). |
| API returns 500 on `/v1/status` | ClickHouse is down or the schema is missing. Run `docker compose run --rm migrate`. |
| No indexed head after a few minutes | Check the indexer log. The first tick comes from the current epoch; a stale or unreachable peer fails over. Try another `--peer`. |
| `s3-init` fails with a credential error | `S3_ACCESS_KEY` and `S3_SECRET_KEY` are written into the shared SeaweedFS config; check the `s3-config` service logs. |
| Port already allocated | Another local stack or the dev scripts own `8080`, `8123`, or `8333`. Stop it or change `API_PORT`, `CLICKHOUSE_HTTP_PORT`, `S3_PORT`. |
| API reachable inside the container but not from the host | Keep `BIND=0.0.0.0:8080` and change the host side with `API_HOST`/`API_PORT`. |
| Disk fills up | Drop old epoch partitions or add disk. Do not delete ClickHouse files under a running server. |

The non-Docker workflow (`scripts/dev-up.sh`, `scripts/dev-down.sh`, and a local
ClickHouse binary) remains available for development. The Docker stack and the
host stack must not run at the same time with default ports.

## Bare-metal systemd + Caddy

Production can run the same two processes without Docker. This is the intended
layout for a small dedicated host:

| Path | Contents |
| --- | --- |
| `/opt/glyph/glyph`, `/opt/glyph/glyph-api` | Release binaries |
| `/etc/glyph/api.env`, `/etc/glyph/indexer.env` | Environment files from `deploy/systemd/` |
| `/var/lib/glyph` | Optional filesystem archive |
| `/etc/systemd/system/glyph-*.service` | Units from `deploy/systemd/` |

Build both binaries with the pinned toolchain and install them as an
unprivileged `glyph` user:

```bash
cargo build --release --locked -p cli -p api
sudo useradd --system --home /var/lib/glyph \
  --shell /usr/sbin/nologin glyph 2>/dev/null || true
sudo install -d -o glyph -g glyph /opt/glyph /etc/glyph /var/lib/glyph
sudo install -m 0755 target/release/glyph target/release/glyph-api /opt/glyph/
sudo install -m 0640 -o root -g glyph \
  deploy/systemd/api.env.example /etc/glyph/api.env
sudo install -m 0640 -o root -g glyph \
  deploy/systemd/indexer.env.example /etc/glyph/indexer.env
sudo install -m 0644 deploy/systemd/glyph-api.service \
  deploy/systemd/glyph-indexer.service /etc/systemd/system/
sudo systemctl daemon-reload
sudo systemctl enable --now clickhouse-server glyph-indexer glyph-api
```

Edit both env files first. `QUBIC_PEERS` is a comma-separated failover list;
`BIND` stays on `127.0.0.1:8080` so only the reverse proxy is public. The API
creates or migrates its own ClickHouse schema at startup.

Terminate TLS with Caddy. `deploy/Caddyfile` proxies `api.glyphq.org` to the
loopback listener, enables compression and HSTS, and writes a rotated access
log:

```bash
sudo cp deploy/Caddyfile /etc/caddy/Caddyfile
sudo systemctl reload caddy
curl -fsS https://api.glyphq.org/healthz
```

The DNS record for `api.glyphq.org` must point at the host before Caddy can
obtain a certificate. Ports 80 and 443 must be reachable from the internet.

Operational checks:

```bash
systemctl status glyph-indexer glyph-api
journalctl -u glyph-indexer -f
curl -fsS http://127.0.0.1:8080/healthz
curl -fsS https://api.glyphq.org/v1/status | jq .data.head
```

The indexer is the only writer. Never run two indexers against the same
ClickHouse database. To upgrade, replace the binaries and restart in order:
`glyph-indexer`, then `glyph-api`.


## Cloudflare cache rules

The API already sends `Cache-Control` on public reads. Cloudflare does not cache
JSON by default, so a Cache Rule is required. The rule below caches only GET
requests under `/v1/`, respects the origin TTL, and bypasses streams,
WebSockets, live peer reads, and webhook administration:

```text
(http.request.method eq "GET"
 and starts_with(http.request.uri.path, "/v1/")
 and not starts_with(http.request.uri.path, "/v1/stream")
 and not starts_with(http.request.uri.path, "/v1/ws")
 and not starts_with(http.request.uri.path, "/v1/live/")
 and not starts_with(http.request.uri.path, "/v1/webhooks"))
```

Action parameters:

```json
{
  "cache": true,
  "edge_ttl": { "mode": "respect_origin" },
  "browser_ttl": { "mode": "respect_origin" }
}
```

With `edge_ttl` and `browser_ttl` set to `respect_origin`, the API controls
freshness. Current policies:

| Class | Header |
| --- | --- |
| Recent reads | `public, max-age=2, stale-while-revalidate=5` |
| Aggregates | `public, max-age=30, stale-while-revalidate=60` |
| Immutable reads | `public, max-age=31536000, immutable` |
| Status and errors | `no-store` |

Verify the rule with:

```bash
curl -sSI https://api.glyphq.org/v1/transactions?limit=50 | grep -i cf-cache-status
curl -sSI https://api.glyphq.org/v1/status | grep -i cf-cache-status
```

The first request should be `MISS`, the next within two seconds `HIT`, and
status should stay `BYPASS`.

## Continuous deployment

`.github/workflows/deploy.yml` deploys every push to `main`:

1. Build the runtime image with BuildKit and a GitHub Actions layer cache.
2. Smoke-test the image with `glyph --version`.
3. `docker save | gzip` the image and copy it to the server over SSH.
4. Load the image and run `bash deploy/docker/run-stack.sh up`.
   The API container now has a Docker healthcheck against `/healthz`.
5. Poll `https://api.glyphq.org/healthz` until it returns `200`.
6. If any step fails, retag the image that was live before the deploy and
   rerun `run-stack.sh up`. This restores the previous API and indexer
   without a manual build.

The workflow needs these repository secrets:

| Secret | Example | Meaning |
| --- | --- | --- |
| `DEPLOY_HOST` | `deploy.example.com` | Server address |
| `DEPLOY_USER` | `alexgeminilabs` | SSH user in the `docker` group |
| `DEPLOY_PATH` | `/home/alexgeminilabs/glyph` | Writable deployment directory |
| `DEPLOY_SSH_KEY` | private key text | Key authorized for `DEPLOY_USER` |
| `HEALTHCHECK_URL` | `https://api.glyphq.org/healthz` | Optional public check |

Set them with the GitHub CLI:

```bash
gh secret set DEPLOY_HOST --body 203.0.113.10
gh secret set DEPLOY_USER --body deploy
gh secret set DEPLOY_PATH --body /home/deploy/glyph
gh secret set HEALTHCHECK_URL --body https://api.glyphq.org/healthz
gh secret set DEPLOY_SSH_KEY < deploy_key
```

The server needs Docker and outbound access to Docker Hub and Qubic peers. The
first deployment must be run once by hand:

```bash
cd "$DEPLOY_PATH"
bash deploy/docker/run-stack.sh up
```

### Hosts without an executable home

GCP Container-Optimized OS mounts `/home` and `/mnt/stateful_partition` with
`noexec`, and the root filesystem is read-only. The Compose plugin cannot run
there. `deploy/docker/run-stack.sh` is the supported path: it uses plain
`docker run`, keeps data in named volumes, and only needs the source directory
for `deploy/Caddyfile.docker` and `deploy/docker/run-stack.sh`.

Always invoke it as `bash deploy/docker/run-stack.sh ...`; do not rely on the
script's executable bit on a `noexec` mount.

The stack exposes:

| Port | Scope | Service |
| --- | --- | --- |
| `80`, `443` | Public | Caddy TLS termination |
| `8080` | Loopback | API |
| `8123` | Loopback | ClickHouse HTTP |
| `8333` | Loopback | SeaweedFS S3 |

Open GCP firewall ports `80` and `443` before the first Caddy start. Caddy
obtains a Let's Encrypt certificate for `api.glyphq.org`; the DNS A record must
already point at the host.
