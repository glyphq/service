# Deployment with Docker

This document describes the single-host Docker Compose deployment. It runs the
same two long-running processes as the development stack, with ClickHouse and
MinIO as dependencies:

| Process | Binary | Role |
| --- | --- | --- |
| API | `glyph-api` | HTTP API. Reads ClickHouse; optional peer-backed routes and broadcast connect directly to Qubic peers. |
| Indexer | `glyph sync` | Fetches and verifies peer ticks, writes ClickHouse. |
| ClickHouse | `clickhouse-server` | Shared analytical store and ingest checkpoint. |
| MinIO | `minio` | S3-compatible storage, provisioned for the raw archive. |

The deployment is intentionally a single host with one indexer. ClickHouse is a
single node and the indexer owns the write path. Scale API replicas only behind
a load balancer; see [Scaling the API](#scaling-the-api).

## Prerequisites

- Docker Engine 24 or newer with the Compose v2 plugin (`docker compose`).
- About 4 GB of RAM for ClickHouse, MinIO, the API, and the indexer together.
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
# Build the image, start ClickHouse, MinIO, the schema migration, the API, and
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

Stop and delete ClickHouse, MinIO, and log volumes. This is irreversible:

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
| `BIND` | `0.0.0.0:8080` | Listen address for `glyph-api` inside the container. Keep the port at `8080` unless you also change the `api` port mapping. |
| `API_HOST` | `127.0.0.1` | Host interface for the published API port. Set `0.0.0.0` to expose it. |
| `API_PORT` | `8080` | Host port for the API. |
| `QUBIC_PEERS` | `157.180.10.49:21841` | Comma-separated peer addresses used by the indexer and by peer-backed API routes. |
| `ALLOW_BROADCAST` | `false` | Set `true` to enable `POST /v1/transactions`. Keep disabled unless the API is trusted. |
| `STREAM_POLL_MS` | `1000` | Stream hub index-head poll interval. |
| `GLYPH_FROM_TICK` | unset | Optional first tick when there is no stored cursor. |
| `INDEXER_POLL_MS` | `750` | Head poll interval once caught up. |
| `INDEXER_PROGRESS_EVERY` | `100` | Log progress every N indexed ticks. |
| `GLYPH_VERIFY_SIGNATURES` | `true` | Set `false` to skip SchnorrQ checks. Keep enabled for live ingest. |
| `S3_ENDPOINT` | `http://minio:9000` | S3 endpoint inside the Compose network, passed to the archive client and `minio-init`. |
| `S3_BUCKET` | `glyph-archive` | Bucket created by `minio-init` and used by the archive client. |
| `S3_ACCESS_KEY` | `glyph` | MinIO root user and S3 access key. |
| `S3_SECRET_KEY` | `glyph-dev-secret` | MinIO root password and S3 secret key. Must be at least 8 characters. |
| `S3_REGION` | `us-east-1` | S3 region. MinIO accepts the default. |
| `ARCHIVE_FAIL_CLOSED` | `false` | Set `true` to stop ingestion when a bundle cannot be archived. |
| `MINIO_PORT` | `9000` | Host loopback port for the MinIO S3 API. |
| `MINIO_CONSOLE_PORT` | `9001` | Host loopback port for the MinIO console. |
| `GLYPH_IMAGE` | `glyph:local` | Image tag built and used by all glyph services. |
| `GLYPH_API_LOG` | `info,tower_http=info` | `RUST_LOG` for the API. |
| `GLYPH_INDEXER_LOG` | `info` | `RUST_LOG` for the indexer. |
| `GLYPH_MIGRATE_LOG` | `info` | `RUST_LOG` for the one-shot migration. |

`CLICKHOUSE_URL` and `S3_ENDPOINT` are resolved inside the glyph containers.
Keep the Compose service names (`clickhouse`, `minio`); `127.0.0.1` would point
at the container itself. Use `API_HOST`/`API_PORT` and the published ClickHouse
and MinIO ports to reach the stack from the host.

The `migrate` service runs `glyph db-init` on every start. The DDL is
`CREATE ... IF NOT EXISTS`, so it is idempotent and safe to rerun.

`minio-init` creates the archive bucket, enables versioning, and removes any
anonymous policy. It is also idempotent. The indexer writes one
zstd-compressed QBB1 bundle per verified tick before persisting that tick, using
the S3 variables from the shared application environment. The API exposes the
same objects through `GET /v1/ticks/{tick}/raw`.

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
| `clickhouse` | `clickhouse/clickhouse-server:26.11` | `127.0.0.1:${CLICKHOUSE_HTTP_PORT}` | `clickhouse-client --query "SELECT 1"` |
| `minio` | `minio/minio:latest` | `127.0.0.1:${MINIO_PORT}`, `127.0.0.1:${MINIO_CONSOLE_PORT}` | `GET /minio/health/live` |
| `minio-init` | `minio/mc:latest` | none, one-shot | exit status |
| `migrate` | local `glyph:local` image | none, one-shot | exit status |
| `api` | local `glyph:local` image | `${API_HOST}:${API_PORT}` | `GET /healthz` |
| `indexer` | local `glyph:local` image | none | process liveness (`kill -0 1`) |

Startup order:

1. ClickHouse becomes healthy.
2. `migrate` runs `glyph db-init` and exits successfully.
3. MinIO becomes healthy; `minio-init` creates and versions the archive bucket.
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
| `glyph_minio-data` | `/data` | Raw archive bucket objects. |

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
- CPU: signature verification is CPU-bound. Keep it enabled. Use
  `GLYPH_VERIFY_SIGNATURES=false` only for a deliberate bulk catch-up, then turn
  it back on for live ingest.
- Network: one peer is a single point of failure. Supply at least two addresses
  in `QUBIC_PEERS`; the indexer fails over across configured peers.
- Log rotation: service logs use the `json-file` driver with a 10 MB limit and
  three files per container. ClickHouse's own logs are in a volume.
- Image tags: `clickhouse/clickhouse-server:26.11` is pinned. `minio/minio` and
  `minio/mc` use `latest`; pin release tags for reproducible production builds.
  The MinIO healthcheck assumes `curl` exists in the server image.

## Security

This Compose file is a single-host deployment, not a hardened multi-tenant
platform.

- The API is read-only by default. `POST /v1/transactions` is disabled unless
  `ALLOW_BROADCAST=true`; peer-backed routes only read peer state. It has
  permissive CORS, no API keys, and no rate limit by default. Put it behind an
  authenticating reverse proxy if it is not on a trusted network.
- ClickHouse is published only on `127.0.0.1` and uses its default user with no
  password. The application does not currently support ClickHouse credentials.
  Never expose ports `8123` or `9000` publicly.
- MinIO ports are published only on `127.0.0.1`. Change `S3_SECRET_KEY` before
  putting the host on an untrusted network. The bucket is private;
  `minio-init` removes anonymous access.
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
| `minio-init` fails with a credential error | `S3_ACCESS_KEY` and `S3_SECRET_KEY` must be identical for `minio` and `minio-init`; the secret must be at least 8 characters. |
| Port already allocated | Another local stack or the dev scripts own `8080`, `8123`, `9000`, or `9001`. Stop it or change `API_PORT`, `CLICKHOUSE_HTTP_PORT`, `MINIO_PORT`, `MINIO_CONSOLE_PORT`. |
| API reachable inside the container but not from the host | Keep `BIND=0.0.0.0:8080` and change the host side with `API_HOST`/`API_PORT`. |
| Disk fills up | Drop old epoch partitions or add disk. Do not delete ClickHouse files under a running server. |

The non-Docker workflow (`scripts/dev-up.sh`, `scripts/dev-down.sh`, and a local
ClickHouse binary) remains available for development. The Docker stack and the
host stack must not run at the same time with default ports.
