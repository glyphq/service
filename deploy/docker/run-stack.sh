#!/usr/bin/env bash
#
# Plain `docker run` deployment for hosts that cannot execute the Compose
# plugin, such as GCP Container-Optimized OS where writable filesystems are
# mounted noexec.
#
# Usage:
#   deploy/docker/run-stack.sh up
#   deploy/docker/run-stack.sh status
#   deploy/docker/run-stack.sh logs indexer
#   deploy/docker/run-stack.sh down
#
# Data lives in named Docker volumes and survives container replacement.
set -euo pipefail

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "$repo_root"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

network="${GLYPH_NETWORK:-glyph}"
clickhouse="${CLICKHOUSE_DB:-glyph}"
clickhouse_user="${CLICKHOUSE_USER:-glyph}"
clickhouse_password="${CLICKHOUSE_PASSWORD:-}"
if [[ -n "$clickhouse_password" ]]; then
  clickhouse_url="http://$clickhouse_user:$clickhouse_password@glyph-clickhouse:8123"
  clickhouse_auth=(--user "$clickhouse_user" --password "$clickhouse_password")
else
  clickhouse_url="http://glyph-clickhouse:8123"
  clickhouse_auth=()
fi
peers="${QUBIC_PEERS:-157.180.10.49:21841}"
image="${GLYPH_IMAGE:-glyph:local}"
api_host="${API_HOST:-127.0.0.1}"
api_port="${API_PORT:-8080}"
s3_user="${S3_ACCESS_KEY:-glyph}"
s3_password="${S3_SECRET_KEY:-glyph-dev-secret}"
s3_bucket="${S3_BUCKET:-glyph-archive}"
s3_config="$repo_root/.data/seaweedfs/s3.json"
log_opts=(--log-opt max-size=10m --log-opt max-file=3)

app_env=(
  -e "CLICKHOUSE_URL=$clickhouse_url"
  -e "CLICKHOUSE_DB=$clickhouse"
  -e "QUBIC_PEERS=$peers"
  -e "S3_ENDPOINT=http://glyph-seaweedfs:8333"
  -e "S3_BUCKET=$s3_bucket"
  -e "S3_ACCESS_KEY=$s3_user"
  -e "S3_SECRET_KEY=$s3_password"
  -e "S3_REGION=${S3_REGION:-us-east-1}"
  -e "ARCHIVE_FAIL_CLOSED=${ARCHIVE_FAIL_CLOSED:-false}"
  -e "ALLOW_BROADCAST=${ALLOW_BROADCAST:-false}"
  -e "BROADCAST_PEERS=${BROADCAST_PEERS:-3}"
  -e "STREAM_POLL_MS=${STREAM_POLL_MS:-1000}"
  -e "PRICE_POLL_MS=${PRICE_POLL_MS:-60000}"
)

ensure_network() {
  docker network inspect "$network" >/dev/null 2>&1 ||
    docker network create "$network" >/dev/null
}

remove_container() {
  docker rm -f "$1" >/dev/null 2>&1 || true
}

wait_http() {
  local name="$1" url="$2" attempts="${3:-60}"
  for _ in $(seq 1 "$attempts"); do
    if docker exec "$name" curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  echo "timed out waiting for $name $url" >&2
  return 1
}

up() {
  ensure_network

  if ! docker image inspect "$image" >/dev/null 2>&1; then
    docker build -t "$image" .
  fi

  # Databases are created once and kept across application deploys.
  if ! docker container inspect glyph-clickhouse >/dev/null 2>&1; then
    docker run -d --name glyph-clickhouse --network "$network" \
      --restart unless-stopped "${log_opts[@]}" \
      -e "CLICKHOUSE_DB=$clickhouse" \
      -e "CLICKHOUSE_USER=$clickhouse_user" \
      -e "CLICKHOUSE_PASSWORD=$clickhouse_password" \
      -p "127.0.0.1:8123:8123" \
      --ulimit nofile=262144:262144 \
      -v glyph-clickhouse-data:/var/lib/clickhouse \
      -v glyph-clickhouse-logs:/var/log/clickhouse-server \
      clickhouse/clickhouse-server:25.8 >/dev/null
  else
    docker start glyph-clickhouse >/dev/null
  fi

  install -d -m 0700 "$(dirname "$s3_config")"
  cat >"$s3_config" <<EOF
{"identities":[{"name":"glyph","credentials":[{"accessKey":"$s3_user","secretKey":"$s3_password"}],"actions":["Admin","Read","Write","List","Tagging"]}]}
EOF
  # SeaweedFS runs as an unprivileged container user.
  chmod 0644 "$s3_config"

  if ! docker container inspect glyph-seaweedfs >/dev/null 2>&1; then
    docker run -d --name glyph-seaweedfs --network "$network" \
      --restart unless-stopped "${log_opts[@]}" \
      -v glyph-seaweedfs-data:/data \
      -v "$s3_config:/etc/seaweedfs/s3.json:ro" \
      chrislusf/seaweedfs:latest \
      server -dir=/data -s3 -s3.port=8333 \
      -s3.config=/etc/seaweedfs/s3.json >/dev/null
  else
    docker start glyph-seaweedfs >/dev/null
  fi

  for _ in $(seq 1 60); do
    if docker exec glyph-clickhouse clickhouse-client "${clickhouse_auth[@]}" \
      --query 'SELECT 1' \
      >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  docker exec glyph-clickhouse clickhouse-client "${clickhouse_auth[@]}" \
    --query 'SELECT 1' >/dev/null

  for _ in $(seq 1 60); do
    if docker run --rm --network "$network" \
      -e "CLICKHOUSE_URL=$clickhouse_url" \
      -e "CLICKHOUSE_DB=$clickhouse" \
      "$image" glyph db-init >/dev/null 2>&1; then
      break
    fi
    sleep 2
  done
  docker run --rm --network "$network" \
    -e "CLICKHOUSE_URL=$clickhouse_url" \
    -e "CLICKHOUSE_DB=$clickhouse" \
    "$image" glyph db-init >/dev/null

  remove_container glyph-caddy
  remove_container glyph-api
  remove_container glyph-indexer

  for _ in $(seq 1 60); do
    if docker run --rm --network "$network" \
      -e "AWS_ACCESS_KEY_ID=$s3_user" \
      -e "AWS_SECRET_ACCESS_KEY=$s3_password" \
      -e "AWS_DEFAULT_REGION=${S3_REGION:-us-east-1}" \
      amazon/aws-cli:latest \
      --endpoint-url http://glyph-seaweedfs:8333 \
      s3api head-bucket --bucket "$s3_bucket" \
      >/dev/null 2>&1; then
      break
    fi
    docker run --rm --network "$network" \
      -e "AWS_ACCESS_KEY_ID=$s3_user" \
      -e "AWS_SECRET_ACCESS_KEY=$s3_password" \
      -e "AWS_DEFAULT_REGION=${S3_REGION:-us-east-1}" \
      amazon/aws-cli:latest \
      --endpoint-url http://glyph-seaweedfs:8333 \
      s3api create-bucket --bucket "$s3_bucket" >/dev/null 2>&1 || true
    sleep 2
  done

  docker run --rm --network "$network" \
    -e "AWS_ACCESS_KEY_ID=$s3_user" \
    -e "AWS_SECRET_ACCESS_KEY=$s3_password" \
    -e "AWS_DEFAULT_REGION=${S3_REGION:-us-east-1}" \
    amazon/aws-cli:latest \
    --endpoint-url http://glyph-seaweedfs:8333 \
    s3api head-bucket --bucket "$s3_bucket" >/dev/null

  docker run -d --name glyph-indexer --network "$network" \
    --restart unless-stopped "${log_opts[@]}" \
    --stop-signal SIGINT --stop-timeout 30 \
    "${app_env[@]}" \
    -e "RUST_LOG=${GLYPH_INDEXER_LOG:-info}" \
    -e "GLYPH_FROM_TICK=${GLYPH_FROM_TICK:-}" \
    -e "INDEXER_FETCH_CONCURRENCY=${INDEXER_FETCH_CONCURRENCY:-32}" \
    -e "INDEXER_FETCH_WINDOW=${INDEXER_FETCH_WINDOW:-512}" \
    -e "INDEXER_WRITE_BATCH=${INDEXER_WRITE_BATCH:-128}" \
    -e "INDEXER_WRITE_FLUSH_MS=${INDEXER_WRITE_FLUSH_MS:-250}" \
    -e "INDEXER_DISCOVER_PEERS=${INDEXER_DISCOVER_PEERS:-false}" \
    -e "GLYPH_VERIFY_SIGNATURES=${GLYPH_VERIFY_SIGNATURES:-true}" \
    "$image" indexer >/dev/null

  docker run -d --name glyph-api --network "$network" \
    --restart unless-stopped "${log_opts[@]}" \
    --stop-timeout 30 \
    "${app_env[@]}" \
    -e "BIND=0.0.0.0:8080" \
    -e "RUST_LOG=${GLYPH_API_LOG:-info,tower_http=info}" \
    -p "$api_host:$api_port:8080" \
    "$image" api >/dev/null

  wait_http glyph-api "http://127.0.0.1:8080/healthz" 45

  docker run -d --name glyph-caddy --network "$network" \
    --restart unless-stopped "${log_opts[@]}" \
    -p 80:80 -p 443:443 -p 443:443/udp \
    -v "$repo_root/deploy/Caddyfile.docker:/etc/caddy/Caddyfile:ro" \
    -v glyph-caddy-data:/data \
    -v glyph-caddy-config:/config \
    caddy:2 >/dev/null

  docker ps --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}'
}

down() {
  remove_container glyph-caddy
  remove_container glyph-api
  remove_container glyph-indexer
  remove_container glyph-seaweedfs
  remove_container glyph-clickhouse
}

case "${1:-up}" in
  up) up ;;
  down) down ;;
  status) docker ps -a --filter "name=glyph-" --format 'table {{.Names}}\t{{.Status}}\t{{.Ports}}' ;;
  logs) shift; docker logs -f --tail=200 "glyph-${1:-api}" ;;
  *)
    echo "usage: $0 {up|down|status|logs <api|indexer|clickhouse|caddy>}" >&2
    exit 2
    ;;
esac
