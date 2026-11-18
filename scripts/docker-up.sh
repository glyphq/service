#!/usr/bin/env bash
#
# Build and start the Docker Compose stack.
#
# Usage:
#   scripts/docker-up.sh [options] [-- <compose up args>]
#
# Options:
#   --no-build       Reuse the existing glyph:local image.
#   --no-wait        Do not wait for healthchecks after starting.
#   --no-indexer     Start ClickHouse, S3, migration, and the API only.
#   --peer ADDR      Add an indexer peer. Repeat for failover; overrides
#                    QUBIC_PEERS from the environment or .env.
#   --timeout SECS   Healthcheck wait budget (default 300).
#   -h, --help       Show this help.
#
# Environment:
#   GLYPH_ENV_FILE    Explicit Compose env file. Defaults to ./.env when present.
#   GLYPH_WAIT_TIMEOUT  Seconds to wait for healthchecks (default 300).
#
# This script never invokes a destructive operation. `scripts/docker-down.sh`
# stops the stack; add --wipe there to delete the named volumes.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.yml"
BUILD=1
WAIT=1
START_INDEXER=1
TIMEOUT="${GLYPH_WAIT_TIMEOUT:-300}"
PEERS=()
EXTRA=()

usage() {
  cat <<'EOF'
Build and start the Docker Compose stack.

Usage:
  scripts/docker-up.sh [options] [-- <compose up args>]

Options:
  --no-build       Reuse the existing glyph:local image.
  --no-wait        Do not wait for healthchecks after starting.
  --no-indexer     Start ClickHouse, S3, migration, and the API only.
  --peer ADDR      Add an indexer peer. Repeat for failover; overrides
                   QUBIC_PEERS from the environment or .env.
  --timeout SECS   Healthcheck wait budget (default 300).
  -h, --help       Show this help.

Environment:
  GLYPH_ENV_FILE      Explicit Compose env file. Defaults to ./.env when present.
  GLYPH_WAIT_TIMEOUT  Seconds to wait for healthchecks (default 300).

This script never invokes a destructive operation. scripts/docker-down.sh
stops the stack; add --wipe there to delete the named volumes.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --no-build)
      BUILD=0
      shift
      ;;
    --no-wait)
      WAIT=0
      shift
      ;;
    --no-indexer)
      START_INDEXER=0
      shift
      ;;
    --peer)
      PEERS+=("${2:?--peer requires an address}")
      shift 2
      ;;
    --peer=*)
      PEERS+=("${1#--peer=}")
      shift
      ;;
    --timeout)
      TIMEOUT="${2:?--timeout requires seconds}"
      shift 2
      ;;
    --timeout=*)
      TIMEOUT="${1#--timeout=}"
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    --)
      shift
      EXTRA+=("$@")
      break
      ;;
    *)
      echo "unknown argument: $1 (pass compose arguments after --)" >&2
      exit 2
      ;;
  esac
done

if ! [[ "$TIMEOUT" =~ ^[0-9]+$ ]] || [[ "$TIMEOUT" -lt 1 ]]; then
  echo "--timeout must be a positive number of seconds" >&2
  exit 2
fi

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not on PATH" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose v2 is required (the 'docker compose' plugin)" >&2
  exit 1
fi
if ! docker info >/dev/null 2>&1; then
  echo "cannot reach the Docker daemon; check that it is running and permitted for $USER" >&2
  exit 1
fi
if [[ ! -f "$COMPOSE_FILE" ]]; then
  echo "compose file not found: $COMPOSE_FILE" >&2
  exit 1
fi

COMPOSE=(docker compose -f "$COMPOSE_FILE")
ENV_FILE="${GLYPH_ENV_FILE:-}"
if [[ -n "$ENV_FILE" ]]; then
  if [[ ! -f "$ENV_FILE" ]]; then
    echo "GLYPH_ENV_FILE does not exist: $ENV_FILE" >&2
    exit 1
  fi
elif [[ -f "$ROOT/.env" ]]; then
  ENV_FILE="$ROOT/.env"
fi
if [[ -n "$ENV_FILE" ]]; then
  COMPOSE+=(--env-file "$ENV_FILE")
  echo "using env file: $ENV_FILE"
fi

if [[ ${#PEERS[@]} -gt 0 ]]; then
  QUBIC_PEERS="$(IFS=,; echo "${PEERS[*]}")"
  export QUBIC_PEERS
  echo "using peers: $QUBIC_PEERS"
fi

UP=("${COMPOSE[@]}" up -d)
if [[ "$BUILD" -eq 1 ]]; then
  UP+=(--build)
fi
if [[ "$START_INDEXER" -eq 0 ]]; then
  UP+=(--scale indexer=0)
  UP+=(clickhouse s3-config seaweedfs s3-init migrate api)
fi
UP+=("${EXTRA[@]}")

echo "==> docker compose up"
"${UP[@]}"

wait_healthy() {
  local service="$1"
  local deadline=$((SECONDS + TIMEOUT))
  local id status

  while ((SECONDS < deadline)); do
    id="$("${COMPOSE[@]}" ps -q "$service" 2>/dev/null | head -n 1 || true)"
    if [[ -n "$id" ]]; then
      status="$(docker inspect \
        --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' \
        "$id" 2>/dev/null || true)"
      case "$status" in
        healthy)
          return 0
          ;;
        exited | dead)
          echo "$service exited before becoming healthy" >&2
          return 1
          ;;
      esac
    fi
    sleep 2
  done
  return 1
}

if [[ "$WAIT" -eq 1 ]]; then
  echo
  echo "waiting up to ${TIMEOUT}s for healthchecks"
  wait_services=(api)
  if [[ "$START_INDEXER" -eq 1 ]]; then
    wait_services+=(indexer)
  fi
  for service in "${wait_services[@]}"; do
    if wait_healthy "$service"; then
      echo "  $service: healthy"
    else
      echo "$service did not become healthy within ${TIMEOUT}s" >&2
      echo "--- compose status ---" >&2
      "${COMPOSE[@]}" ps >&2 || true
      echo "--- $service logs ---" >&2
      "${COMPOSE[@]}" logs --tail=40 "$service" >&2 || true
      exit 1
    fi
  done
fi

echo
echo "stack status"
"${COMPOSE[@]}" ps

api_port="$("${COMPOSE[@]}" port api 8080 2>/dev/null | tail -n 1 || true)"
if [[ -n "$api_port" ]]; then
  api_host="${api_port%:*}"
  api_port="${api_port##*:}"
  case "$api_host" in
    0.0.0.0 | "[::]" | "::") api_host="127.0.0.1" ;;
  esac
  echo
  echo "api:        http://$api_host:$api_port"
  echo "health:     http://$api_host:$api_port/healthz"
fi
echo "clickhouse: http://127.0.0.1:${CLICKHOUSE_HTTP_PORT:-8123}"
echo "s3:         http://127.0.0.1:${S3_PORT:-8333}"
if [[ "$START_INDEXER" -eq 0 ]]; then
  echo "indexer:    not started (--no-indexer)"
fi
echo "logs:       ${COMPOSE[*]} logs -f"
echo "stop:       scripts/docker-down.sh"
