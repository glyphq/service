#!/usr/bin/env bash
#
# Start the local Glyph development stack without Docker.
#
# Starts ClickHouse, applies the schema, builds and starts the API, and starts
# the indexer. Process ids and logs live under .data/.
#
# Usage:
#   scripts/dev-up.sh [--no-indexer] [--peer ADDR] [--bind ADDR]
#
# Environment:
#   GLYPH_PEER        default peer address (default 157.180.10.49:21841)
#   BIND              API listen address (default 127.0.0.1:8080)
#   CLICKHOUSE_URL    ClickHouse HTTP endpoint (default http://127.0.0.1:8123)
#   CLICKHOUSE_DB     database name (default glyph)
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/.data/run"
LOG_DIR="$ROOT/.data/logs"
API_PID_FILE="$RUN_DIR/api.pid"
INDEXER_PID_FILE="$RUN_DIR/indexer.pid"

PEER="${GLYPH_PEER:-157.180.10.49:21841}"
BIND="${BIND:-127.0.0.1:8080}"
CH_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
CH_DB="${CLICKHOUSE_DB:-glyph}"
START_INDEXER=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --with-indexer)
      START_INDEXER=1
      shift
      ;;
    --no-indexer)
      START_INDEXER=0
      shift
      ;;
    --peer)
      PEER="${2:?--peer requires an address}"
      shift 2
      ;;
    --peer=*)
      PEER="${1#--peer=}"
      shift
      ;;
    --bind)
      BIND="${2:?--bind requires an address}"
      shift 2
      ;;
    --bind=*)
      BIND="${1#--bind=}"
      shift
      ;;
    -h | --help)
      sed -n '2,16p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

mkdir -p "$RUN_DIR" "$LOG_DIR"

# 0.0.0.0 and :: are valid listen addresses but not valid curl targets.
case "$BIND" in
  0.0.0.0:*) API_PROBE="127.0.0.1:${BIND#0.0.0.0:}" ;;
  "[::]:"*) API_PROBE="[::1]:${BIND#\[::\]:}" ;;
  "::"*) API_PROBE="[::1]:${BIND#::}" ;;
  *) API_PROBE="$BIND" ;;
esac
API_URL="http://$API_PROBE"

pid_alive() {
  local file="$1"
  [[ -f "$file" ]] && kill -0 "$(cat "$file")" 2>/dev/null
}

wait_http() {
  local url="$1" timeout="${2:-30}" i
  for ((i = 0; i < timeout; i++)); do
    if curl -fsS --max-time 2 "$url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  return 1
}

echo "==> ClickHouse"
"$ROOT/scripts/clickhouse.sh" start

echo "==> Schema"
(
  cd "$ROOT"
  CLICKHOUSE_URL="$CH_URL" CLICKHOUSE_DB="$CH_DB" \
    cargo run --quiet -p cli --bin glyph -- db-init
)

echo "==> Build API"
(
  cd "$ROOT"
  cargo build --quiet -p api --bin glyph-api
)

if pid_alive "$API_PID_FILE" || wait_http "$API_URL/healthz" 1; then
  echo "api already running at http://$BIND"
else
  echo "==> Start API"
  (
    cd "$ROOT"
    BIND="$BIND" CLICKHOUSE_URL="$CH_URL" CLICKHOUSE_DB="$CH_DB" \
      nohup "$ROOT/target/debug/glyph-api" >>"$LOG_DIR/api.log" 2>&1 &
    echo $! >"$API_PID_FILE"
  )
  if ! wait_http "$API_URL/healthz" 30; then
    echo "api did not become ready; last log lines:" >&2
    tail -n 30 "$LOG_DIR/api.log" >&2 || true
    exit 1
  fi
  echo "api started (pid $(cat "$API_PID_FILE"), http://$BIND)"
fi

if [[ "$START_INDEXER" -eq 1 ]]; then
  echo "==> Build indexer"
  (
    cd "$ROOT"
    cargo build --quiet -p cli --bin glyph
  )
  if pid_alive "$INDEXER_PID_FILE"; then
    echo "indexer already running (pid $(cat "$INDEXER_PID_FILE"))"
  else
    echo "==> Start indexer against $PEER"
    (
      cd "$ROOT"
      CLICKHOUSE_URL="$CH_URL" CLICKHOUSE_DB="$CH_DB" \
        nohup "$ROOT/target/debug/glyph" sync --peer "$PEER" >>"$LOG_DIR/indexer.log" 2>&1 &
      echo $! >"$INDEXER_PID_FILE"
    )
    sleep 2
    if pid_alive "$INDEXER_PID_FILE"; then
      echo "indexer started (pid $(cat "$INDEXER_PID_FILE"), log $LOG_DIR/indexer.log)"
    else
      # The sync loop is still being wired in early milestones. Keep ClickHouse
      # and the API up so the rest of the stack remains usable.
      echo "warning: indexer exited during startup; last log lines:" >&2
      tail -n 30 "$LOG_DIR/indexer.log" >&2 || true
      rm -f "$INDEXER_PID_FILE"
    fi
  fi
else
  echo "indexer not started (--no-indexer)"
fi

echo
echo "ready:"
echo "  api:        $API_URL/healthz"
echo "  clickhouse: $CH_URL"
echo "  logs:       $LOG_DIR"
echo "stop with:   scripts/dev-down.sh"
