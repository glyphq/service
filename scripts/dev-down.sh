#!/usr/bin/env bash
#
# Stop the local Glyph development stack.
#
# Usage:
#   scripts/dev-down.sh [--wipe]
#
# --wipe also deletes local ClickHouse data. Logs and pid files are kept.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="$ROOT/.data/run"
API_PID_FILE="$RUN_DIR/api.pid"
INDEXER_PID_FILE="$RUN_DIR/indexer.pid"
WIPE=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wipe)
      WIPE=1
      shift
      ;;
    -h | --help)
      sed -n '2,12p' "$0"
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

stop_pid_file() {
  local file="$1" name="$2" pid i
  if [[ ! -f "$file" ]]; then
    echo "$name: not running"
    return 0
  fi
  pid="$(cat "$file")"
  if ! kill -0 "$pid" 2>/dev/null; then
    echo "$name: not running"
    rm -f "$file"
    return 0
  fi
  kill -TERM "$pid" 2>/dev/null || true
  for ((i = 0; i < 20; i++)); do
    if ! kill -0 "$pid" 2>/dev/null; then
      break
    fi
    sleep 1
  done
  kill -KILL "$pid" 2>/dev/null || true
  rm -f "$file"
  echo "$name: stopped"
}

echo "==> Stop indexer"
stop_pid_file "$INDEXER_PID_FILE" "indexer"
echo "==> Stop api"
stop_pid_file "$API_PID_FILE" "api"
echo "==> Stop ClickHouse"
if [[ "$WIPE" -eq 1 ]]; then
  "$ROOT/scripts/clickhouse.sh" wipe --yes
else
  "$ROOT/scripts/clickhouse.sh" stop
fi

echo "done"
