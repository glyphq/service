#!/usr/bin/env bash
#
# Local ClickHouse helper for Glyph development.
#
# Usage:
#   scripts/clickhouse.sh start|stop|status|run|client|wipe
#
# Environment:
#   CH_BIN           path to the clickhouse binary (default ~/.local/bin/clickhouse)
#   CH_DATA_DIR      data root (default <repo>/.data/clickhouse)
#   CLICKHOUSE_URL   HTTP endpoint used for health checks (default http://127.0.0.1:8123)
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CH_BIN="${CH_BIN:-$HOME/.local/bin/clickhouse}"
DATA_DIR="${CH_DATA_DIR:-$ROOT/.data/clickhouse}"
PID_FILE="$ROOT/.data/clickhouse.pid"
LOG_DIR="$ROOT/.data/logs"
LOG_FILE="$LOG_DIR/clickhouse.log"
CONFIG="$ROOT/deploy/clickhouse/config.xml"
HTTP_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"

require_bin() {
  if [[ ! -x "$CH_BIN" ]]; then
    echo "clickhouse binary not found at $CH_BIN" >&2
    echo "run scripts/install-clickhouse.sh first" >&2
    exit 1
  fi
}

prepare_dirs() {
  mkdir -p "$DATA_DIR"/{data,tmp,user_files,format_schemas}
  mkdir -p "$LOG_DIR"
  cp "$ROOT/deploy/clickhouse/users.xml" "$DATA_DIR/users.xml"
}

pid_from_file() {
  if [[ -f "$PID_FILE" ]]; then
    cat "$PID_FILE" 2>/dev/null || true
  fi
}

http_ok() {
  curl -fsS --max-time 2 "$HTTP_URL/ping" >/dev/null 2>&1
}

running() {
  local pid
  pid="$(pid_from_file)"
  if [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null; then
    return 0
  fi
  http_ok
}

wait_http() {
  local timeout="${1:-30}" i
  for ((i = 0; i < timeout; i++)); do
    if http_ok; then
      return 0
    fi
    sleep 1
  done
  return 1
}

find_clickhouse_pids() {
  # Match both relative and absolute --config-file paths, and include the
  # watchdog plus the server child it spawns.
  pgrep -f "clickhouse server --config-file=.*$(basename "$CONFIG")" 2>/dev/null || true
  if [[ -f "$ROOT/.data/clickhouse/pid" ]]; then
    cat "$ROOT/.data/clickhouse/pid" 2>/dev/null || true
  fi
}

cmd_start() {
  require_bin
  if running; then
    echo "clickhouse is already running at $HTTP_URL"
    return 0
  fi
  prepare_dirs
  cd "$ROOT"
  nohup "$CH_BIN" server --config-file="$CONFIG" >>"$LOG_FILE" 2>&1 &
  echo $! >"$PID_FILE"
  if wait_http 30; then
    echo "clickhouse started (pid $(cat "$PID_FILE"), http $HTTP_URL)"
    return 0
  fi
  echo "clickhouse did not become ready; last log lines:" >&2
  tail -n 20 "$LOG_FILE" >&2 || true
  exit 1
}

cmd_stop() {
  local pids pid i
  pids="$(find_clickhouse_pids | sort -u | tr '\n' ' ')"
  if [[ -z "${pids// /}" ]]; then
    echo "clickhouse is not running"
    rm -f "$PID_FILE"
    return 0
  fi
  # Stop the watchdog first so it does not restart the child.
  for pid in $pids; do
    kill -TERM "$pid" 2>/dev/null || true
  done
  for ((i = 0; i < 20; i++)); do
    if ! http_ok; then
      break
    fi
    sleep 1
  done
  for pid in $pids; do
    kill -KILL "$pid" 2>/dev/null || true
  done
  rm -f "$PID_FILE"
  echo "clickhouse stopped"
}

cmd_status() {
  if running; then
    local pid
    pid="$(pid_from_file)"
    if [[ -n "$pid" ]]; then
      echo "clickhouse: running (pid $pid, http $HTTP_URL)"
    else
      echo "clickhouse: running (pid unknown, http $HTTP_URL)"
    fi
  else
    echo "clickhouse: stopped"
    return 1
  fi
}

cmd_client() {
  require_bin
  exec "$CH_BIN" client --host 127.0.0.1 --port 9000 "$@"
}

cmd_wipe() {
  local confirm="${1:-}"
  if [[ "$confirm" != "--yes" ]]; then
    echo "refusing to wipe $DATA_DIR without --yes" >&2
    exit 1
  fi
  cmd_stop
  rm -rf "$DATA_DIR/data" "$DATA_DIR/tmp" "$DATA_DIR/user_files" "$DATA_DIR/format_schemas"
  rm -f "$DATA_DIR/users.xml"
  echo "wiped ClickHouse data under $DATA_DIR"
}

cmd_run() {
  require_bin
  prepare_dirs
  cd "$ROOT"
  exec "$CH_BIN" server --config-file="$CONFIG"
}

usage() {
  cat <<'EOF'
Usage: scripts/clickhouse.sh <command>

Commands:
  start    Start ClickHouse in the background and wait for HTTP readiness.
  stop     Stop the background ClickHouse process.
  status   Report whether ClickHouse is running (exit 1 when stopped).
  run      Run ClickHouse in the foreground.
  client   Open clickhouse-client against 127.0.0.1:9000.
  wipe     Stop ClickHouse and delete local data (requires --yes).

Environment:
  CH_BIN, CH_DATA_DIR, CLICKHOUSE_URL
EOF
}

main() {
  local cmd="${1:-start}"
  shift || true
  case "$cmd" in
    start) cmd_start ;;
    stop) cmd_stop ;;
    status) cmd_status ;;
    run) cmd_run ;;
    client) cmd_client "$@" ;;
    wipe) cmd_wipe "$@" ;;
    -h | --help | help) usage ;;
    *)
      echo "unknown command: $cmd" >&2
      usage >&2
      exit 2
      ;;
  esac
}

main "$@"
