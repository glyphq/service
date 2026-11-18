#!/usr/bin/env bash
#
# Smoke test for a running Glyph development stack.
#
# Checks ClickHouse, the HTTP API, and (unless skipped) a live peer head and
# tick fetch through the CLI. Exits non-zero on the first failed check group.
#
# Usage:
#   scripts/smoke.sh [--peer ADDR] [--api URL] [--skip-peer] [--skip-api]
#
# Environment:
#   GLYPH_PEER, API_URL, CLICKHOUSE_URL, GLYPH_BIN
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PEER="${GLYPH_PEER:-157.180.10.49:21841}"
API_URL="${API_URL:-http://127.0.0.1:8080}"
CH_URL="${CLICKHOUSE_URL:-http://127.0.0.1:8123}"
GLYPH_BIN="${GLYPH_BIN:-$ROOT/target/debug/glyph}"
SKIP_PEER=0
SKIP_API=0
FAILURES=0
tmp=""
head_json=""
tick_json=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --peer)
      PEER="${2:?--peer requires an address}"
      shift 2
      ;;
    --peer=*)
      PEER="${1#--peer=}"
      shift
      ;;
    --api)
      API_URL="${2:?--api requires a URL}"
      shift 2
      ;;
    --api=*)
      API_URL="${1#--api=}"
      shift
      ;;
    --skip-peer)
      SKIP_PEER=1
      shift
      ;;
    --skip-api)
      SKIP_API=1
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

pass() { printf 'ok   %s\n' "$1"; }
fail() {
  printf 'FAIL %s\n' "$1" >&2
  FAILURES=$((FAILURES + 1))
}
skip() { printf 'skip %s\n' "$1"; }

json_field() {
  local file="$1" field="$2"
  if command -v jq >/dev/null 2>&1; then
    jq -r "${field} // empty" "$file"
  else
    # The fallback only handles a top-level field. Nested lookups require jq.
    if [[ "$field" == *.*.* ]]; then
      return 0
    fi
    sed -n "s/.*\"${field#.}\"[[:space:]]*:[[:space:]]*\"\{0,1\}\([^\",}]*\).*/\1/p" "$file" | head -n 1
  fi
}

cleanup() {
  rm -f "$tmp" "$head_json" "$tick_json" "$head_json.err" "$tick_json.err"
}
trap cleanup EXIT

echo "==> ClickHouse"
if curl -fsS --max-time 3 "$CH_URL/ping" | grep -q Ok; then
  pass "clickhouse ping $CH_URL"
else
  fail "clickhouse ping $CH_URL"
fi

if [[ "$SKIP_API" -eq 0 ]]; then
  echo "==> API"
  tmp="$(mktemp)"
  if curl -fsS --max-time 5 "$API_URL/healthz" -o "$tmp"; then
    pass "GET $API_URL/healthz"
  else
    fail "GET $API_URL/healthz"
  fi
  if curl -fsS --max-time 5 "$API_URL/v1/status" -o "$tmp"; then
    pass "GET $API_URL/v1/status"
    head_tick="$(json_field "$tmp" .data.head.tick)"
    if [[ -n "$head_tick" ]]; then
      pass "indexed head tick $head_tick"
      if curl -fsS --max-time 5 "$API_URL/v1/ticks/$head_tick" -o "$tmp"; then
        pass "GET $API_URL/v1/ticks/$head_tick"
      else
        fail "GET $API_URL/v1/ticks/$head_tick"
      fi
    else
      skip "tick lookup (no indexed head yet)"
    fi
  else
    fail "GET $API_URL/v1/status"
  fi
else
  skip "api checks"
fi

if [[ "$SKIP_PEER" -eq 0 ]]; then
  echo "==> Peer"
  if [[ ! -x "$GLYPH_BIN" ]]; then
    echo "building glyph binary" >&2
    (cd "$ROOT" && cargo build --quiet -p cli --bin glyph)
  fi
  head_json="$(mktemp)"
  tick_json="$(mktemp)"
  if timeout 30s "$GLYPH_BIN" head --peer "$PEER" >"$head_json" 2>"$head_json.err"; then
    pass "peer head from $PEER"
    head_tick="$(json_field "$head_json" .tick)"
    if [[ -n "$head_tick" ]]; then
      if timeout 60s "$GLYPH_BIN" tick "$head_tick" --peer "$PEER" >"$tick_json" 2>"$tick_json.err"; then
        pass "peer tick $head_tick"
      else
        fail "peer tick $head_tick"
        sed -n '1,5p' "$tick_json.err" >&2 || true
      fi
    else
      skip "peer tick (could not parse head tick)"
    fi
  else
    fail "peer head from $PEER"
    sed -n '1,10p' "$head_json.err" >&2 || true
  fi
else
  skip "peer checks"
fi

echo
if [[ "$FAILURES" -eq 0 ]]; then
  echo "smoke: passed"
  exit 0
fi
echo "smoke: $FAILURES failure(s)" >&2
exit 1
