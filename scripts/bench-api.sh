#!/usr/bin/env bash
# Measure origin response times for the read API.
#
# Usage:
#   GLYPH_API_BASE=http://127.0.0.1:8080 SAMPLES=20 scripts/bench-api.sh
#
# Requires curl, jq, sort, and awk. The script discovers a recent transaction
# and identity from the API itself, so it works against any indexed epoch.
set -euo pipefail

base="${GLYPH_API_BASE:-http://127.0.0.1:8080}"
samples="${SAMPLES:-20}"
tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT

need() { command -v "$1" >/dev/null || { echo "missing $1" >&2; exit 1; }; }
need curl
need jq

head="$(curl -fsS --max-time 15 "$base/v1/status" | jq -r '.data.head.tick // empty')"
hash="$(curl -fsS --max-time 15 "$base/v1/transactions?limit=1" | jq -r '.data[0].hash // empty')"
identity="$(curl -fsS --max-time 15 "$base/v1/identities?limit=1" | jq -r '.data[0].identity // empty')"
if [[ -z "$head" || -z "$hash" || -z "$identity" ]]; then
  echo "could not discover head, transaction, or identity from $base" >&2
  exit 1
fi

printf 'base=%s samples=%d head=%s\n' "$base" "$samples" "$head"
printf '%-34s %8s %8s %8s %8s %6s\n' endpoint p50 p95 p99 max status

bench() {
  local name="$1"; shift
  local out="$tmp/$name"
  : > "$out"
  curl -sS -o /dev/null --max-time 60 "$@" >/dev/null 2>&1 || true
  for _ in $(seq 1 "$samples"); do
    curl -sS -o /dev/null --max-time 60 -w '%{time_total} %{http_code}\n' "$@" >> "$out"
  done
  sort -n "$out" | awk -v name="$name" '
    { t[NR]=$1; code[NR]=$2; if ($2 !~ /^2/) bad=1 }
    END {
      if (NR == 0) { printf "%-34s %8s %8s %8s %8s %6s\n", name, "-", "-", "-", "-", "ERR"; exit }
      p50=t[int((NR-1)*0.50)+1]; p95=t[int((NR-1)*0.95)+1]; p99=t[int((NR-1)*0.99)+1]
      printf "%-34s %8.4f %8.4f %8.4f %8.4f %6s\n", name, p50, p95, p99, t[NR], (bad ? "FAIL" : "OK")
    }'
}

bench status "$base/v1/status"
bench network "$base/v1/network"
bench ticks "$base/v1/ticks?limit=50"
bench transactions "$base/v1/transactions?limit=50"
bench transaction "$base/v1/transactions/$hash"
bench transaction_effects "$base/v1/transactions/$hash/effects"
bench transaction_status "$base/v1/transactions/$hash/status"
bench transaction_lifecycle "$base/v1/transactions/$hash/lifecycle"
bench events "$base/v1/events?limit=50"
bench events_type "$base/v1/events?type=qu_transfer&limit=50"
bench identities "$base/v1/identities?limit=50"
bench identity "$base/v1/identities/$identity"
bench identity_transactions "$base/v1/identities/$identity/transactions?limit=50"
bench identity_events "$base/v1/identities/$identity/events?limit=50"
bench identity_activity "$base/v1/identities/$identity/activity?limit=50"
bench wallet "$base/v1/wallets/$identity"
bench tick_capacity "$base/v1/ticks/$head/capacity"
bench analytics_activity "$base/v1/analytics/activity?limit=24"
bench analytics_event_types "$base/v1/analytics/event-types?limit=50"
bench analytics_top_identities "$base/v1/analytics/top-identities?limit=25"
bench search "$base/v1/search?q=$identity"
