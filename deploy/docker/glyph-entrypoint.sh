#!/usr/bin/env bash
#
# Container entrypoint for the Glyph image.
#
# The first argument selects a role:
#
#   api       Run glyph-api (default; the Dockerfile CMD is "api").
#   indexer   Run `glyph sync` with peers from QUBIC_PEERS.
#   glyph     Run the glyph CLI verbatim (for example: glyph head --peer ...).
#   <cmd>     Exec any other command, which keeps `docker run image bash` useful.
#
# Indexer environment:
#   QUBIC_PEERS                 Comma- or whitespace-separated peer addresses.
#   GLYPH_FROM_TICK             Optional starting tick.
#   INDEXER_POLL_MS             Poll interval once caught up (default 750).
#   INDEXER_PROGRESS_EVERY      Log every N indexed ticks (default 100).
#   GLYPH_VERIFY_SIGNATURES     "false" disables SchnorrQ checks.
#
set -euo pipefail

log() {
  printf '%s\n' "glyph-entrypoint: $*" >&2
}

role="${1:-${GLYPH_ROLE:-api}}"
if [[ $# -gt 0 ]]; then
  shift
fi

case "$role" in
  api | glyph-api)
    exec /usr/local/bin/glyph-api "$@"
    ;;
  indexer)
    peers="${QUBIC_PEERS:-157.180.10.49:21841}"
    read -r -a peer_list <<<"${peers//,/ }"

    args=(sync)
    for peer in "${peer_list[@]}"; do
      if [[ -n "$peer" ]]; then
        args+=(--peer "$peer")
      fi
    done

    if [[ ${#args[@]} -eq 1 ]]; then
      log "QUBIC_PEERS did not contain a usable peer address: $peers"
      exit 2
    fi

    if [[ -n "${GLYPH_FROM_TICK:-}" ]]; then
      args+=(--from "$GLYPH_FROM_TICK")
    fi
    args+=(--poll-ms "${INDEXER_POLL_MS:-750}")
    args+=(--progress-every "${INDEXER_PROGRESS_EVERY:-100}")

    case "${GLYPH_VERIFY_SIGNATURES:-true}" in
      0 | false | no | off)
        args+=(--no-verify-signatures)
        ;;
    esac

    log "starting indexer for peers: ${peer_list[*]}"
    exec /usr/local/bin/glyph "${args[@]}"
    ;;
  glyph)
    exec /usr/local/bin/glyph "$@"
    ;;
  *)
    exec "$role" "$@"
    ;;
esac
