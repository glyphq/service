#!/usr/bin/env bash
#
# Stop the Docker Compose stack.
#
# Usage:
#   scripts/docker-down.sh [--wipe] [--images]
#
# Options:
#   --wipe     Also delete the ClickHouse, SeaweedFS, and log named volumes.
#              This permanently deletes indexed history and archived objects.
#   --images   Also remove the locally built glyph image.
#   -h, --help Show this help.
#
# Environment:
#   GLYPH_ENV_FILE   Explicit Compose env file. Defaults to ./.env when present.
#
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="$ROOT/docker-compose.yml"
WIPE=0
REMOVE_IMAGES=0

usage() {
  cat <<'EOF'
Stop the Docker Compose stack.

Usage:
  scripts/docker-down.sh [--wipe] [--images]

Options:
  --wipe     Also delete the ClickHouse, SeaweedFS, and log named volumes.
             This permanently deletes indexed history and archived objects.
  --images   Also remove the locally built glyph image.
  -h, --help Show this help.

Environment:
  GLYPH_ENV_FILE   Explicit Compose env file. Defaults to ./.env when present.
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --wipe)
      WIPE=1
      shift
      ;;
    --images)
      REMOVE_IMAGES=1
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      exit 2
      ;;
  esac
done

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is not installed or not on PATH" >&2
  exit 1
fi
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose v2 is required (the 'docker compose' plugin)" >&2
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
fi

DOWN=("${COMPOSE[@]}" down --remove-orphans)
if [[ "$WIPE" -eq 1 ]]; then
  DOWN+=(--volumes)
fi
if [[ "$REMOVE_IMAGES" -eq 1 ]]; then
  DOWN+=(--rmi local)
fi

echo "==> docker compose down"
"${DOWN[@]}"

if [[ "$WIPE" -eq 1 ]]; then
  echo "removed named volumes: indexed ClickHouse data, ClickHouse logs, and SeaweedFS objects"
fi
echo "done"
