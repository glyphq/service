#!/usr/bin/env bash
set -euo pipefail

VERSION="${CLICKHOUSE_VERSION:-master}"
DEST="${CLICKHOUSE_BIN:-$HOME/.local/bin/clickhouse}"

if [[ -x "$DEST" ]]; then
  echo "clickhouse already installed at $DEST"
  exit 0
fi

mkdir -p "$(dirname "$DEST")"
curl -fL --retry 3 "https://builds.clickhouse.com/${VERSION}/amd64/clickhouse" -o "$DEST"
chmod +x "$DEST"
"$DEST" --version
