#!/bin/zsh
set -euo pipefail

# Restart Vine watcher (stop -> start)

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

echo "Vine watcher を再起動します…"

if [[ -x "$SCRIPT_DIR/stop_vine.command" ]]; then
  "$SCRIPT_DIR/stop_vine.command" || true
else
  echo "stop_vine.command が見つかりません: $SCRIPT_DIR/stop_vine.command"
fi

sleep 1.2

exec "$SCRIPT_DIR/run_vine.command"
