#!/bin/zsh
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Keep bulky venv/pycache out of iCloud Drive.
BASE_CACHE_DIR="$HOME/Library/Caches/vine-watcher"
VENV_DIR="$BASE_CACHE_DIR/.venv"
export PYTHONPYCACHEPREFIX="$BASE_CACHE_DIR/pycache"

mkdir -p "$BASE_CACHE_DIR"

PY="$VENV_DIR/bin/python"
if [[ ! -x "$PY" ]]; then
  echo "初回セットアップ: venv作成 & 依存インストール"
  python3 -m venv "$VENV_DIR"
  "$PY" -m pip install --upgrade pip
  "$PY" -m pip install "playwright>=1.40" "python-dotenv>=1.0"
  "$PY" -m playwright install firefox
fi

cd "$SCRIPT_DIR"
PID_FILE="$BASE_CACHE_DIR/vine_watcher.pid"
{
  echo "pid=$$"
  echo "script=$SCRIPT_DIR/vine_watcher.py"
  echo "started_at=$(date '+%Y-%m-%d %H:%M:%S')"
} > "$PID_FILE"
echo "PIDを書き込み: $PID_FILE (pid=$$)"
exec "$PY" "$SCRIPT_DIR/vine_watcher.py"
