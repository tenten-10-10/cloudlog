#!/bin/zsh
set -euo pipefail

# Stop Vine watcher started by run_vine.command (macOS double-click friendly)

BASE_CACHE_DIR="$HOME/Library/Caches/vine-watcher"
PID_FILE="$BASE_CACHE_DIR/vine_watcher.pid"

echo "Vine watcher 停止を開始します…"

pids=()
firefox_pids=()

add_pid() {
  local pid="$1"
  if [[ -z "${pid:-}" || "$pid" != <-> ]]; then
    return 0
  fi
  if [[ " ${pids[*]-} " != *" $pid "* ]]; then
    pids+=("$pid")
  fi
}

add_firefox_pid() {
  local pid="$1"
  if [[ -z "${pid:-}" || "$pid" != <-> ]]; then
    return 0
  fi
  if [[ " ${firefox_pids[*]-} " != *" $pid "* ]]; then
    firefox_pids+=("$pid")
  fi
}

if [[ -f "$PID_FILE" ]]; then
  echo "PIDファイル: $PID_FILE"
  # PID file format supports either:
  #   12345
  #   pid=12345
  #   pid=12345\nscript=...\nstarted_at=...
  first_line="$(head -n 1 "$PID_FILE" 2>/dev/null || true)"
  pid=""
  if [[ "${first_line:-}" == pid=* ]]; then
    pid="${first_line#pid=}"
  else
    pid="$(echo "${first_line:-}" | tr -d '[:space:]')"
  fi
  if [[ -z "${pid:-}" ]]; then
    pid="$(grep -E '^pid=' "$PID_FILE" 2>/dev/null | head -n 1 | sed 's/^pid=//' | tr -d '[:space:]' || true)"
  fi

  if [[ -n "${pid:-}" && "$pid" == <-> ]]; then
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if [[ -z "${cmd:-}" ]]; then
      echo "PIDファイルの PID は存在しません（stale）: $pid"
      rm -f "$PID_FILE" 2>/dev/null || true
    elif [[ "$cmd" == *"vine_watcher.py"* || "$cmd" == *"vine_watch_ff.py"* ]]; then
      echo "PIDファイルから停止対象を検出: $pid"
      add_pid "$pid"
    else
      echo "PIDファイルの PID は watcher ではなさそうなので無視します: $pid"
      echo "  cmd: $cmd"
    fi
  else
    echo "PIDファイルから PID を読めませんでした。"
  fi
fi

if command -v pgrep >/dev/null 2>&1; then
  # Watcher processes (both new/old script names)
  for pid in $(pgrep -f "vine_(watcher|watch_ff)\\.py" 2>/dev/null || true); do
    add_pid "$pid"
  done

  # Playwright/Firefox processes that can keep the profile locked
  for pid in $(pgrep -f "vine-pw-profile" 2>/dev/null || true); do
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    if [[ -n "${cmd:-}" && "$cmd" == *"firefox"* ]]; then
      add_firefox_pid "$pid"
    fi
  done
fi

if (( ${#pids} == 0 && ${#firefox_pids} == 0 )); then
  echo "稼働中の watcher / 関連Firefox が見つかりませんでした。"
  rm -f "$PID_FILE" 2>/dev/null || true
  exit 0
fi

if (( ${#pids} > 0 )); then
  echo "停止対象（watcher）:"
  for pid in "${pids[@]}"; do
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    echo "  $pid: ${cmd:-?}"
  done
fi

if (( ${#firefox_pids} > 0 )); then
  echo "停止対象（関連Firefox）:"
  for pid in "${firefox_pids[@]}"; do
    cmd="$(ps -p "$pid" -o command= 2>/dev/null || true)"
    echo "  $pid: ${cmd:-?}"
  done
fi

for pid in "${pids[@]}"; do
  kill -TERM "$pid" 2>/dev/null || true
done

for pid in "${firefox_pids[@]}"; do
  kill -TERM "$pid" 2>/dev/null || true
done

deadline=$((SECONDS + 15))
still=("${pids[@]}" "${firefox_pids[@]}")
while (( SECONDS < deadline )); do
  alive=()
  for pid in "${still[@]}"; do
    if kill -0 "$pid" 2>/dev/null; then
      alive+=("$pid")
    fi
  done
  if (( ${#alive} == 0 )); then
    still=()
    break
  fi
  still=("${alive[@]}")
  sleep 0.25
done

if (( ${#still} == 0 )); then
  echo "停止しました。"
  rm -f "$PID_FILE" 2>/dev/null || true
  exit 0
fi

echo "まだ終了していません（強制終了します）: ${still[*]}"
for pid in "${still[@]}"; do
  kill -KILL "$pid" 2>/dev/null || true
done
rm -f "$PID_FILE" 2>/dev/null || true
echo "完了。"
