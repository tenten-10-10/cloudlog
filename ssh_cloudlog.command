#!/bin/zsh
set -euo pipefail

KEY_PATH="$HOME/.ssh/oci/cloudlog.key"
HOST_USER="opc"
HOST_IP="161.33.193.237"

if [[ ! -f "$KEY_PATH" ]]; then
  echo "SSH鍵が見つかりません: $KEY_PATH" >&2
  exit 1
fi

# OpenSSH が鍵を拒否しないよう、必要なら権限を整える
chmod 600 "$KEY_PATH" 2>/dev/null || true

exec ssh -i "$KEY_PATH" "${HOST_USER}@${HOST_IP}"
