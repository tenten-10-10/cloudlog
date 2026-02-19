#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker が見つかりません。Docker Engine / Docker Compose Plugin をインストールしてください。" >&2
  exit 1
fi

if [ ! -f .env.cloudlog ]; then
  cp .env.cloudlog.example .env.cloudlog
  echo ".env.cloudlog を作成しました。値を本番用に編集してから再実行してください。" >&2
  exit 1
fi

if grep -q "CHANGE_ME" .env.cloudlog; then
  echo ".env.cloudlog に CHANGE_ME が残っています。必須項目を本番値へ変更してください。" >&2
  exit 1
fi

for key in DOMAIN CLOUDLOG_SECRET_KEY CLOUDLOG_ADMIN_PASSWORD CLOUDLOG_ALLOWED_HOSTS; do
  if ! grep -q "^${key}=" .env.cloudlog; then
    echo ".env.cloudlog に ${key} がありません。" >&2
    exit 1
  fi
  value="$(awk -F= -v k=\"$key\" '$1==k {print substr($0, length($1)+2)}' .env.cloudlog | tail -n1 | tr -d '[:space:]')"
  if [ -z "$value" ]; then
    echo ".env.cloudlog の ${key} が空です。" >&2
    exit 1
  fi
done

mkdir -p .cloudlog

docker compose -f docker-compose.cloudlog.prod.yml up -d --build

echo "Cloudlog を起動しました。"
echo "- URL: https://$(awk -F= '/^DOMAIN=/{print $2}' .env.cloudlog)"
echo "- 状態確認: docker compose -f docker-compose.cloudlog.prod.yml ps"
echo "- ログ確認: docker compose -f docker-compose.cloudlog.prod.yml logs -f"
