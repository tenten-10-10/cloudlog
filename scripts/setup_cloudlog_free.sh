#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT_DIR"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker が見つかりません。Docker Desktop などを先にインストールしてください。" >&2
  exit 1
fi

if [ ! -f .env.cloudlog ]; then
  cp .env.cloudlog.example .env.cloudlog
  echo ".env.cloudlog を作成しました（必要なら認証情報を編集してください）"
fi

mkdir -p .cloudlog

docker compose -f docker-compose.cloudlog-free.yml up -d --build

echo "Cloudlog を起動しました: http://127.0.0.1:8010"
echo "初期ログインは .env.cloudlog の CLOUDLOG_ADMIN_USER / CLOUDLOG_ADMIN_PASSWORD です。"
echo "無料公開が必要なら: docker compose -f docker-compose.cloudlog-free.yml --profile public up -d"
echo "公開URL確認: docker compose -f docker-compose.cloudlog-free.yml logs -f cloudflared"
