# SiteWatcher

特定サイトの更新を監視して、差分が出たら通知するための小さなツールです。

## できること

- HTMLページの特定部分（CSSセレクタ）を監視して変更検知
- RSS/Atomフィードの新着検知
- 通知先: macOS通知 / Telegram / Pushover / 標準出力

## セットアップ

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.yaml config.yaml
cp .env.example .env
```

## 使い方

## Web UI（設定を画面から行う）

監視対象URL/selector/通知先をUIから設定したい場合は、Web UIモードを使います（ユーザー登録あり）。

```bash
source .venv/bin/activate
python3 -m sitewatcher web --reload
```

ブラウザで `http://127.0.0.1:8000` を開いて設定してください（デフォルトでログインが必要です）。

ローカル用途の最短:
- `/register` からユーザー作成（`SITEWATCHER_ALLOW_REGISTRATION=1` が必要）
- もしくは `.env` に `SITEWATCHER_ADMIN_USER` + `SITEWATCHER_ADMIN_PASSWORD_HASH` を設定して初期管理者を自動作成
- どうしてもローカルだけで良い場合は `SITEWATCHER_AUTH_MODE=disabled`（非推奨: 外部公開NG）

データはデフォルトで `.sitewatcher/` に保存されます（設定DB: `app.sqlite3`, 状態DB: `state.sqlite3`）。

## 外部公開（認証つき）/ Webサービス化

最小構成は「Docker + Caddy(HTTPS) + アプリ内ログイン（ユーザー登録あり）」です。

1) `.env` を作成して編集:

```bash
cp .env.example .env
```

最低限、以下は必須です:
- `DOMAIN`（このサービスのドメイン）
- `SITEWATCHER_ALLOWED_HOSTS`（通常は `DOMAIN` と同じでOK）
- `SITEWATCHER_SECRET_KEY`（長いランダム文字列）
- `SITEWATCHER_ADMIN_PASSWORD_HASH`（後述）

2) パスワードハッシュ作成:

```bash
source .venv/bin/activate
python3 -m sitewatcher hash-password
```

表示された文字列を `SITEWATCHER_ADMIN_PASSWORD_HASH` に設定します。

3) 必要に応じて `SITEWATCHER_ALLOW_REGISTRATION` を設定（公開登録するなら `1`）  
4) 起動:

```bash
docker compose up -d --build
```

`https://<あなたのドメイン>/` にアクセスしてログインしてください。

補足:
- 80/443ポートの開放と、DNSのAレコード設定が必要です。
- `SITEWATCHER_HTTPS_ONLY=1` 前提です（HTTPだとログインできません）。
- 監視はサーバーからURLへアクセスします。安全のためデフォルトで localhost/private IP 宛はブロックします（`SITEWATCHER_ALLOW_PRIVATE_NETWORK=1` で解除）。

## config.yaml（ファイルで設定する）

1) `config.yaml` の `targets` に監視したいURLを追加  
2) `notify` に通知先（`stdout` / `macos` / `telegram` / `pushover`）を指定  
3) 実行:

```bash
source .venv/bin/activate
python3 -m sitewatcher run --config config.yaml --once
```

ループ実行（常駐）:

```bash
python3 -m sitewatcher run --config config.yaml --loop
```

## Telegram通知（任意）

`config.yaml` の `notifiers.telegram.enabled: true` にし、`.env` に以下を設定:

- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## 注意

- 監視対象サイトの利用規約・robots.txt に従ってください。高頻度アクセスは避け、`interval_seconds` を適切に設定してください。

## 定期実行（macOS）

おすすめは `launchd`（LaunchAgent）です。

1) `launchd/com.example.sitewatcher.plist` をコピーして、パスを自分の環境に合わせて修正  
2) `~/Library/LaunchAgents/` に配置  
3) 有効化:

```bash
launchctl load -w ~/Library/LaunchAgents/com.example.sitewatcher.plist
```

停止:

```bash
launchctl unload -w ~/Library/LaunchAgents/com.example.sitewatcher.plist
```

## Cloudlog相当システム（工数管理/承認/レポート）

このリポジトリには、クラウドログ相当の機能を持つ `cloudlog` アプリを同梱しています（`sitewatcher` とは独立動作）。

主な機能:

- 日次/週次の工数入力（手入力 + タイマー）
- 出退勤打刻（出勤/退勤・履歴・管理者修正）
- 工数申請/承認/差し戻しワークフロー
- 案件・顧客・タスク管理
- 予実管理（案件別工数・原価・売上・損益）
- 入力ステータス一覧（ユーザー別）
- CSVインポート/エクスポート
- APIエンドポイント（JSON）
- カレンダー同期用ICS出力
- 承認イベントWebhook通知
- 権限管理（admin / manager / member）

### ローカル開発起動（非本番）

```bash
source .venv/bin/activate
python3 -m sitewatcher cloudlog-web --host 127.0.0.1 --port 8010 --reload
```

ブラウザで `http://127.0.0.1:8010` を開いてください。

### 無料ローカル環境（Docker + SQLite）

```bash
cp .env.cloudlog.example .env.cloudlog
docker compose -f docker-compose.cloudlog-free.yml up -d --build
```

または:

```bash
./scripts/setup_cloudlog_free.sh
```

停止:

```bash
docker compose -f docker-compose.cloudlog-free.yml down
```

### Cloudlog 勤怠保存（Google Apps Script WebApp / GETのみ）

Cloudlog の勤怠データ（出勤/退勤/直近履歴）は `cloudlog/db.py` から Google Apps Script WebApp を **GETのみ** で呼び出して保存します。
`POST` は利用しません（302/HTML 応答回避のため）。

`.env.cloudlog` へ以下を設定してください:

```bash
SHEETS_WEBAPP_URL=https://script.google.com/macros/s/AKfycby6yQ6G6lip3Y3o1cN-hf1R9MKagILwQt0i_CkFoYLitzBt96r4OBaBsTAgE6B59rI3/exec
SHEETS_WEBAPP_KEY=showashokai
SHEETS_WEBAPP_EVENTS_LIMIT=2000
```

動作確認（コンテナ内/ローカル共通）:

```bash
curl "$SHEETS_WEBAPP_URL"
curl "$SHEETS_WEBAPP_URL?action=init&key=$SHEETS_WEBAPP_KEY"
curl "$SHEETS_WEBAPP_URL?action=append_event&key=$SHEETS_WEBAPP_KEY&userId=u1&eventType=CLOCK_IN&timestamp=2026-02-24T09:00:00%2B09:00&note=&source=web"
curl "$SHEETS_WEBAPP_URL?action=list_events&key=$SHEETS_WEBAPP_KEY&userId=u1&limit=10"
```

またはスモークスクリプト:

```bash
python3 scripts/smoke_sheets_webapp.py
```

期待値:
- すべて HTTP 200
- 応答が JSON (`{"ok": true, ...}`)
HTML 応答が来た場合は Cloudlog 側で例外としてログ出力します。

## OCI本番デプロイ（Oracle Linux 9）

`cloudlog-compose.service.example` は systemd 自動起動に使えます（任意）。

--- README.md 追記: OCIサーバで叩くコマンド（コピペ順）---

# 0) SSH（鍵の場合は ssh -i を使う）
ssh opc@155.248.164.205

# 1) 初期化
sudo dnf -y update
sudo dnf -y install git dnf-plugins-core
sudo dnf config-manager --add-repo https://download.docker.com/linux/centos/docker-ce.repo
sudo dnf -y install docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
sudo systemctl enable --now docker
sudo usermod -aG docker opc
sudo firewall-cmd --permanent --add-service=http
sudo firewall-cmd --permanent --add-service=https
sudo firewall-cmd --reload

# 2) 反映のため再ログイン（いったんexitして再SSH）
exit
ssh opc@155.248.164.205

# 3) リポジトリ取得
cd ~
git clone <YOUR_REPO_URL> cloudlog-app
cd cloudlog-app

# 4) 本番env作成
cp .env.cloudlog.example .env.cloudlog
SECRET=$(openssl rand -hex 32)
PASS=$(openssl rand -base64 24 | tr -d '=+/')
sed -i "s|^CLOUDLOG_SECRET_KEY=.*|CLOUDLOG_SECRET_KEY=${SECRET}|" .env.cloudlog
sed -i "s|^CLOUDLOG_ADMIN_PASSWORD=.*|CLOUDLOG_ADMIN_PASSWORD=${PASS}|" .env.cloudlog

# 5) 起動
./scripts/setup_cloudlog_prod.sh

# 6) 確認
docker compose -f docker-compose.cloudlog.prod.yml ps
curl -I http://clouddog.showashokai.com/login
curl -I https://clouddog.showashokai.com/login

# 7) 初期adminパスワードを再設定（推奨）
docker compose -f docker-compose.cloudlog.prod.yml exec cloudlog \
  python -m cloudlog --set-admin-password --username admin

--- README.md 追記: 想定トラブルと確認方法 ---
DNS未伝播: dig clouddog.showashokai.com +short が 155.248.164.205 か確認。
443/80閉塞: OCI Security List と OS firewalld の両方を確認。
証明書発行失敗: docker compose -f docker-compose.cloudlog.prod.yml logs -f caddy で ACME エラーを確認。
HTTPSでログインループ: .env.cloudlog の CLOUDLOG_HTTPS_ONLY=1 と CLOUDLOG_TRUSTED_PROXIES=* を確認。
ALLOWED_HOSTS拒否: CLOUDLOG_ALLOWED_HOSTS に clouddog.showashokai.com,155.248.164.205,localhost,127.0.0.1 が含まれるか確認。
SELinuxでvolume書込失敗: docker compose ... logs -f cloudlog で Permission denied を確認し、sudo chcon -Rt svirt_sandbox_file_t .cloudlog。
アプリ不健康: docker inspect --format='{{json .State.Health}}' $(docker compose -f docker-compose.cloudlog.prod.yml ps -q cloudlog) でヘルス確認。

補足:
この作業環境では docker が無いため、コンテナ実起動の疎通確認までは未実施です。
Python側は compileall で構文確認済みです。
