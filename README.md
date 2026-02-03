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
