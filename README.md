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

監視対象URL/selector/通知先をUIから設定したい場合は、Web UIモードを使います。

```bash
source .venv/bin/activate
python3 -m sitewatcher web --reload
```

ブラウザで `http://127.0.0.1:8000` を開いて設定してください。

データはデフォルトで `.sitewatcher/` に保存されます（設定DB: `app.sqlite3`, 状態DB: `state.sqlite3`）。

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
