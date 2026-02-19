# GAS Vine Logger（Webアプリ）

Webアプリ（`/exec`）に JSON POST で投げた内容を、固定スプレッドシートの `log` シートへ追記します（同日重複: `asin + "|" + price` をスキップ）。

## エンドポイント

`https://script.google.com/macros/s/AKfycbzlRl7HB8tjzEB5RvdDv-jvez-d-U1HgfI0BLQPHMwmsqsghgbnzXGR08KDC8L4IGDk/exec`

## curl テスト

ヘルスチェック（200 & `{"ok":true,"alive":true,...}`）

```bash
curl -sS -L "https://script.google.com/macros/s/AKfycbzlRl7HB8tjzEB5RvdDv-jvez-d-U1HgfI0BLQPHMwmsqsghgbnzXGR08KDC8L4IGDk/exec"
```

JSON POST（同じ payload を 2 回叩く：1回目 appended:true / 2回目 duplicate）

```bash
curl -sS -L "https://script.google.com/macros/s/AKfycbzlRl7HB8tjzEB5RvdDv-jvez-d-U1HgfI0BLQPHMwmsqsghgbnzXGR08KDC8L4IGDk/exec" \
  -H "Content-Type: application/json" \
  -d '{
    "secret":"potluck_secret_123",
    "title":"テスト商品タイトル（POST）",
    "price":"¥12,980",
    "asin":"B0XXXXXXX",
    "queue_url":"https://www.amazon.co.jp/vine/vine-items?queue=potluck",
    "brand":"TEST",
    "priority":"⚡"
  }'
```

GET 追記（互換）

```bash
curl -sS -L -G "https://script.google.com/macros/s/AKfycbzlRl7HB8tjzEB5RvdDv-jvez-d-U1HgfI0BLQPHMwmsqsghgbnzXGR08KDC8L4IGDk/exec" \
  --data-urlencode "secret=potluck_secret_123" \
  --data-urlencode "title=テスト商品タイトル（GET）" \
  --data-urlencode "price=¥12,980" \
  --data-urlencode "asin=B0XXXXXXX" \
  --data-urlencode "queue_url=https://www.amazon.co.jp/vine/vine-items?queue=potluck" \
  --data-urlencode "brand=TEST" \
  --data-urlencode "priority=⚡"
```
