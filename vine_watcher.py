#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Amazon Vine ウォッチャー（Playwright/Firefox｜超低遅延・日本語UI・厳密収集）
"""

from __future__ import annotations

import datetime
import json
import os
import queue
import random
import re
import select
import signal
import sys
import termios
import time
import tty
import unicodedata
import urllib.parse
import urllib.request
import urllib.error
from collections import OrderedDict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from playwright.sync_api import sync_playwright

BASE_DIR = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv

    # .env を自動読込（スクリプトと同じフォルダ優先）
    load_dotenv(dotenv_path=str(BASE_DIR / ".env"))
except Exception:
    pass


# ===== Telegram通知（任意）=====
# 使い方:
#   export TELEGRAM_BOT_TOKEN="xxxx:yyyy"
#   export TELEGRAM_CHAT_ID="123456789"
#   (任意) export TELEGRAM_DISABLE=1  # 完全オフ
#   (任意) export TELEGRAM_SILENT=1   # 通知をサイレント
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()
TELEGRAM_DISABLE = os.getenv("TELEGRAM_DISABLE", "").strip() != ""
TELEGRAM_SILENT = os.getenv("TELEGRAM_SILENT", "").strip() != ""


def tg_send(text: str) -> bool:
    """Telegramへ1メッセージ送信（設定が無ければ何もしない）"""
    if TELEGRAM_DISABLE or (not TELEGRAM_BOT_TOKEN) or (not TELEGRAM_CHAT_ID):
        return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        data = {
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "disable_notification": TELEGRAM_SILENT,
            "disable_web_page_preview": True,
        }
        body = urllib.parse.urlencode(data).encode("utf-8")
        req = urllib.request.Request(url, data=body, method="POST")
        with urllib.request.urlopen(req, timeout=8) as r:
            _ = r.read()
        return True
    except Exception:
        return False


# ===== Google Apps Script（スプレッドシート記録）=====
GAS_WEBAPP_URL = os.getenv(
    "VINE_GAS_URL",
    "https://script.google.com/macros/s/AKfycbzlRl7HB8tjzEB5RvdDv-jvez-d-U1HgfI0BLQPHMwmsqsghgbnzXGR08KDC8L4IGDk/exec",
).strip()
GAS_SECRET = os.getenv("VINE_GAS_SECRET", "potluck_secret_123").strip()
GAS_DISABLE = os.getenv("VINE_GAS_DISABLE", "").strip() != ""
GAS_METHOD = os.getenv("VINE_GAS_METHOD", "auto").strip().lower()  # post | get | auto
try:
    GAS_TIMEOUT = float(os.getenv("VINE_GAS_TIMEOUT", "8"))
except Exception:
    GAS_TIMEOUT = 8.0


def _read_json_response_bytes(raw: bytes) -> Optional[dict]:
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        try:
            return json.loads(raw.decode("utf-8", "ignore"))
        except Exception:
            return None


def gas_append_row(payload: dict) -> Optional[dict]:
    """GAS Webアプリへ追記（POST優先、失敗時はGETへフォールバック）。戻り値は JSON dict or None。"""
    if GAS_DISABLE or (not GAS_WEBAPP_URL):
        return None

    data = dict(payload or {})
    data.setdefault("secret", GAS_SECRET)

    def _post_json() -> Optional[dict]:
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        req = urllib.request.Request(
            GAS_WEBAPP_URL,
            data=body,
            headers={
                "Content-Type": "application/json; charset=utf-8",
                "User-Agent": "vine-watcher/1.0",
            },
            method="POST",
        )
        try:
            # Apps Script は 302 で echo URL へ飛ぶが、POST を維持すると 405 になることがある。
            # urllib の標準リダイレクトは 302/303 で GET に変換して追従するため、それに任せる。
            with urllib.request.urlopen(req, timeout=GAS_TIMEOUT) as r:
                raw = r.read()
            return _read_json_response_bytes(raw)
        except urllib.error.HTTPError as e:
            try:
                raw = e.read() or b""
            except Exception:
                raw = b""
            return _read_json_response_bytes(raw)
        except Exception:
            return None

    def _get_query() -> Optional[dict]:
        try:
            qs = urllib.parse.urlencode(data)
        except Exception:
            return None
        url = GAS_WEBAPP_URL + ("&" if "?" in GAS_WEBAPP_URL else "?") + qs
        req = urllib.request.Request(url, headers={"User-Agent": "vine-watcher/1.0"}, method="GET")
        try:
            with urllib.request.urlopen(req, timeout=GAS_TIMEOUT) as r:
                raw = r.read()
            return _read_json_response_bytes(raw)
        except urllib.error.HTTPError as e:
            try:
                raw = e.read() or b""
            except Exception:
                raw = b""
            return _read_json_response_bytes(raw)
        except Exception:
            return None

    method = (GAS_METHOD or "post").lower()
    if method in ("post", "auto"):
        res = _post_json()
        if isinstance(res, dict):
            return res
        if method == "post":
            return None
    if method in ("get", "auto"):
        res = _get_query()
        if isinstance(res, dict):
            return res
    return None


# ===== ANSI & Emoji =====
COLOR_ENABLED = (
    (os.environ.get("VINE_COLOR", "1").lower() in ("1", "true", "yes"))
    and sys.stdout.isatty()
    and (os.environ.get("NO_COLOR", "") == "")
)
EMOJI_ENABLED = os.environ.get("VINE_EMOJI", "1").lower() in ("1", "true", "yes")


def _ansi(s: str, code: str) -> str:
    return f"\033[{code}m{s}\033[0m" if COLOR_ENABLED else s


def _ansi_keep(s: str, start_code: str, end_code: str) -> str:
    """ANSI を一時的に適用して、属性全体をリセットせずに戻す（太字などを維持したい場合用）"""
    if not COLOR_ENABLED:
        return s
    return f"\033[{start_code}m{s}\033[{end_code}m"


def B(s: str) -> str:
    return _ansi(s, "1")


def C(s: str) -> str:
    return _ansi(s, "36")


def Y(s: str) -> str:
    return _ansi(s, "33")


def Cbg(s: str) -> str:
    """黒文字＋シアン背景（新着の時刻など強調用）"""
    return _ansi(s, "1;30;46")


def Gc(s: str) -> str:
    return _ansi(s, "32")


def R(s: str) -> str:
    return _ansi(s, "31")


def M(s: str) -> str:
    return _ansi(s, "35")


def Gy(s: str) -> str:
    return _ansi(s, "90")


def Cb(s: str) -> str:
    return _ansi(s, "1;36")


def Yb(s: str) -> str:
    return _ansi(s, "1;33")


def Gb(s: str) -> str:
    return _ansi(s, "1;32")


def Rb(s: str) -> str:
    return _ansi(s, "1;31")


def E(sym: str) -> str:
    return sym if EMOJI_ENABLED else ""


# ===== 設定（環境変数）=====
URL = os.environ.get("VINE_URL", "https://www.amazon.co.jp/vine/vine-items?queue=potluck").strip()
INTERVAL = int(os.environ.get("VINE_INTERVAL", "10"))
FAST_INTERVAL = int(os.environ.get("VINE_INTERVAL_FAST", "5"))
FAST_WINDOWS = os.environ.get(
    "VINE_FAST_WINDOWS",
    "07:55-08:10,12:55-13:10,14:40-15:10,15:40-16:10,16:40-17:10,19:55-20:10",
).strip()
SHOTS_DIR = (os.environ.get("VINE_SHOT_DIR") or str(BASE_DIR / "shots")).strip()
DB_PATH = (os.environ.get("VINE_CAPTURE_DB") or str(BASE_DIR / "captured_asins.json")).strip()
ONLY_NEW = os.environ.get("VINE_ONLY_NEW", "1").lower() in ("1", "true", "yes")
AUTO_ORDER = os.environ.get("VINE_AUTO_ORDER", "0").lower() in ("1", "true", "yes")
ORDER_THRESHOLD = int(os.environ.get("VINE_ORDER_THRESHOLD", "14000"))
PROFILE_DIR = os.environ.get("VINE_PROFILE_DIR", os.path.expanduser("~/vine-pw-profile"))
DP_OPEN_MODE = os.environ.get("VINE_DP_OPEN_MODE", "tab").strip().lower()  # "tab" | "same"
TAB_FOREGROUND = os.environ.get("VINE_TAB_FOREGROUND", "0").lower() in ("1", "true", "yes")
BOOT_CATCHUP = os.environ.get("VINE_BOOT_CATCHUP", "1").lower() in ("1", "true", "yes")
NO_BOOT_DEEP = os.environ.get("VINE_NO_BOOT_DEEP", "0").lower() in ("1", "true", "yes")
DEBUG_FIND = os.environ.get("VINE_DEBUG_FIND", "0").lower() in ("1", "true", "yes")
ALLOW_DUP_ORDER = os.environ.get("VINE_ALLOW_DUP_ORDER", "0").lower() in ("1", "true", "yes")
VARY_COLLECT = os.environ.get("VINE_COLLECT_VARIANTS", "1").lower() in ("1", "true", "yes")
VARY_MAX = int(os.environ.get("VINE_VARIANT_MAX", "6"))
ORDER_FRONT = os.environ.get("VINE_ORDER_FRONT", "0").lower() in ("1", "true", "yes")
ORDER_MAX = float(os.environ.get("VINE_ORDER_MAX", "30"))
ORDER_RETRY = float(os.environ.get("VINE_ORDER_RETRY_INTERVAL", "0.05"))

DEFAULT_BRANDS_ALWAYS = ["Anker", "MOFT", "cado", "CASETIFY", "UGREEN", "CIO"]
COLOR_PREF = ["緑", "green", "グリーン", "white", "ホワイト", "黒", "ブラック"]


# ---- ブランド設定（外部ファイル連携） ----
# 環境変数 VINE_BRANDS_FILE があれば優先。未設定なら「このスクリプトと同じフォルダ」に保存。
BRANDS_FILE = os.getenv("VINE_BRANDS_FILE", str(BASE_DIR / "brands.txt"))


def _read_brands_file(path: str) -> Optional[List[str]]:
    try:
        p = Path(path)
        if not p.parent.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
        # ファイルがなければヘッダのみで作成し、空リストを返す（ユーザーが後で追記）
        if not p.exists():
            header = "# 常時ブランド（1行1ブランド）\n"
            p.write_text(header, encoding="utf-8")
            return []
        raw = p.read_text(encoding="utf-8")
        lines = raw.splitlines()
    except Exception:
        return None
    out: List[str] = []
    for line in lines:
        s = unicodedata.normalize("NFKC", str(line)).lstrip("\ufeff").strip()
        if not s:
            continue
        # 先頭コメント行（# / ＃）
        if s.startswith("#") or s.startswith("＃"):
            continue
        # 行内コメントを除去
        s = s.split("#", 1)[0].split("＃", 1)[0].strip()
        if s:
            out.append(s)
    return out


def _write_brands_file(path: str, brands: set) -> bool:
    try:
        p = Path(path)
        if not p.parent.exists():
            p.parent.mkdir(parents=True, exist_ok=True)
        lines = [x for x in sorted(set(str(b).strip() for b in (brands or []) if b)) if x]
        header = "# 常時ブランド（1行1ブランド）\n"
        p.write_text(header + "\n".join(lines) + "\n", encoding="utf-8")
        return True
    except Exception:
        return False


# ---- 消耗品（除外）キーワード（簡易版） ----
NG_CONSUMABLE_KEYWORDS = [
    "インク",
    "トナー",
    "カートリッジ",
    "インクカートリッジ",
    "トナーカートリッジ",
    "ドラム",
    "リフィル",
    "詰め替え",
    "詰替え",
    "替え芯",
    "替芯",
    "替刃",
    "替え刃",
    "補充",
    "補充用",
    "補充液",
    "リボン",
    "テープ",
    "フィルター",
    "フィルタ",
    "交換用",
    "交換パック",
    "交換フィルター",
    "写真用紙",
    "フォトペーパー",
    "コピー用紙",
    "用紙",
    "ink",
    "toner",
    "cartridge",
    "drum",
    "refill",
    "refills",
    "replacement",
    "spare",
    "ribbon",
    "tape",
    "filter",
    "filters",
    "photo paper",
    "paper",
    "consumable",
]


# ===== ユーティリティ =====
def log(msg: str) -> None:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def ensure_dir(p: str) -> None:
    Path(p).mkdir(parents=True, exist_ok=True)


def load_db() -> Dict[str, dict]:
    if not Path(DB_PATH).exists():
        return {}
    try:
        return json.loads(Path(DB_PATH).read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def save_db(d: Dict[str, dict]) -> None:
    tmp = DB_PATH + ".tmp"
    Path(tmp).write_text(json.dumps(d, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, DB_PATH)


def sanitize(name: str, n: int = 80) -> str:
    name = re.sub(r'[\\/:*?"<>|]', "_", name).strip()
    name = re.sub(r"\s+", " ", name)
    return (name[:n].rstrip() or "untitled")


def price_to_int(text: str) -> Optional[int]:
    nums = []
    for m in re.findall(r"\d[\d,]*", text or ""):
        try:
            nums.append(int(m.replace(",", "")))
        except Exception:
            pass
    return max(nums) if nums else None


def uniq_keep_order(seq):
    seen = OrderedDict()
    for x in seq:
        if x and x not in seen:
            seen[x] = True
    return list(seen.keys())


def _vine_queue_label(url: str) -> str:
    """通知ヘッダ用のキュー名（例: queue=potluck -> pot-luck）"""
    try:
        q = (urllib.parse.parse_qs(urllib.parse.urlparse(url).query).get("queue", [""])[0] or "").strip()
        if q == "potluck":
            return "pot-luck"
        return q or "vine"
    except Exception:
        return "vine"


def _fmt_tg_item_event(event: str, *, asin: str, title: str, price_text: str, dp_url: str, vine_url: str) -> str:
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    queue_label = _vine_queue_label(vine_url)
    return "\n".join(
        [
            f"【{queue_label}】[{ts}] {event}",
            (title or "No Title").strip() or "No Title",
            (price_text or "価格不明").strip() or "価格不明",
            f"ASIN: {asin}",
            dp_url,
            vine_url,
        ]
    )


def parse_hhmm(s: str) -> int:
    h, m = s.split(":")
    h = int(h)
    m = int(m)
    if not (0 <= h < 24 and 0 <= m < 60):
        raise ValueError
    return h * 60 + m


def parse_windows(spec: str):
    if not spec:
        return []
    out = []
    for part in spec.split(","):
        part = part.strip()
        if "-" not in part:
            continue
        a, b = part.split("-", 1)
        try:
            sa, sb = parse_hhmm(a), parse_hhmm(b)
            out.append((sa, sb, sb <= sa))
        except Exception:
            pass
    return out


def in_fast_window(now: datetime.datetime, wins) -> bool:
    m = now.hour * 60 + now.minute
    for sa, sb, wrap in wins:
        if (not wrap and sa <= m < sb) or (wrap and (m >= sa or m < sb)):
            return True
    return False


def highlight_brands(title: str, brands) -> str:
    t = title or ""
    for b in sorted({str(x).strip() for x in brands if x}, key=len, reverse=True):
        try:
            # 途中で \033[0m リセットすると外側の装飾（太字など）が切れるため、前景色だけ戻す
            t = re.sub(
                re.escape(b),
                lambda m: _ansi_keep(m.group(0), "35", "39"),
                t,
                flags=re.IGNORECASE,
            )
        except Exception:
            pass
    return t


def fmt_price(price_text: str, threshold: int) -> str:
    p = (price_text or "").strip()
    m = re.findall(r"\d[\d,]*", p)
    val = None
    try:
        val = int(m[-1].replace(",", "")) if m else None
    except Exception:
        val = None
    if val is None:
        return f"{E('💴 ')}［" + Gy("価格不明") + "]"
    col = Rb if val >= max(0, int(threshold or 0)) else Gb
    return f"{E('💴 ')}［" + col(p) + "]"


def color_log_line(dt_iso: str, title: str, price_text: str, threshold: int, brands=None) -> str:
    """商品行のカラー出力。必ず [YYYY-MM-DD HH:MM:SS] を先頭に付与し、
    タイムスタンプ欠落時に先頭が「、」になる不具合を回避。
    """
    # --- タイムスタンプ整形（必ず入れる／ISO8601 の T をスペースに） ---
    try:
        t_raw = (dt_iso or "").strip()
        if not t_raw:
            dt = datetime.datetime.now()
        else:
            if "T" in t_raw and len(t_raw) >= 19:
                t_raw = t_raw.replace("T", " ", 1)
            try:
                dt = datetime.datetime.fromisoformat(t_raw.split(".")[0])
            except Exception:
                dt = datetime.datetime.now()
    except Exception:
        dt = datetime.datetime.now()

    ts = dt.strftime("%Y-%m-%d %H:%M:%S")
    # NOTE: 商品行（新着ログ）の時刻は背景色つきで強調する
    head = Cbg(f"[{ts}]")

    # --- 本文パーツ（空要素は連結しない） ---
    t = highlight_brands((title or "No Title").strip(), brands or [])
    body_parts = []
    if t:
        body_parts.append(f"『{B(t)}』")
    body_parts.append(fmt_price(price_text, threshold))

    body = "、".join([p for p in body_parts if p])
    return f"{head} {body}" if body else head


# ===== ログ補助 =====
def _now_ts() -> str:
    """現在時刻を 'YYYY-MM-DD HH:MM:SS' で返す"""
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _tag_time() -> str:
    """時刻タグ（他ログは時刻に色を付けない）"""
    return f"[{_now_ts()}]"


def _tag(stage: str, color_fn=None) -> str:
    """角括弧タグを色付きで作る"""
    s = f"[{stage}]"
    return color_fn(s) if color_fn else s


def log_info(msg: str) -> None:
    """通常情報ログ（黄系）"""
    print(f"{_tag_time()} {Y(str(msg))}", flush=True)


def log_ok(msg: str) -> None:
    """成功ログ（緑）"""
    print(f"{_tag_time()} {Gb(str(msg))}", flush=True)


def log_warn(msg: str) -> None:
    """注意ログ（紫）"""
    print(f"{_tag_time()} {M(str(msg))}", flush=True)


def log_err(msg: str) -> None:
    """エラーログ（赤）"""
    print(f"{_tag_time()} {Rb(str(msg))}", flush=True)


def log_stage(stage: str, msg: str = "") -> None:
    """処理ステージ表示。例: [CTA押下] xxx"""
    tag = _tag(stage, color_fn=Yb)
    body = f" {msg}" if msg else ""
    print(f"{_tag_time()} {tag}{body}", flush=True)


def log_scan_summary_jp(prefix: str, total: int, newp: int, existp: int, shots: int, skipped: int, errs: int) -> None:
    """
    スキャン結果を日本語で統一フォーマット出力。
    例: [2025-01-23 12:34:56] スキャン: 検知 6 件 | 新規 2 件（スクショ 2 件）| 既知処理 3 件 | 既知スキップ 1 件 | エラー 0 件
    """
    head = f"{prefix or 'スキャン'}:"
    line = (
        f"{head} 検知 {total} 件 | 新規 {newp} 件（スクショ {shots} 件）| "
        f"既知処理 {existp} 件 | 既知スキップ {skipped} 件 | エラー {errs} 件"
    )
    # 件数0のときは淡色、>0で通常色
    if total == 0 and errs == 0:
        print(f"{_tag_time()} {Gy(line)}", flush=True)
    elif errs > 0:
        print(f"{_tag_time()} {Rb(line)}", flush=True)
    else:
        print(f"{_tag_time()} {Y(line)}", flush=True)


def log_order_progress(stage: str, detail: str = "") -> None:
    """
    注文フローの進捗を明示。
    stage 例: 'CTA押下', 'モーダル検出', '確認送信', '注文確定ボタン', 'サンクス判定'
    """
    tag = _tag(stage, color_fn=M)
    body = f" {detail}" if detail else ""
    print(f"{_tag_time()} {tag}{body}", flush=True)


def log_interval(eff_seconds: int, fast: bool) -> None:
    """現在の更新間隔を目立つ形で出力"""
    mode = "高速" if fast else "通常"
    icon = E("⏱ ")
    print(f"{_tag_time()} {icon}現在の更新間隔: {Gb(str(eff_seconds))} 秒（{Yb(mode)}）", flush=True)


# ===== メール通知（任意設定） =====
def _mail_enabled() -> bool:
    try:
        return os.environ.get("VINE_MAIL_ENABLED", "0").strip().lower() in ("1", "true", "yes", "on")
    except Exception:
        return False


def _mail_send(subject: str, body: str) -> bool:
    """
    簡易SMTP送信（SSL/Gmail対応）。環境変数で設定：
      VINE_MAIL_ENABLED=1
      VINE_SMTP_HOST=smtp.gmail.com
      VINE_SMTP_PORT=465
      VINE_SMTP_USER=（SMTPユーザー / Gmailアドレス）
      VINE_SMTP_PASS=（アプリパスワード等）
      VINE_MAIL_TO=通知先（デフォルト: h0301m@gmail.com）
      VINE_MAIL_FROM=送信元（既定は USER または TO）
    """
    if not _mail_enabled():
        return False
    host = os.environ.get("VINE_SMTP_HOST", "smtp.gmail.com")
    try:
        port = int(os.environ.get("VINE_SMTP_PORT", "465"))
    except Exception:
        port = 465
    user = os.environ.get("VINE_SMTP_USER", os.environ.get("VINE_MAIL_USER", ""))
    pwd = os.environ.get("VINE_SMTP_PASS", os.environ.get("VINE_MAIL_PASS", ""))
    to = os.environ.get("VINE_MAIL_TO", "h0301m@gmail.com")
    frm = os.environ.get("VINE_MAIL_FROM", user or to)

    if not user or not pwd or not to:
        log_warn("メール送信スキップ（SMTP資格情報が未設定）")
        return False

    try:
        from email.message import EmailMessage
        import smtplib
        import ssl

        msg = EmailMessage()
        msg["Subject"] = subject
        msg["From"] = frm
        msg["To"] = to
        msg.set_content(body)
        ctx = ssl.create_default_context()
        with smtplib.SMTP_SSL(host, port, context=ctx, timeout=20) as s:
            s.login(user, pwd)
            s.send_message(msg)
        log_info(f"メール送信: {subject}")
        return True
    except Exception as e:
        log_warn(f"メール送信失敗: {e}")
        return False


def notify_high_price(*, asin: str, title: str, price_text: str, dp_url: str, vine_url: str) -> bool:
    """しきい値以上の検知を通知（一度だけ）。"""
    subj = "【Vine】高額候補を検知"
    body = (
        f"タイトル: {title}\n"
        f"価格: {price_text}\n"
        f"ASIN: {asin}\n"
        f"URL: {dp_url}\n"
        f"Vine: {vine_url}\n"
        f"日時: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}\n"
    )
    sent_mail = _mail_send(subj, body)
    sent_tg = tg_send(_fmt_tg_item_event("高額候補", asin=asin, title=title, price_text=price_text, dp_url=dp_url, vine_url=vine_url))
    return bool(sent_mail or sent_tg)


def notify_order_success(*, asin: str, title: str, price_text: str, dp_url: str, vine_url: str, reason: str = "") -> bool:
    """自動注文成功の通知（一度だけ）。"""
    reason_s = f"\n理由: {reason}" if reason else ""
    subj = "【Vine】自動注文 成功"
    body = (
        f"タイトル: {title}\n"
        f"価格: {price_text}\n"
        f"ASIN: {asin}\n"
        f"URL: {dp_url}\n"
        f"Vine: {vine_url}\n"
        f"日時: {datetime.datetime.now():%Y-%m-%d %H:%M:%S}{reason_s}\n"
    )
    sent_mail = _mail_send(subj, body)
    sent_tg = tg_send(_fmt_tg_item_event("自動注文 成功", asin=asin, title=title, price_text=price_text, dp_url=dp_url, vine_url=vine_url))
    return bool(sent_mail or sent_tg)


# ===== キー読み取り =====
class KeyReader:
    def __init__(self, q: "queue.Queue"):
        self.q = q
        self._stop = False
        self.enabled = False
        self.source = ""
        self.fd = None
        self._old = None
        self._tty_fd = None
        try:
            if sys.stdin.isatty():
                self.fd = sys.stdin.fileno()
                self.enabled = True
                self.source = "stdin"
        except Exception:
            pass
        if not self.enabled:
            try:
                self._tty_fd = os.open("/dev/tty", os.O_RDONLY)
                self.fd = self._tty_fd
                self.enabled = True
                self.source = "/dev/tty"
            except Exception:
                self.enabled = False
        if self.enabled and self.fd is not None:
            try:
                self._old = termios.tcgetattr(self.fd)
            except Exception:
                self._old = None

    def start(self):
        if not self.enabled or self.fd is None:
            return
        import threading

        threading.Thread(target=self._run, daemon=True).start()

    def _run(self):
        if self.enabled and self.fd is not None and self._old is not None:
            try:
                tty.setcbreak(self.fd)
            except Exception:
                pass
        try:
            while not self._stop:
                try:
                    r, _, _ = select.select([self.fd], [], [], 0.03)
                except Exception:
                    r = []
                if r:
                    try:
                        chb = os.read(self.fd, 1)
                    except Exception:
                        chb = b""
                    if chb:
                        try:
                            ch = chb.decode("utf-8", "ignore")
                        except Exception:
                            ch = ""
                        if ch:
                            chn = unicodedata.normalize("NFKC", ch)
                            self.q.put(chn)
        finally:
            if self.enabled and self.fd is not None and self._old is not None:
                try:
                    termios.tcsetattr(self.fd, termios.TCSADRAIN, self._old)
                except Exception:
                    pass
            if self._tty_fd is not None:
                try:
                    os.close(self._tty_fd)
                except Exception:
                    pass

    def stop(self):
        self._stop = True


# ===== 収集JS =====
COLLECT_JS = r"""() => {
  // ---- Vine厳密収集 v7（カート/ナビ誤検知対策を追加）----
  // ポリシー:
  //  1) URL が /vine/ を含む場合は「ページ全体＝Vine領域」。Vine外ウィジェットの誤検知は見出しテキストの deny で除外。
  //  2) 可能なら Vine ルート要素（グリッド/スクロールコンテナ）を優先して限定探索。
  //  3) data-asin or /dp/ があれば採用（CTA の有無は問わない）。
  //  4) 0件時は安全なフォールバック（/vine/ページ限定）で全体から拾う。

  const isVinePage = /\/vine\//.test(location.pathname || "") || /queue=potluck/.test(location.search || "");

  const denySections = /(閲覧履歴の新着|閲覧履歴に基づくおすすめ|最近閲覧した商品に関連|最近チェックした|視聴履歴に基づくおすすめ|読書履歴に基づくおすすめ|Amazon\s*おすすめ|お買い得セール|タイムセール(?:祭り)?|プライムデー|ブラックフライデー|ランキング|ベストセラー|セール中|特選タイムセール|Amazon\s*デバイス(?:・アクセサリ)?|Amazon\s*Devices?|Alexa(?:と連動する)?|スマート\s*ホーム(?:商品|デバイス|アクセサリ)?|人気のスマートホーム(?:商品|デバイス)|Works\s*with\s*Alexa|Ring|Blink|Eero|Fire\s*TV|Fire\s*Tablet|Echo(?:\s*Dot|\s*Show|\s*Bud)?|Kindle|Amazon\s*Smart\s*Plug|Amazon\s*basics|ショッピングカート|カートに追加|カート|Shopping\s*Cart|Added\s*to\s*Cart|\bCart\b)/i;

  const allowVineSections = /(\bVine\b|\bVINE\b|Vine\s*メンバー|Vine\s*対象|Vine\s*限定|Vine\s*おすすめ|Vine\s*商品|Vine\s*アイテム)/i;

  // ナビ/カート等のVine外UIを除外（カート内商品の誤検知対策）
  const reAriaCart = /(ショッピングカート|カート|Shopping\s*Cart|Added\s*to\s*Cart|\bCart\b)/i;

  function isExcludedNode(el){
    try{
      if(!el || el.nodeType !== 1) return false;
      const id = (el.id || "");
      if(id === "nav-flyout-ewc") return true;        // mini cart flyout
      if(id && id.startsWith("nav-flyout")) return true;
      if(id && (id === "nav-belt" || id === "nav-main" || id === "navbar" || id === "navFooter" || id === "nav-subnav" || id === "nav-tools")) return true;
      if(id && /(nav-cart|cart|ewc)/i.test(id)) return true;
      const aria = (el.getAttribute && (el.getAttribute("aria-label") || "")) || "";
      if(aria && reAriaCart.test(aria)) return true;
      const cls = String(el.className || "");
      if(cls && /(nav-flyout|ewc|mini-cart|nav-cart)/i.test(cls)) return true;
    }catch(e){}
    return false;
  }

  function inExcludedArea(node){
    let el = (node && node.nodeType === 1) ? node : (node && node.parentElement ? node.parentElement : null);
    let hop = 0;
    while(el && hop < 40){
      if(isExcludedNode(el)) return true;
      let next = null;
      try{ next = el.parentElement; }catch(e){ next = null; }
      if(!next){
        try{
          const rn = el.getRootNode && el.getRootNode();
          next = rn && rn.host ? rn.host : null; // shadow DOM 跨ぎ
        }catch(e){ next = null; }
      }
      el = next;
      hop++;
    }
    return false;
  }

  // Vine ルート候補（環境により変わるため複数）
  const VINE_ROOT_SELECTORS = [
    '.vvp-items-button-scroll-container',
    '#vvp-items-grid-container',
    '.vvp-items-grid-container',
    '.vvp-items-container',
    '.vvp-tab-content',
    '.vvp-items-button-and-search-container',
    '[data-component-type="vvp-item-card"]',
    '[data-component-type="vvp-items-grid"]'
  ];

  function qsaDeep(root, sel){
    let out=[...root.querySelectorAll(sel)];
    const w=document.createTreeWalker(root, NodeFilter.SHOW_ELEMENT);
    const seen=new Set();
    while(w.nextNode()){
      const el=w.currentNode;
      if(el && el.shadowRoot && !seen.has(el.shadowRoot)){
        seen.add(el.shadowRoot);
        out = out.concat(qsaDeep(el.shadowRoot, sel));
      }
    }
    return out;
  }

  function sectionText(node){
    let cur=node, hop=0;
    while(cur && hop<20){
      if(cur.getAttribute){
        const aria=cur.getAttribute('aria-label');
        if(aria && aria.trim()) return aria.trim();
      }
      if(cur.querySelector){
        const hs=cur.querySelectorAll('h1,h2,h3,h4,[role="heading"]');
        for(const h of hs){
          const t=(h.innerText||h.textContent||'').trim(); if(t) return t;
        }
      }
      cur=cur.parentElement; hop++;
    }
    return "";
  }

  function cardText(node){
    try{
      const MAX = 2000;
      const t = (node && (node.innerText||node.textContent||"")) || "";
      return t.slice(0, MAX);
    }catch(e){ return ""; }
  }

  function extractDP(href){
    if(!href) return "";
    const m = href.match(/\/(?:dp|gp\/product|-[^\/]*\/dp)\/([A-Z0-9]{10})/);
    return m ? ('https://www.amazon.co.jp/dp/'+m[1]) : "";
  }

  function findVineRoot(){
    for(const sel of VINE_ROOT_SELECTORS){
      try{
        const el = document.querySelector(sel);
        if(el) return el;
      }catch(e){}
    }
    return null;
  }

  const ROOT = findVineRoot();

	  // Vine領域判定
	  function inVineSection(node){
	    if(inExcludedArea(node || document.body)) return false; // カート/ナビ等は常に除外
	    // Vine ルート内は最優先で許可（body側の見出し汚染で全体が deny されるのを防ぐ）
	    try{ if(ROOT && node && ROOT.contains(node)) return true; }catch(e){}
	    const sect = sectionText(node || document.body);
	    if(sect && denySections.test(sect)) return false; // deny は常に優先除外
	    if(isVinePage) return true;                       // /vine/ ならページ全体を許可
	    // 通常ページではルート内 or 見出しで Vine を確認
	    return allowVineSections.test(sect || "");
	  }

  const uniq = new Set();
  const out = [];

  function pushIfVine(asin, dp, via, ctx){
    if(!asin || !/^[A-Z0-9]{10}$/.test(asin)) return;
    if(!inVineSection(ctx || document.body)) return;
    if(!dp) dp='https://www.amazon.co.jp/dp/'+asin;
    if(!uniq.has(asin)){ uniq.add(asin); out.push({asin, dp, via}); }
  }

  // 優先: ルートが見つかったらその内側のみ探索
  const scope = ROOT || (isVinePage ? document : document);

  // data-asin カード
  qsaDeep(scope, '[data-asin]').forEach(card=>{
    const asin=(card.getAttribute('data-asin')||'').trim();
    if(!/^[A-Z0-9]{10}$/.test(asin)) return;
    let dp="";
    const links=qsaDeep(card, 'a[href*="/dp/"], a[href*="/gp/product/"], a[href*="/-/ja/dp/"]');
    for(const a of links){ const d=extractDP(a.href); if(d){ dp=d; break; } }
    pushIfVine(asin, dp, 'card', card);
  });

  // 裸の /dp/ リンク
  qsaDeep(scope, 'a[href*="/dp/"], a[href*="/gp/product/"], a[href*="/-/ja/dp/"]').forEach(a=>{
    const dp=extractDP(a.href); if(!dp) return;
    const m=dp.match(/([A-Z0-9]{10})$/); const asin=m?m[1]:"";
    if(!asin) return;
    const card = a.closest('[data-asin]') || a.parentElement || a;
    pushIfVine(asin, dp, 'link', card);
  });

  // CTA 起点（Vine領域限定）
  const reCTA = /(商品のリクエスト|この商品をリクエスト|申し込|申し込み|リクエストする|詳細はこちら|詳しく|詳細|See\s*details|More\s*details|Details|View\s*details)/i;
  qsaDeep(scope, 'button, a, span, input[type="submit"]').forEach(el=>{
    const t=(el.innerText||el.textContent||el.value||'').trim();
    const aria=(el.getAttribute && (el.getAttribute('aria-label')||'')) || '';
    if(!(reCTA.test(t) || reCTA.test(aria))) return;
    const card = el.closest('[data-asin]') || el;
    let asin="", dp="";
    if(card){ asin=(card.getAttribute('data-asin')||'').trim(); }
    if(!asin){
      const lk = el.closest('a[href*="/dp/"], a[href*="/gp/product/"]') || (el.querySelector && el.querySelector('a[href*="/dp/"], a[href*="/gp/product/"]'));
      if(lk){ dp=extractDP(lk.href); const m=dp.match(/([A-Z0-9]{10})$/); if(m) asin=m[1]; }
    }
    if(!asin) return;
    pushIfVine(asin, dp, 'cta', card);
  });

  // ---- 0件フォールバック（/vine/ページ限定）----
  if(isVinePage && out.length===0){
    const seen=new Set();
    qsaDeep(document, '[data-asin]').forEach(card=>{
      const asin=(card.getAttribute('data-asin')||'').trim();
      if(!/^[A-Z0-9]{10}$/.test(asin)) return;
      if(inExcludedArea(card)) return;
      const sect = sectionText(card);
      const txt  = cardText(card);
      if(sect && denySections.test(sect)) return;
      if(/Amazon\s*デバイス|Fire\s*TV|Echo|Kindle/i.test(txt)) return;
      const a = card.querySelector('a[href*="/dp/"], a[href*="/gp/product/"]');
      const dp = a ? extractDP(a.href) : ('https://www.amazon.co.jp/dp/'+asin);
      if(!seen.has(asin)){ seen.add(asin); out.push({asin, dp, via:"fallback"}); }
    });
  }

  return out;
}
"""


# ===== 本体 =====
class VineWatcher:
    def _wait_order_placement(self, timeout_s: float = 25.0, asin: Optional[str] = None) -> Tuple[bool, str]:
        """
        「注文を確定する」押下後に、本当に確定(サンクスページ)まで到達したかを強化判定する。
        戻り値: (placed: bool, reason: str)
          placed=True  : サンクスURL/テキスト/注文番号/カード状態変化（Vine一覧復帰時含む）を検出
          placed=False : サインイン要求/追加認証/在庫・上限・一般エラー/タイムアウト等
        """
        end = time.time() + max(5.0, float(timeout_s))
        checked_follow = False
        target_asin = (asin or getattr(self, "_last_captured_asin", "") or "").strip()

        # 正規表現パターン
        re_url_thanks = re.compile(r"/(buy|gp/buy|checkout).*(thank|thanks|complete|confirmation)", re.I)
        re_url_vine = re.compile(r"/vine/(?:vine-items|)", re.I)
        re_url_signin = re.compile(r"/ap/(?:signin|ap-signin)", re.I)
        re_url_auth = re.compile(r"/ap/(?:mfa|cvf|challenge)", re.I)
        re_url_error = re.compile(r"/(gp/error|errors/|error/)", re.I)
        re_order_id = re.compile(r"(\d{3}-\d{7}-\d{7})")

        success_texts = [
            "ご注文が確定しました",
            "ご注文ありがとうございました",
            "ご注文ありがとうございます",
            "注文を承りました",
            "注文が確定しました",
            "Vine へのリクエストを受け付けました",
            "リクエストを受け付けました",
            "申し込みが完了",
            "リクエストを送信しました",
            "申し込みが受け付けられました",
            "ありがとうございました",
            "Thank you, your order has been placed",
            "Your order has been placed",
            "Order placed",
            "Order confirmation",
        ]
        order_number_hints = ["注文番号", "Order Number", "Order #", "注文 #", "ご注文番号"]
        hard_fail_texts = [
            "在庫切れ",
            "申し訳ありません",
            "現在この商品はお申し込みいただけません",
            "処理できません",
            "問題が発生",
            "もう一度お試しください",
            "limit",
            "上限",
            "上限に達しました",
            "既に申し込まれています",
            "already requested",
            "could not process",
            "out of stock",
            "not eligible",
            "not available",
        ]
        auth_texts = [
            "サインイン",
            "ログイン",
            "本人確認",
            "認証コード",
            "二段階認証",
            "2 段階認証",
            "captcha",
            "セキュリティ確認",
        ]

        while time.time() < end:
            # 新規タブに遷移している可能性があるため最初だけ追従
            if not checked_follow:
                try:
                    self._follow_checkout_tab(timeout_s=4.0)
                except Exception:
                    pass
                checked_follow = True

            try:
                url = self.page.url or ""
            except Exception:
                url = ""
            try:
                title = self.page.title() or ""
            except Exception:
                title = ""
            try:
                # bodyのテキストを優先。失敗時はHTML全体
                try:
                    body_text = self.page.locator("body").inner_text(timeout=1200)
                except Exception:
                    body_text = ""
                if not body_text:
                    body_text = self.page.content() or ""
            except Exception:
                body_text = ""

            # --- 成功判定（URL優先）
            try:
                if re_url_thanks.search(url):
                    log_ok("注文完了ページを検出（URL 判定）")
                    return True, "thankyou-url"
            except Exception:
                pass

            # --- 成功判定（本文・タイトル）
            try:
                if any(t in body_text for t in success_texts) or any(t in title for t in success_texts):
                    log_ok("注文完了ページを検出（テキスト 判定）")
                    return True, "thankyou-text"
            except Exception:
                pass

            # --- 成功判定（注文番号の存在）
            try:
                if any(h in body_text for h in order_number_hints):
                    # 形式まで取れたら reason に含める（取れなくても完了扱い）
                    m = re_order_id.search(body_text or "")
                    if m:
                        log_ok("注文番号を検出（完了と判断）")
                        return True, f"order-id:{m.group(1)}"
                    log_ok("注文番号ヒントを検出（完了と判断）")
                    return True, "order-number"
            except Exception:
                pass

            # --- サインイン/追加認証の検出（失敗扱い）
            try:
                if re_url_signin.search(url) or any(t in title for t in ["サインイン", "Sign-In"]) or "サインイン" in body_text:
                    log_warn("サインイン要求で停止しました")
                    return False, "signin-required"
                if re_url_auth.search(url) or any(t.lower() in body_text.lower() for t in [*auth_texts]):
                    log_warn("追加認証（2FA/CAPTCHA）で停止しました")
                    return False, "auth-challenge"
            except Exception:
                pass

            # --- ハードエラー（在庫/上限など）
            try:
                if re_url_error.search(url) or any(t.lower() in body_text.lower() for t in [*hard_fail_texts]):
                    log_err("在庫/上限/一般エラーを検出しました")
                    return False, "unavailable-or-limit"
            except Exception:
                pass

            # --- Vine 一覧へ戻った場合の「カード消失」判定（成功扱い）
            try:
                if re_url_vine.search(url):
                    if target_asin:
                        # 少し待ってから対象カードの存在を確認
                        time.sleep(0.6)
                        try:
                            present = self.page.evaluate(
                                """asin => !!document.querySelector(`[data-asin="${asin}"]`)""",
                                target_asin,
                            )
                        except Exception:
                            present = None
                        if present is False:
                            log_ok("Vine一覧に復帰・対象カードの消失を確認 → 申し込み成功と判断")
                            return True, "returned-vine-gone"
                        # カードが残っていても、状態が requested に変化していれば成功扱い
                        try:
                            card = self._find_card_locator(target_asin)
                            if card and card.count() > 0:
                                ctxt = ""
                                try:
                                    ctxt = card.inner_text(timeout=500)
                                except Exception:
                                    ctxt = ""
                                if any(k in (ctxt or "") for k in ["申し込み済み", "リクエスト済み", "リクエストを取り消す", "Requested"]):
                                    log_ok("Vine一覧に復帰・対象カードの状態変化（申込済み）を確認 → 成功と判断")
                                    return True, "returned-vine-requested"
                        except Exception:
                            pass
            except Exception:
                pass

            # 継続待機
            time.sleep(0.35)

        log_warn("サンクス判定がタイムアウトしました")
        return False, "timeout"

    def _is_checkout_like(self) -> bool:
        """チェックアウト/注文確認系の画面かざっくり判定（誤クリック防止用）"""
        try:
            url = self.page.url or ""
        except Exception:
            url = ""
        try:
            title = self.page.title() or ""
        except Exception:
            title = ""
        patterns_url = r"/(buy|gp/buy|thankyou|checkout|gp/cart|cart|ap/signin|confirm|confirmation|payselect|payments|shipaddress|shipoption)"
        patterns_title = r"(注文|ご注文|レジ|チェックアウト|お支払い|配送|アドレス|確認|支払い方法|ありがとうございます|完了|確定|Thank you|Order placed)"
        try:
            if re.search(patterns_url, url, re.I):
                return True
        except Exception:
            pass
        try:
            if re.search(patterns_title, title):
                return True
        except Exception:
            pass
        return False

    def __init__(self, headed: bool):
        self.headed = headed
        self.fast_wins = parse_windows(FAST_WINDOWS)
        ensure_dir(SHOTS_DIR)
        ensure_dir(PROFILE_DIR)
        self.db = load_db()
        self.q = queue.Queue()
        self.key = KeyReader(self.q)
        self.running = True
        self.paused = False
        self._force = False
        self._sig_quick = False
        self._sig_deep = False
        self._pw = None
        self._ctx = None
        self.page = None

        # runtime-tunable config (キー操作で変更するものはインスタンス属性へ)
        self.interval = int(INTERVAL)
        self.fast_interval = int(FAST_INTERVAL)
        self.allow_dup_order = bool(ALLOW_DUP_ORDER)
        self.tab_foreground = bool(TAB_FOREGROUND)
        try:
            saved_tab_fg = self.db.get("__tab_foreground")
            if isinstance(saved_tab_fg, bool):
                self.tab_foreground = saved_tab_fg
        except Exception:
            pass

        self.order_threshold = ORDER_THRESHOLD
        self.brand_always = set()
        self._load_brands()
        self.auto_order = bool(AUTO_ORDER)
        self.shot_enabled = True
        self._last_captured_asin = ""
        self._last_captured_url = ""
        self._ordering = False
        self._suspend_depth = 0
        self._paused_saved = False
        self._selectors = {}
        self._load_selectors()
        self._current_interval_effective = (
            self.fast_interval if in_fast_window(datetime.datetime.now(), self.fast_wins) else self.interval
        )

    # ---- セレクタ記憶（ボタン位置の学習） ----
    def _load_selectors(self):
        try:
            sel = self.db.get("__selectors")
            if isinstance(sel, dict):
                # 正規化
                norm = {}
                for cat, arr in sel.items():
                    lst = []
                    for it in (arr or []):
                        if isinstance(it, dict) and it.get("sel"):
                            lst.append({"sel": str(it["sel"]), "hits": int(it.get("hits", 0)), "last": str(it.get("last", ""))})
                        elif isinstance(it, str):
                            lst.append({"sel": it, "hits": 0, "last": ""})
                    norm[cat] = lst
                self._selectors = norm
            else:
                self._selectors = {}
        except Exception:
            self._selectors = {}

    def _save_selectors(self):
        try:
            self.db["__selectors"] = self._selectors
            save_db(self.db)
        except Exception:
            pass

    def _remember_selector(self, category: str, sel: str):
        """成功したセレクタを記憶して次回に優先使用"""
        if not category or not sel:
            return
        try:
            arr = self._selectors.get(category, [])
            # 既存があればカウントアップ
            for it in arr:
                if it.get("sel") == sel:
                    it["hits"] = int(it.get("hits", 0)) + 1
                    it["last"] = datetime.datetime.now().isoformat(timespec="seconds")
                    break
            else:
                arr.insert(0, {"sel": sel, "hits": 1, "last": datetime.datetime.now().isoformat(timespec="seconds")})
            # 重複除去＆スコア順に並べ替え
            seen = set()
            dedup = []
            for it in arr:
                k = it.get("sel")
                if k and k not in seen:
                    seen.add(k)
                    dedup.append(it)
            arr = sorted(dedup, key=lambda x: (int(x.get("hits", 0)), x.get("last", "")), reverse=True)[:50]
            self._selectors[category] = arr
            self._save_selectors()
        except Exception:
            pass

    def _iter_selectors(self, category: str, base: list):
        """記憶済みセレクタを優先して列挙（重複は除外）"""
        yielded = set()
        # memory first
        for it in self._selectors.get(category, []) or []:
            s = it.get("sel")
            if not s or s in yielded:
                continue
            yielded.add(s)
            yield s
        # then base fallbacks
        for s in base or []:
            if s and s not in yielded:
                yielded.add(s)
                yield s

    def _try_click_by_selectors(self, scope, category: str, base_selectors: list, timeout: int, success_check_fn):
        """
        指定スコープ（カード/ダイアログ/ページ）内で、学習セレクタ→ベース順にクリックを試す。
        success_check_fn() が True を返した時点で記憶して成功とする。
        戻り値: (clicked: bool, used_selector: str or "")
        """
        used = ""
        for sel in self._iter_selectors(category, base_selectors):
            try:
                loc = scope.locator(sel).first
            except Exception:
                loc = None
            if not loc or not getattr(loc, "is_visible", lambda: False)():
                continue
            if self._click_like_human(loc, timeout=timeout):
                # 条件関数で成功判定
                ok = False
                try:
                    ok = bool(success_check_fn())
                except Exception:
                    ok = False
                if ok:
                    used = sel
                    self._remember_selector(category, sel)
                    return True, used
        return False, used

    # ---- リロード停止/再開（注文中は完全停止） ----
    def _suspend_refresh(self):
        try:
            if self._suspend_depth == 0:
                self._paused_saved = self.paused
                self.paused = True
            self._suspend_depth += 1
        except Exception:
            self.paused = True
            self._suspend_depth = max(1, int(getattr(self, "_suspend_depth", 0) or 0))

    def _resume_refresh(self):
        try:
            if self._suspend_depth > 0:
                self._suspend_depth -= 1
            if self._suspend_depth == 0:
                self.paused = self._paused_saved
        except Exception:
            self.paused = False
            self._suspend_depth = 0

    # ---- 自動リロードブロック（注文時） ----
    def _block_vine_reload_begin(self):
        try:
            self._nav_guard_pattern = "**/*"

            def _handler(route):
                try:
                    req = route.request
                    if req.is_navigation_request() and req.frame == self.page.main_frame:
                        url = req.url or ""
                        if "/vine/vine-items" in url:
                            return route.abort()
                except Exception:
                    pass
                try:
                    return route.continue_()
                except Exception:
                    try:
                        route.fallback()
                    except Exception:
                        pass

            self._nav_guard_handler = _handler
            self._ctx.route(self._nav_guard_pattern, self._nav_guard_handler)
        except Exception:
            pass
        try:
            self.page.evaluate(
                """(function(){
                  try{
                    const re = /\\/vine\\/vine-items/;
                    const _assign = window.location.assign?.bind(window.location);
                    const _replace = window.location.replace?.bind(window.location);
                    window.location.reload = function(){};
                    window.location.assign = function(u){ if(re.test(String(u||''))){ } else { return _assign(u); } };
                    window.location.replace = function(u){ if(re.test(String(u||''))){ } else { return _replace(u); } };
                  }catch(e){}
                })();"""
            )
        except Exception:
            pass

    def _block_vine_reload_end(self):
        try:
            if getattr(self, "_nav_guard_handler", None):
                self._ctx.unroute(getattr(self, "_nav_guard_pattern", "**/*"), self._nav_guard_handler)
        except Exception:
            pass
        self._nav_guard_handler = None

    # ---- Playwright 起動/再起動 ----
    def _open_browser(self):
        if self._pw is None:
            self._pw = sync_playwright().start()
        prefs = {
            "browser.link.open_newwindow": 3,
            "browser.link.open_newwindow.restriction": 0,
            "browser.tabs.loadDivertedInBackground": (not self.tab_foreground),
            "browser.tabs.loadInBackground": (not self.tab_foreground),
            "browser.tabs.opentabfor.middleclick": True,
            "browser.tabs.insertAfterCurrent": True,
            "dom.disable_open_during_load": False,
            "browser.tabs.warnOnOpen": False,
            "browser.link.open_newwindow.override.external": 3,
            "dom.min_background_timeout_value": 100,
            "dom.min_background_interval_value": 100,
            "dom.timeout.enable_budget_timer_throttling": False,
            "privacy.reduceTimerPrecision": False,
        }
        self._ctx = self._pw.firefox.launch_persistent_context(
            user_data_dir=PROFILE_DIR,
            headless=not self.headed,
            viewport={"width": 1280, "height": 900},
            java_script_enabled=True,
            firefox_user_prefs=prefs,
        )
        self._ctx.set_default_timeout(6000)
        self.page = self._ctx.pages[0] if self._ctx.pages else self._ctx.new_page()

    def _close_browser(self):
        try:
            if self.page:
                try:
                    self.page.close()
                except BaseException:
                    pass
            if self._ctx:
                try:
                    self._ctx.close()
                except BaseException:
                    pass
                finally:
                    self._ctx = None
            if self._pw:
                try:
                    self._pw.stop()
                except BaseException:
                    pass
                finally:
                    self._pw = None
        except BaseException:
            pass

    def _restart_toggle_head(self):
        self._close_browser()
        self.headed = not self.headed
        self._open_browser()
        self.page.goto(URL, wait_until="domcontentloaded", timeout=60000)

    def _restart_browser_keep_mode(self):
        self._close_browser()
        self._open_browser()
        self.page.goto(URL, wait_until="domcontentloaded", timeout=60000)

    def _toggle_tab_foreground(self):
        self.tab_foreground = not bool(self.tab_foreground)
        try:
            self.db["__tab_foreground"] = bool(self.tab_foreground)
            save_db(self.db)
        except Exception:
            pass
        self._restart_browser_keep_mode()
        print(f"新規タブ前面化: {'ON' if self.tab_foreground else 'OFF'}（f で切替）")

    # ---- 表示 ----
    def banner(self):
        os.system("clear" if os.name == "posix" else "cls")
        ts = f"{datetime.datetime.now():%Y-%m-%d %H:%M:%S}"
        print(f"[{ts}] Amazon Vine ウォッチャー（Firefox｜日本語UI｜厳密収集｜超低遅延）\n")
        print("ショートカット（主要）")
        print(" 🟢/⏸  状態: p   | 🔄 リロード: r   | 🖥 ヘッドレス切替: w")
        print(" ⏱   更新間隔: [ -1s / ] +1s ； { -10s / } +10s")
        print(" 🔍   スキャン: g 可視 / G 全件深")
        print(" 📦 既知含む: e 可視 / E 全件深（新規以外も処理・自動注文）")
        print(" 🛒 直前を注文: o")
        print(" 📊 週サマリ: u（過去7日・新着件数をTelegramへ）")
        print(" 🗂 新規タブ前面化: f 切替")
        print(" 📒   ログ: l/L 再出力   | 🏷 ブランド: b/B 編集")
        print(" 🧮 しきい値: t/T 変更 | 📸 スクショ: s 切替   | 🤖 自動注文: a 切替")
        print(f" キー入力: {'有効（' + self.key.source + '）' if self.key.enabled else '無効'}\n")
        print("URL：" + URL + "\n")
        print("現在の設定")
        print(f" 🟢 状態：{'稼働中' if not self.paused else '⏸ 一時停止中'}（p で切替）")
        print(f" 📸 スクショ：{'ON' if getattr(self, 'shot_enabled', True) else 'OFF'}（s で切替）")
        print(f" 🤖 自動注文：{'ON' if getattr(self, 'auto_order', bool(AUTO_ORDER)) else 'OFF'}（a で切替）")
        print(f" 🗂 新規タブ前面化：{'ON' if getattr(self, 'tab_foreground', bool(TAB_FOREGROUND)) else 'OFF'}（f で切替）")
        print(f" 🧮 しきい値：{self.order_threshold} 円（t で変更）")
        print(f" 🏷 常時ブランド：{len(self.brand_always)} 件（b で編集）")
        print(f" 📄 ブランドファイル：{Path(BRANDS_FILE).resolve()}")
        print(" 🚫 消耗品フィルタ：有効（NGキーワード簡易版）")
        print(f" 📝 出力：{'新着のみ' if ONLY_NEW else '通常'}")
        print(" ⏱ 更新間隔：")
        print(f"   ・通常：{self.interval} 秒")
        print(f"   ・高速：{self.fast_interval} 秒")
        eff_now = self.fast_interval if in_fast_window(datetime.datetime.now(), self.fast_wins) else self.interval
        print(f"   ・現在：{eff_now} 秒（{'高速' if eff_now == self.fast_interval else '通常'}）")
        print(f"   ・ウィンドウ：{FAST_WINDOWS or '（未設定）'}")
        print(f"   ・重複注文許可：{'ON' if self.allow_dup_order else 'OFF'}（VINE_ALLOW_DUP_ORDER）\n")
        print("保存先")
        print(f" 📂 Shots：{Path(SHOTS_DIR).resolve()}")
        print(f" 🗄 DB：   {Path(DB_PATH).resolve()}")
        print("----------------------------------------------------------")

    def _parse_dt(self, s: str) -> Optional[datetime.datetime]:
        if not s:
            return None
        raw = str(s).strip()
        if not raw:
            return None
        try:
            # "YYYY-MM-DDTHH:MM:SS" / "YYYY-MM-DD HH:MM:SS" を想定
            raw = raw.replace("Z", "")
            raw = raw.replace("T", " ", 1)
            raw = raw.split(".", 1)[0]
            return datetime.datetime.fromisoformat(raw)
        except Exception:
            return None

    def _build_weekly_new_summary(self, days: int = 7) -> str:
        now = datetime.datetime.now()
        since = now - datetime.timedelta(days=max(1, int(days)))
        buckets: Dict[datetime.datetime, int] = {}
        total = 0

        for asin, rec in (self.db or {}).items():
            if not asin or str(asin).startswith("__"):
                continue
            if not isinstance(rec, dict):
                continue
            dt = self._parse_dt(rec.get("first_seen") or rec.get("last_seen") or "")
            if not dt:
                continue
            if dt < since:
                continue
            key = dt.replace(minute=0, second=0, microsecond=0)
            buckets[key] = int(buckets.get(key, 0)) + 1
            total += 1

        queue_label = _vine_queue_label(URL)
        head = f"【{queue_label}】[{now:%Y-%m-%d %H:%M:%S}] 1週間サマリ（新着）"
        period = f"期間: {since:%Y-%m-%d %H:%M}〜{now:%Y-%m-%d %H:%M}"

        lines = []
        for dt in sorted(buckets.keys()):
            lines.append(f"{dt:%Y-%m-%d %H}:00 {buckets[dt]}件")

        if not lines:
            return "\n".join([head, "新着: 0件", period, URL])

        return "\n".join([head, *lines, f"合計: {total}件", period, URL])

    def send_weekly_new_summary(self) -> bool:
        msg = self._build_weekly_new_summary(days=7)
        return tg_send(msg)

    # ---- Vine移動/待機 ----
    def _await_items(self, timeout_ms: int = 12000):
        try:
            self.page.wait_for_load_state("domcontentloaded", timeout=timeout_ms // 2)
        except Exception:
            pass
        deadline = time.time() + (timeout_ms / 1000.0)
        seen = 0
        while time.time() < deadline:
            try:
                # まずは data-asin or /dp/ が現れるのを待つ
                self.page.wait_for_selector('[data-asin], a[href*="/dp/"]', timeout=800)
                # 実際に有効 ASIN が何件あるかを数える
                seen = (
                    self.page.evaluate(
                        """() => {
                    const reg=/^[A-Z0-9]{10}$/;
                    let n=0;
                    document.querySelectorAll('[data-asin]').forEach(e=>{
                        const a=(e.getAttribute('data-asin')||'').trim();
                        if(reg.test(a)) n++;
                    });
                    return n;
                }"""
                    )
                    or 0
                )
                if seen and seen > 0:
                    return
            except Exception:
                pass
            # 画面を少しスクロールしてレンダ促進 + 「もっと見る」押下
            try:
                self.page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight*0.6));")
            except Exception:
                pass
            try:
                for lbl in ["もっと見る", "さらに表示", "More", "See more"]:
                    loc = self.page.locator(f'button:has-text("{lbl}"), a:has-text("{lbl}")').first
                    if loc and loc.is_visible():
                        loc.click(timeout=500)
                        break
            except Exception:
                pass
            time.sleep(0.15)
        # タイムアウト時は何もしない（上位でデバッグ保存あり）

    def _click_more(self):
        for l in ["もっと見る", "さらに表示", "More", "See more"]:
            try:
                loc = self.page.locator(f'button:has-text("{l}"), a:has-text("{l}")').first
                if loc and loc.is_visible():
                    loc.click(timeout=500)
                    time.sleep(0.12)
            except Exception:
                pass

    def _auto_scroll(self, max_steps: int = 160, pause: float = 0.12):
        last_h = 0
        stagnant = 0
        for _ in range(max_steps):
            try:
                self.page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight*0.9));")
            except Exception:
                pass
            time.sleep(pause)
            self._click_more()
            try:
                self.page.wait_for_load_state("networkidle", timeout=700)
            except Exception:
                pass
            try:
                h = self.page.evaluate("document.documentElement.scrollHeight||document.body.scrollHeight||0")
                stagnant = stagnant + 1 if h <= last_h else 0
                last_h = h
            except Exception:
                pass
            if stagnant >= 3:
                break
        try:
            self.page.evaluate("window.scrollTo(0,0)")
        except Exception:
            pass

    def _goto_vine(self, deep: bool = False):
        bust = int(time.time())
        url = URL + (("&_ts=" + str(bust)) if ("?" in URL) else ("?_ts=" + str(bust)))
        self.page.goto(url, wait_until="domcontentloaded", timeout=60000)
        try:
            self.page.wait_for_load_state("networkidle", timeout=4000)
        except Exception:
            pass
        self._await_items()
        if deep:
            self._auto_scroll(max_steps=220, pause=0.10)

    def _safe_reload_vine(self):
        """リロード→失敗時は goto にフォールバック。"""
        try:
            self.page.reload(wait_until="domcontentloaded", timeout=25000)
            try:
                self.page.wait_for_load_state("networkidle", timeout=2000)
            except Exception:
                pass
            self._await_items(timeout_ms=6000)
        except Exception:
            try:
                self._goto_vine(deep=False)
            except Exception:
                pass

    # ---- 収集（全フレーム） ----
    def _collect_all_frames(self) -> List[dict]:
        items = []
        seen = set()

        def collect_from(frame):
            try:
                res = frame.evaluate(COLLECT_JS) or []
            except Exception:
                res = []
            for c in res:
                asin = (c or {}).get("asin", "")
                if asin and asin not in seen:
                    seen.add(asin)
                    items.append(c)

        try:
            collect_from(self.page.main_frame)
        except Exception:
            pass
        for fr in self.page.frames:
            if fr is self.page.main_frame:
                continue
            try:
                _ = fr.url
                collect_from(fr)
            except Exception:
                continue
        return items

    # ---- 価格抽出 ----
    def _get_price_text(self, p) -> str:
        sels = [
            "#corePriceDisplay_desktop_feature_div .a-offscreen",
            "#apex_price_display .a-offscreen",
            "#corePrice_feature_div .a-offscreen",
            'span[data-a-color="price"] .a-offscreen',
            ".a-price .a-offscreen",
            "#priceblock_ourprice",
            "#priceblock_dealprice",
            "#priceblock_saleprice",
            "#newBuyBoxPrice",
            "#tp_price_block_total_price_ww .a-offscreen",
            "#twister-plus-price-data-price",
        ]
        for sel in sels:
            try:
                t = p.locator(sel).first.inner_text(timeout=1200).strip()
                if t:
                    return t
            except Exception:
                pass
        try:
            whole = p.locator(".a-price .a-price-whole").first.inner_text(timeout=800)
            frac = ""
            try:
                frac = p.locator(".a-price .a-price-fraction").first.inner_text(timeout=500)
            except Exception:
                frac = ""
            if whole:
                whole = re.sub(r"[^0-9]", "", whole)
                frac = re.sub(r"[^0-9]", "", frac or "")
                if whole:
                    return f"￥{whole}{('.' + frac) if frac else ''}"
        except Exception:
            pass
        try:
            txt = p.locator('script[type="application/json"]').nth(0).inner_text(timeout=500)
            m = re.search(r"￥\s?\d[\d,]*", txt or "")
            if m:
                return m.group(0)
        except Exception:
            pass
        try:
            body = p.locator("body").inner_text(timeout=800)
        except Exception:
            try:
                body = p.content() or ""
            except Exception:
                body = ""
        m = re.search(r"￥\s?\d[\d,]*", body)
        return m.group(0) if m else ""

    # ---- DP取得・スクショ（同ウィンドウ別タブ） ----
    def _scrape_dp(self, dp_url: str) -> Tuple[str, str, str, str, list]:
        p = None
        try:
            if DP_OPEN_MODE == "tab":
                asin_m = re.search(r"/dp/([A-Z0-9]{10})", dp_url or "")
                target = None
                if asin_m:
                    asin = asin_m.group(1)
                    try:
                        target = self.page.locator(f'[data-asin="{asin}"] a[href*="/dp/"]').first
                        if (not target) or target.count() == 0:
                            target = self.page.locator(f'a[href*="/dp/{asin}"]').first
                    except Exception:
                        target = None
                try:
                    if target and target.count() > 0:
                        with self.page.expect_popup() as pop_info:
                            target.click(timeout=2000, button="left")
                        p = pop_info.value
                    else:
                        aid = "__pw_tmp_open_dp__"
                        self.page.evaluate(
                            "(url,id)=>{ let a=document.getElementById(id); if(!a){ a=document.createElement('a'); a.id=id; a.textContent='open'; a.style.cssText='position:fixed;left:-9999px;top:-9999px;width:1px;height:1px;opacity:0.01;z-index:2147483647;display:block;'; document.body.appendChild(a);} a.href=url; a.target='_blank'; a.rel='noopener'; }",
                            dp_url,
                            aid,
                        )
                        with self.page.expect_popup() as pop_info:
                            self.page.locator(f"#{aid}").click(timeout=1800, button="left")
                        p = pop_info.value
                except Exception:
                    p = self._ctx.new_page()
                    p.goto(dp_url, wait_until="domcontentloaded", timeout=60000)
                try:
                    if p and self.tab_foreground:
                        p.bring_to_front()
                except Exception:
                    pass
                try:
                    p.wait_for_load_state("domcontentloaded", timeout=60000)
                except Exception:
                    pass
                try:
                    p.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass
            else:
                p = self.page
                p.goto(dp_url, wait_until="domcontentloaded", timeout=60000)
                try:
                    p.wait_for_load_state("networkidle", timeout=3000)
                except Exception:
                    pass
            try:
                if "?" not in dp_url:
                    p.goto(dp_url + "?th=1", wait_until="domcontentloaded", timeout=60000)
                    try:
                        p.wait_for_load_state("networkidle", timeout=2000)
                    except Exception:
                        pass
            except Exception:
                pass

            title = ""
            for sel in ["#productTitle", "#title", "#ebooksProductTitle"]:
                try:
                    title = p.locator(sel).first.inner_text(timeout=2500).strip()
                    if title:
                        break
                except Exception:
                    pass
            if not title:
                try:
                    title = (p.title() or "").split(":")[0].strip() or "No Title"
                except Exception:
                    title = "No Title"

            price_text = self._get_price_text(p)
            if not price_text:
                for want in COLOR_PREF:
                    try:
                        opt = p.locator(f'button:has-text("{want}"), [role="option"]:has-text("{want}")').first
                        if opt and opt.is_visible():
                            opt.click(timeout=800, button="left")
                            time.sleep(0.2)
                            price_text = self._get_price_text(p)
                            if price_text:
                                break
                    except Exception:
                        pass

            variant_prices = []
            if VARY_COLLECT:
                try:
                    labels = self._collect_variant_labels_quick(p, VARY_MAX)
                    variant_prices = self._collect_variant_prices_from_dp(p, labels, VARY_MAX)
                except Exception:
                    variant_prices = []

            byline = ""
            for sel in ["#bylineInfo", "a#bylineInfo", ".byline"]:
                try:
                    byline = p.locator(sel).first.inner_text(timeout=800).strip()
                    if byline:
                        break
                except Exception:
                    pass

            page_text = ""
            try:
                page_text = p.locator("body").inner_text(timeout=1200)
            except Exception:
                try:
                    page_text = p.content() or ""
                except Exception:
                    page_text = ""

            shot = ""
            if getattr(self, "shot_enabled", True):
                ensure_dir(SHOTS_DIR)
                shot = os.path.join(SHOTS_DIR, f"{sanitize(title, 60)}_{datetime.datetime.now():%Y%m%d_%H%M%S}.png")
                try:
                    p.screenshot(path=shot, full_page=True)
                except Exception:
                    shot = ""

            return title, price_text, shot, (byline + "\n" + page_text), variant_prices
        finally:
            try:
                if DP_OPEN_MODE == "tab" and p is not None and p is not self.page:
                    p.close()
                elif DP_OPEN_MODE == "same":
                    self._goto_vine(deep=False)
            except Exception:
                pass

    # ---- カード探索＆CTA ----
    def _find_card_locator(self, asin: str):
        return self.page.locator(f'[data-asin="{asin}"]').first

    def _scroll_to_card(self, asin: str, max_steps: int = 64) -> bool:
        try:
            loc = self._find_card_locator(asin)
            if loc and loc.count() > 0:
                try:
                    loc.scroll_into_view_if_needed(timeout=500)
                except Exception:
                    pass
                return True
        except Exception:
            pass
        for _ in range(max_steps):
            try:
                self.page.evaluate("window.scrollBy(0, Math.floor(window.innerHeight*0.85));")
            except Exception:
                pass
            time.sleep(0.06)
            try:
                loc = self._find_card_locator(asin)
                if loc and loc.count() > 0:
                    try:
                        loc.scroll_into_view_if_needed(timeout=500)
                    except Exception:
                        pass
                    return True
            except Exception:
                pass
        try:
            self.page.evaluate("window.scrollTo(0,0)")
        except Exception:
            pass
        return False

    def _locate_cta_candidates(self, card):
        selectors = [
            'button:has-text("詳細はこちら")',
            'span.a-button-text:has-text("詳細はこちら")',
            'a:has-text("詳細はこちら")',
            'button:has-text("詳細")',
            'span.a-button-text:has-text("詳細")',
            'a:has-text("詳細")',
            'button:has-text("商品のリクエスト")',
            'a:has-text("商品のリクエスト")',
            'button:has-text("申し込")',
            'a:has-text("申し込")',
        ]
        out = []
        for sel in selectors:
            try:
                loc = card.locator(sel).first
                if loc and loc.count() > 0 and loc.is_visible():
                    out.append(loc)
            except Exception:
                continue
        return out

    def _find_cta_near_card(self, asin: str):
        card = self._find_card_locator(asin)
        if not card or card.count() == 0:
            return None
        try:
            box_card = card.bounding_box()
        except Exception:
            box_card = None
        if not box_card:
            return None
        cx = box_card["x"] + box_card["width"] / 2
        cy = box_card["y"] + box_card["height"] / 2
        sels = [
            'button:has-text("詳細はこちら")',
            'span.a-button-text:has-text("詳細はこちら")',
            'a:has-text("詳細はこちら")',
            'button:has-text("詳細")',
            'span.a-button-text:has-text("詳細")',
            'a:has-text("詳細")',
            'button:has-text("商品のリクエスト")',
            'a:has-text("商品のリクエスト")',
            'button:has-text("申し込")',
            'a:has-text("申し込")',
        ]
        best = None
        best_d = 1e18
        for sel in sels:
            try:
                for loc in self.page.locator(sel).all():
                    if not loc or not loc.is_visible():
                        continue
                    try:
                        box = loc.bounding_box()
                    except Exception:
                        box = None
                    if not box:
                        continue
                    px = box["x"] + box["width"] / 2
                    py = box["y"] + box["height"] / 2
                    d = (px - cx) * (px - cx) + (py - cy) * (py - cy)
                    if d < best_d:
                        best_d = d
                        best = loc
            except Exception:
                continue
        return best

    def _get_dialog(self, timeout_ms: int = 7000):
        sels = [
            '[role="dialog"]',
            "#a-popover-root .a-popover-wrapper",
            ".a-popover.a-popover-modal",
            ".a-modal-scroller",
            'div[aria-modal="true"]',
        ]
        t0 = time.time()
        dlg = None
        while time.time() - t0 < timeout_ms / 1000.0:
            for sel in sels:
                try:
                    locs = self.page.locator(sel)
                    for loc in locs.all():
                        try:
                            if loc and loc.is_visible():
                                dlg = loc
                        except Exception:
                            continue
                except Exception:
                    continue
            if dlg:
                try:
                    dlg.scroll_into_view_if_needed(timeout=400)
                except Exception:
                    pass
                return dlg
            time.sleep(0.05)
        return None

    def _wait_for_modal(self, timeout_s: float = 6.0) -> bool:
        return self._get_dialog(timeout_ms=int(timeout_s * 1000)) is not None

    def _click_like_human(self, loc, timeout: int = 900) -> bool:
        """
        人間らしいクリック + 多段フォールバックでの頑強クリック。
        手順:
          1) attach/visible/scrollIntoView/hover/focus
          2) Locator.click（通常）
          3) バウンディングボックス中心クリック（微小ジッタ付き）
          4) JS click() の直接発火
          5) Enter/Space キーでの起動
          6) force=True での強制クリック（最後の手段）
        いずれかが成功すれば True。
        """
        if not loc:
            return False
        try:
            # 要素の存在確認
            try:
                if hasattr(loc, "count") and loc.count() == 0:
                    return False
            except Exception:
                pass

            # 画面前面へ
            if ORDER_FRONT and self.headed:
                try:
                    self.page.bring_to_front()
                except Exception:
                    pass

            # アタッチ/可視/スクロール/ホバー/フォーカス
            try:
                loc.wait_for(state="attached", timeout=min(400, timeout))
            except Exception:
                pass
            try:
                loc.wait_for(state="visible", timeout=min(700, timeout))
            except Exception:
                pass
            try:
                loc.scroll_into_view_if_needed(timeout=500)
            except Exception:
                pass
            try:
                loc.hover(timeout=300)
            except Exception:
                pass
            try:
                loc.focus(timeout=200)
            except Exception:
                pass

            # disabled チェック
            try:
                if hasattr(loc, "is_disabled") and loc.is_disabled():
                    return False
            except Exception:
                pass

            # 1) 通常クリック
            try:
                loc.click(timeout=min(800, timeout), button="left")
                return True
            except Exception as e1:
                last_err = e1

            # 2) バウンディングボックスの中心クリック（微小ジッタ）
            box = None
            try:
                box = loc.bounding_box()
            except Exception:
                box = None
            if box:
                try:
                    x = box["x"] + box["width"] / 2 + random.uniform(-1.5, 1.5)
                    y = box["y"] + box["height"] / 2 + random.uniform(-1.5, 1.5)
                    try:
                        self.page.mouse.move(x, y, steps=6)
                    except Exception:
                        pass
                    self.page.mouse.click(x, y, button="left", delay=8)
                    return True
                except Exception as e2:
                    last_err = e2

            # 3) JS 直接 click()
            try:
                h = loc.element_handle()
            except Exception:
                h = None
            if h:
                try:
                    self.page.evaluate(
                        "(el)=>{ try{ el.scrollIntoView && el.scrollIntoView({block:'center', inline:'center'}); el.click(); }catch(_){} }",
                        h,
                    )
                    return True
                except Exception as e3:
                    last_err = e3

            # 4) キー操作（Enter → Space）
            try:
                loc.press("Enter", timeout=200)
                return True
            except Exception:
                pass
            try:
                loc.press(" ", timeout=200)
                return True
            except Exception as e4:
                last_err = e4

            # 5) 最後の手段: force=True
            try:
                loc.click(timeout=min(800, timeout), button="left", force=True)
                return True
            except Exception as e5:
                last_err = e5

            if DEBUG_FIND:
                try:
                    print(f"クリック失敗: {last_err}")
                except Exception:
                    pass
            return False
        except Exception:
            return False

    def _click_cta_for_asin(self, asin: str) -> bool:
        # 既にスクロール位置が下にあると、対象カードが「上」にあって探索できないことがある。
        # まずはトップへ戻してから下方向に探索する。
        try:
            self.page.evaluate("window.scrollTo(0,0)")
            time.sleep(0.08)
        except Exception:
            pass

        # 1) 対象カードまでスクロール
        if not self._scroll_to_card(asin, max_steps=72):
            return False
        card = self._find_card_locator(asin)
        try:
            card.wait_for(state="visible", timeout=2000)
        except Exception:
            pass

        # 2) 学習済み + 既知候補で CTA をクリック（成功すれば記憶更新）
        base_cta_selectors = [
            'button:has-text("詳細はこちら")',
            'span.a-button-text:has-text("詳細はこちら")',
            'a:has-text("詳細はこちら")',
            'button:has-text("詳細")',
            'span.a-button-text:has-text("詳細")',
            'a:has-text("詳細")',
            'button:has-text("商品のリクエスト")',
            'a:has-text("商品のリクエスト")',
            'button:has-text("申し込")',
            'a:has-text("申し込")',
        ]
        ok, _used = self._try_click_by_selectors(
            scope=card,
            category="cta",
            base_selectors=base_cta_selectors,
            timeout=800,
            success_check_fn=lambda: self._wait_for_modal(3.5),
        )
        if ok:
            return True

        # 3) 近傍探索（学習セレクタ順で試行）
        #    近傍は座標優先で当てるため、記憶は付与しない（誤爆リスクを避ける）
        sels = list(self._iter_selectors("cta", base_cta_selectors))
        best = None
        best_d = 1e18
        try:
            box_card = card.bounding_box()
        except Exception:
            box_card = None
        if box_card:
            cx = box_card["x"] + box_card["width"] / 2
            cy = box_card["y"] + box_card["height"] / 2
            for sel in sels:
                try:
                    for loc in self.page.locator(sel).all():
                        if not loc or not loc.is_visible():
                            continue
                        try:
                            box = loc.bounding_box()
                        except Exception:
                            box = None
                        if not box:
                            continue
                        px = box["x"] + box["width"] / 2
                        py = box["y"] + box["height"] / 2
                        d = (px - cx) * (px - cx) + (py - cy) * (py - cy)
                        if d < best_d:
                            best_d = d
                            best = loc
                except Exception:
                    continue
        if best and self._click_like_human(best, timeout=800) and self._wait_for_modal(3.5):
            return True

        return False

    # ---- モーダル前処理 ----
    def _size_rank(self, s: str) -> int:
        t = (s or "").lower().strip()
        t = re.sub(r"[^a-z0-9x\-＋\+ ]", "", t)
        table = {
            "xxs": 0,
            "xs": 1,
            "x-small": 1,
            "s": 2,
            "small": 2,
            "m": 3,
            "medium": 3,
            "l": 4,
            "large": 4,
            "xl": 5,
            "x-large": 5,
            "xlarge": 5,
            "xxl": 6,
            "2xl": 6,
            "xx-large": 6,
            "xxlarge": 6,
            "3xl": 7,
            "xxx-large": 7,
            "xxxl": 7,
        }
        m = re.search(r"(\d)\s*xl", t)
        if m:
            return 4 + int(m.group(1))
        if t in table:
            return table[t]
        for k, v in table.items():
            if k in t:
                return v
        return 999

    def _capacity_value(self, s: str) -> float:
        txt = (s or "").lower()
        m = re.search(r"(\d+(?:\.\d+)?)\s*(tb|gb|mb)", txt)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            return val * {"tb": 1e12, "gb": 1e9, "mb": 1e6}[unit]
        m = re.search(r"(\d+(?:\.\d+)?)\s*(mah|wh|w)", txt)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            return val * {"mah": 1.0, "wh": 1000.0, "w": 10.0}[unit]
        m = re.search(r"(\d+(?:\.\d+)?)\s*(l|ml)", txt)
        if m:
            val = float(m.group(1))
            unit = m.group(2)
            return val * (1000.0 if unit == "l" else 1.0)
        m = re.search(r"(\d+)\s*(個|枚|本|pack|セット)", txt)
        if m:
            return float(m.group(1))
        m = re.findall(r"\d+", txt)
        if m:
            return float(max(int(x) for x in m))
        return -1.0

    def _dialog_select_preferred(self, dialog):
        chose = False
        try:
            groups = [
                dialog.locator('xpath=//*[contains(., "Size") or contains(., "サイズ")]/following::*[self::ul or self::div or self::select][1]'),
                dialog.locator("#twister"),
            ]
            for g in groups:
                if not g or not g.count():
                    continue
                opts = g.locator("button, [role=\"option\"], li, a, option").all()
                ranked = []
                for el in opts:
                    try:
                        t = (el.inner_text(timeout=300) or "").strip()
                    except Exception:
                        t = ""
                    if not t:
                        continue
                    r = self._size_rank(t)
                    if r != 999:
                        ranked.append((r, el))
                if ranked:
                    ranked.sort(key=lambda x: x[0])
                    for _, el in ranked[:3]:
                        try:
                            el.click(timeout=700, button="left")
                            time.sleep(0.12)
                            chose = True
                            break
                        except Exception:
                            continue
                if chose:
                    break
        except Exception:
            pass
        if not chose:
            try:
                cands = dialog.locator("#twister, [role=\"listbox\"], .a-button-toggle-group, .a-dropdown-container").all()
                best = (-1.0, None)
                for g in cands:
                    opts = g.locator("button, [role=\"option\"], li, a, option").all()
                    for el in opts:
                        try:
                            t = (el.inner_text(timeout=300) or "").strip()
                        except Exception:
                            t = ""
                        if not t:
                            continue
                        score = self._capacity_value(t)
                        if score > best[0]:
                            best = (score, el)
                if best[1] is not None and best[0] >= 0:
                    try:
                        best[1].click(timeout=700, button="left")
                        time.sleep(0.12)
                    except Exception:
                        pass
            except Exception:
                pass
        for want in COLOR_PREF:
            try:
                loc = dialog.locator(f'button:has-text("{want}"), [role="option"]:has-text("{want}"), a:has-text("{want}")').first
                if loc and loc.is_visible():
                    loc.click(timeout=600, button="left")
                    time.sleep(0.08)
                    break
            except Exception:
                continue
        try:
            checks = dialog.locator('input[type="checkbox"]').all()
            for c in checks[:4]:
                try:
                    lbl = c.evaluate("e => (e.closest('label')?.innerText||'') + ' ' + (e.getAttribute('aria-label')||'')")
                    if re.search(r"(同意|確認|規約|了承|I agree|confirm)", lbl or "", re.I):
                        if not c.is_checked():
                            c.check(timeout=500, force=False)
                except Exception:
                    continue
        except Exception:
            pass
        return True

    def _follow_checkout_tab(self, timeout_s: float = 8.0) -> bool:
        """
        クリック後にチェックアウト/注文確認が新規タブ・ウィンドウで開かれた場合に追従して
        self.page を切り替える。True: 切替済み（または既に checkout URL）/ False: 見つからず。
        """
        patterns_url = r"/(buy|gp/buy|thankyou|checkout|gp/cart|cart|ap/signin|confirm|confirmation|payselect|payments|shipaddress|shipoption)"
        patterns_title = r"(注文|ご注文|レジ|チェックアウト|お支払い|配送|アドレス|確認|支払い方法|ありがとうございます|完了|確定|Thank you|Order placed)"

        # すでに現在タブがチェックアウト系なら何もしない
        try:
            url = self.page.url or ""
        except Exception:
            url = ""
        try:
            if re.search(patterns_url, url, re.I):
                return True
        except Exception:
            pass

        # 新規ページの生成/既存ページのURL変化をポーリングで捕捉
        deadline = time.time() + max(1.0, float(timeout_s))
        while time.time() < deadline:
            try:
                pages = list(self._ctx.pages or [])
            except Exception:
                pages = []
            for p in pages:
                try:
                    u = p.url or ""
                except Exception:
                    u = ""

                good = False
                try:
                    if re.search(patterns_url, u, re.I):
                        good = True
                except Exception:
                    good = False

                if not good:
                    try:
                        t = p.title() or ""
                        if re.search(patterns_title, t):
                            good = True
                        else:
                            good = False
                    except Exception:
                        good = False

                if good:
                    # チェックアウト系タブにスイッチ
                    self.page = p
                    try:
                        if ORDER_FRONT and self.headed:
                            p.bring_to_front()
                    except Exception:
                        pass
                    try:
                        p.wait_for_load_state("domcontentloaded", timeout=15000)
                    except Exception:
                        pass
                    try:
                        p.wait_for_load_state("networkidle", timeout=5000)
                    except Exception:
                        pass
                    return True
            time.sleep(0.2)

        return False

    # ---- 注文（モーダル→リクエスト→確定） ----
    def _dismiss_overlays(self, tries: int = 2):
        """
        クリックを妨げるポップオーバー/モーダル/ツールチップを閉じる。
        失敗してもスルー。
        """
        patterns = [
            ".a-popover .a-icon-close",
            ".a-popover .a-button-close",
            ".a-modal-scroller .a-icon-close",
            ".a-sheet .a-icon-close",
            'button[aria-label="閉じる"]',
            'button:has-text("閉じる")',
            'button:has-text("後で")',
            'a:has-text("後で")',
            'button:has-text("キャンセル")',
            'a:has-text("キャンセル")',
            'button:has-text("No thanks")',
            'a:has-text("No thanks")',
            "#nav-signin-tooltip .a-button-close",
            "#nav-signin-tooltip .a-declarative .a-icon-close",
        ]
        for _ in range(max(1, int(tries))):
            closed = False
            for sel in patterns:
                try:
                    loc = self.page.locator(sel).first
                except Exception:
                    loc = None
                if loc and getattr(loc, "is_visible", lambda: False)():
                    try:
                        self._click_like_human(loc, timeout=400)
                        closed = True
                    except Exception:
                        pass
            if not closed:
                break

    def _advance_checkout_flow(self, max_steps: int = 6, step_timeout: int = 4000) -> bool:
        """
        チェックアウト/申し込みフローで中間の「続行/次へ/確認/送信」等のボタンを自動で辿る。
        Place/注文確定ボタンがあればそれも押す。
        True: 何らかの前進/確定を行った可能性が高い、False: 見つからず。
        """
        cont_base = [
            # 日本語 Continue 系
            'button:has-text("続行")',
            'a:has-text("続行")',
            'span.a-button-text:has-text("続行")',
            'button:has-text("次へ")',
            'a:has-text("次へ")',
            'span.a-button-text:has-text("次へ")',
            'button:has-text("次に進む")',
            'button:has-text("次に進む")',
            'button:has-text("確認")',
            'input[type="submit"][value*="確認"]',
            'button:has-text("送信")',
            'input[type="submit"][value*="送信"]',
            # 英語 Continue 系
            'button:has-text("Continue")',
            'a:has-text("Continue")',
            'span.a-button-text:has-text("Continue")',
            'button:has-text("Next")',
            'a:has-text("Next")',
            'span.a-button-text:has-text("Next")',
            'button:has-text("Submit")',
            'input[type="submit"][value*="Submit"]',
        ]
        place_base = [
            # 既存の Place/確定系もここで再利用
            'input[name="placeYourOrder1"]',
            'input[type="submit"][name*="placeYourOrder"]',
            '#submitOrderButtonId input[name="placeYourOrder1"]',
            "#submitOrderButtonId .a-button-input",
            "#submitOrderButtonId",
            "#submitOrderButtonId-announce",
            'span.a-button-text:has-text("注文を確定する")',
            'button:has-text("注文を確定する")',
            'input[type="submit"][value*="注文を確定"]',
            'input[aria-labelledby*="placeYourOrder"]',
            'button:has-text("Place your order")',
            'span.a-button-text:has-text("Place your order")',
        ]

        progressed = False
        for _step in range(max(1, int(max_steps))):
            # まずは「確定」ボタンがないか
            clicked, used_place = self._try_click_by_selectors(
                scope=self.page, category="place", base_selectors=place_base, timeout=step_timeout, success_check_fn=lambda: True
            )
            if clicked:
                log_stage("注文確定ボタン", f"使用セレクタ: {used_place or '(自動検出)'}")
                progressed = True
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=step_timeout)
                except Exception:
                    pass
                try:
                    self.page.wait_for_load_state("networkidle", timeout=min(3000, step_timeout))
                except Exception:
                    pass
                ok, reason = self._wait_order_placement(timeout_s=12.0)
                if ok:
                    log_ok(f"自動注文: 成功（{reason}）")
                    return True

            # 次に「続行/次へ/送信」系で前進
            clicked, used_cont = self._try_click_by_selectors(
                scope=self.page, category="advance", base_selectors=cont_base, timeout=step_timeout, success_check_fn=lambda: True
            )
            if clicked:
                progressed = True
                log_stage("チェックアウト前進", f"使用セレクタ: {used_cont or '(自動検出)'}")
                # 邪魔ポップ閉じ & ロード待ち
                try:
                    self._dismiss_overlays()
                except Exception:
                    pass
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=step_timeout)
                except Exception:
                    pass
                try:
                    self.page.wait_for_load_state("networkidle", timeout=min(3000, step_timeout))
                except Exception:
                    pass
                ok, reason = self._wait_order_placement(timeout_s=6.0)
                if ok:
                    log_ok(f"自動注文: 成功（{reason}）")
                    return True
                continue

            self._dismiss_overlays()
            time.sleep(0.2)

            clicked2, used_place2 = self._try_click_by_selectors(
                scope=self.page, category="place", base_selectors=place_base, timeout=1200, success_check_fn=lambda: True
            )
            if clicked2:
                log_stage("注文確定ボタン", f"使用セレクタ: {used_place2 or '(自動検出)'}")
                progressed = True
                try:
                    self.page.wait_for_load_state("domcontentloaded", timeout=step_timeout)
                except Exception:
                    pass
                ok, reason = self._wait_order_placement(timeout_s=10.0)
                if ok:
                    log_ok(f"自動注文: 成功（{reason}）")
                    return True

            if not progressed:
                break

        return progressed

    def _order_via_modal(self, asin: str) -> bool:
        """
        注文フロー: CTA押下→モーダル検出→確認送信→（必要に応じて）チェックアウトタブ追従→注文確定ボタン→サンクス判定
        各ステージで日本語ログを詳細に出力し、どのセレクタで成功したかも記録。
        """
        self._ordering = True
        self._suspend_refresh()
        self._block_vine_reload_begin()
        try:
            if ORDER_FRONT and self.headed:
                try:
                    self.page.bring_to_front()
                except Exception:
                    pass
            # 1) dismiss overlays at start
            try:
                self._dismiss_overlays()
            except Exception:
                pass

            # --- CTA 押下 ---
            if not self._click_cta_for_asin(asin):
                log_err("CTAを押せませんでした（カードのボタンが見つからない/押下不可）")
                return False
            log_stage("CTA押下", f"ASIN: {asin}")

            # --- モーダル検出 ---
            dialog = self._get_dialog(timeout_ms=9000)
            if not dialog:
                log_err("モーダルが検出できませんでした")
                return False
            log_stage("モーダル検出")

            # バリアント/チェック類の事前選択
            self._dialog_select_preferred(dialog)
            # 2) dismiss overlays after dialog selection
            try:
                self._dismiss_overlays()
            except Exception:
                pass

            # --- 「商品のリクエスト/申し込む」送信 ---
            def _iter_modal_targets(dlg):
                yield ("main", dlg, None)
                try:
                    iframes = dlg.locator("iframe").all()
                except Exception:
                    iframes = []
                for ifr in iframes:
                    try:
                        eh = ifr.element_handle()
                        fr = eh.content_frame() if eh else None
                        if fr:
                            yield ("frame", None, fr)
                    except Exception:
                        continue
                for fr in self.page.frames:
                    try:
                        yield ("frame-any", None, fr)
                    except Exception:
                        continue

            confirm_base = [
                # モーダルの黄色ボタン（id/announce・念のため複数指定）
                "#product-details-modal-request-btn",
                "button#product-details-modal-request-btn",
                "#product-details-modal-request-btn-announce",
                "span#product-details-modal-request-btn-announce",
                ".a-button:has(#product-details-modal-request-btn-announce)",
                "button:has(#product-details-modal-request-btn-announce)",
                # 日本語（Vine系）
                'button:has-text("商品のリクエスト")',
                'button:has-text("商品のリクエストを送信")',
                'button[aria-label*="商品のリクエスト"]',
                'button:has-text("申し込む")',
                'button:has-text("申し込")',
                'input[type="submit"][value*="リクエスト"]',
                'input[type="submit"][value*="申し込"]',
                'span.a-button-text:has-text("商品のリクエスト")',
                'span.a-button-text:has-text("申し込")',
                '.a-button:has(span:has-text("商品のリクエスト"))',
                '.a-button:has(span:has-text("申し込"))',
                'a:has-text("商品のリクエスト")',
                'a:has-text("申し込")',
                'div[role="button"]:has-text("商品のリクエスト")',
                'div[role="button"]:has-text("申し込")',
                'span:has-text("商品のリクエスト")',
                'span:has-text("申し込")',
                # 追加の日本語バリエーション
                'button:has-text("今すぐ申し込む")',
                'button:has-text("同意して申し込む")',
                'span.a-button-text:has-text("今すぐ申し込む")',
                'span.a-button-text:has-text("同意して申し込む")',
                # 英語バリエーション
                'button:has-text("Request this item")',
                'span.a-button-text:has-text("Request this item")',
                'button:has-text("Submit request")',
                'span.a-button-text:has-text("Submit request")',
                'button:has-text("Request")',
                'input[type="submit"][value*="Request"]',
                'a:has-text("Request this item")',
            ]
            deadline = time.time() + max(2.0, float(ORDER_MAX))
            placed = False
            used_confirm = ""
            while time.time() < deadline and not placed:
                for _, dlg, fr in _iter_modal_targets(dialog):
                    scope = fr if fr else dlg
                    ok, used = self._try_click_by_selectors(
                        scope=scope, category="confirm", base_selectors=confirm_base, timeout=800, success_check_fn=lambda: True
                    )
                    if ok:
                        used_confirm = used or ""
                        placed = True
                        break

                # --- fallback: broader query / role=button, anchors, spans ---
                if not placed:
                    for _, dlg, fr in _iter_modal_targets(dialog):
                        scope = fr if fr else (dlg or self.page)
                        broader = [
                            'div[role="button"]:has-text("商品のリクエスト")',
                            'div[role="button"]:has-text("申し込")',
                            '.a-button:has(span:has-text("商品のリクエスト"))',
                            '.a-button:has(span:has-text("申し込"))',
                            'a:has-text("商品のリクエスト")',
                            'a:has-text("申し込")',
                            'span:has-text("商品のリクエスト")',
                            'span:has-text("申し込")',
                        ]
                        ok2, used2 = self._try_click_by_selectors(
                            scope=scope, category="confirm_fallback", base_selectors=broader, timeout=900, success_check_fn=lambda: True
                        )
                        if ok2:
                            used_confirm = used2 or "(fallback)"
                            placed = True
                            break

                if not placed:
                    time.sleep(ORDER_RETRY)

            if not placed:
                log_err("確認ボタンが押せませんでした（商品のリクエスト/申し込み）")
                try:
                    self._debug_dump_if_empty(tag=f"order_confirm_fail_{asin}")
                except Exception:
                    pass
                return False
            log_stage("確認送信", f"使用セレクタ: {used_confirm or '(自動検出)'}")

            # まずは Vine 側で完了するケース（モーダル閉→カードが Requested になる等）を優先して判定
            ok0, reason0 = self._wait_order_placement(timeout_s=8.0, asin=asin)
            if ok0:
                log_ok(f"自動注文: 成功（{reason0}）")
                return True
            if reason0 in ("signin-required", "auth-challenge", "unavailable-or-limit"):
                log_err(f"自動注文: 失敗（{reason0}）")
                try:
                    self._debug_dump_if_empty(tag=f"order_fail_{reason0}_{asin}")
                except Exception:
                    pass
                return False

            # チェックアウトに飛ぶケースだけ、以降の自動前進を実施（誤クリック防止）
            try:
                followed = self._follow_checkout_tab(timeout_s=6.0)
                if followed or self._is_checkout_like():
                    log_stage("チェックアウト画面へ移動")
            except Exception:
                pass
            if not self._is_checkout_like():
                log_err("申込後の状態変化を確認できませんでした（Vine完了/チェックアウト遷移のどちらも検出できず）")
                try:
                    self._debug_dump_if_empty(tag=f"order_no_transition_{asin}")
                except Exception:
                    pass
                return False

            # 中間ステップの自動前進（配送/支払い確認など）
            try:
                self._dismiss_overlays()
                self._advance_checkout_flow(max_steps=6, step_timeout=3500)
            except Exception:
                pass

            # --- 「注文を確定する」押下（あれば） ---
            place_base = [
                'input[name="placeYourOrder1"]',
                'input[type="submit"][name*="placeYourOrder"]',
                '#submitOrderButtonId input[name="placeYourOrder1"]',
                "#submitOrderButtonId .a-button-input",
                "#submitOrderButtonId",
                "#submitOrderButtonId-announce",
                'span.a-button-text:has-text("注文を確定する")',
                'button:has-text("注文を確定する")',
                'input[type="submit"][value*="注文を確定"]',
                'input[aria-labelledby*="placeYourOrder"]',
                # English variants
                'button:has-text("Place your order")',
                'span.a-button-text:has-text("Place your order")',
            ]
            clicked, used_place = self._try_click_by_selectors(
                scope=self.page, category="place", base_selectors=place_base, timeout=5000, success_check_fn=lambda: True
            )
            if clicked:
                log_stage("注文確定ボタン", f"使用セレクタ: {used_place or '(自動検出)'}")
            else:
                log_warn("注文確定ボタンが見つからない/押下不可（Vineのリクエスト完了型の可能性）")

            # --- サンクス判定 ---
            ok, reason = self._wait_order_placement(timeout_s=max(12.0, float(ORDER_MAX)), asin=asin)
            if ok:
                log_ok(f"自動注文: 成功（{reason}）")
            else:
                log_err(f"自動注文: 失敗（{reason}）")
                try:
                    self._debug_dump_if_empty(tag=f"order_fail_{reason}_{asin}")
                except Exception:
                    pass

            if DEBUG_FIND:
                try:
                    print(f"注文確定チェック: clicked={clicked} / result={ok} / reason={reason} / url={self.page.url}")
                except Exception:
                    pass
            return ok

        except Exception:
            log_err("注文フロー中に例外が発生しました")
            return False
        finally:
            self._block_vine_reload_end()
            self._ordering = False
            self._resume_refresh()

    # ---- デバッグ保存 ----
    def _debug_dump_if_empty(self, tag: str = ""):
        if not DEBUG_FIND:
            return
        try:
            dbg = Path(SHOTS_DIR) / "_debug"
            ensure_dir(str(dbg))
            ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            self.page.screenshot(path=str(dbg / f"empty_{tag}_{ts}.png"), full_page=True)
            (dbg / f"empty_{tag}_{ts}.html").write_text(self.page.content(), encoding="utf-8")
            log(f"デバッグ保存: {dbg}/empty_{tag}_{ts}.*")
        except Exception:
            pass

    def _is_captured(self, asin: str) -> bool:
        rec = self.db.get(asin)
        if not rec:
            return False
        sp = rec.get("shot_path") or ""
        return bool(sp and Path(sp).exists())

    # ---- 変種候補抽出（軽量） ----
    def _collect_variant_labels_quick(self, p, limit: int) -> list:
        labels = []
        try:
            cands = p.locator("#twister button, #twister [role=\"option\"], #twister li, #twister .a-button").all()
            for el in cands[: max(40, limit * 6)]:
                try:
                    t = (el.inner_text(timeout=250) or "").strip()
                except Exception:
                    t = ""
                if t and not re.search(r"(在庫切れ|選択|カラー|色|サイズを選択)", t):
                    labels.append(t[:40])
        except Exception:
            pass
        return uniq_keep_order(labels)[: max(1, limit)]

    def _collect_variant_prices_from_dp(self, p, labels: list, max_try: int) -> list:
        out = []
        tried = 0
        labels = uniq_keep_order([(lab or "").strip() for lab in (labels or []) if lab])
        for lab in labels:
            if tried >= max_try:
                break
            tried += 1
            ok = False
            selectors = [
                f'#twister button:has-text("{lab}")',
                f'#twister [role="option"]:has-text("{lab}")',
                f'#twister li:has-text("{lab}")',
                f'#twister .a-button:has-text("{lab}")',
                f'button:has-text("{lab}")',
                f'[role="option"]:has-text("{lab}")',
            ]
            for sel in selectors:
                try:
                    loc = p.locator(sel).first
                    if loc and loc.is_visible():
                        loc.click(timeout=700, button="left")
                        time.sleep(0.20)
                        ok = True
                        break
                except Exception:
                    continue
            price = self._get_price_text(p) if ok else ""
            if price:
                out.append({"label": lab, "price": price})
        uniq = []
        seen = set()
        for rec in out:
            key = (rec.get("label", ""), rec.get("price", ""))
            if key in seen:
                continue
            seen.add(key)
            uniq.append(rec)
        return uniq

    # ---- ブランド/サイズ判定・処理 ----
    def _looks_large(self, title: str, page_text: str) -> bool:
        t = (title or "") + "\n" + (page_text or "")
        t = t.lower()
        if any(
            k in t
            for k in [
                "机",
                "デスク",
                "テーブル",
                "ダイニング",
                "学習机",
                "ワークデスク",
                "椅子",
                "チェア",
                "ソファ",
                "ベッド",
                "本棚",
                "ラック",
                "収納棚",
                "テレビ台",
                "ローテーブル",
                "こたつ",
                "コタツ",
                "cabinet",
                "desk",
                "table",
                "sofa",
                "bed",
                "bookshelf",
                "shelf",
                "rack",
            ]
        ):
            return True
        dims = []
        for m in re.findall(r"(\d{2,3})\s*(?:cm|ｃｍ|センチ)", t):
            try:
                dims.append(int(m))
            except Exception:
                pass
        for m in re.findall(r"(\d{2,3})\s*[x×＊\*]\s*(\d{2,3})\s*[x×＊\*]\s*(\d{2,3})\s*(?:cm|ｃｍ|センチ)", t):
            try:
                dims.extend(int(x) for x in m)
            except Exception:
                pass
        if dims:
            big_edges = sum(1 for v in dims if v >= 60)
            if any(v >= 70 for v in dims) or big_edges >= 2:
                return True
        return False

    def _load_brands(self):
        """常時ブランドの読み込み。
        1) 外部ファイル BRANDS_FILE を最優先（存在すれば採用）
        2) なければ DB(__brands) もしくは DEFAULT_BRANDS_ALWAYS を使用
        3) 採用集合を BRANDS_FILE に書き出して初期化
        """
        # 1) ファイルから読み込み
        try:
            arr = _read_brands_file(BRANDS_FILE)
            if isinstance(arr, list):
                self.brand_always = set(arr)
                return
        except Exception:
            pass

        # 2) 既存DB/既定にフォールバック
        try:
            meta = self.db.get("__brands")
            if isinstance(meta, list):
                self.brand_always = set(str(x).strip() for x in meta if x)
            else:
                self.brand_always = set(DEFAULT_BRANDS_ALWAYS)
        except Exception:
            self.brand_always = set(DEFAULT_BRANDS_ALWAYS)

        # 3) 初期ファイルを作成（ベストエフォート）
        try:
            _write_brands_file(BRANDS_FILE, self.brand_always)
        except Exception:
            pass

    def _save_brands(self):
        """常時ブランドの保存。DB と外部ファイルの両方に書き出す（冗長化）。"""
        # DB へ（従来互換）
        try:
            self.db["__brands"] = sorted(self.brand_always)
            save_db(self.db)
        except Exception:
            pass
        # 外部ファイルへ
        try:
            _write_brands_file(BRANDS_FILE, self.brand_always)
        except Exception:
            pass

    def _brand_forced(self, title: str, byline: str = "") -> bool:
        # 消耗品はブランド優先から除外
        text = f"{title or ''} {byline or ''}".lower()
        for w in NG_CONSUMABLE_KEYWORDS:
            try:
                if str(w).lower() in text:
                    try:
                        if DEBUG_FIND:
                            print(f"消耗品除外: {w}")
                    except Exception:
                        pass
                    return False
            except Exception:
                continue
        hay = f"{title}\n{byline}".lower()
        for b in self.brand_always:
            if b and str(b).lower() in hay:
                return True
        return False

    def _handle_one(self, asin: str, dp_url: str, allow_reorder: bool):
        seen_before = asin in self.db
        title, price_text, shot, page_text, variant_prices = self._scrape_dp(dp_url or f"https://www.amazon.co.jp/dp/{asin}")
        price_int = price_to_int(price_text)
        now_iso = datetime.datetime.now().isoformat(timespec="seconds")
        rec = self.db.get(asin) or {}
        rec.update(
            {
                "title": title,
                "price": price_text,
                "last_seen": now_iso,
                "shot_path": shot,
                "url": dp_url or f"https://www.amazon.co.jp/dp/{asin}",
                "variants": variant_prices,
            }
        )
        rec.setdefault("first_seen", now_iso)
        self.db[asin] = rec
        save_db(self.db)
        self._last_captured_asin = asin
        self._last_captured_url = rec["url"]

        print(color_log_line(now_iso, title, price_text or "価格不明", self.order_threshold, self.brand_always))
        if shot:
            print(f"スクショ: {shot}")
        if variant_prices:
            for vr in variant_prices:
                print(f"  ↳ バリアント: {vr.get('label', '')} ｜ 価格: {vr.get('price', '')}")

        # --- 通知: 新着（初回のみ） ---
        try:
            if (not seen_before) and (not rec.get("notified_new")):
                # --- Sheetsログ: 新着（初回のみ） ---
                try:
                    if not rec.get("sheet_logged"):
                        title_s = (title or "").strip()
                        title_l = title_s.lower()
                        priority_brands = list(getattr(self, "brand_always", []) or [])
                        hits = [str(b) for b in priority_brands if b and str(b).lower() in title_l]
                        hit_brand = sorted(hits, key=len, reverse=True)[0] if hits else ""
                        is_priority = bool(hits)
                        res = gas_append_row(
                            {
                                "title": title_s,
                                "price": (price_text or "").strip(),
                                "asin": asin,
                                "queue_url": URL,
                                "brand": hit_brand,
                                "priority": "⚡" if is_priority else "",
                            }
                        )
                        if isinstance(res, dict) and res.get("ok") is True:
                            rec["sheet_logged"] = True
                            if "appended" in res:
                                rec["sheet_appended"] = bool(res.get("appended"))
                            else:
                                # 旧エンドポイント互換: ok=true のみ返る場合がある
                                rec["sheet_appended"] = True
                            rec["sheet_skipped"] = str(res.get("skipped") or "")
                            rec["sheet_last"] = datetime.datetime.now().isoformat(timespec="seconds")
                            save_db(self.db)
                            if rec["sheet_appended"]:
                                log_ok("Sheetsログ: 追記しました")
                            elif rec.get("sheet_skipped") == "duplicate":
                                log_info("Sheetsログ: 既に同日重複（スキップ）")
                        else:
                            if isinstance(res, dict) and str(res.get("error") or "") in ("unauthorized", "forbidden(secret)"):
                                log_warn("Sheetsログ: SECRET不一致（unauthorized）")
                            elif not GAS_DISABLE and GAS_WEBAPP_URL:
                                log_warn("Sheetsログ: 失敗（WebアプリURL/アクセス権/SECRETを確認）")
                except Exception:
                    pass

                msg = _fmt_tg_item_event(
                    "新着",
                    asin=asin,
                    title=title,
                    price_text=price_text or "価格不明",
                    dp_url=rec.get("url", dp_url or f"https://www.amazon.co.jp/dp/{asin}"),
                    vine_url=URL,
                )
                # 1行目だけを統一フォーマットに変更（以降の行は変更しない）
                lines = (msg or "").splitlines()
                if lines:
                    p = (price_text or "").strip()
                    title_s = (title or "").strip()
                    title_l = title_s.lower()
                    priority_brands = list(getattr(self, "brand_always", []) or [])
                    is_priority = any(str(b).lower() in title_l for b in priority_brands if b)
                    prefix = "⚡️【新着】" if is_priority else "【新着】"
                    max_len = 34 if is_priority else 38
                    lines[0] = f"{prefix}{p} / {title_s[:max_len]}"
                    msg = "\n".join(lines)
                if tg_send(msg):
                    rec["notified_new"] = True
                    save_db(self.db)
        except Exception:
            pass

        # --- 通知: 高額検知（1回だけ） ---
        try:
            if price_int is not None and price_int >= self.order_threshold and not rec.get("notified_high"):
                if notify_high_price(
                    asin=asin,
                    title=title,
                    price_text=price_text or "",
                    dp_url=rec["url"],
                    vine_url=URL,
                ):
                    rec["notified_high"] = True
                    save_db(self.db)
        except Exception:
            pass

        # --- 自動注文 ---
        if getattr(self, "auto_order", bool(AUTO_ORDER)):
            if self._brand_forced(title=title, byline=""):
                if allow_reorder or not rec.get("auto_ordered"):
                    ok = self._order_via_modal(asin)
                    rec["auto_ordered"] = bool(ok)
                    save_db(self.db)
                    print("自動注文（ブランド）: 成功" if ok else "自動注文（ブランド）: 失敗")
            else:
                if price_int is not None and price_int >= self.order_threshold:
                    if self._looks_large(title, page_text):
                        print("自動注文: 大型品推定→スキップ")
                    else:
                        if allow_reorder or not rec.get("auto_ordered"):
                            ok = self._order_via_modal(asin)
                            rec["auto_ordered"] = bool(ok)
                            save_db(self.db)
                            print("自動注文: 成功" if ok else "自動注文: 失敗")

        # --- 通知: 自動注文成功（1回だけ） ---
        try:
            if rec.get("auto_ordered") and not rec.get("notified_order"):
                if notify_order_success(
                    asin=asin,
                    title=title,
                    price_text=price_text or "",
                    dp_url=rec["url"],
                    vine_url=URL,
                    reason="auto",
                ):
                    rec["notified_order"] = True
                    save_db(self.db)
        except Exception:
            pass

        if DP_OPEN_MODE != "same":
            try:
                self.page.bring_to_front() if ORDER_FRONT and self.headed else None
            except Exception:
                pass
            try:
                self._goto_vine(deep=False)
            except Exception:
                pass

        return shot

    def order_last_captured(self):
        asin = getattr(self, "_last_captured_asin", "") or ""
        if not asin:
            print("直前の商品がありません。")
            return
        print(f"直前の商品を注文します: {asin}")
        ok = False
        try:
            ok = self._order_via_modal(asin)
        except Exception:
            ok = False
        rec = self.db.get(asin) or {}
        rec["manual_ordered"] = bool(ok)
        rec["last_manual_order"] = datetime.datetime.now().isoformat(timespec="seconds")
        self.db[asin] = rec
        save_db(self.db)
        try:
            self._goto_vine(deep=False)
        except Exception:
            pass
        print("手動注文: 成功" if ok else "手動注文: 失敗")

    # ---- 入力・UI ----
    def _prompt(self, message: str) -> str:
        line = ""
        try:
            with open("/dev/tty", "r+", encoding="utf-8", errors="ignore") as ttyio:
                try:
                    ttyio.write(message)
                    ttyio.flush()
                except Exception:
                    pass
                line = ttyio.readline()
        except Exception:
            try:
                if sys.stdin and sys.stdin.isatty():
                    line = input(message)
                else:
                    print("この端末では直接入力できません。")
                    line = ""
            except Exception:
                line = ""
        s = unicodedata.normalize("NFKC", (line or "")).strip().replace("、", ",")
        try:
            while True:
                self.q.get_nowait()
        except Exception:
            pass
        return s

    def prompt_threshold(self):
        s = self._prompt("新しいしきい値（円）: ").strip().replace(",", "")
        if s.isdigit():
            self.order_threshold = int(s)
            print(f"しきい値を {self.order_threshold} 円に設定しました。")
        else:
            print("数値ではありません。変更をキャンセルしました。")

    def prompt_brand_edit(self):
        cur = ", ".join(sorted(self.brand_always)) or "(なし)"
        print(f"現在の常時ブランド: {cur}")
        s = self._prompt("追加/削除（例: +CIO,+Anker,-Example）: ").strip()
        for tok in [x.strip() for x in s.split(",") if x.strip()]:
            if tok.startswith("+"):
                self.brand_always.add(tok[1:].strip())
            elif tok.startswith("-"):
                self.brand_always.discard(tok[1:].strip())
            else:
                self.brand_always.add(tok)
        self._save_brands()
        print("更新しました。")

    def print_banner(self):
        self.banner()

    # ---- スキャン ----
    def _scan_once(self, deep: bool = False, allow_reorder: bool = False, include_existing: bool = False, label: str = "") -> dict:
        """Vineページをスキャンして処理。結果要約を dict で返す。"""
        summary = {
            "label": label or ("deep" if deep else "quick"),
            "total_detected": 0,
            "new_processed": 0,
            "existing_processed": 0,
            "shots": 0,
            "skipped_existing": 0,
            "errors": 0,
            "status": "ok",
        }
        try:
            if deep:
                self._goto_vine(deep=True)
        except Exception:
            pass

        try:
            items = self._collect_all_frames()
        except Exception:
            items = None

        if not isinstance(items, list):
            summary["status"] = "detect-error"
            summary["errors"] += 1
            return summary

        summary["total_detected"] = len(items)
        if len(items) == 0:
            summary["status"] = "ok-no-new"
            self._debug_dump_if_empty(tag=(label or "empty"))
            return summary

        for it in items:
            try:
                asin = (it or {}).get("asin", "")
                dp = (it or {}).get("dp", "")
                if not asin:
                    continue
                is_existing = self._is_captured(asin)
                if (ONLY_NEW and not include_existing) and is_existing:
                    summary["skipped_existing"] += 1
                    continue
                shot_path = self._handle_one(asin, dp, allow_reorder=allow_reorder)
                if is_existing:
                    summary["existing_processed"] += 1
                else:
                    summary["new_processed"] += 1
                if shot_path:
                    summary["shots"] += 1
            except Exception:
                summary["errors"] += 1
                continue

        return summary

    # ---- メインループ ----
    def loop(self):
        self._open_browser()
        try:
            ensure_dir(SHOTS_DIR)
            boot = os.path.join(SHOTS_DIR, f"_boot_{datetime.datetime.now():%Y%m%d_%H%M%S}.png")
            self.page.goto(URL, wait_until="domcontentloaded", timeout=60000)
            self.page.screenshot(path=boot, full_page=True)
            print(f"起動確認スクショ: {boot}")
        except Exception:
            pass

        self.key.start()
        self.print_banner()

        if not NO_BOOT_DEEP:
            try:
                if BOOT_CATCHUP:
                    res = self._scan_once(deep=True, allow_reorder=self.allow_dup_order, include_existing=False, label="boot-deep")
                    log_scan_summary_jp(
                        "起動スキャン",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
                    if res.get("total_detected", 0) == 0 and res.get("new_processed", 0) == 0:
                        self._goto_vine(deep=False)
                        res2 = self._scan_once(
                            deep=False, allow_reorder=self.allow_dup_order, include_existing=False, label="boot-quick"
                        )
                        log_scan_summary_jp(
                            "起動スキャン（追）",
                            res2.get("total_detected", 0),
                            res2.get("new_processed", 0),
                            res2.get("existing_processed", 0),
                            res2.get("shots", 0),
                            res2.get("skipped_existing", 0),
                            res2.get("errors", 0),
                        )
                else:
                    res = self._scan_once(deep=False, allow_reorder=False, include_existing=False, label="boot-check")
                    log_scan_summary_jp(
                        "起動スキャン",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
            except Exception:
                log_err("起動スキャン: 検知エラー")
        if NO_BOOT_DEEP:
            print("起動スキャン: スキップ（NO_BOOT_DEEP=True）")

        last = time.time() - self.interval
        while self.running:
            while True:
                try:
                    ch = self.q.get_nowait()
                except queue.Empty:
                    break
                if ch == "q":
                    self.running = False
                    break
                elif ch == "p":
                    self.paused = not self.paused
                    print("⏸ 一時停止" if self.paused else "▶ 再開")
                elif ch == "r":
                    self._force = True
                elif ch == "w":
                    self._restart_toggle_head()
                    self.print_banner()
                elif ch == "s":
                    self.shot_enabled = not self.shot_enabled
                    print(f"スクショ: {'ON' if self.shot_enabled else 'OFF'}")
                elif ch == "a":
                    self.auto_order = not self.auto_order
                    print(f"自動注文: {'ON' if self.auto_order else 'OFF'}")
                elif ch == "f":
                    self._toggle_tab_foreground()
                elif ch == "o":
                    self.order_last_captured()
                elif ch == "u":
                    ok = self.send_weekly_new_summary()
                    print("週サマリ送信: 成功" if ok else "週サマリ送信: 失敗（Telegram設定/通信を確認）")
                elif ch == "[":
                    self.interval = max(1, int(self.interval) - 1)
                    print(f"通常間隔: {self.interval}s")
                elif ch == "]":
                    self.interval = int(self.interval) + 1
                    print(f"通常間隔: {self.interval}s")
                elif ch == "{":
                    self.fast_interval = max(1, int(self.fast_interval) - 10)
                    print(f"高速間隔: {self.fast_interval}s")
                elif ch == "}":
                    self.fast_interval = int(self.fast_interval) + 10
                    print(f"高速間隔: {self.fast_interval}s")
                elif ch == "g":
                    res = self._scan_once(
                        deep=False, include_existing=False, allow_reorder=self.allow_dup_order, label="manual-visible"
                    )
                    log_scan_summary_jp(
                        "可視スキャン",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
                elif ch == "G":
                    res = self._scan_once(
                        deep=True, include_existing=False, allow_reorder=self.allow_dup_order, label="manual-deep"
                    )
                    log_scan_summary_jp(
                        "全件深スキャン",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
                elif ch == "e":
                    res = self._scan_once(
                        deep=False, include_existing=True, allow_reorder=self.allow_dup_order, label="manual-visible-all"
                    )
                    log_scan_summary_jp(
                        "可視スキャン（既知含む）",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
                elif ch == "E":
                    res = self._scan_once(
                        deep=True, include_existing=True, allow_reorder=self.allow_dup_order, label="manual-deep-all"
                    )
                    log_scan_summary_jp(
                        "全件深スキャン（既知含む）",
                        res.get("total_detected", 0),
                        res.get("new_processed", 0),
                        res.get("existing_processed", 0),
                        res.get("shots", 0),
                        res.get("skipped_existing", 0),
                        res.get("errors", 0),
                    )
                    for asin, rec in self.db.items():
                        if asin.startswith("__"):
                            continue
                        line = color_log_line(
                            rec.get("first_seen", rec.get("last_seen", "")),
                            rec.get("title", ""),
                            rec.get("price", ""),
                            self.order_threshold,
                            self.brand_always,
                        )
                        print(line)
                elif ch == "L":
                    for asin, rec in sorted(self.db.items(), key=lambda x: x[1].get("last_seen", "")):
                        if asin.startswith("__"):
                            continue
                        line = color_log_line(
                            rec.get("last_seen", ""), rec.get("title", ""), rec.get("price", ""), self.order_threshold, self.brand_always
                        )
                        print(line)
                elif ch == "l":
                    for asin, rec in self.db.items():
                        if asin.startswith("__"):
                            continue
                        line = color_log_line(
                            rec.get("last_seen", ""), rec.get("title", ""), rec.get("price", ""), self.order_threshold, self.brand_always
                        )
                        print(line)
                elif ch == "b":
                    self.prompt_brand_edit()
                elif ch == "B":
                    print(", ".join(sorted(self.brand_always)) or "(なし)")
                elif ch == "t":
                    self.prompt_threshold()
                elif ch == "T":
                    try:
                        delta = int(self._prompt("Δしきい値（±数値）: ").strip())
                        self.order_threshold = max(0, self.order_threshold + delta)
                        print(f"しきい値: {self.order_threshold} 円")
                    except Exception:
                        print("キャンセル")
                elif ch in ("h", "H", "?"):
                    self.print_banner()

            if not self.running:
                break

            # --- シグナルでのスキャン要求（SIGUSR1/2） ---
            if self._sig_quick:
                self._sig_quick = False
                try:
                    self._safe_reload_vine()
                except Exception:
                    pass
                res = self._scan_once(deep=False, include_existing=False, allow_reorder=self.allow_dup_order, label="sigusr1")
                log_scan_summary_jp(
                    "SIGUSR1",
                    res.get("total_detected", 0),
                    res.get("new_processed", 0),
                    res.get("existing_processed", 0),
                    res.get("shots", 0),
                    res.get("skipped_existing", 0),
                    res.get("errors", 0),
                )

            if self._sig_deep:
                self._sig_deep = False
                res = self._scan_once(deep=True, include_existing=False, allow_reorder=self.allow_dup_order, label="sigusr2")
                log_scan_summary_jp(
                    "SIGUSR2",
                    res.get("total_detected", 0),
                    res.get("new_processed", 0),
                    res.get("existing_processed", 0),
                    res.get("shots", 0),
                    res.get("skipped_existing", 0),
                    res.get("errors", 0),
                )

            if self.paused or self._ordering:
                time.sleep(0.05)
                continue

            # --- 定期スキャン（要約ログ + 間隔表示） ---
            eff = self.fast_interval if in_fast_window(datetime.datetime.now(), self.fast_wins) else self.interval
            if eff != getattr(self, "_current_interval_effective", eff):
                self._current_interval_effective = eff
                try:
                    log_interval(eff_seconds=int(eff), fast=(eff == self.fast_interval))
                except Exception:
                    mode = "高速" if eff == self.fast_interval else "通常"
                    print(f"⏱ 現在の更新間隔: {eff} 秒（{mode}）")

            if self._force or (time.time() - last >= eff):
                self._force = False
                last = time.time()
                self._safe_reload_vine()

                try:
                    res = self._scan_once(deep=False, include_existing=False, allow_reorder=self.allow_dup_order, label="periodic")
                except Exception:
                    res = {
                        "total_detected": 0,
                        "new_processed": 0,
                        "existing_processed": 0,
                        "shots": 0,
                        "skipped_existing": 0,
                        "errors": 1,
                        "status": "detect-error",
                    }

                try:
                    changed = (res.get("new_processed", 0) + res.get("existing_processed", 0) + res.get("errors", 0)) > 0
                    if changed:
                        log_scan_summary_jp(
                            "定期スキャン",
                            res.get("total_detected", 0),
                            res.get("new_processed", 0),
                            res.get("existing_processed", 0),
                            res.get("shots", 0),
                            res.get("skipped_existing", 0),
                            res.get("errors", 0),
                        )
                except Exception:
                    pass
            else:
                time.sleep(0.03)

    # ---- シグナル ----
    def setup_signals(self):
        try:
            signal.signal(signal.SIGTERM, lambda *_: setattr(self, "running", False))
        except Exception:
            pass
        try:
            signal.signal(signal.SIGHUP, lambda *_: setattr(self, "_force", True))
        except Exception:
            pass
        try:
            signal.signal(signal.SIGUSR1, lambda *_: setattr(self, "_sig_quick", True))
        except Exception:
            pass
        try:
            signal.signal(signal.SIGUSR2, lambda *_: setattr(self, "_sig_deep", True))
        except Exception:
            pass


# ---- メイン ----
def main():
    import argparse

    parser = argparse.ArgumentParser(description="Amazon Vine ウォッチャー（Firefox｜超低遅延）")
    g = parser.add_mutually_exclusive_group()
    g.add_argument("--headed", action="store_true", help="ウィンドウ表示で起動（デフォルト）")
    g.add_argument("--headless", action="store_true", help="ヘッドレスで起動")
    parser.add_argument("--gas-test", action="store_true", help="スプレッドシート(GAS)追記の疎通テストのみ実行")
    args = parser.parse_args()

    if args.gas_test:
        payload = {
            "title": f"疎通テスト {datetime.datetime.now():%Y-%m-%d %H:%M:%S}",
            "price": "¥12,980",
            "asin": "B0TEST0001",
            "queue_url": URL,
            "brand": "TEST",
            "priority": "⚡",
        }
        res = gas_append_row(payload)
        print(json.dumps(res or {"ok": False, "error": "no_response"}, ensure_ascii=False))
        return

    headed = not bool(getattr(args, "headless", False))
    w = VineWatcher(headed=headed)
    w.setup_signals()

    def cleanup(*_):
        try:
            w.key.stop()
        except Exception:
            pass
        try:
            w._close_browser()
        except Exception:
            pass
        print("\n終了しました。")

    try:
        w.loop()
    except KeyboardInterrupt:
        pass
    finally:
        cleanup()


if __name__ == "__main__":
    main()
