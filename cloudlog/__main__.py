from __future__ import annotations

import argparse
import getpass
import os
from pathlib import Path


def _resolve_data_dir(raw: Path) -> Path:
    p = Path(raw)
    if not p.is_absolute():
        p = (Path.cwd() / p).resolve()
    return p


def _set_admin_password(*, data_dir: Path, username: str) -> int:
    from cloudlog.db import CloudlogDB

    user = str(username or "admin").strip() or "admin"
    first = getpass.getpass("New password: ")
    second = getpass.getpass("Confirm password: ")
    if first != second:
        print("パスワードが一致しません。")
        return 1

    if len(first) < 8:
        print("パスワードは8文字以上にしてください。")
        return 1

    db = CloudlogDB(data_dir / "cloudlog.sqlite3")
    try:
        if db.get_user_by_name(user) is None:
            print(f"ユーザーが見つかりません: {user}")
            return 1
        changed = db.set_user_password(username=user, new_password=first)
        if not changed:
            print("パスワード更新に失敗しました。")
            return 1
    finally:
        db.close()

    print(f"ユーザー '{user}' のパスワードを更新しました。")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cloudlog")
    default_data_dir = Path((os.getenv("CLOUDLOG_DATA_DIR", ".cloudlog") or ".cloudlog").strip())
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--data-dir", type=Path, default=default_data_dir)
    parser.add_argument("--set-admin-password", action="store_true", help="Update password for an existing admin/user")
    parser.add_argument("--username", default="admin")
    args = parser.parse_args(argv)

    data_dir = _resolve_data_dir(args.data_dir)

    if args.set_admin_password:
        return _set_admin_password(data_dir=data_dir, username=args.username)

    os.environ["CLOUDLOG_DATA_DIR"] = str(data_dir)

    try:
        import uvicorn  # type: ignore
    except Exception:
        print("uvicorn がインストールされていません。pip install -r requirements.txt を実行してください。")
        return 1

    uvicorn.run("cloudlog.app:app", host=args.host, port=args.port, reload=bool(args.reload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
