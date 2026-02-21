from __future__ import annotations

import argparse
import getpass
import os


def _set_admin_password(*, email: str) -> int:
    from cloudlog.timeclock_store import TimeclockStore

    target = str(email or "").strip().lower()
    if not target:
        print("--email を指定してください。")
        return 1

    first = getpass.getpass("New password: ")
    second = getpass.getpass("Confirm password: ")
    if first != second:
        print("パスワードが一致しません。")
        return 1
    if len(first) < 8:
        print("パスワードは8文字以上にしてください。")
        return 1

    store = TimeclockStore.from_env()
    user = store.get_user_by_email(target)
    if user is None:
        print(f"ユーザーが見つかりません: {target}")
        return 1
    store.update_user(user_id=user["user_id"], password=first)
    print(f"ユーザー '{target}' のパスワードを更新しました。")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="cloudlog")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8010)
    parser.add_argument("--reload", action="store_true")
    parser.add_argument("--set-admin-password", action="store_true", help="Update password for an existing user")
    parser.add_argument("--email", default="")
    args = parser.parse_args(argv)

    if args.set_admin_password:
        return _set_admin_password(email=args.email)

    try:
        import uvicorn  # type: ignore
    except Exception:
        print("uvicorn がインストールされていません。pip install -r requirements.txt を実行してください。")
        return 1

    os.environ.setdefault("CLOUDLOG_HTTPS_ONLY", "0")
    uvicorn.run("cloudlog.app:app", host=args.host, port=args.port, reload=bool(args.reload))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
