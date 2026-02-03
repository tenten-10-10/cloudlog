from __future__ import annotations

import argparse
import getpass
import logging
import os
import time
from pathlib import Path

from dotenv import load_dotenv

from sitewatcher.config import load_config
from sitewatcher.monitor import run_once
from sitewatcher.worker import loop as worker_loop


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="sitewatcher")
    parser.add_argument("--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    run = sub.add_parser("run", help="Check targets and notify on changes")
    run.add_argument("--config", type=Path, default=Path("config.yaml"))
    mode = run.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run a single check (default)")
    mode.add_argument("--loop", action="store_true", help="Run forever with interval_seconds")

    web = sub.add_parser("web", help="Run the web UI (self-hosted)")
    web.add_argument("--host", default="127.0.0.1")
    web.add_argument("--port", type=int, default=8000)
    web.add_argument("--reload", action="store_true")
    web.add_argument("--data-dir", type=Path, default=Path(".sitewatcher"))

    worker = sub.add_parser("worker", help="Run background scheduler (recommended for production)")
    worker.add_argument("--data-dir", type=Path, default=Path(".sitewatcher"))
    worker.add_argument("--once", action="store_true")

    hp = sub.add_parser("hash-password", help="Generate password hash for SITEWATCHER_ADMIN_PASSWORD_HASH")
    hp.add_argument("--password", default="")
    hp.add_argument("--iterations", type=int, default=260_000)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    load_dotenv()

    if args.command == "run":
        config_path: Path = args.config
        config = load_config(config_path)

        if args.loop:
            interval_seconds = int(config.get("interval_seconds", 300))
            logging.info("Loop mode: interval_seconds=%s", interval_seconds)
            while True:
                run_once(config, config_path=config_path)
                time.sleep(interval_seconds)
        else:
            run_once(config, config_path=config_path)
        return 0

    if args.command == "web":
        os.environ["SITEWATCHER_DATA_DIR"] = str(args.data_dir)
        try:
            import uvicorn  # type: ignore
        except Exception:
            logging.error("uvicorn is not installed. Install dependencies: pip install -r requirements.txt")
            return 1
        uvicorn.run("sitewatcher.web.app:app", host=args.host, port=int(args.port), reload=bool(args.reload))
        return 0

    if args.command == "worker":
        os.environ["SITEWATCHER_DATA_DIR"] = str(args.data_dir)
        worker_loop(once=bool(args.once))
        return 0

    if args.command == "hash-password":
        from sitewatcher.web.auth import hash_password

        password = str(args.password or "").strip()
        if not password:
            password = getpass.getpass("Password: ")
        print(hash_password(password, iterations=int(args.iterations)))
        return 0

    parser.error(f"Unknown command: {args.command}")
    return 2
