#!/usr/bin/env python3
from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_PID_PATH = DATA_DIR / "run" / "polymarket_crypto_5m_btc_paper_trader_v1.pid"
DEFAULT_REPORT_SCRIPT = ROOT / "scripts" / "build_polymarket_crypto_5m_btc_paper_trading_report_v1.py"
DEFAULT_LOG_PATH = DATA_DIR / "logs" / "polymarket_crypto_5m_btc_paper_report_watcher_v1.log"


def utc_now() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(path: Path, message: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(f"[{utc_now()}] {message}\n")


def read_pid(path: Path) -> int | None:
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (FileNotFoundError, ValueError):
        return None


def pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pid-path", type=Path, default=DEFAULT_PID_PATH)
    parser.add_argument("--report-script", type=Path, default=DEFAULT_REPORT_SCRIPT)
    parser.add_argument("--log-path", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--poll-seconds", type=float, default=60.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log(args.log_path, f"watcher started pid_path={args.pid_path}")

    while True:
        pid = read_pid(args.pid_path)
        if pid is None:
            log(args.log_path, "paper trader pid missing; building report")
            break
        if not pid_is_running(pid):
            log(args.log_path, f"paper trader pid={pid} stopped; building report")
            break
        time.sleep(max(5.0, args.poll_seconds))

    result = subprocess.run(
        [sys.executable, str(args.report_script)],
        cwd=str(ROOT),
        text=True,
        capture_output=True,
        check=False,
    )
    if result.stdout:
        log(args.log_path, "report stdout: " + result.stdout.strip())
    if result.stderr:
        log(args.log_path, "report stderr: " + result.stderr.strip())
    log(args.log_path, f"report build finished returncode={result.returncode}")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
