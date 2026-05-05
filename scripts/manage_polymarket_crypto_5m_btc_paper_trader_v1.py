#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
RUN_DIR = DATA_DIR / "run"
LOG_DIR = DATA_DIR / "logs"
PAPER_SCRIPT = ROOT / "scripts" / "paper_trade_polymarket_crypto_5m_btc_v1.py"

DEFAULT_PID_PATH = RUN_DIR / "polymarket_crypto_5m_btc_paper_trader_v1.pid"
DEFAULT_LOG_PATH = LOG_DIR / "polymarket_crypto_5m_btc_paper_trader_v1.log"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv"
DEFAULT_EVENT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_trades_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PAPER_TRADING_RUN_V1_CN.md"


def utc_now() -> str:
    return datetime.now(UTC).isoformat()


def pid_is_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def read_pid(pid_path: Path) -> int | None:
    if not pid_path.exists():
        return None
    try:
        return int(pid_path.read_text(encoding="utf-8").strip())
    except ValueError:
        return None


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8", newline="") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def tail_lines(path: Path, line_count: int) -> list[str]:
    if not path.exists():
        return []
    return path.read_text(encoding="utf-8", errors="replace").splitlines()[-line_count:]


def build_command(args: argparse.Namespace) -> list[str]:
    cmd = [
        sys.executable,
        str(PAPER_SCRIPT),
        "--threshold-usd",
        str(args.threshold_usd),
        "--order-cash-usdc",
        str(args.order_cash_usdc),
        "--entry-cap",
        str(args.entry_cap),
        "--entry-start-seconds",
        str(args.entry_start_seconds),
        "--entry-end-seconds",
        str(args.entry_end_seconds),
        "--profit-target",
        str(args.profit_target),
        "--stop-loss",
        str(args.stop_loss),
        "--interval-seconds",
        str(args.interval_seconds),
        "--duration-seconds",
        str(args.duration_seconds),
        "--timeout-seconds",
        str(args.timeout_seconds),
        "--rtds-warmup-seconds",
        str(args.rtds_warmup_seconds),
        "--snapshot-csv",
        str(args.snapshot_csv),
        "--event-jsonl",
        str(args.event_jsonl),
        "--trades-csv",
        str(args.trades_csv),
        "--report",
        str(args.report),
    ]
    if args.reset_output:
        cmd.append("--reset-output")
    return cmd


def command_start(args: argparse.Namespace) -> int:
    RUN_DIR.mkdir(parents=True, exist_ok=True)
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    existing_pid = read_pid(args.pid_path)
    if existing_pid and pid_is_running(existing_pid):
        print(
            json.dumps(
                {
                    "status": "already_running",
                    "pid": existing_pid,
                    "snapshot_rows": count_csv_rows(args.snapshot_csv),
                    "trade_rows": count_csv_rows(args.trades_csv),
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        return 0

    cmd = build_command(args)
    with args.log_path.open("a", encoding="utf-8") as log:
        log.write(f"\n[{utc_now()}] starting paper trader\n")
        log.write("command: " + " ".join(cmd) + "\n")
        log.flush()
        process = subprocess.Popen(
            cmd,
            cwd=str(ROOT),
            stdout=log,
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )

    args.pid_path.write_text(str(process.pid), encoding="utf-8")
    print(
        json.dumps(
            {
                "status": "started",
                "pid": process.pid,
                "pid_path": str(args.pid_path),
                "log_path": str(args.log_path),
                "snapshot_csv": str(args.snapshot_csv),
                "trades_csv": str(args.trades_csv),
                "duration_seconds": args.duration_seconds,
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


def command_stop(args: argparse.Namespace) -> int:
    pid = read_pid(args.pid_path)
    if not pid:
        print(json.dumps({"status": "not_running", "reason": "missing_pid"}, indent=2))
        return 0
    if not pid_is_running(pid):
        args.pid_path.unlink(missing_ok=True)
        print(json.dumps({"status": "not_running", "pid": pid}, indent=2))
        return 0

    os.killpg(pid, signal.SIGTERM)
    deadline = time.time() + args.stop_timeout_seconds
    while time.time() < deadline:
        if not pid_is_running(pid):
            args.pid_path.unlink(missing_ok=True)
            print(json.dumps({"status": "stopped", "pid": pid}, indent=2))
            return 0
        time.sleep(0.5)

    if args.force:
        os.killpg(pid, signal.SIGKILL)
        args.pid_path.unlink(missing_ok=True)
        print(json.dumps({"status": "force_stopped", "pid": pid}, indent=2))
        return 0

    print(json.dumps({"status": "still_running", "pid": pid}, indent=2))
    return 1


def command_status(args: argparse.Namespace) -> int:
    pid = read_pid(args.pid_path)
    running = bool(pid and pid_is_running(pid))
    payload = {
        "status": "running" if running else "not_running",
        "pid": pid,
        "pid_path": str(args.pid_path),
        "log_path": str(args.log_path),
        "snapshot_csv": str(args.snapshot_csv),
        "event_jsonl": str(args.event_jsonl),
        "trades_csv": str(args.trades_csv),
        "report": str(args.report),
        "snapshot_rows": count_csv_rows(args.snapshot_csv),
        "trade_rows": count_csv_rows(args.trades_csv),
        "event_size_bytes": args.event_jsonl.stat().st_size if args.event_jsonl.exists() else 0,
        "checked_at_utc": utc_now(),
    }
    print(json.dumps(payload, ensure_ascii=False, indent=2))
    return 0


def command_tail(args: argparse.Namespace) -> int:
    lines = tail_lines(args.log_path, args.lines)
    print("\n".join(lines) if lines else f"No log lines found at {args.log_path}")
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Manage the real-time Polymarket BTC 5m paper trader as a background process."
    )
    parser.add_argument("command", choices=["start", "stop", "status", "tail"])
    parser.add_argument("--pid-path", type=Path, default=DEFAULT_PID_PATH)
    parser.add_argument("--log-path", type=Path, default=DEFAULT_LOG_PATH)
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--event-jsonl", type=Path, default=DEFAULT_EVENT_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--threshold-usd", type=float, default=50.0)
    parser.add_argument("--order-cash-usdc", type=float, default=100.0)
    parser.add_argument("--entry-cap", type=float, default=0.65)
    parser.add_argument("--entry-start-seconds", type=float, default=60.0)
    parser.add_argument("--entry-end-seconds", type=float, default=180.0)
    parser.add_argument("--profit-target", type=float, default=0.75)
    parser.add_argument("--stop-loss", type=float, default=0.35)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--duration-seconds", type=float, default=86400.0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--rtds-warmup-seconds", type=float, default=3.0)
    parser.add_argument("--reset-output", action="store_true")
    parser.add_argument("--stop-timeout-seconds", type=float, default=10.0)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--lines", type=int, default=40)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if args.command == "start":
        return command_start(args)
    if args.command == "stop":
        return command_stop(args)
    if args.command == "status":
        return command_status(args)
    if args.command == "tail":
        return command_tail(args)
    raise SystemExit(f"Unknown command: {args.command}")


if __name__ == "__main__":
    raise SystemExit(main())
