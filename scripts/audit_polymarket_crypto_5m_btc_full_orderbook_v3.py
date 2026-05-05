#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_SNAPSHOT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.jsonl"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.csv"
DEFAULT_SIGNAL_EVENTS = DATA_DIR / "polymarket_crypto_5m_btc_signal_events_v3.jsonl"
DEFAULT_SKIP_EVENTS = DATA_DIR / "polymarket_crypto_5m_btc_skip_events_v3.jsonl"
DEFAULT_TRADES_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_FULL_ORDERBOOK_V3_SELF_AUDIT_REPORT_CN.md"


REQUIRED_SNAPSHOT_KEYS = [
    "sampled_at_utc",
    "sampled_at_unix_ms",
    "current_slug",
    "next_slug",
    "price_to_beat",
    "price_to_beat_status",
    "price_to_beat_observed_second",
    "binance_btcusdt",
    "coinbase_btcusd",
    "chainlink_btcusd",
    "up_token_id",
    "down_token_id",
    "up_bids",
    "up_asks",
    "down_bids",
    "down_asks",
    "signal_25_direction",
    "signal_25_reason",
    "signal_35_direction",
    "signal_35_reason",
    "signal_50_direction",
    "signal_50_reason",
    "up_ask_cash_lte_0_65",
    "down_ask_cash_lte_0_65",
    "up_bid_cash_gte_0_75",
    "down_bid_cash_gte_0_75",
    "snapshot_usable_for_formal_backtest",
]

REQUIRED_TRADE_KEYS = [
    "paper_trade_id",
    "strategy_id",
    "exit_rule",
    "slug",
    "direction",
    "order_cash_usdc",
    "entry_latency_ms",
    "signal_time_utc",
    "order_arrival_time_utc",
    "entry_avg_price",
    "entry_worst_price",
    "entry_data_source",
    "entry_fill_levels_json",
    "exit_type",
    "exit_complete",
    "exit_partial",
    "exit_remaining_shares",
    "exit_attempts_json",
    "exit_fill_levels_json",
    "pnl_usdc",
    "roi_on_cash",
]


def read_jsonl(path: Path, limit: int = 0) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                rows.append({"_json_error": line[:200]})
            if limit and len(rows) >= limit:
                break
    return rows


def count_csv_rows(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open(newline="", encoding="utf-8") as handle:
        return max(0, sum(1 for _ in handle) - 1)


def csv_header(path: Path) -> list[str]:
    if not path.exists():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        return next(reader, [])


def has_book_levels(row: dict[str, Any], key: str) -> bool:
    levels = row.get(key)
    return isinstance(levels, list) and all(
        isinstance(level, dict) and "price" in level and "size" in level
        for level in levels
    )


def build_report(args: argparse.Namespace) -> tuple[str, bool]:
    snapshots = read_jsonl(args.snapshot_jsonl, limit=args.max_snapshots_to_check)
    signals = read_jsonl(args.signal_events, limit=0)
    skips = read_jsonl(args.skip_events, limit=0)
    trades = read_jsonl(args.trades_jsonl, limit=0)
    csv_rows = count_csv_rows(args.snapshot_csv)
    trade_csv_rows = count_csv_rows(args.trades_csv)
    header = csv_header(args.snapshot_csv)
    trade_header = csv_header(args.trades_csv)

    failures: list[str] = []
    warnings: list[str] = []

    if not snapshots:
        failures.append("snapshot JSONL 为空或不存在")
    if csv_rows == 0:
        failures.append("snapshot CSV 为空或不存在")
    if snapshots and any("_json_error" in row for row in snapshots):
        failures.append("snapshot JSONL 存在无法解析的 JSON 行")

    missing_snapshot_keys: Counter[str] = Counter()
    book_failures = 0
    usable_count = 0
    for row in snapshots:
        for key in REQUIRED_SNAPSHOT_KEYS:
            if key not in row:
                missing_snapshot_keys[key] += 1
        if not (
            has_book_levels(row, "up_bids")
            and has_book_levels(row, "up_asks")
            and has_book_levels(row, "down_bids")
            and has_book_levels(row, "down_asks")
        ):
            book_failures += 1
        if row.get("snapshot_usable_for_formal_backtest"):
            usable_count += 1

    if missing_snapshot_keys:
        failures.append("snapshot 缺关键字段: " + ", ".join(sorted(missing_snapshot_keys)))
    if book_failures:
        failures.append(f"有 {book_failures} 条 snapshot 没有完整 Up/Down bids/asks 数组")

    missing_csv_keys = [key for key in REQUIRED_SNAPSHOT_KEYS if key not in header and key not in {"up_bids", "up_asks", "down_bids", "down_asks"}]
    if missing_csv_keys:
        failures.append("snapshot CSV 缺关键摘要字段: " + ", ".join(missing_csv_keys))

    if trades:
        missing_trade_keys: Counter[str] = Counter()
        for row in trades:
            for key in REQUIRED_TRADE_KEYS:
                if key not in row:
                    missing_trade_keys[key] += 1
        if missing_trade_keys:
            failures.append("trade JSONL 缺关键字段: " + ", ".join(sorted(missing_trade_keys)))
    else:
        warnings.append("当前 smoke test 没有产生模拟交易。这可能只是测试时间太短，不代表脚本失败。")

    missing_trade_csv_keys = [key for key in REQUIRED_TRADE_KEYS if key not in trade_header]
    if missing_trade_csv_keys:
        failures.append("trade CSV 缺关键字段: " + ", ".join(missing_trade_csv_keys))

    if not signals:
        warnings.append("当前没有 signal event。短 smoke test 可能不会触发信号。")
    if not skips:
        warnings.append("当前没有 skip event。短 smoke test 可能不会触发信号。")

    status = "PASS" if not failures else "FAIL"
    report = f"""# Polymarket BTC 5分钟 V3 完整订单簿采集脚本自审报告

## 一、结论

自审结果：`{status}`

## 二、文件检查

- snapshot JSONL：`{args.snapshot_jsonl}`
- snapshot JSONL 检查行数：`{len(snapshots)}`
- snapshot CSV：`{args.snapshot_csv}`
- snapshot CSV 行数：`{csv_rows}`
- signal events：`{args.signal_events}`，行数 `{len(signals)}`
- skip events：`{args.skip_events}`，行数 `{len(skips)}`
- trades JSONL：`{args.trades_jsonl}`，行数 `{len(trades)}`
- trades CSV：`{args.trades_csv}`，行数 `{trade_csv_rows}`

## 三、关键字段检查

- snapshot 必填字段缺失：`{dict(missing_snapshot_keys) if missing_snapshot_keys else "无"}`
- snapshot 订单簿数组失败行数：`{book_failures}`
- 可用于正式回测的 snapshot 行数：`{usable_count}`
- trade JSONL 行数：`{len(trades)}`

## 四、失败项

{chr(10).join(f"- {item}" for item in failures) if failures else "- 无"}

## 五、警告项

{chr(10).join(f"- {item}" for item in warnings) if warnings else "- 无"}

## 六、自审解释

这个自审只检查“脚本是否按第三轮采集合同把关键字段落盘”。如果 smoke test 时间较短，没有出现交易信号，signal / skip / trade 为空可以接受；但正式 24 小时采集结束后，必须重新运行本自审，并且要求 signal、skip、trade 能够互相对账。
"""
    return report, not failures


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-jsonl", type=Path, default=DEFAULT_SNAPSHOT_JSONL)
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--signal-events", type=Path, default=DEFAULT_SIGNAL_EVENTS)
    parser.add_argument("--skip-events", type=Path, default=DEFAULT_SKIP_EVENTS)
    parser.add_argument("--trades-jsonl", type=Path, default=DEFAULT_TRADES_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--max-snapshots-to-check", type=int, default=500)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report, ok = build_report(args)
    args.report.write_text(report, encoding="utf-8")
    print(f"report={args.report}")
    print(f"status={'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
