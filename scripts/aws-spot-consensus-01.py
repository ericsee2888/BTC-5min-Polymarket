#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import math
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo


HKT = ZoneInfo("Asia/Hong_Kong")
TAKER_FEE_RATE = 0.072


@dataclass(frozen=True)
class AwsSpotConsensus01:
    strategy_id: str
    direction_mode: str
    threshold_usd: float
    entry_start_second: float
    entry_end_second: float
    entry_cap: float
    min_aligned_cvd_15s: float
    min_aligned_book_imbalance: float
    min_aligned_chainlink_delta: float
    max_price_to_beat_second: float


PROFILE = AwsSpotConsensus01(
    strategy_id="aws-spot-consensus-01",
    direction_mode="spot_consensus",
    threshold_usd=20.0,
    entry_start_second=15.0,
    entry_end_second=120.0,
    entry_cap=0.70,
    min_aligned_cvd_15s=30_000.0,
    min_aligned_book_imbalance=0.40,
    min_aligned_chainlink_delta=20.0,
    max_price_to_beat_second=30.0,
)
DEFAULT_SAMPLES = Path("data/aws_native_strategy_research_v1/aws_native_combined_signal_sources_20260524_20260527.csv")
DEFAULT_OUTCOMES = Path("data/aws_native_strategy_research_v1/aws_native_combined_strategy_grid_official_outcomes.csv")
DEFAULT_OUTPUT_DIR = Path("data/aws_spot_consensus_01")


ORDER_FIELDS = [
    "strategy_id",
    "event_time_utc",
    "event_time_hkt",
    "hkt_date",
    "slug",
    "direction",
    "direction_reason",
    "seconds_since_start",
    "price_to_beat_observed_second",
    "entry_price",
    "order_cash_usdc",
    "expected_shares",
    "spot_consensus_delta",
    "binance_delta",
    "coinbase_delta",
    "aligned_cvd_15s",
    "aligned_book_imbalance",
    "aligned_chainlink_delta",
    "official_outcome",
    "pnl",
]


SUMMARY_FIELDS = [
    "strategy_id",
    "scope",
    "hkt_date",
    "trades",
    "resolved",
    "wins",
    "losses",
    "win_rate",
    "pnl",
    "avg_entry_price",
]


def parse_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def hkt_from_utc(value: str) -> datetime:
    return datetime.fromisoformat(value).astimezone(HKT)


def signed(value: float | None, direction: str) -> float | None:
    if value is None or direction not in {"UP", "DOWN"}:
        return None
    return value if direction == "UP" else -value


def expected_shares(order_cash: float, ask: float) -> float:
    return order_cash / (ask + TAKER_FEE_RATE * ask * (1.0 - ask))


def direction_for(row: dict[str, str], profile: AwsSpotConsensus01) -> tuple[str, str]:
    consensus = parse_float(row.get("spot_consensus_delta_to_beat"))
    if consensus is None:
        return "", "missing_spot_consensus"
    if consensus >= profile.threshold_usd:
        return "UP", "spot_consensus_up"
    if consensus <= -profile.threshold_usd:
        return "DOWN", "spot_consensus_down"
    return "", "spot_consensus_not_met"


def cap_suffix(cap: float) -> str:
    return f"0_{int(round(cap * 100)):02d}"


def book_values(row: dict[str, str], direction: str, cap: float) -> tuple[float | None, float | None]:
    prefix = "up" if direction == "UP" else "down"
    return (
        parse_float(row.get(f"{prefix}_best_ask")),
        parse_float(row.get(f"{prefix}_ask_cash_lte_{cap_suffix(cap)}")),
    )


def row_passes(row: dict[str, str], direction: str, profile: AwsSpotConsensus01, order_cash: float) -> tuple[bool, str]:
    seconds = parse_float(row.get("seconds_since_start"))
    if seconds is None or not (profile.entry_start_second <= seconds <= profile.entry_end_second):
        return False, "outside_entry_window"
    price_second = parse_float(row.get("price_to_beat_observed_second"))
    if price_second is None or price_second > profile.max_price_to_beat_second:
        return False, "price_to_beat_too_late"
    ask, depth = book_values(row, direction, profile.entry_cap)
    if ask is None or ask > profile.entry_cap:
        return False, "cap_not_met"
    if depth is None or depth < order_cash:
        return False, "depth_not_enough"
    aligned_cvd = signed(parse_float(row.get("external_trade_cvd_cash_15s_median")), direction)
    if aligned_cvd is None or aligned_cvd < profile.min_aligned_cvd_15s:
        return False, "cvd_not_met"
    aligned_book = signed(parse_float(row.get("external_book_imbalance_median")), direction)
    if aligned_book is None or aligned_book < profile.min_aligned_book_imbalance:
        return False, "book_not_met"
    aligned_chainlink = signed(parse_float(row.get("chainlink_delta_to_beat")), direction)
    if aligned_chainlink is None or aligned_chainlink < profile.min_aligned_chainlink_delta:
        return False, "chainlink_not_met"
    return True, "passed"


def read_outcomes(path: Path) -> dict[str, str]:
    outcomes: dict[str, str] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            slug = row.get("slug") or ""
            outcome = (row.get("official_outcome") or "").upper()
            if slug and outcome in {"UP", "DOWN"}:
                outcomes[slug] = outcome
    return outcomes


def read_rows_by_slug(path: Path) -> dict[str, list[dict[str, str]]]:
    rows_by_slug: dict[str, list[dict[str, str]]] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            slug = row.get("slug") or ""
            if slug:
                rows_by_slug.setdefault(slug, []).append(row)
    for rows in rows_by_slug.values():
        rows.sort(key=lambda item: parse_float(item.get("sampled_at_unix_ms")) or 0.0)
    return rows_by_slug


def build_order(row: dict[str, str], direction: str, reason: str, order_cash: float, outcomes: dict[str, str]) -> dict[str, Any]:
    ask, _depth = book_values(row, direction, PROFILE.entry_cap)
    assert ask is not None
    outcome = outcomes.get(row.get("slug", ""), "")
    shares = expected_shares(order_cash, ask)
    pnl = shares - order_cash if outcome and outcome == direction else (-order_cash if outcome else "")
    event_time_utc = row.get("sampled_at_utc", "")
    event_hkt = hkt_from_utc(event_time_utc) if event_time_utc else None
    return {
        "strategy_id": PROFILE.strategy_id,
        "event_time_utc": event_time_utc,
        "event_time_hkt": event_hkt.isoformat() if event_hkt else "",
        "hkt_date": event_hkt.strftime("%Y-%m-%d") if event_hkt else "",
        "slug": row.get("slug", ""),
        "direction": direction,
        "direction_reason": reason,
        "seconds_since_start": parse_float(row.get("seconds_since_start")),
        "price_to_beat_observed_second": parse_float(row.get("price_to_beat_observed_second")),
        "entry_price": ask,
        "order_cash_usdc": order_cash,
        "expected_shares": shares,
        "spot_consensus_delta": parse_float(row.get("spot_consensus_delta_to_beat")),
        "binance_delta": parse_float(row.get("binance_spot_delta_to_beat")),
        "coinbase_delta": parse_float(row.get("coinbase_spot_delta_to_beat")),
        "aligned_cvd_15s": signed(parse_float(row.get("external_trade_cvd_cash_15s_median")), direction),
        "aligned_book_imbalance": signed(parse_float(row.get("external_book_imbalance_median")), direction),
        "aligned_chainlink_delta": signed(parse_float(row.get("chainlink_delta_to_beat")), direction),
        "official_outcome": outcome,
        "pnl": pnl,
    }


def run_replay(rows_by_slug: dict[str, list[dict[str, str]]], outcomes: dict[str, str], order_cash: float) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for slug, rows in rows_by_slug.items():
        for row in rows:
            direction, reason = direction_for(row, PROFILE)
            if direction not in {"UP", "DOWN"}:
                continue
            passed, _why = row_passes(row, direction, PROFILE, order_cash)
            if not passed:
                continue
            orders.append(build_order(row, direction, reason, order_cash, outcomes))
            break
    orders.sort(key=lambda item: item["event_time_utc"])
    return orders


def summarize(orders: list[dict[str, Any]], scope: str, hkt_date: str = "") -> dict[str, Any]:
    resolved = [row for row in orders if row.get("pnl") not in {"", None}]
    wins = sum(1 for row in resolved if float(row["pnl"]) > 0)
    losses = len(resolved) - wins
    pnl = sum(float(row["pnl"]) for row in resolved)
    avg_entry = sum(float(row["entry_price"]) for row in orders) / len(orders) if orders else ""
    return {
        "strategy_id": PROFILE.strategy_id,
        "scope": scope,
        "hkt_date": hkt_date,
        "trades": len(orders),
        "resolved": len(resolved),
        "wins": wins,
        "losses": losses,
        "win_rate": wins / len(resolved) if resolved else "",
        "pnl": pnl if resolved else "",
        "avg_entry_price": avg_entry,
    }


def write_csv(path: Path, fieldnames: list[str], rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_report(path: Path, samples: Path, outcomes: Path, summary_rows: list[dict[str, Any]], orders_path: Path) -> None:
    total = summary_rows[0]
    by_day = summary_rows[1:]
    lines = [
        "# aws-spot-consensus-01 独立策略回放报告",
        "",
        "## 策略口径",
        f"- 策略ID：`{PROFILE.strategy_id}`",
        "- 方向：spot consensus，相对 price_to_beat 同向超过 $20。",
        "- 入场窗口：15-120秒。",
        "- 入场价格上限：0.70。",
        "- 过滤：CVD>=30000、book>=0.40、Chainlink>=20。",
        "- price_to_beat：必须在30秒内捕获。",
        "- 说明：这是独立策略脚本，不接统一决策引擎，不影响 mainline/live 主链路。",
        "",
        "## 数据",
        f"- 样本：`{samples}`",
        f"- official outcome：`{outcomes}`",
        "",
        "## 总结果",
        f"- 交易数：{total['trades']}",
        f"- 胜率：{float(total['win_rate']) * 100:.2f}%" if total["win_rate"] != "" else "- 胜率：NA",
        f"- PnL：{float(total['pnl']):.2f} USDC" if total["pnl"] != "" else "- PnL：NA",
        f"- 平均入场价：{float(total['avg_entry_price']):.4f}" if total["avg_entry_price"] != "" else "- 平均入场价：NA",
        "",
        "## 逐日结果",
        "| HKT日期 | 交易数 | 胜率 | PnL | 平均入场价 |",
        "|---|---:|---:|---:|---:|",
    ]
    for row in by_day:
        win_rate = f"{float(row['win_rate']) * 100:.2f}%" if row["win_rate"] != "" else "NA"
        pnl = f"{float(row['pnl']):.2f}" if row["pnl"] != "" else "NA"
        avg_entry = f"{float(row['avg_entry_price']):.4f}" if row["avg_entry_price"] != "" else "NA"
        lines.append(f"| {row['hkt_date']} | {row['trades']} | {win_rate} | {pnl} | {avg_entry} |")
    lines.extend(["", "## 输出", f"- 逐笔订单：`{orders_path}`", ""])
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines), encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Replay independent aws-spot-consensus-01 strategy from AWS signal samples.")
    parser.add_argument("--samples-csv", type=Path, default=DEFAULT_SAMPLES)
    parser.add_argument("--outcomes-csv", type=Path, default=DEFAULT_OUTCOMES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--order-cash-usdc", type=float, default=100.0)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows_by_slug = read_rows_by_slug(args.samples_csv)
    outcomes = read_outcomes(args.outcomes_csv)
    orders = run_replay(rows_by_slug, outcomes, args.order_cash_usdc)
    orders_path = args.output_dir / "aws_spot_consensus_01_orders.csv"
    summary_path = args.output_dir / "aws_spot_consensus_01_summary.csv"
    report_path = args.output_dir / "AWS_SPOT_CONSENSUS_01_REPLAY_REPORT_CN.md"
    by_day: dict[str, list[dict[str, Any]]] = {}
    for order in orders:
        by_day.setdefault(str(order.get("hkt_date") or ""), []).append(order)
    summary_rows = [summarize(orders, "total")]
    summary_rows.extend(summarize(by_day[day], "day", day) for day in sorted(by_day))
    write_csv(orders_path, ORDER_FIELDS, orders)
    write_csv(summary_path, SUMMARY_FIELDS, summary_rows)
    write_report(report_path, args.samples_csv, args.outcomes_csv, summary_rows, orders_path)
    total = summary_rows[0]
    print(f"strategy_id={PROFILE.strategy_id}")
    print(f"trades={total['trades']}")
    print(f"win_rate={float(total['win_rate']) * 100:.2f}%" if total["win_rate"] != "" else "win_rate=NA")
    print(f"pnl={float(total['pnl']):.2f}" if total["pnl"] != "" else "pnl=NA")
    print(f"orders={orders_path}")
    print(f"summary={summary_path}")
    print(f"report={report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
