#!/usr/bin/env python3
from __future__ import annotations

import csv
import statistics
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.csv"
ACCOUNT_BACKTEST_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_account_level_backtest.csv"
ACCOUNT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_account_level_executed_trades.csv"
REPORT_MD = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_V3_ACCOUNT_LEVEL_BACKTEST_REPORT_CN.md"

ACCOUNT_CAPITALS = [1000.0, 5000.0, 10000.0]
SMALL_CASH_AMOUNTS = {50.0, 100.0}
REALISTIC_LATENCIES = {250, 500, 1000}


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def pct(numerator: float, denominator: float) -> float:
    return round(numerator / denominator * 100, 4) if denominator else 0.0


def median(values: list[float]) -> float:
    return round(statistics.median(values), 6) if values else 0.0


@dataclass(frozen=True)
class Trade:
    strategy_id: str
    exit_rule: str
    slug: str
    direction: str
    threshold_usd: float
    entry_start_second: float
    entry_cap: float
    order_cash_usdc: float
    entry_latency_ms: int
    entry_time: datetime
    exit_time: datetime
    entry_total_cash_used: float
    pnl_usdc: float
    roi_on_cash: float
    correct: bool
    exit_type: str
    entry_avg_price: float


def read_trades() -> list[Trade]:
    trades: list[Trade] = []
    with TRADES_CSV.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            entry_total = as_float(row.get("entry_total_cash_used"))
            pnl = as_float(row.get("pnl_usdc"))
            roi = as_float(row.get("roi_on_cash"))
            if entry_total is None or pnl is None or roi is None:
                continue
            trades.append(
                Trade(
                    strategy_id=row["strategy_id"],
                    exit_rule=row["exit_rule"],
                    slug=row["slug"],
                    direction=row.get("direction", ""),
                    threshold_usd=as_float(row.get("threshold_usd")) or 0.0,
                    entry_start_second=as_float(row.get("entry_start_second")) or 0.0,
                    entry_cap=as_float(row.get("entry_cap")) or 0.0,
                    order_cash_usdc=as_float(row.get("order_cash_usdc")) or 0.0,
                    entry_latency_ms=int(as_float(row.get("entry_latency_ms")) or 0),
                    entry_time=parse_time(row["entry_time_utc"]),
                    exit_time=parse_time(row["exit_time_utc"]),
                    entry_total_cash_used=entry_total,
                    pnl_usdc=pnl,
                    roi_on_cash=roi,
                    correct=str(row.get("correct")).strip().lower() == "true",
                    exit_type=row.get("exit_type", ""),
                    entry_avg_price=as_float(row.get("entry_avg_price")) or 0.0,
                )
            )
    return trades


def simulate_account(strategy_trades: list[Trade], starting_capital: float) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    ordered = sorted(strategy_trades, key=lambda trade: (trade.entry_time, trade.exit_time, trade.slug))
    cash = starting_capital
    equity = starting_capital
    peak_equity = starting_capital
    max_drawdown = 0.0
    active: list[tuple[datetime, float, Trade]] = []
    executed: list[Trade] = []
    skipped_insufficient_cash = 0
    max_capital_in_use = 0.0

    trade_rows: list[dict[str, Any]] = []

    def release_until(timepoint: datetime) -> None:
        nonlocal cash, equity, peak_equity, max_drawdown, active
        still_active: list[tuple[datetime, float, Trade]] = []
        for exit_time, exit_cash, active_trade in active:
            if exit_time <= timepoint:
                cash += exit_cash
                equity += active_trade.pnl_usdc
                peak_equity = max(peak_equity, equity)
                max_drawdown = max(max_drawdown, peak_equity - equity)
            else:
                still_active.append((exit_time, exit_cash, active_trade))
        active = still_active

    for trade in ordered:
        release_until(trade.entry_time)
        if cash + 1e-9 < trade.entry_total_cash_used:
            skipped_insufficient_cash += 1
            continue
        cash -= trade.entry_total_cash_used
        exit_cash = trade.entry_total_cash_used + trade.pnl_usdc
        active.append((trade.exit_time, exit_cash, trade))
        executed.append(trade)
        capital_in_use = sum(active_trade.entry_total_cash_used for _exit_time, _exit_cash, active_trade in active)
        max_capital_in_use = max(max_capital_in_use, capital_in_use)
        trade_rows.append(
            {
                "account_capital": starting_capital,
                "strategy_id": trade.strategy_id,
                "exit_rule": trade.exit_rule,
                "slug": trade.slug,
                "entry_time_utc": trade.entry_time.isoformat(),
                "exit_time_utc": trade.exit_time.isoformat(),
                "entry_total_cash_used": round(trade.entry_total_cash_used, 6),
                "pnl_usdc": round(trade.pnl_usdc, 6),
                "roi_on_cash": round(trade.roi_on_cash, 6),
                "cash_after_entry": round(cash, 6),
                "capital_in_use_after_entry": round(capital_in_use, 6),
            }
        )

    release_until(datetime.max.replace(tzinfo=ordered[0].entry_time.tzinfo) if ordered else datetime.max)

    pnls = [trade.pnl_usdc for trade in executed]
    wins = [trade for trade in executed if trade.pnl_usdc > 0]
    losses = [trade for trade in executed if trade.pnl_usdc <= 0]
    longest_loss_streak = 0
    current_loss_streak = 0
    for trade in executed:
        if trade.pnl_usdc <= 0:
            current_loss_streak += 1
            longest_loss_streak = max(longest_loss_streak, current_loss_streak)
        else:
            current_loss_streak = 0

    first = ordered[0] if ordered else None
    result = {
        "strategy_id": first.strategy_id if first else "",
        "exit_rule": first.exit_rule if first else "",
        "account_capital": starting_capital,
        "threshold_usd": first.threshold_usd if first else "",
        "entry_start_second": first.entry_start_second if first else "",
        "entry_cap": first.entry_cap if first else "",
        "order_cash_usdc": first.order_cash_usdc if first else "",
        "entry_latency_ms": first.entry_latency_ms if first else "",
        "candidate_class": (
            "realistic_small"
            if first and first.order_cash_usdc in SMALL_CASH_AMOUNTS and first.entry_latency_ms in REALISTIC_LATENCIES
            else "exploratory"
        ),
        "available_trade_count": len(ordered),
        "executed_trade_count": len(executed),
        "skipped_insufficient_cash": skipped_insufficient_cash,
        "win_rate_pct": pct(len(wins), len(executed)),
        "total_pnl_usdc": round(sum(pnls), 6),
        "ending_equity_usdc": round(equity, 6),
        "account_roi_pct": pct(equity - starting_capital, starting_capital),
        "avg_pnl_usdc": round(statistics.mean(pnls), 6) if pnls else 0.0,
        "median_pnl_usdc": median(pnls),
        "max_win_usdc": round(max(pnls), 6) if pnls else 0.0,
        "max_loss_usdc": round(min(pnls), 6) if pnls else 0.0,
        "longest_loss_streak": longest_loss_streak,
        "max_drawdown_usdc_realized": round(max_drawdown, 6),
        "max_drawdown_pct_realized": pct(max_drawdown, starting_capital),
        "max_capital_in_use_usdc": round(max_capital_in_use, 6),
        "max_capital_in_use_pct": pct(max_capital_in_use, starting_capital),
        "first_entry_utc": min((trade.entry_time for trade in executed), default=""),
        "last_exit_utc": max((trade.exit_time for trade in executed), default=""),
        "up_trades": sum(1 for trade in executed if trade.direction == "UP"),
        "down_trades": sum(1 for trade in executed if trade.direction == "DOWN"),
        "profit_target_exits": sum(1 for trade in executed if trade.exit_type == "PROFIT_TARGET"),
        "stop_loss_exits": sum(1 for trade in executed if trade.exit_type == "STOP_LOSS"),
        "resolution_exits": sum(1 for trade in executed if trade.exit_type == "RESOLUTION"),
    }
    return result, trade_rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def table(rows: list[dict[str, Any]], columns: list[str], limit: int = 10) -> str:
    if not rows:
        return "无"
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join("---" for _ in columns) + " |",
    ]
    for row in rows[:limit]:
        lines.append("| " + " | ".join(str(row.get(column, "")) for column in columns) + " |")
    return "\n".join(lines)


def build_report(rows: list[dict[str, Any]]) -> str:
    top_1000_realistic = [
        row
        for row in rows
        if row["account_capital"] == 1000.0
        and row["candidate_class"] == "realistic_small"
        and row["executed_trade_count"] >= 10
    ]
    top_1000_realistic.sort(key=lambda row: (row["account_roi_pct"], row["executed_trade_count"]), reverse=True)

    top_5000_realistic = [
        row
        for row in rows
        if row["account_capital"] == 5000.0
        and row["candidate_class"] == "realistic_small"
        and row["executed_trade_count"] >= 10
    ]
    top_5000_realistic.sort(key=lambda row: (row["account_roi_pct"], row["executed_trade_count"]), reverse=True)

    top_all_1000 = [row for row in rows if row["account_capital"] == 1000.0 and row["executed_trade_count"] >= 10]
    top_all_1000.sort(key=lambda row: (row["account_roi_pct"], row["executed_trade_count"]), reverse=True)

    stable_small = [
        row
        for row in top_1000_realistic
        if row["max_drawdown_pct_realized"] <= 50
        and row["max_capital_in_use_pct"] <= 60
        and row["skipped_insufficient_cash"] == 0
    ]

    non_hold_small = [
        row
        for row in top_1000_realistic
        if row["exit_rule"] != "hold_to_resolution"
    ]

    return f"""# Polymarket BTC 5分钟 V3 账户级真钱回测报告

## 一、结论先说

这一步把参数矩阵压缩成了账户级复盘：每次只跑一个固定策略组合，资金从入场占用到退出后才能复用，不能把不同参数组合收益相加。

账户级复盘显示，部分小额策略在这 24小时样本里仍然是正收益，但这些结果仍然是 REST 轮询数据和 Chainlink 推算结算，不是最终真钱结论。它适合用来筛选候选策略，下一步还需要 WebSocket 盘口流和官方结算校验。

## 二、复盘口径

- 起始账户本金：`1000 / 5000 / 10000 USDC`
- 同一账户一次只跑一个固定策略组合。
- 交易资金从入场到退出期间被占用。
- 如果账户现金不足，则跳过该笔交易。
- 账户收益按实际执行交易累计。
- 本报告优先看 `50 / 100 USDC` 且 `250ms / 500ms / 1000ms` 延迟的组合，因为它们更接近小额真钱执行。

## 三、1000 USDC 账户，小额延迟组合 Top

{table(top_1000_realistic, [
    "strategy_id",
    "exit_rule",
    "executed_trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "account_roi_pct",
    "max_drawdown_pct_realized",
    "max_capital_in_use_pct",
    "max_loss_usdc",
], 12)}

## 四、5000 USDC 账户，小额延迟组合 Top

{table(top_5000_realistic, [
    "strategy_id",
    "exit_rule",
    "executed_trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "account_roi_pct",
    "max_drawdown_pct_realized",
    "max_capital_in_use_pct",
    "max_loss_usdc",
], 12)}

## 五、1000 USDC 账户，全组合 Top

这张表包含 0ms 和 500 USDC 大单，所以只能作为探索参考，不能直接当小额实盘候选。

{table(top_all_1000, [
    "strategy_id",
    "exit_rule",
    "candidate_class",
    "executed_trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "account_roi_pct",
    "max_drawdown_pct_realized",
    "max_capital_in_use_pct",
], 12)}

## 六、较稳小额候选

筛选条件：

- 账户本金 `1000 USDC`
- 单笔 `50 / 100 USDC`
- 延迟 `250 / 500 / 1000ms`
- 至少 10 笔交易
- 没有因为现金不足跳过
- 实现回撤不超过 50%
- 最大资金占用不超过 60%

{table(stable_small, [
    "strategy_id",
    "exit_rule",
    "executed_trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "account_roi_pct",
    "max_drawdown_pct_realized",
    "max_capital_in_use_pct",
], 12)}

## 七、止盈止损小额候选

下面只看非持有到结算的退出规则。它们收益低于持有到结算 Top 组合，但回撤也更温和，后续可以重点检查是否更适合真钱执行。

{table(non_hold_small, [
    "strategy_id",
    "exit_rule",
    "executed_trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "account_roi_pct",
    "max_drawdown_pct_realized",
    "max_capital_in_use_pct",
], 12)}

## 八、关键风险

- 这仍然是 REST 轮询数据，不是 WebSocket 毫秒级盘口。
- `250ms / 500ms / 1000ms` 在 1秒采样下可能落到同一下一次快照，所以延迟结果只能当保守近似。
- 当前胜负仍由 Chainlink 价格推算，不是官方结算字段。
- 这只是 24小时样本，不能证明长期稳定。
- 当前回撤是基于已实现盈亏，不是逐秒盯市净值。真实盘中回撤可能更大。
- 如果下一步真钱测试，只能从 `50 / 100 USDC` 小额组合开始。

## 九、输出文件

- 账户级汇总表：`{ACCOUNT_BACKTEST_CSV}`
- 账户级逐笔表：`{ACCOUNT_TRADES_CSV}`
"""


def main() -> int:
    trades = read_trades()
    grouped: dict[tuple[str, str], list[Trade]] = defaultdict(list)
    for trade in trades:
        grouped[(trade.strategy_id, trade.exit_rule)].append(trade)

    summary_rows: list[dict[str, Any]] = []
    executed_trade_rows: list[dict[str, Any]] = []
    for strategy_trades in grouped.values():
        for capital in ACCOUNT_CAPITALS:
            summary, executed = simulate_account(strategy_trades, capital)
            summary_rows.append(summary)
            executed_trade_rows.extend(executed)

    summary_rows.sort(
        key=lambda row: (
            row["account_capital"],
            -float(row["account_roi_pct"]),
            -int(row["executed_trade_count"]),
        )
    )
    write_csv(ACCOUNT_BACKTEST_CSV, summary_rows)
    write_csv(ACCOUNT_TRADES_CSV, executed_trade_rows)
    REPORT_MD.write_text(build_report(summary_rows), encoding="utf-8")

    print(f"report={REPORT_MD}")
    print(f"summary={ACCOUNT_BACKTEST_CSV}")
    print(f"trades={ACCOUNT_TRADES_CSV}")
    print(f"strategies={len(grouped)}")
    print(f"account_rows={len(summary_rows)}")
    print(f"executed_trade_rows={len(executed_trade_rows)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
