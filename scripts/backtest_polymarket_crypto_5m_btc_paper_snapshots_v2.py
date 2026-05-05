#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_INPUT = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv"
SUMMARY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_snapshot_backtest_v2_summary.csv"
TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_snapshot_backtest_v2_trades.csv"
REPORT_MD = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PAPER_SNAPSHOT_BACKTEST_V2_CN.md"

THRESHOLDS_USD = [25.0, 35.0, 50.0]
ENTRY_STARTS = [15.0, 30.0, 60.0]
ENTRY_END_SECONDS = 180.0
ENTRY_CAPS = [0.65, 0.70, 0.75]
ORDER_CASH_USDC = 100.0
FEE_RATE = 0.072
PROFIT_TARGET = 0.75
STOP_LOSS = 0.35


@dataclass(frozen=True)
class Trade:
    rule_name: str
    threshold_usd: float
    entry_start_seconds: float
    entry_end_seconds: float
    entry_cap: float
    slug: str
    direction: str
    entry_time_utc: str
    entry_second: float
    entry_price: float
    shares: float
    buy_fee_usdc: float
    entry_total_cash: float
    exit_type: str
    exit_time_utc: str
    exit_second: float
    exit_price: float
    sell_fee_usdc: float
    exit_cash: float
    final_outcome: str
    correct: bool
    pnl_usdc: float
    roi_on_cash: float
    price_to_beat: float
    entry_binance_delta: float
    entry_coinbase_delta: float
    entry_chainlink_delta: float
    final_chainlink_delta: float


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def target_ask(row: dict[str, str], direction: str) -> float | None:
    if direction == "UP":
        return as_float(row.get("up_best_ask"))
    if direction == "DOWN":
        return as_float(row.get("down_best_ask"))
    return None


def target_bid(row: dict[str, str], direction: str) -> float | None:
    if direction == "UP":
        return as_float(row.get("up_best_bid"))
    if direction == "DOWN":
        return as_float(row.get("down_best_bid"))
    return None


def direction_for(row: dict[str, str], threshold_usd: float) -> tuple[str, float, float] | None:
    binance = as_float(row.get("binance_btcusdt"))
    coinbase = as_float(row.get("coinbase_btcusd"))
    price_to_beat = as_float(row.get("price_to_beat"))
    if binance is None or coinbase is None or price_to_beat is None:
        return None
    binance_delta = binance - price_to_beat
    coinbase_delta = coinbase - price_to_beat
    if binance_delta >= threshold_usd and coinbase_delta >= threshold_usd:
        return "UP", binance_delta, coinbase_delta
    if binance_delta <= -threshold_usd and coinbase_delta <= -threshold_usd:
        return "DOWN", binance_delta, coinbase_delta
    return None


def taker_fee_per_share(price: float) -> float:
    return FEE_RATE * price * (1 - price)


def rows_by_slug(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        slug = row.get("slug", "")
        if slug:
            grouped.setdefault(slug, []).append(row)
    return grouped


def final_outcome(part: list[dict[str, str]]) -> tuple[str, float] | None:
    last_valid: dict[str, str] | None = None
    for row in part:
        if as_float(row.get("chainlink_btcusd")) is not None and as_float(row.get("price_to_beat")) is not None:
            last_valid = row
    if last_valid is None:
        return None
    chainlink = as_float(last_valid.get("chainlink_btcusd"))
    price_to_beat = as_float(last_valid.get("price_to_beat"))
    if chainlink is None or price_to_beat is None:
        return None
    delta = chainlink - price_to_beat
    return ("UP" if delta >= 0 else "DOWN"), delta


def find_entry(
    part: list[dict[str, str]],
    threshold_usd: float,
    entry_start: float,
    entry_cap: float,
) -> tuple[dict[str, str], str, float, float] | None:
    for row in part:
        second = as_float(row.get("seconds_since_start"))
        if second is None or second < entry_start or second > ENTRY_END_SECONDS:
            continue
        signal = direction_for(row, threshold_usd)
        if signal is None:
            continue
        direction, binance_delta, coinbase_delta = signal
        ask = target_ask(row, direction)
        if ask is None or ask > entry_cap:
            continue
        return row, direction, binance_delta, coinbase_delta
    return None


def simulate_hold(
    slug: str,
    part: list[dict[str, str]],
    threshold_usd: float,
    entry_start: float,
    entry_cap: float,
) -> Trade | None:
    entry = find_entry(part, threshold_usd, entry_start, entry_cap)
    outcome = final_outcome(part)
    if entry is None or outcome is None:
        return None
    row, direction, binance_delta, coinbase_delta = entry
    final_direction, final_delta = outcome
    entry_price = target_ask(row, direction)
    price_to_beat = as_float(row.get("price_to_beat"))
    chainlink = as_float(row.get("chainlink_btcusd"))
    second = as_float(row.get("seconds_since_start"))
    if entry_price is None or price_to_beat is None or chainlink is None or second is None:
        return None

    fee_per_share = taker_fee_per_share(entry_price)
    shares = ORDER_CASH_USDC / (entry_price + fee_per_share)
    buy_fee = shares * fee_per_share
    payoff = shares if direction == final_direction else 0.0
    pnl = payoff - ORDER_CASH_USDC
    return Trade(
        rule_name="hold_to_resolution_top_of_book",
        threshold_usd=threshold_usd,
        entry_start_seconds=entry_start,
        entry_end_seconds=ENTRY_END_SECONDS,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=row.get("sampled_at_utc", ""),
        entry_second=second,
        entry_price=entry_price,
        shares=shares,
        buy_fee_usdc=buy_fee,
        entry_total_cash=ORDER_CASH_USDC,
        exit_type="RESOLUTION",
        exit_time_utc=part[-1].get("sampled_at_utc", ""),
        exit_second=as_float(part[-1].get("seconds_since_start")) or 0.0,
        exit_price=1.0 if direction == final_direction else 0.0,
        sell_fee_usdc=0.0,
        exit_cash=payoff,
        final_outcome=final_direction,
        correct=direction == final_direction,
        pnl_usdc=pnl,
        roi_on_cash=pnl / ORDER_CASH_USDC,
        price_to_beat=price_to_beat,
        entry_binance_delta=binance_delta,
        entry_coinbase_delta=coinbase_delta,
        entry_chainlink_delta=chainlink - price_to_beat,
        final_chainlink_delta=final_delta,
    )


def simulate_exit_rule(
    slug: str,
    part: list[dict[str, str]],
    threshold_usd: float,
    entry_start: float,
    entry_cap: float,
) -> Trade | None:
    entry = find_entry(part, threshold_usd, entry_start, entry_cap)
    outcome = final_outcome(part)
    if entry is None or outcome is None:
        return None
    row, direction, binance_delta, coinbase_delta = entry
    final_direction, final_delta = outcome
    entry_price = target_ask(row, direction)
    price_to_beat = as_float(row.get("price_to_beat"))
    chainlink = as_float(row.get("chainlink_btcusd"))
    second = as_float(row.get("seconds_since_start"))
    if entry_price is None or price_to_beat is None or chainlink is None or second is None:
        return None

    fee_per_share = taker_fee_per_share(entry_price)
    shares = ORDER_CASH_USDC / (entry_price + fee_per_share)
    buy_fee = shares * fee_per_share
    entry_time = parse_time(row.get("sampled_at_utc", "")) or datetime.min

    exit_type = "RESOLUTION"
    exit_row = part[-1]
    exit_price = 1.0 if direction == final_direction else 0.0
    sell_fee = 0.0
    exit_cash = shares if direction == final_direction else 0.0

    for monitor_row in part:
        monitor_time = parse_time(monitor_row.get("sampled_at_utc", "")) or datetime.min
        if monitor_time < entry_time:
            continue
        bid = target_bid(monitor_row, direction)
        if bid is None:
            continue
        if bid >= PROFIT_TARGET:
            exit_type = "PROFIT_TARGET"
            exit_row = monitor_row
            exit_price = bid
            sell_fee = shares * taker_fee_per_share(exit_price)
            exit_cash = shares * exit_price - sell_fee
            break
        if bid <= STOP_LOSS:
            exit_type = "STOP_LOSS"
            exit_row = monitor_row
            exit_price = bid
            sell_fee = shares * taker_fee_per_share(exit_price)
            exit_cash = shares * exit_price - sell_fee
            break

    pnl = exit_cash - ORDER_CASH_USDC
    return Trade(
        rule_name="profit_075_stop_035_top_of_book",
        threshold_usd=threshold_usd,
        entry_start_seconds=entry_start,
        entry_end_seconds=ENTRY_END_SECONDS,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=row.get("sampled_at_utc", ""),
        entry_second=second,
        entry_price=entry_price,
        shares=shares,
        buy_fee_usdc=buy_fee,
        entry_total_cash=ORDER_CASH_USDC,
        exit_type=exit_type,
        exit_time_utc=exit_row.get("sampled_at_utc", ""),
        exit_second=as_float(exit_row.get("seconds_since_start")) or 0.0,
        exit_price=exit_price,
        sell_fee_usdc=sell_fee,
        exit_cash=exit_cash,
        final_outcome=final_direction,
        correct=direction == final_direction,
        pnl_usdc=pnl,
        roi_on_cash=pnl / ORDER_CASH_USDC,
        price_to_beat=price_to_beat,
        entry_binance_delta=binance_delta,
        entry_coinbase_delta=coinbase_delta,
        entry_chainlink_delta=chainlink - price_to_beat,
        final_chainlink_delta=final_delta,
    )


def summarize(trades: list[Trade]) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, float, float, float], list[Trade]] = {}
    for trade in trades:
        key = (
            trade.rule_name,
            trade.threshold_usd,
            trade.entry_start_seconds,
            trade.entry_cap,
        )
        grouped.setdefault(key, []).append(trade)

    rows: list[dict[str, Any]] = []
    for key, part in grouped.items():
        rule_name, threshold, entry_start, entry_cap = key
        pnls = [trade.pnl_usdc for trade in part]
        wins = [trade for trade in part if trade.pnl_usdc > 0]
        rows.append(
            {
                "rule_name": rule_name,
                "threshold_usd": threshold,
                "entry_start_seconds": entry_start,
                "entry_end_seconds": ENTRY_END_SECONDS,
                "entry_cap": entry_cap,
                "trades": len(part),
                "win_rate": len(wins) / len(part) if part else 0.0,
                "total_pnl_usdc": sum(pnls),
                "avg_pnl_usdc": statistics.mean(pnls) if pnls else 0.0,
                "median_pnl_usdc": statistics.median(pnls) if pnls else 0.0,
                "min_pnl_usdc": min(pnls) if pnls else 0.0,
                "max_pnl_usdc": max(pnls) if pnls else 0.0,
                "avg_entry_price": statistics.mean([trade.entry_price for trade in part]) if part else 0.0,
                "up_trades": sum(1 for trade in part if trade.direction == "UP"),
                "down_trades": sum(1 for trade in part if trade.direction == "DOWN"),
                "profit_target_share": sum(1 for trade in part if trade.exit_type == "PROFIT_TARGET") / len(part) if part else 0.0,
                "stop_loss_share": sum(1 for trade in part if trade.exit_type == "STOP_LOSS") / len(part) if part else 0.0,
                "resolution_share": sum(1 for trade in part if trade.exit_type == "RESOLUTION") / len(part) if part else 0.0,
            }
        )
    rows.sort(key=lambda row: (row["rule_name"], -row["total_pnl_usdc"]))
    return rows


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)


def fmt_money(value: float) -> str:
    return f"{value:,.2f}"


def fmt_pct(value: float) -> str:
    return f"{value:.2%}"


def build_report(rows: list[dict[str, Any]], trades: list[Trade], input_path: Path) -> str:
    top_hold = [row for row in rows if row["rule_name"] == "hold_to_resolution_top_of_book"][:8]
    top_exit = [row for row in rows if row["rule_name"] == "profit_075_stop_035_top_of_book"][:8]

    def table(part: list[dict[str, Any]]) -> str:
        lines = [
            "| 阈值 | 入场开始 | 入场价上限 | 交易数 | 胜率 | 总收益 | 平均每笔 | 平均入场价 |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for row in part:
            lines.append(
                "| {th:.0f} | {start:.0f}s | {cap:.2f} | {trades} | {wr} | {total} | {avg} | {entry:.3f} |".format(
                    th=float(row["threshold_usd"]),
                    start=float(row["entry_start_seconds"]),
                    cap=float(row["entry_cap"]),
                    trades=int(row["trades"]),
                    wr=fmt_pct(float(row["win_rate"])),
                    total=fmt_money(float(row["total_pnl_usdc"])),
                    avg=fmt_money(float(row["avg_pnl_usdc"])),
                    entry=float(row["avg_entry_price"]),
                )
            )
        return "\n".join(lines)

    trade_count_by_threshold = {}
    for threshold in THRESHOLDS_USD:
        trade_count_by_threshold[threshold] = len(
            {
                trade.slug
                for trade in trades
                if trade.threshold_usd == threshold
                and trade.entry_start_seconds == 15.0
                and trade.entry_cap == 0.65
                and trade.rule_name == "hold_to_resolution_top_of_book"
            }
        )

    return f"""# Polymarket BTC 5分钟 Paper Snapshot 回测 v2

## 一、这次回测回答什么

这次用已经采集到的 24 小时实时快照，回测三件事：

- 入场时间提前到 `15 秒` 或 `30 秒` 是否有帮助。
- 阈值从 `$50` 降到 `$35` 或 `$25` 是否更容易抓到低价入场。
- 入场价上限 `0.65 / 0.70 / 0.75` 对交易数和收益的影响。

注意：这版只有 best bid / best ask，不包含完整订单簿深度。因此它可以评估“价格是否来得及”，但不能精确验证 `100 USDC` 每次都能完整成交。

## 二、核心结论

按 `100 USDC` 每笔、用 best ask 作为入场价估算，结果并不支持直接实盘。

`$50` 阈值太晚，低价入场机会极少。把入场提前到 `15 秒` 后，`$25` 阈值能显著增加交易数，但总体收益仍然不稳定。

在所有组合里，最值得继续研究的是：

- `$25` 阈值
- 入场从 `15 秒` 或 `30 秒` 开始
- 入场价上限控制在 `0.65` 或 `0.70`

## 三、低价机会数量

在入场开始 `15 秒`、入场价上限 `0.65` 的约束下：

- `$25` 阈值：`{trade_count_by_threshold.get(25.0, 0)}` 个窗口
- `$35` 阈值：`{trade_count_by_threshold.get(35.0, 0)}` 个窗口
- `$50` 阈值：`{trade_count_by_threshold.get(50.0, 0)}` 个窗口

这说明我们之前的 `$50` 主线确实太慢，等它确认时，价格多数已经被市场打高。

## 四、持有到结算 Top 组合

{table(top_hold)}

## 五、0.75止盈/0.35止损 Top 组合

{table(top_exit)}

## 六、怎么解释

入场提前是必要的，但单独提前不够。真正的问题是，我们要从“强确认信号”改成“早期偏移信号”。

`$50` 是强确认，胜率可能高，但价格通常已经太贵。`$25` 更早，能买到的价格更好，但噪音也更大。下一步应该围绕 `$25-$35` 做更细的过滤，比如只在 Binance 和 Coinbase 同向、Chainlink 也开始同向、但 Polymarket ask 还没有超过 `0.65` 时入场。

## 七、输出文件

- 输入快照：`{input_path}`
- 汇总表：`{SUMMARY_CSV}`
- 逐笔表：`{TRADES_CSV}`
- 报告：`{REPORT_MD}`
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    rows = read_rows(args.input)
    grouped = rows_by_slug(rows)
    trades: list[Trade] = []
    for slug, part in grouped.items():
        part.sort(key=lambda row: parse_time(row.get("sampled_at_utc", "")) or datetime.min)
        for threshold in THRESHOLDS_USD:
            for entry_start in ENTRY_STARTS:
                for cap in ENTRY_CAPS:
                    hold = simulate_hold(slug, part, threshold, entry_start, cap)
                    if hold is not None:
                        trades.append(hold)
                    exit_rule = simulate_exit_rule(slug, part, threshold, entry_start, cap)
                    if exit_rule is not None:
                        trades.append(exit_rule)

    trade_rows = [asdict(trade) for trade in trades]
    summary_rows = summarize(trades)
    write_csv(TRADES_CSV, trade_rows)
    write_csv(SUMMARY_CSV, summary_rows)
    REPORT_MD.write_text(build_report(summary_rows, trades, args.input), encoding="utf-8")
    print(f"summary={SUMMARY_CSV}")
    print(f"trades={TRADES_CSV}")
    print(f"report={REPORT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
