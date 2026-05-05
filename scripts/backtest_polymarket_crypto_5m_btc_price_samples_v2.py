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
DEFAULT_INPUT = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v1.csv"
SUMMARY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v2_early_entry_summary.csv"
TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v2_early_entry_trades.csv"
REPORT_MD = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PRICE_SAMPLES_EARLY_ENTRY_BACKTEST_V2_CN.md"

THRESHOLDS_USD = [25.0, 35.0, 50.0]
ENTRY_STARTS = [15.0, 30.0, 60.0]
ENTRY_END_SECONDS = 180.0
ENTRY_CAPS = [0.65, 0.70, 0.75]
ORDER_CASH_USDC = 100.0
DEFAULT_FEE_RATE = 0.072
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


def direction_for(row: dict[str, str], threshold_usd: float) -> tuple[str, float, float] | None:
    binance = as_float(row.get("binance_btcusdt"))
    coinbase = as_float(row.get("coinbase_btcusd"))
    price_to_beat = as_float(row.get("polymarket_price_to_beat"))
    if binance is None or coinbase is None or price_to_beat is None:
        return None
    binance_delta = binance - price_to_beat
    coinbase_delta = coinbase - price_to_beat
    if binance_delta >= threshold_usd and coinbase_delta >= threshold_usd:
        return "UP", binance_delta, coinbase_delta
    if binance_delta <= -threshold_usd and coinbase_delta <= -threshold_usd:
        return "DOWN", binance_delta, coinbase_delta
    return None


def direction_price(row: dict[str, str], direction: str) -> float | None:
    if direction == "UP":
        return as_float(row.get("polymarket_up_price"))
    if direction == "DOWN":
        return as_float(row.get("polymarket_down_price"))
    return None


def taker_fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1 - price)


def fee_rate_for(row: dict[str, str]) -> float:
    return as_float(row.get("polymarket_fee_rate")) or DEFAULT_FEE_RATE


def rows_by_slug(rows: list[dict[str, str]]) -> dict[str, list[dict[str, str]]]:
    grouped: dict[str, list[dict[str, str]]] = {}
    for row in rows:
        slug = row.get("polymarket_market_slug", "")
        if slug:
            grouped.setdefault(slug, []).append(row)
    return grouped


def final_outcome(part: list[dict[str, str]]) -> tuple[str, float] | None:
    last_valid: dict[str, str] | None = None
    for row in part:
        if (
            as_float(row.get("chainlink_btcusd")) is not None
            and as_float(row.get("polymarket_price_to_beat")) is not None
        ):
            last_valid = row
    if last_valid is None:
        return None
    chainlink = as_float(last_valid.get("chainlink_btcusd"))
    price_to_beat = as_float(last_valid.get("polymarket_price_to_beat"))
    if chainlink is None or price_to_beat is None:
        return None
    delta = chainlink - price_to_beat
    return ("UP" if delta >= 0 else "DOWN"), delta


def is_clean_window(part: list[dict[str, str]]) -> bool:
    seconds = [
        as_float(row.get("polymarket_seconds_since_start"))
        for row in part
        if as_float(row.get("polymarket_seconds_since_start")) is not None
    ]
    if len(seconds) < 100:
        return False
    return min(seconds) <= 15 and max(seconds) >= 285


def find_entry(
    part: list[dict[str, str]],
    threshold_usd: float,
    entry_start: float,
    entry_cap: float,
) -> tuple[dict[str, str], str, float, float] | None:
    for row in part:
        second = as_float(row.get("polymarket_seconds_since_start"))
        if second is None or second < entry_start or second > ENTRY_END_SECONDS:
            continue
        signal = direction_for(row, threshold_usd)
        if signal is None:
            continue
        direction, binance_delta, coinbase_delta = signal
        price = direction_price(row, direction)
        if price is None or price > entry_cap:
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
    price = direction_price(row, direction)
    price_to_beat = as_float(row.get("polymarket_price_to_beat"))
    chainlink = as_float(row.get("chainlink_btcusd"))
    second = as_float(row.get("polymarket_seconds_since_start"))
    if price is None or price_to_beat is None or chainlink is None or second is None:
        return None
    fee = taker_fee_per_share(price, fee_rate_for(row))
    shares = ORDER_CASH_USDC / (price + fee)
    buy_fee = shares * fee
    payoff = shares if direction == final_direction else 0.0
    pnl = payoff - ORDER_CASH_USDC
    return Trade(
        rule_name="hold_to_resolution_current_price_proxy",
        threshold_usd=threshold_usd,
        entry_start_seconds=entry_start,
        entry_end_seconds=ENTRY_END_SECONDS,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=row.get("sampled_at_utc", ""),
        entry_second=second,
        entry_price=price,
        shares=shares,
        buy_fee_usdc=buy_fee,
        entry_total_cash=ORDER_CASH_USDC,
        exit_type="RESOLUTION",
        exit_time_utc=part[-1].get("sampled_at_utc", ""),
        exit_second=as_float(part[-1].get("polymarket_seconds_since_start")) or 0.0,
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
    price = direction_price(row, direction)
    price_to_beat = as_float(row.get("polymarket_price_to_beat"))
    chainlink = as_float(row.get("chainlink_btcusd"))
    second = as_float(row.get("polymarket_seconds_since_start"))
    if price is None or price_to_beat is None or chainlink is None or second is None:
        return None
    fee_rate = fee_rate_for(row)
    fee = taker_fee_per_share(price, fee_rate)
    shares = ORDER_CASH_USDC / (price + fee)
    buy_fee = shares * fee
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
        current_price = direction_price(monitor_row, direction)
        if current_price is None:
            continue
        if current_price >= PROFIT_TARGET:
            exit_type = "PROFIT_TARGET"
            exit_row = monitor_row
            exit_price = current_price
            sell_fee = shares * taker_fee_per_share(exit_price, fee_rate)
            exit_cash = shares * exit_price - sell_fee
            break
        if current_price <= STOP_LOSS:
            exit_type = "STOP_LOSS"
            exit_row = monitor_row
            exit_price = current_price
            sell_fee = shares * taker_fee_per_share(exit_price, fee_rate)
            exit_cash = shares * exit_price - sell_fee
            break

    pnl = exit_cash - ORDER_CASH_USDC
    return Trade(
        rule_name="profit_075_stop_035_current_price_proxy",
        threshold_usd=threshold_usd,
        entry_start_seconds=entry_start,
        entry_end_seconds=ENTRY_END_SECONDS,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=row.get("sampled_at_utc", ""),
        entry_second=second,
        entry_price=price,
        shares=shares,
        buy_fee_usdc=buy_fee,
        entry_total_cash=ORDER_CASH_USDC,
        exit_type=exit_type,
        exit_time_utc=exit_row.get("sampled_at_utc", ""),
        exit_second=as_float(exit_row.get("polymarket_seconds_since_start")) or 0.0,
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
        grouped.setdefault(
            (trade.rule_name, trade.threshold_usd, trade.entry_start_seconds, trade.entry_cap),
            [],
        ).append(trade)

    rows: list[dict[str, Any]] = []
    for (rule_name, threshold, entry_start, cap), part in grouped.items():
        pnls = [trade.pnl_usdc for trade in part]
        rows.append(
            {
                "rule_name": rule_name,
                "threshold_usd": threshold,
                "entry_start_seconds": entry_start,
                "entry_end_seconds": ENTRY_END_SECONDS,
                "entry_cap": cap,
                "trades": len(part),
                "win_rate": sum(1 for trade in part if trade.pnl_usdc > 0) / len(part),
                "total_pnl_usdc": sum(pnls),
                "avg_pnl_usdc": statistics.mean(pnls),
                "median_pnl_usdc": statistics.median(pnls),
                "min_pnl_usdc": min(pnls),
                "max_pnl_usdc": max(pnls),
                "avg_entry_price": statistics.mean([trade.entry_price for trade in part]),
                "up_trades": sum(1 for trade in part if trade.direction == "UP"),
                "down_trades": sum(1 for trade in part if trade.direction == "DOWN"),
                "profit_target_share": sum(1 for trade in part if trade.exit_type == "PROFIT_TARGET") / len(part),
                "stop_loss_share": sum(1 for trade in part if trade.exit_type == "STOP_LOSS") / len(part),
                "resolution_share": sum(1 for trade in part if trade.exit_type == "RESOLUTION") / len(part),
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


def build_report(summary_rows: list[dict[str, Any]], clean_windows: int, rows_count: int) -> str:
    hold = [row for row in summary_rows if row["rule_name"] == "hold_to_resolution_current_price_proxy"][:10]
    exit_rule = [row for row in summary_rows if row["rule_name"] == "profit_075_stop_035_current_price_proxy"][:10]

    def table(rows: list[dict[str, Any]]) -> str:
        lines = [
            "| 阈值 | 入场开始 | 入场价上限 | 交易数 | 胜率 | 总收益 | 平均每笔 | 平均入场价 |",
            "|---:|---:|---:|---:|---:|---:|---:|---:|",
        ]
        for row in rows:
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

    return f"""# Polymarket BTC 5分钟第一版采集数据提前入场回测 v2

## 一、能不能用第一版数据

可以用，而且这一批数据覆盖更长：

- 原始样本：`{rows_count}` 条
- 干净 5 分钟窗口：`{clean_windows}` 个
- 覆盖时长：约 `42` 小时

但它只能做“价格信号回测”，不能做完整盘口深度回测。原因是第一版数据有 Up/Down 当前价格、市场 best bid/ask、价格源和目标价，但没有保存 Up/Down 的完整订单簿深度。

## 二、这次测试内容

这次把入场开始提前到 `15 / 30 / 60` 秒，并行测试 `$25 / $35 / $50` 三组阈值，以及 `0.65 / 0.70 / 0.75` 三个入场价上限。

每笔按 `100 USDC` 估算。

## 三、持有到结算 Top 组合

{table(hold)}

## 四、0.75止盈/0.35止损 Top 组合

{table(exit_rule)}

## 五、解释

第一版数据给出的结果明显比 24 小时 CLOB paper run 乐观，说明它适合寻找方向和参数，但不能直接当成实盘收益。

如果两个数据源放在一起看，比较稳的结论是：

- `$50` 阈值方向质量高，但实时可买价格经常已经太贵。
- `$25-$35` 阈值更适合做早期信号研究。
- 入场开始必须提前到 `15-30` 秒。
- 下一轮实时采集必须保存完整订单簿深度，否则无法判断 `100 USDC` 真实成交能力。

## 六、输出文件

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
    clean_grouped = {
        slug: sorted(part, key=lambda row: parse_time(row.get("sampled_at_utc", "")) or datetime.min)
        for slug, part in grouped.items()
        if is_clean_window(part)
    }
    trades: list[Trade] = []
    for slug, part in clean_grouped.items():
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
    REPORT_MD.write_text(build_report(summary_rows, len(clean_grouped), len(rows)), encoding="utf-8")
    print(f"summary={SUMMARY_CSV}")
    print(f"trades={TRADES_CSV}")
    print(f"report={REPORT_MD}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
