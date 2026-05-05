#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv"
DEFAULT_EVENTS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl"
DEFAULT_OUT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_run_posthoc_cap_sweep_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PAPER_RUN_POSTHOC_ANALYSIS_V1_CN.md"

FEE_RATE = 0.072
ORDER_CASH = 100.0
ENTRY_CAPS = [0.55, 0.60, 0.65, 0.70, 0.75, 0.80, 0.85, 0.90, 0.95, 0.99]


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def read_csv(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def taker_fee_per_share(price: float, fee_rate: float = FEE_RATE) -> float:
    return fee_rate * price * (1 - price)


def signal_ask(row: dict[str, str]) -> float | None:
    direction = row.get("signal_direction")
    if direction == "UP":
        return as_float(row.get("up_best_ask"))
    if direction == "DOWN":
        return as_float(row.get("down_best_ask"))
    return None


def final_by_slug(rows: list[dict[str, str]]) -> dict[str, tuple[float, float]]:
    final: dict[str, tuple[float, float]] = {}
    for row in rows:
        slug = row.get("slug", "")
        chainlink = as_float(row.get("chainlink_btcusd"))
        price_to_beat = as_float(row.get("price_to_beat"))
        if slug and chainlink is not None and price_to_beat is not None:
            final[slug] = (chainlink, price_to_beat)
    return final


def first_signal_by_slug(rows: list[dict[str, str]]) -> dict[str, dict[str, str]]:
    signals: dict[str, dict[str, str]] = {}
    for row in rows:
        slug = row.get("slug", "")
        if not slug or not row.get("signal_direction"):
            continue
        signals.setdefault(slug, row)
    return signals


def fmt_money(value: float | None) -> str:
    return "NA" if value is None else f"{value:,.2f}"


def fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2%}"


def quantile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    values = sorted(values)
    idx = min(len(values) - 1, max(0, round((len(values) - 1) * p)))
    return values[idx]


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--out-csv", type=Path, default=DEFAULT_OUT_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    rows = read_csv(args.snapshot_csv)
    first_signals = first_signal_by_slug(rows)
    final = final_by_slug(rows)
    signal_rows = list(first_signals.values())
    asks = [signal_ask(row) for row in signal_rows]
    asks_clean = [value for value in asks if value is not None]
    directions = Counter(row.get("signal_direction", "") for row in signal_rows)

    result_rows: list[dict[str, Any]] = []
    for cap in ENTRY_CAPS:
        trades: list[dict[str, Any]] = []
        for slug, row in first_signals.items():
            ask = signal_ask(row)
            direction = row.get("signal_direction", "")
            if ask is None or ask > cap or slug not in final:
                continue
            final_chainlink, price_to_beat = final[slug]
            final_outcome = "UP" if final_chainlink >= price_to_beat else "DOWN"
            fee = taker_fee_per_share(ask)
            cash_per_share = ask + fee
            shares = ORDER_CASH / cash_per_share
            payoff = shares if direction == final_outcome else 0.0
            pnl = payoff - ORDER_CASH
            trades.append(
                {
                    "cap": cap,
                    "slug": slug,
                    "direction": direction,
                    "entry_time_utc": row.get("sampled_at_utc", ""),
                    "entry_second": as_float(row.get("seconds_since_start")),
                    "entry_ask": ask,
                    "shares": shares,
                    "final_outcome": final_outcome,
                    "correct": direction == final_outcome,
                    "pnl_usdc": pnl,
                    "roi": pnl / ORDER_CASH,
                    "price_to_beat": price_to_beat,
                    "final_chainlink": final_chainlink,
                }
            )

        pnls = [trade["pnl_usdc"] for trade in trades]
        wins = [trade for trade in trades if trade["pnl_usdc"] > 0]
        result_rows.append(
            {
                "entry_cap": cap,
                "trades": len(trades),
                "win_rate": len(wins) / len(trades) if trades else "",
                "total_pnl_usdc": sum(pnls) if pnls else 0.0,
                "avg_pnl_usdc": statistics.mean(pnls) if pnls else "",
                "median_pnl_usdc": statistics.median(pnls) if pnls else "",
                "min_pnl_usdc": min(pnls) if pnls else "",
                "max_pnl_usdc": max(pnls) if pnls else "",
                "avg_entry_ask": statistics.mean([trade["entry_ask"] for trade in trades]) if trades else "",
                "up_trades": sum(1 for trade in trades if trade["direction"] == "UP"),
                "down_trades": sum(1 for trade in trades if trade["direction"] == "DOWN"),
            }
        )

    with args.out_csv.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(result_rows[0].keys()))
        writer.writeheader()
        writer.writerows(result_rows)

    start_times = [parse_time(row.get("sampled_at_utc", "")) for row in rows]
    clean_times = [item for item in start_times if item is not None]
    run_start = min(clean_times).isoformat() if clean_times else "NA"
    run_end = max(clean_times).isoformat() if clean_times else "NA"
    run_hours = ((max(clean_times) - min(clean_times)).total_seconds() / 3600) if clean_times else None

    cap_lines = ["| 入场价格上限 | 交易数 | 胜率 | 总收益 | 平均每笔 | 中位每笔 | 平均入场价 |", "|---:|---:|---:|---:|---:|---:|---:|"]
    for row in result_rows:
        cap_lines.append(
            "| {cap:.2f} | {trades} | {win_rate} | {total} | {avg} | {median} | {avg_entry} |".format(
                cap=float(row["entry_cap"]),
                trades=row["trades"],
                win_rate=fmt_pct(float(row["win_rate"])) if row["win_rate"] != "" else "NA",
                total=fmt_money(float(row["total_pnl_usdc"])),
                avg=fmt_money(float(row["avg_pnl_usdc"])) if row["avg_pnl_usdc"] != "" else "NA",
                median=fmt_money(float(row["median_pnl_usdc"])) if row["median_pnl_usdc"] != "" else "NA",
                avg_entry=f"{float(row['avg_entry_ask']):.3f}" if row["avg_entry_ask"] != "" else "NA",
            )
        )

    cap_counts = "\n".join(
        f"- ask <= `{cap:.2f}`: `{sum(1 for ask in asks_clean if ask <= cap)}` 个窗口"
        for cap in ENTRY_CAPS
    )

    report = f"""# Polymarket BTC 5分钟 Paper Run 事后诊断分析 v1

## 一、结论先说

这次 24 小时实时 paper trading 可以做回测，但主策略 `$50 阈值 + 100 USDC + 入场价不超过 0.65` 的结果是：**没有真实模拟成交。**

原因不是没有信号，而是信号出现时，Polymarket 盘口通常已经把目标方向价格推得很高。我们设定的 `0.65` 入场上限几乎等不到。

## 二、运行数据

- 快照开始：`{run_start}`
- 快照结束：`{run_end}`
- 覆盖时长：`{run_hours:.2f}` 小时
- 快照条数：`{len(rows)}`
- 覆盖 5 分钟窗口：`{len({row.get('slug', '') for row in rows if row.get('slug')})}` 个
- 出现 `$50` 信号的窗口：`{len(signal_rows)}` 个
- 信号方向：UP `{directions.get('UP', 0)}` 个，DOWN `{directions.get('DOWN', 0)}` 个

## 三、信号出现时的入场价格

按每个 5 分钟窗口的第一次 `$50` 信号看，目标方向 best ask：

- 样本数：`{len(asks_clean)}`
- 最低：`{min(asks_clean):.2f}`
- 25 分位：`{quantile(asks_clean, 0.25):.2f}`
- 中位数：`{statistics.median(asks_clean):.2f}`
- 75 分位：`{quantile(asks_clean, 0.75):.2f}`
- 最高：`{max(asks_clean):.2f}`

不同入场上限能等到的窗口数：

{cap_counts}

## 四、如果放宽入场价，会怎样

下面这张表是假设每个信号窗口只在第一次信号时买一次，每笔总投入 `100 USDC`，持有到结算。它不是正式实盘建议，只是用来诊断“放宽价格是否有意义”。

{chr(10).join(cap_lines)}

## 五、解释

这次结果说明一个关键问题：`$50` 价格偏离信号本身不是完全没用，但它在实时盘口里出现得太晚。等 Binance 和 Coinbase 都偏离目标价超过 `$50` 时，Polymarket 的 Up/Down 价格往往已经到 `0.85-0.95`，这个时候再买，即使方向胜率不错，赔率也太差。

所以，旧的历史回测收益看起来很好，是因为用了市场当前价格字段做近似，容易高估真实可买价格。实时 CLOB paper trading 说明：真正可执行策略不能只等 `$50` 阈值，还必须更早识别机会，或者改成盘口提前反应不足时才入场。

## 六、下一步建议

下一轮不要继续只跑 `$50` 阈值。建议并行测试三组：

- `$25` 阈值 + 入场价不超过 `0.60/0.65`
- `$35` 阈值 + 入场价不超过 `0.60/0.65`
- `$50` 阈值 + 只在目标方向 ask 仍低于 `0.70` 时允许入场

同时，paper trader 要增加“跳过原因统计”，特别记录：信号触发但 ask 超过上限、盘口深度不够、价格源缺失、目标价缺失。这样下一份报告会更像策略研发报告，而不是只有最终交易结果。

## 七、输出文件

- 原始快照：`{args.snapshot_csv}`
- 事后扫表：`{args.out_csv}`
- 本报告：`{args.report}`
"""
    args.report.write_text(report, encoding="utf-8")
    print(f"report={args.report}")
    print(f"csv={args.out_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
