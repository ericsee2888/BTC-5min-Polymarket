#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv"
DEFAULT_EVENT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_trades_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PAPER_TRADING_DETAILED_BACKTEST_REPORT_V1_CN.md"


def read_csv(path: Path) -> list[dict[str, str]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def read_events(path: Path) -> list[dict[str, Any]]:
    if not path.exists() or path.stat().st_size == 0:
        return []
    events: list[dict[str, Any]] = []
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            try:
                events.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return events


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def fmt_money(value: float | None) -> str:
    return "NA" if value is None else f"{value:,.2f}"


def fmt_pct(value: float | None) -> str:
    return "NA" if value is None else f"{value:.2%}"


def fmt_num(value: float | None, digits: int = 4) -> str:
    return "NA" if value is None else f"{value:.{digits}f}"


def median(values: list[float]) -> float | None:
    return statistics.median(values) if values else None


def mean(values: list[float]) -> float | None:
    return statistics.mean(values) if values else None


def parse_time(value: str) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def time_span(rows: list[dict[str, str]], key: str) -> tuple[str, str, float | None]:
    times = [parse_time(row.get(key, "")) for row in rows]
    clean = [item for item in times if item is not None]
    if not clean:
        return "NA", "NA", None
    start = min(clean)
    end = max(clean)
    hours = (end - start).total_seconds() / 3600
    return start.isoformat(), end.isoformat(), hours


def max_drawdown(pnls: list[float]) -> float:
    equity = 0.0
    peak = 0.0
    worst = 0.0
    for pnl in pnls:
        equity += pnl
        peak = max(peak, equity)
        worst = min(worst, equity - peak)
    return worst


def build_signal_table(snapshots: list[dict[str, str]]) -> str:
    reasons = Counter(row.get("signal_reason", "") or "blank" for row in snapshots)
    lines = ["| 信号状态 | 快照数 | 占比 |", "|---|---:|---:|"]
    total = sum(reasons.values()) or 1
    for reason, count in reasons.most_common():
        lines.append(f"| {reason} | {count} | {count / total:.2%} |")
    return "\n".join(lines)


def build_trade_table(trades: list[dict[str, str]]) -> str:
    if not trades:
        return "暂无已完成模拟交易。"
    lines = [
        "| 时间 | 方向 | 退出 | 投入 | 收益 | ROI | 结果 |",
        "|---|---|---|---:|---:|---:|---|",
    ]
    for row in trades[-20:]:
        lines.append(
            "| {time} | {direction} | {exit_type} | {cash} | {pnl} | {roi} | {correct} |".format(
                time=row.get("entry_time_utc", "")[:19],
                direction=row.get("direction", ""),
                exit_type=row.get("exit_type", ""),
                cash=fmt_money(as_float(row.get("entry_total_cash"))),
                pnl=fmt_money(as_float(row.get("pnl_usdc"))),
                roi=fmt_pct(as_float(row.get("roi_on_cash"))),
                correct=row.get("correct", ""),
            )
        )
    return "\n".join(lines)


def build_report(args: argparse.Namespace) -> str:
    snapshots = read_csv(args.snapshot_csv)
    trades = read_csv(args.trades_csv)
    events = read_events(args.event_jsonl)

    snap_start, snap_end, snap_hours = time_span(snapshots, "sampled_at_utc")
    trade_start, trade_end, _trade_hours = time_span(trades, "entry_time_utc")
    unique_windows = len({row.get("slug", "") for row in snapshots if row.get("slug")})
    entered_events = [event for event in events if event.get("event_type") == "entry_signal"]
    filled_events = [event for event in events if event.get("event_type") == "entry_filled"]

    pnls = [as_float(row.get("pnl_usdc")) for row in trades]
    pnls_clean = [value for value in pnls if value is not None]
    rois = [as_float(row.get("roi_on_cash")) for row in trades]
    rois_clean = [value for value in rois if value is not None]
    wins = [value for value in pnls_clean if value > 0]
    losses = [value for value in pnls_clean if value < 0]
    exit_types = Counter(row.get("exit_type", "") or "unknown" for row in trades)
    directions = Counter(row.get("direction", "") or "unknown" for row in trades)
    final_outcomes = Counter(row.get("final_outcome", "") or "unknown" for row in trades)
    signal_directions = Counter(row.get("signal_direction", "") or "blank" for row in snapshots)
    ptb_status = Counter(row.get("price_to_beat_status", "") or "blank" for row in snapshots)

    total_pnl = sum(pnls_clean)
    total_cash = sum(as_float(row.get("entry_total_cash")) or 0.0 for row in trades)
    avg_cash = mean([as_float(row.get("entry_total_cash")) or 0.0 for row in trades]) if trades else None
    per_day_pnl = total_pnl / (snap_hours / 24) if snap_hours and snap_hours > 0 else None

    last_snapshot = snapshots[-1] if snapshots else {}
    open_position = bool(last_snapshot.get("open_position_slug"))

    exit_lines = "\n".join(
        f"- {name}: `{count}` 笔" for name, count in exit_types.most_common()
    ) or "- 暂无"
    direction_lines = "\n".join(
        f"- {name}: `{count}` 笔" for name, count in directions.most_common()
    ) or "- 暂无"
    ptb_lines = "\n".join(
        f"- {name}: `{count}` 条" for name, count in ptb_status.most_common()
    ) or "- 暂无"

    return f"""# Polymarket BTC 5分钟实时 Paper Trading 详细回测报告 v1

## 一、运行范围

- 快照开始：`{snap_start}`
- 快照结束：`{snap_end}`
- 覆盖时长：`{fmt_num(snap_hours, 2)}` 小时
- 快照条数：`{len(snapshots)}`
- 覆盖 5 分钟窗口：`{unique_windows}` 个
- 已完成模拟交易：`{len(trades)}` 笔
- 交易开始：`{trade_start}`
- 交易结束：`{trade_end}`
- 当前是否还有未结算模拟仓位：`{"是" if open_position else "否"}`

本报告基于实时 paper trading 文件自动生成，不包含真钱订单。

## 二、核心结果

- 合计模拟投入：`{fmt_money(total_cash)} USDC`
- 平均每笔投入：`{fmt_money(avg_cash)} USDC`
- 合计模拟收益：`{fmt_money(total_pnl)} USDC`
- 折算日收益：`{fmt_money(per_day_pnl)} USDC/天`
- 胜率：`{fmt_pct(len(wins) / len(trades) if trades else None)}`
- 平均单笔收益：`{fmt_money(mean(pnls_clean))} USDC`
- 中位单笔收益：`{fmt_money(median(pnls_clean))} USDC`
- 最大单笔盈利：`{fmt_money(max(pnls_clean) if pnls_clean else None)} USDC`
- 最大单笔亏损：`{fmt_money(min(pnls_clean) if pnls_clean else None)} USDC`
- 最大回撤：`{fmt_money(max_drawdown(pnls_clean))} USDC`
- 平均 ROI：`{fmt_pct(mean(rois_clean))}`
- 中位 ROI：`{fmt_pct(median(rois_clean))}`

## 三、信号与执行

目标价捕捉状态：

{ptb_lines}

快照中的信号方向分布：

- UP: `{signal_directions.get("UP", 0)}` 条
- DOWN: `{signal_directions.get("DOWN", 0)}` 条
- 空信号/未触发: `{signal_directions.get("blank", 0)}` 条

入场信号事件：

- 触发入场信号：`{len(entered_events)}` 次
- 完整模拟成交：`{len(filled_events)}` 次

信号状态明细：

{build_signal_table(snapshots)}

## 四、交易结构

方向分布：

{direction_lines}

退出方式：

{exit_lines}

最终结果分布：

{chr(10).join(f"- {name}: `{count}` 笔" for name, count in final_outcomes.most_common()) or "- 暂无"}

## 五、最近交易明细

{build_trade_table(trades)}

## 六、初步解释

如果交易数足够多，优先看三个问题：第一，`100 USDC` 是否经常能完整成交；第二，盈利交易是否主要来自结算还是提前止盈；第三，亏损是否集中发生在特定市场阶段。如果交易数偏少，说明 `$50` 阈值在这段时间不常触发，下一步应同时比较 `$25` 和 `$75` 阈值，而不是急着调整金额。

如果收益仍然很好，下一步不是直接上大钱，而是跑极小真钱验证，金额从 `10-20 USDC` 开始，主要验证真实成交、延迟、手续费和退出体验。

## 七、文件索引

- 快照文件：`{args.snapshot_csv}`
- 事件文件：`{args.event_jsonl}`
- 交易文件：`{args.trades_csv}`
- 报告文件：`{args.report}`
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--event-jsonl", type=Path, default=DEFAULT_EVENT_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    report = build_report(args)
    args.report.write_text(report, encoding="utf-8")
    print(f"report={args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
