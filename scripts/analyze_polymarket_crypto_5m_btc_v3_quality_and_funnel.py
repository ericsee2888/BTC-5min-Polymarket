#!/usr/bin/env python3
from __future__ import annotations

import csv
import json
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"

SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.csv"
SIGNALS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_signal_events_v3.jsonl"
SKIPS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_skip_events_v3.jsonl"
TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.csv"

QUALITY_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_V3_DATA_QUALITY_AND_OPPORTUNITY_FUNNEL_REPORT_CN.md"
WINDOW_QUALITY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_window_quality.csv"
FUNNEL_BY_STRATEGY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_opportunity_funnel_by_strategy.csv"
EXIT_BY_STRATEGY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_exit_outcome_by_strategy.csv"
SKIP_REASON_CSV = DATA_DIR / "polymarket_crypto_5m_btc_v3_skip_reason_summary.csv"

ENTRY_END_SECONDS = 180.0
ENTRY_STARTS = [15.0, 30.0, 60.0]
ENTRY_CAPS = [0.65, 0.70, 0.75]
ORDER_CASH_AMOUNTS = [50.0, 100.0, 250.0, 500.0]
ENTRY_LATENCIES_MS = [0, 250, 500, 1000]


def as_float(value: Any) -> float | None:
    if value in ("", None):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def as_bool(value: Any) -> bool:
    return str(value).strip().lower() in {"true", "1", "yes"}


def parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def iter_jsonl(path: Path):
    if not path.exists():
        return
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if line:
                yield json.loads(line)


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        return
    fieldnames = list(rows[0].keys())
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def pct(numerator: float, denominator: float) -> float:
    return round(numerator / denominator * 100, 4) if denominator else 0.0


def median(values: list[float]) -> float | None:
    return round(statistics.median(values), 6) if values else None


def mean(values: list[float]) -> float | None:
    return round(statistics.mean(values), 6) if values else None


@dataclass
class WindowStats:
    slug: str
    rows: int = 0
    usable_rows: int = 0
    up_book_success: int = 0
    down_book_success: int = 0
    seconds: list[float] = field(default_factory=list)
    price_statuses: Counter[str] = field(default_factory=Counter)


@dataclass
class StrategyFunnel:
    strategy_id: str
    threshold_usd: float
    entry_start_second: float
    entry_cap: float
    order_cash_usdc: float
    entry_latency_ms: int
    raw_signal_windows: int = 0
    in_entry_window_signal_windows: int = 0
    attempted_windows: set[str] = field(default_factory=set)
    successful_entry_windows: set[str] = field(default_factory=set)
    skipped_windows: set[str] = field(default_factory=set)
    skipped_by_reason: Counter[str] = field(default_factory=Counter)
    up_entries: int = 0
    down_entries: int = 0
    entry_prices: list[float] = field(default_factory=list)
    worst_entry_prices: list[float] = field(default_factory=list)
    signal_to_arrival_ask_changes: list[float] = field(default_factory=list)


def build_strategy_id(threshold: float, start: float, cap: float, cash: float, latency_ms: int) -> str:
    return f"thr{threshold:g}_start{start:g}_cap{cap:g}_cash{cash:g}_lat{latency_ms:g}ms"


def build_window_quality(snapshot_rows: list[dict[str, str]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    times = [parse_time(row["sampled_at_utc"]) for row in snapshot_rows if row.get("sampled_at_utc")]
    times.sort()
    gaps = [(b - a).total_seconds() for a, b in zip(times, times[1:])]

    windows: dict[str, WindowStats] = {}
    price_status_counts: Counter[str] = Counter()
    up_success = 0
    down_success = 0
    usable = 0
    for row in snapshot_rows:
        slug = row.get("current_slug") or ""
        if not slug:
            continue
        stats = windows.setdefault(slug, WindowStats(slug=slug))
        stats.rows += 1
        second = as_float(row.get("current_seconds_since_start"))
        if second is not None:
            stats.seconds.append(second)
        status = row.get("price_to_beat_status") or ""
        if status:
            stats.price_statuses[status] += 1
            price_status_counts[status] += 1
        row_usable = as_bool(row.get("snapshot_usable_for_formal_backtest"))
        if row_usable:
            stats.usable_rows += 1
            usable += 1
        if not row.get("up_book_error"):
            stats.up_book_success += 1
            up_success += 1
        if not row.get("down_book_error"):
            stats.down_book_success += 1
            down_success += 1

    window_rows: list[dict[str, Any]] = []
    clean_windows = 0
    for stats in windows.values():
        min_second = min(stats.seconds) if stats.seconds else None
        max_second = max(stats.seconds) if stats.seconds else None
        has_open = any(0 <= sec <= 15 for sec in stats.seconds)
        has_entry = any(15 <= sec <= 180 for sec in stats.seconds)
        has_end = any(270 <= sec <= 300 for sec in stats.seconds)
        enough_rows = stats.rows >= 100
        book_success = stats.up_book_success / stats.rows >= 0.95 and stats.down_book_success / stats.rows >= 0.95 if stats.rows else False
        clean = bool(has_open and has_entry and has_end and enough_rows and book_success)
        clean_windows += int(clean)
        window_rows.append(
            {
                "slug": stats.slug,
                "rows": stats.rows,
                "usable_rows": stats.usable_rows,
                "usable_pct": pct(stats.usable_rows, stats.rows),
                "min_second": round(min_second, 3) if min_second is not None else "",
                "max_second": round(max_second, 3) if max_second is not None else "",
                "has_0_15s": has_open,
                "has_15_180s": has_entry,
                "has_270_300s": has_end,
                "rows_gte_100": enough_rows,
                "up_book_success_pct": pct(stats.up_book_success, stats.rows),
                "down_book_success_pct": pct(stats.down_book_success, stats.rows),
                "clean_window": clean,
                "dominant_price_to_beat_status": stats.price_statuses.most_common(1)[0][0] if stats.price_statuses else "",
            }
        )
    window_rows.sort(key=lambda row: row["slug"])
    summary = {
        "snapshot_rows": len(snapshot_rows),
        "start_utc": times[0].isoformat() if times else "",
        "end_utc": times[-1].isoformat() if times else "",
        "coverage_hours": round((times[-1] - times[0]).total_seconds() / 3600, 4) if len(times) >= 2 else 0.0,
        "median_gap_sec": round(statistics.median(gaps), 4) if gaps else 0.0,
        "p95_gap_sec": round(sorted(gaps)[int(len(gaps) * 0.95)], 4) if gaps else 0.0,
        "max_gap_sec": round(max(gaps), 4) if gaps else 0.0,
        "windows": len(windows),
        "clean_windows": clean_windows,
        "usable_snapshots": usable,
        "usable_snapshot_pct": pct(usable, len(snapshot_rows)),
        "up_book_success_pct": pct(up_success, len(snapshot_rows)),
        "down_book_success_pct": pct(down_success, len(snapshot_rows)),
        "price_status_counts": dict(price_status_counts),
    }
    return window_rows, summary


def build_signal_indexes() -> tuple[Counter[float], dict[float, set[str]], dict[tuple[float, float], set[str]]]:
    signal_snapshot_counts: Counter[float] = Counter()
    signal_windows: dict[float, set[str]] = defaultdict(set)
    in_entry_windows: dict[tuple[float, float], set[str]] = defaultdict(set)
    for row in iter_jsonl(SIGNALS_JSONL):
        threshold = float(row["threshold_usd"])
        slug = str(row["slug"])
        second = as_float(row.get("seconds_since_start"))
        signal_snapshot_counts[threshold] += 1
        signal_windows[threshold].add(slug)
        if second is not None:
            for start in ENTRY_STARTS:
                if start <= second <= ENTRY_END_SECONDS:
                    in_entry_windows[(threshold, start)].add(slug)
    return signal_snapshot_counts, signal_windows, in_entry_windows


def build_funnel(
    signal_windows: dict[float, set[str]],
    in_entry_windows: dict[tuple[float, float], set[str]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    strategies: dict[str, StrategyFunnel] = {}

    for threshold, slugs in signal_windows.items():
        for start in ENTRY_STARTS:
            in_window_slugs = in_entry_windows.get((threshold, start), set())
            for cap in ENTRY_CAPS:
                for cash in ORDER_CASH_AMOUNTS:
                    for latency in ENTRY_LATENCIES_MS:
                        sid = build_strategy_id(threshold, start, cap, cash, latency)
                        strategies[sid] = StrategyFunnel(
                            strategy_id=sid,
                            threshold_usd=threshold,
                            entry_start_second=start,
                            entry_cap=cap,
                            order_cash_usdc=cash,
                            entry_latency_ms=latency,
                            raw_signal_windows=len(slugs),
                            in_entry_window_signal_windows=len(in_window_slugs),
                        )

    skip_reason_total: Counter[str] = Counter()
    skip_reason_by_strategy: Counter[tuple[str, str]] = Counter()
    for row in iter_jsonl(SKIPS_JSONL):
        sid = row["strategy_id"]
        strategy = strategies.get(sid)
        if not strategy:
            continue
        slug = row.get("skip_slug") or ""
        reason = row.get("skip_reason") or "unknown"
        strategy.attempted_windows.add(slug)
        strategy.skipped_windows.add(slug)
        strategy.skipped_by_reason[reason] += 1
        skip_reason_total[reason] += 1
        skip_reason_by_strategy[(sid, reason)] += 1

    exit_rows_by_strategy: dict[tuple[str, str], list[dict[str, str]]] = defaultdict(list)
    unique_entries_seen: set[tuple[str, str]] = set()
    for row in read_csv_rows(TRADES_CSV):
        sid = row["strategy_id"]
        strategy = strategies.get(sid)
        if not strategy:
            continue
        slug = row.get("slug") or ""
        strategy.attempted_windows.add(slug)
        entry_key = (sid, slug)
        if entry_key not in unique_entries_seen:
            unique_entries_seen.add(entry_key)
            strategy.successful_entry_windows.add(slug)
            direction = row.get("direction")
            if direction == "UP":
                strategy.up_entries += 1
            elif direction == "DOWN":
                strategy.down_entries += 1
            for source, target in [
                ("entry_avg_price", strategy.entry_prices),
                ("entry_worst_price", strategy.worst_entry_prices),
                ("signal_to_arrival_ask_change", strategy.signal_to_arrival_ask_changes),
            ]:
                value = as_float(row.get(source))
                if value is not None:
                    target.append(value)
        exit_rows_by_strategy[(sid, row["exit_rule"])].append(row)

    funnel_rows: list[dict[str, Any]] = []
    for strategy in strategies.values():
        attempted = len(strategy.attempted_windows)
        success = len(strategy.successful_entry_windows)
        skipped = len(strategy.skipped_windows)
        latency_fail = (
            strategy.skipped_by_reason["latency_moved_ask_above_cap"]
            + strategy.skipped_by_reason["latency_depth_disappeared"]
            + strategy.skipped_by_reason["latency_direction_changed"]
        )
        funnel_rows.append(
            {
                "strategy_id": strategy.strategy_id,
                "threshold_usd": strategy.threshold_usd,
                "entry_start_second": strategy.entry_start_second,
                "entry_cap": strategy.entry_cap,
                "order_cash_usdc": strategy.order_cash_usdc,
                "entry_latency_ms": strategy.entry_latency_ms,
                "raw_signal_windows": strategy.raw_signal_windows,
                "in_entry_window_signal_windows": strategy.in_entry_window_signal_windows,
                "attempted_windows": attempted,
                "skipped_windows": skipped,
                "successful_entry_windows": success,
                "entry_success_pct_of_attempts": pct(success, attempted),
                "entry_success_pct_of_entry_signal_windows": pct(success, strategy.in_entry_window_signal_windows),
                "target_ask_above_cap": strategy.skipped_by_reason["target_ask_above_cap"],
                "latency_fail_count": latency_fail,
                "latency_moved_ask_above_cap": strategy.skipped_by_reason["latency_moved_ask_above_cap"],
                "latency_depth_disappeared": strategy.skipped_by_reason["latency_depth_disappeared"],
                "latency_direction_changed": strategy.skipped_by_reason["latency_direction_changed"],
                "insufficient_depth_count": sum(
                    count
                    for reason, count in strategy.skipped_by_reason.items()
                    if reason.startswith("insufficient_depth_for_")
                ),
                "missing_orderbook": strategy.skipped_by_reason["missing_orderbook"],
                "late_price_to_beat": strategy.skipped_by_reason["late_price_to_beat"],
                "avg_entry_price": mean(strategy.entry_prices),
                "avg_worst_entry_price": mean(strategy.worst_entry_prices),
                "median_signal_to_arrival_ask_change": median(strategy.signal_to_arrival_ask_changes),
                "up_entry_windows": strategy.up_entries,
                "down_entry_windows": strategy.down_entries,
            }
        )
    funnel_rows.sort(
        key=lambda row: (
            -row["successful_entry_windows"],
            -row["entry_success_pct_of_attempts"],
            row["threshold_usd"],
            row["entry_cap"],
            row["order_cash_usdc"],
            row["entry_latency_ms"],
        )
    )

    exit_rows: list[dict[str, Any]] = []
    for (sid, exit_rule), rows in exit_rows_by_strategy.items():
        first = rows[0]
        pnls = [as_float(row.get("pnl_usdc")) or 0.0 for row in rows]
        rois = [as_float(row.get("roi_on_cash")) or 0.0 for row in rows]
        exit_types = Counter(row.get("exit_type") or "" for row in rows)
        partial_count = sum(1 for row in rows if as_bool(row.get("exit_partial")))
        complete_count = sum(1 for row in rows if as_bool(row.get("exit_complete")))
        wins = sum(1 for pnl in pnls if pnl > 0)
        exit_rows.append(
            {
                "strategy_id": sid,
                "exit_rule": exit_rule,
                "threshold_usd": as_float(first.get("threshold_usd")),
                "entry_start_second": as_float(first.get("entry_start_second")),
                "entry_cap": as_float(first.get("entry_cap")),
                "order_cash_usdc": as_float(first.get("order_cash_usdc")),
                "entry_latency_ms": int(as_float(first.get("entry_latency_ms")) or 0),
                "trade_count": len(rows),
                "win_rate_pct": pct(wins, len(rows)),
                "total_pnl_usdc": round(sum(pnls), 6),
                "avg_pnl_usdc": round(statistics.mean(pnls), 6) if pnls else 0.0,
                "median_pnl_usdc": median(pnls),
                "avg_roi_pct": round((statistics.mean(rois) * 100), 4) if rois else 0.0,
                "median_roi_pct": round((statistics.median(rois) * 100), 4) if rois else 0.0,
                "max_loss_usdc": round(min(pnls), 6) if pnls else 0.0,
                "max_win_usdc": round(max(pnls), 6) if pnls else 0.0,
                "complete_exit_count": complete_count,
                "partial_exit_count": partial_count,
                "profit_target_count": exit_types["PROFIT_TARGET"],
                "stop_loss_count": exit_types["STOP_LOSS"],
                "resolution_count": exit_types["RESOLUTION"],
            }
        )
    exit_rows.sort(key=lambda row: (row["exit_rule"], -row["total_pnl_usdc"]))

    skip_rows = [
        {"skip_reason": reason, "count": count, "share_pct": pct(count, sum(skip_reason_total.values()))}
        for reason, count in skip_reason_total.most_common()
    ]

    summary = {
        "strategies": len(strategies),
        "unique_successful_entries": len(unique_entries_seen),
        "exit_trade_rows": sum(len(rows) for rows in exit_rows_by_strategy.values()),
        "skip_reason_total": dict(skip_reason_total),
    }
    return funnel_rows, exit_rows, skip_rows, summary


def top_table(rows: list[dict[str, Any]], columns: list[str], limit: int = 8) -> str:
    if not rows:
        return "无"
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    lines = [header, sep]
    for row in rows[:limit]:
        lines.append("| " + " | ".join(str(row.get(col, "")) for col in columns) + " |")
    return "\n".join(lines)


def build_report(
    quality: dict[str, Any],
    funnel_rows: list[dict[str, Any]],
    exit_rows: list[dict[str, Any]],
    skip_rows: list[dict[str, Any]],
    funnel_summary: dict[str, Any],
) -> str:
    hold_rows = [row for row in exit_rows if row["exit_rule"] == "hold_to_resolution"]
    conservative_rows = [
        row
        for row in exit_rows
        if row["entry_latency_ms"] in {250, 500, 1000}
        and row["order_cash_usdc"] in {50.0, 100.0}
        and row["exit_rule"] == "hold_to_resolution"
    ]
    return f"""# Polymarket BTC 5分钟 V3 数据质量验收与机会漏斗报告

## 一、结论

第三轮完整订单簿数据已经达到进入回测分析的最低要求。

这批数据覆盖 `{quality["coverage_hours"]}` 小时、`{quality["windows"]}` 个 5分钟窗口，快照中位间隔 `{quality["median_gap_sec"]}` 秒，可用于正式回测的快照占 `{quality["usable_snapshot_pct"]}%`。

需要注意：`{funnel_summary["exit_trade_rows"]}` 行模拟交易不是可直接相加的真实交易笔数。它们是参数矩阵结果，真实策略要从机会漏斗里收敛到少数主策略后再算账户收益。

本报告还有两个边界：

- 当前数据源是 REST 轮询，不是 WebSocket 毫秒级盘口流。因此 `250ms / 500ms / 1000ms` 延迟测试在 1秒采样下可能落到同一个下一次快照，不能当作真实毫秒级执行结果。
- 当前 `final_outcome` 仍是 Chainlink 价格推算，不是 Polymarket 官方结算字段。后续真钱测试前，需要补官方结算校验。

## 二、数据质量验收

| 指标 | 数值 |
|---|---:|
| 覆盖开始 UTC | {quality["start_utc"]} |
| 覆盖结束 UTC | {quality["end_utc"]} |
| 覆盖小时 | {quality["coverage_hours"]} |
| 快照行数 | {quality["snapshot_rows"]} |
| 5分钟窗口数 | {quality["windows"]} |
| 干净窗口数 | {quality["clean_windows"]} |
| 快照中位间隔秒 | {quality["median_gap_sec"]} |
| 快照 P95 间隔秒 | {quality["p95_gap_sec"]} |
| 快照最大间隔秒 | {quality["max_gap_sec"]} |
| 可用快照占比 | {quality["usable_snapshot_pct"]}% |
| Up 订单簿成功率 | {quality["up_book_success_pct"]}% |
| Down 订单簿成功率 | {quality["down_book_success_pct"]}% |

price_to_beat 状态分布：

```json
{json.dumps(quality["price_status_counts"], ensure_ascii=False, indent=2)}
```

## 三、机会漏斗总览

| 指标 | 数值 |
|---|---:|
| 策略参数组合数 | {funnel_summary["strategies"]} |
| 成功入场的唯一策略窗口数 | {funnel_summary["unique_successful_entries"]} |
| 模拟交易行数 | {funnel_summary["exit_trade_rows"]} |

跳过原因 Top：

{top_table(skip_rows, ["skip_reason", "count", "share_pct"], 10)}

初步解释：

- 最大瓶颈是 `target_ask_above_cap`，说明多数信号出现时 Polymarket 已经涨贵。
- 第二大瓶颈是 `outside_entry_window`，说明不少信号出现在我们允许入场窗口之外。
- 延迟相关失败存在，但不是第一大问题；更大的问题是信号到来时价格本身已经不便宜。

## 四、入场机会漏斗 Top 组合

下面按“成功入场窗口数”排序。它只代表可成交机会多，不代表最终收益最好。

{top_table(funnel_rows, [
    "strategy_id",
    "raw_signal_windows",
    "in_entry_window_signal_windows",
    "attempted_windows",
    "successful_entry_windows",
    "entry_success_pct_of_attempts",
    "target_ask_above_cap",
    "latency_fail_count",
    "avg_entry_price",
], 12)}

## 五、持有到结算收益 Top 组合

这张表仍是参数扫描结果，不能直接把收益相加当真实账户收益。

{top_table(hold_rows, [
    "strategy_id",
    "trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "avg_pnl_usdc",
    "median_pnl_usdc",
    "avg_roi_pct",
    "max_loss_usdc",
], 12)}

## 六、延迟后仍可用的小额组合观察

这里先看 `50 / 100 USDC` 且 `250ms / 500ms / 1000ms` 延迟后的持有到结算结果，用来判断 edge 是否只存在于理想 0ms。

{top_table(conservative_rows, [
    "strategy_id",
    "trade_count",
    "win_rate_pct",
    "total_pnl_usdc",
    "avg_pnl_usdc",
    "avg_roi_pct",
    "max_loss_usdc",
], 12)}

## 七、输出文件

- 窗口质量表：`{WINDOW_QUALITY_CSV}`
- 机会漏斗表：`{FUNNEL_BY_STRATEGY_CSV}`
- 退出收益表：`{EXIT_BY_STRATEGY_CSV}`
- 跳过原因表：`{SKIP_REASON_CSV}`

## 八、下一步建议

下一步不要直接选最高收益组合。

建议按这个顺序继续：

1. 先看机会漏斗，排除主要靠 0ms、容量太小、或只有孤立参数有效的组合。
2. 再看 `50 / 100 USDC` 小额组合，因为它更接近真实可执行。
3. 再做账户级复盘：同一窗口只允许一个主策略入场，不能把所有参数组合收益相加。
4. 最后才做真钱小额测试候选清单。
"""


def main() -> int:
    snapshot_rows = read_csv_rows(SNAPSHOT_CSV)
    window_rows, quality = build_window_quality(snapshot_rows)
    signal_counts, signal_windows, in_entry_windows = build_signal_indexes()
    funnel_rows, exit_rows, skip_rows, funnel_summary = build_funnel(signal_windows, in_entry_windows)

    write_csv(WINDOW_QUALITY_CSV, window_rows)
    write_csv(FUNNEL_BY_STRATEGY_CSV, funnel_rows)
    write_csv(EXIT_BY_STRATEGY_CSV, exit_rows)
    write_csv(SKIP_REASON_CSV, skip_rows)
    QUALITY_REPORT.write_text(
        build_report(quality, funnel_rows, exit_rows, skip_rows, funnel_summary),
        encoding="utf-8",
    )

    print(f"report={QUALITY_REPORT}")
    print(f"window_quality={WINDOW_QUALITY_CSV}")
    print(f"funnel={FUNNEL_BY_STRATEGY_CSV}")
    print(f"exit_outcome={EXIT_BY_STRATEGY_CSV}")
    print(f"skip_reason={SKIP_REASON_CSV}")
    print(f"snapshot_rows={quality['snapshot_rows']}")
    print(f"coverage_hours={quality['coverage_hours']}")
    print(f"strategies={funnel_summary['strategies']}")
    print(f"unique_successful_entries={funnel_summary['unique_successful_entries']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
