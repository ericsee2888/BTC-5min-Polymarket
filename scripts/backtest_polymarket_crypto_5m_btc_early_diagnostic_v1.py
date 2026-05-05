#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_INPUT = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v1.csv"
SUMMARY_CSV = DATA_DIR / "polymarket_crypto_5m_btc_early_diagnostic_summary_v1.csv"
TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_early_diagnostic_trades_v1.csv"
NOTE_MD = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_EARLY_DIAGNOSTIC_BACKTEST_V1_CN.md"


THRESHOLDS_USD = [0, 10, 25, 50, 75, 100]
ENTRY_CAPS = [0.55, 0.60, 0.65, 0.70]
ENTRY_START_SECONDS = 60
ENTRY_END_SECONDS = 180
PROFIT_TARGET = 0.75
STOP_LOSS = 0.35


@dataclass(frozen=True)
class TradeResult:
    rule_name: str
    threshold_usd: float
    entry_cap: float | None
    slug: str
    direction: str
    entry_time_utc: str
    entry_second: float
    entry_price: float
    exit_type: str
    exit_time_utc: str
    exit_second: float
    exit_price: float
    final_outcome: str
    correct: bool
    fee_rate: float
    buy_fee_per_share: float
    sell_fee_per_share: float
    pnl_per_share: float
    cash_used_per_share: float
    roi_on_cash: float
    price_to_beat: float
    entry_binance_delta: float
    entry_coinbase_delta: float
    entry_chainlink_delta: float
    final_chainlink_delta: float


def taker_fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1 - price)


def load_samples(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    numeric_cols = [
        "polymarket_seconds_since_start",
        "polymarket_seconds_to_end",
        "polymarket_up_price",
        "polymarket_down_price",
        "polymarket_price_to_beat",
        "polymarket_fee_rate",
        "binance_btcusdt",
        "coinbase_btcusd",
        "chainlink_btcusd",
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    df = df.sort_values(["polymarket_market_slug", "sampled_at_unix_ms"]).reset_index(drop=True)
    return df


def build_clean_samples(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    valid_rows = df[
        (df["polymarket_seconds_since_start"] >= 0)
        & (df["polymarket_seconds_since_start"] <= 305)
        & df["polymarket_market_slug"].notna()
        & df["polymarket_price_to_beat"].notna()
        & df["chainlink_btcusd"].notna()
    ].copy()

    window_stats = (
        valid_rows.groupby("polymarket_market_slug", as_index=False)
        .agg(
            rows=("sampled_at_utc", "size"),
            first_sample=("sampled_at_utc", "min"),
            last_sample=("sampled_at_utc", "max"),
            min_second=("polymarket_seconds_since_start", "min"),
            max_second=("polymarket_seconds_since_start", "max"),
            price_to_beat=("polymarket_price_to_beat", "first"),
            final_chainlink=("chainlink_btcusd", "last"),
            first_up_price=("polymarket_up_price", "first"),
            last_up_price=("polymarket_up_price", "last"),
        )
        .copy()
    )
    window_stats["final_chainlink_delta"] = (
        window_stats["final_chainlink"] - window_stats["price_to_beat"]
    )
    window_stats["final_outcome"] = window_stats["final_chainlink_delta"].map(
        lambda x: "UP" if x >= 0 else "DOWN"
    )
    window_stats["is_clean_window"] = (
        (window_stats["min_second"] <= 15)
        & (window_stats["max_second"] >= 285)
        & (window_stats["rows"] >= 100)
    )
    clean_slugs = set(
        window_stats.loc[window_stats["is_clean_window"], "polymarket_market_slug"]
    )
    clean_rows = valid_rows[valid_rows["polymarket_market_slug"].isin(clean_slugs)].copy()
    return clean_rows, window_stats


def final_outcome_for_window(part: pd.DataFrame) -> tuple[str, float]:
    final_row = part.sort_values("sampled_at_unix_ms").iloc[-1]
    final_delta = float(final_row["chainlink_btcusd"] - final_row["polymarket_price_to_beat"])
    return ("UP" if final_delta >= 0 else "DOWN"), final_delta


def direction_price(row: pd.Series, direction: str) -> float | None:
    price = row["polymarket_up_price"] if direction == "UP" else row["polymarket_down_price"]
    if pd.isna(price):
        return None
    return float(price)


def find_entry(
    part: pd.DataFrame,
    threshold_usd: float,
    entry_cap: float | None,
) -> tuple[str, pd.Series] | None:
    entry_window = part[
        (part["polymarket_seconds_since_start"] >= ENTRY_START_SECONDS)
        & (part["polymarket_seconds_since_start"] <= ENTRY_END_SECONDS)
        & part["binance_btcusdt"].notna()
        & part["coinbase_btcusd"].notna()
        & part["polymarket_price_to_beat"].notna()
    ].copy()
    for _, row in entry_window.iterrows():
        price_to_beat = float(row["polymarket_price_to_beat"])
        binance_delta = float(row["binance_btcusdt"] - price_to_beat)
        coinbase_delta = float(row["coinbase_btcusd"] - price_to_beat)

        direction = ""
        if binance_delta >= threshold_usd and coinbase_delta >= threshold_usd:
            direction = "UP"
        elif binance_delta <= -threshold_usd and coinbase_delta <= -threshold_usd:
            direction = "DOWN"
        else:
            continue

        entry_price = direction_price(row, direction)
        if entry_price is None:
            continue
        if entry_cap is not None and entry_price > entry_cap:
            continue
        return direction, row
    return None


def simulate_hold_to_resolution(
    slug: str,
    part: pd.DataFrame,
    threshold_usd: float,
    entry_cap: float | None,
) -> TradeResult | None:
    entry = find_entry(part, threshold_usd=threshold_usd, entry_cap=entry_cap)
    if entry is None:
        return None
    direction, row = entry
    final_outcome, final_delta = final_outcome_for_window(part)
    entry_price = direction_price(row, direction)
    if entry_price is None:
        return None
    fee_rate = float(row["polymarket_fee_rate"]) if not pd.isna(row["polymarket_fee_rate"]) else 0.0
    buy_fee = taker_fee_per_share(entry_price, fee_rate)
    payoff = 1.0 if direction == final_outcome else 0.0
    pnl = payoff - entry_price - buy_fee
    cash_used = entry_price + buy_fee
    return TradeResult(
        rule_name="hold_to_resolution",
        threshold_usd=threshold_usd,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=str(row["sampled_at_utc"]),
        entry_second=float(row["polymarket_seconds_since_start"]),
        entry_price=entry_price,
        exit_type="RESOLUTION",
        exit_time_utc=str(part.iloc[-1]["sampled_at_utc"]),
        exit_second=float(part.iloc[-1]["polymarket_seconds_since_start"]),
        exit_price=payoff,
        final_outcome=final_outcome,
        correct=direction == final_outcome,
        fee_rate=fee_rate,
        buy_fee_per_share=buy_fee,
        sell_fee_per_share=0.0,
        pnl_per_share=pnl,
        cash_used_per_share=cash_used,
        roi_on_cash=pnl / cash_used if cash_used else 0.0,
        price_to_beat=float(row["polymarket_price_to_beat"]),
        entry_binance_delta=float(row["binance_btcusdt"] - row["polymarket_price_to_beat"]),
        entry_coinbase_delta=float(row["coinbase_btcusd"] - row["polymarket_price_to_beat"]),
        entry_chainlink_delta=float(row["chainlink_btcusd"] - row["polymarket_price_to_beat"]),
        final_chainlink_delta=final_delta,
    )


def simulate_exit_rule(
    slug: str,
    part: pd.DataFrame,
    threshold_usd: float,
    entry_cap: float | None,
) -> TradeResult | None:
    entry = find_entry(part, threshold_usd=threshold_usd, entry_cap=entry_cap)
    if entry is None:
        return None
    direction, row = entry
    final_outcome, final_delta = final_outcome_for_window(part)
    entry_price = direction_price(row, direction)
    if entry_price is None:
        return None

    fee_rate = float(row["polymarket_fee_rate"]) if not pd.isna(row["polymarket_fee_rate"]) else 0.0
    buy_fee = taker_fee_per_share(entry_price, fee_rate)
    after_entry = part[
        part["sampled_at_unix_ms"] >= row["sampled_at_unix_ms"]
    ].copy()

    exit_type = "RESOLUTION"
    exit_row = after_entry.iloc[-1]
    exit_price = 1.0 if direction == final_outcome else 0.0
    sell_fee = 0.0

    for _, monitor_row in after_entry.iterrows():
        current_price = direction_price(monitor_row, direction)
        if current_price is None:
            continue
        if current_price >= PROFIT_TARGET:
            exit_type = "PROFIT_TARGET"
            exit_row = monitor_row
            exit_price = current_price
            sell_fee = taker_fee_per_share(exit_price, fee_rate)
            break
        if current_price <= STOP_LOSS:
            exit_type = "STOP_LOSS"
            exit_row = monitor_row
            exit_price = current_price
            sell_fee = taker_fee_per_share(exit_price, fee_rate)
            break

    pnl = exit_price - entry_price - buy_fee - sell_fee
    cash_used = entry_price + buy_fee
    return TradeResult(
        rule_name="profit_075_stop_035_else_resolution",
        threshold_usd=threshold_usd,
        entry_cap=entry_cap,
        slug=slug,
        direction=direction,
        entry_time_utc=str(row["sampled_at_utc"]),
        entry_second=float(row["polymarket_seconds_since_start"]),
        entry_price=entry_price,
        exit_type=exit_type,
        exit_time_utc=str(exit_row["sampled_at_utc"]),
        exit_second=float(exit_row["polymarket_seconds_since_start"]),
        exit_price=exit_price,
        final_outcome=final_outcome,
        correct=direction == final_outcome,
        fee_rate=fee_rate,
        buy_fee_per_share=buy_fee,
        sell_fee_per_share=sell_fee,
        pnl_per_share=pnl,
        cash_used_per_share=cash_used,
        roi_on_cash=pnl / cash_used if cash_used else 0.0,
        price_to_beat=float(row["polymarket_price_to_beat"]),
        entry_binance_delta=float(row["binance_btcusdt"] - row["polymarket_price_to_beat"]),
        entry_coinbase_delta=float(row["coinbase_btcusd"] - row["polymarket_price_to_beat"]),
        entry_chainlink_delta=float(row["chainlink_btcusd"] - row["polymarket_price_to_beat"]),
        final_chainlink_delta=final_delta,
    )


def summarize_trades(trades: pd.DataFrame, clean_window_count: int) -> pd.DataFrame:
    if trades.empty:
        return pd.DataFrame()
    grouped = (
        trades.groupby(["rule_name", "threshold_usd", "entry_cap"], dropna=False)
        .agg(
            trades=("slug", "size"),
            trade_window_share=("slug", lambda x: len(x) / clean_window_count),
            win_rate=("correct", "mean"),
            avg_entry_price=("entry_price", "mean"),
            avg_pnl_per_share=("pnl_per_share", "mean"),
            total_pnl_per_share=("pnl_per_share", "sum"),
            avg_roi_on_cash=("roi_on_cash", "mean"),
            median_roi_on_cash=("roi_on_cash", "median"),
            profit_target_share=("exit_type", lambda x: (x == "PROFIT_TARGET").mean()),
            stop_loss_share=("exit_type", lambda x: (x == "STOP_LOSS").mean()),
            resolution_share=("exit_type", lambda x: (x == "RESOLUTION").mean()),
        )
        .reset_index()
    )
    for col in [
        "trade_window_share",
        "win_rate",
        "avg_entry_price",
        "avg_pnl_per_share",
        "total_pnl_per_share",
        "avg_roi_on_cash",
        "median_roi_on_cash",
        "profit_target_share",
        "stop_loss_share",
        "resolution_share",
    ]:
        grouped[col] = grouped[col].round(6)
    return grouped.sort_values(
        ["rule_name", "avg_pnl_per_share", "trades"], ascending=[True, False, False]
    )


def markdown_table(frame: pd.DataFrame) -> str:
    if frame.empty:
        return "无交易结果。"
    display = frame.copy()
    for col in display.columns:
        display[col] = display[col].map(lambda value: "" if pd.isna(value) else str(value))
    columns = list(display.columns)
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for _, row in display.iterrows():
        values = [str(row[col]).replace("|", "\\|") for col in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def build_markdown(
    df: pd.DataFrame,
    window_stats: pd.DataFrame,
    clean_rows: pd.DataFrame,
    summary: pd.DataFrame,
    trades: pd.DataFrame,
) -> str:
    clean_windows = window_stats[window_stats["is_clean_window"]].copy()
    outcome_counts = clean_windows["final_outcome"].value_counts().to_dict()
    best_hold = summary[summary["rule_name"] == "hold_to_resolution"].head(5)
    best_exit = summary[summary["rule_name"] == "profit_075_stop_035_else_resolution"].head(5)

    lines: list[str] = []
    lines.append("# Polymarket BTC 5分钟盘口早期诊断回测 v1")
    lines.append("")
    lines.append("## 数据范围")
    lines.append("")
    lines.append(f"- 原始样本数：`{len(df)}`")
    lines.append(f"- 原始窗口数：`{df['polymarket_market_slug'].nunique()}`")
    lines.append(f"- 可用于本轮诊断的干净窗口数：`{len(clean_windows)}`")
    lines.append(f"- 干净样本数：`{len(clean_rows)}`")
    lines.append(f"- 第一条样本：`{df.iloc[0]['sampled_at_utc']}`")
    lines.append(f"- 最后一条样本：`{df.iloc[-1]['sampled_at_utc']}`")
    lines.append(f"- 最终 UP 窗口：`{outcome_counts.get('UP', 0)}`")
    lines.append(f"- 最终 DOWN 窗口：`{outcome_counts.get('DOWN', 0)}`")
    lines.append("")
    lines.append("## 回测口径")
    lines.append("")
    lines.append("- 只使用覆盖接近完整的 5 分钟窗口：开盘后 15 秒内有样本，且接近收盘也有样本。")
    lines.append("- 方向信号：Binance 和 Coinbase 同时高于目标价，做 UP；同时低于目标价，做 DOWN。")
    lines.append(f"- 入场时间：开盘后 `{ENTRY_START_SECONDS}-{ENTRY_END_SECONDS}` 秒。")
    lines.append("- 交易价格：使用采集到的 Up/Down 当前价格，作为粗略成交代理。")
    lines.append("- 成本：按样本里的 crypto fee rate `0.072` 估算 taker fee。")
    lines.append("- 这版还没有订单簿深度和排队位置，所以只能验证价格源延迟方向，不能验证完整做市或订单簿策略。")
    lines.append("")
    lines.append("## 主要观察")
    lines.append("")
    if not best_hold.empty:
        top = best_hold.iloc[0]
        lines.append(
            f"- 持有到结算的最好组合：阈值 `${top['threshold_usd']}`，入场价格上限 `{top['entry_cap']}`，"
            f"交易 `{int(top['trades'])}` 次，胜率 `{top['win_rate']:.2%}`，"
            f"单股平均收益 `{top['avg_pnl_per_share']:.4f}`。"
        )
    if not best_exit.empty:
        top = best_exit.iloc[0]
        lines.append(
            f"- 0.75 止盈 / 0.35 止损组合的最好结果：阈值 `${top['threshold_usd']}`，入场价格上限 `{top['entry_cap']}`，"
            f"交易 `{int(top['trades'])}` 次，胜率 `{top['win_rate']:.2%}`，"
            f"单股平均收益 `{top['avg_pnl_per_share']:.4f}`。"
        )
    lines.append("- 初步看，这批数据足够做第一轮方向诊断，但还不足以直接证明文章里的 71% 胜率和月化收益。")
    lines.append("- 如果要接近文章策略，下一步必须补订单簿深度、bid/ask 可成交价格、以及真实成交模拟。")
    lines.append("")
    lines.append("## Top 5：持有到结算")
    lines.append("")
    lines.append(markdown_table(best_hold))
    lines.append("")
    lines.append("## Top 5：止盈止损")
    lines.append("")
    lines.append(markdown_table(best_exit))
    lines.append("")
    lines.append("## 输出文件")
    lines.append("")
    lines.append(f"- 汇总表：`{SUMMARY_CSV}`")
    lines.append(f"- 逐笔交易表：`{TRADES_CSV}`")
    lines.append(f"- 回测脚本：`{ROOT / 'scripts' / 'backtest_polymarket_crypto_5m_btc_early_diagnostic_v1.py'}`")
    lines.append("")
    return "\n".join(lines)


def run_backtest(input_path: Path) -> dict[str, Any]:
    df = load_samples(input_path)
    clean_rows, window_stats = build_clean_samples(df)
    grouped = {
        slug: part.sort_values("sampled_at_unix_ms").reset_index(drop=True)
        for slug, part in clean_rows.groupby("polymarket_market_slug")
    }

    trade_rows: list[TradeResult] = []
    for threshold in THRESHOLDS_USD:
        for entry_cap in ENTRY_CAPS:
            for slug, part in grouped.items():
                hold_trade = simulate_hold_to_resolution(slug, part, threshold, entry_cap)
                if hold_trade:
                    trade_rows.append(hold_trade)
                exit_trade = simulate_exit_rule(slug, part, threshold, entry_cap)
                if exit_trade:
                    trade_rows.append(exit_trade)

    trades = pd.DataFrame([trade.__dict__ for trade in trade_rows])
    summary = summarize_trades(trades, clean_window_count=len(grouped))

    SUMMARY_CSV.parent.mkdir(parents=True, exist_ok=True)
    summary.to_csv(SUMMARY_CSV, index=False)
    trades.to_csv(TRADES_CSV, index=False)
    NOTE_MD.write_text(
        build_markdown(df, window_stats, clean_rows, summary, trades),
        encoding="utf-8",
    )

    return {
        "input_path": str(input_path),
        "raw_samples": int(len(df)),
        "raw_windows": int(df["polymarket_market_slug"].nunique()),
        "clean_samples": int(len(clean_rows)),
        "clean_windows": int(len(grouped)),
        "trade_rows": int(len(trades)),
        "summary_rows": int(len(summary)),
        "summary_csv": str(SUMMARY_CSV),
        "trades_csv": str(TRADES_CSV),
        "note_md": str(NOTE_MD),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Early diagnostic backtest for Polymarket BTC 5m samples.")
    parser.add_argument("--input", type=Path, default=DEFAULT_INPUT)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    result = run_backtest(args.input)
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
