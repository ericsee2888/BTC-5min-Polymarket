#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import signal
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from collect_polymarket_crypto_price_samples_v1 import (  # noqa: E402
    COINBASE_BTCUSD_URL,
    BINANCE_BTCUSDT_URL,
    RtdsPriceCache,
    fetch_polymarket_market,
    parse_coinbase_price,
    parse_binance_price,
    parse_float,
    parse_iso_to_unix_ms,
    parse_jsonish_list,
    slim_polymarket_market,
)


DATA_DIR = ROOT / "data"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv"
DEFAULT_EVENT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trading_trades_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_PAPER_TRADING_RUN_V1_CN.md"

USER_AGENT = "CodexResearch/1.0"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"
STOP_REQUESTED = False


SNAPSHOT_FIELDS = [
    "sampled_at_utc",
    "sampled_at_unix_ms",
    "slug",
    "question",
    "seconds_since_start",
    "seconds_to_end",
    "price_to_beat",
    "price_to_beat_status",
    "binance_btcusdt",
    "coinbase_btcusd",
    "chainlink_btcusd",
    "binance_delta",
    "coinbase_delta",
    "signal_direction",
    "signal_reason",
    "up_best_bid",
    "up_best_ask",
    "down_best_bid",
    "down_best_ask",
    "open_position_slug",
    "open_position_direction",
    "open_position_shares",
    "open_position_entry_total_cash",
    "open_position_unrealized_exit_cash",
    "notes",
]


TRADE_FIELDS = [
    "trade_id",
    "slug",
    "question",
    "direction",
    "entry_time_utc",
    "entry_second",
    "threshold_usd",
    "entry_cap",
    "order_cash_usdc",
    "entry_avg_price",
    "entry_worst_price",
    "entry_shares",
    "entry_notional",
    "entry_fee",
    "entry_total_cash",
    "exit_type",
    "exit_time_utc",
    "exit_second",
    "exit_avg_price",
    "exit_worst_price",
    "exit_notional",
    "exit_fee",
    "exit_cash_after_fee",
    "final_outcome",
    "correct",
    "pnl_usdc",
    "roi_on_cash",
    "price_to_beat",
    "entry_binance_delta",
    "entry_coinbase_delta",
    "entry_chainlink_delta",
    "exit_chainlink_delta",
]


@dataclass
class FillResult:
    complete: bool
    shares: float
    notional: float
    fee: float
    total_cash: float
    avg_price: float | None
    worst_price: float | None
    levels_used: int


@dataclass
class OpenPosition:
    trade_id: str
    slug: str
    question: str
    direction: str
    token_id: str
    entry_time_utc: str
    entry_second: float
    threshold_usd: float
    entry_cap: float
    order_cash_usdc: float
    entry_avg_price: float
    entry_worst_price: float
    entry_shares: float
    entry_notional: float
    entry_fee: float
    entry_total_cash: float
    price_to_beat: float
    entry_binance_delta: float
    entry_coinbase_delta: float
    entry_chainlink_delta: float
    fee_rate: float
    end_time_ms: int


def handle_stop_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global STOP_REQUESTED
    STOP_REQUESTED = True


def fetch_json(url: str, timeout: float) -> tuple[Any | None, float | None, str]:
    started = time.perf_counter()
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        return payload, round((time.perf_counter() - started) * 1000, 3), ""
    except Exception as exc:  # noqa: BLE001
        return None, round((time.perf_counter() - started) * 1000, 3), f"{type(exc).__name__}: {exc}"


def taker_fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1 - price)


def price_size_levels(levels: list[dict[str, Any]], reverse: bool) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    for level in levels:
        price = parse_float(level.get("price"))
        size = parse_float(level.get("size"))
        if price is not None and size is not None and price > 0 and size > 0:
            parsed.append((price, size))
    parsed.sort(key=lambda item: item[0], reverse=reverse)
    return parsed


def fetch_book(token_id: str, timeout: float) -> tuple[list[tuple[float, float]], list[tuple[float, float]], str]:
    payload, _latency_ms, error = fetch_json(f"{CLOB_BOOK_URL}?{urlencode({'token_id': token_id})}", timeout)
    if error:
        return [], [], error
    if not isinstance(payload, dict):
        return [], [], "unexpected_book_response"
    bids = price_size_levels(payload.get("bids") or [], reverse=True)
    asks = price_size_levels(payload.get("asks") or [], reverse=False)
    return bids, asks, ""


def quote_buy_total_cash(
    asks: list[tuple[float, float]],
    cash_budget: float,
    fee_rate: float,
    max_price: float,
) -> FillResult:
    remaining_cash = cash_budget
    shares = 0.0
    notional = 0.0
    fees = 0.0
    worst_price: float | None = None
    levels_used = 0

    for price, available_shares in asks:
        if price > max_price or remaining_cash <= 1e-9:
            continue
        cost_per_share = price + taker_fee_per_share(price, fee_rate)
        if cost_per_share <= 0:
            continue
        buy_shares = min(available_shares, remaining_cash / cost_per_share)
        if buy_shares <= 0:
            continue
        level_notional = buy_shares * price
        level_fee = buy_shares * taker_fee_per_share(price, fee_rate)
        shares += buy_shares
        notional += level_notional
        fees += level_fee
        remaining_cash -= level_notional + level_fee
        worst_price = price
        levels_used += 1

    total_cash = notional + fees
    complete = remaining_cash <= max(0.01, cash_budget * 1e-6)
    avg_price = notional / shares if shares > 0 else None
    return FillResult(complete, shares, notional, fees, total_cash, avg_price, worst_price, levels_used)


def quote_sell_shares(
    bids: list[tuple[float, float]],
    shares_to_sell: float,
    fee_rate: float,
    min_price: float,
) -> FillResult:
    remaining_shares = shares_to_sell
    sold_shares = 0.0
    notional = 0.0
    fees = 0.0
    worst_price: float | None = None
    levels_used = 0

    for price, available_shares in bids:
        if price < min_price or remaining_shares <= 1e-9:
            continue
        sell_shares = min(remaining_shares, available_shares)
        if sell_shares <= 0:
            continue
        level_notional = sell_shares * price
        level_fee = sell_shares * taker_fee_per_share(price, fee_rate)
        sold_shares += sell_shares
        notional += level_notional
        fees += level_fee
        remaining_shares -= sell_shares
        worst_price = price
        levels_used += 1

    cash_after_fee = notional - fees
    complete = remaining_shares <= max(0.0001, shares_to_sell * 1e-6)
    avg_price = notional / sold_shares if sold_shares > 0 else None
    return FillResult(complete, sold_shares, notional, fees, cash_after_fee, avg_price, worst_price, levels_used)


def market_tokens(market: dict[str, Any]) -> dict[str, str]:
    outcomes = [str(item).upper() for item in parse_jsonish_list(market.get("outcomes"))]
    tokens = [str(item) for item in parse_jsonish_list(market.get("clobTokenIds"))]
    return {outcome: token for outcome, token in zip(outcomes, tokens, strict=False)}


def ensure_csv(path: Path, fieldnames: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.stat().st_size > 0:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()


def append_csv(path: Path, fieldnames: list[str], row: dict[str, Any]) -> None:
    ensure_csv(path, fieldnames)
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writerow({key: row.get(key, "") for key in fieldnames})


def append_event(path: Path, event: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, ensure_ascii=False) + "\n")


def json_safe_args(args: argparse.Namespace) -> dict[str, Any]:
    safe: dict[str, Any] = {}
    for key, value in vars(args).items():
        safe[key] = str(value) if isinstance(value, Path) else value
    return safe


def maybe_float(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def decide_signal(
    seconds_since_start: float | None,
    price_to_beat: float | None,
    binance_price: float | None,
    coinbase_price: float | None,
    threshold_usd: float,
    entry_start: float,
    entry_end: float,
) -> tuple[str, str, float | None, float | None]:
    if seconds_since_start is None:
        return "", "missing_market_time", None, None
    if seconds_since_start < entry_start or seconds_since_start > entry_end:
        return "", "outside_entry_window", None, None
    if price_to_beat is None:
        return "", "missing_price_to_beat", None, None
    if binance_price is None or coinbase_price is None:
        return "", "missing_price_source", None, None

    binance_delta = binance_price - price_to_beat
    coinbase_delta = coinbase_price - price_to_beat
    if binance_delta >= threshold_usd and coinbase_delta >= threshold_usd:
        return "UP", "binance_and_coinbase_above_threshold", binance_delta, coinbase_delta
    if binance_delta <= -threshold_usd and coinbase_delta <= -threshold_usd:
        return "DOWN", "binance_and_coinbase_below_threshold", binance_delta, coinbase_delta
    return "", "threshold_not_met", binance_delta, coinbase_delta


def close_position_row(
    position: OpenPosition,
    exit_type: str,
    exit_time_utc: str,
    exit_second: float | None,
    exit_avg_price: float | None,
    exit_worst_price: float | None,
    exit_notional: float,
    exit_fee: float,
    exit_cash_after_fee: float,
    final_outcome: str,
    chainlink_price: float | None,
) -> dict[str, Any]:
    pnl = exit_cash_after_fee - position.entry_total_cash
    return {
        "trade_id": position.trade_id,
        "slug": position.slug,
        "question": position.question,
        "direction": position.direction,
        "entry_time_utc": position.entry_time_utc,
        "entry_second": round(position.entry_second, 3),
        "threshold_usd": position.threshold_usd,
        "entry_cap": position.entry_cap,
        "order_cash_usdc": position.order_cash_usdc,
        "entry_avg_price": round(position.entry_avg_price, 6),
        "entry_worst_price": round(position.entry_worst_price, 6),
        "entry_shares": round(position.entry_shares, 6),
        "entry_notional": round(position.entry_notional, 6),
        "entry_fee": round(position.entry_fee, 6),
        "entry_total_cash": round(position.entry_total_cash, 6),
        "exit_type": exit_type,
        "exit_time_utc": exit_time_utc,
        "exit_second": round(exit_second, 3) if exit_second is not None else "",
        "exit_avg_price": round(exit_avg_price, 6) if exit_avg_price is not None else "",
        "exit_worst_price": round(exit_worst_price, 6) if exit_worst_price is not None else "",
        "exit_notional": round(exit_notional, 6),
        "exit_fee": round(exit_fee, 6),
        "exit_cash_after_fee": round(exit_cash_after_fee, 6),
        "final_outcome": final_outcome,
        "correct": position.direction == final_outcome if final_outcome else "",
        "pnl_usdc": round(pnl, 6),
        "roi_on_cash": round(pnl / position.entry_total_cash, 6) if position.entry_total_cash else "",
        "price_to_beat": round(position.price_to_beat, 6),
        "entry_binance_delta": round(position.entry_binance_delta, 6),
        "entry_coinbase_delta": round(position.entry_coinbase_delta, 6),
        "entry_chainlink_delta": round(position.entry_chainlink_delta, 6),
        "exit_chainlink_delta": (
            round(chainlink_price - position.price_to_beat, 6)
            if chainlink_price is not None
            else ""
        ),
    }


def build_report(trades_csv: Path, report_path: Path, started_at: str, ended_at: str, args: argparse.Namespace) -> None:
    rows: list[dict[str, str]] = []
    if trades_csv.exists():
        with trades_csv.open(newline="", encoding="utf-8") as handle:
            rows = list(csv.DictReader(handle))

    total_pnl = sum(float(row["pnl_usdc"]) for row in rows if row.get("pnl_usdc"))
    wins = [
        row for row in rows
        if row.get("pnl_usdc") not in ("", None) and float(row["pnl_usdc"]) > 0
    ]
    report = f"""# Polymarket BTC 5分钟实时 Paper Trading v1

## 本次运行

- 开始时间：`{started_at}`
- 结束时间：`{ended_at}`
- 信号阈值：`${args.threshold_usd:g}`
- 模拟单笔总投入：`{args.order_cash_usdc:g} USDC`
- 入场窗口：开盘后 `{args.entry_start_seconds:g}-{args.entry_end_seconds:g}` 秒
- 入场价格上限：`{args.entry_cap:g}`
- 止盈观察价：`{args.profit_target:g}`
- 压力退出价：`{args.stop_loss:g}`

## 结果

- 已完成模拟交易：`{len(rows)}` 笔
- 盈利交易：`{len(wins)}` 笔
- 胜率：`{(len(wins) / len(rows) if rows else 0):.2%}`
- 合计模拟收益：`{total_pnl:.2f} USDC`

## 输出文件

- 快照：`{args.snapshot_csv}`
- 事件：`{args.event_jsonl}`
- 交易：`{args.trades_csv}`

## 说明

这是真实数据驱动的模拟交易，不会发真钱订单。它用实时 Binance / Coinbase / Chainlink 和 Polymarket CLOB 订单簿来判断信号，并按盘口深度模拟买入和退出。
"""
    report_path.write_text(report, encoding="utf-8")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run real-time paper trading for the Polymarket BTC 5-minute market."
    )
    parser.add_argument("--threshold-usd", type=float, default=50.0)
    parser.add_argument("--order-cash-usdc", type=float, default=100.0)
    parser.add_argument("--entry-cap", type=float, default=0.65)
    parser.add_argument("--entry-start-seconds", type=float, default=60.0)
    parser.add_argument("--entry-end-seconds", type=float, default=180.0)
    parser.add_argument("--profit-target", type=float, default=0.75)
    parser.add_argument("--stop-loss", type=float, default=0.35)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--duration-seconds", type=float, default=0.0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--rtds-warmup-seconds", type=float, default=3.0)
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--event-jsonl", type=Path, default=DEFAULT_EVENT_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--reset-output", action="store_true")
    parser.add_argument("--print-events", action="store_true")
    return parser.parse_args()


def main() -> int:
    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    args = parse_args()

    if args.reset_output:
        args.snapshot_csv.unlink(missing_ok=True)
        args.event_jsonl.unlink(missing_ok=True)
        args.trades_csv.unlink(missing_ok=True)
        args.report.unlink(missing_ok=True)

    ensure_csv(args.snapshot_csv, SNAPSHOT_FIELDS)
    ensure_csv(args.trades_csv, TRADE_FIELDS)

    started_at = datetime.now(UTC).isoformat()
    started_monotonic = time.monotonic()
    price_to_beat_by_slug: dict[str, float] = {}
    price_to_beat_status_by_slug: dict[str, str] = {}
    entered_slugs: set[str] = set()
    open_position: OpenPosition | None = None
    trade_counter = 0

    rtds_cache = RtdsPriceCache()
    rtds_cache.start()
    time.sleep(max(0.0, args.rtds_warmup_seconds))

    append_event(
        args.event_jsonl,
        {"event_type": "run_started", "started_at_utc": started_at, "args": json_safe_args(args)},
    )

    try:
        while not STOP_REQUESTED:
            loop_started = time.perf_counter()
            now = datetime.now(UTC)
            now_ms = int(now.timestamp() * 1000)

            if args.duration_seconds > 0 and time.monotonic() - started_monotonic >= args.duration_seconds:
                break

            with ThreadPoolExecutor(max_workers=3) as executor:
                binance_future = executor.submit(fetch_json, BINANCE_BTCUSDT_URL, args.timeout_seconds)
                coinbase_future = executor.submit(fetch_json, COINBASE_BTCUSD_URL, args.timeout_seconds)
                market_future = executor.submit(fetch_polymarket_market, args.timeout_seconds)
                binance_payload, _binance_latency, binance_error = binance_future.result()
                coinbase_payload, _coinbase_latency, coinbase_error = coinbase_future.result()
                polymarket_market, polymarket_error = market_future.result()

            binance_price = parse_binance_price(binance_payload) if not binance_error else None
            coinbase_price = parse_coinbase_price(coinbase_payload) if not coinbase_error else None
            rtds = rtds_cache.snapshot()
            chainlink_price = rtds["chainlink_btcusd"]
            market = polymarket_market or {}
            slim = slim_polymarket_market(market) if market else {}
            slug = str(slim.get("slug") or "")
            question = str(slim.get("question") or "")
            seconds_since_start = slim.get("seconds_since_start")
            seconds_to_end = slim.get("seconds_to_end")
            fee_rate = parse_float((market.get("feeSchedule") or {}).get("rate")) if market else None
            fee_rate = 0.0 if fee_rate is None else fee_rate

            if slug and chainlink_price is not None and slug not in price_to_beat_by_slug:
                if seconds_since_start is not None and seconds_since_start <= 15:
                    price_to_beat_by_slug[slug] = chainlink_price
                    price_to_beat_status_by_slug[slug] = "early_observed"
                else:
                    price_to_beat_status_by_slug[slug] = "late_window_skip"

            price_to_beat = price_to_beat_by_slug.get(slug)
            price_to_beat_status = price_to_beat_status_by_slug.get(slug, "")
            tokens = market_tokens(market) if market else {}

            signal_direction, signal_reason, binance_delta, coinbase_delta = decide_signal(
                seconds_since_start=seconds_since_start,
                price_to_beat=price_to_beat,
                binance_price=binance_price,
                coinbase_price=coinbase_price,
                threshold_usd=args.threshold_usd,
                entry_start=args.entry_start_seconds,
                entry_end=args.entry_end_seconds,
            )

            book_by_outcome: dict[str, tuple[list[tuple[float, float]], list[tuple[float, float]], str]] = {}
            for outcome, token_id in tokens.items():
                if outcome in {"UP", "DOWN"}:
                    book_by_outcome[outcome] = fetch_book(token_id, args.timeout_seconds)

            def best_bid(outcome: str) -> float | None:
                bids, _asks, _error = book_by_outcome.get(outcome, ([], [], ""))
                return bids[0][0] if bids else None

            def best_ask(outcome: str) -> float | None:
                _bids, asks, _error = book_by_outcome.get(outcome, ([], [], ""))
                return asks[0][0] if asks else None

            notes: list[str] = []
            if polymarket_error:
                notes.append(f"polymarket_error={polymarket_error}")
            if binance_error:
                notes.append(f"binance_error={binance_error}")
            if coinbase_error:
                notes.append(f"coinbase_error={coinbase_error}")
            if rtds["rtds_error"]:
                notes.append(f"rtds_error={rtds['rtds_error']}")

            # Entry: one simulated trade per 5-minute market.
            if (
                open_position is None
                and slug
                and slug not in entered_slugs
                and signal_direction in {"UP", "DOWN"}
                and price_to_beat is not None
                and chainlink_price is not None
            ):
                token_id = tokens.get(signal_direction)
                bids, asks, book_error = book_by_outcome.get(signal_direction, ([], [], "missing_book"))
                if token_id and not book_error:
                    fill = quote_buy_total_cash(
                        asks=asks,
                        cash_budget=args.order_cash_usdc,
                        fee_rate=fee_rate,
                        max_price=args.entry_cap,
                    )
                    event = {
                        "event_type": "entry_signal",
                        "sampled_at_utc": now.isoformat(),
                        "slug": slug,
                        "direction": signal_direction,
                        "reason": signal_reason,
                        "fill_complete": fill.complete,
                        "entry_avg_price": maybe_float(fill.avg_price),
                        "entry_worst_price": maybe_float(fill.worst_price),
                        "entry_shares": round(fill.shares, 6),
                        "entry_total_cash": round(fill.total_cash, 6),
                    }
                    append_event(args.event_jsonl, event)
                    if args.print_events:
                        print(json.dumps(event, ensure_ascii=False), flush=True)
                    if fill.complete and fill.avg_price is not None and fill.worst_price is not None:
                        trade_counter += 1
                        end_time_ms = parse_iso_to_unix_ms(str(slim.get("end_time") or "")) or now_ms
                        open_position = OpenPosition(
                            trade_id=f"paper-{int(now.timestamp())}-{trade_counter}",
                            slug=slug,
                            question=question,
                            direction=signal_direction,
                            token_id=token_id,
                            entry_time_utc=now.isoformat(),
                            entry_second=float(seconds_since_start or 0),
                            threshold_usd=args.threshold_usd,
                            entry_cap=args.entry_cap,
                            order_cash_usdc=args.order_cash_usdc,
                            entry_avg_price=fill.avg_price,
                            entry_worst_price=fill.worst_price,
                            entry_shares=fill.shares,
                            entry_notional=fill.notional,
                            entry_fee=fill.fee,
                            entry_total_cash=fill.total_cash,
                            price_to_beat=price_to_beat,
                            entry_binance_delta=binance_delta or 0.0,
                            entry_coinbase_delta=coinbase_delta or 0.0,
                            entry_chainlink_delta=chainlink_price - price_to_beat,
                            fee_rate=fee_rate,
                            end_time_ms=end_time_ms,
                        )
                        entered_slugs.add(slug)
                        append_event(args.event_jsonl, {"event_type": "entry_filled", **asdict(open_position)})
                else:
                    notes.append(f"entry_book_error={book_error}")

            # Exit: profit/stop if the current book can fully receive the simulated shares.
            unrealized_exit_cash: float | None = None
            if open_position is not None:
                bids, _asks, book_error = fetch_book(open_position.token_id, args.timeout_seconds)
                if not book_error:
                    best_current_bid = bids[0][0] if bids else None
                    if best_current_bid is not None:
                        stress_quote = quote_sell_shares(
                            bids=bids,
                            shares_to_sell=open_position.entry_shares,
                            fee_rate=open_position.fee_rate,
                            min_price=0.0,
                        )
                        unrealized_exit_cash = stress_quote.total_cash

                    exit_type = ""
                    min_exit_price = 0.0
                    if best_current_bid is not None and best_current_bid >= args.profit_target:
                        exit_type = "PROFIT_TARGET"
                        min_exit_price = args.profit_target
                    elif best_current_bid is not None and best_current_bid <= args.stop_loss:
                        exit_type = "STOP_LOSS"
                        min_exit_price = 0.0

                    if exit_type:
                        sell = quote_sell_shares(
                            bids=bids,
                            shares_to_sell=open_position.entry_shares,
                            fee_rate=open_position.fee_rate,
                            min_price=min_exit_price,
                        )
                        if sell.complete and sell.avg_price is not None:
                            final_outcome = ""
                            if chainlink_price is not None:
                                final_outcome = "UP" if chainlink_price >= open_position.price_to_beat else "DOWN"
                            trade_row = close_position_row(
                                open_position,
                                exit_type=exit_type,
                                exit_time_utc=now.isoformat(),
                                exit_second=seconds_since_start if slug == open_position.slug else None,
                                exit_avg_price=sell.avg_price,
                                exit_worst_price=sell.worst_price,
                                exit_notional=sell.notional,
                                exit_fee=sell.fee,
                                exit_cash_after_fee=sell.total_cash,
                                final_outcome=final_outcome,
                                chainlink_price=chainlink_price,
                            )
                            append_csv(args.trades_csv, TRADE_FIELDS, trade_row)
                            append_event(args.event_jsonl, {"event_type": "position_closed", **trade_row})
                            if args.print_events:
                                print(json.dumps({"event_type": "position_closed", **trade_row}, ensure_ascii=False), flush=True)
                            open_position = None
                else:
                    notes.append(f"open_position_book_error={book_error}")

            # Resolution: settle if the market has ended and Chainlink is available.
            if open_position is not None and now_ms >= open_position.end_time_ms and chainlink_price is not None:
                final_outcome = "UP" if chainlink_price >= open_position.price_to_beat else "DOWN"
                payoff = open_position.entry_shares if open_position.direction == final_outcome else 0.0
                trade_row = close_position_row(
                    open_position,
                    exit_type="RESOLUTION",
                    exit_time_utc=now.isoformat(),
                    exit_second=seconds_since_start if slug == open_position.slug else None,
                    exit_avg_price=1.0 if open_position.direction == final_outcome else 0.0,
                    exit_worst_price=1.0 if open_position.direction == final_outcome else 0.0,
                    exit_notional=payoff,
                    exit_fee=0.0,
                    exit_cash_after_fee=payoff,
                    final_outcome=final_outcome,
                    chainlink_price=chainlink_price,
                )
                append_csv(args.trades_csv, TRADE_FIELDS, trade_row)
                append_event(args.event_jsonl, {"event_type": "position_resolved", **trade_row})
                if args.print_events:
                    print(json.dumps({"event_type": "position_resolved", **trade_row}, ensure_ascii=False), flush=True)
                open_position = None

            append_csv(
                args.snapshot_csv,
                SNAPSHOT_FIELDS,
                {
                    "sampled_at_utc": now.isoformat(),
                    "sampled_at_unix_ms": now_ms,
                    "slug": slug,
                    "question": question,
                    "seconds_since_start": maybe_float(seconds_since_start),
                    "seconds_to_end": maybe_float(seconds_to_end),
                    "price_to_beat": maybe_float(price_to_beat),
                    "price_to_beat_status": price_to_beat_status,
                    "binance_btcusdt": maybe_float(binance_price),
                    "coinbase_btcusd": maybe_float(coinbase_price),
                    "chainlink_btcusd": maybe_float(chainlink_price),
                    "binance_delta": maybe_float(binance_delta),
                    "coinbase_delta": maybe_float(coinbase_delta),
                    "signal_direction": signal_direction,
                    "signal_reason": signal_reason,
                    "up_best_bid": maybe_float(best_bid("UP")),
                    "up_best_ask": maybe_float(best_ask("UP")),
                    "down_best_bid": maybe_float(best_bid("DOWN")),
                    "down_best_ask": maybe_float(best_ask("DOWN")),
                    "open_position_slug": open_position.slug if open_position else "",
                    "open_position_direction": open_position.direction if open_position else "",
                    "open_position_shares": round(open_position.entry_shares, 6) if open_position else "",
                    "open_position_entry_total_cash": round(open_position.entry_total_cash, 6) if open_position else "",
                    "open_position_unrealized_exit_cash": maybe_float(unrealized_exit_cash),
                    "notes": "; ".join(notes),
                },
            )

            sleep_for = max(0.0, args.interval_seconds - (time.perf_counter() - loop_started))
            time.sleep(sleep_for)

    finally:
        rtds_cache.stop()

    ended_at = datetime.now(UTC).isoformat()
    append_event(args.event_jsonl, {"event_type": "run_finished", "ended_at_utc": ended_at})
    build_report(args.trades_csv, args.report, started_at, ended_at, args)
    print(f"snapshot_csv={args.snapshot_csv}")
    print(f"event_jsonl={args.event_jsonl}")
    print(f"trades_csv={args.trades_csv}")
    print(f"report={args.report}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
