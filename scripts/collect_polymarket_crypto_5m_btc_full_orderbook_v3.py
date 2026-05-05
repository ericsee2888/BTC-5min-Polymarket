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
    BINANCE_BTCUSDT_URL,
    COINBASE_BTCUSD_URL,
    POLYMARKET_RTDS_WS_URL,
    RtdsPriceCache,
    parse_binance_price,
    parse_coinbase_price,
    parse_float,
    parse_iso_to_unix_ms,
    parse_jsonish_list,
)


DATA_DIR = ROOT / "data"
DEFAULT_SNAPSHOT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.jsonl"
DEFAULT_SNAPSHOT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.csv"
DEFAULT_SIGNAL_EVENTS = DATA_DIR / "polymarket_crypto_5m_btc_signal_events_v3.jsonl"
DEFAULT_SKIP_EVENTS = DATA_DIR / "polymarket_crypto_5m_btc_skip_events_v3.jsonl"
DEFAULT_TRADES_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_paper_trades_v3.csv"

USER_AGENT = "CodexResearch/1.0"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

THRESHOLDS = [25.0, 35.0, 50.0]
ENTRY_STARTS = [15.0, 30.0, 60.0]
ENTRY_CAPS = [0.65, 0.70, 0.75]
ENTRY_LATENCIES_MS = [0, 250, 500, 1000]
ORDER_CASH_AMOUNTS = [50.0, 100.0, 250.0, 500.0]
PROFIT_TARGETS = [0.70, 0.75, 0.80]
STOP_LOSSES = [0.35, 0.40, 0.45]
EXIT_RULES = ["hold_to_resolution"] + [
    f"profit_{int(profit * 1000):03d}_stop_{int(stop * 1000):03d}"
    for profit in PROFIT_TARGETS
    for stop in STOP_LOSSES
]
PROFIT_TARGET_BY_RULE = {
    f"profit_{int(profit * 1000):03d}_stop_{int(stop * 1000):03d}": profit
    for profit in PROFIT_TARGETS
    for stop in STOP_LOSSES
}
STOP_LOSS_BY_RULE = {
    f"profit_{int(profit * 1000):03d}_stop_{int(stop * 1000):03d}": stop
    for profit in PROFIT_TARGETS
    for stop in STOP_LOSSES
}
ENTRY_END_SECONDS = 180.0
DEFAULT_FEE_RATE = 0.072
MAX_BOOK_LEVELS = 50
STOP_REQUESTED = False


SNAPSHOT_CSV_FIELDS = [
    "sampled_at_utc",
    "sampled_at_unix_ms",
    "current_slug",
    "current_question",
    "current_seconds_since_start",
    "current_seconds_to_end",
    "next_slug",
    "price_to_beat",
    "price_to_beat_status",
    "price_to_beat_observed_second",
    "binance_btcusdt",
    "coinbase_btcusd",
    "rtds_binance_btcusdt",
    "chainlink_btcusd",
    "binance_minus_price_to_beat",
    "coinbase_minus_price_to_beat",
    "chainlink_minus_price_to_beat",
    "binance_coinbase_diff_usd",
    "price_source_agreement_direction",
    "signal_25_direction",
    "signal_25_reason",
    "signal_35_direction",
    "signal_35_reason",
    "signal_50_direction",
    "signal_50_reason",
    "up_token_id",
    "down_token_id",
    "up_best_bid",
    "up_best_ask",
    "down_best_bid",
    "down_best_ask",
    "up_bid_levels_count",
    "up_ask_levels_count",
    "down_bid_levels_count",
    "down_ask_levels_count",
    "up_book_error",
    "down_book_error",
    "up_ask_cash_lte_0_65",
    "up_ask_cash_lte_0_70",
    "up_ask_cash_lte_0_75",
    "down_ask_cash_lte_0_65",
    "down_ask_cash_lte_0_70",
    "down_ask_cash_lte_0_75",
    "up_bid_cash_gte_0_35",
    "up_bid_cash_gte_0_70",
    "up_bid_cash_gte_0_75",
    "down_bid_cash_gte_0_35",
    "down_bid_cash_gte_0_70",
    "down_bid_cash_gte_0_75",
    "snapshot_usable_for_formal_backtest",
    "notes",
]


TRADE_CSV_FIELDS = [
    "paper_trade_id",
    "strategy_id",
    "exit_rule",
    "slug",
    "direction",
    "token_id",
    "order_cash_usdc",
    "threshold_usd",
    "entry_start_second",
    "entry_cap",
    "entry_time_utc",
    "entry_second",
    "entry_latency_ms",
    "signal_time_utc",
    "order_arrival_time_utc",
    "signal_best_ask",
    "arrival_best_ask",
    "signal_to_arrival_ask_change",
    "entry_avg_price",
    "entry_worst_price",
    "entry_shares",
    "entry_total_cash_used",
    "entry_fee_usdc",
    "entry_slippage_vs_best_ask",
    "entry_data_source",
    "entry_fill_levels_json",
    "exit_type",
    "exit_time_utc",
    "exit_second",
    "exit_avg_price",
    "exit_worst_price",
    "exit_complete",
    "exit_partial",
    "exit_sold_shares",
    "exit_remaining_shares",
    "exit_cash_after_fee",
    "exit_fee_usdc",
    "exit_fill_levels_json",
    "exit_attempts_json",
    "official_final_outcome",
    "final_outcome_source",
    "final_outcome",
    "correct",
    "pnl_usdc",
    "roi_on_cash",
    "max_favorable_price_seen",
    "max_adverse_price_seen",
    "max_unrealized_pnl",
    "max_unrealized_drawdown",
]


@dataclass
class FillQuote:
    complete: bool
    partial: bool
    filled_cash: float
    unfilled_cash: float
    shares: float
    avg_price: float | None
    worst_price: float | None
    levels_used: int
    fee_usdc: float
    total_cash_used: float
    fill_levels: list[dict[str, float]]


@dataclass
class OpenTrade:
    paper_trade_id: str
    strategy_id: str
    exit_rule: str
    slug: str
    question: str
    direction: str
    token_id: str
    threshold_usd: float
    entry_start_second: float
    entry_cap: float
    order_cash_usdc: float
    entry_time_utc: str
    entry_second: float
    entry_latency_ms: int
    signal_time_utc: str
    order_arrival_time_utc: str
    entry_price_to_beat: float
    entry_binance_delta: float
    entry_coinbase_delta: float
    entry_chainlink_delta: float
    signal_best_ask: float | None
    arrival_best_ask: float
    entry_best_ask: float
    entry_best_bid: float | None
    entry_avg_price: float
    entry_worst_price: float
    entry_shares: float
    entry_total_cash_used: float
    entry_fee_usdc: float
    entry_fill_levels_json: str
    fee_rate: float
    end_time_ms: int
    remaining_shares: float
    realized_exit_cash: float
    realized_exit_fee: float
    exit_attempts: list[dict[str, Any]]
    max_favorable_price_seen: float
    max_adverse_price_seen: float
    max_unrealized_pnl: float
    max_unrealized_drawdown: float


@dataclass
class PendingEntry:
    strategy_id: str
    slug: str
    question: str
    direction: str
    token_id: str
    threshold_usd: float
    entry_start_second: float
    entry_cap: float
    entry_latency_ms: int
    order_cash_usdc: float
    signal_time_utc: str
    signal_unix_ms: int
    due_unix_ms: int
    signal_second: float
    signal_price_to_beat: float
    signal_binance_delta: float
    signal_coinbase_delta: float
    signal_chainlink_delta: float
    signal_best_ask: float | None
    signal_best_bid: float | None
    fee_rate: float
    end_time_ms: int


def handle_stop_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now() -> datetime:
    return datetime.now(UTC)


def fetch_json(url: str, timeout: float) -> tuple[Any | None, float, str]:
    started = time.perf_counter()
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        return payload, round((time.perf_counter() - started) * 1000, 3), ""
    except Exception as exc:  # noqa: BLE001
        return None, round((time.perf_counter() - started) * 1000, 3), f"{type(exc).__name__}: {exc}"


def current_start_ts(now_unix: int | None = None) -> int:
    now_unix = int(time.time()) if now_unix is None else now_unix
    return now_unix - now_unix % 300


def slug_for_start(start_ts: int) -> str:
    return f"btc-updown-5m-{start_ts}"


def fetch_market_by_slug(slug: str, timeout: float) -> tuple[dict[str, Any] | None, float, str]:
    payload, latency_ms, error = fetch_json(f"{GAMMA_MARKETS_URL}?{urlencode({'slug': slug})}", timeout)
    if error:
        return None, latency_ms, error
    if not isinstance(payload, list) or not payload:
        return None, latency_ms, "market_not_found"
    return payload[0], latency_ms, ""


def market_tokens(market: dict[str, Any] | None) -> dict[str, str]:
    if not market:
        return {}
    outcomes = [str(item).upper() for item in parse_jsonish_list(market.get("outcomes"))]
    tokens = [str(item) for item in parse_jsonish_list(market.get("clobTokenIds"))]
    return {outcome: token for outcome, token in zip(outcomes, tokens, strict=False)}


def fee_rate_for_market(market: dict[str, Any] | None) -> float:
    if not market:
        return DEFAULT_FEE_RATE
    fee_schedule = market.get("feeSchedule") or {}
    return parse_float(fee_schedule.get("rate")) or DEFAULT_FEE_RATE


def market_timing(market: dict[str, Any] | None, now_ms: int) -> tuple[float | None, float | None, int | None]:
    if not market:
        return None, None, None
    start_ms = parse_iso_to_unix_ms(str(market.get("eventStartTime") or ""))
    end_ms = parse_iso_to_unix_ms(str(market.get("endDate") or ""))
    since = round((now_ms - start_ms) / 1000, 3) if start_ms else None
    to_end = round((end_ms - now_ms) / 1000, 3) if end_ms else None
    return since, to_end, end_ms


def price_size_levels(levels: list[dict[str, Any]], reverse: bool, limit: int) -> list[dict[str, float]]:
    parsed: list[tuple[float, float]] = []
    for level in levels:
        price = parse_float(level.get("price"))
        size = parse_float(level.get("size"))
        if price is not None and size is not None and price > 0 and size > 0:
            parsed.append((price, size))
    parsed.sort(key=lambda item: item[0], reverse=reverse)
    return [{"price": price, "size": size} for price, size in parsed[:limit]]


def fetch_book(token_id: str, timeout: float, levels: int) -> dict[str, Any]:
    payload, latency_ms, error = fetch_json(f"{CLOB_BOOK_URL}?{urlencode({'token_id': token_id})}", timeout)
    if error or not isinstance(payload, dict):
        return {
            "token_id": token_id,
            "book_fetch_latency_ms": latency_ms,
            "book_error": error or "unexpected_book_response",
            "bids": [],
            "asks": [],
            "best_bid": None,
            "best_ask": None,
            "spread": None,
            "mid": None,
            "book_timestamp_ms": None,
            "book_hash": "",
            "last_trade_price": None,
            "min_order_size": None,
            "tick_size": None,
        }
    bids = price_size_levels(payload.get("bids") or [], reverse=True, limit=levels)
    asks = price_size_levels(payload.get("asks") or [], reverse=False, limit=levels)
    best_bid = bids[0]["price"] if bids else None
    best_ask = asks[0]["price"] if asks else None
    spread = best_ask - best_bid if best_bid is not None and best_ask is not None else None
    mid = (best_bid + best_ask) / 2 if best_bid is not None and best_ask is not None else None
    return {
        "token_id": token_id,
        "book_fetch_latency_ms": latency_ms,
        "book_error": "",
        "bids": bids,
        "asks": asks,
        "best_bid": best_bid,
        "best_ask": best_ask,
        "spread": spread,
        "mid": mid,
        "book_timestamp_ms": payload.get("timestamp"),
        "book_hash": payload.get("hash") or "",
        "last_trade_price": parse_float(payload.get("last_trade_price")),
        "min_order_size": parse_float(payload.get("min_order_size")),
        "tick_size": parse_float(payload.get("tick_size")),
    }


def depth_cash_asks_lte(asks: list[dict[str, float]], cap: float) -> float:
    return sum(level["price"] * level["size"] for level in asks if level["price"] <= cap)


def depth_shares_asks_lte(asks: list[dict[str, float]], cap: float) -> float:
    return sum(level["size"] for level in asks if level["price"] <= cap)


def depth_cash_bids_gte(bids: list[dict[str, float]], floor: float) -> float:
    return sum(level["price"] * level["size"] for level in bids if level["price"] >= floor)


def depth_shares_bids_gte(bids: list[dict[str, float]], floor: float) -> float:
    return sum(level["size"] for level in bids if level["price"] >= floor)


def near_cash(levels: list[dict[str, float]], low: float = 0.45, high: float = 0.55) -> float:
    return sum(level["price"] * level["size"] for level in levels if low <= level["price"] <= high)


def taker_fee_per_share(price: float, fee_rate: float) -> float:
    return fee_rate * price * (1 - price)


def quote_buy_with_total_cash(
    asks: list[dict[str, float]],
    cash_budget: float,
    fee_rate: float,
    max_price: float,
) -> FillQuote:
    remaining_cash = cash_budget
    shares = 0.0
    notional = 0.0
    fee_total = 0.0
    worst_price: float | None = None
    fill_levels: list[dict[str, float]] = []
    for level in asks:
        price = level["price"]
        if price > max_price or remaining_cash <= 1e-9:
            continue
        fee_per_share = taker_fee_per_share(price, fee_rate)
        cash_per_share = price + fee_per_share
        if cash_per_share <= 0:
            continue
        available_shares = level["size"]
        use_shares = min(available_shares, remaining_cash / cash_per_share)
        if use_shares <= 0:
            continue
        level_cash = use_shares * price
        level_fee = use_shares * fee_per_share
        shares += use_shares
        notional += level_cash
        fee_total += level_fee
        remaining_cash -= level_cash + level_fee
        worst_price = price
        fill_levels.append(
            {
                "price": round(price, 6),
                "shares": round(use_shares, 6),
                "cash": round(level_cash, 6),
                "fee": round(level_fee, 6),
            }
        )
    total_used = notional + fee_total
    complete = remaining_cash <= max(1.0, cash_budget * 0.001)
    partial = shares > 0 and not complete
    avg_price = notional / shares if shares else None
    return FillQuote(
        complete=complete,
        partial=partial,
        filled_cash=notional,
        unfilled_cash=max(0.0, remaining_cash),
        shares=shares,
        avg_price=avg_price,
        worst_price=worst_price,
        levels_used=len(fill_levels),
        fee_usdc=fee_total,
        total_cash_used=total_used,
        fill_levels=fill_levels,
    )


def quote_sell_shares(
    bids: list[dict[str, float]],
    shares_to_sell: float,
    fee_rate: float,
    min_price: float,
) -> FillQuote:
    remaining_shares = shares_to_sell
    sold_shares = 0.0
    notional = 0.0
    fee_total = 0.0
    worst_price: float | None = None
    fill_levels: list[dict[str, float]] = []
    for level in bids:
        price = level["price"]
        if price < min_price or remaining_shares <= 1e-9:
            continue
        use_shares = min(level["size"], remaining_shares)
        if use_shares <= 0:
            continue
        level_cash = use_shares * price
        level_fee = use_shares * taker_fee_per_share(price, fee_rate)
        sold_shares += use_shares
        notional += level_cash
        fee_total += level_fee
        remaining_shares -= use_shares
        worst_price = price
        fill_levels.append(
            {
                "price": round(price, 6),
                "shares": round(use_shares, 6),
                "cash": round(level_cash, 6),
                "fee": round(level_fee, 6),
            }
        )
    cash_after_fee = notional - fee_total
    complete = remaining_shares <= max(0.0001, shares_to_sell * 0.001)
    partial = sold_shares > 0 and not complete
    avg_price = notional / sold_shares if sold_shares else None
    return FillQuote(
        complete=complete,
        partial=partial,
        filled_cash=notional,
        unfilled_cash=0.0 if complete else remaining_shares,
        shares=sold_shares,
        avg_price=avg_price,
        worst_price=worst_price,
        levels_used=len(fill_levels),
        fee_usdc=fee_total,
        total_cash_used=cash_after_fee,
        fill_levels=fill_levels,
    )


def price_to_beat_status(seconds_since_start: float | None) -> str:
    if seconds_since_start is None:
        return "missing_market_time"
    if seconds_since_start <= 5:
        return "early_0_5s"
    if seconds_since_start <= 15:
        return "early_but_late_5s"
    return "late_price_to_beat_skip_formal_backtest"


def determine_signal(
    binance: float | None,
    coinbase: float | None,
    price_to_beat: float | None,
    threshold: float,
) -> tuple[str, str, float | None, float | None]:
    if price_to_beat is None:
        return "", "missing_price_to_beat", None, None
    if binance is None:
        return "", "missing_binance", None, None
    if coinbase is None:
        return "", "missing_coinbase", None, None
    binance_delta = binance - price_to_beat
    coinbase_delta = coinbase - price_to_beat
    if binance_delta >= threshold and coinbase_delta >= threshold:
        return "UP", "above_threshold", binance_delta, coinbase_delta
    if binance_delta <= -threshold and coinbase_delta <= -threshold:
        return "DOWN", "below_threshold", binance_delta, coinbase_delta
    if (binance_delta > 0 > coinbase_delta) or (coinbase_delta > 0 > binance_delta):
        return "", "price_sources_disagree", binance_delta, coinbase_delta
    return "", "threshold_not_met", binance_delta, coinbase_delta


def append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


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


def rounded(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def strategy_id(threshold: float, start: float, cap: float, cash: float, latency_ms: int) -> str:
    return f"thr{threshold:g}_start{start:g}_cap{cap:g}_cash{cash:g}_lat{latency_ms:g}ms"


def trade_id(slug: str, strategy: str, exit_rule: str) -> str:
    return f"{slug}:{strategy}:{exit_rule}"


def close_trade_row(
    trade: OpenTrade,
    exit_type: str,
    exit_time_utc: str,
    exit_second: float | None,
    exit_avg_price: float | None,
    exit_worst_price: float | None,
    exit_cash_after_fee: float,
    exit_fee: float,
    exit_complete: bool,
    exit_partial: bool,
    exit_sold_shares: float,
    exit_remaining_shares: float,
    final_outcome: str,
    exit_fill_levels: list[dict[str, float]],
) -> dict[str, Any]:
    pnl = exit_cash_after_fee - trade.entry_total_cash_used
    row = {
        "paper_trade_id": trade.paper_trade_id,
        "strategy_id": trade.strategy_id,
        "exit_rule": trade.exit_rule,
        "slug": trade.slug,
        "direction": trade.direction,
        "token_id": trade.token_id,
        "order_cash_usdc": trade.order_cash_usdc,
        "threshold_usd": trade.threshold_usd,
        "entry_start_second": trade.entry_start_second,
        "entry_cap": trade.entry_cap,
        "entry_time_utc": trade.entry_time_utc,
        "entry_second": rounded(trade.entry_second, 3),
        "entry_latency_ms": trade.entry_latency_ms,
        "signal_time_utc": trade.signal_time_utc,
        "order_arrival_time_utc": trade.order_arrival_time_utc,
        "signal_best_ask": rounded(trade.signal_best_ask, 6),
        "arrival_best_ask": rounded(trade.arrival_best_ask, 6),
        "signal_to_arrival_ask_change": rounded(trade.arrival_best_ask - trade.signal_best_ask, 6) if trade.signal_best_ask is not None else "",
        "entry_price_to_beat": rounded(trade.entry_price_to_beat, 6),
        "entry_binance_delta": rounded(trade.entry_binance_delta, 6),
        "entry_coinbase_delta": rounded(trade.entry_coinbase_delta, 6),
        "entry_chainlink_delta": rounded(trade.entry_chainlink_delta, 6),
        "entry_best_ask": rounded(trade.entry_best_ask, 6),
        "entry_best_bid": rounded(trade.entry_best_bid, 6),
        "entry_avg_price": rounded(trade.entry_avg_price, 6),
        "entry_worst_price": rounded(trade.entry_worst_price, 6),
        "entry_shares": rounded(trade.entry_shares, 6),
        "entry_total_cash_used": rounded(trade.entry_total_cash_used, 6),
        "entry_fee_usdc": rounded(trade.entry_fee_usdc, 6),
        "entry_fill_levels_json": trade.entry_fill_levels_json,
        "entry_slippage_vs_best_ask": rounded(trade.entry_avg_price - trade.entry_best_ask, 6),
        "entry_data_source": "rest_polling",
        "exit_type": exit_type,
        "exit_time_utc": exit_time_utc,
        "exit_second": rounded(exit_second, 3),
        "exit_avg_price": rounded(exit_avg_price, 6),
        "exit_worst_price": rounded(exit_worst_price, 6),
        "exit_complete": exit_complete,
        "exit_partial": exit_partial,
        "exit_sold_shares": rounded(exit_sold_shares, 6),
        "exit_remaining_shares": rounded(exit_remaining_shares, 6),
        "exit_cash_after_fee": rounded(exit_cash_after_fee, 6),
        "exit_fee_usdc": rounded(exit_fee, 6),
        "exit_fill_levels_json": json.dumps(exit_fill_levels, ensure_ascii=False),
        "exit_attempts_json": json.dumps(trade.exit_attempts, ensure_ascii=False),
        "official_final_outcome": "",
        "final_outcome_source": "chainlink_inferred",
        "final_outcome": final_outcome,
        "correct": trade.direction == final_outcome if final_outcome else "",
        "pnl_usdc": rounded(pnl, 6),
        "roi_on_cash": rounded(pnl / trade.entry_total_cash_used, 6) if trade.entry_total_cash_used else "",
        "max_favorable_price_seen": rounded(trade.max_favorable_price_seen, 6),
        "max_adverse_price_seen": rounded(trade.max_adverse_price_seen, 6),
        "max_unrealized_pnl": rounded(trade.max_unrealized_pnl, 6),
        "max_unrealized_drawdown": rounded(trade.max_unrealized_drawdown, 6),
    }
    return row


def update_open_trade_marks(trade: OpenTrade, best_bid: float | None) -> None:
    if best_bid is None:
        return
    trade.max_favorable_price_seen = max(trade.max_favorable_price_seen, best_bid)
    trade.max_adverse_price_seen = min(trade.max_adverse_price_seen, best_bid)
    unrealized_cash = trade.realized_exit_cash + trade.remaining_shares * best_bid
    unrealized_pnl = unrealized_cash - trade.entry_total_cash_used
    trade.max_unrealized_pnl = max(trade.max_unrealized_pnl, unrealized_pnl)
    trade.max_unrealized_drawdown = min(trade.max_unrealized_drawdown, unrealized_pnl)


def book_for_direction(direction: str, up_book: dict[str, Any], down_book: dict[str, Any]) -> dict[str, Any]:
    return up_book if direction == "UP" else down_book


def build_depth_summary(prefix: str, book: dict[str, Any]) -> dict[str, Any]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    near_bid = near_cash(bids)
    near_ask = near_cash(asks)
    imbalance = (near_bid - near_ask) / (near_bid + near_ask) if (near_bid + near_ask) else None
    return {
        f"{prefix}_best_bid": rounded(book.get("best_bid")),
        f"{prefix}_best_ask": rounded(book.get("best_ask")),
        f"{prefix}_spread": rounded(book.get("spread")),
        f"{prefix}_mid": rounded(book.get("mid")),
        f"{prefix}_book_timestamp_ms": book.get("book_timestamp_ms"),
        f"{prefix}_book_hash": book.get("book_hash") or "",
        f"{prefix}_book_fetch_latency_ms": book.get("book_fetch_latency_ms"),
        f"{prefix}_book_error": book.get("book_error") or "",
        f"{prefix}_bid_levels_count": len(bids),
        f"{prefix}_ask_levels_count": len(asks),
        f"{prefix}_ask_cash_lte_0_55": round(depth_cash_asks_lte(asks, 0.55), 6),
        f"{prefix}_ask_cash_lte_0_60": round(depth_cash_asks_lte(asks, 0.60), 6),
        f"{prefix}_ask_cash_lte_0_65": round(depth_cash_asks_lte(asks, 0.65), 6),
        f"{prefix}_ask_cash_lte_0_70": round(depth_cash_asks_lte(asks, 0.70), 6),
        f"{prefix}_ask_cash_lte_0_75": round(depth_cash_asks_lte(asks, 0.75), 6),
        f"{prefix}_ask_shares_lte_0_65": round(depth_shares_asks_lte(asks, 0.65), 6),
        f"{prefix}_ask_shares_lte_0_70": round(depth_shares_asks_lte(asks, 0.70), 6),
        f"{prefix}_ask_shares_lte_0_75": round(depth_shares_asks_lte(asks, 0.75), 6),
        f"{prefix}_bid_cash_gte_0_35": round(depth_cash_bids_gte(bids, 0.35), 6),
        f"{prefix}_bid_cash_gte_0_50": round(depth_cash_bids_gte(bids, 0.50), 6),
        f"{prefix}_bid_cash_gte_0_60": round(depth_cash_bids_gte(bids, 0.60), 6),
        f"{prefix}_bid_cash_gte_0_70": round(depth_cash_bids_gte(bids, 0.70), 6),
        f"{prefix}_bid_cash_gte_0_75": round(depth_cash_bids_gte(bids, 0.75), 6),
        f"{prefix}_bid_shares_gte_0_35": round(depth_shares_bids_gte(bids, 0.35), 6),
        f"{prefix}_bid_shares_gte_0_50": round(depth_shares_bids_gte(bids, 0.50), 6),
        f"{prefix}_bid_shares_gte_0_70": round(depth_shares_bids_gte(bids, 0.70), 6),
        f"{prefix}_bid_shares_gte_0_75": round(depth_shares_bids_gte(bids, 0.75), 6),
        f"{prefix}_near_bid_cash_0_45_0_55": round(near_bid, 6),
        f"{prefix}_near_ask_cash_0_45_0_55": round(near_ask, 6),
        f"{prefix}_depth_imbalance_near_mid": rounded(imbalance),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--interval-seconds", type=float, default=1.0)
    parser.add_argument("--duration-seconds", type=float, default=0.0)
    parser.add_argument("--samples", type=int, default=0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--rtds-warmup-seconds", type=float, default=3.0)
    parser.add_argument("--book-levels", type=int, default=MAX_BOOK_LEVELS)
    parser.add_argument("--snapshot-jsonl", type=Path, default=DEFAULT_SNAPSHOT_JSONL)
    parser.add_argument("--snapshot-csv", type=Path, default=DEFAULT_SNAPSHOT_CSV)
    parser.add_argument("--signal-events", type=Path, default=DEFAULT_SIGNAL_EVENTS)
    parser.add_argument("--skip-events", type=Path, default=DEFAULT_SKIP_EVENTS)
    parser.add_argument("--trades-jsonl", type=Path, default=DEFAULT_TRADES_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--reset-output", action="store_true")
    parser.add_argument("--print-each", action="store_true")
    return parser.parse_args()


def main() -> int:
    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    args = parse_args()

    if args.reset_output:
        for path in [
            args.snapshot_jsonl,
            args.snapshot_csv,
            args.signal_events,
            args.skip_events,
            args.trades_jsonl,
            args.trades_csv,
        ]:
            path.unlink(missing_ok=True)

    ensure_csv(args.snapshot_csv, SNAPSHOT_CSV_FIELDS)
    ensure_csv(args.trades_csv, TRADE_CSV_FIELDS)

    rtds_cache = RtdsPriceCache()
    rtds_cache.start()
    time.sleep(max(0.0, args.rtds_warmup_seconds))

    price_to_beat_by_slug: dict[str, dict[str, Any]] = {}
    attempted_entries: set[str] = set()
    pending_entries: dict[str, PendingEntry] = {}
    open_trades: dict[str, OpenTrade] = {}
    collected = 0
    started_monotonic = time.monotonic()

    try:
        while not STOP_REQUESTED:
            loop_started = time.perf_counter()
            now = utc_now()
            now_ms = int(now.timestamp() * 1000)
            start_ts = current_start_ts(now_ms // 1000)
            current_slug = slug_for_start(start_ts)
            next_slug = slug_for_start(start_ts + 300)

            if args.duration_seconds > 0 and time.monotonic() - started_monotonic >= args.duration_seconds:
                break
            if args.samples > 0 and collected >= args.samples:
                break

            with ThreadPoolExecutor(max_workers=8) as executor:
                binance_future = executor.submit(fetch_json, BINANCE_BTCUSDT_URL, args.timeout_seconds)
                coinbase_future = executor.submit(fetch_json, COINBASE_BTCUSD_URL, args.timeout_seconds)
                current_market_future = executor.submit(fetch_market_by_slug, current_slug, args.timeout_seconds)
                next_market_future = executor.submit(fetch_market_by_slug, next_slug, args.timeout_seconds)

                binance_payload, binance_latency_ms, binance_error = binance_future.result()
                coinbase_payload, coinbase_latency_ms, coinbase_error = coinbase_future.result()
                current_market, current_market_latency_ms, current_market_error = current_market_future.result()
                next_market, next_market_latency_ms, next_market_error = next_market_future.result()

            binance_price = None if binance_error else parse_binance_price(binance_payload)
            coinbase_price = None if coinbase_error else parse_coinbase_price(coinbase_payload)
            rtds = rtds_cache.snapshot()
            rtds_binance = rtds.get("rtds_binance_btcusdt")
            chainlink_price = rtds.get("chainlink_btcusd")
            chainlink_ts = rtds.get("chainlink_timestamp_ms")

            current_since, current_to_end, current_end_ms = market_timing(current_market, now_ms)
            next_since, next_to_end, _next_end_ms = market_timing(next_market, now_ms)
            current_tokens = market_tokens(current_market)
            next_tokens = market_tokens(next_market)
            fee_rate = fee_rate_for_market(current_market)

            if current_slug and chainlink_price is not None and current_slug not in price_to_beat_by_slug:
                status = price_to_beat_status(current_since)
                price_to_beat_by_slug[current_slug] = {
                    "price": chainlink_price,
                    "source": "rtds_chainlink_first_seen_for_window",
                    "observed_at_utc": now.isoformat(),
                    "observed_second": current_since,
                    "status": status,
                }

            price_meta = price_to_beat_by_slug.get(current_slug, {})
            price_to_beat = price_meta.get("price")
            price_status = price_meta.get("status", "")
            price_observed_second = price_meta.get("observed_second")
            usable_for_formal = price_status in {"early_0_5s", "early_but_late_5s"}

            with ThreadPoolExecutor(max_workers=4) as executor:
                up_book_future = executor.submit(
                    fetch_book,
                    current_tokens.get("UP", ""),
                    args.timeout_seconds,
                    args.book_levels,
                )
                down_book_future = executor.submit(
                    fetch_book,
                    current_tokens.get("DOWN", ""),
                    args.timeout_seconds,
                    args.book_levels,
                )
                next_up_book_future = executor.submit(
                    fetch_book,
                    next_tokens.get("UP", ""),
                    args.timeout_seconds,
                    args.book_levels,
                )
                next_down_book_future = executor.submit(
                    fetch_book,
                    next_tokens.get("DOWN", ""),
                    args.timeout_seconds,
                    args.book_levels,
                )
                up_book = up_book_future.result() if current_tokens.get("UP") else {"bids": [], "asks": [], "book_error": "missing_up_token"}
                down_book = down_book_future.result() if current_tokens.get("DOWN") else {"bids": [], "asks": [], "book_error": "missing_down_token"}
                next_up_book = next_up_book_future.result() if next_tokens.get("UP") else {"bids": [], "asks": [], "book_error": "missing_next_up_token"}
                next_down_book = next_down_book_future.result() if next_tokens.get("DOWN") else {"bids": [], "asks": [], "book_error": "missing_next_down_token"}

            binance_delta = binance_price - price_to_beat if binance_price is not None and price_to_beat is not None else None
            coinbase_delta = coinbase_price - price_to_beat if coinbase_price is not None and price_to_beat is not None else None
            chainlink_delta = chainlink_price - price_to_beat if chainlink_price is not None and price_to_beat is not None else None
            binance_coinbase_diff = binance_price - coinbase_price if binance_price is not None and coinbase_price is not None else None
            agreement_direction = ""
            if binance_delta is not None and coinbase_delta is not None:
                if binance_delta > 0 and coinbase_delta > 0:
                    agreement_direction = "UP"
                elif binance_delta < 0 and coinbase_delta < 0:
                    agreement_direction = "DOWN"
                else:
                    agreement_direction = "DISAGREE"

            signal_by_threshold: dict[float, tuple[str, str, float | None, float | None]] = {}
            for threshold in THRESHOLDS:
                signal_by_threshold[threshold] = determine_signal(
                    binance_price,
                    coinbase_price,
                    price_to_beat,
                    threshold,
                )

            snapshot: dict[str, Any] = {
                "sampled_at_utc": now.isoformat(),
                "sampled_at_unix_ms": now_ms,
                "current_slug": current_slug,
                "current_question": current_market.get("question") if current_market else "",
                "current_event_start_time": current_market.get("eventStartTime") if current_market else "",
                "current_end_time": current_market.get("endDate") if current_market else "",
                "current_seconds_since_start": current_since,
                "current_seconds_to_end": current_to_end,
                "current_market_error": current_market_error,
                "current_market_latency_ms": current_market_latency_ms,
                "next_slug": next_slug,
                "next_question": next_market.get("question") if next_market else "",
                "next_event_start_time": next_market.get("eventStartTime") if next_market else "",
                "next_end_time": next_market.get("endDate") if next_market else "",
                "next_seconds_since_start": next_since,
                "next_seconds_to_end": next_to_end,
                "next_market_error": next_market_error,
                "next_market_latency_ms": next_market_latency_ms,
                "price_to_beat": price_to_beat,
                "price_to_beat_source": price_meta.get("source", ""),
                "price_to_beat_observed_at_utc": price_meta.get("observed_at_utc", ""),
                "price_to_beat_observed_second": price_observed_second,
                "price_to_beat_status": price_status,
                "price_to_beat_is_early": usable_for_formal,
                "binance_btcusdt": binance_price,
                "binance_timestamp_ms": now_ms,
                "binance_latency_ms": binance_latency_ms,
                "binance_error": binance_error,
                "coinbase_btcusd": coinbase_price,
                "coinbase_timestamp_ms": now_ms,
                "coinbase_latency_ms": coinbase_latency_ms,
                "coinbase_error": coinbase_error,
                "rtds_binance_btcusdt": rtds_binance,
                "rtds_binance_timestamp_ms": rtds.get("rtds_binance_timestamp_ms"),
                "chainlink_btcusd": chainlink_price,
                "chainlink_timestamp_ms": chainlink_ts,
                "chainlink_latency_ms_or_age_ms": now_ms - int(chainlink_ts) if chainlink_ts else None,
                "rtds_error": rtds.get("rtds_error") or "",
                "binance_minus_price_to_beat": binance_delta,
                "coinbase_minus_price_to_beat": coinbase_delta,
                "chainlink_minus_price_to_beat": chainlink_delta,
                "binance_coinbase_diff_usd": binance_coinbase_diff,
                "binance_coinbase_diff_bps": (
                    (binance_coinbase_diff / ((binance_price + coinbase_price) / 2)) * 10000
                    if binance_coinbase_diff is not None and binance_price and coinbase_price
                    else None
                ),
                "price_source_agreement_direction": agreement_direction,
                "price_source_agreement_strength": min(abs(binance_delta), abs(coinbase_delta)) if binance_delta is not None and coinbase_delta is not None else None,
                "up_token_id": current_tokens.get("UP", ""),
                "down_token_id": current_tokens.get("DOWN", ""),
                "next_up_token_id": next_tokens.get("UP", ""),
                "next_down_token_id": next_tokens.get("DOWN", ""),
                "up_bids": up_book.get("bids", []),
                "up_asks": up_book.get("asks", []),
                "down_bids": down_book.get("bids", []),
                "down_asks": down_book.get("asks", []),
                "next_up_bids": next_up_book.get("bids", []),
                "next_up_asks": next_up_book.get("asks", []),
                "next_down_bids": next_down_book.get("bids", []),
                "next_down_asks": next_down_book.get("asks", []),
                "snapshot_usable_for_formal_backtest": usable_for_formal and not up_book.get("book_error") and not down_book.get("book_error"),
                "notes": "",
            }
            snapshot.update(build_depth_summary("up", up_book))
            snapshot.update(build_depth_summary("down", down_book))
            snapshot.update(build_depth_summary("next_up", next_up_book))
            snapshot.update(build_depth_summary("next_down", next_down_book))

            for threshold, (direction, reason, _bd, _cd) in signal_by_threshold.items():
                key = str(int(threshold))
                snapshot[f"signal_{key}_direction"] = direction
                snapshot[f"signal_{key}_reason"] = reason

            append_jsonl(args.snapshot_jsonl, snapshot)
            append_csv(args.snapshot_csv, SNAPSHOT_CSV_FIELDS, snapshot)

            # Record every threshold signal even when it is not tradable.
            if current_since is not None:
                for threshold in THRESHOLDS:
                    direction, reason, bd, cd = signal_by_threshold[threshold]
                    if not direction:
                        continue
                    book = book_for_direction(direction, up_book, down_book)
                    append_jsonl(
                        args.signal_events,
                        {
                            "event_type": "signal",
                            "sampled_at_utc": now.isoformat(),
                            "slug": current_slug,
                            "threshold_usd": threshold,
                            "direction": direction,
                            "reason": reason,
                            "seconds_since_start": current_since,
                            "best_ask": book.get("best_ask"),
                            "best_bid": book.get("best_bid"),
                            "price_to_beat": price_to_beat,
                            "binance_delta": bd,
                            "coinbase_delta": cd,
                            "chainlink_delta": chainlink_delta,
                        },
                    )

            # Evaluate entries for all strategy combinations.
            if current_since is not None and current_end_ms:
                for threshold in THRESHOLDS:
                    direction, reason, bd, cd = signal_by_threshold[threshold]
                    if not direction:
                        continue
                    book = book_for_direction(direction, up_book, down_book)
                    token_id = current_tokens.get(direction, "")
                    best_ask = book.get("best_ask")
                    best_bid = book.get("best_bid")
                    for entry_start in ENTRY_STARTS:
                        for cap in ENTRY_CAPS:
                            for cash in ORDER_CASH_AMOUNTS:
                                for latency_ms in ENTRY_LATENCIES_MS:
                                    sid = strategy_id(threshold, entry_start, cap, cash, latency_ms)
                                    attempt_key = f"{current_slug}:{sid}"
                                    if attempt_key in attempted_entries or attempt_key in pending_entries:
                                        continue
                                    skip_base = {
                                        "event_type": "skip",
                                        "sampled_at_utc": now.isoformat(),
                                        "strategy_id": sid,
                                        "skip_slug": current_slug,
                                        "skip_time": now.isoformat(),
                                        "skip_direction": direction,
                                        "threshold_usd": threshold,
                                        "entry_start_second": entry_start,
                                        "entry_cap": cap,
                                        "entry_latency_ms": latency_ms,
                                        "order_cash_usdc": cash,
                                        "target_ask": best_ask,
                                        "available_cash_depth": depth_cash_asks_lte(book.get("asks", []), cap),
                                        "required_cash": cash,
                                    }
                                    if current_since < entry_start:
                                        continue
                                    if current_since > ENTRY_END_SECONDS:
                                        append_jsonl(args.skip_events, {**skip_base, "skip_reason": "outside_entry_window"})
                                        attempted_entries.add(attempt_key)
                                        continue
                                    if price_status == "late_price_to_beat_skip_formal_backtest":
                                        append_jsonl(args.skip_events, {**skip_base, "skip_reason": "late_price_to_beat"})
                                        attempted_entries.add(attempt_key)
                                        continue
                                    if book.get("book_error") or not token_id:
                                        append_jsonl(args.skip_events, {**skip_base, "skip_reason": "missing_orderbook"})
                                        attempted_entries.add(attempt_key)
                                        continue
                                    if best_ask is None or best_ask > cap:
                                        append_jsonl(args.skip_events, {**skip_base, "skip_reason": "target_ask_above_cap"})
                                        attempted_entries.add(attempt_key)
                                        continue
                                    pending_entries[attempt_key] = PendingEntry(
                                        strategy_id=sid,
                                        slug=current_slug,
                                        question=current_market.get("question") if current_market else "",
                                        direction=direction,
                                        token_id=token_id,
                                        threshold_usd=threshold,
                                        entry_start_second=entry_start,
                                        entry_cap=cap,
                                        entry_latency_ms=latency_ms,
                                        order_cash_usdc=cash,
                                        signal_time_utc=now.isoformat(),
                                        signal_unix_ms=now_ms,
                                        due_unix_ms=now_ms + latency_ms,
                                        signal_second=current_since,
                                        signal_price_to_beat=float(price_to_beat),
                                        signal_binance_delta=float(bd or 0.0),
                                        signal_coinbase_delta=float(cd or 0.0),
                                        signal_chainlink_delta=float(chainlink_delta or 0.0),
                                        signal_best_ask=best_ask,
                                        signal_best_bid=best_bid,
                                        fee_rate=fee_rate,
                                        end_time_ms=current_end_ms,
                                    )

            # Execute pending taker entries after the configured signal-to-order latency.
            for attempt_key, pending in list(pending_entries.items()):
                if now_ms < pending.due_unix_ms:
                    continue
                attempted_entries.add(attempt_key)
                del pending_entries[attempt_key]
                if pending.slug != current_slug:
                    append_jsonl(
                        args.skip_events,
                        {
                            "event_type": "skip",
                            "sampled_at_utc": now.isoformat(),
                            "strategy_id": pending.strategy_id,
                            "skip_slug": pending.slug,
                            "skip_time": now.isoformat(),
                            "skip_direction": pending.direction,
                            "threshold_usd": pending.threshold_usd,
                            "entry_start_second": pending.entry_start_second,
                            "entry_cap": pending.entry_cap,
                            "entry_latency_ms": pending.entry_latency_ms,
                            "order_cash_usdc": pending.order_cash_usdc,
                            "skip_reason": "market_too_close_to_end",
                        },
                    )
                    continue
                arrival_signal = signal_by_threshold.get(pending.threshold_usd)
                if arrival_signal and arrival_signal[0] and arrival_signal[0] != pending.direction:
                    append_jsonl(
                        args.skip_events,
                        {
                            "event_type": "skip",
                            "sampled_at_utc": now.isoformat(),
                            "strategy_id": pending.strategy_id,
                            "skip_slug": pending.slug,
                            "skip_time": now.isoformat(),
                            "skip_direction": pending.direction,
                            "threshold_usd": pending.threshold_usd,
                            "entry_start_second": pending.entry_start_second,
                            "entry_cap": pending.entry_cap,
                            "entry_latency_ms": pending.entry_latency_ms,
                            "order_cash_usdc": pending.order_cash_usdc,
                            "skip_reason": "latency_direction_changed",
                        },
                    )
                    continue
                book = book_for_direction(pending.direction, up_book, down_book)
                best_ask = book.get("best_ask")
                best_bid = book.get("best_bid")
                skip_base = {
                    "event_type": "skip",
                    "sampled_at_utc": now.isoformat(),
                    "strategy_id": pending.strategy_id,
                    "skip_slug": pending.slug,
                    "skip_time": now.isoformat(),
                    "skip_direction": pending.direction,
                    "threshold_usd": pending.threshold_usd,
                    "entry_start_second": pending.entry_start_second,
                    "entry_cap": pending.entry_cap,
                    "entry_latency_ms": pending.entry_latency_ms,
                    "order_cash_usdc": pending.order_cash_usdc,
                    "target_ask": best_ask,
                    "available_cash_depth": depth_cash_asks_lte(book.get("asks", []), pending.entry_cap),
                    "required_cash": pending.order_cash_usdc,
                    "signal_best_ask": pending.signal_best_ask,
                    "arrival_best_ask": best_ask,
                    "order_arrival_lag_ms": now_ms - pending.signal_unix_ms,
                }
                if book.get("book_error") or not pending.token_id:
                    append_jsonl(args.skip_events, {**skip_base, "skip_reason": "missing_orderbook"})
                    continue
                if best_ask is None or best_ask > pending.entry_cap:
                    reason_key = "latency_moved_ask_above_cap" if pending.entry_latency_ms else "target_ask_above_cap"
                    append_jsonl(args.skip_events, {**skip_base, "skip_reason": reason_key})
                    continue
                fill = quote_buy_with_total_cash(book.get("asks", []), pending.order_cash_usdc, pending.fee_rate, pending.entry_cap)
                if not fill.complete or fill.avg_price is None or fill.worst_price is None:
                    reason_key = "latency_depth_disappeared" if pending.entry_latency_ms else f"insufficient_depth_for_{int(pending.order_cash_usdc)}"
                    append_jsonl(
                        args.skip_events,
                        {
                            **skip_base,
                            "skip_reason": reason_key,
                            "entry_partial": fill.partial,
                            "entry_filled_cash": fill.filled_cash,
                            "entry_unfilled_cash": fill.unfilled_cash,
                            "entry_shares": fill.shares,
                            "entry_fill_levels_json": json.dumps(fill.fill_levels, ensure_ascii=False),
                        },
                    )
                    continue
                for exit_rule in EXIT_RULES:
                    tid = trade_id(current_slug, pending.strategy_id, exit_rule)
                    open_trades[tid] = OpenTrade(
                        paper_trade_id=tid,
                        strategy_id=pending.strategy_id,
                        exit_rule=exit_rule,
                        slug=current_slug,
                        question=pending.question,
                        direction=pending.direction,
                        token_id=pending.token_id,
                        threshold_usd=pending.threshold_usd,
                        entry_start_second=pending.entry_start_second,
                        entry_cap=pending.entry_cap,
                        order_cash_usdc=pending.order_cash_usdc,
                        entry_time_utc=now.isoformat(),
                        entry_second=current_since,
                        entry_latency_ms=pending.entry_latency_ms,
                        signal_time_utc=pending.signal_time_utc,
                        order_arrival_time_utc=now.isoformat(),
                        entry_price_to_beat=pending.signal_price_to_beat,
                        entry_binance_delta=pending.signal_binance_delta,
                        entry_coinbase_delta=pending.signal_coinbase_delta,
                        entry_chainlink_delta=pending.signal_chainlink_delta,
                        signal_best_ask=pending.signal_best_ask,
                        arrival_best_ask=float(best_ask),
                        entry_best_ask=float(best_ask),
                        entry_best_bid=best_bid,
                        entry_avg_price=float(fill.avg_price),
                        entry_worst_price=float(fill.worst_price),
                        entry_shares=fill.shares,
                        entry_total_cash_used=fill.total_cash_used,
                        entry_fee_usdc=fill.fee_usdc,
                        entry_fill_levels_json=json.dumps(fill.fill_levels, ensure_ascii=False),
                        fee_rate=pending.fee_rate,
                        end_time_ms=pending.end_time_ms,
                        remaining_shares=fill.shares,
                        realized_exit_cash=0.0,
                        realized_exit_fee=0.0,
                        exit_attempts=[],
                        max_favorable_price_seen=best_bid or fill.avg_price,
                        max_adverse_price_seen=best_bid or fill.avg_price,
                        max_unrealized_pnl=0.0,
                        max_unrealized_drawdown=0.0,
                    )

            # Monitor and close open trades.
            for tid, trade in list(open_trades.items()):
                active_book = book_for_direction(trade.direction, up_book, down_book) if trade.slug == current_slug else {"bids": [], "asks": []}
                best_bid = active_book.get("best_bid")
                update_open_trade_marks(trade, best_bid)

                should_close = False
                exit_type = ""
                sell_quote: FillQuote | None = None
                final_outcome = ""
                exit_second = current_since if trade.slug == current_slug else None

                if trade.exit_rule in PROFIT_TARGET_BY_RULE and best_bid is not None:
                    profit_target = PROFIT_TARGET_BY_RULE[trade.exit_rule]
                    stop_loss = STOP_LOSS_BY_RULE[trade.exit_rule]
                    if best_bid >= profit_target:
                        sell_quote = quote_sell_shares(active_book.get("bids", []), trade.remaining_shares, trade.fee_rate, profit_target)
                        if sell_quote.complete:
                            should_close = True
                            exit_type = "PROFIT_TARGET"
                        else:
                            trade.exit_attempts.append(
                                {
                                    "time_utc": now.isoformat(),
                                    "exit_type": "PROFIT_TARGET_PARTIAL_OR_FAILED",
                                    "trigger_bid": best_bid,
                                    "sold_shares": round(sell_quote.shares, 6),
                                    "remaining_before": round(trade.remaining_shares, 6),
                                    "cash_after_fee": round(sell_quote.total_cash_used, 6),
                                    "fee": round(sell_quote.fee_usdc, 6),
                                    "complete": sell_quote.complete,
                                    "partial": sell_quote.partial,
                                    "fill_levels": sell_quote.fill_levels,
                                }
                            )
                            if sell_quote.shares > 0:
                                trade.realized_exit_cash += sell_quote.total_cash_used
                                trade.realized_exit_fee += sell_quote.fee_usdc
                                trade.remaining_shares = max(0.0, trade.remaining_shares - sell_quote.shares)
                    elif best_bid <= stop_loss:
                        sell_quote = quote_sell_shares(active_book.get("bids", []), trade.remaining_shares, trade.fee_rate, 0.0)
                        if sell_quote.complete:
                            should_close = True
                            exit_type = "STOP_LOSS"
                        else:
                            trade.exit_attempts.append(
                                {
                                    "time_utc": now.isoformat(),
                                    "exit_type": "STOP_LOSS_PARTIAL_OR_FAILED",
                                    "trigger_bid": best_bid,
                                    "sold_shares": round(sell_quote.shares, 6),
                                    "remaining_before": round(trade.remaining_shares, 6),
                                    "cash_after_fee": round(sell_quote.total_cash_used, 6),
                                    "fee": round(sell_quote.fee_usdc, 6),
                                    "complete": sell_quote.complete,
                                    "partial": sell_quote.partial,
                                    "fill_levels": sell_quote.fill_levels,
                                }
                            )
                            if sell_quote.shares > 0:
                                trade.realized_exit_cash += sell_quote.total_cash_used
                                trade.realized_exit_fee += sell_quote.fee_usdc
                                trade.remaining_shares = max(0.0, trade.remaining_shares - sell_quote.shares)

                if not should_close and now_ms >= trade.end_time_ms and chainlink_price is not None:
                    final_outcome = "UP" if chainlink_price >= trade.entry_price_to_beat else "DOWN"
                    payoff = trade.remaining_shares if trade.direction == final_outcome else 0.0
                    sell_quote = FillQuote(
                        complete=True,
                        partial=False,
                        filled_cash=payoff,
                        unfilled_cash=0.0,
                        shares=trade.remaining_shares,
                        avg_price=1.0 if trade.direction == final_outcome else 0.0,
                        worst_price=1.0 if trade.direction == final_outcome else 0.0,
                        levels_used=0,
                        fee_usdc=0.0,
                        total_cash_used=payoff,
                        fill_levels=[],
                    )
                    should_close = True
                    exit_type = "RESOLUTION"

                if should_close and sell_quote is not None:
                    if not final_outcome and chainlink_price is not None:
                        final_outcome = "UP" if chainlink_price >= trade.entry_price_to_beat else "DOWN"
                    exit_cash_total = trade.realized_exit_cash + sell_quote.total_cash_used
                    exit_fee_total = trade.realized_exit_fee + sell_quote.fee_usdc
                    remaining_after_exit = max(0.0, trade.remaining_shares - sell_quote.shares)
                    row = close_trade_row(
                        trade,
                        exit_type=exit_type,
                        exit_time_utc=now.isoformat(),
                        exit_second=exit_second,
                        exit_avg_price=sell_quote.avg_price,
                        exit_worst_price=sell_quote.worst_price,
                        exit_cash_after_fee=exit_cash_total,
                        exit_fee=exit_fee_total,
                        exit_complete=sell_quote.complete and remaining_after_exit <= max(0.0001, trade.entry_shares * 0.001),
                        exit_partial=bool(trade.exit_attempts) or sell_quote.partial,
                        exit_sold_shares=trade.entry_shares - remaining_after_exit,
                        exit_remaining_shares=remaining_after_exit,
                        final_outcome=final_outcome,
                        exit_fill_levels=sell_quote.fill_levels,
                    )
                    append_jsonl(args.trades_jsonl, row)
                    append_csv(args.trades_csv, TRADE_CSV_FIELDS, row)
                    del open_trades[tid]

            collected += 1
            if args.print_each:
                print(json.dumps({"sample": collected, "slug": current_slug, "open_trades": len(open_trades)}, ensure_ascii=False), flush=True)
            sleep_for = max(0.0, args.interval_seconds - (time.perf_counter() - loop_started))
            time.sleep(sleep_for)

    finally:
        rtds_cache.stop()

    print(f"snapshots_jsonl={args.snapshot_jsonl}")
    print(f"snapshots_csv={args.snapshot_csv}")
    print(f"signal_events={args.signal_events}")
    print(f"skip_events={args.skip_events}")
    print(f"trades_jsonl={args.trades_jsonl}")
    print(f"trades_csv={args.trades_csv}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
