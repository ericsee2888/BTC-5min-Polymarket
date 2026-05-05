#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import signal
import sys
import threading
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from collect_polymarket_crypto_5m_btc_full_orderbook_v3 import (  # noqa: E402
    BINANCE_BTCUSDT_URL,
    COINBASE_BTCUSD_URL,
    DEFAULT_FEE_RATE,
    PendingEntry,
    append_csv,
    append_jsonl,
    current_start_ts,
    depth_cash_asks_lte,
    depth_cash_bids_gte,
    determine_signal,
    fee_rate_for_market,
    fetch_book,
    fetch_json,
    fetch_market_by_slug,
    market_timing,
    market_tokens,
    parse_binance_price,
    parse_coinbase_price,
    price_to_beat_status,
    quote_buy_with_total_cash,
    rounded,
    slug_for_start,
    strategy_id,
)
from collect_polymarket_crypto_price_samples_v1 import RtdsPriceCache  # noqa: E402


DATA_DIR = ROOT / "data"
DEFAULT_EVENTS_GZ = DATA_DIR / "polymarket_crypto_5m_btc_ws_orderbook_events_v1.jsonl.gz"
DEFAULT_SNAPSHOTS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_ws_book_snapshots_v1.jsonl"
DEFAULT_SNAPSHOTS_CSV = DATA_DIR / "polymarket_crypto_5m_btc_ws_book_snapshots_v1.csv"
DEFAULT_SIGNALS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_ws_signal_events_v1.jsonl"
DEFAULT_SKIPS_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_ws_skip_events_v1.jsonl"
DEFAULT_TRADES_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_ws_paper_trades_v1.jsonl"
DEFAULT_TRADES_CSV = DATA_DIR / "polymarket_crypto_5m_btc_ws_paper_trades_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_WEBSOCKET_SMOKE_TEST_REPORT_V1_CN.md"

CLOB_MARKET_WS_URL = "wss://ws-subscriptions-clob.polymarket.com/ws/market"
ENTRY_END_SECONDS = 180.0
STOP_REQUESTED = False


STRATEGIES = [
    {
        "threshold": 25.0,
        "entry_start": 60.0,
        "entry_cap": 0.75,
        "cash": 100.0,
        "latency_ms": 250,
        "name": "main_90_trade_candidate",
    },
    {
        "threshold": 25.0,
        "entry_start": 15.0,
        "entry_cap": 0.65,
        "cash": 100.0,
        "latency_ms": 250,
        "name": "strict_high_roi_candidate",
    },
]

SNAPSHOT_FIELDS = [
    "sampled_at_utc",
    "sampled_at_unix_ms",
    "current_slug",
    "current_seconds_since_start",
    "current_seconds_to_end",
    "price_to_beat",
    "price_to_beat_status",
    "binance_btcusdt",
    "coinbase_btcusd",
    "rtds_binance_btcusdt",
    "chainlink_btcusd",
    "binance_minus_price_to_beat",
    "coinbase_minus_price_to_beat",
    "chainlink_minus_price_to_beat",
    "signal_25_direction",
    "signal_25_reason",
    "up_token_id",
    "down_token_id",
    "up_best_bid",
    "up_best_ask",
    "down_best_bid",
    "down_best_ask",
    "up_ask_cash_lte_0_65",
    "up_ask_cash_lte_0_70",
    "up_ask_cash_lte_0_75",
    "down_ask_cash_lte_0_65",
    "down_ask_cash_lte_0_70",
    "down_ask_cash_lte_0_75",
    "up_bid_cash_gte_0_35",
    "up_bid_cash_gte_0_50",
    "up_bid_cash_gte_0_70",
    "down_bid_cash_gte_0_35",
    "down_bid_cash_gte_0_50",
    "down_bid_cash_gte_0_70",
    "up_book_age_ms",
    "down_book_age_ms",
    "ws_event_counts_json",
    "notes",
]

TRADE_FIELDS = [
    "paper_trade_id",
    "strategy_id",
    "strategy_label",
    "slug",
    "direction",
    "token_id",
    "threshold_usd",
    "entry_start_second",
    "entry_cap",
    "order_cash_usdc",
    "entry_latency_ms",
    "signal_time_utc",
    "order_arrival_time_utc",
    "entry_time_utc",
    "entry_second",
    "signal_best_ask",
    "arrival_best_ask",
    "entry_avg_price",
    "entry_worst_price",
    "entry_shares",
    "entry_total_cash_used",
    "entry_fee_usdc",
    "entry_fill_levels_json",
    "price_to_beat",
    "entry_binance_delta",
    "entry_coinbase_delta",
    "entry_chainlink_delta",
    "exit_type",
    "exit_time_utc",
    "final_outcome",
    "final_outcome_source",
    "correct",
    "pnl_usdc",
    "roi_on_cash",
]


@dataclass
class MarketState:
    current_slug: str = ""
    current_market: dict[str, Any] | None = None
    current_tokens: dict[str, str] | None = None
    current_fee_rate: float = DEFAULT_FEE_RATE
    current_end_ms: int | None = None
    next_slug: str = ""
    next_tokens: dict[str, str] | None = None
    last_refresh_ms: int = 0


@dataclass
class OpenWsTrade:
    paper_trade_id: str
    strategy_id: str
    strategy_label: str
    slug: str
    direction: str
    token_id: str
    threshold_usd: float
    entry_start_second: float
    entry_cap: float
    order_cash_usdc: float
    entry_latency_ms: int
    signal_time_utc: str
    order_arrival_time_utc: str
    entry_time_utc: str
    entry_second: float
    signal_best_ask: float | None
    arrival_best_ask: float
    entry_avg_price: float
    entry_worst_price: float
    entry_shares: float
    entry_total_cash_used: float
    entry_fee_usdc: float
    entry_fill_levels_json: str
    price_to_beat: float
    entry_binance_delta: float
    entry_coinbase_delta: float
    entry_chainlink_delta: float
    end_time_ms: int


class JsonlGzipWriter:
    def __init__(self, path: Path, enabled: bool) -> None:
        self.path = path
        self.enabled = enabled
        self._handle: gzip.GzipFile | None = None
        self._lock = threading.Lock()

    def __enter__(self) -> "JsonlGzipWriter":
        if self.enabled:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self._handle = gzip.open(self.path, "at", encoding="utf-8")
        return self

    def __exit__(self, exc_type: Any, exc: Any, tb: Any) -> None:
        if self._handle:
            self._handle.close()

    def write(self, row: dict[str, Any]) -> None:
        if not self.enabled or not self._handle:
            return
        with self._lock:
            self._handle.write(json.dumps(row, ensure_ascii=False, separators=(",", ":")) + "\n")


class PricePoller:
    def __init__(self, interval_seconds: float, timeout_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._snapshot: dict[str, Any] = {
            "binance_btcusdt": None,
            "coinbase_btcusd": None,
            "binance_timestamp_ms": None,
            "coinbase_timestamp_ms": None,
            "binance_error": "not_started",
            "coinbase_error": "not_started",
        }

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return dict(self._snapshot)

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.perf_counter()
            now_ms = int(time.time() * 1000)
            binance_payload, binance_latency_ms, binance_error = fetch_json(
                BINANCE_BTCUSDT_URL,
                self.timeout_seconds,
            )
            coinbase_payload, coinbase_latency_ms, coinbase_error = fetch_json(
                COINBASE_BTCUSD_URL,
                self.timeout_seconds,
            )
            with self._lock:
                self._snapshot = {
                    "binance_btcusdt": None if binance_error else parse_binance_price(binance_payload),
                    "coinbase_btcusd": None if coinbase_error else parse_coinbase_price(coinbase_payload),
                    "binance_timestamp_ms": now_ms,
                    "coinbase_timestamp_ms": now_ms,
                    "binance_latency_ms": binance_latency_ms,
                    "coinbase_latency_ms": coinbase_latency_ms,
                    "binance_error": binance_error,
                    "coinbase_error": coinbase_error,
                }
            elapsed = time.perf_counter() - started
            self._stop.wait(max(0.0, self.interval_seconds - elapsed))


class OrderBookStore:
    def __init__(self) -> None:
        self.bids: dict[str, dict[float, float]] = {}
        self.asks: dict[str, dict[float, float]] = {}
        self.meta: dict[str, dict[str, Any]] = {}
        self.event_counts: dict[str, int] = {}
        self._lock = threading.Lock()

    def apply_event(self, event: dict[str, Any], received_ms: int) -> None:
        event_type = str(event.get("event_type") or "")
        if not event_type and "bids" in event and "asks" in event:
            event_type = "book"
        if not event_type and "price_changes" in event:
            event_type = "price_change"
        with self._lock:
            self.event_counts[event_type or "unknown"] = self.event_counts.get(event_type or "unknown", 0) + 1
            if event_type == "book":
                self._apply_book(event, received_ms)
            elif event_type == "price_change":
                self._apply_price_change(event, received_ms)
            elif event_type == "last_trade_price":
                asset_id = str(event.get("asset_id") or "")
                if asset_id:
                    self.meta.setdefault(asset_id, {})["last_trade_price"] = event.get("price")
                    self.meta.setdefault(asset_id, {})["last_received_ms"] = received_ms
            elif event_type == "best_bid_ask":
                asset_id = str(event.get("asset_id") or "")
                if asset_id:
                    self.meta.setdefault(asset_id, {})["best_bid_ask_event"] = event
                    self.meta.setdefault(asset_id, {})["last_received_ms"] = received_ms

    def snapshot_book(self, token_id: str, max_levels: int) -> dict[str, Any]:
        with self._lock:
            bids_map = dict(self.bids.get(token_id, {}))
            asks_map = dict(self.asks.get(token_id, {}))
            meta = dict(self.meta.get(token_id, {}))
        bids = [
            {"price": price, "size": size}
            for price, size in sorted(bids_map.items(), key=lambda item: item[0], reverse=True)
            if size > 0
        ][:max_levels]
        asks = [
            {"price": price, "size": size}
            for price, size in sorted(asks_map.items(), key=lambda item: item[0])
            if size > 0
        ][:max_levels]
        best_bid = bids[0]["price"] if bids else None
        best_ask = asks[0]["price"] if asks else None
        return {
            "token_id": token_id,
            "bids": bids,
            "asks": asks,
            "best_bid": best_bid,
            "best_ask": best_ask,
            "book_timestamp_ms": meta.get("book_timestamp_ms"),
            "last_received_ms": meta.get("last_received_ms"),
            "hash": meta.get("hash") or "",
            "book_error": "" if bids or asks else "missing_ws_book",
        }

    def counts(self) -> dict[str, int]:
        with self._lock:
            return dict(self.event_counts)

    def seed_book(self, token_id: str, book: dict[str, Any], received_ms: int) -> None:
        bids = book.get("bids") or []
        asks = book.get("asks") or []
        if not token_id or (not bids and not asks):
            return
        with self._lock:
            self.bids[token_id] = self._levels_to_map(bids)
            self.asks[token_id] = self._levels_to_map(asks)
            self.meta[token_id] = {
                **self.meta.get(token_id, {}),
                "book_timestamp_ms": book.get("book_timestamp_ms"),
                "hash": book.get("book_hash") or book.get("hash") or "",
                "last_received_ms": received_ms,
                "seed_source": "rest_book",
            }
            self.event_counts["rest_book_seed"] = self.event_counts.get("rest_book_seed", 0) + 1

    def _apply_book(self, event: dict[str, Any], received_ms: int) -> None:
        asset_id = str(event.get("asset_id") or "")
        if not asset_id:
            return
        self.bids[asset_id] = self._levels_to_map(event.get("bids") or [])
        self.asks[asset_id] = self._levels_to_map(event.get("asks") or [])
        self.meta[asset_id] = {
            **self.meta.get(asset_id, {}),
            "market": event.get("market"),
            "book_timestamp_ms": event.get("timestamp"),
            "hash": event.get("hash"),
            "last_received_ms": received_ms,
        }

    def _apply_price_change(self, event: dict[str, Any], received_ms: int) -> None:
        for change in event.get("price_changes") or []:
            asset_id = str(change.get("asset_id") or "")
            side = str(change.get("side") or "").upper()
            price = _to_float(change.get("price"))
            size = _to_float(change.get("size"))
            if not asset_id or price is None or size is None:
                continue
            target = self.bids if side == "BUY" else self.asks if side == "SELL" else None
            if target is None:
                continue
            levels = target.setdefault(asset_id, {})
            if size <= 0:
                levels.pop(price, None)
            else:
                levels[price] = size
            self.meta.setdefault(asset_id, {})["hash"] = change.get("hash") or self.meta.get(asset_id, {}).get("hash")
            self.meta.setdefault(asset_id, {})["last_received_ms"] = received_ms
            self.meta.setdefault(asset_id, {})["book_timestamp_ms"] = event.get("timestamp")

    @staticmethod
    def _levels_to_map(levels: list[dict[str, Any]]) -> dict[float, float]:
        parsed: dict[float, float] = {}
        for level in levels:
            price = _to_float(level.get("price"))
            size = _to_float(level.get("size"))
            if price is not None and size is not None and size > 0:
                parsed[price] = size
        return parsed


def handle_stop_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def _to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def remove_outputs(paths: list[Path]) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def build_depth_summary(prefix: str, book: dict[str, Any], now_ms: int) -> dict[str, Any]:
    bids = book.get("bids") or []
    asks = book.get("asks") or []
    last_received_ms = book.get("last_received_ms")
    return {
        f"{prefix}_best_bid": rounded(book.get("best_bid")),
        f"{prefix}_best_ask": rounded(book.get("best_ask")),
        f"{prefix}_ask_cash_lte_0_65": round(depth_cash_asks_lte(asks, 0.65), 6),
        f"{prefix}_ask_cash_lte_0_70": round(depth_cash_asks_lte(asks, 0.70), 6),
        f"{prefix}_ask_cash_lte_0_75": round(depth_cash_asks_lte(asks, 0.75), 6),
        f"{prefix}_bid_cash_gte_0_35": round(depth_cash_bids_gte(bids, 0.35), 6),
        f"{prefix}_bid_cash_gte_0_50": round(depth_cash_bids_gte(bids, 0.50), 6),
        f"{prefix}_bid_cash_gte_0_70": round(depth_cash_bids_gte(bids, 0.70), 6),
        f"{prefix}_book_age_ms": now_ms - int(last_received_ms) if last_received_ms else "",
    }


def trade_row(trade: OpenWsTrade, final_outcome: str, exit_time_utc: str) -> dict[str, Any]:
    exit_cash = trade.entry_shares if trade.direction == final_outcome else 0.0
    pnl = exit_cash - trade.entry_total_cash_used
    return {
        "paper_trade_id": trade.paper_trade_id,
        "strategy_id": trade.strategy_id,
        "strategy_label": trade.strategy_label,
        "slug": trade.slug,
        "direction": trade.direction,
        "token_id": trade.token_id,
        "threshold_usd": trade.threshold_usd,
        "entry_start_second": trade.entry_start_second,
        "entry_cap": trade.entry_cap,
        "order_cash_usdc": trade.order_cash_usdc,
        "entry_latency_ms": trade.entry_latency_ms,
        "signal_time_utc": trade.signal_time_utc,
        "order_arrival_time_utc": trade.order_arrival_time_utc,
        "entry_time_utc": trade.entry_time_utc,
        "entry_second": rounded(trade.entry_second, 3),
        "signal_best_ask": rounded(trade.signal_best_ask),
        "arrival_best_ask": rounded(trade.arrival_best_ask),
        "entry_avg_price": rounded(trade.entry_avg_price),
        "entry_worst_price": rounded(trade.entry_worst_price),
        "entry_shares": rounded(trade.entry_shares),
        "entry_total_cash_used": rounded(trade.entry_total_cash_used),
        "entry_fee_usdc": rounded(trade.entry_fee_usdc),
        "entry_fill_levels_json": trade.entry_fill_levels_json,
        "price_to_beat": rounded(trade.price_to_beat),
        "entry_binance_delta": rounded(trade.entry_binance_delta),
        "entry_coinbase_delta": rounded(trade.entry_coinbase_delta),
        "entry_chainlink_delta": rounded(trade.entry_chainlink_delta),
        "exit_type": "hold_to_resolution",
        "exit_time_utc": exit_time_utc,
        "final_outcome": final_outcome,
        "final_outcome_source": "chainlink_inferred",
        "correct": trade.direction == final_outcome,
        "pnl_usdc": rounded(pnl),
        "roi_on_cash": rounded(pnl / trade.entry_total_cash_used) if trade.entry_total_cash_used else "",
    }


def write_report(
    path: Path,
    started_at: str,
    ended_at: str,
    duration_seconds: float,
    event_counts: dict[str, int],
    snapshot_count: int,
    signal_count: int,
    skip_count: int,
    trade_count: int,
    websocket_error: str,
    output_paths: dict[str, Path],
) -> None:
    status = "PASS" if event_counts.get("book", 0) >= 2 and event_counts.get("price_change", 0) > 0 and snapshot_count > 0 else "CHECK"
    text = f"""# Polymarket 5分钟 BTC WebSocket 冒烟测试报告 v1

生成时间：{ended_at}

## 结论

状态：`{status}`

本次测试验证的是 BTC 5分钟 taker 策略的 WebSocket 盘口采集能力，不涉及 maker。

## 测试统计

| 项目 | 数值 |
|---|---:|
| 开始时间 | {started_at} |
| 结束时间 | {ended_at} |
| 运行秒数 | {round(duration_seconds, 3)} |
| book 事件 | {event_counts.get("book", 0)} |
| price_change 事件 | {event_counts.get("price_change", 0)} |
| last_trade_price 事件 | {event_counts.get("last_trade_price", 0)} |
| snapshot 行数 | {snapshot_count} |
| signal 行数 | {signal_count} |
| skip 行数 | {skip_count} |
| paper trade 行数 | {trade_count} |
| WebSocket 错误 | {websocket_error or ""} |

## 文件

- 原始压缩事件：`{output_paths["events_gz"]}`
- 盘口快照 JSONL：`{output_paths["snapshots_jsonl"]}`
- 盘口快照 CSV：`{output_paths["snapshots_csv"]}`
- 信号事件：`{output_paths["signals_jsonl"]}`
- 跳过事件：`{output_paths["skips_jsonl"]}`
- 模拟交易 JSONL：`{output_paths["trades_jsonl"]}`
- 模拟交易 CSV：`{output_paths["trades_csv"]}`

## 下一步

如果状态为 `PASS`，可以进入后台 24小时正式采集。正式采集前不需要再改策略范围，第一轮只验证两个候选策略。
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


async def refresh_market_state(state: MarketState, timeout: float) -> None:
    now_ms = int(time.time() * 1000)
    start_ts = current_start_ts(now_ms // 1000)
    current_slug = slug_for_start(start_ts)
    next_slug = slug_for_start(start_ts + 300)
    current_market, _latency, current_error = fetch_market_by_slug(current_slug, timeout)
    next_market, _next_latency, next_error = fetch_market_by_slug(next_slug, timeout)
    if current_error or not current_market:
        return
    current_since, _current_to_end, current_end_ms = market_timing(current_market, now_ms)
    state.current_slug = current_slug
    state.current_market = current_market
    state.current_tokens = market_tokens(current_market)
    state.current_fee_rate = fee_rate_for_market(current_market)
    state.current_end_ms = current_end_ms
    state.next_slug = next_slug
    state.next_tokens = market_tokens(next_market) if not next_error and next_market else {}
    state.last_refresh_ms = now_ms
    if current_since is None:
        return


def subscribed_token_ids(state: MarketState) -> list[str]:
    token_ids: list[str] = []
    for tokens in [state.current_tokens or {}, state.next_tokens or {}]:
        for outcome in ["UP", "DOWN"]:
            token_id = tokens.get(outcome)
            if token_id and token_id not in token_ids:
                token_ids.append(token_id)
    return token_ids


def book_for_direction(direction: str, state: MarketState, store: OrderBookStore, max_levels: int) -> dict[str, Any]:
    token_id = (state.current_tokens or {}).get(direction, "")
    return store.snapshot_book(token_id, max_levels) if token_id else {"bids": [], "asks": [], "book_error": "missing_token"}


def final_outcome_from_chainlink(chainlink_price: float | None, price_to_beat: float | None) -> str:
    if chainlink_price is None or price_to_beat is None:
        return ""
    return "UP" if chainlink_price > price_to_beat else "DOWN"


def seed_subscribed_books(args: argparse.Namespace, state: MarketState, store: OrderBookStore) -> None:
    received_ms = int(time.time() * 1000)
    for token_id in subscribed_token_ids(state):
        current = store.snapshot_book(token_id, args.book_levels)
        if not current.get("book_error"):
            continue
        book = fetch_book(token_id, args.timeout_seconds, args.book_levels)
        if not book.get("book_error"):
            store.seed_book(token_id, book, received_ms)


async def collect(args: argparse.Namespace) -> int:
    started_at = iso_now()
    started_mono = time.monotonic()
    signal_count = 0
    skip_count = 0
    trade_count = 0
    snapshot_count = 0
    websocket_error = ""
    stream_error_count = 0

    state = MarketState()
    store = OrderBookStore()
    rtds_cache = RtdsPriceCache()
    price_poller = PricePoller(args.price_poll_interval_seconds, args.timeout_seconds)
    rtds_cache.start()
    price_poller.start()
    await asyncio.sleep(max(0.0, args.rtds_warmup_seconds))
    await refresh_market_state(state, args.timeout_seconds)

    price_to_beat_by_slug: dict[str, dict[str, Any]] = {}
    attempted_entries: set[str] = set()
    pending_entries: dict[str, PendingEntry] = {}
    open_trades: dict[str, OpenWsTrade] = {}
    completed_trades: set[str] = set()

    try:
        with JsonlGzipWriter(args.events_gz, not args.disable_raw_events) as raw_writer:
            while not STOP_REQUESTED:
                if args.duration_seconds > 0 and time.monotonic() - started_mono >= args.duration_seconds:
                    break
                await refresh_market_state(state, args.timeout_seconds)
                try:
                    await subscribe_and_stream(
                        args,
                        state,
                        store,
                        raw_writer,
                        price_poller,
                        rtds_cache,
                        price_to_beat_by_slug,
                        attempted_entries,
                        pending_entries,
                        open_trades,
                        completed_trades,
                        started_mono=started_mono,
                    )
                except Exception as exc:  # noqa: BLE001
                    stream_error_count += 1
                    websocket_error = f"recoverable_stream_error_{stream_error_count}: {type(exc).__name__}: {exc}"
                counts = collect_runtime_counts()
                signal_count += counts["signal_count"]
                skip_count += counts["skip_count"]
                trade_count += counts["trade_count"]
                snapshot_count += counts["snapshot_count"]
                if args.duration_seconds > 0 and time.monotonic() - started_mono >= args.duration_seconds:
                    break
                await asyncio.sleep(args.reconnect_sleep_seconds)
    except Exception as exc:  # noqa: BLE001
        websocket_error = f"{type(exc).__name__}: {exc}"
        return_code = 1
    else:
        return_code = 0
    finally:
        rtds_cache.stop()
        price_poller.stop()
        ended_at = iso_now()
        write_report(
            args.report,
            started_at=started_at,
            ended_at=ended_at,
            duration_seconds=time.monotonic() - started_mono,
            event_counts=store.counts(),
            snapshot_count=snapshot_count,
            signal_count=signal_count,
            skip_count=skip_count,
            trade_count=trade_count,
            websocket_error=websocket_error,
            output_paths={
                "events_gz": args.events_gz,
                "snapshots_jsonl": args.snapshots_jsonl,
                "snapshots_csv": args.snapshots_csv,
                "signals_jsonl": args.signals_jsonl,
                "skips_jsonl": args.skips_jsonl,
                "trades_jsonl": args.trades_jsonl,
                "trades_csv": args.trades_csv,
            },
        )
    return return_code


RUNTIME_COUNTERS = {
    "signal_count": 0,
    "skip_count": 0,
    "trade_count": 0,
    "snapshot_count": 0,
}


def collect_runtime_counts() -> dict[str, int]:
    global RUNTIME_COUNTERS
    counts = dict(RUNTIME_COUNTERS)
    RUNTIME_COUNTERS = {key: 0 for key in RUNTIME_COUNTERS}
    return counts


async def subscribe_and_stream(
    args: argparse.Namespace,
    state: MarketState,
    store: OrderBookStore,
    raw_writer: JsonlGzipWriter,
    price_poller: PricePoller,
    rtds_cache: RtdsPriceCache,
    price_to_beat_by_slug: dict[str, dict[str, Any]],
    attempted_entries: set[str],
    pending_entries: dict[str, PendingEntry],
    open_trades: dict[str, OpenWsTrade],
    completed_trades: set[str],
    started_mono: float,
) -> None:
    token_ids = subscribed_token_ids(state)
    if not token_ids:
        await asyncio.sleep(1)
        return
    seed_subscribed_books(args, state, store)
    last_snapshot_mono = 0.0
    last_ping_mono = time.monotonic()
    last_refresh_mono = time.monotonic()
    async with websockets_connect(args.ws_url) as websocket:
        subscription = {"assets_ids": token_ids, "type": "market"}
        if args.custom_feature_enabled:
            subscription["custom_feature_enabled"] = True
        await websocket.send(json.dumps(subscription, separators=(",", ":")))
        while not STOP_REQUESTED:
            if args.duration_seconds > 0 and time.monotonic() - started_mono >= args.duration_seconds:
                break
            if time.monotonic() - last_ping_mono >= args.ping_seconds:
                await websocket.send("PING")
                last_ping_mono = time.monotonic()
            if time.monotonic() - last_refresh_mono >= args.market_refresh_seconds:
                old_tokens = token_ids
                await refresh_market_state(state, args.timeout_seconds)
                token_ids = subscribed_token_ids(state)
                if token_ids != old_tokens:
                    break
                last_refresh_mono = time.monotonic()
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=0.25)
            except asyncio.TimeoutError:
                message = ""
            if message:
                received_ms = int(time.time() * 1000)
                received_utc = iso_now()
                try:
                    parsed = json.loads(message)
                except json.JSONDecodeError:
                    parsed = {"event_type": "raw", "payload": message}
                events = parsed if isinstance(parsed, list) else [parsed]
                for event in events:
                    if not isinstance(event, dict):
                        continue
                    event_type = str(event.get("event_type") or "")
                    if not event_type and "bids" in event and "asks" in event:
                        event_type = "book"
                    if not event_type and "price_changes" in event:
                        event_type = "price_change"
                    store.apply_event(event, received_ms)
                    raw_writer.write(
                        {
                            "received_at_utc": received_utc,
                            "received_at_unix_ms": received_ms,
                            "event_type": event_type,
                            "raw": event,
                        }
                    )
            if time.monotonic() - last_snapshot_mono >= args.snapshot_interval_seconds:
                counts = process_once(
                    args,
                    state,
                    store,
                    price_poller,
                    rtds_cache,
                    price_to_beat_by_slug,
                    attempted_entries,
                    pending_entries,
                    open_trades,
                    completed_trades,
                )
                for key, value in counts.items():
                    RUNTIME_COUNTERS[key] += value
                last_snapshot_mono = time.monotonic()


def websockets_connect(url: str) -> Any:
    try:
        import websockets
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(f"websockets_import_failed: {exc}") from exc
    return websockets.connect(url, ping_interval=None, open_timeout=15, proxy=None)


def process_once(
    args: argparse.Namespace,
    state: MarketState,
    store: OrderBookStore,
    price_poller: PricePoller,
    rtds_cache: RtdsPriceCache,
    price_to_beat_by_slug: dict[str, dict[str, Any]],
    attempted_entries: set[str],
    pending_entries: dict[str, PendingEntry],
    open_trades: dict[str, OpenWsTrade],
    completed_trades: set[str],
) -> dict[str, int]:
    signal_count = 0
    skip_count = 0
    trade_count = 0
    snapshot_count = 0
    now = utc_now()
    now_ms = int(now.timestamp() * 1000)
    if not state.current_market or not state.current_slug:
        return {"signal_count": 0, "skip_count": 0, "trade_count": 0, "snapshot_count": 0}

    current_since, current_to_end, current_end_ms = market_timing(state.current_market, now_ms)
    rtds = rtds_cache.snapshot()
    prices = price_poller.snapshot()
    chainlink_price = rtds.get("chainlink_btcusd")
    chainlink_ts = rtds.get("chainlink_timestamp_ms")

    if state.current_slug and chainlink_price is not None and state.current_slug not in price_to_beat_by_slug:
        price_to_beat_by_slug[state.current_slug] = {
            "price": chainlink_price,
            "source": "rtds_chainlink_first_seen_for_window",
            "observed_at_utc": now.isoformat(),
            "observed_second": current_since,
            "status": price_to_beat_status(current_since),
        }

    price_meta = price_to_beat_by_slug.get(state.current_slug, {})
    price_to_beat = price_meta.get("price")
    price_status = str(price_meta.get("status") or "")
    binance_price = prices.get("binance_btcusdt")
    coinbase_price = prices.get("coinbase_btcusd")
    binance_delta = binance_price - price_to_beat if binance_price is not None and price_to_beat is not None else None
    coinbase_delta = coinbase_price - price_to_beat if coinbase_price is not None and price_to_beat is not None else None
    chainlink_delta = chainlink_price - price_to_beat if chainlink_price is not None and price_to_beat is not None else None
    signal_direction, signal_reason, signal_bd, signal_cd = determine_signal(
        binance_price,
        coinbase_price,
        price_to_beat,
        25.0,
    )

    up_token = (state.current_tokens or {}).get("UP", "")
    down_token = (state.current_tokens or {}).get("DOWN", "")
    up_book = store.snapshot_book(up_token, args.book_levels) if up_token else {"bids": [], "asks": [], "book_error": "missing_up_token"}
    down_book = store.snapshot_book(down_token, args.book_levels) if down_token else {"bids": [], "asks": [], "book_error": "missing_down_token"}

    snapshot = {
        "sampled_at_utc": now.isoformat(),
        "sampled_at_unix_ms": now_ms,
        "current_slug": state.current_slug,
        "current_seconds_since_start": current_since,
        "current_seconds_to_end": current_to_end,
        "price_to_beat": rounded(price_to_beat),
        "price_to_beat_status": price_status,
        "binance_btcusdt": rounded(binance_price),
        "coinbase_btcusd": rounded(coinbase_price),
        "rtds_binance_btcusdt": rounded(rtds.get("rtds_binance_btcusdt")),
        "chainlink_btcusd": rounded(chainlink_price),
        "binance_minus_price_to_beat": rounded(binance_delta),
        "coinbase_minus_price_to_beat": rounded(coinbase_delta),
        "chainlink_minus_price_to_beat": rounded(chainlink_delta),
        "signal_25_direction": signal_direction,
        "signal_25_reason": signal_reason,
        "up_token_id": up_token,
        "down_token_id": down_token,
        "ws_event_counts_json": json.dumps(store.counts(), ensure_ascii=False),
        "notes": "",
    }
    snapshot.update(build_depth_summary("up", up_book, now_ms))
    snapshot.update(build_depth_summary("down", down_book, now_ms))
    append_jsonl(args.snapshots_jsonl, snapshot)
    append_csv(args.snapshots_csv, SNAPSHOT_FIELDS, snapshot)
    snapshot_count += 1

    if signal_direction and current_since is not None and current_end_ms is not None and price_to_beat is not None:
        signal_book = up_book if signal_direction == "UP" else down_book
        signal_base = {
            "signal_time_utc": now.isoformat(),
            "signal_unix_ms": now_ms,
            "slug": state.current_slug,
            "direction": signal_direction,
            "token_id": (state.current_tokens or {}).get(signal_direction, ""),
            "threshold_usd": 25.0,
            "price_to_beat": rounded(price_to_beat),
            "binance_btcusdt": rounded(binance_price),
            "coinbase_btcusd": rounded(coinbase_price),
            "chainlink_btcusd": rounded(chainlink_price),
            "binance_delta": rounded(signal_bd),
            "coinbase_delta": rounded(signal_cd),
            "chainlink_delta": rounded(chainlink_delta),
            "second_since_start": rounded(current_since, 3),
            "best_ask": rounded(signal_book.get("best_ask")),
            "best_bid": rounded(signal_book.get("best_bid")),
        }
        append_jsonl(args.signals_jsonl, signal_base)
        signal_count += 1

        for strategy in STRATEGIES:
            sid = strategy_id(
                strategy["threshold"],
                strategy["entry_start"],
                strategy["entry_cap"],
                strategy["cash"],
                strategy["latency_ms"],
            )
            attempt_key = f"{state.current_slug}:{sid}"
            if attempt_key in attempted_entries:
                continue
            if attempt_key in pending_entries:
                continue
            skip_base = {
                **signal_base,
                "strategy_id": sid,
                "strategy_label": strategy["name"],
                "entry_start_second": strategy["entry_start"],
                "entry_cap": strategy["entry_cap"],
                "order_cash_usdc": strategy["cash"],
                "entry_latency_ms": strategy["latency_ms"],
            }
            if current_since < strategy["entry_start"]:
                append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "before_entry_start"})
                skip_count += 1
                continue
            if current_since > ENTRY_END_SECONDS:
                append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "outside_entry_window"})
                skip_count += 1
                continue
            if signal_book.get("book_error"):
                append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "missing_orderbook"})
                skip_count += 1
                continue
            best_ask = signal_book.get("best_ask")
            if best_ask is None:
                append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "missing_best_ask"})
                skip_count += 1
                continue
            if best_ask > strategy["entry_cap"]:
                append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "target_ask_above_cap"})
                skip_count += 1
                continue
            pending_entries[attempt_key] = PendingEntry(
                strategy_id=sid,
                slug=state.current_slug,
                question=str(state.current_market.get("question") or ""),
                direction=signal_direction,
                token_id=(state.current_tokens or {}).get(signal_direction, ""),
                threshold_usd=strategy["threshold"],
                entry_start_second=strategy["entry_start"],
                entry_cap=strategy["entry_cap"],
                entry_latency_ms=int(strategy["latency_ms"]),
                order_cash_usdc=strategy["cash"],
                signal_time_utc=now.isoformat(),
                signal_unix_ms=now_ms,
                due_unix_ms=now_ms + int(strategy["latency_ms"]),
                signal_second=float(current_since),
                signal_price_to_beat=float(price_to_beat),
                signal_binance_delta=float(signal_bd or 0.0),
                signal_coinbase_delta=float(signal_cd or 0.0),
                signal_chainlink_delta=float(chainlink_delta or 0.0),
                signal_best_ask=best_ask,
                signal_best_bid=signal_book.get("best_bid"),
                fee_rate=state.current_fee_rate,
                end_time_ms=current_end_ms,
            )

    for key, pending in list(pending_entries.items()):
        if now_ms < pending.due_unix_ms:
            continue
        book = book_for_direction(pending.direction, state, store, args.book_levels)
        arrival_best_ask = book.get("best_ask")
        skip_base = {
            "signal_time_utc": pending.signal_time_utc,
            "order_arrival_time_utc": now.isoformat(),
            "slug": pending.slug,
            "direction": pending.direction,
            "token_id": pending.token_id,
            "strategy_id": pending.strategy_id,
            "threshold_usd": pending.threshold_usd,
            "entry_start_second": pending.entry_start_second,
            "entry_cap": pending.entry_cap,
            "order_cash_usdc": pending.order_cash_usdc,
            "entry_latency_ms": pending.entry_latency_ms,
            "arrival_best_ask": rounded(arrival_best_ask),
        }
        if book.get("book_error"):
            append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "latency_missing_orderbook"})
            pending_entries.pop(key, None)
            skip_count += 1
            continue
        if arrival_best_ask is None or arrival_best_ask > pending.entry_cap:
            append_jsonl(args.skips_jsonl, {**skip_base, "skip_reason": "latency_moved_ask_above_cap"})
            pending_entries.pop(key, None)
            skip_count += 1
            continue
        fill = quote_buy_with_total_cash(
            book.get("asks", []),
            pending.order_cash_usdc,
            pending.fee_rate,
            pending.entry_cap,
        )
        if not fill.complete or not fill.avg_price or not fill.worst_price or fill.shares <= 0:
            append_jsonl(
                args.skips_jsonl,
                {
                    **skip_base,
                    "skip_reason": "latency_insufficient_depth",
                    "partial": fill.partial,
                    "filled_cash": rounded(fill.filled_cash),
                    "unfilled_cash": rounded(fill.unfilled_cash),
                },
            )
            pending_entries.pop(key, None)
            skip_count += 1
            continue
        paper_trade_id = f"{pending.slug}:{pending.strategy_id}:hold_to_resolution"
        attempted_entries.add(key)
        open_trades[paper_trade_id] = OpenWsTrade(
            paper_trade_id=paper_trade_id,
            strategy_id=pending.strategy_id,
            strategy_label=next(
                strategy["name"]
                for strategy in STRATEGIES
                if strategy_id(
                    strategy["threshold"],
                    strategy["entry_start"],
                    strategy["entry_cap"],
                    strategy["cash"],
                    strategy["latency_ms"],
                )
                == pending.strategy_id
            ),
            slug=pending.slug,
            direction=pending.direction,
            token_id=pending.token_id,
            threshold_usd=pending.threshold_usd,
            entry_start_second=pending.entry_start_second,
            entry_cap=pending.entry_cap,
            order_cash_usdc=pending.order_cash_usdc,
            entry_latency_ms=pending.entry_latency_ms,
            signal_time_utc=pending.signal_time_utc,
            order_arrival_time_utc=datetime.fromtimestamp(pending.due_unix_ms / 1000, UTC).isoformat(),
            entry_time_utc=now.isoformat(),
            entry_second=float(current_since or pending.signal_second),
            signal_best_ask=pending.signal_best_ask,
            arrival_best_ask=float(arrival_best_ask),
            entry_avg_price=float(fill.avg_price),
            entry_worst_price=float(fill.worst_price),
            entry_shares=float(fill.shares),
            entry_total_cash_used=float(fill.total_cash_used),
            entry_fee_usdc=float(fill.fee_usdc),
            entry_fill_levels_json=json.dumps(fill.fill_levels, ensure_ascii=False),
            price_to_beat=pending.signal_price_to_beat,
            entry_binance_delta=pending.signal_binance_delta,
            entry_coinbase_delta=pending.signal_coinbase_delta,
            entry_chainlink_delta=pending.signal_chainlink_delta,
            end_time_ms=pending.end_time_ms,
        )
        pending_entries.pop(key, None)

    for paper_trade_id, trade in list(open_trades.items()):
        if now_ms < trade.end_time_ms or paper_trade_id in completed_trades:
            continue
        final_outcome = final_outcome_from_chainlink(chainlink_price, trade.price_to_beat)
        if not final_outcome:
            continue
        row = trade_row(trade, final_outcome, now.isoformat())
        append_jsonl(args.trades_jsonl, row)
        append_csv(args.trades_csv, TRADE_FIELDS, row)
        completed_trades.add(paper_trade_id)
        open_trades.pop(paper_trade_id, None)
        trade_count += 1

    return {
        "signal_count": signal_count,
        "skip_count": skip_count,
        "trade_count": trade_count,
        "snapshot_count": snapshot_count,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=float, default=900.0)
    parser.add_argument("--snapshot-interval-seconds", type=float, default=0.25)
    parser.add_argument("--price-poll-interval-seconds", type=float, default=0.5)
    parser.add_argument("--market-refresh-seconds", type=float, default=8.0)
    parser.add_argument("--reconnect-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--ping-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--rtds-warmup-seconds", type=float, default=2.0)
    parser.add_argument("--book-levels", type=int, default=50)
    parser.add_argument("--ws-url", default=CLOB_MARKET_WS_URL)
    parser.add_argument("--events-gz", type=Path, default=DEFAULT_EVENTS_GZ)
    parser.add_argument("--snapshots-jsonl", type=Path, default=DEFAULT_SNAPSHOTS_JSONL)
    parser.add_argument("--snapshots-csv", type=Path, default=DEFAULT_SNAPSHOTS_CSV)
    parser.add_argument("--signals-jsonl", type=Path, default=DEFAULT_SIGNALS_JSONL)
    parser.add_argument("--skips-jsonl", type=Path, default=DEFAULT_SKIPS_JSONL)
    parser.add_argument("--trades-jsonl", type=Path, default=DEFAULT_TRADES_JSONL)
    parser.add_argument("--trades-csv", type=Path, default=DEFAULT_TRADES_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--reset-output", action="store_true")
    parser.add_argument("--disable-raw-events", action="store_true")
    parser.add_argument("--custom-feature-enabled", action="store_true")
    return parser.parse_args()


def main() -> int:
    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    args = parse_args()
    if args.reset_output:
        remove_outputs(
            [
                args.events_gz,
                args.snapshots_jsonl,
                args.snapshots_csv,
                args.signals_jsonl,
                args.skips_jsonl,
                args.trades_jsonl,
                args.trades_csv,
                args.report,
            ]
        )
    return asyncio.run(collect(args))


if __name__ == "__main__":
    raise SystemExit(main())
