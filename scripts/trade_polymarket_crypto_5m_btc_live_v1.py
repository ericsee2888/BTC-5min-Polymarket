#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import gzip
import json
import os
import signal
import sys
import threading
import time
from dataclasses import asdict
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_DIR = ROOT / "scripts"
if str(SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPTS_DIR))

from collect_polymarket_crypto_5m_btc_full_orderbook_v3 import (  # noqa: E402
    BINANCE_BTCUSDT_URL,
    COINBASE_BTCUSD_URL,
    DEFAULT_FEE_RATE,
    append_csv,
    append_jsonl,
    current_start_ts,
    depth_cash_asks_lte,
    determine_signal,
    fetch_book,
    fetch_json,
    fetch_market_by_slug,
    fee_rate_for_market,
    market_timing,
    market_tokens,
    parse_binance_price,
    parse_coinbase_price,
    price_to_beat_status,
    quote_buy_with_total_cash,
    rounded,
    slug_for_start,
)
from collect_polymarket_crypto_5m_btc_ws_orderbook_v1 import (  # noqa: E402
    CLOB_MARKET_WS_URL,
    JsonlGzipWriter,
    OrderBookStore,
    build_depth_summary,
    subscribed_token_ids,
)
from collect_polymarket_crypto_price_samples_v1 import RtdsPriceCache  # noqa: E402


DATA_DIR = ROOT / "data"
DEFAULT_RAW_EVENTS_GZ = DATA_DIR / "polymarket_crypto_5m_btc_live_orderbook_events_v1.jsonl.gz"
DEFAULT_AUDIT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_live_audit_events_v1.jsonl"
DEFAULT_ORDERS_CSV = DATA_DIR / "polymarket_crypto_5m_btc_live_orders_v1.csv"
DEFAULT_SETTLEMENTS_CSV = DATA_DIR / "polymarket_crypto_5m_btc_live_settlements_v1.csv"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_LIVE_TRADING_RUN_V1_CN.md"
DEFAULT_STATE_JSON = DATA_DIR / "polymarket_crypto_5m_btc_live_state_v1.json"

HOST = "https://clob.polymarket.com"
DATA_API_POSITIONS_URL = "https://data-api.polymarket.com/positions"
CHAIN_ID = 137
STOP_REQUESTED = False

THRESHOLD_USD = 25.0
ENTRY_START_SECOND = 60.0
ENTRY_END_SECOND = 180.0
ENTRY_CAP = 0.75
ORDER_CASH_USDC = 50.0
ENTRY_LATENCY_MS = 250
STRATEGY_ID = "thr25_start60_cap0.75_cash50_lat250ms"

MAX_LOCKED_USDC = 500.0
MAX_DAILY_LOSS_USDC = 300.0
MAX_CONSECUTIVE_ORDER_FAILURES = 3
MAX_BOOK_AGE_MS = 1500
MAX_PRICE_SOURCE_AGE_MS = 3000
DEFAULT_USDC_BALANCE_FLOOR = 50.0

ORDER_FIELDS = [
    "event_time_utc",
    "mode",
    "event_type",
    "strategy_id",
    "slug",
    "direction",
    "token_id",
    "order_cash_usdc",
    "entry_cap",
    "entry_latency_ms",
    "signal_time_utc",
    "order_arrival_time_utc",
    "seconds_since_start",
    "price_to_beat",
    "binance_btcusdt",
    "coinbase_btcusd",
    "chainlink_btcusd",
    "binance_delta",
    "coinbase_delta",
    "chainlink_delta",
    "signal_best_ask",
    "arrival_best_ask",
    "paper_avg_price",
    "paper_worst_price",
    "paper_shares",
    "paper_total_cash_used",
    "pre_order_locked_usdc",
    "available_cash_usdc",
    "order_id",
    "status",
    "success",
    "error",
    "response_json",
    "sdk_latency_ms",
    "confirm_success",
    "confirm_json",
    "critical_uncertain",
]

SETTLEMENT_FIELDS = [
    "event_time_utc",
    "mode",
    "strategy_id",
    "slug",
    "direction",
    "token_id",
    "market_end_time_utc",
    "entry_time_utc",
    "cash_locked_seconds",
    "price_to_beat",
    "chainlink_btcusd",
    "final_outcome",
    "expected_correct",
    "expected_pnl_usdc",
    "settlement_status",
    "balance_probe_json",
]


@dataclass
class MarketState:
    current_slug: str = ""
    current_market: dict[str, Any] | None = None
    current_tokens: dict[str, str] | None = None
    current_end_ms: int | None = None
    next_slug: str = ""
    next_market: dict[str, Any] | None = None
    next_tokens: dict[str, str] | None = None
    current_fee_rate: float = DEFAULT_FEE_RATE


@dataclass
class PriceSnapshot:
    binance: float | None
    coinbase: float | None
    binance_ts_ms: int | None
    coinbase_ts_ms: int | None
    binance_error: str
    coinbase_error: str


@dataclass
class PendingLiveOrder:
    attempt_key: str
    slug: str
    direction: str
    token_id: str
    signal_time_utc: str
    signal_unix_ms: int
    due_unix_ms: int
    signal_second: float
    signal_best_ask: float | None
    signal_best_bid: float | None
    price_to_beat: float
    binance_price: float
    coinbase_price: float
    chainlink_price: float
    binance_delta: float
    coinbase_delta: float
    chainlink_delta: float
    end_time_ms: int
    fee_rate: float


@dataclass
class LivePosition:
    strategy_id: str
    slug: str
    direction: str
    token_id: str
    entry_time_utc: str
    end_time_ms: int
    price_to_beat: float
    expected_shares: float
    expected_total_cash_used: float
    order_id: str
    mode: str
    settlement_logged: bool = False
    cash_released_utc: str = ""


class PricePoller:
    def __init__(self, interval_seconds: float, timeout_seconds: float) -> None:
        self.interval_seconds = interval_seconds
        self.timeout_seconds = timeout_seconds
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._snapshot = PriceSnapshot(None, None, None, None, "not_started", "not_started")

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def snapshot(self) -> PriceSnapshot:
        with self._lock:
            return self._snapshot

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.perf_counter()
            now_ms = int(time.time() * 1000)
            binance_payload, _binance_latency_ms, binance_error = fetch_json(BINANCE_BTCUSDT_URL, self.timeout_seconds)
            coinbase_payload, _coinbase_latency_ms, coinbase_error = fetch_json(COINBASE_BTCUSD_URL, self.timeout_seconds)
            with self._lock:
                self._snapshot = PriceSnapshot(
                    binance=None if binance_error else parse_binance_price(binance_payload),
                    coinbase=None if coinbase_error else parse_coinbase_price(coinbase_payload),
                    binance_ts_ms=now_ms,
                    coinbase_ts_ms=now_ms,
                    binance_error=binance_error,
                    coinbase_error=coinbase_error,
                )
            elapsed = time.perf_counter() - started
            self._stop.wait(max(0.0, self.interval_seconds - elapsed))


class LiveClobClient:
    def __init__(self, config: dict[str, str], mode: str) -> None:
        self.config = config
        self.mode = mode
        self.client: Any | None = None
        self.sdk_error = ""

    def initialize(self) -> None:
        try:
            from py_clob_client.client import ClobClient
            from py_clob_client.clob_types import ApiCreds
        except Exception as exc:  # noqa: BLE001
            self.sdk_error = f"py_clob_client_import_failed: {type(exc).__name__}: {exc}"
            if self.mode == "live":
                raise RuntimeError(self.sdk_error) from None
            return

        if self.mode not in {"live", "preflight"}:
            return

        try:
            signature_type = int(self.config["POLYMARKET_SIGNATURE_TYPE"])
            creds = ApiCreds(
                api_key=self.config["POLYMARKET_API_KEY"],
                api_secret=self.config["POLYMARKET_API_SECRET"],
                api_passphrase=self.config["POLYMARKET_API_PASSPHRASE"],
            )
            self.client = ClobClient(
                HOST,
                key=self.config["POLYMARKET_PRIVATE_KEY"],
                chain_id=CHAIN_ID,
                creds=creds,
                signature_type=signature_type,
                funder=self.config.get("POLYMARKET_FUNDER_ADDRESS") or None,
            )
        except Exception as exc:  # noqa: BLE001
            self.sdk_error = f"py_clob_client_initialize_failed: {type(exc).__name__}: {exc}"
            raise RuntimeError(self.sdk_error) from None

    def place_fok_buy(self, token_id: str, amount_usdc: float) -> dict[str, Any]:
        if self.mode != "live":
            return {
                "dry_run": True,
                "status": "not_sent",
                "order_id": "",
                "success": False,
                "message": "dry-run mode: no real order sent",
            }
        if self.client is None:
            raise RuntimeError("live client is not initialized")
        from py_clob_client.clob_types import MarketOrderArgs, OrderType
        from py_clob_client.order_builder.constants import BUY

        order_args = MarketOrderArgs(
            token_id=token_id,
            amount=amount_usdc,
            side=BUY,
            order_type=OrderType.FOK,
        )
        signed_order = self.client.create_market_order(order_args)
        response = self.client.post_order(signed_order, OrderType.FOK)
        return response if isinstance(response, dict) else {"raw_response": str(response)}

    def collateral_probe(self) -> dict[str, Any]:
        if self.client is None:
            return {"ok": False, "error": self.sdk_error or "sdk_not_initialized"}
        try:
            from py_clob_client.clob_types import AssetType, BalanceAllowanceParams

            params = BalanceAllowanceParams(
                asset_type=AssetType.COLLATERAL,
                signature_type=int(self.config["POLYMARKET_SIGNATURE_TYPE"]),
            )
            response = self.client.get_balance_allowance(params)
            return {"ok": True, "response": response}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    def open_orders_probe(self) -> dict[str, Any]:
        if self.client is None:
            return {"ok": False, "error": self.sdk_error or "sdk_not_initialized"}
        try:
            response = self.client.get_orders()
            return {"ok": True, "response": response}
        except Exception as exc:  # noqa: BLE001
            return {"ok": False, "error": f"{type(exc).__name__}: {exc}"}

    def confirm_order_fill(self, order_id: str, token_id: str) -> dict[str, Any]:
        if self.mode != "live":
            return {"ok": True, "dry_run": True, "order": {}, "trades": []}
        if self.client is None:
            return {"ok": False, "error": "sdk_not_initialized"}
        result: dict[str, Any] = {"ok": True, "order": {}, "trades": []}
        if not order_id:
            return {"ok": False, "error": "missing_order_id"}
        try:
            result["order"] = self.client.get_order(order_id)
        except Exception as exc:  # noqa: BLE001
            result["ok"] = False
            result["order_error"] = f"{type(exc).__name__}: {exc}"
        try:
            from py_clob_client.clob_types import TradeParams

            result["trades"] = self.client.get_trades(TradeParams(asset_id=token_id))
        except Exception as exc:  # noqa: BLE001
            result["ok"] = False
            result["trades_error"] = f"{type(exc).__name__}: {exc}"
        return result

    def account_probe(self) -> dict[str, Any]:
        if self.client is None:
            return {"sdk_initialized": False, "sdk_error": self.sdk_error}
        return {
            "sdk_initialized": True,
            "collateral": self.collateral_probe(),
            "open_orders": self.open_orders_probe(),
        }


def handle_stop_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global STOP_REQUESTED
    STOP_REQUESTED = True


def utc_now() -> datetime:
    return datetime.now(UTC)


def iso_now() -> str:
    return utc_now().isoformat()


def append_audit(path: Path, event_type: str, row: dict[str, Any]) -> None:
    append_jsonl(path, {"event_time_utc": iso_now(), "event_type": event_type, **row})


def mask_secret(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 10:
        return "***"
    return f"{value[:6]}...{value[-4:]}"


def load_env_file(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


def load_config(env_file: Path | None) -> dict[str, str]:
    if env_file:
        load_env_file(env_file)
    keys = [
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_FUNDER_ADDRESS",
        "POLYMARKET_SIGNATURE_TYPE",
    ]
    return {key: os.environ.get(key, "") for key in keys}


def validate_mode(args: argparse.Namespace) -> str:
    selected = [args.dry_run, args.preflight, args.live]
    if sum(1 for item in selected if item) != 1:
        raise SystemExit("Must specify exactly one mode: --dry-run, --preflight, or --live.")
    if args.live and not args.confirm_live_trading:
        raise SystemExit("Live mode requires --confirm-live-trading.")
    if args.live:
        return "live"
    if args.preflight:
        return "preflight"
    return "dry-run"


def validate_config(config: dict[str, str], mode: str) -> list[str]:
    required = [
        "POLYMARKET_PRIVATE_KEY",
        "POLYMARKET_API_KEY",
        "POLYMARKET_API_SECRET",
        "POLYMARKET_API_PASSPHRASE",
        "POLYMARKET_SIGNATURE_TYPE",
    ]
    if mode in {"live", "preflight"}:
        required.append("POLYMARKET_FUNDER_ADDRESS")
    missing = [key for key in required if not config.get(key)]
    try:
        int(config.get("POLYMARKET_SIGNATURE_TYPE") or "")
    except ValueError:
        missing.append("POLYMARKET_SIGNATURE_TYPE_INT")
    return missing


def parse_numeric(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        cleaned = value.replace(",", "").strip()
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None


def find_numeric_by_keys(payload: Any, keys: set[str]) -> float | None:
    if isinstance(payload, dict):
        for key, value in payload.items():
            lowered = str(key).lower()
            if lowered in keys:
                parsed = parse_numeric(value)
                if parsed is not None:
                    return parsed
        for value in payload.values():
            found = find_numeric_by_keys(value, keys)
            if found is not None:
                return found
    elif isinstance(payload, list):
        for item in payload:
            found = find_numeric_by_keys(item, keys)
            if found is not None:
                return found
    return None


def normalize_list_response(response: Any) -> list[Any]:
    if isinstance(response, list):
        return response
    if isinstance(response, dict):
        for key in ["data", "orders", "trades", "results"]:
            value = response.get(key)
            if isinstance(value, list):
                return value
    return []


def is_btc_5m_position(position: dict[str, Any]) -> bool:
    text = " ".join(
        str(position.get(key) or "")
        for key in ["slug", "market_slug", "marketSlug", "title", "market", "eventSlug"]
    ).lower()
    return "bitcoin-up-or-down" in text or "bitcoin up or down" in text


def fetch_user_positions(user_address: str, timeout: float, size_threshold: float = 0.0001) -> tuple[list[dict[str, Any]], str]:
    if not user_address:
        return [], "missing_user_address"
    query = urlencode({"user": user_address, "sizeThreshold": size_threshold})
    payload, _latency, error = fetch_json(f"{DATA_API_POSITIONS_URL}?{query}", timeout)
    if error:
        return [], error
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], ""
    return [], "unexpected_positions_response"


def position_matches_bot_key(position: dict[str, Any], managed_positions: dict[str, LivePosition]) -> bool:
    asset = str(position.get("asset") or position.get("assetId") or position.get("token_id") or "")
    slug = str(position.get("slug") or position.get("market_slug") or position.get("marketSlug") or "")
    for live_position in managed_positions.values():
        if asset and asset == live_position.token_id:
            return True
        if slug and slug == live_position.slug:
            return True
    return False


def load_state(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"version": 1, "positions": []}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:  # noqa: BLE001
        return {"version": 1, "positions": [], "state_error": "unreadable_state_json"}
    return payload if isinstance(payload, dict) else {"version": 1, "positions": [], "state_error": "unexpected_state_json"}


def live_position_from_state(item: dict[str, Any]) -> LivePosition | None:
    try:
        return LivePosition(
            strategy_id=str(item["strategy_id"]),
            slug=str(item["slug"]),
            direction=str(item["direction"]),
            token_id=str(item["token_id"]),
            entry_time_utc=str(item["entry_time_utc"]),
            end_time_ms=int(item["end_time_ms"]),
            price_to_beat=float(item["price_to_beat"]),
            expected_shares=float(item["expected_shares"]),
            expected_total_cash_used=float(item["expected_total_cash_used"]),
            order_id=str(item.get("order_id") or ""),
            mode=str(item.get("mode") or "live"),
            settlement_logged=bool(item.get("settlement_logged", False)),
            cash_released_utc=str(item.get("cash_released_utc") or ""),
        )
    except Exception:  # noqa: BLE001
        return None


def restore_positions_from_state(path: Path) -> dict[str, LivePosition]:
    state = load_state(path)
    positions: dict[str, LivePosition] = {}
    for item in state.get("positions", []):
        if not isinstance(item, dict):
            continue
        position = live_position_from_state(item)
        if position and not position.cash_released_utc:
            positions[f"{position.slug}:{position.strategy_id}"] = position
    return positions


def save_state(path: Path, positions: dict[str, LivePosition]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "version": 1,
        "updated_at_utc": iso_now(),
        "positions": [asdict(position) for position in positions.values()],
    }
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    tmp_path.replace(path)


def run_account_preflight(
    clob: LiveClobClient,
    config: dict[str, str],
    args: argparse.Namespace,
    positions: dict[str, LivePosition],
) -> dict[str, Any]:
    collateral_probe = clob.collateral_probe()
    open_orders_probe = clob.open_orders_probe()
    user_positions, positions_error = fetch_user_positions(config.get("POLYMARKET_FUNDER_ADDRESS", ""), args.timeout_seconds)
    btc_positions = [item for item in user_positions if is_btc_5m_position(item)]
    unknown_btc_positions = [item for item in btc_positions if not position_matches_bot_key(item, positions)]
    open_orders = normalize_list_response(open_orders_probe.get("response"))
    balance = find_numeric_by_keys(collateral_probe.get("response"), {"balance", "usdc", "collateral"})
    allowance = find_numeric_by_keys(collateral_probe.get("response"), {"allowance"})
    errors: list[str] = []
    if not collateral_probe.get("ok"):
        errors.append(f"collateral_probe_failed: {collateral_probe.get('error')}")
    if balance is None or balance < args.order_cash_usdc:
        errors.append("insufficient_or_unreadable_collateral_balance")
    if allowance is None or allowance < args.order_cash_usdc:
        errors.append("insufficient_or_unreadable_collateral_allowance")
    if not open_orders_probe.get("ok"):
        errors.append(f"open_orders_probe_failed: {open_orders_probe.get('error')}")
    if open_orders:
        errors.append(f"existing_open_orders: {len(open_orders)}")
    if positions_error:
        errors.append(f"user_positions_probe_failed: {positions_error}")
    if unknown_btc_positions:
        errors.append(f"unknown_btc_5m_positions: {len(unknown_btc_positions)}")
    return {
        "ok": not errors,
        "errors": errors,
        "collateral_probe": collateral_probe,
        "balance": balance,
        "allowance": allowance,
        "open_orders_count": len(open_orders),
        "btc_positions_count": len(btc_positions),
        "unknown_btc_positions_count": len(unknown_btc_positions),
    }


async def refresh_market_state(state: MarketState, timeout: float) -> None:
    now_ms = int(time.time() * 1000)
    base_start = current_start_ts(now_ms // 1000)
    current_market: dict[str, Any] | None = None
    next_market: dict[str, Any] | None = None
    current_slug = ""
    next_slug = ""
    for start_ts in [base_start - 300, base_start, base_start + 300]:
        slug = slug_for_start(start_ts)
        market, _latency, error = fetch_market_by_slug(slug, timeout)
        if error or not market:
            continue
        start_since, to_end, end_ms = market_timing(market, now_ms)
        if start_since is not None and to_end is not None and start_since >= 0 and to_end > 0:
            current_market = market
            current_slug = slug
            state.current_end_ms = end_ms
            break
    if current_market:
        next_slug = slug_for_start(int(current_slug.rsplit("-", 1)[-1]) + 300)
        next_market, _latency, error = fetch_market_by_slug(next_slug, timeout)
        if error:
            next_market = None
    state.current_slug = current_slug
    state.current_market = current_market
    state.current_tokens = market_tokens(current_market) if current_market else {}
    state.current_fee_rate = fee_rate_for_market(current_market)
    state.next_slug = next_slug
    state.next_market = next_market
    state.next_tokens = market_tokens(next_market) if next_market else {}


def seed_subscribed_books(args: argparse.Namespace, state: MarketState, store: OrderBookStore) -> None:
    received_ms = int(time.time() * 1000)
    for token_id in subscribed_token_ids(state):
        book = store.snapshot_book(token_id, args.book_levels)
        if not book.get("book_error"):
            continue
        rest_book = fetch_book(token_id, args.timeout_seconds, args.book_levels)
        if not rest_book.get("book_error"):
            store.seed_book(token_id, rest_book, received_ms)


def book_for_direction(direction: str, state: MarketState, store: OrderBookStore, max_levels: int) -> dict[str, Any]:
    token_id = (state.current_tokens or {}).get(direction, "")
    return store.snapshot_book(token_id, max_levels) if token_id else {"bids": [], "asks": [], "book_error": "missing_token"}


def is_price_fresh(now_ms: int, prices: PriceSnapshot, rtds: dict[str, Any]) -> bool:
    chainlink_ts = rtds.get("chainlink_timestamp_ms")
    timestamps = [prices.binance_ts_ms, prices.coinbase_ts_ms, chainlink_ts]
    return all(ts and now_ms - int(ts) <= MAX_PRICE_SOURCE_AGE_MS for ts in timestamps)


def final_outcome(chainlink_price: float | None, price_to_beat: float | None) -> str:
    if chainlink_price is None or price_to_beat is None:
        return ""
    return "UP" if chainlink_price > price_to_beat else "DOWN"


def maybe_capture_price_to_beat(
    args: argparse.Namespace,
    mode: str,
    slug: str,
    market: dict[str, Any] | None,
    now_ms: int,
    chainlink_price: float | None,
    price_to_beat_by_slug: dict[str, dict[str, Any]],
) -> None:
    if not slug or not market or chainlink_price is None or slug in price_to_beat_by_slug:
        return
    observed_second, _to_end, _end_ms = market_timing(market, now_ms)
    observed_second_float = float(observed_second) if observed_second is not None else None
    price_is_valid = (
        observed_second_float is not None
        and 0 <= observed_second_float <= args.max_price_to_beat_observed_second
    )
    price_to_beat_by_slug[slug] = {
        "price": chainlink_price if price_is_valid else None,
        "observed_at_utc": iso_now(),
        "observed_second": observed_second_float,
        "status": price_to_beat_status(observed_second) if price_is_valid else "late_or_missing_price_to_beat",
    }
    if not price_is_valid:
        append_audit(
            args.audit_jsonl,
            "price_to_beat_rejected",
            {
                "mode": mode,
                "slug": slug,
                "observed_second": observed_second_float,
                "max_price_to_beat_observed_second": args.max_price_to_beat_observed_second,
                "chainlink_btcusd": rounded(chainlink_price),
            },
        )


def response_success(response: dict[str, Any]) -> bool:
    if response.get("success") is True:
        return True
    status = str(response.get("status") or response.get("orderStatus") or "").lower()
    return status in {"matched", "filled", "success"}


def response_order_id(response: dict[str, Any]) -> str:
    for key in ["orderID", "order_id", "id"]:
        if response.get(key):
            return str(response[key])
    return ""


def count_existing_lines(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def write_report(
    path: Path,
    mode: str,
    started_at: str,
    ended_at: str,
    counters: dict[str, int | float],
    stop_reason: str,
    config: dict[str, str],
    sdk_error: str,
    output_paths: dict[str, Path],
) -> None:
    text = f"""# Polymarket BTC 5分钟真钱交易 v1 运行报告

生成时间：{ended_at}

## 结论

- 运行模式：`{mode}`
- 停止原因：`{stop_reason}`
- SDK 状态：`{sdk_error or "ok"}`
- 本报告是真钱执行器报告，不与 paper 回测收益混合。

## 账户配置检查

| 项目 | 值 |
|---|---|
| API Key | `{mask_secret(config.get("POLYMARKET_API_KEY", ""))}` |
| Funder | `{mask_secret(config.get("POLYMARKET_FUNDER_ADDRESS", ""))}` |
| Signature Type | `{config.get("POLYMARKET_SIGNATURE_TYPE", "")}` |

## 运行统计

| 项目 | 数值 |
|---|---:|
| 开始时间 | {started_at} |
| 结束时间 | {ended_at} |
| 信号数 | {counters.get("signals", 0)} |
| 下单尝试数 | {counters.get("order_attempts", 0)} |
| 成功成交数 | {counters.get("orders_success", 0)} |
| 失败下单数 | {counters.get("orders_failed", 0)} |
| 风控跳过数 | {counters.get("risk_skips", 0)} |
| 结算记录数 | {counters.get("settlements", 0)} |
| 预估已实现盈亏 | {round(float(counters.get("expected_realized_pnl", 0.0)), 6)} |

## 输出文件

- 审计事件：`{output_paths["audit_jsonl"]}`
- 订单记录：`{output_paths["orders_csv"]}`
- 结算记录：`{output_paths["settlements_csv"]}`
- 原始盘口压缩流：`{output_paths["raw_events_gz"]}`
"""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


async def trade_loop(args: argparse.Namespace, mode: str, config: dict[str, str]) -> int:
    started_at = iso_now()
    state = MarketState()
    store = OrderBookStore()
    rtds_cache = RtdsPriceCache()
    price_poller = PricePoller(args.price_poll_interval_seconds, args.timeout_seconds)
    clob = LiveClobClient(config, mode)
    clob.initialize()
    positions: dict[str, LivePosition] = restore_positions_from_state(args.state_json) if mode in {"live", "preflight"} else {}
    filled_markets: set[str] = set(positions.keys())
    restored_locked_cash = sum(position.expected_total_cash_used for position in positions.values() if not position.cash_released_utc)

    counters: dict[str, int | float] = {
        "signals": 0,
        "order_attempts": 0,
        "orders_success": 0,
        "orders_failed": 0,
        "risk_skips": 0,
        "settlements": 0,
        "expected_realized_pnl": 0.0,
        "restored_open_positions": len(positions),
    }
    stop_reason = "duration_complete"
    consecutive_failures = 0
    locked_cash = restored_locked_cash
    attempted_pending: set[str] = set()
    pending_orders: dict[str, PendingLiveOrder] = {}
    price_to_beat_by_slug: dict[str, dict[str, Any]] = {}
    preflight_result: dict[str, Any] = {"ok": True, "skipped": mode == "dry-run"}
    if mode in {"live", "preflight"}:
        preflight_result = run_account_preflight(clob, config, args, positions)
        if not preflight_result.get("ok"):
            stop_reason = "account_preflight_failed"

    append_audit(
        args.audit_jsonl,
        "startup",
        {
            "mode": mode,
            "strategy_id": STRATEGY_ID,
            "order_cash_usdc": args.order_cash_usdc,
            "max_locked_usdc": args.max_locked_usdc,
            "max_daily_loss_usdc": args.max_daily_loss_usdc,
            "restored_locked_usdc": rounded(locked_cash),
            "restored_open_positions": len(positions),
            "config_masked": {
                "api_key": mask_secret(config.get("POLYMARKET_API_KEY", "")),
                "funder": mask_secret(config.get("POLYMARKET_FUNDER_ADDRESS", "")),
                "signature_type": config.get("POLYMARKET_SIGNATURE_TYPE", ""),
            },
            "account_preflight": preflight_result,
        },
    )

    if mode == "preflight" or not preflight_result.get("ok"):
        ended_at = iso_now()
        write_report(
            args.report,
            mode,
            started_at,
            ended_at,
            counters,
            stop_reason if not preflight_result.get("ok") else "preflight_complete",
            config,
            clob.sdk_error,
            {
                "audit_jsonl": args.audit_jsonl,
                "orders_csv": args.orders_csv,
                "settlements_csv": args.settlements_csv,
                "raw_events_gz": args.raw_events_gz,
            },
        )
        return 0 if preflight_result.get("ok") else 1

    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)
    rtds_cache.start()
    price_poller.start()
    await asyncio.sleep(max(0.0, args.rtds_warmup_seconds))
    start_mono = time.monotonic()

    try:
        with JsonlGzipWriter(args.raw_events_gz, not args.disable_raw_events) as raw_writer:
            while not STOP_REQUESTED:
                if args.duration_seconds > 0 and time.monotonic() - start_mono >= args.duration_seconds:
                    break
                try:
                    await refresh_market_state(state, args.timeout_seconds)
                    await stream_once(
                        args,
                        mode,
                        state,
                        store,
                        raw_writer,
                        rtds_cache,
                        price_poller,
                        clob,
                        price_to_beat_by_slug,
                        attempted_pending,
                        filled_markets,
                        pending_orders,
                        positions,
                        counters,
                        consecutive_failures_ref={"value": consecutive_failures},
                        locked_cash_ref={"value": locked_cash},
                        start_mono=start_mono,
                    )
                    consecutive_failures = int(counters.get("_consecutive_failures", 0))
                    locked_cash = float(counters.get("_locked_cash", locked_cash))
                    if counters.get("_critical_stop_reason"):
                        stop_reason = str(counters["_critical_stop_reason"])
                        break
                    if consecutive_failures >= args.max_consecutive_failures:
                        stop_reason = "max_consecutive_order_failures"
                        break
                    if float(counters.get("expected_realized_pnl", 0.0)) <= -args.max_daily_loss_usdc:
                        stop_reason = "max_daily_loss"
                        break
                except Exception as exc:  # noqa: BLE001
                    append_audit(args.audit_jsonl, "recoverable_stream_error", {"error": f"{type(exc).__name__}: {exc}"})
                    await asyncio.sleep(args.reconnect_sleep_seconds)
    finally:
        rtds_cache.stop()
        price_poller.stop()
        ended_at = iso_now()
        write_report(
            args.report,
            mode,
            started_at,
            ended_at,
            counters,
            stop_reason,
            config,
            clob.sdk_error,
            {
                "audit_jsonl": args.audit_jsonl,
                "orders_csv": args.orders_csv,
                "settlements_csv": args.settlements_csv,
                "raw_events_gz": args.raw_events_gz,
            },
        )
    return 0


async def stream_once(
    args: argparse.Namespace,
    mode: str,
    state: MarketState,
    store: OrderBookStore,
    raw_writer: JsonlGzipWriter,
    rtds_cache: RtdsPriceCache,
    price_poller: PricePoller,
    clob: LiveClobClient,
    price_to_beat_by_slug: dict[str, dict[str, Any]],
    attempted_pending: set[str],
    filled_markets: set[str],
    pending_orders: dict[str, PendingLiveOrder],
    positions: dict[str, LivePosition],
    counters: dict[str, int | float],
    consecutive_failures_ref: dict[str, int],
    locked_cash_ref: dict[str, float],
    start_mono: float,
) -> None:
    token_ids = subscribed_token_ids(state)
    if not token_ids:
        await asyncio.sleep(1)
        return
    seed_subscribed_books(args, state, store)
    last_process_mono = 0.0
    last_ping_mono = time.monotonic()
    last_refresh_mono = time.monotonic()

    import websockets

    async with websockets.connect(args.ws_url, ping_interval=None, open_timeout=15, proxy=None) as websocket:
        await websocket.send(json.dumps({"assets_ids": token_ids, "type": "market"}, separators=(",", ":")))
        while not STOP_REQUESTED:
            if args.duration_seconds > 0 and time.monotonic() - start_mono >= args.duration_seconds:
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
            if time.monotonic() - last_process_mono >= args.process_interval_seconds:
                process_live_once(
                    args,
                    mode,
                    state,
                    store,
                    rtds_cache,
                    price_poller,
                    clob,
                    price_to_beat_by_slug,
                    attempted_pending,
                    filled_markets,
                    pending_orders,
                    positions,
                    counters,
                    consecutive_failures_ref,
                    locked_cash_ref,
                )
                counters["_consecutive_failures"] = consecutive_failures_ref["value"]
                counters["_locked_cash"] = locked_cash_ref["value"]
                if counters.get("_critical_stop_reason"):
                    break
                if consecutive_failures_ref["value"] >= args.max_consecutive_failures:
                    break
                if float(counters.get("expected_realized_pnl", 0.0)) <= -args.max_daily_loss_usdc:
                    break
                last_process_mono = time.monotonic()


def process_live_once(
    args: argparse.Namespace,
    mode: str,
    state: MarketState,
    store: OrderBookStore,
    rtds_cache: RtdsPriceCache,
    price_poller: PricePoller,
    clob: LiveClobClient,
    price_to_beat_by_slug: dict[str, dict[str, Any]],
    attempted_pending: set[str],
    filled_markets: set[str],
    pending_orders: dict[str, PendingLiveOrder],
    positions: dict[str, LivePosition],
    counters: dict[str, int | float],
    consecutive_failures_ref: dict[str, int],
    locked_cash_ref: dict[str, float],
) -> None:
    now = utc_now()
    now_ms = int(now.timestamp() * 1000)
    if not state.current_market or not state.current_slug:
        return
    current_since, _current_to_end, current_end_ms = market_timing(state.current_market, now_ms)
    prices = price_poller.snapshot()
    rtds = rtds_cache.snapshot()
    chainlink_price = rtds.get("chainlink_btcusd")
    chainlink_ts = rtds.get("chainlink_timestamp_ms")

    maybe_capture_price_to_beat(
        args,
        mode,
        state.current_slug,
        state.current_market,
        now_ms,
        chainlink_price,
        price_to_beat_by_slug,
    )
    maybe_capture_price_to_beat(
        args,
        mode,
        state.next_slug,
        state.next_market,
        now_ms,
        chainlink_price,
        price_to_beat_by_slug,
    )
    price_meta = price_to_beat_by_slug.get(state.current_slug, {})
    price_to_beat = price_meta.get("price")
    direction, reason, bd, cd = determine_signal(prices.binance, prices.coinbase, price_to_beat, THRESHOLD_USD)
    if direction:
        counters["signals"] = int(counters.get("signals", 0)) + 1

    if direction and current_since is not None and current_end_ms is not None and price_to_beat is not None:
        book = book_for_direction(direction, state, store, args.book_levels)
        best_ask = book.get("best_ask")
        last_received_ms = book.get("last_received_ms")
        book_age_ms = now_ms - int(last_received_ms) if last_received_ms else None
        chainlink_delta = float(chainlink_price) - float(price_to_beat) if chainlink_price is not None else 0.0
        signal_base = {
            "mode": mode,
            "strategy_id": STRATEGY_ID,
            "slug": state.current_slug,
            "direction": direction,
            "token_id": (state.current_tokens or {}).get(direction, ""),
            "seconds_since_start": rounded(current_since, 3),
            "price_to_beat": rounded(price_to_beat),
            "binance_btcusdt": rounded(prices.binance),
            "coinbase_btcusd": rounded(prices.coinbase),
            "chainlink_btcusd": rounded(chainlink_price),
            "binance_delta": rounded(bd),
            "coinbase_delta": rounded(cd),
            "chainlink_delta": rounded(chainlink_delta),
            "signal_best_ask": rounded(best_ask),
            "signal_best_bid": rounded(book.get("best_bid")),
            "book_age_ms": book_age_ms,
        }
        append_audit(args.audit_jsonl, "signal", signal_base)
        attempt_key = f"{state.current_slug}:{STRATEGY_ID}"
        if attempt_key not in filled_markets and attempt_key not in attempted_pending and attempt_key not in pending_orders:
            skip_reason = ""
            if current_since < ENTRY_START_SECOND:
                skip_reason = "before_entry_start"
            elif current_since > ENTRY_END_SECOND:
                skip_reason = "outside_entry_window"
            elif not is_price_fresh(now_ms, prices, rtds):
                skip_reason = "stale_price_source"
            elif book.get("book_error") or best_ask is None:
                skip_reason = "missing_orderbook"
            elif book_age_ms is None or book_age_ms > args.max_book_age_ms:
                skip_reason = "stale_orderbook"
            elif best_ask > ENTRY_CAP:
                skip_reason = "target_ask_above_cap"
            elif locked_cash_ref["value"] + args.order_cash_usdc > args.max_locked_usdc:
                skip_reason = "max_locked_capital"
            if skip_reason:
                counters["risk_skips"] = int(counters.get("risk_skips", 0)) + 1
                append_audit(args.audit_jsonl, "skip", {**signal_base, "skip_reason": skip_reason})
            else:
                attempted_pending.add(attempt_key)
                pending_orders[attempt_key] = PendingLiveOrder(
                    attempt_key=attempt_key,
                    slug=state.current_slug,
                    direction=direction,
                    token_id=(state.current_tokens or {}).get(direction, ""),
                    signal_time_utc=now.isoformat(),
                    signal_unix_ms=now_ms,
                    due_unix_ms=now_ms + args.entry_latency_ms,
                    signal_second=float(current_since),
                    signal_best_ask=best_ask,
                    signal_best_bid=book.get("best_bid"),
                    price_to_beat=float(price_to_beat),
                    binance_price=float(prices.binance or 0.0),
                    coinbase_price=float(prices.coinbase or 0.0),
                    chainlink_price=float(chainlink_price or 0.0),
                    binance_delta=float(bd or 0.0),
                    coinbase_delta=float(cd or 0.0),
                    chainlink_delta=float(chainlink_delta or 0.0),
                    end_time_ms=current_end_ms,
                    fee_rate=state.current_fee_rate,
                )

    for key, pending in list(pending_orders.items()):
        if now_ms < pending.due_unix_ms:
            continue
        book = book_for_direction(pending.direction, state, store, args.book_levels)
        arrival_best_ask = book.get("best_ask")
        base_row = {
            "mode": mode,
            "event_type": "order_attempt",
            "strategy_id": STRATEGY_ID,
            "slug": pending.slug,
            "direction": pending.direction,
            "token_id": pending.token_id,
            "order_cash_usdc": args.order_cash_usdc,
            "entry_cap": ENTRY_CAP,
            "entry_latency_ms": args.entry_latency_ms,
            "signal_time_utc": pending.signal_time_utc,
            "order_arrival_time_utc": datetime.fromtimestamp(pending.due_unix_ms / 1000, UTC).isoformat(),
            "seconds_since_start": rounded(pending.signal_second, 3),
            "price_to_beat": rounded(pending.price_to_beat),
            "binance_btcusdt": rounded(pending.binance_price),
            "coinbase_btcusd": rounded(pending.coinbase_price),
            "chainlink_btcusd": rounded(pending.chainlink_price),
            "binance_delta": rounded(pending.binance_delta),
            "coinbase_delta": rounded(pending.coinbase_delta),
            "chainlink_delta": rounded(pending.chainlink_delta),
            "signal_best_ask": rounded(pending.signal_best_ask),
            "arrival_best_ask": rounded(arrival_best_ask),
            "pre_order_locked_usdc": rounded(locked_cash_ref["value"]),
            "available_cash_usdc": "",
        }
        if book.get("book_error") or arrival_best_ask is None or arrival_best_ask > ENTRY_CAP:
            append_audit(args.audit_jsonl, "order_skip_after_latency", {**base_row, "skip_reason": "price_or_book_invalid_after_latency"})
            pending_orders.pop(key, None)
            attempted_pending.discard(key)
            continue
        fill = quote_buy_with_total_cash(book.get("asks", []), args.order_cash_usdc, pending.fee_rate, ENTRY_CAP)
        if not fill.complete or not fill.avg_price or not fill.worst_price or fill.shares <= 0:
            append_audit(args.audit_jsonl, "order_skip_after_latency", {**base_row, "skip_reason": "insufficient_depth_after_latency"})
            pending_orders.pop(key, None)
            attempted_pending.discard(key)
            continue
        counters["order_attempts"] = int(counters.get("order_attempts", 0)) + 1
        started = time.perf_counter()
        error = ""
        try:
            response = clob.place_fok_buy(pending.token_id, args.order_cash_usdc)
        except Exception as exc:  # noqa: BLE001
            response = {}
            error = f"{type(exc).__name__}: {exc}"
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        success = response_success(response) if not error else False
        order_id = response_order_id(response)
        confirm_result = clob.confirm_order_fill(order_id, pending.token_id) if success else {}
        confirm_success = bool(confirm_result.get("ok")) if success else False
        critical_uncertain = bool(success and mode == "live" and not confirm_success)
        row = {
            **base_row,
            "event_time_utc": iso_now(),
            "paper_avg_price": rounded(fill.avg_price),
            "paper_worst_price": rounded(fill.worst_price),
            "paper_shares": rounded(fill.shares),
            "paper_total_cash_used": rounded(fill.total_cash_used),
            "order_id": order_id,
            "status": response.get("status") or response.get("orderStatus") or "",
            "success": success,
            "error": error,
            "response_json": json.dumps(response, ensure_ascii=False),
            "sdk_latency_ms": latency_ms,
            "confirm_success": confirm_success,
            "confirm_json": json.dumps(confirm_result, ensure_ascii=False),
            "critical_uncertain": critical_uncertain,
        }
        append_csv(args.orders_csv, ORDER_FIELDS, row)
        append_audit(args.audit_jsonl, "order_result", row)
        pending_orders.pop(key, None)
        attempted_pending.discard(key)
        if success:
            counters["orders_success"] = int(counters.get("orders_success", 0)) + 1
            consecutive_failures_ref["value"] = 0
            filled_markets.add(key)
            locked_cash_ref["value"] += args.order_cash_usdc
            positions[key] = LivePosition(
                strategy_id=STRATEGY_ID,
                slug=pending.slug,
                direction=pending.direction,
                token_id=pending.token_id,
                entry_time_utc=iso_now(),
                end_time_ms=pending.end_time_ms,
                price_to_beat=pending.price_to_beat,
                expected_shares=float(fill.shares),
                expected_total_cash_used=float(fill.total_cash_used),
                order_id=order_id,
                mode=mode,
            )
            save_state(args.state_json, positions)
            if critical_uncertain:
                counters["_critical_stop_reason"] = "order_fill_confirmation_uncertain"
        else:
            counters["orders_failed"] = int(counters.get("orders_failed", 0)) + 1
            consecutive_failures_ref["value"] += 1

    for key, position in list(positions.items()):
        if position.cash_released_utc or now_ms < position.end_time_ms + args.settlement_grace_seconds * 1000:
            continue
        outcome = final_outcome(chainlink_price, position.price_to_beat)
        if not outcome:
            continue
        correct = outcome == position.direction
        expected_pnl = (position.expected_shares if correct else 0.0) - position.expected_total_cash_used
        user_positions, positions_error = fetch_user_positions(clob.config.get("POLYMARKET_FUNDER_ADDRESS", ""), args.timeout_seconds)
        position_still_open = any(
            str(item.get("asset") or item.get("assetId") or item.get("token_id") or "") == position.token_id
            for item in user_positions
        )
        if positions_error or position_still_open:
            if not position.settlement_logged:
                settlement = {
                    "event_time_utc": iso_now(),
                    "mode": mode,
                    "strategy_id": position.strategy_id,
                    "slug": position.slug,
                    "direction": position.direction,
                    "token_id": position.token_id,
                    "market_end_time_utc": datetime.fromtimestamp(position.end_time_ms / 1000, UTC).isoformat(),
                    "entry_time_utc": position.entry_time_utc,
                    "cash_locked_seconds": rounded((now_ms - position.end_time_ms) / 1000),
                    "price_to_beat": rounded(position.price_to_beat),
                    "chainlink_btcusd": rounded(chainlink_price),
                    "final_outcome": outcome,
                    "expected_correct": correct,
                    "expected_pnl_usdc": rounded(expected_pnl),
                    "settlement_status": "position_still_open" if position_still_open else f"positions_probe_failed: {positions_error}",
                    "balance_probe_json": json.dumps(clob.account_probe(), ensure_ascii=False),
                }
                append_csv(args.settlements_csv, SETTLEMENT_FIELDS, settlement)
                append_audit(args.audit_jsonl, "settlement_probe", settlement)
                position.settlement_logged = True
                save_state(args.state_json, positions)
            continue
        locked_cash_ref["value"] = max(0.0, locked_cash_ref["value"] - position.expected_total_cash_used)
        counters["expected_realized_pnl"] = float(counters.get("expected_realized_pnl", 0.0)) + expected_pnl
        counters["settlements"] = int(counters.get("settlements", 0)) + 1
        settlement = {
            "event_time_utc": iso_now(),
            "mode": mode,
            "strategy_id": position.strategy_id,
            "slug": position.slug,
            "direction": position.direction,
            "token_id": position.token_id,
            "market_end_time_utc": datetime.fromtimestamp(position.end_time_ms / 1000, UTC).isoformat(),
            "entry_time_utc": position.entry_time_utc,
            "cash_locked_seconds": rounded((now_ms - position.end_time_ms) / 1000),
            "price_to_beat": rounded(position.price_to_beat),
            "chainlink_btcusd": rounded(chainlink_price),
            "final_outcome": outcome,
            "expected_correct": correct,
            "expected_pnl_usdc": rounded(expected_pnl),
            "settlement_status": "cash_released_after_positions_probe",
            "balance_probe_json": json.dumps(clob.account_probe(), ensure_ascii=False),
        }
        append_csv(args.settlements_csv, SETTLEMENT_FIELDS, settlement)
        append_audit(args.audit_jsonl, "settlement_probe", settlement)
        position.settlement_logged = True
        position.cash_released_utc = settlement["event_time_utc"]
        save_state(args.state_json, positions)


def remove_outputs(paths: list[Path]) -> None:
    for path in paths:
        path.unlink(missing_ok=True)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument("--dry-run", action="store_true")
    mode_group.add_argument("--preflight", action="store_true")
    mode_group.add_argument("--live", action="store_true")
    parser.add_argument("--confirm-live-trading", action="store_true")
    parser.add_argument("--env-file", type=Path)
    parser.add_argument("--duration-seconds", type=float, default=900.0)
    parser.add_argument("--process-interval-seconds", type=float, default=0.2)
    parser.add_argument("--price-poll-interval-seconds", type=float, default=0.5)
    parser.add_argument("--market-refresh-seconds", type=float, default=8.0)
    parser.add_argument("--reconnect-sleep-seconds", type=float, default=2.0)
    parser.add_argument("--ping-seconds", type=float, default=10.0)
    parser.add_argument("--timeout-seconds", type=float, default=8.0)
    parser.add_argument("--rtds-warmup-seconds", type=float, default=2.0)
    parser.add_argument("--book-levels", type=int, default=50)
    parser.add_argument("--ws-url", default=CLOB_MARKET_WS_URL)
    parser.add_argument("--order-cash-usdc", type=float, default=ORDER_CASH_USDC)
    parser.add_argument("--entry-latency-ms", type=int, default=ENTRY_LATENCY_MS)
    parser.add_argument("--max-locked-usdc", type=float, default=MAX_LOCKED_USDC)
    parser.add_argument("--max-daily-loss-usdc", type=float, default=MAX_DAILY_LOSS_USDC)
    parser.add_argument("--max-consecutive-failures", type=int, default=MAX_CONSECUTIVE_ORDER_FAILURES)
    parser.add_argument("--max-book-age-ms", type=int, default=MAX_BOOK_AGE_MS)
    parser.add_argument("--max-price-to-beat-observed-second", type=float, default=5.0)
    parser.add_argument("--settlement-grace-seconds", type=float, default=15.0)
    parser.add_argument("--raw-events-gz", type=Path, default=DEFAULT_RAW_EVENTS_GZ)
    parser.add_argument("--audit-jsonl", type=Path, default=DEFAULT_AUDIT_JSONL)
    parser.add_argument("--orders-csv", type=Path, default=DEFAULT_ORDERS_CSV)
    parser.add_argument("--settlements-csv", type=Path, default=DEFAULT_SETTLEMENTS_CSV)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--state-json", type=Path, default=DEFAULT_STATE_JSON)
    parser.add_argument("--reset-output", action="store_true")
    parser.add_argument("--disable-raw-events", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    mode = validate_mode(args)
    config = load_config(args.env_file)
    missing = validate_config(config, mode)
    if missing:
        raise SystemExit(f"Missing or invalid Polymarket config: {', '.join(missing)}")
    if args.live:
        print("LIVE TRADING ENABLED", flush=True)
        print(f"strategy={STRATEGY_ID} order_cash={args.order_cash_usdc} max_locked={args.max_locked_usdc}", flush=True)
        print(f"funder={mask_secret(config.get('POLYMARKET_FUNDER_ADDRESS', ''))}", flush=True)
    if args.reset_output:
        remove_outputs([args.raw_events_gz, args.audit_jsonl, args.orders_csv, args.settlements_csv, args.report])
    try:
        return asyncio.run(trade_loop(args, mode, config))
    except RuntimeError as exc:
        if str(exc).startswith(("py_clob_client_import_failed", "py_clob_client_initialize_failed")):
            raise SystemExit(f"Live preflight failed: {exc}") from None
        raise


if __name__ == "__main__":
    raise SystemExit(main())
