#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import csv
import json
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v1.csv"
DEFAULT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_price_samples_v1.jsonl"

USER_AGENT = "Mozilla/5.0 (compatible; CodexResearch/1.0)"
BINANCE_BTCUSDT_URL = "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT"
COINBASE_BTCUSD_URL = "https://api.coinbase.com/v2/prices/BTC-USD/spot"
GAMMA_MARKET_URL_TEMPLATE = "https://gamma-api.polymarket.com/markets?slug={slug}"
POLYMARKET_RTDS_WS_URL = "wss://ws-live-data.polymarket.com"

STOP_REQUESTED = False


@dataclass
class PriceSample:
    sampled_at_utc: str
    sampled_at_unix_ms: int
    polymarket_market_slug: str
    polymarket_market_question: str
    polymarket_event_start_time: str
    polymarket_end_time: str
    polymarket_seconds_since_start: float | None
    polymarket_seconds_to_end: float | None
    polymarket_up_price: float | None
    polymarket_down_price: float | None
    polymarket_best_bid: float | None
    polymarket_best_ask: float | None
    polymarket_spread: float | None
    polymarket_liquidity_clob: float | None
    polymarket_volume_24h: float | None
    polymarket_fee_rate: float | None
    polymarket_price_to_beat: float | None
    polymarket_price_to_beat_source: str
    binance_btcusdt: float | None
    rtds_binance_btcusdt: float | None
    rtds_binance_timestamp_ms: int | None
    coinbase_btcusd: float | None
    chainlink_btcusd: float | None
    chainlink_timestamp_ms: int | None
    chainlink_minus_price_to_beat_usd: float | None
    chainlink_minus_binance_usd: float | None
    binance_coinbase_diff_usd: float | None
    binance_coinbase_diff_bps: float | None
    binance_latency_ms: float | None
    coinbase_latency_ms: float | None
    binance_error: str
    coinbase_error: str
    polymarket_error: str
    rtds_error: str
    notes: str


FIELDNAMES = list(PriceSample.__dataclass_fields__.keys())


def handle_stop_signal(signum: int, frame: Any) -> None:  # noqa: ARG001
    global STOP_REQUESTED
    STOP_REQUESTED = True


def fetch_json(url: str, timeout: float) -> tuple[Any | None, float | None, str]:
    started = time.perf_counter()
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "application/json",
        },
    )
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        return payload, latency_ms, ""
    except Exception as exc:  # noqa: BLE001
        latency_ms = round((time.perf_counter() - started) * 1000, 3)
        return None, latency_ms, f"{type(exc).__name__}: {exc}"


def parse_binance_price(payload: Any | None) -> float | None:
    if not payload:
        return None
    return float(payload["price"])


def parse_coinbase_price(payload: Any | None) -> float | None:
    if not payload:
        return None
    return float(payload["data"]["amount"])


def parse_jsonish_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def parse_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def parse_iso_to_unix_ms(value: str) -> int | None:
    if not value:
        return None
    try:
        normalized = value.replace("Z", "+00:00")
        return int(datetime.fromisoformat(normalized).timestamp() * 1000)
    except ValueError:
        return None


def current_btc_5m_slug(now_unix: int | None = None) -> str:
    now_unix = int(time.time()) if now_unix is None else now_unix
    start = now_unix - (now_unix % 300)
    return f"btc-updown-5m-{start}"


def fetch_polymarket_market(timeout: float) -> tuple[dict[str, Any], str]:
    errors: list[str] = []
    now_ms = int(time.time() * 1000)
    now_unix = now_ms // 1000
    base_start = now_unix - (now_unix % 300)
    candidates = [base_start - 300, base_start, base_start + 300]

    fallback: dict[str, Any] = {}
    for start_ts in candidates:
        slug = f"btc-updown-5m-{start_ts}"
        payload, _latency_ms, error = fetch_json(
            GAMMA_MARKET_URL_TEMPLATE.format(slug=slug),
            timeout=timeout,
        )
        if error:
            errors.append(f"{slug}: {error}")
            continue
        if not isinstance(payload, list) or not payload:
            errors.append(f"{slug}: no_market")
            continue

        market = payload[0]
        if not fallback:
            fallback = market
        start_ms = parse_iso_to_unix_ms(str(market.get("eventStartTime") or ""))
        end_ms = parse_iso_to_unix_ms(str(market.get("endDate") or ""))
        is_current = (
            start_ms is not None
            and end_ms is not None
            and start_ms <= now_ms < end_ms
            and not bool(market.get("closed"))
        )
        if is_current:
            return market, "; ".join(errors)

    if fallback:
        return fallback, "; ".join(errors + ["used_fallback_market"])
    return {}, "; ".join(errors) or "no_polymarket_market_found"


def slim_polymarket_market(market: dict[str, Any]) -> dict[str, Any]:
    outcomes = parse_jsonish_list(market.get("outcomes"))
    outcome_prices = parse_jsonish_list(market.get("outcomePrices"))
    price_by_outcome = {
        str(outcome).lower(): parse_float(price)
        for outcome, price in zip(outcomes, outcome_prices, strict=False)
    }
    fee_schedule = market.get("feeSchedule") or {}
    start_time = str(market.get("eventStartTime") or "")
    end_time = str(market.get("endDate") or "")
    now_ms = int(time.time() * 1000)
    start_ms = parse_iso_to_unix_ms(start_time)
    end_ms = parse_iso_to_unix_ms(end_time)

    return {
        "slug": str(market.get("slug") or ""),
        "question": str(market.get("question") or ""),
        "event_start_time": start_time,
        "end_time": end_time,
        "seconds_since_start": round((now_ms - start_ms) / 1000, 3) if start_ms else None,
        "seconds_to_end": round((end_ms - now_ms) / 1000, 3) if end_ms else None,
        "up_price": price_by_outcome.get("up"),
        "down_price": price_by_outcome.get("down"),
        "best_bid": parse_float(market.get("bestBid")),
        "best_ask": parse_float(market.get("bestAsk")),
        "spread": parse_float(market.get("spread")),
        "liquidity_clob": parse_float(market.get("liquidityClob")),
        "volume_24h": parse_float(market.get("volume24hr")),
        "fee_rate": parse_float(fee_schedule.get("rate")),
    }


class RtdsPriceCache:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._thread: threading.Thread | None = None
        self._stop_event = threading.Event()
        self._latest_binance: tuple[float, int] | None = None
        self._latest_chainlink: tuple[float, int] | None = None
        self._latest_error = ""

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            binance_price, binance_ts = self._latest_binance or (None, None)
            chainlink_price, chainlink_ts = self._latest_chainlink or (None, None)
            return {
                "rtds_binance_btcusdt": binance_price,
                "rtds_binance_timestamp_ms": binance_ts,
                "chainlink_btcusd": chainlink_price,
                "chainlink_timestamp_ms": chainlink_ts,
                "rtds_error": self._latest_error,
            }

    def _set_error(self, error: str) -> None:
        with self._lock:
            self._latest_error = error

    def _set_price(self, topic: str, payload: dict[str, Any]) -> None:
        value = parse_float(payload.get("value"))
        timestamp_ms = payload.get("timestamp")
        if value is None or timestamp_ms is None:
            return
        with self._lock:
            if topic == "crypto_prices":
                self._latest_binance = (value, int(timestamp_ms))
            elif topic == "crypto_prices_chainlink":
                self._latest_chainlink = (value, int(timestamp_ms))
            self._latest_error = ""

    def _run(self) -> None:
        try:
            asyncio.run(self._listen_forever())
        except Exception as exc:  # noqa: BLE001
            self._set_error(f"{type(exc).__name__}: {exc}")

    async def _listen_forever(self) -> None:
        try:
            import websockets
        except Exception as exc:  # noqa: BLE001
            self._set_error(f"websockets_import_failed: {type(exc).__name__}: {exc}")
            return

        subscriptions = {
            "action": "subscribe",
            "subscriptions": [
                {
                    "topic": "crypto_prices",
                    "type": "update",
                    "filters": json.dumps({"symbol": "btcusdt"}, separators=(",", ":")),
                },
                {
                    "topic": "crypto_prices_chainlink",
                    "type": "*",
                    "filters": json.dumps({"symbol": "btc/usd"}, separators=(",", ":")),
                },
            ],
        }

        while not self._stop_event.is_set():
            try:
                async with websockets.connect(
                    POLYMARKET_RTDS_WS_URL,
                    ping_interval=None,
                    open_timeout=15,
                    proxy=None,
                ) as websocket:
                    await websocket.send(json.dumps(subscriptions))
                    while not self._stop_event.is_set():
                        message = await asyncio.wait_for(websocket.recv(), timeout=5)
                        self._handle_message(message)
            except asyncio.TimeoutError:
                continue
            except Exception as exc:  # noqa: BLE001
                self._set_error(f"{type(exc).__name__}: {exc}")
                await asyncio.sleep(2)

    def _handle_message(self, message: str) -> None:
        try:
            data = json.loads(message)
        except json.JSONDecodeError:
            return
        topic = data.get("topic")
        payload = data.get("payload")
        if topic in {"crypto_prices", "crypto_prices_chainlink"} and isinstance(payload, dict):
            self._set_price(topic, payload)


def collect_one_sample(
    timeout: float,
    rtds_cache: RtdsPriceCache | None,
    price_to_beat_by_slug: dict[str, float],
) -> PriceSample:
    sampled_at = datetime.now(UTC)
    sampled_at_unix_ms = int(sampled_at.timestamp() * 1000)

    with ThreadPoolExecutor(max_workers=3) as executor:
        binance_future = executor.submit(fetch_json, BINANCE_BTCUSDT_URL, timeout)
        coinbase_future = executor.submit(fetch_json, COINBASE_BTCUSD_URL, timeout)
        polymarket_future = executor.submit(fetch_polymarket_market, timeout)
        binance_payload, binance_latency_ms, binance_error = binance_future.result()
        coinbase_payload, coinbase_latency_ms, coinbase_error = coinbase_future.result()
        polymarket_market, polymarket_error = polymarket_future.result()

    binance_price = parse_binance_price(binance_payload)
    coinbase_price = parse_coinbase_price(coinbase_payload)
    polymarket = slim_polymarket_market(polymarket_market) if polymarket_market else {}
    rtds = rtds_cache.snapshot() if rtds_cache else {
        "rtds_binance_btcusdt": None,
        "rtds_binance_timestamp_ms": None,
        "chainlink_btcusd": None,
        "chainlink_timestamp_ms": None,
        "rtds_error": "rtds_disabled",
    }

    slug = str(polymarket.get("slug") or "")
    chainlink_price = rtds["chainlink_btcusd"]
    if slug and slug not in price_to_beat_by_slug and chainlink_price is not None:
        price_to_beat_by_slug[slug] = chainlink_price

    price_to_beat = price_to_beat_by_slug.get(slug)
    price_to_beat_source = (
        "rtds_chainlink_first_seen_for_window" if price_to_beat is not None else ""
    )

    diff_usd = None
    diff_bps = None
    notes: list[str] = []
    if binance_price is not None and coinbase_price is not None:
        diff_usd = round(binance_price - coinbase_price, 6)
        midpoint = (binance_price + coinbase_price) / 2
        diff_bps = round((diff_usd / midpoint) * 10000, 6) if midpoint else None
    else:
        notes.append("one_or_more_rest_price_sources_failed")

    chainlink_minus_price_to_beat = None
    if chainlink_price is not None and price_to_beat is not None:
        chainlink_minus_price_to_beat = round(chainlink_price - price_to_beat, 6)

    chainlink_minus_binance = None
    if chainlink_price is not None and binance_price is not None:
        chainlink_minus_binance = round(chainlink_price - binance_price, 6)

    seconds_since_start = polymarket.get("seconds_since_start")
    if price_to_beat is not None and seconds_since_start and seconds_since_start > 15:
        notes.append("price_to_beat_proxy_first_observed_after_window_start")

    return PriceSample(
        sampled_at_utc=sampled_at.isoformat(),
        sampled_at_unix_ms=sampled_at_unix_ms,
        polymarket_market_slug=slug,
        polymarket_market_question=str(polymarket.get("question") or ""),
        polymarket_event_start_time=str(polymarket.get("event_start_time") or ""),
        polymarket_end_time=str(polymarket.get("end_time") or ""),
        polymarket_seconds_since_start=seconds_since_start,
        polymarket_seconds_to_end=polymarket.get("seconds_to_end"),
        polymarket_up_price=polymarket.get("up_price"),
        polymarket_down_price=polymarket.get("down_price"),
        polymarket_best_bid=polymarket.get("best_bid"),
        polymarket_best_ask=polymarket.get("best_ask"),
        polymarket_spread=polymarket.get("spread"),
        polymarket_liquidity_clob=polymarket.get("liquidity_clob"),
        polymarket_volume_24h=polymarket.get("volume_24h"),
        polymarket_fee_rate=polymarket.get("fee_rate"),
        polymarket_price_to_beat=price_to_beat,
        polymarket_price_to_beat_source=price_to_beat_source,
        binance_btcusdt=binance_price,
        rtds_binance_btcusdt=rtds["rtds_binance_btcusdt"],
        rtds_binance_timestamp_ms=rtds["rtds_binance_timestamp_ms"],
        coinbase_btcusd=coinbase_price,
        chainlink_btcusd=chainlink_price,
        chainlink_timestamp_ms=rtds["chainlink_timestamp_ms"],
        chainlink_minus_price_to_beat_usd=chainlink_minus_price_to_beat,
        chainlink_minus_binance_usd=chainlink_minus_binance,
        binance_coinbase_diff_usd=diff_usd,
        binance_coinbase_diff_bps=diff_bps,
        binance_latency_ms=binance_latency_ms,
        coinbase_latency_ms=coinbase_latency_ms,
        binance_error=binance_error,
        coinbase_error=coinbase_error,
        polymarket_error=polymarket_error,
        rtds_error=str(rtds["rtds_error"] or ""),
        notes="; ".join(notes),
    )


def ensure_csv_header(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists() or path.stat().st_size == 0:
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
            writer.writeheader()
        return

    with path.open("r", encoding="utf-8", newline="") as handle:
        existing_header = handle.readline().strip().split(",")
    if existing_header != FIELDNAMES:
        raise RuntimeError(
            f"CSV header mismatch for {path}. Use a new --csv-path or rotate the old file."
        )


def append_sample(csv_path: Path, jsonl_path: Path, sample: PriceSample) -> None:
    row = asdict(sample)
    ensure_csv_header(csv_path)
    with csv_path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=FIELDNAMES)
        writer.writerow(row)

    jsonl_path.parent.mkdir(parents=True, exist_ok=True)
    with jsonl_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Collect BTC spot price samples for Polymarket 5-minute crypto market research. "
            "Records Binance, Coinbase, current Polymarket 5-minute BTC market metadata, "
            "and optional Polymarket RTDS Binance/Chainlink prices."
        )
    )
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument(
        "--samples",
        type=int,
        default=0,
        help="Number of samples to collect. Use 0 for continuous collection.",
    )
    parser.add_argument("--timeout-seconds", type=float, default=10.0)
    parser.add_argument("--csv-path", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--jsonl-path", type=Path, default=DEFAULT_JSONL)
    parser.add_argument(
        "--disable-rtds",
        action="store_true",
        help="Disable Polymarket RTDS WebSocket collection for Binance/Chainlink.",
    )
    parser.add_argument(
        "--rtds-warmup-seconds",
        type=float,
        default=2.0,
        help="Seconds to wait for RTDS prices before the first sample.",
    )
    parser.add_argument(
        "--print-each",
        action="store_true",
        help="Print each collected sample as JSON while writing files.",
    )
    return parser.parse_args()


def main() -> int:
    signal.signal(signal.SIGINT, handle_stop_signal)
    signal.signal(signal.SIGTERM, handle_stop_signal)

    args = parse_args()
    if args.interval_seconds <= 0:
        raise SystemExit("--interval-seconds must be positive")
    if args.samples < 0:
        raise SystemExit("--samples must be 0 or a positive integer")

    collected = 0
    started_at = datetime.now(UTC).isoformat()
    price_to_beat_by_slug: dict[str, float] = {}
    rtds_cache = None if args.disable_rtds else RtdsPriceCache()
    if rtds_cache:
        rtds_cache.start()
        time.sleep(max(0.0, args.rtds_warmup_seconds))

    try:
        while not STOP_REQUESTED:
            loop_started = time.perf_counter()
            sample = collect_one_sample(
                timeout=args.timeout_seconds,
                rtds_cache=rtds_cache,
                price_to_beat_by_slug=price_to_beat_by_slug,
            )
            append_sample(args.csv_path, args.jsonl_path, sample)
            collected += 1

            if args.print_each:
                print(json.dumps(asdict(sample), ensure_ascii=False))
                sys.stdout.flush()

            if args.samples and collected >= args.samples:
                break

            elapsed = time.perf_counter() - loop_started
            sleep_seconds = max(0.0, args.interval_seconds - elapsed)
            time.sleep(sleep_seconds)
    finally:
        if rtds_cache:
            rtds_cache.stop()

    summary = {
        "started_at_utc": started_at,
        "finished_at_utc": datetime.now(UTC).isoformat(),
        "samples_collected": collected,
        "csv_path": str(args.csv_path),
        "jsonl_path": str(args.jsonl_path),
        "stopped_by_signal": STOP_REQUESTED,
        "rtds_enabled": not args.disable_rtds,
        "tracked_price_to_beat_windows": len(price_to_beat_by_slug),
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
