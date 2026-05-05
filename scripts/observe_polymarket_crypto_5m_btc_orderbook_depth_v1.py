#!/usr/bin/env python3
from __future__ import annotations

import argparse
import csv
import json
import statistics
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT / "data"
DEFAULT_CSV = DATA_DIR / "polymarket_crypto_5m_btc_orderbook_depth_observation_v1.csv"
DEFAULT_JSONL = DATA_DIR / "polymarket_crypto_5m_btc_orderbook_depth_observation_v1.jsonl"
DEFAULT_REPORT = DATA_DIR / "POLYMARKET_CRYPTO_5M_BTC_ORDERBOOK_DEPTH_OBSERVATION_V1_CN.md"

USER_AGENT = "CodexResearch/1.0"
GAMMA_MARKETS_URL = "https://gamma-api.polymarket.com/markets"
CLOB_BOOK_URL = "https://clob.polymarket.com/book"

ENTRY_CAPS = [0.55, 0.60, 0.65]
EXIT_FLOORS = [0.35, 0.50, 0.70, 0.75]
ORDER_CASH_SIZES = [50.0, 100.0, 250.0, 500.0]


@dataclass
class FillQuote:
    complete: bool
    shares: float
    cash: float
    avg_price: float | None
    worst_price: float | None
    levels_used: int


def utc_now() -> datetime:
    return datetime.now(UTC)


def fetch_json(url: str, timeout: float = 8) -> tuple[Any | None, float, str]:
    started = time.perf_counter()
    request = Request(url, headers={"User-Agent": USER_AGENT, "Accept": "application/json"})
    try:
        with urlopen(request, timeout=timeout) as response:
            payload = json.load(response)
        return payload, (time.perf_counter() - started) * 1000, ""
    except Exception as exc:  # noqa: BLE001
        return None, (time.perf_counter() - started) * 1000, f"{type(exc).__name__}: {exc}"


def current_and_next_slugs() -> list[str]:
    now = int(time.time())
    start = now - now % 300
    return [f"btc-updown-5m-{start}", f"btc-updown-5m-{start + 300}"]


def parse_jsonish_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
            return parsed if isinstance(parsed, list) else []
        except json.JSONDecodeError:
            return []
    return []


def fetch_market(slug: str) -> tuple[dict[str, Any] | None, float, str]:
    payload, latency_ms, error = fetch_json(f"{GAMMA_MARKETS_URL}?{urlencode({'slug': slug})}")
    if error:
        return None, latency_ms, error
    if not isinstance(payload, list) or not payload:
        return None, latency_ms, "market_not_found"
    return payload[0], latency_ms, ""


def market_tokens(market: dict[str, Any]) -> list[tuple[str, str]]:
    outcomes = [str(item) for item in parse_jsonish_list(market.get("outcomes"))]
    tokens = [str(item) for item in parse_jsonish_list(market.get("clobTokenIds"))]
    return list(zip(outcomes, tokens, strict=False))


def price_size_levels(levels: list[dict[str, Any]], reverse: bool) -> list[tuple[float, float]]:
    parsed: list[tuple[float, float]] = []
    for level in levels:
        try:
            price = float(level["price"])
            size = float(level["size"])
        except (KeyError, TypeError, ValueError):
            continue
        if price > 0 and size > 0:
            parsed.append((price, size))
    parsed.sort(key=lambda item: item[0], reverse=reverse)
    return parsed


def quote_buy_with_cash(
    asks: list[tuple[float, float]],
    cash_size: float,
    max_price: float | None = None,
) -> FillQuote:
    remaining_cash = cash_size
    shares = 0.0
    spent = 0.0
    worst_price: float | None = None
    levels_used = 0

    for price, available_shares in asks:
        if max_price is not None and price > max_price:
            continue
        if remaining_cash <= 1e-9:
            break
        level_cash = price * available_shares
        use_cash = min(remaining_cash, level_cash)
        if use_cash <= 0:
            continue
        shares += use_cash / price
        spent += use_cash
        remaining_cash -= use_cash
        worst_price = price
        levels_used += 1

    complete = remaining_cash <= max(0.01, cash_size * 1e-6)
    avg_price = spent / shares if shares > 0 else None
    return FillQuote(complete, shares, spent, avg_price, worst_price, levels_used)


def quote_sell_shares(
    bids: list[tuple[float, float]],
    shares_to_sell: float,
    min_price: float | None = None,
) -> FillQuote:
    remaining_shares = shares_to_sell
    sold_shares = 0.0
    proceeds = 0.0
    worst_price: float | None = None
    levels_used = 0

    for price, available_shares in bids:
        if min_price is not None and price < min_price:
            continue
        if remaining_shares <= 1e-9:
            break
        use_shares = min(remaining_shares, available_shares)
        if use_shares <= 0:
            continue
        sold_shares += use_shares
        proceeds += use_shares * price
        remaining_shares -= use_shares
        worst_price = price
        levels_used += 1

    complete = remaining_shares <= max(0.0001, shares_to_sell * 1e-6)
    avg_price = proceeds / sold_shares if sold_shares > 0 else None
    return FillQuote(complete, sold_shares, proceeds, avg_price, worst_price, levels_used)


def depth_cash_lte(asks: list[tuple[float, float]], cap: float) -> float:
    return sum(price * size for price, size in asks if price <= cap)


def depth_shares_lte(asks: list[tuple[float, float]], cap: float) -> float:
    return sum(size for price, size in asks if price <= cap)


def depth_cash_gte(bids: list[tuple[float, float]], floor: float) -> float:
    return sum(price * size for price, size in bids if price >= floor)


def depth_shares_gte(bids: list[tuple[float, float]], floor: float) -> float:
    return sum(size for price, size in bids if price >= floor)


def rounded(value: float | None, digits: int = 6) -> float | None:
    return None if value is None else round(value, digits)


def fetch_book_row(slug: str, market: dict[str, Any], outcome: str, token_id: str) -> dict[str, Any]:
    sampled_at = utc_now()
    payload, latency_ms, error = fetch_json(f"{CLOB_BOOK_URL}?{urlencode({'token_id': token_id})}")
    base: dict[str, Any] = {
        "sampled_at_utc": sampled_at.isoformat(),
        "sampled_at_unix_ms": int(sampled_at.timestamp() * 1000),
        "market_slug": slug,
        "question": market.get("question") or "",
        "event_start_time": market.get("eventStartTime") or "",
        "end_time": market.get("endDate") or "",
        "outcome": outcome,
        "token_id": token_id,
        "book_latency_ms": round(latency_ms, 3),
        "book_error": error,
    }
    if error or not isinstance(payload, dict):
        return base

    bids = price_size_levels(payload.get("bids") or [], reverse=True)
    asks = price_size_levels(payload.get("asks") or [], reverse=False)
    best_bid = bids[0][0] if bids else None
    best_ask = asks[0][0] if asks else None

    base.update(
        {
            "book_timestamp_ms": payload.get("timestamp"),
            "book_hash": payload.get("hash") or "",
            "bid_levels": len(bids),
            "ask_levels": len(asks),
            "best_bid": rounded(best_bid),
            "best_ask": rounded(best_ask),
            "spread": rounded(best_ask - best_bid, 6) if best_bid is not None and best_ask is not None else None,
            "mid": rounded((best_bid + best_ask) / 2, 6) if best_bid is not None and best_ask is not None else None,
            "last_trade_price": payload.get("last_trade_price"),
        }
    )

    for cap in ENTRY_CAPS:
        suffix = str(cap).replace(".", "_")
        base[f"ask_cash_lte_{suffix}"] = rounded(depth_cash_lte(asks, cap), 3)
        base[f"ask_shares_lte_{suffix}"] = rounded(depth_shares_lte(asks, cap), 3)

    for floor in EXIT_FLOORS:
        suffix = str(floor).replace(".", "_")
        base[f"bid_cash_gte_{suffix}"] = rounded(depth_cash_gte(bids, floor), 3)
        base[f"bid_shares_gte_{suffix}"] = rounded(depth_shares_gte(bids, floor), 3)

    for cash_size in ORDER_CASH_SIZES:
        size_suffix = str(int(cash_size))
        buy = quote_buy_with_cash(asks, cash_size, max_price=0.65)
        base[f"buy_{size_suffix}_complete_cap_0_65"] = buy.complete
        base[f"buy_{size_suffix}_avg_price_cap_0_65"] = rounded(buy.avg_price)
        base[f"buy_{size_suffix}_worst_price_cap_0_65"] = rounded(buy.worst_price)
        base[f"buy_{size_suffix}_shares_cap_0_65"] = rounded(buy.shares, 3)
        base[f"buy_{size_suffix}_cash_filled_cap_0_65"] = rounded(buy.cash, 3)
        base[f"buy_{size_suffix}_levels_cap_0_65"] = buy.levels_used

        sell_profit = quote_sell_shares(bids, buy.shares, min_price=0.75) if buy.shares else FillQuote(False, 0, 0, None, None, 0)
        sell_stress = quote_sell_shares(bids, buy.shares, min_price=0.35) if buy.shares else FillQuote(False, 0, 0, None, None, 0)
        base[f"sell_{size_suffix}_shares_complete_floor_0_75"] = sell_profit.complete
        base[f"sell_{size_suffix}_shares_avg_price_floor_0_75"] = rounded(sell_profit.avg_price)
        base[f"sell_{size_suffix}_shares_complete_floor_0_35"] = sell_stress.complete
        base[f"sell_{size_suffix}_shares_avg_price_floor_0_35"] = rounded(sell_stress.avg_price)

    return base


def write_rows(csv_path: Path, jsonl_path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    csv_exists = csv_path.exists()
    fieldnames = list(rows[0].keys())
    with csv_path.open("a", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if not csv_exists:
            writer.writeheader()
        writer.writerows(rows)
    with jsonl_path.open("a") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False) + "\n")


def median(values: list[float]) -> float | None:
    clean = [value for value in values if value is not None]
    return statistics.median(clean) if clean else None


def pct_true(values: list[Any]) -> float | None:
    if not values:
        return None
    return sum(str(value) == "True" or value is True for value in values) / len(values)


def load_rows(csv_path: Path) -> list[dict[str, str]]:
    if not csv_path.exists():
        return []
    with csv_path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def as_float(row: dict[str, str], key: str) -> float | None:
    try:
        value = row.get(key)
        return float(value) if value not in (None, "") else None
    except ValueError:
        return None


def build_report(csv_path: Path, report_path: Path, duration_seconds: int) -> None:
    rows = [row for row in load_rows(csv_path) if not row.get("book_error")]
    if not rows:
        report_path.write_text("# Polymarket BTC 5分钟实时订单簿深度观察 v1\n\n没有可用盘口样本。\n")
        return

    by_size: list[str] = []
    for cash_size in ORDER_CASH_SIZES:
        suffix = str(int(cash_size))
        buy_complete = pct_true([row.get(f"buy_{suffix}_complete_cap_0_65") for row in rows])
        avg_entry = median([as_float(row, f"buy_{suffix}_avg_price_cap_0_65") for row in rows])
        worst_entry = median([as_float(row, f"buy_{suffix}_worst_price_cap_0_65") for row in rows])
        exit_075 = pct_true([row.get(f"sell_{suffix}_shares_complete_floor_0_75") for row in rows])
        exit_035 = pct_true([row.get(f"sell_{suffix}_shares_complete_floor_0_35") for row in rows])
        by_size.append(
            "| {size} | {buy_ok:.1%} | {avg_entry} | {worst_entry} | {exit_075:.1%} | {exit_035:.1%} |".format(
                size=int(cash_size),
                buy_ok=buy_complete or 0,
                avg_entry=f"{avg_entry:.4f}" if avg_entry is not None else "",
                worst_entry=f"{worst_entry:.4f}" if worst_entry is not None else "",
                exit_075=exit_075 or 0,
                exit_035=exit_035 or 0,
            )
        )

    spread_values = [as_float(row, "spread") for row in rows]
    ask_065 = [as_float(row, "ask_cash_lte_0_65") for row in rows]
    bid_075 = [as_float(row, "bid_cash_gte_0_75") for row in rows]
    bid_035 = [as_float(row, "bid_cash_gte_0_35") for row in rows]
    slugs = sorted({row["market_slug"] for row in rows})

    report = f"""# Polymarket BTC 5分钟实时订单簿深度观察 v1

## 一、观察范围

- 观察时长：约 `{duration_seconds}` 秒
- 有效盘口样本：`{len(rows)}` 条
- 覆盖市场窗口：`{len(slugs)}` 个
- 数据文件：`{csv_path}`

## 二、盘口常态

- 中位 spread：`{median(spread_values):.4f}`
- `ask <= 0.65` 的中位可买金额：`{median(ask_065):.2f} USDC`
- `bid >= 0.75` 的中位可卖金额：`{median(bid_075):.2f} USDC`
- `bid >= 0.35` 的中位可卖金额：`{median(bid_035):.2f} USDC`

## 三、不同单笔金额的可成交观察

假设入场最多接受 `0.65`，止盈退出看 `bid >= 0.75`，压力退出看 `bid >= 0.35`。

| 单笔金额 USDC | 入场可完整成交比例 | 中位入场均价 | 中位最差成交价 | 止盈退出可接住比例 | 压力退出可接住比例 |
|---:|---:|---:|---:|---:|---:|
{chr(10).join(by_size)}

## 四、早期解释

这份短样本只用于判断盘口量级，不能直接代表全天。

如果 `50` 和 `100 USDC` 的入场完整成交比例接近 `100%`，说明小单基本不用担心进场。

如果 `250` 或 `500 USDC` 的入场完整成交比例明显下降，说明策略初期不宜直接放大到这个级别。

退出比入场更重要：如果 `bid >= 0.75` 的接盘能力不足，止盈可能要改成分批卖出，或降低止盈目标，不能假设看到 `0.75` 就一定能全额卖出。
"""
    report_path.write_text(report)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-seconds", type=int, default=180)
    parser.add_argument("--interval-seconds", type=float, default=2.0)
    parser.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    parser.add_argument("--jsonl", type=Path, default=DEFAULT_JSONL)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    args = parser.parse_args()

    args.csv.unlink(missing_ok=True)
    args.jsonl.unlink(missing_ok=True)
    started = time.time()
    iterations = 0

    while time.time() - started < args.duration_seconds:
        iteration_started = time.time()
        rows: list[dict[str, Any]] = []
        for slug in current_and_next_slugs():
            market, _latency_ms, market_error = fetch_market(slug)
            if market_error or not market:
                continue
            for outcome, token_id in market_tokens(market):
                rows.append(fetch_book_row(slug, market, outcome, token_id))

        write_rows(args.csv, args.jsonl, rows)
        iterations += 1
        if iterations % 10 == 0:
            print(f"sampled_iterations={iterations} rows_written~={iterations * 4}", flush=True)
        sleep_for = max(0, args.interval_seconds - (time.time() - iteration_started))
        time.sleep(sleep_for)

    build_report(args.csv, args.report, args.duration_seconds)
    print(f"csv={args.csv}", flush=True)
    print(f"jsonl={args.jsonl}", flush=True)
    print(f"report={args.report}", flush=True)


if __name__ == "__main__":
    main()
