# BTC 5min Polymarket

Research and execution toolkit for the Polymarket BTC 5-minute taker strategy.

## What Is Included

- `scripts/collect_polymarket_crypto_price_samples_v1.py`: BTC price sampling helpers.
- `scripts/collect_polymarket_crypto_5m_btc_full_orderbook_v3.py`: REST full orderbook collector and paper strategy evaluator.
- `scripts/collect_polymarket_crypto_5m_btc_ws_orderbook_v1.py`: WebSocket orderbook collector and paper signal runner.
- `scripts/trade_polymarket_crypto_5m_btc_live_v1.py`: live trading executor with explicit `--dry-run`, `--preflight`, and `--live --confirm-live-trading` modes.
- `scripts/backtest_*.py` and `scripts/analyze_*.py`: backtest, account-level replay, quality checks, and report builders.
- `data/*.md`: strategy notes, data collection specs, backtest reports, and live execution guide.

## What Is Not Included

This repository intentionally excludes raw market data, logs, process files, live order outputs, state files, and secrets.

Do not commit `.env`, private keys, API credentials, raw `jsonl/csv/gz` captures, or live trading outputs.

## Install

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Live Executor Safety

The live executor has no default mode. You must explicitly choose one mode:

```bash
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py --dry-run --env-file .env
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py --preflight --env-file .env
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py --live --confirm-live-trading --env-file .env
```

Recommended order before live trading:

1. Run WebSocket collection and paper replay.
2. Run `--dry-run` until the strategy path is healthy.
3. Run `--preflight` with the real Polymarket account.
4. Only then run `--live --confirm-live-trading`.

## Current Main Strategy

`thr25_start60_cap0.75_cash100_lat250ms`

- BTC spot deviation threshold: `25 USD`
- Entry window: `60-180s` after market open
- Entry cap: `0.75`
- Order size: `100 USDC`
- Execution: FOK buy only
- Position handling: hold to settlement
- Risk defaults: `500 USDC` max unsettled capital, `600 USDC` daily loss stop

