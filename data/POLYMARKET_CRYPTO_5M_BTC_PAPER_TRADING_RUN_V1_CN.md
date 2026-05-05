# Polymarket BTC 5分钟实时 Paper Trading v1

## 本次运行

- 开始时间：`2026-05-02T13:00:53.630019+00:00`
- 结束时间：`2026-05-03T13:01:00.591998+00:00`
- 信号阈值：`$50`
- 模拟单笔总投入：`100 USDC`
- 入场窗口：开盘后 `60-180` 秒
- 入场价格上限：`0.65`
- 止盈观察价：`0.75`
- 压力退出价：`0.35`

## 结果

- 已完成模拟交易：`0` 笔
- 盈利交易：`0` 笔
- 胜率：`0.00%`
- 合计模拟收益：`0.00 USDC`

## 输出文件

- 快照：`/data/polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv`
- 事件：`/data/polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl`
- 交易：`/data/polymarket_crypto_5m_btc_paper_trading_trades_v1.csv`

## 说明

这是真实数据驱动的模拟交易，不会发真钱订单。它用实时 Binance / Coinbase / Chainlink 和 Polymarket CLOB 订单簿来判断信号，并按盘口深度模拟买入和退出。
