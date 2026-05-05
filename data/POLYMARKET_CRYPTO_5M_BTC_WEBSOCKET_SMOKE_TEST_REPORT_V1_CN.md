# Polymarket 5分钟 BTC WebSocket 冒烟测试报告 v1

生成时间：2026-05-04T23:06:03.552512+00:00

## 结论

状态：`PASS`

本次测试验证的是 BTC 5分钟 taker 策略的 WebSocket 盘口采集能力，不涉及 maker。

## 测试统计

| 项目 | 数值 |
|---|---:|
| 开始时间 | 2026-05-04T17:17:24.738102+00:00 |
| 结束时间 | 2026-05-04T23:06:03.552512+00:00 |
| 运行秒数 | 20918.749 |
| book 事件 | 376495 |
| price_change 事件 | 6209943 |
| last_trade_price 事件 | 185374 |
| snapshot 行数 | 35505 |
| signal 行数 | 15596 |
| skip 行数 | 17515 |
| paper trade 行数 | 44 |
| WebSocket 错误 | ConnectionClosedError: no close frame received or sent |

## 文件

- 原始压缩事件：`/data/polymarket_crypto_5m_btc_ws_orderbook_events_v1.jsonl.gz`
- 盘口快照 JSONL：`/data/polymarket_crypto_5m_btc_ws_book_snapshots_v1.jsonl`
- 盘口快照 CSV：`/data/polymarket_crypto_5m_btc_ws_book_snapshots_v1.csv`
- 信号事件：`/data/polymarket_crypto_5m_btc_ws_signal_events_v1.jsonl`
- 跳过事件：`/data/polymarket_crypto_5m_btc_ws_skip_events_v1.jsonl`
- 模拟交易 JSONL：`/data/polymarket_crypto_5m_btc_ws_paper_trades_v1.jsonl`
- 模拟交易 CSV：`/data/polymarket_crypto_5m_btc_ws_paper_trades_v1.csv`

## 下一步

如果状态为 `PASS`，可以进入后台 24小时正式采集。正式采集前不需要再改策略范围，第一轮只验证两个候选策略。
