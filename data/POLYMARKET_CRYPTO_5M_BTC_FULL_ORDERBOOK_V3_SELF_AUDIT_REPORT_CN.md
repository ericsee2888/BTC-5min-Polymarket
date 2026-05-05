# Polymarket BTC 5分钟 V3 完整订单簿采集脚本自审报告

## 一、结论

自审结果：`PASS`

## 二、文件检查

- snapshot JSONL：`/data/polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.jsonl`
- snapshot JSONL 检查行数：`500`
- snapshot CSV：`/data/polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.csv`
- snapshot CSV 行数：`45178`
- signal events：`/data/polymarket_crypto_5m_btc_signal_events_v3.jsonl`，行数 `44954`
- skip events：`/data/polymarket_crypto_5m_btc_skip_events_v3.jsonl`，行数 `79843`
- trades JSONL：`/data/polymarket_crypto_5m_btc_paper_trades_v3.jsonl`，行数 `170701`
- trades CSV：`/data/polymarket_crypto_5m_btc_paper_trades_v3.csv`，行数 `170701`

## 三、关键字段检查

- snapshot 必填字段缺失：`无`
- snapshot 订单簿数组失败行数：`0`
- 可用于正式回测的 snapshot 行数：`408`
- trade JSONL 行数：`170701`

## 四、失败项

- 无

## 五、警告项

- 无

## 六、自审解释

这个自审只检查“脚本是否按第三轮采集合同把关键字段落盘”。如果 smoke test 时间较短，没有出现交易信号，signal / skip / trade 为空可以接受；但正式 24 小时采集结束后，必须重新运行本自审，并且要求 signal、skip、trade 能够互相对账。
