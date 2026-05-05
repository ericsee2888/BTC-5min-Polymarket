# Polymarket 5分钟 BTC Taker 策略 WebSocket 下一步执行方案 v1

生成时间：2026-05-05

## 1. 当前结论

WebSocket 下一步不是重新发明策略，而是验证第三轮 REST 订单簿回测里最有希望的策略，在更接近真实交易速度的盘口数据下是否仍然成立。

当前主线只研究 taker 信号交易：

- 看到 BTC 现货价格相对 Polymarket `price_to_beat` 出现偏离；
- 去买对应方向的 UP / DOWN；
- 不研究 maker 挂单、排队、返利、resting order。

## 2. 为什么要做 WebSocket

第三轮 REST 数据已经能做策略筛选，但它有一个天然缺口：订单簿大约每 1 秒采一次。

5分钟 BTC 市场变化很快，真正可成交窗口可能只存在几十到几百毫秒。REST 数据会带来两类误差：

- 可能漏掉很短的好价格，所以低估收益和成交笔数；
- 也可能刚好采到一个很短暂、实盘未必能抢到的好盘口，所以高估收益。

因此 WebSocket 的任务是回答一个问题：

> 我们主推的 90 笔策略，真实毫秒级盘口下到底更强、更弱，还是只是 REST 采样造成的幻觉？

## 3. 官方接口确认

根据 Polymarket 官方文档，CLOB 市场 WebSocket 使用：

- 地址：`wss://ws-subscriptions-clob.polymarket.com/ws/market`
- 认证：不需要
- 订阅对象：UP / DOWN 的 CLOB token id
- 订阅格式：`assets_ids + type=market`
- 建议打开：`custom_feature_enabled=true`
- 心跳：每 10 秒发送 `PING`

需要重点接收的事件：

- `book`：首次订阅时的完整订单簿快照；
- `price_change`：订单簿价格档位变化；
- `best_bid_ask`：最优买价 / 卖价变化；
- `last_trade_price`：真实成交价格；
- `market_resolved`：官方结算事件。

官方文档：

- https://docs.polymarket.com/market-data/websocket/overview
- https://docs.polymarket.com/market-data/websocket/market-channel

本地连通性探针结果：

- 测试时间：2026-05-05；
- 测试市场：当前 5分钟 BTC UP / DOWN；
- 测试长度：约 `20秒`；
- 结果：成功订阅；
- 收到 `book`、`price_change`、`last_trade_price` 三类事件；
- 20秒内收到事件统计：`book 406`、`price_change 4855`、`last_trade_price 202`。

结论：官方 WebSocket 盘口数据可以使用，下一步可以直接写正式采集脚本。

## 4. 第一轮 WebSocket 只验证两个策略

第一轮不再全量扫所有组合，先聚焦我们已经从 REST 回测里选出的两个候选：

### 主推策略

- 策略名：`thr25_start60_cap0.75_cash100_lat250ms`
- 含义：`$25` 偏离阈值，开盘后 `60秒` 开始，入场价不高于 `0.75`，单笔 `100 USDC`，模拟 `250ms` 延迟，持有到结算。
- REST 账户级结果：`90` 笔，约 `+820.53 USDC`，1000 USDC 账户收益约 `+82.05%`。

### 次关注策略

- 策略名：`thr25_start15_cap0.65_cash100_lat250ms`
- 含义：`$25` 偏离阈值，开盘后 `15秒` 开始，入场价不高于 `0.65`，单笔 `100 USDC`，模拟 `250ms` 延迟，持有到结算。
- REST 账户级结果：`41` 笔，约 `+1028.60 USDC`，1000 USDC 账户收益约 `+102.86%`。

## 5. WebSocket 采集要保存什么

这一次不能只保存成交结果，必须保存能复盘的原始过程。

### 原始盘口事件

保存所有 WebSocket 原始事件：

- 收到时间；
- Polymarket 事件时间；
- 事件类型；
- token id；
- 原始 bids / asks 或价格变动；
- 原始 JSON。

用途：以后如果纸上交易逻辑有争议，可以回放原始盘口。

### 重建后的盘口快照

用 `book` + `price_change` 维护本地订单簿，并定期保存快照：

- UP / DOWN 最优买价；
- UP / DOWN 最优卖价；
- 0.65 / 0.70 / 0.75 以下可买深度；
- 0.35 / 0.50 / 0.70 以上可卖深度；
- 盘口更新时间；
- 本地接收时间；
- 盘口是否过旧。

用途：验证能不能买进去，也能不能退出。

### 信号事件

保存价格信号：

- price_to_beat；
- Binance 价格；
- Coinbase 价格；
- Chainlink / RTDS 价格；
- 偏离方向；
- 偏离幅度；
- 触发时间；
- 当时盘口。

用途：判断“价格源已经偏了，但 Polymarket 还没完全涨上去”的窗口是否真实存在。

### 纸上交易事件

保存每一笔模拟交易：

- 入场策略；
- 信号时间；
- 订单到达时间；
- 买入均价；
- 买入最差价；
- 买入份额；
- 是否完整成交；
- 结算结果；
- 盈亏；
- 是否因为价格太高、深度不足、盘口过旧而跳过。

## 6. 第一轮 WebSocket 数据文件

建议新增以下文件，不覆盖第三轮 REST 数据：

- `data/polymarket_crypto_5m_btc_ws_orderbook_events_v1.jsonl`
- `data/polymarket_crypto_5m_btc_ws_book_snapshots_v1.jsonl`
- `data/polymarket_crypto_5m_btc_ws_book_snapshots_v1.csv`
- `data/polymarket_crypto_5m_btc_ws_signal_events_v1.jsonl`
- `data/polymarket_crypto_5m_btc_ws_paper_trades_v1.jsonl`
- `data/polymarket_crypto_5m_btc_ws_paper_trades_v1.csv`
- `data/POLYMARKET_CRYPTO_5M_BTC_WEBSOCKET_VALIDATION_REPORT_V1_CN.md`

## 7. 第一轮运行方式

第一轮建议先跑 `10-15分钟` 冒烟测试：

- 确认能订阅当前市场和下一市场；
- 确认 UP / DOWN 两边都有 `book` 快照；
- 确认 `price_change` 能正确更新本地订单簿；
- 确认 `PING/PONG` 心跳正常；
- 确认市场切换时能自动换 token；
- 确认纸上交易事件能落盘。

冒烟通过后，再跑正式 `24小时`。

## 8. 验收标准

正式 WebSocket 采集至少要满足：

- 覆盖时间不少于 `24小时`；
- 覆盖完整 5分钟窗口不少于 `280` 个；
- WebSocket 断线后可以自动重连；
- 每个窗口都有 UP / DOWN 初始 `book`；
- 原始 WebSocket 事件可回放；
- 纸上交易结果可以和 REST 第三轮结果逐项对比；
- 能单独统计主推 90 笔策略是否更好、更差或无明显变化。

## 9. WebSocket 复盘重点

最终报告不只看总收益，要重点看：

- 主推策略成交笔数有没有增加；
- 买入均价有没有下降；
- 低价窗口是否比 REST 看到得更多；
- REST 里看起来能成交的机会，在 WebSocket 下是否消失；
- 250ms / 500ms / 1000ms 延迟是否拉开差距；
- 100 USDC 单笔是否仍然容易进出；
- 持有到结算是否仍然优于提前止盈止损。

## 10. 下一步执行顺序

1. 写 WebSocket 采集脚本 v1。
2. 跑 10-15 分钟冒烟测试。
3. 生成冒烟测试验收报告。
4. 如果通过，后台跑 24小时正式采集。
5. 采集完成后做 WebSocket 账户级回测。
6. 将 WebSocket 结果和第三轮 REST 结果对比，决定是否进入小额真钱测试。

## 11. 当前不做什么

当前不做：

- maker；
- 挂单；
- maker rebate；
- 多市场泛化；
- 自动真钱下单；
- 大额账户收益推演。

这一步只服务一个目标：

> 用毫秒级盘口验证 BTC 5分钟 taker 策略的真实可执行性。
