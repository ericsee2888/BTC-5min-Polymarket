# Polymarket BTC 5分钟第三轮完整订单簿采集规则设计 v1

## 一、这份文档的目的

这份文档是第三轮数据采集的硬规格。

第三轮采集不能再只做“方向信号数据”，必须一次性满足完整真实成交回测的需要。

跑完后必须能回答：

- `$25 / $35 / $50` 哪个阈值更适合实盘。
- `15秒 / 30秒 / 60秒` 哪个入场开始时间更好。
- 入场价上限 `0.65 / 0.70 / 0.75` 下，分别有多少真实可成交机会。
- 信号出现后，按 `0ms / 250ms / 500ms / 1000ms` 下单延迟，还能不能成交。
- 每笔 `50 / 100 / 250 / 500 USDC` 能不能完整买入。
- 买入时平均成交价、最差成交价、滑点是多少。
- 止盈、止损、结算三种退出方式下，能不能真实卖出。
- 看到价格到 `0.75` 时，盘口买盘是否足够接住我们。
- 以真实账户本金计算，单策略日收益率、最大资金占用和最大回撤是多少。
- 哪些信号被跳过，跳过原因是什么。
- 哪些信号方向对但价格已经太贵。
- 哪些信号价格合适但盘口深度不够。

如果采集字段不能回答这些问题，就不能开跑。

## 二、第三轮采集的核心原则

第三轮必须遵守五条原则：

1. **价格源、盘口、信号、模拟成交必须在同一条时间线上。**
2. **不只记录 best bid / best ask，必须记录完整订单簿深度。**
3. **不只记录成交信号，也要记录没有成交的原因。**
4. **不只保存最终结果，也要保存每次模拟买入和模拟退出的逐档过程。**
5. **采集前就定义好验收标准，跑完不能再说缺字段。**

## 二点五、策略边界：只研究 taker 信号交易

第三轮研究对象是：

**看到价格源信号后，用可立即成交的限价单吃 Polymarket 盘口流动性。**

也就是：

- 入场：买入目标方向 token，吃 asks。
- 退出：卖出目标方向 token，吃 bids。
- 订单类型：marketable limit order / taker order。
- 手续费：按 taker fee 计算。

第三轮不研究：

- maker 做市。
- resting limit order 排队。
- maker rebate。
- 挂单等待别人来成交。

如果后续报告出现收益，必须解释为“taker 信号交易收益”，不能解释为 maker 收益。

## 三、采集对象

市场：

- Polymarket BTC Up or Down 5-minute markets
- 当前窗口
- 下一窗口

必须同时采集当前窗口和下一窗口，原因是：

- 当前窗口用于实盘模拟。
- 下一窗口用于提前拿 token id、预热盘口、避免窗口切换时漏数据。

每个市场必须识别：

- market slug
- question
- event start time
- end time
- seconds since start
- seconds to end
- market closed / active 状态
- Up token id
- Down token id
- condition id
- fee rate

## 四、采样频率

基础频率：

- 每 `1 秒` 采集一次价格源。
- 每 `1 秒` 采集一次当前窗口 Up / Down 完整订单簿。
- 每 `2 秒` 采集一次下一窗口 Up / Down 完整订单簿。
- 如果 WebSocket 盘口流可用，必须额外保存 WebSocket 盘口事件。

最低可接受频率：

- 如果接口压力太大，价格源和当前窗口订单簿可以降到每 `2 秒`。
- 但不能低于每 `2 秒`。
- 只靠 REST 轮询得到的数据，正式报告必须标记为 `rest_polling_backtest`。
- 如果没有 WebSocket 盘口事件，正式报告不能宣称捕捉了毫秒级成交窗口。

建议运行时长：

- 第一轮正式采集：至少 `24 小时`。
- 如果 24 小时有效信号少于 `50` 个，继续跑到 `72 小时`。
- 如果 72 小时仍少于 `100` 个有效信号，说明策略机会频率本身偏低，需要重新评估。

## 五、价格源字段

每条快照必须记录：

- `sampled_at_utc`
- `sampled_at_unix_ms`
- `binance_btcusdt`
- `binance_timestamp_ms`
- `binance_latency_ms`
- `binance_error`
- `coinbase_btcusd`
- `coinbase_timestamp_ms`
- `coinbase_latency_ms`
- `coinbase_error`
- `rtds_binance_btcusdt`
- `rtds_binance_timestamp_ms`
- `chainlink_btcusd`
- `chainlink_timestamp_ms`
- `chainlink_latency_ms_or_age_ms`
- `rtds_error`

必须计算：

- `binance_minus_price_to_beat`
- `coinbase_minus_price_to_beat`
- `chainlink_minus_price_to_beat`
- `binance_coinbase_diff_usd`
- `binance_coinbase_diff_bps`
- `price_source_agreement_direction`
- `price_source_agreement_strength`

## 六、目标价字段

每个 5 分钟窗口必须记录目标价：

- `price_to_beat`
- `price_to_beat_source`
- `price_to_beat_observed_at_utc`
- `price_to_beat_observed_second`
- `price_to_beat_is_early`

目标价规则：

- 优先使用窗口开盘后前 `0-5 秒` 内第一次 Chainlink 价格。
- 如果 `0-5 秒` 没拿到，允许使用 `0-15 秒` 内第一次 Chainlink 价格，但标记为 `early_but_late_5s`。
- 如果超过 `15 秒` 才拿到目标价，该窗口不能用于正式回测，只能用于观察，标记为 `late_price_to_beat_skip_formal_backtest`。

必须保存跳过原因，不能静默丢掉。

## 七、订单簿字段

每次采集订单簿，Up 和 Down 两个 token 必须分别保存完整结构。

每个 token 必须保存：

- `token_id`
- `outcome`
- `book_timestamp_ms`
- `book_hash`
- `book_fetch_latency_ms`
- `book_error`
- `best_bid`
- `best_ask`
- `spread`
- `mid`
- `last_trade_price`
- `bid_levels_count`
- `ask_levels_count`
- `min_order_size`
- `tick_size`

完整订单簿必须保存：

- bids 前 `50` 档
- asks 前 `50` 档

保存格式：

- JSONL 中保存完整数组：
  - `up_bids_json`
  - `up_asks_json`
  - `down_bids_json`
  - `down_asks_json`
- CSV 中保存摘要字段，避免 CSV 过大。

如果 API 返回不足 50 档，也要保存实际档数。

如果某次订单簿请求失败，必须保存：

- 哪个 token 失败
- 失败时间
- 错误信息
- 本次快照是否可用于回测

如果使用 WebSocket，必须额外保存：

- `ws_event_time_utc`
- `ws_received_time_utc`
- `ws_token_id`
- `ws_side`
- `ws_price`
- `ws_size`
- `ws_event_type`
- `ws_book_hash`
- `ws_sequence_or_timestamp`

REST 快照和 WebSocket 事件必须能按时间合并。REST 用于校验完整盘口，WebSocket 用于减少短窗口漏采。

## 八、订单簿深度摘要字段

每个方向必须计算以下深度字段。

入场可买深度：

- `up_ask_cash_lte_0_55`
- `up_ask_cash_lte_0_60`
- `up_ask_cash_lte_0_65`
- `up_ask_cash_lte_0_70`
- `up_ask_cash_lte_0_75`
- `down_ask_cash_lte_0_55`
- `down_ask_cash_lte_0_60`
- `down_ask_cash_lte_0_65`
- `down_ask_cash_lte_0_70`
- `down_ask_cash_lte_0_75`

入场可买 shares：

- `up_ask_shares_lte_0_65`
- `up_ask_shares_lte_0_70`
- `up_ask_shares_lte_0_75`
- `down_ask_shares_lte_0_65`
- `down_ask_shares_lte_0_70`
- `down_ask_shares_lte_0_75`

退出可卖深度：

- `up_bid_cash_gte_0_35`
- `up_bid_cash_gte_0_50`
- `up_bid_cash_gte_0_60`
- `up_bid_cash_gte_0_70`
- `up_bid_cash_gte_0_75`
- `down_bid_cash_gte_0_35`
- `down_bid_cash_gte_0_50`
- `down_bid_cash_gte_0_60`
- `down_bid_cash_gte_0_70`
- `down_bid_cash_gte_0_75`

退出可卖 shares：

- `up_bid_shares_gte_0_35`
- `up_bid_shares_gte_0_50`
- `up_bid_shares_gte_0_70`
- `up_bid_shares_gte_0_75`
- `down_bid_shares_gte_0_35`
- `down_bid_shares_gte_0_50`
- `down_bid_shares_gte_0_70`
- `down_bid_shares_gte_0_75`

近端盘口强度：

- `up_near_bid_cash_0_45_0_55`
- `up_near_ask_cash_0_45_0_55`
- `down_near_bid_cash_0_45_0_55`
- `down_near_ask_cash_0_45_0_55`
- `up_depth_imbalance_near_mid`
- `down_depth_imbalance_near_mid`

## 九、信号组设置

第三轮必须并行记录三组阈值：

- `$25`
- `$35`
- `$50`

每组必须并行记录三组入场开始时间：

- `15 秒`
- `30 秒`
- `60 秒`

每组必须并行记录三组入场价上限：

- `0.65`
- `0.70`
- `0.75`

每组必须并行记录四组下单延迟：

- `0ms`
- `250ms`
- `500ms`
- `1000ms`

也就是至少有：

- 3 个阈值
- 3 个入场开始时间
- 3 个入场价上限
- 4 个下单延迟
- 共 `108` 个策略组合

每条快照都要记录每组策略是否触发：

- `signal_25_direction`
- `signal_35_direction`
- `signal_50_direction`
- `signal_25_reason`
- `signal_35_reason`
- `signal_50_reason`

信号方向规则：

- Binance 和 Coinbase 同时高于目标价至少阈值，方向为 UP。
- Binance 和 Coinbase 同时低于目标价至少阈值，方向为 DOWN。
- 如果不同向，记为 `price_sources_disagree`。
- 如果同向但未达到阈值，记为 `threshold_not_met`。

## 十、跳过原因必须记录

每次出现信号但没有模拟入场，必须保存跳过原因。

跳过原因包括：

- `outside_entry_window`
- `missing_price_to_beat`
- `late_price_to_beat`
- `missing_binance`
- `missing_coinbase`
- `missing_chainlink`
- `missing_orderbook`
- `target_ask_above_cap`
- `insufficient_depth_for_50`
- `insufficient_depth_for_100`
- `insufficient_depth_for_250`
- `insufficient_depth_for_500`
- `book_too_stale`
- `market_too_close_to_end`
- `already_entered_this_market`
- `position_already_open`
- `latency_moved_ask_above_cap`
- `latency_depth_disappeared`
- `latency_direction_changed`
- `entry_partial_not_accepted`

必须保存：

- `skip_strategy_id`
- `skip_slug`
- `skip_time`
- `skip_direction`
- `skip_reason`
- `target_ask`
- `entry_cap`
- `entry_latency_ms`
- `available_cash_depth`
- `required_cash`

不能只保存“没有成交”。

## 十一、模拟入场规则

第三轮必须实时模拟以下单笔金额：

- `50 USDC`
- `100 USDC`
- `250 USDC`
- `500 USDC`

模拟方式：

- 以 taker 方式扫对应方向 asks。
- 按订单簿逐档吃单。
- 每档都扣 taker fee。
- `order_cash_usdc` 指含手续费的总投入上限。
- 如果预算无法完整花出，并且剩余超过 `1 USDC`，标记为部分成交。
- `entry_latency_ms` 代表从信号出现到订单真正到达盘口之间的延迟。
- `0ms` 使用信号当时盘口。
- `250ms / 500ms / 1000ms` 使用延迟后的最新盘口。
- 如果延迟后目标方向 ask 超过入场上限，记为 `latency_moved_ask_above_cap`。
- 如果延迟后盘口深度不够，记为 `latency_depth_disappeared`。
- 如果延迟后价格源方向已经反向或明显失效，记为 `latency_direction_changed`。

每次模拟入场必须记录：

- `strategy_id`
- `order_cash_usdc`
- `entry_latency_ms`
- `signal_time_utc`
- `order_arrival_time_utc`
- `signal_best_ask`
- `arrival_best_ask`
- `signal_to_arrival_ask_change`
- `entry_complete`
- `entry_partial`
- `entry_filled_cash`
- `entry_unfilled_cash`
- `entry_shares`
- `entry_avg_price`
- `entry_worst_price`
- `entry_levels_used`
- `entry_fee_usdc`
- `entry_total_cash_used`
- `entry_slippage_vs_best_ask`
- `entry_book_hash`
- `entry_book_timestamp_ms`
- `entry_data_source`

逐档成交必须保存 JSON：

- `entry_fill_levels_json`

格式示例：

```json
[
  {"price": 0.61, "shares": 40.0, "cash": 24.4, "fee": 1.71},
  {"price": 0.62, "shares": 80.0, "cash": 49.6, "fee": 3.39}
]
```

## 十二、模拟退出规则

第三轮必须同时模拟三类退出。

### 1. 止盈退出

默认止盈观察区：

- `0.70`
- `0.75`
- `0.80`

规则：

- 当目标方向 best bid 达到止盈价，模拟以 taker 方式卖出。
- 逐档吃 bids。
- 扣卖出 fee。
- 如果买盘不足，记录部分退出。
- 如果只能部分退出，不允许静默当作完整退出。
- 部分退出后的剩余仓位必须继续跟踪，直到再次触发退出或持有到结算。

### 2. 压力退出

默认压力退出观察价：

- `0.35`
- `0.40`
- `0.45`

规则：

- 当目标方向 best bid 跌到压力价附近，模拟卖出。
- 记录是否能完整卖出。
- 记录压力退出滑点。
- 如果止损时买盘很薄，必须记录实际能卖出的金额和剩余仓位。

### 3. 持有到结算

规则：

- 如果没有触发止盈或压力退出，持有到窗口结束。
- 最终结果用窗口末端 Chainlink 和 price_to_beat 比较。
- 正式报告必须同时校验 Polymarket 官方结算结果。
- UP：final_chainlink >= price_to_beat
- DOWN：final_chainlink < price_to_beat
- 如果 Chainlink 推算结果与官方结算结果不一致，该窗口必须标记为 `resolution_mismatch_review_required`。

每个模拟仓位必须至少输出三种结果：

- `result_hold_to_resolution`
- `result_profit_070_stop_035`
- `result_profit_075_stop_035`

## 十三、模拟仓位字段

每个策略组合、每个金额、每个市场最多允许一笔模拟入场。

模拟仓位必须记录：

- `paper_trade_id`
- `strategy_id`
- `slug`
- `direction`
- `token_id`
- `entry_time_utc`
- `entry_second`
- `entry_latency_ms`
- `signal_time_utc`
- `order_arrival_time_utc`
- `entry_price_to_beat`
- `entry_binance_delta`
- `entry_coinbase_delta`
- `entry_chainlink_delta`
- `entry_best_ask`
- `entry_best_bid`
- `entry_avg_price`
- `entry_worst_price`
- `entry_shares`
- `entry_total_cash_used`
- `entry_fee_usdc`
- `entry_fill_levels_json`
- `exit_type`
- `exit_time_utc`
- `exit_second`
- `exit_avg_price`
- `exit_worst_price`
- `exit_complete`
- `exit_partial`
- `exit_sold_shares`
- `exit_remaining_shares`
- `exit_cash_after_fee`
- `exit_fee_usdc`
- `exit_fill_levels_json`
- `exit_attempts_json`
- `official_final_outcome`
- `final_outcome_source`
- `final_outcome`
- `correct`
- `pnl_usdc`
- `roi_on_cash`
- `max_favorable_price_seen`
- `max_adverse_price_seen`
- `max_unrealized_pnl`
- `max_unrealized_drawdown`

## 十四、输出文件

第三轮至少输出以下文件。

### 1. 完整快照 JSONL

文件名：

- `polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.jsonl`

用途：

- 保存完整订单簿。
- 作为最原始数据。
- 后续所有回测都应该能从它重建。

必须包含：

- 价格源
- 市场信息
- Up/Down token id
- 完整 bids / asks
- 信号状态
- 深度摘要

### 2. 快照摘要 CSV

文件名：

- `polymarket_crypto_5m_btc_full_orderbook_snapshots_v3.csv`

用途：

- 快速检查和统计。
- 不一定保存完整逐档盘口，但必须保存摘要字段。

### 3. 信号事件 JSONL

文件名：

- `polymarket_crypto_5m_btc_signal_events_v3.jsonl`

用途：

- 每次任意策略组合触发信号都写一条。
- 无论是否成交，都必须记录。

### 4. 跳过事件 JSONL

文件名：

- `polymarket_crypto_5m_btc_skip_events_v3.jsonl`

用途：

- 专门记录“为什么没有入场”。
- 防止跑完只看到 0 笔交易却不知道原因。

### 5. 模拟交易 JSONL

文件名：

- `polymarket_crypto_5m_btc_paper_trades_v3.jsonl`

用途：

- 保存每一笔模拟交易完整生命周期。
- 包含入场逐档成交和退出逐档成交。
- 包含延迟入场结果、部分退出记录和剩余仓位处理。

### 6. 模拟交易 CSV

文件名：

- `polymarket_crypto_5m_btc_paper_trades_v3.csv`

用途：

- 汇总分析。
- 每行一笔模拟交易结果。

### 7. 运行状态日志

文件名：

- `polymarket_crypto_5m_btc_full_orderbook_collector_v3.log`

用途：

- 记录启动、停止、错误、重连、接口失败。

## 十五、数据质量验收标准

第三轮采集结束后，必须先做数据质量验收。

硬性验收标准：

- 覆盖时间不少于 `24` 小时。
- 当前窗口快照间隔中位数 <= `2.5 秒`。
- 当前窗口 Up/Down 订单簿成功率 >= `95%`。
- price_to_beat 早期捕捉成功率 >= `95%`。
- 每个 5 分钟窗口至少有 `100` 条有效当前窗口快照。
- 每个有效窗口必须有开盘 `0-15 秒` 样本。
- 每个有效窗口必须有 `15-180 秒` 样本。
- 每个有效窗口必须有结算前 `270-300 秒` 样本。
- 信号事件、跳过事件、模拟交易事件数量必须能互相对账。
- `0ms / 250ms / 500ms / 1000ms` 四组延迟必须都有入场尝试或明确跳过原因。
- 退出事件必须区分完整退出、部分退出、无法退出、持有到结算。
- 正式收益报告必须标明是否完成官方结算校验。

如果任何硬性标准不满足，报告必须标记：

- `data_quality_failed`
- 失败原因
- 可用子集
- 不可用于正式回测的部分

## 十六、回测验收标准

第三轮数据必须能生成以下回测表。

### 1. 策略组合总表

维度：

- threshold
- entry_start_second
- entry_cap
- entry_latency_ms
- order_cash_usdc
- exit_rule

指标：

- signal_count
- entry_attempt_count
- full_fill_count
- partial_fill_count
- skip_count
- latency_fail_count
- fill_success_rate
- avg_entry_price
- avg_worst_entry_price
- avg_entry_slippage
- avg_exit_price
- avg_exit_slippage
- win_rate
- total_pnl
- avg_pnl
- median_pnl
- max_drawdown
- max_loss
- avg_roi
- median_roi
- capital_used_usdc
- max_concurrent_capital_usdc
- daily_roi_on_allocated_capital
- max_account_drawdown

### 2. 跳过原因表

必须按策略组合统计：

- target_ask_above_cap
- insufficient_depth
- missing_orderbook
- late_price_to_beat
- price_sources_disagree
- already_entered
- latency_moved_ask_above_cap
- latency_depth_disappeared
- latency_direction_changed

### 2.5 机会漏斗表

必须按策略组合输出：

- 原始信号数
- 信号发生在入场窗口内的数量
- 延迟后仍可交易的数量
- ask 不超过上限的数量
- 深度足够完整成交的数量
- 完整入场数量
- 完整止盈退出数量
- 部分退出数量
- 持有到结算数量
- 盈利交易数量
- 亏损交易数量
- 最终净收益

机会漏斗的目的：

- 判断问题是信号少。
- 还是价格已经太贵。
- 还是深度不够。
- 还是退出流动性不够。
- 还是方向本身没有优势。

### 3. 容量表

必须比较：

- `50 USDC`
- `100 USDC`
- `250 USDC`
- `500 USDC`

输出：

- 每档金额的完整成交率
- 平均滑点
- 最差滑点
- 平均退出可成交率
- 止盈退出成功率
- 压力退出成功率

### 3.5 账户收益表

不能把所有参数组合的收益直接相加。

正式报告必须先选定主策略，再按真实账户口径计算：

- 假设账户本金：`1,000 / 5,000 / 10,000 USDC`
- 单窗口最大投入
- 同一窗口最多开仓次数
- 同一时刻最大持仓数量
- 资金占用时间
- 日交易次数
- 日净收益
- 日收益率
- 最大回撤
- 最差连续亏损

参数扫描结果只能用于选策略，不能当作真实账户收益。

### 4. 时间段表

必须按入场秒数统计：

- `0-15`
- `15-30`
- `30-45`
- `45-60`
- `60-90`
- `90-120`
- `120-180`

输出每个时间段：

- 信号数
- 可成交数
- 平均 ask
- 平均深度
- 胜率
- PnL

## 十七、第三轮默认主测组合

默认主测组合如下：

| 参数 | 默认值 |
|---|---|
| 阈值 | `$25 / $35 / $50` 并行 |
| 入场开始 | `15秒 / 30秒 / 60秒` 并行 |
| 入场结束 | `180秒` |
| 入场价上限 | `0.65 / 0.70 / 0.75` 并行 |
| 下单延迟 | `0ms / 250ms / 500ms / 1000ms` 并行 |
| 单笔金额 | `50 / 100 / 250 / 500 USDC` 并行 |
| 止盈 | `0.70 / 0.75 / 0.80` |
| 压力退出 | `0.35 / 0.40 / 0.45` |
| 默认正式报告重点 | `$35 + 15/30秒 + 0.65/0.70 + 100 USDC` |

## 十八、不能再缺的字段清单

以下字段如果缺失，第三轮数据就不能算完成：

- 完整 Up bids
- 完整 Up asks
- 完整 Down bids
- 完整 Down asks
- Up token id
- Down token id
- book timestamp
- book hash
- price_to_beat
- price_to_beat observed second
- Binance price
- Coinbase price
- Chainlink price
- 每组阈值信号方向
- 每组策略跳过原因
- 每组下单延迟结果
- 每档金额模拟入场结果
- 每档金额模拟退出结果
- 部分退出和剩余仓位记录
- 官方结算校验字段
- 机会漏斗统计
- 账户收益统计
- 逐档成交 JSON
- 数据质量统计

## 十九、第三轮开跑前检查清单

开跑前必须确认：

- 脚本能抓到当前窗口 Up/Down token id。
- 脚本能抓到下一窗口 Up/Down token id。
- 脚本能抓到 Up 完整订单簿。
- 脚本能抓到 Down 完整订单簿。
- JSONL 中完整保存 bids / asks。
- CSV 中保存深度摘要。
- 信号事件文件会写入。
- 跳过事件文件会写入。
- 模拟交易文件会写入。
- 运行 10 分钟 smoke test 后，至少覆盖 2 个 5 分钟窗口。
- smoke test 报告必须显示字段完整率。

没有通过 smoke test，不允许启动 24 小时正式采集。

## 二十、结论

第三轮采集的目标不是再证明方向信号有没有用。

第三轮的目标是：

**一次性采够完整成交回测所需数据，避免跑完后再发现缺盘口深度、缺跳过原因、缺退出模拟、缺容量验证。**

只有第三轮数据通过本规格的验收，后续才允许讨论小额真钱测试。
