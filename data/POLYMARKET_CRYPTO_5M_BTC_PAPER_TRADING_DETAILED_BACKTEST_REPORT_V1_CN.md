# Polymarket BTC 5分钟实时 Paper Trading 详细回测报告 v1

## 一、运行范围

- 快照开始：`2026-05-02T13:00:56.631278+00:00`
- 快照结束：`2026-05-03T13:00:52.996005+00:00`
- 覆盖时长：`24.00` 小时
- 快照条数：`34449`
- 覆盖 5 分钟窗口：`290` 个
- 已完成模拟交易：`0` 笔
- 交易开始：`NA`
- 交易结束：`NA`
- 当前是否还有未结算模拟仓位：`否`

本报告基于实时 paper trading 文件自动生成，不包含真钱订单。

## 二、核心结果

- 合计模拟投入：`0.00 USDC`
- 平均每笔投入：`NA USDC`
- 合计模拟收益：`0.00 USDC`
- 折算日收益：`0.00 USDC/天`
- 胜率：`NA`
- 平均单笔收益：`NA USDC`
- 中位单笔收益：`NA USDC`
- 最大单笔盈利：`NA USDC`
- 最大单笔亏损：`NA USDC`
- 最大回撤：`0.00 USDC`
- 平均 ROI：`NA`
- 中位 ROI：`NA`

## 三、信号与执行

目标价捕捉状态：

- early_observed: `34359` 条
- late_window_skip: `90` 条

快照中的信号方向分布：

- UP: `414` 条
- DOWN: `229` 条
- 空信号/未触发: `33806` 条

入场信号事件：

- 触发入场信号：`640` 次
- 完整模拟成交：`0` 次

信号状态明细：

| 信号状态 | 快照数 | 占比 |
|---|---:|---:|
| outside_entry_window | 20751 | 60.24% |
| threshold_not_met | 12920 | 37.50% |
| binance_and_coinbase_above_threshold | 414 | 1.20% |
| binance_and_coinbase_below_threshold | 229 | 0.66% |
| missing_price_source | 92 | 0.27% |
| missing_price_to_beat | 43 | 0.12% |

## 四、交易结构

方向分布：

- 暂无

退出方式：

- 暂无

最终结果分布：

- 暂无

## 五、最近交易明细

暂无已完成模拟交易。

## 六、初步解释

如果交易数足够多，优先看三个问题：第一，`100 USDC` 是否经常能完整成交；第二，盈利交易是否主要来自结算还是提前止盈；第三，亏损是否集中发生在特定市场阶段。如果交易数偏少，说明 `$50` 阈值在这段时间不常触发，下一步应同时比较 `$25` 和 `$75` 阈值，而不是急着调整金额。

如果收益仍然很好，下一步不是直接上大钱，而是跑极小真钱验证，金额从 `10-20 USDC` 开始，主要验证真实成交、延迟、手续费和退出体验。

## 七、文件索引

- 快照文件：`/data/polymarket_crypto_5m_btc_paper_trading_snapshots_v1.csv`
- 事件文件：`/data/polymarket_crypto_5m_btc_paper_trading_events_v1.jsonl`
- 交易文件：`/data/polymarket_crypto_5m_btc_paper_trading_trades_v1.csv`
- 报告文件：`/data/POLYMARKET_CRYPTO_5M_BTC_PAPER_TRADING_DETAILED_BACKTEST_REPORT_V1_CN.md`
