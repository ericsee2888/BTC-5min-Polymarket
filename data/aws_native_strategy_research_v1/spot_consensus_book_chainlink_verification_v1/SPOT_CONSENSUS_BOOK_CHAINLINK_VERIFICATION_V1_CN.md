# Spot Consensus + Book/Chainlink 候选策略 book 口径复核 v1

## 复核对象
- 策略：`broad_spot_consensus_thr20_15-120_cap0.70_cvd30k_book0.40_cl20`
- 数据覆盖：AWS combined signal sources，HKT 2026-05-25 04:10 到 2026-05-27 14:13。
- 候选交易：105 笔，胜率 74.29%，PnL 1148.58 USDC。

| HKT日期 | 笔数 | 胜率 | PnL |
|---|---:|---:|---:|
| 2026-05-25 | 23 | 65.22% | 8.96 |
| 2026-05-26 | 53 | 75.47% | 664.72 |
| 2026-05-27 | 29 | 79.31% | 474.90 |

## 结论 1：book 的计算口径是可复现的
- `book` 不是 Polymarket 盘口，而是外部交易所盘口强弱：Binance spot、Binance perp、OKX swap、Bybit perp 四个盘口的 top-5 现金深度不平衡度。
- 单个来源公式是 `(bid_cash - ask_cash) / (bid_cash + ask_cash)`；UP 方向要求买盘更强，DOWN 方向会按方向取反。
- 策略使用的是四个来源的中位数，避免单一交易所盘口异常直接决定信号。
- 本次 105 笔样本全部能回连到原始 AWS sample；replay 订单里的 `aligned_book_imbalance` 与原始 sample 重新计算的方向化 book 差异最大为 0.000000000000。

## 结论 2：AWS 这批样本的外部 book 数据完整度足够
- 4个外部盘口来源全部可用：105/105 笔。
- 外部盘口错误字段非空：0/105 笔。
- 方向一致来源数量：4个全一致 65/105 笔；至少3个一致 105/105 笔。
- 至少2个来源自身已经达到 `0.40` 强度：105/105 笔。
- replay 入场记录满足 `aligned_book >= 0.40`：105/105 笔。

## 结论 3：AWS 盘口捕获本身不是这一组策略的主要疑点
- 外部深度 age 最大值分布：n=105, min=404.0, p25=621.0, median=951.0, p75=1262.0, max=1747.0, mean=959.5 ms。
- 外部深度 age 中位数分布：n=105, min=404.0, p25=621.0, median=951.0, p75=1262.0, max=1747.0, mean=959.5 ms。
- 外部深度请求 latency 中位数分布：n=105, min=214.0, p25=222.7, median=225.7, p75=228.7, max=314.5, mean=227.8 ms。
- Polymarket 方向盘口 age 分布：n=105, min=0.0, p25=0.0, median=0.0, p75=0.0, max=3125.0, mean=35.3 ms；其中 <=1500ms 的交易 104/105 笔。
- cap 内 Polymarket 深度分布：n=105, min=101.77, p25=291.86, median=730.96, p75=1802.18, max=8563.16, mean=1300.12 USDC。

## 结论 4：现在最大的风险不是“book 敏感”，而是执行代码还不能完全跑这组新策略
- replay/search 已经用同一个 AWS sample 字段验证了这组策略，`book` 口径本身可以复现。
- 但当前 dry-run/live 主链路仍主要服务 `old_bc/mainline/aws-native-robust` 这类方向逻辑；这组 `spot_consensus + book>=0.40 + Chainlink>=20` 候选还没有被落成独立策略 profile。
- 所以现在不能直接说它可以上 dry-run/live。正确下一步是把它显式实现成一个策略 profile，并要求 replay、dry-run、live 都读取同一组字段和同一套判断函数。

## 本次输出
- 逐笔诊断 CSV：`data/aws_native_strategy_research_v1/spot_consensus_book_chainlink_verification_v1/candidate_trade_book_diagnostics.csv`
