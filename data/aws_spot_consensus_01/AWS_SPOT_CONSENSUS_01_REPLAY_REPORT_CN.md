# aws-spot-consensus-01 独立策略回放报告

## 策略口径
- 策略ID：`aws-spot-consensus-01`
- 方向：spot consensus，相对 price_to_beat 同向超过 $20。
- 入场窗口：15-120秒。
- 入场价格上限：0.70。
- 过滤：CVD>=30000、book>=0.40、Chainlink>=20。
- price_to_beat：必须在30秒内捕获。
- 说明：这是独立策略脚本，不接统一决策引擎，不影响 mainline/live 主链路。

## 数据
- 样本：`data/aws_native_strategy_research_v1/aws_native_combined_signal_sources_20260524_20260527.csv`
- official outcome：`data/aws_native_strategy_research_v1/aws_native_combined_strategy_grid_official_outcomes.csv`

## 总结果
- 交易数：105
- 胜率：74.29%
- PnL：1148.58 USDC
- 平均入场价：0.6571

## 逐日结果
| HKT日期 | 交易数 | 胜率 | PnL | 平均入场价 |
|---|---:|---:|---:|---:|
| 2026-05-25 | 23 | 65.22% | 8.96 | 0.6513 |
| 2026-05-26 | 53 | 75.47% | 664.72 | 0.6545 |
| 2026-05-27 | 29 | 79.31% | 474.90 | 0.6666 |

## 输出
- 逐笔订单：`data/aws_spot_consensus_01/aws_spot_consensus_01_orders.csv`
