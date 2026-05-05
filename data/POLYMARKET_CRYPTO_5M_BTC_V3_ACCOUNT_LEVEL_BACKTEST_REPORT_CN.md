# Polymarket BTC 5分钟 V3 账户级真钱回测报告

## 一、结论先说

这一步把参数矩阵压缩成了账户级复盘：每次只跑一个固定策略组合，资金从入场占用到退出后才能复用，不能把不同参数组合收益相加。

账户级复盘显示，部分小额策略在这 24小时样本里仍然是正收益，但这些结果仍然是 REST 轮询数据和 Chainlink 推算结算，不是最终真钱结论。它适合用来筛选候选策略，下一步还需要 WebSocket 盘口流和官方结算校验。

## 二、复盘口径

- 起始账户本金：`1000 / 5000 / 10000 USDC`
- 同一账户一次只跑一个固定策略组合。
- 交易资金从入场到退出期间被占用。
- 如果账户现金不足，则跳过该笔交易。
- 账户收益按实际执行交易累计。
- 本报告优先看 `50 / 100 USDC` 且 `250ms / 500ms / 1000ms` 延迟的组合，因为它们更接近小额真钱执行。

## 三、1000 USDC 账户，小额延迟组合 Top

| strategy_id | exit_rule | executed_trade_count | win_rate_pct | total_pnl_usdc | account_roi_pct | max_drawdown_pct_realized | max_capital_in_use_pct | max_loss_usdc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thr25_start15_cap0.65_cash100_lat250ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 | -100.0 |
| thr25_start15_cap0.65_cash100_lat500ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 | -100.0 |
| thr25_start15_cap0.65_cash100_lat1000ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat250ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat500ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat1000ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat250ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat500ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat1000ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat250ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat500ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat1000ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 | -100.0 |

## 四、5000 USDC 账户，小额延迟组合 Top

| strategy_id | exit_rule | executed_trade_count | win_rate_pct | total_pnl_usdc | account_roi_pct | max_drawdown_pct_realized | max_capital_in_use_pct | max_loss_usdc |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thr25_start15_cap0.65_cash100_lat250ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 20.572 | 6.7469 | 2.0 | -100.0 |
| thr25_start15_cap0.65_cash100_lat500ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 20.572 | 6.7469 | 2.0 | -100.0 |
| thr25_start15_cap0.65_cash100_lat1000ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 20.572 | 6.7469 | 2.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat250ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 18.8102 | 4.323 | 2.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat500ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 18.8102 | 4.323 | 2.0 | -100.0 |
| thr35_start30_cap0.75_cash100_lat1000ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 18.8102 | 4.323 | 2.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat250ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 18.3411 | 4.323 | 2.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat500ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 18.3411 | 4.323 | 2.0 | -100.0 |
| thr35_start15_cap0.75_cash100_lat1000ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 18.3411 | 4.323 | 2.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat250ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 16.4106 | 6.7812 | 2.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat500ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 16.4106 | 6.7812 | 2.0 | -100.0 |
| thr25_start60_cap0.75_cash100_lat1000ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 16.4106 | 6.7812 | 2.0 | -100.0 |

## 五、1000 USDC 账户，全组合 Top

这张表包含 0ms 和 500 USDC 大单，所以只能作为探索参考，不能直接当小额实盘候选。

| strategy_id | exit_rule | candidate_class | executed_trade_count | win_rate_pct | total_pnl_usdc | account_roi_pct | max_drawdown_pct_realized | max_capital_in_use_pct |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| thr35_start30_cap0.75_cash500_lat0ms | hold_to_resolution | exploratory | 53 | 75.4717 | 4679.206671 | 467.9207 | 177.7903 | 50.0 |
| thr35_start15_cap0.7_cash500_lat0ms | hold_to_resolution | exploratory | 37 | 75.6757 | 4563.963764 | 456.3964 | 100.0 | 50.0 |
| thr35_start15_cap0.65_cash500_lat0ms | hold_to_resolution | exploratory | 18 | 83.3333 | 4526.535995 | 452.6536 | 50.0 | 50.0 |
| thr35_start30_cap0.7_cash500_lat0ms | hold_to_resolution | exploratory | 33 | 75.7576 | 4275.798994 | 427.5799 | 100.0 | 50.0 |
| thr35_start15_cap0.75_cash500_lat0ms | hold_to_resolution | exploratory | 57 | 73.6842 | 4233.027359 | 423.3027 | 162.5231 | 50.0 |
| thr35_start30_cap0.75_cash500_lat250ms | hold_to_resolution | exploratory | 44 | 77.2727 | 4230.09633 | 423.0096 | 111.0991 | 50.0 |
| thr35_start30_cap0.75_cash500_lat500ms | hold_to_resolution | exploratory | 44 | 77.2727 | 4230.09633 | 423.0096 | 111.0991 | 50.0 |
| thr35_start30_cap0.75_cash500_lat1000ms | hold_to_resolution | exploratory | 44 | 77.2727 | 4230.09633 | 423.0096 | 111.0991 | 50.0 |
| thr35_start30_cap0.65_cash500_lat0ms | hold_to_resolution | exploratory | 17 | 82.3529 | 4173.862205 | 417.3862 | 50.0 | 50.0 |
| thr25_start30_cap0.65_cash500_lat250ms | hold_to_resolution | exploratory | 27 | 66.6667 | 3962.554417 | 396.2554 | 150.0 | 50.0 |
| thr25_start30_cap0.65_cash500_lat500ms | hold_to_resolution | exploratory | 27 | 66.6667 | 3962.554417 | 396.2554 | 150.0 | 50.0 |
| thr25_start30_cap0.65_cash500_lat1000ms | hold_to_resolution | exploratory | 27 | 66.6667 | 3962.554417 | 396.2554 | 150.0 | 50.0 |

## 六、较稳小额候选

筛选条件：

- 账户本金 `1000 USDC`
- 单笔 `50 / 100 USDC`
- 延迟 `250 / 500 / 1000ms`
- 至少 10 笔交易
- 没有因为现金不足跳过
- 实现回撤不超过 50%
- 最大资金占用不超过 60%

| strategy_id | exit_rule | executed_trade_count | win_rate_pct | total_pnl_usdc | account_roi_pct | max_drawdown_pct_realized | max_capital_in_use_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| thr25_start15_cap0.65_cash100_lat250ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 |
| thr25_start15_cap0.65_cash100_lat500ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 |
| thr25_start15_cap0.65_cash100_lat1000ms | hold_to_resolution | 41 | 68.2927 | 1028.601571 | 102.8602 | 33.7344 | 10.0 |
| thr35_start30_cap0.75_cash100_lat250ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 |
| thr35_start30_cap0.75_cash100_lat500ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 |
| thr35_start30_cap0.75_cash100_lat1000ms | hold_to_resolution | 55 | 76.3636 | 940.508565 | 94.0509 | 21.6152 | 10.0 |
| thr35_start15_cap0.75_cash100_lat250ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 |
| thr35_start15_cap0.75_cash100_lat500ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 |
| thr35_start15_cap0.75_cash100_lat1000ms | hold_to_resolution | 58 | 75.8621 | 917.056516 | 91.7057 | 21.6152 | 10.0 |
| thr25_start60_cap0.75_cash100_lat250ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 |
| thr25_start60_cap0.75_cash100_lat500ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 |
| thr25_start60_cap0.75_cash100_lat1000ms | hold_to_resolution | 90 | 70.0 | 820.530346 | 82.053 | 33.9058 | 10.0 |

## 七、止盈止损小额候选

下面只看非持有到结算的退出规则。它们收益低于持有到结算 Top 组合，但回撤也更温和，后续可以重点检查是否更适合真钱执行。

| strategy_id | exit_rule | executed_trade_count | win_rate_pct | total_pnl_usdc | account_roi_pct | max_drawdown_pct_realized | max_capital_in_use_pct |
| --- | --- | --- | --- | --- | --- | --- | --- |
| thr50_start30_cap0.7_cash100_lat250ms | profit_800_stop_400 | 19 | 84.2105 | 394.123592 | 39.4124 | 9.7529 | 10.0 |
| thr50_start30_cap0.7_cash100_lat500ms | profit_800_stop_400 | 19 | 84.2105 | 394.123592 | 39.4124 | 9.7529 | 10.0 |
| thr50_start30_cap0.7_cash100_lat1000ms | profit_800_stop_400 | 19 | 84.2105 | 394.123592 | 39.4124 | 9.7529 | 10.0 |
| thr50_start30_cap0.7_cash100_lat250ms | profit_800_stop_350 | 19 | 84.2105 | 370.1339 | 37.0134 | 11.0305 | 10.0 |
| thr50_start30_cap0.7_cash100_lat500ms | profit_800_stop_350 | 19 | 84.2105 | 370.1339 | 37.0134 | 11.0305 | 10.0 |
| thr50_start30_cap0.7_cash100_lat1000ms | profit_800_stop_350 | 19 | 84.2105 | 370.1339 | 37.0134 | 11.0305 | 10.0 |
| thr50_start60_cap0.7_cash100_lat250ms | profit_800_stop_400 | 16 | 87.5 | 366.900113 | 36.69 | 5.1189 | 10.0 |
| thr50_start60_cap0.7_cash100_lat500ms | profit_800_stop_400 | 16 | 87.5 | 366.900113 | 36.69 | 5.1189 | 10.0 |
| thr50_start60_cap0.7_cash100_lat1000ms | profit_800_stop_400 | 16 | 87.5 | 366.900113 | 36.69 | 5.1189 | 10.0 |
| thr50_start15_cap0.7_cash100_lat250ms | profit_800_stop_400 | 18 | 83.3333 | 356.233619 | 35.6234 | 9.7529 | 10.0 |
| thr50_start15_cap0.7_cash100_lat500ms | profit_800_stop_400 | 18 | 83.3333 | 356.233619 | 35.6234 | 9.7529 | 10.0 |
| thr50_start15_cap0.7_cash100_lat1000ms | profit_800_stop_400 | 18 | 83.3333 | 356.233619 | 35.6234 | 9.7529 | 10.0 |

## 八、关键风险

- 这仍然是 REST 轮询数据，不是 WebSocket 毫秒级盘口。
- `250ms / 500ms / 1000ms` 在 1秒采样下可能落到同一下一次快照，所以延迟结果只能当保守近似。
- 当前胜负仍由 Chainlink 价格推算，不是官方结算字段。
- 这只是 24小时样本，不能证明长期稳定。
- 当前回撤是基于已实现盈亏，不是逐秒盯市净值。真实盘中回撤可能更大。
- 如果下一步真钱测试，只能从 `50 / 100 USDC` 小额组合开始。

## 九、输出文件

- 账户级汇总表：`/data/polymarket_crypto_5m_btc_v3_account_level_backtest.csv`
- 账户级逐笔表：`/data/polymarket_crypto_5m_btc_v3_account_level_executed_trades.csv`
