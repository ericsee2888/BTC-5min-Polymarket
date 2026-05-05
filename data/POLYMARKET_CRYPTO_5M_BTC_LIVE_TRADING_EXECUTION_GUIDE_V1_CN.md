# Polymarket BTC 5分钟真钱自动交易执行指南 v1

生成时间：2026-05-05

## 1. 当前定位

这份执行器服务于 BTC 5分钟 taker 策略真钱试运行，不做 maker。

第一版只跑：

- 策略：`thr25_start60_cap0.75_cash50_lat250ms`
- 阈值：BTC 现货相对 `price_to_beat` 偏离 `$25`
- 入场窗口：开盘后 `60-180秒`
- 入场价格上限：`0.75`
- 单笔金额：`50 USDC`
- 延迟模拟：信号触发后等待 `250ms`
- 订单类型：`FOK`，必须完整成交，否则取消
- 持仓方式：持有到结算

## 2. 安全模式

脚本没有默认运行模式，必须显式选择：

```bash
--dry-run
```

或者：

```bash
--preflight
```

或者：

```bash
--live --confirm-live-trading
```

如果不写模式，脚本会拒绝启动。

如果只写 `--live`，但不写 `--confirm-live-trading`，脚本也会拒绝启动。

## 3. 凭证配置

脚本从环境变量或 `--env-file` 读取配置：

```bash
POLYMARKET_PRIVATE_KEY=
POLYMARKET_API_KEY=
POLYMARKET_API_SECRET=
POLYMARKET_API_PASSPHRASE=
POLYMARKET_FUNDER_ADDRESS=
POLYMARKET_SIGNATURE_TYPE=
```

注意：

- 不要把真实私钥写进代码；
- 不要把真实 `.env` 提交到 Git；
- `POLYMARKET_FUNDER_ADDRESS` 和 `POLYMARKET_SIGNATURE_TYPE` 必须和现有 Polymarket 账户结构一致；
- 当前本机已经安装官方 `py_clob_client`，但 live 前仍必须用真实账户做一次账户预检。

## 4. Dry-run 示例

Dry-run 不会发真钱订单：

```bash
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py \
  --dry-run \
  --env-file /path/to/polymarket_live.env \
  --duration-seconds 900 \
  --reset-output
```

Dry-run 会验证：

- 配置是否完整；
- SDK 是否可导入；
- WebSocket 盘口是否可用；
- 价格信号是否可用；
- 风控是否会跳过；
- 如果触发下单条件，会记录 dry-run order result，但不会发单。

Dry-run 不验证真实账户权限；真实账户权限必须用 `--preflight` 单独验证。

## 5. Preflight 示例

Preflight 会初始化真钱 SDK、检查余额/授权/开放订单/BTC 5分钟未知持仓，但不会进入行情循环，也不会下单：

```bash
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py \
  --preflight \
  --env-file /path/to/polymarket_live.env \
  --reset-output
```

Preflight 不通过时，live 不应启动。

## 6. Live 示例

真钱模式必须双确认：

```bash
python scripts/trade_polymarket_crypto_5m_btc_live_v1.py \
  --live \
  --confirm-live-trading \
  --env-file /path/to/polymarket_live.env \
  --duration-seconds 86400 \
  --reset-output
```

启动时会打印：

- `LIVE TRADING ENABLED`
- 策略 ID
- 单笔金额
- 最大资金占用
- 脱敏 funder address

## 7. 风控参数

默认风控：

- 单笔金额：`50 USDC`
- 最大未结算占用：`500 USDC`
- 最大日亏损：`300 USDC`
- 连续真实下单失败：`3次` 停机
- 最大盘口年龄：`1500ms`
- 价格源最大年龄：`3000ms`
- `price_to_beat` 必须在开盘前 `5秒` 内捕获，否则该市场不交易

可以通过命令行覆盖：

```bash
--order-cash-usdc 50
--max-locked-usdc 500
--max-daily-loss-usdc 300
--max-consecutive-failures 3
--max-price-to-beat-observed-second 5
```

## 8. 输出文件

真钱执行器单独输出，不和 paper 数据混在一起：

- `data/polymarket_crypto_5m_btc_live_orderbook_events_v1.jsonl.gz`
- `data/polymarket_crypto_5m_btc_live_audit_events_v1.jsonl`
- `data/polymarket_crypto_5m_btc_live_orders_v1.csv`
- `data/polymarket_crypto_5m_btc_live_settlements_v1.csv`
- `data/polymarket_crypto_5m_btc_live_state_v1.json`
- `data/POLYMARKET_CRYPTO_5M_BTC_LIVE_TRADING_RUN_V1_CN.md`

## 9. 当前实现状态

已完成：

- 显式 `--dry-run / --preflight / --live` 模式；
- live 必须二次确认；
- live 账户预检不通过会停止；
- 持仓状态会落盘，重启后恢复未释放仓位；
- `price_to_beat` 捕获过晚会拒绝交易；
- 成交后会做订单/成交确认探针；
- 结算后会通过用户持仓探针确认资金释放，再释放本地占用；
- 环境变量 / env-file 配置读取；
- WebSocket 盘口订阅；
- BTC 价格源读取；
- 策略信号判断；
- 250ms 延迟后重新检查盘口；
- FOK 下单函数封装；
- dry-run 不发单；
- live 缺 SDK、坏私钥或 SDK 初始化失败时拒绝启动；
- 审计日志、订单日志、结算探针日志；
- 运行报告。

待真实运行前必须完成：

- 用真实现有 Polymarket 账户确认 `signature_type` 和 `funder`；
- 用 dry-run 跑到至少一次触发下单条件；
- 用 `--preflight` 确认 allowance / balance / open orders / BTC 5分钟未知持仓都通过；
- 再进入 live。
