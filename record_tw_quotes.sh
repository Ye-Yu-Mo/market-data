#!/bin/bash
# 台股盘中行情采集 —— crontab 每分钟拉起, 由脚本自己的锁保证只有一个实例在跑.
#
# 为什么不是 systemd: leaf 上 factor_maker 与日更都是 crontab, 这里跟它们一致.
# 为什么每分钟拉起: 采集进程要连续跑 4.5 小时, 中途崩了 (券商会话掉了、隧道断)
# 没人会知道. 每分钟一次 `flock` 式的自检, 崩了就下一分钟自己回来.
#
# crontab: `* 0-5 * * 1-5` (UTC) = 08:00-13:59 台北时间.
# 时段判断在脚本里 (它用 Asia/Taipei, 不受 leaf 的 UTC 时区影响).
set -u
export PATH="$HOME/.local/bin:$PATH"

# token 与券商地址在 600 的文件里; 告警 webhook 与日更共用一份.
set -a
. "$HOME/.config/factor_maker/tw.env"
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi
set +a

# 快照源: xiaoxin 的 TWSE MIS 中转. **不能用本地 18000**: 那是 paper broker,
# 只按收盘价返回 stk_code/deal_price, 快照缺交易所时间, 采集器会整天静默写 0 行
# (2026-11-12 → 11-16 就是这样).
if [ -f "$HOME/.config/tw_quote/env" ]; then
  set -a
  . "$HOME/.config/tw_quote/env"
  set +a
fi
: "${TW_QUOTE_URL:=${FACTOR_MAKER_BROKER_URL:-http://127.0.0.1:18000}}"
: "${TW_QUOTE_TOKEN:=${FACTOR_MAKER_BROKER_TOKEN:-}}"

cd "$HOME/apps/market_data" || exit 20
# token 走环境变量, **不要放进 argv**: `ps` 对同机所有用户可见.
export FACTOR_MAKER_BROKER_TOKEN="$TW_QUOTE_TOKEN"
# PYTHONUNBUFFERED: 输出重定向到文件时 python 默认整块缓冲, 盘中看日志会永远是空的.
# **--no-duckdb**: 2026-11-17 起台股盘中只写 psql quote_samples, 单文件 DuckDB 退役.
PYTHONUNBUFFERED=1 exec .venv/bin/python scripts/record_tw_quotes.py \
  --broker-url "$TW_QUOTE_URL" --no-duckdb \
  >> "$HOME/data/market/intraday_tw.log" 2>&1
