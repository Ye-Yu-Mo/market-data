#!/bin/bash
# 台股的**每周**全量刷新 (FinMind): 历史、兴柜、身份表、分红原始数据.
#
# ⚠️ 2026-10-27 起**日线不再走这里**. 这条链路是按标的取数的 (3,134 只 = 3,134 个
# 请求, 540/小时 -> 6-11 小时), 而 05:40 开跑时 FinMind 还没有当天的数据 ——
# 结果每天稳定落后一天, 而且常常"只更新了一半标的" (10-27 实测: 2,831 只里只写了
# 943 只). 日线改由 VPS 抓交易所的**单日全市场** (TWSE CSV + TPEX JSON, 两个请求),
# 见 scripts/tw_daily_openapi.py + scripts/sync_tw_daily.sh.
#
# 这条仍然是唯一的: 兴柜 (ESB, 交易所单日接口不含)、分红/复权因子的原始数据、
# 身份表. 所以改成**每周六**跑一次.
#
# crontab: `40 5 * * 6` (UTC) = 台北时间周六 13:40.
set -u
export PATH="$HOME/.local/bin:$PATH"

# 告警与 token 都在 600 的文件里, 不进 crontab. **必须 `set -a`**: 文件里没有
# `export`, 不导出的话 notify.py 的子进程看不到 LARK_WEBHOOK.
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi
if [ -f "$HOME/.config/stock-tw/env" ]; then
  set -a
  . "$HOME/.config/stock-tw/env"
  set +a
fi

LOG="$HOME/data/market/daily_tw.log"
echo "=== $(date '+%F %T %Z') 台股日更开始 ===" >> "$LOG"

fail() {
  echo "!! $1" >> "$LOG"
  echo "=== $(date '+%F %T %Z') 台股日更失败 ===" >> "$LOG"
  # 卡片而不是纯文本: 只有卡片上有发送时间.
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    "$HOME/apps/market_data/.venv/bin/python" \
      "$HOME/apps/market_data/scripts/notify.py" \
      --title "台股日更失败" --service market-data "日更失败: $1" \
      >> "$LOG" 2>&1 || echo "!! 告警没发出去" >> "$LOG"
  fi
  exit "$2"
}

cd "$HOME/apps/stock_tw" || fail "stock_tw 目录不存在" 20
.venv/bin/stock_tw fetch-history --requests-per-hour 540 >> "$LOG" 2>&1 \
  || fail "fetch-history 退出码 $?" 21

# 身份旁表: **直接从 FinMind 拉** (服务器上没有 stock_tw 的原始抓包目录).
# 全量重建, 变化很少, 但转板/下市会让旧身份变成错的.
cd "$HOME/apps/market_data" || fail "market_data 目录不存在" 30
.venv/bin/python scripts/build_tw_symbols.py --api \
  --out "$HOME/data/market/market_tw_symbols.duckdb" >> "$LOG" 2>&1 \
  || fail "身份旁表重建失败" 31

echo "=== $(date '+%F %T %Z') 台股日更完成 ===" >> "$LOG"
