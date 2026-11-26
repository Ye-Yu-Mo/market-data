#!/bin/bash
# xiaoxin: 台股日线 + 三大法人, 通过台湾代理抓交易所, 推到 leaf.
# cron 16:50 / 17:30 CST = 08:50 / 09:30 UTC, 对应 leaf 09:00 / 09:40 的合并.
set -u
cd /home/jas/market_crypto || exit 1
export HTTPS_PROXY=http://127.0.0.1:7890
export HTTP_PROXY=http://127.0.0.1:7890
LOG=/home/jas/market_crypto/tw_daily.log
exec 9>/home/jas/market_crypto/.tw_daily.lock
flock -n 9 || exit 0
{
  echo "== $(date -Is)"
  python3 -u tw_daily_openapi.py --out /home/jas/data/market || exit 1
  python3 -u tw_flows_openapi.py --out /home/jas/data/market --days 10 || exit 1
  rsync -az -e "ssh -o BatchMode=yes -o ConnectTimeout=5"     /home/jas/data/market/tw_daily_recent.parquet     /home/jas/data/market/tw_flows_recent.parquet     jas@192.168.0.109:/home/jas/data/market/ || exit 1
  echo "pushed"
} >> "$LOG" 2>&1
