#!/bin/bash
# 在 leaf 上拉取 VPS 导出的**增量 parquet**, 合并进研究库.
#
# **2026-11-12 起停用**: VPS 关机, crypto 采集改由 leaf 直接跑
# `scripts/collect_crypto.py`（经 xiaoxin 代理）。这个脚本只作为 VPS 方案的回退保留.
#
# 为什么走 HTTP 而不是 rsync: leaf 到 VPS 的 SSH key 是受限的
# (restrict, port-forwarding, permitopen), 不能执行命令. 所以 VPS 上跑一个
# **只监听 127.0.0.1** 的只读文件服务, leaf 通过已有的 SSH 隧道访问它.
#
# 前置 (都在 VPS 侧):
#   1. /etc/systemd/system/crypto-files.service  (python3 -m http.server 18081)
#   2. authorized_keys 里 permitopen 加上 127.0.0.1:18081
#   3. leaf 的 tunnel_vps.sh 转发 18081
#
# crontab: `0 2 * * * /home/jas/apps/market_data/sync_crypto.sh` (VPS 01:15 采完之后)
set -u
export PATH="$HOME/.local/bin:$PATH"
# **失败要喊出来**: 2026-10-30 起这里连续三天插入失败 (列数不匹配), 只有日志知道,
# 而币圈调仓照常跑 —— 用的是两天前的 bar.
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi
fail() {
  echo "!! $1" >&2
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    "$HOME/apps/market_data/.venv/bin/python" "$HOME/apps/market_data/scripts/notify.py" \
      --title "币圈行情同步失败" --service crypto-sync "$1" || echo "!! 告警没发出去"
  fi
  exit 1
}

DB="$HOME/data/research/market_crypto.duckdb"
BASE="http://127.0.0.1:18081"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

for name in crypto_delta_klines crypto_delta_funding; do
  if ! curl -sf -m 300 -o "$TMP/$name.parquet" "$BASE/$name.parquet"; then
    fail "拉不到 $name.parquet (隧道/文件服务没起来?)"
  fi
done

cd "$HOME/apps/market_data" || exit 20
.venv/bin/python - "$DB" "$TMP" <<'PY' || exit 30
import sys

import duckdb

db, folder = sys.argv[1], sys.argv[2]
con = duckdb.connect(db)
before = con.execute("SELECT count(*) FROM klines").fetchone()[0]
# **显式列名, 不用 SELECT ***: 采集侧加过 taker_buy_* 两列, 而这里的表没有,
# 于是 "10 columns but 12 values" 每天静默失败了两天 (2026-11-02 实测).
con.execute(
    "INSERT INTO klines (symbol, interval, open_time, open, high, low, close, volume,"
    " quote_volume, trades, taker_buy_base, taker_buy_quote)"
    f" SELECT symbol, interval, open_time, open, high, low, close, volume, quote_volume,"
    f" trades, taker_buy_base, taker_buy_quote FROM read_parquet('{folder}/crypto_delta_klines.parquet')"
    " ON CONFLICT DO NOTHING"
)
con.execute(
    f"INSERT INTO funding SELECT * FROM read_parquet('{folder}/crypto_delta_funding.parquet')"
    " ON CONFLICT DO NOTHING"
)
after = con.execute("SELECT count(*) FROM klines").fetchone()[0]
latest = con.execute("SELECT max(open_time) FROM klines WHERE interval='1h'").fetchone()[0]
print(f"合并完成: 新增 {after - before:,} 行, 1h 最新 {latest}")
con.close()
PY
