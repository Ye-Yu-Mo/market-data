#!/bin/bash
# 拉 VPS 上的台股三大法人买卖超 parquet, 合并进 market_v2.duckdb 的 `inst_flow`.
#
# 表: (date, symbol, foreign_net, trust, dealer) —— 单位**股**, 当日买卖超.
# 数据来自交易所的单日全市场接口 (TWSE T86 + TPEX 3insti), 见
# scripts/tw_flows_openapi.py. 只保留 candles 里当天有 bar 的标的 (过滤权证).
#
# 用法:
#   sync_tw_flows.sh                          # 每日滚动 (最近 60 天)
#   sync_tw_flows.sh tw_flows_history.parquet # 全量回补 (VPS 上的完整历史)
#
# crontab: 与日线同步同一条链, 09:05 (日线 09:00 之后)
set -u
export PATH="$HOME/.local/bin:$PATH"
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi

fail() {
  echo "!! $1"
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    "$HOME/apps/market_data/.venv/bin/python" \
      "$HOME/apps/market_data/scripts/notify.py" \
      --title "台股法人数据同步失败" --service market-data "$1" \
      || echo "!! 告警没发出去"
  fi
  exit "${2:-1}"
}

DB="$HOME/data/market/market_v2.duckdb"
BASE="http://127.0.0.1:18081"
SOURCE="${1:-tw_flows_recent.parquet}"
LOCAL="$HOME/data/market/$SOURCE"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 2026-11-12 起由 xiaoxin 抓取并推到这个目录; VPS 关机时保留 HTTP 回退.
# 本地文件超过 3 天没更新就当作没推, 不能拿旧数据静默 upsert.
LOCAL_AGE=$([ -f "$LOCAL" ] && echo $(( $(date +%s) - $(stat -c %Y "$LOCAL") )) || echo 999999)
if [ "$LOCAL_AGE" -lt 259200 ]; then
  cp "$LOCAL" "$TMP/tw_flows.parquet" || fail "复制 $LOCAL 失败"
elif ! curl -sf -m 600 -o "$TMP/tw_flows.parquet" "$BASE/$SOURCE"; then
  fail "拉不到 $SOURCE (xiaoxin 没推, 或 VPS 隧道/文件服务没起来)"
fi

cd "$HOME/apps/market_data" || exit 20
.venv/bin/python - "$DB" "$TMP/tw_flows.parquet" <<'PY' || fail "合并失败, 见上一条输出"
import sys

import duckdb

db, source = sys.argv[1], sys.argv[2]
con = duckdb.connect(db)
try:
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS inst_flow (
            date DATE NOT NULL,
            symbol VARCHAR NOT NULL,
            foreign_net BIGINT NOT NULL,
            trust BIGINT NOT NULL,
            dealer BIGINT NOT NULL,
            PRIMARY KEY (date, symbol)
        )
        """
    )
    # 只保留当天真的在交易的标的: 交易所接口含权证 (每天一万多只),
    # 它们不在 candles 里, 存进来只会把表撑大.
    con.execute(
        f"""
        INSERT INTO inst_flow (date, symbol, foreign_net, trust, dealer)
        SELECT f.date::DATE, f.symbol, f.foreign_net, f.trust, f.dealer
        FROM read_parquet('{source}') f
        JOIN candles c ON c.symbol = f.symbol AND c.date = f.date::DATE
        ON CONFLICT (date, symbol) DO UPDATE SET
            foreign_net = excluded.foreign_net, trust = excluded.trust, dealer = excluded.dealer
        """
    )
    latest, rows, symbols = con.execute(
        "SELECT max(date), count(*), count(DISTINCT symbol) FROM inst_flow"
    ).fetchone()
finally:
    con.close()

print(f"inst_flow: {rows:,} 行 / {symbols} 只 / 最新 {latest}")
PY
