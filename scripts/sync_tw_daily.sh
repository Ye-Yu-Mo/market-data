#!/bin/bash
# 拉 VPS 上的台股**单日全市场** parquet, 合并进 market_v2.duckdb.
#
# 数据源是交易所自己的接口 (TWSE / TPEX), 一个请求一天全市场 —— 取代原来
# "3134 只标的逐个问 FinMind、跑 6-11 小时" 的日更. 见 scripts/tw_daily_openapi.py.
#
# 两件事必须一起做, 否则等于没更新:
#   1. candles 按 (symbol, date) upsert;
#   2. **adj_factor 顺延到新日期** —— dataset 加载是 INNER JOIN, 没有复权因子的
#      bar 会被整根丢掉, 策略看到的还是旧日期.
#      因子在两次除息之间是常数, 所以新日期沿用"该标的之前最近一天"的值;
#      除息当天的修正仍由 daily_tw_dividends.sh 每晚报废重建.
#
# crontab: `0 9 * * 1-5` UTC (xiaoxin 16:50/17:30 CST 抓完并推 parquet 之后)
set -u
export PATH="$HOME/.local/bin:$PATH"

# 告警 webhook (与其它链路共用一份 600 文件).
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
      --title "台股日线同步失败" --service market-data "$1" \
      || echo "!! 告警没发出去"
  fi
  exit "${2:-1}"
}

DB="$HOME/data/market/market_v2.duckdb"
BASE="http://127.0.0.1:18081"
LOCAL="$HOME/data/market/tw_daily_recent.parquet"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

# 2026-11-12 起由 xiaoxin 抓取并推到这个目录; VPS 关机时保留 HTTP 回退.
# 本地文件超过 3 天没更新就当作没推, 不能拿旧数据静默 upsert.
LOCAL_AGE=$([ -f "$LOCAL" ] && echo $(( $(date +%s) - $(stat -c %Y "$LOCAL") )) || echo 999999)
if [ "$LOCAL_AGE" -lt 259200 ]; then
  cp "$LOCAL" "$TMP/tw_daily.parquet" || fail "复制 $LOCAL 失败"
elif ! curl -sf -m 600 -o "$TMP/tw_daily.parquet" "$BASE/tw_daily_recent.parquet"; then
  fail "拉不到 tw_daily_recent.parquet (xiaoxin 没推, 或 VPS 隧道/文件服务没起来)"
fi

cd "$HOME/apps/market_data" || exit 20
.venv/bin/python - "$DB" "$TMP/tw_daily.parquet" <<'PY' || fail "合并/校验失败, 见上一条输出"
import sys

import duckdb

db, source = sys.argv[1], sys.argv[2]
con = duckdb.connect(db)
try:
    before = con.execute("SELECT max(date), count(*) FROM candles").fetchone()
    con.execute(
        f"""
        INSERT INTO candles (symbol, date, open, high, low, close, volume, amount, turnover)
        SELECT symbol, date::DATE, open, high, low, close, volume, amount, NULL
        FROM read_parquet('{source}')
        ON CONFLICT (symbol, date) DO UPDATE SET
            open = excluded.open, high = excluded.high, low = excluded.low,
            close = excluded.close, volume = excluded.volume, amount = excluded.amount
        """
    )
    # 复权因子: 补齐**所有缺行的 (symbol, date)** —— 不只是"新日期". 只按
    # date > max(adj) 补会漏掉"老日期新标的" (2026-10-28: TAIEX 的 10-27 行
    # 就是这样漏的, 校验直接拦下). 因子在两次除息之间是常数, 沿用最近一天.
    con.execute(
        """
        INSERT INTO adj_factor (symbol, date, factor)
        SELECT c.symbol, c.date, COALESCE(a.factor, 1.0)
        FROM (
            SELECT c.symbol, c.date
            FROM candles c
            ANTI JOIN adj_factor x ON x.symbol = c.symbol AND x.date = c.date
            WHERE c.date >= (SELECT max(date) FROM adj_factor) - INTERVAL 10 DAY
        ) c
        ASOF LEFT JOIN adj_factor a ON c.symbol = a.symbol AND c.date > a.date
        """
    )
    candle_max, rows = con.execute("SELECT max(date), count(*) FROM candles").fetchone()
    adj_max = con.execute("SELECT max(date) FROM adj_factor").fetchone()[0]
    latest = con.execute(
        "SELECT count(*) FROM candles WHERE date = ?", [candle_max]
    ).fetchone()[0]
    covered = con.execute(
        "SELECT count(*) FROM candles c JOIN adj_factor a"
        " ON a.symbol = c.symbol AND a.date = c.date WHERE c.date = ?",
        [candle_max],
    ).fetchone()[0]
finally:
    con.close()

print(f"candles: {before[1]:,} -> {rows:,} 行, 最新 {candle_max} ({latest} 只)")
print(f"adj_factor 最新 {adj_max}; 最新交易日有复权因子的 {covered}/{latest} 只")
if adj_max != candle_max:
    raise SystemExit(f"!! 复权因子止于 {adj_max}, 行情到 {candle_max} —— 加载时会丢根")
if covered < latest:
    raise SystemExit(f"!! 最新一天只有 {covered}/{latest} 只有复权因子, 会少一半标的")
PY
