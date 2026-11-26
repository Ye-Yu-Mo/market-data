#!/bin/bash
# 台股**复权因子**的生产与维护: 分红原始数据 -> `adj_factor` (market_v2.duckdb).
#
# 为什么存在: 日更只推 `candles`, 复权因子一直是"别人给的一份快照" (止于 2026-08-24).
# 没有它, 带 window 的因子会被 `runner.require_fresh_history` 拒绝 —— 这是对的,
# 但台股研究就只剩单 bar 因子可做.
#
# **三个阶段, 每次跑只走其中一个**:
#
# 1. **回补** (一次性): 清单里还有没抓过的 -> 继续抓, 抓完才重建.
# 2. **增量** (常态): 从 TWSE 的除权息公告里挑出"最近 14 天除过息"的标的,
#    只重抓这几只 —— FinMind 的分红结果数据集按除权息日落行, 除息之后才有数据.
# 3. **月度全量**: 每 30 天把清单换成新的截止日期全量重抓一遍 —— 兜住增量漏掉的
#    (新上市、job 停过几天、清单生成之前的除权息).
#
# **为什么是晚上跑**: FinMind 额度 (600/小时) 与日更共用. 日更 05:40 UTC 开始,
# 要跑 6-11 小时; 这里从 18:00 UTC 起, 两者不重叠.
#
# crontab: `0 18 * * 1-5`
set -u
export PATH="$HOME/.local/bin:$PATH"

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

MARKET_DIR="$HOME/data/market"
RAW_ROOT="$MARKET_DIR/tw_raw"
MANIFEST="$MARKET_DIR/tw_dividend_manifest.json"
SYMBOLS="$MARKET_DIR/tw_dividend_symbols.txt"
REFRESH_SYMBOLS="$MARKET_DIR/tw_dividend_refresh.txt"
REFRESH_MANIFEST="$MARKET_DIR/tw_dividend_refresh.json"
FULL_STAMP="$MARKET_DIR/.tw_dividend_full"
LOG="$MARKET_DIR/tw_dividends.log"
DB="$MARKET_DIR/market_v2.duckdb"
BACKUP="$MARKET_DIR/adj_factor_backup.parquet"
STOCK_TW="$HOME/apps/stock_tw/.venv/bin/stock_tw"
PY="$HOME/apps/market_data/.venv/bin/python"

# 每轮最多跑 5 小时: 18:00 -> 23:00 UTC, 离日更 (05:40) 还有 6 个半小时的余量.
BUDGET_SECONDS=18000
# 除权息之后多少天内的标的要重抓. FinMind 的结果数据在除息日之后才有.
REFRESH_WINDOW_DAYS=14
# 全量重抓的间隔 (天). 兜住增量漏掉的.
FULL_REFRESH_DAYS=30

echo "=== $(date '+%F %T %Z') 分红数据开始 ===" >> "$LOG"

fail() {
  echo "!! $1" >> "$LOG"
  echo "=== $(date '+%F %T %Z') 分红数据失败 ===" >> "$LOG"
  # 卡片而不是纯文本: 只有卡片上有发送时间.
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    "$PY" "$HOME/apps/market_data/scripts/notify.py" \
      --title "台股分红数据失败" --service market-data "分红数据失败: $1" \
      >> "$LOG" 2>&1 || echo "!! 告警没发出去" >> "$LOG"
  fi
  exit "$2"
}

rebuild() {
  # 重建 = 清空 + 重算. `stock_tw import-adj-factors` 是 INSERT OR IGNORE, 不清空的话
  # 旧行永远盖不掉, 新旧两份因子会按日期拼在一起 —— 那比没有更糟.
  if ! "$PY" - "$DB" "$BACKUP" >> "$LOG" 2>&1 <<'PY'
import sys

import duckdb

db, backup = sys.argv[1], sys.argv[2]
connection = duckdb.connect(db)
connection.execute(f"COPY (SELECT * FROM adj_factor) TO '{backup}' (FORMAT PARQUET)")
connection.execute("DELETE FROM adj_factor")
connection.close()
print(f"adj_factor 已清空, 备份 -> {backup}")
PY
  then
    fail "清空 adj_factor 失败" 31
  fi

  # `--db-path` 必须显式给: 这个子命令用的是它自己的参数, **不读 STOCK_TW_DB_PATH**
  # (2026-10-22 在副本上验证时踩过 —— 以为指了副本, 实际写进了生产库).
  "$STOCK_TW" import-adj-factors --raw-root "$RAW_ROOT" --db-path "$DB" >> "$LOG" 2>&1 \
    || fail "复权因子重建失败 (原表备份在 $BACKUP)" 32

  if ! "$PY" - "$DB" >> "$LOG" 2>&1 <<'PY'
import sys

import duckdb

connection = duckdb.connect(sys.argv[1], read_only=True)
rows, factor_max = connection.execute("SELECT count(*), max(date) FROM adj_factor").fetchone()
candle_max = connection.execute("SELECT max(date) FROM candles").fetchone()[0]
symbols = connection.execute("SELECT count(DISTINCT symbol) FROM adj_factor").fetchone()[0]
connection.close()
print(f"adj_factor: {rows} 行 / {symbols} 只 / 最新 {factor_max}; candles 最新 {candle_max}")
# 差一天, 带 window 的因子就会被 require_fresh_history 拒掉 —— 那等于没修.
if factor_max != candle_max:
    raise SystemExit(f"复权因子止于 {factor_max}, 行情到 {candle_max}")
PY
  then
    fail "复权因子覆盖不全" 33
  fi
}

# --- 1. 清单 (缺了才生成) ----------------------------------------------------
#
# ⚠️ **清单一旦生成就不要随手重生成**: request_id 是 (source, dataset, params) 的哈希,
# 而 params 里带着起止日期. 换一个 `--end` 就是另一套 id, 全部原始包会被当成"没抓过"
# 重来一遍 —— 那正是月度全量要做的事, 不能在这里顺手做掉.
if [ ! -f "$MANIFEST" ]; then
  if ! "$PY" - "$MARKET_DIR/market_tw_symbols.duckdb" "$SYMBOLS" >> "$LOG" 2>&1 <<'PY'
import sys

import duckdb

source, target = sys.argv[1], sys.argv[2]
connection = duckdb.connect(source, read_only=True)
symbols = [
    row[0]
    for row in connection.execute(
        "SELECT symbol FROM symbols WHERE market IN ('TWSE','TPEX') ORDER BY symbol"
    ).fetchall()
]
connection.close()
with open(target, "w", encoding="utf-8") as handle:
    handle.write("\n".join(symbols) + "\n")
print(f"{len(symbols)} symbols -> {target}")
PY
  then
    fail "生成标的清单失败" 21
  fi

  cd "$HOME/apps/stock_tw" || fail "stock_tw 目录不存在" 20
  "$STOCK_TW" raw-plan --source finmind --dataset TaiwanStockDividendResult \
    --symbols-file "$SYMBOLS" --start 2000-01-01 --end "$(date +%F)" \
    --output "$MANIFEST" >> "$LOG" 2>&1 || fail "生成请求清单失败" 22
fi

cd "$HOME/apps/stock_tw" || fail "stock_tw 目录不存在" 20

# --- 2. 回补: 清单里还有没抓过的, 先抓完 ------------------------------------
BEFORE=$(find "$RAW_ROOT" -name "*.raw.gz" 2>/dev/null | wc -l)
TOTAL=$("$PY" -c "import json,sys; print(len(json.load(open(sys.argv[1], encoding='utf-8'))))" "$MANIFEST")
if [ "$BEFORE" -lt "$TOTAL" ]; then
  timeout "$BUDGET_SECONDS" "$STOCK_TW" raw-fetch \
    --manifest "$MANIFEST" --root "$RAW_ROOT" --requests-per-hour 540 >> "$LOG" 2>&1
  AFTER=$(find "$RAW_ROOT" -name "*.raw.gz" 2>/dev/null | wc -l)
  echo "$(date '+%F %T') 回补: $BEFORE -> $AFTER / $TOTAL" >> "$LOG"
  if [ "$AFTER" -lt "$TOTAL" ]; then
    echo "=== $(date '+%F %T %Z') 还差 $((TOTAL - AFTER)) 只, 下一轮继续 ===" >> "$LOG"
    exit 0
  fi
  rebuild
  echo "=== $(date '+%F %T %Z') 回补完成并重建 ===" >> "$LOG"
  exit 0
fi

# --- 3. 增量: 最近除过息的标的 ----------------------------------------------
#
# TWSE 的 `TWT48U_ALL` 是**未来的**除权息日程 (约一个月), 不是全年 —— 所以它只能
# 当"触发器": 挑出除息日已经过去的几只, 去 FinMind 重抓它们的完整分红历史.
#
# **从 leaf 直连 TWSE 会被解析到一个连不通的 IP** (2026-10-22 实测: 域名解到
# Facebook 的地址段, 连接超时; 而 FinMind 正常). 所以直连失败就经 VPS 取一次 ——
# 那台在日本, 解析到的是 Hinet 的真实 CDN 地址, 200. TW 链路本来就依赖它 (券商
# 服务端在那儿).
TWSE_URL="https://openapi.twse.com.tw/v1/exchangeReport/TWT48U_ALL"
TWSE_JSON="$MARKET_DIR/.twse_t48u.json"

# leaf 的 DNS 把这个域名解到一个连不通的地址段 (2026-10-22 实测是 Facebook 的 IP,
# 连接被拒), 而 DoH 走 HTTPS, 拿得到真正的 Hinet CDN 地址. 没有 DoH 就退回直连 ——
# 别的机器上 DNS 可能是正常的.
TWSE_CANDIDATES=$("$PY" - <<'PY'
import json
import urllib.request

url = "https://dns.alidns.com/resolve?name=openapi.twse.com.tw&type=A"
ips: list[str] = []
try:
    with urllib.request.urlopen(url, timeout=15) as response:
        payload = json.load(response)
    ips = [a["data"] for a in payload.get("Answer", []) if a.get("type") == 1 and a.get("data")]
except (OSError, ValueError):
    pass
# DoH 给的地址里有一半从大陆是连不通的 (实测 185.45.x / 124.11.x 超时, Hinet 那两个通).
# 所以再加两个已知可用的 Hinet CDN 地址当兜底; 真全不通就告警, 不静默.
print(" ".join(dict.fromkeys([*ips, "203.74.221.3", "203.74.220.1"])))
PY
)

fetch_twse() {
  for ip in $TWSE_CANDIDATES; do
    if curl -sS -m 8 --resolve "openapi.twse.com.tw:443:$ip" "$TWSE_URL" \
      -o "$TWSE_JSON" 2>/dev/null && [ "$(head -c 1 "$TWSE_JSON")" = "[" ]; then
      echo "$(date '+%F %T') TWSE 公告经 $ip 取到" >> "$LOG"
      return 0
    fi
  done
  # 直连兜底: 别的机器上 DNS 可能是正常的.
  curl -sS -m 10 "$TWSE_URL" -o "$TWSE_JSON" 2>/dev/null \
    && [ "$(head -c 1 "$TWSE_JSON")" = "[" ]
}

fetch_twse || fail "取 TWSE 除权息公告失败 (DoH 解析与直连都没成功)" 41

if ! "$PY" - "$REFRESH_SYMBOLS" "$REFRESH_WINDOW_DAYS" "$TWSE_JSON" >> "$LOG" 2>&1 <<'PY'
import json
import sys
from datetime import date, timedelta

target, window_days, source = sys.argv[1], int(sys.argv[2]), sys.argv[3]
with open(source, encoding="utf-8") as handle:
    rows = json.load(handle)


def roc_to_date(raw: str) -> date:
    return date(int(raw[:3]) + 1911, int(raw[3:5]), int(raw[5:7]))


today = date.today()
start = today - timedelta(days=window_days)
codes = sorted(
    {
        row["Code"]
        for row in rows
        if row.get("Code") and start <= roc_to_date(row["Date"]) <= today
    }
)
with open(target, "w", encoding="utf-8") as handle:
    handle.write("\n".join(codes) + "\n")
print(f"最近 {window_days} 天除权息: {len(codes)} 只 -> {target}")
PY
then
  fail "解析 TWSE 除权息公告失败" 42
fi

REFRESH_COUNT=$(grep -c . "$REFRESH_SYMBOLS" || true)
if [ "$REFRESH_COUNT" -gt 0 ]; then
  "$STOCK_TW" raw-plan --source finmind --dataset TaiwanStockDividendResult \
    --symbols-file "$REFRESH_SYMBOLS" --start 2000-01-01 --end "$(date +%F)" \
    --output "$REFRESH_MANIFEST" >> "$LOG" 2>&1 || fail "生成增量清单失败" 42
  timeout "$BUDGET_SECONDS" "$STOCK_TW" raw-fetch \
    --manifest "$REFRESH_MANIFEST" --root "$RAW_ROOT" --requests-per-hour 540 >> "$LOG" 2>&1
fi

# --- 4. 月度全量: 兜住增量漏掉的 --------------------------------------------
#
# 换一份 `--end` 就是一套新的 request_id, 于是所有标的都会重抓一遍. 这是**唯一**
# 应该定期做的事, 也是"新上市的标的一次都抓不到"的兜底.
#
# ⚠️ 月度清单**一个整月都用同一份**: 每晚重新生成的话 `--end` 变了, id 就变了,
# 前一晚抓的全白抓. 也所以"完成"按状态文件数出来, 不是抓到超时就当完了.
MONTHLY="$MANIFEST.full.$(date +%Y%m)"
if [ ! -f "$FULL_STAMP" ] || [ $(( ( $(date +%s) - $(stat -c %Y "$FULL_STAMP") ) / 86400 )) -ge "$FULL_REFRESH_DAYS" ]; then
  if [ ! -f "$MONTHLY" ]; then
    echo "$(date '+%F %T') 生成月度清单" >> "$LOG"
    "$STOCK_TW" raw-plan --source finmind --dataset TaiwanStockDividendResult \
      --symbols-file "$SYMBOLS" --start 2000-01-01 --end "$(date +%F)" \
      --output "$MONTHLY" >> "$LOG" 2>&1 || fail "生成月度清单失败" 51
  fi
  timeout "$BUDGET_SECONDS" "$STOCK_TW" raw-fetch \
    --manifest "$MONTHLY" --root "$RAW_ROOT" --requests-per-hour 540 >> "$LOG" 2>&1
  DONE=$("$PY" - "$MONTHLY" "$RAW_ROOT" <<'PY'
import json
import os
import sys

manifest, root = sys.argv[1], sys.argv[2]
requests = json.load(open(manifest, encoding="utf-8"))
done = 0
for request in requests:
    path = os.path.join(root, "state", f"{request['request_id']}.json")
    try:
        with open(path, encoding="utf-8") as handle:
            if json.load(handle).get("status") == "completed":
                done += 1
    except OSError:
        continue
print(done)
PY
)
  if [ "$DONE" -ge "$TOTAL" ]; then
    touch "$FULL_STAMP"
    echo "$(date '+%F %T') 月度全量完成 ($DONE/$TOTAL)" >> "$LOG"
  else
    echo "$(date '+%F %T') 月度全量未完成 ($DONE/$TOTAL), 明晚继续" >> "$LOG"
  fi
fi

# --- 5. 有新的原始包就重建 ---------------------------------------------------
AFTER=$(find "$RAW_ROOT" -name "*.raw.gz" 2>/dev/null | wc -l)
echo "$(date '+%F %T') 增量: 原始包 $BEFORE -> $AFTER (窗口内 $REFRESH_COUNT 只)" >> "$LOG"
if [ "$AFTER" -gt "$BEFORE" ]; then
  rebuild
fi

echo "=== $(date '+%F %T %Z') 分红数据完成 ===" >> "$LOG"
