#!/bin/bash
# 备份: 盘中数据的**当日归档** + 复权因子与留痕的滚动快照.
#
# 为什么是这几样:
#
# - **盘中数据不可再生**. 台股的分钟线券商只放只读 snapshot, 今天不录就永远没有;
#   `market_tw_intraday.duckdb` 一旦损坏, 唯一的来源就是它自己的逐日归档.
# - 复权因子可以重算, 但要重抓 2,770 只分红数据、再清表重建 —— 有份 parquet
#   的话, 出事时是一条 INSERT 的距离. 2026-10-22 就是这么救回来的.
# - 留痕是审计, 它记的是"那天为什么下这些单", 别的任何地方都推不出来.
#
# crontab: `30 6 * * 1-5` UTC = 14:30 台北, 正好在 14:00 的 1 分钟 bar 重建之后.
#
# 归档是按天的 parquet, 不是整库拷贝: 整库每天多一份 140MB 级的副本, 而按天归档
# 大约 10MB, 而且**可以直接当恢复源**:
#
#   INSERT INTO quotes  SELECT * FROM read_parquet('..._quotes.parquet');
#   INSERT INTO bars_1m SELECT * FROM read_parquet('..._bars_1m.parquet');
#
# 用法: `daily_backup.sh [YYYY-MM-DD]` —— 不带参数就是台北时间的今天.
set -u
export PATH="$HOME/.local/bin:$PATH"

if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi

DATA="$HOME/data/market"
BACKUP="$HOME/data/backup"
LOG="$DATA/backup.log"
PY="$HOME/apps/market_data/.venv/bin/python"

DAY="${1:-$(TZ=Asia/Taipei date +%F)}"
ADJ_RETAIN=7      # 复权因子: 一周足够, 再往前重建也就半小时
JOURNAL_RETAIN=30 # 留痕是审计, 留一个月

mkdir -p "$BACKUP/intraday" "$BACKUP/adj_factor" "$BACKUP/journal"
echo "=== $(date '+%F %T %Z') 备份 $DAY ===" >> "$LOG"

fail() {
  echo "!! $1" >> "$LOG"
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    curl -sS -m 5 -X POST -H 'Content-Type: application/json' \
      -d "{\"msg_type\":\"text\",\"content\":{\"text\":\"[market_data] 台股备份失败: $1\"}}" \
      "$LARK_WEBHOOK" >/dev/null || echo "!! 告警没发出去" >> "$LOG"
  fi
  exit 1
}

if ! "$PY" - "$DAY" "$DATA/market_tw_intraday.duckdb" "$BACKUP/intraday" >> "$LOG" 2>&1 <<'PY'
import os
import sys

import duckdb

day, db, out_dir = sys.argv[1], sys.argv[2], sys.argv[3]
if not os.path.isfile(db):
    print("没有采集库 —— 今天没采?")
    raise SystemExit(0)

connection = duckdb.connect(db, read_only=True)
try:
    for table in ("quotes", "bars_1m"):
        exists = connection.execute(
            "SELECT count(*) FROM information_schema.tables WHERE table_name = ?", [table]
        ).fetchone()[0]
        if not exists:
            print(f"{table}: 表还不存在, 跳过")
            continue
        rows = connection.execute(
            f"SELECT count(*) FROM {table} WHERE trade_date = ?", [day]
        ).fetchone()[0]
        if rows == 0:
            print(f"{table}: {day} 没有数据 (节假日?)")
            continue
        target = os.path.join(out_dir, f"{day}_{table}.parquet")
        connection.execute(
            f"COPY (SELECT * FROM {table} WHERE trade_date = ?) TO '{target}'"
            " (FORMAT PARQUET, COMPRESSION ZSTD)",
            [day],
        )
        size = os.path.getsize(target) / 1e6
        print(f"{table}: {rows} 行 -> {target} ({size:.1f} MB)")
finally:
    connection.close()
PY
then
  fail "盘中数据归档失败"
fi

if ! "$PY" - "$DATA/market_v2.duckdb" "$BACKUP/adj_factor/adj_factor_$DAY.parquet" >> "$LOG" 2>&1 <<'PY'
import sys

import duckdb

db, target = sys.argv[1], sys.argv[2]
connection = duckdb.connect(db, read_only=True)
rows = connection.execute("SELECT count(*) FROM adj_factor").fetchone()[0]
connection.execute(
    f"COPY (SELECT * FROM adj_factor) TO '{target}' (FORMAT PARQUET, COMPRESSION ZSTD)"
)
connection.close()
print(f"adj_factor: {rows} 行 -> {target}")
PY
then
  fail "复权因子备份失败"
fi

# 留痕在 `~/data/factor_maker/`, 不在 `~/data/market/` 下面 —— 这里原来拼成了
# `$DATA/factor_maker/journal.db` (即 ~/data/market/factor_maker/...), 于是一直
# 打印"没有留痕库, 跳过": 备份看起来成功, 但审计从来没被拷过 (2026-10-22 发现).
JOURNAL="$HOME/data/factor_maker/journal.db"
if [ -f "$JOURNAL" ]; then
  # 留痕库很小 (百 KB 级), 直接拷; 它只被 factor_maker 在一轮结束时写.
  cp "$JOURNAL" "$BACKUP/journal/journal_$DAY.db" || fail "留痕备份失败"
else
  echo "没有留痕库, 跳过" >> "$LOG"
fi

# 滚动清理. **归档的盘中数据不在这条规则里** —— 它是研究资产, 只增不删.
find "$BACKUP/adj_factor" -name "*.parquet" -type f -mtime +"$ADJ_RETAIN" -delete
find "$BACKUP/journal" -name "*.db" -type f -mtime +"$JOURNAL_RETAIN" -delete

echo "=== $(date '+%F %T %Z') 备份完成 ($(du -sh "$BACKUP" | cut -f1)) ===" >> "$LOG"
