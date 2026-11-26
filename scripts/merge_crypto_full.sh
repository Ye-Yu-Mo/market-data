#!/bin/bash
# **一次性**把全量 1d + 1h 合并进 leaf 的研究库 (标的池从 100 扩到 523 只时跑).
#
# 日常的 `sync_crypto.sh` 只拉 7 天增量 —— 对新标的来说, 那只有 7 根日线, 做不了
# 带窗口的研究. 这个脚本拉 VPS 上 `export_full.py` 导出的全量 parquet, 一次补齐.
#
# 前置 (VPS 侧): `.venv/bin/python /root/market_crypto/export_full.py` 已经跑过,
# `/root/data/market/crypto_full_klines.parquet` 与 `crypto_full_funding.parquet`
# 在文件服务的目录下 (同一个 18081 端口).
#
# 用法: `bash scripts/merge_crypto_full.sh` (在 leaf 上, 与 sync_crypto.sh 同一套隧道)
set -u
export PATH="$HOME/.local/bin:$PATH"

DB="$HOME/data/research/market_crypto.duckdb"
BASE="http://127.0.0.1:18081"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

for name in crypto_full_klines crypto_full_funding; do
  # 全量比增量大得多: 300 秒不够就加到 1800.
  if ! curl -sf -m 1800 -o "$TMP/$name.parquet" "$BASE/$name.parquet"; then
    echo "!! 拉不到 $name.parquet (隧道/文件服务没起来, 或者 VPS 还没导出?)" >&2
    exit 1
  fi
  echo "$name.parquet $(du -h "$TMP/$name.parquet" | cut -f1)"
done

cd "$HOME/apps/market_data" || exit 20
.venv/bin/python - "$DB" "$TMP" <<'PY'
import sys

import duckdb

db, folder = sys.argv[1], sys.argv[2]
con = duckdb.connect(db)
before = con.execute("SELECT count(*) FROM klines").fetchone()[0]
# **DO UPDATE 而不是 DO NOTHING**: 全量里也可能有当天的半根 bar, 覆盖成最终值.
con.execute(
    f"INSERT INTO klines SELECT * FROM read_parquet('{folder}/crypto_full_klines.parquet')"
    " ON CONFLICT (symbol, interval, open_time) DO UPDATE SET"
    " open = excluded.open, high = excluded.high, low = excluded.low, close = excluded.close,"
    " volume = excluded.volume, quote_volume = excluded.quote_volume, trades = excluded.trades"
)
con.execute(
    f"INSERT INTO funding SELECT * FROM read_parquet('{folder}/crypto_full_funding.parquet')"
    " ON CONFLICT (symbol, funding_time) DO UPDATE SET rate = excluded.rate"
)
after = con.execute("SELECT count(*) FROM klines").fetchone()[0]
symbols = con.execute("SELECT count(DISTINCT symbol) FROM klines").fetchone()[0]
latest = con.execute("SELECT max(open_time) FROM klines WHERE interval='1d'").fetchone()[0]
print(f"合并完成: 新增 {after - before:,} 行, 共 {symbols} 只, 1d 最新 {latest}")
con.close()
PY
