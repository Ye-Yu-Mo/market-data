#!/bin/bash
# 双周轮动纸上账户: 每日收盘后 mark-to-market, 报告周自动调仓.
# 不改目标, 只把最新收盘价写进 ~/data/rotation/paper.json / paper_history.jsonl.
set -u
RESEARCH="$HOME/apps/research"
FM_PY="$HOME/apps/factor_maker/.venv/bin/python"
DB="$HOME/data/market/market_a.duckdb"
cd "$RESEARCH" || exit 20
"$FM_PY" scripts/rotation_paper.py --db "$DB" >> "$HOME/data/rotation/paper.log" 2>&1
