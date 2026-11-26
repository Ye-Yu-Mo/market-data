#!/bin/bash
# ETF 双周轮动 (四象擒龙) 的对账报告. 每周一跑一次, 由脚本自己判断"是不是报告周"
# (ISO 周奇偶交替 = 每两周一次), 不是报告周就什么都不发.
#
# 只做参考与对照, **不下单**: 目标组合写进 ~/data/rotation/latest.json,
# 回测与近 6 个月成绩走飞书卡片.
set -u
export PATH="$HOME/.local/bin:$PATH"
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi

RESEARCH="$HOME/apps/research"
PY="$HOME/apps/market_data/.venv/bin/python"
FM_PY="$HOME/apps/factor_maker/.venv/bin/python"
NOTIFY="$HOME/apps/market_data/scripts/notify.py"

OUT=$(cd "$RESEARCH" && .venv/bin/python scripts/rotation_report.py \
  --db "$HOME/data/market/market_a.duckdb" --biweekly 2>&1)
STATUS=$?
echo "$OUT"
if [ "$STATUS" -ne 0 ]; then
  # **失败必须喊出来**: 报告是每周一唯一一次机会, 静默失败等于这周没有参考.
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    echo "$OUT" | tail -20 | "$PY" "$NOTIFY" --title "ETF 双周轮动报告失败" \
      --service rotation --severity error || echo "!! 告警没发出去"
  fi
  exit "$STATUS"
fi
echo "$OUT" | grep -q "非报告周" && exit 0

# 报告成功后同步跑纸上账户: 同一组目标等权, 用收盘价/成本模拟成交.
PAPER=$(cd "$RESEARCH" && "$FM_PY" scripts/rotation_paper.py \
  --db "$HOME/data/market/market_a.duckdb" 2>&1)
PAPER_STATUS=$?
echo "$PAPER"
if [ "$PAPER_STATUS" -ne 0 ]; then
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    echo "$PAPER" | tail -20 | "$PY" "$NOTIFY" --title "双周轮动纸盘失败" \
      --service rotation --severity error || echo "!! 告警没发出去"
  fi
  exit "$PAPER_STATUS"
fi

[ -z "${LARK_WEBHOOK:-}" ] && exit 0
echo "$OUT" | "$PY" "$NOTIFY" --title "ETF 双周轮动" --service rotation --severity info \
  || echo "!! 告警没发出去"
