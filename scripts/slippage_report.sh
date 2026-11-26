#!/bin/bash
# 每周一: 上周订单的**纯滑点** (下单前最新价 -> 成交价) 与相对目标的偏离.
#
# 纯滑点那一列才能和回测的 4bp/边比; 相对目标的那一列里还混着"决策到执行"
# 几个小时的行情漂移, 两者要分开看. 见 board/slippage.py.
#
# 报告走飞书 (与其它任务一致), 同时留一份日志. **失败也要发** —— 周报没人看和
# 没跑一样.
set -u
export PATH="$HOME/.local/bin:$PATH"
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi
NOTIFY="$HOME/apps/market_data/scripts/notify.py"
PY="$HOME/apps/market_data/.venv/bin/python"
LOG="$HOME/data/market_board/slippage.log"
mkdir -p "$(dirname "$LOG")"

cd "$HOME/apps/market_board/backend" || exit 20
set -a
. "$HOME/.config/market_board/env"
set +a

OUT=$(.venv/bin/python -m board.slippage --days 30 2>&1)
STATUS=$?
{
  echo "=== $(date '+%F %T %Z') 退出码 $STATUS ==="
  echo "$OUT"
} >> "$LOG"

if [ -n "${LARK_WEBHOOK:-}" ]; then
  if [ "$STATUS" -eq 0 ]; then
    echo "$OUT" | "$PY" "$NOTIFY" --title "滑点周报" --service market-board \
      --severity info || echo "!! 告警没发出去"
  else
    echo "$OUT" | tail -20 | "$PY" "$NOTIFY" --title "滑点周报失败" \
      --service market-board --severity error || echo "!! 告警没发出去"
  fi
fi
exit "$STATUS"
