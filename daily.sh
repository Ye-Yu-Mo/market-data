#!/bin/bash
# 交易日的日更：先把源同步到最新，再把新的交易日落库。
#
# crontab 里以绝对路径调用，日志由那一行的重定向收集：
#   43 8 * * 1-5 /home/jas/apps/market_data/daily.sh >> /home/jas/apps/market_data/daily.log 2>&1
#
# ⚠️ leaf 的系统时区是 **UTC**：08:43 UTC = **16:43 北京时间**，也就是当天收盘之后，
# 不是开盘前。收盘后跑反而更安全（`END=$(date +%F)` 拿到的是已 final 的日线）；
# 盘中手工跑才有把半根 bar 落进历史库的风险。
#
# 这个脚本存在的原因是「连续」二字：手工跑通一次说明不了任何事，而这个
# 项目里每个问题都是「第一次好好的、第二次才炸」——_stored_days 的日期
# 格式就是第一次增量 0 失败、第二次每只标的都会撞。
set -u
export PATH="$HOME/.local/bin:$PATH"

# 告警 webhook（变量名 LARK_WEBHOOK，与 factor_maker 共用一份）。
# 文件权限 600 —— 值不进仓库，也不进 crontab（crontab 对同机所有用户可读）。
# **必须 `set -a`**: 文件里是 `LARK_WEBHOOK='...'`，不导出的话 notify.py 的子进程
# 看不到它 —— 告警看起来配置好了，实际一条都发不出去（2026-10-28 实测）。
if [ -f "$HOME/.config/alerts/env" ]; then
  set -a
  . "$HOME/.config/alerts/env"
  set +a
fi

LOG=/home/jas/apps/market_data/daily.log
echo "=== $(date "+%F %T %Z") 日更开始 ==="

# 日志按体积轮转：只留上一份。cron 只负责追加，不管它长多大。
if [ -f "$LOG" ] && [ "$(stat -c %s "$LOG")" -gt 20000000 ]; then
  mv -f "$LOG" "$LOG.1"
  echo "(日志超过 20MB，已轮转为 $LOG.1)"
fi

fail() {
  echo "!! $1"
  echo "=== $(date "+%F %T %Z") 日更失败 ==="
  # 告警发不出去不能让退出码变形：钱没动，但这台机器上的数据没更新，必须被看见。
  # 发**卡片**（带时间）：纯文本在飞书里没有发送时间，事后分不清是哪天的失败。
  if [ -n "${LARK_WEBHOOK:-}" ]; then
    /home/jas/apps/market_data/.venv/bin/python \
      /home/jas/apps/market_data/scripts/notify.py \
      --title "A 股日更失败" --service market-data "落库失败: $1" \
      || echo "!! 告警没发出去"
  fi
  exit "$2"
}

# --- 1) 源侧：vendor 同步 -------------------------------------------------
# 它会改写 leaf 上 23G 的 LevelDB。这是本脚本唯一有破坏性的步骤，也是它
# 只能跑在 leaf 上的原因。
cd /home/jas/apps/free_stockdb/stockdb || fail "stockdb 目录不存在" 10
# **必须接住输出再找成功标记**：厂商程序把进度写到 stdout，`sync.log` 停在
# 2026-09-01——原来是 `tail sync.log | grep`，那个文件两个月没更新过，检查
# 永远"通过"。这正是本脚本开头说的"告警看起来配置好了、其实一条都没有"。
sync_out=$(./数据更新 2>&1)
sync_rc=$?
printf '%s\n' "$sync_out"
[ "$sync_rc" -eq 0 ] || fail "数据更新 退出码 $sync_rc" 11
# 退出码之外再看一次成功标记：9/1 那次同步 100% 完成、退出码正常，唯一的异常
# 是最后一步 reload 被跳过（旧二进制坏了），而那正是数据停在 8/31 的直接原因。
grep -q "同步已 100% 完成" <<<"$sync_out" || fail "数据更新 未报告成功，见 daily.log" 12
echo "-- 源同步完成"

# --- 2) 落库 ---------------------------------------------------------------
# 窗口回看 10 天而不是 1 天：写入是幂等的，宽窗口能自动补上漏掉的日子
# （vendor 晚发、某天没跑成、机器重启），而代价几乎相同——遍历的标的一样多。
cd /home/jas/apps/market_data || fail "market_data 目录不存在" 20
START=$(date -d "-10 days" +%F)
END=$(date +%F)
echo "-- 窗口 $START .. $END"

# --source-timeout 120: 同步刚把 LevelDB 的 .ldb 文件重写/合并过，服务进程手里
# 还是旧句柄，之后第一次全市场查询要读冷数据——2026-11-17 实测第一个请求
# 直接在 30 秒超时，重试后 8 秒答完。慢但能答 ≠ 源不可达，别让它告警。
update() {
  .venv/bin/market_data update --market cn --start "$START" --end "$END" --source-timeout 120
}

update
rc=$?
if [ "$rc" -ne 0 ]; then
  # **只重试"源不可达" (3).** 2026-10-22 实测: stockdb 同步刚结束的那一分钟, 本地源
  # 对 key:20261017 的查询超时了, 而同一接口平时 74ms 就答 —— 那是瞬时状态, 不是
  # 源坏掉. 门禁拒绝 (4) 与部分失败 (5) 重跑不会变, 多试一次只是浪费一小时.
  if [ "$rc" -eq 3 ]; then
    echo "!! 源不可达, 120 秒后重试一次"
    sleep 120
    update || fail "落库退出码 $?（3=源不可达 4=门禁拒绝 5=部分失败，已重试一次）" "$?"
  else
    fail "落库退出码 $rc（3=源不可达 4=门禁拒绝 5=部分失败）" "$rc"
  fi
fi
echo "=== $(date "+%F %T %Z") 日更完成 ==="
