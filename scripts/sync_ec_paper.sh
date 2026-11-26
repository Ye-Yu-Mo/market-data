#!/bin/bash
# 事件合约的纸上记录: 从 VPS 的只读文件服务拉到 leaf, 给看板读.
#
# 链路与 `sync_crypto.sh` 相同: VPS 上 `crypto-files.service` 监听 127.0.0.1:18081,
# leaf 通过已有的 SSH 隧道访问 —— leaf 到 VPS 的 key 是受限的, 不能执行命令.
#
# **前置**: VPS 上 `ec_record.py` 必须写到 `/root/data/market/ec_paper.jsonl`
# (那是文件服务的根目录). 记录器原先写的是 `/root/data/ec_paper.jsonl`,
# 在根目录之外, 拉不到.
set -u
export PATH="$HOME/.local/bin:$PATH"
BASE="http://127.0.0.1:18081"
OUT="$HOME/data/market"
TMP="$(mktemp -d)"
trap 'rm -rf "$TMP"' EXIT

ok=0
for name in ec_paper.jsonl ec_paper_b.jsonl ec_signal.json ec_signal_b.json ec_paper_bq.jsonl ec_signal_bq.json ec_paper_bq10.jsonl ec_signal_bq10.json ec_paper_bq10_lowvol.jsonl ec_signal_bq10_lowvol.json ec_paper_bq10b.jsonl ec_signal_bq10b.json ec_paper_delay10.jsonl ec_paper_delay05.jsonl ec_paper_delay10s.jsonl ec_latency_summary.json lob_status.json ec_backtest.json ec_curve_a_backtest.json ec_curve_b_backtest.json ec_curve_a_replay.json ec_curve_b_replay.json ec_replay_a_summary.json ec_replay_b_summary.json; do
  if curl -sf -m 30 -o "$TMP/$name" "$BASE/$name"; then
    # **先落临时文件再换**: 看板的预热线程随时在读这个文件, 半个 JSON 会让它
    # 把上一次的数据当成错误吞掉.
    mv "$TMP/$name" "$OUT/$name"
    ok=$((ok + 1))
  else
    echo "!! 拉不到 $name (隧道或文件服务没起来?)" >&2
  fi
done

[ "$ok" -eq 0 ] && exit 1
echo "同步 $ok 个文件到 $OUT"
