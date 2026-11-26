"""幂等地给 crontab 加一条: 每 10 分钟拉事件合约的纸上记录."""

import subprocess

LINE = "*/10 * * * * /home/jas/apps/market_data/scripts/sync_ec_paper.sh >> /home/jas/data/market/ec_sync.log 2>&1"
COMMENT = "# 事件合约纸上记录: VPS 每 20 秒写一次, 经 18081 隧道拉到本地给看板读 (2026-11-04)."

current = subprocess.run(["crontab", "-l"], capture_output=True, text=True, check=True).stdout
if LINE in current:
    print("已存在, 不动")
    raise SystemExit(0)
payload = current.rstrip("\n") + "\n" + COMMENT + "\n" + LINE + "\n"
subprocess.run(["crontab", "-"], input=payload, text=True, check=True)
print("已添加:")
print("  ", COMMENT)
print("  ", LINE)
