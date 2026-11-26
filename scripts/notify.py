"""从 shell 脚本发一条**卡片**告警.

日更 (``daily.sh`` / ``daily_tw.sh`` / ``daily_tw_dividends.sh``) 是 bash, 但它们
的告警不该是第四种格式. 这个小工具只做一件事: 把 ``LARK_WEBHOOK`` 和一段话交给
``market_data.offline.alerts.notify`` —— 于是 bash、盘中采集、新鲜度检查发出来的
卡片长得一模一样, 都带 服务/节点/时间.

用法 (webhook 从环境变量 ``LARK_WEBHOOK`` 读, 脚本里已经 source 过):

    .venv/bin/python scripts/notify.py --title "A 股日更失败" "落库退出码 3"
    echo "多行细节" | .venv/bin/python scripts/notify.py --title "台股日更失败"

退出码: ``0`` = 发出去了, 或者**本来就没配** webhook (那是运维选择, 不是故障);
``1`` = 配了但没发出去 —— 只有这一种值得调用方再喊一声.
"""

from __future__ import annotations

import argparse
import os
import sys

from market_data.offline.alerts import notify


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="发一条卡片告警")
    parser.add_argument("--title", required=True, help="卡片标题")
    parser.add_argument("--service", default="market_data", help="卡片上的服务名")
    parser.add_argument(
        "--severity",
        default="error",
        choices=("info", "success", "warning", "error", "critical"),
    )
    parser.add_argument("message", nargs="*", help="正文; 留空则从 stdin 读")
    args = parser.parse_args(argv)

    content = " ".join(args.message).strip() or sys.stdin.read().strip()
    if not content:
        print("没有正文, 不发", file=sys.stderr)
        return 0
    webhook = os.environ.get("LARK_WEBHOOK", "").strip()
    if not webhook:
        print(f"LARK_WEBHOOK 没配置, 告警只打到 stderr: [{args.title}] {content}",
              file=sys.stderr)
        return 0
    return 0 if notify(
        webhook,
        title=args.title,
        content=content,
        severity=args.severity,
        service=args.service,
    ) else 1


if __name__ == "__main__":
    raise SystemExit(main())
