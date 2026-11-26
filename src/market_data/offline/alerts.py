"""Best-effort, deduplicated lark_alert integration.

**告警一律发卡片, 不发纯文本.** 飞书的文本消息**不显示发送时间** —— 事后翻记录,
"[tw-intraday] 采集连续 5 轮失败" 是哪天几点发的得靠猜. 卡片自带 服务/节点/时间
三个字段, 与两个 Rust 服务端、看板摘要长得一样, 一条时间线可以直接对着读.

**一个发送实现.** 以前 daily.sh 用 curl、盘中采集与新鲜度检查各自手拼 JSON, 三处
格式各不相同, 而且只有它们各自的作者知道为什么. 现在都走 `notify()`: 卡片是默认,
装不上 `lark_alert` 的机器 (比如没编过扩展的开发机) 才退回纯文本 POST —— 告警
本身不能因为缺扩展就消失.

**失败方向: 告警发不出去不改退出码.** 数据没更新是事实, 告警通道也坏了是另一件事;
两者混在一个退出码里, 就再也分不清是哪一件.
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass
from datetime import datetime
from typing import Any
from zoneinfo import ZoneInfo

logger = logging.getLogger(__name__)

__all__ = ["AlertEvent", "AlertSink", "notify"]

#: 卡片上的时间按台北时间写: A 股与台股都在 +08:00, 一条时间线不需要换算.
TAIPEI = ZoneInfo("Asia/Taipei")


@dataclass
class AlertEvent:
    market: str
    dataset: str
    code: str
    message: str
    detail: str = ""


def _client(webhook: str) -> Any:
    """`LarkAlert` 客户端. 每次新建: 告警一天没几条, 不用操心连接池."""
    from lark_alert import LarkAlert

    return LarkAlert(
        webhook,
        secret=os.environ.get("LARK_SECRET") or None,
        timeout_secs=int(os.environ.get("LARK_TIMEOUT_SECS", "5")),
        max_retries=int(os.environ.get("LARK_MAX_RETRIES", "2")),
    )


def _post_text(webhook: str, text: str) -> bool:
    """没有 `lark_alert` 时的兜底: 一条 urllib POST 的纯文本."""
    body = json.dumps(
        {"msg_type": "text", "content": {"text": text}}, ensure_ascii=False
    ).encode("utf-8")
    request = urllib.request.Request(
        webhook, data=body, headers={"Content-Type": "application/json"}
    )
    try:
        with urllib.request.urlopen(request, timeout=5) as response:
            return 200 <= int(response.status) < 300
    except (urllib.error.URLError, TimeoutError, OSError):
        return False


def notify(
    webhook: str,
    *,
    title: str,
    content: str,
    severity: str = "error",
    service: str = "market_data",
    node: str | None = None,
) -> bool:
    """发一条告警. 有 `lark_alert` 发卡片, 没有就发纯文本.

    `severity` 是 `info|success|warning|error|critical`; 写错了按 `error` 处理 ——
    一条颜色不对的告警仍然是有用的, 一条因为拼错颜色而消失的告警不是.
    """
    if not webhook.strip():
        logger.warning("LARK_WEBHOOK is not configured; alert: [%s] %s", title, content)
        return False
    try:
        from lark_alert import Card, Severity

        stamp = datetime.now(TAIPEI).isoformat(timespec="minutes")
        card = (
            Card(
                service=service,
                node=node or os.uname().nodename,
                timestamp=stamp,
                content=content,
            )
            .severity(getattr(Severity, severity.capitalize(), Severity.Error))
            .title(title)
        )
        _client(webhook).send_card(card)
        return True
    except ImportError:
        return _post_text(webhook, f"[{title}] {content}")
    except Exception:
        logger.exception("failed to send alert: %s", title)
        return False


class AlertSink:
    """Send one alert per key during a process lifetime."""

    def __init__(self, service: str = "market_data", node: str | None = None) -> None:
        self.service = service
        self.node = node or os.uname().nodename
        self._sent: dict[str, float] = {}

    def send(self, event: AlertEvent, dedup_seconds: float = 900.0) -> bool:
        key = f"{event.market}/{event.dataset}/{event.code}/{event.message}"
        now = time.monotonic()
        if (previous := self._sent.get(key)) is not None and now - previous < dedup_seconds:
            return False
        content = f"[{event.market}/{event.dataset}] {event.message}"
        if event.detail:
            content += f"\n{event.detail}"
        sent = notify(
            os.environ.get("LARK_WEBHOOK", "").strip(),
            title=f"{self.service}: {event.code}",
            content=content,
            severity="error",
            service=self.service,
            node=self.node,
        )
        if sent:
            self._sent[key] = now
        return sent
