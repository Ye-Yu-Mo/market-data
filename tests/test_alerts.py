# pyright: reportUnknownParameterType=false, reportMissingParameterType=false, reportUnknownMemberType=false

import sys
from unittest.mock import ANY, MagicMock, patch

from lark_alert import Card, LarkAlert, Severity

from market_data.offline.alerts import AlertEvent, AlertSink, notify


def event() -> AlertEvent:
    return AlertEvent("TEST", "candles", "smoke", "local smoke test")


def test_lark_alert_api_is_importable() -> None:
    card = Card(service="svc", node="node", timestamp="now", content="content")
    configured = card.severity(Severity.Error).title("title").summary("summary")
    assert type(configured) is Card


def test_alert_passes_integer_options_and_builds_card(monkeypatch) -> None:
    monkeypatch.setenv("LARK_WEBHOOK", "https://example.invalid/placeholder")
    monkeypatch.setenv("LARK_TIMEOUT_SECS", "11")
    monkeypatch.setenv("LARK_MAX_RETRIES", "4")
    monkeypatch.delenv("LARK_SECRET", raising=False)

    card = MagicMock()
    card.severity.return_value = card
    card.title.return_value = card
    content = "[TEST/candles] local smoke test"

    with (
        patch("lark_alert.Card", return_value=card) as card_class,
        patch("lark_alert.LarkAlert") as alert_class,
    ):
        assert AlertSink(service="svc", node="node").send(event()) is True

    alert_class.assert_called_once_with(
        "https://example.invalid/placeholder",
        secret=None,
        timeout_secs=11,
        max_retries=4,
    )
    options = alert_class.call_args.kwargs
    assert type(options["timeout_secs"]) is int
    assert type(options["max_retries"]) is int
    card_class.assert_called_once_with(
        service="svc",
        node="node",
        timestamp=ANY,
        content=content,
    )
    card.severity.assert_called_once_with(Severity.Error)
    card.title.assert_called_once_with("svc: smoke")
    alert_class.return_value.send_card.assert_called_once_with(card)


def test_alert_is_safe_when_webhook_is_unconfigured(monkeypatch) -> None:
    monkeypatch.delenv("LARK_WEBHOOK", raising=False)
    with patch.object(LarkAlert, "send_card") as send_card:
        assert AlertSink().send(event()) is False
    send_card.assert_not_called()


def test_alert_sends_once_and_deduplicates(monkeypatch) -> None:
    monkeypatch.setenv("LARK_WEBHOOK", "https://example.invalid/placeholder")
    sink = AlertSink()
    with patch.object(LarkAlert, "send_card", return_value=None) as send_card:
        assert sink.send(event()) is True
        assert sink.send(event()) is False
    assert send_card.call_count == 1


def test_alert_failure_does_not_escape(monkeypatch) -> None:
    monkeypatch.setenv("LARK_WEBHOOK", "https://example.invalid/placeholder")
    with patch.object(LarkAlert, "send_card", side_effect=RuntimeError("mock")):
        assert AlertSink().send(event()) is False


def test_notify_falls_back_to_text_without_the_sdk(monkeypatch) -> None:
    """没编过扩展的机器上告警**必须还能发出去** —— 只是没有卡片与时间字段."""
    # `sys.modules["x"] = None` 让 `import x` 抛 ImportError, 这是模拟"没装"的标准做法.
    monkeypatch.setitem(sys.modules, "lark_alert", None)
    with patch("market_data.offline.alerts._post_text", return_value=True) as post:
        assert notify("https://example.invalid/hook", title="日更失败", content="源不可达")
    post.assert_called_once_with("https://example.invalid/hook", "[日更失败] 源不可达")


def test_notify_without_webhook_is_silent(monkeypatch) -> None:
    with patch("market_data.offline.alerts._post_text") as post:
        assert notify("", title="x", content="y") is False
    post.assert_not_called()
