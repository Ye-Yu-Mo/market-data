#!/usr/bin/env python3
"""台股盘中快照中转 (xiaoxin): TWSE/TPEX MIS -> 与券商快照相同的 JSON 形状.

为什么需要它: 盘中快照的原始来源是券商服务端 (VPS), 而 VPS 这个月停着. xiaoxin
能直连 `mis.twse.com.tw` (leaf 连不上), 所以由它做只读中转, leaf 上的
`record_tw_quotes.py` 只换地址, 其余不动.

**MIS 的怪癖**: 批量请求时 `z` (最后成交价) 经常是 `-`, 但 `b`/`a` (买一/卖一)
一直在. 价格优先取 `z`, 缺了取买卖中间价 —— 15 秒采样下误差不超过一个价差,
比"没有今天的数据"强.

    TW_QUOTE_TOKEN=xxx python3 tw_quote_relay.py --port 18001

接口 (与券商服务端一致):

    GET /api/v1/quotes/snapshot?symbols=2330,2317&market_type=TWSE
    -> {"code": 0, "message": "ok", "data": {"query_watch_list": [...]}}

`market_type` 收 `TWSE` / `TWOTC` (leaf 侧的 VENDOR_MARKET 就是这么发的).
"""

from __future__ import annotations

import argparse
import json
import os
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Mapping

MIS_URL = "https://mis.twse.com.tw/stock/api/getStockInfo.jsp"
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
_CHANNEL = {"TWSE": "tse", "TWOTC": "otc"}
#: MIS 一次 100 只实测 6 秒左右; 再多 URL 长度和延迟都不划算.
BATCH = 100
TIMEOUT = 25.0


def _num(raw: Any) -> float | None:
    if raw in (None, "", "-"):
        return None
    try:
        value = float(raw)
    except (TypeError, ValueError):
        return None
    return value if value != 0 else None


def _first(raw: Any) -> float | None:
    """`b`/`a` 是下划线分隔的五档, 取第一档."""
    if not isinstance(raw, str) or not raw:
        return None
    return _num(raw.split("_")[0])


def _qty(raw: Any) -> int | None:
    if not isinstance(raw, str) or not raw:
        return None
    try:
        return int(float(raw.split("_")[0]))
    except (TypeError, ValueError):
        return None


def _stamp(item: Mapping[str, Any]) -> dict[str, int] | None:
    day = str(item.get("d") or "")
    clock = str(item.get("t") or "")
    if len(day) != 8 or len(clock.split(":")) != 3:
        return None
    try:
        return {
            "year": int(day[0:4]),
            "month": int(day[4:6]),
            "day": int(day[6:8]),
            "hour": int(clock[0:2]),
            "minute": int(clock[3:5]),
            "second": int(clock[6:8]),
        }
    except ValueError:
        return None


def _item(raw: Mapping[str, Any]) -> dict[str, Any] | None:
    symbol = str(raw.get("c") or "").strip()
    stamp = _stamp(raw)
    if not symbol or stamp is None:
        return None
    bid = _first(raw.get("b"))
    ask = _first(raw.get("a"))
    price = _num(raw.get("z"))
    if price is None and bid is not None and ask is not None:
        price = round((bid + ask) / 2, 4)
    if price is None:
        price = bid if bid is not None else ask
    if price is None:
        price = _num(raw.get("y"))
    volume = _qty(raw.get("v")) if isinstance(raw.get("v"), str) else None
    amount = round(volume * price / 10, 2) if volume and price else None
    return {
        "stk_code": symbol,
        "deal_price": price,
        "open_price": _num(raw.get("o")),
        "high_price": _num(raw.get("h")),
        "low_price": _num(raw.get("l")),
        "buy_price": bid,
        "sell_price": ask,
        "bid_qty": _qty(raw.get("f")),
        "ask_qty": _qty(raw.get("g")),
        "total_vol": volume,
        "total_deal_amt": amount,
        "yst_price": _num(raw.get("y")),
        "time": stamp,
    }


def _fetch(symbols: list[str], market_type: str) -> list[dict[str, Any]]:
    channel = _CHANNEL.get(market_type)
    if channel is None or not symbols:
        return []
    items: list[dict[str, Any]] = []
    for start in range(0, len(symbols), BATCH):
        chunk = symbols[start : start + BATCH]
        query = urllib.parse.urlencode(
            {
                "ex_ch": "|".join(f"{channel}_{symbol}.tw" for symbol in chunk),
                "json": "1",
                "delay": "0",
            }
        )
        request = urllib.request.Request(
            f"{MIS_URL}?{query}", headers={"User-Agent": USER_AGENT}
        )
        with urllib.request.urlopen(request, timeout=TIMEOUT) as response:
            # MIS 在 JSON 前面塞了一串空行.
            payload = json.loads(response.read().lstrip())
        for raw in payload.get("msgArray") or []:
            if isinstance(raw, dict):
                item = _item(raw)
                if item is not None:
                    items.append(item)
    return items


class _Handler(BaseHTTPRequestHandler):
    server_version = "tw-quote-relay/1.0"
    _token = ""

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003
        return

    def _send(self, status: int, body: Mapping[str, Any]) -> None:
        raw = json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urllib.parse.urlsplit(self.path)
        if parsed.path == "/health":
            self._send(200, {"status": "ok"})
            return
        if parsed.path != "/api/v1/quotes/snapshot":
            self._send(404, {"code": 404, "message": "not found"})
            return
        if self._token:
            if self.headers.get("Authorization") != f"Bearer {self._token}":
                self._send(401, {"code": 401, "message": "unauthorized"})
                return
        params = urllib.parse.parse_qs(parsed.query)
        symbols = [s for s in params.get("symbols", [""])[0].split(",") if s]
        market_type = params.get("market_type", ["TWSE"])[0]
        try:
            items = _fetch(symbols, market_type)
        except OSError as exc:
            # 上游挂了要说清楚, 不能静默返回空表 —— 那正是 11-12 到 11-16 的坑.
            self._send(502, {"code": 502, "message": f"mis: {exc}"})
            return
        self._send(200, {"code": 0, "message": "ok", "data": {"query_watch_list": items}})


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=18001)
    args = parser.parse_args()
    _Handler._token = os.environ.get("TW_QUOTE_TOKEN", "")
    server = ThreadingHTTPServer((args.host, args.port), _Handler)
    print(f"tw_quote_relay listening on {args.host}:{args.port}", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
