"""台股身份表: 交易所必须来自数据, **不能从 4 位代码猜**.

上市 (TWSE) 与上柜 (TPEX) 都是 4 位数字; `factor_research.resolve_market` 把 4 位
一律判成 TWSE, 于是上柜会被静默标成上市 —— 与 A 股裸代码全被标成 TPEX 是同一个坑.
这张表的数据源是 stock_tw 抓下来的 FinMind `TaiwanStockInfo` 原始包.
"""

# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# pandas 的类型桩在 DataFrame 的列索引上给不出具体类型, 与其它测试文件同样的抑制范围.

from __future__ import annotations

import gzip
import json
from typing import Any

import pytest

from market_data.markets.tw import build_symbols, parse_stock_info


def row(
    stock_id: str,
    name: str,
    type_: str,
    date: str = "2026-08-24",
) -> dict[str, Any]:
    return {
        "industry_category": "x",
        "stock_id": stock_id,
        "stock_name": name,
        "type": type_,
        "date": date,
    }


def payload(rows: list[dict[str, Any]]) -> dict[str, Any]:
    return {"msg": "success", "status": 200, "data": rows}


def test_maps_the_three_taiwan_types() -> None:
    frame = build_symbols(
        [row("2330", "台積電", "twse"), row("6488", "環球晶", "tpex"), row("4174", "浩鼎生技", "emerging")]
    )

    assert dict(zip(frame["symbol"], frame["market"], strict=True)) == {
        "2330": "TWSE",
        "6488": "TPEX",
        "4174": "ESB",
    }
    assert list(frame.columns) == ["symbol", "name", "market", "status"]
    assert set(frame["status"]) == {"listed"}


def test_latest_row_per_symbol_wins() -> None:
    """同一只可能先上柜后转上市 —— 取最新那条, 否则身份停在旧交易所."""
    frame = build_symbols(
        [row("1234", "X", "tpex", "2024-01-01"), row("1234", "X", "twse", "2026-08-24")]
    )

    assert frame.set_index("symbol").loc["1234", "market"] == "TWSE"
    assert len(frame) == 1


def test_indices_are_not_securities() -> None:
    """指数不是标的.

    FinMind 把加权指数 (TAIEX) 与类股指数 (Semiconductor) 也标成 `type: twse`,
    于是它们和普通上市公司一起进了身份表 —— 实测 32 个. 身份表的消费方都当它是
    可交易标的: 采集器浪费名额, 而 `universe_markets` 拦不住它 (市场就是 TWSE),
    复权因子一旦覆盖到它, 策略就可能选中一个指数去下单.
    """
    frame = build_symbols(
        [
            row("2330", "台積電", "twse"),
            row("TAIEX", "加權指數", "twse"),
            row("Semiconductor", "半導體類指數", "twse"),
            row("00637L", "元大滬深300正2", "twse"),
        ]
    )

    assert list(frame["symbol"]) == ["00637L", "2330"]


def test_unknown_type_is_refused_not_guessed() -> None:
    with pytest.raises(ValueError, match="type"):
        build_symbols([row("9999", "Y", "otc")])


def test_missing_name_is_refused() -> None:
    """`symbols.name` 是 NOT NULL —— 丢一行比编一个名字更隐蔽."""
    with pytest.raises(ValueError, match="stock_name"):
        build_symbols([row("9999", "", "twse")])


def test_parse_accepts_the_raw_gzip_bytes() -> None:
    raw = gzip.compress(json.dumps(payload([row("2330", "台積電", "twse")])).encode())

    rows = parse_stock_info(raw)

    assert rows[0]["stock_id"] == "2330"


def test_parse_accepts_plain_json_from_the_api() -> None:
    """FinMind API 直接返回 JSON; 抓包落盘的是 gzip. 两种都要收.

    服务器上没有 `stock_tw` 的原始抓包目录, 日更只能走 API.
    """
    raw = json.dumps(payload([row("2330", "台積電", "twse")])).encode()

    rows = parse_stock_info(raw)

    assert rows[0]["stock_id"] == "2330"


def test_parse_refuses_an_unexpected_shape() -> None:
    raw = gzip.compress(json.dumps({"msg": "error"}).encode())

    with pytest.raises(ValueError, match="TaiwanStockInfo"):
        parse_stock_info(raw)
