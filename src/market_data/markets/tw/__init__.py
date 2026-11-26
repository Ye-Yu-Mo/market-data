"""台股的身份表: 从 FinMind 的 ``TaiwanStockInfo`` 生成 ``symbols``.

**为什么需要这张表**

``market_v2.duckdb`` 的 ``symbols`` 表是空的, 而台股的交易所**不能从代码猜**:
上市 (TWSE) 与上柜 (TPEX) 都是 4 位数字. ``factor_research.resolve_market`` 把
4 位一律判成 TWSE —— 上柜的 1,133 只会被静默标成上市, 于是 ``security`` 变成错的,
按它索引的一切 (行业中性化、留痕对账、目标持仓) 都对不上, 而没有任何地方会报错.

这和 A 股那边"裸代码全被标成 TPEX"是同一个坑, 解法也一样: **身份来自数据**.
``stock_tw`` 抓的 FinMind ``TaiwanStockInfo`` 里写着 ``type`` (twse / tpex /
emerging), 这里把它读成一张**旁表**, 不去动 ``market_v2.duckdb``
(它的生产归 ``stock_tw``).

**为什么取"每只标的的最新一行"**

同一只标的会因转板 (上柜 → 上市) 或行业调整而出现多行. 身份要在**今天成立**,
所以按 ``date`` 取最新的一条; 取任意一条会让身份停在旧交易所.
"""

# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# 抑制范围与 `cn/free_stockdb.py` 相同: 外部 JSON 与 pandas 的桩都给不出具体类型.
# 形状在运行期由 `parse_stock_info` / `build_symbols` 的显式检查保证.

from __future__ import annotations

import gzip
import json
import re
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Iterable, Mapping
from typing import Any

import pandas as pd

#: 台股证券代码的形状: 4-6 位数字 + 可选字母后缀 (``00637L``). 指数 (``TAIEX``)
#: 与类股指数 (``Semiconductor``) 不符合 —— 见 `build_symbols`.
_SECURITY_CODE = re.compile(r"^[0-9]{4,6}[A-Z]?$")

__all__ = ["FINMIND_DATA_URL", "MARKET_BY_TYPE", "build_symbols", "fetch_stock_info", "parse_stock_info"]

#: FinMind 的 ``type`` -> ``factor_research`` 的交易所枚举.
#:
#: 只写核实过的三个. 出现别的取值时**报错**, 不替它编一个市场 ——
#: 编出来的身份不会让任何计算失败, 只会让 ``security`` 变成错的.
MARKET_BY_TYPE: Mapping[str, str] = {
    "twse": "TWSE",
    "tpex": "TPEX",
    "emerging": "ESB",
}


def parse_stock_info(raw: bytes) -> list[dict[str, Any]]:
    """解 `TaiwanStockInfo` 的响应, 返回 ``data`` 里的行.

    **两种形状都收**: 从 FinMind API 直接拿到的是 JSON; 抓包存下来的是 gzip 压缩
    的 JSON (`stock_tw` 的 raw 落盘格式). 先按 JSON 解, 失败再 gunzip —— 两种都
    失败才报错: 一个解析失败被当成"空表", 表现会是"台股没有标的".
    """
    try:
        payload = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, ValueError):
        try:
            payload = json.loads(gzip.decompress(raw))
        except (OSError, ValueError) as exc:
            raise ValueError(f"TaiwanStockInfo 响应既不是 JSON 也不是 gzip: {exc}") from exc
    if not isinstance(payload, dict):
        raise ValueError("TaiwanStockInfo 原始包不是 JSON 对象")
    rows = payload.get("data")
    if not isinstance(rows, list):
        raise ValueError("TaiwanStockInfo 原始包没有 data 数组 —— 形状不对")
    return [row for row in rows if isinstance(row, dict)]


#: FinMind 的通用数据接口. `TaiwanStockInfo` 一次请求就能拿到全部标的与交易所.
FINMIND_DATA_URL = "https://api.finmindtrade.com/api/v4/data"


def fetch_stock_info(*, token: str = "", timeout: float = 30.0) -> bytes:
    """**直接从 FinMind** 拉一份 `TaiwanStockInfo` 的响应 (JSON).

    日更用它刷新身份旁表 —— 这样就不依赖 `stock_tw` 的原始抓包目录: 服务器上
    只装了 `stock_tw` 的**源码**, 没装它的历史数据目录.
    """
    params = {"dataset": "TaiwanStockInfo"}
    if token:
        params["token"] = token
    url = f"{FINMIND_DATA_URL}?{urllib.parse.urlencode(params)}"
    try:
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return bytes(response.read())
    except (urllib.error.URLError, TimeoutError, OSError) as exc:
        raise RuntimeError(f"拉取 TaiwanStockInfo 失败: {exc}") from exc


def build_symbols(rows: Iterable[Mapping[str, Any]]) -> pd.DataFrame:
    """组装 ``symbols`` 契约表: ``(symbol, name, market, status)``, 按 symbol 取最新一行.

    **会丢掉非证券的代码** (指数与类股指数). FinMind 的 `TaiwanStockInfo` 把加权
    指数 `TAIEX`、`Semiconductor` 这些也标成 ``type: twse`` —— 2026-10-22 实测
    有 32 个, 它们和普通上市公司一起进了身份表, 而身份表的消费方都当它是可交易
    标的: 采集器把 20 个名额浪费在它们身上 (券商的 watchlist 不返回指数), 而
    `factor_maker` 的 `universe_markets` 也拦不住它们 (市场就是 TWSE) —— 复权
    因子一旦覆盖到它们, 策略就可能选中一个指数去下单.

    判据是**代码形状**: 台股证券代码是 4-6 位数字 + 可选字母后缀 (``00637L``),
    而指数是纯字母名字. 这条规则不依赖 FinMind 的 type 字段 (那个字段在这里不可信).
    """
    latest: dict[str, Mapping[str, Any]] = {}
    for item in rows:
        symbol = str(item.get("stock_id", "")).strip()
        if not symbol:
            raise ValueError(f"TaiwanStockInfo 行缺少 stock_id: {item!r}")
        if not _SECURITY_CODE.match(symbol):
            continue
        day = str(item.get("date", ""))
        if symbol not in latest or day > str(latest[symbol].get("date", "")):
            latest[symbol] = item

    records: list[dict[str, str]] = []
    for symbol in sorted(latest):
        item = latest[symbol]
        raw_type = str(item.get("type", "")).strip().lower()
        market = MARKET_BY_TYPE.get(raw_type)
        if market is None:
            raise ValueError(
                f"{symbol}: 未知的 type={raw_type!r} —— 不为没有核实过的类型编一个市场"
            )
        name = str(item.get("stock_name", "")).strip()
        if not name:
            raise ValueError(f"{symbol}: 缺少 stock_name —— symbols.name 不能为空")
        # FinMind 的 TaiwanStockInfo 只列**当前**在册的标的; 已下市的不在其中.
        # 所以这里只有 listed. 将来若接入下市名单, 才是加 delisted 的地方.
        records.append({"symbol": symbol, "name": name, "market": market, "status": "listed"})

    return pd.DataFrame(records, columns=pd.Index(["symbol", "name", "market", "status"]))
