# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# 抑制范围与 `markets/tw/__init__.py` 相同: pandas 的桩给不出具体类型, 形状由下面的
# 显式列名与数值转换保证.

"""盘中数据的**读取入口** —— 研究侧从这里拿 `bars_1m`.

**为什么单独一个模块, 而不是让人自己写 SQL**

`bars_1m` 有三个坑, 每一个都会让研究结论错而不报错:

1. **时间是台北本地时间, 不带时区**. 直接用会按运行机器解释 —— 在 UTC 机器上
   整条时间轴偏 8 小时, 而"隔夜跳空"这类东西会凭空出现;
2. **成交量已经是股 (不是张), 成交额已经是元 (不是万元)** —— 采集器存原值,
   重建器换算过; 直接拿原始 `quotes` 表的人很容易把单位搞混;
3. **库在 08:00-13:36 台北被采集器独占写锁** —— 那时读会抛 `Conflicting lock`,
   而"读不到"与"今天没数据"是两件事.

所以这里做三件事: 锚定时区、把列名与单位固定下来、把锁的问题变成一个明确的异常。

用法::

    from market_data.markets.tw.intraday import load_bars

    bars = load_bars("~/data/market/market_tw_intraday.duckdb",
                     symbols=["2330"], start=date(2026, 10, 22))
    bars.head()
"""

from __future__ import annotations

from datetime import date
from pathlib import Path
from typing import Any

import duckdb
import pandas as pd

__all__ = ["INTRADAY_LOCKED", "IntradayLocked", "load_bars"]

#: 采集器正在写库时抛出的稳定标识 (与交易看板用同一个词).
INTRADAY_LOCKED = "INTRADAY_LOCKED"


class IntradayLocked(RuntimeError):
    """盘中采集器正持有写锁. **这是预期状态**, 不是坏库 —— 收盘后重试即可."""


_COLUMNS = ("symbol", "ts", "open", "high", "low", "close", "volume", "amount", "samples")


def load_bars(
    db: str | Path,
    *,
    symbols: list[str] | None = None,
    start: date | None = None,
    end: date | None = None,
) -> pd.DataFrame:
    """读 1 分钟 bar. 列: `symbol, ts, open, high, low, close, volume, amount, samples`.

    - `ts` 是 **带 `Asia/Taipei` 时区**的时间戳;
    - `volume` 是**股**, `amount` 是**元** (与 `market_v2.candles` 同一口径);
    - `samples` 是这一分钟里采了几帧 (15 秒一帧 → 正常是 3-4; 明显偏少说明那分钟
      采集中断过, 研究时可以用来判断数据质量).
    """
    path = Path(db).expanduser()
    if not path.is_file():
        raise FileNotFoundError(f"{path} 不存在 —— 采集器跑过吗?")

    where: list[str] = []
    params: list[Any] = []
    if symbols:
        where.append(f"symbol IN ({','.join('?' * len(symbols))})")
        params.extend(symbols)
    if start is not None:
        where.append("trade_date >= ?")
        params.append(start)
    if end is not None:
        where.append("trade_date <= ?")
        params.append(end)
    clause = f" WHERE {' AND '.join(where)}" if where else ""

    try:
        connection = duckdb.connect(str(path), read_only=True)
    except duckdb.Error as exc:
        if "lock" in str(exc).lower() or "different configuration" in str(exc).lower():
            raise IntradayLocked(
                "盘中采集器正在写库 (08:00-13:36 台北), 收盘后再读"
            ) from exc
        raise

    try:
        frame = connection.execute(
            f"SELECT {', '.join(_COLUMNS)} FROM bars_1m{clause} ORDER BY symbol, ts",
            params,
        ).df()
    finally:
        connection.close()

    if frame.empty:
        return frame

    # 库里存的是**台北本地墙钟时间**. `tz_localize` 显式指定时区, 不按运行机器
    # 解释 —— 换成 `.dt.tz_convert` 或 `.timestamp()` 都会把机器时区带进来
    # (leaf 是 UTC, 整条时间轴会偏 8 小时, 2026-10-22 实测过).
    frame["ts"] = pd.to_datetime(frame["ts"]).dt.tz_localize("Asia/Taipei")
    for column in ("open", "high", "low", "close", "volume", "amount"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    return frame
