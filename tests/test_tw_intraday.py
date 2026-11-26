"""盘中读取入口的测试: 时区、单位、锁.

**时区这条必须与运行测试的机器无关** —— 原来那条看板用例在 Mac (UTC+8) 上是绿的,
在 leaf (UTC) 上是错的. 所以这里断言的是"挂钟小时"与"时区标签", 不是 epoch.
"""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path

import duckdb
import pytest

from market_data.markets.tw.intraday import IntradayLocked, load_bars


def _db(path: Path) -> Path:
    connection = duckdb.connect(str(path))
    connection.execute(
        "CREATE TABLE bars_1m (trade_date DATE, ts TIMESTAMP, symbol VARCHAR, market VARCHAR,"
        " open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE, volume BIGINT, amount BIGINT,"
        " samples INTEGER, PRIMARY KEY (trade_date, ts, symbol))"
    )
    connection.executemany(
        "INSERT INTO bars_1m VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        [
            (date(2026, 10, 22), datetime(2026, 10, 22, 9, 1), "2330", "TWSE", 2400, 2405, 2398, 2402, 7000, 1_680_000, 4),
            (date(2026, 10, 22), datetime(2026, 10, 22, 13, 30), "2330", "TWSE", 2385, 2390, 2380, 2388, 1000, 238_000, 3),
        ],
    )
    connection.close()
    return path


def test_timestamps_are_taipei_local(tmp_path: Path) -> None:
    """库里是台北墙钟时间, 读出来必须还锚在台北 —— 不随运行机器的时区漂."""
    frame = load_bars(_db(tmp_path / "i.duckdb"), symbols=["2330"])
    assert len(frame) == 2
    assert str(frame["ts"].dt.tz) == "Asia/Taipei"
    assert frame["ts"].dt.hour.tolist() == [9, 13]
    assert frame["ts"].dt.minute.tolist() == [1, 30]


def test_volume_is_shares_and_amount_is_twd(tmp_path: Path) -> None:
    """单位必须与 `market_v2.candles` 一致 (股 / 元), 不能再是张 / 万元."""
    frame = load_bars(_db(tmp_path / "i.duckdb"))
    assert frame["volume"].tolist() == [7000.0, 1000.0]
    assert frame["amount"].tolist() == [1_680_000.0, 238_000.0]


def test_locked_db_is_a_typed_error(tmp_path: Path) -> None:
    """采集器持锁是**预期状态**: 要能分辨, 而不是当成坏库."""
    path = _db(tmp_path / "i.duckdb")
    holder = duckdb.connect(str(path))
    try:
        with pytest.raises(IntradayLocked):
            load_bars(path)
    finally:
        holder.close()
