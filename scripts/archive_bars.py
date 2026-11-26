#!/usr/bin/env python3
"""把行情从单文件 DuckDB 归档成 parquet, 按**标的 + 日期**分区.

布局（Hive 风格, DuckDB / Spark / pandas 都能直接读分区）:

    <root>/bars/<market>/1d/symbol=2330/year=2026/*.parquet
    <root>/bars/<market>/1m/symbol=2330/date=2026-11-16/*.parquet
    <root>/symbols/<market>.parquet

为什么这样分: 查一只标的 = 一个目录; 查一天 = 所有标的里那一天的目录.
单文件 DuckDB 的写锁会挡住所有读者 —— 这是 2026-11 反复踩到的坑.

    archive_bars.py --what daily --market tw
    archive_bars.py --what daily --market cn --since-year 2026   # 只更当年
    archive_bars.py --what intraday
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

ROOT = Path("/home/jas/data/archive")
SOURCES = {
    "cn": Path("/home/jas/data/market/market_a.duckdb"),
    "tw": Path("/home/jas/data/market/market_v2.duckdb"),
}
INTRADAY = Path("/home/jas/data/market/market_tw_intraday.duckdb")
DSN = "postgresql://postgres:postgres@localhost:5432/stock_a_spider"


def _daily(con: duckdb.DuckDBPyConnection, market: str, root: Path, since: int | None) -> None:
    source = SOURCES[market]
    where = f"WHERE year(c.date) >= {since}" if since else ""
    by_symbol = root / "bars" / market / "1d"
    by_date = root / "bars" / market / "1d_by_date"
    (root / "symbols").mkdir(parents=True, exist_ok=True)
    expression = f"""
        SELECT c.symbol, c.date, c.open, c.high, c.low, c.close,
               c.volume, c.amount, c.turnover,
               coalesce(a.factor, 1.0) AS factor,
               year(c.date) AS year
        FROM src.candles c
        LEFT JOIN src.adj_factor a ON a.symbol = c.symbol AND a.date = c.date
        {where}
    """
    con.execute(f"ATTACH '{source}' AS src (READ_ONLY)")
    # 按 date 分区时 DuckDB 默认保留插入顺序, 几千个分区会把内存吃光
    # (实测 12.4 GiB 全满). 关掉它, 顺便快一截.
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    # 分开执行: 任何一步失败都能看到是哪一步, 而不是整段静默.
    con.execute(
        f"COPY ({expression}) TO '{by_symbol}'"
        " (FORMAT PARQUET, PARTITION_BY (symbol, year), OVERWRITE_OR_IGNORE)"
    )
    con.execute(
        f"COPY ({expression}) TO '{by_date}'"
        " (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE)"
    )
    con.execute(
        f"COPY (SELECT * FROM src.symbols) TO '{root}/symbols/{market}.parquet' (FORMAT PARQUET)"
    )


def _intraday(con: duckdb.DuckDBPyConnection, root: Path, since: str | None) -> None:
    target = root / "bars" / "tw" / "1m"
    target.parent.mkdir(parents=True, exist_ok=True)
    since_ms = 0
    if since:
        day = date.fromisoformat(since)
        since_ms = int(
            datetime(day.year, day.month, day.day, tzinfo=ZoneInfo("Asia/Taipei")).timestamp()
            * 1000
        )
    # 在线库优先（2026-11-17 起台股 1m 以 psql 为准）；连不上再退回 DuckDB.
    try:
        con.execute(f"ATTACH '{DSN}' AS pgp (TYPE POSTGRES)")
        con.execute(
            f"""
            COPY (
                SELECT code AS symbol,
                       (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei') AS ts,
                       (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE AS trade_date,
                       'tw' AS market, open, high, low, close, volume, amount,
                       CAST(NULL AS INTEGER) AS samples
                FROM pgp.klines
                WHERE market = 'tw' AND period = '1m'
                {"AND ts >= " + str(since_ms) if since_ms else ""}
            ) TO '{target}' (FORMAT PARQUET, PARTITION_BY (symbol, trade_date), OVERWRITE_OR_IGNORE)
            """
        )
        return
    except duckdb.Error:
        pass
    where = f"WHERE trade_date >= DATE '{since}'" if since else ""
    con.execute(
        f"""
        ATTACH '{INTRADAY}' AS intra (READ_ONLY);
        COPY (
            SELECT symbol, ts, trade_date, market, open, high, low, close,
                   volume, amount, samples
            FROM intra.bars_1m
            {where}
        ) TO '{target}' (FORMAT PARQUET, PARTITION_BY (symbol, trade_date), OVERWRITE_OR_IGNORE);
        """
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--what", choices=("daily", "intraday"), required=True)
    parser.add_argument("--market", choices=("cn", "tw"), default="tw")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--since-year", type=int, default=None, help="日线只导出这一年及以后")
    parser.add_argument("--since-date", default=None, help="分钟线只导出这一天及以后")
    args = parser.parse_args(argv)

    con = duckdb.connect()
    try:
        if args.what == "daily":
            _daily(con, args.market, args.root, args.since_year)
        else:
            _intraday(con, args.root, args.since_date)
    except duckdb.Error as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()
    print(f"done: {args.what} {args.market} -> {args.root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
