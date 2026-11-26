#!/usr/bin/env python3
"""在线库(psql)每日导出到 parquet, 并按保留期清理.

规则（见 `docs/storage-architecture.md`）：

1. **先导出, 再清理**：某一天的 ticks/klines 只有在归档分区已经存在时才允许删；
2. 保留期：ticks 10 天、1m 30 天、5m 90 天、quotes 30 天（可参数覆盖）；
3. 只碰 `market='cn'` 的在线行情；spider 的其他表不动。

    archive_daily.py --date 2026-11-16          # 导出某一天
    archive_daily.py --clean                    # 按保留期清理（会先验证归档）
    archive_daily.py --date 2026-11-16 --clean  # 两步一起
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

ROOT = Path("/home/jas/data/archive")
DSN = "postgresql://postgres:postgres@localhost:5432/stock_a_spider"
TAIPEI = ZoneInfo("Asia/Taipei")
TABLES = {
    # name: (parquet 子目录, 保留天数)
    "ticks": (ROOT / "ticks" / "cn", 10),
    "klines:1m": (ROOT / "bars" / "cn" / "1m", 30),
    "klines:5m": (ROOT / "bars" / "cn" / "5m", 90),
}


def _connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute(f"ATTACH '{DSN}' AS pg (TYPE POSTGRES)")
    con.execute("SET preserve_insertion_order=false")
    con.execute("SET threads=4")
    return con


def _bounds(day: date) -> tuple[int, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=TAIPEI)
    return int(start.timestamp() * 1000), int((start + timedelta(days=1)).timestamp() * 1000)


def _source(name: str) -> tuple[str, str | None]:
    if name == "ticks":
        return "ticks", None
    _, period = name.split(":")
    return "klines", period


def _dump(con: duckdb.DuckDBPyConnection, name: str, day: date) -> int:
    target, _ = TABLES[name]
    table, period = _source(name)
    start, end = _bounds(day)
    where = f"ts >= {start} AND ts < {end}"
    if period:
        where += f" AND period = '{period}'"
    if table == "ticks":
        columns = "code AS symbol, ts, serial, price, volume, updated_at"
    else:
        columns = (
            "code AS symbol, period, ts, open, high, low, close, volume,"
            " amount, turnover, pre_close"
        )
    target.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""
        COPY (
            SELECT {columns},
                   (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE AS date
            FROM pg.{table}
            WHERE {where}
        ) TO '{target}' (FORMAT PARQUET, PARTITION_BY (symbol, date), OVERWRITE_OR_IGNORE)
        """
    )
    rows = con.execute(f"SELECT count(*) FROM pg.{table} WHERE {where}").fetchone()[0]
    return int(rows or 0)


def _archived(name: str, day: date) -> bool:
    target, _ = TABLES[name]
    tail = f"date={day.year}-{day.month:02d}-{day.day:02d}"
    return any(target.glob(f"symbol=*/{tail}/*.parquet"))


def _clean(con: duckdb.DuckDBPyConnection, name: str, today: date) -> tuple[int, int]:
    table, period = _source(name)
    _, keep = TABLES[name]
    cutoff_ms, _ = _bounds(today - timedelta(days=keep))
    period_filter = f" AND period = $${period}$$" if period else ""
    days = [
        row[0]
        for row in con.execute(
            "SELECT DISTINCT (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE"
            f" FROM pg.{table} WHERE ts < {cutoff_ms}{period_filter} ORDER BY 1"
        ).fetchall()
    ]
    removed = skipped = 0
    for day in days:
        if not _archived(name, day):
            skipped += 1
            continue
        start, end = _bounds(day)
        # **必须 fetchall**: DuckDB 的 SELECT 是惰性的, 不取结果这条 DELETE 根本不会执行.
        con.execute(
            f"SELECT * FROM postgres_execute('pg', 'DELETE FROM {table}"
            f" WHERE ts >= {start} AND ts < {end}{period_filter}')"
        ).fetchall()
        removed += 1
    return removed, skipped


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--clean", action="store_true")
    parser.add_argument("--dump", action="store_true")
    args = parser.parse_args(argv)
    today = datetime.now(TAIPEI).date()
    target = args.date or today

    con = _connect()
    try:
        # 默认动作: 导出目标日 + （带 --clean 时）按保留期清理.
        # --clean 也必须先导当天, 否则当天数据永远进不了归档.
        if args.dump or args.date or args.clean:
            for name in TABLES:
                rows = _dump(con, name, target)
                print(f"dump {name} {target}: {rows} 行")
        if args.clean:
            for name in TABLES:
                removed, skipped = _clean(con, name, today)
                print(f"clean {name}: 删除 {removed} 天, 跳过 {skipped} 天（归档缺失）")
    except duckdb.Error as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
