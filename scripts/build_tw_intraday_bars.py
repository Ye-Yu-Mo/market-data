#!/usr/bin/env python
"""把盘中的**快照**重建成 1 分钟 bar.

`record_tw_quotes.py` 每 15 秒记一帧快照, 直接拿它做研究有两个问题:
同一只标的几十帧里大部分是重复的 (价格没动), 而"这一分钟成交了多少"藏在
**累计量**的差值里. 所以原始快照是一份流水, 不是一份可用的面板 —— 这个脚本
把它压成每只标的每分钟一根 bar.

**成交量怎么来的**

`total_vol` / `total_amount` 是当日累计. 相邻两帧相减就是这两帧之间的成交量 ——
即使中间缺了几帧 (隧道断、进程重启), 差值仍然对, 只是这一笔算到了后一帧所在的
分钟. **不是**每个 bucket 内取 max-min: 那样跨 bucket 的成交会凭空消失.

**单位**

券商快照里 `total_vol` 的单位是**张** (1000 股), `total_deal_amt` 是**万元**
(10,000 元) —— 与 `market_v2.candles` 的 (股, 元) 不是同一套. 这里统一换成
(股, 元), 否则"盘中量"与"日线量"放在一起会差三个数量级, 而且不会报错.
单位是实测出来的: 八只标的的 `total_deal_amt / total_vol` 与当日价格都差
正好约 10 倍 (VWAP 落在当日高低区间内), 2026-10-22.

**时间用哪一个**

分桶用 `ts` (交易所成交时间), 排序用 `ts, recv_at`. 用采集时间分桶的话, 一帧
迟到的快照会把上一分钟的成交算到这一分钟.

用法:

    uv run python scripts/build_tw_intraday_bars.py                 # 建今天的
    uv run python scripts/build_tw_intraday_bars.py --date 2026-10-22
    uv run python scripts/build_tw_intraday_bars.py --all           # 库里所有日期
"""

from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import duckdb

TAIPEI = ZoneInfo("Asia/Taipei")

DEFAULT_DB = Path.home() / "data/market/market_tw_intraday.duckdb"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS bars_1m (
    trade_date DATE    NOT NULL,
    ts         TIMESTAMP NOT NULL,
    symbol     VARCHAR NOT NULL,
    market     VARCHAR NOT NULL,
    open       DOUBLE,
    high       DOUBLE,
    low        DOUBLE,
    close      DOUBLE,
    volume     BIGINT,
    amount     BIGINT,
    samples    INTEGER,
    PRIMARY KEY (trade_date, ts, symbol)
)
"""

#: 按天重算. 先算到临时表, 再按天删/插.
_BARS = """
WITH ordered AS (
    SELECT trade_date, symbol, market, ts, recv_at, last, total_vol, total_amount,
           lag(total_vol)    OVER (PARTITION BY trade_date, symbol ORDER BY recv_at) AS prev_vol,
           lag(total_amount) OVER (PARTITION BY trade_date, symbol ORDER BY recv_at) AS prev_amt
    {source}
),
deltas AS (
    SELECT *,
           -- 帧与帧之间没有成交时累计量不动, 差值 0; 第一帧没有前值, 差值就是它自己
           -- (开盘前的集合竞价成交量本来就在里面).
           COALESCE(total_vol - prev_vol, total_vol)        AS dvol,
           COALESCE(total_amount - prev_amt, total_amount)  AS damt
    FROM ordered
)
SELECT trade_date,
       date_trunc('minute', ts)                                  AS ts,
       symbol,
       market,
       first(last ORDER BY ts, recv_at)                          AS open,
       max(last)                                                 AS high,
       min(last)                                                 AS low,
       last(last ORDER BY ts, recv_at)                           AS close,
       -- 累计量回退只可能是数据故障; 把它夹到 0, 让总量仍然可比, 异常另有计数.
       -- 张 -> 股, 万元 -> 元: 与 candles 对齐.
       sum(CASE WHEN dvol > 0 THEN dvol ELSE 0 END) * 1000       AS volume,
       sum(CASE WHEN damt > 0 THEN damt ELSE 0 END) * 10000      AS amount,
       count(*)                                                  AS samples
FROM deltas
GROUP BY 1, 2, 3, 4
"""

#: 两个来源产出的列必须一模一样（trade_date, symbol, market, ts, recv_at, last,
#: total_vol, total_amount）。`ts`/`recv_at` 都是**无时区的本地时间**，与 DuckDB
#: 那份口径一致；psql 里的 epoch 毫秒在 SQL 里转成本地时间。
_SOURCE_DUCKDB = "FROM quotes WHERE trade_date = ? AND last IS NOT NULL"
_SOURCE_PSQL = """
FROM (
    SELECT (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE AS trade_date,
           (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei') AS ts,
           code AS symbol,
           market,
           (to_timestamp(recv_ms / 1000) AT TIME ZONE 'UTC') AS recv_at,
           last,
           total_vol,
           total_amount
    FROM pgp.quote_samples
    WHERE (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE = ?
      AND last IS NOT NULL
) quotes
"""

PSQL_DSN = "postgresql://postgres:postgres@localhost:5432/stock_a_spider"


def _psql_dates(connection: duckdb.DuckDBPyConnection) -> list[date]:
    rows = connection.execute(
        "SELECT DISTINCT (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE"
        " FROM pgp.quote_samples ORDER BY 1"
    ).fetchall()
    return [row[0] for row in rows]


def _bounds(day: date) -> tuple[int, int]:
    start = datetime(day.year, day.month, day.day, tzinfo=TAIPEI)
    return int(start.timestamp() * 1000), int(
        (start + timedelta(days=1)).timestamp() * 1000
    )


def _dates(connection: duckdb.DuckDBPyConnection, wanted: date | None, every: bool) -> list[date]:
    if every:
        rows = connection.execute("SELECT DISTINCT trade_date FROM quotes ORDER BY 1").fetchall()
        return [row[0] for row in rows]
    if wanted is None:
        wanted = datetime.now(TAIPEI).date()
    return [wanted]


def build(
    db: Path,
    *,
    wanted: date | None = None,
    every: bool = False,
    psql_dsn: str | None = None,
    write_duckdb: bool = True,
) -> int:
    # 采集器盘中会短暂持有 DuckDB 的写锁；只写 psql 时用内存库, 完全不碰那个文件.
    connection = duckdb.connect(str(db)) if write_duckdb else duckdb.connect()
    try:
        if write_duckdb:
            connection.execute(_SCHEMA)
        if psql_dsn:
            connection.execute(f"ATTACH '{psql_dsn}' AS pgp (TYPE POSTGRES)")
            connection.execute("SET preserve_insertion_order=false")
        days = (
            _psql_dates(connection)
            if (psql_dsn and every)
            else _dates(connection, wanted, every)
        )
        total = 0
        for day in days:
            source = _SOURCE_PSQL if psql_dsn else _SOURCE_DUCKDB
            connection.execute(
                f"CREATE OR REPLACE TEMP TABLE bars AS {_BARS.format(source=source)}",
                [day],
            )
            if write_duckdb:
                connection.execute("DELETE FROM bars_1m WHERE trade_date = ?", [day])
                connection.execute("INSERT INTO bars_1m SELECT * FROM bars")
            if psql_dsn:
                # 在线库: 先删这一天再插, 保证重跑幂等.
                start, end = _bounds(day)
                connection.execute(
                    "DELETE FROM pgp.klines WHERE market = 'tw' AND period = '1m'"
                    " AND ts >= ? AND ts < ?",
                    [start, end],
                )
                connection.execute(
                    "INSERT INTO pgp.klines"
                    " (market, code, period, ts, open, high, low, close, volume, amount)"
                    " SELECT 'tw', symbol, '1m',"
                    " epoch_ms(ts AT TIME ZONE 'Asia/Taipei'),"
                    " open, high, low, close, volume, amount FROM bars"
                )
            row = connection.execute("SELECT count(*) FROM bars").fetchone() or (0,)
            count = int(row[0])
            print(f"{day}: {count} 根 bar")
            total += count
    finally:
        connection.close()
    return total


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="台股盘中快照 -> 1 分钟 bar")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="采集库 (quotes 表)")
    parser.add_argument("--date", type=date.fromisoformat, default=None, help="建哪一天 (默认今天)")
    parser.add_argument("--all", action="store_true", help="库里的所有交易日都重建")
    parser.add_argument(
        "--psql-dsn",
        default="",
        help="在线库（读写 klines 的 tw/1m）；默认空 = 只用 DuckDB。"
        "在线库的读写由 tw_intraday_migrate.py 负责 —— 14:00 的定时构建必须走"
        "DuckDB 源，否则会拿 psql 里半天的双写数据建出残缺的 bar。",
    )
    parser.add_argument(
        "--no-duckdb",
        action="store_true",
        help="只写 psql —— 盘中采集器持锁时用",
    )
    args = parser.parse_args(argv)

    if not args.db.is_file():
        print(f"{args.db} 不存在 —— 采集器跑过吗?", file=sys.stderr)
        return 2
    total = build(
        args.db,
        wanted=args.date,
        every=args.all,
        psql_dsn=args.psql_dsn or None,
        write_duckdb=not args.no_duckdb,
    )
    if total == 0:
        # 节假日就是这个样子: 采集器没数据, 这里当然建不出 bar. 不是错误 ——
        # 采集本身有没有跑, 由 recorder 自己的告警负责, 不在这里重复喊.
        print("那天没有采集数据 (节假日?)", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
