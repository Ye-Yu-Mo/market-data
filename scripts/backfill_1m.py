"""把本地源的**分钟 K** 回填到独立的研究库.

**为什么单独一个库**: 分钟线是 1,700 万行量级, 混进 `market_a` 会把日线的读写和
备份都拖慢; 研究侧只需要它, 生产侧不需要.

**为什么一只一只取**: 2026-10-23 用"一整年 x 全市场"的查询把 stockdb 打到 15GB
(OOM), 机器 IO 卡死半小时. 这里的每次请求都是**单标的**, 源返回该标的全部可得
分钟线 (2025-01-02 起, 约 10 万根 / 11MB), 不会无界扫描.

用法::

    python scripts/backfill_1m.py --symbols-file /tmp/psql_symbols.txt
    python scripts/backfill_1m.py --symbols-file ... --limit 1   # 先验一只
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb
import pandas as pd

from market_data.markets.cn.free_stockdb import FreeStockDbAdapter, Query, _decode

DEFAULT_DB = Path.home() / "data" / "research" / "market_a_1m.duckdb"
SCHEMA = """
CREATE TABLE IF NOT EXISTS bars_1m (
    code VARCHAR NOT NULL,
    ts BIGINT NOT NULL,
    open DOUBLE,
    high DOUBLE,
    low DOUBLE,
    close DOUBLE,
    volume DOUBLE,
    amount DOUBLE,
    PRIMARY KEY (code, ts)
)
"""


def write(con: duckdb.DuckDBPyConnection, records: list[dict[str, object]], code: str) -> int:
    rows = []
    for item in records:
        if item.get("code") != code:
            continue
        # 源里偶尔有整根空 bar (停牌/集合竞价的占位). 价格是空的就不是一根 bar,
        # 宁可丢这一行, 也不要把它写成 0 —— 0 会被下游当成"跌到 0"。
        prices = (item.get("open"), item.get("high"), item.get("low"), item.get("close"))
        if any(value is None for value in prices):
            continue
        rows.append(
            (
                str(item["code"]),
                int(item["date"]),  # YYYYMMDDhhmmss
                float(item["open"]),
                float(item["high"]),
                float(item["low"]),
                float(item["close"]),
                float(item.get("volume") or 0.0),
                float(item.get("amount") or 0.0),
            )
        )
    if not rows:
        return 0
    # **不要用 executemany**: 10 万行走逐行插入 + 主键索引, 一只标的要 7 分钟
    # (2026-10-26 实测, 进程一直挂在 D 状态). 走 DataFrame 批量插入是 0.5 秒.
    before = con.execute("SELECT count(*) FROM bars_1m").fetchone()[0]
    frame = pd.DataFrame(
        rows, columns=["code", "ts", "open", "high", "low", "close", "volume", "amount"]
    )
    con.register("incoming", frame)
    con.execute("INSERT INTO bars_1m SELECT * FROM incoming ON CONFLICT DO NOTHING")
    con.unregister("incoming")
    after = con.execute("SELECT count(*) FROM bars_1m").fetchone()[0]
    return int(after - before)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--symbols-file", type=Path, required=True)
    parser.add_argument("--db", type=Path, default=DEFAULT_DB)
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=0.3)
    args = parser.parse_args(argv)

    codes = [
        line.split(",")[0].strip()
        for line in args.symbols_file.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    if args.limit:
        codes = codes[: args.limit]

    args.db.parent.mkdir(parents=True, exist_ok=True)
    adapter = FreeStockDbAdapter("http://127.0.0.1:7899", timeout=60)
    con = duckdb.connect(str(args.db))
    con.execute(SCHEMA)

    started = time.time()
    total = 0
    for index, code in enumerate(codes, 1):
        try:
            raw = adapter.fetch(Query("分钟k", f"key:{code}", cmd="vals"))
            records = _decode(raw)
        except Exception as exc:
            print(f"  [{index}/{len(codes)}] {code} 失败: {type(exc).__name__}: {exc}", flush=True)
            continue
        if not isinstance(records, list):
            print(f"  [{index}/{len(codes)}] {code} 返回不是数组", flush=True)
            continue
        written = write(con, records, code)
        total += written
        if index % 10 == 0 or written == 0:
            print(
                f"  [{index}/{len(codes)}] {code}: {len(records):,} 条, 新增 {written:,},"
                f" 累计 {total:,}, 用时 {time.time() - started:.0f}s",
                flush=True,
            )
        time.sleep(args.sleep)

    print(f"完成: {len(codes)} 只, 新增 {total:,} 行, {time.time() - started:.0f}s -> {args.db}")
    print("库内:", con.execute("SELECT count(*), count(DISTINCT code) FROM bars_1m").fetchone())
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
