#!/usr/bin/env python3
"""把信号数据导出成 parquet，按日期分区.

- factor_maker 每轮的目标/委托/评分（journal.db 的 runs 表）
- 双周轮动的 history.jsonl + latest.json

事件合约的信号历史在 xiaoxin，由那边的任务导出（同一套布局）。

    archive_signals.py
"""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

import duckdb
import pandas as pd

ROOT = Path("/home/jas/data/archive")
JOURNAL = Path("/home/jas/data/factor_maker/journal.db")
ROTATION = Path("/home/jas/data/rotation")


def _write(con: duckdb.DuckDBPyConnection, frame: pd.DataFrame, target: Path) -> None:
    con.register("frame", frame)
    target.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"COPY (SELECT * FROM frame) TO '{target}'"
        " (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE)"
    )
    con.unregister("frame")


def export_runs(con: duckdb.DuckDBPyConnection) -> int:
    if not JOURNAL.is_file():
        return 0
    connection = sqlite3.connect(f"file:{JOURNAL}?mode=ro", uri=True)
    try:
        cursor = connection.execute("SELECT * FROM runs")
        columns = [item[0] for item in cursor.description]
        rows = cursor.fetchall()
    finally:
        connection.close()
    if not rows:
        return 0
    frame = pd.DataFrame(rows, columns=columns)
    frame["date"] = pd.to_datetime(frame["trade_date"], format="%Y%m%d").dt.date
    _write(con, frame, ROOT / "signals" / "factor_maker" / "runs")
    return len(frame)


def export_rotation(con: duckdb.DuckDBPyConnection) -> int:
    records: dict[str, dict] = {}
    history = ROTATION / "history.jsonl"
    if history.is_file():
        for line in history.read_text(encoding="utf-8").splitlines():
            if line.strip():
                row = json.loads(line)
                records[str(row.get("generated"))] = row
    latest = ROTATION / "latest.json"
    if latest.is_file():
        row = json.loads(latest.read_text(encoding="utf-8"))
        records[str(row.get("generated"))] = row
    if not records:
        return 0
    frame = pd.DataFrame([records[key] for key in sorted(records)])
    for column in ("picks", "ranking", "metrics", "stops", "halves"):
        if column in frame.columns:
            frame[column] = frame[column].map(
                lambda value: json.dumps(value, ensure_ascii=False)
            )
    frame["date"] = frame["generated"]
    _write(con, frame, ROOT / "signals" / "rotation")
    return len(frame)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=ROOT)
    args = parser.parse_args(argv)
    con = duckdb.connect()
    try:
        runs = export_runs(con)
        rotation = export_rotation(con)
    except (duckdb.Error, ValueError, OSError) as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()
    print(f"runs={runs} rotation={rotation} -> {args.root}/signals")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
