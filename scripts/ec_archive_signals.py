#!/usr/bin/env python3
"""事件合约信号历史 -> parquet（按日期分区），xiaoxin 本地执行.

    signals/event/<line>/date=YYYY-MM-DD/data_0.parquet

之后由 sync 推到 leaf 的归档目录。只读 JSONL，不动服务。
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

MARKET = Path("/home/jas/data/market")
OUT = Path("/home/jas/data/archive/signals/event")
LINES = {
    "bq10": MARKET / "ec_signals_bq10.jsonl",
    "bq": MARKET / "ec_signals_bq.jsonl",
}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args()
    con = duckdb.connect()
    try:
        for line, path in LINES.items():
            if not path.is_file() or path.stat().st_size == 0:
                print(f"{line}: 没有 {path}")
                continue
            target = args.out / line
            target.mkdir(parents=True, exist_ok=True)
            con.execute("SET preserve_insertion_order=false")
            con.execute(
                f"""
                COPY (
                    SELECT *, (to_timestamp(ts / 1000) AT TIME ZONE 'Asia/Taipei')::DATE AS date
                    FROM read_json_auto('{path}', union_by_name=true, sample_size=-1)
                    WHERE ts IS NOT NULL
                ) TO '{target}' (FORMAT PARQUET, PARTITION_BY (date), OVERWRITE_OR_IGNORE)
                """
            )
            print(f"{line}: {path.stat().st_size / 1e6:.1f}MB -> {target}")
    finally:
        con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
