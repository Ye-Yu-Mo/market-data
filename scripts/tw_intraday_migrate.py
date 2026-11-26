#!/usr/bin/env python3
"""把 DuckDB 的台股盘中快照搬进 psql，重建 1m bar，并与 DuckDB 的 bar 逐值对比.

收盘后跑（采集器已退出、14:00 的 DuckDB bar 已构建）:

    tw_intraday_migrate.py            # 全部日期
    tw_intraday_migrate.py --date 2026-11-17

结果写 `~/data/market/tw_intraday_migrate.json`：每天两边条数、匹配数、
最大价格/成交量差。**只有某天 matched == 两边条数且 max diff == 0，才算对齐。**
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent))
from build_tw_intraday_bars import PSQL_DSN, build, _bounds  # noqa: E402

DUCK = Path.home() / "data/market/market_tw_intraday.duckdb"
OUT = Path.home() / "data/market/tw_intraday_migrate.json"

_COPY = """
INSERT INTO pgp.quote_samples
    (market, code, ts, recv_ms, last, open, high, low, buy, sell,
     bid_qty, ask_qty, total_vol, total_amount, prev_close)
SELECT 'tw', symbol,
       epoch_ms(ts AT TIME ZONE 'Asia/Taipei'),
       epoch_ms(recv_at),
       last, open, high, low, buy, sell,
       bid_qty, ask_qty, total_vol, total_amount, prev_close
FROM q.quotes
WHERE trade_date = ?
"""

_COMPARE = """
WITH p AS (
    SELECT code, ts, open, high, low, close, volume
    FROM pgp.klines
    WHERE market = 'tw' AND period = '1m' AND ts >= ? AND ts < ?
),
d AS (
    SELECT symbol AS code,
           epoch_ms(ts AT TIME ZONE 'Asia/Taipei') AS ts,
           open, high, low, close, volume
    FROM q.bars_1m
    WHERE trade_date = ?
)
SELECT
    (SELECT count(*) FROM p),
    (SELECT count(*) FROM d),
    count(*),
    coalesce(max(abs(p.open - d.open)), 0),
    coalesce(max(abs(p.high - d.high)), 0),
    coalesce(max(abs(p.low - d.low)), 0),
    coalesce(max(abs(p.close - d.close)), 0),
    coalesce(max(abs(p.volume - d.volume)), 0)
FROM p JOIN d USING (code, ts)
"""


def migrate(con: duckdb.DuckDBPyConnection, dsn: str, days: list[date]) -> list[dict]:
    report: list[dict] = []
    for day in days:
        start, end = _bounds(day)
        con.execute(
            "DELETE FROM pgp.quote_samples WHERE ts >= ? AND ts < ?",
            [start, end],
        )
        con.execute(_COPY, [day])
        build(DUCK, wanted=day, psql_dsn=dsn, write_duckdb=False)
        psql_n, duck_n, matched, d_open, d_high, d_low, d_close, d_vol = con.execute(
            _COMPARE, [start, end, day]
        ).fetchone()
        row = {
            "date": str(day),
            "psql_bars": int(psql_n),
            "duckdb_bars": int(duck_n),
            "matched": int(matched),
            "max_open_diff": float(d_open),
            "max_high_diff": float(d_high),
            "max_low_diff": float(d_low),
            "max_close_diff": float(d_close),
            "max_volume_diff": float(d_vol),
        }
        row["aligned"] = (
            row["psql_bars"] == row["duckdb_bars"] == row["matched"]
            and max(row["max_open_diff"], row["max_high_diff"], row["max_low_diff"],
                    row["max_close_diff"], row["max_volume_diff"]) == 0
        )
        report.append(row)
        print(
            f"{day}: psql={row['psql_bars']} duck={row['duckdb_bars']} "
            f"matched={row['matched']} max_close={row['max_close_diff']:.6f} "
            f"max_vol={row['max_volume_diff']:.0f} aligned={row['aligned']}"
        )
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--date", type=date.fromisoformat, default=None)
    parser.add_argument("--dsn", default=PSQL_DSN)
    parser.add_argument("--out", type=Path, default=OUT)
    args = parser.parse_args(argv)

    con = duckdb.connect()
    try:
        con.execute(f"ATTACH '{DUCK}' AS q (READ_ONLY)")
        con.execute(f"ATTACH '{args.dsn}' AS pgp (TYPE POSTGRES)")
        con.execute("SET preserve_insertion_order=false")
        if args.date:
            days = [args.date]
        else:
            days = [
                row[0]
                for row in con.execute(
                    "SELECT DISTINCT trade_date FROM q.quotes ORDER BY 1"
                ).fetchall()
            ]
        report = migrate(con, args.dsn, days)
    except duckdb.Error as exc:
        print(f"失败: {exc}", file=sys.stderr)
        return 1
    finally:
        con.close()

    args.out.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    bad = [row["date"] for row in report if not row["aligned"]]
    print(f"→ {args.out}；未对齐: {bad if bad else '无'}")
    return 0 if not bad else 1


if __name__ == "__main__":
    raise SystemExit(main())
