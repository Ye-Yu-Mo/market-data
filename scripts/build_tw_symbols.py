#!/usr/bin/env python
"""生成台股的**身份旁表** `market_tw_symbols.duckdb`.

为什么需要: `market_v2.duckdb` 的 `symbols` 表是空的, 而台股的交易所不能从代码猜 ——
上市 (TWSE) 与上柜 (TPEX) 都是 4 位数字. 详见 `market_data.markets.tw` 的模块文档.

**不碰 `market_v2.duckdb`**: 它的生产归 `stock_tw`. 这里只读 stock_tw 抓下来的
FinMind `TaiwanStockInfo` 原始包, 写出一个与会话库同构的 `symbols` 表 (单独的库文件),
消费方 (factor_maker) 读它拿到 `symbol -> 交易所`.

用法:

    uv run python scripts/build_tw_symbols.py
    uv run python scripts/build_tw_symbols.py --raw <dir-or-file> --out <path.duckdb>
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import duckdb

from market_data.markets.contracts import SYMBOLS, create_table_sql
from market_data.markets.tw import build_symbols, fetch_stock_info, parse_stock_info
from market_data.offline.duckdb_store import write_rows

DEFAULT_RAW = Path.home() / "Documents/stock_tw/data/raw/raw/finmind/TaiwanStockInfo"
DEFAULT_OUT = Path.home() / "data/market/market_tw_symbols.duckdb"


def _latest_raw(directory: Path) -> Path:
    """目录里最新的 `*.raw.gz`. 取最新的那一份: 旧包里的 `type` 可能已经过时."""
    files = sorted(directory.glob("*.raw.gz"), key=lambda path: path.stat().st_mtime)
    if not files:
        raise SystemExit(f"{directory} 里没有 *.raw.gz —— 抓取的原始包在哪?")
    return files[-1]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="生成台股身份旁表 (symbols)")
    parser.add_argument("--raw", type=Path, default=DEFAULT_RAW, help="原始包文件或目录")
    parser.add_argument(
        "--api",
        action="store_true",
        help="直接从 FinMind 拉 (服务器上用; 不依赖 stock_tw 的原始抓包目录)",
    )
    parser.add_argument(
        "--token", default=None, help="FinMind token; 默认读环境变量 STOCK_TW_FINMIND_TOKEN"
    )
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT, help="输出的 DuckDB 路径")
    args = parser.parse_args(argv)

    if args.api:
        token = args.token or os.environ.get("STOCK_TW_FINMIND_TOKEN", "")
        payload = fetch_stock_info(token=token)
        source = "finmind-api"
    elif args.raw.is_file():
        payload = args.raw.read_bytes()
        source = args.raw.name
    else:
        raw = _latest_raw(args.raw)
        payload = raw.read_bytes()
        source = raw.name
    frame = build_symbols(parse_stock_info(payload))

    args.out.parent.mkdir(parents=True, exist_ok=True)
    connection = duckdb.connect(str(args.out))
    try:
        # 全量重建: 上柜转上市、下市都会让旧行变成错的, 增量 upsert 会把它们留下.
        connection.execute("DROP TABLE IF EXISTS symbols")
        connection.execute(create_table_sql(SYMBOLS, if_not_exists=True))
        write_rows(connection, SYMBOLS, frame)
        (count,) = connection.execute("SELECT count(*) FROM symbols").fetchone()  # type: ignore[misc]
    finally:
        connection.close()

    by_market = frame["market"].value_counts().sort_index().to_dict()
    print(f"{source} -> {args.out}: {count} symbols {by_market}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
