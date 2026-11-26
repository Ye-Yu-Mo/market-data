"""币安 USDT-M 永续行情采集: K 线 + 资金费率 -> `market_crypto.duckdb`.

**为什么不复用 broker 的客户端**: K 线与资金费率都是**公共接口**, 不需要签名;
采集脚本只依赖 httpx, 不和交易链路共用进程 —— 采集挂了不该影响下单.

**节流**: `/fapi/v1/klines` 在 `limit=1500` 时权重 10, 上限 2400/分钟.
这里每次请求后固定 sleep `--sleep` (默认 0.35s ≈ 2850 权重/分钟), 跑满还要靠
`X-MBX-USED-WEIGHT-1M` 兜底 —— 触顶就停 60 秒.

用法::

    python scripts/collect_crypto.py --top 30 --days-1m 90
    python scripts/collect_crypto.py --symbols BTCUSDT,ETHUSDT   # 先验两只
"""

from __future__ import annotations

import argparse
import time
from pathlib import Path

import duckdb
import httpx
import pandas as pd

BASE = "https://fapi.binance.com"
DB = Path.home() / "data" / "market" / "market_crypto.duckdb"

SCHEMA = """
CREATE TABLE IF NOT EXISTS klines (
    symbol VARCHAR NOT NULL,
    interval VARCHAR NOT NULL,
    open_time BIGINT NOT NULL,
    open DOUBLE, high DOUBLE, low DOUBLE, close DOUBLE,
    volume DOUBLE, quote_volume DOUBLE, trades BIGINT,
    taker_buy_base DOUBLE, taker_buy_quote DOUBLE,
    PRIMARY KEY (symbol, interval, open_time)
);
CREATE TABLE IF NOT EXISTS funding (
    symbol VARCHAR NOT NULL,
    funding_time BIGINT NOT NULL,
    rate DOUBLE NOT NULL,
    PRIMARY KEY (symbol, funding_time)
);
"""

INTERVAL_MS = {"1m": 60_000, "5m": 300_000, "1h": 3_600_000, "4h": 14_400_000, "1d": 86_400_000}

COLUMNS_KLINE = ["symbol", "interval", "open_time", "open", "high", "low", "close",
                 "volume", "quote_volume", "trades", "taker_buy_base", "taker_buy_quote"]
COLUMNS_FUNDING = ["symbol", "funding_time", "rate"]


def _insert(
    con: duckdb.DuckDBPyConnection,
    table: str,
    columns: list[str],
    values: list[tuple],
    *,
    keys: list[str],
) -> int:
    """**批量插入, 不要 executemany**: 逐行插入 10 万行要几分钟 (A 股分钟 K 实测).

    **冲突时更新, 不是忽略**: 增量采集从"最后一根"开始重拉, 而最后一根可能是
    还没收的分钟/日线 —— `DO NOTHING` 会把这根半截 bar 永久留下. 返回的是
    **新增行数** (更新不改行数), 所以计数不受影响.
    """
    if not values:
        return 0
    before = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    frame = pd.DataFrame(values, columns=columns)
    con.register("incoming", frame)
    updates = ", ".join(f"{name} = excluded.{name}" for name in columns if name not in keys)
    con.execute(
        f"INSERT INTO {table} SELECT * FROM incoming"
        f" ON CONFLICT ({', '.join(keys)}) DO UPDATE SET {updates}"
    )
    con.unregister("incoming")
    after = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    return int(after - before)


def top_symbols(http: httpx.Client, count: int) -> list[str]:
    """按 24h 成交额排序的 **USDT 永续**. `count <= 0` = 全部.

    只要 `contractType == PERPETUAL`: 交割合约会到期, TradFi 代币股票 (AAPL/TSLA
    这类) 跟着美股走 —— 都不是这个策略要交易的东西. 2026-10-27 实测, 主网有
    703 个 PERPETUAL + 229 个 TRADIFI_PERPETUAL, 不筛的话三分之一是股票.
    """
    info = http.get(f"{BASE}/fapi/v1/exchangeInfo").json()
    tradable = {
        row["symbol"]
        for row in info["symbols"]
        if row.get("contractType") == "PERPETUAL"
        and row.get("status") == "TRADING"
        and row.get("quoteAsset") == "USDT"
    }
    rows = http.get(f"{BASE}/fapi/v1/ticker/24hr").json()
    perps = [
        row
        for row in rows
        if row["symbol"] in tradable and float(row["quoteVolume"]) > 0
    ]
    perps.sort(key=lambda row: -float(row["quoteVolume"]))
    return [row["symbol"] for row in perps[: count or None]]


class Throttle:
    """请求节流 + **403 退避**.

    权重上限看 `X-MBX-USED-WEIGHT-1M`, 但 2026-10-27 实测还有另一种: 连续请求几百次
    之后, 同一秒内同一个 URL 返回 **403** (纯 nginx 页面, 不是 JSON 错误), 几十秒后
    自己恢复. 那不是"参数错了", 是 IP 被临时挡 —— 当错误抛出去会把整只标的放弃掉,
    而它其实只是需要等一下.
    """

    def __init__(self, http: httpx.Client, sleep: float) -> None:
        self.http = http
        self.sleep = sleep
        self.requests = 0

    def _reset(self) -> None:
        """扔掉连接池, 换一条. 403 是**按连接**的: 同一个进程复用同一条到被挡的
        边缘节点的连接, 怎么重试都是 403, 而新进程/新连接立刻就通 (2026-10-27 实测:
        回补进程连吃 30/60/90 秒三次 403, 同时另开一个进程同一 URL 200)."""
        self.http.close()
        self.http = httpx.Client(timeout=httpx.Timeout(20.0, connect=8.0))

    def get(self, path: str, params: dict[str, object]) -> list:
        for _attempt in range(3):
            response = self.http.get(f"{BASE}{path}", params=params)
            self.requests += 1
            if response.status_code in (403, 418, 429):
                wait = int(response.headers.get("Retry-After", "20"))
                print(f"  {response.status_code}: 断开连接重试 (停 {wait}s)", flush=True)
                self._reset()
                time.sleep(wait)
                continue
            used = int(response.headers.get("X-MBX-USED-WEIGHT-1M", "0") or 0)
            if used > 2100:
                print(f"  权重 {used} 接近上限, 停 60 秒", flush=True)
                time.sleep(60)
            response.raise_for_status()
            time.sleep(self.sleep)
            return response.json()
        response.raise_for_status()
        return []


def collect_klines(
    throttle: Throttle,
    con: duckdb.DuckDBPyConnection,
    symbol: str,
    interval: str,
    start_ms: int,
    end_ms: int,
) -> int:
    step = INTERVAL_MS[interval]
    cursor = start_ms
    written = 0
    while cursor < end_ms:
        rows = throttle.get(
            "/fapi/v1/klines",
            {"symbol": symbol, "interval": interval, "startTime": cursor,
             "limit": 1500},
        )
        if not rows:
            break
        values = [
            (symbol, interval, int(r[0]), float(r[1]), float(r[2]), float(r[3]),
             float(r[4]), float(r[5]), float(r[7]), int(r[8]),
             float(r[9]), float(r[10]))
            for r in rows
        ]
        written += _insert(
            con, "klines", COLUMNS_KLINE, values, keys=["symbol", "interval", "open_time"]
        )
        cursor = int(rows[-1][0]) + step
    return written


def collect_funding(
    throttle: Throttle, con: duckdb.DuckDBPyConnection, symbol: str, start_ms: int
) -> int:
    written = 0
    cursor = start_ms
    while True:
        rows = throttle.get(
            "/fapi/v1/fundingRate", {"symbol": symbol, "startTime": cursor, "limit": 1000}
        )
        if not rows:
            break
        values = [(symbol, int(r["fundingTime"]), float(r["fundingRate"])) for r in rows]
        written += _insert(
            con, "funding", COLUMNS_FUNDING, values, keys=["symbol", "funding_time"]
        )
        cursor = int(rows[-1]["fundingTime"]) + 1
        if len(rows) < 1000:
            break
    return written


def last_bar(con: duckdb.DuckDBPyConnection, symbol: str, interval: str) -> int | None:
    row = con.execute(
        "SELECT max(open_time) FROM klines WHERE symbol = ? AND interval = ?",
        [symbol, interval],
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def last_funding(con: duckdb.DuckDBPyConnection, symbol: str) -> int | None:
    row = con.execute(
        "SELECT max(funding_time) FROM funding WHERE symbol = ?", [symbol]
    ).fetchone()
    return int(row[0]) if row and row[0] is not None else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", type=Path, default=DB)
    parser.add_argument(
        "--top", type=int, default=30, help="按 24h 成交额取前 N 个 USDT 永续; 0 = 全部"
    )
    parser.add_argument("--symbols", help="逗号分隔, 覆盖 --top")
    parser.add_argument("--days-1m", type=int, default=90)
    parser.add_argument("--days-1h", type=int, default=730)
    parser.add_argument("--days-1d", type=int, default=3000)
    parser.add_argument("--sleep", type=float, default=0.35)
    args = parser.parse_args(argv)

    args.db.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(args.db))
    con.execute(SCHEMA)
    # **老库要补列**: 主动买入量是后加的, 已有库没有这两列; 采集口径必须一致,
    # 否则同一张表里有的行有 taker 数据、有的没有.
    for column in ("taker_buy_base", "taker_buy_quote"):
        con.execute(f"ALTER TABLE klines ADD COLUMN IF NOT EXISTS {column} DOUBLE")

    http = httpx.Client(timeout=httpx.Timeout(20.0, connect=8.0))
    throttle = Throttle(http, args.sleep)
    symbols = (
        [s.strip() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else top_symbols(http, args.top)
    )
    print(f"标的 {len(symbols)} 个: {symbols[:6]}{'...' if len(symbols) > 6 else ''}")

    now = int(time.time() * 1000)
    day = 86_400_000
    total = 0
    started = time.time()
    for index, symbol in enumerate(symbols, 1):
        stats = []
        try:
            # **增量**: 每档从"库里最后一根"开始重拉, 而不是每次把整段历史再下
            # 一遍. 500 只标的全量重拉要 7 小时, 增量只要十几分钟 —— 日更能不能
            # 每天跑, 就取决于这一句. 从最后一根 (不是 +1) 开始, 是为了让还没收
            # 的那根半截 bar 被 upsert 修正.
            for interval, days in (
                ("1d", args.days_1d),
                ("1h", args.days_1h),
                ("1m", args.days_1m),
            ):
                base = now - days * day
                last = last_bar(con, symbol, interval)
                start = max(base, last) if last is not None else base
                stats.append(
                    (interval, collect_klines(throttle, con, symbol, interval, start, now))
                )
            funding_base = now - args.days_1h * day
            funding_last = last_funding(con, symbol)
            funding_start = (
                max(funding_base, funding_last + 1) if funding_last is not None else funding_base
            )
            stats.append(("funding", collect_funding(throttle, con, symbol, funding_start)))
        except httpx.HTTPError as exc:
            print(f"  [{index}/{len(symbols)}] {symbol} 失败: {exc}", flush=True)
            continue
        total += sum(count for _, count in stats)
        print(
            f"  [{index}/{len(symbols)}] {symbol}: "
            + " ".join(f"{name}+{count:,}" for name, count in stats)
            + f"  请求 {throttle.requests}  用时 {time.time() - started:.0f}s",
            flush=True,
        )

    print(f"完成: 新增 {total:,} 行, 请求 {throttle.requests}, {time.time() - started:.0f}s")
    print("库内:", con.execute(
        "SELECT interval, count(*), count(DISTINCT symbol) FROM klines GROUP BY 1 ORDER BY 1"
    ).fetchall())
    con.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
