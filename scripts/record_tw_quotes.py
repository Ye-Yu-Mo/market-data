#!/usr/bin/env python
"""台股**盘中行情采集器**: 把券商服务端的实时快照按固定间隔落进 `market_tw_intraday.duckdb`.

**为什么要有它**

台股没有可回补的历史分钟线 (券商只允许只读的 `snapshot`, `kline`/`ticks` 都被拒),
而日内因子、做 T 的信号**只能从现在开始往前攒** —— 今天不录, 今天这一段就永远没有.
所以它跟日更一样属于"每天必须跑"的东西, 而不是研究阶段的临时脚本.

**为什么不写进 `market_v2.duckdb`**

那个库的生产归 `stock_tw` (`market_data/PLAN.md`: 一个字节都不动). 这里另开一个库,
只追加, 谁写谁读都清楚.

**为什么是快照而不是逐笔**

一次调用能覆盖上百只 (`GetWatchListAll` 收一个代码列表), 15 秒一轮两只市场两笔请求.
逐笔要一只一只查, 而且券商对 tick 的态度是明确拒绝的.

**单位是券商自己的, 原样存**

快照里 `total_vol` 的单位是**张** (1000 股), `total_deal_amt` 是**万元** ——
与 `market_v2.candles` 的 (股, 元) 不是一套. 这张表**原样**存券商给的值,
换算 (x1000 / x10000) 放在 `build_tw_intraday_bars.py` 里做一次, 两个地方
各说各的单位.

**指数拿不到**

`TAIEX` / `TPEx` 这两个代码在 `GetWatchListAll` 里返回空 (两个市场都试过,
2026-10-22), 所以盘中指数的基准序列这条路走不通 —— 要基准就用成份股自己算,
或者另找源.

**累计量是重点**

`total_vol` / `total_amount` 是**当日累计**, 不是区间量. 相邻两次快照相减就是这段时间
的成交量 —— 1 分钟 bar 由此重建. 只存当前价的话, 这些信息就丢了.

**两个时间戳, 主键是采集时间**

`ts` 是快照里自带的**成交时间**, 不动的标的会停在上一笔成交那一刻 (实测 10:27 采集时
有标的停在 10:17). `recv_at` 是采集时间. 主键用 `recv_at`: 盘口 (买卖价与挂单量) 在
没有成交的时候照样在变, 按 `ts` 去重会把这些帧全丢掉. 要建 bar 就按 `ts` 归桶,
要研究价差就用 `recv_at`.

用法:

    uv run python scripts/record_tw_quotes.py --once          # 采一轮看看
    uv run python scripts/record_tw_quotes.py                 # 跑完整个交易日 (crontab 每分钟拉起)
    uv run python scripts/record_tw_quotes.py --top 50 --interval 10
"""

from __future__ import annotations

import argparse
import fcntl
import json
import os
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Mapping, Sequence
from datetime import datetime
from pathlib import Path
from typing import Any, TextIO, cast
from zoneinfo import ZoneInfo

import duckdb

from market_data.offline.alerts import notify

TAIPEI = ZoneInfo("Asia/Taipei")

DEFAULT_DB = Path.home() / "data/market/market_tw_intraday.duckdb"
DEFAULT_HISTORY = Path.home() / "data/market/market_v2.duckdb"
DEFAULT_IDENTITY = Path.home() / "data/market/market_tw_symbols.duckdb"
DEFAULT_LOCK = Path.home() / "data/market/.record_tw_quotes.lock"

#: 券商服务端的市场名与本地身份表里的交易所**不是同一套写法**:
#: 上柜在券商那边叫 `TWOTC`, 兴柜叫 `TWEMERGING`. 混用会让请求静默返回空列表.
VENDOR_MARKET = {"TWSE": "TWSE", "TPEX": "TWOTC"}

#: 只录连续竞价的市场. 兴柜 (ESB) 是议价交易, 快照没有意义.
RECORDED_MARKETS = ("TWSE", "TPEX")

#: 收盘集合竞价在 13:30-13:33, 多录几分钟是为了把收盘价的那一帧带上.
SESSION_OPEN = (8, 58)
SESSION_CLOSE = (13, 36)

#: 开盘集合竞价的几分钟里没有成交是正常的; 09:05 之后还连续写 0 行就是链路坏了.
#: 11-12 → 11-16 券商快照只剩收盘价, 每行都缺交易所时间被丢掉, 日志却一片安静.
_ZERO_ALERT_FROM = (9, 5)
_ZERO_ROUNDS_ALERT = 20

_SNAPSHOT_PATH = "/api/v1/quotes/snapshot"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS quotes (
    trade_date   DATE    NOT NULL,
    ts           TIMESTAMP NOT NULL,
    recv_at      TIMESTAMP NOT NULL,
    symbol       VARCHAR NOT NULL,
    market       VARCHAR NOT NULL,
    last         DOUBLE,
    open         DOUBLE,
    high         DOUBLE,
    low          DOUBLE,
    buy          DOUBLE,
    sell         DOUBLE,
    bid_qty      BIGINT,
    ask_qty      BIGINT,
    total_vol    BIGINT,
    total_amount BIGINT,
    prev_close   DOUBLE,
    PRIMARY KEY (trade_date, recv_at, symbol)
)
"""

_COLUMNS = (
    "trade_date",
    "ts",
    "recv_at",
    "symbol",
    "market",
    "last",
    "open",
    "high",
    "low",
    "buy",
    "sell",
    "bid_qty",
    "ask_qty",
    "total_vol",
    "total_amount",
    "prev_close",
)

#: 在线库（psql）里的同一份快照. 单文件 DuckDB 只留作过渡, 最终以这张表为准.
#: PK 带上 `recv_ms`: 同一根交易所时间会被采到多次, 每一帧都要留.
_PSQL_SCHEMA = """
CREATE TABLE IF NOT EXISTS quote_samples (
    market       TEXT NOT NULL,
    code         TEXT NOT NULL,
    ts           BIGINT NOT NULL,
    recv_ms      BIGINT NOT NULL,
    last         DOUBLE PRECISION,
    open         DOUBLE PRECISION,
    high         DOUBLE PRECISION,
    low          DOUBLE PRECISION,
    buy          DOUBLE PRECISION,
    sell         DOUBLE PRECISION,
    bid_qty      BIGINT,
    ask_qty      BIGINT,
    total_vol    BIGINT,
    total_amount BIGINT,
    prev_close   DOUBLE PRECISION,
    PRIMARY KEY (market, code, ts, recv_ms)
)
"""

PSQL_DSN = os.environ.get(
    "RECORDER_PSQL_DSN",
    "postgresql://postgres:postgres@localhost:5432/stock_a_spider",
)


def _literal(value: object) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return repr(value)
    return "'" + str(value).replace("'", "''") + "'"


def _ms(stamp: datetime, zone: ZoneInfo) -> int:
    return int(stamp.replace(tzinfo=zone).timestamp() * 1000)


class PsqlSink:
    """把每一批快照写进 psql 的 `quote_samples`（在线库, 多进程并发）."""

    def __init__(self, dsn: str = PSQL_DSN) -> None:
        self._con = duckdb.connect()
        self._con.execute(f"ATTACH '{dsn}' AS pgp (TYPE POSTGRES)")
        self._run(_PSQL_SCHEMA)

    def _run(self, sql: str) -> list[tuple[object, ...]]:
        # **必须 fetchall**: DuckDB 的 SELECT 是惰性的, 不取结果就什么都不执行.
        return self._con.execute(
            "SELECT * FROM postgres_execute('pgp', ?)", [sql]
        ).fetchall()

    def write(self, rows: list[tuple[object, ...]]) -> int:
        if not rows:
            return 0
        values: list[str] = []
        for row in rows:
            trade_date, ts, recv_at, symbol, _market, last, open_v, high, low = row[:9]
            buy, sell, bid_qty, ask_qty, total_vol, total_amount, prev_close = row[9:]
            values.append(
                "("
                + ",".join(
                    [
                        _literal("tw"),
                        _literal(symbol),
                        str(_ms(ts, TAIPEI)),
                        str(_ms(recv_at, ZoneInfo("UTC"))),
                        _literal(last),
                        _literal(open_v),
                        _literal(high),
                        _literal(low),
                        _literal(buy),
                        _literal(sell),
                        _literal(bid_qty),
                        _literal(ask_qty),
                        _literal(total_vol),
                        _literal(total_amount),
                        _literal(prev_close),
                    ]
                )
                + ")"
            )
        self._run(
            "INSERT INTO quote_samples"
            " (market, code, ts, recv_ms, last, open, high, low, buy, sell,"
            "  bid_qty, ask_qty, total_vol, total_amount, prev_close)"
            " VALUES " + ",".join(values) + " ON CONFLICT DO NOTHING"
        )
        return len(rows)

    def close(self) -> None:
        self._con.close()


class RecorderError(RuntimeError):
    """采集器自己的错误 —— 与"这一轮没采到"区分开, 后者只记日志."""


def _number(item: Mapping[str, Any], key: str) -> float | None:
    value = item.get(key)
    if isinstance(value, bool) or value is None:
        return None
    if isinstance(value, (int, float)):
        # 券商把"没有这一档"写成 0, 而不是缺字段. 0 价的买一是**不存在**, 不是"0 元".
        return float(value) if value != 0 else None
    return None


def _quantity(item: Mapping[str, Any], key: str) -> int | None:
    value = item.get(key)
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return int(value)
    return None


def _exchange_time(item: Mapping[str, Any]) -> datetime | None:
    """快照里自带的**交易所时间**. 用它当主键的一部分, 而不是本地时钟.

    本地时钟会漂, 而且重采同一秒时会造出一条看起来像新数据的行.
    """
    raw = item.get("time")
    if not isinstance(raw, dict):
        return None
    stamp = cast("Mapping[str, Any]", raw)
    try:
        return datetime(
            int(stamp["year"]),
            int(stamp["month"]),
            int(stamp["day"]),
            int(stamp["hour"]),
            int(stamp["minute"]),
            int(stamp["second"]),
            tzinfo=TAIPEI,
        )
    except (KeyError, TypeError, ValueError):
        return None


def _row(item: Mapping[str, Any], *, market: str, now: datetime) -> tuple[object, ...] | None:
    symbol = str(item.get("stk_code") or "").strip()
    stamp = _exchange_time(item)
    if not symbol or stamp is None:
        return None
    return (
        stamp.date(),
        stamp.replace(tzinfo=None),
        now.astimezone(ZoneInfo("UTC")).replace(tzinfo=None),
        symbol,
        market,
        _number(item, "deal_price"),
        _number(item, "open_price"),
        _number(item, "high_price"),
        _number(item, "low_price"),
        _number(item, "buy_price"),
        _number(item, "sell_price"),
        _quantity(item, "bid_qty"),
        _quantity(item, "ask_qty"),
        _quantity(item, "total_vol"),
        _quantity(item, "total_deal_amt"),
        _number(item, "yst_price"),
    )


def build_watchlist(
    *, history: Path, identity: Path, top: int
) -> dict[str, tuple[str, ...]]:
    """每个市场按**最近一个交易日的成交额**取前 `top` 只.

    为什么按成交额: 采集的预算是固定的, 钱流向哪里, 日内信号就在哪里. 按代码顺序取
    前 N 只, 采到的会是一堆几乎不动的标的.

    为什么交易所取自身份表: 台股的上市/上柜都是 4 位数字, 从代码猜不出来
    (见 `market_data.markets.tw`), 而券商要求按市场分别请求.
    """
    connection = duckdb.connect(str(history), read_only=True)
    try:
        row = connection.execute("SELECT max(date) FROM candles").fetchone() or (None,)
        latest = row[0]
        if latest is None:
            raise RecorderError(f"{history} 里一根 bar 都没有 —— 先跑日更")
        rows = connection.execute(
            "SELECT symbol, sum(amount) AS amount FROM candles WHERE date = ?"
            " AND symbol <> '' GROUP BY symbol ORDER BY amount DESC",
            [latest],
        ).fetchall()
    finally:
        connection.close()

    connection = duckdb.connect(str(identity), read_only=True)
    try:
        markets = {
            str(symbol): str(market).upper()
            for symbol, market in connection.execute("SELECT symbol, market FROM symbols").fetchall()
        }
    finally:
        connection.close()

    picked: dict[str, list[str]] = {market: [] for market in RECORDED_MARKETS}
    for symbol, _amount in rows:
        market = markets.get(str(symbol))
        if market in picked and len(picked[market]) < top:
            picked[market].append(str(symbol))
    result = {market: tuple(symbols) for market, symbols in picked.items() if symbols}
    if not result:
        raise RecorderError("身份表与历史库没有交集 —— 身份表重建过吗?")
    return result


def fetch_snapshot(
    *, base_url: str, token: str, symbols: Sequence[str], market: str, timeout: float
) -> list[Mapping[str, Any]]:
    url = (
        f"{base_url.rstrip('/')}{_SNAPSHOT_PATH}"
        f"?symbols={','.join(symbols)}&market_type={VENDOR_MARKET[market]}"
    )
    request = urllib.request.Request(url, headers={"Authorization": f"Bearer {token}"})
    with urllib.request.urlopen(request, timeout=timeout) as response:
        payload: Any = json.load(response)
    data = payload.get("data")
    if not isinstance(data, dict):
        raise RecorderError(f"响应里没有 data: {str(payload)[:200]}")
    envelope = cast("Mapping[str, Any]", data)
    items = envelope.get("query_watch_list")
    if not isinstance(items, list):
        raise RecorderError(f"响应里没有 query_watch_list: {str(payload)[:200]}")
    return [cast("Mapping[str, Any]", item) for item in cast("list[object]", items) if isinstance(item, dict)]


def _flush(db: Path, rows: list[tuple[object, ...]]) -> int:
    """把这一批写进去, 返回**真正新增**的行数.

    每次开关一次库, 但**盘中这个文件基本是写者独占的**: DuckDB 的写锁在持锁期间
    会挡住只读连接, 实测盘中读十次有八次拿到 "Conflicting lock" (2026-10-22).
    这不是 bug, 是取舍: 采集要连续跑四个半小时, 而研究侧真正需要这些数据是在收盘
    之后 —— 那时采集器已经退出. 要在盘中读, 就别读这个库, 直接问券商服务端.

    `ON CONFLICT DO NOTHING` 是必须的: 同一秒被采两次 (或者节假日返回上一交易日的
    时间戳) 会撞主键, 那不是错误, 是重复数据.
    """
    if not rows:
        return 0
    connection = duckdb.connect(str(db))
    try:
        connection.execute(_SCHEMA)
        before = connection.execute("SELECT count(*) FROM quotes").fetchone()
        connection.executemany(
            f"INSERT INTO quotes ({', '.join(_COLUMNS)}) VALUES ({', '.join('?' * len(_COLUMNS))})"
            " ON CONFLICT DO NOTHING",
            rows,
        )
        after = connection.execute("SELECT count(*) FROM quotes").fetchone()
    finally:
        connection.close()
    return int(after[0]) - int(before[0]) if before and after else 0


def _alert(message: str, webhook: str) -> None:
    """一条告警. **发不出去不改退出码** —— 与 factor_maker 同样的理由.

    走卡片: 飞书的纯文本不带发送时间, 而"采集 09:21 断的"这个信息只在卡片上.
    """
    notify(webhook, title="台股盘中采集中断", content=message, severity="error",
           service="tw-intraday")


def _in_session(now: datetime) -> bool:
    if now.weekday() >= 5:
        return False
    start = now.replace(hour=SESSION_OPEN[0], minute=SESSION_OPEN[1], second=0, microsecond=0)
    close = now.replace(hour=SESSION_CLOSE[0], minute=SESSION_CLOSE[1], second=0, microsecond=0)
    return start <= now <= close


def _acquire_lock(path: Path) -> TextIO:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle = path.open("w")
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        handle.close()
        raise RecorderError(f"已经有一个采集器在跑 ({path})") from None
    return handle


def _poll_once(
    *,
    watchlist: dict[str, tuple[str, ...]],
    base_url: str,
    token: str,
    db: Path | None,
    timeout: float,
    psql: PsqlSink | None = None,
) -> tuple[int, list[str]]:
    now = datetime.now(TAIPEI)
    rows: list[tuple[object, ...]] = []
    failures: list[str] = []
    for market, symbols in watchlist.items():
        try:
            items = fetch_snapshot(
                base_url=base_url, token=token, symbols=symbols, market=market, timeout=timeout
            )
        except (urllib.error.URLError, TimeoutError, OSError, RecorderError, json.JSONDecodeError) as exc:
            failures.append(f"{market}: {exc}")
            continue
        for item in items:
            row = _row(item, market=market, now=now)
            if row is not None:
                rows.append(row)
    written = 0
    if db is not None:
        try:
            written = _flush(db, rows)
        except duckdb.Error as exc:
            # 研究侧正开着这个库 (DuckDB 的写锁是进程级的). 丢一批比让整个交易日停掉好,
            # 下一轮 15 秒后还会再来.
            return 0, [*failures, f"写库失败: {exc}"]
    if psql is not None:
        try:
            psql.write(rows)
        except duckdb.Error as exc:
            failures.append(f"psql 写失败: {exc}")
    return written, failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="台股盘中行情采集 (券商快照 -> DuckDB)")
    parser.add_argument("--db", type=Path, default=DEFAULT_DB, help="输出库")
    parser.add_argument("--history-db", type=Path, default=DEFAULT_HISTORY, help="取成交额排序")
    parser.add_argument("--identity-db", type=Path, default=DEFAULT_IDENTITY, help="symbol -> 交易所")
    parser.add_argument("--top", type=int, default=100, help="每个市场采多少只 (按成交额)")
    parser.add_argument("--interval", type=float, default=15.0, help="两次采样的间隔 (秒)")
    parser.add_argument("--timeout", type=float, default=20.0, help="单次请求超时 (秒)")
    parser.add_argument("--once", action="store_true", help="只采一次就退 (验证链路)")
    parser.add_argument(
        "--psql-dsn",
        default=PSQL_DSN,
        help="在线库 DSN（quote_samples）。空字符串 = 不写 psql",
    )
    parser.add_argument(
        "--no-duckdb", action="store_true", help="只写 psql —— 过渡期验证用"
    )
    parser.add_argument("--lock", type=Path, default=DEFAULT_LOCK, help="防并发的锁文件")
    parser.add_argument(
        "--broker-url",
        default=os.environ.get("FACTOR_MAKER_BROKER_URL", "http://127.0.0.1:18000"),
        help="券商服务端地址 (leaf 上是隧道端口)",
    )
    parser.add_argument(
        "--token", default=os.environ.get("FACTOR_MAKER_BROKER_TOKEN", ""), help="Bearer token"
    )
    parser.add_argument(
        "--webhook", default=os.environ.get("LARK_WEBHOOK", ""), help="失败告警 (可选)"
    )
    args = parser.parse_args(argv)

    if not args.token:
        print("缺 token: 用 --token 或 FACTOR_MAKER_BROKER_TOKEN", file=sys.stderr)
        return 2

    psql: PsqlSink | None = None
    if args.psql_dsn:
        try:
            psql = PsqlSink(args.psql_dsn)
        except duckdb.Error as exc:
            # psql 挂了不能把采集整个停掉: DuckDB 那份还在写, 收盘后能补.
            print(f"psql 不可用, 本轮只写 DuckDB: {exc}", file=sys.stderr)
    db = None if args.no_duckdb else args.db

    now = datetime.now(TAIPEI)
    # 非交易时段**不吭声**: crontab 每分钟拉起一次, 每个非交易时段的小时都打一行
    # "不在时段" 只会把日志淹掉. 真正的开始与结束各有一行.
    if not args.once and not _in_session(now):
        return 0

    if args.once:
        lock = None
    else:
        try:
            lock = _acquire_lock(args.lock)
        except RecorderError:
            # 已经有一个在跑 —— 这是 crontab 每分钟拉起时的正常情况, 不是错误.
            return 0
    try:
        watchlist = build_watchlist(
            history=args.history_db, identity=args.identity_db, top=args.top
        )
        counts = ", ".join(f"{market} {len(symbols)} 只" for market, symbols in watchlist.items())
        print(f"{now:%F %T} 开始采集: {counts}; 间隔 {args.interval}s -> {args.db}")
        if args.once:
            written, failures = _poll_once(
                watchlist=watchlist,
                base_url=args.broker_url,
                token=args.token,
                db=db,
                timeout=args.timeout,
                psql=psql,
            )
            print(f"写入 {written} 行" + (f"; 失败 {failures}" if failures else ""))
            return 0 if not failures else 1

        written = 0
        failures = 0
        zeros = 0
        while _in_session(datetime.now(TAIPEI)):
            started = time.monotonic()
            rows, errors = _poll_once(
                watchlist=watchlist,
                base_url=args.broker_url,
                token=args.token,
                db=db,
                timeout=args.timeout,
                psql=psql,
            )
            written += rows
            if errors:
                failures += 1
                print(f"{datetime.now(TAIPEI):%F %T} 采集中断: {errors}", file=sys.stderr)
                # 告警里带上**原始错误**: "检查券商服务端"在 2026-10-23 是错的诊断 ——
                # 那次是 TWSE/TPEX 自己返回 502, 券商服务端一直好好的.
                if failures == 5:
                    _alert(f"连续 5 轮采集中断, 台股盘中数据停止: {errors}", args.webhook)
            else:
                failures = 0
                # "成功"地写 0 行是最坏的失败: 快照字段不全时每一行都被丢掉,
                # 不报错、不中断, 只是从早到晚什么都没落地.
                now = datetime.now(TAIPEI)
                if rows == 0 and (now.hour, now.minute) >= _ZERO_ALERT_FROM:
                    zeros += 1
                    if zeros == _ZERO_ROUNDS_ALERT or zeros % (_ZERO_ROUNDS_ALERT * 12) == 0:
                        print(f"{now:%F %T} 连续 {zeros} 轮写入 0 行", file=sys.stderr)
                        _alert(
                            f"连续 {zeros} 轮写入 0 行, 台股盘中数据没有落地",
                            args.webhook,
                        )
                else:
                    zeros = 0
            elapsed = time.monotonic() - started
            if elapsed < args.interval:
                time.sleep(args.interval - elapsed)
        print(f"{datetime.now(TAIPEI):%F %T} 收盘, 采集结束 (累计写入 {written} 行)")
        return 0
    finally:
        if psql is not None:
            psql.close()
        if lock is not None:
            lock.close()


if __name__ == "__main__":
    raise SystemExit(main())
