#!/usr/bin/env python3
"""台股三大法人买卖超: 交易所**单日全市场**接口 (TWSE T86 + TPEX 3insti).

为什么要它: 法人资金流是**新信息**——量价因子已经被反复挖过 (三次 ML 尝试都输给
单因子), 而资金流不在价里. 库存 (`market_v2.fx_net_buy`) 是空的, 需要自己建.

两个接口都是"一天一个请求返回全市场", 与日线同一套模式:

- TWSE `rwd/zh/fund/T86?date=YYYYMMDD&selectType=ALL` (JSON);
- TPEX `web/stock/3insti/daily_trade/3itrade_hedge_result.php?d=YYY/MM/DD&se=EW&t=D`.

单位是**股数**, 不是金额 —— 研究里除以成交量得到净买占比.

输出长表: (date, symbol, foreign_net, trust, dealer), 单位股, 当日买卖超.

    python tw_flows_openapi.py --out /root/data/market --days 750
"""

from __future__ import annotations

import argparse
import fcntl
import json
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import date, timedelta
from pathlib import Path

import duckdb
import httpx
import pandas as pd

_client = httpx.Client(
    timeout=httpx.Timeout(60, connect=10),
    follow_redirects=True,
    headers={"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"},
)

TWSE = "https://www.twse.com.tw/rwd/zh/fund/T86?date={day}&selectType=ALL&response=json"
TPEX = (
    "https://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_result.php"
    "?l=zh-tw&o=json&d={roc}&se=EW&t=D"
)
COLUMNS = ["date", "symbol", "foreign_net", "trust", "dealer"]
# 保留窗口默认给足 11 年: 这个文件只有 ~100MB, 截断换不来什么, 却能悄悄删掉
# 一个跑了三小时的回补 (2026-10-28 实测: 日更任务插进回补中间, 用 900 天窗口
# 把 2020-2022 剪掉, 还把它写的 chunk 一并删了).
KEEP_DAYS = 4000


def _get(url: str) -> bytes:
    """带退避的 GET.

    TWSE 前面是 **HiNetCDN**, 连发几百个请求之后会回 **428 Precondition Required**
    (和 308 重定向混着来), 单个请求隔一会儿又正常 —— 2026-10-28 用 4 并发回补时
    实测. 所以: 自动跟随重定向 (httpx 认 308), 遇到 428/403/429 就退避重试.

    **传输层异常也要重试**: 回补跑到 2025-09 时 HiNetCDN 直接断开连接
    (`RemoteProtocolError`), 一次没兜住就掀翻了整个进程. 连接断了重发就行.
    """
    delay = 0.0
    for attempt in range(5):
        if delay:
            time.sleep(delay)
        try:
            response = _client.get(url)
        except httpx.TransportError as exc:
            print(f"  连接断开 ({exc.__class__.__name__}), 重试", file=sys.stderr)
            delay = 5
            continue
        status = response.status_code
        if status not in {403, 428, 429} and status < 500:
            response.raise_for_status()
            return response.content
        delay = 30 * (attempt + 1)
        print(f"  {status}: 退避 {delay}s", file=sys.stderr)
    raise RuntimeError(f"取不到 {url}")


def _num(text: object) -> int | None:
    raw = str(text or "").replace(",", "").strip()
    if not raw or raw in {"--", "-", "null", "None"}:
        return None
    try:
        return int(float(raw))
    except ValueError:
        return None


def _code(text: object) -> str | None:
    symbol = str(text or "").strip()
    return symbol if symbol[:1].isdigit() and symbol.isalnum() and 4 <= len(symbol) <= 6 else None


def parse_twse(payload: bytes) -> list[dict[str, object]]:
    document = json.loads(payload)
    fields = list(document.get("fields", []))
    if len(fields) < 19:
        return []
    day = document.get("date")
    rows: list[dict[str, object]] = []
    for record in document.get("data", []):
        symbol = _code(record[0])
        if symbol is None:
            continue
        # 外資 = 外陸資(不含外資自營商) + 外資自營商; 自營 = 合計列 (11).
        foreign = (_num(record[4]) or 0) + (_num(record[7]) or 0)
        trust = _num(record[10]) or 0
        dealer = _num(record[11]) or 0
        rows.append(
            {
                "date": day,
                "symbol": symbol,
                "foreign_net": foreign,
                "trust": trust,
                "dealer": dealer,
            }
        )
    return rows


def parse_tpex(payload: bytes) -> list[dict[str, object]]:
    document = json.loads(payload)
    tables = document.get("tables") or []
    if not tables:
        return []
    table = tables[0]
    raw_date = str(table.get("date") or "").replace("/", "")
    if len(raw_date) != 7:
        return []
    day = f"{int(raw_date[:3]) + 1911}{raw_date[3:]}"
    rows: list[dict[str, object]] = []
    for record in table.get("data", []):
        symbol = _code(record[0])
        if symbol is None:
            continue
        # 7 组三联: 外資(不含自營) / 外資自營 / 外資合計 / 投信 / 自營自行 /
        # 自營避險 / 自營合計. 取"合計"以免重复.
        foreign = _num(record[10]) or 0
        trust = _num(record[13]) or 0
        dealer = _num(record[22]) or 0
        rows.append(
            {
                "date": day,
                "symbol": symbol,
                "foreign_net": foreign,
                "trust": trust,
                "dealer": dealer,
            }
        )
    return rows


def trading_days(start: date, end: date) -> list[date]:
    days: list[date] = []
    cursor = start
    while cursor <= end:
        if cursor.weekday() < 5:
            days.append(cursor)
        cursor += timedelta(days=1)
    return days


def write(frame, out: Path) -> None:
    con = duckdb.connect()
    try:
        con.register("incoming", frame)
        con.execute(f"COPY (SELECT * FROM incoming) TO '{out}' (FORMAT PARQUET)")
    finally:
        con.close()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("/root/data/market"))
    parser.add_argument("--days", type=int, default=30, help="回补最近 N 个自然日")
    parser.add_argument("--start", type=date.fromisoformat)
    parser.add_argument("--end", type=date.fromisoformat)
    parser.add_argument("--sleep", type=float, default=0.4)
    parser.add_argument("--workers", type=int, default=1, help="并发天数 (TWSE 一天 2MB, 4 个足够)")
    parser.add_argument(
        "--keep-days",
        type=int,
        default=KEEP_DAYS,
        help="history 里保留最近多少天 (默认 900, 够每日同步; 全量回补要传大值)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="忽略 chunk 与 history 里已有的日期, 按 --start/--end 重抓 (修数据用)",
    )
    args = parser.parse_args(argv)

    end = args.end or date.today()
    start = args.start or (end - timedelta(days=args.days))
    args.out.mkdir(parents=True, exist_ok=True)
    chunks = args.out / "flow_chunks"
    chunks.mkdir(exist_ok=True)
    history_path = args.out / "tw_flows_history.parquet"

    # **整脚本互斥**: 采集与合并必须是一个原子动作. cron 里用 `pgrep` 挡并发挡不住
    # (2026-10-28: 日更任务插进三小时的回补中间, 合并时把回补的 chunk 删了、还用
    # 900 天窗口截断了 history). flock 不依赖进程名, 也不会误杀.
    lock_path = args.out / "tw_flows.lock"
    lock_handle = lock_path.open("w")
    try:
        fcntl.flock(lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        print("另一个采集进程在跑, 退出", file=sys.stderr)
        return 0

    # **按天落盘**: VPS 只有 3GB 内存, 把几百万行攒在内存里再写是等着被 OOM
    # (2026-10-28 实测: 两个进程 1.9G/3.4G, 双双被杀, 全丢). 每天一个 parquet,
    # 中途死掉进度还在, 重跑会跳过已有的 chunk.
    known_days = {path.stem.removeprefix("flow_") for path in chunks.glob("flow_*.parquet")}
    if args.force:
        known_days = set()  # 修数据: 按 --start/--end 重抓, 不看已有落盘
    elif history_path.is_file():
        con = duckdb.connect()
        try:
            known_days |= {
                str(row[0])
                for row in con.execute(
                    f"SELECT DISTINCT strftime(date, '%Y%m%d') FROM read_parquet('{history_path}')"
                ).fetchall()
            }
        finally:
            con.close()

    requests = 0
    new_days = 0
    lock = threading.Lock()

    def fetch_day(day: date) -> None:
        nonlocal requests, new_days
        stamp = day.strftime("%Y%m%d")
        rows: list[dict[str, object]] = []
        for label, url, parser in (
            ("TWSE", TWSE.format(day=stamp), parse_twse),
            (
                "TPEX",
                TPEX.format(roc=f"{day.year - 1911}/{day.month:02d}/{day.day:02d}"),
                parse_tpex,
            ),
        ):
            try:
                rows.extend(parser(_get(url)))
                with lock:
                    requests += 1
            except (OSError, ValueError, httpx.HTTPError, RuntimeError) as exc:
                print(f"  {day} {label} 失败: {exc}", file=sys.stderr)
                continue
            time.sleep(args.sleep)
        if not rows:
            return  # 假日: 不写 chunk, 下次重试也无妨
        frame = pd.DataFrame(rows, columns=COLUMNS)
        # **日期在落盘前就转成日期类型**: 存成字符串 "20221219" 的话, 合并时
        # `date::DATE` 会报 "invalid date field format" (2026-10-28 实测).
        frame["date"] = pd.to_datetime(frame["date"], format="%Y%m%d")
        write(frame, chunks / f"flow_{stamp}.parquet")
        with lock:
            new_days += 1
            if new_days % 20 == 0:
                print(f"  …{day}: 已落盘 {new_days} 天, 请求 {requests}", flush=True)

    pending = [day for day in trading_days(start, end) if day.strftime("%Y%m%d") not in known_days]
    if args.workers > 1:
        with ThreadPoolExecutor(max_workers=args.workers) as pool:
            list(pool.map(fetch_day, pending))
    else:
        for day in pending:
            fetch_day(day)

    if new_days == 0 and not any(chunks.glob("flow_*.parquet")):
        print("没有新数据 (假日, 或已全部回补)", file=sys.stderr)
        return 0

    # 合并: 历史 + 所有 chunk, 去重后写回, 然后清掉 chunk.
    # **先写临时文件再 replace**: 同一条语句里读历史又写历史, DuckDB 会拒绝.
    con = duckdb.connect()
    temp_path = args.out / "tw_flows_history.tmp.parquet"
    try:
        sources = []
        if history_path.is_file():
            sources.append(f"SELECT * FROM read_parquet('{history_path}')")
        sources.append(f"SELECT * FROM read_parquet('{chunks}/flow_*.parquet')")
        merged = (
            "SELECT date::DATE AS date, symbol, foreign_net, trust, dealer FROM ("
            + " UNION ALL ".join(sources)
            + ") QUALIFY row_number() OVER (PARTITION BY date, symbol ORDER BY 1) = 1"
        )
        con.execute(
            f"COPY (SELECT * FROM ({merged})"
            f" WHERE date >= (SELECT max(date) FROM ({merged})) - INTERVAL {args.keep_days} DAY"
            f" ORDER BY date, symbol) TO '{temp_path}' (FORMAT PARQUET)"
        )
        temp_path.replace(history_path)
        con.execute(
            f"COPY (SELECT * FROM read_parquet('{history_path}')"
            f" WHERE date >= (SELECT max(date) FROM read_parquet('{history_path}'))"
            " - INTERVAL 60 DAY) TO '"
            f"{args.out / 'tw_flows_recent.parquet'}' (FORMAT PARQUET)"
        )
        total, latest, latest_symbols = con.execute(
            f"SELECT count(*), max(date), count(*) FILTER (WHERE date ="
            f" (SELECT max(date) FROM read_parquet('{history_path}')))"
            f" FROM read_parquet('{history_path}')"
        ).fetchone()
    finally:
        con.close()
    for path in chunks.glob("flow_*.parquet"):
        path.unlink()
    print(
        f"写盘: {total:,} 行, {latest_symbols} 只 @ {latest}, 本轮 {new_days} 天, 请求 {requests}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
