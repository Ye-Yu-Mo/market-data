#!/usr/bin/env python3
"""台股日线: 直接从交易所取**全市场单日**数据 (TWSE + TPEX), 一分钟跑完.

**为什么不用 FinMind 的日更**: 它是**按标的**取数的, 3134 只 = 3134 个请求,
540 请求/小时 -> 每天 6-11 小时. 而且它 05:40 开始跑的时候当天的数据还没发布
(台股 13:30 收盘, FinMind 通常要几小时后才有), 于是每天稳定落后一天, 还常常
"只更新了一部分标的" —— 那比不更新更糟: 策略以为当天是全市场, 实际只有半场.

交易所自己有**单日全市场**的接口:

- TWSE `afterTrading/STOCK_DAY_ALL?date=YYYYMMDD` (上市, 一个 CSV);
- TPEX `openapi/v1/tpex_mainboard_quotes` (上柜, 一个 JSON, 只给最新一日).

两个请求, 盘后就能拿到. **本脚本跑在 VPS 上** (leaf 到 twse.com.tw 网络不可达),
产物写到文件服务目录, leaf 通过隧道拉取 —— 与币圈那套同一个模式.

    python tw_daily_openapi.py --out /root/data/market
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import sys
import urllib.request
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pandas as pd

TWSE = "https://www.twse.com.tw/rwd/zh/afterTrading/STOCK_DAY_ALL?date={day}&response=json"
TPEX = "https://www.tpex.org.tw/openapi/v1/tpex_mainboard_quotes"
#: 大盤成交統計: 一天一行, 含 TAIEX 收盘与全市场成交股数/金额. 指数风控要用.
FMTQIK = "https://www.twse.com.tw/rwd/zh/afterTrading/FMTQIK?date={day}&response=json"
COLUMNS = ["symbol", "date", "open", "high", "low", "close", "volume", "amount", "market"]
#: 保留多少天的历史在 VPS 上. leaf 每天整份拉过去做 upsert, 大一点没关系.
KEEP_DAYS = 60


def _get(url: str) -> bytes:
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def roc_date(text: str) -> date | None:
    """`1151027` -> 2026-10-27. 民国年 + 1911."""
    raw = str(text).strip().replace("/", "").replace("-", "")
    if len(raw) != 7 or not raw.isdigit():
        return None
    return date(int(raw[:3]) + 1911, int(raw[3:5]), int(raw[5:7]))


def _number(text: object) -> float | None:
    raw = str(text or "").replace(",", "").strip()
    if not raw or raw in {"--", "-", "null", "None"}:
        return None
    try:
        return float(raw)
    except ValueError:
        return None


def parse_twse(payload: bytes) -> list[dict[str, object]]:
    """TWSE 的 CSV (带 BOM, 逗号在引号里)."""
    text = payload.decode("utf-8-sig")
    rows: list[dict[str, object]] = []
    for record in csv.DictReader(io.StringIO(text)):
        symbol = str(record.get("證券代號") or "").strip()
        day = roc_date(record.get("日期") or "")
        close = _number(record.get("收盤價"))
        if not symbol or day is None or close is None:
            continue
        volume = _number(record.get("成交股數"))
        rows.append(
            {
                "symbol": symbol,
                "date": day,
                "open": _number(record.get("開盤價")),
                "high": _number(record.get("最高價")),
                "low": _number(record.get("最低價")),
                "close": close,
                "volume": int(volume) if volume is not None else None,
                "amount": _number(record.get("成交金額")),
                "market": "TWSE",
            }
        )
    return rows


def parse_tpex(payload: bytes) -> list[dict[str, object]]:
    """TPEX 的 JSON 数组 (字段名是英文, 日期是民国年)."""
    rows: list[dict[str, object]] = []
    for record in json.loads(payload):
        symbol = str(record.get("SecuritiesCompanyCode") or "").strip()
        day = roc_date(record.get("Date") or "")
        close = _number(record.get("Close"))
        if not symbol or day is None or close is None:
            continue
        volume = _number(record.get("TradingShares"))
        rows.append(
            {
                "symbol": symbol,
                "date": day,
                "open": _number(record.get("Open")),
                "high": _number(record.get("High")),
                "low": _number(record.get("Low")),
                "close": close,
                "volume": int(volume) if volume is not None else None,
                "amount": _number(record.get("TransactionAmount")),
                "market": "TPEX",
            }
        )
    return rows


def parse_fmtqik(payload: bytes) -> list[dict[str, object]]:
    """大盤成交統計 -> TAIEX 日线行 (open=high=low=close, volume/amount 是全市场).

    指数没有 OHLC, 只有收盘; 因子用不到它的日内形态, 风控门只要收盘序列.
    `volume` 必须是正数 —— dataset 加载过滤 `volume > 0`, 存 0 等于把这根丢掉.
    """
    document = json.loads(payload.decode("utf-8-sig"))
    rows: list[dict[str, object]] = []
    for record in document.get("data", []):
        if len(record) < 5:
            continue
        day = roc_date(str(record[0]).replace("/", ""))
        close = _number(record[4])
        volume = _number(record[1])
        if day is None or close is None:
            continue
        rows.append(
            {
                "symbol": "TAIEX",
                "date": day,
                "open": close,
                "high": close,
                "low": close,
                "close": close,
                "volume": int(volume) if volume else 1,
                "amount": _number(record[2]),
                "market": "TWSE",
            }
        )
    return rows


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out", type=Path, default=Path("/root/data/market"))
    parser.add_argument(
        "--day", type=date.fromisoformat, help="补某一天 (默认: 交易所最新一日)"
    )
    args = parser.parse_args(argv)

    fetched: list[dict[str, object]] = []
    # TWSE 要显式给日期; 不给就用今天 (周末/假日会返回空表, 不当成失败).
    day = args.day or date.today()
    for candidate in (day, day - timedelta(days=1), day - timedelta(days=2)):
        rows = parse_twse(_get(TWSE.format(day=candidate.strftime("%Y%m%d"))))
        if rows:
            fetched += rows
            print(f"TWSE {candidate}: {len(rows)} 只")
            break
    else:
        print("TWSE 近三天都没有数据 (假日?)", file=sys.stderr)

    tpex_rows = parse_tpex(_get(TPEX))
    fetched += tpex_rows
    if tpex_rows:
        print(f"TPEX {tpex_rows[0]['date']}: {len(tpex_rows)} 只")
    else:
        print("TPEX 没有数据 (假日?)", file=sys.stderr)

    try:
        index_rows = parse_fmtqik(_get(FMTQIK.format(day=day.strftime("%Y%m%d"))))
    except (OSError, ValueError) as exc:
        print(f"指数取不到: {exc}", file=sys.stderr)
        index_rows = []
    fetched += index_rows
    if index_rows:
        print(f"TAIEX: {len(index_rows)} 天 (最新 {index_rows[-1]['date']} {index_rows[-1]['close']})")

    if not fetched:
        print("两边都没有数据", file=sys.stderr)
        return 1

    frame = pd.DataFrame(fetched, columns=COLUMNS)
    history_path = args.out / "tw_daily_history.parquet"
    args.out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    if history_path.is_file():
        old = con.execute(f"SELECT * FROM read_parquet('{history_path}')").df()
        frame = pd.concat([old, frame], ignore_index=True)
    # **先统一 date 类型再去重**: 历史 parquet 读回来是 datetime64, 新抓的是
    # 字符串; 直接 drop_duplicates 会把同一天当成两个值, 文件每天翻一倍.
    frame["date"] = pd.to_datetime(frame["date"])
    frame = frame.drop_duplicates(subset=["symbol", "date"], keep="last")
    cutoff = frame["date"].max() - pd.Timedelta(days=KEEP_DAYS)
    frame = frame[frame["date"] >= cutoff].sort_values(["date", "symbol"])
    # **用 DuckDB 写 parquet, 不用 pandas.to_parquet**: 后者要 pyarrow, 而 VPS 的
    # 采集 venv 里没有 (也不想为一次写盘多装 40MB).
    con.register("outgoing", frame)
    for target in (history_path, args.out / "tw_daily_recent.parquet"):
        con.execute(f"COPY (SELECT * FROM outgoing) TO '{target}' (FORMAT PARQUET)")
    con.unregister("outgoing")
    con.close()
    latest = frame["date"].max()
    print(
        f"写盘: {history_path.name} {len(frame):,} 行, "
        f"{frame.loc[frame['date'] == latest].shape[0]} 只 @ {latest.date()}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
