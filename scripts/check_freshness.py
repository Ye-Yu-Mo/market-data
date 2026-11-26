#!/usr/bin/env python
"""数据新鲜度检查: 每个源"最新到哪一天", 落后了就发飞书.

**为什么需要它**: 看板能把新鲜度摆在眼前, 但没人天天盯着看. 2026-10-22 早上
`adj_factor` 是空的, 直到 09:40 那一轮失败才被发现 —— 而它前一天晚上就该被报出来。

**节假日与故障在数据上分不出来**: A 股春节能停 9 天. 所以判据是"落后超过 N 个
**工作日**"而不是"不是今天". 一个正常的长假不会报警, 而真停摆超过两天一定报.

用法 (crontab `0 23 * * 1-5` UTC = 次日 07:00 台北, 两个日更都跑完了):

    uv run python scripts/check_freshness.py
    uv run python scripts/check_freshness.py --dry-run     # 只打印, 不发告警
"""

from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

import duckdb

from market_data.offline.alerts import notify

MARKET_DIR = Path.home() / "data" / "market"

#: 允许落后多少个**工作日**. 1 天是正常的 (日更在收盘后跑), 2 天就值得看一眼;
#: 设成 2 是为了让单日节假日不误报.
MAX_LAG_BUSINESS_DAYS = 2


@dataclass(frozen=True, slots=True)
class Check:
    name: str
    ok: bool
    detail: str


def business_days_between(start: date, end: date) -> int:
    """`start` 之后到 `end`(含) 之间的工作日数. 不含交易日历, 只排周末."""
    if end <= start:
        return 0
    days = 0
    cursor = start + timedelta(days=1)
    while cursor <= end:
        if cursor.weekday() < 5:
            days += 1
        cursor += timedelta(days=1)
    return days


def _max_date(path: Path, table: str, column: str = "date") -> date | None:
    if not path.is_file():
        return None
    connection = duckdb.connect(str(path), read_only=True)
    try:
        row = connection.execute(f"SELECT max({column}) FROM {table}").fetchone()
    except duckdb.Error:
        return None
    finally:
        connection.close()
    value = row[0] if row else None
    return value.date() if isinstance(value, datetime) else value


def check_candles(name: str, path: Path, today: date) -> Check:
    latest = _max_date(path, "candles")
    if latest is None:
        return Check(name, False, f"{path.name} 读不到或没有 bar")
    lag = business_days_between(latest, today)
    return Check(
        name,
        lag <= MAX_LAG_BUSINESS_DAYS,
        f"最新 {latest}, 落后 {lag} 个工作日" + ("" if lag <= MAX_LAG_BUSINESS_DAYS else " —— 日更停了?"),
    )


def check_adj_factor(path: Path) -> Check:
    """复权因子必须追平 candles: 差一天, 带 window 的因子就会被 require_fresh_history 拒."""
    candles = _max_date(path, "candles")
    factors = _max_date(path, "adj_factor")
    if candles is None:
        return Check("台股复权因子", False, "读不到 candles")
    if factors is None:
        return Check("台股复权因子", False, "adj_factor 是空的")
    lag = business_days_between(factors, candles)
    return Check(
        "台股复权因子",
        lag <= MAX_LAG_BUSINESS_DAYS,
        f"因子 {factors} / 行情 {candles}, 差 {lag} 个工作日"
        + ("" if lag <= MAX_LAG_BUSINESS_DAYS else " —— 带 window 的因子会被拒"),
    )


def check_candle_coverage(path: Path) -> Check:
    """最新一天不能只有半个市场. TPEX 数据晚到会让 TWSE 先落盘, 之后忘了补."""
    if not path.is_file():
        return Check("台股日线覆盖", False, f"{path.name} 不存在")
    connection = duckdb.connect(str(path), read_only=True)
    try:
        rows = connection.execute(
            "SELECT date, count(*) FROM candles GROUP BY date ORDER BY date DESC LIMIT 2"
        ).fetchall()
    finally:
        connection.close()
    if len(rows) < 2:
        return Check("台股日线覆盖", False, "candles 不足两天")
    (latest, latest_n), (prev, prev_n) = rows
    ratio = latest_n / prev_n if prev_n else 0.0
    return Check(
        "台股日线覆盖",
        ratio >= 0.8,
        f"{latest} {latest_n} 只 / 前一日 {prev} {prev_n} 只 ({ratio:.0%})"
        + ("" if ratio >= 0.8 else " —— TPEX 没补上?"),
    )


def check_crypto(path: Path, now: datetime) -> Check:
    """币圈 1h 增量每天 02:00 UTC 跑一次; 超过 26 小时没有新 bar 就是停了."""
    if not path.is_file():
        return Check("币圈 1h", False, f"{path.name} 不存在")
    connection = duckdb.connect(str(path), read_only=True)
    try:
        row = connection.execute(
            "SELECT max(open_time) FROM klines WHERE interval = '1h'"
        ).fetchone()
    except duckdb.Error:
        return Check("币圈 1h", False, "读不到 klines")
    finally:
        connection.close()
    stamp_ms = row[0] if row else None
    if stamp_ms is None:
        return Check("币圈 1h", False, "1h 是空的")
    stamp = datetime.fromtimestamp(int(stamp_ms) / 1000, UTC)
    age_h = (now - stamp).total_seconds() / 3600
    return Check(
        "币圈 1h",
        age_h < 26,
        f"最新 {stamp:%Y-%m-%d %H:%M} UTC, 距今 {age_h:.0f} 小时"
        + ("" if age_h < 26 else " —— 采集停了?"),
    )


def check_intraday(path: Path, now: datetime) -> Check:
    """盘中采集: 交易时段内看最近一帧, 收盘后看今天有没有数据."""
    if not path.is_file():
        return Check("台股盘中采集", False, f"{path.name} 不存在")
    try:
        connection = duckdb.connect(str(path), read_only=True)
    except duckdb.Error:
        # 盘中采集器持锁是**预期状态**, 不是故障.
        return Check("台股盘中采集", True, "正在采集 (库被写者占用)")
    try:
        row = connection.execute("SELECT max(recv_at) FROM quotes").fetchone()
    finally:
        connection.close()
    latest = row[0] if row else None
    if latest is None:
        return Check("台股盘中采集", False, "quotes 是空的")
    stamp = latest if isinstance(latest, datetime) else datetime.fromisoformat(str(latest))
    if stamp.tzinfo is None:
        # 采集库里的 recv_at 是 UTC 的裸时间戳 (写入时已 astimezone 过).
        stamp = stamp.replace(tzinfo=UTC)
    # 台北时间 09:00-13:35 是交易时段; 采集器每 15 秒一帧, 超过 10 分钟没新数据就是停了.
    taipei_now = now + timedelta(hours=8)
    if taipei_now.weekday() < 5 and 9 <= taipei_now.hour < 14:
        gap = (datetime.now(UTC) - stamp).total_seconds()
        return Check(
            "台股盘中采集",
            gap < 600,
            f"最近一帧 {stamp} (距今 {gap / 60:.0f} 分钟)" + ("" if gap < 600 else " —— 采集停了?"),
        )
    lag = business_days_between(stamp.date(), taipei_now.date())
    return Check("台股盘中采集", lag <= MAX_LAG_BUSINESS_DAYS, f"最近一帧 {stamp}")


def _alert(webhook: str, checks: list[Check]) -> None:
    if not webhook:
        return
    failed = [check for check in checks if not check.ok]
    if not failed:
        return
    lines = [f"- {check.name}: {check.detail}" for check in failed]
    notify(
        webhook,
        title=f"数据新鲜度异常 ({len(failed)} 项)",
        content="\n".join(lines),
        severity="error",
        service="market_data",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="数据新鲜度检查")
    parser.add_argument("--dry-run", action="store_true", help="只打印, 不发告警")
    parser.add_argument("--market-dir", type=Path, default=MARKET_DIR)
    args = parser.parse_args(argv)

    now = datetime.now(UTC)
    today = now.date()
    checks = [
        check_candles("A 股日线", args.market_dir / "market_a.duckdb", today),
        check_candles("台股日线", args.market_dir / "market_v2.duckdb", today),
        check_adj_factor(args.market_dir / "market_v2.duckdb"),
        check_candle_coverage(args.market_dir / "market_v2.duckdb"),
        check_crypto(Path.home() / "data" / "research" / "market_crypto.duckdb", now),
        check_intraday(args.market_dir / "market_tw_intraday.duckdb", now),
    ]

    failed = 0
    for check in checks:
        mark = "ok  " if check.ok else "FAIL"
        print(f"{mark} {check.name}: {check.detail}")
        failed += 0 if check.ok else 1

    if failed and not args.dry_run:
        _alert(os.environ.get("LARK_WEBHOOK", ""), checks)
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
