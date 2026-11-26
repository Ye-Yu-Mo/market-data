"""The one-way load: fetch, normalize, gate, write — in that order, or not at all.

The order is not a formality. Writing first and checking afterwards means the
recovery path runs through the database, and every rollback is a chance to leave
a half-batch behind; checking first means the only state that ever needs undoing
is one this module undid itself.

That matters because a *half* batch is the worst outcome available. ``candles``
written without ``adj_factor`` leaves bars the consumer's inner join silently
discards — no error, no warning, just less history — and ``write_rows`` commits
one table at a time, so the boundary that makes a load atomic has to live here,
at the layer that knows what one load is.

``offline/duckdb_store.py`` owns the shape of a write: whether a frame can be
stored at all. This module owns its *values*.

The one place the two overlap is duplicate keys, and it is deliberate. A batch
naming the same ``(symbol, date)`` twice is refused, here and there, with
different reporting: the gate says which rows and how many, ``write_rows`` says
nothing beyond the refusal. Neither keeps the last row — DuckDB would keep the
*first* without saying so, which is why neither delegates the decision to it.
"""

# pyright: reportAttributeAccessIssue=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# Silenced for this module only, and only where ``pandas``' stubs force it: a
# coercion like ``pd.to_numeric`` reads back as a union that includes plain
# scalars, so calling ``.isna()`` on the result is reported against ``float``.
# ``reportArgumentType`` — the rule that caught a real mistake in M2 — stays on.

from __future__ import annotations

import datetime as dt
import hashlib
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass

import duckdb
import pandas as pd

from market_data.markets.base import SourceEmpty, SourceMalformed, SourceUnavailable
from market_data.markets.cn.free_stockdb import (
    FreeStockDbAdapter,
    build_symbols,
    daily_factors,
    extract_names,
    extract_shares,
    parse_code_table,
    parse_delisted,
    plan_all_adjustments,
    plan_code_table,
    plan_delisted,
    plan_history,
    plan_market_day,
    stock_groups,
)
from market_data.markets.contracts import (
    ADJ_FACTOR,
    CANDLES,
    LOAD_LOG,
    SHARES_OUTSTANDING,
    SOURCE_GAP,
    SYMBOLS,
    TableContract,
)
from market_data.offline.duckdb_store import SCHEMA_VERSION_VALUE, write_rows
from market_data.offline.quality import (
    QualityGateFailed,
    QualityReport,
    check_candles,
    require_clean,
)

__all__ = [
    "MARKETS",
    "Batch",
    "LoadRecord",
    "RunOutcome",
    "SymbolFailure",
    "clear_gap",
    "load",
    "record_gap",
    "run_full",
    "run_window",
    "store_report",
    "trim_load_log",
]

#: Every factor in the store is forward-adjusted, so a symbol's most recent
#: trading day carries exactly 1.0. Recorded per load because it is the one
#: property a reader of the factor table cannot infer from the numbers.
ADJUSTMENT = "qfq"


@dataclass(frozen=True, slots=True)
class Batch:
    """One load's frames, already normalized onto the contract."""

    candles: pd.DataFrame
    adj_factor: pd.DataFrame
    shares_outstanding: pd.DataFrame
    symbols: pd.DataFrame


@dataclass(frozen=True, slots=True)
class LoadRecord:
    """What one load put into the store, and what it covered."""

    market: str
    dataset: str
    start_date: dt.date
    end_date: dt.date
    candles: int
    adj_factor: int
    shares_outstanding: int
    symbols: int
    checksum: str
    adjustment: str
    schema_version: int
    loaded_at: dt.datetime


def load(connection: duckdb.DuckDBPyConnection, *, market: str, batch: Batch) -> LoadRecord:
    """Gate ``batch``, then write all four tables in one transaction.

    Raises ``QualityGateFailed`` before anything is written, and rolls back on
    any later failure, so a store is never left holding part of a load.
    """
    require_clean(_gate(batch, market))
    record = _describe(market, batch)
    connection.execute("BEGIN TRANSACTION")
    try:
        for contract, frame in _tables(batch):
            write_rows(connection, contract, frame)
        require_clean(_store_report(connection, market, symbols_of(batch)))
        write_rows(connection, LOAD_LOG, _log_frame(record))
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise
    return record


def symbols_of(batch: Batch) -> tuple[str, ...]:
    """The symbols a batch writes, which is what its store checks are about.

    ``candles`` is always non-empty by the time a load reaches the write — an
    empty one is refused by the gate — so this never answers "nothing".
    """
    return tuple(str(symbol) for symbol in batch.candles["symbol"].unique())


def _tables(batch: Batch) -> tuple[tuple[TableContract, pd.DataFrame], ...]:
    """The four tables, in the order they are written."""
    return (
        (CANDLES, batch.candles),
        (ADJ_FACTOR, batch.adj_factor),
        (SHARES_OUTSTANDING, batch.shares_outstanding),
        (SYMBOLS, batch.symbols),
    )


def _gate(batch: Batch, market: str) -> QualityReport:
    """Value rules for every table being written — never shape rules.

    ``duckdb_store.write_rows`` already refuses a frame whose column set differs,
    whose dtype cannot hold the SQL type, or which repeats a primary key. Doing
    that again here would only make "which layer refused this" harder to answer.

    The report is labelled ``batch`` rather than ``candles`` because it can carry
    issues from any of the four tables; each message names its own table.
    """
    report = check_candles(batch.candles, market, dataset="batch")
    _require_positive(report, ADJ_FACTOR, batch.adj_factor, "factor", "non_positive_factor")
    _require_positive(
        report, SHARES_OUTSTANDING, batch.shares_outstanding, "shares", "non_positive_shares"
    )
    return report


def _require_positive(
    report: QualityReport, contract: TableContract, frame: pd.DataFrame, column: str, code: str
) -> None:
    """A factor or a share count of zero or less is not a value, it is a mistake.

    Zero shares cannot be divided by, and a factor of zero would make an adjusted
    price vanish. NaN counts too: it is what a missing value silently becomes.
    """
    values = pd.to_numeric(frame[column], errors="coerce")
    bad = values.isna() | values.le(0)
    if not bad.any():
        return
    sample = tuple(frame.loc[bad, "symbol"].astype(str).head(5))
    report.add(
        code,
        f"{contract.name}.{column} must be positive and present",
        int(bad.sum()),
        sample,
    )


def _store_report(
    connection: duckdb.DuckDBPyConnection, market: str, symbols: Sequence[str]
) -> QualityReport:
    """Invariants the tables alone can answer, checked before the commit.

    A batch can be internally perfect and still break the store, and every
    failure here is silent downstream: the consumer's inner join drops an
    orphaned bar without a word, and a backward-adjusted factor is just a larger
    ordinary-looking number. This runs inside the transaction so the answer is
    "roll back", not "find out later".

    **Scoped to the symbols this batch wrote.** The questions are about a symbol,
    and asking them across the whole store made every transaction scan 19.5M
    rows twice over plus a 19.5M-row grouping — measured at ~1.5 s, which is why
    a 19-day catch-up of 7,887 symbols ran at 0.56 symbols/s and would have taken
    four hours to learn nineteen days. The answer is identical for the rows the
    batch touched, and ``store_report`` asks the whole-store version once per
    run, where a table-sized scan belongs.
    """
    report = QualityReport(market, "store")
    # Each side is narrowed *before* the join. Naming the symbols in a `where`
    # clause on top of a join over the whole tables is not the same thing and
    # costs about 175x more — measured 0.77 s against 0.004 s on a 19.5M-row
    # store, because DuckDB builds the hash table from everything and filters
    # afterwards. Narrowing first turns it back into a lookup.
    checks = (
        (
            2,
            "factor_gap",
            "bars with no adj_factor row; the consumer's inner join discards them",
            "select count(*) from"
            " (select symbol, date from candles where symbol in (select unnest(?))) c"
            " left join (select symbol, date from adj_factor where symbol in (select unnest(?))) a"
            " using (symbol, date) where a.symbol is null",
        ),
        (
            2,
            "orphan_factor",
            "adj_factor rows with no matching bar; the reference database has none",
            "select count(*) from"
            " (select symbol, date from adj_factor where symbol in (select unnest(?))) a"
            " left join (select symbol, date from candles where symbol in (select unnest(?))) c"
            " using (symbol, date) where c.symbol is null",
        ),
        # There is deliberately no "no factor above 1.0" check. It looks like the
        # qfq convention and is not: it assumes a monotonically rising ``cum``,
        # which holds for a stock paying dividends and fails for anything that
        # has been split or had its capital reduced — a fund's 份额拆分, a
        # company's 減資. The reference database itself carries 2,574 such rows
        # across three symbols (max 1.0445), so the rule would have rejected the
        # database it was copied from. What the convention actually promises is
        # the row below.
        (
            2,
            "weekend_bar",
            "candles on a Saturday/Sunday; the exchange trades none"
            " (2026-11-03: 600000 有 5 行周末占位数据, 直到删掉 K 线才被"
            " orphan_factor 间接暴露)",
            "select count(*) from candles where symbol in (select unnest(?))"
            " and dayofweek(date) in (0, 6)",
        ),
        (
            1,
            "factor_not_forward_adjusted",
            "a symbol's most recent trading day must carry exactly 1.0",
            "with latest as (select symbol, max(date) as date from adj_factor"
            " where symbol in (select unnest(?)) group by symbol)"
            " select count(*) from latest l join adj_factor a"
            " on a.symbol = l.symbol and a.date = l.date where a.factor <> 1.0",
        ),
    )
    wanted = list(symbols)
    for _arity, code, message, sql in checks:
        # 参数个数跟 SQL 里的占位符走, 不跟严重度走 —— 两者恰好相等是巧合,
        # 新加的检查 (一个占位符但 severity=2) 会因此报参数不匹配.
        found = _count(connection, sql, [wanted] * sql.count("?"))
        if found:
            report.add(code, message, found)
    return report


def store_report(connection: duckdb.DuckDBPyConnection, market: str) -> QualityReport:
    """The same invariants across every symbol — once per run, not per batch.

    The scoped check above is what a transaction can afford; this is the one that
    answers "is the whole store sound", and it costs a table-sized scan, so it
    belongs at the end of a run rather than inside every one of its commits.
    """
    whole = QualityReport(market, "store")
    scoped = _store_report(
        connection,
        market,
        [row[0] for row in connection.execute("select distinct symbol from candles").fetchall()],
    )
    for issue in scoped.issues:
        whole.add(issue.code, issue.message, issue.count, issue.sample)
    return whole


def _count(
    connection: duckdb.DuckDBPyConnection, sql: str, params: Sequence[object] | None = None
) -> int:
    row = connection.execute(sql, params if params is not None else []).fetchone()
    if row is None:  # a bare count(*) always returns exactly one row
        raise AssertionError(f"count query returned nothing: {sql}")
    return int(row[0])


def _describe(market: str, batch: Batch) -> LoadRecord:
    dates = pd.to_datetime(batch.candles["date"])
    return LoadRecord(
        market=market,
        dataset="batch",
        start_date=dates.min().date(),
        end_date=dates.max().date(),
        candles=len(batch.candles),
        adj_factor=len(batch.adj_factor),
        shares_outstanding=len(batch.shares_outstanding),
        symbols=len(batch.symbols),
        checksum=_checksum(batch),
        adjustment=ADJUSTMENT,
        schema_version=SCHEMA_VERSION_VALUE,
        loaded_at=dt.datetime.now(dt.UTC),
    )


def _log_frame(record: LoadRecord) -> pd.DataFrame:
    """The provenance row, shaped for the ``load_log`` contract."""
    return pd.DataFrame(
        [
            {
                "loaded_at": record.loaded_at.isoformat(),
                "market": record.market,
                "dataset": record.dataset,
                "start_date": record.start_date,
                "end_date": record.end_date,
                "candles": record.candles,
                "adj_factor": record.adj_factor,
                "shares_outstanding": record.shares_outstanding,
                "symbols": record.symbols,
                "checksum": record.checksum,
                "adjustment": record.adjustment,
                "schema_version": record.schema_version,
            }
        ],
        columns=pd.Index([column.name for column in LOAD_LOG.columns]),
    )


def _checksum(batch: Batch) -> str:
    """A digest of the batch's content, not of the store file.

    A file hash cannot answer "which load produced what is in here": rewriting
    the same rows idempotently leaves the content identical, and it would be the
    file that changed. Hashing the ordered rows answers the question the record
    is actually asked.
    """
    digest = hashlib.sha256()
    for contract, frame in _tables(batch):
        digest.update(contract.name.encode())
        for row in frame.sort_values(list(contract.primary_key)).itertuples(index=False, name=None):
            digest.update(repr(tuple(row)).encode())
    return digest.hexdigest()


# --- orchestration ----------------------------------------------------------
#
# A run is a sequence of per-symbol loads, not one large one. The unit has to be
# the symbol: a symbol's adjustment factors are a series — ``daily_factors``
# needs all of its dates and all of its events at once — so slicing by date would
# split one series across batches and no batch could compute a correct factor.
# It also happens to cost less (5,837 requests of ~1.9 MB, against ~7,000 of
# 2.5 MB), but correctness is the argument that does not depend on the service.
#
# Resume state is read from the store and never from a sidecar. A checkpoint file
# that is wrong is wrong silently, one symbol short, with nobody to notice;
# the store is what the question is about, M2's writes are idempotent and M3's
# loads are atomic, so re-running the same command is safe by construction.

#: The markets this pipeline loads. ``--market tw`` is a separate decision — it
#: would have to write the store ``stock_tw`` produces, which is read-only here.
#: Spelled the way the command line spells it, since it is stored verbatim in
#: ``load_log`` and ``source_gap``.
MARKETS = ("cn",)

#: What ``daily_factors`` gets for a symbol the event reply says nothing about:
#: no distribution ever happened, so every day carries 1.0.
_NO_EVENTS = pd.DataFrame(
    {
        "symbol": pd.Series(dtype="object"),
        "date": pd.Series(dtype="datetime64[s]"),
        "cum": pd.Series(dtype="float64"),
    }
)


@dataclass(frozen=True, slots=True)
class SymbolFailure:
    """One symbol that could not be loaded, and why.

    ``gated`` separates "the data was wrong" from every other cause, because the
    two need opposite responses from whoever is on call: a gate rejection means
    re-running changes nothing until someone looks at the data, while a failure
    like a dropped connection is worth retrying.
    """

    symbol: str
    error: str
    gated: bool = False


@dataclass(frozen=True, slots=True)
class RunOutcome:
    """What one run did, in the terms an operator asks about it."""

    market: str
    loaded: int
    skipped: int
    no_data: int
    failures: tuple[SymbolFailure, ...]
    trading_days: tuple[dt.date, ...]

    @property
    def partial(self) -> bool:
        return bool(self.failures)


def run_full(
    connection: duckdb.DuckDBPyConnection,
    adapter: FreeStockDbAdapter,
    *,
    market: str,
    refresh: bool = False,
    on_event: Callable[[str], None] | None = None,
) -> RunOutcome:
    """Load every symbol's full history — one request per symbol.

    Symbols already in the store are skipped, which is what makes an interrupted
    run resumable by re-running the same command. The cost is that a source
    revision to old rows goes unnoticed, which is why ``refresh`` exists; the
    default is the safe one, and ``--help`` says so.
    """
    _require_market(market)
    catalogue = parse_code_table(adapter.fetch(plan_code_table()))
    delisted = parse_delisted(adapter.fetch(plan_delisted()))
    # Every symbol's events in one reply (4.1 MB) rather than one reply per
    # symbol: the bars are what needs a request each, and the events do not.
    events = _by_symbol(adapter.adjustment_events(adapter.fetch(plan_all_adjustments())))
    universe = _universe(stock_groups(catalogue), delisted)
    settled = set() if refresh else _settled(connection, market)
    delisted_symbols = set(delisted)

    loaded = skipped = no_data = 0
    failures: list[SymbolFailure] = []
    for symbol in universe:
        if symbol in settled:
            skipped += 1
            continue
        try:
            if _load_one(
                connection,
                adapter,
                market=market,
                symbol=symbol,
                delisted=symbol in delisted_symbols,
                events=events.get(symbol),
                on_event=on_event,
            ):
                loaded += 1
            else:
                no_data += 1
        except SourceUnavailable:
            # The service is down: every remaining symbol would fail the same
            # way, so this is the run's outcome, not a per-symbol casualty.
            raise
        except Exception as exc:
            failures.append(
                SymbolFailure(symbol, str(exc), gated=isinstance(exc, QualityGateFailed))
            )
            _emit(on_event, f"{symbol}: failed — {exc}")
    return RunOutcome(market, loaded, skipped, no_data, tuple(failures), ())


def run_window(
    connection: duckdb.DuckDBPyConnection,
    adapter: FreeStockDbAdapter,
    *,
    market: str,
    start: dt.date,
    end: dt.date,
    on_event: Callable[[str], None] | None = None,
) -> RunOutcome:
    """Bring the store up to date over a date range, in two requests plus repairs.

    One request per calendar day in the range for the market's bars, and one for
    every symbol's events — the request count does not grow with the number of
    symbols, which is what makes a daily update minutes rather than hours.

    Two cases need more than the window's own bars, and both are about factor
    series rather than about bars:

    * A symbol seen for the first time is fetched in full, because a listing with
      no earlier rows cannot be patched into existence.
    * A symbol whose events changed has **every** historical factor rewritten.
      ``cum_latest`` moves when a distribution happens, so the whole series
      changes basis; writing only the new day would leave the rest on the old
      one, wrong and entirely normal-looking.
    """
    _require_market(market)
    if start > end:
        raise ValueError("start must not be after end")
    events = _by_symbol(adapter.adjustment_events(adapter.fetch(plan_all_adjustments())))

    frames: list[pd.DataFrame] = []
    names: dict[str, str] = {}
    shares: list[pd.DataFrame] = []
    trading_days: list[dt.date] = []
    for day in _days_between(start, end):
        raw = adapter.fetch(plan_market_day(day))
        try:
            bars = adapter.normalize(raw)
        except SourceEmpty:
            continue  # the service answers a holiday with the same empty array
        trading_days.append(day)
        frames.append(bars)
        names.update(extract_names(raw))
        shares.append(extract_shares(raw))

    if not frames:
        return RunOutcome(market, 0, 0, 0, (), ())

    fresh = pd.concat(frames, ignore_index=True)
    shares_by_symbol = pd.concat(shares, ignore_index=True)
    loaded = 0
    failures: list[SymbolFailure] = []
    for symbol in sorted(fresh["symbol"].unique()):
        try:
            _load_window_symbol(
                connection,
                adapter,
                market=market,
                symbol=str(symbol),
                name=_require_name(names, str(symbol)),
                fresh=fresh.loc[fresh["symbol"] == symbol],
                shares=shares_by_symbol.loc[shares_by_symbol["symbol"] == symbol],
                events=events.get(str(symbol)),
                on_event=on_event,
            )
            loaded += 1
        except SourceUnavailable:
            raise
        except Exception as exc:
            failures.append(
                SymbolFailure(str(symbol), str(exc), gated=isinstance(exc, QualityGateFailed))
            )
            _emit(on_event, f"{symbol}: failed — {exc}")
    # ``no_data`` is structurally zero here: every symbol in this loop came out of
    # the window's own bars, so the source demonstrably has data for it. A symbol
    # the source is silent about only appears on the ``--full`` path, where the
    # universe comes from the code table rather than from a reply.
    return RunOutcome(market, loaded, 0, 0, tuple(failures), tuple(trading_days))


def record_gap(connection: duckdb.DuckDBPyConnection, *, market: str, symbol: str) -> None:
    """Note that the source has nothing for a symbol, so a rerun will not ask again.

    A code with no bars answers HTTP 200 with an empty array rather than an
    error, so without this it looks "not done yet" on every later run.
    """
    write_rows(
        connection,
        SOURCE_GAP,
        pd.DataFrame(
            [
                {
                    "market": market,
                    "symbol": symbol,
                    "observed_at": dt.datetime.now(dt.UTC).isoformat(),
                }
            ],
            columns=pd.Index([column.name for column in SOURCE_GAP.columns]),
        ),
    )


def clear_gap(connection: duckdb.DuckDBPyConnection, *, market: str, symbol: str) -> None:
    """Drop the gap record for a symbol that has started answering."""
    connection.execute(
        f'delete from "{SOURCE_GAP.name}" where market = ? and symbol = ?', [market, symbol]
    )


def trim_load_log(connection: duckdb.DuckDBPyConnection, *, older_than: dt.date) -> int:
    """Drop provenance rows written before ``older_than``; returns how many went.

    A load is one symbol, so ``load_log`` grows with the work rather than with the
    calendar — about a megabyte per full run. Keeping every row forever is not
    the intent, and a table that only grows is a decision nobody made; this is
    the decision, expressed as a function a schedule can call.
    """
    before = _count(connection, f'select count(*) from "{LOAD_LOG.name}"')
    connection.execute(
        f'delete from "{LOAD_LOG.name}" where cast(loaded_at as date) < ?', [older_than]
    )
    return before - _count(connection, f'select count(*) from "{LOAD_LOG.name}"')


def _load_one(
    connection: duckdb.DuckDBPyConnection,
    adapter: FreeStockDbAdapter,
    *,
    market: str,
    symbol: str,
    delisted: bool,
    events: pd.DataFrame | None,
    on_event: Callable[[str], None] | None,
) -> bool:
    """Load one symbol's history. ``False`` when the source has nothing for it."""
    history = adapter.fetch(plan_history(symbol))
    try:
        bars = adapter.normalize(history)
    except SourceEmpty:
        record_gap(connection, market=market, symbol=symbol)
        _emit(on_event, f"{symbol}: the source has no data — recorded, will not be retried")
        return False
    _write(
        connection,
        market=market,
        symbol=symbol,
        name=_name_of(history, symbol),
        delisted=delisted,
        candles=bars,
        factor_days=bars,
        events=events,
        shares=extract_shares(history),
    )
    # After the load commits, not before: a failed load must leave the gap record
    # standing. A crash between the two leaves a stale row, which is harmless —
    # the symbol is in `candles` either way, so the skip decision is the same.
    clear_gap(connection, market=market, symbol=symbol)
    _emit(on_event, f"{symbol}: {len(bars)} bars")
    return True


def _load_window_symbol(
    connection: duckdb.DuckDBPyConnection,
    adapter: FreeStockDbAdapter,
    *,
    market: str,
    symbol: str,
    name: str,
    fresh: pd.DataFrame,
    shares: pd.DataFrame,
    events: pd.DataFrame | None,
    on_event: Callable[[str], None] | None,
) -> None:
    """Write one symbol out of a windowed fetch, repairing its factor series."""
    last_stored = _last_stored_day(connection, symbol)
    if last_stored is None:
        # Never seen: its factor series has no earlier days to repair, and the
        # window's single day is not its history.
        history = adapter.fetch(plan_history(symbol))
        bars = adapter.normalize(history)
        _write(
            connection,
            market=market,
            symbol=symbol,
            name=_name_of(history, symbol),
            delisted=False,
            candles=bars,
            factor_days=bars,
            events=events,
            shares=extract_shares(history),
        )
        _emit(on_event, f"{symbol}: new to the store, {len(bars)} bars")
        return

    # Re-basing every stored day is the *safe* answer and the wrong default: a
    # symbol has thousands of them and only a distribution moves the base they
    # sit on. Doing it unconditionally cost hours for a 19-day catch-up —
    # measured 0.42 symbols/s against a `PLAN.md` requirement of minutes — so it
    # is now asked for rather than assumed.
    if _needs_rebase(events, last_stored):
        stored = _stored_days(connection, symbol)
        # Only the two key columns, so de-duplicating on every column is
        # de-duplicating on the key — and needs no `subset` the stubs mistype.
        factor_days = pd.concat(
            [stored.filter(items=["symbol", "date"]), fresh.filter(items=["symbol", "date"])],
            ignore_index=True,
        ).drop_duplicates(ignore_index=True)
        note = f"{len(factor_days)} days re-based"
    else:
        factor_days = fresh
        note = f"{len(fresh)} days"
    _write(
        connection,
        market=market,
        symbol=symbol,
        name=name,
        delisted=False,
        candles=fresh,
        factor_days=factor_days,
        events=events,
        shares=shares,
    )
    _emit(on_event, f"{symbol}: {len(fresh)} new bars, {note}")


def _last_stored_day(connection: duckdb.DuckDBPyConnection, symbol: str) -> dt.date | None:
    """The newest day the store holds for ``symbol``, or ``None`` if it has none.

    One row rather than the thousands ``_stored_days`` reads: the question here
    is only whether a distribution could have moved the base, and that is a
    question about the end of the series.
    """
    row = connection.execute("select max(date) from candles where symbol = ?", [symbol]).fetchone()
    if row is None or row[0] is None:
        return None
    return dt.date.fromisoformat(str(row[0]))


def _needs_rebase(events: pd.DataFrame | None, last_stored: dt.date) -> bool:
    """Whether the stored factors still sit on the base they were computed on.

    That base is ``cum_latest``, the newest distribution's cumulative factor, and
    it moves only when a *new* distribution appears. One dated on or after the
    symbol's last stored day is one this store has never seen, so every stored
    factor has to be recomputed; an earlier one was already applied — or if it
    was not, recomputing is harmless and this test merely costs the work.

    Erring towards re-basing is deliberate. The other mistake is invisible:
    stale factors keep their arithmetic, pass every invariant, and are simply
    wrong for the whole history.
    """
    if events is None or events.empty:
        return False
    return bool(events["date"].max() >= pd.Timestamp(last_stored))


def _write(
    connection: duckdb.DuckDBPyConnection,
    *,
    market: str,
    symbol: str,
    name: str,
    delisted: bool,
    candles: pd.DataFrame,
    factor_days: pd.DataFrame,
    events: pd.DataFrame | None,
    shares: pd.DataFrame,
) -> None:
    """Gate and write one symbol's four tables in a single transaction."""
    load(
        connection,
        market=market,
        batch=Batch(
            candles=candles,
            adj_factor=daily_factors(_NO_EVENTS if events is None else events, factor_days),
            shares_outstanding=shares,
            symbols=build_symbols(
                codes={symbol[:1]: (symbol,)},
                delisted=(symbol,) if delisted else (),
                names={symbol: name},
            ),
        ),
    )


def _by_symbol(events: pd.DataFrame) -> dict[str, pd.DataFrame]:
    """Index the all-market event reply once, so a per-symbol lookup is not a scan."""
    return {str(symbol): frame for symbol, frame in events.groupby("symbol", sort=False)}


def _require_name(names: Mapping[str, str], symbol: str) -> str:
    """The name a windowed reply should have carried for ``symbol``.

    A window asks for one day at a time, and only an instrument's recent rows
    carry a name — so a symbol that appears in the window but never in a named
    row has none to give. That is worth saying plainly rather than letting a
    ``KeyError`` stand in for it.
    """
    name = names.get(symbol)
    if name is None:
        raise SourceMalformed(f"{symbol}: the window's reply carried no name for it")
    return name


def _name_of(raw: bytes, symbol: str) -> str:
    """The symbol's name, which only the bars carry.

    A reply without one is refused rather than defaulted: ``symbols.name`` is
    NOT NULL, and a fabricated name is worse than a loud failure.
    """
    names = extract_names(raw)
    if symbol not in names:
        raise SourceMalformed(f"{symbol}: the reply carried no name for it")
    return names[symbol]


def _universe(codes: dict[str, tuple[str, ...]], delisted: tuple[str, ...]) -> tuple[str, ...]:
    """Every symbol to load: the live codes plus the delisted ones.

    The delisted are fetched like any other because their history is the point of
    listing them — survivorship bias is stopped by *keeping* those bars — and
    because a name only ever arrives on a bar, so a symbol never fetched can never
    earn a ``symbols`` row either.
    """
    live = [symbol for group in codes.values() for symbol in group]
    return tuple(dict.fromkeys([*live, *delisted]))


def _settled(connection: duckdb.DuckDBPyConnection, market: str) -> set[str]:
    """Symbols a run may skip: those with bars, and those the source has nothing for."""
    with_bars = {
        row[0] for row in connection.execute("select distinct symbol from candles").fetchall()
    }
    gaps = {
        row[0]
        for row in connection.execute(
            f'select symbol from "{SOURCE_GAP.name}" where market = ?', [market]
        ).fetchall()
    }
    return with_bars | gaps


def _stored_days(connection: duckdb.DuckDBPyConnection, symbol: str) -> pd.DataFrame:
    """Every day the store already holds for ``symbol``, key columns only.

    Empty rather than ``None`` for a symbol it has never seen: the caller is
    about to concatenate it, and an empty frame concatenates to the other side
    while ``None`` would need a branch that says nothing.
    """
    rows = connection.execute(
        "select symbol, date from candles where symbol = ?", [symbol]
    ).fetchall()
    frame = pd.DataFrame(rows, columns=pd.Index(["symbol", "date"]))
    # Normalised to what ``normalize`` produces, because DuckDB hands back
    # ``datetime.date`` objects and a ``date`` is not a ``Timestamp``: they are
    # different objects, compare unequal, and survive a ``drop_duplicates`` on
    # the key as two rows. Concatenating the two and de-duplicating then keeps
    # *both*, which `write_rows` refuses as a duplicate primary key — measured
    # on two symbols whose stored history already contained the window's days.
    # This is the third place in this project where two spellings of one date
    # had to be reconciled; the other two are in ``_align_merge_keys``.
    frame["date"] = pd.to_datetime(frame["date"])
    frame["symbol"] = frame["symbol"].astype("object")
    return frame


def _days_between(start: dt.date, end: dt.date) -> Iterator[dt.date]:
    """Every *weekday* in the range; which of them traded is the source's answer.

    Weekends are not asked. Neither the CN nor the TW market has ever held a
    Saturday session, so the only thing a weekend query can return is "nothing"
    — and it is the expensive kind of nothing: ``k1=all:`` for a date with no
    rows makes stockdb scan the cross-section instead of hitting a key. Measured
    on leaf 2026-11-17: 20261116 (Monday) 0.10s, 20261114/20261107 (Saturdays)
    ~8s warm and past the 30s client timeout right after a full sync, which is
    exactly how the daily update raised a false ``source_unavailable``. A ten-day
    window contains four such days.

    Holidays are still asked: that calendar belongs to the source, not to us.
    """
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += dt.timedelta(days=1)


def _require_market(market: str) -> None:
    if market not in MARKETS:
        raise ValueError(f"unknown market {market!r}; this pipeline loads {', '.join(MARKETS)}")


def _emit(on_event: Callable[[str], None] | None, message: str) -> None:
    if on_event is not None:
        on_event(message)
