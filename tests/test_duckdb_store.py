"""Building and filling the A-share store.

The store is a DuckDB file whose tables mirror ``market_v2`` column for column,
because ``factor_research`` reads that schema and must be able to read this one
unchanged. Every test here works on a temporary file; nothing touches the shared
data root except the tests that assert it stays untouched.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, the trade the other test modules make: pytest fixture parameters
# are inherently unannotated, and what remains crosses into ``pandas``.

from __future__ import annotations

import duckdb
import pandas as pd
import pytest

from fixtures.free_stockdb import load
from market_data.markets.cn import (
    FreeStockDbAdapter,
    build_symbols,
    daily_factors,
    parse_code_table,
    parse_delisted,
)
from market_data.markets.contracts import (
    ADJ_FACTOR,
    CANDLES,
    MARKET_V2_TABLES,
    REFERENCE_DB_PATH,
    SYMBOLS,
    read_contract,
)
from market_data.offline.duckdb_store import open_store, write_rows
from market_data.offline.manifest import sha256_file

#: The reference database's fingerprint, taken while the test modules are being
#: collected — before any test has run. Comparing against it afterwards is the
#: cheap evidence that a whole session wrote nothing to a 2.3 GB file the rest of
#: this project depends on.
_REFERENCE_AT_COLLECTION = (
    (
        REFERENCE_DB_PATH.stat().st_size,
        sha256_file(REFERENCE_DB_PATH),
    )
    if REFERENCE_DB_PATH.exists()
    else None
)

#: The code table's groups that are stocks. ``1`` and ``5`` are funds and bonds,
#: whose exchange cannot be derived from the leading digit — ``11xxxx`` is a
#: Shanghai bond and ``12xxxx`` a Shenzhen one — so ``build_symbols`` refuses
#: them and the caller filters.
STOCK_GROUPS = ("0", "3", "6", "9")


@pytest.fixture
def store_path(tmp_path):
    return tmp_path / "market_a.duckdb"


@pytest.fixture
def bars() -> pd.DataFrame:
    """Five real bars: the source's own reply, through the adapter.

    Real bytes rather than a hand-built frame, so the types under test are the
    types the pipeline really produces — ``datetime64[s]`` dates, ``object``
    symbols, ``int64`` volume.
    """
    return FreeStockDbAdapter().normalize(load("daily_range_600633.bin"))


# --- 功能1: building the schema --------------------------------------------


def test_opening_a_new_path_creates_every_contract_table(store_path) -> None:
    """The tables are built from the contract, so they read back equal to it."""
    with open_store(store_path) as connection:
        for contract in MARKET_V2_TABLES:
            assert read_contract(connection, contract.name) == contract


def test_opening_an_existing_store_leaves_it_alone(store_path) -> None:
    with open_store(store_path):
        pass
    with open_store(store_path) as connection:
        assert read_contract(connection, CANDLES.name) == CANDLES


def test_the_reference_database_can_never_be_the_target(monkeypatch) -> None:
    """`market_v2` is read-only forever, and the guard runs before the open.

    Asserting the exception is not enough — a guard placed after ``connect``
    would still raise, having already opened a 2.3 GB production database. So
    ``connect`` is replaced with something that records and refuses.
    """
    attempts: list[object] = []

    def refuse(path: object, **kwargs: object) -> object:
        attempts.append(path)
        raise AssertionError(f"connect() was reached with {path!r}")

    monkeypatch.setattr(duckdb, "connect", refuse)
    with pytest.raises(ValueError, match="reference database"):
        open_store(REFERENCE_DB_PATH)
    assert attempts == []


def test_a_path_that_merely_resolves_to_the_reference_is_also_refused() -> None:
    """Symlinks and ``..`` are how a guard gets walked past."""
    twisted = (
        REFERENCE_DB_PATH.parent / ".." / REFERENCE_DB_PATH.parent.name / REFERENCE_DB_PATH.name
    )
    with pytest.raises(ValueError, match="reference database"):
        open_store(twisted)


# --- 功能2: writing, idempotently ------------------------------------------


def test_writing_the_same_batch_twice_changes_nothing(store_path, bars) -> None:
    with open_store(store_path) as connection:
        assert write_rows(connection, CANDLES, bars) == len(bars)
        first = connection.execute("select * from candles order by symbol, date").fetchall()
        write_rows(connection, CANDLES, bars)
        assert connection.execute("select * from candles order by symbol, date").fetchall() == first


def test_a_changed_value_replaces_the_row_rather_than_duplicating_it(store_path, bars) -> None:
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        edited = bars.copy()
        edited.loc[edited.index[0], "close"] = 999.0
        write_rows(connection, CANDLES, edited)
        assert connection.execute("select count(*) from candles").fetchone() == (len(bars),)
        assert connection.execute(
            "select close from candles where symbol = ? and date = ?",
            [edited["symbol"].iloc[0], edited["date"].iloc[0].date()],
        ).fetchone() == (999.0,)


def test_a_batch_with_duplicate_keys_is_refused(store_path, bars) -> None:
    """DuckDB accepts duplicates inside one INSERT and silently keeps the first.

    Measured, not assumed: inserting two rows with the same ``(symbol, date)``
    and different closes leaves the *first* close in the table, with no error.
    That is exactly the silent de-duplication ``PLAN.md`` forbids, so a batch
    that cannot be represented faithfully is refused instead — and the store is
    left as it was.
    """
    doubled = pd.concat([bars, bars.iloc[[0]]], ignore_index=True)
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        before = connection.execute("select count(*) from candles").fetchone()
        with pytest.raises(ValueError, match="duplicate"):
            write_rows(connection, CANDLES, doubled)
        assert connection.execute("select count(*) from candles").fetchone() == before


def test_an_empty_batch_writes_nothing_and_says_so(store_path, bars) -> None:
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        before = connection.execute("select count(*) from candles").fetchone()
        assert write_rows(connection, CANDLES, bars.iloc[0:0]) == 0
        assert connection.execute("select count(*) from candles").fetchone() == before


def test_the_stored_types_are_the_contract_types(store_path, bars) -> None:
    """The one cross-boundary claim worth asserting: nothing was coerced.

    A ``symbol`` that became numeric on the way in has already lost its leading
    zeros and cannot get them back, so the type is checked on both the column
    and a value.
    """
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        assert connection.execute(
            "select typeof(symbol), typeof(date), typeof(volume) from candles limit 1"
        ).fetchone() == ("VARCHAR", "DATE", "BIGINT")


def test_a_symbol_keeps_its_leading_zeros_through_the_store(store_path) -> None:
    """``000011`` is the case: it survives msgpack, pandas and DuckDB."""
    frame = FreeStockDbAdapter().normalize(load("daily_oneprice_000011_20260831.bin"))
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, frame)
        assert connection.execute("select symbol from candles").fetchone() == ("000011",)
        assert connection.execute(
            "select count(*) from candles where symbol = '000011'"
        ).fetchone() == (1,)


# --- 功能3: the factor table must line up with the bars -----------------------


def test_the_factor_table_covers_exactly_the_days_the_bars_do(store_path) -> None:
    """M2's core assertion, and the one the milestone's original acceptance missed.

    The consumer inner-joins these two tables, so a bar without a factor row
    vanishes from the result and a factor row without a bar is dead weight. The
    reference database has zero of each — 10,700,422 rows in both, no exception
    either way. Writing the source's distribution *events* instead of daily rows
    leaves thousands of orphaned bars and reports nothing at all, which is why
    "the loader can open it" was never going to catch it.
    """
    bars = FreeStockDbAdapter().normalize(load("daily_range_600633.bin"))
    events = FreeStockDbAdapter().adjustment_events(load("adjust_600633.bin"))
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        write_rows(connection, ADJ_FACTOR, daily_factors(events, bars))
        orphaned_bars = connection.execute(
            "select count(*) from candles c left join adj_factor a using (symbol, date)"
            " where a.symbol is null"
        ).fetchone()
        orphaned_factors = connection.execute(
            "select count(*) from adj_factor a left join candles c using (symbol, date)"
            " where c.symbol is null"
        ).fetchone()
        assert (
            connection.execute("select count(*) from candles").fetchone()
            == connection.execute("select count(*) from adj_factor").fetchone()
        )
    assert orphaned_bars == (0,)
    assert orphaned_factors == (0,)


def test_the_factor_expansion_accepts_bars_read_back_from_the_store(store_path) -> None:
    """A frame read back out of the store is a legitimate trading calendar.

    ``normalize`` casts ``symbol`` to ``object``; DuckDB hands back pandas 3's
    ``str`` dtype; and ``merge_asof(by="symbol")`` refuses two frames whose key
    dtypes differ. Neither the error nor the column name says which key is at
    fault, so this is written against the read-back path rather than a cast.
    """
    adapter = FreeStockDbAdapter()
    bars = adapter.normalize(load("daily_range_600633.bin"))
    events = adapter.adjustment_events(load("adjust_600633.bin"))
    with open_store(store_path) as connection:
        write_rows(connection, CANDLES, bars)
        read_back = connection.execute("select symbol, date from candles").df()

    assert read_back["symbol"].dtype != bars["symbol"].dtype, "the premise of this test"
    assert len(daily_factors(events, read_back)) == len(bars)


def test_a_moved_distribution_shifts_every_earlier_day(store_path) -> None:
    """A distribution rescales history instead of appending to it.

    It moves ``cum_latest``, and every factor is ``cum / cum_latest``, so all the
    days before it change value while the row count stays put. ``DO NOTHING``
    would leave them on the old base — silently, because nothing about the table
    looks wrong afterwards. The bars here straddle an event so there is something
    to move; ``test_writing_the_same_batch_twice_changes_nothing`` covers the
    case where nothing should.
    """
    events = FreeStockDbAdapter().adjustment_events(load("adjust_600633.bin"))
    days = pd.DataFrame(
        {
            "symbol": ["600633", "600633", "600633"],
            "date": pd.to_datetime(["2026-06-11", "2026-06-12", "2026-06-15"]),
        }
    )
    moved = events.copy()
    moved.loc[moved.index[-1], "cum"] = moved["cum"].iloc[-1] * 1.1

    with open_store(store_path) as connection:
        write_rows(connection, ADJ_FACTOR, daily_factors(events, days))
        before = connection.execute("select date, factor from adj_factor order by date").fetchall()
        write_rows(connection, ADJ_FACTOR, daily_factors(moved, days))
        after = connection.execute("select date, factor from adj_factor order by date").fetchall()

    assert len(before) == len(after) == 3, "an overwrite, not an append"
    assert after[0][1] != before[0][1], "the day before the event moves with the new base"
    assert after[1:] == before[1:], "days from the last event on stay exactly 1.0"


# --- the reference database is never touched --------------------------------


def test_the_reference_database_is_byte_for_byte_what_it_was() -> None:
    """A whole test session's worth of writing, and none of it landed there."""
    if _REFERENCE_AT_COLLECTION is None:
        pytest.skip(f"reference database missing: {REFERENCE_DB_PATH}")
    assert (
        REFERENCE_DB_PATH.stat().st_size,
        sha256_file(REFERENCE_DB_PATH),
    ) == _REFERENCE_AT_COLLECTION


# --- 功能4: the two bookkeeping tables --------------------------------------


def test_the_store_records_the_schema_version_it_was_built_at(store_path) -> None:
    """One row, not the five ``market_v2`` carries.

    That table is a migration ledger — a row per migration ever applied, with the
    current version being ``MAX(version)``. This store replays none of them; it is
    created directly in their final shape, so five rows would assert a history it
    never had.
    """
    with open_store(store_path) as connection:
        assert connection.execute("select version from schema_version").fetchall() == [(5,)]


def test_reopening_does_not_add_a_second_version_row(store_path) -> None:
    with open_store(store_path):
        pass
    with open_store(store_path) as connection:
        assert connection.execute("select count(*) from schema_version").fetchone() == (1,)


def test_symbols_cover_the_stock_groups_plus_the_delisted(store_path) -> None:
    """5837 rows where ``market_v2.symbols`` is empty, and that is the point.

    ``PLAN.md`` names survivorship bias as something to stop before it reaches
    the database; the delisted codes are how that is stopped. Same columns,
    more rows — nothing a consumer reads changes.
    """
    table = parse_code_table(load("codes.bin"))
    codes = {group: table[group] for group in STOCK_GROUPS}
    delisted = parse_delisted(load("delisted.bin"))
    names = {code: f"股票{code}" for group in codes.values() for code in group}
    names.update({code: f"退市{code}" for code in delisted if code not in names})
    frame = build_symbols(codes=codes, delisted=delisted, names=names)

    with open_store(store_path) as connection:
        assert write_rows(connection, SYMBOLS, frame) == len(frame)
        assert connection.execute("select count(*) from symbols").fetchone() == (len(frame),)
        assert connection.execute("select distinct status from symbols order by 1").fetchall() == [
            ("delisted",),
            ("listed",),
        ]


def test_a_symbol_status_outside_the_contract_is_refused_by_the_database(store_path) -> None:
    """The contract's CHECK is doing its job, not merely decorating the DDL."""
    frame = pd.DataFrame(
        [{"symbol": "600633", "name": "浙数文化", "market": "SH", "status": "halted"}],
        columns=pd.Index([column.name for column in SYMBOLS.columns]),
    )
    with open_store(store_path) as connection:
        with pytest.raises(duckdb.ConstraintException):
            write_rows(connection, SYMBOLS, frame)
        assert connection.execute("select count(*) from symbols").fetchone() == (0,)
