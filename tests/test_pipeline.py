"""The one-way load: fetch, normalize, gate, write — and never half-write.

``write_rows`` commits one table at a time, and a load writes four. If
``candles`` lands and ``adj_factor`` then fails, the store holds a batch whose
bars the consumer's inner join silently discards, and nothing anywhere reports
it. The transaction boundary therefore lives here, at the layer that knows what
"one load" means.

``market_v2`` is opened read-only or not at all in this file; the store under
test is always a temporary file.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, the trade every other test module here makes: pytest fixture
# parameters are inherently unannotated, and values coming back out of
# ``pandas`` and ``duckdb`` are untyped.

from __future__ import annotations

import datetime as dt

import duckdb
import pandas as pd
import pytest

from fixtures.free_stockdb import load as captured
from market_data.markets.cn import (
    FreeStockDbAdapter,
    build_symbols,
    daily_factors,
    extract_shares,
)
from market_data.markets.contracts import (
    LOAD_LOG,
    MARKET_V2_TABLES,
    SOURCE_GAP,
    STORE_TABLES,
    create_table_sql,
    read_contract,
)
from market_data.offline import duckdb_store, pipeline
from market_data.offline.duckdb_store import open_store
from market_data.offline.quality import QualityGateFailed

CODE = "600633"


@pytest.fixture
def store(tmp_path):
    with open_store(tmp_path / "market_a.duckdb") as connection:
        yield connection


def batch_events() -> pd.DataFrame:
    """The captured adjustment events for the symbol the tests load."""
    return FreeStockDbAdapter().adjustment_events(captured("adjust_600633.bin"))


@pytest.fixture
def batch() -> pipeline.Batch:
    """One real load's worth of frames, taken from captured responses."""
    adapter = FreeStockDbAdapter()
    bars = adapter.normalize(captured("daily_range_600633.bin"))
    codes = {"6": tuple(bars["symbol"].unique())}
    return pipeline.Batch(
        candles=bars,
        adj_factor=daily_factors(batch_events(), bars),
        shares_outstanding=extract_shares(captured("daily_range_600633.bin")),
        symbols=build_symbols(
            codes=codes,
            delisted=(),
            names={CODE: "浙数文化"},
        ),
    )


def contents(connection: duckdb.DuckDBPyConnection) -> dict[str, list[tuple[object, ...]]]:
    """Every contract table's rows, ordered by its own key, for before/after."""
    result = {}
    for contract in MARKET_V2_TABLES:
        keys = ", ".join(f'"{name}"' for name in contract.primary_key)
        result[contract.name] = connection.execute(
            f'select * from "{contract.name}" order by {keys}'
        ).fetchall()
    return result


# --- a load that works -----------------------------------------------------


def test_a_complete_load_writes_every_table(store, batch) -> None:
    record = pipeline.load(store, market="CN", batch=batch)
    assert record.candles == len(batch.candles)
    assert store.execute("select count(*) from candles").fetchone() == (len(batch.candles),)
    assert store.execute("select count(*) from adj_factor").fetchone() == (len(batch.adj_factor),)
    assert store.execute("select count(*) from symbols").fetchone() == (len(batch.symbols),)


def test_loading_the_same_batch_twice_is_idempotent(store, batch) -> None:
    pipeline.load(store, market="CN", batch=batch)
    once = contents(store)
    pipeline.load(store, market="CN", batch=batch)
    assert contents(store) == once


# --- and one that must not half-write --------------------------------------


def test_a_failure_partway_through_leaves_every_table_untouched(
    store, batch, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The failure the transaction exists for: three tables written, then one not.

    The stub fails on the third table, so ``candles`` and ``adj_factor`` have
    already been written when it raises. The second batch must differ from the
    first — reloading the same rows would leave a half-write indistinguishable
    from what was already there, which is a test that cannot fail.
    """
    pipeline.load(store, market="CN", batch=batch)
    before = contents(store)

    later = _extend(batch.candles, dt.date(2026, 6, 29))
    real_write = duckdb_store.write_rows
    calls = {"n": 0}

    def fail_on_third(connection, contract, frame):
        calls["n"] += 1
        if calls["n"] == 3:
            raise RuntimeError("injected failure on the third table")
        return real_write(connection, contract, frame)

    monkeypatch.setattr(pipeline, "write_rows", fail_on_third)
    with pytest.raises(RuntimeError, match="injected failure"):
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=later,
                adj_factor=daily_factors(batch_events(), later),
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert contents(store) == before


def test_a_gate_failure_writes_nothing(store, batch) -> None:
    broken = pipeline.Batch(
        candles=_with_broken_ohlc(batch.candles),
        adj_factor=batch.adj_factor,
        shares_outstanding=batch.shares_outstanding,
        symbols=batch.symbols,
    )
    before = contents(store)
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(store, market="CN", batch=broken)
    assert [issue.code for issue in caught.value.report.issues] == ["invalid_ohlc"]
    assert contents(store) == before


def test_the_gate_runs_before_the_first_write(store, batch, monkeypatch) -> None:
    """Order is not a formality: writing first means rolling back afterwards."""
    written: list[str] = []

    def record(connection, contract, frame):
        written.append(contract.name)
        return 0

    monkeypatch.setattr(pipeline, "write_rows", record)
    broken = pipeline.Batch(
        candles=_with_broken_ohlc(batch.candles),
        adj_factor=batch.adj_factor,
        shares_outstanding=batch.shares_outstanding,
        symbols=batch.symbols,
    )
    with pytest.raises(QualityGateFailed):
        pipeline.load(store, market="CN", batch=broken)
    assert written == []


def test_an_empty_batch_is_refused_rather_than_treated_as_success(store, batch) -> None:
    """An empty frame reaching here means the source said nothing, not "no news"."""
    empty = pipeline.Batch(
        candles=batch.candles.iloc[0:0],
        adj_factor=batch.adj_factor.iloc[0:0],
        shares_outstanding=batch.shares_outstanding.iloc[0:0],
        symbols=batch.symbols.iloc[0:0],
    )
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(store, market="CN", batch=empty)
    assert "empty_dataset" in [issue.code for issue in caught.value.report.issues]


def test_a_batch_repeating_a_key_is_refused_by_the_gate(store, batch) -> None:
    """Same verdict as M2's check, reached earlier and with a readable sample."""
    doubled = pipeline.Batch(
        candles=pd.concat([batch.candles, batch.candles], ignore_index=True),
        adj_factor=batch.adj_factor,
        shares_outstanding=batch.shares_outstanding,
        symbols=batch.symbols,
    )
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(store, market="CN", batch=doubled)
    assert "duplicate_key" in [issue.code for issue in caught.value.report.issues]


def test_a_non_positive_factor_is_refused(store, batch) -> None:
    """A factor of zero or less is not a qfq factor at all."""
    broken = batch.adj_factor.copy()
    broken.loc[broken.index[0], "factor"] = -1.0
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=broken,
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "non_positive_factor" in [issue.code for issue in caught.value.report.issues]


def test_a_non_positive_share_count_is_refused(store, batch) -> None:
    broken = batch.shares_outstanding.copy()
    broken.loc[broken.index[0], "shares"] = 0
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=batch.adj_factor,
                shares_outstanding=broken,
                symbols=batch.symbols,
            ),
        )
    assert "non_positive_shares" in [issue.code for issue in caught.value.report.issues]


def _with_broken_ohlc(bars: pd.DataFrame) -> pd.DataFrame:
    broken = bars.copy()
    broken.loc[broken.index[0], "low"] = broken.loc[broken.index[0], "high"] * 2
    return broken


def test_the_record_says_what_was_loaded(store, batch) -> None:
    record = pipeline.load(store, market="CN", batch=batch)
    assert record.market == "CN"
    assert record.adjustment == "qfq"
    assert record.start_date == dt.date(2026, 6, 22)
    assert record.end_date == dt.date(2026, 6, 26)
    assert record.checksum


# --- 功能3: invariants that only the store as a whole can answer -------------
#
# A batch can be internally perfect and still break the store. The consumer
# inner-joins candles to adj_factor, so a bar without a factor row vanishes from
# research without an error — and that is exactly the shape an incremental load
# takes when one of the two fetches fails.


def test_bars_written_without_matching_factors_are_rolled_back(store, batch) -> None:
    before = contents(store)
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=batch.adj_factor.iloc[0:0],
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "factor_gap" in [issue.code for issue in caught.value.report.issues]
    assert contents(store) == before


def test_factor_rows_without_matching_bars_are_rolled_back(store, batch) -> None:
    """The other direction. The reference database has zero of these too."""
    before = contents(store)
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=_extend(batch.adj_factor, dt.date(2026, 7, 1)),
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "orphan_factor" in [issue.code for issue in caught.value.report.issues]
    assert contents(store) == before


def test_a_gap_opened_by_a_later_load_is_still_caught(store, batch) -> None:
    """The reason the check is database-level rather than batch-level.

    The first load is complete and correct. The second fetches bars for a new day
    but no factors — a batch that looks entirely reasonable on its own, and that
    would quietly orphan its own bars. Only the whole store can see it.
    """
    pipeline.load(store, market="CN", batch=batch)
    before = contents(store)
    later = _extend(batch.candles, dt.date(2026, 6, 29))
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=later,
                adj_factor=batch.adj_factor,
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "factor_gap" in [issue.code for issue in caught.value.report.issues]
    assert contents(store) == before


def test_a_backward_adjusted_batch_is_refused(store, batch) -> None:
    """Writing the source's raw ``cum`` is *hfq*, and looks perfectly normal.

    ``cum`` runs to 5.649 for this symbol; stored without dividing by its own
    latest value it is off by that factor for the whole history, with nothing in
    the numbers to give it away.

    The rule that catches it is the latest-day one, not a "nothing above 1.0"
    bound: catching it that way would also have caught every capital reduction
    and fund split, and the reference database carries 2,574 such rows.
    """
    backward = batch.adj_factor.copy()
    backward["factor"] = backward["factor"] * 5.649
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=backward,
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "factor_not_forward_adjusted" in [issue.code for issue in caught.value.report.issues]


def test_a_factor_above_one_is_not_by_itself_wrong(store, batch) -> None:
    """A split or a capital reduction makes old prices higher, not lower.

    ``factor`` above 1.0 is what that looks like under forward adjustment, and
    refusing it refused four Shenzhen funds on the first full load. The
    reference database holds 2,574 such rows across three symbols, so a rule
    against them would have rejected the database the contract was copied from.
    """
    split = batch.adj_factor.copy()
    oldest = split["date"].min()
    split.loc[split["date"] == oldest, "factor"] = 1.25
    pipeline.load(
        store,
        market="CN",
        batch=pipeline.Batch(
            candles=batch.candles,
            adj_factor=split,
            shares_outstanding=batch.shares_outstanding,
            symbols=batch.symbols,
        ),
    )


def test_a_series_whose_latest_day_is_not_one_is_refused(store, batch) -> None:
    """qfq means the most recent trading day carries exactly 1.0."""
    skewed = batch.adj_factor.copy()
    newest = skewed["date"].max()
    skewed.loc[skewed["date"] == newest, "factor"] = 0.98
    with pytest.raises(QualityGateFailed) as caught:
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=batch.candles,
                adj_factor=skewed,
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert "factor_not_forward_adjusted" in [issue.code for issue in caught.value.report.issues]


def _extend(frame: pd.DataFrame, day: dt.date) -> pd.DataFrame:
    """The same frame with one more day, copied from its last row."""
    extra = frame.iloc[[-1]].copy()
    extra["date"] = pd.Timestamp(day)
    return pd.concat([frame, extra], ignore_index=True)


# --- 功能4: provenance ------------------------------------------------------
#
# "Which load put this in the store?" is a question asked *on the database*, so
# the answer lives in it. A sidecar JSON would not travel with a copy, a
# rollback, or a move to another machine — which are exactly the moments the
# question gets asked.


def test_a_load_records_what_it_loaded(store, batch) -> None:
    record = pipeline.load(store, market="CN", batch=batch)
    rows = store.execute("select * from load_log").fetchall()
    assert len(rows) == 1
    stored = dict(zip([c.name for c in LOAD_LOG.columns], rows[0], strict=True))
    assert stored["market"] == "CN"
    assert stored["checksum"] == record.checksum
    assert stored["candles"] == len(batch.candles)
    assert stored["start_date"] == dt.date(2026, 6, 22)
    assert stored["end_date"] == dt.date(2026, 6, 26)
    assert stored["adjustment"] == "qfq"
    assert stored["loaded_at"] == record.loaded_at.isoformat()


def test_a_failed_load_leaves_no_record(store, batch) -> None:
    """Provenance is inside the transaction, so it rolls back with the data."""
    pipeline.load(store, market="CN", batch=batch)
    before = store.execute("select * from load_log").fetchall()
    with pytest.raises(QualityGateFailed):
        pipeline.load(
            store,
            market="CN",
            batch=pipeline.Batch(
                candles=_extend(batch.candles, dt.date(2026, 6, 29)),
                adj_factor=batch.adj_factor,
                shares_outstanding=batch.shares_outstanding,
                symbols=batch.symbols,
            ),
        )
    assert store.execute("select * from load_log").fetchall() == before
    assert store.execute("select count(*) from candles").fetchone() == (len(batch.candles),)


def test_two_loads_are_two_records(store, batch) -> None:
    first = pipeline.load(store, market="CN", batch=batch)
    second = pipeline.load(store, market="CN", batch=batch)
    assert store.execute("select count(*) from load_log").fetchone() == (2,)
    assert first.checksum == second.checksum


def test_a_changed_batch_has_a_different_checksum(store, batch) -> None:
    """What makes the record answer "which batch is in there" rather than "a batch was"."""
    first = pipeline.load(store, market="CN", batch=batch)
    edited = batch.candles.copy()
    edited.loc[edited.index[0], "volume"] = edited.loc[edited.index[0], "volume"] + 1
    second = pipeline.load(
        store,
        market="CN",
        batch=pipeline.Batch(
            candles=edited,
            adj_factor=batch.adj_factor,
            shares_outstanding=batch.shares_outstanding,
            symbols=batch.symbols,
        ),
    )
    assert first.checksum != second.checksum


def test_the_checksum_does_not_depend_on_row_order(store, batch) -> None:
    """The same data is the same load, however the source happened to order it."""
    shuffled = pipeline.Batch(
        candles=batch.candles.iloc[::-1].reset_index(drop=True),
        adj_factor=batch.adj_factor.iloc[::-1].reset_index(drop=True),
        shares_outstanding=batch.shares_outstanding.iloc[::-1].reset_index(drop=True),
        symbols=batch.symbols.iloc[::-1].reset_index(drop=True),
    )
    first = pipeline.load(store, market="CN", batch=batch)
    second = pipeline.load(store, market="CN", batch=shuffled)
    assert first.checksum == second.checksum


def test_a_store_written_before_this_milestone_gains_the_table(tmp_path) -> None:
    """M2's stores must keep working; the new table is created, not required."""
    path = tmp_path / "from_m2.duckdb"
    with duckdb.connect(str(path)) as connection:
        for contract in MARKET_V2_TABLES:
            connection.execute(create_table_sql(contract))
    with open_store(path) as connection:
        assert read_contract(connection, LOAD_LOG.name) == LOAD_LOG


def test_the_store_has_bookkeeping_the_reference_database_does_not() -> None:
    """`market_a` carries two tables of its own, on purpose, recorded in contracts.

    ``load_log`` says what each load wrote; ``source_gap`` says which symbols the
    source has nothing for, which is what lets a resumed run stop asking. Neither
    is a mirror, and both are declared where the mirrored tables are so that
    neither gets "fixed" by someone reasonably assuming drift.
    """
    assert set(STORE_TABLES) - set(MARKET_V2_TABLES) == {LOAD_LOG, SOURCE_GAP}
    assert STORE_TABLES[: len(MARKET_V2_TABLES)] == MARKET_V2_TABLES
