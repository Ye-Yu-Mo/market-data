"""Orchestration: what one run does, and what it refuses to redo.

The unit of work is a **symbol**, not a date. Two reasons, and the second is the
one that matters: a symbol's adjustment factors are a series — ``daily_factors``
needs all of its dates and all of its events at once — so slicing by date would
split one series across batches and no batch could compute a correct factor.
Cost agrees (5,837 requests of ~1.9 MB against ~7,000 of 2.5 MB), but cost is
the smaller argument.

Resume state is derived from the store, never from a sidecar. A checkpoint file
that is wrong is wrong *silently* — one symbol short, and nobody finds out —
whereas the store is the thing being asked about, and M2's writes are idempotent
and M3's loads are atomic, so re-running is safe by construction.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, the trade every other test module here makes: pytest fixture
# parameters are inherently unannotated, and values coming back out of
# ``pandas`` and ``duckdb`` are untyped.

from __future__ import annotations

import datetime as dt
from collections.abc import Iterable

import pytest

from fixtures.free_stockdb import daily_row, pack
from market_data.markets.cn.free_stockdb import (
    TABLE_ADJUST,
    TABLE_CODES,
    TABLE_DAILY,
    TABLE_DELISTED,
    FreeStockDbAdapter,
    Query,
)
from market_data.offline import pipeline
from market_data.offline.duckdb_store import open_store

#: Three live codes and one delisted. The delisted one is fetched like any other
#: because ``symbols.name`` is NOT NULL and the source only carries names on
#: bars — and because keeping delisted history is the point of listing them.
SYMBOLS = ("600001", "600002", "600003")
DELISTED = ("600999",)
UNIVERSE = SYMBOLS + DELISTED
NAMES = {symbol: f"股票{symbol}" for symbol in UNIVERSE}


def bars_payload(symbol: str, days: int = 3) -> bytes:
    """Consecutive daily bars for one symbol, as the source would send them."""
    return pack(
        [
            daily_row(
                20260601 + offset,
                symbol,
                NAMES[symbol],
                price=10.0 + offset,
                volume=1_000,
                float_share=1_000_000,
            )
            for offset in range(days)
        ]
    )


def one_day_payload(day: int, symbols: Iterable[str] = UNIVERSE) -> bytes:
    """A whole-market reply for one trading day."""
    return pack(
        [
            daily_row(day, symbol, NAMES[symbol], price=10.0, volume=1_000, float_share=1_000_000)
            for symbol in symbols
        ]
    )


def events_payload(*events: tuple[str, int, float]) -> bytes:
    return pack(
        [
            [
                f"复权:{symbol}:{day}",
                {"div": 0.0, "give": 0.0, "trans": 0.0, "mult": 1.0, "cum": cum},
            ]
            for symbol, day, cum in events
        ]
    )


class ScriptedSource(FreeStockDbAdapter):
    """A source that answers from a script and records what it was asked.

    Every request has to be routed for, so an orchestration that fetches
    something it should not — one request per symbol where two would do, or a
    symbol it was supposed to skip — fails instead of quietly succeeding.
    """

    def __init__(self, routes: dict[tuple[str, str | None, str | None], bytes]) -> None:
        super().__init__()
        self.routes = routes
        self.asked: list[tuple[str, str | None, str | None]] = []

    def fetch(self, query: Query) -> bytes:
        key = (query.table, query.k1, query.k2)
        self.asked.append(key)
        if key not in self.routes:
            raise AssertionError(f"unrouted request: {key}")
        return self.routes[key]

    def symbols_asked(self) -> list[str]:
        asked = []
        for _, k1, _ in self.asked:
            if k1 is not None and k1.startswith("key:"):
                asked.append(k1.removeprefix("key:"))
        return asked


def catalogue() -> dict[tuple[str, str | None, str | None], bytes]:
    """What any run needs before it can start: the universe, and every event.

    The events arrive in one all-market reply rather than one per symbol — that
    is what keeps a per-symbol run at *one* request per symbol.
    """
    return {
        (TABLE_CODES, None, None): pack({"6": list(SYMBOLS)}),
        (TABLE_DELISTED, "all:", "all:"): pack(list(DELISTED)),
        (TABLE_ADJUST, "all:", "all:"): events_payload(("600001", 20200101, 1.0)),
    }


def histories(*symbols: str, days: int = 3):
    return {
        (TABLE_DAILY, f"key:{symbol}", "all:"): bars_payload(symbol, days) for symbol in symbols
    }


@pytest.fixture
def store(tmp_path):
    with open_store(tmp_path / "market_a.duckdb") as connection:
        yield connection


# --- full runs --------------------------------------------------------------
#
# `--full` rewrites **in place**: it never clears the store or builds a second
# one, and each symbol is written in its own transaction. A consumer watching a
# rebuild therefore sees each symbol wholly old or wholly new — never half of one
# — and symbols a run does not reach keep what they had. The alternative, a
# temporary file swapped in at the end, would give all-or-nothing across the
# whole store, at the cost of a second copy on disk and of reopening every
# handle. In-place won because it reuses the transaction boundary M3 already
# verified and keeps the invariants true at every instant, not only at the end.


def test_a_full_run_loads_every_symbol_in_the_universe(store) -> None:
    source = ScriptedSource(catalogue() | histories(*UNIVERSE))
    outcome = pipeline.run_full(store, source, market="cn")
    assert outcome.loaded == len(UNIVERSE)
    assert store.execute("select count(distinct symbol) from candles").fetchone() == (
        len(UNIVERSE),
    )
    assert store.execute("select count(*) from symbols").fetchone() == (len(UNIVERSE),)


def test_a_full_run_asks_once_per_symbol(store) -> None:
    """Three requests to set up, then one history request each — no market-day sweep."""
    source = ScriptedSource(catalogue() | histories(*UNIVERSE))
    pipeline.run_full(store, source, market="cn")
    assert source.symbols_asked() == list(UNIVERSE)
    assert len(source.asked) == len(UNIVERSE) + 3


def test_a_second_full_run_skips_what_is_already_there(store) -> None:
    """Re-running the same command is how resume works, so it must be cheap."""
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    again = ScriptedSource(catalogue())
    outcome = pipeline.run_full(store, again, market="cn")
    assert outcome.loaded == 0
    assert outcome.skipped == len(UNIVERSE)
    assert again.symbols_asked() == []


def test_refresh_re_fetches_symbols_that_are_already_there(store) -> None:
    """Skipping is the default because the source revises old rows — hence the way out."""
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    refreshed = ScriptedSource(catalogue() | histories(*UNIVERSE))
    outcome = pipeline.run_full(store, refreshed, market="cn", refresh=True)
    assert outcome.loaded == len(UNIVERSE)
    assert refreshed.symbols_asked() == list(UNIVERSE)


def test_a_symbol_the_source_has_nothing_for_is_recorded_not_retried(store) -> None:
    """A missing code answers HTTP 200 with an empty array, not an error.

    Without a record of that, the symbol looks "not done yet" on every later run
    and is requested forever.
    """
    routes = catalogue() | histories("600002", "600003")
    routes[(TABLE_DAILY, "key:600001", "all:")] = pack([])
    routes[(TABLE_DAILY, "key:600999", "all:")] = pack([])
    first = pipeline.run_full(store, ScriptedSource(routes), market="cn")
    assert first.no_data == 2
    assert sorted(row[0] for row in store.execute("select symbol from source_gap").fetchall()) == [
        "600001",
        "600999",
    ]

    later = ScriptedSource(catalogue())
    outcome = pipeline.run_full(store, later, market="cn")
    assert later.symbols_asked() == []
    assert outcome.no_data == 0
    assert outcome.skipped == len(UNIVERSE)


def test_one_bad_symbol_does_not_stop_the_rest(store) -> None:
    """Each symbol is its own transaction, so a failure is contained to one."""
    routes = catalogue() | histories("600002", "600003", "600999")
    routes[(TABLE_DAILY, "key:600001", "all:")] = b"not msgpack"
    outcome = pipeline.run_full(store, ScriptedSource(routes), market="cn")
    assert outcome.loaded == 3
    assert [failure.symbol for failure in outcome.failures] == ["600001"]
    assert store.execute("select count(distinct symbol) from candles").fetchone() == (3,)


def test_a_full_run_reports_progress_as_it_goes(store) -> None:
    seen: list[str] = []
    pipeline.run_full(
        store,
        ScriptedSource(catalogue() | histories(*UNIVERSE)),
        market="cn",
        on_event=seen.append,
    )
    assert any("600001" in line for line in seen)
    assert len(seen) >= len(UNIVERSE)


# --- windowed runs ----------------------------------------------------------


def window_routes(day: int, events: bytes) -> dict[tuple[str, str | None, str | None], bytes]:
    return {
        (TABLE_DAILY, "all:", f"key:{day}"): one_day_payload(day),
        (TABLE_ADJUST, "all:", "all:"): events,
    }


def test_a_window_costs_two_requests_however_many_symbols(store) -> None:
    """The market-day reply and the event reply — not one request per symbol."""
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    source = ScriptedSource(window_routes(20260604, events_payload(("600001", 20200101, 1.5))))
    outcome = pipeline.run_window(
        store, source, market="cn", start=dt.date(2026, 6, 4), end=dt.date(2026, 6, 4)
    )
    assert outcome.loaded == len(UNIVERSE)
    assert len(source.asked) == 2


def test_a_window_only_writes_the_days_in_the_window(store) -> None:
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    pipeline.run_window(
        store,
        ScriptedSource(window_routes(20260604, events_payload(("600001", 20200101, 1.5)))),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )
    assert store.execute("select max(date) from candles").fetchone() == (dt.date(2026, 6, 4),)
    assert store.execute("select count(*) from candles").fetchone() == (len(UNIVERSE) * 4,)


def test_a_window_never_asks_the_source_about_weekends() -> None:
    """Sat/Sun are never trading days, and asking costs seconds, not milliseconds.

    Every weekend date in the window used to be a whole-market ``all:`` query
    (0.1s on a trading day, ~8s on a date with no rows, past the client timeout
    when cold) whose only possible answer was "nothing".
    """
    days = list(pipeline._days_between(dt.date(2026, 11, 7), dt.date(2026, 11, 17)))
    assert days[0] == dt.date(2026, 11, 9)
    assert all(day.weekday() < 5 for day in days)
    # A weekday holiday is still asked: that calendar is the source's answer.
    assert dt.date(2026, 11, 11) in days


def test_a_new_event_re_writes_every_history_row_of_that_symbol(store) -> None:
    """The shift M3's ``factor_not_forward_adjusted`` would otherwise have to catch.

    ``cum_latest`` moves when a distribution happens, so *every* earlier factor of
    that symbol changes basis. Appending only the new day would leave the rest on
    the old one — wrong, and entirely normal-looking.
    """
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    before = store.execute(
        "select date, factor from adj_factor where symbol = '600001' order by date"
    ).fetchall()

    pipeline.run_window(
        store,
        ScriptedSource(
            window_routes(
                20260604,
                events_payload(("600001", 20200101, 1.5), ("600001", 20260604, 3.0)),
            )
        ),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )

    after = store.execute(
        "select date, factor from adj_factor where symbol = '600001' order by date"
    ).fetchall()
    assert len(after) == len(before) + 1
    assert [factor for _, factor in after[:-1]] != [factor for _, factor in before]
    assert after[-1][1] == 1.0


def test_a_window_with_no_new_event_leaves_the_older_factors_alone(store) -> None:
    """The other half of the re-base decision, and the half that makes it affordable.

    Re-writing every stored day is always correct and cost hours: a 19-day
    catch-up ran at 0.42 symbols/s against `PLAN.md`'s requirement of minutes,
    because every one of 7,887 symbols had its whole factor history re-based to
    learn nineteen new days. A symbol whose newest distribution predates its
    stored history has not moved its base — the stored rows are already right —
    so only the window's days are written.
    """
    full = catalogue() | histories(*UNIVERSE)
    events = events_payload(("600001", 20200101, 1.5), ("600001", 20260602, 3.0))
    full[(TABLE_ADJUST, "all:", "all:")] = events
    pipeline.run_full(store, ScriptedSource(full), market="cn")
    before = store.execute(
        "select date, factor from adj_factor where symbol = '600001' order by date"
    ).fetchall()
    assert [factor for _, factor in before] == pytest.approx([0.5, 1.0, 1.0]), (
        "the stored factors have to be non-trivial, or this test proves nothing"
    )

    # The same events: no distribution has happened since the last stored day.
    pipeline.run_window(
        store,
        ScriptedSource(window_routes(20260604, events)),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )

    after = store.execute(
        "select date, factor from adj_factor where symbol = '600001' order by date"
    ).fetchall()
    assert [factor for _, factor in after[: len(before)]] == [factor for _, factor in before]
    assert len(after) == len(before) + 1
    assert after[-1][1] == 1.0


def test_a_second_window_over_the_same_days_does_not_duplicate_them(store) -> None:
    """The shape a daily update actually has: a window over days already stored.

    A re-base unions the stored days with the window's, and the two sides spell a
    date differently — DuckDB hands back ``datetime.date`` where ``normalize``
    produces ``Timestamp``. They compare unequal, so de-duplicating kept both and
    the write was refused as a duplicate primary key. Two symbols hit it on the
    first catch-up; every symbol would have on the second, which is the one that
    matters.
    """
    events = events_payload(("600001", 20200101, 1.5))
    full = catalogue() | histories(*UNIVERSE)
    full[(TABLE_ADJUST, "all:", "all:")] = events
    pipeline.run_full(store, ScriptedSource(full), market="cn")

    # The store has none of this day yet.
    pipeline.run_window(
        store,
        ScriptedSource(window_routes(20260604, events)),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )
    # The same day again, now stored, with a distribution forcing a re-base.
    outcome = pipeline.run_window(
        store,
        ScriptedSource(
            window_routes(
                20260604, events_payload(("600001", 20200101, 1.5), ("600001", 20260604, 3.0))
            )
        ),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )

    # Asserting on the table alone is not enough, and that is how this test first
    # passed against the bug it was written for: a symbol that fails is caught per
    # symbol, so "no duplicate keys in adj_factor" was true because nothing at all
    # had been written for it. The run has to have succeeded.
    assert outcome.failures == ()
    duplicated = store.execute(
        "select symbol, date from adj_factor group by symbol, date having count(*) > 1"
    ).fetchall()
    assert duplicated == []
    assert store.execute("select count(*) from adj_factor where symbol = '600001'").fetchone() == (
        4,
    )


def test_a_window_loads_the_full_history_of_a_symbol_it_has_never_seen(store) -> None:
    """A listing that appears mid-window has no earlier rows to patch."""
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    newcomer = "600004"
    routes = window_routes(20260604, events_payload(("600001", 20200101, 1.5)))
    routes[(TABLE_DAILY, "all:", "key:20260604")] = pack(
        [
            daily_row(
                20260604, s, NAMES.get(s, "新股"), price=10.0, volume=1_000, float_share=1_000_000
            )
            for s in (*UNIVERSE, newcomer)
        ]
    )
    routes[(TABLE_DAILY, f"key:{newcomer}", "all:")] = pack(
        [
            daily_row(
                20260601 + offset,
                newcomer,
                "新股",
                price=10.0 + offset,
                volume=1_000,
                float_share=1_000_000,
            )
            for offset in range(5)
        ]
    )
    pipeline.run_window(
        store,
        ScriptedSource(routes),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )
    assert store.execute(
        "select count(*) from candles where symbol = ?", [newcomer]
    ).fetchone() == (5,)


def test_a_window_with_no_trading_days_says_so(store) -> None:
    """Silence would be indistinguishable from a job that ran and did nothing."""
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    routes: dict[tuple[str, str | None, str | None], bytes] = {
        (TABLE_DAILY, "all:", "key:20260606"): pack([]),
        (TABLE_ADJUST, "all:", "all:"): events_payload(("600001", 20200101, 1.5)),
    }
    outcome = pipeline.run_window(
        store,
        ScriptedSource(routes),
        market="cn",
        start=dt.date(2026, 6, 6),
        end=dt.date(2026, 6, 6),
    )
    assert outcome.trading_days == ()
    assert outcome.loaded == 0


def test_the_store_keeps_its_invariants_after_a_window(store) -> None:
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    pipeline.run_window(
        store,
        ScriptedSource(window_routes(20260604, events_payload(("600001", 20200101, 1.5)))),
        market="cn",
        start=dt.date(2026, 6, 4),
        end=dt.date(2026, 6, 4),
    )
    gaps = store.execute(
        "select count(*) from candles c left join adj_factor a using (symbol, date)"
        " where a.symbol is null"
    ).fetchone()
    assert gaps == (0,)


def test_a_symbol_with_bars_is_not_also_recorded_as_a_gap(store) -> None:
    """Recording both would be a contradiction: it has data and it has none."""
    routes = catalogue() | histories("600002", "600003", "600999")
    routes[(TABLE_DAILY, "key:600001", "all:")] = pack([])
    pipeline.run_full(store, ScriptedSource(routes), market="cn")
    both = store.execute(
        "select count(*) from candles c join source_gap g using (symbol)"
    ).fetchone()
    assert both == (0,)


def test_a_symbol_that_starts_answering_leaves_the_gap_table(store) -> None:
    """A new listing, or a source that was fixed, must be able to come back."""
    routes = catalogue() | histories("600002", "600003", "600999")
    routes[(TABLE_DAILY, "key:600001", "all:")] = pack([])
    pipeline.run_full(store, ScriptedSource(routes), market="cn")
    assert store.execute("select count(*) from source_gap").fetchone() == (1,)

    routes[(TABLE_DAILY, "key:600001", "all:")] = bars_payload("600001")
    pipeline.run_full(store, ScriptedSource(routes), market="cn", refresh=True)

    assert store.execute("select count(*) from source_gap").fetchone() == (0,)
    assert store.execute("select count(*) from candles where symbol = '600001'").fetchone() == (3,)


def test_the_load_log_grows_with_the_work_and_can_be_trimmed(store) -> None:
    """One row per load, and a load is a symbol — so the log needs a horizon.

    A per-run row would be smaller, and would not be able to say which load put a
    given symbol's rows in. The trade is deliberate; the horizon is too.
    """
    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
    assert store.execute("select count(*) from load_log").fetchone() == (len(UNIVERSE),)

    trimmed = pipeline.trim_load_log(store, older_than=dt.date(2000, 1, 1))
    assert trimmed == 0, "nothing is older than 2000"

    trimmed = pipeline.trim_load_log(store, older_than=dt.date(2100, 1, 1))
    assert trimmed == len(UNIVERSE)
    assert store.execute("select count(*) from load_log").fetchone() == (0,)
    # Trimming provenance must not touch what it describes.
    assert store.execute("select count(distinct symbol) from candles").fetchone() == (
        len(UNIVERSE),
    )


def test_an_unknown_market_is_refused(store) -> None:
    """``--market tw`` silently doing nothing would be worse than refusing it."""
    with pytest.raises(ValueError, match="market"):
        pipeline.run_full(store, ScriptedSource(catalogue()), market="TW")


# --- resume -----------------------------------------------------------------


class InterruptingSource(ScriptedSource):
    """Stops the run the way Ctrl-C does, before the next symbol is fetched.

    Counts symbols rather than requests, so how many requests the setup happens
    to take cannot change what the test is about.
    """

    def __init__(self, routes, *, symbols: int) -> None:
        super().__init__(routes)
        self.symbols = symbols
        self.histories = 0

    def fetch(self, query: Query) -> bytes:
        is_history = query.table == TABLE_DAILY and (query.k1 or "").startswith("key:")
        if is_history:
            self.histories += 1
            if self.histories > self.symbols:
                raise KeyboardInterrupt
        return super().fetch(query)


def contents(connection) -> dict[str, list[tuple[object, ...]]]:
    """Every table's rows, ordered, so two stores can be compared row for row."""
    tables = [row[0] for row in connection.execute("show tables").fetchall()]
    return {
        name: connection.execute(f'select * from "{name}" order by all').fetchall()
        for name in tables
    }


def test_an_interrupted_run_resumes_to_the_same_store(store, tmp_path) -> None:
    """Ctrl-C halfway, then the same command again — the point of skipping.

    A checkpoint file would be the other way to do this, and the reason not to is
    here too: the store is already the answer to "what is done", so there is no
    second thing that can disagree with it.
    """
    with pytest.raises(KeyboardInterrupt):
        pipeline.run_full(
            store,
            InterruptingSource(catalogue() | histories(*UNIVERSE), symbols=2),
            market="cn",
        )
    assert store.execute("select count(distinct symbol) from candles").fetchone() == (2,)

    pipeline.run_full(store, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")

    with open_store(tmp_path / "uninterrupted.duckdb") as whole:
        pipeline.run_full(whole, ScriptedSource(catalogue() | histories(*UNIVERSE)), market="cn")
        # `load_log` carries timestamps, so compare everything else.
        assert {k: v for k, v in contents(store).items() if k != "load_log"} == {
            k: v for k, v in contents(whole).items() if k != "load_log"
        }
