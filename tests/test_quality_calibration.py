"""What the quality gate does to real market data, and to real bad data.

Every rule the gate enforces is calibrated against captured responses, because
the alternative has already been tried twice and failed twice: a price-unit
tolerance was set to ``1e-9`` from first principles and rejected 4.5% of a whole
market day, then re-set to ``1e-3`` from a single day and still rejected ~10 of
53,039 rows. Both thresholds were guesses. The numbers behind the current rules
live in ``offline/quality.py`` next to the rules themselves; this file is the
evidence that they behave.

The categories below are not hypothetical. Each one is a real response kept in
``tests/fixtures/free_stockdb/`` precisely because a rule that rejected it would
look perfectly reasonable and would quietly remove history.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, the trade every other test module here makes: pytest fixture
# parameters are inherently unannotated, and values coming back out of
# ``pandas`` are untyped.

from __future__ import annotations

import datetime as dt

import pandas as pd
import pytest

from fixtures.free_stockdb import load
from market_data.markets.base import SourceUnavailable
from market_data.markets.cn import (
    FreeStockDbAdapter,
    plan_history,
    plan_market_day,
)
from market_data.offline.quality import QualityGateFailed, check_candles, require_clean

#: One real response per boundary category. A rule that rejects any of these is
#: wrong, and the fixture is what makes that falsifiable rather than arguable.
LEGITIMATE = {
    "one-price-large-cap": "daily_oneprice_600371_20260831.bin",
    "one-price-leading-zero": "daily_oneprice_000011_20260831.bin",
    "one-price-small": "daily_oneprice_002731_20260831.bin",
    "zero-amount-bond-fund": "daily_zeroamount_160513_20260831.bin",
    "suspended-fund": "daily_suspended_162307_20260831.bin",
    "st-stock": "daily_st_000010_20260831.bin",
    "delisted-no-trade-day": "daily_delisted_600421_20260624.bin",
    "legacy-2001-07-06": "daily_legacy_600633_20010706.bin",
    "legacy-2001-07-27": "daily_legacy_600633_20010727.bin",
    "ordinary-day": "daily_single_600633_20260625.bin",
}


@pytest.fixture
def adapter() -> FreeStockDbAdapter:
    return FreeStockDbAdapter()


def frame(name: str) -> pd.DataFrame:
    """The captured reply, normalized exactly as the pipeline would."""
    return FreeStockDbAdapter().normalize(load(name))


# --- the gate must let real data through -----------------------------------


@pytest.mark.parametrize("name", LEGITIMATE.values(), ids=list(LEGITIMATE))
def test_a_real_boundary_case_passes_the_gate(adapter, name) -> None:
    report = check_candles(frame(name), "CN")
    assert report.ok, [issue.message for issue in report.issues]


def test_a_whole_real_slice_passes_the_gate() -> None:
    """78 symbols on one real trading day, gate-clean."""
    report = check_candles(frame("daily_prefix6006_20260831.bin"), "CN")
    assert report.ok, [issue.message for issue in report.issues]
    assert report.rows == 78
    assert report.symbols == 78


def test_no_price_limit_rule_exists() -> None:
    """Measured: ST stocks move 17.29% and 22.78% in a day, legitimately.

    Those are suspension-resumption gaps, and no threshold can tell them from
    corruption — which is why the gate has no price-limit rule, and why
    stratifying by ``is_st`` is not a fix either. Expressed on real bars: a 30%
    overnight gap, well past any A-share limit, has to pass.
    """
    bars = frame("daily_range_600633.bin")
    gapped = bars.loc[0, "close"] * 1.30
    bars.loc[1, ["open", "high", "low", "close"]] = gapped
    assert check_candles(bars, "CN").ok


def test_the_unreliable_fields_never_reach_the_gate() -> None:
    """000010 contradicts ``close / pre_close - 1`` on 44.1% of its 5463 rows.

    The disagreement is concentrated in 2000-2013, so a rule built on either
    field would reject a decade of real history. The guarantee is structural
    rather than a matter of remembering: the adapter drops both fields when it
    maps a reply onto the contract, so no rule here can read them.
    """
    bars = frame("daily_st_000010_20260831.bin")
    assert "pct_chg" not in bars.columns
    assert "pre_close" not in bars.columns
    assert check_candles(bars, "CN").ok


def test_a_real_delisted_symbols_history_passes_the_gate() -> None:
    """The 339 delisted codes still trade nothing on some days, and must load.

    Survivorship bias is something ``PLAN.md`` asks to stop *before* the database
    — by keeping the delisted symbols, not by rejecting them. A symbol in its
    delisting period has days with no trades at all, recorded as zeros for
    ``open``/``high``/``low`` with ``close`` carried forward, and the bounds check
    has to read that as "no bounds to violate" rather than as corruption.

    This is a real capture, and that matters: the earlier version of this test
    composed a *live* bar with a delisted *code* — its own docstring named the
    missing capture — and passed while the gate rejected the real row.
    """
    bars = frame("daily_delisted_600421_20260624.bin")
    assert bars["volume"].iloc[0] == 0, "the premise: a day with no trades"
    assert bars["high"].iloc[0] == 0
    assert bars["close"].iloc[0] != 0
    assert check_candles(bars, "CN").ok


def test_the_no_trade_exemption_does_not_blind_the_bounds_check() -> None:
    """The exemption is about what the numbers mean, not a hole in the rule.

    A day that traded still has to satisfy its own bounds. Widening the exemption
    — to every row, or dropping the rule — fails here.
    """
    bars = frame("daily_range_600633.bin")
    traded = bars["volume"] > 0
    assert traded.all(), "the premise: every bar here traded"
    bars.loc[traded, "low"] = bars.loc[traded, "high"] + 1
    report = check_candles(bars, "CN")
    assert not report.ok
    assert [issue.code for issue in report.issues] == ["invalid_ohlc"]


# --- and must still catch real bad data ------------------------------------


def test_an_ohlc_violation_is_caught() -> None:
    bars = frame("daily_single_600633_20260625.bin")
    bars.loc[0, "low"] = bars.loc[0, "high"] * 2
    report = check_candles(bars, "CN")
    assert [issue.code for issue in report.issues] == ["invalid_ohlc"]


def test_a_duplicate_key_is_caught_whole_batch() -> None:
    """The semantics M2 already chose, restated where the gate can report it.

    M2's ``_require_unique_keys`` refuses a batch with a repeated
    ``(symbol, date)`` because DuckDB silently keeps the first row. The gate
    reaches the same verdict with a sample of the offenders, so the two layers
    agree and only the reporting granularity differs.
    """
    bars = pd.concat([frame("daily_range_600633.bin")] * 2, ignore_index=True)
    report = check_candles(bars, "CN")
    assert [issue.code for issue in report.issues] == ["duplicate_key"]
    assert report.issues[0].count == 10
    assert report.issues[0].sample


def test_a_negative_value_is_caught() -> None:
    bars = frame("daily_single_600633_20260625.bin")
    bars.loc[0, "volume"] = -1
    assert [issue.code for issue in check_candles(bars, "CN").issues] == ["negative_value"]


def test_a_non_numeric_ohlc_value_is_caught() -> None:
    bars = frame("daily_single_600633_20260625.bin")
    bars["close"] = bars["close"].astype(object)
    bars.loc[0, "close"] = "not a number"
    assert [issue.code for issue in check_candles(bars, "CN").issues] == ["invalid_numeric"]


def test_an_unparsable_date_is_caught() -> None:
    bars = frame("daily_single_600633_20260625.bin")
    bars["date"] = bars["date"].astype(object)
    bars.loc[0, "date"] = "2026-13-45"
    assert [issue.code for issue in check_candles(bars, "CN").issues] == ["invalid_date"]


def test_an_empty_batch_is_not_a_silent_success() -> None:
    assert [
        issue.code
        for issue in check_candles(frame("daily_range_600633.bin").iloc[0:0], "CN").issues
    ] == ["empty_dataset"]


# --- failure semantics -----------------------------------------------------


def test_a_gate_failure_carries_the_report() -> None:
    """``except QualityGateFailed as exc: exc.report.issues`` — not a bare string."""
    bars = frame("daily_single_600633_20260625.bin")
    bars.loc[0, "low"] = bars.loc[0, "high"] * 2
    with pytest.raises(QualityGateFailed) as caught:
        require_clean(check_candles(bars, "CN"))
    assert [issue.code for issue in caught.value.report.issues] == ["invalid_ohlc"]
    assert caught.value.report.rows == 1


def test_the_failure_is_still_a_value_error() -> None:
    """``require_clean``'s public behaviour is frozen; only its type is sharpened.

    ``PLAN.md`` lists ``check_candles`` / ``require_clean`` among the interfaces
    that must not break, so callers written against ``ValueError`` keep working.
    """
    with pytest.raises(ValueError, match="quality gate failed"):
        require_clean(check_candles(frame("daily_single_600633_20260625.bin").iloc[0:0], "CN"))


def test_a_clean_report_comes_back_unchanged() -> None:
    report = check_candles(frame("daily_range_600633.bin"), "CN")
    assert require_clean(report) is report
    assert report.min_date == dt.date(2026, 6, 22).isoformat()
    assert report.max_date == dt.date(2026, 6, 26).isoformat()


# --- re-calibrating (layer three) -------------------------------------------
#
# The numbers in ``quality.py`` came from scanning far more data than belongs in
# a repository: 7,516 rows of one market day and 40,245 rows of eight symbols'
# history. Those scans are a tool, not a fixture — run them when a threshold is
# touched, and never read a skip as a pass. The fixtures above are the defence;
# this is how they get re-derived.


@pytest.mark.network
def test_a_whole_market_day_still_calibrates_the_same_way() -> None:
    """Re-derives the market-day numbers: 38 one-price rows, 2 suspended funds.

    The date is the one the thresholds were measured on, so the counts are
    comparable rather than merely plausible.
    """
    adapter = FreeStockDbAdapter()
    try:
        raw = adapter.fetch(plan_market_day(dt.date(2026, 8, 31)))
    except SourceUnavailable as exc:
        pytest.skip(f"free-stockdb is not reachable: {exc}")
    bars = adapter.normalize(raw)
    assert check_candles(bars, "CN").ok
    # If the categories the rules were calibrated against have gone, this run
    # says nothing about them and passing would be misleading.
    assert (bars["high"] == bars["low"]).any(), "no one-price day to calibrate against"
    assert (bars["volume"] == 0).any(), "no suspended row to calibrate against"


@pytest.mark.network
def test_one_symbols_full_history_still_calibrates_the_same_way() -> None:
    """Where the 2001-2002 contradictions live, and where they must stay accepted."""
    adapter = FreeStockDbAdapter()
    try:
        raw = adapter.fetch(plan_history("600633"))
    except SourceUnavailable as exc:
        pytest.skip(f"free-stockdb is not reachable: {exc}")
    bars = adapter.normalize(raw)
    assert len(bars) > 5_000, "this symbol's history is the sample the rules were set on"
    assert check_candles(bars, "CN").ok
