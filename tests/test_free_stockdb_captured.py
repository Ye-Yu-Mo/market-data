"""The adapter against responses the live service actually sent.

Every other adapter test runs over bytes this repository wrote itself, so it can
only confirm that the parser agrees with its author. These run over captures
(leaf ``127.0.0.1:7899``, 2026-09-23/24 — see ``fixtures/free_stockdb.py``) and
are the ones that can falsify something.

The reason this file exists at all is the one-price day. ``high == low`` gives
the price check a zero-width band, and the source rounds ``amount``, so
``amount / volume`` lands slightly outside it. Against constructed fixtures that
case is unreachable, so the check shipped at a tolerance of ``1e-9`` and
rejected 11 of the 5488 stock rows in a real market day — a bug no amount of
self-written fixtures could surface.

Tightening that to ``1e-3`` was calibrated on the same single day and was also
wrong: scanning twelve symbols' full history turns up one-price days from 2001
where the source disagrees with itself by 2.7e-2. Hence ``LEGACY_OUTLIERS``, and
a tolerance expressed as a factor rather than an epsilon.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false, reportPrivateUsage=false
# Test-only, and the same trade the other test modules make: every `Unknown`
# here crosses into `msgpack` or `pandas`, neither of which ships usable types.
# ``_PRICE_TOLERANCE`` is private but is exactly what these tests pin.

from __future__ import annotations

import datetime as dt

import msgpack
import pytest

from fixtures.free_stockdb import DAILY_FIELDS, load
from market_data.markets.base import SourceEmpty
from market_data.markets.cn import FreeStockDbAdapter, parse_code_table, parse_delisted, parse_keys
from market_data.markets.cn.free_stockdb import _PRICE_TOLERANCE
from market_data.markets.contracts import CANDLES

#: Real one-price days drawn from the 2026-08-31 whole-market response: the
#: three with the largest band excursion of the eleven that exist that day.
ONE_PRICE_DAYS = (
    "daily_oneprice_600371_20260831.bin",
    "daily_oneprice_000011_20260831.bin",
    "daily_oneprice_002731_20260831.bin",
)

#: The widest disagreements in the source's own history, found by scanning ~53k
#: rows across 12 symbols. Both are one-price days from 2001 whose ``pre_close``
#: is impossible as well — 600633 closes at 2.55x it, past any A-share limit —
#: so these are the source's oldest records disagreeing with themselves rather
#: than a units problem. They are what sets the tolerance: no rounding-scale
#: check can accept them, which is why the tolerance is a factor and not an
#: epsilon.
LEGACY_OUTLIERS = (
    "daily_legacy_600633_20010727.bin",
    "daily_legacy_600633_20010706.bin",
)


@pytest.fixture
def adapter() -> FreeStockDbAdapter:
    return FreeStockDbAdapter()


def decode(name: str) -> object:
    return msgpack.unpackb(load(name), raw=False, strict_map_key=False)


# --- the format ------------------------------------------------------------


def test_a_captured_reply_carries_the_measured_header() -> None:
    """map16 with 21 keys — the one shape the documentation never stated."""
    raw = load("daily_single_600633_20260625.bin")
    assert raw[:3] == b"\xde\x00\x15"
    record = decode("daily_single_600633_20260625.bin")
    assert isinstance(record, dict)
    assert tuple(record) == DAILY_FIELDS


def test_a_captured_reply_matches_the_publishers_own_sample() -> None:
    """``调用方式/http/rd_test.py`` prints this exact row; the two must agree.

    An anchor to a source outside this repository: if the vendor's documented
    numbers and our capture ever diverge, one of the two moved.
    """
    record = decode("daily_single_600633_20260625.bin")
    assert isinstance(record, dict)
    assert record == {
        "date": 20260625,
        "code": "600633",
        "name": "浙数文化",
        "open": 10.45,
        "high": 10.62,
        "low": 10.37,
        "close": 10.45,
        "pre_close": 10.52,
        "volume": 18031500,
        "amount": 189010000,
        "turnover": 1.42,
        "pct_chg": -0.67,
        "amplitude": 2.38,
        "is_st": False,
        "vol_ratio": 0.9,
        "total_share": 1268074472,
        "float_share": 1268074472,
        "total_mv": 13251000000,
        "float_mv": 13251000000,
        "pe_ttm": 22.5,
        "pb": 1.3,
    }


def test_the_source_keeps_the_leading_zero_itself() -> None:
    """``code`` arrives as a 6-character string, not an integer to pad later."""
    record = decode("daily_oneprice_000011_20260831.bin")
    assert isinstance(record, dict)
    assert record["code"] == "000011"


# --- the one-price day: the regression this file exists for ----------------


@pytest.mark.parametrize("name", ONE_PRICE_DAYS)
def test_a_real_one_price_day_is_accepted(adapter, name) -> None:
    """A zero-width band plus the source's rounding must not be a rejection."""
    frame = adapter.normalize(load(name))
    assert len(frame) == 1
    assert frame["high"].iloc[0] == frame["low"].iloc[0]


@pytest.mark.parametrize("name", LEGACY_OUTLIERS)
def test_a_legacy_outlier_is_accepted(adapter, name) -> None:
    """The rows that rule out any rounding-scale tolerance."""
    assert len(adapter.normalize(load(name))) == 1


def test_the_tolerance_clears_real_data_and_still_catches_a_unit_error() -> None:
    """Why the check tolerates a factor of ten, in numbers.

    Three bounds; drop any one and a known failure comes back. Every measured row
    must clear the tolerance (or real data is rejected), the smallest unit error
    there is must not (or the check catches nothing), and the worst row must sit
    well above rounding scale (or the check was never about units at all).
    """
    worst = 0.0
    for name in ONE_PRICE_DAYS + LEGACY_OUTLIERS:
        record = decode(name)
        assert isinstance(record, dict)
        ratio = record["amount"] / record["volume"]
        worst = max(worst, abs(ratio - record["close"]) / record["close"])

    assert 1 + worst <= _PRICE_TOLERANCE, "real data would be rejected"
    assert 100.0 / _PRICE_TOLERANCE >= 10.0, "手 against 股, the tightest unit error"
    assert worst > 1e-3, "a rounding-scale tolerance would have sufficed"


def test_an_amount_of_zero_is_skipped_not_judged(adapter) -> None:
    """Funds and bonds carry ``amount = 0``; a zero says nothing about units."""
    record = decode("daily_oneprice_600371_20260831.bin")
    assert isinstance(record, dict)
    record["amount"] = 0
    frame = adapter.normalize(msgpack.packb(record, use_bin_type=True))
    assert frame["amount"].iloc[0] == 0.0


# --- multi-row, real -------------------------------------------------------


def test_a_real_prefix_slice_parses_every_row(adapter) -> None:
    frame = adapter.normalize(load("daily_prefix6006_20260831.bin"))
    assert len(frame) == 78
    assert list(frame.columns) == [column.name for column in CANDLES.columns]
    assert frame["symbol"].nunique() == 78
    assert frame["date"].dt.date.unique().tolist() == [dt.date(2026, 8, 31)]


def test_a_real_range_comes_back_sorted_by_symbol_then_date(adapter) -> None:
    frame = adapter.normalize(load("daily_range_600633.bin"))
    assert len(frame) == 5
    assert [value.date() for value in frame["date"]] == [
        dt.date(2026, 6, 22),
        dt.date(2026, 6, 23),
        dt.date(2026, 6, 24),
        dt.date(2026, 6, 25),
        dt.date(2026, 6, 26),
    ]


def test_a_real_frame_leaves_turnover_null(adapter) -> None:
    """``market_v2.candles.turnover`` holds trade counts; the source sends a
    percentage. Until the two are reconciled the column stays empty rather than
    holding a number that means something else — see ``contracts.py``."""
    frame = adapter.normalize(load("daily_range_600633.bin"))
    assert frame["turnover"].isna().all()


# --- adjustment factors ----------------------------------------------------


def test_a_real_factor_series_is_forward_adjusted(adapter) -> None:
    frame = adapter.normalize_factors(load("adjust_600633.bin"))
    assert len(frame) == 25
    assert set(frame["symbol"]) == {"600633"}
    assert [value.date() for value in frame["date"]] == sorted(
        value.date() for value in frame["date"]
    )
    assert frame["factor"].iloc[-1] == 1.0
    assert (frame["factor"] <= 1.0).all()


def test_a_real_keys_reply_agrees_with_the_get_reply() -> None:
    """Two reply shapes, one series — the key strings must be identical.

    ``cmd=keys`` returns bare key strings and ``cmd=get`` returns ``[key, value]``
    pairs for the very same query. Comparing the raw strings checks the two
    captures describe one series without re-implementing a date parser to do it.
    """
    keys = msgpack.unpackb(load("adjust_keys_600633.bin"), raw=False, strict_map_key=False)
    pairs = decode("adjust_600633.bin")
    assert isinstance(keys, list)
    assert isinstance(pairs, list)
    assert [pair[0] for pair in pairs] == keys

    dates = parse_keys(load("adjust_keys_600633.bin"))
    assert len(dates) == 25
    assert dates == tuple(sorted(dates))
    assert dates[0] == dt.date(1993, 7, 5)
    assert dates[-1] == dt.date(2026, 6, 12)


# --- the symbol universe ---------------------------------------------------


def test_a_real_code_table_has_the_measured_groups() -> None:
    table = parse_code_table(load("codes.bin"))
    assert {group: len(codes) for group, codes in table.items()} == {
        "0": 1493,
        "1": 995,
        "3": 1397,
        "5": 1058,
        "6": 2311,
        "9": 309,
    }
    assert sum(len(codes) for codes in table.values()) == 7563


def test_a_real_delisted_list_parses() -> None:
    """918 *entries* describing 339 distinct codes — the survivorship-bias fix.

    The count is asserted on the reply, not on the symbols table: the source
    repeats a code once per table the ``退市*`` prefix matches, up to 47 times,
    and ``build_symbols`` collapses them because the table is keyed on symbol.
    """
    codes = parse_delisted(load("delisted.bin"))
    assert len(codes) == 918
    assert len(set(codes)) == 339
    assert "600001" in codes


# --- failure semantics, from real bytes ------------------------------------


def test_a_real_empty_reply_is_source_empty(adapter) -> None:
    """A code with no data answers HTTP 200 with a bare empty array."""
    raw = load("empty.bin")
    assert raw == b"\x90"
    with pytest.raises(SourceEmpty):
        adapter.normalize(raw)


def test_a_real_empty_keys_reply_is_source_empty() -> None:
    with pytest.raises(SourceEmpty):
        parse_keys(b"\x90")


# --- witnesses for the quality gate (M3) ------------------------------------
#
# M3's rules have to accept several things that look wrong and are not. Each is
# pinned here by its defining property, so the fixture keeps saying what it was
# captured to say and the gate's calibration has a stable input — the full-market
# and full-history scans behind those thresholds were one-time analyses and are
# deliberately not in the repository.


def test_the_suspended_witness_is_a_fund_with_a_whole_zero_bar() -> None:
    """``volume == 0``: the source's own kind of suspension.

    No *stock* in eight symbols' 40,245 rows has one — this source appears to
    omit suspended days rather than emit an empty bar, and the consumer's
    ``volume > 0`` filter was written for a source that does not. The witness is
    therefore a fund, which is the only shape this source produces.
    """
    record = decode("daily_suspended_162307_20260831.bin")
    assert isinstance(record, dict)
    assert record["volume"] == 0
    assert record["amount"] == 0
    assert record["high"] == record["low"]


def test_the_zero_amount_witness_traded_without_a_printed_value() -> None:
    """``amount == 0`` with real volume: 40 such rows market-wide, none a stock."""
    record = decode("daily_zeroamount_160513_20260831.bin")
    assert isinstance(record, dict)
    assert record["amount"] == 0
    assert record["volume"] > 0


def test_the_st_witness_defeats_any_price_limit_rule() -> None:
    """Why M3 must not gate on 涨跌幅, with or without an ``is_st`` band.

    ``is_st`` is the field ``PLAN.md:189`` names as the reason to calibrate
    against real data, and the data says the calibration has no clean answer:
    ST rows carry single-day moves of 17% and 22% (a resumption gap clears any
    limit), so an ``is_st``-keyed band rejects real history.
    """
    record = decode("daily_st_000010_20260831.bin")
    assert isinstance(record, dict)
    assert record["is_st"] is True
    assert record["high"] != record["low"]
