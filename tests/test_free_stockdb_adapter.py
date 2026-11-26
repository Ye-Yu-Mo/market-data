"""Behaviour of the free-stockdb adapter, against constructed fixtures.

Nothing here opens a socket. ``fetch`` is exercised through a patched
``urlopen`` so the failure mapping is covered offline; everything else is a pure
function over stored bytes.

The bytes here are *constructed* (``fixtures/free_stockdb.py``) and exist to be
wrong in one chosen way — a bad unit, a missing column. What the source actually
sends is pinned separately, from captures, in
``test_free_stockdb_captured.py``.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownLambdaType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, and the same trade `test_alerts.py` makes: pytest fixture parameters
# are inherently unannotated, and every remaining `Unknown` crosses into
# `pandas` or `msgpack`, neither of which ships usable types.

from __future__ import annotations

import datetime as dt
import http.client
import io
import urllib.error
import urllib.request

import msgpack
import pandas as pd
import pytest

from fixtures.free_stockdb import DAILY_FIELDS, adjust_event, daily_row, load, pack
from market_data.markets.base import SourceEmpty, SourceMalformed, SourceUnavailable
from market_data.markets.cn.free_stockdb import (
    DEFAULT_BASE_URL,
    TABLE_ADJUST,
    TABLE_DAILY,
    FreeStockDbAdapter,
    Query,
    build_symbols,
    daily_factors,
    extract_names,
    extract_shares,
    parse_code_table,
    parse_delisted,
    parse_keys,
    plan_adjustments,
    plan_code_table,
    plan_delisted,
    plan_history,
    plan_market_day,
)
from market_data.markets.contracts import ADJ_FACTOR, CANDLES, SHARES_OUTSTANDING, SYMBOLS

DAY = 20260625
NEXT_DAY = 20260626
CODE = "600633"
FLOAT_SHARE = 70_000_000.0


def row(day: int = DAY, code: str = CODE, *, price: float = 10.48, volume: int = 1_000_000):
    return daily_row(day, code, "浙数文化", price=price, volume=volume, float_share=FLOAT_SHARE)


@pytest.fixture
def adapter() -> FreeStockDbAdapter:
    return FreeStockDbAdapter()


class _Response:
    """The slice of ``http.client.HTTPResponse`` that ``fetch`` uses."""

    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> _Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class StubService:
    """Stands in for ``urlopen``: records the URLs asked for, serves fixed bytes."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.urls: list[str] = []

    def __call__(self, url: str, timeout: float) -> _Response:
        self.urls.append(url)
        return _Response(self.payload)


def stub_service(monkeypatch: pytest.MonkeyPatch, payload: bytes) -> StubService:
    stub = StubService(payload)
    monkeypatch.setattr(urllib.request, "urlopen", stub)
    return stub


# --- the wire format -------------------------------------------------------


def test_single_day_response_carries_the_measured_header() -> None:
    """A real 日k response starts map16 with 21 keys; the fixture must too."""
    raw = pack(row())
    assert raw[:3] == b"\xde\x00\x15"
    assert len(msgpack.unpackb(raw, raw=False, strict_map_key=False)) == len(DAILY_FIELDS)


# --- response shapes -------------------------------------------------------


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param(lambda r: r, id="bare-map"),
        pytest.param(lambda r: [r], id="array-of-maps"),
        pytest.param(lambda r: [[f"日k:{CODE}:{DAY}", r]], id="array-of-pairs"),
    ],
)
def test_every_record_shape_normalizes_the_same(adapter, payload) -> None:
    frame = adapter.normalize(pack(payload(row())))
    assert frame["symbol"].tolist() == [CODE]
    assert frame["close"].tolist() == [10.48]


def test_array_of_maps_yields_one_row_per_record(adapter) -> None:
    frame = adapter.normalize(pack([row(), row(NEXT_DAY)]))
    assert len(frame) == 2


def test_keys_shape_parses_dates() -> None:
    raw = pack([f"复权:{CODE}:19930705", f"复权:{CODE}:20260612"])
    assert parse_keys(raw) == (dt.date(1993, 7, 5), dt.date(2026, 6, 12))


# --- field mapping ---------------------------------------------------------


def test_normalize_produces_exactly_the_contract_columns(adapter) -> None:
    frame = adapter.normalize(pack(row()))
    assert list(frame.columns) == [column.name for column in CANDLES.columns]


def test_excluded_symbol_is_dropped_before_the_unit_gate(adapter) -> None:
    """513330 的 `volume` 在一部分历史里是「手」—— 整只排除, 而不是替它乘 100.

    证据 (2026-10-20 实测 leaf 的服务): 1,376 行里 **810** 行
    ``amount / volume / close ≈ 100``, 其余 ≈ 1, 而 `amount` 跨这些日子是连续的。
    所以 bar 可用、`volume` 这一列不可用; 哪一天用的是哪种口径只能从比值本身看出来
    —— 乘 100 就是替源编数据。没有这条排除时, 单位门禁每次更新都拒掉它,
    而一只坏标的会让整次日更以"部分失败"(exit 5)结束 —— 永久状况变成了告警噪音。
    """
    by_hand = daily_row(
        DAY,
        "513330",
        "恒生互联网ETF华夏",
        price=0.341,
        volume=26_521,  # 股数的 1/100
        amount=904_376.0,
        float_share=FLOAT_SHARE,
    )
    good = row()

    frame = adapter.normalize(
        pack([[f"日k:513330:{DAY}", by_hand], [f"日k:{CODE}:{DAY}", good]])
    )

    assert frame["symbol"].tolist() == [CODE], "被排除的标的不该出现在 frame 里"
    assert len(frame) == 1, "排除只该丢那一只, 不该动别的"


def test_leading_zeros_survive(adapter) -> None:
    frame = adapter.normalize(pack(row(code="000001")))
    assert frame["symbol"].tolist() == ["000001"]


def test_date_is_a_real_date_not_a_number(adapter) -> None:
    frame = adapter.normalize(pack(row()))
    assert frame["date"].dtype.kind == "M"
    assert [value.date() for value in frame["date"]] == [dt.date(2026, 6, 25)]


def test_volume_is_integral_and_amount_is_float(adapter) -> None:
    frame = adapter.normalize(pack(row()))
    assert frame["volume"].dtype == "int64"
    assert frame["amount"].dtype == "float64"


@pytest.mark.parametrize("as_float", [False, True], ids=["int-encoded", "float-encoded"])
def test_unstable_numeric_encoding_normalizes_identically(adapter, as_float) -> None:
    """The source sends the same quantity as int one day and float the next."""
    record = row()
    if as_float:
        record["volume"] = float(record["volume"])
        record["amount"] = float(record["amount"])
    frame = adapter.normalize(pack(record))
    assert frame["volume"].tolist() == [1_000_000]
    assert frame["amount"].tolist() == [10.48 * 1_000_000]


def test_rows_come_back_sorted_by_symbol_then_date(adapter) -> None:
    payload = [row(NEXT_DAY), row(DAY), row(NEXT_DAY, code="000001"), row(DAY, code="000001")]
    frame = adapter.normalize(pack(payload))
    assert list(zip(frame["symbol"], frame["date"].dt.date, strict=True)) == [
        ("000001", dt.date(2026, 6, 25)),
        ("000001", dt.date(2026, 6, 26)),
        ("600633", dt.date(2026, 6, 25)),
        ("600633", dt.date(2026, 6, 26)),
    ]


# --- nullability -----------------------------------------------------------


def test_turnover_is_never_written(adapter) -> None:
    """Two different quantities share the name, so the column stays NULL.

    The source's value is a 换手率 percentage; the reference database's column
    holds integer trade counts. Writing either into the other is the failure
    ``contracts.py`` warns about, so nothing is written until it is settled.
    """
    frame = adapter.normalize(pack([row(), row(NEXT_DAY)]))
    assert frame["turnover"].isna().all()


def test_missing_open_keeps_the_bar(adapter) -> None:
    """The contract lets consumers keep a bar with no open."""
    record = row()
    del record["open"]
    frame = adapter.normalize(pack(record))
    assert len(frame) == 1
    assert pd.isna(frame["open"].iloc[0])


def test_missing_close_is_rejected(adapter) -> None:
    """close is NOT NULL in the contract, so a bar without one is not usable."""
    record = row()
    del record["close"]
    with pytest.raises(SourceMalformed):
        adapter.normalize(pack(record))


def test_missing_amount_is_rejected(adapter) -> None:
    """Consumers substitute 0 for a missing amount without complaining."""
    record = row()
    del record["amount"]
    with pytest.raises(SourceMalformed, match="amount"):
        adapter.normalize(pack(record))


# --- unit assertions -------------------------------------------------------


def test_units_hold_on_a_well_formed_row(adapter) -> None:
    assert len(adapter.normalize(pack(row()))) == 1


def test_amount_in_wrong_units_is_rejected(adapter) -> None:
    """万元 instead of 元: amount / volume lands far outside [low, high]."""
    record = row()
    record["amount"] = record["amount"] / 10_000
    with pytest.raises(SourceMalformed, match="amount"):
        adapter.normalize(pack(record))


def test_a_one_price_day_with_a_rounded_amount_is_accepted(adapter) -> None:
    """The real case the tolerance exists for — 600633 on 20260831.

    A one-price day has a zero-width band, so ``amount / volume`` has to land
    exactly on it, but the source rounds ``amount``: here it misses by 5e-4
    relatively. That is four orders of magnitude below any unit error.
    """
    record = daily_row(
        DAY,
        CODE,
        "浙数文化",
        price=6.65,
        volume=1_000_000,
        float_share=FLOAT_SHARE,
        spread=0.0,
        amount=6_653_372.0,
    )
    frame = adapter.normalize(pack(record))
    assert frame["close"].tolist() == [6.65]


def test_the_smallest_unit_error_is_still_rejected(adapter) -> None:
    """手 instead of 股: a factor of 100, the tightest unit mistake there is.

    This is a unit check, not a data-quality check. The source's own history
    contains one-price days where ``amount`` and the day's range disagree by
    2.7e-2 (``test_free_stockdb_captured.py``), so the tolerance cannot be tight
    enough to reject a 1% discrepancy as well — the two requirements are
    mutually exclusive, and accepting real data wins.
    """
    record = daily_row(
        DAY, CODE, "浙数文化", price=10.0, volume=1_000, float_share=FLOAT_SHARE, spread=0.0
    )
    record["volume"] = record["volume"] / 100
    with pytest.raises(SourceMalformed, match="amount"):
        adapter.normalize(pack(record))


def test_one_bad_row_does_not_condemn_the_symbol(adapter) -> None:
    """000417's whole history went missing this way on the first full load.

    One 2006 row disagreed with the rest by 10^6, and the symbol was refused —
    6,395 rows of good OHLCV discarded over one doubtful ``amount``. The middle
    of a history barely notices a row like that, which is the whole reason the
    judgement is made on the middle rather than on each row in turn.
    """
    rows = [row(20260601 + offset, price=10.0 + offset) for offset in range(11)]
    rows[5]["amount"] = rows[5]["amount"] * 1_000_000
    assert len(adapter.normalize(pack(rows))) == 11
    assert rows[5]["amount"] != 0, "the odd row is still carried, not quietly dropped"


def test_a_systematic_offset_is_still_refused(adapter) -> None:
    """Every row off by one factor is exactly what a unit change looks like."""
    rows = [row(20260601 + offset, price=10.0 + offset) for offset in range(11)]
    for record in rows:
        record["amount"] = record["amount"] / 10_000
    with pytest.raises(SourceMalformed, match="amount / volume"):
        adapter.normalize(pack(rows))


def test_a_row_with_no_amount_is_not_a_unit_error(adapter) -> None:
    """The source leaves ``amount`` at 0 for funds and bonds, with volume still set.

    A zero says nothing about a unit, so it is skipped rather than judged —
    otherwise a whole-market reply cannot be parsed at all (292 such rows on
    20260831 alone). A unit error is never zero; it is off by 10² or 10⁴.
    """
    record = daily_row(
        DAY, "511990", "华宝添益", price=100.0, volume=1_000, float_share=1e9, spread=0.0
    )
    record["amount"] = 0.0
    frame = adapter.normalize(pack(record))
    assert frame["amount"].tolist() == [0.0]


def test_turnover_is_ignored_whatever_units_it_arrives_in(adapter) -> None:
    """The field is discarded, so its units are not this adapter's business.

    Validating a value we never write could only produce false failures — and
    did: the identity ``volume / float_share * 100`` fails on 2 of 600633's 5424
    rows, both 2003 records that disagree with themselves. The column lands NULL
    either way (see ``test_turnover_is_never_written``).
    """
    record = row()
    record["turnover"] = 12345.0
    frame = adapter.normalize(pack(record))
    assert frame["turnover"].isna().all()


# --- failure semantics -----------------------------------------------------


def test_empty_array_is_source_empty(adapter) -> None:
    with pytest.raises(SourceEmpty):
        adapter.normalize(b"\x90")


def test_empty_body_is_source_empty(adapter) -> None:
    with pytest.raises(SourceEmpty):
        adapter.normalize(b"")


def test_undecodable_body_is_source_malformed(adapter) -> None:
    with pytest.raises(SourceMalformed) as caught:
        adapter.normalize(b"not msgpack at all")
    assert caught.value.body == b"not msgpack at all"


def test_row_without_a_contract_field_is_source_malformed(adapter) -> None:
    record = row()
    del record["code"]
    with pytest.raises(SourceMalformed, match="code"):
        adapter.normalize(pack(record))


def test_unreachable_service_is_source_unavailable(adapter, monkeypatch) -> None:
    def refuse(*args: object, **kwargs: object) -> object:
        raise urllib.error.URLError("connection refused")

    monkeypatch.setattr(urllib.request, "urlopen", refuse)
    with pytest.raises(SourceUnavailable, match="connection refused"):
        adapter.fetch(plan_market_day(dt.date(2026, 8, 31)))


def test_non_2xx_keeps_the_response_body(adapter, monkeypatch) -> None:
    def fail(*args: object, **kwargs: object) -> object:
        raise urllib.error.HTTPError(
            "http://x/", 500, "boom", http.client.HTTPMessage(), io.BytesIO(b"upstream exploded")
        )

    monkeypatch.setattr(urllib.request, "urlopen", fail)
    with pytest.raises(SourceUnavailable) as caught:
        adapter.fetch(plan_market_day(dt.date(2026, 8, 31)))
    assert "500" in str(caught.value)
    assert caught.value.body == b"upstream exploded"


# --- queries ---------------------------------------------------------------


def test_a_prefix_scan_over_all_history_cannot_be_built() -> None:
    """Measured at 179 MB for one prefix; make the state unrepresentable."""
    with pytest.raises(ValueError, match="179 MB"):
        Query(table=TABLE_DAILY, k1="qz:6006", k2="all:")


def test_a_prefix_scan_over_a_date_range_is_fine() -> None:
    assert Query(table=TABLE_DAILY, k1="qz:6006", k2="fwd:20260101,20260131")


def test_market_day_is_a_single_request_for_the_whole_market() -> None:
    query = plan_market_day(dt.date(2026, 8, 31))
    assert (query.k1, query.k2) == ("all:", "key:20260831")


def test_history_plans_one_bounded_request_for_one_symbol() -> None:
    assert plan_history(CODE).k1 == f"key:{CODE}"
    assert plan_history(CODE).k2 == "all:"
    ranged = plan_history(CODE, start=dt.date(2026, 6, 22), end=dt.date(2026, 6, 26))
    assert ranged.k2 == "fwd:20260622,20260626"


def test_history_refuses_a_half_open_range() -> None:
    """``fwd:`` needs both ends; guessing one would silently change the scope."""
    with pytest.raises(ValueError, match="both start and end"):
        plan_history(CODE, start=dt.date(2026, 6, 22))


def test_num_slices_server_side_only_when_asked() -> None:
    sliced = Query(table=TABLE_DAILY, k1=f"key:{CODE}", k2="all:", num=-1)
    assert sliced.parameters()["num"] == "-1"
    assert "num" not in Query(table=TABLE_DAILY, k1=f"key:{CODE}", k2="all:").parameters()


def test_the_code_table_is_asked_without_symbol_or_date_parameters() -> None:
    """With ``k1``/``k2`` the service answers a one-byte empty array.

    Verified against the live service: ``cmd=get&t=股票代码`` returns the groups,
    the same query carrying ``k1=all:&k2=all:`` returns nothing, while ``t=退市*``
    accepts them. The parameters are per-table, which is why they are not a
    default that every planner inherits.
    """
    parameters = plan_code_table().parameters()
    assert "k1" not in parameters
    assert "k2" not in parameters


def test_delisted_accepts_the_parameters_the_code_table_rejects() -> None:
    assert plan_delisted().cmd == "vals"
    assert plan_delisted().parameters()["k1"] == "all:"


def test_fetch_asks_the_service_in_its_own_vocabulary(monkeypatch) -> None:
    service = stub_service(monkeypatch, pack([row()]))
    FreeStockDbAdapter().fetch(plan_market_day(dt.date(2026, 8, 31)))
    url = service.urls[0]
    assert url.startswith(DEFAULT_BASE_URL)
    assert "t=%E6%97%A5k" in url
    assert "k1=all%3A" in url
    assert "k2=key%3A20260831" in url


def test_base_url_is_configurable_and_has_no_transport_baked_in() -> None:
    assert FreeStockDbAdapter().base_url == DEFAULT_BASE_URL
    assert FreeStockDbAdapter("http://127.0.0.1:17999/").base_url == "http://127.0.0.1:17999"


# --- adjustment factors ----------------------------------------------------


def test_factor_is_forward_adjusted_relative_to_the_latest_cum(adapter) -> None:
    raw = pack([adjust_event(19930705, cum=1.1), adjust_event(20260612, cum=5.649)])
    frame = adapter.normalize_factors(raw)
    assert list(frame.columns) == [column.name for column in ADJ_FACTOR.columns]
    assert frame["factor"].tolist() == pytest.approx([1.1 / 5.649, 1.0])


def test_the_most_recent_factor_is_exactly_one(adapter) -> None:
    """The reference database's qfq convention; see contracts.py."""
    raw = pack([adjust_event(19930705, cum=1.1), adjust_event(20260612, cum=5.649)])
    frame = adapter.normalize_factors(raw)
    assert frame["factor"].iloc[-1] == 1.0


def test_factors_come_back_sorted(adapter) -> None:
    raw = pack([adjust_event(20260612, cum=5.649), adjust_event(19930705, cum=1.1)])
    frame = adapter.normalize_factors(raw)
    assert frame["date"].dt.date.tolist() == [dt.date(1993, 7, 5), dt.date(2026, 6, 12)]


# --- symbols ---------------------------------------------------------------


def test_code_table_parses_into_prefix_groups() -> None:
    raw = pack({"0": ["000001", "000002"], "6": ["600633"]})
    assert parse_code_table(raw) == {"0": ("000001", "000002"), "6": ("600633",)}


def test_delisted_list_parses_into_codes() -> None:
    assert parse_delisted(pack(["600001", "000003"])) == ("600001", "000003")


def test_symbols_carry_market_and_status() -> None:
    frame = build_symbols(
        codes={"0": ("000001",), "3": ("300750",), "6": ("600633",), "9": ("920001",)},
        delisted=("600001",),
        names={
            "000001": "平安银行",
            "300750": "宁德时代",
            "600633": "浙数文化",
            "920001": "北交所股",
            "600001": "退市股",
        },
    )
    assert list(frame.columns) == [column.name for column in SYMBOLS.columns]
    by_symbol = {
        symbol: (market, status)
        for symbol, market, status in zip(
            frame["symbol"], frame["market"], frame["status"], strict=True
        )
    }
    assert by_symbol["000001"] == ("SZ", "listed")
    assert by_symbol["300750"] == ("SZ", "listed")
    assert by_symbol["600633"] == ("SH", "listed")
    assert by_symbol["920001"] == ("BJ", "listed")
    assert by_symbol["600001"] == ("SH", "delisted")


def test_suspension_is_never_a_symbol_status() -> None:
    """Suspension is a daily state, not an attribute of a listing."""
    frame = build_symbols(codes={"6": ("600633",)}, delisted=(), names={"600633": "浙数文化"})
    assert set(frame["status"]) == {"listed"}


def test_a_symbol_without_a_name_fails_loudly() -> None:
    """``name`` is NOT NULL; dropping the row silently is the thing to avoid."""
    with pytest.raises(SourceMalformed, match="600633"):
        build_symbols(codes={"6": ("600633",)}, delisted=(), names={})


def test_names_come_from_the_most_recent_daily_row() -> None:
    """The code table carries no names, so bars are the only source for them."""
    older = daily_row(DAY, CODE, "旧名", price=10.0, volume=1_000, float_share=FLOAT_SHARE)
    newer = daily_row(NEXT_DAY, CODE, "新名", price=10.0, volume=1_000, float_share=FLOAT_SHARE)
    assert extract_names(pack([newer, older])) == {CODE: "新名"}


def test_a_bar_without_a_name_does_not_refuse_the_symbol() -> None:
    """ETFs send ``name: null`` on every row but their most recent few.

    Refusing such a row refused the whole symbol — 1,021 instruments, every one
    an ETF, were lost to it on the first full load, and they are a research
    target rather than noise. A name is needed once per symbol, so a bar without
    one is skipped and the newest row that has one still wins.
    """
    unnamed = daily_row(DAY, CODE, "占位", price=10.0, volume=1_000, float_share=FLOAT_SHARE)
    del unnamed["name"]
    nulled = daily_row(DAY, "000002", "占位", price=10.0, volume=1_000, float_share=FLOAT_SHARE)
    nulled["name"] = None
    named = daily_row(NEXT_DAY, CODE, "浙数文化", price=10.0, volume=1_000, float_share=FLOAT_SHARE)

    assert extract_names(pack([unnamed, named])) == {CODE: "浙数文化"}
    assert extract_names(pack([named, unnamed])) == {CODE: "浙数文化"}
    assert extract_names(pack([nulled])) == {}


def test_a_reply_whose_rows_all_lack_names_is_not_empty() -> None:
    """No names is not the same as no records, and the error should not say so."""
    bar = daily_row(DAY, CODE, "占位", price=10.0, volume=1_000, float_share=FLOAT_SHARE)
    del bar["name"]
    assert extract_names(pack([bar])) == {}
    with pytest.raises(SourceEmpty):
        extract_names(b"\x90")


# --- every planner must reach its parser ------------------------------------
#
# A normalizer no planner can feed is dead code. That is exactly what happened
# to ``normalize_factors``: it had tests, and they all passed, while no public
# call could produce the bytes it takes. Each case below drives the whole path —
# plan, fetch, parse — through the public API, so a planner that cannot produce
# a query its parser accepts fails here rather than at a call site.


def test_the_candle_path_is_reachable(monkeypatch) -> None:
    stub_service(monkeypatch, pack([row()]))
    adapter = FreeStockDbAdapter()
    frame = adapter.normalize(adapter.fetch(plan_market_day(dt.date(2026, 8, 31))))
    assert frame["symbol"].tolist() == [CODE]


def test_the_adjustment_path_is_reachable(monkeypatch) -> None:
    stub_service(monkeypatch, pack([adjust_event(19930705, cum=1.1)]))
    adapter = FreeStockDbAdapter()
    query = plan_adjustments(CODE)
    assert query.table == TABLE_ADJUST
    frame = adapter.normalize_factors(adapter.fetch(query))
    assert frame["date"].dt.date.tolist() == [dt.date(1993, 7, 5)]


def test_the_code_table_path_is_reachable(monkeypatch) -> None:
    stub_service(monkeypatch, pack({"6": [CODE]}))
    adapter = FreeStockDbAdapter()
    assert parse_code_table(adapter.fetch(plan_code_table())) == {"6": (CODE,)}


def test_the_delisted_path_is_reachable(monkeypatch) -> None:
    stub_service(monkeypatch, pack(["600001"]))
    adapter = FreeStockDbAdapter()
    assert parse_delisted(adapter.fetch(plan_delisted())) == ("600001",)


def test_names_come_off_the_same_reply_as_the_bars(monkeypatch) -> None:
    stub_service(monkeypatch, pack([row()]))
    adapter = FreeStockDbAdapter()
    raw = adapter.fetch(plan_market_day(dt.date(2026, 8, 31)))
    assert extract_names(raw) == {CODE: "浙数文化"}


# --- expanding events into a daily series (M2 功能3) -------------------------
#
# ``market_v2.adj_factor`` holds one row per trading day, not one per
# distribution — 10,700,422 rows, exactly matching ``candles``, with zero gaps
# in either direction, and 2330's dates stepping through consecutive trading
# days. The source, by contrast, publishes events: 600633's entire history is 25
# of them. Writing events straight through would leave the consumer's inner join
# with 25 rows instead of 5424, and no error anywhere.


def events(rows) -> pd.DataFrame:
    """An event series as ``adjustment_events`` returns it: (symbol, date, cum)."""
    return pd.DataFrame(rows, columns=pd.Index(["symbol", "date", "cum"]))


def trading_days(rows) -> pd.DataFrame:
    return pd.DataFrame(rows, columns=pd.Index(["symbol", "date"]))


def test_a_day_takes_the_most_recent_event_at_or_before_it() -> None:
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5), (CODE, dt.date(2021, 1, 1), 3.0)]),
        trading_days([(CODE, dt.date(2020, 6, 1))]),
    )
    assert series["factor"].tolist() == [0.5]


def test_an_event_takes_effect_on_its_own_day() -> None:
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5), (CODE, dt.date(2021, 1, 1), 3.0)]),
        trading_days([(CODE, dt.date(2020, 12, 31)), (CODE, dt.date(2021, 1, 1))]),
    )
    assert series["factor"].tolist() == [0.5, 1.0]


def test_days_before_the_first_event_use_cum_one() -> None:
    """No distribution has happened yet, so the cumulative factor is 1.0."""
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5), (CODE, dt.date(2021, 1, 1), 3.0)]),
        trading_days([(CODE, dt.date(2019, 12, 30)), (CODE, dt.date(2019, 12, 31))]),
    )
    assert series["factor"].tolist() == pytest.approx([1 / 3, 1 / 3])


def test_the_last_trading_day_is_exactly_one() -> None:
    """The qfq convention the contract records; see ``contracts.py``."""
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5), (CODE, dt.date(2021, 1, 1), 3.0)]),
        trading_days([(CODE, dt.date(2020, 6, 1)), (CODE, dt.date(2021, 1, 1))]),
    )
    assert series["factor"].iloc[-1] == 1.0


def test_the_series_is_contract_shaped_and_sorted() -> None:
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5)]),
        trading_days([(CODE, dt.date(2020, 6, 1)), ("000001", dt.date(2020, 6, 1))]),
    )
    assert list(series.columns) == [column.name for column in ADJ_FACTOR.columns]
    assert series["date"].dtype.kind == "M"
    assert series["factor"].notna().tolist() == [True] * len(series)
    assert list(zip(series["symbol"], strict=True)) == [("000001",), (CODE,)]


def test_a_symbol_with_bars_but_no_events_is_unadjusted() -> None:
    """No distribution ever happened, so every day carries factor 1.0."""
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5)]),
        trading_days([("000001", dt.date(2020, 6, 1)), ("000001", dt.date(2020, 6, 2))]),
    )
    assert series["factor"].tolist() == [1.0, 1.0]


def test_events_without_bars_produce_no_rows() -> None:
    """A delisted symbol whose history was fetched but whose bars were not.

    The reference database has zero such rows, and the consumer's inner join
    would ignore them anyway, so they are not invented here.
    """
    series = daily_factors(
        events([("600001", dt.date(2020, 1, 1), 1.5)]),
        trading_days([(CODE, dt.date(2020, 6, 1))]),
    )
    assert set(series["symbol"]) == {CODE}


def test_events_after_the_last_bar_do_not_invent_days() -> None:
    series = daily_factors(
        events([(CODE, dt.date(2020, 1, 1), 1.5), (CODE, dt.date(2030, 1, 1), 9.0)]),
        trading_days([(CODE, dt.date(2020, 6, 1))]),
    )
    assert len(series) == 1
    # The bar precedes the 2030 event, so it is measured against that event's cum.
    assert series["factor"].tolist() == [1.5 / 9.0]


def test_the_expansion_is_one_row_per_bar() -> None:
    """The property the consumer's inner join depends on, on real captures."""
    raw_bars = load("daily_range_600633.bin")
    bars = FreeStockDbAdapter().normalize(raw_bars)
    adapter = FreeStockDbAdapter()
    series = daily_factors(adapter.adjustment_events(load("adjust_600633.bin")), bars)
    assert set(zip(series["symbol"], series["date"], strict=True)) == set(
        zip(bars["symbol"], bars["date"], strict=True)
    )
    # Every captured bar falls after the last captured event (2026-06-12).
    assert series["factor"].tolist() == [1.0] * len(bars)


# --- shares outstanding (M2 功能5) ------------------------------------------


def test_shares_are_taken_from_the_daily_rows() -> None:
    """No separate source exists for them — every bar carries the figure."""
    frame = extract_shares(load("daily_single_600633_20260625.bin"))
    assert list(frame.columns) == [column.name for column in SHARES_OUTSTANDING.columns]
    assert frame["symbol"].tolist() == [CODE]
    assert frame["shares"].tolist() == [1_268_074_472]
    assert frame["date"].dtype.kind == "M"


def test_shares_are_the_total_and_not_the_float() -> None:
    """The choice that decides what a downstream turnover factor means.

    Taiwan's ``週轉率`` divides by shares issued, and the reference database
    stores that; an A-share's conventional turnover divides by the *float*. One
    column cannot answer both, and this column has to keep meaning what it means
    in the database it mirrors — so a stock with restricted shares is written
    with its total, and the difference is recorded in ``contracts.py`` rather
    than buried here.
    """
    record = daily_row(DAY, CODE, "浙数文化", price=10.0, volume=1_000, float_share=1_000_000)
    record["total_share"] = 9_000_000
    record["float_share"] = 1_000_000
    assert extract_shares(pack(record))["shares"].tolist() == [9_000_000]


def test_an_instrument_without_shares_simply_has_none() -> None:
    """ETFs carry no share count, and that is not a reason to lose the ETF.

    Refusing the row refused the instrument — 1,021 of them on the first full
    load, one field after the same mistake over ``name``. The contract expects
    the absence: the reference database's ``shares_outstanding`` covers 2,359 of
    its 2,806 symbols, and the consumer left-joins it.
    """
    missing = row()
    del missing["total_share"]
    assert extract_shares(pack(missing)).empty

    nulled = row()
    nulled["total_share"] = None
    assert extract_shares(pack(nulled)).empty

    kept = row()
    assert extract_shares(pack(kept))["shares"].tolist() == [FLOAT_SHARE]


def test_no_shares_is_not_the_same_as_no_records() -> None:
    """The two absences mean different things and must not share an error."""
    with pytest.raises(SourceEmpty):
        extract_shares(b"\x90")


# --- against a live service (excluded by default) ---------------------------


@pytest.mark.network
def test_live_service_agrees_with_the_documented_units() -> None:
    """The one claim no offline fixture can settle.

    Everything above is built from the byte formats the source is documented to
    emit; this is the check that those formats, and the two unit invariants
    ``normalize`` enforces, still hold against the real thing. Run it with::

        uv run --extra alerts --extra dev pytest -m network

    Skipped, not failed, when the service is not answering — a laptop without a
    tunnel still gets a green suite from the offline tests.
    """
    adapter = FreeStockDbAdapter()
    try:
        raw = adapter.fetch(plan_history("600633"))
    except SourceUnavailable as exc:
        pytest.skip(f"free-stockdb is not reachable: {exc}")
    frame = adapter.normalize(raw)
    assert list(frame.columns) == [column.name for column in CANDLES.columns]
    assert len(frame) > 0
    assert set(frame["symbol"]) == {"600633"}
