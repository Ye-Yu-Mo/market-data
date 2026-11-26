"""free-stockdb adapter: A-share daily bars, adjustment factors and code lists.

free-stockdb answers ``GET /?cmd=&t=&k1=&k2=`` with MessagePack. ``k1`` selects
the symbol side (``key:<code>`` / ``qz:<prefix>`` / ``all:``) and ``k2`` the date
side (``key:<YYYYMMDD>`` / ``fwd:<start>,<end>`` / ``all:``); ``key:``-style
replies carry their date in the key, which is why anything that needs dates must
use ``get`` rather than ``vals``.

Two source behaviours are load-bearing enough to be encoded here rather than
trusted:

* A request for a prefix's entire history returned **179 MB**. ``Query`` refuses
  to represent that shape at all, so no planner can produce one by accident.
* A query the service cannot answer — including one with a typo — comes back as
  **HTTP 200 with an empty array**, not an error. That is ``SourceEmpty``, never
  "this symbol has no data".

``normalize`` ends by re-deriving the two unit invariants the source does not
document (``amount / volume`` inside the day's range, and
``volume / float_share * 100`` equal to ``turnover``). A row that fails them is
rejected instead of producing a frame: wrong units that look like numbers are
exactly what the reference database's ``turnover`` column already suffers from.

Three decisions worth stating outright
--------------------------------------

``candles.turnover`` is written as **NULL**
    The source's value is a 换手率 percentage; the reference database's
    same-named column holds integer trade counts. No mapping between the two is
    known to be correct, and writing the percentage would produce a column that
    looks entirely reasonable and is wrong. See ``contracts.py``.

A zero is skipped by the unit checks, not judged by them
    The source leaves ``amount`` at ``0`` for funds and bonds, and a suspended
    day has no volume. A zero says nothing about a field's *unit*, which is the
    only question these checks ask, so they stay silent instead of rejecting. A
    unit error is never zero — it is off by 10² or 10⁴.

The frame carries whatever the service returned
    ``plan_market_day`` reaches funds and bonds as well as stocks. Choosing a
    symbol universe is the caller's decision; dropping rows here would be the
    silent filtering this project keeps having to undo.

Verified against the live service
---------------------------------
Every byte format below was checked against captures from leaf's service
(2026-09-23/24; see ``tests/fixtures/free_stockdb/``), and ``pytest -m network``
re-runs the parts that need a socket. Three things that only observation could
settle:

* ``plan_code_table`` takes **no** ``k1``/``k2``. Sending ``all:``/``all:``
  returns one empty byte from a service that is otherwise answering normally —
  an empty reply that means "you asked wrong", not "there are no symbols".
* The ``9`` block holds 309 codes and every one is ``920xxx``; no ``900xxx``
  B-share is in it, so reading the block as Beijing is safe *for this source*.
* A whole-market day is ~2.5 MB and reaches funds and bonds as well as stocks.
"""

# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# Silenced for this module only. ``msgpack`` ships no stubs and ``pandas``' stubs
# are partial, so every value crossing either boundary is ``Unknown`` — nothing
# this module owns is untyped, and the same trade is already made in
# ``offline/quality.py``.

from __future__ import annotations

import datetime as dt
import logging
import statistics
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Final

import msgpack
import pandas as pd

from market_data.markets.base import SourceEmpty, SourceMalformed, SourceUnavailable
from market_data.markets.contracts import ADJ_FACTOR, CANDLES, SHARES_OUTSTANDING, SYMBOLS

#: The service listens on loopback; how a caller's HTTP gets there (directly, or
#: through a tunnel) is not this module's concern.
DEFAULT_BASE_URL: Final = "http://127.0.0.1:7899"

TABLE_DAILY: Final = "日k"
TABLE_ADJUST: Final = "复权"
TABLE_CODES: Final = "股票代码"
TABLE_DELISTED: Final = "退市*"

MATCH_ALL: Final = "all:"
KEY_LOOKUP: Final = "key:"
PREFIX_SCAN: Final = "qz:"
FORWARD_RANGE: Final = "fwd:"

#: How far a symbol's *median* ``amount / volume / close`` may sit from 1.0.
#:
#: The check asks one question — are ``amount`` and ``volume`` in 元 and 股 — and
#: a wrong unit answers it by 10² (手 against 股) or 10⁻⁴ (万元 against 元) on
#: **every** row, so it belongs at the scale of "same order of magnitude", not at
#: the scale of rounding or of a single bad day.
#:
#: Two earlier versions judged row by row and both were wrong, each time because
#: the tolerance was set from too little data: ``1e-9`` rejected 11 of 5,488
#: stock rows on one market day, and ``1e-3`` — calibrated on that same day —
#: still rejected ~10 rows across 53k rows of twelve symbols. Judging every row
#: was the deeper mistake: on the first whole-market load it rejected 99 symbols,
#: including one whose entire 6,395-row history was discarded over a single 2006
#: row that disagreed by 10⁶.
#:
#: A factor of ten rejects the smallest unit error there is by a factor of ten,
#: and clears the widest *systematic* disagreement measured — 000043, ~3x for
#: twelve years. That symbol's ``amount`` may well be wrong or computed
#: differently; its OHLCV is not, and the median is what separates the two.
_PRICE_TOLERANCE: Final = 10.0

#: A-share codes are allocated to exchanges in blocks.
#:
#: ``0``/``3`` are Shenzhen, ``6`` Shanghai, ``9`` Beijing — the source's own
#: grouping holds 309 codes there and every one is ``920xxx``, with no Shanghai
#: B-share (``900xxx``) among them, which is what makes that mapping safe rather
#: than merely plausible.
#:
#: ``5`` and ``8`` are here because the **delisted** list reaches blocks the live
#: code table does not: ``5xxxxx`` is Shanghai's fund block, and the three
#: ``8xxxxx`` codes are 新三板, which is Beijing-based like the rest of that
#: ecosystem. Refusing them would drop three real delisted symbols, which is the
#: survivorship bias this table exists to prevent.
#:
#: ``1`` is refused on purpose: ``11xxxx`` is a Shanghai bond and ``12xxxx`` a
#: Shenzhen convertible, so the leading digit genuinely does not decide.
_EXCHANGE_BY_LEADING: Final[dict[str, str]] = {
    "0": "SZ",
    "3": "SZ",
    "5": "SH",
    "6": "SH",
    "8": "BJ",
    "9": "BJ",
}

#: Funds that need a second digit to place.
#:
#: ``1`` alone decides nothing — it spans Shanghai's ``11xxxx`` bonds and
#: Shenzhen's funds — but this table's ``1`` group holds only the Shenzhen
#: prefixes, measured: 683 ``15xxxx``, 285 ``16xxxx``, 27 ``18xxxx``, and no
#: ``11xxxx`` or ``12xxxx`` among them. Listing the prefixes rather than the
#: digit keeps that a statement about what was counted: a bond arriving under
#: ``1`` later resolves to no exchange and is refused, instead of being filed
#: under Shenzhen because the block it sits in usually means that.
#:
#: These are ETFs and LOFs, and they are a research target rather than noise —
#: A-share ETF trading is commission-free for this project's owner, which is why
#: ``1`` and ``5`` are in the universe at all.
_EXCHANGE_BY_FUND_PREFIX: Final[dict[str, str]] = {
    "15": "SZ",
    "16": "SZ",
    "18": "SZ",
}


@dataclass(frozen=True, slots=True)
class Query:
    """One bounded free-stockdb request.

    ``k1``/``k2`` are the service's own vocabulary, kept visible rather than
    hidden behind an abstraction that would only have to be translated back.
    Either may be ``None``, which omits the parameter entirely — that is the
    shape ``t=股票代码`` actually wants, and sending it ``all:`` returns nothing.
    """

    table: str
    k1: str | None = None
    k2: str | None = None
    cmd: str = "get"
    num: int | None = None

    def __post_init__(self) -> None:
        if self.k1 is not None and self.k1.startswith(PREFIX_SCAN) and self.k2 == MATCH_ALL:
            raise ValueError(
                "a prefix scan over unbounded history measured 179 MB in one "
                "response; chunk by whole-market day or by single symbol instead"
            )

    def parameters(self) -> dict[str, str]:
        parameters = {"cmd": self.cmd, "t": self.table}
        if self.k1 is not None:
            parameters["k1"] = self.k1
        if self.k2 is not None:
            parameters["k2"] = self.k2
        if self.num is not None:
            parameters["num"] = str(self.num)
        return parameters


def plan_market_day(day: dt.date) -> Query:
    """Whole market on one trading day — the measured 2.4 MB single request."""
    return Query(TABLE_DAILY, MATCH_ALL, f"{KEY_LOOKUP}{day:%Y%m%d}")


def plan_history(code: str, *, start: dt.date | None = None, end: dt.date | None = None) -> Query:
    """One symbol's full history, or a bounded range of it."""
    if (start is None) != (end is None):
        raise ValueError("give both start and end, or neither")
    if start is None or end is None:
        return Query(TABLE_DAILY, f"{KEY_LOOKUP}{code}", MATCH_ALL)
    return Query(TABLE_DAILY, f"{KEY_LOOKUP}{code}", f"{FORWARD_RANGE}{start:%Y%m%d},{end:%Y%m%d}")


def plan_adjustments(code: str) -> Query:
    """One symbol's adjustment-factor events, dates included in the keys.

    ``get`` rather than ``vals``: the date of an adjustment event lives in the
    reply's key, and ``vals`` drops keys.
    """
    return Query(TABLE_ADJUST, f"{KEY_LOOKUP}{code}", MATCH_ALL, cmd="get")


def plan_all_adjustments() -> Query:
    """Every symbol's adjustment events in one reply — 4.1 MB, 58,527 events.

    The reason an incremental update is two requests rather than one per symbol:
    the events for the whole market arrive at once.
    """
    return Query(TABLE_ADJUST, MATCH_ALL, MATCH_ALL)


def plan_code_table() -> Query:
    """The service's symbol universe, grouped by leading digit.

    No ``k1``/``k2``: with them the reply is a one-byte empty array. Verified
    against the live service on 2026-09-23.
    """
    return Query(TABLE_CODES)


def plan_delisted() -> Query:
    """Every delisted code, as a flat array of strings."""
    return Query(TABLE_DELISTED, MATCH_ALL, MATCH_ALL, cmd="vals")


class FreeStockDbAdapter:
    """Fetches A-share data from a free-stockdb service.

    ``base_url`` is the whole story: whether a request reaches that URL
    directly, through a tunnel or through a port-forward is not this class's
    business, and baking it in would mean the data layer changes whenever the
    deployment does.
    """

    def __init__(self, base_url: str = DEFAULT_BASE_URL, *, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    @property
    def base_url(self) -> str:
        return self._base_url

    # --- the only half that touches the network ---------------------------

    def fetch(self, query: Query) -> bytes:
        url = f"{self._base_url}/?{urllib.parse.urlencode(query.parameters())}"
        try:
            with urllib.request.urlopen(url, timeout=self._timeout) as response:
                return bytes(response.read())
        except urllib.error.HTTPError as exc:
            raise SourceUnavailable(f"HTTP {exc.code} from {url}", body=exc.read()) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise SourceUnavailable(f"{url}: {exc}") from exc

    # --- pure halves ------------------------------------------------------

    def normalize(self, raw: bytes) -> pd.DataFrame:
        """Daily bars, as the ``candles`` contract describes them."""
        records = _records(_decode(raw))
        if not records:
            raise SourceEmpty("the service returned no records", body=raw)
        records = _drop_excluded(records)
        _require_consistent_units(records)
        rows = [_candle_row(record) for record in records]
        frame = pd.DataFrame(rows, columns=pd.Index([column.name for column in CANDLES.columns]))
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.astype(
            {
                "symbol": "object",
                "open": "float64",
                "high": "float64",
                "low": "float64",
                "close": "float64",
                "volume": "int64",
                "amount": "float64",
                "turnover": "float64",
            }
        )
        return frame.sort_values(["symbol", "date"], ignore_index=True)

    def adjustment_events(self, raw: bytes) -> pd.DataFrame:
        """The source's raw cumulative series: one row per distribution.

        Not the contract's shape — ``market_v2.adj_factor`` holds a factor per
        trading day, thousands of rows for a symbol whose event history is a few
        dozen. This is what ``daily_factors`` expands.
        """
        entries = _adjustment_entries(_decode(raw))
        if not entries:
            raise SourceEmpty("the service returned no records", body=raw)
        rows = [{"symbol": symbol, "date": day, "cum": cum} for symbol, day, cum in entries]
        frame = pd.DataFrame(rows, columns=pd.Index(["symbol", "date", "cum"]))
        frame["date"] = pd.to_datetime(frame["date"])
        frame = frame.astype({"symbol": "object", "cum": "float64"})
        return frame.sort_values(["symbol", "date"], ignore_index=True)

    def normalize_factors(self, raw: bytes) -> pd.DataFrame:
        """Adjustment factors, as the ``adj_factor`` contract describes them.

        The source publishes a raw, monotonically increasing ``cum`` series,
        which is *backward* adjustment. The contract holds *forward* adjustment,
        where a symbol's most recent factor is exactly 1.0, so every value is
        divided by its own symbol's latest ``cum``.

        These are the **event** rows, not one row per trading day. Handing them
        to ``adj_factor`` would leave the consumer's inner join with 25 rows
        where it expects 5424 — see ``daily_factors``.

        Note for whoever writes these rows: ``cum_latest`` moves every time a new
        distribution happens, which shifts the whole series rather than appending
        to it. A writer must be able to overwrite previously written factor rows.
        """
        events = self.adjustment_events(raw)
        latest = (
            events.drop_duplicates("symbol", keep="last")
            .filter(items=["symbol", "cum"])
            .rename(columns={"cum": "latest_cum"})
        )
        factors = events.merge(latest, on="symbol", how="left")
        factors["factor"] = factors["cum"] / factors["latest_cum"]
        frame = pd.DataFrame(
            factors.filter(items=[column.name for column in ADJ_FACTOR.columns]),
            columns=pd.Index([column.name for column in ADJ_FACTOR.columns]),
        )
        return frame.astype({"symbol": "object", "factor": "float64"}).sort_values(
            ["symbol", "date"], ignore_index=True
        )


def daily_factors(events: pd.DataFrame, trading_days: pd.DataFrame) -> pd.DataFrame:
    """Expand a symbol's distribution events into one factor per trading day.

    ``market_v2.adj_factor`` is a **daily** table: 10,700,422 rows, matching
    ``candles`` exactly, with no gap in either direction, and a symbol's dates
    stepping through consecutive trading days rather than jumping between
    distributions. The source publishes events instead — 600633's entire history
    is 25 of them — so the events are expanded before anything is written.

    The rule reproduces the reference database's shape: a day carries the ``cum``
    of the most recent event at or before it, or 1.0 if no event has happened
    yet, and every value is then divided by the symbol's own latest ``cum`` so
    the most recent day is exactly 1.0 — forward adjustment, as ``contracts.py``
    records.

    Only days present in ``trading_days`` produce rows, so an event history
    fetched without bars yields nothing rather than an orphan factor. The
    reference database has zero such rows and the consumer's inner join would
    discard them anyway.
    """
    # merge_asof needs its key sorted, and ``by`` does not relax that: a frame
    # ordered by (symbol, date) is not sorted by date. Both sides are therefore
    # ordered by date and the result is re-sorted at the end.
    days = _align_merge_keys(trading_days.filter(items=["symbol", "date"])).sort_values(
        "date", ignore_index=True
    )
    ordered = _align_merge_keys(events.filter(items=["symbol", "date", "cum"])).sort_values(
        "date", ignore_index=True
    )
    expanded = pd.merge_asof(days, ordered, on="date", by="symbol", direction="backward")
    # A symbol's latest ``cum`` has to reach every one of its days. Merging a
    # one-row-per-symbol frame says that in a way pandas' stubs type precisely;
    # ``Series.map`` with a mapping does not, because a mapping's key parameter
    # is invariant and so refuses a ``dict[str, float]``.
    latest = (
        ordered.drop_duplicates("symbol", keep="last")
        .filter(items=["symbol", "cum"])
        .rename(columns={"cum": "latest_cum"})
    )
    expanded = expanded.merge(latest, on="symbol", how="left")
    expanded["factor"] = expanded["cum"].fillna(1.0) / expanded["latest_cum"].fillna(1.0)
    frame = pd.DataFrame(
        expanded.filter(items=[column.name for column in ADJ_FACTOR.columns]),
        columns=pd.Index([column.name for column in ADJ_FACTOR.columns]),
    )
    return frame.astype({"symbol": "object", "factor": "float64"}).sort_values(
        ["symbol", "date"], ignore_index=True
    )


def _align_merge_keys(frame: pd.DataFrame) -> pd.DataFrame:
    """Make both join keys comparable before ``merge_asof`` sees them.

    ``merge_asof`` refuses two frames whose key dtypes differ, and its error
    names neither the column nor the reason, so both keys are pinned here:

    * ``symbol`` is pandas 3's ``str`` dtype from some producers and plain
      ``object`` from others — ``normalize`` casts it, a ``read_sql`` does not.
    * ``date`` reaches three different resolutions: ``datetime.date`` objects
      from a hand-built frame, ``datetime64[s]`` from the adapter, and
      ``datetime64[us]`` from DuckDB or from parsing strings. ``to_datetime``
      keeps whatever it was handed, so the unit is pinned rather than converted.

    Both sides of every merge below go through here, so they agree by
    construction rather than by the caller's luck.
    """
    frame = frame.copy()
    frame["date"] = pd.to_datetime(frame["date"]).astype("datetime64[us]")
    frame["symbol"] = frame["symbol"].astype("object")
    return frame


# --- decoding --------------------------------------------------------------


def _decode(raw: bytes) -> object:
    if not raw:
        raise SourceEmpty("the service returned HTTP 200 with an empty body", body=raw)
    try:
        return msgpack.unpackb(raw, raw=False, strict_map_key=False)
    except (msgpack.exceptions.UnpackException, ValueError) as exc:
        raise SourceMalformed(f"body is not msgpack: {exc}", body=raw) from exc


def _records(payload: object) -> list[Mapping[str, Any]]:
    """Field maps from either of the two shapes a symbol query comes back in.

    ``cmd=vals`` yields an array of maps, ``cmd=get`` a bare map for one key and
    an array of ``[key, map]`` pairs for several.
    """
    if isinstance(payload, Mapping):
        return [payload]
    if not isinstance(payload, list):
        raise SourceMalformed(
            f"expected an object or array of records, got {type(payload).__name__}"
        )
    records: list[Mapping[str, Any]] = []
    for item in payload:
        if isinstance(item, Mapping):
            records.append(item)
        elif isinstance(item, list) and len(item) == 2 and isinstance(item[1], Mapping):
            records.append(item[1])
        else:
            raise SourceMalformed(f"unsupported record shape: {type(item).__name__}")
    return records


def parse_keys(raw: bytes) -> tuple[dt.date, ...]:
    """A ``cmd=keys`` reply: the dates the symbol has adjustment events on."""
    payload = _decode(raw)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise SourceMalformed("expected an array of key strings")
    if not payload:
        raise SourceEmpty("the service returned no keys", body=raw)
    return tuple(_split_key(key)[1] for key in payload)


def parse_code_table(raw: bytes) -> dict[str, tuple[str, ...]]:
    """The symbol universe, grouped by leading digit."""
    payload = _decode(raw)
    if not isinstance(payload, Mapping):
        raise SourceMalformed("expected a mapping of digit group to codes")
    table: dict[str, tuple[str, ...]] = {}
    for group, codes in payload.items():
        if not isinstance(codes, list) or not all(isinstance(code, str) for code in codes):
            raise SourceMalformed(f"group {group!r} is not an array of code strings")
        table[str(group)] = tuple(codes)
    if not table:
        raise SourceEmpty("the service returned no code groups", body=raw)
    return table


def parse_delisted(raw: bytes) -> tuple[str, ...]:
    """The delisted codes, as a flat array of strings."""
    payload = _decode(raw)
    if not isinstance(payload, list) or not all(isinstance(item, str) for item in payload):
        raise SourceMalformed("expected a flat array of code strings")
    if not payload:
        raise SourceEmpty("the service returned no delisted codes", body=raw)
    return tuple(payload)


def extract_names(raw: bytes) -> dict[str, str]:
    """Symbol names, harvested from daily rows.

    ``日k`` is the only reply that carries a name — the code table does not — so
    names have to come off bars, which also means they arrive for delisted
    symbols as a by-product of fetching their history. Where a symbol appears on
    more than one day the most recent wins, so the result does not depend on the
    order the service chose to reply in.

    Only the most recent rows of an instrument carry a name, and the rest send
    ``null`` — measured on ETFs, where 44 rows of a 2,459-row history have one.
    A row without a name is therefore skipped rather than refused: refusing it
    refused the whole symbol, which is how 1,021 instruments were lost on the
    first full load. A symbol with no named row at all yields no entry here and
    fails at the point that needs it, loudly, rather than being given a name
    somebody made up.
    """
    records = _records(_decode(raw))
    if not records:
        raise SourceEmpty("the service returned no records", body=raw)
    latest: dict[str, tuple[dt.date, str]] = {}
    for record in records:
        code = _text(record, "code")
        name = record.get("name")
        if not isinstance(name, str) or not name:
            continue
        day = _date(record, "date", code)
        current = latest.get(code)
        if current is None or day >= current[0]:
            latest[code] = (day, name)
    return {code: name for code, (_, name) in latest.items()}


# --- field mapping ---------------------------------------------------------


def _candle_row(record: Mapping[str, Any]) -> dict[str, object]:
    # ``turnover`` is deliberately left NULL. The source's value is a 换手率
    # percentage; the reference database's same-named column holds integer trade
    # counts (mean 935 — see ``contracts.py``). No mapping between two different
    # quantities is correct, so nothing is written until that is settled. Writing
    # the percentage would look entirely reasonable and be wrong — the failure
    # mode this project keeps having to undo.
    code = _text(record, "code")
    return {
        "symbol": code,
        "date": _date(record, "date", code),
        "open": _optional_number(record, "open", code),
        "high": _number(record, "high", code),
        "low": _number(record, "low", code),
        "close": _number(record, "close", code),
        "volume": int(_number(record, "volume", code)),
        "amount": _number(record, "amount", code),
        "turnover": None,
    }


def _adjustment_entries(payload: object) -> list[tuple[str, dt.date, float]]:
    if not isinstance(payload, list):
        raise SourceMalformed("expected an array of keyed adjustment records")
    entries: list[tuple[str, dt.date, float]] = []
    for item in payload:
        if not (isinstance(item, list) and len(item) == 2 and isinstance(item[1], Mapping)):
            raise SourceMalformed(f"unsupported adjustment shape: {type(item).__name__}")
        symbol, day = _split_key(item[0] if isinstance(item[0], str) else "")
        entries.append((symbol, day, float(_number(item[1], "cum", symbol))))
    return entries


def extract_shares(raw: bytes) -> pd.DataFrame:
    """Shares outstanding, read off the daily rows, where the source has them.

    No separate source exists for them: a bar carries the figure. This reads
    ``total_share``, which is shares *issued* — what the reference database's
    ``shares_outstanding.shares`` holds, since Taiwan's ``週轉率`` divides by
    shares issued.

    The source also publishes ``float_share``, and an A-share's conventional
    turnover ratio divides by the float, so the two are not interchangeable: a
    company with restricted shares gets a different number from each. One column
    cannot answer both, and this one has to keep meaning what it means in the
    database it mirrors. What that costs is written down in ``contracts.py``.

    A row without ``total_share`` is skipped rather than refused, because **not
    every instrument has one**: ETFs carry no share count at all, and refusing
    such a row refused the instrument — 1,021 of them, one field after the same
    mistake over ``name``. The contract expects the absence: the reference
    database's ``shares_outstanding`` covers 2,359 of its 2,806 symbols, and the
    consumer left-joins it. A frame with no rows is therefore a normal answer
    here, not the ``SourceEmpty`` that a reply with no records is.
    """
    records = _records(_decode(raw))
    if not records:
        raise SourceEmpty("the service returned no records", body=raw)
    rows = []
    for record in records:
        total = record.get("total_share")
        if isinstance(total, bool) or not isinstance(total, (int, float)):
            continue
        rows.append(
            {
                "symbol": _text(record, "code"),
                "date": _date(record, "date", _code_of(record)),
                "shares": int(total),
            }
        )
    frame = pd.DataFrame(
        rows, columns=pd.Index([column.name for column in SHARES_OUTSTANDING.columns])
    )
    frame["date"] = pd.to_datetime(frame["date"])
    return frame.astype({"symbol": "object", "shares": "int64"}).sort_values(
        ["symbol", "date"], ignore_index=True
    )


def _split_key(key: str) -> tuple[str, dt.date]:
    """``复权:600633:19930705`` → the symbol and the date it names."""
    parts = key.split(":")
    if len(parts) != 3:
        raise SourceMalformed(f"cannot read a symbol and date out of key {key!r}")
    symbol = parts[1]
    try:
        day = _to_date(int(parts[2]), symbol)
    except ValueError as exc:
        raise SourceMalformed(f"{symbol}: key date {parts[2]!r} is not YYYYMMDD") from exc
    return symbol, day


def _text(record: Mapping[str, Any], field: str) -> str:
    value = record.get(field)
    if not isinstance(value, str):
        raise SourceMalformed(f"{field} must be a string, got {type(value).__name__}")
    return value


def _number(record: Mapping[str, Any], field: str, code: str) -> float:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceMalformed(f"{code}: {field} is missing or not numeric")
    return float(value)


def _code_of(record: Mapping[str, Any]) -> str:
    """The symbol a row is about, for error messages; ``?`` when it is absent."""
    value = record.get("code")
    if isinstance(value, str):
        return value
    return "?"


def _optional_number(record: Mapping[str, Any], field: str, code: str) -> float | None:
    """``open`` and ``turnover`` may be absent; absent means NULL, never 0."""
    value = record.get(field)
    if value is None:
        return None
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise SourceMalformed(f"{code}: {field} is not numeric")
    return float(value)


def _date(record: Mapping[str, Any], field: str, code: str) -> dt.date:
    value = record.get(field)
    if isinstance(value, bool) or not isinstance(value, int):
        raise SourceMalformed(f"{code}: {field} must be the uint32 YYYYMMDD the source documents")
    return _to_date(value, code)


def _to_date(value: int, code: str) -> dt.date:
    try:
        return dt.datetime.strptime(f"{value:08d}", "%Y%m%d").date()
    except ValueError as exc:
        raise SourceMalformed(f"{code}: {value!r} is not a calendar date") from exc


# --- unit assertions -------------------------------------------------------


_LOGGER = logging.getLogger(__name__)

#: Symbols dropped **before** the unit gate, with the reason written down here.
#:
#: ``513330`` (恒生互联网ETF华夏) — the source's ``volume`` is in 手 on part of its
#: history and in 股 on the rest: of 1,376 rows (2021-02 → 2026-10) **810** have
#: ``amount / volume / close ≈ 100`` while their neighbours sit at ``≈ 1``, and
#: ``amount`` is continuous across those days. So the bar is usable but the volume
#: column is not, and which convention a given day used can only be told from the
#: ratio itself — "repairing" it by multiplying by 100 would be inventing data.
#: Without this list, `_require_consistent_units` rejects the symbol on every
#: update, and a single bad symbol turns the whole daily run into "partial failure"
#: (exit 5) — which is how a permanent condition becomes alert noise.
#:
#: Measured 2026-10-20 against leaf's service (``GET /?cmd=get&t=日k&k1=key:513330``).
#: Adding to this list is a **claim about the source**, not a workaround: only put
#: a symbol here after checking which field is wrong, and say so above.
#:
#: Excluded symbols are skipped by `normalize` and never reach the store, so
#: `build_symbols` still lists them (the code table is a separate reply) — a
#: consumer filtering on ``candles`` simply will not find them.
EXCLUDED_SYMBOLS: Final[frozenset[str]] = frozenset({"513330"})


#: Codes already warned about in this process — the warning is per process, not
#: per symbol-day, or a whole-market load prints it thousands of times.
_warned_excluded: set[str] = set()


def _drop_excluded(records: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    """Remove `EXCLUDED_SYMBOLS` from a reply, saying so once per process.

    Logged at WARNING rather than dropped in silence: "this symbol is missing"
    has to be answerable from the run's log, not only from this file.
    """
    kept: list[Mapping[str, Any]] = []
    for record in records:
        code = _code_of(record)
        if code in EXCLUDED_SYMBOLS:
            if code not in _warned_excluded:
                _warned_excluded.add(code)
                _LOGGER.warning(
                    "cn: %s 被排除 —— 源的 volume 单位不一致, 见 EXCLUDED_SYMBOLS", code
                )
            continue
        kept.append(record)
    return kept


def _require_consistent_units(records: Sequence[Mapping[str, Any]]) -> None:
    """Re-derive the units the source never documents, and refuse to guess.

    ``amount`` is in 元 and ``volume`` in 股, so ``amount / volume`` is a day's
    average price and ``amount / volume / close`` should sit near 1. The question
    is whether that holds *for the symbol*, and the middle of a history answers
    it where a single row cannot: a unit change moves every row by the same
    factor and so moves the median by exactly that much, while a handful of bad
    rows barely moves it at all.

    That distinction is the whole point. Judging each row rejected 99 symbols on
    the first whole-market load — among them 000417, whose 6,395-row history was
    discarded over one 2006 row that disagreed by 10⁶. Its OHLCV is fine; only
    ``amount`` is doubtful, and only on the days the source got wrong.

    Only ``amount``/``volume`` are checked, because only they are written.
    ``turnover`` was verified the same way (the identity holds on 5422 of
    600633's 5424 rows) and is then discarded — see ``_candle_row``. Validating
    a field we throw away can only produce failures, never correctness.

    A zero is skipped rather than judged: the source leaves ``amount`` at 0 for
    funds and bonds, and a suspended day has no volume. A zero says nothing
    about a unit, and a unit error is never zero — it is off by 10² or 10⁴.
    """
    by_symbol: dict[str, list[float]] = {}
    for record in records:
        code = _code_of(record)
        volume = _number(record, "volume", code)
        amount = _number(record, "amount", code)
        close = _number(record, "close", code)
        if volume > 0 and amount > 0 and close > 0:
            by_symbol.setdefault(code, []).append(amount / volume / close)
    for code, values in by_symbol.items():
        middle = statistics.median(values)
        if not 1 / _PRICE_TOLERANCE <= middle <= _PRICE_TOLERANCE:
            raise SourceMalformed(
                f"{code}: amount / volume is {middle:.6g}x the closing price across "
                f"{len(values)} days; amount and volume are not both in 元 and 股"
            )


# --- symbols ---------------------------------------------------------------


def stock_groups(codes: Mapping[str, Sequence[str]]) -> dict[str, tuple[str, ...]]:
    """The code-table groups ``build_symbols`` can place, minus codes it cannot.

    This is the symbol universe: stocks, Beijing, and the fund blocks (``1``
    Shenzhen, ``5`` Shanghai) that are a research target here rather than noise.
    Which codes qualify is asked of ``_exchange`` one at a time rather than
    inferred from a group name, so the answer stays right if a bond ever appears
    under ``1`` — that bond alone drops out instead of taking 995 ETFs with it.
    """
    groups = {
        str(group): tuple(code for code in block if _placeable(code))
        for group, block in codes.items()
    }
    return {group: block for group, block in groups.items() if block}


def build_symbols(
    *,
    codes: Mapping[str, Sequence[str]],
    delisted: Sequence[str],
    names: Mapping[str, str],
) -> pd.DataFrame:
    """Assemble the ``symbols`` contract table.

    Only code blocks whose exchange the leading digit actually decides are
    accepted. ``1`` is not one of them — ``11xxxx`` is a Shanghai bond, ``12xxxx``
    a Shenzhen convertible — so it is refused rather than given an invented
    exchange. The live code table's stock groups are ``0``/``3``/``6``/``9``; the
    delisted list also reaches ``5`` and ``8``, which is why those resolve too.

    Duplicate codes are collapsed: the source's delisted list is 918 entries
    describing 339 distinct symbols, one code appearing as many as 47 times. The
    table is keyed on symbol, so the repeats carry no information.

    Suspension is deliberately never a ``status``: in A-shares it is a daily
    state of an instrument, not an attribute of its listing, so only ``listed``
    and ``delisted`` are ever written.

    ``symbols.name`` is NOT NULL. A symbol with no available name is refused
    rather than dropped, because a silently shorter table is the failure this
    project keeps having to fix.
    """
    delisted_codes = set(delisted)
    ordered: dict[str, None] = {}
    for group in codes.values():
        ordered.update(dict.fromkeys(group))
    ordered.update(dict.fromkeys(delisted_codes))

    rows = []
    for symbol in ordered:
        if symbol not in names:
            raise SourceMalformed(f"{symbol}: no name is known and symbols.name is NOT NULL")
        rows.append(
            {
                "symbol": symbol,
                "name": names[symbol],
                "market": _exchange(symbol),
                "status": "delisted" if symbol in delisted_codes else "listed",
            }
        )
    frame = pd.DataFrame(rows, columns=pd.Index([column.name for column in SYMBOLS.columns]))
    return frame.sort_values("symbol", ignore_index=True)


def _exchange(symbol: str) -> str:
    """Where ``symbol`` trades, or a refusal naming the block that decided nothing.

    Two digits are tried before one: a fund's block is only meaningful as a pair.
    """
    exchange = _EXCHANGE_BY_FUND_PREFIX.get(symbol[:2]) or _EXCHANGE_BY_LEADING.get(symbol[:1])
    if exchange is None:
        raise SourceMalformed(f"{symbol}: this code block has no exchange")
    return exchange


def _placeable(symbol: str) -> bool:
    try:
        _exchange(symbol)
    except SourceMalformed:
        return False
    return True
