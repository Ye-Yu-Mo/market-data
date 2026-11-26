"""Sample free-stockdb responses: captured bytes, plus a builder.

Two kinds live here, and they answer different questions.

``load`` hands back a **captured** response, byte for byte as the live service
sent it (leaf ``127.0.0.1:7899``, 2026-09-23/24; the files are in
``free_stockdb/``). These are the fixtures that can falsify something: a parser
checked only against bytes we wrote ourselves is checked against its own
assumptions, which is exactly how the first version of this file let a real
one-price day slip past the unit check.

``daily_row`` / ``adjust_event`` **construct** a response instead. They exist for
what a capture cannot supply on demand — one malformed field, a wrong unit, a
missing column — where the whole point is to be wrong in one specific way.

``is_st`` is a bool because the real source sends one; every other numeric field
keeps the type the source uses for it.
"""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any

import msgpack

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false
# ``msgpack`` ships no stubs, so its one call site here is untyped; the assert
# below is what actually establishes the type at runtime.

#: Captured responses, one file per query shape. See ``load``.
CAPTURED_DIR = Path(__file__).with_name("free_stockdb")


def load(name: str) -> bytes:
    """A captured response, exactly as the live service sent it.

    Raises ``FileNotFoundError`` for a name that was never captured — a missing
    fixture must fail loudly rather than quietly become an empty response.
    """
    return (CAPTURED_DIR / name).read_bytes()


#: The 21 fields a ``日k`` row carries, in the order the service emits them.
DAILY_FIELDS = (
    "date",
    "code",
    "name",
    "open",
    "high",
    "low",
    "close",
    "pre_close",
    "volume",
    "amount",
    "turnover",
    "pct_chg",
    "amplitude",
    "is_st",
    "vol_ratio",
    "total_share",
    "float_share",
    "total_mv",
    "float_mv",
    "pe_ttm",
    "pb",
)


def pack(value: object) -> bytes:
    """Encode the way the service does: ``str`` stays ``str``, not ``bin``."""
    packed: object = msgpack.packb(value, use_bin_type=True)
    assert isinstance(packed, bytes), "msgpack.packb must produce bytes"
    return packed


def daily_row(
    day: int,
    code: str,
    name: str,
    *,
    price: float,
    volume: int,
    float_share: float,
    spread: float = 0.01,
    amount: float | None = None,
    turnover: float | None = None,
) -> dict[str, Any]:
    """A ``日k`` row whose unit invariants hold by construction.

    ``spread`` sets the high/low band around ``price``; ``spread=0.0`` produces a
    one-price day, which is where the price check is actually load-bearing.
    ``amount`` defaults to ``price * volume`` — pass it explicitly to reproduce
    the source's own rounding, which is what pushes a one-price day off its
    zero-width band.
    """
    return {
        "date": day,
        "code": code,
        "name": name,
        "open": price,
        "high": price * (1 + spread),
        "low": price * (1 - spread),
        "close": price,
        "pre_close": price,
        "volume": volume,
        "amount": price * volume if amount is None else amount,
        "turnover": round(volume / float_share * 100, 2) if turnover is None else turnover,
        "pct_chg": 0.0,
        "amplitude": 0.0,
        "is_st": False,
        "vol_ratio": 1.0,
        "total_share": float_share,
        "float_share": float_share,
        "total_mv": price * float_share,
        "float_mv": price * float_share,
        "pe_ttm": 10.0,
        "pb": 1.0,
    }


def adjust_event(day: int, *, cum: float, mult: float = 1.0) -> list[Any]:
    """One ``复权`` entry: a keyed pair, because the date lives in the key."""
    return [
        f"复权:600633:{day}",
        {"div": 0.0, "give": 0.0, "trans": 0.0, "mult": mult, "cum": cum},
    ]


# --- constructed replies, for tests that need a whole market -----------------
#
# The captures above are single responses; a pipeline test needs a *market* to
# answer for, so these build replies around the same ``daily_row`` the parser
# tests use, and the parser they exercise is the real one either way.


def daily_series(symbol: str, name: str, days: int = 3, *, first: int = 20260601) -> bytes:
    """Consecutive bars for one symbol, as ``k1=key:<code>&k2=all:`` returns."""
    return pack(
        [
            daily_row(
                first + offset,
                symbol,
                name,
                price=10.0 + offset,
                volume=1_000,
                float_share=1_000_000,
            )
            for offset in range(days)
        ]
    )


def market_day(day: int, names: Mapping[str, str]) -> bytes:
    """A whole-market reply for one trading day, as ``k1=all:&k2=key:<date>`` returns."""
    return pack(
        [
            daily_row(day, symbol, name, price=10.0, volume=1_000, float_share=1_000_000)
            for symbol, name in names.items()
        ]
    )


def adjustments(*events: tuple[str, int, float]) -> bytes:
    """An all-market adjustment reply, as ``复权`` with ``all:``/``all:`` returns."""
    return pack(
        [
            [
                f"复权:{symbol}:{day}",
                {"div": 0.0, "give": 0.0, "trans": 0.0, "mult": 1.0, "cum": cum},
            ]
            for symbol, day, cum in events
        ]
    )
