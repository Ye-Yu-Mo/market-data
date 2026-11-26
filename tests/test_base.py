"""Tests for the source-adapter interface in ``market_data.markets.base``.

Two properties matter here and nowhere else: every failure a source can report
is catchable as one thing while staying distinguishable as three, and an adapter
can be exercised end to end without a network.
"""

from __future__ import annotations

import pandas as pd
import pytest

from market_data.markets.base import (
    SourceAdapter,
    SourceEmpty,
    SourceError,
    SourceMalformed,
    SourceUnavailable,
)

FAILURES = (SourceUnavailable, SourceEmpty, SourceMalformed)


@pytest.mark.parametrize("kind", FAILURES)
def test_one_except_clause_catches_every_source_failure(kind: type[SourceError]) -> None:
    with pytest.raises(SourceError):
        raise kind("source did not cooperate")


def test_failure_kinds_do_not_overlap() -> None:
    """Catching one kind must never swallow another."""
    for kind in FAILURES:
        others = [other for other in FAILURES if other is not kind]
        assert not any(issubclass(kind, other) for other in others)


@pytest.mark.parametrize("kind", FAILURES)
def test_every_failure_carries_the_raw_payload(kind: type[SourceError]) -> None:
    """Diagnostics live on the base class so logging code never has to branch."""
    assert kind("why", body=b"\x90").body == b"\x90"


def test_unavailable_keeps_the_body_and_the_message() -> None:
    failure = SourceUnavailable("HTTP 500 from upstream", body=b"upstream exploded")
    assert failure.body == b"upstream exploded"
    assert str(failure) == "HTTP 500 from upstream"


def test_body_is_empty_when_there_was_no_response() -> None:
    """A connection that never opened has no payload; that is not an error."""
    assert SourceUnavailable("connection refused").body == b""


class _StubAdapter:
    """A source that answers from memory, to prove the interface is implementable."""

    def __init__(self, payload: bytes) -> None:
        self.payload = payload
        self.queries: list[str] = []

    def fetch(self, query: str) -> bytes:
        self.queries.append(query)
        return self.payload

    def normalize(self, raw: bytes) -> pd.DataFrame:
        return pd.DataFrame({"payload_len": [len(raw)]})


def test_interface_is_satisfiable_without_a_network() -> None:
    adapter: SourceAdapter[str] = _StubAdapter(b"\x91\x01")
    assert adapter.fetch("600633") == b"\x91\x01"
    frame = adapter.normalize(b"\x91\x01")
    assert isinstance(frame, pd.DataFrame)
    assert frame.shape == (1, 1)


def test_fetch_is_the_only_half_that_touches_the_source() -> None:
    """``normalize`` must stay a pure function of its argument."""
    adapter = _StubAdapter(b"\x91\x01")
    adapter.fetch("600633")
    before = list(adapter.queries)
    adapter.normalize(b"\x92\x02\x03")
    adapter.normalize(b"\x92\x02\x03")
    assert adapter.queries == before
