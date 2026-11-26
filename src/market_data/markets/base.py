"""Interface for market-data source adapters.

An adapter is split into two halves that never mix:

``fetch``
    The only half that touches the network. It takes one bounded request,
    performs the call, and hands back the raw response body untouched.

``normalize``
    A pure function from those bytes to a contract-shaped ``DataFrame`` — no
    network, no clock, no ambient state. Keeping the halves apart is what makes
    every parsing rule testable from a stored fixture.

Transport is deliberately absent from this interface. An adapter is handed a
``base_url`` and nothing else; whether that URL is direct, tunnelled or
forwarded is a deployment concern, and letting it leak down here would mean the
data layer changes whenever the network does. This project reaches free-stockdb
over an SSH tunnel today — the adapter neither knows nor cares.

Failures come as three distinct types rather than one, because two of them look
identical on the wire:

``SourceUnavailable``
    The source could not be reached, timed out, or answered with a non-2xx
    status.

``SourceEmpty``
    HTTP 200 carrying an empty payload. **This is not "the symbol has no
    data"** — a mistyped query returns exactly the same bytes. Collapsing the
    two would turn a broken pipeline into a quiet shortage of history.

``SourceMalformed``
    The payload decoded, but does not carry the fields the contract requires.

All three derive from ``SourceError``, so a caller that only needs to say "the
source did not give me usable data" can catch one thing.
"""

from __future__ import annotations

from typing import Protocol

import pandas as pd

__all__ = [
    "SourceAdapter",
    "SourceEmpty",
    "SourceError",
    "SourceMalformed",
    "SourceUnavailable",
]


class SourceError(Exception):
    """Base class for every failure a source adapter reports.

    ``body`` holds whatever the source actually sent. It sits on the base class
    rather than on one subclass so that logging code can always read
    ``exc.body`` without first asking which kind of failure it is holding. A
    connection that never opened leaves it empty rather than missing.
    """

    def __init__(self, message: str, *, body: bytes = b"") -> None:
        super().__init__(message)
        self.body = body


class SourceUnavailable(SourceError):
    """The source could not be reached, timed out, or answered with a non-2xx status."""


class SourceEmpty(SourceError):
    """The source answered 200 with an empty payload."""


class SourceMalformed(SourceError):
    """The payload decoded, but does not carry the fields the contract requires."""


class SourceAdapter[RequestT](Protocol):
    """A market-data source as everything above it sees one.

    Generic over the request type: what a request has to contain is the source's
    own business (free-stockdb spells it in ``k1``/``k2``), and this module has
    no business guessing at it.
    """

    def fetch(self, query: RequestT) -> bytes:
        """Perform one bounded request and return the raw response body."""
        ...

    def normalize(self, raw: bytes) -> pd.DataFrame:
        """Turn a raw response body into a contract-shaped frame."""
        ...
