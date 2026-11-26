"""Schema contract for the shared market store.

The contract is a frozen snapshot of the reference database
``~/data/market/market_v2.duckdb`` (schema v5, produced by ``stock_tw``). It is
the single alignment authority for writing A-share data: the A-share store must
match these tables column for column, or the research code that reads
``market_v2`` cannot read it unchanged. ``read_contract`` derives a contract
from a live database, and ``tests/test_contracts.py`` asserts the constants
below still equal what the real file says — edit them by hand and the suite
goes red, which is the point.

Column semantics that a type signature cannot express
-----------------------------------------------------

``candles`` / ``adj_factor`` must stay one-to-one
    ``factor_research`` loads them with an **inner** join on ``(symbol, date)``
    (``factor_research/qlib/duckdb.py``). A candle without a matching factor row
    is dropped silently — no error, just a shorter history. The same query also
    filters ``close > 0 AND volume > 0``, so a suspended A-share symbol
    (``volume = 0``) loses the whole row by design. That filter is the consumer's
    contract, not corrupt data: the quality gate must not reject suspensions as
    if they were bad rows.

``candles.open`` is nullable, and that is load-bearing
    ``stock_tw``'s ``0005_open_nullable`` migration made ``open`` nullable so an
    unusable open could be recorded as NULL rather than a fake price. Consumers
    keep a bar whose ``open IS NULL`` and only drop one whose ``close IS NULL``.
    Backfilling A-share opens with ``0`` instead of NULL would silently turn
    those bars into a different kind of bar.

``candles.amount`` must never be NULL
    Consumers substitute ``Decimal(0)`` for a missing ``amount`` without
    complaining, which poisons any factor built on it. The A-share source always
    has an amount, so a NULL here means our adapter dropped it.

``adj_factor.factor`` is **qfq** (forward-adjusted)
    Every symbol's most recent ``factor`` is exactly ``1.0``; older rows scale
    the price back to today's basis. The free-stockdb source exposes a raw
    ``cum`` series instead, which is *hfq*; it must be divided by its own latest
    value (``factor = cum / cum_latest``) before it is stored here. Writing
    ``cum`` directly stores hfq, which for a long-listed symbol is off by a
    large factor while still looking like an ordinary number — no error, wrong
    research. Note also that ``cum_latest`` moves whenever a new distribution
    happens, which shifts the whole series: a writer must be able to overwrite
    previously written factor rows, not only append.

``candles.turnover`` has no agreed meaning yet
    Measured on 2026-09-23: every value in the reference database is an integer
    (mean 935, max 1,150,086) and correlates 0.89 in log space with ``volume``;
    TSMC (``2330``) sits around 85k. That is the shape of a **trade count**, not
    a percentage. The A-share source's ``turnover`` is provably a percentage
    (``volume / float_share * 100``). So the two same-named fields mean
    different things, and the mapping into this column is **not yet decided** —
    resolving it is a prerequisite for the adapter, not a detail of it.
    ``factor_research`` currently ignores this column; it maps ``amount`` into
    its own ``turnover`` field.

``schema_version`` is a migration ledger, not a version number
    The reference database holds **five** rows, ``1`` through ``5`` — every
    migration ever applied, not the current version. Writing a single row (or
    all five) into the A-share store is a decision to make deliberately rather
    than a shape to copy.

``shares_outstanding.shares`` is shares **issued**, not the free float
    Taiwan's ``週轉率`` divides traded shares by shares issued, so that is what
    this column holds, and it is what an A-share's ``total_share`` means too. The
    A-share source also publishes ``float_share``, and an A-share's *conventional*
    turnover ratio divides by the float — ``factor_research``'s ``turnover``
    helper (``volume / shares``) therefore understates for a company with
    restricted shares, while its ``mktcap`` (``close * shares``) gets the
    conventional total. The two readings cannot both be right from one column,
    and adding a second would change the contract, so the choice is recorded here
    instead: an A-share turnover factor built on this column is a turnover of
    *issued* shares.

``load_log`` is a sixth table, and deliberately not a mirror
    "Which load put this in the store?" is asked *on* the database, so it is
    answered inside it — where the answer travels with a copy, survives a
    rollback correctly, and moves with the file. A sidecar JSON does none of
    those. The mirroring rule constrains the columns of the tables that are
    mirrored; it does not forbid a table a consumer never reads. It is recorded
    here, beside ``symbols`` holding more rows than the reference database's and
    ``schema_version`` holding one row instead of five, so that none of the three
    is later "fixed" by someone who reasonably assumes drift.

``source_gap`` records symbols the source has nothing for
    Some codes in the universe have no bars at all — asked about, they answer
    HTTP 200 with an empty array rather than an error. That is a different fact
    from "not fetched yet", and without it a resumable run would ask for them
    again forever. It is not a mirror either, for the same reason ``load_log``
    is not: it is bookkeeping about the load, not about the market.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import duckdb

#: Canonical location of the reference database. It lives outside every
#: repository because it is a shared research artifact, not anyone's source
#: tree. Never write to it: every connection in this project opens it
#: ``read_only=True``.
REFERENCE_DB_PATH = Path.home() / "data" / "market" / "market_v2.duckdb"

#: Where the A-share store lives. It sits beside the reference database because
#: both are shared research artifacts rather than anyone's source tree, and in a
#: separate file because ``market_v2.candles`` has no ``market`` column — six
#: digit A-share codes and six digit Taiwan codes would collide in one table.
A_SHARE_DB_PATH = Path.home() / "data" / "market" / "market_a.duckdb"


@dataclass(frozen=True, slots=True)
class Column:
    """One column of a table contract."""

    name: str
    sql_type: str
    nullable: bool


@dataclass(frozen=True, slots=True)
class TableContract:
    """A table's columns, primary key and CHECK constraints.

    ``checks`` holds DuckDB's own normalised ``constraint_text`` verbatim, so the
    comparison against a live database is exact rather than approximate.
    """

    name: str
    columns: tuple[Column, ...]
    primary_key: tuple[str, ...] = ()
    checks: tuple[str, ...] = ()


CANDLES = TableContract(
    name="candles",
    columns=(
        Column("symbol", "VARCHAR", False),
        Column("date", "DATE", False),
        Column("open", "DOUBLE", True),
        Column("high", "DOUBLE", False),
        Column("low", "DOUBLE", False),
        Column("close", "DOUBLE", False),
        Column("volume", "BIGINT", False),
        Column("amount", "DOUBLE", True),
        Column("turnover", "DOUBLE", True),
    ),
    primary_key=("symbol", "date"),
)

ADJ_FACTOR = TableContract(
    name="adj_factor",
    columns=(
        Column("symbol", "VARCHAR", False),
        Column("date", "DATE", False),
        Column("factor", "DOUBLE", False),
    ),
    primary_key=("symbol", "date"),
)

SHARES_OUTSTANDING = TableContract(
    name="shares_outstanding",
    columns=(
        Column("symbol", "VARCHAR", False),
        Column("date", "DATE", False),
        Column("shares", "BIGINT", False),
    ),
    primary_key=("symbol", "date"),
)

SYMBOLS = TableContract(
    name="symbols",
    columns=(
        Column("symbol", "VARCHAR", False),
        Column("name", "VARCHAR", False),
        Column("market", "VARCHAR", False),
        Column("status", "VARCHAR", False),
    ),
    primary_key=("symbol",),
    checks=("CHECK((status IN ('listed', 'delisted', 'suspended')))",),
)

SCHEMA_VERSION = TableContract(
    name="schema_version",
    columns=(Column("version", "INTEGER", False),),
    primary_key=("version",),
)

LOAD_LOG = TableContract(
    name="load_log",
    columns=(
        Column("loaded_at", "VARCHAR", False),
        Column("market", "VARCHAR", False),
        Column("dataset", "VARCHAR", False),
        Column("start_date", "DATE", False),
        Column("end_date", "DATE", False),
        Column("candles", "BIGINT", False),
        Column("adj_factor", "BIGINT", False),
        Column("shares_outstanding", "BIGINT", False),
        Column("symbols", "BIGINT", False),
        Column("checksum", "VARCHAR", False),
        Column("adjustment", "VARCHAR", False),
        Column("schema_version", "INTEGER", False),
    ),
    #: A load is identified by when it ran. Re-running the same batch writes a
    #: second row on purpose: it happened twice, and hiding that would make the
    #: log unable to say how many times the store was rewritten.
    #:
    #: **This table grows with the work, not with the calendar.** A load is one
    #: symbol, so a full run writes about 5,800 rows and a daily update about as
    #: many — roughly a megabyte a day at current row size. That is a deliberate
    #: trade, not an oversight: it is what lets the log answer *which load put
    #: this symbol's rows in*, which a per-run row could not. The growth is
    #: bounded by ``pipeline.trim_load_log``, which is the "for how long".
    primary_key=("market", "dataset", "loaded_at"),
)

#: The reference tables this project mirrors. ``market_v2`` also holds
#: ``fx_net_buy``, ``margin`` and ``monthly_revenue``; those are Taiwan-specific
#: datasets with no A-share counterpart in this milestone and are deliberately
#: absent here.
MARKET_V2_TABLES = (CANDLES, ADJ_FACTOR, SHARES_OUTSTANDING, SYMBOLS, SCHEMA_VERSION)

SOURCE_GAP = TableContract(
    name="source_gap",
    columns=(
        Column("market", "VARCHAR", False),
        Column("symbol", "VARCHAR", False),
        Column("observed_at", "VARCHAR", False),
    ),
    #: One row per symbol, however often it is asked about: the fact is "the
    #: source has nothing for this code", and it stays true until it does not.
    primary_key=("market", "symbol"),
)

#: Everything an A-share store contains. It is ``MARKET_V2_TABLES`` plus
#: ``LOAD_LOG`` and ``SOURCE_GAP``, which the reference database has no
#: equivalent of — the two places this store is deliberately not a mirror,
#: alongside ``symbols`` being fuller than the reference database's and
#: ``schema_version`` holding a single row instead of five.
STORE_TABLES = (*MARKET_V2_TABLES, LOAD_LOG, SOURCE_GAP)


def create_table_sql(contract: TableContract, *, if_not_exists: bool = False) -> str:
    """Render ``CREATE TABLE`` DDL for a contract.

    Every identifier is quoted unconditionally. Quoting all of them rather than
    detecting reserved words removes the special case entirely: ``close`` needs
    quotes here, and ``select`` or ``order`` would be a syntax error without
    them.
    """
    columns = ", ".join(
        f'"{column.name}" {column.sql_type}' + ("" if column.nullable else " NOT NULL")
        for column in contract.columns
    )
    clauses = [columns]
    if contract.primary_key:
        keys = ", ".join(f'"{name}"' for name in contract.primary_key)
        clauses.append(f"PRIMARY KEY({keys})")
    clauses.extend(contract.checks)
    guard = "IF NOT EXISTS " if if_not_exists else ""
    return f'CREATE TABLE {guard}"{contract.name}" ({", ".join(clauses)})'


def read_contract(connection: duckdb.DuckDBPyConnection, table: str) -> TableContract:
    """Derive a table contract from a live database.

    This is the authority the module-level constants are copies of. A table that
    does not exist raises DuckDB's own ``CatalogException``, which already names
    the table — there is nothing to add by wrapping it.
    """
    rows = connection.execute("select * from pragma_table_info(?)", [table]).fetchall()
    columns = tuple(Column(row[1], row[2], not row[3]) for row in rows)
    constraints = connection.execute(
        "select constraint_type, constraint_column_names, constraint_text"
        " from duckdb_constraints() where table_name = ? order by constraint_index",
        [table],
    ).fetchall()
    primary_key = tuple(
        name for kind, names, _ in constraints if kind == "PRIMARY KEY" for name in names
    )
    checks = tuple(text for kind, _, text in constraints if kind == "CHECK")
    return TableContract(table, columns, primary_key, checks)
