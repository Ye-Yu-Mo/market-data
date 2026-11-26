"""Contract-vs-reality tests for the ``market_v2`` schema.

``market_data.markets.contracts`` is a frozen snapshot of the reference
database ``~/data/market/market_v2.duckdb``. These tests are what keeps that
snapshot from drifting: when ``stock_tw`` ships a schema migration, they go red
here instead of silently corrupting cross-market research downstream.
"""

from __future__ import annotations

from collections.abc import Iterator

import duckdb
import pytest

from market_data.markets.contracts import (
    MARKET_V2_TABLES,
    REFERENCE_DB_PATH,
    Column,
    TableContract,
    create_table_sql,
    read_contract,
)


@pytest.fixture(scope="module")
def reference() -> Iterator[duckdb.DuckDBPyConnection]:
    """Open the reference database read-only.

    Skips with an explicit reason when it is missing. A silent pass here would
    defeat the entire point of the contract, so the reason names the path.
    """
    if not REFERENCE_DB_PATH.exists():
        pytest.skip(f"reference database missing: {REFERENCE_DB_PATH}")
    connection = duckdb.connect(str(REFERENCE_DB_PATH), read_only=True)
    try:
        yield connection
    finally:
        connection.close()


def test_contract_covers_the_expected_tables() -> None:
    assert [table.name for table in MARKET_V2_TABLES] == [
        "candles",
        "adj_factor",
        "shares_outstanding",
        "symbols",
        "schema_version",
    ]


@pytest.mark.parametrize("contract", MARKET_V2_TABLES, ids=lambda item: item.name)
def test_contract_matches_reference_columns(
    contract: TableContract, reference: duckdb.DuckDBPyConnection
) -> None:
    """Column names, SQL types and nullability match ``PRAGMA table_info``."""
    rows = reference.execute(f'PRAGMA table_info("{contract.name}")').fetchall()
    actual = [(row[1], row[2], not row[3]) for row in rows]
    expected = [(column.name, column.sql_type, column.nullable) for column in contract.columns]
    assert actual == expected


@pytest.mark.parametrize("contract", MARKET_V2_TABLES, ids=lambda item: item.name)
def test_contract_matches_reference_constraints(
    contract: TableContract, reference: duckdb.DuckDBPyConnection
) -> None:
    """Primary key and CHECK constraints match ``duckdb_constraints()``."""
    rows = reference.execute(
        "select constraint_type, constraint_column_names, constraint_text"
        " from duckdb_constraints() where table_name = ? order by constraint_index",
        [contract.name],
    ).fetchall()
    primary_key = tuple(name for kind, names, _ in rows if kind == "PRIMARY KEY" for name in names)
    checks = tuple(text for kind, _, text in rows if kind == "CHECK")
    assert primary_key == contract.primary_key
    assert checks == contract.checks


def test_adj_factor_is_forward_adjusted(reference: duckdb.DuckDBPyConnection) -> None:
    """``factor`` is qfq: every symbol's most recent factor is exactly 1.0.

    Storing free-stockdb's raw ``cum`` here instead would yield hfq, which for
    a long-listed symbol is off by a large factor while still looking like a
    plausible number.
    """
    violations = reference.execute(
        """
        with latest as (select symbol, max(date) as date from adj_factor group by symbol)
        select count(*) from adj_factor a
        join latest l on a.symbol = l.symbol and a.date = l.date
        where a.factor <> 1.0
        """
    ).fetchone()
    assert violations == (0,)


@pytest.mark.parametrize("contract", MARKET_V2_TABLES, ids=lambda item: item.name)
def test_generated_ddl_rebuilds_the_same_schema(contract: TableContract) -> None:
    """DDL built from a contract must produce a table that reads back identical."""
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(create_table_sql(contract))
        assert read_contract(connection, contract.name) == contract
    finally:
        connection.close()


def test_generated_ddl_quotes_identifiers_that_need_it() -> None:
    """Identifiers are quoted unconditionally, so reserved words are harmless.

    DuckDB happens to tolerate an unquoted ``close``, but rejects ``select`` and
    ``order`` outright — exercising those proves the quoting is load-bearing
    rather than decorative.
    """
    awkward = TableContract(
        name="order",
        columns=(
            Column("select", "VARCHAR", False),
            Column("close", "DOUBLE", False),
            Column("values", "INTEGER", True),
        ),
        primary_key=("select",),
    )
    connection = duckdb.connect(":memory:")
    try:
        connection.execute(create_table_sql(awkward))
        assert read_contract(connection, "order") == awkward
    finally:
        connection.close()
