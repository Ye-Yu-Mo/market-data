"""Building and filling the A-share DuckDB store.

The store mirrors ``market_v2`` column for column, because its consumer reads
that schema: ``factor_research``'s loader opens a DuckDB file, queries
``candles`` and inner-joins ``adj_factor``, and knows nothing about where the
rows came from. Anything this module gets wrong about the schema is a failure
that appears in someone else's research result rather than here.

Two rules run through the whole module.

**The reference database is never opened for writing.** It is shared with
``stock_tw`` and ``factor_research``, it is 2.3 GB of irreplaceable history, and
the only thing protecting it is the guard in ``open_store`` — which runs before
the connection is made, not after, because after is too late.

**Writes are idempotent and overwriting.** M3 and M4 will re-run the same batch
against the same file repeatedly, and a distribution moves every historical
factor rather than appending one row, so every table is written with
``ON CONFLICT DO UPDATE``. A plain ``INSERT`` would fail on the second run, and
``DO NOTHING`` would leave stale factors behind under the new base — silently.
"""

# pyright: reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownVariableType=false
# Silenced for this module only, and only for the three rules that fire because
# ``pandas``' stubs are partial — the frames crossing this module are typed, but
# a grouped selection or a ``dtypes`` lookup reads back as ``Unknown``. The rule
# that catches real mistakes, ``reportArgumentType``, stays on.

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

from market_data.markets.contracts import (
    A_SHARE_DB_PATH,
    REFERENCE_DB_PATH,
    SCHEMA_VERSION,
    STORE_TABLES,
    TableContract,
    create_table_sql,
)

__all__ = ["SCHEMA_VERSION_VALUE", "open_store", "write_rows"]

#: The schema version a store built by this module records.
#:
#: ``market_v2.schema_version`` holds **five** rows, ``1`` through ``5``: one per
#: migration ``stock_tw`` ever applied, with the current version being
#: ``MAX(version)``. This store replays none of those migrations — it is created
#: directly in their final shape — so writing all five would claim a history it
#: never had, and writing anything but ``5`` would claim a shape it does not
#: have. One row, the version whose shape was copied.
SCHEMA_VERSION_VALUE = 5

#: The view a batch is staged under. One name, because one batch is in flight at
#: a time and reusing it keeps the SQL above independent of what is being written.
_STAGING_VIEW = "incoming_rows"

#: Which pandas dtype kinds can hold each SQL type without a silent conversion.
#: ``VARCHAR`` deliberately excludes the numeric kinds: a symbol that became a
#: number has already lost its leading zeros, and no later step can tell.
_STORABLE_KINDS: dict[str, tuple[str, ...]] = {
    "VARCHAR": ("O", "U", "S"),
    "DATE": ("M", "O"),
    "DOUBLE": ("f", "i", "u"),
    "BIGINT": ("i", "u"),
    "INTEGER": ("i", "u"),
}


def open_store(path: Path = A_SHARE_DB_PATH) -> duckdb.DuckDBPyConnection:
    """Open the A-share store, creating the contract's tables if they are absent.

    The guard compares *resolved* paths, so a symlink, a relative path or a
    ``..`` detour onto the reference database is refused just the same. It runs
    before ``duckdb.connect``: once the connection exists the file has already
    been opened, and for the reference database that is the whole harm.
    """
    if path.resolve() == REFERENCE_DB_PATH.resolve():
        raise ValueError(
            f"refusing to open the reference database {REFERENCE_DB_PATH} for writing: "
            "it is read-only and shared with stock_tw and factor_research"
        )
    connection = duckdb.connect(str(path))
    for contract in STORE_TABLES:
        connection.execute(create_table_sql(contract, if_not_exists=True))
    # Recorded here rather than by the caller: the tables just created *are* that
    # version's shape, so the two cannot drift apart. On an existing store the
    # insert conflicts on the primary key and does nothing.
    write_rows(connection, SCHEMA_VERSION, pd.DataFrame({"version": [SCHEMA_VERSION_VALUE]}))
    return connection


def write_rows(
    connection: duckdb.DuckDBPyConnection, contract: TableContract, frame: pd.DataFrame
) -> int:
    """Upsert ``frame`` into ``contract``'s table, keyed on its primary key.

    Returns how many rows were offered. DuckDB does not report inserted versus
    updated rows and the caller only needs to know the batch was accepted.

    An empty frame is a no-op that returns ``0`` rather than an error — by the
    time a batch reaches here the source adapter has already refused an empty
    reply as ``SourceEmpty``, so an empty frame means "nothing new today", not
    "the fetch failed".
    """
    if frame.empty:
        return 0
    _require_contract_frame(contract, frame)
    _require_unique_keys(contract, frame)
    connection.register(_STAGING_VIEW, _for_storage(contract, frame))
    try:
        connection.execute(_upsert_sql(contract))
    finally:
        connection.unregister(_STAGING_VIEW)
    return len(frame)


def _upsert_sql(contract: TableContract) -> str:
    """``INSERT ... BY NAME ... ON CONFLICT DO UPDATE``, built from the contract.

    ``BY NAME`` matches on column names rather than position, so a frame whose
    columns are ordered differently still lands correctly. When the primary key
    covers every column there is nothing to update and the conflict clause
    becomes ``DO NOTHING`` — SQL has no empty ``SET`` list.
    """
    conflict = ", ".join(f'"{name}"' for name in contract.primary_key)
    keys = set(contract.primary_key)
    assignments = ", ".join(
        f'"{column.name}" = excluded."{column.name}"'
        for column in contract.columns
        if column.name not in keys
    )
    action = f"DO UPDATE SET {assignments}" if assignments else "DO NOTHING"
    return (
        f'INSERT INTO "{contract.name}" BY NAME SELECT * FROM {_STAGING_VIEW} '
        f"ON CONFLICT ({conflict}) {action}"
    )


def _require_contract_frame(contract: TableContract, frame: pd.DataFrame) -> None:
    """Refuse a frame the contract cannot hold, before DuckDB coerces it.

    The column set is compared exactly: ``BY NAME`` quietly ignores a column it
    does not recognise, so a frame that grew one would lose it without a word.
    """
    expected = [column.name for column in contract.columns]
    if list(frame.columns) != expected:
        raise ValueError(f"{contract.name}: expected columns {expected}, got {list(frame.columns)}")
    for column in contract.columns:
        allowed = _STORABLE_KINDS[column.sql_type]
        kind = frame.dtypes[column.name].kind
        if kind not in allowed:
            raise ValueError(
                f"{contract.name}.{column.name}: a {kind!r}-kind column cannot hold "
                f"{column.sql_type} without a silent conversion"
            )


def _require_unique_keys(contract: TableContract, frame: pd.DataFrame) -> None:
    """Refuse a batch that names the same key twice.

    Measured on DuckDB 1.5.5: two rows sharing a key inside one ``INSERT`` are
    accepted silently and the **first** survives. Nothing downstream can tell
    that this happened, which is the failure mode this project keeps paying for,
    so the batch is rejected instead. Diagnosing *why* a batch had duplicates is
    the quality gate's job (M3) — this only refuses to store the result.
    """
    keys = list(contract.primary_key)
    duplicated = frame.duplicated(subset=keys, keep=False)
    if not duplicated.any():
        return
    sample = [
        tuple(str(value) for value in row)
        for row in frame.loc[duplicated, keys].head(3).itertuples(index=False, name=None)
    ]
    raise ValueError(f"{contract.name}: duplicate primary key rows, e.g. {sample}")


def _for_storage(contract: TableContract, frame: pd.DataFrame) -> pd.DataFrame:
    """Hand DuckDB values in the shape its columns expect.

    ``date`` arrives as ``datetime64`` and leaves as plain ``date`` objects.
    Leaving the conversion to DuckDB would work, but as an implicit cast whose
    failure is a wrong value rather than an exception.
    """
    payload = frame.copy()
    for column in contract.columns:
        if column.sql_type == "DATE" and payload[column.name].dtype.kind == "M":
            payload[column.name] = payload[column.name].dt.date
    return payload
