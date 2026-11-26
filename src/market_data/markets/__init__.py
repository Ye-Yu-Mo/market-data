"""Market data contracts and per-market source adapters."""

from market_data.markets.contracts import (
    ADJ_FACTOR,
    CANDLES,
    MARKET_V2_TABLES,
    REFERENCE_DB_PATH,
    SCHEMA_VERSION,
    SHARES_OUTSTANDING,
    SYMBOLS,
    Column,
    TableContract,
    create_table_sql,
    read_contract,
)

__all__ = [
    "ADJ_FACTOR",
    "CANDLES",
    "MARKET_V2_TABLES",
    "REFERENCE_DB_PATH",
    "SCHEMA_VERSION",
    "SHARES_OUTSTANDING",
    "SYMBOLS",
    "Column",
    "TableContract",
    "create_table_sql",
    "read_contract",
]
