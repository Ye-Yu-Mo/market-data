"""A-share sources: the free-stockdb adapter and the schema it fills.

This is the single import point for everything A-share. The loader (M2), the
quality gate (M3) and the CLI (M4) reach for ``market_data.markets.cn`` and
nothing deeper, so ``free_stockdb`` can be reorganised — split, renamed, given
siblings for other sources — without any of them noticing.

The names below are re-exports, not copies, and they come from three places:

* ``market_data.markets.contracts`` owns the schema — the tables an A-share
  store must match, and the DDL builder that produces them.
* ``market_data.markets.base`` owns the source interface and the three failures
  a source can report.
* ``market_data.markets.cn.free_stockdb`` owns the free-stockdb adapter: how to
  ask it a bounded question, and how to turn its replies into those tables.

The quality gate is deliberately *not* here: ``market_data.offline.quality``
serves every market, and re-exporting it would put two import paths on one
frozen public API.
"""

from market_data.markets.base import (
    SourceAdapter,
    SourceEmpty,
    SourceError,
    SourceMalformed,
    SourceUnavailable,
)
from market_data.markets.cn.free_stockdb import (
    DEFAULT_BASE_URL,
    TABLE_ADJUST,
    TABLE_CODES,
    TABLE_DAILY,
    TABLE_DELISTED,
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
    "DEFAULT_BASE_URL",
    "MARKET_V2_TABLES",
    "REFERENCE_DB_PATH",
    "SCHEMA_VERSION",
    "SHARES_OUTSTANDING",
    "SYMBOLS",
    "TABLE_ADJUST",
    "TABLE_CODES",
    "TABLE_DAILY",
    "TABLE_DELISTED",
    "Column",
    "FreeStockDbAdapter",
    "Query",
    "SourceAdapter",
    "SourceEmpty",
    "SourceError",
    "SourceMalformed",
    "SourceUnavailable",
    "TableContract",
    "build_symbols",
    "create_table_sql",
    "daily_factors",
    "extract_names",
    "extract_shares",
    "parse_code_table",
    "parse_delisted",
    "parse_keys",
    "plan_adjustments",
    "plan_code_table",
    "plan_delisted",
    "plan_history",
    "plan_market_day",
    "read_contract",
]
