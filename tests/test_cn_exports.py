"""The A-share package's public surface.

M2/M3/M4 import from ``market_data.markets.cn`` and nowhere deeper, so this file
pins what that promise means: every advertised name resolves to the object its
defining module owns (a re-export, never a copy that could drift), and nothing
public sneaks in unadvertised.
"""

from __future__ import annotations

import types

import market_data.markets.cn as markets_cn
from market_data.markets import base, contracts
from market_data.markets.cn import free_stockdb


def test_every_advertised_name_is_importable() -> None:
    for name in markets_cn.__all__:
        assert hasattr(markets_cn, name), name


def test_nothing_public_is_left_unadvertised() -> None:
    """A name added to the module but forgotten in ``__all__`` is not exported."""
    public = {
        name
        for name, value in vars(markets_cn).items()
        if not name.startswith("_") and not isinstance(value, types.ModuleType)
    }
    assert public == set(markets_cn.__all__)


def test_names_are_re_exports_rather_than_copies() -> None:
    assert markets_cn.FreeStockDbAdapter is free_stockdb.FreeStockDbAdapter
    assert markets_cn.plan_market_day is free_stockdb.plan_market_day
    assert markets_cn.plan_adjustments is free_stockdb.plan_adjustments
    assert markets_cn.build_symbols is free_stockdb.build_symbols
    assert markets_cn.CANDLES is contracts.CANDLES
    assert markets_cn.create_table_sql is contracts.create_table_sql
    assert markets_cn.read_contract is contracts.read_contract
    assert markets_cn.SourceUnavailable is base.SourceUnavailable
    assert markets_cn.SourceAdapter is base.SourceAdapter


def test_the_calling_side_needs_nothing_deeper() -> None:
    """A caller's whole vocabulary: fetch, normalise, build the schema."""
    adapter = markets_cn.FreeStockDbAdapter(markets_cn.DEFAULT_BASE_URL)
    assert adapter.base_url == markets_cn.DEFAULT_BASE_URL
    query = markets_cn.plan_history("600633")
    assert query.table == markets_cn.TABLE_DAILY
    ddl = markets_cn.create_table_sql(markets_cn.CANDLES)
    assert '"candles"' in ddl
