"""The A-share store, read back through the consumer's own loader.

``PLAN.md`` set this milestone's acceptance as "``factor_research``'s loader can
open it". Real data shows that is not enough. Write the adjustment **events**
rather than the daily series and the loader opens the file, runs its SQL
happily, and returns a fraction of the history — the inner join drops every bar
without a matching factor row and says nothing at all. So these tests assert
**counts**, which is the only form of this check that can fail.

The loader lives in another repository, so this module skips — with a reason,
never silently — when that repository is not where it is expected.
"""

# pyright: reportMissingImports=false, reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only. ``factor_research`` is reached through a path insert rather than an
# installed package, so pyright cannot resolve it — the module-level skip above is
# what makes that safe. The rest is the same pytest/pandas trade as elsewhere.

from __future__ import annotations

import os
import sys
from pathlib import Path

import pytest

from fixtures.free_stockdb import load
from market_data.markets.cn import FreeStockDbAdapter, daily_factors
from market_data.markets.contracts import ADJ_FACTOR, CANDLES
from market_data.offline.duckdb_store import open_store, write_rows

FACTOR_RESEARCH_SRC = Path(
    os.environ.get("FACTOR_RESEARCH_SRC")
    # factor-research 已移到 trade_system/ 下 (原先在 ~/Documents/ 顶层).
    # 旧路径会让这个模块静默 skip, M2 最核心的"行数等于消费者口径"验收就跑了空.
    or Path.home() / "Documents" / "trade_system" / "factor-research" / "src"
)
if not (FACTOR_RESEARCH_SRC / "factor_research" / "qlib" / "duckdb.py").is_file():
    pytest.skip(
        f"factor_research not found under {FACTOR_RESEARCH_SRC}; "
        "set FACTOR_RESEARCH_SRC to its src/ directory",
        allow_module_level=True,
    )
sys.path.insert(0, str(FACTOR_RESEARCH_SRC))

from factor_research.qlib.contracts import Market  # noqa: E402
from factor_research.qlib.duckdb import load_standard_dataset  # noqa: E402

#: The loader's own filter, restated so the expectation is computed the same way
#: the query computes it: `PLAN.md` never mentions that suspended A-share days
#: (``volume = 0``) are dropped by the consumer, and a test that forgot would
#: disagree with the loader for the wrong reason.
LOADER_ROWS = (
    "select count(*) from candles c join adj_factor a using (symbol, date)"
    " where c.close > 0 and c.volume > 0"
)


def _borrowed_market(symbol: str) -> Market:
    """``Market`` has no A-share member, so one has to be borrowed.

    ``TWSE``/``TPEX``/``ESB`` are the only values, and the loader keys securities
    as ``f"{market.value}:{symbol}"``. The counts below do not depend on which is
    borrowed — but needing to borrow one is the counterexample to "the research
    code works unchanged across markets".
    """
    return Market.TWSE


@pytest.fixture
def populated(tmp_path):
    """A store holding five real bars and the daily factors that match them."""
    bars = FreeStockDbAdapter().normalize(load("daily_range_600633.bin"))
    events = FreeStockDbAdapter().adjustment_events(load("adjust_600633.bin"))
    path = tmp_path / "market_a.duckdb"
    with open_store(path) as connection:
        write_rows(connection, CANDLES, bars)
        write_rows(connection, ADJ_FACTOR, daily_factors(events, bars))
        counted = connection.execute(LOADER_ROWS).fetchone()
    assert counted is not None
    return path, counted[0]


def test_the_loader_returns_every_bar_the_filters_keep(populated) -> None:
    path, expected = populated
    dataset = load_standard_dataset(path, market_resolver=_borrowed_market)
    assert expected > 0, "the fixture must survive the consumer's own filters"
    assert len(dataset.bars) == expected


def test_writing_events_instead_of_days_loses_the_history_silently(tmp_path) -> None:
    """The failure this module exists for, reproduced rather than described.

    ``normalize_factors`` returns one row per distribution — 25 for this symbol's
    whole life. Written straight through, the loader returns **zero** bars for
    these dates and raises nothing: no exception, no warning, just missing
    history. If this ever starts agreeing with the test above, the assertion
    above has stopped meaning anything.
    """
    bars = FreeStockDbAdapter().normalize(load("daily_range_600633.bin"))
    events = FreeStockDbAdapter().normalize_factors(load("adjust_600633.bin"))
    path = tmp_path / "events_only.duckdb"
    with open_store(path) as connection:
        write_rows(connection, CANDLES, bars)
        write_rows(connection, ADJ_FACTOR, events)
        counted = connection.execute(LOADER_ROWS).fetchone()
    dataset = load_standard_dataset(path, market_resolver=_borrowed_market)
    assert counted == (0,)
    assert dataset.bars == ()
