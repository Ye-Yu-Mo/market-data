"""Shared OHLCV quality gates.

Every rule below is calibrated against captured A-share responses, because the
alternative has already been tried twice and failed twice: a price-unit tolerance
set to ``1e-9`` from first principles rejected 11 of 5,488 stock rows in a single
market day, and its replacement — set to ``1e-3`` from that same one day — still
rejected about 10 rows across twelve symbols' history. Both were guesses wearing
the costume of thresholds. The numbers behind the current rules are recorded
here; the fixtures that can falsify them live in ``tests/fixtures/free_stockdb/``
and ``tests/test_quality_calibration.py`` runs each boundary case through.

Calibration, measured over 40,245 rows of eight stocks' 26-year history plus one
whole market day (7,516 rows, 2026-08-31)
------------------------------------------------------------------------------

``invalid_ohlc``
    **0 rows.** The rule is safe.
``negative_value``
    **0 rows.** Safe.
one-price days (``high == low``)
    **38 rows market-wide on 2026-08-31, 11 of them stocks.** Legal. The rule
    fires on ``high < low`` only, so equality passes.
``amount == 0`` with ``volume > 0``
    **40 rows market-wide, every one a fund or bond, none a stock.** Accepted:
    a zero cannot be judged for units, and the adapter skips it for the same
    reason.
suspended rows (``volume == 0``)
    **0 of 40,245 rows from eight live stocks** — and that first measurement was
    wrong to generalise from. A symbol in its delisting period trades nothing,
    and it is recorded as ``volume == 0`` with ``open``/``high``/``low`` zeroed
    and ``close`` carried forward: 600421 on 20260624 reads 0 / 0 / 0 against a
    close of 0.24. **2 of 150 uniformly sampled delisted symbols** carry one,
    sometimes mid-series. (An earlier count of 12% came from sampling the 918
    -entry delisted list, which over-selects the codes that stay in it longest —
    the same biased-sample mistake this module's other rules were calibrated to
    avoid.) On 2026-08-31 itself, only two rows, both funds. So nothing here
    rejects a zero volume, and ``invalid_ohlc`` is only applied to rows that
    traded, because a day with no trades has no bounds to violate.
``pct_chg`` / ``pre_close``
    **Unreliable.** 000010 disagrees with ``close / pre_close - 1`` on 2,409 of
    5,463 rows (44.1%), concentrated in 2000-2013. No rule reads either field,
    and the adapter does not carry them onto the contract at all.
price-limit rules
    **There are none, and stratifying by ``is_st`` is not a fix.** Measured
    single-day moves on ST stocks reach 17.29%, 22.78% and 12.53%, with 366-632
    days above 5.5%; these are suspension-resumption gaps, which no threshold can
    tell from corruption. A price-limit rule would quietly delete real history.
2001-2003 contradictions
    **Accepted, deliberately.** 600633 has 63 rows whose ``amount / volume``
    falls outside the day's range — worst 2.7e-2, all in 2001-2002 — and some
    carry an impossible ``pre_close`` as well. They are the source's oldest
    records disagreeing with themselves; rejecting them would put a hole in 2002
    rather than in a place anyone would look. This is the decision, recorded
    here so it does not have to be re-made.

What the gate deliberately does not check
-----------------------------------------
Column *presence* is verified; nothing else about a frame's shape is. M2's
``write_rows`` already refuses a frame whose column set differs, whose dtype
cannot hold the SQL type, or which repeats a primary key, and duplicating that
here would only make "which layer rejected this" harder to answer. The rules
below are about values.

Duplicate keys are the one overlap, and it is intentional: M2 refuses the batch
because DuckDB silently keeps the first of a repeated key, and the gate reaches
the same verdict with a readable sample. Both reject the whole batch — there is
no "keep the last one" anywhere in this project.
"""

# pyright: reportUnknownMemberType=false, reportUnknownVariableType=false, reportUnknownArgumentType=false, reportArgumentType=false, reportReturnType=false, reportAttributeAccessIssue=false, reportIndexIssue=false

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd


@dataclass(frozen=True)
class QualityIssue:
    code: str
    message: str
    count: int = 1
    sample: tuple[str, ...] = ()


@dataclass
class QualityReport:
    market: str
    dataset: str
    rows: int = 0
    symbols: int = 0
    min_date: str | None = None
    max_date: str | None = None
    issues: list[QualityIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.issues

    def add(self, code: str, message: str, count: int = 1, sample: tuple[str, ...] = ()) -> None:
        self.issues.append(QualityIssue(code, message, count, sample))


def check_candles(frame: pd.DataFrame, market: str, dataset: str = "candles") -> QualityReport:
    report = QualityReport(market, dataset, rows=len(frame))
    required = {"symbol", "date", "open", "high", "low", "close", "volume"}
    missing = sorted(required - set(frame.columns))
    if missing:
        report.add("missing_columns", f"missing columns: {', '.join(missing)}")
        return report
    if frame.empty:
        report.add("empty_dataset", "dataset has no rows")
        return report

    report.symbols = int(frame["symbol"].nunique())
    dates = pd.to_datetime(frame["date"], errors="coerce")
    if dates.isna().any():
        report.add("invalid_date", "date contains invalid values", int(dates.isna().sum()))
    else:
        report.min_date = dates.min().date().isoformat()
        report.max_date = dates.max().date().isoformat()

    duplicate = frame.duplicated(subset=["symbol", "date"], keep=False)
    if duplicate.any():
        sample = tuple(frame.loc[duplicate, "symbol"].astype(str).head(5))
        report.add("duplicate_key", "duplicate symbol/date keys", int(duplicate.sum()), sample)

    numeric = [
        column for column in ("open", "high", "low", "close", "volume", "amount") if column in frame
    ]
    values = frame[numeric].apply(pd.to_numeric, errors="coerce")
    invalid_numeric = values.isna().any(axis=1)
    if invalid_numeric.any():
        report.add(
            "invalid_numeric",
            "numeric columns contain non-numeric values",
            int(invalid_numeric.sum()),
        )

    if all(column in frame for column in ("open", "high", "low", "close")):
        # The bounds are a claim about a day's trades, so a day without trades has
        # none to violate. The source marks one by zeroing ``open``/``high``/``low``
        # and carrying ``close`` forward with ``volume == 0``: 600421 on 20260624
        # reads 0 / 0 / 0 against a close of 0.24. Those are real rows for a symbol
        # in its delisting period — 2 of 150 uniformly sampled delisted symbols have
        # one, sometimes mid-series rather than last — and rejecting them deletes
        # exactly the history the survivorship-bias work exists to keep. Measured:
        # no *live* stock's bars do this (0 of 400 sampled), so nothing is lost.
        traded = values["volume"].fillna(0) > 0
        invalid_ohlc = (
            (values["high"] < values["low"])
            | (values["open"] < values["low"])
            | (values["open"] > values["high"])
            | (values["close"] < values["low"])
            | (values["close"] > values["high"])
        ) & traded
        if invalid_ohlc.any():
            report.add(
                "invalid_ohlc", "OHLC values violate low/high bounds", int(invalid_ohlc.sum())
            )

    negative = values.lt(0).any(axis=1)
    if negative.any():
        report.add("negative_value", "numeric values must not be negative", int(negative.sum()))
    return report


class QualityGateFailed(ValueError):
    """A batch did not pass its gates, and the report says which ones failed.

    Subclasses ``ValueError`` because ``require_clean`` already raised one and
    ``PLAN.md`` lists it among the interfaces that must not break: a caller
    written against ``ValueError`` keeps working, while a caller that wants the
    per-rule breakdown — which rule, how many rows, a sample — asks for
    ``report``.
    """

    def __init__(self, report: QualityReport) -> None:
        details = "; ".join(
            f"{issue.code}: {issue.message} ({issue.count})" for issue in report.issues
        )
        super().__init__(f"quality gate failed for {report.market}/{report.dataset}: {details}")
        self.report = report


def require_clean(report: QualityReport) -> QualityReport:
    if report.issues:
        raise QualityGateFailed(report)
    return report
