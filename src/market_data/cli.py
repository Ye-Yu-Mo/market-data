"""Command-line entry point for the market-data pipeline.

Two facts shape this file more than the rest.

**Where it runs decides whether it finishes.** The whole-market load is about
four minutes against a local service and about twenty-three hours through the
SSH tunnel — the same request, measured at 0.042 s and 14.4 s — because the
service scans one query at a time and concurrency does not help it (2/4/8 in
flight measured 0.99x, 0.92x, 0.99x). That is why the default base URL is
loopback and why running this on a laptop is a deployment mistake, not a slow
one. It is repeated in ``--help`` because that is where it will be read.

**The exit code is the interface.** "The source is down, try again later" and
"this data is wrong, retrying will not help" need opposite responses, and a run
that loaded 5,836 symbols and lost one is neither. A single non-zero code cannot
carry that, so each outcome has its own.
"""

from __future__ import annotations

import argparse
import datetime as dt
import logging
import os
import sys
from collections.abc import Sequence
from enum import IntEnum
from pathlib import Path

import duckdb

from market_data.markets.base import SourceUnavailable
from market_data.markets.cn import DEFAULT_BASE_URL, FreeStockDbAdapter
from market_data.markets.contracts import A_SHARE_DB_PATH
from market_data.offline.alerts import AlertEvent, AlertSink
from market_data.offline.duckdb_store import open_store
from market_data.offline.manifest import build_manifest, load_manifest, verify_manifest
from market_data.offline.pipeline import (
    MARKETS,
    RunOutcome,
    run_full,
    run_window,
    trim_load_log,
)
from market_data.offline.quality import QualityGateFailed

logger = logging.getLogger("market_data")


class Exit(IntEnum):
    """What the shell is told, and what an operator does about it."""

    OK = 0
    UNEXPECTED = 1
    USAGE = 2
    SOURCE_UNAVAILABLE = 3
    GATE_REJECTED = 4
    PARTIAL = 5


class UsageError(Exception):
    """The invocation is wrong — bad flags, or an environment that would fail silently."""


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.command == "manifest":
        build_manifest(
            args.root, market=args.market, dataset=args.dataset, snapshot_id=args.snapshot_id
        ).write(args.output)
        return int(Exit.OK)
    if args.command == "verify-manifest":
        errors = verify_manifest(args.root, load_manifest(args.manifest))
        if errors:
            raise SystemExit("\n".join(errors))
        return int(Exit.OK)
    try:
        return int(_update(args))
    except UsageError as exc:
        print(f"market_data: {exc}", file=sys.stderr)
        return int(Exit.USAGE)
    except Exception:
        logger.exception("the run ended unexpectedly")
        _alert(args.market, "unexpected", "the run ended unexpectedly; see the log")
        return int(Exit.UNEXPECTED)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="market_data",
        description=(
            "Load A-share history into a DuckDB store shaped like market_v2. "
            "Run this where the source is: a full load takes minutes against a local "
            "service and most of a day through the tunnel."
        ),
    )
    parser.add_argument("--verbose", action="store_true", help="log every symbol as it is handled")
    subparsers = parser.add_subparsers(dest="command", required=True)

    update = subparsers.add_parser(
        "update",
        help="load or refresh the store",
        description=(
            "Symbols already present are skipped, so re-running after an interruption "
            "continues where it stopped — and so a source revision to old rows is NOT "
            "picked up. Pass --refresh to re-fetch them. A window is cheap only against "
            "a populated store: every symbol in it that the store has never seen is "
            "fetched in full, so a range run against an empty store costs a full load."
        ),
    )
    mode = update.add_mutually_exclusive_group(required=True)
    mode.add_argument(
        "--full",
        action="store_true",
        help=(
            "every symbol's whole history, one request per symbol. Rewrites in place: "
            "each symbol is its own transaction, so a consumer sees each symbol wholly "
            "old or wholly new, never half of one."
        ),
    )
    mode.add_argument(
        "--start",
        type=_iso_date,
        metavar="YYYY-MM-DD",
        help="first day of a range; a symbol not yet in the store is still fetched in full",
    )
    update.add_argument("--end", type=_iso_date, metavar="YYYY-MM-DD", help="last day of the range")
    update.add_argument("--market", default="CN", choices=list(MARKETS), help="market to load")
    update.add_argument(
        "--refresh",
        action="store_true",
        help="re-fetch symbols already in the store, instead of skipping them",
    )
    update.add_argument(
        "--keep-load-log",
        type=_non_negative_int,
        default=90,
        metavar="DAYS",
        help=(
            "after a successful run, drop provenance rows older than this many days; "
            "0 keeps every row (default: 90). One row is written per symbol, so an "
            "untrimmed log grows by about a megabyte a day"
        ),
    )
    update.add_argument(
        "--db",
        type=Path,
        default=A_SHARE_DB_PATH,
        help=f"store to write (default: {A_SHARE_DB_PATH})",
    )
    update.add_argument(
        "--base-url",
        default=DEFAULT_BASE_URL,
        help=f"where the source answers (default: {DEFAULT_BASE_URL})",
    )
    update.add_argument(
        "--source-timeout",
        type=_positive_float,
        default=30.0,
        metavar="SECONDS",
        help=(
            "how long one source request may take (default: 30). A whole-market reply "
            "normally lands in ~0.1s, but the first query after a full vendor sync "
            "reads cold LevelDB that was rewritten underneath the service and has "
            "been measured past 30s; the daily cron passes 120 for that reason"
        ),
    )

    # The two older subcommands keep their arguments and their meaning exactly.
    manifest = subparsers.add_parser("manifest")
    manifest.add_argument("root", type=Path)
    manifest.add_argument("--market", required=True)
    manifest.add_argument("--dataset", required=True)
    manifest.add_argument("--snapshot-id", required=True)
    manifest.add_argument("--output", type=Path, required=True)
    verify = subparsers.add_parser("verify-manifest")
    verify.add_argument("root", type=Path)
    verify.add_argument("manifest", type=Path)
    return parser


def _update(args: argparse.Namespace) -> Exit:
    _require_int_env()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    if args.full and args.refresh:
        logger.info("--refresh: re-fetching symbols that are already in the store")
    if args.refresh and not args.full:
        # A window already rewrites its own days, so --refresh adds nothing there.
        # Accepting it silently would be the "flag that does nothing" this project
        # keeps having to dig out of logs.
        raise UsageError("--refresh applies to --full only; a window always re-fetches its days")
    if not args.full and args.end is None:
        raise UsageError("--start needs --end (or use --full)")

    try:
        with open_store(args.db) as connection:
            adapter = FreeStockDbAdapter(args.base_url, timeout=args.source_timeout)
            if args.full:
                outcome = run_full(
                    connection,
                    adapter,
                    market=args.market,
                    refresh=args.refresh,
                    on_event=_progress if args.verbose else None,
                )
            else:
                outcome = run_window(
                    connection,
                    adapter,
                    market=args.market,
                    start=args.start,
                    end=args.end,
                    on_event=_progress if args.verbose else None,
                )
            _trim_load_log(connection, args.keep_load_log)
    except SourceUnavailable as exc:
        # Not the same as "there was nothing new": a wrong parameter and a dead
        # service both answer here, and only one of them is worth retrying.
        logger.error("the source is not answering: %s", exc)
        _alert(args.market, "source_unavailable", str(exc))
        return Exit.SOURCE_UNAVAILABLE
    except QualityGateFailed as exc:
        logger.error("the gate refused the batch: %s", exc)
        _alert(args.market, "quality_gate", str(exc))
        return Exit.GATE_REJECTED

    _summarise(args.market, outcome)
    if any(failure.gated for failure in outcome.failures):
        _alert(args.market, "quality_gate", _failure_detail(outcome))
        return Exit.GATE_REJECTED
    if outcome.failures:
        _alert(args.market, "partial", _failure_detail(outcome))
        return Exit.PARTIAL
    return Exit.OK


def _trim_load_log(connection: duckdb.DuckDBPyConnection, keep_days: int) -> None:
    """Drop provenance older than ``keep_days``; ``0`` keeps every row.

    One row is written per symbol, so the log grows with the work rather than
    with the calendar — about a megabyte per full run and per daily update.
    Left alone it reaches a quarter of a gigabyte a year, which is a decision
    nobody made; this is the decision, taken after a run rather than before one
    so a failed run's provenance is never the thing that got deleted.
    """
    if keep_days <= 0:
        return
    cutoff = dt.datetime.now(dt.UTC).date() - dt.timedelta(days=keep_days)
    removed = trim_load_log(connection, older_than=cutoff)
    if removed:
        logger.info("trimmed %d provenance row(s) older than %s", removed, cutoff)


def _summarise(market: str, outcome: RunOutcome) -> None:
    if outcome.trading_days:
        logger.info("%s: %d trading days loaded", market, len(outcome.trading_days))
    elif not outcome.loaded:
        # Saying nothing would be indistinguishable from a run that worked.
        logger.warning("%s: no trading days in the requested range — nothing was loaded", market)
    logger.info(
        "%s: %d loaded, %d already present, %d with no data at the source, %d failed",
        market,
        outcome.loaded,
        outcome.skipped,
        outcome.no_data,
        len(outcome.failures),
    )
    if outcome.no_data:
        logger.warning(
            "%s: %d symbols have no data at the source and will not be asked again "
            "(see the source_gap table)",
            market,
            outcome.no_data,
        )
    for failure in outcome.failures:
        logger.error("%s: %s", failure.symbol, failure.error)


def _failure_detail(outcome: RunOutcome) -> str:
    return "; ".join(f"{failure.symbol}: {failure.error}" for failure in outcome.failures[:5])


def _progress(message: str) -> None:
    logger.info("%s", message)


def _alert(market: str, code: str, message: str) -> None:
    """Send an alert, and ignore the answer.

    ``AlertSink.send`` returns ``False`` for every way an alert can fail — no
    webhook configured, no network, ``lark_alert`` not installed — and swallows
    the exception behind it. Reading that value would let a broken *alert*
    channel rewrite the run's outcome, which is backwards: the exit code
    describes the data, and a run that loaded everything must not report failure
    because the notifier is misconfigured.
    """
    AlertSink(service="market_data").send(
        AlertEvent(market=market, dataset="update", code=code, message=message)
    )


def _require_int_env() -> None:
    """Check the alert timeouts before anything can be silently downgraded.

    ``lark_alert``'s ``timeout_secs`` and ``max_retries`` take integers; a float
    or a quoted number raises ``TypeError`` *inside* ``AlertSink.send``'s broad
    ``except``, so a typo would make every alert of the run disappear without a
    trace. Failing at startup instead costs one error message and keeps the
    channel honest.
    """
    for name in ("LARK_TIMEOUT_SECS", "LARK_MAX_RETRIES"):
        raw = os.environ.get(name, "").strip()
        if not raw:
            continue
        try:
            int(raw)
        except ValueError as exc:
            raise UsageError(f"{name} must be a whole number, got {raw!r}") from exc


def _iso_date(value: str) -> dt.date:
    try:
        return dt.date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value!r} is not a YYYY-MM-DD date") from exc


def _non_negative_int(value: str) -> int:
    try:
        number = int(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value!r} is not a whole number") from exc
    if number < 0:
        raise argparse.ArgumentTypeError(f"{value!r} must not be negative")
    return number


def _positive_float(value: str) -> float:
    try:
        number = float(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"{value!r} is not a number") from exc
    if not number > 0:
        raise argparse.ArgumentTypeError(f"{value!r} must be greater than zero")
    return number
