"""The command line: one command, and an exit code an operator can act on.

Every code below is triggered here, because the table is the interface. The one
that matters most is the difference between 3 and 5: a source that is not
answering and a symbol whose data is wrong both end a run early, and only one of
them is worth retrying — reporting the first as the second is how a nightly job
"runs successfully" for weeks without loading anything.
"""

# pyright: reportMissingParameterType=false, reportUnknownArgumentType=false, reportUnknownMemberType=false, reportUnknownParameterType=false, reportUnknownVariableType=false
# Test-only, as in every other test module here.

from __future__ import annotations

import urllib.error
import urllib.parse

import pytest

from fixtures.free_stockdb import adjustments, daily_series, market_day, pack
from market_data import cli
from market_data.markets.cn.free_stockdb import (
    TABLE_ADJUST,
    TABLE_CODES,
    TABLE_DAILY,
    TABLE_DELISTED,
)
from market_data.offline.duckdb_store import open_store

SYMBOLS = ("600001", "600002")
DELISTED = ("600999",)
NAMES = {symbol: f"股票{symbol}" for symbol in (*SYMBOLS, *DELISTED)}


class Response:
    def __init__(self, payload: bytes) -> None:
        self._payload = payload

    def read(self) -> bytes:
        return self._payload

    def __enter__(self) -> Response:
        return self

    def __exit__(self, *exc: object) -> None:
        return None


class Service:
    """A ``urlopen`` stand-in that routes by the query it is asked."""

    def __init__(self, routes=None, *, fail: Exception | None = None) -> None:
        self.routes = routes or {}
        self.fail = fail
        self.asked: list[tuple[str | None, str | None, str | None]] = []

    def __call__(self, url: str, timeout: float) -> Response:
        params = dict(urllib.parse.parse_qsl(urllib.parse.urlparse(url).query))
        key = (params.get("t"), params.get("k1"), params.get("k2"))
        self.asked.append(key)
        if self.fail is not None:
            raise self.fail
        if key not in self.routes:
            raise AssertionError(f"unrouted request: {key}")
        return Response(self.routes[key])


def routes(*, history_for=SYMBOLS) -> dict[tuple[str, str | None, str | None], bytes]:
    table: dict[tuple[str, str | None, str | None], bytes] = {
        (TABLE_CODES, None, None): pack({"6": list(SYMBOLS)}),
        (TABLE_DELISTED, "all:", "all:"): pack(list(DELISTED)),
        (TABLE_ADJUST, "all:", "all:"): adjustments(("600001", 20200101, 1.0)),
    }
    for symbol in (*history_for, *DELISTED):
        table[(TABLE_DAILY, f"key:{symbol}", "all:")] = daily_series(symbol, NAMES[symbol])
    return table


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch):
    def install(service: Service) -> Service:
        monkeypatch.setattr("urllib.request.urlopen", service)
        return service

    return install


@pytest.fixture(autouse=True)
def no_ambient_alert_config(monkeypatch: pytest.MonkeyPatch):
    """No test may accidentally depend on the developer's own webhook."""
    for name in ("LARK_WEBHOOK", "LARK_SECRET", "LARK_TIMEOUT_SECS", "LARK_MAX_RETRIES"):
        monkeypatch.delenv(name, raising=False)


def full_args(tmp_path, *extra: str) -> list[str]:
    return ["update", "--market", "cn", "--full", "--db", str(tmp_path / "a.duckdb"), *extra]


# --- the older subcommands keep working -------------------------------------


def test_manifest_still_writes_a_manifest(tmp_path) -> None:
    root = tmp_path / "snapshot"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    output = tmp_path / "m.json"
    code = cli.main(
        [
            "manifest",
            str(root),
            "--market",
            "TW",
            "--dataset",
            "candles",
            "--snapshot-id",
            "v1",
            "--output",
            str(output),
        ]
    )
    assert code == 0
    assert output.is_file()


def test_verify_manifest_fails_loudly_on_a_missing_file(tmp_path) -> None:
    root = tmp_path / "snapshot"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    output = tmp_path / "m.json"
    cli.main(
        [
            "manifest",
            str(root),
            "--market",
            "TW",
            "--dataset",
            "candles",
            "--snapshot-id",
            "v1",
            "--output",
            str(output),
        ]
    )
    (root / "a.txt").write_text("changed", encoding="utf-8")
    with pytest.raises(SystemExit, match="checksum mismatch"):
        cli.main(["verify-manifest", str(root), str(output)])


# --- usage ------------------------------------------------------------------


def test_full_and_a_range_cannot_both_be_asked_for(tmp_path, capsys) -> None:
    with pytest.raises(SystemExit) as caught:
        cli.main(full_args(tmp_path, "--start", "2026-06-01", "--end", "2026-06-02"))
    assert caught.value.code == 2


def test_a_start_without_an_end_is_a_usage_error(tmp_path, served) -> None:
    served(Service(routes()))
    code = cli.main(
        ["update", "--market", "cn", "--start", "2026-06-01", "--db", str(tmp_path / "a.duckdb")]
    )
    assert code == 2


def test_an_unknown_market_is_refused_by_the_parser(tmp_path) -> None:
    with pytest.raises(SystemExit) as caught:
        cli.main(["update", "--market", "tw", "--full", "--db", str(tmp_path / "a.duckdb")])
    assert caught.value.code == 2


def test_an_alert_timeout_that_is_not_a_whole_number_stops_the_run(
    tmp_path, served, monkeypatch: pytest.MonkeyPatch, capsys
) -> None:
    """A typo here would otherwise make every alert vanish inside a broad except."""
    monkeypatch.setenv("LARK_TIMEOUT_SECS", "5.5")
    served(Service(routes()))
    code = cli.main(full_args(tmp_path))
    assert code == 2
    assert "LARK_TIMEOUT_SECS" in capsys.readouterr().err


def test_refresh_without_full_is_a_usage_error(tmp_path, served) -> None:
    """A window rewrites its own days already, so --refresh would do nothing.

    Accepting it silently is the "flag that does nothing" this file's exit codes
    exist to avoid.
    """
    served(Service(routes()))
    code = cli.main(
        [
            "update",
            "--market",
            "cn",
            "--start",
            "2026-06-01",
            "--end",
            "2026-06-02",
            "--refresh",
            "--db",
            str(tmp_path / "a.duckdb"),
        ]
    )
    assert code == 2


@pytest.mark.parametrize("value", ["-1", "many", "1.5"])
def test_a_keep_load_log_that_is_not_a_count_is_a_usage_error(tmp_path, value: str) -> None:
    with pytest.raises(SystemExit) as caught:
        cli.main(full_args(tmp_path, "--keep-load-log", value))
    assert caught.value.code == 2


def _backdate_provenance(connection, day: str = "2000-01-01") -> None:
    """Age every provenance row, keeping the primary key distinct.

    ``loaded_at`` is part of the key, so they cannot all be set to one instant;
    DuckDB will not evaluate a window function inside ``UPDATE`` either, so each
    row moves on its own.
    """
    rows = connection.execute("select market, dataset, loaded_at from load_log").fetchall()
    for offset, (market, dataset, loaded_at) in enumerate(rows):
        connection.execute(
            "update load_log set loaded_at = ? where market = ? and dataset = ? and loaded_at = ?",
            [f"{day}T00:00:{offset:02d}+00:00", market, dataset, loaded_at],
        )


def test_provenance_older_than_the_retention_is_trimmed(tmp_path, served) -> None:
    """One row per symbol, so an untrimmed log grows by about a megabyte a day."""
    served(Service(routes()))
    db = tmp_path / "a.duckdb"
    assert cli.main(full_args(tmp_path, "--keep-load-log", "0")) == 0
    with open_store(db) as connection:
        assert connection.execute("select count(*) from load_log").fetchone() == (
            len(SYMBOLS) + len(DELISTED),
        )
        _backdate_provenance(connection)

    assert cli.main(full_args(tmp_path, "--refresh")) == 0
    with open_store(db) as connection:
        assert connection.execute(
            "select count(*) from load_log where loaded_at < '2020-01-01'"
        ).fetchone() == (0,), "the backdated rows should be gone"
        assert connection.execute("select count(*) from load_log").fetchone() == (
            len(SYMBOLS) + len(DELISTED),
        ), "and this run's own rows should have replaced them"


def test_a_failed_run_does_not_trim_provenance(tmp_path, served) -> None:
    """Trimming after the run, not before, so a failure cannot delete the record of itself."""
    served(Service(routes()))
    db = tmp_path / "a.duckdb"
    assert cli.main(full_args(tmp_path, "--keep-load-log", "0")) == 0
    with open_store(db) as connection:
        _backdate_provenance(connection)

    served(Service(fail=urllib.error.URLError("connection refused")))
    assert cli.main(full_args(tmp_path)) == 3
    with open_store(db) as connection:
        assert connection.execute("select count(*) from load_log").fetchone() == (
            len(SYMBOLS) + len(DELISTED),
        ), "an unreachable source must leave the log alone"


# --- the runs ---------------------------------------------------------------


def test_a_full_run_loads_the_store_and_exits_zero(tmp_path, served) -> None:
    served(Service(routes()))
    assert cli.main(full_args(tmp_path)) == 0
    with open_store(tmp_path / "a.duckdb") as connection:
        assert connection.execute("select count(distinct symbol) from candles").fetchone() == (
            len(SYMBOLS) + len(DELISTED),
        )


def test_a_window_run_loads_the_requested_day(tmp_path, served) -> None:
    served(Service(routes()))
    assert cli.main(full_args(tmp_path)) == 0
    served(
        Service(
            {
                (TABLE_DAILY, "all:", "key:20260604"): market_day(20260604, NAMES),
                (TABLE_ADJUST, "all:", "all:"): adjustments(("600001", 20200101, 1.0)),
            }
        )
    )
    code = cli.main(
        [
            "update",
            "--market",
            "cn",
            "--start",
            "2026-06-04",
            "--end",
            "2026-06-04",
            "--db",
            str(tmp_path / "a.duckdb"),
        ]
    )
    assert code == 0
    with open_store(tmp_path / "a.duckdb") as connection:
        assert connection.execute("select max(date) from candles").fetchone() == (
            __import__("datetime").date(2026, 6, 4),
        )


def test_a_range_with_no_trading_days_says_so(tmp_path, served, caplog) -> None:
    """Exit 0 is right — nothing failed — but silence would read as a good run."""
    served(Service(routes()))
    assert cli.main(full_args(tmp_path)) == 0
    served(
        Service(
            {
                (TABLE_DAILY, "all:", "key:20260606"): pack([]),
                (TABLE_ADJUST, "all:", "all:"): adjustments(("600001", 20200101, 1.0)),
            }
        )
    )
    with caplog.at_level("WARNING"):
        code = cli.main(
            [
                "update",
                "--market",
                "cn",
                "--start",
                "2026-06-06",
                "--end",
                "2026-06-06",
                "--db",
                str(tmp_path / "a.duckdb"),
            ]
        )
    assert code == 0
    assert "no trading days" in caplog.text


# --- the exit-code table ----------------------------------------------------


def test_an_unreachable_source_is_three_and_says_so(tmp_path, served, caplog) -> None:
    """Not 0, and not "nothing new" — the failure that hides for weeks."""
    served(Service(fail=urllib.error.URLError("connection refused")))
    with caplog.at_level("ERROR"):
        code = cli.main(full_args(tmp_path))
    assert code == 3
    assert "not answering" in caplog.text


def test_a_gate_rejection_is_four_and_does_not_stop_the_other_symbols(tmp_path, served) -> None:
    """One symbol is one transaction; the rest of the run is unaffected."""
    broken = routes()
    broken[(TABLE_DAILY, "key:600002", "all:")] = _bars_with_a_broken_ohlc("600002")
    served(Service(broken))
    assert cli.main(full_args(tmp_path)) == 4
    with open_store(tmp_path / "a.duckdb") as connection:
        assert connection.execute(
            "select count(*) from candles where symbol = '600001'"
        ).fetchone() == (3,)
        assert connection.execute(
            "select count(*) from candles where symbol = '600002'"
        ).fetchone() == (0,)


def test_a_symbol_that_fails_for_another_reason_is_five(tmp_path, served) -> None:
    broken = routes()
    broken[(TABLE_DAILY, "key:600002", "all:")] = b"not msgpack"
    served(Service(broken))
    assert cli.main(full_args(tmp_path)) == 5


def test_a_clean_run_is_zero(tmp_path, served) -> None:
    served(Service(routes()))
    assert cli.main(full_args(tmp_path)) == 0


# --- alerts never change the answer -----------------------------------------


@pytest.mark.parametrize(
    "webhook",
    [None, "https://example.invalid/hook"],
    ids=["unconfigured", "unreachable"],
)
def test_a_broken_alert_channel_does_not_change_the_exit_code(
    tmp_path, served, monkeypatch: pytest.MonkeyPatch, webhook
) -> None:
    """The exit code describes the data. The notifier's health is not the data."""
    if webhook is None:
        monkeypatch.delenv("LARK_WEBHOOK", raising=False)
    else:
        monkeypatch.setenv("LARK_WEBHOOK", webhook)
    broken = routes()
    broken[(TABLE_DAILY, "key:600002", "all:")] = b"not msgpack"
    served(Service(broken))
    assert cli.main(full_args(tmp_path)) == 5

    served(Service(routes()))
    assert cli.main([*full_args(tmp_path), "--refresh"]) == 0


def test_a_failed_run_does_not_hide_the_original_error(tmp_path, served, caplog) -> None:
    served(Service(fail=urllib.error.URLError("connection refused")))
    with caplog.at_level("ERROR"):
        cli.main(full_args(tmp_path))
    assert "connection refused" in caplog.text


def test_a_full_run_rewrites_in_place_rather_than_rebuilding(tmp_path, served) -> None:
    """Option (a): no clear, no temporary file, no second copy on disk.

    A second run over a smaller universe must leave the symbols it does not
    mention exactly as they were — which is what tells a rebuild-in-place apart
    from a rebuild-from-scratch.
    """
    served(Service(routes()))
    assert cli.main(full_args(tmp_path)) == 0

    smaller = routes()
    smaller[(TABLE_CODES, None, None)] = pack({"6": ["600001"]})
    served(Service(smaller))
    assert cli.main([*full_args(tmp_path), "--refresh"]) == 0

    with open_store(tmp_path / "a.duckdb") as connection:
        assert connection.execute(
            "select count(*) from candles where symbol = '600002'"
        ).fetchone() == (3,)
        assert connection.execute(
            "select count(*) from candles c left join adj_factor a using (symbol, date)"
            " where a.symbol is null"
        ).fetchone() == (0,)


def _bars_with_a_broken_ohlc(symbol: str) -> bytes:
    """Real bars, one of them made internally impossible *after* the adapter's checks.

    ``open`` rather than ``low``: moving ``low`` would push ``amount / volume``
    out of the day's range, which the adapter refuses as a malformed *source*
    reply. This has to get past the adapter and be refused by the gate, or the
    exit code under test would be the wrong one.
    """
    from fixtures.free_stockdb import daily_row

    rows = [
        daily_row(
            20260601 + offset,
            symbol,
            NAMES[symbol],
            price=10.0,
            volume=1_000,
            float_share=1_000_000,
        )
        for offset in range(3)
    ]
    rows[1]["open"] = rows[1]["high"] * 2
    return pack(rows)
