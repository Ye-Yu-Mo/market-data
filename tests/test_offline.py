# pyright: reportUnknownArgumentType=false

from pathlib import Path

import pandas as pd
import pytest

from market_data.offline.manifest import build_manifest, load_manifest, verify_manifest
from market_data.offline.publish import publish_snapshot
from market_data.offline.quality import check_candles, require_clean


def candles() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "symbol": "2330",
                "date": "2026-01-02",
                "open": 100,
                "high": 110,
                "low": 99,
                "close": 105,
                "volume": 10,
            },
            {
                "symbol": "2330",
                "date": "2026-01-05",
                "open": 105,
                "high": 111,
                "low": 104,
                "close": 110,
                "volume": 12,
            },
        ]
    )


def test_quality_accepts_valid_candles() -> None:
    report = check_candles(candles(), "TW")
    assert report.ok
    assert report.rows == 2
    assert report.min_date == "2026-01-02"
    assert report.max_date == "2026-01-05"
    assert require_clean(report) is report


def test_quality_rejects_duplicate_and_invalid_ohlc() -> None:
    frame = pd.concat([candles(), candles().iloc[[0]]], ignore_index=True)
    frame.loc[0, "low"] = 200
    report = check_candles(frame, "TW")
    assert {issue.code for issue in report.issues} == {"duplicate_key", "invalid_ohlc"}
    with pytest.raises(ValueError, match="quality gate failed"):
        require_clean(report)


def test_manifest_roundtrip_and_checksum(tmp_path: Path) -> None:
    root = tmp_path / "snapshot"
    root.mkdir()
    (root / "a.txt").write_text("a", encoding="utf-8")
    (root / "nested").mkdir()
    (root / "nested" / "b.txt").write_text("b", encoding="utf-8")
    manifest = build_manifest(root, "TW", "candles", "2026-01-05", "2026-01-05")
    path = tmp_path / "manifest.json"
    manifest.write(path)
    loaded = load_manifest(path)
    assert verify_manifest(root, loaded) == []
    (root / "a.txt").write_text("changed", encoding="utf-8")
    assert verify_manifest(root, loaded) == ["checksum mismatch: a.txt"]


def test_publish_switches_current_atomically(tmp_path: Path) -> None:
    snapshots = tmp_path / "snapshots"
    snapshots.mkdir()
    first = snapshots / "v1"
    first.mkdir()
    (first / "data").write_text("one", encoding="utf-8")
    current = publish_snapshot(snapshots, "v1")
    assert current.is_symlink()
    assert current.resolve() == first.resolve()
    second = snapshots / "v2"
    second.mkdir()
    (second / "data").write_text("two", encoding="utf-8")
    publish_snapshot(snapshots, "v2")
    assert current.resolve() == second.resolve()
