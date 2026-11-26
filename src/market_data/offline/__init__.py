"""Shared offline-data orchestration primitives."""

from market_data.offline.duckdb_store import open_store
from market_data.offline.manifest import DatasetManifest, build_manifest, verify_manifest
from market_data.offline.publish import publish_snapshot
from market_data.offline.quality import (
    QualityGateFailed,
    QualityReport,
    check_candles,
    require_clean,
)

__all__ = [
    "DatasetManifest",
    "QualityGateFailed",
    "QualityReport",
    "build_manifest",
    "check_candles",
    "open_store",
    "publish_snapshot",
    "require_clean",
    "verify_manifest",
]
