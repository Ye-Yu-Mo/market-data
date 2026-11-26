"""Atomic snapshot publication for offline datasets."""

from __future__ import annotations

import os
from pathlib import Path


def publish_snapshot(snapshots_root: Path, snapshot_id: str, current_name: str = "current") -> Path:
    """Atomically switch current to a completed snapshot directory."""
    snapshots_root = snapshots_root.resolve()
    snapshot = (snapshots_root / snapshot_id).resolve()
    if snapshots_root not in snapshot.parents or not snapshot.is_dir():
        raise ValueError(f"snapshot is not a child directory: {snapshot}")
    current = snapshots_root / current_name
    link = snapshots_root / f".{current_name}.{snapshot_id}.new"
    if link.exists() or link.is_symlink():
        link.unlink()
    link.symlink_to(snapshot.name, target_is_directory=True)
    os.replace(link, current)
    return current
