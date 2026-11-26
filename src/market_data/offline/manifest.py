"""Deterministic manifests for offline data snapshots."""

from __future__ import annotations

import hashlib
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import NamedTemporaryFile


@dataclass(frozen=True)
class ManifestFile:
    path: str
    size: int
    sha256: str


@dataclass(frozen=True)
class DatasetManifest:
    schema_version: int
    market: str
    dataset: str
    snapshot_id: str
    created_at: str
    file_count: int
    byte_count: int
    max_date: str | None
    files: tuple[ManifestFile, ...]

    def to_dict(self) -> dict[str, object]:
        value = asdict(self)
        value["files"] = [asdict(item) for item in self.files]
        return value

    def write(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        payload = json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True)
        with NamedTemporaryFile("w", encoding="utf-8", dir=path.parent, delete=False) as temp:
            temp.write(payload + "\n")
            temp_path = Path(temp.name)
        os.replace(temp_path, path)


def sha256_file(path: Path, chunk_size: int = 1024 * 1024) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(chunk_size):
            digest.update(chunk)
    return digest.hexdigest()


def build_manifest(
    root: Path,
    market: str,
    dataset: str,
    snapshot_id: str,
    max_date: str | None = None,
) -> DatasetManifest:
    files = tuple(
        ManifestFile(str(path.relative_to(root)), path.stat().st_size, sha256_file(path))
        for path in sorted(root.rglob("*"))
        if path.is_file()
    )
    return DatasetManifest(
        1,
        market,
        dataset,
        snapshot_id,
        datetime.now(UTC).isoformat(),
        len(files),
        sum(item.size for item in files),
        max_date,
        files,
    )


def load_manifest(path: Path) -> DatasetManifest:
    raw = json.loads(path.read_text(encoding="utf-8"))
    if raw.get("schema_version") != 1:
        raise ValueError("unsupported manifest schema version")
    files = tuple(ManifestFile(**item) for item in raw.get("files", []))
    result = DatasetManifest(
        1,
        str(raw["market"]),
        str(raw["dataset"]),
        str(raw["snapshot_id"]),
        str(raw["created_at"]),
        int(raw["file_count"]),
        int(raw["byte_count"]),
        raw.get("max_date"),
        files,
    )
    if result.file_count != len(files) or result.byte_count != sum(item.size for item in files):
        raise ValueError("manifest counts do not match files")
    return result


def verify_manifest(root: Path, manifest: DatasetManifest) -> list[str]:
    expected = {item.path: item for item in manifest.files}
    actual = {str(path.relative_to(root)): path for path in root.rglob("*") if path.is_file()}
    errors: list[str] = []
    for relative, item in expected.items():
        path = actual.get(relative)
        if path is None:
            errors.append(f"missing file: {relative}")
        elif path.stat().st_size != item.size or sha256_file(path) != item.sha256:
            errors.append(f"checksum mismatch: {relative}")
    errors.extend(f"unexpected file: {path}" for path in sorted(set(actual) - set(expected)))
    return errors
