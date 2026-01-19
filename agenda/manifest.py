from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

from .utils.io import read_json, write_json
from .utils.paths import resolve_path


def load_manifest(path: str | Path) -> list[dict[str, Any]]:
    p = resolve_path(path)
    if not p.exists():
        return []
    return read_json(p)


def save_manifest(path: str | Path, entries: list[dict[str, Any]]) -> None:
    write_json(path, entries)


def register_artifact(
    manifest_path: str | Path,
    artifact_path: str | Path,
    source: str,
    version: str | None,
    checksum: str,
    notes: str | None = None,
    inputs_checksum: str | None = None,
) -> None:
    entries = load_manifest(manifest_path)
    entry = {
        "artifact": str(resolve_path(artifact_path)),
        "source": source,
        "version": version,
        "checksum": checksum,
        "inputs_checksum": inputs_checksum,
        "timestamp_utc": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "notes": notes,
    }
    entries.append(entry)
    save_manifest(manifest_path, entries)
