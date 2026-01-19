from __future__ import annotations

import hashlib
from pathlib import Path


def hash_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def hash_file(path: str | Path) -> str:
    p = Path(path)
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def hash_paths(paths: list[str | Path]) -> str:
    h = hashlib.sha256()
    for path in sorted([str(Path(p)) for p in paths]):
        h.update(path.encode("utf-8"))
        p = Path(path)
        if p.exists() and p.is_file():
            h.update(hash_file(p).encode("utf-8"))
    return h.hexdigest()
