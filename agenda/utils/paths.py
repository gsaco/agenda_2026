from __future__ import annotations

from pathlib import Path


def project_root() -> Path:
    # agenda/utils/paths.py -> agenda/utils -> agenda -> repo root
    return Path(__file__).resolve().parents[2]


def resolve_path(path_str: str | Path) -> Path:
    path = Path(path_str)
    if path.is_absolute():
        return path
    return project_root() / path


def ensure_dir(path: str | Path) -> Path:
    p = resolve_path(path)
    p.mkdir(parents=True, exist_ok=True)
    return p
