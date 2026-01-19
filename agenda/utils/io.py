from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pandas as pd

try:
    from ruamel.yaml import YAML
except Exception:  # pragma: no cover - fallback
    YAML = None

try:
    import yaml as pyyaml
except Exception:  # pragma: no cover - fallback
    pyyaml = None

from .paths import resolve_path


def read_yaml(path: str | Path) -> dict[str, Any]:
    p = resolve_path(path)
    if YAML is not None:
        yaml = YAML(typ="safe")
        with p.open("r", encoding="utf-8") as f:
            return yaml.load(f) or {}
    if pyyaml is None:
        raise RuntimeError("No YAML parser available")
    with p.open("r", encoding="utf-8") as f:
        return pyyaml.safe_load(f) or {}


def write_yaml(path: str | Path, data: dict[str, Any]) -> None:
    p = resolve_path(path)
    if YAML is not None:
        yaml = YAML()
        with p.open("w", encoding="utf-8") as f:
            yaml.dump(data, f)
        return
    if pyyaml is None:
        raise RuntimeError("No YAML parser available")
    with p.open("w", encoding="utf-8") as f:
        pyyaml.safe_dump(data, f, sort_keys=False)


def read_json(path: str | Path) -> Any:
    p = resolve_path(path)
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)


def write_json(path: str | Path, data: Any) -> None:
    p = resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    with p.open("w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, sort_keys=True)


def read_parquet(path: str | Path) -> pd.DataFrame:
    return pd.read_parquet(resolve_path(path))


def write_parquet(path: str | Path, df: pd.DataFrame) -> None:
    p = resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(p, index=False)


def read_csv(path: str | Path) -> pd.DataFrame:
    return pd.read_csv(resolve_path(path))


def write_csv(path: str | Path, df: pd.DataFrame) -> None:
    p = resolve_path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(p, index=False)
