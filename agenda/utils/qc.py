from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from .io import write_json
from .paths import resolve_path


def missingness_report(df: pd.DataFrame) -> dict[str, Any]:
    total = len(df)
    report: dict[str, Any] = {"rows": total, "columns": {}}
    for col in df.columns:
        missing = int(df[col].isna().sum())
        report["columns"][col] = {
            "missing": missing,
            "missing_pct": float(missing / total) if total else 0.0,
        }
    return report


def uniqueness_report(df: pd.DataFrame, keys: list[str]) -> dict[str, Any]:
    total = len(df)
    dups = int(df.duplicated(keys).sum())
    return {
        "rows": total,
        "keys": keys,
        "duplicates": dups,
        "duplicates_pct": float(dups / total) if total else 0.0,
    }


def write_qc_json(path: str | Path, payload: dict[str, Any]) -> None:
    write_json(resolve_path(path), payload)
