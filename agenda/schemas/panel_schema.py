from __future__ import annotations

from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["ubigeo", "anio", "pib", "pob", "area_km2"]


def validate(df: pd.DataFrame) -> dict[str, Any]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return {"valid": len(missing) == 0, "missing_columns": missing}
