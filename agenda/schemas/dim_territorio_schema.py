from __future__ import annotations

from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["ubigeo", "area_km2", "centroid_lat", "centroid_lon"]


def validate(df: pd.DataFrame) -> dict[str, Any]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return {"valid": len(missing) == 0, "missing_columns": missing}
