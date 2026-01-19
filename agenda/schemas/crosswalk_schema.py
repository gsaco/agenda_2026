from __future__ import annotations

from typing import Any

import pandas as pd

REQUIRED_COLUMNS = ["ubigeo_origen", "anio_origen", "ubigeo_base", "peso"]


def validate(df: pd.DataFrame) -> dict[str, Any]:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    return {"valid": len(missing) == 0, "missing_columns": missing}
