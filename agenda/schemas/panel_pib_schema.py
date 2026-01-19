from __future__ import annotations

from typing import Any

import pandas as pd

try:
    import pandera.pandas as pa
except Exception:  # pragma: no cover
    pa = None


REQUIRED_COLUMNS = ["ubigeo", "anio", "pib"]


def validate(df: pd.DataFrame) -> dict[str, Any]:
    if pa is None:
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        return {"valid": len(missing) == 0, "missing_columns": missing}

    schema = pa.DataFrameSchema(
        {
            "ubigeo": pa.Column(str, nullable=False),
            "anio": pa.Column(int, nullable=False),
            "pib": pa.Column(float, nullable=False),
        },
        strict=False,
    )
    schema.validate(df, lazy=True)
    return {"valid": True, "missing_columns": []}
