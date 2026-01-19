from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def _percentile_rank(series: pd.Series) -> pd.Series:
    return series.rank(pct=True)


def build_indice_vulnerabilidad(cfg: Config, panel_path: Path) -> Path:
    df = read_parquet(panel_path)

    df = df.copy()
    df["nivel_pc"] = 1 - _percentile_rank(df["pib_pc"])
    df["nivel_km2"] = 1 - _percentile_rank(df["pib_km2"])
    df["comp_nivel"] = df[["nivel_pc", "nivel_km2"]].mean(axis=1)

    if "dlog_tot" in df.columns:
        df["shock_macro_abs"] = df["dlog_tot"].abs()
    else:
        df["shock_macro_abs"] = 0.0

    if "precip_anom" in df.columns:
        df["shock_clima_abs"] = df["precip_anom"].abs()
    else:
        df["shock_clima_abs"] = 0.0

    if "mineria_expo" in df.columns:
        df["expo_mineria"] = df["mineria_expo"]
    else:
        df["expo_mineria"] = 0.0

    df["shock_macro_abs"] = df["shock_macro_abs"].fillna(0.0)
    df["shock_clima_abs"] = df["shock_clima_abs"].fillna(0.0)
    df["expo_mineria"] = df["expo_mineria"].fillna(0.0)

    df["comp_shock"] = (
        0.5 * df["shock_clima_abs"]
        + 0.5 * df["shock_macro_abs"] * (1 + df["expo_mineria"])
    )

    df["vulnerabilidad"] = 0.5 * df["comp_nivel"] + 0.5 * df["comp_shock"]

    df["rank_vulnerabilidad"] = df.groupby("anio")["vulnerabilidad"].rank(ascending=False, method="first")
    df["quintil_vulnerabilidad"] = df.groupby("anio")["vulnerabilidad"].transform(
        lambda s: pd.qcut(s, 5, labels=False, duplicates="drop")
    )

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "indice_vulnerabilidad.parquet"
    write_parquet(dest, df[[
        "ubigeo",
        "anio",
        "vulnerabilidad",
        "rank_vulnerabilidad",
        "quintil_vulnerabilidad",
        "comp_nivel",
        "comp_shock",
    ]])

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="politicas_vulnerabilidad",
        version=None,
        checksum=hash_file(dest),
        notes="vulnerability index",
    )
    logger.info("indice_vulnerabilidad saved to %s", dest)
    return dest
