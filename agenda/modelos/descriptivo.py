from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_csv
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def _top_share(df: pd.DataFrame, share: float) -> float:
    if df.empty:
        return np.nan
    n = max(int(len(df) * share), 1)
    return df.nlargest(n, "pib")["pib"].sum() / df["pib"].sum()


def build_concentracion(cfg: Config, panel_path: Path) -> Path:
    df = read_parquet(panel_path)
    out = []
    for year, g in df.groupby("anio"):
        out.append(
            {
                "anio": int(year),
                "top_1_pct": _top_share(g, 0.01),
                "top_5_pct": _top_share(g, 0.05),
                "top_10_pct": _top_share(g, 0.10),
            }
        )
    out_df = pd.DataFrame(out)

    tables_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "tables")
    dest = tables_dir / "concentracion_topshares.csv"
    write_csv(dest, out_df)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_concentracion",
        version=None,
        checksum=hash_file(dest),
        notes="top shares",
    )
    logger.info("concentracion table written to %s", dest)
    return dest


def build_contribucion_crecimiento(cfg: Config, panel_path: Path) -> Path:
    df = read_parquet(panel_path)
    df = df.sort_values(["ubigeo", "anio"])
    df["delta_pib"] = df.groupby("ubigeo")["pib"].diff()
    df["share_growth"] = df.groupby("anio")["delta_pib"].transform(lambda s: s / s.sum())
    out = df[["ubigeo", "anio", "delta_pib", "share_growth"]].copy()

    tables_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "tables")
    dest = tables_dir / "contribucion_crecimiento.csv"
    write_csv(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_contribucion_crecimiento",
        version=None,
        checksum=hash_file(dest),
        notes="territorial growth contributions",
    )
    logger.info("contribucion table written to %s", dest)
    return dest
