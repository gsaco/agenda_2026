from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..schemas.indicadores_core_schema import validate as validate_schema
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


def _zscore(series: pd.Series) -> pd.Series:
    return (series - series.mean()) / series.std(ddof=0)


def build_indicadores_core(
    cfg: Config,
    pib_path: Path,
    poblacion_path: Path,
    dim_territorio_path: Path,
) -> Path:
    pib = read_parquet(pib_path)
    pob = read_parquet(poblacion_path)
    dim = read_parquet(dim_territorio_path)

    df = pib.merge(pob, on=["ubigeo", "anio"], how="left")
    df = df.merge(dim[["ubigeo", "area_km2"]], on="ubigeo", how="left")

    df["pib_pc"] = df["pib"] / df["pob"].replace({0: np.nan})
    df["pib_km2"] = df["pib"] / df["area_km2"].replace({0: np.nan})

    iae_def = cfg.project.iae_def.upper()
    if iae_def == "A1":
        df["iae"] = df["pib_km2"] / df["pib_pc"]
    elif iae_def == "A2":
        df["iae"] = df["pib_pc"] / df["pib_km2"]
    else:
        df["iae"] = np.exp(0.5 * (_zscore(df["pib_pc"]) + _zscore(df["pib_km2"])))

    df = df.sort_values(["ubigeo", "anio"])
    df["dlog_pib"] = np.log(df["pib"]) - np.log(df.groupby("ubigeo")["pib"].shift(1))

    window = 5
    df["cagr_5y"] = (df["pib"] / df.groupby("ubigeo")["pib"].shift(window)) ** (1 / window) - 1

    df["share_pib_nac"] = df.groupby("anio")["pib"].transform(lambda s: s / s.sum())

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "indicadores_core.parquet"
    write_parquet(dest, df)

    schema_result = validate_schema(df)
    qc = {
        "schema": schema_result,
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_indicadores_core.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_indicadores_core",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([pib_path, poblacion_path, dim_territorio_path]),
        notes=f"IAE definition {iae_def}",
    )
    logger.info("indicadores_core built at %s", dest)
    return dest
