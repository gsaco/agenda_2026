from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_clima(cfg: Config, raw_path: Path) -> Path:
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path, sep="\\s+", skiprows=1, names=["SEAS", "YR", "TOTAL", "ANOM"])

    if {"SEAS", "YR", "ANOM"}.issubset(df.columns):
        df = df[pd.to_numeric(df["YR"], errors="coerce").notna()].copy()
        df["anio"] = df["YR"].astype(int)
        annual = df.groupby("anio")["ANOM"].mean().reset_index()
        annual = annual.rename(columns={"ANOM": "precip_anom"})
        annual = annual[annual["anio"].between(cfg.project.years.start, cfg.project.years.end)]

        dim_path = Path(cfg.paths.geo_dir) / "dim_territorio_base.parquet"
        dim = pd.read_parquet(dim_path)[["ubigeo"]].copy()
        dim["key"] = 1
        annual["key"] = 1
        df = annual.merge(dim, on="key").drop(columns=["key"])
    else:
        df = df.rename(columns={
            "UBIGEO": "ubigeo",
            "ubigeo": "ubigeo",
            "anio": "anio",
            "year": "anio",
            "precip_anom": "precip_anom",
        })
        if not {"ubigeo", "anio", "precip_anom"}.issubset(df.columns):
            raise ValueError("clima data must include ubigeo, anio, precip_anom")

    df = df.copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["precip_anom"] = pd.to_numeric(df["precip_anom"], errors="coerce")

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "shock_clima.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_clima.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_clima",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="clima shocks",
    )
    logger.info("clima shocks built at %s", dest)
    return dest
