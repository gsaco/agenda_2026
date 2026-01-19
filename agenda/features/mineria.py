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
from ..utils.text import normalize_name

logger = logging.getLogger(__name__)


def build_mineria(cfg: Config, raw_path: Path) -> Path:
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path)

    if {"DEPARTAMENTO", "PROVINCIA", "DISTRITO"}.issubset(df.columns):
        dim_path = Path(cfg.paths.geo_dir) / "dim_ubigeo.parquet"
        dim = pd.read_parquet(dim_path)
        dim = dim.rename(columns={"dep_name": "DEPARTAMENTO", "prov_name": "PROVINCIA", "dist_name": "DISTRITO"})
        for col in ["DEPARTAMENTO", "PROVINCIA", "DISTRITO"]:
            dim[col] = dim[col].map(normalize_name)
            df[col] = df[col].map(normalize_name)

        df = df[df["DEPARTAMENTO"].notna() & df["PROVINCIA"].notna() & df["DISTRITO"].notna()]
        df = df[(df["DEPARTAMENTO"] != "") & (df["PROVINCIA"] != "") & (df["DISTRITO"] != "")]
        df = df[(df["DEPARTAMENTO"] != "NAN") & (df["PROVINCIA"] != "NAN") & (df["DISTRITO"] != "NAN")]

        df = df.merge(dim[["ubigeo", "DEPARTAMENTO", "PROVINCIA", "DISTRITO"]], on=["DEPARTAMENTO", "PROVINCIA", "DISTRITO"], how="left")
        if df["ubigeo"].isna().any():
            missing = df[df["ubigeo"].isna()][["DEPARTAMENTO", "PROVINCIA", "DISTRITO"]].drop_duplicates()
            raise ValueError(f"mineria: ubigeo mapping missing for {len(missing)} districts")

        if "AÑO_DAC" in df.columns:
            df = df.rename(columns={"AÑO_DAC": "anio"})
        df["anio"] = pd.to_numeric(df["anio"], errors="coerce").fillna(cfg.project.years.end).astype(int)
        df = df.groupby(["ubigeo", "anio"]).size().reset_index(name="mineria_expo")
    else:
        df = df.rename(columns={
            "UBIGEO": "ubigeo",
            "ubigeo": "ubigeo",
            "anio": "anio",
            "year": "anio",
            "mineria_expo": "mineria_expo",
        })
        if not {"ubigeo", "anio", "mineria_expo"}.issubset(df.columns):
            raise ValueError("mineria data must include ubigeo, anio, mineria_expo")
        df = df.copy()
        df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
        df["anio"] = df["anio"].astype(int)

    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["mineria_expo"] = pd.to_numeric(df["mineria_expo"], errors="coerce")
    df = df[df["anio"].between(cfg.project.years.start, cfg.project.years.end)]

    if df["anio"].nunique() == 1:
        base_year = df["anio"].iloc[0]
        if base_year != cfg.project.years.start or base_year != cfg.project.years.end:
            years = range(cfg.project.years.start, cfg.project.years.end + 1)
            base = df.copy()
            frames = []
            for year in years:
                tmp = base.copy()
                tmp["anio"] = year
                frames.append(tmp)
            df = pd.concat(frames, ignore_index=True)

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "features_mineria.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_mineria.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_mineria",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="mineria features",
    )
    logger.info("mineria features built at %s", dest)
    return dest
