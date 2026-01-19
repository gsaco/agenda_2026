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


def build_ntl(cfg: Config, raw_path: Path) -> Path:
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path)
    df = df.rename(columns={
        "UBIGEO": "ubigeo",
        "ubigeo": "ubigeo",
        "anio": "anio",
        "year": "anio",
        "ntl": "ntl",
        "lights": "ntl",
    })

    if not {"ubigeo", "anio", "ntl"}.issubset(df.columns):
        raise ValueError("ntl data must include ubigeo, anio, ntl")

    df = df[["ubigeo", "anio", "ntl"]].copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["ntl"] = pd.to_numeric(df["ntl"], errors="coerce")

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "ntl_distrito_anual.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_ntl.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_ntl",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="ntl processed",
    )
    logger.info("ntl built at %s", dest)
    return dest
