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


def build_bosques(cfg: Config, raw_path: Path) -> Path:
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path, sep=";", encoding="latin1")

    df = df.rename(columns={
        "UBIGEO": "ubigeo",
        "ubigeo": "ubigeo",
        "ANIO": "anio",
        "anio": "anio",
        "year": "anio",
        "PERDIDA_BOSQUE": "deforest_ha",
        "deforest_ha": "deforest_ha",
    })

    if not {"ubigeo", "anio", "deforest_ha"}.issubset(df.columns):
        raise ValueError("bosques data must include ubigeo, anio, deforest_ha")

    df = df.copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["deforest_ha"] = pd.to_numeric(df["deforest_ha"], errors="coerce")
    df = df[df["anio"].between(cfg.project.years.start, cfg.project.years.end)]

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "features_bosques.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_bosques.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_bosques",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="bosques features",
    )
    logger.info("bosques features built at %s", dest)
    return dest
