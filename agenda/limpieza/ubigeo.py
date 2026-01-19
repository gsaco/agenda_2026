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

COL_MAP = {
    "UBIGEO": "ubigeo",
    "ubigeo": "ubigeo",
    "IDDIST": "ubigeo",
    "dep": "dep",
    "prov": "prov",
    "dist": "dist",
    "departamento": "dep_name",
    "provincia": "prov_name",
    "distrito": "dist_name",
    "NOMBDEP": "dep_name",
    "NOMBPROV": "prov_name",
    "NOMBDIST": "dist_name",
}


def clean_ubigeo(cfg: Config, raw_path: Path) -> Path:
    if raw_path.suffix.lower() in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path)
    df = df.rename(columns={c: COL_MAP.get(c, c) for c in df.columns})

    if "ubigeo" not in df.columns:
        raise ValueError("missing ubigeo column")

    df = df[df["ubigeo"].notna()].copy()
    df["ubigeo"] = df["ubigeo"].astype(float).astype(int).astype(str).str.zfill(6)
    if "dep" not in df.columns or "prov" not in df.columns or "dist" not in df.columns:
        df["dep"] = df["ubigeo"].str[:2]
        df["prov"] = df["ubigeo"].str[2:4]
        df["dist"] = df["ubigeo"].str[4:6]

    df = df.copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["dep"] = df["dep"].astype(str).str.zfill(2)
    df["prov"] = df["prov"].astype(str).str.zfill(2)
    df["dist"] = df["dist"].astype(str).str.zfill(2)

    staging_dir = ensure_dir(Path(cfg.paths.staging_dir))
    dest = staging_dir / "ubigeo.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_ubigeo.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="limpieza_ubigeo",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="cleaned and standardized",
    )
    logger.info("ubigeo cleaned to %s", dest)
    return dest
