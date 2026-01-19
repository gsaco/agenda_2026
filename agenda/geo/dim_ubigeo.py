from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_dim_ubigeo(cfg: Config, staging_path: Path) -> Path:
    df = read_parquet(staging_path)
    df = df.copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["dep"] = df["dep"].astype(str).str.zfill(2)
    df["prov"] = df["prov"].astype(str).str.zfill(2)
    df["dist"] = df["dist"].astype(str).str.zfill(2)

    geo_dir = ensure_dir(Path(cfg.paths.geo_dir))
    dest = geo_dir / "dim_ubigeo.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_dim_ubigeo.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="geo_dim_ubigeo",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([staging_path]),
        notes="dim ubigeo",
    )
    logger.info("dim_ubigeo built at %s", dest)
    return dest
