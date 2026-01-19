from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..schemas.crosswalk_schema import validate as validate_schema
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_crosswalk(cfg: Config, dim_ubigeo_path: Path) -> Path:
    df = read_parquet(dim_ubigeo_path)
    years = range(cfg.project.years.start, cfg.project.years.end + 1)
    records = []
    for y in years:
        for ubigeo in df["ubigeo"].astype(str).tolist():
            records.append(
                {
                    "ubigeo_origen": ubigeo,
                    "anio_origen": y,
                    "ubigeo_base": ubigeo,
                    "peso": 1.0,
                }
            )
    cw = pd.DataFrame(records)

    geo_dir = ensure_dir(Path(cfg.paths.geo_dir))
    dest = geo_dir / "crosswalk_ubigeo.parquet"
    write_parquet(dest, cw)

    schema_result = validate_schema(cw)
    qc = {"schema": schema_result, "missingness": missingness_report(cw)}
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_crosswalk.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="geo_crosswalk",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([dim_ubigeo_path]),
        notes="identity crosswalk",
    )
    logger.info("crosswalk built at %s", dest)
    return dest
