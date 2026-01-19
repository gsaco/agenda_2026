from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def harmonize_panel(
    cfg: Config,
    panel_path: Path,
    crosswalk_path: Path,
    value_cols: Iterable[str],
    output_name: str,
) -> Path:
    df = read_parquet(panel_path)
    cw = read_parquet(crosswalk_path)

    df = df.copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)

    cw = cw.copy()
    cw["ubigeo_origen"] = cw["ubigeo_origen"].astype(str).str.zfill(6)
    cw["anio_origen"] = cw["anio_origen"].astype(int)

    merged = df.merge(
        cw,
        left_on=["ubigeo", "anio"],
        right_on=["ubigeo_origen", "anio_origen"],
        how="left",
    )
    if merged["ubigeo_base"].isna().any():
        raise ValueError("crosswalk merge produced missing ubigeo_base")

    for col in value_cols:
        merged[col] = merged[col] * merged["peso"]

    out = merged.groupby(["ubigeo_base", "anio"], as_index=False)[list(value_cols)].sum()
    out = out.rename(columns={"ubigeo_base": "ubigeo"})

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / output_name
    write_parquet(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="geo_harmonize",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([panel_path, crosswalk_path]),
        notes="harmonized to base ubigeo",
    )
    logger.info("harmonized panel saved to %s", dest)
    return dest
