from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..schemas.panel_schema import validate as validate_schema
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_panel(
    cfg: Config,
    indicadores_path: Path,
    ntl_path: Path | None = None,
    transporte_path: Path | None = None,
    mineria_path: Path | None = None,
    bosques_path: Path | None = None,
    clima_path: Path | None = None,
    shock_macro_path: Path | None = None,
) -> Path:
    df = read_parquet(indicadores_path)

    if ntl_path and Path(ntl_path).exists():
        df = df.merge(read_parquet(ntl_path), on=["ubigeo", "anio"], how="left")
    if transporte_path and Path(transporte_path).exists():
        df = df.merge(read_parquet(transporte_path), on=["ubigeo", "anio"], how="left")
    if mineria_path and Path(mineria_path).exists():
        df = df.merge(read_parquet(mineria_path), on=["ubigeo", "anio"], how="left")
    if bosques_path and Path(bosques_path).exists():
        df = df.merge(read_parquet(bosques_path), on=["ubigeo", "anio"], how="left")
    if clima_path and Path(clima_path).exists():
        df = df.merge(read_parquet(clima_path), on=["ubigeo", "anio"], how="left")
    if shock_macro_path and Path(shock_macro_path).exists():
        df = df.merge(read_parquet(shock_macro_path), on=["anio"], how="left")

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "panel_analitico.parquet"
    write_parquet(dest, df)

    schema_result = validate_schema(df)
    qc = {
        "schema": schema_result,
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_panel.json", qc)

    input_paths = [p for p in [indicadores_path, ntl_path, transporte_path, mineria_path, bosques_path, clima_path, shock_macro_path] if p]
    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_panel",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths(input_paths),
        notes="panel analitico",
    )
    logger.info("panel_analitico built at %s", dest)
    return dest
