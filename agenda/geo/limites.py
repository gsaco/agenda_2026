from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir, resolve_path
from ..utils.qc import missingness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_dim_territorio(cfg: Config, dim_ubigeo_path: Path, limites_path: Path) -> Path:
    geo_dir = ensure_dir(Path(cfg.paths.geo_dir))
    dest = geo_dir / "dim_territorio_base.gpkg"
    parquet_dest = geo_dir / "dim_territorio_base.parquet"

    source_path = resolve_path(limites_path)
    if not source_path.exists():
        raise FileNotFoundError(f"limites source not found: {source_path}")
    gdf = gpd.read_file(source_path)
    col_map = {"UBIGEO": "ubigeo", "ubigeo": "ubigeo"}
    gdf = gdf.rename(columns=col_map)
    if "ubigeo" not in gdf.columns:
        raise ValueError("limites data must include ubigeo column")
    gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    gdf = gdf[["ubigeo", "geometry"]].copy()
    gdf = gdf.set_geometry("geometry")

    gdf_proj = gdf.to_crs("EPSG:3857")
    gdf["area_km2"] = gdf_proj.area / 1_000_000.0
    centroids = gdf_proj.centroid.to_crs("EPSG:4326")
    gdf["centroid_lon"] = centroids.x
    gdf["centroid_lat"] = centroids.y

    gdf.to_file(dest, driver="GPKG")

    df = pd.DataFrame(gdf.drop(columns=["geometry"]))
    write_parquet(parquet_dest, df)

    qc = {
        "missingness": missingness_report(df),
        "rows": len(df),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_dim_territorio.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="geo_limites",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([dim_ubigeo_path, source_path]),
        notes="dim territorio base",
    )
    logger.info("dim_territorio built at %s", dest)
    return dest
