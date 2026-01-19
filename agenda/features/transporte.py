from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
import geopandas as gpd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


def build_transporte(cfg: Config, raw_path: Path) -> Path:
    if raw_path.is_dir():
        files = sorted(list(raw_path.glob("*.zip")) + list(raw_path.glob("*.shp")) + list(raw_path.glob("*.kml")) + list(raw_path.glob("*.kmz")))
        if not files:
            raise ValueError("transporte data directory has no supported files")

        limits_path = Path(cfg.paths.geo_dir) / "dim_territorio_base.gpkg"
        limites = gpd.read_file(limits_path)[["ubigeo", "geometry"]].copy()

        roads = []
        target_crs = "EPSG:4326"
        for file_path in files:
            try:
                if file_path.suffix.lower() == ".zip":
                    gdf = gpd.read_file(f"zip://{file_path}")
                else:
                    gdf = gpd.read_file(file_path)
                if gdf.crs:
                    gdf = gdf.to_crs(target_crs)
                else:
                    gdf = gdf.set_crs(target_crs)
                roads.append(gdf[["geometry"]])
            except Exception as exc:
                logger.warning("transporte: failed to read %s (%s)", file_path.name, exc)
        if not roads:
            raise ValueError("transporte data directory has no readable files")
        roads = pd.concat(roads, ignore_index=True)
        roads = gpd.GeoDataFrame(roads, geometry="geometry", crs=limites.crs)

        limits_proj = limites.to_crs("EPSG:3857")
        roads_proj = roads.to_crs("EPSG:3857")
        inter = gpd.overlay(roads_proj, limits_proj, how="intersection")
        inter["length_km"] = inter.geometry.length / 1000.0
        lengths = inter.groupby("ubigeo")["length_km"].sum().reset_index()

        dim_path = Path(cfg.paths.geo_dir) / "dim_territorio_base.parquet"
        dim = pd.read_parquet(dim_path)[["ubigeo", "area_km2"]]
        df = lengths.merge(dim, on="ubigeo", how="left")
        df["road_density"] = df["length_km"] / df["area_km2"]
        df = df[["ubigeo", "road_density"]].copy()
        years = range(cfg.project.years.start, cfg.project.years.end + 1)
        frames = []
        for year in years:
            temp = df.copy()
            temp["anio"] = year
            frames.append(temp)
        df = pd.concat(frames, ignore_index=True)
    else:
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
            "dist_road": "dist_road",
            "road_density": "road_density",
        })

        if not {"ubigeo", "anio"}.issubset(df.columns):
            raise ValueError("transporte data must include ubigeo and anio")

        df = df.copy()
        df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
        df["anio"] = df["anio"].astype(int)

    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "features_transporte.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_transporte.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_transporte",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="transporte features",
    )
    logger.info("transporte features built at %s", dest)
    return dest
