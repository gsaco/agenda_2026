from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import matplotlib.pyplot as plt
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def plot_mapa_indicador(
    cfg: Config,
    limites_path: Path,
    indicadores_path: Path,
    indicador: str,
    anio: int | None = None,
) -> Path:
    gdf = gpd.read_file(limites_path)
    gdf = gdf.rename(columns={"UBIGEO": "ubigeo", "ubigeo": "ubigeo"})
    gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    df = read_parquet(indicadores_path)
    if anio is None:
        anio = int(df["anio"].max())
    df = df[df["anio"] == anio][["ubigeo", indicador]]

    merged = gdf.merge(df, on="ubigeo", how="left")

    fig_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "figures")
    dest = fig_dir / f"mapa_{indicador}_{anio}.png"

    ax = merged.plot(column=indicador, legend=True, figsize=(6, 6))
    ax.set_axis_off()
    plt.title(f"{indicador} {anio}")
    plt.tight_layout()
    plt.savefig(dest, dpi=150)
    plt.close()

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="figuras_mapa",
        version=None,
        checksum=hash_file(dest),
        notes=f"mapa {indicador} {anio}",
    )
    logger.info("mapa saved to %s", dest)
    return dest
