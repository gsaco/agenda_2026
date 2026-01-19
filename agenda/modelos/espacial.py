from __future__ import annotations

import logging
from pathlib import Path

import geopandas as gpd
import pandas as pd
import warnings

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def _try_import_pysal():
    try:
        from libpysal.weights import Queen, KNN
        from esda.moran import Moran, Moran_Local
    except Exception:  # pragma: no cover
        return None
    return Queen, KNN, Moran, Moran_Local


def build_lisa(cfg: Config, panel_path: Path, limites_path: Path) -> Path:
    pysal = _try_import_pysal()
    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "lisa_resultados.parquet"

    if pysal is None:
        logger.warning("pysal not available; writing placeholder lisa_resultados")
        empty = pd.DataFrame(columns=["ubigeo", "anio", "indicador", "cluster"])
        write_parquet(dest, empty)
        register_artifact(
            cfg.paths.manifest,
            dest,
            source="modelos_lisa_placeholder",
            version=None,
            checksum=hash_file(dest),
            notes="pysal missing",
        )
        return dest

    Queen, KNN, Moran, Moran_Local = pysal
    gdf = gpd.read_file(limites_path)
    gdf = gdf.rename(columns={"UBIGEO": "ubigeo", "ubigeo": "ubigeo"})
    gdf["ubigeo"] = gdf["ubigeo"].astype(str).str.zfill(6)

    panel = read_parquet(panel_path)
    year = cfg.project.years.end
    panel = panel[panel["anio"] == year].copy()

    merged = gdf.merge(panel, on="ubigeo", how="left")
    indicators = ["pib", "pib_pc", "pib_km2", "iae"]

    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore",
            message="The weights matrix is not fully connected.*",
            category=UserWarning,
        )
        w = Queen.from_dataframe(merged)
    if w.islands:
        w = KNN.from_dataframe(merged, k=5)

    records = []
    for ind in indicators:
        values = merged[ind].fillna(0).values
        moran = Moran(values, w)
        lisa = Moran_Local(values, w)
        for ubigeo, cluster in zip(merged["ubigeo"], lisa.q):
            records.append(
                {
                    "ubigeo": ubigeo,
                    "anio": year,
                    "indicador": ind,
                    "cluster": int(cluster),
                    "moran_i": float(moran.I),
                    "p_value": float(moran.p_sim),
                }
            )

    out = pd.DataFrame(records)
    write_parquet(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_lisa",
        version=None,
        checksum=hash_file(dest),
        notes=f"lisa for year {year}",
    )
    logger.info("lisa results saved to %s", dest)
    return dest
