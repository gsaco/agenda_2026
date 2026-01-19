from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def build_modalidades(cfg: Config, panel_path: Path, k_list: list[int] | None = None) -> Path:
    k_list = k_list or [3, 4, 5]
    df = read_parquet(panel_path)
    year = cfg.project.years.end
    df = df[df["anio"] == year].copy()

    features = ["pib_pc", "pib_km2", "iae"]
    X = df[features].fillna(0.0).values
    X = StandardScaler().fit_transform(X)

    records = []
    for k in k_list:
        km = KMeans(n_clusters=k, random_state=cfg.project.seed, n_init=10)
        labels = km.fit_predict(X)
        for ubigeo, label in zip(df["ubigeo"], labels):
            records.append({"ubigeo": ubigeo, "anio": year, "k": k, "cluster": int(label)})

    out = pd.DataFrame(records)
    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "modalidades_kmeans.parquet"
    write_parquet(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_modalidades",
        version=None,
        checksum=hash_file(dest),
        notes=f"kmeans for year {year}",
    )
    logger.info("modalidades_kmeans saved to %s", dest)
    return dest
