from __future__ import annotations

import logging
from pathlib import Path

import numpy as np
import pandas as pd
from scipy.stats import spearmanr

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_csv
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def build_sensibilidad(cfg: Config, indice_path: Path) -> Path:
    df = read_parquet(indice_path)
    year = cfg.project.years.end
    df = df[df["anio"] == year].copy()

    if "vulnerabilidad" not in df.columns:
        raise ValueError("indice_vulnerabilidad must include vulnerabilidad column")

    base = df.set_index("ubigeo")["vulnerabilidad"]
    weights = [
        (0.5, 0.5),
        (0.7, 0.3),
        (0.3, 0.7),
    ]

    records = []
    for w_level, w_shock in weights:
        if "comp_nivel" not in df.columns or "comp_shock" not in df.columns:
            continue
        alt = w_level * df.set_index("ubigeo")["comp_nivel"] + w_shock * df.set_index("ubigeo")["comp_shock"]
        rho = spearmanr(base, alt).correlation
        records.append({"w_level": w_level, "w_shock": w_shock, "spearman": float(rho)})

    out = pd.DataFrame(records)
    tables_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "tables")
    dest = tables_dir / "sensibilidad_indice.csv"
    write_csv(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_sensibilidad",
        version=None,
        checksum=hash_file(dest),
        notes="sensitivity of vulnerability weights",
    )
    logger.info("sensibilidad table saved to %s", dest)
    return dest
