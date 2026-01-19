from __future__ import annotations

import logging
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def plot_concentracion(cfg: Config, panel_path: Path) -> Path:
    df = read_parquet(panel_path)
    fig_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "figures")
    dest = fig_dir / "pib_total_por_anio.png"

    totals = df.groupby("anio")["pib"].sum().reset_index()
    plt.figure(figsize=(6, 4))
    plt.plot(totals["anio"], totals["pib"], marker="o")
    plt.title("PIB total por anio")
    plt.xlabel("anio")
    plt.ylabel("pib")
    plt.tight_layout()
    plt.savefig(dest, dpi=150)
    plt.close()

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="figuras_pib_total",
        version=None,
        checksum=hash_file(dest),
        notes="simple trend plot",
    )
    logger.info("figure saved to %s", dest)
    return dest
