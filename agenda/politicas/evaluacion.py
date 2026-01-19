from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_csv, write_csv
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def evaluar_cobertura(cfg: Config, escenario_path: Path) -> Path:
    df = read_csv(escenario_path)
    coverage = {
        "beneficiarios": int(df["beneficiario"].sum()),
        "total": int(len(df)),
        "share": float(df["beneficiario"].mean()),
    }
    out = pd.DataFrame([coverage])

    outputs_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "policy")
    dest = outputs_dir / f"{Path(escenario_path).stem}_cobertura.csv"
    write_csv(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="politicas_evaluacion",
        version=None,
        checksum=hash_file(dest),
        notes="coverage metrics",
    )
    logger.info("cobertura saved to %s", dest)
    return dest
