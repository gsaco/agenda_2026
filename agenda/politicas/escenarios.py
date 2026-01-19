from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..politicas.targeting import seleccionar_beneficiarios
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_csv
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def simular_escenario(
    cfg: Config,
    panel_path: Path,
    indice_path: Path,
    lisa_path: Path | None,
    escenario_id: str,
    regla: str,
    parametros: dict[str, float],
    horizonte: int = 5,
) -> Path:
    panel = read_parquet(panel_path)
    indice = read_parquet(indice_path)

    df = panel.merge(indice, on=["ubigeo", "anio"], how="left")
    year = cfg.project.years.end
    df = df[df["anio"] == year].copy()

    lisa_df = read_parquet(lisa_path) if lisa_path else pd.DataFrame()
    beneficiarios = seleccionar_beneficiarios(df, regla, lisa_df=lisa_df)
    df["beneficiario"] = beneficiarios

    impact = parametros.get("impacto", 0.01)
    df["impacto_esperado"] = df["beneficiario"].astype(float) * impact
    df["horizonte"] = horizonte

    outputs_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "policy")
    dest = outputs_dir / f"escenario_{escenario_id}_beneficiarios.csv"
    write_csv(dest, df[["ubigeo", "anio", "beneficiario", "impacto_esperado", "horizonte"]])

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="politicas_escenario",
        version=None,
        checksum=hash_file(dest),
        notes=f"escenario {escenario_id} regla {regla}",
    )
    logger.info("escenario %s saved to %s", escenario_id, dest)
    return dest
