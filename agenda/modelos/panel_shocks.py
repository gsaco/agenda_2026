from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet, write_csv
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def build_panel_shocks(cfg: Config, panel_path: Path) -> Path:
    try:
        from linearmodels.panel import PanelOLS
    except Exception as exc:  # pragma: no cover
        raise RuntimeError("linearmodels not available") from exc

    df = read_parquet(panel_path)
    df = df.dropna(subset=["dlog_pib"]).copy()

    df = df.set_index(["ubigeo", "anio"])
    exog = df[["dlog_tot", "precip_anom", "mineria_expo"]].fillna(0.0)
    # Drop any zero-variance columns to avoid rank issues.
    exog = exog.loc[:, exog.nunique() > 1]
    if exog.empty:
        raise ValueError("no valid exog columns after variance check")

    mod = PanelOLS(
        df["dlog_pib"],
        exog,
        entity_effects=True,
        time_effects=True,
        drop_absorbed=True,
        check_rank=False,
    )
    res = mod.fit(cov_type="clustered", cluster_entity=True)

    out = pd.DataFrame({
        "param": res.params.index,
        "coef": res.params.values,
        "std_err": res.std_errors.values,
        "p_value": res.pvalues.values,
    })

    tables_dir = ensure_dir(Path(cfg.paths.outputs_dir) / "tables")
    dest = tables_dir / "regresiones_principales.csv"
    write_csv(dest, out)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="modelos_panel_shocks",
        version=None,
        checksum=hash_file(dest),
        notes="panel FE shocks",
    )
    logger.info("panel_shocks table saved to %s", dest)
    return dest
