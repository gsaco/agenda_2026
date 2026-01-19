from __future__ import annotations

import logging
from pathlib import Path
import json

import numpy as np
import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_csv, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json

logger = logging.getLogger(__name__)


MONTH_MAP = {
    "Ene": 1,
    "Feb": 2,
    "Mar": 3,
    "Abr": 4,
    "May": 5,
    "Jun": 6,
    "Jul": 7,
    "Ago": 8,
    "Sep": 9,
    "Oct": 10,
    "Nov": 11,
    "Dic": 12,
}


def _parse_bcrp_json(raw_path: Path) -> pd.DataFrame:
    obj = json.loads(raw_path.read_text(encoding="utf-8"))
    rows = []
    for period in obj.get("periods", []):
        name = period.get("name", "")
        val = period.get("values", [None])[0]
        if "." in name:
            try:
                mon_str, year_str = name.split(".")
                mes = MONTH_MAP.get(mon_str)
                anio = int(year_str)
            except Exception:
                continue
            rows.append({"anio": anio, "mes": mes, "tot": val})
        else:
            if str(name).isdigit():
                rows.append({"anio": int(name), "tot": val})
    df = pd.DataFrame(rows)
    df["tot"] = pd.to_numeric(df["tot"], errors="coerce")
    return df.dropna(subset=["anio"])


def build_shocks_macro(cfg: Config, raw_path: Path) -> Path:
    if raw_path.suffix.lower() == ".json":
        df = _parse_bcrp_json(raw_path)
    else:
        df = read_csv(raw_path)
        df = df.rename(columns={"year": "anio", "anio": "anio", "tot": "tot"})

    if not {"anio", "tot"}.issubset(df.columns):
        raise ValueError("bcrp data must include anio, tot")

    if "mes" in df.columns and df["mes"].notna().any():
        df = df[["anio", "tot"]].copy()
        df["anio"] = df["anio"].astype(int)
        df["tot"] = pd.to_numeric(df["tot"], errors="coerce")
        df = df.groupby("anio", as_index=False)["tot"].mean()
        df = df.sort_values("anio")
        df["dlog_tot"] = np.log(df["tot"]) - np.log(df["tot"].shift(1))
    else:
        df = df[["anio", "tot"]].copy()
        df["anio"] = df["anio"].astype(int)
        df["tot"] = pd.to_numeric(df["tot"], errors="coerce")
        df = df.sort_values("anio")
        df["dlog_tot"] = df["tot"] / 100.0

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "shock_macro.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_shock_macro.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_shock_macro",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="macro shocks",
    )
    logger.info("shock_macro built at %s", dest)
    return dest
