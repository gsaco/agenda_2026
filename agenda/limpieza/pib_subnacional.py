from __future__ import annotations

import logging
from pathlib import Path
import re

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..schemas.panel_pib_schema import validate as validate_schema
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import read_parquet, write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json
from ..utils.text import normalize_name

logger = logging.getLogger(__name__)


COL_MAP = {
    "UBIGEO": "ubigeo",
    "ubigeo": "ubigeo",
    "codigo": "ubigeo",
    "anio": "anio",
    "year": "anio",
    "pib": "pib",
    "pbi": "pib",
    "gdp": "pib",
}


def clean_pib_subnacional(cfg: Config, raw_path: Path) -> Path:
    notes = "cleaned and standardized"
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    elif suffix == ".dta":
        df = pd.read_stata(raw_path)
    else:
        df = pd.read_csv(raw_path)

    df = df.rename(columns={c: COL_MAP.get(c, c) for c in df.columns})
    expected = {"ubigeo", "anio", "pib"}
    if not expected.issubset(df.columns):
        if suffix not in [".xlsx", ".xls"]:
            missing = expected - set(df.columns)
            raise ValueError(f"missing required columns: {sorted(missing)}")

        df_dep = _parse_inei_departamental(cfg, raw_path)
        df = df_dep
        notes = "INEI PBI departamental allocated to districts using population shares"

    df = df[["ubigeo", "anio", "pib"]].copy()
    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["pib"] = pd.to_numeric(df["pib"], errors="coerce")

    staging_dir = ensure_dir(Path(cfg.paths.staging_dir))
    dest = staging_dir / "pib_subnacional.parquet"
    write_parquet(dest, df)

    schema_result = validate_schema(df)
    qc = {
        "schema": schema_result,
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_pib_subnacional.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="limpieza_pib_subnacional",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes=notes,
    )
    logger.info("pib_subnacional cleaned to %s", dest)
    return dest


def _parse_inei_departamental(cfg: Config, raw_path: Path) -> pd.DataFrame:
    df_raw = pd.read_excel(raw_path, sheet_name=0, header=None)
    header_row = None
    for idx in range(min(40, len(df_raw))):
        row = df_raw.loc[idx]
        row_str = row.astype(str).str.strip()
        if (row_str == "Departamentos").any():
            header_row = idx
            break
    if header_row is None:
        raise ValueError("INEI PBI: header row not found")

    header = df_raw.loc[header_row].tolist()
    df = df_raw.loc[header_row + 1 :].copy()
    df.columns = header
    first_col = header[0]
    df = df.rename(columns={first_col: "departamento"})
    df = df[df["departamento"].notna()].copy()
    df["departamento"] = df["departamento"].astype(str).str.strip()
    df = df[~df["departamento"].str.contains("TOTAL", case=False, na=False)]

    year_map = {}
    for col in df.columns:
        match = re.match(r"(\d{4})", str(col))
        if match:
            year_map[col] = int(match.group(1))

    if not year_map:
        raise ValueError("INEI PBI: year columns not found")

    df = df.rename(columns=year_map)
    id_cols = ["departamento"]
    year_cols = sorted(year_map.values())
    df = df[id_cols + year_cols]
    df = df.melt(id_vars=["departamento"], var_name="anio", value_name="pib_dep")
    df["anio"] = df["anio"].astype(int)
    df["pib_dep"] = pd.to_numeric(df["pib_dep"], errors="coerce")

    dim_path = Path(cfg.paths.geo_dir) / "dim_ubigeo.parquet"
    dim = read_parquet(dim_path)
    dep_map = dim[["dep", "dep_name"]].drop_duplicates().copy()
    dep_map["dep_norm"] = dep_map["dep_name"].map(normalize_name)
    df["dep_norm"] = df["departamento"].map(normalize_name)
    aliases = {
        "PROV CONST DEL CALLAO": "CALLAO",
        "PROVINCIA DE LIMA": "LIMA",
        "REGION LIMA": "LIMA",
        "REGION METROPOLITANA DE LIMA": "LIMA",
    }
    df["dep_norm"] = df["dep_norm"].replace(aliases)
    df = df.merge(dep_map[["dep", "dep_norm"]], on="dep_norm", how="left")

    df = df[df["dep"].notna()].copy()
    df = df.groupby(["dep", "anio"], as_index=False)["pib_dep"].sum()

    pob_path = Path(cfg.paths.processed_dir) / "poblacion.parquet"
    if not pob_path.exists():
        raise ValueError("INEI PBI requires poblacion.parquet to allocate to districts")
    pob = read_parquet(pob_path)
    pob["dep"] = pob["ubigeo"].astype(str).str[:2]
    pob_tot = pob.groupby(["dep", "anio"])["pob"].sum().rename("pob_dep").reset_index()
    pob = pob.merge(pob_tot, on=["dep", "anio"], how="left")
    pob["share"] = pob["pob"] / pob["pob_dep"]

    df = df.merge(pob[["ubigeo", "dep", "anio", "share"]], on=["dep", "anio"], how="inner")
    df["pib"] = df["pib_dep"] * df["share"]
    df = df[["ubigeo", "anio", "pib"]].copy()
    df = df[df["anio"].between(cfg.project.years.start, cfg.project.years.end)]
    return df
