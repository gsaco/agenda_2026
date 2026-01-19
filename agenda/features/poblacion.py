from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file, hash_paths
from ..utils.io import write_parquet
from ..utils.paths import ensure_dir
from ..utils.qc import missingness_report, uniqueness_report, write_qc_json
from ..utils.text import normalize_name

logger = logging.getLogger(__name__)


def _build_from_worldpop(cfg: Config, df: pd.DataFrame) -> pd.DataFrame:
    pop_cols = [c for c in df.columns if c.startswith("Pop_")]
    if not pop_cols or "Name" not in df.columns:
        raise ValueError("WorldPop L2 data must include Name and Pop_YYYY columns")

    dep_fixes = {
        "CONSTITUCIONAL DEL CALLAO": "CALLAO",
    }
    prov_fixes = {
        "PUERTO": "PUERTO INCA",
        "PUIRA": "PIURA",
        "NAZCA": "NASCA",
        "VILCAS HUMAN": "VILCAS HUAMAN",
        "SAN ANTIONO DE PUTINA": "SAN ANTONIO DE PUTINA",
        "CONSTITUCIONAL DEL CALLAO": "CALLAO",
    }

    df = df.copy()
    name_split = df["Name"].astype(str).str.split("_", n=1, expand=True)
    df["dep_name"] = name_split[0].str.replace("_", " ", regex=False).fillna("")
    df["prov_name"] = name_split[1].str.replace("_", " ", regex=False).fillna("")
    df = df[~df["dep_name"].str.upper().isin(["NA", "N/A", ""])]
    df["dep_name"] = df["dep_name"].str.upper().replace(dep_fixes)
    df["prov_name"] = df["prov_name"].str.upper().replace(prov_fixes)

    dim_path = Path(cfg.paths.geo_dir) / "dim_ubigeo.parquet"
    dim = pd.read_parquet(dim_path)
    prov = dim[["dep", "prov", "dep_name", "prov_name"]].drop_duplicates().copy()
    prov["dep_norm"] = prov["dep_name"].map(normalize_name)
    prov["prov_norm"] = prov["prov_name"].map(normalize_name)
    prov["prov_code"] = prov["dep"].astype(str).str.zfill(2) + prov["prov"].astype(str).str.zfill(2)

    df["dep_norm"] = df["dep_name"].map(normalize_name)
    df["prov_norm"] = df["prov_name"].map(normalize_name)
    df = df.merge(prov[["prov_code", "dep_norm", "prov_norm"]], on=["dep_norm", "prov_norm"], how="left")

    missing = df["prov_code"].isna().sum()
    if missing:
        raise ValueError(f"WorldPop provinces not matched to ubigeo: {missing}")

    pop_long = df.melt(
        id_vars=["prov_code"],
        value_vars=pop_cols,
        var_name="anio",
        value_name="pob_prov",
    )
    pop_long["anio"] = pop_long["anio"].str.replace("Pop_", "", regex=False).astype(int)
    pop_long["pob_prov"] = pd.to_numeric(pop_long["pob_prov"], errors="coerce")

    dim_terr_path = Path(cfg.paths.geo_dir) / "dim_territorio_base.parquet"
    dim_terr = pd.read_parquet(dim_terr_path)
    dim_terr["prov_code"] = dim_terr["ubigeo"].astype(str).str[:4]
    dim_terr["area_km2"] = pd.to_numeric(dim_terr["area_km2"], errors="coerce")
    area_sum = dim_terr.groupby("prov_code")["area_km2"].sum().rename("prov_area")
    dim_terr = dim_terr.merge(area_sum, on="prov_code", how="left")
    dim_terr["area_share"] = dim_terr["area_km2"] / dim_terr["prov_area"]

    pop_dist = pop_long.merge(dim_terr[["ubigeo", "prov_code", "area_share"]], on="prov_code", how="left")
    pop_dist["pob"] = pop_dist["pob_prov"] * pop_dist["area_share"]
    return pop_dist[["ubigeo", "anio", "pob"]].copy()


def build_poblacion(cfg: Config, raw_path: Path) -> Path:
    suffix = raw_path.suffix.lower()
    if suffix in [".xlsx", ".xls"]:
        df = pd.read_excel(raw_path)
    else:
        df = pd.read_csv(raw_path)

    if "Name" in df.columns and any(c.startswith("Pop_") for c in df.columns):
        df = _build_from_worldpop(cfg, df)
    else:
        df = df.rename(columns={
            "UBIGEO": "ubigeo",
            "ubigeo": "ubigeo",
            "anio": "anio",
            "year": "anio",
            "pob": "pob",
            "poblacion": "pob",
            "population": "pob",
        })
        if not {"ubigeo", "anio", "pob"}.issubset(df.columns):
            raise ValueError("poblacion data must include ubigeo, anio, pob")
        df = df[["ubigeo", "anio", "pob"]].copy()

    df["ubigeo"] = df["ubigeo"].astype(str).str.zfill(6)
    df["anio"] = df["anio"].astype(int)
    df["pob"] = pd.to_numeric(df["pob"], errors="coerce")

    min_year = df["anio"].min()
    if cfg.project.years.start < min_year:
        base = df[df["anio"] == min_year].copy()
        extra = []
        for year in range(cfg.project.years.start, min_year):
            add = base.copy()
            add["anio"] = year
            extra.append(add)
        if extra:
            df = pd.concat([df] + extra, ignore_index=True)

    df = df[df["anio"].between(cfg.project.years.start, cfg.project.years.end)]

    processed_dir = ensure_dir(Path(cfg.paths.processed_dir))
    dest = processed_dir / "poblacion.parquet"
    write_parquet(dest, df)

    qc = {
        "missingness": missingness_report(df),
        "uniqueness": uniqueness_report(df, ["ubigeo", "anio"]),
    }
    write_qc_json(Path(cfg.paths.outputs_dir) / "qc" / "qc_poblacion.json", qc)

    register_artifact(
        cfg.paths.manifest,
        dest,
        source="features_poblacion",
        version=None,
        checksum=hash_file(dest),
        inputs_checksum=hash_paths([raw_path]),
        notes="poblacion compiled",
    )
    logger.info("poblacion built at %s", dest)
    return dest
