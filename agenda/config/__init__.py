from __future__ import annotations

from typing import Optional

from pydantic import BaseModel, Field, field_validator

from ..utils.io import read_yaml
from ..utils.paths import resolve_path


class Years(BaseModel):
    start: int = Field(..., ge=1900)
    end: int = Field(..., ge=1900)

    @field_validator("end")
    @classmethod
    def validate_end(cls, v: int, info):
        start = info.data.get("start")
        if start is not None and v < start:
            raise ValueError("end year must be >= start year")
        return v


class ProjectConfig(BaseModel):
    name: str = "agenda_2026"
    mode: str = "real"  # real only (demo disabled)
    seed: int = 123
    years: Years
    base_year: int = 2023
    iae_def: str = "A1"

    @field_validator("mode")
    @classmethod
    def validate_mode(cls, v: str) -> str:
        if v != "real":
            raise ValueError("demo mode is disabled; use real data inputs")
        return v


class PathsConfig(BaseModel):
    data_dir: str = "data"
    raw_dir: str = "data/raw"
    staging_dir: str = "data/staging"
    processed_dir: str = "data/processed"
    geo_dir: str = "data/geo"
    outputs_dir: str = "outputs"
    dist_dir: str = "dist"
    logs_dir: str = "logs"
    manifest: str = "data/manifest.json"


class FlagsConfig(BaseModel):
    enable_geo: bool = True
    enable_ntl: bool = True
    enable_transporte: bool = True
    enable_mineria: bool = True
    enable_bosques: bool = True
    enable_clima: bool = True
    enable_shocks_macro: bool = True
    enable_models: bool = True
    enable_policy: bool = True
    enable_figures: bool = True
    enable_paper: bool = True


class IngestSourceConfig(BaseModel):
    auto: bool = False
    path: Optional[str] = None
    version: Optional[str] = None
    url: Optional[str] = None
    fallback_urls: list[str] = []


class IngestConfig(BaseModel):
    pib_subnacional: IngestSourceConfig = IngestSourceConfig()
    ubigeo: IngestSourceConfig = IngestSourceConfig()
    limites: IngestSourceConfig = IngestSourceConfig()
    poblacion: IngestSourceConfig = IngestSourceConfig()
    ntl: IngestSourceConfig = IngestSourceConfig()
    transporte: IngestSourceConfig = IngestSourceConfig()
    mineria: IngestSourceConfig = IngestSourceConfig()
    bosques: IngestSourceConfig = IngestSourceConfig()
    clima: IngestSourceConfig = IngestSourceConfig()
    bcrp: IngestSourceConfig = IngestSourceConfig()


class QCConfig(BaseModel):
    max_agg_gap_pct: float = 0.5
    missing_threshold_pct: float = 0.02
    outlier_p: float = 0.999


class CacheConfig(BaseModel):
    use_cache: bool = True


class Config(BaseModel):
    project: ProjectConfig
    paths: PathsConfig = PathsConfig()
    flags: FlagsConfig = FlagsConfig()
    ingest: IngestConfig = IngestConfig()
    qc: QCConfig = QCConfig()
    cache: CacheConfig = CacheConfig()


def load_config(path: str) -> Config:
    cfg_dict = read_yaml(resolve_path(path))
    return Config.model_validate(cfg_dict)
