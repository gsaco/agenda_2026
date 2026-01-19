from __future__ import annotations

import logging
from pathlib import Path
from typing import Callable

from .config import Config
from .geo.armonizar import harmonize_panel
from .geo.crosswalk import build_crosswalk
from .geo.dim_ubigeo import build_dim_ubigeo
from .geo.limites import build_dim_territorio
from .ingesta.bcrp import ingest_bcrp
from .ingesta.bosques import ingest_bosques
from .ingesta.clima import ingest_clima
from .ingesta.limites import ingest_limites
from .ingesta.mineria import ingest_mineria
from .ingesta.ntl import ingest_ntl
from .ingesta.pib_subnacional import ingest_pib_subnacional
from .ingesta.poblacion import ingest_poblacion
from .ingesta.transporte import ingest_transporte
from .ingesta.ubigeo import ingest_ubigeo
from .limpieza.pib_subnacional import clean_pib_subnacional
from .limpieza.ubigeo import clean_ubigeo
from .features.bosques import build_bosques
from .features.clima import build_clima
from .features.indicadores_core import build_indicadores_core
from .features.mineria import build_mineria
from .features.ntl import build_ntl
from .features.panel import build_panel
from .features.poblacion import build_poblacion
from .features.shocks_macro import build_shocks_macro
from .features.transporte import build_transporte
from .modelos.clusters import build_modalidades
from .modelos.descriptivo import build_concentracion, build_contribucion_crecimiento
from .modelos.espacial import build_lisa
from .modelos.panel_shocks import build_panel_shocks
from .modelos.sensibilidad import build_sensibilidad
from .paper.compile import build_paper
from .politicas.escenarios import simular_escenario
from .politicas.evaluacion import evaluar_cobertura
from .politicas.vulnerabilidad import build_indice_vulnerabilidad
from .utils.io import read_json
from .utils.paths import resolve_path

logger = logging.getLogger(__name__)


def _maybe_skip(cfg: Config, path: Path, action: Callable[[], Path]) -> Path:
    if cfg.cache.use_cache and path.exists():
        logger.info("cache hit for %s", path)
        return path
    return action()


class Pipeline:
    def __init__(self, cfg: Config) -> None:
        self.cfg = cfg

    def _first_raw_file(self, folder: Path) -> Path:
        candidates = sorted(folder.glob("*"))
        if not candidates:
            raise FileNotFoundError(f"no files found in {folder}")
        return candidates[0]

    def validate_config(self) -> None:
        # Accessing cfg ensures pydantic validation already ran.
        if self.cfg.project.mode != "real":
            raise ValueError("demo mode is disabled; provide real data sources")
        manifest_path = resolve_path(self.cfg.paths.manifest)
        if manifest_path.exists():
            entries = read_json(manifest_path)
            demo_entries = [
                e
                for e in entries
                if str(e.get("source", "")).startswith("demo")
                or "synthetic" in str(e.get("notes", "")).lower()
            ]
            if demo_entries:
                raise ValueError(
                    "demo artifacts detected in manifest; remove data/ and outputs/ "
                    "from prior demo runs before proceeding"
                )
        logger.info("config validated")

    def ingest(self, target: str | None = None) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if target in (None, "pib_subnacional"):
            if self.cfg.ingest.pib_subnacional.path or self.cfg.ingest.pib_subnacional.auto:
                outputs["pib_subnacional"] = ingest_pib_subnacional(self.cfg)
            else:
                logger.info("skip pib_subnacional ingest: no source configured")
        if target in (None, "ubigeo"):
            if self.cfg.ingest.ubigeo.path or self.cfg.ingest.ubigeo.auto:
                outputs["ubigeo"] = ingest_ubigeo(self.cfg)
            else:
                logger.info("skip ubigeo ingest: no source configured")
        if target in (None, "limites") and self.cfg.flags.enable_geo:
            if self.cfg.ingest.limites.path or self.cfg.ingest.limites.auto:
                outputs["limites"] = ingest_limites(self.cfg)
            else:
                logger.info("skip limites ingest: no source configured")
        if target in (None, "poblacion"):
            if self.cfg.ingest.poblacion.path or self.cfg.ingest.poblacion.auto:
                outputs["poblacion"] = ingest_poblacion(self.cfg)
            else:
                logger.info("skip poblacion ingest: no source configured")
        if target in (None, "ntl") and self.cfg.flags.enable_ntl:
            if self.cfg.ingest.ntl.path or self.cfg.ingest.ntl.auto:
                outputs["ntl"] = ingest_ntl(self.cfg)
            else:
                logger.info("skip ntl ingest: no source configured")
        if target in (None, "transporte") and self.cfg.flags.enable_transporte:
            if self.cfg.ingest.transporte.path or self.cfg.ingest.transporte.auto:
                outputs["transporte"] = ingest_transporte(self.cfg)
            else:
                logger.info("skip transporte ingest: no source configured")
        if target in (None, "mineria") and self.cfg.flags.enable_mineria:
            if self.cfg.ingest.mineria.path or self.cfg.ingest.mineria.auto:
                outputs["mineria"] = ingest_mineria(self.cfg)
            else:
                logger.info("skip mineria ingest: no source configured")
        if target in (None, "bosques") and self.cfg.flags.enable_bosques:
            if self.cfg.ingest.bosques.path or self.cfg.ingest.bosques.auto:
                outputs["bosques"] = ingest_bosques(self.cfg)
            else:
                logger.info("skip bosques ingest: no source configured")
        if target in (None, "clima") and self.cfg.flags.enable_clima:
            if self.cfg.ingest.clima.path or self.cfg.ingest.clima.auto:
                outputs["clima"] = ingest_clima(self.cfg)
            else:
                logger.info("skip clima ingest: no source configured")
        if target in (None, "bcrp") and self.cfg.flags.enable_shocks_macro:
            if self.cfg.ingest.bcrp.path or self.cfg.ingest.bcrp.auto:
                outputs["bcrp"] = ingest_bcrp(self.cfg)
            else:
                logger.info("skip bcrp ingest: no source configured")
        return outputs

    def build(self, target: str | None = None) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        if target in (None, "ubigeo"):
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "ubigeo"
            raw = self._first_raw_file(raw_dir)
            outputs["ubigeo"] = clean_ubigeo(self.cfg, raw)
        if target in (None, "dim_ubigeo"):
            staging = resolve_path(self.cfg.paths.staging_dir) / "ubigeo.parquet"
            outputs["dim_ubigeo"] = build_dim_ubigeo(self.cfg, staging)
        if target in (None, "dim_territorio") and self.cfg.flags.enable_geo:
            dim_ubigeo_path = resolve_path(self.cfg.paths.geo_dir) / "dim_ubigeo.parquet"
            limites_path = resolve_path(self.cfg.paths.raw_dir) / "limites"
            # Use the first file in raw/limites as the source.
            candidates = sorted(limites_path.glob("*"))
            if not candidates:
                raise FileNotFoundError("no limites files found in data/raw/limites")
            outputs["dim_territorio"] = build_dim_territorio(self.cfg, dim_ubigeo_path, candidates[0])
        if target in (None, "poblacion"):
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "poblacion"
            raw = self._first_raw_file(raw_dir)
            outputs["poblacion"] = build_poblacion(self.cfg, raw)
        if target in (None, "pib_subnacional"):
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "pib_subnacional"
            raw = self._first_raw_file(raw_dir)
            outputs["pib_subnacional"] = clean_pib_subnacional(self.cfg, raw)
        if target in (None, "crosswalk") and self.cfg.flags.enable_geo:
            dim_ubigeo_path = resolve_path(self.cfg.paths.geo_dir) / "dim_ubigeo.parquet"
            outputs["crosswalk"] = build_crosswalk(self.cfg, dim_ubigeo_path)
        if target in (None, "pib_armonizado") and self.cfg.flags.enable_geo:
            staging = resolve_path(self.cfg.paths.staging_dir) / "pib_subnacional.parquet"
            crosswalk = resolve_path(self.cfg.paths.geo_dir) / "crosswalk_ubigeo.parquet"
            outputs["pib_armonizado"] = harmonize_panel(
                self.cfg,
                staging,
                crosswalk,
                value_cols=["pib"],
                output_name="pib_armonizado.parquet",
            )
        if target in (None, "indicadores_core"):
            pib_path = resolve_path(self.cfg.paths.processed_dir) / "pib_armonizado.parquet"
            pob_path = resolve_path(self.cfg.paths.processed_dir) / "poblacion.parquet"
            dim_path = resolve_path(self.cfg.paths.geo_dir) / "dim_territorio_base.parquet"
            outputs["indicadores_core"] = build_indicadores_core(self.cfg, pib_path, pob_path, dim_path)
        if target in (None, "ntl") and self.cfg.flags.enable_ntl:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "ntl"
            raw = self._first_raw_file(raw_dir)
            outputs["ntl"] = build_ntl(self.cfg, raw)
        if target in (None, "transporte") and self.cfg.flags.enable_transporte:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "transporte"
            raw = raw_dir if raw_dir.exists() else self._first_raw_file(raw_dir)
            outputs["transporte"] = build_transporte(self.cfg, raw)
        if target in (None, "mineria") and self.cfg.flags.enable_mineria:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "mineria"
            raw = self._first_raw_file(raw_dir)
            outputs["mineria"] = build_mineria(self.cfg, raw)
        if target in (None, "bosques") and self.cfg.flags.enable_bosques:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "bosques"
            raw = self._first_raw_file(raw_dir)
            outputs["bosques"] = build_bosques(self.cfg, raw)
        if target in (None, "clima") and self.cfg.flags.enable_clima:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "clima"
            raw = self._first_raw_file(raw_dir)
            outputs["clima"] = build_clima(self.cfg, raw)
        if target in (None, "shocks_macro") and self.cfg.flags.enable_shocks_macro:
            raw_dir = resolve_path(self.cfg.paths.raw_dir) / "bcrp"
            raw = self._first_raw_file(raw_dir)
            outputs["shocks_macro"] = build_shocks_macro(self.cfg, raw)
        if target in (None, "panel_analitico"):
            indicadores = resolve_path(self.cfg.paths.processed_dir) / "indicadores_core.parquet"
            ntl = resolve_path(self.cfg.paths.processed_dir) / "ntl_distrito_anual.parquet"
            transporte = resolve_path(self.cfg.paths.processed_dir) / "features_transporte.parquet"
            mineria = resolve_path(self.cfg.paths.processed_dir) / "features_mineria.parquet"
            bosques = resolve_path(self.cfg.paths.processed_dir) / "features_bosques.parquet"
            clima = resolve_path(self.cfg.paths.processed_dir) / "shock_clima.parquet"
            shocks_macro = resolve_path(self.cfg.paths.processed_dir) / "shock_macro.parquet"
            outputs["panel_analitico"] = build_panel(
                self.cfg,
                indicadores,
                ntl_path=ntl if self.cfg.flags.enable_ntl else None,
                transporte_path=transporte if self.cfg.flags.enable_transporte else None,
                mineria_path=mineria if self.cfg.flags.enable_mineria else None,
                bosques_path=bosques if self.cfg.flags.enable_bosques else None,
                clima_path=clima if self.cfg.flags.enable_clima else None,
                shock_macro_path=shocks_macro if self.cfg.flags.enable_shocks_macro else None,
            )
        return outputs

    def model(self, target: str | None = None) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        panel = resolve_path(self.cfg.paths.processed_dir) / "panel_analitico.parquet"
        limites = resolve_path(self.cfg.paths.geo_dir) / "dim_territorio_base.gpkg"
        if target in (None, "concentracion"):
            outputs["concentracion"] = build_concentracion(self.cfg, panel)
        if target in (None, "contribucion"):
            outputs["contribucion"] = build_contribucion_crecimiento(self.cfg, panel)
        if target in (None, "espacial"):
            outputs["lisa"] = build_lisa(self.cfg, panel, limites)
        if target in (None, "clusters"):
            outputs["modalidades"] = build_modalidades(self.cfg, panel)
        if target in (None, "panel_shocks"):
            outputs["panel_shocks"] = build_panel_shocks(self.cfg, panel)
        return outputs

    def policy(self, target: str | None = None) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        panel = resolve_path(self.cfg.paths.processed_dir) / "panel_analitico.parquet"
        indice = resolve_path(self.cfg.paths.processed_dir) / "indice_vulnerabilidad.parquet"
        lisa = resolve_path(self.cfg.paths.processed_dir) / "lisa_resultados.parquet"

        if target in (None, "indice"):
            outputs["indice"] = build_indice_vulnerabilidad(self.cfg, panel)
        if target in (None, "sensibilidad"):
            outputs["sensibilidad"] = build_sensibilidad(self.cfg, indice)
        if target in (None, "escenarios"):
            outputs["escenario_a"] = simular_escenario(
                self.cfg,
                panel,
                indice,
                lisa if lisa.exists() else None,
                escenario_id="A",
                regla="bottom_tail",
                parametros={"impacto": 0.01},
                horizonte=5,
            )
            outputs["escenario_b"] = simular_escenario(
                self.cfg,
                panel,
                indice,
                lisa if lisa.exists() else None,
                escenario_id="B",
                regla="persistencia",
                parametros={"impacto": 0.015},
                horizonte=5,
            )
        if target in (None, "evaluacion"):
            if "escenario_a" in outputs:
                outputs["eval_a"] = evaluar_cobertura(self.cfg, outputs["escenario_a"])
            if "escenario_b" in outputs:
                outputs["eval_b"] = evaluar_cobertura(self.cfg, outputs["escenario_b"])
        return outputs

    def render(self, target: str | None = None) -> dict[str, Path]:
        outputs: dict[str, Path] = {}
        panel = resolve_path(self.cfg.paths.processed_dir) / "panel_analitico.parquet"
        indicadores = resolve_path(self.cfg.paths.processed_dir) / "indicadores_core.parquet"
        limites = resolve_path(self.cfg.paths.geo_dir) / "dim_territorio_base.gpkg"

        if target in (None, "plot"):
            from .figuras.plots import plot_concentracion

            outputs["plot_pib"] = plot_concentracion(self.cfg, panel)
        if target in (None, "mapa"):
            from .figuras.mapas import plot_mapa_indicador

            outputs["mapa_pib_pc"] = plot_mapa_indicador(self.cfg, limites, indicadores, "pib_pc")
        return outputs

    def paper(self) -> list[Path]:
        panel = resolve_path(self.cfg.paths.processed_dir) / "panel_analitico.parquet"
        return build_paper(self.cfg, panel)

    def run_all(self) -> None:
        self.validate_config()
        self.ingest()
        self.build()
        if self.cfg.flags.enable_models:
            self.model()
        if self.cfg.flags.enable_policy:
            self.policy()
        if self.cfg.flags.enable_figures:
            self.render()
        if self.cfg.flags.enable_paper:
            self.paper()
