from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_POBLACION_URLS = [
    "https://data.worldpop.org/GIS/Population/Global_2000_2020/PopTablesSum/PER/PER_2000_20_L2_Pop_WPGP.csv",
]


def ingest_poblacion(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "poblacion")
    dest = ingest_from_source(cfg.ingest.poblacion, raw_dir, default_urls=DEFAULT_POBLACION_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.poblacion.version,
        checksum=hash_file(dest),
        notes="poblacion raw",
    )
    logger.info("poblacion ingested to %s", dest)
    return dest
