from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def ingest_limites(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "limites")
    default_urls = [
        "https://www.datosabiertos.gob.pe/sites/default/files/DISTRITOS_LIMITES.zip",
        "https://www.datosabiertos.gob.pe/sites/default/files/DEPARTAMENTOS_LIMITES.zip",
    ]
    dest = ingest_from_source(cfg.ingest.limites, raw_dir, default_urls=default_urls)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.limites.version,
        checksum=hash_file(dest),
        notes="limites raw",
    )
    logger.info("limites ingested to %s", dest)
    return dest
