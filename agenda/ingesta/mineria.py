from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_MINERIA_URLS = [
    "https://www.datosabiertos.gob.pe/sites/default/files/1_Directorio_de_Empresas_Mineras.xlsx",
]


def ingest_mineria(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "mineria")
    dest = ingest_from_source(cfg.ingest.mineria, raw_dir, default_urls=DEFAULT_MINERIA_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.mineria.version,
        checksum=hash_file(dest),
        notes="mineria raw",
    )
    logger.info("mineria ingested to %s", dest)
    return dest
