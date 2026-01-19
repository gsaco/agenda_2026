from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


DEFAULT_UBIGEO_URLS = [
    "https://www.datosabiertos.gob.pe/sites/default/files/UBIGEO%202022_1891%20distritos.xlsx",
    "https://www.datosabiertos.gob.pe/sites/default/files/Data_Muestra_ubigeos.csv",
]


def ingest_ubigeo(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "ubigeo")
    dest = ingest_from_source(cfg.ingest.ubigeo, raw_dir, default_urls=DEFAULT_UBIGEO_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.ubigeo.version,
        checksum=hash_file(dest),
        notes="ubigeo raw",
    )
    logger.info("ubigeo ingested to %s", dest)
    return dest
