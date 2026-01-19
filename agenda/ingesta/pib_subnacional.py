from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_PIB_URLS = [
    "https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/pbi_peru_15.xlsx",
]


def ingest_pib_subnacional(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "pib_subnacional")
    dest = ingest_from_source(cfg.ingest.pib_subnacional, raw_dir, default_urls=DEFAULT_PIB_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.pib_subnacional.version,
        checksum=hash_file(dest),
        notes="pib subnacional raw",
    )
    logger.info("pib_subnacional ingested to %s", dest)
    return dest
