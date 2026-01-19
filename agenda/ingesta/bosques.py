from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_BOSQUES_URLS = [
    "https://www.datosabiertos.gob.pe/sites/default/files/3a%20Dataset%20Bosques_V2.0.csv",
]


def ingest_bosques(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "bosques")
    dest = ingest_from_source(cfg.ingest.bosques, raw_dir, default_urls=DEFAULT_BOSQUES_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.bosques.version,
        checksum=hash_file(dest),
        notes="bosques raw",
    )
    logger.info("bosques ingested to %s", dest)
    return dest
