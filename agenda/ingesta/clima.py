from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_CLIMA_URLS = [
    "https://www.cpc.ncep.noaa.gov/data/indices/oni.ascii.txt",
]


def ingest_clima(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "clima")
    dest = ingest_from_source(cfg.ingest.clima, raw_dir, default_urls=DEFAULT_CLIMA_URLS)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.clima.version,
        checksum=hash_file(dest),
        notes="clima raw",
    )
    logger.info("clima ingested to %s", dest)
    return dest
