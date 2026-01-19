from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)


def ingest_ntl(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "ntl")
    dest = ingest_from_source(cfg.ingest.ntl, raw_dir)
    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.ntl.version,
        checksum=hash_file(dest),
        notes="ntl raw",
    )
    logger.info("ntl ingested to %s", dest)
    return dest
