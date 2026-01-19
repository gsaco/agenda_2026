from __future__ import annotations

import logging
from pathlib import Path

import json
import requests

from ..config import Config
from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.paths import ensure_dir, resolve_path

logger = logging.getLogger(__name__)


DEFAULT_BCRP_SERIES = "PM04863AA"


def _download_bcrp_series(series_code: str, dest: Path) -> Path:
    url = f"https://estadisticas.bcrp.gob.pe/estadisticas/series/api/{series_code}/json"
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    dest.write_text(resp.text, encoding="utf-8")
    return dest


def ingest_bcrp(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "bcrp")
    dest = raw_dir / "bcrp_tot.json"

    if cfg.ingest.bcrp.path:
        source_path = resolve_path(cfg.ingest.bcrp.path)
        if not source_path.exists():
            raise FileNotFoundError(f"bcrp source not found: {source_path}")
        dest = ingest_from_source(cfg.ingest.bcrp, raw_dir)
    elif cfg.ingest.bcrp.auto:
        if cfg.ingest.bcrp.url:
            dest = ingest_from_source(cfg.ingest.bcrp, raw_dir)
        else:
            dest = _download_bcrp_series(DEFAULT_BCRP_SERIES, dest)
    else:
        raise ValueError("bcrp ingestion requires ingest.bcrp.path or auto=true")

    register_artifact(
        cfg.paths.manifest,
        dest,
        source=str(dest),
        version=cfg.ingest.bcrp.version,
        checksum=hash_file(dest),
        notes="bcrp terms of trade series",
    )
    logger.info("bcrp ingested to %s", dest)
    return dest
