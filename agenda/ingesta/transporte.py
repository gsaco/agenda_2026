from __future__ import annotations

import logging
from pathlib import Path

from ..config import Config
from urllib.parse import urlparse

from ..ingesta.common import ingest_from_source
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.downloads import download_file
from ..utils.paths import ensure_dir

logger = logging.getLogger(__name__)

DEFAULT_TRANSPORTE_URLS = [
    "https://portal.mtc.gob.pe/transportes/caminos/normas_carreteras/Info_espacial/2019/RVN_Eje.zip",
    "https://portal.mtc.gob.pe/transportes/caminos/normas_carreteras/Info_espacial/2019/RVD_Eje.zip",
    "https://portal.mtc.gob.pe/transportes/caminos/normas_carreteras/Info_espacial/2019/RVV_Eje.zip",
]


def ingest_transporte(cfg: Config) -> Path:
    raw_dir = ensure_dir(Path(cfg.paths.raw_dir) / "transporte")
    if cfg.ingest.transporte.path or cfg.ingest.transporte.url:
        dest = ingest_from_source(cfg.ingest.transporte, raw_dir)
        register_artifact(
            cfg.paths.manifest,
            dest,
            source=str(dest),
            version=cfg.ingest.transporte.version,
            checksum=hash_file(dest),
            notes="transporte raw",
        )
        logger.info("transporte ingested to %s", dest)
        return dest

    if cfg.ingest.transporte.auto:
        downloaded = []
        for url in DEFAULT_TRANSPORTE_URLS:
            filename = Path(urlparse(url).path).name
            dest = raw_dir / filename
            download_file(url, dest)
            register_artifact(
                cfg.paths.manifest,
                dest,
                source=str(dest),
                version=cfg.ingest.transporte.version,
                checksum=hash_file(dest),
                notes="transporte raw",
            )
            downloaded.append(dest)
        logger.info("transporte ingested %d files", len(downloaded))
        return raw_dir

    raise ValueError("transporte ingestion requires ingest.path, ingest.url or auto=true")
