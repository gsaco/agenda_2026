from __future__ import annotations

import logging
from pathlib import Path
from typing import Iterable
from urllib.parse import urlparse

import requests

from .hashing import hash_file
from .paths import ensure_dir

logger = logging.getLogger(__name__)


def _filename_from_url(url: str) -> str:
    parsed = urlparse(url)
    name = Path(parsed.path).name
    return name or "download.bin"


def download_file(url: str, dest: Path, timeout: int = 60) -> Path:
    dest.parent.mkdir(parents=True, exist_ok=True)
    with requests.get(url, stream=True, timeout=timeout) as r:
        r.raise_for_status()
        with dest.open("wb") as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
    logger.info("downloaded %s to %s", url, dest)
    return dest


def download_with_fallbacks(urls: Iterable[str], dest_dir: Path) -> Path:
    errors = []
    for url in urls:
        try:
            filename = _filename_from_url(url)
            dest = dest_dir / filename
            return download_file(url, dest)
        except Exception as exc:  # pragma: no cover - network dependent
            logger.warning("download failed for %s: %s", url, exc)
            errors.append(str(exc))
    raise RuntimeError(f"all download attempts failed: {errors}")
