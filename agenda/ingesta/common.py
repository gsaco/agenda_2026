from __future__ import annotations

import shutil
from pathlib import Path
from typing import Iterable

from ..config import IngestSourceConfig
from ..utils.downloads import download_with_fallbacks
from ..utils.paths import ensure_dir, resolve_path


def ingest_from_source(
    source: IngestSourceConfig,
    raw_dir: Path,
    default_urls: Iterable[str] | None = None,
) -> Path:
    raw_dir = ensure_dir(raw_dir)

    if source.path:
        src = resolve_path(source.path)
        if not src.exists():
            raise FileNotFoundError(f"source not found: {src}")
        dest = raw_dir / src.name
        if src.resolve() != dest.resolve():
            shutil.copy2(src, dest)
        return dest

    urls = []
    if source.url:
        urls.append(source.url)
    if source.fallback_urls:
        urls.extend(source.fallback_urls)
    if default_urls:
        urls.extend(list(default_urls))

    if source.auto and urls:
        return download_with_fallbacks(urls, raw_dir)

    raise ValueError("missing data source: set ingest.path or ingest.url with auto=true")
