from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path

from .paths import ensure_dir, resolve_path


def init_logging(logs_dir: str | Path, name: str = "agenda") -> Path:
    log_dir = ensure_dir(logs_dir)
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_path = resolve_path(log_dir / f"run_{ts}.log")

    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(fmt)
    logger.addHandler(file_handler)

    console = logging.StreamHandler()
    console.setFormatter(fmt)
    logger.addHandler(console)

    logging.getLogger(name).info("logging started")
    return log_path
