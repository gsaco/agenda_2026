from __future__ import annotations

import logging
import subprocess
from datetime import datetime
from pathlib import Path

import pandas as pd

from ..config import Config
from ..manifest import register_artifact
from ..utils.hashing import hash_file
from ..utils.io import read_parquet
from ..utils.paths import ensure_dir, resolve_path

logger = logging.getLogger(__name__)


def _render_template(template_path: Path, out_path: Path, context: dict[str, str]) -> None:
    text = template_path.read_text(encoding="utf-8")
    out_path.write_text(text.format(**context), encoding="utf-8")


def _run_pandoc_pdf(src: Path, dest: Path) -> None:
    cmd = ["pandoc", str(src), "-o", str(dest), "--pdf-engine=xelatex"]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def _run_pandoc_html(src: Path, dest: Path) -> None:
    cmd = ["pandoc", str(src), "-o", str(dest)]
    subprocess.run(cmd, check=True, capture_output=True, text=True)


def build_paper(cfg: Config, panel_path: Path) -> list[Path]:
    dist_dir = ensure_dir(Path(cfg.paths.dist_dir))
    template_dir = resolve_path("agenda/paper/templates")

    context = {
        "build_date": datetime.utcnow().strftime("%Y-%m-%d"),
        "year_start": str(cfg.project.years.start),
        "year_end": str(cfg.project.years.end),
        "base_year": str(cfg.project.base_year),
        "mode": cfg.project.mode,
        "manifest_path": str(resolve_path(cfg.paths.manifest)),
    }

    paper_md = dist_dir / "paper.md"
    appendix_md = dist_dir / "appendix.md"
    data_appendix_md = dist_dir / "data_appendix.md"

    _render_template(template_dir / "paper.md", paper_md, context)
    _render_template(template_dir / "appendix.md", appendix_md, context)
    _render_template(template_dir / "data_appendix.md", data_appendix_md, context)

    # Variable dictionary
    df = read_parquet(panel_path)
    dict_path = dist_dir / "diccionario_variables.csv"
    pd.DataFrame({"variable": sorted(df.columns)}).to_csv(dict_path, index=False)

    # Compile PDFs, fallback to HTML if PDF engine fails.
    paper_pdf = dist_dir / "paper.pdf"
    appendix_pdf = dist_dir / "appendix.pdf"
    data_appendix_pdf = dist_dir / "data_appendix.pdf"

    pdf_outputs = []
    try:
        _run_pandoc_pdf(paper_md, paper_pdf)
        _run_pandoc_pdf(appendix_md, appendix_pdf)
        _run_pandoc_pdf(data_appendix_md, data_appendix_pdf)
        pdf_outputs = [paper_pdf, appendix_pdf, data_appendix_pdf]
    except subprocess.CalledProcessError as exc:
        logger.warning("pdf build failed, falling back to html: %s", exc)
        _run_pandoc_html(paper_md, dist_dir / "paper.html")
        _run_pandoc_html(appendix_md, dist_dir / "appendix.html")
        _run_pandoc_html(data_appendix_md, dist_dir / "data_appendix.html")

    for path in [dict_path] + pdf_outputs:
        register_artifact(
            cfg.paths.manifest,
            path,
            source="paper_build",
            version=None,
            checksum=hash_file(path),
            notes="paper outputs",
        )

    logger.info("paper outputs saved to %s", dist_dir)
    return pdf_outputs + [dict_path]
