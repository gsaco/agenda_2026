# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.19.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # 01 Ingestion and cleaning
# This notebook inspects raw and staging datasets and explains the
# cleaning steps and QC outputs. It uses existing data when available
# to avoid unnecessary recomputation.
#
# Sources used in the default config:
# - INEI PBI departamental (allocated to districts via population shares)
# - WorldPop admin2 population (allocated to districts by area within province)
# - UBIGEO (INEI) and district boundaries (Datos Abiertos)

# %%
from pathlib import Path
import subprocess
import json
import pandas as pd

ROOT = Path("..").resolve()
CONFIG = ROOT / "config.yaml"
PYTHON = ROOT / ".venv" / "bin" / "python"
AUTO_RUN = False


def run_cmd(args):
    subprocess.run(args, check=True, cwd=ROOT)


def ensure_artifacts(paths, auto_run=True):
    missing = [p for p in paths if not p.exists()]
    if missing and auto_run:
        run_cmd([str(PYTHON), "-m", "run", "all", "--config", str(CONFIG)])
    return missing


def first_file(folder: Path) -> Path | None:
    candidates = sorted(folder.glob("*"))
    return candidates[0] if candidates else None


raw_pib = first_file(ROOT / "data" / "raw" / "pib_subnacional")
raw_ubigeo = first_file(ROOT / "data" / "raw" / "ubigeo")
staging_pib = ROOT / "data" / "staging" / "pib_subnacional.parquet"
staging_ubigeo = ROOT / "data" / "staging" / "ubigeo.parquet"

paths = [p for p in [raw_pib, raw_ubigeo, staging_pib, staging_ubigeo] if p is not None]
ensure_artifacts(paths, auto_run=AUTO_RUN)

# %% [markdown]
# ## Raw inputs
# Raw data should remain immutable and only mirror source content.

# %%
if raw_pib is None:
    print("No raw PIB file found")
else:
    if raw_pib.suffix.lower() in [".xlsx", ".xls"]:
        raw_pib_df = pd.read_excel(raw_pib)
    else:
        raw_pib_df = pd.read_csv(raw_pib)
    raw_pib_df.head()

# %%
if raw_ubigeo is None:
    print("No raw UBIGEO file found")
else:
    if raw_ubigeo.suffix.lower() in [".xlsx", ".xls"]:
        raw_ubigeo_df = pd.read_excel(raw_ubigeo)
    else:
        raw_ubigeo_df = pd.read_csv(raw_ubigeo)
    raw_ubigeo_df.head()

# %% [markdown]
# ## Cleaned staging outputs
# Staging datasets are standardized (types, columns, names) and used downstream.

# %%
if staging_pib.exists():
    staging_pib_df = pd.read_parquet(staging_pib)
    staging_pib_df.head()
else:
    print("Staging PIB not found")

# %%
if staging_ubigeo.exists():
    staging_ubigeo_df = pd.read_parquet(staging_ubigeo)
    staging_ubigeo_df.head()
else:
    print("Staging UBIGEO not found")

# %% [markdown]
# ## QC reports
# QC JSON files track missingness and key uniqueness checks.

# %%
qc_dir = ROOT / "outputs" / "qc"
qc_files = sorted(p.name for p in qc_dir.glob("qc_*.json"))
qc_files

# %%
qc_pib = json.loads((qc_dir / "qc_pib_subnacional.json").read_text())
qc_pib
