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
# # 00 Setup and pipeline overview
# This notebook explains the project layout, configuration, and how the
# reproducible pipeline is executed with **real data**. It checks if the main
# processed artifacts exist and only runs the pipeline if you explicitly allow it.
#
# Key goals:
# - Confirm paths and config.
# - Load the manifest for traceability.
# - Ensure the main panel exists before analysis.
#
# Notes:
# - Set `AUTO_RUN = True` if you want the notebook to trigger `run all`.
# - The pipeline will refuse demo data and will fail early if required inputs
#   are missing from `config.yaml`.
# - Default sources: INEI PBI departamental (allocated to districts), WorldPop
#   population, MTC 2019 roads, MINAM deforestation, MINEM mining directory,
#   NOAA ONI climate index. NTL is disabled by default.

# %%
from pathlib import Path
import json
import subprocess

ROOT = Path("..").resolve()
CONFIG = ROOT / "config.yaml"
PYTHON = ROOT / ".venv" / "bin" / "python"
AUTO_RUN = False


def run_cmd(args):
    subprocess.run(args, check=True, cwd=ROOT)


def ensure_artifacts(paths, auto_run=True):
    missing = [p for p in paths if not p.exists()]
    if missing:
        print("Missing artifacts:")
        for p in missing:
            print(" -", p)
        if auto_run:
            print("Running full pipeline to build artifacts...")
            run_cmd([str(PYTHON), "-m", "run", "all", "--config", str(CONFIG)])
        else:
            print("AUTO_RUN disabled. Please run the pipeline manually.")
    else:
        print("All required artifacts are present.")


panel_path = ROOT / "data" / "processed" / "panel_analitico.parquet"
ensure_artifacts([panel_path], auto_run=AUTO_RUN)

# %% [markdown]
# ## Data availability check
# This lists which raw inputs are present. If anything is missing, the pipeline
# will not be able to run end to end.

# %%
from pathlib import Path

raw_root = ROOT / "data" / "raw"
expected = {
    "pib_subnacional": raw_root / "pib_subnacional",
    "ubigeo": raw_root / "ubigeo",
    "limites": raw_root / "limites",
    "poblacion": raw_root / "poblacion",
    "ntl": raw_root / "ntl",
    "transporte": raw_root / "transporte",
    "mineria": raw_root / "mineria",
    "bosques": raw_root / "bosques",
    "clima": raw_root / "clima",
    "bcrp": raw_root / "bcrp",
}

status = []
for name, folder in expected.items():
    present = folder.exists() and any(folder.iterdir())
    status.append({"source": name, "present": present, "path": str(folder)})

import pandas as pd

pd.DataFrame(status)

# %% [markdown]
# ## Repository layout
# - data/raw: immutable inputs
# - data/staging: cleaned source tables
# - data/processed: harmonized and analytical outputs
# - outputs: figures, tables, and policy outputs
# - dist: paper/appendix outputs

# %%
sorted([p.name for p in (ROOT / "data").iterdir()])

# %% [markdown]
# ## Config summary
# The config controls years, mode (demo/real), and which modules are enabled.

# %%
import yaml

config = yaml.safe_load(CONFIG.read_text())
config

# %% [markdown]
# ## Manifest
# The manifest lists artifacts with hashes for reproducibility.

# %%
manifest_path = ROOT / "data" / "manifest.json"
manifest = json.loads(manifest_path.read_text()) if manifest_path.exists() else []
manifest[:5]

# %% [markdown]
# ## Quick check of main panel
# This is the key dataset used throughout the analysis.

# %%
import pandas as pd

panel = pd.read_parquet(panel_path)
panel.head()
