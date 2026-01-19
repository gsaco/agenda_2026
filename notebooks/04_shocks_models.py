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
# # 04 Shocks and models
# This notebook inspects shock series and regression outputs.
# We only run model estimation if outputs are missing.
#
# Shock notes:
# - Macro shock uses BCRP annual PBI growth (PM04863AA) as a proxy.
# - Climate shock uses NOAA ONI (national index) replicated across districts
#   when CHIRPS is not used.

# %%
from pathlib import Path
import subprocess
import pandas as pd
import matplotlib.pyplot as plt

ROOT = Path("..").resolve()
CONFIG = ROOT / "config.yaml"
PYTHON = ROOT / ".venv" / "bin" / "python"
AUTO_RUN = False

try:
    from IPython import get_ipython

    _IN_NOTEBOOK = get_ipython() is not None
except Exception:
    _IN_NOTEBOOK = False

if not _IN_NOTEBOOK:
    import matplotlib

    matplotlib.use("Agg")


def run_cmd(args):
    subprocess.run(args, check=True, cwd=ROOT)


def ensure_artifacts(paths, auto_run=True):
    missing = [p for p in paths if not p.exists()]
    if missing and auto_run:
        run_cmd([str(PYTHON), "-m", "run", "model", "panel_shocks", "--config", str(CONFIG)])
    return missing


shock_macro = ROOT / "data" / "processed" / "shock_macro.parquet"
shock_clima = ROOT / "data" / "processed" / "shock_clima.parquet"
reg_path = ROOT / "outputs" / "tables" / "regresiones_principales.csv"
ensure_artifacts([shock_macro, shock_clima, reg_path], auto_run=AUTO_RUN)

# %% [markdown]
# ## Macro shock series

# %%
macro = pd.read_parquet(shock_macro)
macro.head()

# %%
plt.plot(macro["anio"], macro["dlog_tot"])
plt.title("dlog_tot")
plt.xlabel("anio")
plt.show()

# %% [markdown]
# ## Climate shock series

# %%
clima = pd.read_parquet(shock_clima)
clima.head()

# %% [markdown]
# ## Panel regression results
# Coefficients represent association between shocks and territorial growth.

# %%
reg = pd.read_csv(reg_path)
reg
