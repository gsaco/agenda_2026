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
# # 02 EDA and core indicators
# This notebook covers descriptive statistics, distributions, and
# concentration metrics for PIB, PIB per capita, PIB per km2, and IAE.
# It uses existing processed artifacts to avoid long compute time.
# Data coverage matches `config.yaml` (default 2007–2018).

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
        run_cmd([str(PYTHON), "-m", "run", "all", "--config", str(CONFIG)])
    return missing


indicadores_path = ROOT / "data" / "processed" / "indicadores_core.parquet"
concentracion_path = ROOT / "outputs" / "tables" / "concentracion_topshares.csv"
ensure_artifacts([indicadores_path, concentracion_path], auto_run=AUTO_RUN)

# %% [markdown]
# ## Load indicators

# %%
ind = pd.read_parquet(indicadores_path)
ind.head()

# %% [markdown]
# ## Latest year summaries
# The latest year is a convenient snapshot for ranking and distribution analysis.

# %%
year = int(ind["anio"].max())
ind_y = ind[ind["anio"] == year].copy()
ind_y[["ubigeo", "pib_pc", "pib_km2", "iae"]].head()

# %% [markdown]
# ### Top and bottom territories by PIB per capita
# These rankings are used later for tail analysis and policy targeting.

# %%
ind_y.nlargest(10, "pib_pc")[["ubigeo", "pib_pc"]]

# %%
ind_y.nsmallest(10, "pib_pc")[["ubigeo", "pib_pc"]]

# %% [markdown]
# ## Distributions
# Histograms give a quick view of inequality and heavy tails.

# %%
plt.hist(ind_y["pib_pc"], bins=20)
plt.title("PIB per capita distribution")
plt.xlabel("pib_pc")
plt.show()

# %% [markdown]
# ## Concentration shares
# These are the shares of total PIB captured by top percentiles.

# %%
conc = pd.read_csv(concentracion_path)
conc.head()

# %% [markdown]
# ## PIB growth summary
# A quick time series of total PIB across years.

# %%
series = ind.groupby("anio")["pib"].sum().reset_index()
plt.plot(series["anio"], series["pib"], marker="o")
plt.title("Total PIB by year")
plt.xlabel("anio")
plt.ylabel("pib")
plt.show()
