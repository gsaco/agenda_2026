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
# # 03 Spatial analysis
# This notebook reviews spatial clustering outputs: LISA and k-means
# modalities. It also shows a map of a key indicator.
#
# Note: LISA can be expensive on large datasets. We only run the spatial
# model if results are missing.

# %%
from pathlib import Path
import subprocess
import pandas as pd
import geopandas as gpd
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
        run_cmd([str(PYTHON), "-m", "run", "model", "espacial", "--config", str(CONFIG)])
        run_cmd([str(PYTHON), "-m", "run", "model", "clusters", "--config", str(CONFIG)])
    return missing


lisa_path = ROOT / "data" / "processed" / "lisa_resultados.parquet"
modal_path = ROOT / "data" / "processed" / "modalidades_kmeans.parquet"
ensure_artifacts([lisa_path, modal_path], auto_run=AUTO_RUN)

# %% [markdown]
# ## LISA results
# Cluster labels follow the standard local Moran quadrants (1..4).

# %%
lisa = pd.read_parquet(lisa_path)
lisa.head()

# %%
lisa.groupby(["indicador", "cluster"]).size().reset_index(name="n").head()

# %% [markdown]
# ## Modalities (k-means)
# These are non-spatial typologies based on standardized indicators.

# %%
modal = pd.read_parquet(modal_path)
modal.head()

# %% [markdown]
# ## Map example
# This uses the latest year of PIB per capita.

# %%
limites = gpd.read_file(ROOT / "data" / "geo" / "dim_territorio_base.gpkg")
ind = pd.read_parquet(ROOT / "data" / "processed" / "indicadores_core.parquet")
year = int(ind["anio"].max())
ind_y = ind[ind["anio"] == year][["ubigeo", "pib_pc"]]
merged = limites.merge(ind_y, on="ubigeo", how="left")

ax = merged.plot(column="pib_pc", legend=True, figsize=(5, 5))
ax.set_axis_off()
plt.title(f"PIB per capita {year}")
