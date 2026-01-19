# agenda_2026

Reproducible pipeline for subnational economic activity, spatial metrics,
vulnerability index, and policy scenarios. The pipeline runs **only** on real
data sources provided in `config.yaml` or downloaded via ingestion.

Quickstart
```
python -m run all --config config.yaml
```

Note: The default config currently targets 2007–2018 due to INEI PBI coverage.
Extend earlier years by providing the CIUP/UP district GDP dataset or another
subnational GDP source.

Before running, set real data sources in `config.yaml` under `ingest.*.path`
or enable `ingest.*.auto` with URLs where supported. Current defaults use:
- INEI PBI departamental (fallback proxy for district GDP)
- WorldPop 2000–2020 population (admin2, allocated to districts by area)
- MTC 2019 road network (density, replicated across years)
- MINAM GeoBosques district deforestation dataset (2001+)
- MINEM directorio de empresas mineras (2018, replicated across years)
- BCRP PBI growth series (PM04863AA) as macro shock
- NOAA ONI climate index (annual mean, replicated across districts)

Common commands
- `python -m run validate-config --config config.yaml`
- `python -m run ingest`
- `python -m run build`
- `python -m run model`
- `python -m run policy`
- `python -m run render`
- `python -m run paper`

Inputs and outputs
- Raw inputs: `data/raw`
- Processed outputs: `data/processed`
- Figures and tables: `outputs/figures`, `outputs/tables`
- Policy outputs: `outputs/policy`
- PDFs: `dist`

Dependencies
- Minimal: `requirements-core.txt`
- Full (adds LISA and schema validation): `requirements-full.txt`

Notebooks
- Jupytext paired notebooks live in `notebooks/` as `.py` and `.ipynb`.
- Sync after edits: `./.venv/bin/jupytext --sync notebooks/*.py`
