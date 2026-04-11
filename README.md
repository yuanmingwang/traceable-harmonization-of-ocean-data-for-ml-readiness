# Traceable Harmonization of Ocean Data for ML Readiness

This project is designed for the capstone on **Traceable Harmonization of Ocean Data for ML Readiness**.

It is built around four ideas from the brief:

1. **Raw NetCDF files stay immutable**.
2. **Interpretation is separated from ingestion** through YAML config files.
3. **Traceability is first-class** through per-source manifests, request snapshots, and file hashes.
4. **The final artifact is an ML-ready Zarr store** plus provenance outputs.

## Suggested workflow

1. Put the ERDDAP catalog file at `data/catalog/allDataset.csv`.
2. Adjust `configs/pipeline.yaml`.
3. Build a download plan.
4. Download `.ncCFMA` files plus `.ncCFMAHeader` text files.
   The downloader resolves variables per dataset from ERDDAP `.dds` first,
   so it does not assume every profile dataset exposes the same columns.
5. Generate source manifests.
6. Harmonize profile datasets into a Zarr store.

## Important note about ERDDAP CSV exports

ERDDAP `.csv` downloads include:
- line 1 = column names
- line 2 = units
- remaining lines = data rows

So `allDataset.csv` should not be read as a completely ordinary CSV without handling the units row.

## Example commands

```bash
python scripts/01_build_download_plan.py   --config configs/pipeline.yaml

python scripts/02_download_raw.py   --config configs/pipeline.yaml

python scripts/03_manifest_sources.py   --config configs/pipeline.yaml

python scripts/04_harmonize_profiles_to_zarr.py   --config configs/pipeline.yaml
```
