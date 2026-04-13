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

## Traceability Requirement

The capstone requirement says:

> the system should make it possible to trace the lineage of a data point or derived variable back to its original source file and transformation steps, ensuring transparency and auditability of the processing pipeline

The current pipeline now satisfies that requirement for the harmonized Zarr output produced by `scripts/04_harmonize_profiles_to_zarr.py`.

It does this in three layers:

1. Raw-source provenance
   Each downloaded dataset keeps:
   - the raw NetCDF file
   - the corresponding `.ncCFMAHeader` text file
   - the original ERDDAP request in `request.json`
   - a per-source manifest with file hash and source metadata

2. Observation-level lineage
   Each harmonized Zarr row carries stable lineage columns:
   - `lineage_record_id`
   - `source_dataset_id`
   - `source_profile_id`
   - `source_file`
   - `source_sha256`
   - `source_profile_index`
   - `source_obs_index`
   - `dataset_id`

   This means a single record in the Zarr output can be traced back to:
   - the exact raw file on disk
   - the exact profile within that file
   - the exact observation index within that profile

3. Variable-level lineage
   The pipeline also writes `data/manifests/variable_lineage.json`, which records for each canonical output variable:
   - the raw source variable name
   - the canonical output variable name
   - the declared canonical unit from `configs/variable_map.yaml`
   - the transformation steps applied

   In the current version of the project, the transformation steps are intentionally simple and explicit:
   - extract the raw variable from the downloaded `.ncCFMA` file
   - flatten `Profile` data from `(profile, obs)` to one observation row per record
   - rename the raw variable to the canonical name defined in `configs/variable_map.yaml`

## Provenance Outputs

After running the full pipeline, the main provenance artifacts are:

- `data/raw/onc_erddap/<dataset_id>/source.nc`
- `data/raw/onc_erddap/<dataset_id>/source.header.txt`
- `data/raw/onc_erddap/<dataset_id>/request.json`
- `data/manifests/<dataset_id>.manifest.json`
- `data/manifests/source_manifest_index.csv`
- `data/manifests/lineage.csv`
- `data/manifests/observation_lineage.csv`
- `data/manifests/variable_lineage.json`
- `data/manifests/latest_harmonization_run.json`

## How To Trace A Data Point

Suppose you inspect one value in the Zarr store, for example a value in `turbidity_raw`.

1. Read the corresponding row's `lineage_record_id` from the Zarr store.
2. Look up that `lineage_record_id` in `data/manifests/observation_lineage.csv`.
3. From that row, get:
   - `source_file`
   - `source_sha256`
   - `source_profile_id`
   - `source_profile_index`
   - `source_obs_index`
4. Open the matching raw file and header:
   - `source.nc`
   - `source.header.txt`
5. Open `data/manifests/variable_lineage.json` and inspect the entry for the canonical variable you care about, for example `turbidity_raw`.
6. That entry tells you:
   - which raw variable produced it
   - which transformation steps were applied
   - which canonical unit and notes were declared in the mapping config

This gives a full audit path from a harmonized Zarr value back to:
- the raw ONC file
- the exact observation position inside that file
- the request used to obtain the file
- the transformation definition used to rename or reinterpret the variable

## Limitation

The current implementation records lineage for the transformations that exist in this repository now, which are mainly:
- download
- flattening profile data into observation rows
- variable renaming through `configs/variable_map.yaml`

If you later add unit conversion, QC filtering, gap filling, interpolation, aggregation, or model-derived variables, you should append those steps explicitly to `variable_lineage.json` and the run manifest so the audit trail stays complete.

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
