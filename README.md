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

python scripts/validate_zarr.py data/derived/ml_ready_profiles.zarr

python scripts/validate_zarr.py \
  data/derived/ml_ready_profiles.zarr \
  --source-nc data/raw/onc_erddap/profile_1214852_12474/source.nc
```

## How The Project Works

The pipeline is designed as a sequence of layers. Each layer has a narrow responsibility and writes artifacts that the next layer can consume without changing the earlier data.

### 1. Catalog layer

Input:
- `data/catalog/allDataset.csv`

Responsibility:
- store the ERDDAP catalog snapshot used for the run
- provide a stable, auditable list of candidate datasets
- keep the source catalog separate from download and harmonization logic

Implementation:
- `scripts/01_build_download_plan.py`
- `src/traceable_ocean/catalog.py`

Output:
- `data/manifests/download_plan.csv`

The download plan is important because it freezes the subset of datasets selected for a run. If the remote ERDDAP catalog changes later, you still know exactly which rows were chosen at the time of execution.

### 2. Raw ingestion layer

Input:
- `data/manifests/download_plan.csv`

Responsibility:
- download raw `.ncCFMA` files
- download matching `.ncCFMAHeader` files
- capture the request used for each dataset
- keep raw data immutable after download

Implementation:
- `scripts/02_download_raw.py`
- `src/traceable_ocean/download.py`

Output per dataset:
- `data/raw/onc_erddap/<dataset_id>/source.nc`
- `data/raw/onc_erddap/<dataset_id>/source.header.txt`
- `data/raw/onc_erddap/<dataset_id>/request.json`

Run-level output:
- `data/logs/download_run_<timestamp>.csv`
- `data/logs/download_run_<timestamp>.summary.json`

The ingestion layer does not reinterpret variables. It only fetches, records, and stores the source artifacts.

### 3. Source manifest layer

Input:
- downloaded raw NetCDF and header files

Responsibility:
- compute content hashes
- extract source metadata and dimensions
- write machine-readable manifests for each downloaded dataset

Implementation:
- `scripts/03_manifest_sources.py`
- `src/traceable_ocean/provenance.py`

Output:
- `data/manifests/<dataset_id>.manifest.json`
- `data/manifests/source_manifest_index.csv`

This layer gives each source file an integrity fingerprint with SHA-256, plus a snapshot of the metadata present in the raw file at the moment it entered the pipeline.

### 4. Harmonization layer

Input:
- raw NetCDF files
- interpretation rules from YAML

Responsibility:
- flatten profile-style NetCDF structures into an observation table
- rename variables into a canonical schema
- write the ML-ready Zarr store
- attach row-level and variable-level lineage metadata

Implementation:
- `scripts/04_harmonize_profiles_to_zarr.py`
- `src/traceable_ocean/harmonize.py`
- `configs/variable_map.yaml`

Output:
- `data/derived/ml_ready_profiles.zarr`
- `data/manifests/lineage.csv`
- `data/manifests/observation_lineage.csv`
- `data/manifests/variable_lineage.json`
- `data/manifests/latest_harmonization_run.json`

## Why Interpretation Changes Do Not Require Rewriting Ingestion

The system separates "getting the data" from "deciding what the data means".

### Ingestion is fixed and source-preserving

The ingestion code:
- downloads the source file
- stores the header and request used to fetch it
- hashes the raw file
- does not alter values inside the raw file

That means the raw layer is a preserved evidence layer. If you later decide a variable was mislabeled, needed unit conversion, or should be aggregated differently, the source file does not need to be fetched again unless you want a different upstream subset.

### Interpretation lives in configuration and harmonization logic

The meaning of variables is expressed separately in:
- `configs/variable_map.yaml`
- the harmonization code in `src/traceable_ocean/harmonize.py`

This is the key design decision. For example:
- if `Temperature` should be treated as Celsius instead of Kelvin, you change the mapping or add an explicit normalization step in harmonization
- if `depth` should be sign-corrected or converted to another unit, you change the transformation rule, not the downloader
- if multiple observations should be aggregated into a coarser representation, you add that aggregation in the derived layer, not in the raw ingestion layer

Because raw source files remain untouched, you can rerun only the manifest and harmonization stages after changing interpretation rules. This preserves reproducibility:
- same source files
- different interpretation rules
- new derived output
- updated lineage describing the new transformation path

### Why this matters

If interpretation were embedded directly in ingestion, any change in semantics would require rewriting download logic or mutating the raw source data. That would make it harder to:
- compare old and new interpretations
- reproduce earlier derived outputs
- audit what exactly changed
- trust that the original evidence was preserved

This project avoids that by making raw ingestion conservative and derived transformation explicit.

## How Provenance And Integrity Are Verified

The architecture supports provenance and integrity verification at multiple points in the pipeline.

### Raw-file integrity

Each downloaded source file is hashed with SHA-256 in `scripts/03_manifest_sources.py`.

Artifacts:
- `data/manifests/<dataset_id>.manifest.json`
- `data/manifests/source_manifest_index.csv`

This allows you to verify:
- whether the raw file changed after download
- whether two runs are using the same source artifact
- whether a derived dataset still points to the exact expected source file

### Request provenance

Each dataset keeps the exact request metadata used to fetch it:
- `request.json`
- `.ncCFMAHeader` text
- ERDDAP request URL embedded in source metadata and manifests

This allows you to verify:
- where the data came from
- what subset and variables were requested
- whether a later run used a different extraction request

### Observation-level lineage

Each row in the Zarr output carries:
- `lineage_record_id`
- `source_dataset_id`
- `source_file`
- `source_sha256`
- `source_profile_index`
- `source_obs_index`

This allows a single data point in the derived store to be traced back to the exact observation in the original raw file.

### Variable-level lineage

`data/manifests/variable_lineage.json` records, for each canonical output variable:
- the raw source variable
- the canonical output name
- unit metadata from the mapping config
- the transformation steps used to derive it

This allows you to verify not just where a value came from, but how it became the specific derived variable found in the Zarr store.

### Run-level provenance

`data/manifests/latest_harmonization_run.json` records:
- the output Zarr path
- the columns written
- the variable map used
- the lineage artifacts associated with that run

This gives each derived run a manifest that can be checked independently.

## How To Verify A Derived Zarr Dataset

There are two levels of verification in this repository.

### 1. Structural and schema verification

Use `scripts/validate_zarr.py` to:
- open the Zarr store
- print dimensions and variables
- check for common expected provenance columns
- flag suspicious latitude, longitude, depth, and temperature issues

### 2. Source-to-derived comparison

The same script can compare the Zarr output against one raw NetCDF source file. It checks:
- row count consistency between raw `profile * obs` and Zarr rows referencing that source
- value equality for mapped variables that exist in both representations

This is the strongest practical integrity check in the current codebase because it verifies that the derived dataset is still numerically consistent with the raw source for a chosen file.

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

