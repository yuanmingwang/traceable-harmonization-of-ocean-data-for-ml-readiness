# Architecture Notes

## Layers

### 1. Catalog layer
Stores the ERDDAP catalog snapshot used for the run.

### 2. Raw source layer
Contains immutable downloaded NetCDF files and header text files.
Each dataset lives in its own directory.

### 3. Manifest layer
Contains hashes, request metadata, extracted source metadata, and run manifests.

### 4. Harmonized layer
Contains the ML-ready Zarr store and a lineage table.

## We use this layout because it matches the following capstone brief

- Raw sources remain untouched.
- Reprocessing only changes manifests and derived outputs.
- Interpretation rules live in YAML instead of hardcoded ingestion logic.
- Per-file hashes and request snapshots make lineage auditable.
