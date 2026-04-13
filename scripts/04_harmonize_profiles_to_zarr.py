from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.traceable_ocean.config import load_yaml
from src.traceable_ocean.provenance import sha256_file, write_json
from src.traceable_ocean.harmonize import (
    open_netcdf_any,
    profile_to_observation_table,
    apply_variable_map,
    build_observation_lineage_table,
    build_variable_lineage_map,
    dataframe_to_zarr,
    write_variable_lineage_json,
)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    raw_root = Path(cfg['paths']['raw_root'])
    manifests_root = Path(cfg['paths']['manifests_root'])
    zarr_path = Path(cfg['paths']['zarr_path'])
    variable_map = load_yaml(cfg['harmonization']['variable_map_path'])

    frames = []
    lineage_rows = []
    variable_lineage_map = {}

    for dataset_dir in sorted(p for p in raw_root.iterdir() if p.is_dir()):
        nc_path = dataset_dir / 'source.nc'
        if not nc_path.exists():
            continue

        ds = open_netcdf_any(nc_path)
        cdm_data_type = ds.attrs.get('cdm_data_type')
        if cdm_data_type != 'Profile':
            print(f'Skipping {dataset_dir.name}: cdm_data_type={cdm_data_type}')
            continue

        source_sha = sha256_file(nc_path)
        df = profile_to_observation_table(ds, str(nc_path), source_sha)
        variable_lineage_map.update(build_variable_lineage_map(df, variable_map))
        df = apply_variable_map(df, variable_map)
        df['dataset_id'] = ds.attrs.get('id', dataset_dir.name)
        frames.append(df)

        lineage_rows.append({
            'dataset_id': ds.attrs.get('id', dataset_dir.name),
            'source_file': str(nc_path),
            'source_sha256': source_sha,
            'history': ds.attrs.get('history'),
            'title': ds.attrs.get('title'),
            'featureType': ds.attrs.get('featureType'),
            'cdm_data_type': ds.attrs.get('cdm_data_type'),
        })

    if not frames:
        raise RuntimeError('No profile datasets found to harmonize.')

    combined = pd.concat(frames, ignore_index=True)
    dataframe_to_zarr(combined, zarr_path)
    lineage_path = manifests_root / 'lineage.csv'
    pd.DataFrame(lineage_rows).to_csv(lineage_path, index=False)
    observation_lineage_path = manifests_root / 'observation_lineage.csv'
    build_observation_lineage_table(combined).to_csv(observation_lineage_path, index=False)
    variable_lineage_path = manifests_root / 'variable_lineage.json'
    write_variable_lineage_json(variable_lineage_path, variable_lineage_map)

    run_manifest = {
        'zarr_path': str(zarr_path),
        'row_count': int(len(combined)),
        'columns': list(combined.columns),
        'lineage_csv': str(lineage_path),
        'observation_lineage_csv': str(observation_lineage_path),
        'variable_lineage_json': str(variable_lineage_path),
        'variable_map_path': cfg['harmonization']['variable_map_path'],
        'traceability': {
            'row_key_column': 'lineage_record_id',
            'source_profile_column': 'source_profile_id',
            'source_indices': ['source_profile_index', 'source_obs_index'],
            'variable_lineage_lookup': 'Use variable_lineage.json to map each canonical Zarr variable to its raw source variable and transformation steps.',
        },
    }
    write_json(manifests_root / 'latest_harmonization_run.json', run_manifest)
    print(f'Wrote Zarr store to {zarr_path}')
    print(f'Wrote lineage table to {lineage_path}')
    print(f'Wrote observation lineage table to {observation_lineage_path}')
    print(f'Wrote variable lineage map to {variable_lineage_path}')


if __name__ == '__main__':
    from src.traceable_ocean.config import load_yaml
    main()
