from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd
import xarray as xr

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.traceable_ocean.config import load_yaml
from src.traceable_ocean.provenance import sha256_file, write_json
from src.traceable_ocean.harmonize import open_netcdf_any


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    raw_root = Path(cfg['paths']['raw_root'])
    manifests_root = Path(cfg['paths']['manifests_root'])
    manifests_root.mkdir(parents=True, exist_ok=True)

    rows = []
    for dataset_dir in sorted(p for p in raw_root.iterdir() if p.is_dir()):
        nc_path = dataset_dir / 'source.nc'
        header_path = dataset_dir / 'source.header.txt'
        request_path = dataset_dir / 'request.json'
        if not nc_path.exists():
            continue

        ds = open_netcdf_any(nc_path)
        sha = sha256_file(nc_path)

        manifest = {
            'dataset_id': ds.attrs.get('id', dataset_dir.name),
            'source_file': str(nc_path),
            'header_file': str(header_path) if header_path.exists() else None,
            'request_file': str(request_path) if request_path.exists() else None,
            'sha256': sha,
            'featureType': ds.attrs.get('featureType'),
            'cdm_data_type': ds.attrs.get('cdm_data_type'),
            'institution': ds.attrs.get('institution'),
            'history': ds.attrs.get('history'),
            'title': ds.attrs.get('title'),
            'dims': {k: int(v) for k, v in ds.dims.items()},
            'variables': list(ds.variables),
            'global_attrs': {k: (str(v) if not isinstance(v, (str, int, float, bool, list, dict, type(None))) else v) for k, v in ds.attrs.items()},
        }
        write_json(manifests_root / f'{dataset_dir.name}.manifest.json', manifest)
        rows.append({
            'dataset_id': manifest['dataset_id'],
            'source_file': manifest['source_file'],
            'sha256': manifest['sha256'],
            'featureType': manifest['featureType'],
            'cdm_data_type': manifest['cdm_data_type'],
            'title': manifest['title'],
        })

    pd.DataFrame(rows).to_csv(manifests_root / 'source_manifest_index.csv', index=False)
    print(f'Wrote {len(rows)} source manifests to {manifests_root}')


if __name__ == '__main__':
    main()
