from __future__ import annotations

import argparse
from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.traceable_ocean.config import load_yaml
from src.traceable_ocean.catalog import read_erddap_csv_with_units, filter_catalog


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    catalog_path = cfg['catalog']['csv_path']
    raw_root = Path(cfg['paths']['raw_root'])
    manifests_root = Path(cfg['paths']['manifests_root'])
    raw_root.mkdir(parents=True, exist_ok=True)
    manifests_root.mkdir(parents=True, exist_ok=True)

    df, units = read_erddap_csv_with_units(catalog_path)
    selected = filter_catalog(
        df,
        accessible=cfg['selection'].get('accessible'),
        cdm_data_types=cfg['selection'].get('cdm_data_types'),
        limit=cfg['selection'].get('limit'),
    )

    selected = selected.copy()
    selected['dataset_dir'] = selected['datasetID'].apply(lambda x: str(raw_root / str(x)))
    selected['nc_path'] = selected['datasetID'].apply(lambda x: str(raw_root / str(x) / 'source.nc'))
    selected['header_path'] = selected['datasetID'].apply(lambda x: str(raw_root / str(x) / 'source.header.txt'))
    selected['dds_url'] = selected['datasetID'].apply(lambda x: f"{cfg['catalog']['base_erddap_tabledap']}/{x}.dds")
    selected['request_time_min'] = selected['minTime'] if 'minTime' in selected.columns else ''
    selected['request_time_max'] = selected['maxTime'] if 'maxTime' in selected.columns else ''

    plan_path = manifests_root / 'download_plan.csv'
    selected.to_csv(plan_path, index=False)
    print(f'Wrote download plan to {plan_path}')
    print(f'Selected {len(selected)} datasets')


if __name__ == '__main__':
    main()
