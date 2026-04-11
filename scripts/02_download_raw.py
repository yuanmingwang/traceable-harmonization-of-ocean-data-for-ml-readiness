from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
import sys
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from src.traceable_ocean.config import load_yaml
from src.traceable_ocean.download import (
    build_tabledap_url,
    download_file,
    fetch_dataset_variables_from_dds,
    resolve_requested_variables,
)
from src.traceable_ocean.provenance import write_json


def _clean_catalog_value(value: object) -> str:
    if pd.isna(value):
        return ''
    return str(value)


def build_time_constraints(row: dict, enabled: bool) -> list[str]:
    if not enabled:
        return []

    constraints: list[str] = []
    start = _clean_catalog_value(row.get('request_time_min')) or _clean_catalog_value(row.get('minTime'))
    end = _clean_catalog_value(row.get('request_time_max')) or _clean_catalog_value(row.get('maxTime'))
    if start:
        constraints.append(f'&time>={start}')
    if end:
        constraints.append(f'&time<={end}')
    return constraints


def remove_dir_if_empty(path: Path) -> None:
    if not path.exists() or not path.is_dir():
        return
    if any(path.iterdir()):
        return
    path.rmdir()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument('--config', required=True)
    args = parser.parse_args()

    cfg = load_yaml(args.config)
    plan_path = Path(cfg['paths']['manifests_root']) / 'download_plan.csv'
    logs_root = Path(cfg['paths']['logs_root'])
    logs_root.mkdir(parents=True, exist_ok=True)
    df = pd.read_csv(plan_path)
    run_started_at = datetime.now(timezone.utc)
    run_id = run_started_at.strftime('%Y%m%dT%H%M%SZ')
    log_rows: list[dict[str, object]] = []

    base_tabledap = cfg['catalog']['base_erddap_tabledap']
    file_type = cfg['request']['file_type']
    header_type = cfg['request']['header_file_type']
    preferred_variables = cfg['request'].get('preferred_variables', cfg['request'].get('requested_variables', []))
    required_variables = cfg['request'].get('required_variables', ['profile_id', 'time', 'latitude', 'longitude', 'depth'])
    request_all_variables = cfg['request'].get('request_all_variables', False)
    extra_constraints = cfg['request'].get('extra_constraints', [])
    resolve_variables_from_dds = cfg['request'].get('resolve_variables_from_dds', True)
    include_all_if_no_match = cfg['request'].get('include_all_if_no_match', True)
    use_catalog_time_range = cfg['request'].get('use_catalog_time_range', True)
    continue_on_error = cfg['request'].get('continue_on_error', True)
    timeout_seconds = cfg['request'].get('timeout_seconds', 120)
    user_agent = cfg['request'].get('user_agent', 'traceable-ocean-starter/0.1')

    for row in df.to_dict(orient='records'):
        dataset_id = row['datasetID']
        dataset_dir = Path(row['dataset_dir'])
        dataset_dir.mkdir(parents=True, exist_ok=True)
        nc_path = dataset_dir / 'source.nc'
        header_path = dataset_dir / 'source.header.txt'
        req_path = dataset_dir / 'request.json'

        dataset_constraints = build_time_constraints(row, enabled=use_catalog_time_range) + list(extra_constraints)
        log_entry: dict[str, object] = {
            'dataset_id': dataset_id,
            'started_at': datetime.now(timezone.utc).isoformat(),
            'status': 'failed',
            'error': '',
            'nc_path': str(nc_path),
            'header_path': str(header_path),
            'nc_exists': False,
            'header_exists': False,
            'nc_url': '',
            'header_url': '',
            'resolved_variable_count': 0,
        }

        try:
            available_variables = []
            if resolve_variables_from_dds or not preferred_variables:
                available_variables = fetch_dataset_variables_from_dds(
                    base_tabledap,
                    dataset_id,
                    timeout_seconds=timeout_seconds,
                    user_agent=user_agent,
                )

            if request_all_variables:
                requested_variables = list(available_variables)
            else:
                requested_variables = resolve_requested_variables(
                    preferred_variables=preferred_variables,
                    available_variables=available_variables or preferred_variables,
                    required_variables=required_variables,
                    include_all_if_no_match=include_all_if_no_match,
                )

            nc_url = build_tabledap_url(base_tabledap, dataset_id, file_type, requested_variables, dataset_constraints)
            header_url = build_tabledap_url(base_tabledap, dataset_id, header_type, requested_variables, dataset_constraints)
            log_entry['nc_url'] = nc_url
            log_entry['header_url'] = header_url
            log_entry['resolved_variable_count'] = len(requested_variables)

            print(f'Downloading {dataset_id} -> {nc_path}')
            download_file(nc_url, nc_path, timeout_seconds=timeout_seconds, user_agent=user_agent)
            print(f'Downloading {dataset_id} header -> {header_path}')
            download_file(header_url, header_path, timeout_seconds=timeout_seconds, user_agent=user_agent)

            write_json(req_path, {
                'dataset_id': dataset_id,
                'dds_url': row.get('dds_url') or build_tabledap_url(base_tabledap, dataset_id, 'dds'),
                'nc_url': nc_url,
                'header_url': header_url,
                'request_all_variables': request_all_variables,
                'preferred_variables': preferred_variables,
                'required_variables': required_variables,
                'resolved_variables': requested_variables,
                'available_variables': available_variables,
                'extra_constraints': dataset_constraints,
            })
            log_entry['status'] = 'success'
            log_entry['nc_exists'] = nc_path.exists()
            log_entry['header_exists'] = header_path.exists()
        except Exception as exc:
            log_entry['error'] = str(exc)
            log_entry['nc_exists'] = nc_path.exists()
            log_entry['header_exists'] = header_path.exists()
            if not log_entry['nc_exists'] and not log_entry['header_exists'] and not req_path.exists():
                remove_dir_if_empty(dataset_dir)
            if not continue_on_error:
                pending_rows = log_rows + [
                    {
                        **log_entry,
                        'finished_at': datetime.now(timezone.utc).isoformat(),
                    }
                ]
                write_run_logs(logs_root, run_id, run_started_at, pending_rows, plan_path)
                raise
            print(f'Skipping {dataset_id}: {exc}')
        finally:
            log_entry['finished_at'] = datetime.now(timezone.utc).isoformat()
            log_rows.append(log_entry)

    write_run_logs(logs_root, run_id, run_started_at, log_rows, plan_path)


def write_run_logs(
    logs_root: Path,
    run_id: str,
    run_started_at: datetime,
    log_rows: list[dict[str, object]],
    plan_path: Path,
) -> None:
    log_csv_path = logs_root / f'download_run_{run_id}.csv'
    summary_path = logs_root / f'download_run_{run_id}.summary.json'

    pd.DataFrame(log_rows).to_csv(log_csv_path, index=False)

    success_count = sum(1 for row in log_rows if row['status'] == 'success')
    failure_count = len(log_rows) - success_count
    failed_ids = [str(row['dataset_id']) for row in log_rows if row['status'] != 'success']
    summary = {
        'run_id': run_id,
        'run_started_at': run_started_at.isoformat(),
        'run_finished_at': datetime.now(timezone.utc).isoformat(),
        'plan_path': str(plan_path),
        'log_csv_path': str(log_csv_path),
        'total_datasets': len(log_rows),
        'success_count': success_count,
        'failure_count': failure_count,
        'failed_dataset_ids': failed_ids,
    }
    write_json(summary_path, summary)
    print(f'Wrote download log to {log_csv_path}')
    print(f'Wrote download summary to {summary_path}')
    print(f'Download summary: {success_count} succeeded, {failure_count} failed')


if __name__ == '__main__':
    main()
