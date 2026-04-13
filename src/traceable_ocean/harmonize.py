from __future__ import annotations

from pathlib import Path
from typing import Dict, Any, List
import json
import numpy as np
import pandas as pd
import xarray as xr


def open_netcdf_any(path: str | Path) -> xr.Dataset:
    path = str(path)
    last_err = None
    for engine in (None, 'scipy', 'netcdf4', 'h5netcdf'):
        try:
            if engine is None:
                return xr.open_dataset(path)
            return xr.open_dataset(path, engine=engine)
        except Exception as e:
            last_err = e
    raise RuntimeError(f'Could not open {path}: {last_err}')


def _decode_char_if_needed(arr: xr.DataArray):
    try:
        return arr.astype(str)
    except Exception:
        return arr


def profile_to_observation_table(ds: xr.Dataset, source_file: str, source_sha256: str) -> pd.DataFrame:
    """Flatten a Profile dataset with dims like (profile, obs) into an observation table."""
    profile_dim = 'profile' if 'profile' in ds.dims else None
    obs_dim = 'obs' if 'obs' in ds.dims else None

    if not profile_dim or not obs_dim:
        raise ValueError(f'Expected dims profile and obs, found {dict(ds.dims)}')

    profile_count = ds.dims[profile_dim]
    obs_count = ds.dims[obs_dim]

    rows: List[Dict[str, Any]] = []
    dataset_id = ds.attrs.get('id', Path(source_file).parent.name)
    for p in range(profile_count):
        base: Dict[str, Any] = {
            'source_dataset_id': dataset_id,
            'source_file': source_file,
            'source_sha256': source_sha256,
            'source_profile_index': p,
        }

        for name, var in ds.variables.items():
            dims = var.dims
            if dims == (profile_dim,):
                try:
                    base[name] = var.isel({profile_dim: p}).item()
                except Exception:
                    base[name] = str(var.isel({profile_dim: p}).values)

        for o in range(obs_count):
            row = dict(base)
            row['source_obs_index'] = o
            row['lineage_record_id'] = f'{dataset_id}:p{p}:o{o}'
            for name, var in ds.variables.items():
                dims = var.dims
                if dims == (profile_dim, obs_dim):
                    value = var.isel({profile_dim: p, obs_dim: o}).values
                    if hasattr(value, 'item'):
                        try:
                            value = value.item()
                        except Exception:
                            pass
                    row[name] = value
            rows.append(row)

    return pd.DataFrame(rows)


def apply_variable_map(df: pd.DataFrame, variable_map: Dict[str, Any]) -> pd.DataFrame:
    rename_map = {}
    keep_cols = []

    variables = variable_map.get('variables', {})
    for raw_name, spec in variables.items():
        if raw_name in df.columns and spec.get('keep', True):
            rename_map[raw_name] = spec.get('canonical_name', raw_name)
            keep_cols.append(raw_name)

    # Always keep provenance columns.
    for c in [
        'lineage_record_id',
        'source_dataset_id',
        'source_file',
        'source_sha256',
        'source_profile_index',
        'source_obs_index',
    ]:
        if c in df.columns:
            keep_cols.append(c)

    keep_cols = [c for c in df.columns if c in set(keep_cols)]
    out = df[keep_cols].rename(columns=rename_map).copy()
    return out


def build_variable_lineage_map(df: pd.DataFrame, variable_map: Dict[str, Any]) -> Dict[str, Any]:
    variables = variable_map.get('variables', {})
    lineage: Dict[str, Any] = {}

    for raw_name, spec in variables.items():
        canonical_name = spec.get('canonical_name', raw_name)
        if raw_name not in df.columns or not spec.get('keep', True):
            continue

        transformation_steps = [
            {
                'step': 1,
                'operation': 'extract_from_ncCFMA',
                'details': f'Read variable "{raw_name}" from the raw NetCDF dataset.',
            },
            {
                'step': 2,
                'operation': 'flatten_profile_to_observation_table',
                'details': 'Expand (profile, obs) values into one row per observation.',
            },
        ]

        if canonical_name != raw_name:
            transformation_steps.append({
                'step': 3,
                'operation': 'rename_variable',
                'details': f'Rename "{raw_name}" to canonical variable "{canonical_name}" via configs/variable_map.yaml.',
            })
        else:
            transformation_steps.append({
                'step': 3,
                'operation': 'retain_variable_name',
                'details': f'Keep variable name "{raw_name}" unchanged in the harmonized output.',
            })

        lineage[canonical_name] = {
            'source_variable': raw_name,
            'canonical_variable': canonical_name,
            'canonical_unit': spec.get('canonical_unit'),
            'notes': spec.get('notes'),
            'transformation_steps': transformation_steps,
        }

    return lineage


def build_observation_lineage_table(df: pd.DataFrame) -> pd.DataFrame:
    lineage_cols = [
        'lineage_record_id',
        'source_dataset_id',
        'source_profile_id',
        'source_file',
        'source_sha256',
        'source_profile_index',
        'source_obs_index',
        'dataset_id',
    ]
    existing = [col for col in lineage_cols if col in df.columns]
    return df[existing].copy()


def write_variable_lineage_json(path: str | Path, lineage_map: Dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open('w', encoding='utf-8') as f:
        json.dump(lineage_map, f, indent=2, ensure_ascii=False)


def dataframe_to_zarr(df: pd.DataFrame, zarr_path: str | Path) -> None:
    ds = xr.Dataset.from_dataframe(df)
    zarr_path = Path(zarr_path)
    zarr_path.parent.mkdir(parents=True, exist_ok=True)
    ds.to_zarr(zarr_path, mode='w')
