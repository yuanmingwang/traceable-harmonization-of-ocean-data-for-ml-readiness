from __future__ import annotations

import argparse
from pathlib import Path
import sys
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


def profile_to_observation_table(ds: xr.Dataset, source_file: str) -> pd.DataFrame:
    profile_dim = 'profile' if 'profile' in ds.dims else None
    obs_dim = 'obs' if 'obs' in ds.dims else None
    if not profile_dim or not obs_dim:
        raise ValueError(f'Expected dims profile and obs, found {dict(ds.dims)}')

    rows = []
    for p in range(ds.dims[profile_dim]):
        base = {'source_file': source_file}
        for name, var in ds.variables.items():
            if var.dims == (profile_dim,):
                try:
                    base[name] = var.isel({profile_dim: p}).item()
                except Exception:
                    base[name] = str(var.isel({profile_dim: p}).values)
        for o in range(ds.dims[obs_dim]):
            row = dict(base)
            for name, var in ds.variables.items():
                if var.dims == (profile_dim, obs_dim):
                    value = var.isel({profile_dim: p, obs_dim: o}).values
                    if hasattr(value, 'item'):
                        try:
                            value = value.item()
                        except Exception:
                            pass
                    row[name] = value
            rows.append(row)
    return pd.DataFrame(rows)


def summarize_zarr(zarr_path: Path) -> xr.Dataset:
    ds = xr.open_zarr(zarr_path)
    print(f'Opened Zarr: {zarr_path}')
    print('\nDimensions:')
    for k, v in ds.sizes.items():
        print(f'  {k}: {v}')
    print('\nVariables:')
    for name in ds.data_vars:
        da = ds[name]
        print(f'  {name}: dims={da.dims}, dtype={da.dtype}')
    return ds


def check_basic_schema(ds: xr.Dataset) -> int:
    errors = 0
    if 'index' not in ds.dims:
        print('[WARN] No index dimension found. This may be okay if you changed the layout.')
    expected_some = ['source_file', 'source_sha256', 'time', 'latitude', 'longitude', 'depth_m']
    missing = [v for v in expected_some if v not in ds]
    if missing:
        print(f'[WARN] Missing commonly expected variables: {missing}')
    else:
        print('[OK] Common schema variables are present.')

    if 'latitude' in ds:
        lat = ds['latitude'].values
        finite = np.isfinite(lat)
        if finite.any() and ((lat[finite] < -90).any() or (lat[finite] > 90).any()):
            print('[ERR] Latitude outside [-90, 90].')
            errors += 1
        else:
            print('[OK] Latitude range looks valid.')

    if 'longitude' in ds:
        lon = ds['longitude'].values
        finite = np.isfinite(lon)
        if finite.any() and ((lon[finite] < -180).any() or (lon[finite] > 360).any()):
            print('[ERR] Longitude outside expected range [-180, 360].')
            errors += 1
        else:
            print('[OK] Longitude range looks valid.')

    if 'depth_m' in ds:
        depth = ds['depth_m'].values
        finite = np.isfinite(depth)
        if finite.any() and (depth[finite] < 0).any():
            print('[WARN] Negative depth values found. Check whether depth/altitude sign handling is correct.')
        else:
            print('[OK] Depth values are non-negative.')

    if 'sea_water_temperature_raw' in ds:
        temp = ds['sea_water_temperature_raw'].values
        finite = np.isfinite(temp)
        if finite.any() and np.nanmedian(temp) < 100:
            print('[WARN] Temperature variable is labeled raw and may be metadata-sensitive. Values <100 suggest it may not actually be Kelvin.')

    return errors


def compare_with_source(ds_zarr: xr.Dataset, source_nc: Path) -> int:
    print(f'\nComparing against source NetCDF: {source_nc}')
    errors = 0
    ds_nc = open_netcdf_any(source_nc)
    expected_rows = int(ds_nc.sizes.get('profile', 0) * ds_nc.sizes.get('obs', 0))
    print(f'Expected flattened rows from source: {expected_rows}')

    if 'source_file' not in ds_zarr:
        print('[WARN] Cannot filter by source_file because source_file is missing in the Zarr store.')
        return errors

    source_file_values = ds_zarr['source_file'].astype(str).values
    source_name = source_nc.name
    source_full = str(source_nc)
    mask = (source_file_values == source_full) | np.char.endswith(source_file_values, f'/{source_name}')
    actual_rows = int(mask.sum())
    print(f'Rows in Zarr referencing this source_file: {actual_rows}')
    if actual_rows != expected_rows:
        print('[ERR] Row count mismatch for this source file.')
        errors += 1
    else:
        print('[OK] Row count matches source profile*obs.')

    src_df = profile_to_observation_table(ds_nc, source_full)
    src_cols = set(src_df.columns)

    variable_pairs = [
        ('profile_id', 'source_profile_id'),
        ('time', 'time'),
        ('latitude', 'latitude'),
        ('longitude', 'longitude'),
        ('depth', 'depth_m'),
        ('pressure', 'sea_water_pressure_dbar'),
        ('absolute_pressure', 'absolute_pressure_dbar'),
        ('salinity', 'practical_salinity_raw'),
        ('Temperature', 'sea_water_temperature_raw'),
        ('oxygen_saturation', 'oxygen_saturation_fraction'),
        ('chlorophyll', 'chlorophyll_ug_l'),
        ('cdom', 'cdom_ppb_qsde'),
        ('density', 'density_kg_m3'),
        ('cond', 'conductivity_s_m'),
        ('Sound_Speed', 'sound_speed_m_s'),
    ]

    zdf = ds_zarr.where(mask, drop=True).to_dataframe().reset_index(drop=True)
    for src_name, zarr_name in variable_pairs:
        if src_name not in src_cols or zarr_name not in zdf.columns:
            continue
        a = pd.to_numeric(src_df[src_name], errors='coerce')
        b = pd.to_numeric(zdf[zarr_name], errors='coerce')
        if len(a) != len(b):
            print(f'[ERR] Length mismatch for {src_name} -> {zarr_name}: {len(a)} vs {len(b)}')
            errors += 1
            continue
        if not np.allclose(a.fillna(np.nan), b.fillna(np.nan), equal_nan=True):
            print(f'[ERR] Value mismatch for {src_name} -> {zarr_name}')
            errors += 1
        else:
            print(f'[OK] Values match for {src_name} -> {zarr_name}')
    return errors


def main() -> int:
    ap = argparse.ArgumentParser(description='Validate a harmonized Zarr store for the capstone pipeline.')
    ap.add_argument('zarr_path', help='Path to the .zarr store')
    ap.add_argument('--source-nc', help='Optional source .nc file to compare against')
    args = ap.parse_args()

    ds = summarize_zarr(Path(args.zarr_path))
    errors = check_basic_schema(ds)
    if args.source_nc:
        errors += compare_with_source(ds, Path(args.source_nc))

    print('\nValidation finished.')
    if errors == 0:
        print('No hard errors found.')
    else:
        print(f'Found {errors} error(s).')
    return 1 if errors else 0


if __name__ == '__main__':
    sys.exit(main())
