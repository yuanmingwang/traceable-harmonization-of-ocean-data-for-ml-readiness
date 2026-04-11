from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Tuple
import csv
import pandas as pd


def read_erddap_csv_with_units(path: str | Path) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Read an ERDDAP .csv file where line 1 is names and line 2 is units."""
    path = Path(path)
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.reader(f)
        rows = list(reader)

    if len(rows) < 2:
        raise ValueError(f'ERDDAP CSV at {path} must have at least two rows (names + units).')

    header = rows[0]
    units = rows[1]
    body = rows[2:]

    df = pd.DataFrame(body, columns=header)
    unit_map = {col: units[i] if i < len(units) else '' for i, col in enumerate(header)}
    return df, unit_map


def filter_catalog(
    df: pd.DataFrame,
    accessible: str | None = None,
    cdm_data_types: List[str] | None = None,
    limit: int | None = None,
) -> pd.DataFrame:
    out = df.copy()

    if accessible and 'accessible' in out.columns:
        out = out[out['accessible'].fillna('').eq(accessible)]

    if cdm_data_types and 'cdm_data_type' in out.columns:
        out = out[out['cdm_data_type'].fillna('').isin(cdm_data_types)]

    out = out.reset_index(drop=True)
    if limit:
        out = out.head(limit)
    return out
