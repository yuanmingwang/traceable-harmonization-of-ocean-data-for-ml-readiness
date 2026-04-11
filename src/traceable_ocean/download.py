from __future__ import annotations

from pathlib import Path
import re
from urllib.parse import quote
import requests


def build_tabledap_url(
    base_tabledap: str,
    dataset_id: str,
    file_type: str,
    requested_variables: list[str] | None = None,
    extra_constraints: list[str] | None = None,
) -> str:
    query_parts: list[str] = []
    if requested_variables:
        query_parts.append(','.join(requested_variables))
    if extra_constraints:
        query_parts.extend(extra_constraints)

    if not query_parts:
        return f"{base_tabledap}/{dataset_id}.{file_type}"

    query = ''.join(query_parts).lstrip('&')
    encoded_query = quote(query, safe=',&=><!()"._-:/')
    return f"{base_tabledap}/{dataset_id}.{file_type}?{encoded_query}"


def fetch_text(url: str, timeout_seconds: int = 120, user_agent: str = 'traceable-ocean-starter/0.1') -> str:
    with requests.get(url, timeout=timeout_seconds, headers={'User-Agent': user_agent}) as r:
        r.raise_for_status()
        return r.text


def fetch_dataset_variables_from_dds(
    base_tabledap: str,
    dataset_id: str,
    timeout_seconds: int = 120,
    user_agent: str = 'traceable-ocean-starter/0.1',
) -> list[str]:
    dds_url = build_tabledap_url(base_tabledap, dataset_id, 'dds')
    dds_text = fetch_text(dds_url, timeout_seconds=timeout_seconds, user_agent=user_agent)

    variables: list[str] = []
    seen: set[str] = set()
    for raw_line in dds_text.splitlines():
        line = raw_line.strip()
        if not line.endswith(';'):
            continue
        if line in {'Sequence {', 'Grid {', 'Structure {'}:
            continue

        match = re.match(r'^[A-Za-z0-9_]+\s+([A-Za-z0-9_]+)(?:\[.*)?;$', line)
        if not match:
            continue

        variable = match.group(1)
        if variable not in seen:
            variables.append(variable)
            seen.add(variable)

    if not variables:
        raise ValueError(f'No variables could be parsed from DDS for dataset {dataset_id}.')

    return variables


def resolve_requested_variables(
    preferred_variables: list[str] | None,
    available_variables: list[str],
    required_variables: list[str] | None = None,
    include_all_if_no_match: bool = True,
) -> list[str]:
    preferred_variables = preferred_variables or []
    required_variables = required_variables or []

    chosen: list[str] = []
    seen: set[str] = set()

    for variable in required_variables:
        if variable in available_variables and variable not in seen:
            chosen.append(variable)
            seen.add(variable)

    for variable in preferred_variables:
        if variable in available_variables and variable not in seen:
            chosen.append(variable)
            seen.add(variable)

    if chosen:
        return chosen

    if include_all_if_no_match:
        return list(available_variables)

    raise ValueError('No requested variables were available for the dataset.')


def download_file(url: str, output_path: str | Path, timeout_seconds: int = 120, user_agent: str = 'traceable-ocean-starter/0.1') -> None:
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with requests.get(url, stream=True, timeout=timeout_seconds, headers={'User-Agent': user_agent}) as r:
        r.raise_for_status()
        with output_path.open('wb') as f:
            for chunk in r.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)
