"""HTTP transport for harmonization and discovery API calls.

'why': isolate httpx usage to enable centralized timeout, retry, and auth policy changes
"""
from __future__ import annotations

import csv
import gzip
import json
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Final
from urllib.parse import quote

import httpx

from ._adapter import normalize_manifest_mapping

SCHEMA_VERSION: Final[str] = "1.0"
DEFAULT_MODEL_VERSION: Final[str] = "v1"
MAX_COMPRESSED_BYTES: Final[int] = 10 * 1024 * 1024

def build_harmonize_payload(
    csv_path: Path,
    manifest: Path | Mapping[str, object] | None,
    model_version: str = DEFAULT_MODEL_VERSION,
) -> bytes:
    """Return gzip-compressed harmonization payload for the given CSV and manifest."""

    rows = _read_tabular(csv_path)
    header = rows[0] if rows else []
    data_rows = rows[1:] if len(rows) > 1 else []

    envelope: dict[str, object] = {
        "schemaVersion": SCHEMA_VERSION,
        "modelVersion": model_version,
        "document": {
            "name": csv_path.name,
            "sheetName": None,
            "header": header,
            "rows": data_rows,
        },
    }

    mapping = normalize_manifest_mapping(manifest)
    if mapping:
        envelope["mapping"] = mapping

    raw = json.dumps(envelope, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    compressed = gzip.compress(raw)
    if len(compressed) > MAX_COMPRESSED_BYTES:
        raise ValueError("compressed harmonization payload exceeds 10 MiB")
    return compressed

async def submit_harmonize_job(
    base_url: str,
    api_key: str,
    payload_gz: bytes,
    timeout: float,
    idempotency_key: str | None = None,
) -> httpx.Response:
    """Submit a harmonization job request and return the raw response."""

    url = _build_job_submit_url(base_url)
    headers = {
        "x-api-key": api_key,
        "Content-Type": "application/json",
        "Content-Encoding": "gzip",
    }
    if idempotency_key:
        headers["Idempotency-Key"] = idempotency_key

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.post(url, content=payload_gz, headers=headers)

async def fetch_job_status(
    base_url: str,
    api_key: str,
    job_id: str,
    timeout: float,
) -> httpx.Response:
    """Return the status response for a previously submitted harmonization job."""

    url = _build_job_status_url(base_url, job_id)
    headers = {"x-api-key": api_key}
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.get(url, headers=headers)

async def request_mapping_discovery(
    base_url: str,
    api_key: str,
    timeout: float,
    schema: str,
    version: str,
    columns: Mapping[str, Sequence[str]],
    top_k: int | None = None,
) -> httpx.Response:
    """Submit column samples for mapping recommendations."""

    url = _build_discovery_url(base_url)
    headers = {
        "Content-Type": "application/json",
        "x-api-key": api_key,
    }
    body: dict[str, object] = {
        "target_schema": schema,
        "target_version": version,
        "data": columns,
    }
    if top_k is not None:
        body["top_k"] = top_k
    payload = {"body": json.dumps(body)}
    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.post(url, headers=headers, json=payload)


async def fetch_data_models(
    base_url: str,
    api_key: str,
    timeout: float,
    query: str | None = None,
    include_versions: bool = False,
    include_counts: bool = False,
    limit: int | None = None,
    offset: int = 0,
) -> httpx.Response:
    """Fetch data models from the Data Model Store."""

    url = f"{base_url.rstrip('/')}/data-models"
    headers = {"x-api-key": api_key}
    params = _build_data_models_params(query, include_versions, include_counts, limit, offset)

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.get(url, headers=headers, params=params)


def _build_data_models_params(
    query: str | None,
    include_versions: bool,
    include_counts: bool,
    limit: int | None,
    offset: int,
) -> dict[str, str | int]:
    """Build query parameters for data models endpoint."""

    candidates: list[tuple[str, str | int | None]] = [
        ("offset", offset),
        ("q", query),
        ("include_versions", "true" if include_versions else None),
        ("include_counts", "true" if include_counts else None),
        ("limit", limit),
    ]
    return {k: v for k, v in candidates if v is not None}


async def fetch_cdes(
    base_url: str,
    api_key: str,
    timeout: float,
    model_key: str,
    version: str,
    include_description: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> httpx.Response:
    """Fetch CDEs for a data model version from the Data Model Store."""

    url = f"{base_url.rstrip('/')}/data-models/{quote(model_key, safe='')}/versions/{quote(version, safe='')}/cdes"
    headers = {"x-api-key": api_key}
    params: dict[str, str | int] = {"offset": offset}
    if include_description:
        params["include_description"] = "true"
    if query:
        params["q"] = query
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.get(url, headers=headers, params=params)


async def fetch_pvs(
    base_url: str,
    api_key: str,
    timeout: float,
    model_key: str,
    version: str,
    cde_key: str,
    include_inactive: bool = False,
    query: str | None = None,
    limit: int | None = None,
    offset: int = 0,
) -> httpx.Response:
    """Fetch permissible values for a CDE from the Data Model Store."""

    path = (
        f"/data-models/{quote(model_key, safe='')}"
        f"/versions/{quote(version, safe='')}"
        f"/cdes/{quote(cde_key, safe='')}/pvs"
    )
    url = f"{base_url.rstrip('/')}{path}"
    headers = {"x-api-key": api_key}
    params: dict[str, str | int] = {"offset": offset}
    if include_inactive:
        params["include_inactive"] = "true"
    if query:
        params["q"] = query
    if limit is not None:
        params["limit"] = limit

    async with httpx.AsyncClient(timeout=httpx.Timeout(timeout)) as client:
        return await client.get(url, headers=headers, params=params)

def _build_job_submit_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}/v1/jobs/harmonize"

def _build_job_status_url(base_url: str, job_id: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}/v1/jobs/{quote(job_id, safe='')}"

def _build_discovery_url(base_url: str) -> str:
    base = base_url.rstrip("/")
    return f"{base}/cde-recommendation"

def _read_tabular(path: Path) -> list[list[str]]:
    if not path.exists():
        raise FileNotFoundError(path)
    ext = path.suffix.lower()
    if ext not in {".csv", ".tsv"}:
        raise ValueError("harmonization only supports CSV or TSV inputs")
    delimiter = "," if ext == ".csv" else "\t"
    with path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle, delimiter=delimiter)
        return [list(row) for row in reader]

