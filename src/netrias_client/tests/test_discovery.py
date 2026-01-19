"""Test mapping discovery workflows.

'why': guarantee recommendation calls integrate cleanly and surface results
"""
from __future__ import annotations

import json
from typing import cast

import httpx
import pytest

from pathlib import Path

from netrias_client import NetriasClient
from netrias_client._errors import MappingDiscoveryError, NetriasAPIUnavailable

from ._utils import install_mock_transport, json_failure, json_success, transport_error


def test_discover_mapping_from_csv_success(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Return structured recommendations when the API succeeds."""

    payload = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "target_schema": "ccdi",
                "recommendations": [
                    {
                        "column": "a",
                        "suggestions": [
                            {"target": "Sample.name", "confidence": 0.92},
                            {"target": "Sample.display_name", "confidence": 0.5},
                        ],
                    },
                    {
                        "column": "b",
                        "suggestions": [],
                    },
                ],
            }
        ),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )

    column_mappings = manifest.get("column_mappings", {})
    assert "a" in column_mappings
    assert column_mappings["a"]["targetField"] == "Sample.name"

    request = capture.requests[0]
    assert request.headers.get("x-api-key") == "test-api-key"
    payload_raw = cast(object, json.loads(request.content.decode("utf-8")))
    assert isinstance(payload_raw, dict)
    payload_sent = cast(dict[str, object], payload_raw)
    body_raw = payload_sent.get("body")
    assert isinstance(body_raw, str)
    assert "\"target_schema\": \"ccdi\"" in body_raw
    assert "\"target_version\": \"v1\"" in body_raw


def test_discover_mapping_from_csv_parses_dict_body(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Handle responses that already present body as a JSON object."""

    payload = {
        "schema": "sage_rnaseq",
        "columns": [
            {
                "source_column": "a",
                "targets": [
                    {"field": "biospecimen.sample_id", "score": 0.88},
                ],
            }
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="sage_rnaseq",
        target_version="v1",
    )

    column_mappings = manifest.get("column_mappings", {})
    assert column_mappings["a"]["targetField"] == "biospecimen.sample_id"


def test_discover_mapping_from_csv_samples_csv_data(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """CSV convenience wrapper derives samples and forwards to discovery API."""

    payload = {
        "body": json.dumps({"target_schema": "ccdi", "recommendations": []}),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    body = cast(dict[str, object], json.loads(cast(str, content["body"])))
    data_section = cast(dict[str, object], body.get("data", {}))
    assert any(column in data_section for column in ("a", "b", "c"))
    assert isinstance(manifest.get("column_mappings"), dict)


def test_discover_mapping_from_csv_handles_api_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise MappingDiscoveryError when the API returns a non-2xx status."""

    capture = json_failure({"message": "unsupported schema"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping_from_csv(
            source_csv=sample_csv_path,
            target_schema="bogus",
            target_version="v1",
        )

    assert "unsupported schema" in str(exc.value)


def test_discover_mapping_from_csv_raises_on_transport_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise NetriasAPIUnavailable when transport fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = configured_client.discover_mapping_from_csv(
            source_csv=sample_csv_path,
            target_schema="ccdi",
            target_version="v1",
        )


@pytest.mark.asyncio
async def test_discover_mapping_from_csv_async(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async variant yields the same structure as the sync function."""

    payload = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "target_schema": "ccdi",
                "recommendations": [
                    {"column": "a", "suggestions": []},
                ],
            }
        ),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_csv_async(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
    )
    column_mappings = manifest.get("column_mappings", {})
    assert isinstance(column_mappings, dict)


def test_discover_mapping_from_csv_handles_new_results_dict_format(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Handle the new dict-keyed results format with similarity and target_cde_id.

    'why': the updated CDE recommendation API returns results keyed by column name
    """

    payload = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "target_schema": "gc",
                "target_version": "v1",
                "target_columns": 40,
                "target_rows": 188,
                "results": {
                    "a": [
                        {"target": "age", "target_cde_id": 900, "similarity": 1.0},
                        {"target": "ageUnit", "target_cde_id": 904, "similarity": 0.1},
                    ],
                    "b": [
                        {"target": "sex", "target_cde_id": 901, "similarity": 0.95},
                    ],
                },
            }
        ),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="gc",
        target_version="v1",
    )

    # Then: the manifest should have mapped columns based on similarity threshold
    column_mappings = manifest.get("column_mappings", {})
    assert "a" in column_mappings
    assert column_mappings["a"]["targetField"] == "age"
    assert "b" in column_mappings
    assert column_mappings["b"]["targetField"] == "sex"


def test_discover_mapping_from_csv_sends_top_k_parameter(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Verify top_k parameter is included in the request payload.

    'why': callers need to limit recommendation count per column
    """

    payload = {
        "body": json.dumps({"target_schema": "gc", "recommendations": []}),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.discover_mapping_from_csv(
        source_csv=sample_csv_path,
        target_schema="gc",
        target_version="v1",
        top_k=5,
    )

    # Then: the request body should include top_k
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    body = cast(dict[str, object], json.loads(cast(str, content["body"])))
    assert body.get("top_k") == 5


@pytest.mark.asyncio
async def test_discover_mapping_from_csv_async_includes_version(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Async CSV discovery wrapper includes target_version in request.

    'why': ensure async variant sends version to discovery API
    """

    payload = {
        "body": json.dumps({"target_schema": "ccdi", "recommendations": []}),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_from_csv_async(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        target_version="v1",
        sample_limit=1,
    )

    assert isinstance(manifest.get("column_mappings"), dict)
    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    body = cast(dict[str, object], json.loads(cast(str, content["body"])))
    assert body.get("target_version") == "v1"
