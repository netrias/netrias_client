"""Test mapping discovery workflows.

'why': guarantee recommendation calls integrate cleanly and surface results
"""
from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from typing import cast

import httpx
import pytest

from pathlib import Path

from netrias_client import NetriasClient
from netrias_client._errors import MappingDiscoveryError, MappingValidationError, NetriasAPIUnavailable

from ._utils import install_mock_transport, json_failure, json_success, transport_error


def _sample_columns() -> Mapping[str, Sequence[object]]:
    return {
        "sample_name": ["A", "B"],
        "geo_id": ["X", "Y"],
    }


def test_discover_mapping_success(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return structured recommendations when the API succeeds."""

    payload = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "target_schema": "ccdi",
                "recommendations": [
                    {
                        "column": "sample_name",
                        "suggestions": [
                            {"target": "Sample.name", "confidence": 0.92},
                            {"target": "Sample.display_name", "confidence": 0.5},
                        ],
                    },
                    {
                        "column": "geo_id",
                        "suggestions": [],
                    },
                ],
            }
        ),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping(
        target_schema="ccdi",
        column_samples=_sample_columns(),
    )

    column_mappings = manifest.get("column_mappings", {})
    assert "sample_name" in column_mappings
    assert column_mappings["sample_name"]["targetField"] == "Sample.name"

    request = capture.requests[0]
    assert request.headers.get("x-api-key") == "test-api-key"
    payload_raw = cast(object, json.loads(request.content.decode("utf-8")))
    assert isinstance(payload_raw, dict)
    payload_sent = cast(Mapping[str, object], payload_raw)
    body_raw = payload_sent.get("body")
    assert isinstance(body_raw, str)
    assert "\"target_schema\": \"ccdi\"" in body_raw
    assert "sample_name" in body_raw


def test_discover_mapping_parses_dict_body(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Handle responses that already present body as a JSON object."""

    payload = {
        "schema": "sage_rnaseq",
        "columns": [
            {
                "source_column": "sample_name",
                "targets": [
                    {"field": "biospecimen.sample_id", "score": 0.88},
                ],
            }
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = configured_client.discover_mapping(
        target_schema="sage_rnaseq",
        column_samples=_sample_columns(),
    )

    column_mappings = manifest.get("column_mappings", {})
    assert column_mappings["sample_name"]["targetField"] == "biospecimen.sample_id"


def test_discover_mapping_from_csv(
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

    manifest = configured_client.discover_cde_mapping(
        source_csv=sample_csv_path,
        target_schema="ccdi",
        sample_limit=1,
    )

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    body = cast(dict[str, object], json.loads(cast(str, content["body"])))
    data_section = cast(dict[str, object], body.get("data", {}))
    assert any(column in data_section for column in ("a", "b", "c"))
    assert isinstance(manifest.get("column_mappings"), dict)


def test_discover_mapping_handles_api_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise MappingDiscoveryError when the API returns a non-2xx status."""

    capture = json_failure({"message": "unsupported schema"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = configured_client.discover_mapping(
            target_schema="bogus",
            column_samples=_sample_columns(),
        )

    assert "unsupported schema" in str(exc.value)


def test_discover_mapping_validates_inputs(configured_client: NetriasClient) -> None:
    """Input validation rejects empty column data."""

    with pytest.raises(MappingValidationError):
        _ = configured_client.discover_mapping(target_schema="ccdi", column_samples={})


def test_discover_mapping_raises_on_transport_error(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise NetriasAPIUnavailable when transport fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = configured_client.discover_mapping(
            target_schema="ccdi",
            column_samples=_sample_columns(),
        )


@pytest.mark.asyncio
async def test_discover_mapping_async(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async variant yields the same structure as the sync function."""

    payload = {
        "statusCode": 200,
        "body": json.dumps(
            {
                "target_schema": "ccdi",
                "recommendations": [
                    {"column": "sample_name", "suggestions": []},
                ],
            }
        ),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    manifest = await configured_client.discover_mapping_async(
        target_schema="ccdi",
        column_samples=_sample_columns(),
    )
    column_mappings = manifest.get("column_mappings", {})
    assert isinstance(column_mappings, dict)
