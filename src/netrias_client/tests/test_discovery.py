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

from netrias_client import (
    discover_mapping,
    discover_mapping_async,
    discover_mapping_from_csv,
)
from netrias_client._errors import MappingDiscoveryError, MappingValidationError, NetriasAPIUnavailable
from netrias_client._models import MappingDiscoveryResult

from ._utils import install_mock_transport, json_failure, json_success, transport_error


def _sample_columns() -> Mapping[str, Sequence[object]]:
    return {
        "sample_name": ["A", "B"],
        "geo_id": ["X", "Y"],
    }


@pytest.mark.usefixtures("configured_client")
def test_discover_mapping_success(monkeypatch: pytest.MonkeyPatch) -> None:
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

    result: MappingDiscoveryResult = discover_mapping(target_schema="ccdi", column_samples=_sample_columns())

    assert result.schema == "ccdi"
    assert len(result.suggestions) == 2
    first = result.suggestions[0]
    assert first.source_column == "sample_name"
    assert first.options[0].target == "Sample.name"
    confidence = first.options[0].confidence or 0.0
    assert abs(confidence - 0.92) < 1e-9

    request = capture.requests[0]
    assert request.headers.get("x-api-key") == "test-api-key"
    payload_raw = cast(object, json.loads(request.content.decode("utf-8")))
    assert isinstance(payload_raw, dict)
    payload_sent = cast(Mapping[str, object], payload_raw)
    body_raw = payload_sent.get("body")
    assert isinstance(body_raw, str)
    assert "\"target_schema\": \"ccdi\"" in body_raw
    assert "sample_name" in body_raw


@pytest.mark.usefixtures("configured_client")
def test_discover_mapping_parses_dict_body(monkeypatch: pytest.MonkeyPatch) -> None:
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

    result = discover_mapping(target_schema="sage_rnaseq", column_samples=_sample_columns())

    assert result.schema == "sage_rnaseq"
    assert len(result.suggestions) == 1
    suggestion = result.suggestions[0]
    assert suggestion.source_column == "sample_name"
    assert suggestion.options[0].target == "biospecimen.sample_id"
    confidence = suggestion.options[0].confidence or 0.0
    assert abs(confidence - 0.88) < 1e-9


@pytest.mark.usefixtures("configured_client")
def test_discover_mapping_from_csv(monkeypatch: pytest.MonkeyPatch, sample_csv_path: Path) -> None:
    """CSV convenience wrapper derives samples and forwards to discovery API."""

    payload = {
        "body": json.dumps({"target_schema": "ccdi", "recommendations": []}),
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = discover_mapping_from_csv(target_schema="ccdi", source_csv=sample_csv_path, sample_limit=1)

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    body = cast(dict[str, object], json.loads(cast(str, content["body"])))
    data_section = cast(dict[str, object], body.get("data", {}))
    assert any(column in data_section for column in ("a", "b", "c"))


@pytest.mark.usefixtures("configured_client")
def test_discover_mapping_handles_api_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise MappingDiscoveryError when the API returns a non-2xx status."""

    capture = json_failure({"message": "unsupported schema"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(MappingDiscoveryError) as exc:
        _ = discover_mapping(target_schema="bogus", column_samples=_sample_columns())

    assert "unsupported schema" in str(exc.value)


def test_discover_mapping_validates_inputs() -> None:
    """Input validation rejects empty column data."""

    with pytest.raises(MappingValidationError):
        _ = discover_mapping(target_schema="ccdi", column_samples={})


@pytest.mark.usefixtures("configured_client")
def test_discover_mapping_raises_on_transport_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise NetriasAPIUnavailable when transport fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = discover_mapping(target_schema="ccdi", column_samples=_sample_columns())


@pytest.mark.asyncio
@pytest.mark.usefixtures("configured_client")
async def test_discover_mapping_async(monkeypatch: pytest.MonkeyPatch) -> None:
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

    result = await discover_mapping_async(target_schema="ccdi", column_samples=_sample_columns())

    assert result.schema == "ccdi"
    assert len(result.suggestions) == 1
