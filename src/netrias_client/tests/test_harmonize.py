"""Exercise harmonization workflows against mocked transports.

'why': validate success, failure, and transport error handling end-to-end
"""
from __future__ import annotations

import json
from pathlib import Path
from typing import cast

import httpx
import pytest

from netrias_client import NetriasClient
from netrias_client._errors import NetriasAPIUnavailable
from netrias_client._models import HarmonizationResult

from ._utils import install_mock_transport, job_success, json_failure, transport_error


def test_harmonize_streaming_success(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write harmonized output and return a success result when the API streams CSV bytes."""

    capture = job_success(chunks=(b"col1,col2\n", b"7,8\n"))
    install_mock_transport(monkeypatch, capture)

    result: HarmonizationResult = configured_client.harmonize(
        source_path=sample_csv_path,
        manifest=sample_manifest_path,
        output_path=output_directory,
    )

    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "succeeded"
    assert result.file_path == expected_output
    assert expected_output.exists()
    assert expected_output.read_text(encoding="utf-8") == "col1,col2\n7,8\n"
    assert len(capture.requests) == 3
    submit_request = capture.requests[0]
    poll_request = capture.requests[1]
    final_request = capture.requests[2]

    assert submit_request.method == "POST"
    assert submit_request.url.path.endswith("/v1/jobs/harmonize")
    assert submit_request.headers.get("x-api-key") == "test-api-key"
    assert poll_request.method == "GET"
    assert "/v1/jobs/" in poll_request.url.path
    assert final_request.method == "GET"


def test_harmonize_handles_api_failure(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return a failed HarmonizationResult when the API responds with an error payload."""

    capture = json_failure({"message": "invalid mapping"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    result: HarmonizationResult = configured_client.harmonize(
        source_path=sample_csv_path,
        manifest=sample_manifest_path,
        output_path=output_directory,
    )

    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "failed"
    assert result.description == "invalid mapping"
    assert result.file_path == expected_output
    assert not expected_output.exists()
    assert len(capture.requests) == 1


def test_harmonize_raises_on_transport_error(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise NetriasAPIUnavailable when the transport layer fails."""

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable):
        _ = configured_client.harmonize(
            source_path=sample_csv_path,
            manifest=sample_manifest_path,
            output_path=output_directory,
        )

    assert len(capture.requests) == 1


def test_harmonize_accepts_manifest_mapping(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_mapping: dict[str, object],
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Allow callers to provide manifest data without writing a file."""

    capture = job_success(chunks=(b"col1,col2\n", b"7,8\n"))
    install_mock_transport(monkeypatch, capture)

    result: HarmonizationResult = configured_client.harmonize(
        source_path=sample_csv_path,
        manifest=sample_manifest_mapping,
        output_path=output_directory,
    )

    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "succeeded"
    assert result.file_path == expected_output
    assert expected_output.exists()


def test_harmonize_writes_manifest_when_requested(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_mapping: dict[str, object],
    output_directory: Path,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Persist manifest data to disk when a destination path is supplied."""

    capture = job_success(chunks=(b"col1,col2\n", b"7,8\n"))
    install_mock_transport(monkeypatch, capture)

    manifest_output = tmp_path / "manifest.json"

    _ = configured_client.harmonize(
        source_path=sample_csv_path,
        manifest=sample_manifest_mapping,
        output_path=output_directory,
        manifest_output_path=manifest_output,
    )

    assert manifest_output.exists()
    loaded = cast(dict[str, object], json.loads(manifest_output.read_text(encoding="utf-8")))
    assert loaded == sample_manifest_mapping


@pytest.mark.asyncio
async def test_harmonize_async_success(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Async client method returns a successful result."""

    capture = job_success(chunks=(b"col1,col2\n", b"7,8\n"))
    install_mock_transport(monkeypatch, capture)

    result = await configured_client.harmonize_async(
        source_path=sample_csv_path,
        manifest=sample_manifest_path,
        output_path=output_directory,
    )

    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "succeeded"
    assert result.file_path == expected_output
    assert expected_output.exists()
