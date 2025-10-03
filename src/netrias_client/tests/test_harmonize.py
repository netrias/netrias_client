"""Exercise harmonization workflows against mocked transports.

'why': validate success, failure, and transport error handling end-to-end
"""
from __future__ import annotations

from pathlib import Path

import httpx
import pytest

from netrias_client import harmonize
from netrias_client._errors import NetriasAPIUnavailable
from netrias_client._models import HarmonizationResult

from ._utils import install_mock_transport, json_failure, streaming_success, transport_error


@pytest.mark.usefixtures("configured_client")
def test_harmonize_streaming_success(
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Write harmonized output and return a success result when the API streams CSV bytes.

    'why': confirm the happy path covers validation, HTTP, and disk IO coherently
    """

    capture = streaming_success(chunks=(b"col1,col2\n", b"7,8\n"))
    install_mock_transport(monkeypatch, capture)

    # Given valid inputs and a successful transport response
    # When harmonize executes
    result: HarmonizationResult = harmonize(
        source_path=sample_csv_path,
        manifest_path=sample_manifest_path,
        output_path=output_directory,
    )

    # Then the harmonized file is written and the result reports success
    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "succeeded"
    assert result.file_path == expected_output
    assert expected_output.exists()
    assert expected_output.read_text(encoding="utf-8") == "col1,col2\n7,8\n"
    assert len(capture.requests) == 1
    request = capture.requests[0]
    assert request.method == "POST"
    assert request.url.path.endswith("/v1/harmonization/run")
    assert request.headers.get("authorization") == "Bearer test-api-key"


@pytest.mark.usefixtures("configured_client")
def test_harmonize_handles_api_failure(
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Return a failed HarmonizationResult when the API responds with an error payload.

    'why': ensure error messaging propagates without raising
    """

    capture = json_failure({"message": "invalid mapping"}, status_code=400)
    install_mock_transport(monkeypatch, capture)

    # Given valid inputs but a failing API response
    # When harmonize executes
    result: HarmonizationResult = harmonize(
        source_path=sample_csv_path,
        manifest_path=sample_manifest_path,
        output_path=output_directory,
    )

    # Then the result surfaces the failure message and no file is written
    expected_output = output_directory / "sample.harmonized.csv"
    assert result.status == "failed"
    assert result.description == "invalid mapping"
    assert result.file_path == expected_output
    assert not expected_output.exists()
    assert len(capture.requests) == 1


@pytest.mark.usefixtures("configured_client")
def test_harmonize_raises_on_transport_error(
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise NetriasAPIUnavailable when the transport layer fails.

    'why': bubble critical network failures to the caller immediately
    """

    capture = transport_error(httpx.ConnectError("boom"))
    install_mock_transport(monkeypatch, capture)

    # Given valid inputs but the transport raises
    # When harmonize executes
    with pytest.raises(NetriasAPIUnavailable):
        _ = harmonize(
            source_path=sample_csv_path,
            manifest_path=sample_manifest_path,
            output_path=output_directory,
        )

    # Then the request attempt was recorded once
    assert len(capture.requests) == 1
