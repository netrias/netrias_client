"""Test validation behaviour prior to network interaction.

'why': guarantee inputs fail fast with clear exceptions
"""
from __future__ import annotations

import os
from pathlib import Path

import pytest

from netrias_client import NetriasClient
from netrias_client._errors import FileValidationError, OutputLocationError
from netrias_client._validators import HARD_MAX_CSV_BYTES, validate_column_samples


def test_missing_source_file_raises(
    configured_client: NetriasClient,
    sample_manifest_path: Path,
    output_directory: Path,
) -> None:
    """Raise FileValidationError when the source CSV is absent.

    'why': prevent network calls when the input file cannot be found
    """

    missing_path = sample_manifest_path.with_name("does_not_exist.csv")

    # Given a missing CSV path
    # When harmonize executes
    with pytest.raises(FileValidationError) as exc:
        _ = configured_client.harmonize(
            source_path=missing_path,
            manifest=sample_manifest_path,
            data_commons_key="ccdi",
            output_path=output_directory,
        )

    # Then the error mentions the missing file
    assert "not found" in str(exc.value)


def test_directory_source_path_rejected(
    configured_client: NetriasClient,
    sample_manifest_path: Path,
    output_directory: Path,
) -> None:
    """Reject directory paths for the source CSV input.

    'why': enforce file-only inputs before uploading
    """

    directory_path = sample_manifest_path.parent

    # Given a directory in place of a CSV file
    # When harmonize executes
    with pytest.raises(FileValidationError) as exc:
        _ = configured_client.harmonize(
            source_path=directory_path,
            manifest=sample_manifest_path,
            data_commons_key="ccdi",
            output_path=output_directory,
        )

    # Then the error clarifies the path is not a file
    assert "not a file" in str(exc.value)


def test_invalid_source_extension_rejected(
    configured_client: NetriasClient,
    sample_manifest_path: Path,
    output_directory: Path,
    tmp_path: Path,
) -> None:
    """Reject non-CSV source files.

    'why': prevent unsupported file formats from reaching the API
    """

    wrong_extension = tmp_path / "input.txt"
    _ = wrong_extension.write_text("data", encoding="utf-8")

    # Given a non-CSV source path
    # When harmonize executes
    with pytest.raises(FileValidationError) as exc:
        _ = configured_client.harmonize(
            source_path=wrong_extension,
            manifest=sample_manifest_path,
            data_commons_key="ccdi",
            output_path=output_directory,
        )

    # Then the error references the unsupported extension
    assert "extension" in str(exc.value)


def test_source_file_too_large(
    configured_client: NetriasClient,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    sample_csv_path: Path,
) -> None:
    """Raise when the source CSV exceeds the hard size limit.

    'why': avoid uploading files the API will reject for size reasons
    """

    def fake_getsize(path: Path) -> int:
        if path == sample_csv_path:
            return HARD_MAX_CSV_BYTES + 1
        return os.path.getsize(path)

    monkeypatch.setattr("netrias_client._validators.os.path.getsize", fake_getsize)

    # Given a CSV that appears larger than the limit
    # When harmonize executes
    with pytest.raises(FileValidationError) as exc:
        _ = configured_client.harmonize(
            source_path=sample_csv_path,
            manifest=sample_manifest_path,
            data_commons_key="ccdi",
            output_path=output_directory,
        )

    # Then an explicit size error is raised
    assert "exceeds" in str(exc.value)


def test_manifest_must_be_json(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    output_directory: Path,
    tmp_path: Path,
) -> None:
    """Reject non-JSON manifest paths.

    'why': avoid uploading unsupported manifest formats
    """

    bad_manifest = tmp_path / "manifest.txt"
    _ = bad_manifest.write_text("oops", encoding="utf-8")

    # Given a manifest that is not a .json file
    # When harmonize executes
    with pytest.raises(FileValidationError) as exc:
        _ = configured_client.harmonize(
            source_path=sample_csv_path,
            manifest=bad_manifest,
            data_commons_key="ccdi",
            output_path=output_directory,
        )

    # Then the error highlights the extension issue
    assert "manifest" in str(exc.value)


def test_output_path_existing_file_versioned(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Version output filenames rather than overwriting existing harmonized results.

    'why': ensure callers keep prior outputs while receiving the new artifact
    """

    from ._utils import install_mock_transport, job_success

    existing = output_directory / "sample.harmonized.csv"
    _ = existing.write_text("old data", encoding="utf-8")

    capture = job_success(chunks=(b"col1,col2\n", b"1,2\n"))
    install_mock_transport(monkeypatch, capture)

    result = configured_client.harmonize(
        source_path=sample_csv_path,
        manifest=sample_manifest_path,
        data_commons_key="ccdi",
        output_path=output_directory,
    )

    expected_new = output_directory / "sample.harmonized.v1.csv"
    assert result.status == "succeeded"
    assert result.file_path == expected_new
    assert expected_new.exists()
    assert expected_new.read_text(encoding="utf-8") == "col1,col2\n1,2\n"
    assert existing.read_text(encoding="utf-8") == "old data"
    assert len(capture.requests) == 3


def test_output_directory_must_be_writable(
    configured_client: NetriasClient,
    sample_csv_path: Path,
    sample_manifest_path: Path,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Raise OutputLocationError when the destination directory is not writable.

    'why': guard against unwritable destinations before network calls
    """

    parent = output_directory

    def fake_access(path: Path, mode: int) -> bool:
        if path == parent and mode == os.W_OK:
            return False
        return os.access(path, mode)

    monkeypatch.setattr("netrias_client._validators.os.access", fake_access)

    target = output_directory / "custom.csv"

    # Given an unwritable output directory
    # When harmonize executes
    with pytest.raises(OutputLocationError) as exc:
        _ = configured_client.harmonize(
            source_path=sample_csv_path,
            manifest=sample_manifest_path,
            data_commons_key="ccdi",
            output_path=target,
        )

    # Then an OutputLocationError is raised with a helpful message
    assert "not writable" in str(exc.value)


def test_column_samples_are_deduplicated() -> None:
    """Column samples are deduplicated while preserving order.

    'why': duplicate values waste API bandwidth without improving recommendations
    """

    # Given column samples with duplicates
    columns = {"col": ["a", "b", "a", "c", "b", "a"]}

    # When validating the samples
    result = validate_column_samples(columns)

    # Then duplicates are removed and order is preserved
    assert result["col"] == ["a", "b", "c"]
