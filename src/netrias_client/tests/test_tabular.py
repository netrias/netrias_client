"""Exercise first-class tabular file handling.

'why': CSV and TSV are edge formats; the SDK should preserve positional column
identity internally so duplicate headers never collapse into dict keys.
"""
from __future__ import annotations

import gzip
import json
from pathlib import Path
from typing import cast

import pytest

from netrias_client import NetriasClient, TabularFormat, read_tabular

from ._utils import install_mock_transport, job_success, json_success


def _array_payload(results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}),
    }


def test_read_tabular_preserves_duplicate_tsv_headers(tmp_path: Path) -> None:
    """A TSV with duplicate headers keeps separate positional column keys."""

    # Given: a TSV with duplicate display headers and comma-containing values
    source = tmp_path / "patients.tsv"
    source.write_text("name\tname\tnote\nAlice\tSmith\tkeeps, comma\n", encoding="utf-8")
    assert "," in source.read_text(encoding="utf-8")

    # When: the SDK reads it as tabular data
    dataset = read_tabular(source)

    # Then: identity is positional and data is not split on commas
    assert dataset.source_format == TabularFormat.TSV
    assert [column.key for column in dataset.columns] == ["col_0000", "col_0001", "col_0002"]
    assert dataset.headers == ["name", "name", "note"]
    assert dataset.rows == [["Alice", "Smith", "keeps, comma"]]


def test_discover_mapping_from_tabular_returns_column_keyed_manifest(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """Discovery exposes stable column keys while sending unique backend names."""

    # Given: a duplicate-header TSV and a backend response using SDK-generated names
    source = tmp_path / "patients.tsv"
    source.write_text("name\tname\nAlice\tSmith\n", encoding="utf-8")
    payload = _array_payload(
        [
            {
                "column_name": "col_0000__name",
                "matches": [
                    {
                        "target": "first_name",
                        "target_cde_id": 10,
                        "confidence": 0.91,
                        "harmonization": "harmonizable",
                    }
                ],
            },
            {
                "column_name": "col_0001__name",
                "matches": [
                    {
                        "target": "last_name",
                        "target_cde_id": 11,
                        "confidence": 0.9,
                        "harmonization": "harmonizable",
                    }
                ],
            },
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When: discovery runs through the tabular API
    manifest = configured_client.discover_mapping_from_tabular(
        source_path=source,
        target_schema="ccdi",
        target_version="v1",
    )

    # Then: callers receive stable source column keys, not backend names
    column_mappings = manifest["column_mappings"]
    assert list(column_mappings) == ["col_0000", "col_0001"]
    assert column_mappings["col_0000"]["cde_key"] == "first_name"
    assert column_mappings["col_0000"]["column_name"] == "name"
    assert column_mappings["col_0001"]["cde_key"] == "last_name"

    request = capture.requests[0]
    content = cast(dict[str, object], json.loads(request.content.decode("utf-8")))
    columns = cast(list[dict[str, object]], content["columns"])
    assert [column["column_name"] for column in columns] == ["col_0000__name", "col_0001__name"]


def test_harmonize_tsv_writes_tsv_output(
    configured_client: NetriasClient,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """TSV input produces TSV output while the remote CSV stream is converted."""

    # Given: a TSV source and a column-keyed manifest
    source = tmp_path / "patients.tsv"
    source.write_text("name\tnote\nAlice\tkeeps, comma\n", encoding="utf-8")
    manifest = {
        "column_mappings": {
            "col_0000": {
                "column_name": "name",
                "cde_key": "first_name",
                "cde_id": 10,
                "harmonization": "harmonizable",
                "alternatives": [],
            }
        }
    }
    capture = job_success(chunks=(b'name,note\nAlice,"keeps, comma"\n',))
    install_mock_transport(monkeypatch, capture)
    assert not (output_directory / "patients.harmonized.tsv").exists()

    # When: harmonization runs
    result = configured_client.harmonize(
        source_path=source,
        manifest=manifest,
        data_commons_key="ccdi",
        output_path=output_directory,
    )

    # Then: the downloaded CSV stream is written back as TSV
    expected = output_directory / "patients.harmonized.tsv"
    assert result.file_path == expected
    assert expected.read_text(encoding="utf-8") == "name\tnote\nAlice\tkeeps, comma\n"

    submit_request = capture.requests[0]
    envelope = json.loads(gzip.decompress(submit_request.content).decode("utf-8"))
    document = cast(dict[str, object], envelope["document"])
    assert document["header"] == ["name", "note"]
    assert document["rows"] == [["Alice", "keeps, comma"]]
    assert envelope["column_mappings"][0]["cde_key"] == "first_name"
