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
from openpyxl import Workbook, load_workbook
from openpyxl.worksheet.worksheet import Worksheet

from netrias_client import (
    ColumnKeyedManifestPayload,
    NetriasClient,
    TabularFormat,
    dataset_from_rows,
    list_workbook_sheets,
    read_tabular,
    write_tabular,
)

from ._utils import install_mock_transport, job_success, json_success


def _active_sheet(workbook: Workbook) -> Worksheet:
    return cast(Worksheet, workbook.active)


def _create_sheet(workbook: Workbook, title: str) -> Worksheet:
    return cast(Worksheet, workbook.create_sheet(title))


def _workbook_cell_value(path: Path, sheet_name: str, cell: str) -> object:
    workbook = load_workbook(path, data_only=True)
    sheet = cast(Worksheet, workbook[sheet_name])
    return cast(object, sheet[cell].value)


def _array_payload(results: list[dict[str, object]]) -> dict[str, object]:
    return {
        "statusCode": 200,
        "body": json.dumps({"results": results}),
    }


def test_read_tabular_preserves_duplicate_tsv_headers() -> None:
    """A TSV with duplicate headers keeps separate positional column keys."""

    # Given: a TSV fixture with duplicate display headers and comma-containing values
    source = Path(__file__).parent / "fixtures" / "duplicate_headers.tsv"
    assert "," in source.read_text(encoding="utf-8")

    # When: the SDK reads it as tabular data
    dataset = read_tabular(source)

    # Then: identity is positional and data is not split on commas
    assert dataset.source_format == TabularFormat.TSV
    assert [column.key for column in dataset.columns] == ["col_0000", "col_0001", "col_0002"]
    assert dataset.headers == ["name", "name", "note"]
    assert dataset.rows == [["Alice", "Smith", "keeps, comma"]]


def test_read_tabular_selects_xlsx_sheet_and_preserves_duplicate_headers(tmp_path: Path) -> None:
    """XLSX input is read from the selected worksheet without collapsing duplicate headers."""

    # Given: a workbook with two sheets and duplicate headers on the second sheet
    source = tmp_path / "workbook.xlsx"
    workbook = Workbook()
    first = _active_sheet(workbook)
    first.title = "First"
    first.append(["ignored"])
    first.append(["nope"])
    second = _create_sheet(workbook, "Patients")
    second.append(["name", "name", "note"])
    second.append(["Alice", "Smith", "keeps, comma"])
    workbook.save(source)
    assert list_workbook_sheets(source)[0].name == "First"

    # When: the second sheet is selected
    dataset = read_tabular(source, sheet_name="Patients")

    # Then: values come from that sheet and duplicate headers keep distinct keys
    assert dataset.source_format == TabularFormat.XLSX
    assert dataset.sheet_name == "Patients"
    assert dataset.headers == ["name", "name", "note"]
    assert [column.key for column in dataset.columns] == ["col_0000", "col_0001", "col_0002"]
    assert dataset.rows == [["Alice", "Smith", "keeps, comma"]]


def test_write_tabular_xlsx_updates_selected_sheet_and_keeps_other_sheets(tmp_path: Path) -> None:
    """XLSX output updates only the selected sheet when a template workbook is provided."""

    # Given: a template workbook with a sheet that should remain untouched
    template = tmp_path / "source.xlsx"
    output = tmp_path / "output.xlsx"
    workbook = Workbook()
    first = _active_sheet(workbook)
    first.title = "Keep"
    first.append(["status"])
    first.append(["unchanged"])
    selected = _create_sheet(workbook, "Patients")
    selected.append(["name", "note"])
    selected.append(["old", "old note"])
    workbook.save(template)
    dataset = dataset_from_rows(
        headers=["name", "note"],
        rows=[["Alice", "updated"]],
        source_format=TabularFormat.XLSX,
        sheet_name="Patients",
    )

    # When: the dataset is written back as XLSX
    write_tabular(output, dataset, template_path=template)

    # Then: the selected sheet is replaced and the other sheet stays present
    output_workbook = load_workbook(output, data_only=True)
    assert output_workbook.sheetnames == ["Keep", "Patients"]
    assert _workbook_cell_value(output, "Keep", "A2") == "unchanged"
    assert _workbook_cell_value(output, "Patients", "A1") == "name"
    assert _workbook_cell_value(output, "Patients", "A2") == "Alice"


def test_discover_mapping_from_tabular_returns_column_keyed_manifest(
    configured_client: NetriasClient,
    monkeypatch: pytest.MonkeyPatch,
    duplicate_headers_tsv_path: Path,
) -> None:
    """Discovery exposes stable column keys while sending unique backend names."""

    # Given: a duplicate-header TSV and a backend response using SDK-generated names
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
            {"column_name": "col_0002__note", "matches": []},
        ]
    )
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    # When: discovery runs through the tabular API
    manifest = configured_client.discover_mapping_from_tabular(
        source_path=duplicate_headers_tsv_path,
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
    data = cast(dict[str, list[str]], content["data"])
    assert list(data) == [
        "col_0000__name",
        "col_0001__name",
        "col_0002__note",
    ]


def test_harmonize_tsv_writes_tsv_output(
    configured_client: NetriasClient,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    sample_tsv_path: Path,
) -> None:
    """TSV input produces TSV output while the remote CSV stream is converted."""

    # Given: a TSV source and a column-keyed manifest
    manifest: ColumnKeyedManifestPayload = {
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
        source_path=sample_tsv_path,
        manifest=manifest,
        data_commons_key="ccdi",
        output_path=output_directory,
    )

    # Then: the downloaded CSV stream is written back as TSV
    expected = output_directory / "sample.harmonized.tsv"
    assert result.file_path == expected
    assert expected.read_text(encoding="utf-8") == "name\tnote\nAlice\tkeeps, comma\n"

    submit_request = capture.requests[0]
    envelope = cast(dict[str, object], json.loads(gzip.decompress(submit_request.content).decode("utf-8")))
    document = cast(dict[str, object], envelope["document"])
    assert document["header"] == ["name", "note"]
    assert document["rows"] == [["Alice", "keeps, comma"]]
    column_mappings = cast(list[dict[str, object]], envelope["column_mappings"])
    assert column_mappings[0]["cde_key"] == "first_name"


def test_harmonize_xlsx_writes_xlsx_output_for_selected_sheet(
    configured_client: NetriasClient,
    output_directory: Path,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    """XLSX input produces XLSX output while updating only the selected worksheet."""

    # Given: an XLSX source with two sheets and a column-keyed manifest
    source = tmp_path / "patients.xlsx"
    workbook = Workbook()
    keep = _active_sheet(workbook)
    keep.title = "Keep"
    keep.append(["status"])
    keep.append(["unchanged"])
    patients = _create_sheet(workbook, "Patients")
    patients.append(["name", "note"])
    patients.append(["Alice", "old"])
    workbook.save(source)
    manifest: ColumnKeyedManifestPayload = {
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
    capture = job_success(chunks=(b"name,note\nAlice,updated\n",))
    install_mock_transport(monkeypatch, capture)

    # When: harmonization runs for the selected sheet
    result = configured_client.harmonize(
        source_path=source,
        manifest=manifest,
        data_commons_key="ccdi",
        output_path=output_directory,
        sheet_name="Patients",
    )

    # Then: the output remains XLSX and only the selected sheet is updated
    expected = output_directory / "patients.harmonized.xlsx"
    assert result.file_path == expected
    output_workbook = load_workbook(expected, data_only=True)
    assert output_workbook.sheetnames == ["Keep", "Patients"]
    assert _workbook_cell_value(expected, "Keep", "A2") == "unchanged"
    assert _workbook_cell_value(expected, "Patients", "B2") == "updated"

    submit_request = capture.requests[0]
    envelope = cast(dict[str, object], json.loads(gzip.decompress(submit_request.content).decode("utf-8")))
    document = cast(dict[str, object], envelope["document"])
    assert document["sheetName"] == "Patients"
    assert document["rows"] == [["Alice", "old"]]
