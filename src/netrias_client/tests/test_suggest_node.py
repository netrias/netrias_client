"""Test suggest_node — classify a harmonized CSV's columns by model node.

'why': verify node classification handles multi-node CDEs, silently
excludes schema CDEs the harmonized file doesn't touch, warns on a 
harmonized column matching no CDE in the schema,and every validation 
guard raises on the exact condition it claims to.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from netrias_client._suggest_node import suggest_node

ModelJson = dict[str, dict[str, dict[str, object]]]


@pytest.fixture
def model_json() -> ModelJson:
    """CTDC-shaped model with a CDE ('shared_id') deliberately shared across
    two nodes, and one node ('assay') the harmonized fixture never touches.
    """
    return {
        "demographic": {
            "ethnicity": {"Enum": ["Hispanic or Latino"], "Req": True, "Type": "list"},
            "sex": {"Enum": ["Male", "Female"], "Req": True, "Type": "string"},
            "race": {"Enum": [], "Req": True, "Type": "list"},
            "shared_id": {"Enum": [], "Req": True, "Type": "string"},
        },
        "diagnosis": {
            "primary_disease_site": {"Enum": [], "Req": True, "Type": "list"},
            "stage_of_disease": {"Enum": [], "Req": "Preferred", "Type": "list"},
            "shared_id": {"Enum": [], "Req": True, "Type": "string"},
        },
        "assay": {
            "assay_type": {"Enum": [], "Req": True, "Type": "string"},
        },
    }


@pytest.fixture
def workspace(tmp_path: Path, model_json: ModelJson) -> Path:
    """Write model_json to CTDC/CTDC.json and a harmonized CSV
    touching demographic + diagnosis (via shared_id) but not assay, plus
    one column that matches no CDE in the schema at all.
    """
    dm_outputs_dir = tmp_path / "CTDC"
    dm_outputs_dir.mkdir(parents=True)
    _ = (dm_outputs_dir / "CTDC.json").write_text(json.dumps(model_json), encoding="utf-8")

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _ = (output_dir / "harmonized.csv").write_text(
        "ethnicity,sex,primary_disease_site,shared_id,some_unrelated_col\n"
        + "Hispanic or Latino,Male,Lung,ID001,foo\n",
        encoding="utf-8",
    )

    return tmp_path


def test_classifies_touched_nodes_exactly(workspace: Path) -> None:
    """Result contains exactly the nodes the harmonized CSV touches, with
    exactly the matched CDEs under each — not a superset, not a subset."""

    # Given: harmonized.csv has ethnicity, sex (demographic) and
    # primary_disease_site (diagnosis), plus shared_id (both), and
    # some_unrelated_col (matches nothing)

    # When: suggest_node classifies the harmonized CSV
    result = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",    )

    # Then: exactly demographic and diagnosis are present, each with its
    # exact matched CDE set — assay is absent entirely (zero matches)
    assert set(result.keys()) == {"demographic", "diagnosis"}
    assert set(result["demographic"]) == {"ethnicity", "sex", "shared_id"}
    assert set(result["diagnosis"]) == {"primary_disease_site", "shared_id"}


def test_shared_cde_listed_under_every_matching_node(workspace: Path) -> None:
    """A CDE belonging to more than one node is attributed to all of them,
    not arbitrarily assigned to just one."""

    # Given: shared_id is defined under both demographic and diagnosis in model_json

    # When: suggest_node classifies the harmonized CSV
    result = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",
    )

    # Then: shared_id appears in both node lists, each exactly once
    assert result["demographic"].count("shared_id") == 1
    assert result["diagnosis"].count("shared_id") == 1


def test_excludes_cdes_absent_from_harmonized_csv(workspace: Path) -> None:
    """CDEs defined on a matched node but not present in the harmonized
    CSV are not pulled in just because their node matched."""

    # Given: race (demographic) and stage_of_disease (diagnosis) are real
    # CDEs on matched nodes, but neither is a column in harmonized.csv

    # When: suggest_node classifies the harmonized CSV
    result = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",
    )

    # Then: neither CDE appears anywhere in the result
    all_matched = {cde for cdes in result.values() for cde in cdes}
    assert "race" not in all_matched
    assert "stage_of_disease" not in all_matched


def test_unmatched_column_excluded_and_warned(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """A harmonized column matching no CDE in the schema is excluded from
    the result and triggers a printed warning naming that exact column."""

    # Given: some_unrelated_col is in harmonized.csv but not in model_json at all

    # When: suggest_node classifies the harmonized CSV
    result = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",
    )

    # Then: it's absent from every node's list, and a warning names it specifically
    all_matched = {cde for cdes in result.values() for cde in cdes}
    assert "some_unrelated_col" not in all_matched

    captured = capsys.readouterr()
    assert "some_unrelated_col" in captured.out
    assert "ctdc" in captured.out


def test_node_with_zero_matches_is_absent(workspace: Path) -> None:
    """A node whose CDEs are entirely untouched by the harmonized CSV is
    omitted from the result rather than included with an empty list."""

    # Given: assay_type (assay's only CDE) is not a column in harmonized.csv

    # When: suggest_node classifies the harmonized CSV
    result = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",
    )

    # Then: assay is not a key in the result at all
    assert "assay" not in result


def test_missing_harmonized_csv_raises_file_not_found(workspace: Path) -> None:
    """A nonexistent harmonized CSV path raises FileNotFoundError naming that path."""
    
    # Given: the path does not exist on disk
    missing_path = workspace / "output" / "does_not_exist.csv"
    assert not missing_path.exists()

    # When / Then: suggest_node raises FileNotFoundError naming the path
    with pytest.raises(FileNotFoundError, match="does_not_exist.csv"):
        _ = suggest_node(
            harmonized_csv_path=missing_path,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "suggested_nodes.json",
        )


def test_empty_harmonized_csv_raises_value_error(workspace: Path) -> None:
    """A completely empty harmonized CSV file raises ValueError for having no header row."""

    # Given: a CSV file that exists but has zero bytes
    empty_csv = workspace / "output" / "empty.csv"
    _ = empty_csv.write_text("", encoding="utf-8")

    # When / Then: suggest_node raises ValueError about the missing header row
    with pytest.raises(ValueError, match="no usable header row"):
        _ = suggest_node(
            harmonized_csv_path=empty_csv,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "suggested_nodes.json",
        )


def test_blank_first_row_raises_value_error(workspace: Path) -> None:
    """A harmonized CSV whose first row is blank (no columns) fails the same way as a fully empty file."""

    # Given: a CSV whose first row is present but empty (no columns at all)
    blank_header_csv = workspace / "output" / "blank_header.csv"
    _ = blank_header_csv.write_text("\ndata\n", encoding="utf-8")

    # When / Then: suggest_node raises the same ValueError as a fully empty file
    with pytest.raises(ValueError, match="no usable header row"):
        _ = suggest_node(
            harmonized_csv_path=blank_header_csv,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "suggested_nodes.json",
        )


@pytest.mark.parametrize("bad_key", ["", "   ", "not_a_real_schema", "ccdi"])
def test_unsupported_target_schema_raises_value_error(workspace: Path, bad_key: str) -> None:
    """A target_schema that is empty, whitespace, or not one of the 4 supported 
    schemas raises ValueError naming the valid options."""
    
    # Given: a target_schema that is empty, whitespace, or not one of the 4 supported schemas

    # When / Then: suggest_node rejects it before any file lookup, naming the valid options
    with pytest.raises(ValueError, match="must be one of"):
        _ = suggest_node(
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            target_schema=bad_key,
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "suggested_nodes.json",
        )


def test_summary_counts_mapped_and_unmapped(workspace: Path, capsys: pytest.CaptureFixture[str]) -> None:
    """Printed summary reports correct total/mapped/unmapped counts."""

    # Given: harmonized.csv has 5 columns — 4 map to CDEs (ethnicity, sex,
    # primary_disease_site, shared_id), 1 does not (some_unrelated_col)

    # When: suggest_node classifies the harmonized CSV
    _ = suggest_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "suggested_nodes.json",
    )

    # Then: summary counts match exactly
    captured = capsys.readouterr()
    assert "Total CDEs in harmonized CSV: 5" in captured.out
    assert "Mapped: 4" in captured.out
    assert "Unmapped: 1" in captured.out
