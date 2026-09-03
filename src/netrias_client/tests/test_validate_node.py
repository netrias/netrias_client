"""Test validate_node — chunk a harmonized CSV by node and validate each
node's sheet against its schema.

'why': covers every function in the module individually (pure unit tests
for the check_* functions, no filesystem involved) plus file-based tests
for the path/CSV/JSON helpers and the two orchestrators
(chunk_by_node, validate_node, chunk_and_validate).
"""
from __future__ import annotations

from netrias_client._models import CDEProps, ValidationReport

import json
from pathlib import Path
from typing import cast

import pytest

from netrias_client._validate_node import (
    _FkColumnResult,
    _check_fk_column_values,
    _check_path,
    _evaluate_fk_column,
    _load_model_json,
    _load_relationships,
    _matches_boolean_type,
    _matches_datetime_type,
    _matches_numeric_type,
    _matches_pattern_type,
    _matches_semantic_type,
    _read_csv_rows,
    _should_check_type,
    _summarize_fk_results,
    _value_matches_type,
    check_foreign_keys,
    check_required_cdes,
    check_required_values,
    check_type_values,
    chunk_and_validate,
    chunk_by_node,
    validate_node,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def diagnosis_cdes() -> dict[str, CDEProps]:
    """CTDC-shaped model with a CDE ('shared_id') deliberately shared across
    two nodes, and one node ('assay') the harmonized fixture never touches.
    """
    return {
        "study_diagnosis_id": {"Enum": [], "Req": True, "Type": "string"},
        "primary_diagnosis": {"Enum": [], "Req": True, "Type": None},
        "age_at_diagnosis": {"Enum": [], "Req": False, "Type": "number"},
        "diagnosis_count": {"Enum": [], "Req": False, "Type": "integer"},
        "is_relapsed": {"Enum": [], "Req": False, "Type": "boolean"},
        "diagnosis_date": {"Enum": [], "Req": False, "Type": "datetime"},
        "study_accession": {"Enum": [], "Req": False, "Type": "^phs[0-9]+$"},
        "undefined_field": {"Enum": [], "Req": False, "Type": "TBD"},
        "participant.study_participant_id": {"Enum": [], "Req": True, "Type": "string"},
        "sample.sample_id": {"Enum": [], "Req": False, "Type": "string"},
    }


@pytest.fixture
def workspace(tmp_path: Path, diagnosis_cdes: dict[str, CDEProps]) -> Path:
    """Write CTDC.json (diagnosis node only), a harmonized CSV touching
    every diagnosis CDE except sample.sample_id (the omitted optional FK),
    and a dummy source CSV (needed by chunk_and_validate's source_path param)."""
    model_json = {"diagnosis": diagnosis_cdes}
    ctdc_dir = tmp_path / "CTDC"
    ctdc_dir.mkdir(parents=True)
    _ = (ctdc_dir / "CTDC.json").write_text(json.dumps(model_json), encoding="utf-8")
    _ = (ctdc_dir / "relationships.json").write_text(
        json.dumps({"diagnosis": ["participant", "sample"]}), encoding="utf-8"
    )

    output_dir = tmp_path / "output"
    output_dir.mkdir()
    _ = (output_dir / "harmonized.csv").write_text(
        "study_diagnosis_id,primary_diagnosis,age_at_diagnosis,diagnosis_count,"
        + "is_relapsed,diagnosis_date,study_accession,undefined_field,"
        + "participant.study_participant_id\n"
        + "SID1,Lung Cancer,45,2,true,2024-01-15,phs001234,foo,P001\n"
        + "SID2,Breast Cancer,abc,1,maybe,not-a-date,phs00abc,bar,P999\n",
        encoding="utf-8",
    )

    parent_dir = output_dir / "node_sheets"
    parent_dir.mkdir()
    _ = (parent_dir / "participant.csv").write_text(
        "study_participant_id\nP001\nP002\n", encoding="utf-8"
    )

    # 'why': chunk_and_validate takes source_path for naming the run folder
    _ = (tmp_path / "my_source_data.csv").write_text("a\n1\n", encoding="utf-8")

    return tmp_path


# ---------------------------------------------------------------------------
# _check_path
# ---------------------------------------------------------------------------

def test_check_path_missing_raises_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError, match="does not exist"):
        _check_path(tmp_path / "nope", "file")


def test_check_path_expects_file_but_is_dir_raises(tmp_path: Path) -> None:
    with pytest.raises(IsADirectoryError):
        _check_path(tmp_path, "file")


def test_check_path_expects_dir_but_is_file_raises(tmp_path: Path) -> None:
    a_file = tmp_path / "f.txt"
    _ = a_file.write_text("x")
    with pytest.raises(NotADirectoryError):
        _check_path(a_file, "dir")


def test_check_path_valid_file_passes(tmp_path: Path) -> None:
    a_file = tmp_path / "f.txt"
    _ = a_file.write_text("x")
    _check_path(a_file, "file")  # no raise


# ---------------------------------------------------------------------------
# _read_csv_rows
# ---------------------------------------------------------------------------

def test_read_csv_rows_returns_header_and_rows(tmp_path: Path) -> None:
    csv_path = tmp_path / "x.csv"
    _ = csv_path.write_text("a,b\n1,2\n3,4\n", encoding="utf-8")

    header, rows = _read_csv_rows(csv_path)

    assert header == ["a", "b"]
    assert rows == [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]


def test_read_csv_rows_empty_file_raises_value_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "empty.csv"
    _ = csv_path.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="no usable header row"):
        _ = _read_csv_rows(csv_path)


def test_read_csv_rows_missing_file_raises_file_not_found(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _ = _read_csv_rows(tmp_path / "nope.csv")


def test_read_csv_rows_blank_first_row_raises_value_error(tmp_path: Path) -> None:
    csv_path = tmp_path / "blank_header.csv"
    _ = csv_path.write_text("\ndata\n", encoding="utf-8")

    with pytest.raises(ValueError, match="no usable header row"):
        _ = _read_csv_rows(csv_path)


# ---------------------------------------------------------------------------
# _load_model_json
# ---------------------------------------------------------------------------

def test_load_model_json_returns_parsed_json(workspace: Path) -> None:
    model_json = _load_model_json("ctdc", workspace)
    assert "diagnosis" in model_json


def test_load_model_json_unsupported_schema_raises_value_error(workspace: Path) -> None:
    with pytest.raises(ValueError, match="must be one of"):
        _ = _load_model_json("not_a_schema", workspace)


def test_load_model_json_missing_root_raises(workspace: Path) -> None:
    with pytest.raises((FileNotFoundError, NotADirectoryError)):
        _ = _load_model_json("ctdc", workspace / "does_not_exist")


def test_load_model_json_missing_file_raises_file_not_found(tmp_path: Path) -> None:
    (tmp_path / "CTDC").mkdir()
    with pytest.raises(FileNotFoundError):
        _ = _load_model_json("ctdc", tmp_path)


def test_load_model_json_json_path_is_dir_not_file_raises(tmp_path: Path) -> None:
    ctdc_dir = tmp_path / "CTDC"
    ctdc_dir.mkdir()
    (ctdc_dir / "CTDC.json").mkdir()  # a directory named CTDC.json, not a real file

    with pytest.raises(IsADirectoryError):
        _ = _load_model_json("ctdc", tmp_path)


def test_load_model_json_root_is_file_not_dir_raises(tmp_path: Path) -> None:
    root_as_file = tmp_path / "not_a_dir.txt"
    _ = root_as_file.write_text("x", encoding="utf-8")

    with pytest.raises(NotADirectoryError):
        _ = _load_model_json("ctdc", root_as_file)


def test_load_model_json_empty_dict_raises_value_error(tmp_path: Path) -> None:
    ctdc_dir = tmp_path / "CTDC"
    ctdc_dir.mkdir()
    _ = (ctdc_dir / "CTDC.json").write_text("{}", encoding="utf-8")

    with pytest.raises(ValueError, match="empty"):
        _ = _load_model_json("ctdc", tmp_path)


# ---------------------------------------------------------------------------
# _load_relationships
# ---------------------------------------------------------------------------

def test_load_relationships_returns_parsed_json(workspace: Path) -> None:
    relationships = _load_relationships("ctdc", workspace)

    assert relationships == {"diagnosis": ["participant", "sample"]}


def test_load_relationships_missing_file_returns_empty_dict(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A missing relationships.json falls back to an empty dict with a
    warning, rather than raising — FK checks still run correctly, they
    just report all parents as missing."""
    ctdc_dir = tmp_path / "CTDC"
    ctdc_dir.mkdir()

    relationships = _load_relationships("ctdc", tmp_path)
    captured = capsys.readouterr()

    assert relationships == {}
    assert "WARNING" in captured.out
    assert "relationships.json" in captured.out


def test_load_relationships_wrong_schema_returns_empty_dict(
    workspace: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """A schema whose folder exists but has no relationships.json returns
    empty, not raise — same graceful fallback as a fully missing file."""
    # workspace has CTDC/relationships.json but not GC/relationships.json
    (workspace / "GC").mkdir(exist_ok=True)

    relationships = _load_relationships("gc", workspace)
    captured = capsys.readouterr()

    assert relationships == {}
    assert "WARNING" in captured.out


# ---------------------------------------------------------------------------
# chunk_by_node
# ---------------------------------------------------------------------------

def test_chunk_by_node_missing_harmonized_csv_raises(workspace: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _ = chunk_by_node(
            harmonized_csv_path=workspace / "output" / "does_not_exist.csv",
            node_recommendations={"diagnosis": ["study_diagnosis_id"]},
            output_dir=workspace / "output" / "chunks_missing",
        )


def test_chunk_by_node_empty_harmonized_csv_raises(workspace: Path) -> None:
    empty_csv = workspace / "output" / "empty.csv"
    _ = empty_csv.write_text("", encoding="utf-8")

    with pytest.raises(ValueError, match="no usable header row"):
        _ = chunk_by_node(
            harmonized_csv_path=empty_csv,
            node_recommendations={"diagnosis": ["study_diagnosis_id"]},
            output_dir=workspace / "output" / "chunks_empty",
        )


def test_chunk_by_node_filters_cdes_absent_from_harmonized_header(workspace: Path) -> None:
    """A node_recommendations entry can list a CDE that isn't actually in the
    harmonized CSV — it should be silently dropped, not crash or appear as a
    blank column."""
    node_recommendations = {
        "diagnosis": ["study_diagnosis_id", "this_cde_does_not_exist_in_csv"],
    }
    result = chunk_by_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations=node_recommendations,
        output_dir=workspace / "output" / "chunks_filter",
    )

    header, _ = _read_csv_rows(result["diagnosis"])
    assert header == ["study_diagnosis_id"]  # only the real CDE survives


def test_chunk_by_node_writes_one_csv_per_node(workspace: Path) -> None:
    node_recommendations = {
        "diagnosis": ["study_diagnosis_id", "primary_diagnosis"],
        "empty_node": ["some_cde_not_in_csv"],
    }
    result = chunk_by_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations=node_recommendations,
        output_dir=workspace / "output" / "chunks",
    )

    assert set(result.keys()) == {"diagnosis"}  # empty_node produced no file
    assert result["diagnosis"].exists()

    header, rows = _read_csv_rows(result["diagnosis"])
    assert header == ["study_diagnosis_id", "primary_diagnosis"]
    assert len(rows) == 2


def test_chunk_by_node_shared_cde_appears_in_both_chunks(workspace: Path) -> None:
    node_recommendations = {
        "diagnosis": ["study_diagnosis_id"],
        "other": ["study_diagnosis_id"],
    }
    result = chunk_by_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations=node_recommendations,
        output_dir=workspace / "output" / "chunks2",
    )

    assert "study_diagnosis_id" in _read_csv_rows(result["diagnosis"])[0]
    assert "study_diagnosis_id" in _read_csv_rows(result["other"])[0]


def test_chunk_by_node_creates_output_dir(workspace: Path) -> None:
    output_dir = workspace / "output" / "brand_new_dir"
    assert not output_dir.exists()

    _ = chunk_by_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations={"diagnosis": ["study_diagnosis_id"]},
        output_dir=output_dir,
    )

    assert output_dir.exists()


def test_chunk_by_node_ignores_columns_not_listed_under_any_node(workspace: Path) -> None:
    """The harmonized CSV has columns (e.g. undefined_field) that exist in the
    file but aren't listed in node_recommendations for this node — they must
    not leak into the chunked output."""
    result = chunk_by_node(
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations={"diagnosis": ["study_diagnosis_id"]},  # deliberately narrow
        output_dir=workspace / "output" / "chunks_narrow",
    )

    header, _ = _read_csv_rows(result["diagnosis"])
    assert header == ["study_diagnosis_id"]
    assert "undefined_field" not in header
    assert "primary_diagnosis" not in header


# ---------------------------------------------------------------------------
# check_required_cdes
# ---------------------------------------------------------------------------

def test_check_required_cdes_flags_missing_required_cde(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"primary_diagnosis"}  # study_diagnosis_id and the required FK are absent
    result = check_required_cdes(header_set, diagnosis_cdes)

    assert "study_diagnosis_id" in result
    assert "participant.study_participant_id" in result
    assert "primary_diagnosis" not in result  # present, not flagged


def test_check_required_cdes_ignores_optional_cdes(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set: set[str] = set()  # nothing present at all
    result = check_required_cdes(header_set, diagnosis_cdes)

    assert "age_at_diagnosis" not in result  # optional, never flagged regardless


# ---------------------------------------------------------------------------
# check_required_values
# ---------------------------------------------------------------------------

def test_check_required_values_reports_correct_physical_line_numbers(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"study_diagnosis_id"}
    rows = [
        {"study_diagnosis_id": "SID1"},
        {"study_diagnosis_id": ""},   # blank -> line 3 (header=1, this is 2nd data row)
        {"study_diagnosis_id": "  "},  # whitespace-only -> line 4
    ]
    result = check_required_values(header_set, rows, diagnosis_cdes)

    assert result["study_diagnosis_id"]["empty_lines"] == [3, 4]
    assert result["study_diagnosis_id"]["count"] == 2


def test_check_required_values_no_findings_when_all_populated(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"study_diagnosis_id"}
    rows = [{"study_diagnosis_id": "SID1"}, {"study_diagnosis_id": "SID2"}]
    result = check_required_values(header_set, rows, diagnosis_cdes)

    assert result == {}


def test_check_required_values_ignores_optional_cde_even_if_empty(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"age_at_diagnosis"}  # Req=False
    rows = [{"age_at_diagnosis": ""}]
    result = check_required_values(header_set, rows, diagnosis_cdes)

    assert result == {}


def test_check_required_values_count_matches_number_of_empty_lines(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"study_diagnosis_id"}
    rows = [
        {"study_diagnosis_id": "SID1"},
        {"study_diagnosis_id": ""},
        {"study_diagnosis_id": "SID3"},
        {"study_diagnosis_id": ""},
        {"study_diagnosis_id": ""},
    ]
    result = check_required_values(header_set, rows, diagnosis_cdes)

    finding = result["study_diagnosis_id"]
    count = finding["count"]
    empty_lines = finding["empty_lines"]

    assert count == 3
    assert isinstance(empty_lines, list)
    assert count == len(empty_lines)


# ---------------------------------------------------------------------------
# _should_check_type / type-matching helpers
# ---------------------------------------------------------------------------

def test_should_check_type_skips_permissive_types() -> None:
    assert _should_check_type("cde", "string", {"cde"}) is False
    assert _should_check_type("cde", "list", {"cde"}) is False


def test_should_check_type_skips_undefined_type_but_warns(capsys: pytest.CaptureFixture[str]) -> None:
    result = _should_check_type("undefined_field", "TBD", {"undefined_field"})
    captured = capsys.readouterr()

    assert result is False
    assert "undefined_field" in captured.out
    assert "TBD" in captured.out


def test_should_check_type_skips_when_none() -> None:
    assert _should_check_type("cde", None, {"cde"}) is False


def test_should_check_type_skips_when_column_absent() -> None:
    assert _should_check_type("cde", "number", set()) is False


def test_should_check_type_true_for_real_type_and_present_column() -> None:
    assert _should_check_type("cde", "number", {"cde"}) is True


def test_matches_numeric_type_number() -> None:
    assert _matches_numeric_type("45.5", "number") is True
    assert _matches_numeric_type("abc", "number") is False


def test_matches_numeric_type_integer() -> None:
    assert _matches_numeric_type("45", "integer") is True
    assert _matches_numeric_type("3.5", "integer") is False


def test_matches_pattern_type_valid_and_invalid() -> None:
    assert _matches_pattern_type("phs001234", "^phs[0-9]+$") is True
    assert _matches_pattern_type("phs00abc", "^phs[0-9]+$") is False


def test_matches_pattern_type_malformed_pattern_returns_false_not_raise() -> None:
    assert _matches_pattern_type("anything", "(unclosed[") is False


def test_matches_boolean_type_recognized_values() -> None:
    for v in ("true", "FALSE", "1", "0", "Yes", "no"):
        assert _matches_boolean_type(v) is True


def test_matches_boolean_type_unrecognized_value() -> None:
    assert _matches_boolean_type("maybe") is False


def test_matches_datetime_type_valid_iso() -> None:
    assert _matches_datetime_type("2024-01-15") is True


def test_matches_datetime_type_invalid() -> None:
    assert _matches_datetime_type("not-a-date") is False


def test_matches_semantic_type_boolean() -> None:
    assert _matches_semantic_type("true", "boolean") is True
    assert _matches_semantic_type("maybe", "boolean") is False


def test_matches_semantic_type_datetime() -> None:
    assert _matches_semantic_type("2024-01-15", "datetime") is True
    assert _matches_semantic_type("nope", "datetime") is False


def test_matches_semantic_type_returns_none_for_unrelated_type() -> None:
    """'why' None, not False: a type_value that isn't boolean/datetime at all
    (e.g. a numeric type or a regex pattern) must signal 'not my job' so the
    caller falls through to the right matcher, rather than being
    misreported as a failed boolean/datetime check."""
    assert _matches_semantic_type("45", "number") is None
    assert _matches_semantic_type("phs001234", "^phs[0-9]+$") is None


def test_value_matches_type_none_type_always_true() -> None:
    assert _value_matches_type("anything", None) is True


def test_value_matches_type_dispatches_to_numeric() -> None:
    assert _value_matches_type("45", "integer") is True
    assert _value_matches_type("abc", "integer") is False


def test_value_matches_type_dispatches_to_boolean() -> None:
    assert _value_matches_type("true", "boolean") is True
    assert _value_matches_type("maybe", "boolean") is False


def test_value_matches_type_dispatches_to_datetime() -> None:
    assert _value_matches_type("2024-01-15", "datetime") is True
    assert _value_matches_type("nope", "datetime") is False


def test_value_matches_type_dispatches_to_pattern() -> None:
    assert _value_matches_type("phs001234", "^phs[0-9]+$") is True
    assert _value_matches_type("bad", "^phs[0-9]+$") is False


# ---------------------------------------------------------------------------
# check_type_values
# ---------------------------------------------------------------------------

def test_check_type_values_flags_every_branch(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"age_at_diagnosis", "diagnosis_count", "is_relapsed", "diagnosis_date", "study_accession"}
    rows = [
        {
            "age_at_diagnosis": "abc",       # bad number
            "diagnosis_count": "3.5",        # bad integer
            "is_relapsed": "maybe",          # bad boolean
            "diagnosis_date": "not-a-date",  # bad datetime
            "study_accession": "phs00abc",   # bad pattern
        }
    ]
    result = check_type_values(header_set, rows, diagnosis_cdes)

    assert set(result.keys()) == {
        "age_at_diagnosis", "diagnosis_count", "is_relapsed", "diagnosis_date", "study_accession"
    }
    assert result["age_at_diagnosis"][0]["line"] == 2  # first (only) data row


def test_check_type_values_no_findings_for_valid_values(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"age_at_diagnosis", "diagnosis_count", "is_relapsed", "diagnosis_date", "study_accession"}
    rows = [
        {
            "age_at_diagnosis": "45",
            "diagnosis_count": "2",
            "is_relapsed": "true",
            "diagnosis_date": "2024-01-15",
            "study_accession": "phs001234",
        }
    ]
    result = check_type_values(header_set, rows, diagnosis_cdes)

    assert result == {}


def test_check_type_values_skips_undefined_type_field(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"undefined_field"}
    rows = [{"undefined_field": "literally anything"}]
    result = check_type_values(header_set, rows, diagnosis_cdes)

    assert "undefined_field" not in result


def test_check_type_values_skips_empty_cells(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set = {"age_at_diagnosis"}
    rows: list[dict[str, str]] = [{"age_at_diagnosis": ""}]
    result = check_type_values(header_set, rows, diagnosis_cdes)

    assert result == {}  # emptiness is check_required_values's job, not this one


def test_check_type_values_skips_cde_absent_from_header(diagnosis_cdes: dict[str, CDEProps]) -> None:
    header_set: set[str] = set()  # age_at_diagnosis not present at all
    rows: list[dict[str, str]] = [{}]
    result = check_type_values(header_set, rows, diagnosis_cdes)

    assert result == {}


# ---------------------------------------------------------------------------
# _check_fk_column_values / _evaluate_fk_column / _summarize_fk_results
# ---------------------------------------------------------------------------

def test_check_fk_column_values_flags_value_not_in_parent(tmp_path: Path) -> None:
    parent_path = tmp_path / "participant.csv"
    _ = parent_path.write_text("study_participant_id\nP001\nP002\n", encoding="utf-8")

    rows = [
        {"participant.study_participant_id": "P001"},  # valid
        {"participant.study_participant_id": "P999"},  # invalid
    ]
    bad = _check_fk_column_values("participant.study_participant_id", "study_participant_id", rows, parent_path)

    assert len(bad) == 1
    assert bad[0]["line"] == 3  # second data row
    assert bad[0]["value"] == "P999"


def test_check_fk_column_values_no_findings_for_all_valid(tmp_path: Path) -> None:
    parent_path = tmp_path / "participant.csv"
    _ = parent_path.write_text("study_participant_id\nP001\nP002\n", encoding="utf-8")

    rows = [{"participant.study_participant_id": "P001"}, {"participant.study_participant_id": "P002"}]
    bad = _check_fk_column_values("participant.study_participant_id", "study_participant_id", rows, parent_path)

    assert bad == []


def test_check_fk_column_values_missing_parent_file_raises(tmp_path: Path) -> None:
    """'why' this propagates rather than being swallowed: _evaluate_fk_column
    only calls this when it already believes parent_path is real (from
    parent_sheets) — a bad path reaching here is a genuine caller error
    worth surfacing loudly, not silently treating as 'no data to check'."""
    rows = [{"participant.study_participant_id": "P001"}]
    with pytest.raises(FileNotFoundError):
        _ = _check_fk_column_values(
            "participant.study_participant_id", "study_participant_id", rows, tmp_path / "does_not_exist.csv"
        )


def test_evaluate_fk_column_present_with_valid_values(tmp_path: Path) -> None:
    parent_path = tmp_path / "participant.csv"
    _ = parent_path.write_text("study_participant_id\nP001\n", encoding="utf-8")

    rows = [{"participant.study_participant_id": "P001"}]
    result = _evaluate_fk_column(
        "participant.study_participant_id", {"participant.study_participant_id"}, rows,
        {"participant": parent_path},
    )

    assert result.fk_column_missing is False
    assert result.parent_missing is None
    assert result.bad_rows == []


def test_evaluate_fk_column_present_with_invalid_values(tmp_path: Path) -> None:
    parent_path = tmp_path / "participant.csv"
    _ = parent_path.write_text("study_participant_id\nP001\n", encoding="utf-8")

    rows = [{"participant.study_participant_id": "P999"}]
    result = _evaluate_fk_column(
        "participant.study_participant_id", {"participant.study_participant_id"}, rows,
        {"participant": parent_path},
    )

    assert result.bad_rows and result.bad_rows[0]["value"] == "P999"


def test_evaluate_fk_column_present_no_parent_sheet() -> None:
    rows = [{"participant.study_participant_id": "P001"}]
    result = _evaluate_fk_column(
        "participant.study_participant_id", {"participant.study_participant_id"}, rows, {},
    )

    assert result.fk_column_missing is False
    assert result.parent_missing == "participant"
    assert result.bad_rows == []  # couldn't check, not "no problems found"


def test_evaluate_fk_column_missing_column_entirely() -> None:
    rows = [{"unrelated": "x"}]
    result = _evaluate_fk_column(
        "participant.study_participant_id", set(), rows, {},
    )

    assert result.fk_column_missing is True
    assert result.parent_missing == "participant"  # both problems captured together


def test_summarize_fk_results_rolls_up_correctly() -> None:
    results = [
        _FkColumnResult("a.x", fk_column_missing=False, parent_missing=None, bad_rows=[{"line": 2, "cde": "a.x", "value": "bad"}]),
        _FkColumnResult("b.y", fk_column_missing=True, parent_missing="b", bad_rows=[]),
    ]
    invalid_values, missing_fk_columns, missing_parent_sheets = _summarize_fk_results(results)

    assert list(invalid_values.keys()) == ["a.x"]
    assert missing_fk_columns == ["b.y"]
    assert missing_parent_sheets == ["b"]


def test_summarize_fk_results_empty_input_returns_all_empty() -> None:
    invalid_values, missing_fk_columns, missing_parent_sheets = _summarize_fk_results([])

    assert invalid_values == {}
    assert missing_fk_columns == []
    assert missing_parent_sheets == []


# ---------------------------------------------------------------------------
# check_foreign_keys
# ---------------------------------------------------------------------------

def test_check_foreign_keys_no_fk_cdes_defined_returns_all_empty() -> None:
    """A node whose schema has no parent_node.property CDEs at all — distinct
    from an FK CDE that exists in the schema but is missing from the sheet."""
    node_cdes_no_fk: dict[str, CDEProps] = {"plain_field": {"Enum": [], "Req": False, "Type": "string"}}
    invalid_values, missing_fk_columns, missing_parent_sheets = check_foreign_keys(
        {"plain_field"}, [{"plain_field": "x"}], node_cdes_no_fk, None
    )

    assert invalid_values == {}
    assert missing_fk_columns == []
    assert missing_parent_sheets == []


def test_check_foreign_keys_omitted_optional_fk(tmp_path: Path, diagnosis_cdes: dict[str, CDEProps]) -> None:
    """sample.sample_id (Req=False) absent from the sheet entirely."""
    dummy_parent = tmp_path / "participant.csv"
    _ = dummy_parent.write_text("study_participant_id\nP001\n", encoding="utf-8")

    header_set = {"participant.study_participant_id"}  # sample.sample_id NOT present
    rows: list[dict[str, str]] = [{"participant.study_participant_id": "P001"}]
    _, missing_fk_columns, _ = check_foreign_keys(
        header_set, rows, diagnosis_cdes, {"participant": dummy_parent}
    )

    assert "sample.sample_id" in missing_fk_columns


def test_check_foreign_keys_omitted_required_fk(diagnosis_cdes: dict[str, CDEProps]) -> None:
    """participant.study_participant_id (Req=True) absent from the sheet entirely."""
    header_set: set[str] = set()  # neither FK column present
    rows: list[dict[str, str]] = []
    _, missing_fk_columns, _ = check_foreign_keys(
        header_set, rows, diagnosis_cdes, None
    )

    assert "participant.study_participant_id" in missing_fk_columns
    # 'why': required-ness of a missing FK column is separately caught by
    # check_required_cdes, not by check_foreign_keys itself


# ---------------------------------------------------------------------------
# validate_node
# ---------------------------------------------------------------------------

def test_validate_node_pass_on_clean_sheet(tmp_path: Path, diagnosis_cdes: dict[str, CDEProps]) -> None:
    ctdc_dir = tmp_path / "CTDC"
    ctdc_dir.mkdir()
    _ = (ctdc_dir / "CTDC.json").write_text(json.dumps({"diagnosis": diagnosis_cdes}), encoding="utf-8")

    node_csv = tmp_path / "diagnosis.csv"
    _ = node_csv.write_text(
        "study_diagnosis_id,primary_diagnosis,age_at_diagnosis,diagnosis_count,is_relapsed,"
        + "diagnosis_date,study_accession,undefined_field,participant.study_participant_id,sample.sample_id\n"
        + "SID1,Lung Cancer,45,2,true,2024-01-15,phs001234,foo,P001,S001\n",
        encoding="utf-8",
    )
    participant_path = tmp_path / "participant.csv"
    _ = participant_path.write_text("study_participant_id\nP001\n", encoding="utf-8")
    sample_path = tmp_path / "sample.csv"
    _ = sample_path.write_text("sample_id\nS001\n", encoding="utf-8")

    report = validate_node(
        node_csv_path=node_csv,
        node="diagnosis",
        target_schema="ctdc",
        data_model_outputs_root=tmp_path,
        output_path=tmp_path / "report.json",
        parent_sheets={"participant": participant_path, "sample": sample_path},
    )

    assert report["status"] == "PASS"


def test_validate_node_fail_status_when_any_finding(workspace: Path) -> None:
    node_csv = workspace / "output" / "harmonized.csv"  # has bad values (see fixture)
    report = validate_node(
        node_csv_path=node_csv,
        node="diagnosis",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=workspace / "output" / "report.json",
    )

    assert report["status"] == "FAIL"


def test_validate_node_writes_report_to_output_path(workspace: Path) -> None:
    output_path = workspace / "output" / "written_report.json"
    _ = validate_node(
        node_csv_path=workspace / "output" / "harmonized.csv",
        node="diagnosis",
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_path=output_path,
    )

    assert output_path.exists()
    on_disk = cast(dict[str, object], json.loads(output_path.read_text(encoding="utf-8")))
    assert on_disk["node"] == "diagnosis"


def test_validate_node_unknown_node_raises_value_error(workspace: Path) -> None:
    with pytest.raises(ValueError, match="not defined"):
        _ = validate_node(
            node_csv_path=workspace / "output" / "harmonized.csv",
            node="not_a_real_node",
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "report.json",
        )


def test_validate_node_missing_csv_raises_file_not_found(workspace: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _ = validate_node(
            node_csv_path=workspace / "output" / "does_not_exist.csv",
            node="diagnosis",
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_path=workspace / "output" / "report.json",
        )


# ---------------------------------------------------------------------------
# chunk_and_validate
# ---------------------------------------------------------------------------

def _call_chunk_and_validate(workspace: Path, recs_filename: str = "suggested_nodes.json") -> dict[str, dict[str, ValidationReport | Path]]:
    """Helper to reduce boilerplate in chunk_and_validate tests."""
    node_recommendations_path = workspace / "output" / recs_filename
    if not node_recommendations_path.exists():
        _ = node_recommendations_path.write_text(
            json.dumps({"diagnosis": list(_read_csv_rows(workspace / "output" / "harmonized.csv")[0])}),
            encoding="utf-8",
        )

    return chunk_and_validate(
        source_path=workspace / "my_source_data.csv",
        harmonized_csv_path=workspace / "output" / "harmonized.csv",
        node_recommendations_path=node_recommendations_path,
        target_schema="ctdc",
        data_model_outputs_root=workspace,
        output_dir=workspace / "output" / "runs",
    )


def test_chunk_and_validate_empty_recommendations_raises_value_error(workspace: Path) -> None:
    empty_recs_path = workspace / "output" / "empty_recommendations.json"
    _ = empty_recs_path.write_text(json.dumps({}), encoding="utf-8")

    with pytest.raises(ValueError, match="empty"):
        _ = chunk_and_validate(
            source_path=workspace / "my_source_data.csv",
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            node_recommendations_path=empty_recs_path,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_dir=workspace / "output" / "runs",
        )
        

def test_chunk_and_validate_creates_run_folder_with_chunks_and_reports(workspace: Path) -> None:
    """A successful run creates a uniquely-named run folder inside output_dir
    containing both a chunks/ and a reports/ subdirectory."""
    results = _call_chunk_and_validate(workspace)

    assert "diagnosis" in results
    csv_path = results["diagnosis"]["csv_path"]
    report_path = results["diagnosis"]["report_path"]
    assert isinstance(csv_path, Path)
    assert isinstance(report_path, Path)

    assert csv_path.exists()
    assert report_path.exists()
    assert csv_path.parent.name == "chunks"
    assert report_path.parent.name == "reports"
    assert csv_path.parent.parent == report_path.parent.parent  # same run folder


def test_chunk_and_validate_run_folder_named_after_source_file(workspace: Path) -> None:
    """The run folder's name starts with the source file's stem."""
    results = _call_chunk_and_validate(workspace)

    csv_path = results["diagnosis"]["csv_path"]
    assert isinstance(csv_path, Path)
    run_folder = csv_path.parent.parent
    assert run_folder.name.startswith("my_source_data_")


def test_chunk_and_validate_returns_report_data_in_memory(workspace: Path) -> None:
    """The return value includes the report dict itself, not just the file path."""
    results = _call_chunk_and_validate(workspace)

    report = results["diagnosis"]["report"]
    assert isinstance(report, dict)
    assert report["node"] == "diagnosis"
    assert "status" in report


def test_chunk_and_validate_consecutive_runs_create_separate_folders(workspace: Path) -> None:
    """Two consecutive runs against the same output_dir each get their own
    run folder — nothing from the first run is modified or deleted."""
    results_1 = _call_chunk_and_validate(workspace, "recs_run1.json")
    results_2 = _call_chunk_and_validate(workspace, "recs_run2.json")

    csv_1 = results_1["diagnosis"]["csv_path"]
    csv_2 = results_2["diagnosis"]["csv_path"]
    assert isinstance(csv_1, Path)
    assert isinstance(csv_2, Path)

    run_folder_1 = csv_1.parent.parent
    run_folder_2 = csv_2.parent.parent

    assert run_folder_1 != run_folder_2  # different folders
    assert run_folder_1.exists()         # first run's folder still intact
    assert run_folder_2.exists()


def test_chunk_and_validate_failed_run_leaves_no_run_folder(workspace: Path) -> None:
    """If validation raises partway through, no run folder is published —
    the scratch directory is cleaned up and output_dir is left untouched."""
    output_dir = workspace / "output" / "runs_fail_test"

    bad_recs_path = workspace / "output" / "bad_recs.json"
    _ = bad_recs_path.write_text(
        json.dumps({"not_a_real_node": ["study_diagnosis_id"]}), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="not defined"):
        _ = chunk_and_validate(
            source_path=workspace / "my_source_data.csv",
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            node_recommendations_path=bad_recs_path,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_dir=output_dir,
        )

    # output_dir should either not exist or contain no run folders
    if output_dir.exists():
        run_folders = [p for p in output_dir.iterdir() if not p.name.startswith(".")]
        assert run_folders == []


def test_chunk_and_validate_scratch_dir_cleaned_up_on_failure(workspace: Path) -> None:
    """Even on failure, no .tmp-* scratch directories linger in output_dir."""
    output_dir = workspace / "output" / "runs_scratch_test"

    bad_recs_path = workspace / "output" / "bad_recs2.json"
    _ = bad_recs_path.write_text(
        json.dumps({"not_a_real_node": ["study_diagnosis_id"]}), encoding="utf-8"
    )

    with pytest.raises(ValueError, match="not defined"):
        _ = chunk_and_validate(
            source_path=workspace / "my_source_data.csv",
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            node_recommendations_path=bad_recs_path,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_dir=output_dir,
        )

    if output_dir.exists():
        scratch_dirs = [p for p in output_dir.iterdir() if p.name.startswith(".tmp-")]
        assert scratch_dirs == []


def test_chunk_and_validate_missing_recommendations_file_raises(workspace: Path) -> None:
    with pytest.raises(FileNotFoundError):
        _ = chunk_and_validate(
            source_path=workspace / "my_source_data.csv",
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            node_recommendations_path=workspace / "output" / "does_not_exist.json",
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_dir=workspace / "output" / "runs_missing",
        )


def test_chunk_and_validate_empty_recommendations_raises(workspace: Path) -> None:
    """'why' this raises now instead of returning {}: chunk_by_node itself
    rejects empty node_recommendations with a ValueError, so the error
    surfaces before any run folder is ever created."""
    empty_recs_path = workspace / "output" / "empty_recs.json"
    _ = empty_recs_path.write_text(json.dumps({}), encoding="utf-8")

    with pytest.raises(ValueError, match="empty"):
        _ = chunk_and_validate(
            source_path=workspace / "my_source_data.csv",
            harmonized_csv_path=workspace / "output" / "harmonized.csv",
            node_recommendations_path=empty_recs_path,
            target_schema="ctdc",
            data_model_outputs_root=workspace,
            output_dir=workspace / "output" / "runs_empty_recs",
        )
