"""Validate a per-node harmonized sheet against its schema.

Companion to suggest_node: suggest_node classifies which columns of a
harmonized CSV belong to which node; chunk_by_node uses that classification
to actually slice the CSV into one file per node; validate_node then runs
schema checks against a single node's chunk.

Checks performed, all against the node's enriched model JSON entry
({cde: {Enum, Req, Type}}):
    1. missing_required_cdes    — a Req=True CDE has no column in the sheet at all
    2. missing_required_values  — a present Req=True column has an empty cell
    3. invalid_type_values      — a value doesn't match its CDE's declared Type
    4. invalid_fk_values / missing_fk_columns / missing_parent_sheets
       — a `parent_node.property` column's value doesn't exist in that
       parent node's own sheet, or the column/parent sheet is missing entirely
"""

from __future__ import annotations
from dataclasses import dataclass, field

from ._models import CDEProps
from ._models import ValidationReport
 
import csv
import json
from typing import cast
import re
from pathlib import Path
 
 
# 'why' restricted to these two: "string" is free text with no inherent
# format to check, and "list" is this model's way of saying "categorical" —
# its real constraint (if any) is the CDE's Enum, which would be checked
# separately (not currently present in this file). Numeric types ("number",
# "integer") DO have a real format to verify here; anything else is assumed
# to be a regex pattern.
_PERMISSIVE_TYPES = frozenset({"string", "list"})
_NUMERIC_TYPES = frozenset({"number", "integer"})

 
@dataclass
class _FkColumnResult:
    """One FK column's evaluation outcome — presence, parent-sheet
    availability, and any bad values found (if both were available to check)."""
    column: str
    fk_column_missing: bool
    parent_missing: str | None
    bad_rows: list[dict[str, int | str]] = field(default_factory=list)


ModelJson = dict[str, dict[str, CDEProps]]
NodeRecommendations = dict[str, list[str]]



def _check_path(path: Path, expected: str) -> None:
    """Validate a path exists and is the expected type ('file' or 'dir')

    'why' a dedicated helper: this module handles many different paths
    (node CSVs, parent sheet CSVs, dm_outputs_root, model JSON files)
    Distinguishing them makes the actual mistake obvious from the error
    alone, rather than requiring the caller to go inspect the path by hand.

    Raises:
        FileNotFoundError: nothing exists at path at all.
        NotADirectoryError: expected='dir' but path is a file (or other non-dir).
        IsADirectoryError: expected='file' but path is a directory.
    """
    if not path.exists():
        raise FileNotFoundError(f"Path does not exist: {path}")

    if expected == "file" and not path.is_file():
        raise IsADirectoryError(f"Expected a file but found a directory: {path}")

    if expected == "dir" and not path.is_dir():
        raise NotADirectoryError(f"Expected a directory but found a file: {path}")

 
def _read_csv_rows(csv_path: Path) -> tuple[list[str], list[dict[str, str]]]:
    """Read the harmonized CSV's header and all data rows as dicts.
 
    Raises:
        FileNotFoundError: csv_path doesn't exist.
        IsADirectoryError: csv_path exists but is a directory, not a file.
        ValueError: the file has no usable header row.
    """
    _check_path(csv_path, "file")

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        harm_header = reader.fieldnames
        rows = list(reader)
 
    if not harm_header:
        raise ValueError(f"CSV has no usable header row: {csv_path}")
 
    return list(harm_header), rows
 
 
def _load_model_json(target_schema: str, dm_outputs_root: Path) -> ModelJson:
    """Load the target schema's enriched model JSON.
 
    'why': mirrors suggest_node's own _load_model_json exactly — kept as a
    separate copy here rather than importing from suggest_node so this
    module has no dependency on suggest_node's internals, only its output shape.
 
    Raises:
        ValueError: target_schema isn't one of the 4 supported schemas,
        dm_outputs_root isn't a valid directory, or the model JSON is an empty dict.
        FileNotFoundError: no model JSON exists at the resolved path.
    """
    valid_keys = frozenset({"ctdc", "gc", "icdc", "psdc"})
 
    if not target_schema or target_schema.strip().lower() not in valid_keys:
        raise ValueError(f"target_schema must be one of {sorted(valid_keys)}, got: {target_schema!r}")
 
    _check_path(dm_outputs_root, "dir")
 
    model = target_schema.upper()
    json_path = dm_outputs_root / model / f"{model}.json"
 
    _check_path(json_path, expected="file")
 
    with open(json_path, "r", encoding="utf-8") as f:
        model_json = cast(ModelJson, json.load(f))
        
    if not model_json:
        raise ValueError(f"Model JSON at {json_path} is empty")

    return model_json
 
 
def chunk_by_node(
    harmonized_csv_path: Path,
    node_recommendations: NodeRecommendations,
    output_dir: Path,
) -> dict[str, Path]:
    """Slice a harmonized CSV into one CSV per node, using suggest_node's
    {node: [matched cde, ...]} classification to pick each node's cdes.
 
    Returns {node: path_to_that_node's_chunked_csv}.
 
    'why' a shared CDE (present under multiple nodes) is copied into every
    node's chunk rather than only one — matches suggest_node's own decision
    to attribute a shared CDE to every node it belongs to, not just one.
    """
    harm_header, rows = _read_csv_rows(harmonized_csv_path)
    harm_header_set = set(harm_header)
 
    output_dir.mkdir(parents=True, exist_ok=True)
 
    result: dict[str, Path] = {}
    for node, cdes in node_recommendations.items():
        node_columns = [cde for cde in cdes if cde in harm_header_set]
        if not node_columns:
            continue
 
        node_path = output_dir / f"{node}.csv"
        with open(node_path, "w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=node_columns)
            writer.writeheader()
            for row in rows:
                writer.writerow({col: row.get(col, "") for col in node_columns})
 
        result[node] = node_path
 
    return result
 
 
def check_required_presence(harm_header_set: set[str], node_cdes: dict[str, CDEProps]) -> list[str]:
    """Return the names of Req=True CDEs that have no column in harm_header at all."""
    return [cde for cde, props in node_cdes.items() if props.get("Req") is True and cde not in harm_header_set]
 
 
def check_required_values(
    harm_header_set: set[str], rows: list[dict[str, str]], node_cdes: dict[str, CDEProps]
) -> dict[str, dict[str, list[int] | int]]:
    """Return {cde: {"empty_rows": [row numbers], "count": N}} for present Req=True
    columns with an empty cell.
 
    'why' rows/count: knowing exactly which rows are missing a value lets
    someone go straight to those rows in the source file to fix them, and
    the count gives an at-a-glance sense of how widespread the gap is.
    """
    findings: dict[str, dict[str, list[int] | int]] = {}
 
    for cde, props in node_cdes.items():
        if props.get("Req") is not True or cde not in harm_header_set:
            continue
        empty_row_numbers = [i for i, row in enumerate(rows) if not row.get(cde, "").strip()]
        if empty_row_numbers:
            findings[cde] = {"empty_rows": empty_row_numbers, "count": len(empty_row_numbers)}
 
    return findings
 

def _should_check_type(cde: str, type_value: str | None, harm_header_set: set[str]) -> bool:
    """Whether a CDE's Type is worth checking at all for this sheet."""
    return type_value is not None and type_value not in _PERMISSIVE_TYPES and cde in harm_header_set


def _matches_numeric_type(value: str, type_value: str) -> bool:
    """Whether value parses as the declared numeric type ('number' or 'integer')."""
    try:
        float(value) if type_value == "number" else int(value)
        return True
    except ValueError:
        return False


def _matches_pattern_type(value: str, type_value: str) -> bool:
    """Whether value matches type_value treated as a regex pattern.

    'why' malformed regex patterns don't raise: a broken pattern in the
    schema itself is a data problem to report, not a reason to crash validation.
    """
    try:
        return bool(re.fullmatch(type_value, value))
    except re.error:
        return False


def _value_matches_type(value: str, type_value: str | None) -> bool:
    """Whether a single value conforms to its CDE's declared Type.

    'why' the None check happens here, not just at the call site: type_value
    genuinely can be None for real data (a CDE with no declared Type), so
    this function stays correct on its own rather than depending on a
    caller to have already filtered that case out elsewhere.
    """
    if type_value is None:
        return True  # 'why': nothing declared to validate against, so nothing can fail

    if type_value in _NUMERIC_TYPES:
        return _matches_numeric_type(value, type_value)

    return _matches_pattern_type(value, type_value)


def check_type_values(
    harm_header_set: set[str], rows: list[dict[str, str]], node_cdes: dict[str, CDEProps]
) -> dict[str, list[dict[str, int | str]]]:
    """Return {cde: [{row, cde, value}]} for values that don't match their CDE's Type.

    - Type in _PERMISSIVE_TYPES ("string", "list"), or the CDE has no
      declared Type at all: not checked — this is the guard that keeps
      "string"/"list" from being misinterpreted as patterns in the regex
      branch below, and a CDE with no Type has nothing to validate against.
    - Type in _NUMERIC_TYPES ("number", "integer"): value must parse as a
      float or int, respectively.
    - Anything else: treated as a regex pattern, value must re.fullmatch it.
    Empty cell values are skipped — required-ness is checked separately.
    """
    findings: dict[str, list[dict[str, int | str]]] = {}

    for cde, props in node_cdes.items():
        type_value = props.get("Type")
        if not _should_check_type(cde, type_value, harm_header_set):
            continue

        bad_rows = [
            {"row": i, "cde": cde, "value": row.get(cde, "").strip()}
            for i, row in enumerate(rows)
            if row.get(cde, "").strip() and not _value_matches_type(row.get(cde, "").strip(), type_value)
        ]
        if bad_rows:
            findings[cde] = bad_rows

    return findings
 

def _check_fk_column_values(
    column: str, property_name: str, rows: list[dict[str, str]], parent_path: Path
) -> list[dict[str, int | str]]:
    """Bad rows for one FK column whose value doesn't exist in the parent's own property column."""
    _, parent_rows = _read_csv_rows(parent_path)
    parent_values = {row.get(property_name, "").strip() for row in parent_rows}

    return [
        {"row": i, "cde": column, "value": row.get(column, "")}
        for i, row in enumerate(rows)
        if row.get(column, "").strip() and row.get(column, "").strip() not in parent_values
    ]


def _evaluate_fk_column(
    column: str,
    harm_header_set: set[str],
    rows: list[dict[str, str]],
    parent_sheets: dict[str, Path],
) -> _FkColumnResult:
    """Evaluate one FK column in isolation: is it present in the sheet, is
    its parent sheet available, and if both hold, which values are bad."""
    column_present = column in harm_header_set
    parent_node, _, property_name = column.partition(".")
    parent_path = parent_sheets.get(parent_node)

    bad_rows: list[dict[str, int | str]] = []
    if column_present and parent_path is not None:
        bad_rows = _check_fk_column_values(column, property_name, rows, parent_path)

    return _FkColumnResult(
        column=column,
        fk_column_missing=not column_present,
        parent_missing=None if parent_path is not None else parent_node,
        bad_rows=bad_rows,
    )


def _summarize_fk_results(
    results: list[_FkColumnResult],
) -> tuple[dict[str, list[dict[str, int | str]]], list[str], list[str]]:
    """Roll up per-column results into the 3 report buckets check_foreign_keys returns."""
    invalid_values = {r.column: r.bad_rows for r in results if r.bad_rows}
    missing_fk_columns = [r.column for r in results if r.fk_column_missing]
    missing_parent_sheets = sorted({r.parent_missing for r in results if r.parent_missing})
    return invalid_values, missing_fk_columns, missing_parent_sheets


def check_foreign_keys(
    harm_header_set: set[str],
    rows: list[dict[str, str]],
    node_cdes: dict[str, CDEProps],
    parent_sheets: dict[str, Path] | None,
) -> tuple[dict[str, list[dict[str, int | str]]], list[str], list[str]]:
    """Return (invalid_values, missing_fk_columns, missing_parent_sheets) for
    every node.property CDE defined in the schema for this node.

    'why' fk candidates come from node_cdes (the schema), not the CSV header:
    a relationship column that's entirely absent from the harmonized data is
    itself a reportable problem (missing_fk_columns), not something that
    should silently produce zero findings just because it isn't there to look at.

    'why' the split on the first '.': matches the dot-notation convention
    used for parent relationship columns elsewhere in this pipeline
    (e.g. participant.study_participant_id).

    Returns:
        invalid_values: {column: [{row, cde, value}]} for values that don't
            exist in the parent's own property column.
        missing_fk_columns: FK CDEs defined in the schema but absent from
            this node's CSV header entirely.
        missing_parent_sheets: parent node names referenced by a present FK
            column, but not supplied in parent_sheets — so no value check
            could be run for that column at all.
    """

    parent_sheets = parent_sheets or {}
    fk_cdes = [cde for cde in node_cdes.keys() if "." in cde]
    results = [_evaluate_fk_column(c, harm_header_set, rows, parent_sheets) for c in fk_cdes]
    return _summarize_fk_results(results)


def validate_node(
    node_csv_path: Path,
    node: str,
    target_schema: str,
    dm_outputs_root: Path,
    output_path: Path,
    parent_sheets: dict[str, Path] | None = None,
) -> ValidationReport:
    """Run all 4 validation checks against a single node's harmonized sheet.
 
    Returns a combined report and always writes it to output_path as JSON.
 
    Raises:
        FileNotFoundError: node_csv_path, a parent sheet path, or the resolved
            model JSON path don't exist.
        ValueError: node_csv_path has no usable header row, target_schema
            isn't one of the 4 supported schemas, dm_outputs_root isn't a
            valid directory, or node isn't a key in that model JSON.
        AssertionError: the model JSON is not a dict (currently checked via
            an unguarded assert in _load_model_json rather than a raised
            ValueError with a descriptive message).
    """
    harm_header, rows = _read_csv_rows(node_csv_path)
    model_json = _load_model_json(target_schema, dm_outputs_root)
 
    if node not in model_json:
        raise ValueError(f"node '{node}' is not defined in the '{target_schema}' schema")
 
    node_cdes = model_json[node]

    harm_header_set = set(harm_header)
    missing_required_cdes = check_required_presence(harm_header_set, node_cdes)
    missing_required_values = check_required_values(harm_header_set, rows, node_cdes)
    invalid_type_values = check_type_values(harm_header_set, rows, node_cdes)
    invalid_fk_values, missing_fk_columns, missing_parent_sheets = check_foreign_keys(harm_header_set, rows, node_cdes, parent_sheets)
 
    has_findings = any([
        missing_required_cdes,
        missing_required_values,
        invalid_type_values,
        invalid_fk_values,
        missing_fk_columns,
        missing_parent_sheets,
    ])
 
    report = {
        "node": node,
        "row_count": len(rows),
        "missing_required_cdes": missing_required_cdes,
        "missing_required_values": missing_required_values,
        "invalid_type_values": invalid_type_values,
        "invalid_fk_values": invalid_fk_values,
        "missing_fk_columns": missing_fk_columns,
        "missing_parent_sheets": missing_parent_sheets,
        "status": "FAIL" if has_findings else "PASS",
    }
 
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _ = output_path.write_text(json.dumps(report, indent=2), encoding="utf-8")
 
    return report
 
 
def chunk_and_validate(
    harmonized_csv_path: Path,
    node_recommendations_path: Path,
    target_schema: str,
    dm_outputs_root: Path,
    chunks_output_dir: Path,
    reports_output_dir: Path,
) -> dict[str, dict[str, ValidationReport | Path]]:
    """Chunk a harmonized CSV by node, then validate every resulting chunk.

    node_recommendations_path points at the JSON file suggest_node already
    wrote via its own output_path — this function doesn't call suggest_node
    itself, it just reads that JSON back in, keeping suggest_node's
    classification step and this chunk+validate step as two explicit,
    separately-run stages.
 
    'why' every other chunk is passed as parent_sheets for each node: this
    module doesn't yet cross-reference relationships.json to know which
    nodes are actually a given node's real parents, so the safe default is
    to make every other chunk available — check_foreign_keys only looks up
    the parent names a node's own dot-notation columns actually reference,
    so unused entries here are harmless, just possibly more than needed.
 
    Returns {node: {"csv_path": Path, "report": dict}}.
    """
    if not Path(node_recommendations_path).exists():
        raise FileNotFoundError(f"File not found: {node_recommendations_path}")

    with open(node_recommendations_path, "r", encoding="utf-8") as f:
        node_recommendations = cast(NodeRecommendations, json.load(f))

    chunks = chunk_by_node(
        harmonized_csv_path=harmonized_csv_path,
        node_recommendations=node_recommendations,
        output_dir=chunks_output_dir,
    )

    print(f"Node chunks successfully created: {chunks_output_dir}")
 
    results: dict[str, dict[str, ValidationReport | Path]] = {}
    for node, csv_path in chunks.items():
        parent_sheets = {p: path for p, path in chunks.items() if p != node}
 
        report = validate_node(
            node_csv_path=csv_path,
            node=node,
            target_schema=target_schema,
            dm_outputs_root=dm_outputs_root,
            output_path=reports_output_dir / f"{node}_validation.json",
            parent_sheets=parent_sheets,
        )
 
        results[node] = {"csv_path": csv_path, "report": report}
    
    print(f"Node validation reports ready: {reports_output_dir}")

    print("Node Validation Summary:")
    for node, entry in results.items():
        report = entry["report"]
        status = report["status"] if isinstance(report, dict) else "UNKNOWN"
        print(f"{node}: {status}")
 
    return results
