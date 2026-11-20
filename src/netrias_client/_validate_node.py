"""Validate a per-node harmonized sheet against its schema.

Companion to suggest_node: suggest_node classifies which columns of a
harmonized CSV belong to which node; chunk_by_node uses that classification
to actually slice the CSV into one file per node; validate_node then runs
schema checks against a single node's chunk.

Checks performed, all against the node's enriched model JSON entry
({cde: {Enum, Req, Type}}). Every finding reports a physical CSV line
number (1-based, header counted as line 1), not a 0-based row index:
    1. missing_required_cdes    — a Req=True CDE has no column in the sheet at all
    2. missing_required_values  — a present Req=True column has an empty cell
    3. invalid_type_values      — a value doesn't match its CDE's declared Type
    4. invalid_fk_values / missing_fk_columns / missing_parent_sheets
       — a `parent_node.property` column's value doesn't exist in that
       parent node's own sheet, or the column/parent sheet is missing entirely
"""

from __future__ import annotations
from dataclasses import dataclass, field
from datetime import datetime, timezone
import os
import shutil
from uuid import uuid4


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

# 'why': the set of string representations accepted for a boolean-typed
# CDE. The harmonized data is always plain CSV text, so there's no native
# bool to check against — this list covers the common conventions actually
# seen across the schema files; may need widening if real data uses a
# format not covered here.
_BOOLEAN_VALUES = frozenset({"true", "false", "1", "0", "yes", "no"})

# 'why': a placeholder Type the schema itself hasn't finalized yet — not a
# real format to validate against, but worth flagging (once per CDE, not
# per row) since it signals incomplete schema coverage rather than a
# deliberate "no check needed" like _PERMISSIVE_TYPES.
_UNDEFINED_TYPE = "TBD"

# 'why': the "_{MMDDYY-HHMMSSffffff}_{4-char-uuid}" tail that
# chunk_and_validate appends to a source file's stem when it names a run
# folder — matched here so the stem can be recovered from the folder name
# when an existing run is revalidated.
_RUN_FOLDER_SUFFIX = re.compile(r"_\d{6}-\d{12}_[0-9a-f]{4}$")


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
    (node CSVs, parent sheet CSVs, data_model_outputs_root, model JSON files)
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
 
 
def _load_model_json(target_schema: str, data_model_outputs_root: Path) -> ModelJson:
    """Load the target schema's enriched model JSON.
 
    'why': mirrors suggest_node's own _load_model_json exactly — kept as a
    separate copy here rather than importing from suggest_node so this
    module has no dependency on suggest_node's internals, only its output shape.
 
    Raises:
        ValueError: target_schema isn't one of the 4 supported schemas,
        data_model_outputs_root isn't a valid directory, or the model JSON is an empty dict.
        FileNotFoundError: no model JSON exists at the resolved path.
    """
    valid_keys = frozenset({"ctdc", "gc", "icdc", "psdc"})
 
    if not target_schema or target_schema.strip().lower() not in valid_keys:
        raise ValueError(f"target_schema must be one of {sorted(valid_keys)}, got: {target_schema!r}")
 
    _check_path(data_model_outputs_root, "dir")
 
    model = target_schema.upper()
    json_path = data_model_outputs_root / model / f"{model}.json"
 
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
    Each chunk has a "type" column added with the node name repeated on
    every row to meet the submission format requirement.
 
    Returns {node: path_to_that_node_chunked_csv}.
 
    'why' a shared CDE (present under multiple nodes) is copied into every
    node's chunk rather than only one — matches suggest_node's own decision
    to attribute a shared CDE to every node it belongs to, not just one.
    """
    harm_header, rows = _read_csv_rows(harmonized_csv_path)
    harm_header_set = set(harm_header)

    _ = output_dir.mkdir(parents=True, exist_ok=True)

    result: dict[str, Path] = {}
    for node, cdes in node_recommendations.items():
        node_path = output_dir / f"{node}.csv"
        _write_node_chunk(node, [cde for cde in cdes if cde in harm_header_set], rows, node_path)
        result[node] = node_path

    return result


def _write_node_chunk(node: str, node_cdes: list[str], rows: list[dict[str, str]], node_path: Path) -> None:
    """Write one node's chunk: a "type" column holding the node name, then that node's cdes."""
    with open(node_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["type"] + node_cdes)
        writer.writeheader()
        for row in rows:
            writer.writerow({"type": node} | {cde: row.get(cde, "") for cde in node_cdes})
 
 
def check_required_cdes(harm_header_set: set[str], node_cdes: dict[str, CDEProps]) -> list[str]:
    """Return the names of Req=True CDEs that have no column in harm_header at all."""
    return [cde for cde, props in node_cdes.items() if props.get("Req") is True and cde not in harm_header_set]
 
 
def check_required_values(
    harm_header_set: set[str], rows: list[dict[str, str]], node_cdes: dict[str, CDEProps]
) -> dict[str, dict[str, list[int] | int]]:
    """Return {cde: {"empty_lines": [line numbers], "count": N}} for present Req=True
    columns with an empty cell.

    'why' rows/count: knowing exactly which rows are missing a value lets
    someone go straight to those rows in the source file to fix them, and
    the count gives an at-a-glance sense of how widespread the gap is.
    """
    findings: dict[str, dict[str, list[int] | int]] = {}
 
    for cde, props in node_cdes.items():
        if props.get("Req") is not True or cde not in harm_header_set:
            continue
        empty_line_numbers = [i + 2 for i, row in enumerate(rows) if not row.get(cde, "").strip()]
        if empty_line_numbers:
            findings[cde] = {"empty_lines": empty_line_numbers, "count": len(empty_line_numbers)}
 
    return findings


def _should_check_type(cde: str, type_value: str | None, harm_header_set: set[str]) -> bool:
    """Whether a CDE's type is worth checking at all."""
    if type_value == _UNDEFINED_TYPE and cde in harm_header_set:
        print(f"WARNING: CDE '{cde}' has an undefined Type ('{_UNDEFINED_TYPE}') in the schema — type validation skipped for this column.")

    return (
        type_value is not None
        and type_value not in _PERMISSIVE_TYPES
        and type_value != _UNDEFINED_TYPE
        and cde in harm_header_set
    )


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


def _matches_boolean_type(value: str) -> bool:
    """Whether value is a recognized string representation of a boolean."""
    return value.strip().lower() in _BOOLEAN_VALUES


def _matches_datetime_type(value: str) -> bool:
    """Whether value parses as an ISO 8601 date or datetime.

    'why' only ISO format is attempted: this covers the formats produced by
    fromisoformat() (YYYY-MM-DD, full timestamps); a non-ISO format in real
    data would need a fallback added here once actually observed.
    """
    try:
        _ = datetime.fromisoformat(value.strip())
        return True
    except ValueError:
        return False


def _matches_semantic_type(value: str, type_value: str) -> bool | None:
    """Dispatch for named semantic types (boolean, datetime).

    'why' returns None (not False) when type_value isn't one of these:
    lets the caller distinguish "checked and failed" from "not one of my
    types, try something else" — collapsing both into False would make a
    real boolean/datetime mismatch indistinguishable from "this is
    actually a regex pattern."
    """
    if type_value == "boolean":
        return _matches_boolean_type(value)
    if type_value == "datetime":
        return _matches_datetime_type(value)
    return None


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

    semantic_result = _matches_semantic_type(value, type_value)
    if semantic_result is not None:
        return semantic_result

    return _matches_pattern_type(value, type_value)


def check_type_values(
    harm_header_set: set[str], rows: list[dict[str, str]], node_cdes: dict[str, CDEProps]
) -> dict[str, list[dict[str, int | str]]]:
    """Return {cde: [{line, value}]} for values that don't match their CDE's Type.

    - Type in _PERMISSIVE_TYPES ("string", "list"), or the CDE has no
      declared Type at all: not checked — this is the guard that keeps
      "string"/"list" from being misinterpreted as patterns in the regex
      branch below, and a CDE with no Type has nothing to validate against.
    - Type == "TBD": not checked, but prints a one-time warning naming the
      CDE — the schema itself hasn't finalized a real type here yet.
    - Type in _NUMERIC_TYPES ("number", "integer"): value must parse as a
      float or int, respectively.
    - Type == "boolean": value must be a recognized boolean string
      representation (true/false/1/0/yes/no, case-insensitive).
    - Type == "datetime": value must parse as an ISO 8601 date/datetime.
    - Anything else: treated as a regex pattern, value must re.fullmatch it.
    Empty cell values are skipped — required-ness is checked separately.
    """
    findings: dict[str, list[dict[str, int | str]]] = {}

    for cde, props in node_cdes.items():
        type_value = props.get("Type")
        if not _should_check_type(cde, type_value, harm_header_set):
            continue

        bad_rows = [
            {"line": i + 2, "value": row.get(cde, "").strip()}
            for i, row in enumerate(rows)
            if row.get(cde, "").strip() and not _value_matches_type(row.get(cde, "").strip(), type_value)
        ]
        if bad_rows:
            findings[cde] = bad_rows

    return findings


def _check_fk_column_values(
    column: str, property_name: str, rows: list[dict[str, str]], parent_path: Path
) -> list[dict[str, int | str]]:
    """Bad lines for one FK column whose value doesn't exist in the parent's own property column."""
    _, parent_rows = _read_csv_rows(parent_path)
    parent_values = {row.get(property_name, "").strip() for row in parent_rows}

    return [
        {"line": i + 2, "value": row.get(column, "").strip()}
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
        invalid_values: {column: [{line, value}]} for values that don't
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
    data_model_outputs_root: Path,
    output_path: Path,
    parent_sheets: dict[str, Path] | None = None,
) -> ValidationReport:
    """Run all 4 validation checks against a single node's harmonized sheet.

    Returns a combined report and always writes it to output_path as JSON if 
    the run is successful.

    Raises:
        FileNotFoundError: node_csv_path, a parent sheet path, or the resolved
            model JSON path don't exist.
        NotADirectoryError: data_model_outputs_root isn't valid directory,
        ValueError: node_csv_path has no usable header row, target_schema
            isn't one of the 4 supported schemas, the model JSON is empty, or
            node isn't a key in that model JSON.
    """
    harm_header, rows = _read_csv_rows(node_csv_path)
    model_json = _load_model_json(target_schema, data_model_outputs_root)
 
    if node not in model_json:
        raise ValueError(f"node '{node}' is not defined in the '{target_schema}' schema")
 
    node_cdes = model_json[node]

    harm_header_set = set(harm_header)
    missing_required_cdes = check_required_cdes(harm_header_set, node_cdes)
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


def _load_relationships(target_schema: str, data_model_outputs_root: Path) -> dict[str, list[str]]:
    """Load the {child: [parents]} relationship map for a schema.

    'why' a missing relationships.json falls back to an empty dict rather
    than raising: the FK check still runs correctly without it — it just
    falls back to passing no parent sheets at all, which surfaces as
    missing_parent_sheets findings rather than silently skipping the check.
    """
    model = target_schema.strip().upper()
    rel_path = data_model_outputs_root / model / "relationships.json"

    if not rel_path.exists():
        print(f"WARNING: relationships.json not found at {rel_path} — FK checks will report all parents as missing.")
        return {}

    with open(rel_path, "r", encoding="utf-8") as f:
        return cast(dict[str, list[str]], json.load(f))


def _build_final_results(
    results: dict[str, dict[str, ValidationReport | Path]],
    run_folder: Path,
) -> dict[str, dict[str, ValidationReport | Path]]:
    """Remap scratch-dir paths to their final locations inside the published
    run folder, and include the report file path alongside the in-memory report."""
    final: dict[str, dict[str, ValidationReport | Path]] = {}
    for node, entry in results.items():
        csv_path = entry["csv_path"]
        assert isinstance(csv_path, Path)
        final[node] = {
            "csv_path": run_folder / "chunks" / csv_path.name,
            "report_path": run_folder / "reports" / f"{node}_validation.json",
            "report": entry["report"],
        }
    return final


def _run_folder_name(source_stem: str) -> str:
    """Build a fresh run folder name: the source file's stem, plus the time
    the run finished and a short uuid so no two runs ever collide."""
    return f"{source_stem}_{datetime.now(timezone.utc):%m%d%y-%H%M%S%f}_{uuid4().hex[:4]}"


def _source_stem_from_run_folder(run_folder_name: str) -> str:
    """Recover the source file's stem from an existing run folder's name.

    'why' a regex rather than rsplit("_", 2): the stem itself usually
    contains underscores (e.g. "sampled_cds_single_sheet"), and a folder
    renamed by hand may carry no timestamp/uuid suffix at all. The pattern
    only strips a suffix that actually looks like one this module
    generated, and otherwise leaves the name alone.
    """
    return _RUN_FOLDER_SUFFIX.sub("", run_folder_name)


def _collect_existing_chunks(run_folder: Path) -> dict[str, Path]:
    """Read a previous run's chunks/ folder back as {node: csv_path}.

    'why' the node name comes from the filename: chunk_by_node writes
    "{node}.csv", so once the recommendations file is out of the picture
    the filename is the only record of which node a chunk belongs to.

    Raises:
        FileNotFoundError / NotADirectoryError: run_folder or its chunks/
            subfolder isn't there.
        ValueError: chunks/ exists but holds no CSVs, so there would be
            nothing to validate.
    """
    _check_path(run_folder, "dir")
    chunks_dir = run_folder / "chunks"
    _check_path(chunks_dir, "dir")

    chunks = {path.stem: path for path in sorted(chunks_dir.glob("*.csv"))}
    if not chunks:
        raise ValueError(f"No chunk CSVs found in {chunks_dir}")

    return chunks


def _stage_existing_chunks(run_folder: Path, dest_dir: Path) -> dict[str, Path]:
    """Mirror a previous run's chunks into dest_dir and return
    {node: staged_csv_path}.

    'why' the chunks are staged into the scratch dir instead of being
    validated where they lie: it lets a revalidation finish through exactly
    the same single atomic rename a fresh run uses. The scratch dir ends up
    holding the complete next run — old chunks, new reports — so that one
    rename publishes both at once, and nothing about the existing run
    folder is touched until after it has already succeeded.

    'why' hard links rather than copies: a link is a second name for the
    very same bytes on disk, so staging costs no extra space and cannot
    diverge from the original. Once the old run folder is removed, the
    staged names are the only ones left and the data lives on under them.
    Filesystems that refuse links fall back to a real copy.
    """
    chunks = _collect_existing_chunks(run_folder)
    dest_dir.mkdir(parents=True, exist_ok=True)

    staged: dict[str, Path] = {}
    for node, source in chunks.items():
        destination = dest_dir / source.name
        try:
            os.link(source, destination)
        except OSError:
            _ = shutil.copy2(source, destination)
        staged[node] = destination

    return staged


def _load_node_recommendations(node_recommendations_path: Path) -> NodeRecommendations:
    """Read suggest_node's {node: [cde, ...]} output off disk.

    Raises:
        FileNotFoundError / IsADirectoryError: the path isn't a readable file.
        ValueError: the file holds an empty mapping, so there'd be no node
            to chunk by.
    """
    _check_path(node_recommendations_path, "file")

    with open(node_recommendations_path, "r", encoding="utf-8") as f:
        node_recommendations = cast(NodeRecommendations, json.load(f))

    if not node_recommendations:
        raise ValueError("node_recommendations is empty or None")

    return node_recommendations


def _validate_chunks(
    chunks: dict[str, Path],
    target_schema: str,
    data_model_outputs_root: Path,
    reports_dir: Path,
) -> dict[str, dict[str, ValidationReport | Path]]:
    """Validate every node chunk, writing one report JSON per node into
    reports_dir, and return {node: {"csv_path", "report"}}.

    'why' shared by both the fresh and the revalidate path: the only thing
    that differs between them is where the chunks came from, not how they
    get checked or how their parent sheets are resolved.
    """
    relationships = _load_relationships(target_schema, data_model_outputs_root)

    results: dict[str, dict[str, ValidationReport | Path]] = {}
    for node, csv_path in chunks.items():
        parent_names = relationships.get(node, [])
        parent_sheets = {p: chunks[p] for p in parent_names if p in chunks}

        report = validate_node(
            node_csv_path=csv_path,
            node=node,
            target_schema=target_schema,
            data_model_outputs_root=data_model_outputs_root,
            output_path=reports_dir / f"{node}_validation.json",
            parent_sheets=parent_sheets,
        )

        results[node] = {"csv_path": csv_path, "report": report}

    if len(results) != len(chunks):
        raise RuntimeError(
            f"Expected {len(chunks)} validated node(s), got {len(results)} — refusing to publish partial results."
        )

    return results


def _publish_new_run(scratch_dir: Path, output_dir: Path, source_stem: str) -> Path:
    """Move a completed scratch build into its own new run folder.

    'why' this is the one atomic step: scratch_dir and the run folder are
    both direct children of output_dir (same filesystem), so this rename is
    guaranteed atomic by the OS — the run folder either doesn't exist yet,
    or exists complete with everything inside it. Every chunk CSV and every
    report JSON physically moves in this one step; nothing needs to be
    individually copied or re-written afterward.
    """
    run_folder = output_dir / _run_folder_name(source_stem)
    _ = scratch_dir.rename(run_folder)
    return run_folder


def _remove_existing_folder(folder: Path) -> None:
    """Delete the run folder a revalidation has just replaced.

    'why' a failure here warns instead of raising: this only ever runs
    after the publishing rename, so every chunk worth keeping is already
    reachable under the new folder's name and this folder is pure
    redundancy. A delete that fails costs a stale duplicate on disk, never
    data — and raising would turn that into a failed run whose results had
    in fact been published successfully.

    'why' it's reported rather than ignored outright: a leftover duplicate
    is confusing to whoever reads output_dir next, since two run folders
    for the same source file give no hint as to which one is valid.
    """
    try:
        shutil.rmtree(folder)
    except OSError as exc:
        print(f"WARNING: could not remove the old run folder {folder}: {exc}")


def chunk_and_validate(
    source_path: Path,
    harmonized_csv_path: Path,
    node_recommendations_path: Path,
    target_schema: str,
    data_model_outputs_root: Path,
    output_dir: Path,
    existing_run_folder: str | None = None,
) -> dict[str, dict[str, ValidationReport | Path]]:
    """Chunk a harmonized CSV by node, validate every resulting chunk, and
    publish the results into a brand-new, uniquely-named run folder inside
    output_dir — never modifying or deleting anything from a previous run.

    The run folder is named
    "{source_path.stem}_{MMDDYY-HHMMSSffffff}_{4-char-uuid}", and
    contains "chunks/" (one CSV per node) and "reports/" (one validation
    JSON per node).

    Pass existing_run_folder — the *name* of a run folder already sitting
    in output_dir, e.g. "sampled_cds_single_sheet_092926-201321357818_c717"
    — to skip chunking entirely and revalidate that run's existing chunks
    instead. Those chunks carry over unchanged, its reports/ are replaced
    with freshly-written ones, and the folder ends up renamed: same
    source-file stem, new timestamp and uuid. harmonized_csv_path and
    node_recommendations_path are unused in that mode.

    'why' the revalidated run still gets a new folder name: the timestamp
    in the name is meant to say when the reports inside were produced, so
    keeping the old name on freshly-written reports would be wrong.

    'why' a revalidation goes through the same publish-then-delete path as
    a fresh run rather than editing the existing folder in place: replacing
    reports/ and renaming the folder are two distinct changes, and making
    them in place means a crash between the two leaves a folder whose name
    disagrees with its contents. Staging the existing chunks (as hard
    links, so it costs no space) next to the new reports instead makes the
    scratch dir a complete run in its own right, so the single rename that
    publishes it commits both changes at once. The superseded folder is
    deleted only afterwards, where failing to delete it costs nothing but
    a stale duplicate.

    'why' everything is built in a hidden scratch folder inside output_dir,
    not tempfile.TemporaryDirectory (which usually resolves to /tmp, a
    different filesystem): Path.rename() is only atomic when source and
    destination are on the same filesystem. Building directly in output_dir
    and renaming within it guarantees the real run folder either doesn't
    exist at all, or exists complete with every chunk and every report
    inside it — never a partial state visible to anyone reading output_dir
    concurrently.

    'why' a raised exception still propagates after cleanup: the scratch
    folder is always removed (success or failure) so it never lingers on
    disk, but the caller still needs to know validation failed and why.

    Returns {node: {"csv_path": Path, "report_path": Path, "report": ValidationReport}},
    with csv_path and report_path pointing at the files' final locations
    inside the new (or newly-renamed) run folder.

    Raises:
        FileNotFoundError: node_recommendations_path, or the named
            existing run folder, doesn't exist.
        ValueError: node_recommendations is empty, or the named existing
            run folder holds no chunk CSVs.
        Whatever chunk_by_node or validate_node raise, propagated unchanged.
    """
    _ = output_dir.mkdir(parents=True, exist_ok=True)
    scratch_dir = output_dir / f".tmp-{uuid4().hex[:4]}"

    existing_folder = output_dir / existing_run_folder if existing_run_folder is not None else None

    try:
        if existing_folder is not None:
            chunks = _stage_existing_chunks(existing_folder, scratch_dir / "chunks")
            source_stem = _source_stem_from_run_folder(existing_folder.name)
        else:
            chunks = chunk_by_node(
                harmonized_csv_path=harmonized_csv_path,
                node_recommendations=_load_node_recommendations(node_recommendations_path),
                output_dir=scratch_dir / "chunks",
            )
            source_stem = source_path.stem

        results = _validate_chunks(
            chunks=chunks,
            target_schema=target_schema,
            data_model_outputs_root=data_model_outputs_root,
            reports_dir=scratch_dir / "reports",
        )

        # 'why' the run folder's timestamp is computed now, right before
        # publishing, not at the top of the function: it reflects when
        # the run actually completed
        run_folder = _publish_new_run(scratch_dir, output_dir, source_stem)

        # 'why' strictly after the rename above: that rename is the commit
        # point for both the new reports and the new folder name, so until
        # it has succeeded the existing run folder is still the only copy
        # of the chunks and has to stay exactly as it was.
        if existing_folder is not None:
            _remove_existing_folder(existing_folder)

        final_results = _build_final_results(results, run_folder)

        print(f"Run complete: {run_folder}")

        return final_results

    finally:
        # 'why' unconditional cleanup: if the rename above succeeded,
        # scratch_dir no longer exists under this name, so this is a no-op.
        # If anything raised before the rename, this removes the partial
        # scratch work so failed runs never accumulate on disk.
        shutil.rmtree(scratch_dir, ignore_errors=True)
