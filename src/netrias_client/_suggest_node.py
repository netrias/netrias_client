"""Classify a harmonized CSV's columns by which node they belong to.

'why': helps users understand which node(s) the harmonized CSV's columns
belong to, so they can pass them through the appropriate submission
readiness checklist and submit the right sheets to the portal.
"""

from __future__ import annotations

import csv
import json
from ._models import CDEProps
from pathlib import Path

# 'why': names this shape once so every function signature that touches the
# model JSON reflects its actual structure (node -> cde -> {Enum, Req, Type})
# instead of a generic, uninformative `dict`.
ModelJson = dict[str, dict[str, CDEProps]]

# 'why': suggest_node should reject any data_commons_key that isn't one of
# the four schemas this pipeline actually builds
VALID_DATA_COMMONS_KEYS = frozenset({"ctdc", "gc", "icdc", "psdc"})


def _read_csv_header(csv_path: Path) -> list[str]:
    """Read and validate the header row of a harmonized CSV.

    'why': only the header row is needed for classification, so we stop
    reading after the first row rather than loading the full file — this
    matters since harmonized outputs can be large.

    'why' both empty cases are checked here (rather than one being caught
    later by the caller): a file with zero rows (header=None from
    StopIteration) and a file whose first row is blank (header=[]) are
    both "no columns to classify," and should fail the same way at the
    same point instead of being caught in two different places.

    Raises:
        FileNotFoundError: csv_path doesn't exist or isn't a file.
        ValueError: the file has no header row, or the header row is empty.
    """
    if not csv_path.exists() or not csv_path.is_file():
        raise FileNotFoundError(f"Harmonized CSV not found: {csv_path}")

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.reader(f)
        header = next(reader, None)

    if not header:
        raise ValueError(f"Harmonized CSV has no usable header row: {csv_path}")

    return header


def _validate_data_commons_key(data_commons_key: str) -> str:
    """Validate data_commons_key and return its uppercased model folder name.

    Raises:
        ValueError: data_commons_key isn't one of the 4 supported schemas.
    """
    if not data_commons_key or data_commons_key.strip().lower() not in VALID_DATA_COMMONS_KEYS:
        raise ValueError(
            f"data_commons_key must be one of {sorted(VALID_DATA_COMMONS_KEYS)}, got: {data_commons_key!r}"
        )
    return data_commons_key.upper()


def _resolve_model_json_path(model: str, dm_outputs_root: Path) -> Path:
    """Resolve and confirm the model JSON path exists under dm_outputs_root.

    Raises:
        ValueError: dm_outputs_root isn't a directory.
        FileNotFoundError: no model JSON exists at the resolved path.
    """
    if not dm_outputs_root.exists() or not dm_outputs_root.is_dir():
        raise ValueError(f"dm_outputs_root is not a valid directory: {dm_outputs_root}")

    json_path = dm_outputs_root / model / f"{model}.json"
    if not json_path.exists():
        raise FileNotFoundError(f"No model JSON found at {json_path}")
    return json_path


def _load_model_json(data_commons_key: str, dm_outputs_root: Path) -> ModelJson:
    """Load only the target schema's enriched model JSON.

    'why': data_commons_key restricts which model JSON gets read — this
    function never scans dm_outputs_root or loads other schemas' JSONs
    even if several exist side by side under the same root. Key validation
    and path resolution are still split into their own helpers; parsing
    stays inline here since it's now just a single isinstance check.

    Raises:
        ValueError: data_commons_key isn't one of the 4 supported schemas,
            dm_outputs_root isn't a valid directory, or the model JSON isn't
            a dict at the top level. Note: an empty dict currently passes
            this check silently — the docstring previously implied that
            case was also rejected, but the code doesn't check for it.
        FileNotFoundError: no model JSON exists at the resolved path.
    """
    model = _validate_data_commons_key(data_commons_key)
    json_path = _resolve_model_json_path(model, dm_outputs_root)

    with open(json_path, "r", encoding="utf-8") as f:
        model_json = json.load(f)

    if not isinstance(model_json, dict):
        raise ValueError(f"Model JSON at {json_path} is empty or not a valid node->cde mapping")

    return model_json


def _build_cde_to_nodes_index(model_json: ModelJson) -> dict[str, list[str]]:
    """Build a reverse {cde: [nodes]} index from the model JSON.

    'why': a CDE can belong to more than one node (e.g. a shared linkage
    ID), so the index maps to a list, not a single node — preserving every
    node a CDE actually belongs to instead of picking one arbitrarily.
    """
    index: dict[str, list[str]] = {}
    for node, cdes in model_json.items():
        for cde in cdes.keys():
            index.setdefault(cde, []).append(node)
    return index


def _classify_harmonized_columns(
    harmonized_cdes: list[str],
    cde_to_nodes: dict[str, list[str]],
) -> tuple[dict[str, list[str]], list[str]]:
    """Classify each harmonized column by node, tracking any that match no CDE.

    Returns (result, unmapped) — result is {node: [matched cde, ...]}, unmapped
    is the list of harmonized columns that matched no CDE in the schema at all.

    'why' every harmonized column should already be a valid CDE for this
    schema by the time it reaches this step (harmonize() only writes columns
    it mapped against the target schema) — a miss here signals a real
    mismatch upstream, not an expected case to silently skip.
    """
    result: dict[str, list[str]] = {}
    unmapped: list[str] = []

    for cde in harmonized_cdes:
        nodes = cde_to_nodes.get(cde)
        if nodes is None:
            unmapped.append(cde)
            continue
        for node in nodes:
            result.setdefault(node, []).append(cde)

    return result, unmapped


def suggest_node(
    harmonized_csv_path: Path,
    data_commons_key: str,
    dm_outputs_root: Path,              # 'why': the root of the directory tree containing the model JSONs for all 4 schemas
    output_path: Path,

) -> dict[str, list[str]]:
    """Classify a harmonized CSV's columns by which model node they belong to.

    Raises:
        FileNotFoundError: harmonized_csv_path or the resolved model JSON path don't exist.
        ValueError: harmonized CSV has no usable header row, data_commons_key isn't one
            of the 4 supported schemas, dm_outputs_root isn't a valid directory, or the
            model JSON is empty/malformed.
    """
    harmonized_cdes = _read_csv_header(harmonized_csv_path)
    model_json = _load_model_json(data_commons_key, dm_outputs_root)
    cde_to_nodes = _build_cde_to_nodes_index(model_json)

    result, unmapped = _classify_harmonized_columns(harmonized_cdes, cde_to_nodes)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    _ = output_path.write_text(json.dumps(result, indent=2), encoding="utf-8")

    print("========================================")
    print(f"Node recommendations successfully created: {output_path}")

    if unmapped:
        print(f"WARNING: harmonized column(s) {unmapped} do not match any CDE in the '{data_commons_key}' schema.")

    mapped_count = len(harmonized_cdes) - len(unmapped)

    print(f"Total CDEs in harmonized CSV: {len(harmonized_cdes)}")
    print(f"Mapped: {mapped_count}")
    print(f"Unmapped: {len(unmapped)}")
    print("========================================")
    

    return result
