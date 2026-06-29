"""Post-discovery overlap report: compare raw column values against full CDE PV sets.

Runs after discover_mapping_from_tabular produces a manifest. For each
harmonizable column, fetches the complete PV set and measures how well
the raw data aligns with the target CDE's permissible values.
"""

from __future__ import annotations

import json
import logging
from collections import Counter
from pathlib import Path

import pandas as pd

from ._data_model_store import get_pv_set_async
from ._models import ColumnKeyedManifestPayload, Settings
from ._tabular import TabularDataset


def _normalize(v: object) -> str | None:
    if v is None:
        return None
    s = str(v).strip().lower()
    if not s:
        return None
    return s


def _compute_match_stats(
    raw_counts: Counter[str],
    pv_normalized: set[str | None],
) -> tuple[list[tuple[str, int]], list[tuple[str, int]], int]:
    matched: list[tuple[str, int]] = []
    unmatched: list[tuple[str, int]] = []
    matched_total = 0
    for value, count in raw_counts.items():
        norm = _normalize(value)
        if norm and norm in pv_normalized:
            matched.append((value, count))
            matched_total += count
        elif norm:
            unmatched.append((value, count))
    matched.sort(key=lambda x: x[1], reverse=True)
    unmatched.sort(key=lambda x: x[1], reverse=True)
    return matched, unmatched, matched_total


def _build_top_matches(matched: list[tuple[str, int]], matched_total: int) -> list[dict[str, object]]:
    if not matched_total:
        return []
    return [
        {"value": str(v), "rate": round(c / matched_total, 2)}
        for v, c in matched[:3]
    ]


def _build_top_unmatched(unmatched: list[tuple[str, int]]) -> list[dict[str, object]]:
    return [
        {"value": str(v), "count": c}
        for v, c in unmatched[:3]
    ]


def _build_flat_rows(
    col_key: str,
    col_name: str,
    cde_key: str,
    raw_counts: Counter[str],
    pv_normalized: set[str | None],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for value, count in raw_counts.items():
        norm = _normalize(value)
        rows.append({
            "column_key": col_key,
            "column_name": col_name,
            "cde_key": cde_key,
            "value": str(value),
            "normalized_value": norm,
            "count": count,
            "in_pv_set": bool(norm and norm in pv_normalized),
        })
    return rows


def _make_entry(col_key: str, col_name: str, cde_key: object) -> dict[str, object]:
    return {
        "column_key": col_key,
        "column_name": col_name,
        "cde_key": cde_key,
        "status": None,
        "distinct_raw_values": None,
        "matched_distinct_raw_values": None,
        "matched_total_raw_values": None,
        "missing_count": None,
        "match_rate_including_nulls": None,
        "match_rate_excluding_nulls": None,
        "top_raw_matches": [],
        "top_raw_unmatched": [],
    }


def _write_reports(
    report: list[dict[str, object]],
    flat_rows: list[dict[str, object]],
    output_dir: Path,
    logger: logging.Logger,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_json = output_dir / "overlap_report.json"
    with open(out_json, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Wrote JSON report: %s successfully ✅", out_json)

    out_csv = output_dir / "overlap_report.csv"
    columns = [
        "column_key",
        "column_name",
        "cde_key",
        "value",
        "normalized_value",
        "count",
        "in_pv_set",
    ]
    pd.DataFrame(flat_rows, columns=columns).to_csv(out_csv, index=False)
    logger.info("Wrote CSV report: %s successfully ✅", out_csv)


async def _process_column(
    col_key: str,
    info: dict[str, object],
    column_indexes: dict[str, int],
    dataset: TabularDataset,
    settings: Settings,
    target_schema: str,
    external_version_number: str,
    logger: logging.Logger,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    """Fetch PVs and compute overlap metrics for a single harmonizable column."""

    col_name = str(info["column_name"])
    cde_key = info.get("cde_key")
    entry = _make_entry(col_key, col_name, cde_key)

    if not cde_key:
        entry["status"] = "no_cde_mapped"
        return entry, []

    index = column_indexes.get(col_key)
    if index is None:
        entry["status"] = "skipped_missing_column"
        return entry, []

    values = [row[index] for row in dataset.rows]
    distinct_raw_counts = Counter(values)
    missing_count = sum(1 for v in values if _normalize(v) is None)
    total_rows = len(values)
    non_null_rows = total_rows - missing_count
    entry["distinct_raw_values"] = len(distinct_raw_counts)

    try:
        pv_set = await get_pv_set_async(
            settings=settings,
            model_key=target_schema,
            version=external_version_number,
            cde_key=str(cde_key),
            include_inactive=False,
        )
    except Exception as e:
        entry["status"] = f"error_fetching_pvs: {e}"
        logger.warning("PV fetch failed for %s/%s: %s", target_schema, cde_key, e)
        return entry, []

    pv_normalized = {_normalize(pv) for pv in pv_set}
    matched, unmatched, matched_total = _compute_match_stats(distinct_raw_counts, pv_normalized)

    match_rate_including_nulls = (matched_total / total_rows) if total_rows else 0
    match_rate_excluding_nulls = (matched_total / non_null_rows) if non_null_rows else 0

    entry["status"] = "ok"
    entry["matched_distinct_raw_values"] = len(matched)
    entry["matched_total_raw_values"] = matched_total
    entry["missing_count"] = missing_count
    entry["match_rate_including_nulls"] = round(match_rate_including_nulls, 2)
    entry["match_rate_excluding_nulls"] = round(match_rate_excluding_nulls, 2)
    entry["top_raw_matches"] = _build_top_matches(matched, matched_total)
    entry["top_raw_unmatched"] = _build_top_unmatched(unmatched)

    flat_rows = _build_flat_rows(col_key, col_name, str(cde_key), distinct_raw_counts, pv_normalized)
    return entry, flat_rows


async def run_overlap_analysis(
    dataset: TabularDataset,
    manifest: ColumnKeyedManifestPayload,
    settings: Settings,
    target_schema: str,
    external_version_number: str,
    output_dir: Path,
    logger: logging.Logger,
) -> None:
    """Compare raw column values against each mapped CDE's full PV set."""

    column_indexes = {col.key: col.index for col in dataset.columns}
    report: list[dict[str, object]] = []
    flat_rows: list[dict[str, object]] = []

    for col_key, info in manifest["column_mappings"].items():
        if info.get("harmonization") != "harmonizable":
            continue

        entry, column_flat_rows = await _process_column(
            col_key, info, column_indexes, dataset,
            settings, target_schema, external_version_number, logger,
        )
        report.append(entry)
        flat_rows.extend(column_flat_rows)

    _write_reports(report, flat_rows, output_dir, logger)
