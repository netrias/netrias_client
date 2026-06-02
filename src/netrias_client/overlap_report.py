"""Post-discovery overlap report: compare raw column values against full CDE PV sets.

Runs after discover_mapping_from_tabular produces a manifest. For each
harmonizable column, fetches the complete PV set and measures how well
the raw data aligns with the target CDE's permissible values.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import pandas as pd

from ._tabular import TabularDataset
from ._data_model_store import get_pv_set_async
from ._models import ColumnKeyedManifestPayload, Settings

SKIP_THRESHOLD = 50


def _normalize(v: object) -> str | None:
    if pd.isna(v):
        return None
    return str(v).strip().lower()


async def run_overlap_analysis(
    dataset: TabularDataset,
    manifest: ColumnKeyedManifestPayload,
    settings: Settings,
    target_schema: str,
    target_version: str,
    output_dir: Path,
    logger: logging.Logger,
) -> None:
    """Compare raw column values against each mapped CDE's full PV set."""

    df = pd.DataFrame(dataset.rows, columns=dataset.headers)    
    report: list[dict] = []
    flat_rows: list[dict] = []

    for col_key, info in manifest["column_mappings"].items():
        col_name = info["column_name"]
        cde_key = info.get("cde_key")
        harmonization = info.get("harmonization")

        if harmonization != "harmonizable":
            continue

        entry: dict = {
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

        if not cde_key:
            entry["status"] = "no_cde_mapped"
            report.append(entry)
            continue

        if col_name not in df.columns:
            entry["status"] = "skipped_missing_column"
            report.append(entry)
            continue

        distinct_raw_counts = df[col_name].value_counts(dropna=False)
        distinct_count = len(distinct_raw_counts)
        missing_count = int(df[col_name].isna().sum())
        total_rows = len(df[col_name])
        non_null_rows = total_rows - missing_count
        entry["distinct_raw_values"] = distinct_count

        if distinct_count > SKIP_THRESHOLD:
            entry["status"] = "skipped_too_many_distinct"
            report.append(entry)
            continue

        try:
            pv_set = await get_pv_set_async(
                settings=settings,
                model_key=target_schema,
                version=target_version,
                cde_key=cde_key,
                include_inactive=False,
            )
        except Exception as e:
            entry["status"] = f"error_fetching_pvs: {e}"
            report.append(entry)
            logger.warning("PV fetch failed for %s/%s: %s", target_schema, cde_key, e)
            continue

        pv_normalized = {_normalize(pv) for pv in pv_set if pv is not None}

        matched = []
        matched_total = 0
        for value, count in distinct_raw_counts.items():
            c = int(count)
            norm = _normalize(value)
            if norm and norm in pv_normalized:
                matched.append((value, c))
                matched_total += c

        matched.sort(key=lambda x: x[1], reverse=True)
        match_rate_including_nulls = (matched_total / total_rows) if total_rows else 0
        match_rate_excluding_nulls = (matched_total / non_null_rows) if non_null_rows else 0

        top_matches = [
            {"value": str(v), "rate": round(c / matched_total, 2)}
            for v, c in matched[:3]
        ] if matched_total else []

        unmatched = []
        for value, count in distinct_raw_counts.items():
            c = int(count)
            norm = _normalize(value)
            if norm and norm not in pv_normalized:
                unmatched.append((value, c))

        unmatched.sort(key=lambda x: x[1], reverse=True)

        top_raw_unmatched = [
            {"value": str(v), "count": c}
            for v, c in unmatched[:3]
        ]

        entry["status"] = "ok"
        entry["matched_distinct_raw_values"] = len(matched)
        entry["matched_total_raw_values"] = matched_total
        entry["missing_count"] = missing_count
        entry["match_rate_including_nulls"] = round(match_rate_including_nulls, 2)
        entry["match_rate_excluding_nulls"] = round(match_rate_excluding_nulls, 2)
        entry["top_raw_matches"] = top_matches
        entry["top_raw_unmatched"] = top_raw_unmatched

        for value, count in distinct_raw_counts.items():
            norm = _normalize(value)
            flat_rows.append({
                "column_name": col_name,
                "cde_key": cde_key,
                "value": str(value),
                "normalized_value": norm,
                "count": int(count),
                "in_pv_set": norm is not None and norm in pv_normalized,
        })

        report.append(entry)

    # Write outputs
    out_json = output_dir / "overlap_report.json"
    with open(out_json, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Wrote JSON report: %s successfully ✅", out_json)

    if flat_rows:
        out_csv = output_dir / "overlap_report.csv"
        pd.DataFrame(flat_rows).to_csv(out_csv, index=False)
        logger.info("Wrote CSV report: %s successfully ✅", out_csv)