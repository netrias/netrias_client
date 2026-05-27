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

from ._data_model_store import get_pv_set_async
from ._models import ColumnKeyedManifestPayload, Settings

SKIP_THRESHOLD = 50


def _normalize(v: object) -> str | None:
    if pd.isna(v):
        return None
    return str(v).strip().lower()


async def run_overlap_analysis(
    source_path: Path,
    manifest: ColumnKeyedManifestPayload,
    settings: Settings,
    target_schema: str,
    target_version: str,
    output_dir: Path,
    logger: logging.Logger,
) -> None:
    """Compare raw column values against each mapped CDE's full PV set."""

    df = pd.read_csv(source_path, low_memory=False)
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
            "distinct_values": None,
            "matched_distinct": None,
            "matched_total": None,
            "total_value_match_rate": None,
            "top_matches": [],
        }

        if not cde_key:
            entry["status"] = "no_cde_mapped"
            report.append(entry)
            continue

        if col_name not in df.columns:
            entry["status"] = "skipped_missing_column"
            report.append(entry)
            continue

        raw_counts = df[col_name].value_counts(dropna=False)
        distinct_count = len(raw_counts)
        entry["distinct_values"] = distinct_count

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
        matched_total, total_rows = 0, 0
        for value, count in raw_counts.items():
            c = int(count)
            total_rows += c
            norm = _normalize(value)
            if norm and norm in pv_normalized:
                matched.append((value, c))
                matched_total += c

        matched.sort(key=lambda x: x[1], reverse=True)
        total_value_match_rate = (matched_total / total_rows) if total_rows else 0

        top_matches = [
            {"value": str(v), "rate": round(c / matched_total, 2)}
            for v, c in matched[:3]
        ] if matched_total else []

        entry["status"] = "ok"
        entry["matched_distinct"] = len(matched)
        entry["matched_total"] = matched_total
        entry["total_value_match_rate"] = round(total_value_match_rate, 2)
        entry["top_matches"] = top_matches

        for v, c in matched:
            flat_rows.append({
                "column_name": col_name,
                "cde_key": cde_key,
                "value": str(v),
                "count": c,
                "in_pv_set": True,
            })

        report.append(entry)
        logger.info(
            "%s -> %s: total_value_match_rate %.1f%% (%d/%d)",
            col_name, cde_key, total_value_match_rate * 100, matched_total, total_rows,
        )

    # Write outputs
    out_json = output_dir / "overlap_report.json"
    with open(out_json, "w") as f:
        json.dump(report, f, indent=2)
    logger.info("Wrote JSON report: %s", out_json)

    if flat_rows:
        out_csv = output_dir / "overlap_report.csv"
        pd.DataFrame(flat_rows).to_csv(out_csv, index=False)
        logger.info("Wrote CSV report: %s", out_csv)