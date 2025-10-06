"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import dotenv_values

ROOT = Path(__file__).resolve().parent
SRC_ROOT = ROOT.parent / "src"
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from netrias_client import configure, discover_mapping_from_csv, harmonize  # noqa: E402
from netrias_client._adapter import build_column_mapping_payload  # noqa: E402

ENV = dotenv_values(ROOT / ".env")


def _env_value(key: str, default: str | None = None) -> str | None:
    value = ENV.get(key)
    if value is None or not value.strip():
        return default
    return value.strip()


csv_path = Path(ROOT) / "data/primary_diagnosis_1.csv"
schema = "ccdi"

api_key = _env_value("NETRIAS_API_KEY") or ""
configure(api_key=api_key)

discovery = discover_mapping_from_csv(csv_path, target_schema=schema)

mapping_payload = build_column_mapping_payload(discovery)
print(mapping_payload)

result = harmonize(csv_path, mapping_payload)
print(f"Harmonize status: {result.status}")
print(f"Harmonized file: {result.file_path}")
