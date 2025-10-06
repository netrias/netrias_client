"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import dotenv_values

from .. import configure, discover_mapping_from_csv, harmonize
from .._adapter import build_column_mapping_payload

ROOT = Path(__file__).resolve().parent
ENV = dotenv_values(ROOT / ".env")


def _env_value(key: str, default: str | None = None) -> str | None:
    value = ENV.get(key)
    if value is None or not value.strip():
        return default
    return value.strip()


def main() -> None:
    csv_path = ROOT / "data" / "primary_diagnosis_1.csv"
    schema = "ccdi"

    api_key = _env_value("NETRIAS_API_KEY") or ""
    configure(api_key=api_key)

    discovery = discover_mapping_from_csv(csv_path, target_schema=schema)
    manifest_payload = build_column_mapping_payload(discovery)
    print(manifest_payload)

    result = harmonize(csv_path, manifest_payload)
    print(f"Harmonize status: {result.status}")
    print(f"Harmonized file: {result.file_path}")


if __name__ == "__main__":
    sys.exit(main())
