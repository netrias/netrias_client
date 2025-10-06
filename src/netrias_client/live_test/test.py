"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

import sys
from pathlib import Path

from dotenv import dotenv_values

from .. import configure, discover_cde_mapping, harmonize

ROOT = Path(__file__).resolve().parent
ENV = dotenv_values(ROOT / ".env")
csv_path = ROOT / "data" / "primary_diagnosis_1.csv"


api_key = ENV.get("NETRIAS_API_KEY")
configure(api_key=api_key)

cde_mapping = discover_cde_mapping(csv_path, target_schema="ccdi")

result = harmonize(csv_path, cde_mapping)

print(f"Harmonize status: {result.status}")
print(f"Harmonized file: {result.file_path}")
