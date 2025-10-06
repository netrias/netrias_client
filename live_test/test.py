"""Minimal live test harness for the Netrias client."""

from __future__ import annotations

import json
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
manifest_path = ROOT / "data/generated_manifest.json"
schema = "ccdi"

api_key = _env_value("NETRIAS_API_KEY") or ""
discovery_url = _env_value("NETRIAS_API_URL", "https://api.netriasbdf.cloud")
harmonization_url = _env_value("NETRIAS_HARMONIZATION_URL", discovery_url)

configure(
    api_key=api_key,
    api_url=discovery_url,
    discovery_use_gateway_bypass=True,
)
discovery = discover_mapping_from_csv(csv_path, target_schema=schema)

mapping_payload = build_column_mapping_payload(discovery)
print(mapping_payload)

_ = manifest_path.write_text(json.dumps(mapping_payload, indent=2), encoding="utf-8")

if harmonization_url != discovery_url:
    configure(api_key=api_key, api_url=harmonization_url)

result = harmonize(csv_path, manifest_path)
print(f"Harmonize status: {result.status}")
print(f"Harmonized file: {result.file_path}")
