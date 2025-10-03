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

# The client expects the API root because discovery and harmonization append
# their respective paths internally.
ENV = dotenv_values(ROOT / ".env")
API_URL = ENV.get("NETRIAS_API_URL") or "https://api.netriasbdf.cloud"


def _harmonization_override(key: str) -> str | None:
    """Temporary helper: fetch a harmonization override from the environment."""

    return ENV.get(key)

csv_path = Path(ROOT) / "data/primary_diagnosis_1.csv"
manifest_path = ROOT / "data/generated_manifest.json"
schema = "ccdi"

configure(api_key=(ENV.get("NETRIAS_API_KEY") or ""), api_url=API_URL)
discovery = discover_mapping_from_csv(csv_path, target_schema=schema)

mapping_payload = build_column_mapping_payload(discovery)
print(mapping_payload)

_ = manifest_path.write_text(json.dumps(mapping_payload, indent=2), encoding="utf-8")

# Temporary: harmonization still requires a separate credential in some environments.
harm_key = _harmonization_override("NETRIAS_HARMONIZATION_KEY")
harm_url = _harmonization_override("NETRIAS_HARMONIZATION_URL")
if harm_key is not None and harm_url is not None:
    configure(api_key=harm_key, api_url=harm_url)

result = harmonize(csv_path, manifest_path)
print(f"Harmonize status: {result.status}")
print(f"Harmonized file: {result.file_path}")
