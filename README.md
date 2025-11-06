# Netrias Client

"""Explain how to install and exercise the Netrias harmonization client."""

## Install with `uv`
- Install `uv`: `curl -LsSf https://astral.sh/uv/install.sh | sh`
- Create, activate, and sync virtual environment:
```
uv venv
source .venv/bin/activate
uv sync
```

"""Use the Netrias to harmonize data via the Netrias API."""

```
import os
from pathlib import Path
from typing import Final

from dotenv import load_dotenv
from netrias_client import NetriasClient

# Create a Netrias Client and populate it with your API key
netrias_client = NetriasClient()
netrias_client.configure(api_key={Insert API key here})

# Point to the csv file to harmonize
CSV_PATH: Final[Path] = Path("data") / "primary_diagnosis_1.csv"

# Determine the data -> CDE mapping
manifest = netrias_client.discover_cde_mapping(source_csv=CSV_PATH, target_schema="ccdi")

# Harmonize the data
result = netrias_client.harmonize(source_path=CSV_PATH, manifest=manifest)
```


## Configuration Options
`NetriasClient.configure(...)` accepts additional tuning knobs. You can mix and match the ones you need:

| Parameter | Type | Purpose |
| --- | --- | --- |
| `api_key` | `str` | **Required.** Bearer token for authenticating with the Netrias services. |
| `confidence_threshold` | `float | None` | Minimum score (0.0–1.0) for keeping discovery recommendations; lower it to capture more tentative matches. |
| `timeout` | `float | None` | Override the default timeout for harmonization jobs. |
| `log_level` | `LogLevel | str | None` | Control verbosity (`INFO` by default). Accepts enum members or string names. |
| `discovery_use_gateway_bypass` | `bool | None` | Toggle the temporary AWS Lambda bypass path for discovery (defaults to `True`). |
| `log_directory` | `Path | str | None` | Directory for per-client log files. When omitted, logs stay on stdout. |

Configure only the options you need; unspecified values fall back to sensible defaults.

## Usage Notes
- `discover_cde_mapping(...)` samples CSV values and returns a manifest-ready payload
- Call `harmonize(...)` or `harmonize_async(...)` (async) with the manifest to download a harmonized CSV. The result object reports status, description, and the output path.
- The package exposes `__version__` so callers can assert the installed release.
- Optional extras (`netrias_client[aws]`) add boto3 helpers for the temporary gateway bypass.
