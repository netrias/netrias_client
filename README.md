# Netrias Client

A Python client for the Netrias discovery and harmonization services.

Use it to map columns in tabular files to standard data models, then harmonize values to the target model’s permissible values. The client supports CSV, TSV, and XLSX files.

---

## Quickstart

This section walks through a complete first run using `uv`, a `.env` file, discovery, and harmonization.

### 1. Create a project

If you are starting in a brand-new directory, initialize the `uv` project first. `uv add` expects a `pyproject.toml` to already exist.

```bash
mkdir testing_netrias_client
cd testing_netrias_client

uv init
uv add netrias_client
```

For local testing, Python 3.12 is recommended:

```bash
uv python install 3.12
uv python pin 3.12
uv sync
```

### 2. Store your API key

Create a `.env` file:

```bash
echo "NETRIAS_API_KEY=your-api-key-here" > .env
echo ".env" >> .gitignore
```

Do not commit `.env` or print your API key in logs.

Check that `uv` can load the key:

```bash
uv run --env-file .env python -c "import os; print('NETRIAS_API_KEY present:', bool(os.environ.get('NETRIAS_API_KEY')))"
```

Expected output:

```text
NETRIAS_API_KEY present: True
```

### 3. Create a client

For normal production use, explicitly use `Environment.PROD`:

```python
import os
from netrias_client import Environment, NetriasClient

client = NetriasClient(
    api_key=os.environ["NETRIAS_API_KEY"],
    environment=Environment.PROD,
)
```

### 4. Inspect a tabular file

Create `inspect_csv.py`:

```python
from pathlib import Path
from netrias_client import read_tabular

source_path = Path("cds_submission_10col.csv")

dataset = read_tabular(source_path)

print("Source format:", dataset.source_format)
print("Number of columns:", len(dataset.columns))
print("Number of rows:", len(dataset.rows))

print("\nHeaders:")
for i, header in enumerate(dataset.headers):
    print(f"{i}: {header!r}")

print("\nStable column keys:")
for col in dataset.columns:
    print(f"{col.key}: {col.header!r}")
```

Run:

```bash
uv run --env-file .env python inspect_csv.py
```

### 5. Discover mappings

Discovery maps source columns to CDEs in the target data model.

For General Commons v2, use:

| Purpose | Value |
|---|---|
| Discovery schema | `target_schema="gc"` |
| Discovery external version number | `external_version_number="11.0.4"` |
| Harmonization data commons key | `data_commons_key="gc"` |
| Harmonization external version number | `external_version_number="11.0.4"` |

Create `discover_gc_v2.py`:

```python
import json
import os
from pathlib import Path
from pprint import pprint

from netrias_client import Environment, NetriasClient

client = NetriasClient(
    api_key=os.environ["NETRIAS_API_KEY"],
    environment=Environment.PROD,
)

source_path = Path("cds_submission_10col.csv")

manifest = client.discover_mapping_from_tabular(
    source_path=source_path,
    target_schema="gc",
    external_version_number="11.0.4",
    sample_limit=25,
    top_k=3,
    confidence_threshold=None,
)

print("\n--- RAW MANIFEST ---")
pprint(manifest)

output_path = Path("gc_v2_discovery_manifest.json")
with output_path.open("w", encoding="utf-8") as f:
    json.dump(manifest, f, indent=2, default=str)

print(f"\nSaved manifest to: {output_path.resolve()}")
```

Run:

```bash
uv run --env-file .env python discover_gc_v2.py
```

### 6. Review the discovery manifest

Create `inspect_manifest.py`:

```python
import json
from pathlib import Path

manifest_path = Path("gc_v2_discovery_manifest.json")

with manifest_path.open("r", encoding="utf-8") as f:
    manifest = json.load(f)

column_mappings = manifest["column_mappings"]

print(f"Columns in manifest: {len(column_mappings)}")

for column_key, mapping in column_mappings.items():
    print("\n---")
    print(f"Column key:      {column_key}")
    print(f"Source column:   {mapping.get('column_name')}")
    print(f"Chosen CDE:      {mapping.get('cde_key')}")
    print(f"CDE ID:          {mapping.get('cde_id')}")
    print(f"Harmonization:   {mapping.get('harmonization')}")

    print("Alternatives:")
    for alt in mapping.get("alternatives", []):
        print(
            f"  - {alt.get('target')} "
            f"(cde_id={alt.get('cde_id')}, "
            f"confidence={alt.get('confidence')}, "
            f"harmonization={alt.get('harmonization')})"
        )
```

Run:

```bash
uv run --env-file .env python inspect_manifest.py
```

### 7. Harmonize the file

Create `harmonize_gc_v2.py`:

```python
import os
from pathlib import Path
from pprint import pprint

from netrias_client import Environment, NetriasClient

client = NetriasClient(
    api_key=os.environ["NETRIAS_API_KEY"],
    environment=Environment.PROD,
)

result = client.harmonize(
    source_path=Path("cds_submission_10col.csv"),
    manifest=Path("gc_v2_discovery_manifest.json"),
    data_commons_key="gc",
    external_version_number="11.0.4",
    output_path=Path("output/cds_submission_10col.harmonized.csv"),
    manifest_output_path=Path("output/cds_submission_10col.manifest.json"),
    use_cache=True,
)

print("\n--- HARMONIZATION RESULT ---")
pprint(result)

print("\nStatus:", result.status)
print("Description:", result.description)
print("Output file:", result.file_path)
print("Job ID:", result.job_id)
print("Mapping ID:", result.mapping_id)
print("Downloaded manifest path:", result.manifest_path)
```

Run:

```bash
mkdir -p output
uv run --env-file .env python harmonize_gc_v2.py
```

A successful run produces files like:

```text
output/cds_submission_10col.harmonized.csv
output/cds_submission_10col.manifest.json
output/cds_submission_10col.harmonized.manifest.parquet
```

### 8. Compare source and harmonized outputs

Create `compare_source_vs_harmonized.py`:

```python
import csv
from collections import Counter, defaultdict
from pathlib import Path

source_path = Path("cds_submission_10col.csv")
harmonized_path = Path("output/cds_submission_10col.harmonized.csv")

def read_csv(path: Path):
    with path.open("r", encoding="utf-8-sig", newline="") as f:
        return list(csv.reader(f))

source_rows = read_csv(source_path)
harmonized_rows = read_csv(harmonized_path)

headers = source_rows[0]
source_data = source_rows[1:]
harmonized_data = harmonized_rows[1:]

print("Source rows:", len(source_data))
print("Harmonized rows:", len(harmonized_data))

changed_counts = Counter()
examples = defaultdict(list)

for row_idx, (source_row, harmonized_row) in enumerate(zip(source_data, harmonized_data), start=2):
    for col_idx, (before, after) in enumerate(zip(source_row, harmonized_row)):
        if before != after:
            header = headers[col_idx]
            changed_counts[header] += 1

            if len(examples[header]) < 10:
                examples[header].append((row_idx, before, after))

print("\nChanged cells by column:")
for header in headers:
    print(f"{header}: {changed_counts[header]}")

print("\nExamples:")
for header in headers:
    if changed_counts[header] == 0:
        continue

    print(f"\n--- {header} ---")
    for row_idx, before, after in examples[header]:
        print(f"row {row_idx}: {before!r} -> {after!r}")
```

Run:

```bash
uv run --env-file .env python compare_source_vs_harmonized.py
```

---

## Installation

### With `uv`

For a new project:

```bash
mkdir my_netrias_project
cd my_netrias_project

uv init
uv add netrias_client
```

For an existing `uv` project:

```bash
uv add netrias_client
```

### With `pip`

```bash
python -m pip install netrias_client
```

### Installing an unreleased GitHub branch or commit

If a fix has been merged to GitHub but is not yet on PyPI, install directly from GitHub:

```bash
uv add "netrias-client @ git+https://github.com/netrias/netrias_client.git@COMMIT_HASH"
```

Example:

```bash
uv add "netrias-client @ git+https://github.com/netrias/netrias_client.git@30a677f9804a1c4228f76fed850324c228b4e997"
```

If the environment gets into a broken state after switching between PyPI, TestPyPI, and GitHub installs, rebuild it:

```bash
rm -rf .venv
rm -f uv.lock
uv sync
```

---

## Concepts

### External version number

Discovery and harmonization both use the external Data Model Store version number.
For General Commons, use the same concrete value with the discovery schema and
the harmonization data commons key:

| Field | Used by | Example | Meaning |
|---|---|---|---|
| `external_version_number` | `discover_mapping_from_tabular()` | `"11.0.4"` | External data-model version number used for CDE recommendation |
| `external_version_number` | `harmonize()` | `"11.0.4"` | External data-model version number used by harmonization |

For General Commons v2:

```python
target_schema = "gc"
external_version_number = "11.0.4"
data_commons_key = "gc"
```

### Data commons key casing

`data_commons_key` is case-sensitive and should match the key returned by the Data Model Store. For General Commons, use lowercase:

```python
data_commons_key="gc"
```

Using uppercase `"GC"` may fail if the underlying service/database stores the key as `"gc"`.

### Stable column keys

The client represents tabular columns positionally. Each source column gets a stable key:

```text
col_0000
col_0001
col_0002
```

Headers are display labels. Stable column keys prevent data loss when files contain duplicate, blank, or repeated headers.

---

## API Reference

### `NetriasClient(...)`

Create a new client instance.

```python
import os
from netrias_client import Environment, NetriasClient

client = NetriasClient(
    api_key=os.environ["NETRIAS_API_KEY"],
    environment=Environment.PROD,
)
```

| Parameter | Type | Description |
|---|---|---|
| `api_key` | `str` | Required. Netrias API key. Store securely and never commit to version control. |
| `environment` | `Environment \| None` | Environment to use. For production usage, pass `Environment.PROD`. |

### `configure(...)`

Optionally adjust settings after initialization.

```python
from pathlib import Path

client.configure(
    timeout=1200.0,
    log_level="INFO",
    log_directory=Path("./logs"),
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `timeout` | `float \| None` | `1200.0` | Request timeout in seconds. |
| `log_level` | `str \| None` | `"INFO"` | Logging verbosity: `"CRITICAL"`, `"ERROR"`, `"WARNING"`, `"INFO"`, `"DEBUG"`. |
| `log_directory` | `Path \| str \| None` | `None` | Directory for per-client log files. When omitted, logs go to stdout only. |
| `discovery_url` | `str \| None` | Environment default | Override discovery API URL for development/testing. |
| `harmonization_url` | `str \| None` | Environment default | Override harmonization API URL for development/testing. |
| `data_model_store_url` | `str \| None` | Environment default | Override Data Model Store API URL for development/testing. |

Calling `configure()` with partial parameters preserves previously set values. Only the parameters you specify are updated.

---

## Discovery Methods

### Tabular files and column identity

CSV, TSV, and XLSX are file formats at the SDK boundary. Inside the client, data is represented as a positional tabular dataset:

```python
from pathlib import Path
from netrias_client import read_tabular

dataset = read_tabular(Path("cds_submission_10col.csv"))

print(dataset.source_format)
print(dataset.headers)
print(dataset.columns[0].key)
print(dataset.rows[0])
```

For XLSX workbooks, select one worksheet at the boundary:

```python
from pathlib import Path
from netrias_client import list_workbook_sheets, read_tabular

sheets = list_workbook_sheets(Path("source_workbook.xlsx"))
dataset = read_tabular(Path("source_workbook.xlsx"), sheet_name=sheets[0].name)
```

Supported tabular formats are exposed in code:

```python
from netrias_client import SUPPORTED_TABULAR_FORMATS, SUPPORTED_TABULAR_SUFFIXES, TabularFormat

assert tuple(SUPPORTED_TABULAR_FORMATS) == (
    TabularFormat.CSV,
    TabularFormat.TSV,
    TabularFormat.XLSX,
)
assert set(SUPPORTED_TABULAR_SUFFIXES) == {".csv", ".tsv", ".xlsx"}
```

### `discover_mapping_from_tabular(...)`

Reads a supported tabular file, samples values, and returns a manifest keyed by stable source column keys. Optionally generates an overlap report comparing raw input values against full CDE permissible value sets.

```python
from pathlib import Path

manifest = client.discover_mapping_from_tabular(
    source_path=Path("cds_submission_10col.xlsx"),
    target_schema="gc",
    external_version_number="11.0.4",
    sample_limit=25,
    top_k=3,
    confidence_threshold=0.8,
    generate_raw_overlap_report=True,           # optional, defaults to False
    overlap_report_output_dir=Path("output"),   # optional, defaults to "output"
)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source_path` | `Path` | - | Required. Path to a supported tabular file: CSV, TSV, or XLSX. |
| `target_schema` | `str` | - | Required. Target schema key, such as `"gc"`. |
| `external_version_number` | `str` | - | Required. Concrete external data-model version number, such as `"11.0.4"`. Do not use `"latest"`. |
| `sheet_name` | `str \| None` | `None` | Worksheet to read for XLSX input. Defaults to the first sheet. |
| `sample_limit` | `int` | `25` | Maximum rows to sample for discovery. |
| `top_k` | `int` | `3` | Number of top recommendations to return per column. |
| `confidence_threshold` | `float \| None` | `0.8` | Minimum confidence score for keeping recommendations. Use `None` to keep all returned recommendations. |
| `generate_raw_overlap_report` | `bool` | `False` | When `True`, compares raw column values against full CDE PV sets and writes `overlap_report.json` and `overlap_report.csv` to the specified output directory. Supports all tabular formats handled by `read_tabular`. |
| `overlap_report_output_dir` | `Path \| None` | `Path("output")` | Directory for overlap report files. Defaults to `Path("output")` when not provided. Only used when `generate_raw_overlap_report` is `True`. |

Returns a `ColumnKeyedManifestPayload`, a dictionary suitable for passing to `harmonize()`:

```python
{
    "column_mappings": {
        "col_0000": {
            "column_name": "diagnosis",
            "cde_key": "primary_diagnosis",
            "cde_id": 376,
            "harmonization": "harmonizable",
            "alternatives": [
                {
                    "target": "primary_diagnosis",
                    "confidence": 0.95,
                    "harmonization": "harmonizable",
                    "cde_id": 376,
                }
            ],
        }
    }
}
```

**Overlap Report Output** (when `generate_raw_overlap_report=True`):

Two files are written to `overlap_report_output_dir`:

- `overlap_report.json` — per-column null and non-null match rates, top matched/unmatched values, null counts.
```python
{
  "column_name": "race",
  "cde_key": "race",
  "status": "ok",
  "distinct_raw_values": 5,
  "matched_distinct_raw_values": 4,
  "matched_total_raw_values": 850,
  "missing_count": 12,
  "match_rate_including_nulls": 0.89,
  "match_rate_excluding_nulls": 0.92,
  "top_raw_matches": [
    {"value": "White", "rate": 0.55},
    {"value": "Black or African American", "rate": 0.28},
    {"value": "Asian", "rate": 0.09}
  ],
  "top_raw_unmatched": [
    {"value": "Unkown", "count": 8},
    {"value": "N/A", "count": 5},
    {"value": "other", "count": 3}
  ]
}
```

- `overlap_report.csv` — flat row-per-value format with all distinct values, respective counts and `in_pv_set: True/False`

---

## Harmonization Methods

## Harmonization Methods

### `harmonize(...)`

Submit a harmonization job, poll for completion, and download the result.

```python
from pathlib import Path

result = client.harmonize(
    source_path=Path("cds_submission_10col.csv"),
    manifest=manifest,
    data_commons_key="gc",
    external_version_number="11.0.4",
    output_path=Path("output/cds_submission_10col.harmonized.csv"),
    manifest_output_path=Path("output/cds_submission_10col.manifest.json"),
    use_cache=True,
)

print(result.status)
print(result.file_path)
print(result.description)
print(result.job_id)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `source_path` | `Path` | - | Required. Path to the source tabular file: CSV, TSV, or XLSX. |
| `manifest` | `Path \| Mapping[str, object]` | - | Required. Mapping manifest from discovery or a path to a JSON manifest file. |
| `data_commons_key` | `str` | - | Required. Target data commons key, such as `"gc"`. This is case-sensitive. |
| `external_version_number` | `str` | - | Required. Concrete external data-model version number, such as `"11.0.4"`. |
| `output_path` | `Path \| None` | `None` | Where to write the harmonized file. Auto-generated when omitted. |
| `manifest_output_path` | `Path \| None` | `None` | Where to write the manifest JSON for debugging. |
| `sheet_name` | `str \| None` | `None` | Worksheet to read and update for XLSX input. Defaults to the first sheet. |
| `use_cache` | `bool` | `True` | When `False`, asks the service to bypass cached harmonization results. |

Returns a `HarmonizationResult`:

| Field | Type | Description |
|---|---|---|
| `file_path` | `Path` | Path to the harmonized output file. |
| `status` | `"succeeded" \| "failed" \| "timeout"` | Job outcome. |
| `description` | `str` | Human-readable status message. |
| `job_id` | `str \| None` | API job identifier, when submission succeeded. |
| `mapping_id` | `str \| None` | Internal mapping identifier, if available. |
| `manifest_path` | `Path \| None` | Path to the downloaded manifest parquet file, if available. |

---

## Data Model Store Methods

Use these methods to discover available data models, versions, CDEs, and permissible values.

### `list_data_models(...)`

```python
models = client.list_data_models(
    query="gc",
    include_versions=True,
    include_counts=True,
    limit=100,
)

for model in models:
    print(f"{model.key}: {model.name}")
    for version in model.versions or ():
        print(version)
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `query` | `str \| None` | `None` | Substring search on model key or name. |
| `include_versions` | `bool` | `False` | Include version metadata per model. |
| `include_counts` | `bool` | `False` | Include CDE/PV counts per version. |
| `limit` | `int \| None` | `None` | Maximum number of results. |
| `offset` | `int` | `0` | Number of results to skip. |

### `list_cdes(...)`

```python
cdes = client.list_cdes(
    model_key="gc",
    version="2",
    include_description=True,
    query="diagnosis",
    limit=100,
)

for cde in cdes:
    print(f"{cde.cde_key}: {cde.description}")
```

| Parameter | Type | Default | Description |
|---|---|---|---|
| `model_key` | `str` | - | Required. Data model key, such as `"gc"`. |
| `version` | `str` | - | Required. Concrete model version string used by the Data Model Store route. |
| `include_description` | `bool` | `False` | Include CDE descriptions. |
| `query` | `str \| None` | `None` | Substring search on `cde_key`. |
| `limit` | `int \| None` | `None` | Maximum number of results. |
| `offset` | `int` | `0` | Number of results to skip. |

### `get_pv_set(...)`

Fetch all permissible values as a `frozenset` for O(1) membership testing. Auto-paginates to retrieve all values.

```python
pv_set = client.get_pv_set(
    model_key="gc",
    version="2",
    cde_key="sex",
    include_inactive=False,
)

if "Male" in pv_set:
    print("Valid value")
```

### `validate_value(...)`

Check if a single value is valid for a CDE. For many values against the same CDE, call `get_pv_set()` once and reuse the returned set.

```python
is_valid = client.validate_value(
    value="Male",
    model_key="gc",
    version="2",
    cde_key="sex",
)
```

---



---

## Async Support

All main client methods have async variants with the `_async` suffix.

```python
from pathlib import Path
from netrias_client import Environment, NetriasClient

client = NetriasClient(
    api_key="your-api-key",
    environment=Environment.PROD,
)

async def process_file():
    manifest = await client.discover_mapping_from_tabular_async(
        source_path=Path("cds_submission_10col.csv"),
        target_schema="gc",
        external_version_number="11.0.4",
    )

    result = await client.harmonize_async(
        source_path=Path("cds_submission_10col.csv"),
        manifest=manifest,
        data_commons_key="gc",
        external_version_number="11.0.4",
    )

    return result
```

| Sync Method | Async Method |
|---|---|
| `discover_mapping_from_tabular()` | `discover_mapping_from_tabular_async()` |
| `harmonize()` | `harmonize_async()` |
| `list_data_models()` | `list_data_models_async()` |
| `list_cdes()` | `list_cdes_async()` |
| `list_pvs()` | `list_pvs_async()` |
| `get_pv_set()` | `get_pv_set_async()` |
| `validate_value()` | `validate_value_async()` |

---

## Error Handling

The client raises typed exceptions that inherit from `NetriasClientError`.

```python
from netrias_client import NetriasAPIUnavailable, NetriasClientError

try:
    result = client.harmonize(
        source_path=csv_path,
        manifest=manifest,
        data_commons_key="gc",
        external_version_number="11.0.4",
    )
except NetriasAPIUnavailable as e:
    print(f"Service unavailable: {e}")
except NetriasClientError as e:
    print(f"Client error: {e}")
```

| Exception | When Raised |
|---|---|
| `ClientConfigurationError` | Invalid client configuration. |
| `FileValidationError` | Source file does not exist or is invalid. |
| `MappingDiscoveryError` | Discovery API returned a client error or invalid response. |
| `MappingValidationError` | Manifest validation failed. |
| `OutputLocationError` | Cannot write to the output path. |
| `NetriasAPIUnavailable` | Network error, timeout, or server error. |
| `HarmonizationJobError` | Harmonization job failed or timed out. |
| `DataModelStoreError` | Data Model Store API returned a client error. |

---

## Troubleshooting

| Error or symptom | Likely cause | Fix |
|---|---|---|
| `No pyproject.toml found` | `uv add` was run in a directory that is not a uv project. | Run `uv init` first, then `uv add netrias_client`. |
| `KeyError: 'NETRIAS_API_KEY'` | Python cannot see the API key. | Put `NETRIAS_API_KEY=...` in `.env` and run scripts with `uv run --env-file .env ...`. |
| `Missing Authentication Token` during discovery | Client is using the wrong API route/environment. | Create the client with `environment=Environment.PROD`. |
| `unknown data-model version ... label=None, number=None` | Missing `external_version_number`, or `data_commons_key` does not match the stored key. | Use a concrete external version, e.g. `"11.0.4"`, and lowercase `data_commons_key="gc"` for General Commons. |
| `ModuleNotFoundError` after switching package sources | Local `.venv` is inconsistent after switching between PyPI, TestPyPI, and GitHub installs. | Run `rm -rf .venv uv.lock && uv sync`. |
| Harmonization succeeds but some mappings look surprising | The chosen PV may be valid but semantically questionable. | Inspect the output CSV and the downloaded manifest parquet; consider reviewing the discovery manifest before harmonization. |

---

## Logging

The client uses the `netrias_client` logger namespace.

```python
import logging
from netrias_client import LOGGER_NAMESPACE

logging.getLogger(LOGGER_NAMESPACE).setLevel(logging.INFO)
```

To write logs to a file through client configuration:

```python
from pathlib import Path

client.configure(
    log_level="DEBUG",
    log_directory=Path("./logs"),
)
```

---

## Version

Access the installed package version:

```python
from netrias_client import __version__

print(__version__)
```

If you install from a GitHub commit, the printed version may not change unless the branch also updates the package version metadata. To confirm the import path:

```bash
uv run python -c "import netrias_client; print(netrias_client.__version__); print(netrias_client.__file__)"
```

---

## Future Development

The `boto3` dependency and gateway-bypass discovery configuration currently exist as a temporary workaround for discovery API Gateway timeout limitations. Once the direct API path fully supports the needed workloads, the bypass path may be removed or made optional.
