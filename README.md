# Netrias Client

A Python client for the Netrias discovery and harmonization services. Transform CSV datasets to conform to standard data models (e.g., CCDI) with AI-powered column mapping.

## Installation

### With `uv` (recommended)
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh  # install uv once
uv add netrias_client
```

### With `pip`
```bash
python -m pip install netrias_client
```

---

## API Reference

### `NetriasClient(api_key)`

Create a new client instance with your API key. The client is ready to use immediately with default settings.

```python
from netrias_client import NetriasClient

# Provide your API key securely (e.g., from a secrets manager, environment variable, or config file)
client = NetriasClient(api_key="your-api-key")
```

| Parameter | Type | Description |
|-----------|------|-------------|
| `api_key` | `str` | **Required.** Bearer token for Netrias API authentication. Store securely and never commit to version control. |

---

### `configure(...)`

Optionally adjust settings after initialization. All parameters are optional.

```python
client.configure(
    timeout=1200.0,                    # Optional: request timeout in seconds (default: 20 minutes)
    log_level="INFO",                  # Optional: CRITICAL, ERROR, WARNING, INFO, DEBUG
    log_directory=Path("./logs"),      # Optional: directory for log files
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `timeout` | `float \| None` | `1200.0` | Request timeout in seconds (default: 20 minutes). |
| `log_level` | `str \| None` | `"INFO"` | Logging verbosity: `"CRITICAL"`, `"ERROR"`, `"WARNING"`, `"INFO"`, `"DEBUG"`. |
| `log_directory` | `Path \| str \| None` | `None` | Directory for per-client log files. When omitted, logs go to stdout only. |
| `discovery_url` | `str \| None` | Production URL | Override discovery API URL (for staging/testing). |
| `harmonization_url` | `str \| None` | Production URL | Override harmonization API URL (for staging/testing). |
| `data_model_store_url` | `str \| None` | Production URL | Override Data Model Store API URL (for staging/testing). |

**Note:** Calling `configure()` with partial parameters preserves previously-set values. Only the parameters you specify are updated.

---

## Discovery Methods

Discover how source columns map to target schema CDEs using AI recommendations.

### `discover_mapping_from_csv(...)`

Reads a CSV file, samples values, and returns a manifest mapping columns to the target schema.

```python
manifest = client.discover_mapping_from_csv(
    source_csv=Path("data/patients.csv"),
    target_schema="ccdi",
    target_version="latest",
    sample_limit=25,
    top_k=3,
    confidence_threshold=0.8,          # Optional: minimum confidence for recommendations
)
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_csv` | `Path` | — | **Required.** Path to the CSV file to analyze. |
| `target_schema` | `str` | — | **Required.** Target schema key. Available schemas (as of Jan 19, 2026): `ccdi`, `gc`, `synapse`, `sage_chipseq_template`, `sage_clinical_assay_template`, `sage_imaging_assay_template`, `sage_rnaseq_template`. |
| `target_version` | `str` | `"latest"` | Schema version to target. |
| `sample_limit` | `int` | `25` | Maximum rows to sample from the CSV for discovery. |
| `top_k` | `int` | `3` | Number of top recommendations to return per column. |
| `confidence_threshold` | `float \| None` | `0.8` | Minimum confidence score (0–1) for keeping recommendations. Lower values capture more tentative matches. |

**Returns:** `ManifestPayload` — A dictionary suitable for passing to `harmonize()`.

**Example Response:**

```python
{
    "column_mappings": {
        "patient_id": {"targetField": "participant_id"},
        "gender": {"targetField": "sex_at_birth"},
        "diagnosis": {
            "targetField": "primary_diagnosis",
            "route": "sagemaker:primary",
            "cdeId": -200,
            "cde_id": -200
        }
    }
}
```

---

## Harmonization Methods

Transform source CSV data using the discovered column mappings.

### `harmonize(...)`

Execute the harmonization workflow: submit job, poll for completion, download result.

```python
result = client.harmonize(
    source_path=Path("data/patients.csv"),
    manifest=manifest,                           # from discover_*
    output_path=Path("output/harmonized.csv"),   # optional
    manifest_output_path=Path("output/manifest.json"),  # optional
)

print(result.status)       # "succeeded", "failed", or "timeout"
print(result.file_path)    # Path to the harmonized CSV
print(result.description)  # Human-readable status message
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `source_path` | `Path` | — | **Required.** Path to the source CSV file. |
| `manifest` | `Path \| Mapping[str, object]` | — | **Required.** Mapping manifest (from discovery) or path to a JSON manifest file. |
| `output_path` | `Path \| None` | `None` | Where to write the harmonized CSV. Auto-generated if omitted (e.g., `source.harmonized.csv`). |
| `manifest_output_path` | `Path \| None` | `None` | Where to write the manifest JSON for debugging. |

**Returns:** `HarmonizationResult` with these fields:

| Field | Type | Description |
|-------|------|-------------|
| `file_path` | `Path` | Path to the output CSV file. |
| `status` | `"succeeded" \| "failed" \| "timeout"` | Job outcome. |
| `description` | `str` | Human-readable status message. |
| `mapping_id` | `str \| None` | Internal mapping identifier (if available). |

---

## Data Model Store Methods

Query reference data for validation: available data models, CDEs, and permissible values.

### `list_data_models(...)`

Fetch available data models (data commons).

```python
models = client.list_data_models(
    query="ccdi",              # optional: search by key/name
    include_versions=True,     # optional: include version metadata
    include_counts=True,       # optional: include CDE/PV counts
    limit=100,                 # optional: max results
    offset=0,                  # optional: skip N results
)

for model in models:
    print(f"{model.key}: {model.name}")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `query` | `str \| None` | `None` | Substring search on model key or name. |
| `include_versions` | `bool` | `False` | Include version metadata per model. |
| `include_counts` | `bool` | `False` | Include CDE/PV counts per version. |
| `limit` | `int \| None` | `None` | Maximum number of results. |
| `offset` | `int` | `0` | Number of results to skip (for pagination). |

**Returns:** `tuple[DataModel, ...]` where each `DataModel` has:

| Field | Type | Description |
|-------|------|-------------|
| `data_commons_id` | `int` | Internal ID. |
| `key` | `str` | Model key (e.g., `"ccdi"`). |
| `name` | `str` | Display name. |
| `description` | `str \| None` | Optional description. |
| `is_active` | `bool` | Whether the model is active. |

**Example:**

```python
(
    DataModel(data_commons_id=1, key="ccdi", name="CCDI", description="Childhood Cancer Data Initiative", is_active=True),
    DataModel(data_commons_id=2, key="gc", name="Genomic Commons", description=None, is_active=True),
)
```

---

### `list_cdes(...)`

Fetch Common Data Elements for a specific model version.

```python
cdes = client.list_cdes(
    model_key="ccdi",
    version="v1",
    include_description=True,  # optional
    query="diagnosis",         # optional: search by cde_key
    limit=100,                 # optional
    offset=0,                  # optional
)

for cde in cdes:
    print(f"{cde.cde_key}: {cde.description}")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_key` | `str` | — | **Required.** Data model key (e.g., `"ccdi"`). |
| `version` | `str` | — | **Required.** Version label (e.g., `"v1"`). |
| `include_description` | `bool` | `False` | Include CDE descriptions. |
| `query` | `str \| None` | `None` | Substring search on `cde_key`. |
| `limit` | `int \| None` | `None` | Maximum number of results. |
| `offset` | `int` | `0` | Number of results to skip. |

**Returns:** `tuple[CDE, ...]` where each `CDE` has:

| Field | Type | Description |
|-------|------|-------------|
| `cde_key` | `str` | CDE identifier (e.g., `"sex_at_birth"`). |
| `cde_id` | `int` | Internal CDE ID. |
| `cde_version_id` | `int` | Internal version ID. |
| `description` | `str \| None` | Optional description (if `include_description=True`). |

**Example:**

```python
(
    CDE(cde_key="sex_at_birth", cde_id=12345, cde_version_id=100, description="Biological sex assigned at birth"),
    CDE(cde_key="primary_diagnosis", cde_id=12346, cde_version_id=101, description="Primary cancer diagnosis"),
)
```

---

### `list_pvs(...)`

Fetch permissible values for a specific CDE.

```python
pvs = client.list_pvs(
    model_key="ccdi",
    version="v1",
    cde_key="sex_at_birth",
    include_inactive=False,    # optional
    query="Male",              # optional: search by value
    limit=100,                 # optional
    offset=0,                  # optional
)

for pv in pvs:
    print(f"{pv.value} (active={pv.is_active})")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_key` | `str` | — | **Required.** Data model key. |
| `version` | `str` | — | **Required.** Version label. |
| `cde_key` | `str` | — | **Required.** CDE key (e.g., `"sex_at_birth"`). |
| `include_inactive` | `bool` | `False` | Include inactive permissible values. |
| `query` | `str \| None` | `None` | Substring search on PV value. |
| `limit` | `int \| None` | `None` | Maximum number of results. |
| `offset` | `int` | `0` | Number of results to skip. |

**Returns:** `tuple[PermissibleValue, ...]` where each `PermissibleValue` has:

| Field | Type | Description |
|-------|------|-------------|
| `pv_id` | `int` | Internal PV ID. |
| `value` | `str` | The permissible value string. |
| `description` | `str \| None` | Optional description. |
| `is_active` | `bool` | Whether the PV is active. |

**Example:**

```python
(
    PermissibleValue(pv_id=1001, value="Male", description="Male sex at birth", is_active=True),
    PermissibleValue(pv_id=1002, value="Female", description="Female sex at birth", is_active=True),
    PermissibleValue(pv_id=1003, value="Unknown", description="Sex at birth unknown", is_active=True),
)
```

---

### `get_pv_set(...)`

Fetch all permissible values as a `frozenset` for O(1) membership testing. Auto-paginates to retrieve all values.

```python
pv_set = client.get_pv_set(
    model_key="ccdi",
    version="v1",
    cde_key="sex_at_birth",
    include_inactive=False,  # optional
)

# O(1) membership testing
if "Male" in pv_set:
    print("Valid value!")
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `model_key` | `str` | — | **Required.** Data model key. |
| `version` | `str` | — | **Required.** Version label. |
| `cde_key` | `str` | — | **Required.** CDE key. |
| `include_inactive` | `bool` | `False` | Include inactive permissible values. |

**Returns:** `frozenset[str]` — All permissible value strings for the CDE.

**Example:**

```python
frozenset({"Male", "Female", "Unknown"})
```

---

### `validate_value(...)`

Check if a single value is valid for a CDE. Convenience wrapper around `get_pv_set()`.

```python
is_valid = client.validate_value(
    value="Male",
    model_key="ccdi",
    version="v1",
    cde_key="sex_at_birth",
)
# Returns: True
```

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `value` | `str` | — | **Required.** The value to validate. |
| `model_key` | `str` | — | **Required.** Data model key. |
| `version` | `str` | — | **Required.** Version label. |
| `cde_key` | `str` | — | **Required.** CDE key. |

**Returns:** `bool` — `True` if the value is in the CDE's permissible values.

> **Note:** This method makes a network call on each invocation. For validating multiple values against the same CDE, call `get_pv_set()` once and reuse the returned `frozenset`.

---

## Async Support

All methods have async variants with the `_async` suffix. Use these when running in an async context (FastAPI, aiohttp, etc.):

```python
import asyncio
from pathlib import Path
from netrias_client import NetriasClient

client = NetriasClient(api_key="your-api-key")

# Sync usage (scripts, Jupyter notebooks)
manifest = client.discover_mapping_from_csv(
    source_csv=Path("data/patients.csv"),
    target_schema="ccdi",
)
result = client.harmonize(source_path=Path("data/patients.csv"), manifest=manifest)

# Async usage (FastAPI, async frameworks)
async def process_file():
    manifest = await client.discover_mapping_from_csv_async(
        source_csv=Path("data/patients.csv"),
        target_schema="ccdi",
    )
    result = await client.harmonize_async(
        source_path=Path("data/patients.csv"),
        manifest=manifest,
    )
    return result
```

| Sync Method | Async Method |
|-------------|--------------|
| `discover_mapping_from_csv()` | `discover_mapping_from_csv_async()` |
| `harmonize()` | `harmonize_async()` |
| `list_data_models()` | `list_data_models_async()` |
| `list_cdes()` | `list_cdes_async()` |
| `list_pvs()` | `list_pvs_async()` |
| `get_pv_set()` | `get_pv_set_async()` |
| `validate_value()` | `validate_value_async()` |

Sync methods work correctly in Jupyter notebooks and async web frameworks without event loop conflicts.

---

## Error Handling

The client raises typed exceptions that inherit from `NetriasClientError`:

| Exception | When Raised |
|-----------|-------------|
| `ClientConfigurationError` | Invalid configuration or `configure()` not called. |
| `FileValidationError` | Source file doesn't exist or is invalid. |
| `MappingDiscoveryError` | Discovery API returned a client error (4xx). |
| `MappingValidationError` | Manifest validation failed. |
| `OutputLocationError` | Cannot write to the output path. |
| `NetriasAPIUnavailable` | Network error, timeout, or server error (5xx). |
| `HarmonizationJobError` | Harmonization job failed or timed out. |
| `DataModelStoreError` | Data Model Store API returned a client error (4xx). |

```python
from netrias_client import NetriasClient, NetriasClientError, NetriasAPIUnavailable

try:
    result = client.harmonize(source_path=csv_path, manifest=manifest)
except NetriasAPIUnavailable as e:
    print(f"Service unavailable: {e}")
except NetriasClientError as e:
    print(f"Client error: {e}")
```

---

## Version

Access the installed package version:

```python
from netrias_client import __version__
print(__version__)  # e.g., "0.2.0"
```

---

## Logging Configuration

The client uses the `netrias_client` logger namespace. Configure logging externally before creating a client:

```python
import logging

# Set log level for all client instances
logging.getLogger("netrias_client").setLevel(logging.WARNING)

# Or add a custom handler
handler = logging.FileHandler("netrias.log")
handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
logging.getLogger("netrias_client").addHandler(handler)

# Then create your client
client = NetriasClient(api_key="your-api-key")
```

The `LOGGER_NAMESPACE` constant is exported for programmatic access:

```python
from netrias_client import LOGGER_NAMESPACE
logging.getLogger(LOGGER_NAMESPACE).setLevel(logging.DEBUG)
```

---

## Future Development

**Gateway Bypass Removal:** The `boto3` dependency and `discovery_use_gateway_bypass` configuration option exist as a temporary workaround for API Gateway timeout limitations during CDE discovery. Once the API Gateway timeout issues are resolved, the direct Lambda bypass will be removed and `boto3` will become an optional dependency. This change will be communicated in release notes.
