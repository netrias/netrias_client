# Netrias Client Contract

## Responsibility
- Deliver a minimal, ergonomic Python interface for interacting with the Netrias harmonization API.
- Shield consumers from transport, validation, and format-selection details while remaining transparent about failures.

## Scope
- Initial release supports CSV sources exclusively; XLSX support will follow once validation strategy is proven.
- Client targets simple, synchronous workflows first while offering async counterparts for parallel file processing.
- Mapping discovery is supported via a dedicated recommendation endpoint that emits potential CDE alignments for each input column.

## Supported Workflows
1. Configure the client once with `configure(api_key=..., api_url=..., timeout=..., log_level=...)` (explicit call required; no env auto-load).
2. Generate column mapping suggestions with `discover_mapping(target_schema=..., column_samples=...)` (sync) or `discover_mapping_async`, yielding a structured summary of recommended CDEs.
3. Run harmonization on a CSV with a provided manifest JSON via `harmonize` (sync) or `harmonize_async`, yielding a harmonized CSV written to disk.

## Module-Level State
- `_settings: Settings`
  - Internal dataclass storing API URL, API key, timeout, and logging level.
  - No environment reads on import; explicit `configure(...)` is required before any calls.
- `_logger`
  - Module logger honoring the configured log level.

## Public Functions

### `configure(*, api_key: str, api_url: str, timeout: float | None = None, log_level: str | None = None) -> None`
- 'why': normalize client setup and logging without exposing transport internals
- Explicit configuration is required. No default environment reads.
- Validates inputs immediately (e.g., non-empty API key and URL, positive timeout, supported log level).
- Raises `ClientConfigurationError` for invalid inputs.

### Mapping Discovery
- `'why': resolve how each CSV column should be harmonized before running the heavy transform`
- Public surface:
  - `discover_mapping(*, target_schema: str, column_samples: Mapping[str, Sequence[object]]) -> MappingDiscoveryResult`
  - `discover_mapping_async(*, target_schema: str, column_samples: Mapping[str, Sequence[object]]) -> Awaitable[MappingDiscoveryResult]`
  - `discover_mapping_from_csv(*, target_schema: str, source_csv: Path, sample_limit: int = 25) -> MappingDiscoveryResult`
  - `discover_mapping_from_csv_async(*, target_schema: str, source_csv: Path, sample_limit: int = 25) -> Awaitable[MappingDiscoveryResult]`
- Validates schema identifier and sample data locally, calls the CDE recommendation endpoint (`POST /cde-recommendation`) with payload:
  ```json
  {
    "body": "{\"target_schema\": \"<schema>\", \"data\": {"<column>": ["value", ...]}}"
  }
  ```
- Uses the configured `api_key` as an `x-api-key` header; respects the shared timeout setting.
- Returns a `MappingDiscoveryResult` with structured recommendations plus the raw response for callers that need the full payload.
- Raises `MappingValidationError` for bad inputs, `MappingDiscoveryError` for API domain failures, and `NetriasAPIUnavailable` for network faults.
 - CSV helpers read the header row and up to `sample_limit` rows to auto-assemble the discovery payload, eliminating client pre-processing.

### `harmonize(source_path: Path, manifest_path: Path, *, output_path: Path | None = None) -> HarmonizationResult`
- 'why': execute the harmonization workflow end-to-end for a single CSV
- Validates the CSV, manifest file, and output target; uploads source plus manifest; streams harmonized CSV to disk, matching the input format.
- Returns a `HarmonizationResult` for all outcomes. Does not raise on API domain errors; failures are reported via `status == "failed"`.

### `harmonize_async(source_path: Path, manifest_path: Path, *, output_path: Path | None = None) -> Awaitable[HarmonizationResult]`
- 'why': enable concurrent harmonization jobs without duplicating business logic
- Async counterpart to `harmonize` with identical validation and result semantics.

## Data Models

### `MappingDiscoveryResult`
- `'why': encapsulates discovery response so callers can inspect and persist recommendations`
- Fields:
  - `schema: str` — resolved schema identifier (echoed from the API when provided).
  - `suggestions: tuple[MappingSuggestion, ...]` — ordered list describing recommendations per source column.
  - `raw: Mapping[str, object]` — full JSON payload returned by the API for advanced diagnostics.

### `MappingSuggestion`
- `'why': communicate per-column recommendation context`
- Fields:
  - `source_column: str` — column header supplied in the request.
  - `options: tuple[MappingRecommendationOption, ...]` — zero or more recommendation candidates.
  - `raw: Mapping[str, object] | None` — original suggestion object for callers that need extra metadata.

### `MappingRecommendationOption`
- `'why': capture a single recommended target value and associated confidence'`
- Fields:
  - `target: str | None` — target CDE identifier or `None` when the API cannot determine a specific mapping.
  - `confidence: float | None` — optional numeric score provided by the service.
  - `raw: Mapping[str, object] | None` — original option payload for callers who need the low-level details.

### `HarmonizationResult`
- `'why': communicates harmonization outcome in a consistent shape`
- Fields:
  - `file_path: Path` — absolute path to the generated harmonized CSV.
  - `status: Literal["succeeded", "failed", "timeout"]` — overall result of the harmonization attempt.
  - `description: str` — human-readable summary (success details or failure cause).
  - `mapping_id: str | None` — echoed mapping identifier when supplied by the API.

## Exceptions
- `NetriasClientError` — base class for all client-specific exceptions.
- `ClientConfigurationError` — raised when configuration is incomplete or malformed.
- `FileValidationError` — raised for unreadable files, unsupported extensions, or invalid encodings.
- `MappingValidationError` — raised when mapping discovery inputs are malformed (e.g., missing columns, non-sequence samples).
- `MappingDiscoveryError` — raised when the discovery API reports a domain failure (non-2xx status).
- `OutputLocationError` — raised when the output path is unwritable or collides with an existing directory.
- `NetriasAPIUnavailable` — raised for timeouts or network failures.
  - Note: Harmonization failures from the API are returned as `HarmonizationResult(status="failed")` instead of raising.

## Logging
- Configure log level via `configure(log_level=...)`; defaults to `INFO` when unspecified.
- Emit INFO logs for harmonization start/finish with durations.
- Emit WARN logs for recoverable validation warnings.
- Emit ERROR logs before raising any exception described above.

## Thread Safety & Concurrency
- Configure once during process startup. Public sync functions operate by driving the shared async core with blocking semantics; async variants await the same coroutines without blocking.
- HTTP client implementation must avoid shared mutable session state across async tasks without proper locking.

## Extensibility Guardrails
- Keep public surface limited to functions enumerated above until v1.0.
- Versioning follows semantic versioning with breaking changes requiring major version increments.

## Documentation Commitments
- Quickstart examples in README demonstrating both sync and async flows.
- API reference auto-generated from docstrings once implementation lands.
