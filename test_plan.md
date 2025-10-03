# Netrias Client Test Plan

## Philosophy
- Exercise behaviours through the public API (`configure`, `harmonize`, `harmonize_async`) so coverage aligns with real usage.
- Prefer feature-level scenarios spanning validation → HTTP orchestration → disk writes → logging, stubbing external services only at the HTTP transport boundary.
- Keep fixtures minimal and colocated under `src/netrias_client/tests` so the library stays self-contained and portable.
- Adopt the `#Given`, `#When`, `#Then` structure inside pytest cases to keep intent obvious.

## Coverage Matrix
- **Configuration Guardrails** → `netrias_client/_config.py`, `netrias_client/_logging.py`
- **Input Validation** → `netrias_client/_validators.py`
- **Transport & Result Handling** → `netrias_client/_http.py`, `netrias_client/_core.py`, `netrias_client/_models.py`
- **Local IO Guarantees** → `netrias_client/_io.py`
- **Concurrency Safety** → shared async core inside `netrias_client/_core.py`

## Test Data & Utilities
- CSV fixtures that cover: valid baseline, oversized (>250 MB simulated via monkeypatched `os.path.getsize`), malformed extension.
- Minimal manifest JSON fixtures (valid + invalid extension).
- Output directories created within `tmp_path_factory` to avoid touching the repo.
- `MockTransportFactory` helper wrapping `httpx.MockTransport` plus convenience builders for success/failure/timeout payloads.
- Logging capture helper to assert error/info lines without polluting stdout.

## Execution Flow
- Default command: `uv run pytest src/netrias_client/tests`.
- Quality gates to run after tests: `uv run ruff check src/netrias_client` and `uv run basedpyright src/netrias_client`.

## Scenarios

### Configuration Guardrails
- **Configuration Required**
  - #Given `configure` has not been called.
  - #When `harmonize` executes.
  - #Then a `ClientConfigurationError` bubbles with the "call configure" guidance.
- **Input Normalization**
  - #Given attempts to call `configure` with blank `api_key`, blank `api_url`, unsupported `log_level`, or non-positive `timeout`.
  - #When the function executes.
  - #Then each invalid input raises `ClientConfigurationError` with precise messaging.
- **Logging Level Applied**
  - #Given `configure(log_level="DEBUG")` has been executed.
  - #When harmonization runs against a mocked success response.
  - #Then the module logger reports at DEBUG level (captured via `caplog`) and emits the expected start/complete entries once.

### Validation & Output Location
- **Source CSV Validation**
  - #Given permutations of: missing file, directory path, wrong suffix, oversized file (size patched), unreadable file (permission error).
  - #When `harmonize` is invoked.
  - #Then `FileValidationError` is raised before any HTTP call (assert no transport mock hits).
- **Manifest Path Validation**
  - #Given manifest path that is missing, directory, or incorrect suffix.
  - #When `harmonize` executes.
  - #Then `FileValidationError` surfaces early.
- **Output Path Handling**
  - #Given permutations: default `None`, explicit directory, explicit file path in writable directory, path colliding with existing file, path under non-existent parent, path under non-writable directory.
  - #When `harmonize` is called with a mocked success response.
  - #Then the destination path resolves correctly, directories are created as needed, and `OutputLocationError` is raised for unwritable/colliding cases prior to HTTP traffic.

### Harmonization Success Path
- **Streaming Success Writes File**
  - #Given valid CSV & manifest fixtures and a mocked 200 response streaming CSV bytes plus JSON metadata header if available.
  - #When `harmonize` runs with default output location.
  - #Then a `.harmonized.csv` file is created beside the fixture, file contents match the streamed payload, and `HarmonizationResult` reports `status == "succeeded"` with description text.
- **Explicit Output Target**
  - #Given an explicit file path.
  - #When `harmonize` runs.
  - #Then the harmonized contents land at the specified path and the result reflects that exact `Path`.
- **Manifest Echo / Mapping ID**
  - #Given the mocked API includes a JSON trailer with `mapping_id`.
  - #When harmonization completes.
  - #Then the `HarmonizationResult.mapping_id` carries the value.

### Harmonization Failure Surfaces
- **HTTP 4xx Domain Failure**
  - #Given mocked 400/422 responses with JSON payloads describing the failure.
  - #When `harmonize` executes.
  - #Then the function returns `HarmonizationResult(status="failed")` populated with the message from `message`/`detail`/`error` and logs an ERROR once; no exception raised.
- **Non-JSON Error Payload**
  - #Given mocked 500 response with plain text body.
  - #When `harmonize` executes.
  - #Then description falls back to "harmonization failed (HTTP 500)".
- **Transport Errors**
  - #Given mocked transports raising `httpx.TransportError` subclasses.
  - #When `harmonize` executes.
  - #Then `NetriasAPIUnavailable` is raised and error logs contain the transport message.
- **Client Timeout**
  - #Given `configure(timeout=0.1)` and transport coroutine sleeping beyond the deadline.
  - #When `harmonize` executes.
  - #Then `HarmonizationResult.status == "timeout"`, description matches "request timed out", and the partial file is not written (destination path absent).

### Async Concurrency
- **Concurrent Harmonizations**
  - #Given three CSV fixtures and an `asyncio.gather` over `harmonize_async` with unique mocked responses.
  - #When all tasks complete.
  - #Then each result is successful, outputs are distinct (no overwrites), and the underlying mock transport verifies independent request lifecycles.
- **Sync vs Async Parity**
  - #Given identical fixture inputs.
  - #When running `harmonize` followed by `harmonize_async`.
  - #Then both return matching `HarmonizationResult` structures (status/description/mapping_id) and write equivalent files.

### Local IO Guarantees
- **Atomic Tempfile Replace**
  - #Given a mocked response streaming in two chunks.
  - #When `stream_download_to_file` executes.
  - #Then a temporary `.partial` file is created and replaced atomically, leaving no residue.
- **Partial Write Cleanup On Error**
  - #Given an async iterator raising mid-stream.
  - #When `stream_download_to_file` is invoked inside a try/except.
  - #Then the temporary file is cleaned up and no incomplete output remains.

### HTTP Layer Contract
- **Request Construction**
  - #Given known `api_url` and key.
  - #When `stream_harmonize` runs under `MockTransport`.
  - #Then the request targets `/v1/harmonization/run`, includes the Bearer token header, and posts multipart parts for `file` and `manifest` (verified via captured request).
- **File Handle Closure**
  - #Given the context manager exits.
  - #When the transport finishes.
  - #Then CSV and manifest file descriptors are closed (check via `gc.get_objects` or `os.fstat` against open handles).

### Regression Placeholders
- **Retry Backoff (Future)**
  - #Given transient 5xx responses followed by success.
  - #When retry logic is introduced.
  - #Then verify retry count, exponential backoff timing, and logging of each attempt.

## Reporting & CI Hooks
- Collect coverage reports (optional) via `uv run pytest --cov=netrias_client src/netrias_client/tests` to track scenario completeness.
- Integrate the commands into CI so pull requests run: tests → `ruff check` → `basedpyright`.
- Document failure triage steps: reproduce with `pytest -k <scenario>`, inspect logs captured via `caplog`, review generated artifacts under `.pytest_tmpdir`.
