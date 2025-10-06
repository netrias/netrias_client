# Netrias Client Refactor Plan – Stateful NetriasClient (Plan B)

## Overview
Introduce a `NetriasClient` class that encapsulates configuration state and exposes the discovery and harmonization APIs (sync/async). Module-level functions remain as shims during a transition period, delegating to a default client instance.

## Detailed Steps
1. **Client Class & Settings Management**
   - Implement `NetriasClient` (new module or in `__init__.py`) containing:
     - An immutable `Settings` snapshot (API key, URL, timeout, log level, confidence threshold, gateway bypass options).
     - Methods: `configure`, `discover_mapping`, `discover_mapping_async`, `discover_mapping_from_csv`, `discover_mapping_from_csv_async`, `harmonize`, `harmonize_async`.
   - `configure` validates inputs (route through existing helpers) and stores the normalized `Settings` on the instance. Expose a read-only accessor (`client.settings`) for debugging/tests.

2. **Refactor Core Modules to Use Instance State**
   - Update `_discovery`, `_core`, `_adapter`, `_gateway_bypass`, `_http`, `_validators` to accept a `Settings` argument supplied by the client rather than calling `get_settings()`.
   - Ensure async workflows capture the `Settings` snapshot at entry so configuration changes do not affect in-flight calls.
   - Maintain logging calls but ensure the configured log level is applied when instantiating the client (consider whether logger should remain global or become per-client).

3. **Module-Level Compatibility Layer**
   - Create a module-scoped default client (`DEFAULT_CLIENT = NetriasClient()`).
   - Keep existing functions (`configure`, `discover_mapping*`, `harmonize*`) but have them delegate to `DEFAULT_CLIENT`.
   - Document the legacy entry points as supported but encourage new code to instantiate and use `NetriasClient` directly.

4. **Documentation & Discoverability**
   - Update README and docstrings to showcase the object-oriented workflow:
     ```python
     from netrias_client import NetriasClient

     client = NetriasClient()
     client.configure(api_key="...", api_url="...")
     discovery = client.discover_mapping_from_csv(Path("data.csv"))
     result = client.harmonize(Path("data.csv"), manifest_path)
     ```
   - Highlight async usage (`await client.harmonize_async(...)`) and mention that multiple clients can operate simultaneously with different credentials.

5. **Testing Adjustments**
   - Replace fixtures that mutate global configuration with fixtures that yield a configured `NetriasClient` instance.
   - Add regression tests ensuring the module-level shims still behave as expected.
   - Add tests for multiple clients to verify isolation (e.g., different API URLs/keys) and ensure gateway bypass respects per-client settings.

6. **Type Hints & Public Surface**
   - Export `NetriasClient` via `__all__`.
   - Add comprehensive type hints/docstrings for each public method to improve IDE discoverability.
   - Consider a `Protocol` or TypedDict for harmonization/discovery results if it improves clarity.

7. **Future Enhancements (Post-Refactor)**
   - Consider optional constructor arguments (e.g., `NetriasClient(api_key=..., api_url=...)`).
   - Evaluate whether to deprecate the module-level functions in a future major release once consumers migrate.

## Pros
- **Isolated State:** Multiple clients can run concurrently with different credentials and configuration without touching global mutable state.
- **Improved Ergonomics:** Users interact with a single, discoverable object exposing all capabilities (sync and async).
- **Better Testability:** Tests can instantiate dedicated clients with precise settings, reducing reliance on monkeypatching globals.
- **Extensibility:** Easy to add per-client features (custom transports, metrics, caching) without impacting other users.

## Cons / Risks
- **Refactor Complexity:** Requires threading `Settings` through all key modules; thorough review and regression testing needed to avoid breakage.
- **Transition Overhead:** Need to maintain both object and function APIs until users migrate; documentation must clearly indicate the preferred path.
- **Logging Behavior:** Decide whether log level remains process-wide or becomes per-client; inconsistent handling could surprise users.
- **Backward Compatibility:** Must ensure existing behavior (error types, logging messages, defaults) remains intact through shims.
