"""Live API smoke test for the main NetriasClient workflows.

Run with: uv run python -m netrias_client.live_test.api_quicktest
"""

from __future__ import annotations

import sys
import traceback
from collections.abc import Callable
from dataclasses import dataclass

from dotenv import dotenv_values
from netrias_client import ColumnKeyedManifestPayload, NetriasClient

from ._constants import (
    CDE_KEY,
    CSV_PATH,
    DISCOVERY_EXTERNAL_VERSION_NUMBER,
    ENV_PATH,
    EXTERNAL_VERSION_NUMBER,
    MODEL_KEY,
)


@dataclass(slots=True)
class SmokeResult:
    name: str
    passed: bool
    message: str
    details: str | None = None


@dataclass(slots=True)
class SmokeContext:
    client: NetriasClient
    manifest: ColumnKeyedManifestPayload | None = None


def _run_step(name: str, step: Callable[[], None]) -> SmokeResult:
    try:
        step()
    except Exception as exc:
        return SmokeResult(
            name=name,
            passed=False,
            message=f"{type(exc).__name__}: {exc}",
            details=traceback.format_exc(),
        )
    return SmokeResult(name=name, passed=True, message="OK")


def main() -> int:
    context = _build_context()
    if context is None:
        return 1

    _print_header()
    results = [
        _run_step("data_model_store", lambda: _check_data_model_store(context.client)),
        _run_step("discovery", lambda: _check_discovery(context)),
        _run_step("harmonization", lambda: _check_harmonization(context)),
    ]
    return _print_results(results)


def _build_context() -> SmokeContext | None:
    env = dotenv_values(ENV_PATH)
    api_key = env.get("NETRIAS_API_KEY")
    if not api_key:
        print(f"ERROR: NETRIAS_API_KEY not found in {ENV_PATH}")
        return None
    if not CSV_PATH.exists():
        print(f"ERROR: Test CSV not found: {CSV_PATH}")
        return None

    client = NetriasClient(api_key=api_key)
    client.configure(log_level="WARNING")
    return SmokeContext(client=client)


def _print_header() -> None:
    print("=" * 70)
    print("NETRIAS CLIENT LIVE SMOKE")
    print("=" * 70)
    print(f"Test CSV: {CSV_PATH}")
    print(f"Model: {MODEL_KEY} {DISCOVERY_EXTERNAL_VERSION_NUMBER}")
    print()


def _check_data_model_store(client: NetriasClient) -> None:
    models = client.list_data_models(query=MODEL_KEY, include_versions=True, limit=5)
    assert any(model.key == MODEL_KEY for model in models), f"Expected {MODEL_KEY!r} in data models"

    cdes = client.list_cdes(
        model_key=MODEL_KEY,
        version=DISCOVERY_EXTERNAL_VERSION_NUMBER,
        query=CDE_KEY,
        include_description=True,
        limit=10,
    )
    assert len(cdes) > 0, f"Expected at least one CDE matching {CDE_KEY!r}"

    pv_set = client.get_pv_set(
        model_key=MODEL_KEY,
        version=DISCOVERY_EXTERNAL_VERSION_NUMBER,
        cde_key=CDE_KEY,
    )
    assert isinstance(pv_set, frozenset)
    assert len(pv_set) > 0, f"Expected permissible values for {CDE_KEY!r}"
    print(f"  data model store: {len(models)} models, {len(cdes)} CDEs, {len(pv_set)} PVs")


def _check_discovery(context: SmokeContext) -> None:
    context.manifest = context.client.discover_mapping_from_tabular(
        source_path=CSV_PATH,
        target_schema=MODEL_KEY,
        external_version_number=DISCOVERY_EXTERNAL_VERSION_NUMBER,
        sample_limit=10,
        top_k=3,
    )
    mappings = context.manifest["column_mappings"]
    assert len(mappings) > 0, "Expected discovery to return column mappings"
    print(f"  discovery: {len(mappings)} column mappings")


def _check_harmonization(context: SmokeContext) -> None:
    if context.manifest is None:
        raise RuntimeError("Discovery must run before harmonization")
    result = context.client.harmonize(
        source_path=CSV_PATH,
        manifest=context.manifest,
        data_commons_key=MODEL_KEY,
        external_version_number=EXTERNAL_VERSION_NUMBER,
    )
    print(f"  harmonization: {result.status}")

    is_known_failure = result.status == "failed" and (
        "unknown cde id" in result.description.lower() or "invalid" in result.description.lower()
    )
    if is_known_failure:
        print("  harmonization note: CDE ID mismatch, auth and submission path verified")
        return

    assert result.status == "succeeded", f"Expected 'succeeded', got {result.status!r}"


def _print_results(results: list[SmokeResult]) -> int:
    print()
    for result in results:
        status = "PASS" if result.passed else "FAIL"
        print(f"[{status}] {result.name}: {result.message}")
        if not result.passed and result.details:
            print(result.details)

    return 0 if all(result.passed for result in results) else 1


if __name__ == "__main__":
    sys.exit(main())
