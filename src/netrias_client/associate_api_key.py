"""Associate an API Gateway key with one or more usage plans.

Axis of change: CLI interface and boto3 wiring for key–plan association.
Excluded from PyPI builds — dev/admin tool only.
"""
from __future__ import annotations

import argparse
import sys
import textwrap
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any, Final

import boto3

_DEFAULT_REGION: Final[str] = "us-east-2"


@dataclass(frozen=True)
class AssociateKeyOptions:
    """Capture associate_api_key CLI arguments as structured options.

    'why': keep parsing separate from orchestration logic
    """

    key_id: str
    plan_ids: list[str]
    dry_run: bool
    region: str


def associate_api_key(argv: Sequence[str] | None = None) -> None:
    """Associate an API Gateway key with one or more usage plans.

    'why': generic admin tool — plan IDs passed as arguments, not hardcoded
    """

    options = _parse_associate_key_args(argv)
    client = boto3.client("apigateway", region_name=options.region)  # type: ignore[attr-defined]

    key_name = _validate_key_exists(client, options.key_id)
    if key_name is None:
        print(f"Error: API key '{options.key_id}' not found in {options.region}", file=sys.stderr)
        sys.exit(1)

    print(f"Key: {options.key_id} ({key_name})")
    existing = _get_existing_plan_ids(client, options.key_id)
    _associate_plans(client, options.key_id, options.plan_ids, existing, options.dry_run)
    print("\nDone.")


# --- Private helpers ---


def _associate_plans(
    client: Any,
    key_id: str,
    plan_ids: list[str],
    existing: set[str],
    dry_run: bool,
) -> None:
    for plan_id in plan_ids:
        if plan_id in existing:
            print(f"  [skip] {plan_id} — already associated")
            continue
        if dry_run:
            print(f"  [dry-run] would associate with {plan_id}")
            continue
        client.create_usage_plan_key(usagePlanId=plan_id, keyId=key_id, keyType="API_KEY")
        print(f"  [done] associated with {plan_id}")


def _parse_associate_key_args(argv: Sequence[str] | None) -> AssociateKeyOptions:
    """Parse CLI arguments into an `AssociateKeyOptions` instance.

    'why': isolate argparse wiring for straightforward testing
    """

    parser = argparse.ArgumentParser(
        prog="uv run associate_api_key",
        description="Associate an API Gateway key with one or more usage plans.",
        epilog=textwrap.dedent("""\
            examples:
                uv run associate_api_key tg9wjkvhcb abc123 def456 ghi789
                uv run associate_api_key tg9wjkvhcb abc123 def456 --dry-run
        """),
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    _ = parser.add_argument("key_id", help="API Gateway API key ID (e.g. tg9wjkvhcb)")
    _ = parser.add_argument("plan_ids", nargs="+", help="Usage plan IDs to associate with the key")
    _ = parser.add_argument("--dry-run", action="store_true", help="Print what would be done without making changes")
    _ = parser.add_argument("--region", default=_DEFAULT_REGION, help=f"AWS region (default: {_DEFAULT_REGION})")
    namespace = parser.parse_args(list(argv) if argv is not None else sys.argv[1:])
    return AssociateKeyOptions(
        key_id=namespace.key_id,
        plan_ids=namespace.plan_ids,
        dry_run=namespace.dry_run,
        region=namespace.region,
    )


def _validate_key_exists(client: Any, key_id: str) -> str | None:
    """Return the key name, or None if the key doesn't exist."""

    try:
        info = client.get_api_key(apiKey=key_id)
    except client.exceptions.NotFoundException:
        return None
    return info["name"]


def _get_existing_plan_ids(client: Any, key_id: str) -> set[str]:
    """Return usage plan IDs the key is already associated with."""

    response = client.get_usage_plans(keyId=key_id)
    return {plan["id"] for plan in response.get("items", [])}
