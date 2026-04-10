"""Associate an API Gateway key with one or more usage plans.

Axis of change: CLI interface and boto3 wiring for key–plan association.
Excluded from PyPI builds — dev/admin tool only.

## Key Minting Guide

"Key minting" is the process of creating an API Gateway key in AWS and then
associating it with the correct usage plans so the key holder can call our APIs.

### Prerequisites

1. **AWS CLI credentials** configured for the Netrias account in us-east-2.
   Run `aws sts get-caller-identity` to verify. You need permissions for
   `apigateway:GET` and `apigateway:POST` actions.

2. **uv** installed (https://docs.astral.sh/uv/).

3. This repository cloned and dependencies synced (`uv sync`).

### Step 1: Create the API key in AWS

Go to the AWS Console → API Gateway → API Keys → Create API Key, or use the CLI:

    aws apigateway create-api-key \
        --name "DESCRIPTIVE_NAME" \
        --description "Who/what this key is for" \
        --enabled \
        --region us-east-2

The response includes an `"id"` field (e.g. `6nmgeolldd`). This is the key_id
you pass to this script. Save it — you'll need it below.

### Step 2: Identify the usage plans for your target environment

Each API (discovery, harmonization, etc.) has a usage plan per environment.
List them with:

    aws apigateway get-usage-plans --region us-east-2

Look for plans tagged with the environment you want. As of 2026-02-20:

**Staging plans:**

    Plan ID   Name                                  API service
    --------  ------------------------------------  -------------------------
    4pcdp7    harmonization-pipeline-staging-plan    Harmonization (staging)
    8fr3fm    cde-recommend-staging                  Discovery / CDE recommend (staging)
    h8daoc    cde-recommendation-staging-plan        Async discovery via Step Functions (staging)
    772ahq    Usage plan                             Data Model Store (shared, both envs)

**Production plans:**

    Plan ID   Name                                  API service
    --------  ------------------------------------  -------------------------
    0mmoxo    harmonization-pipeline-prod-plan       Harmonization (prod)
    2bywoq    cde-recommend-prod                     Discovery / CDE recommend (prod)
    772ahq    Usage plan                             Data Model Store (shared, both envs)

The Data Model Store plan (772ahq) is shared across staging and production.

### Step 3: Dry-run the association

Always dry-run first to verify what will happen:

    uv run associate_api_key <key_id> <plan_id_1> <plan_id_2> ... --dry-run

Example for staging:

    uv run associate_api_key 6nmgeolldd 4pcdp7 8fr3fm h8daoc 772ahq --dry-run

The output shows `[dry-run]` for each plan that would be associated, and
`[skip]` for any plans the key already belongs to.

### Step 4: Run for real

Drop the --dry-run flag:

    uv run associate_api_key 6nmgeolldd 4pcdp7 8fr3fm h8daoc 772ahq

Each plan prints `[done]` on success.

### Step 5: Distribute the key value

The key *value* (the actual secret token callers use in the x-api-key header)
is different from the key *id* you've been using above. Retrieve it with:

    aws apigateway get-api-key --api-key <key_id> --include-value --region us-east-2

The `"value"` field is the bearer token. Give this to the key holder. They use
it as:

    from netrias_client import NetriasClient
    from netrias_client._config import Environment

    client = NetriasClient(api_key="<the value>", environment=Environment.STAGING)

### Troubleshooting

- **"API key not found"**: Double-check the key_id and region. Keys are
  regional — ours are all in us-east-2.
- **"Access denied"**: Your AWS credentials lack apigateway permissions.
  Ask for IAM access to the apigateway:* actions.
- **Key exists but API calls return 403**: The key is not associated with the
  right usage plan(s). Re-run this script with the missing plan IDs.
- **"already associated"**: Harmless — the script skips plans the key already
  belongs to.
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
