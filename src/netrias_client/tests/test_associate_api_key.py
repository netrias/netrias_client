from unittest.mock import MagicMock, patch

import pytest
from netrias_client.associate_api_key import associate_api_key


@patch("netrias_client.associate_api_key.boto3")
def test_dry_run_lists_plans_without_aws_calls(mock_boto3: MagicMock):
    """
    Given: two plan IDs and --dry-run flag
    When: associate_api_key runs
    Then: both plans are listed and no create_usage_plan_key calls are made
    Negative: no plans are actually created
    """
    client = MagicMock()
    mock_boto3.client.return_value = client
    client.get_api_key.return_value = {"name": "test-key"}
    client.get_usage_plans.return_value = {"items": []}

    associate_api_key(["tg9wjkvhcb", "abc123", "def456", "--dry-run"])

    client.create_usage_plan_key.assert_not_called()


@patch("netrias_client.associate_api_key._validate_key_exists", return_value=None)
@patch("netrias_client.associate_api_key.boto3")
def test_invalid_key_exits_with_error(
    mock_boto3: MagicMock,
    _mock_validate: MagicMock,
):
    """
    Given: an invalid API key ID
    When: associate_api_key runs
    Then: it exits with code 1 and no associations are created
    Negative: no create_usage_plan_key calls
    """
    client = MagicMock()
    mock_boto3.client.return_value = client

    with pytest.raises(SystemExit) as exc_info:
        associate_api_key(["bad-key-id", "abc123"])

    assert exc_info.value.code == 1
    client.create_usage_plan_key.assert_not_called()


@patch("netrias_client.associate_api_key.boto3")
def test_skips_already_associated_plans(mock_boto3: MagicMock):
    """
    Given: the key is already associated with the first plan
    When: associate_api_key runs with two plan IDs (not dry-run)
    Then: only the second plan gets a create_usage_plan_key call
    Negative: the first plan is not re-associated
    """
    client = MagicMock()
    mock_boto3.client.return_value = client
    client.get_api_key.return_value = {"name": "test-key"}
    client.get_usage_plans.return_value = {"items": [{"id": "abc123"}]}

    associate_api_key(["tg9wjkvhcb", "abc123", "def456"])

    calls = client.create_usage_plan_key.call_args_list
    created_plan_ids = [c.kwargs["usagePlanId"] for c in calls]
    assert "abc123" not in created_plan_ids
    assert created_plan_ids == ["def456"]
