"""Test Data Model Store workflows.

'why': verify typed access to data models, CDEs, and PVs
"""
from __future__ import annotations

import pytest

from netrias_client import CDE, DataModel, DataModelStoreError, DataModelVersion, NetriasClient, PermissibleValue
from netrias_client._errors import NetriasAPIUnavailable

from ._utils import install_mock_transport, json_failure, json_success, paginated_pv_responses


def test_list_data_models_success(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return data models from the API response.

    'why': verify basic happy path for listing data models
    """

    payload = {
        "total": 2,
        "limit": None,
        "offset": 0,
        "items": [
            {"data_commons_id": 1, "key": "ccdi", "name": "CCDI Data Model", "description": "Test", "is_active": True},
            {"data_commons_id": 2, "key": "gc", "name": "GC Model", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    models = configured_client.list_data_models()

    assert len(models) == 2
    assert models[0] == DataModel(
        data_commons_id=1, key="ccdi", name="CCDI Data Model", description="Test", is_active=True, versions=None
    )
    assert models[1] == DataModel(
        data_commons_id=2, key="gc", name="GC Model", description=None, is_active=True, versions=None
    )


def test_list_data_models_with_versions(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Parse version data when include_versions is true.

    'why': callers need version labels for CDE/PV queries
    """

    payload = {
        "total": 1,
        "items": [
            {
                "data_commons_id": 1,
                "key": "ccdi",
                "name": "CCDI Data Model",
                "description": "Test",
                "is_active": True,
                "versions": [
                    {"version_label": "v1"},
                    {"version_label": "v2"},
                ],
            },
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    models = configured_client.list_data_models(include_versions=True)

    assert len(models) == 1
    assert models[0].key == "ccdi"
    assert models[0].versions is not None
    assert len(models[0].versions) == 2
    assert models[0].versions[0] == DataModelVersion(version_label="v1")
    assert models[0].versions[1] == DataModelVersion(version_label="v2")


def test_list_data_models_with_version_number(
    configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Parse version_number (int) from the actual DMS API response format.

    'why': the DMS API returns version_number (int), not version_label (str)
    """

    payload = {
        "total": 1,
        "items": [
            {
                "data_commons_id": 1,
                "key": "ccdi",
                "name": "CCDI Data Model",
                "description": "Test",
                "is_active": True,
                "versions": [
                    {"data_model_version_id": 1, "version_number": 1},
                    {"data_model_version_id": 2, "version_number": 2},
                ],
            },
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    models = configured_client.list_data_models(include_versions=True)

    assert len(models[0].versions) == 2
    assert models[0].versions[0] == DataModelVersion(version_label="1")
    assert models[0].versions[1] == DataModelVersion(version_label="2")


def test_list_data_models_empty(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return empty tuple when no models found.

    'why': verify graceful handling of empty results
    """

    payload = {"total": 0, "limit": None, "offset": 0, "items": []}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    models = configured_client.list_data_models()

    assert models == ()


def test_list_cdes_success(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return CDEs from the API response.

    'why': verify basic happy path for listing CDEs
    """

    payload = {
        "data_commons_key": "ccdi",
        "version_label": "v1",
        "total": 2,
        "limit": None,
        "offset": 0,
        "items": [
            {"cde_key": "age_at_diagnosis", "cde_id": 123, "cde_version_id": 456},
            {"cde_key": "sex_at_birth", "cde_id": 124, "cde_version_id": 457, "column_description": "Biological sex"},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    cdes = configured_client.list_cdes("ccdi", "v1")

    assert len(cdes) == 2
    assert cdes[0] == CDE(cde_key="age_at_diagnosis", cde_id=123, cde_version_id=456, description=None)
    assert cdes[1] == CDE(cde_key="sex_at_birth", cde_id=124, cde_version_id=457, description="Biological sex")


def test_list_pvs_success(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return permissible values from the API response.

    'why': verify basic happy path for listing PVs
    """

    payload = {
        "data_commons_key": "ccdi",
        "version_label": "v1",
        "cde_key": "sex_at_birth",
        "total": 3,
        "limit": None,
        "offset": 0,
        "items": [
            {"pv_id": 1, "value": "Male", "description": None, "is_active": True},
            {"pv_id": 2, "value": "Female", "description": "Biological female", "is_active": True},
            {"pv_id": 3, "value": "Unknown", "description": None, "is_active": False},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    pvs = configured_client.list_pvs("ccdi", "v1", "sex_at_birth")

    assert len(pvs) == 3
    assert pvs[0] == PermissibleValue(pv_id=1, value="Male", description=None, is_active=True)
    assert pvs[1] == PermissibleValue(pv_id=2, value="Female", description="Biological female", is_active=True)
    assert pvs[2] == PermissibleValue(pv_id=3, value="Unknown", description=None, is_active=False)


def test_get_pv_set_returns_frozenset(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return a frozenset of PV values for efficient membership testing.

    'why': verify the validation use case returns O(1) lookup structure
    """

    payload = {
        "total": 3,
        "items": [
            {"pv_id": 1, "value": "Male", "description": None, "is_active": True},
            {"pv_id": 2, "value": "Female", "description": None, "is_active": True},
            {"pv_id": 3, "value": "Unknown", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    pv_set = configured_client.get_pv_set("ccdi", "v1", "sex_at_birth")

    assert isinstance(pv_set, frozenset)
    assert pv_set == frozenset({"Male", "Female", "Unknown"})
    assert "Male" in pv_set
    assert "InvalidValue" not in pv_set


def test_validate_value_returns_true_for_valid(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return True when value is in permissible values.

    'why': verify validation convenience method works for valid input
    """

    payload = {
        "total": 2,
        "items": [
            {"pv_id": 1, "value": "Male", "description": None, "is_active": True},
            {"pv_id": 2, "value": "Female", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    result = configured_client.validate_value("Male", "ccdi", "v1", "sex_at_birth")

    assert result is True


def test_validate_value_returns_false_for_invalid(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Return False when value is not in permissible values.

    'why': verify validation convenience method rejects invalid input
    """

    payload = {
        "total": 2,
        "items": [
            {"pv_id": 1, "value": "Male", "description": None, "is_active": True},
            {"pv_id": 2, "value": "Female", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    result = configured_client.validate_value("InvalidValue", "ccdi", "v1", "sex_at_birth")

    assert result is False


def test_list_data_models_raises_on_client_error(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise DataModelStoreError on 4xx responses.

    'why': verify client errors are surfaced with appropriate exception type
    """

    capture = json_failure({"message": "Invalid model key"}, 404)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(DataModelStoreError) as exc_info:
        configured_client.list_data_models()

    assert "Invalid model key" in str(exc_info.value)


def test_list_data_models_raises_on_server_error(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise NetriasAPIUnavailable on 5xx responses.

    'why': verify server errors are surfaced with appropriate exception type
    """

    capture = json_failure({"message": "Internal server error"}, 500)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable) as exc_info:
        configured_client.list_data_models()

    assert "server error" in str(exc_info.value)


def test_list_cdes_raises_on_not_found(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise DataModelStoreError when model/version not found.

    'why': verify 404 responses are handled correctly
    """

    capture = json_failure({"message": "Version not found"}, 404)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(DataModelStoreError) as exc_info:
        configured_client.list_cdes("ccdi", "v999")

    assert "Version not found" in str(exc_info.value)


def test_list_pvs_raises_on_not_found(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise DataModelStoreError when CDE not found.

    'why': verify 404 responses are handled correctly for PV endpoint
    """

    capture = json_failure({"message": "CDE not found"}, 404)
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(DataModelStoreError) as exc_info:
        configured_client.list_pvs("ccdi", "v1", "nonexistent_cde")

    assert "CDE not found" in str(exc_info.value)


@pytest.mark.asyncio
async def test_list_data_models_async(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify async variant returns same results.

    'why': ensure async API works correctly
    """

    payload = {
        "total": 1,
        "items": [{"data_commons_id": 1, "key": "ccdi", "name": "CCDI", "description": None, "is_active": True}],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    models = await configured_client.list_data_models_async()

    assert len(models) == 1
    assert models[0].key == "ccdi"


@pytest.mark.asyncio
async def test_list_cdes_async(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify async variant returns same results for CDEs.

    'why': ensure async API works correctly
    """

    payload = {
        "total": 1,
        "items": [{"cde_key": "age", "cde_id": 1, "cde_version_id": 1}],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    cdes = await configured_client.list_cdes_async("ccdi", "v1")

    assert len(cdes) == 1
    assert cdes[0].cde_key == "age"


@pytest.mark.asyncio
async def test_list_pvs_async(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify async variant returns same results for PVs.

    'why': ensure async API works correctly
    """

    payload = {
        "total": 1,
        "items": [{"pv_id": 1, "value": "Test", "description": None, "is_active": True}],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    pvs = await configured_client.list_pvs_async("ccdi", "v1", "test_cde")

    assert len(pvs) == 1
    assert pvs[0].value == "Test"


@pytest.mark.asyncio
async def test_get_pv_set_async(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify async variant returns frozenset.

    'why': ensure async validation API works correctly
    """

    payload = {
        "total": 2,
        "items": [
            {"pv_id": 1, "value": "A", "description": None, "is_active": True},
            {"pv_id": 2, "value": "B", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    pv_set = await configured_client.get_pv_set_async("ccdi", "v1", "test_cde")

    assert pv_set == frozenset({"A", "B"})


def test_list_data_models_raises_on_timeout(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Raise NetriasAPIUnavailable on timeout.

    'why': verify timeout errors are surfaced with appropriate exception type
    """

    import httpx
    from ._utils import transport_error

    capture = transport_error(httpx.TimeoutException("connection timed out"))
    install_mock_transport(monkeypatch, capture)

    with pytest.raises(NetriasAPIUnavailable) as exc_info:
        configured_client.list_data_models()

    assert "timed out" in str(exc_info.value)


@pytest.mark.asyncio
async def test_validate_value_async(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify async validate_value works correctly.

    'why': ensure async validation convenience method works
    """

    payload = {
        "total": 2,
        "items": [
            {"pv_id": 1, "value": "Male", "description": None, "is_active": True},
            {"pv_id": 2, "value": "Female", "description": None, "is_active": True},
        ],
    }
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    result = await configured_client.validate_value_async("Male", "ccdi", "v1", "sex_at_birth")

    assert result is True


def test_get_pv_set_paginates_multiple_pages(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Auto-paginate when more results exist than the page size.

    'why': get_pv_set must fetch all pages to return complete validation set
    """

    page_size = 1000
    page1 = [{"pv_id": i, "value": f"Value{i}", "description": None, "is_active": True} for i in range(page_size)]
    page2 = [{"pv_id": i + page_size, "value": f"Value{i + page_size}", "description": None, "is_active": True} for i in range(500)]

    capture = paginated_pv_responses([page1, page2])
    install_mock_transport(monkeypatch, capture)

    pv_set = configured_client.get_pv_set("ccdi", "v1", "large_cde")

    assert len(pv_set) == 1500
    assert "Value0" in pv_set
    assert "Value999" in pv_set
    assert "Value1000" in pv_set
    assert "Value1499" in pv_set
    assert len(capture.requests) == 2


def test_list_data_models_sends_query_params(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Query parameters are included in the data models request.

    'why': callers need to filter and control results via query params
    """

    payload = {"total": 0, "items": []}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.list_data_models(
        query="ccdi",
        include_versions=True,
        include_counts=True,
        limit=10,
        offset=5,
    )

    request = capture.requests[0]
    url = str(request.url)
    assert "q=ccdi" in url
    assert "include_versions=true" in url
    assert "include_counts=true" in url
    assert "limit=10" in url
    assert "offset=5" in url


def test_list_cdes_sends_query_params(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Query parameters are included in the CDEs request.

    'why': callers need to filter CDEs and include descriptions
    """

    payload = {"total": 0, "items": []}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.list_cdes(
        model_key="ccdi",
        version="v1",
        include_description=True,
        query="age",
        limit=25,
        offset=10,
    )

    request = capture.requests[0]
    url = str(request.url)
    assert "include_description=true" in url
    assert "q=age" in url
    assert "limit=25" in url
    assert "offset=10" in url


def test_list_pvs_sends_query_params(configured_client: NetriasClient, monkeypatch: pytest.MonkeyPatch) -> None:
    """Query parameters are included in the PVs request.

    'why': callers need to filter PVs and include inactive values
    """

    payload = {"total": 0, "items": []}
    capture = json_success(payload)
    install_mock_transport(monkeypatch, capture)

    _ = configured_client.list_pvs(
        model_key="ccdi",
        version="v1",
        cde_key="sex_at_birth",
        include_inactive=True,
        query="Male",
        limit=50,
        offset=0,
    )

    request = capture.requests[0]
    url = str(request.url)
    assert "include_inactive=true" in url
    assert "q=Male" in url
    assert "limit=50" in url
