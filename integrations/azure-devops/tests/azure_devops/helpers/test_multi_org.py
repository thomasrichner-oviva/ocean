import json
from typing import Any, AsyncGenerator, Generator
from unittest.mock import MagicMock

import pytest

from port_ocean.context.event import _event_context_stack, EventContext
from port_ocean.context.ocean import ocean

from azure_devops.client.azure_devops_client import AzureDevopsClient
from azure_devops.client.client_manager import AzureDevopsClientManager
from azure_devops.helpers.multi_org import (
    CONCURRENT_ORG_RESYNCS,
    iterate_per_organization,
)


@pytest.fixture
def event_context() -> Generator[None, None, None]:
    ctx = EventContext(event_type="TEST", attributes={})
    _event_context_stack.push(ctx)
    yield
    _event_context_stack.pop()


@pytest.fixture
def set_legacy_single_org() -> Generator[None, None, None]:
    previous: dict[str, Any] = {
        "organization_url": ocean.integration_config.get("organization_url"),
        "personal_access_token": ocean.integration_config.get("personal_access_token"),
        "organization_token_mapping": ocean.integration_config.get(
            "organization_token_mapping"
        ),
    }
    ocean.integration_config["organization_url"] = "https://dev.azure.com/single-org"
    ocean.integration_config["personal_access_token"] = "single-pat"
    ocean.integration_config["organization_token_mapping"] = None
    yield
    for key, value in previous.items():
        ocean.integration_config[key] = value


@pytest.fixture
def set_multi_org_mapping() -> Generator[dict[str, str], None, None]:
    mapping = {
        "https://dev.azure.com/org-one": "pat-one",
        "https://dev.azure.com/org-two": "pat-two",
    }
    previous: dict[str, Any] = {
        "organization_url": ocean.integration_config.get("organization_url"),
        "personal_access_token": ocean.integration_config.get("personal_access_token"),
        "organization_token_mapping": ocean.integration_config.get(
            "organization_token_mapping"
        ),
    }
    ocean.integration_config["organization_url"] = None
    ocean.integration_config["personal_access_token"] = None
    ocean.integration_config["organization_token_mapping"] = json.dumps(mapping)
    yield mapping
    for key, value in previous.items():
        ocean.integration_config[key] = value


def _fake_batches(
    batches: list[list[dict[str, Any]]],
) -> AsyncGenerator[list[dict[str, Any]], None]:
    async def gen() -> AsyncGenerator[list[dict[str, Any]], None]:
        for batch in batches:
            yield batch

    return gen()


@pytest.mark.asyncio
async def test_single_org_yields_enriched_batches(
    set_legacy_single_org: None, event_context: None
) -> None:
    async def handler(
        client: AzureDevopsClient,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        yield [{"id": "p1"}, {"id": "p2"}]
        yield [{"id": "p3"}]

    results: list[list[dict[str, Any]]] = []
    async for batch in iterate_per_organization(handler):
        results.append(batch)

    # Two batches in the same order the handler yielded them.
    assert len(results) == 2
    assert [item["id"] for item in results[0]] == ["p1", "p2"]
    assert [item["id"] for item in results[1]] == ["p3"]

    # Every entity is enriched with the single org's metadata.
    for batch in results:
        for entity in batch:
            assert entity["__organizationUrl"] == "https://dev.azure.com/single-org"
            assert entity["__organizationName"] == "single-org"


@pytest.mark.asyncio
async def test_single_org_propagates_handler_exception(
    set_legacy_single_org: None, event_context: None
) -> None:
    class BoomError(RuntimeError):
        pass

    async def handler(
        client: AzureDevopsClient,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        yield [{"id": "p1"}]
        raise BoomError("simulated resync failure")

    with pytest.raises(BoomError, match="simulated resync failure"):
        async for _ in iterate_per_organization(handler):
            pass


@pytest.mark.asyncio
async def test_multi_org_yields_batches_from_every_org(
    set_multi_org_mapping: dict[str, str], event_context: None
) -> None:
    calls: list[str] = []

    async def handler(
        client: AzureDevopsClient,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        # Each client carries its org URL; use it to emit a distinct
        # entity per org so we can check enrichment below.
        calls.append(client._organization_base_url)
        yield [{"id": f"{client._organization_base_url}::entity"}]

    results: list[dict[str, Any]] = []
    async for batch in iterate_per_organization(handler):
        results.extend(batch)

    # Handler ran once per configured org.
    assert sorted(calls) == sorted(set_multi_org_mapping.keys())

    # Each returned entity is enriched with the org it came from.
    per_org = {entity["__organizationUrl"]: entity for entity in results}
    assert set(per_org.keys()) == set(set_multi_org_mapping.keys())
    assert per_org["https://dev.azure.com/org-one"]["__organizationName"] == "org-one"
    assert per_org["https://dev.azure.com/org-two"]["__organizationName"] == "org-two"


@pytest.mark.asyncio
async def test_multi_org_error_isolation_one_failing_org(
    set_multi_org_mapping: dict[str, str], event_context: None
) -> None:
    async def handler(
        client: AzureDevopsClient,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        if client._organization_base_url == "https://dev.azure.com/org-one":
            # First org fails hard after yielding nothing.
            raise RuntimeError("org-one is broken")
        yield [{"id": "ok-from-org-two"}]

    results: list[dict[str, Any]] = []
    async for batch in iterate_per_organization(handler):
        results.extend(batch)

    # org-two's data is present and correctly enriched; org-one's
    # failure was logged and swallowed without aborting the resync.
    assert len(results) == 1
    assert results[0]["id"] == "ok-from-org-two"
    assert results[0]["__organizationUrl"] == "https://dev.azure.com/org-two"


@pytest.mark.asyncio
async def test_multi_org_respects_concurrency_bound(
    event_context: None,
) -> None:
    """With CONCURRENT_ORG_RESYNCS=5 and 7 configured orgs, at most 5
    handler coroutines should be in flight at any one moment.
    """
    mapping = {f"https://dev.azure.com/org-{i}": f"pat-{i}" for i in range(7)}
    previous_mapping = ocean.integration_config.get("organization_token_mapping")
    previous_url = ocean.integration_config.get("organization_url")
    previous_pat = ocean.integration_config.get("personal_access_token")
    ocean.integration_config["organization_url"] = None
    ocean.integration_config["personal_access_token"] = None
    ocean.integration_config["organization_token_mapping"] = json.dumps(mapping)

    in_flight = 0
    max_in_flight = 0

    try:

        async def handler(
            client: AzureDevopsClient,
        ) -> AsyncGenerator[list[dict[str, Any]], None]:
            nonlocal in_flight, max_in_flight
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            # Yield once then release the semaphore.
            yield [{"id": client._organization_base_url}]
            in_flight -= 1

        batches: list[list[dict[str, Any]]] = []
        async for batch in iterate_per_organization(handler):
            batches.append(batch)

        assert len(batches) == 7, "Every org should have yielded a batch."
        assert max_in_flight <= CONCURRENT_ORG_RESYNCS
    finally:
        ocean.integration_config["organization_token_mapping"] = previous_mapping
        ocean.integration_config["organization_url"] = previous_url
        ocean.integration_config["personal_access_token"] = previous_pat


@pytest.mark.asyncio
async def test_empty_manager_yields_nothing(
    monkeypatch: pytest.MonkeyPatch, event_context: None
) -> None:
    empty_manager = MagicMock(spec=AzureDevopsClientManager)
    empty_manager.get_clients.return_value = []
    monkeypatch.setattr(
        "azure_devops.helpers.multi_org.AzureDevopsClientManager.create_from_ocean_config",
        lambda: empty_manager,
    )

    async def handler(
        client: AzureDevopsClient,
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        yield [{"id": "never"}]

    results: list[list[dict[str, Any]]] = []
    async for batch in iterate_per_organization(handler):
        results.append(batch)

    assert results == []
