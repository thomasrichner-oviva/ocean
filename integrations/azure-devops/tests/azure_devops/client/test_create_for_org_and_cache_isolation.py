import json
from typing import Any, Generator

import pytest

from port_ocean.context.event import _event_context_stack, EventContext
from port_ocean.context.ocean import ocean
from port_ocean.utils.cache import hash_func

from azure_devops.client.azure_devops_client import AzureDevopsClient


@pytest.fixture
def event_context() -> Generator[None, None, None]:
    ctx = EventContext(event_type="TEST", attributes={})
    _event_context_stack.push(ctx)
    yield
    _event_context_stack.pop()


@pytest.fixture
def set_multi_org_mapping() -> Generator[dict[str, str], None, None]:
    mapping = {
        "https://dev.azure.com/org-alpha": "pat-alpha",
        "https://dev.azure.com/org-beta": "pat-beta",
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


def test_create_for_org_returns_matching_client(
    set_multi_org_mapping: dict[str, str], event_context: None
) -> None:
    client = AzureDevopsClient.create_for_org("https://dev.azure.com/org-alpha")
    assert isinstance(client, AzureDevopsClient)
    assert client._organization_base_url == "https://dev.azure.com/org-alpha"


def test_create_for_org_normalizes_trailing_slash(
    set_multi_org_mapping: dict[str, str], event_context: None
) -> None:
    client = AzureDevopsClient.create_for_org("https://dev.azure.com/org-alpha/")
    assert client._organization_base_url == "https://dev.azure.com/org-alpha"


def test_create_for_org_unknown_url_raises(
    set_multi_org_mapping: dict[str, str], event_context: None
) -> None:
    with pytest.raises(ValueError, match="No client configured for organization"):
        AzureDevopsClient.create_for_org("https://dev.azure.com/unknown-org")


CACHED_BACKING_METHODS = [
    "_generate_projects_cached",
    "_generate_teams_cached",
    "_generate_groups_cached",
    "_generate_repositories_cached",
    "_generate_environments_cached",
    "_get_boards_in_organization_cached",
]


@pytest.mark.parametrize("method_name", CACHED_BACKING_METHODS)
def test_cached_backing_method_keys_differ_across_orgs(method_name: str) -> None:
    client_alpha = AzureDevopsClient(
        "https://dev.azure.com/org-alpha", "pat-alpha", "port"
    )
    client_beta = AzureDevopsClient(
        "https://dev.azure.com/org-beta", "pat-beta", "port"
    )
    func = getattr(client_alpha, method_name).__wrapped__

    key_alpha = hash_func(func, client_alpha, client_alpha._organization_base_url)
    key_beta = hash_func(func, client_beta, client_beta._organization_base_url)

    assert key_alpha != key_beta, (
        f"Cache keys for {method_name} must differ across org clients "
        f"(got {key_alpha} for both)."
    )


def test_cached_backing_method_key_stable_for_same_org() -> None:
    """Same org on two client instances should produce identical cache keys
    (so single-org deployments still benefit from the cache, and the
    instance identity genuinely doesn't matter — only the org URL does).
    """
    client_one = AzureDevopsClient("https://dev.azure.com/only-org", "pat-one", "port")
    client_two = AzureDevopsClient(
        "https://dev.azure.com/only-org", "pat-two-different-instance", "port"
    )
    func = client_one._generate_projects_cached.__wrapped__  # type: ignore[attr-defined]

    key_one = hash_func(func, client_one, client_one._organization_base_url)
    key_two = hash_func(func, client_two, client_two._organization_base_url)

    assert key_one == key_two
