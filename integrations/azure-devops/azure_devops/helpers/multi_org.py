import asyncio
import functools
from typing import Any, AsyncGenerator, Callable

from loguru import logger

from port_ocean.utils.async_iterators import (
    semaphore_async_iterator,
    stream_async_iterators_tasks,
)

from azure_devops.client.azure_devops_client import AzureDevopsClient
from azure_devops.client.client_manager import AzureDevopsClientManager
from azure_devops.misc import extract_org_name_from_url


CONCURRENT_ORG_RESYNCS = 5

OrgHandler = Callable[[AzureDevopsClient], AsyncGenerator[list[dict[str, Any]], None]]


def _enrich_batch(
    batch: list[dict[str, Any]], org_url: str, org_name: str
) -> list[dict[str, Any]]:
    for entity in batch:
        entity["__organizationUrl"] = org_url
        entity["__organizationName"] = org_name
    return batch


async def iterate_per_organization(
    handler: OrgHandler,
) -> AsyncGenerator[list[dict[str, Any]], None]:
    """Iterates over every configured Azure DevOps organization.

    - Single-org deployments go through a transparent pass-through
    - Multi-org deployments fan out with a bounded
      :const:`CONCURRENT_ORG_RESYNCS` semaphore and wrap each org's
      iteration in try/except so that a single failing org is logged and skipped without blocking the
      other orgs from finishing their resync.
    """
    manager = AzureDevopsClientManager.create_from_ocean_config()
    clients = manager.get_clients()

    if not clients:
        logger.warning(
            "iterate_per_organization invoked with no configured organizations."
        )
        return

    if len(clients) == 1:
        org_url, client = clients[0]
        org_name = extract_org_name_from_url(org_url)
        async for batch in handler(client):
            yield _enrich_batch(batch, org_url, org_name)
        return

    semaphore = asyncio.BoundedSemaphore(CONCURRENT_ORG_RESYNCS)
    tasks = [
        semaphore_async_iterator(
            semaphore,
            functools.partial(_iterate_one_organization, handler, org_url, client),
        )
        for org_url, client in clients
    ]
    async for batch in stream_async_iterators_tasks(*tasks):
        yield batch


async def _iterate_one_organization(
    handler: OrgHandler,
    org_url: str,
    client: AzureDevopsClient,
) -> AsyncGenerator[list[dict[str, Any]], None]:
    org_name = extract_org_name_from_url(org_url)
    try:
        async for batch in handler(client):
            yield _enrich_batch(batch, org_url, org_name)
    except Exception as exc:
        logger.error(f"Failed to resync Azure DevOps organization {org_url}: {exc}")
