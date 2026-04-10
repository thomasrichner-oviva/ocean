import base64
from typing import Any, Dict
import pytest
from port_ocean.core.handlers.port_app_config.models import ResourceConfig
from port_ocean.core.handlers.webhook.webhook_event import (
    WebhookEvent,
    WebhookEventRawResults,
)
from azure_devops.webhooks.webhook_processors.base_processor import (
    AzureDevOpsBaseWebhookProcessor,
)


class AzureDevOpsWebhookProcessorImpl(AzureDevOpsBaseWebhookProcessor):
    async def get_matching_kinds(self, event: WebhookEvent) -> list[str]:
        return ["test-kind"]

    async def _handle_webhook_event(
        self, payload: dict[str, Any], resource_config: ResourceConfig
    ) -> WebhookEventRawResults:
        return WebhookEventRawResults(updated_raw_results=[], deleted_raw_results=[])

    async def should_process_event(self, event: WebhookEvent) -> bool:
        return True


@pytest.fixture
def base_processor(event: WebhookEvent) -> AzureDevOpsWebhookProcessorImpl:
    return AzureDevOpsWebhookProcessorImpl(event)


@pytest.mark.asyncio
async def test_base_authenticate_failure_wrong_secret(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    encoded = base64.b64encode(b":wrong-secret").decode("utf-8")
    headers = {"authorization": f"Basic {encoded}"}
    assert await base_processor.authenticate({}, headers) is False


@pytest.mark.asyncio
async def test_base_authenticate_failure_wrong_auth_type(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    headers = {"authorization": "Bearer token"}
    assert await base_processor.authenticate({}, headers) is False


@pytest.mark.asyncio
async def test_base_authenticate_failure_malformed(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    headers = {"authorization": "Basic malformed_token"}
    assert await base_processor.authenticate({}, headers) is False


@pytest.mark.asyncio
async def test_base_authenticate_failure_no_auth(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    assert await base_processor.authenticate({}, {}) is True


@pytest.mark.asyncio
async def test_base_validate_payload_success(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    valid_payload: Dict[str, Any] = {
        "eventType": "user.created",
        "publisherId": "tfs",
        "resource": {"id": "123"},
    }
    assert await base_processor.validate_payload(valid_payload) is True


@pytest.mark.asyncio
async def test_base_validate_payload_failure(
    base_processor: AzureDevOpsWebhookProcessorImpl,
) -> None:
    invalid_payloads: list[Dict[str, Any]] = [
        {"eventType": "user.created"},
        {"publisherId": "tfs", "resource": {}},
        {"eventType": "user.created", "publisherId": "tfs"},
    ]

    for payload in invalid_payloads:
        assert await base_processor.validate_payload(payload) is False
