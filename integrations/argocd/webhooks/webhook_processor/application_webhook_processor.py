from port_ocean.core.handlers.webhook.abstract_webhook_processor import (
    AbstractWebhookProcessor,
)
from port_ocean.core.handlers.webhook.webhook_event import (
    WebhookEvent,
    EventPayload,
    EventHeaders,
)
from port_ocean.core.handlers.port_app_config.models import ResourceConfig
from port_ocean.core.handlers.webhook.webhook_event import WebhookEventRawResults
from loguru import logger
from argocd.misc import ResourceKindsWithSpecialHandling
from argocd.main import init_client
from argocd.integration import ApplicationResourceConfig
from typing import cast


class ArgocdApplicationWebhookProcessor(AbstractWebhookProcessor):
    """Handles ArgoCD notification webhooks for application upsert events."""

    _ACTION = "upsert"

    async def get_matching_kinds(self, event: WebhookEvent) -> list[str]:
        return [ResourceKindsWithSpecialHandling.APPLICATION]

    async def authenticate(self, payload: EventPayload, headers: EventHeaders) -> bool:
        return True

    async def validate_payload(self, payload: EventPayload) -> bool:
        return bool(payload.get("application_name"))

    async def should_process_event(self, event: WebhookEvent) -> bool:
        return event.payload.get("action") == self._ACTION

    async def handle_event(
        self, payload: EventPayload, resource_config: ResourceConfig
    ) -> WebhookEventRawResults:
        argocd_client = init_client()
        application_name = payload["application_name"]
        namespace = payload.get("application_namespace")
        selector = cast(ApplicationResourceConfig, resource_config).selector
        query_params = (
            selector.query_params.generate_request_params
            if selector.query_params
            else None
        )

        logger.info(f"Processing webhook upsert for application: {application_name}")
        application = await argocd_client.get_application_by_name(
            application_name,
            namespace=namespace,
            params=query_params,
        )

        if not application:
            logger.warning(
                f"Application {application_name} not found, skipping webhook processing"
            )
            return WebhookEventRawResults(
                updated_raw_results=[], deleted_raw_results=[]
            )

        logger.info(f"Application {application_name} found, registering raw data")

        return WebhookEventRawResults(
            updated_raw_results=[application],
            deleted_raw_results=[],
        )
