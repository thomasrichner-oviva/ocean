from webhook.processors._base_processor import (
    ServicenowAbstractWebhookProcessor,
)
from port_ocean.core.handlers.webhook.webhook_event import (
    EventPayload,
    WebhookEvent,
    WebhookEventRawResults,
)
from port_ocean.core.handlers.port_app_config.models import ResourceConfig
from webhook.initialize_client import initialize_webhook_client


class UserMembershipWebhookProcessor(ServicenowAbstractWebhookProcessor):

    async def get_matching_kinds(self, event: WebhookEvent) -> list[str]:
        return ["sys_user"]

    def _should_process_event(self, event: WebhookEvent) -> bool:
        payload = event.payload
        return "user" in payload and "group" in payload

    async def handle_event(
        self, payload: EventPayload, resource_config: ResourceConfig
    ) -> WebhookEventRawResults:
        client = initialize_webhook_client()

        user_sys_id = (
            payload["user"]["value"]
            if isinstance(payload["user"], dict)
            else payload["user"]
        )

        user = await client.get_record_by_sys_id("sys_user", user_sys_id)
        if not user:
            return WebhookEventRawResults(
                updated_raw_results=[], deleted_raw_results=[]
            )

        teams = await client.get_memberships_for_user(user_sys_id)
        user["__teams"] = teams

        return WebhookEventRawResults(
            updated_raw_results=[user],
            deleted_raw_results=[],
        )
