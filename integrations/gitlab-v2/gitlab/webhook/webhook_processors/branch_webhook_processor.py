import re
from typing import cast

from loguru import logger
from port_ocean.core.handlers.port_app_config.models import ResourceConfig
from port_ocean.core.handlers.webhook.webhook_event import (
    EventPayload,
    WebhookEvent,
    WebhookEventRawResults,
)

from gitlab.helpers.utils import ObjectKind
from gitlab.webhook.webhook_processors._gitlab_abstract_webhook_processor import (
    _GitlabAbstractWebhookProcessor,
)
from integration import BranchResourceConfig

# GitLab sets the `after` commit SHA to all zeros when a branch is deleted
DELETED_COMMIT_SHA = "0000000000000000000000000000000000000000"


class BranchWebhookProcessor(_GitlabAbstractWebhookProcessor):
    events = ["push"]
    hooks = ["Push Hook"]

    async def get_matching_kinds(self, event: WebhookEvent) -> list[str]:
        return [ObjectKind.BRANCH]

    async def handle_event(
        self, payload: EventPayload, resource_config: ResourceConfig
    ) -> WebhookEventRawResults:
        project_id = payload["project"]["id"]
        project_path = payload["project"]["path_with_namespace"]
        # Use replace rather than split("/")[-1] to preserve slashes in branch names
        # e.g. refs/heads/feature/my-feature -> feature/my-feature
        branch_name = payload["ref"].replace("refs/heads/", "", 1)

        logger.info(
            f"Handling branch webhook event for project '{project_path}' and branch '{branch_name}'"
        )

        selector = cast(BranchResourceConfig, resource_config).selector
        if selector.default_branch_only:
            default_branch = payload["project"].get("default_branch")
            if branch_name != default_branch:
                logger.info(
                    f"Skipping branch '{branch_name}' for project '{project_path}': "
                    f"not the default branch ('{default_branch}')"
                )
                return WebhookEventRawResults(
                    updated_raw_results=[], deleted_raw_results=[]
                )

        if selector.regex:
            compiled = re.compile(selector.regex)
            if not compiled.fullmatch(branch_name):
                logger.info(
                    f"Skipping branch '{branch_name}' for project '{project_path}': "
                    f"regex {selector.regex} does not match ('{branch_name}')"
                )
                return WebhookEventRawResults(
                    updated_raw_results=[], deleted_raw_results=[]
                )
        elif selector.search:
            if selector.search not in branch_name:
                logger.info(
                    f"Skipping branch '{branch_name}' for project '{project_path}': "
                    f"search string - {selector.search} - does not match ('{branch_name}')"
                )
                return WebhookEventRawResults(
                    updated_raw_results=[], deleted_raw_results=[]
                )

        if payload.get("after") == DELETED_COMMIT_SHA:
            deleted_branch = self._gitlab_webhook_client.enrich_with_project_path(
                {"name": branch_name}, project_path
            )
            return WebhookEventRawResults(
                updated_raw_results=[], deleted_raw_results=[deleted_branch]
            )

        project = {"id": project_id, "path_with_namespace": project_path}
        branch = await self._gitlab_webhook_client.get_single_branch(
            project, branch_name
        )
        if branch:
            return WebhookEventRawResults(
                updated_raw_results=[branch], deleted_raw_results=[]
            )

        return WebhookEventRawResults(updated_raw_results=[], deleted_raw_results=[])
