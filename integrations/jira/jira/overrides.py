from typing import Literal
from pydantic import Field, validator

from port_ocean.core.handlers.port_app_config.models import (
    PortAppConfig,
    ResourceConfig,
    Selector,
)


class TeamSelector(Selector):
    include_members: bool = Field(
        alias="includeMembers",
        default=False,
        description="Whether to include the members of the team, defaults to false",
    )


class TeamResourceConfig(ResourceConfig):
    kind: Literal["team"]
    selector: TeamSelector


class JiraIssueSelector(Selector):
    jql: str = Field(
        default="(statusCategory != Done) OR (created >= -1w) OR (updated >= -1w)",
        description="JQL query to filter issues. Defaults to fetching all issues across all projects.",
    )
    fields: str | None = Field(
        description="Additional fields to be included in the API response",
        default="*all",
    )
    expand: str | None = Field(
        description="A comma-separated list of parameters to expand in the API response. Supported values depend on the Jira API and may include 'renderedFields', 'names', 'schema', etc.",
        default=None,
    )


class JiraIssueConfig(ResourceConfig):
    selector: JiraIssueSelector
    kind: Literal["issue"]


class JiraProjectSelector(Selector):
    expand: str = Field(
        description="A comma-separated list of the parameters to expand.",
        default="insight",
    )


class JiraProjectResourceConfig(ResourceConfig):
    selector: JiraProjectSelector
    kind: Literal["project"]


class JiraBoardSelector(Selector):
    board_type: Literal["scrum", "kanban", "simple"] | None = Field(
        alias="boardType",
        default=None,
        title="Board Type",
        description=("Filter boards by type. Omit to fetch all board types."),
    )
    project_key: str | None = Field(
        alias="projectKey",
        default=None,
        title="Project Key",
        description=(
            "Filter boards scoped to a specific Jira project. "
            "Accepts a project key (e.g. PORT) or project ID."
        ),
    )

    @validator("project_key")
    def project_key_must_not_be_empty(cls, v: str | None) -> str | None:
        if v is not None and not v.strip():
            raise ValueError("projectKey must not be an empty string")
        return v


class JiraBoardResourceConfig(ResourceConfig):
    kind: Literal["board"] = Field(
        title="Jira Board",
        description="Jira board resource kind.",
    )
    selector: JiraBoardSelector = Field(
        title="Board Selector",
        description="Selector for Jira board resources.",
    )


class JiraPortAppConfig(PortAppConfig):
    resources: list[
        TeamResourceConfig
        | JiraIssueConfig
        | JiraProjectResourceConfig
        | JiraBoardResourceConfig
        | ResourceConfig
    ]
