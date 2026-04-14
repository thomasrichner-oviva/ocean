from typing import Literal

from pydantic import Field

from kinds import ObjectKind
from port_ocean.core.handlers.port_app_config.api import APIPortAppConfig
from port_ocean.core.handlers.port_app_config.models import (
    PortAppConfig,
    ResourceConfig,
)
from port_ocean.core.integrations.base import BaseIntegration


class ComponentResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.COMPONENT] = Field(
        title="Backstage Component",
        description="Backstage component entity kind.",
    )


class ApiResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.API] = Field(
        title="Backstage API",
        description="Backstage API entity kind.",
    )


class GroupResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.GROUP] = Field(
        title="Backstage Group",
        description="Backstage group entity kind.",
    )


class UserResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.USER] = Field(
        title="Backstage User",
        description="Backstage user entity kind.",
    )


class SystemResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.SYSTEM] = Field(
        title="Backstage System",
        description="Backstage system entity kind.",
    )


class DomainResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.DOMAIN] = Field(
        title="Backstage Domain",
        description="Backstage domain entity kind.",
    )


class ResourceEntityResourceConfig(ResourceConfig):
    kind: Literal[ObjectKind.RESOURCE] = Field(
        title="Backstage Resource",
        description="Backstage resource entity kind.",
    )


class BackstagePortAppConfig(PortAppConfig):
    resources: list[
        ComponentResourceConfig
        | ApiResourceConfig
        | GroupResourceConfig
        | UserResourceConfig
        | SystemResourceConfig
        | DomainResourceConfig
        | ResourceEntityResourceConfig
    ] = Field(
        default_factory=list,
    )  # type: ignore[assignment]


class BackstageIntegration(BaseIntegration):
    class AppConfigHandlerClass(APIPortAppConfig):
        CONFIG_CLASS = BackstagePortAppConfig
