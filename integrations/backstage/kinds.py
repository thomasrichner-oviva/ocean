from enum import StrEnum


class ObjectKind(StrEnum):
    COMPONENT = "component"
    API = "api"
    GROUP = "group"
    USER = "user"
    SYSTEM = "system"
    DOMAIN = "domain"
    RESOURCE = "resource"
