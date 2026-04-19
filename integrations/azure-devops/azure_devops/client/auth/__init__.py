from azure_devops.client.auth.base import Authenticator
from azure_devops.client.auth.pat import PATAuthenticator
from azure_devops.client.auth.service_principal import (
    AZURE_DEVOPS_DEFAULT_SCOPE,
    AZURE_DEVOPS_RESOURCE_ID,
    EntraIdToken,
    ServicePrincipalAuthenticator,
)

__all__ = [
    "Authenticator",
    "PATAuthenticator",
    "ServicePrincipalAuthenticator",
    "EntraIdToken",
    "AZURE_DEVOPS_RESOURCE_ID",
    "AZURE_DEVOPS_DEFAULT_SCOPE",
]
