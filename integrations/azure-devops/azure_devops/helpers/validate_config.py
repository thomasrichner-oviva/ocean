import json
from urllib.parse import urlparse

from loguru import logger


def validate_azure_devops_config(
    organization_url: str | None,
    personal_access_token: str | None,
    organization_token_mapping: str | None,
) -> None:
    """Validate Azure DevOps integration config at startup.

    Requires either the legacy single-org pair (organizationUrl +
    personalAccessToken) or the multi-org organizationTokenMapping JSON
    string.
    """
    is_single_org = bool(organization_url) and bool(personal_access_token)
    multi_org_mapping_str = (
        organization_token_mapping.strip() if organization_token_mapping else ""
    )

    if multi_org_mapping_str and is_single_org:
        logger.warning(
            "Both legacy organizationUrl/personalAccessToken and "
            "organizationTokenMapping are set; organizationTokenMapping takes precedence."
        )

    if not multi_org_mapping_str and not is_single_org:
        raise ValueError(
            "Azure DevOps integration requires either organizationUrl + "
            "personalAccessToken or organizationTokenMapping."
        )

    if not multi_org_mapping_str:
        logger.debug(
            "An `organizationTokenMapping` was not provided, skipping multi-org setup"
        )
        return

    try:
        multi_org_mapping = json.loads(multi_org_mapping_str)
    except json.JSONDecodeError as exc:
        raise ValueError(f"organizationTokenMapping must be a valid JSON string: {exc}")

    if not isinstance(multi_org_mapping, dict):
        raise ValueError(
            "organizationTokenMapping must be a JSON object of {org_url: PAT}."
        )

    if not multi_org_mapping:
        raise ValueError("organizationTokenMapping is empty.")

    for org_url, pat in multi_org_mapping.items():
        if not isinstance(org_url, str) or not org_url.strip():
            raise ValueError(
                "organizationTokenMapping keys must be non-empty organization URL strings."
            )
        parsed = urlparse(org_url)
        if not parsed.scheme or not parsed.netloc:
            raise ValueError(
                f"organizationTokenMapping key '{org_url}' is not a well-formed URL "
                f"(expected e.g. 'https://dev.azure.com/{{organization}}')."
            )
        if not isinstance(pat, str) or not pat.strip():
            raise ValueError(
                f"organizationTokenMapping value for '{org_url}' must be a non-empty Personal Access Token."
            )
