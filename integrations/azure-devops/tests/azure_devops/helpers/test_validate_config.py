import json

import pytest

from azure_devops.helpers.validate_config import validate_azure_devops_config


def test_legacy_single_org_config_passes() -> None:
    validate_azure_devops_config(
        organization_url="https://dev.azure.com/myorg",
        personal_access_token="pat-12345",
        organization_token_mapping=None,
    )


def test_legacy_config_missing_pat_raises() -> None:
    with pytest.raises(ValueError, match="requires either"):
        validate_azure_devops_config(
            organization_url="https://dev.azure.com/myorg",
            personal_access_token="",
            organization_token_mapping=None,
        )


def test_legacy_config_missing_url_raises() -> None:
    with pytest.raises(ValueError, match="requires either"):
        validate_azure_devops_config(
            organization_url="",
            personal_access_token="pat-12345",
            organization_token_mapping=None,
        )


def test_no_config_at_all_raises() -> None:
    with pytest.raises(ValueError, match="requires either"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=None,
        )


def test_multi_org_valid_mapping_passes() -> None:
    validate_azure_devops_config(
        organization_url=None,
        personal_access_token=None,
        organization_token_mapping=json.dumps(
            {
                "https://dev.azure.com/org1": "pat-org1",
                "https://dev.azure.com/org2": "pat-org2",
            }
        ),
    )


def test_multi_org_single_entry_passes() -> None:
    validate_azure_devops_config(
        organization_url=None,
        personal_access_token=None,
        organization_token_mapping=json.dumps(
            {"https://dev.azure.com/only-org": "pat-only"}
        ),
    )


def test_multi_org_visualstudio_url_passes() -> None:
    validate_azure_devops_config(
        organization_url=None,
        personal_access_token=None,
        organization_token_mapping=json.dumps(
            {"https://myorg.visualstudio.com": "pat-vs"}
        ),
    )


def test_malformed_json_raises() -> None:
    with pytest.raises(ValueError, match="valid JSON string"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping="{not valid json",
        )


def test_json_list_instead_of_dict_raises() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps(
                [
                    {
                        "organizationUrl": "https://dev.azure.com/a",
                        "personalAccessToken": "p",
                    }
                ]
            ),
        )


def test_json_scalar_raises() -> None:
    with pytest.raises(ValueError, match="JSON object"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps("just a string"),
        )


def test_empty_json_dict_raises() -> None:
    with pytest.raises(ValueError, match="empty"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps({}),
        )


def test_malformed_url_key_raises() -> None:
    with pytest.raises(ValueError, match="well-formed URL"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps({"not-a-url": "pat-12345"}),
        )


def test_bare_path_url_raises() -> None:
    with pytest.raises(ValueError, match="well-formed URL"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps({"/some/path": "pat-12345"}),
        )


def test_empty_pat_value_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps({"https://dev.azure.com/org1": ""}),
        )


def test_whitespace_only_pat_value_raises() -> None:
    with pytest.raises(ValueError, match="non-empty"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps(
                {"https://dev.azure.com/org1": "   "}
            ),
        )


def test_non_string_pat_value_raises() -> None:
    with pytest.raises(ValueError, match="non-empty Personal Access Token"):
        validate_azure_devops_config(
            organization_url=None,
            personal_access_token=None,
            organization_token_mapping=json.dumps(
                {"https://dev.azure.com/org1": 12345}
            ),
        )


def test_both_legacy_and_mapping_passes() -> None:
    # Both set: validation passes (mapping wins at runtime), warning is logged.
    validate_azure_devops_config(
        organization_url="https://dev.azure.com/legacy",
        personal_access_token="legacy-pat",
        organization_token_mapping=json.dumps(
            {"https://dev.azure.com/new-org": "new-pat"}
        ),
    )
