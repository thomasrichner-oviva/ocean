from port_ocean.core.handlers.port_app_config.models import (
    ResourceConfig,
    PortAppConfig,
    Selector,
)
from pydantic import Field, BaseModel
from typing import ClassVar, List


class RegionPolicy(BaseModel):
    allow: List[str] = Field(
        default_factory=list,
        title="Allow List",
        description="List of AWS regions to explicitly include when syncing resources. If non-empty, only these regions will be synced. Example: ['us-east-1', 'eu-west-1']",
    )
    deny: List[str] = Field(
        default_factory=list,
        title="Deny List",
        description="List of AWS regions to explicitly exclude when syncing resources. Regions in this list will always be skipped. Example: ['ap-southeast-1', 'sa-east-1']",
    )


class AWSDescribeResourcesSelector(Selector):
    use_get_resource_api: bool = Field(
        alias="useGetResourceAPI",
        default=False,
        title="Use Get Resource API",
        description="When enabled, uses the AWS Cloud Control GetResource API to fetch full resource details instead of relying solely on list results.",
    )
    region_policy: RegionPolicy = Field(
        alias="regionPolicy",
        default_factory=RegionPolicy,
        title="Region Policy",
        description="Controls which AWS regions are included or excluded when syncing resources. Uses allow/deny lists to filter regions.",
    )
    list_group_resources: bool = Field(
        alias="listGroupResources",
        default=False,
        title="List Group Resources",
        description="When enabled, lists resources belonging to AWS Resource Groups instead of querying all resources of the given type.",
    )

    def is_region_allowed(self, region: str) -> bool:
        """
        Determines if a given region is allowed based on the query regions policy.
        This method checks the `region_policy` attribute to decide if the specified
        region should be allowed or denied. The policy can contain "allow" and "deny" lists
        which dictate the behavior.

        Scenarios:
        - If `region_policy` is not set or empty, the method returns True, allowing all regions.
        - If the region is listed in the "deny" list of `region_policy`, the method returns False.
        - If the region is listed in the "allow" list of `region_policy`, the method returns True.
        - If the region is not listed in either "allow" or "deny" lists, the method returns False.
        - If the region is listed in both "allow" and "deny" lists, the method returns False.
        - If the policy denies regions but does not explicitly allow any, and the specific region is not in the deny list, then the region is considered allowed.
        - If the policy allows regions but does not explicitly deny any, and the specific region is not in the allow list, then the region is considered denied.
        Args:
            region (str): The region to be checked.

        Returns:
            bool: True if the region is allowed, False otherwise.
        """
        if not self.region_policy.allow and not self.region_policy.deny:
            return True
        if region in self.region_policy.deny:
            return False
        if region in self.region_policy.allow:
            return True
        if self.region_policy.deny and not self.region_policy.allow:
            return True
        if self.region_policy.allow and not self.region_policy.deny:
            return False
        return False


class AWSResourceConfig(ResourceConfig):
    selector: AWSDescribeResourcesSelector = Field(
        title="Selector",
        description="Defines which AWS resources to include in the sync, including region filtering and API options.",
    )
    kind: str = Field(
        title="AWS Resource Config",
        description="Use this to map AWS resources supported by the <a target='_blank' href='https://docs.aws.amazon.com/cloudcontrolapi/latest/userguide/supported-resources.html'>AWS Cloud Control API</a> by setting the kind name to the resource type.\n\nExample: AWS::S3::Bucket",
    )


class AWSPortAppConfig(PortAppConfig):
    resources: list[AWSResourceConfig] = Field(default_factory=list)  # type: ignore
    allow_custom_kinds: ClassVar[bool] = True
