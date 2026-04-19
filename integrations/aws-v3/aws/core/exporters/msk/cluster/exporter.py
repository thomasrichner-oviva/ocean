from typing import Any, AsyncGenerator, Type

from aws.core.client.proxy import AioBaseClientProxy
from aws.core.exporters.msk.cluster.actions import MskClusterActionsMap
from aws.core.exporters.msk.cluster.models import MskCluster
from aws.core.exporters.msk.cluster.models import (
    SingleMskClusterRequest,
    PaginatedMskClusterRequest,
)
from aws.core.helpers.types import SupportedServices
from aws.core.interfaces.exporter import IResourceExporter
from aws.core.modeling.resource_inspector import ResourceInspector


class MskClusterExporter(IResourceExporter):
    """Exporter for MSK cluster resources."""

    _service_name: SupportedServices = "kafka"
    _model_cls: Type[MskCluster] = MskCluster
    _actions_map: Type[MskClusterActionsMap] = MskClusterActionsMap

    async def get_resource(self, options: SingleMskClusterRequest) -> dict[str, Any]:
        """Fetch detailed attributes of a single MSK cluster."""

        async with AioBaseClientProxy(
            self.session, options.region, self._service_name
        ) as proxy:

            inspector = ResourceInspector(
                proxy.client, self._actions_map(), lambda: self._model_cls()
            )
            response = await inspector.inspect([options.cluster_arn], options.include)

            return response[0] if response else {}

    async def get_paginated_resources(
        self, options: PaginatedMskClusterRequest
    ) -> AsyncGenerator[list[dict[str, Any]], None]:
        """Fetch all MSK clusters in the region."""

        async with AioBaseClientProxy(
            self.session, options.region, self._service_name
        ) as proxy:
            inspector = ResourceInspector(
                proxy.client, self._actions_map(), lambda: self._model_cls()
            )
            paginator = proxy.get_paginator("list_clusters", "ClusterInfoList")

            async for clusters in paginator.paginate():
                if clusters:
                    cluster_arns = [c["ClusterArn"] for c in clusters]
                    action_result = await inspector.inspect(
                        cluster_arns,
                        options.include,
                        extra_context={
                            "AccountId": options.account_id,
                            "Region": options.region,
                        },
                    )
                    yield action_result
                else:
                    yield []
