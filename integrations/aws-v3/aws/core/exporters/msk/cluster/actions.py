import asyncio
from typing import Dict, Any, List, Type, cast

from aws.core.interfaces.action import Action, ActionMap
from aws.core.helpers.utils import is_recoverable_aws_exception
from loguru import logger


class DescribeClusterAction(Action):
    """Fetches detailed information for each MSK cluster."""

    async def _execute(self, cluster_arns: List[str]) -> List[Dict[str, Any]]:
        if not cluster_arns:
            return []

        cluster_results = await asyncio.gather(
            *(self._fetch_cluster(arn) for arn in cluster_arns),
            return_exceptions=True,
        )

        results: List[Dict[str, Any]] = []
        for idx, result in enumerate(cluster_results):
            if isinstance(result, Exception):
                cluster_arn = (
                    cluster_arns[idx] if idx < len(cluster_arns) else "unknown"
                )
                if is_recoverable_aws_exception(result):
                    logger.warning(
                        f"Skipping MSK cluster '{cluster_arn}' due to error: {result}"
                    )
                    continue
                else:
                    logger.error(f"Error fetching MSK cluster '{cluster_arn}'")
                    raise result
            results.append(cast(Dict[str, Any], result))

        return results

    async def _fetch_cluster(self, cluster_arn: str) -> Dict[str, Any]:
        """Fetch a single cluster by ARN."""
        response = await self.client.describe_cluster(ClusterArn=cluster_arn)
        logger.info(f"Successfully fetched MSK cluster '{cluster_arn}'")
        return response["ClusterInfo"]


class MskClusterActionsMap(ActionMap):
    """Groups all actions for MSK cluster resources."""

    defaults: List[Type[Action]] = [DescribeClusterAction]
    options: List[Type[Action]] = []
