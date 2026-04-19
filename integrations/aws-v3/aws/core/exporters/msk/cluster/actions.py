from typing import Dict, Any, List, Type, cast
import asyncio

from aws.core.interfaces.action import Action, ActionMap
from loguru import logger


class ListClustersAction(Action):
    """Processes the initial list of clusters from the paginator."""

    async def _execute(self, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        results: List[Dict[str, Any]] = []
        for cluster in clusters:
            data = {
                "ClusterArn": cluster.get("ClusterArn", ""),
                "ClusterName": cluster.get("ClusterName", ""),
                "State": cluster.get("State"),
                "CreationTime": cluster.get("CreationTime"),
            }
            results.append(data)
        return results


class DescribeClustersAction(Action):
    """Fetches detailed information for each MSK cluster."""

    async def _execute(self, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        if not clusters:
            return []

        details = await asyncio.gather(
            *(self._fetch_cluster_details(cluster) for cluster in clusters),
            return_exceptions=True,
        )

        results: List[Dict[str, Any]] = []
        for idx, detail_result in enumerate(details):
            if isinstance(detail_result, Exception):
                cluster_arn = clusters[idx].get("ClusterArn", "unknown")
                logger.error(
                    f"Error fetching MSK cluster details for '{cluster_arn}': {detail_result}"
                )
                continue
            results.append(cast(Dict[str, Any], detail_result))
        return results

    async def _fetch_cluster_details(self, cluster: Dict[str, Any]) -> Dict[str, Any]:
        cluster_arn = cluster.get("ClusterArn", "")
        if not cluster_arn:
            return {}

        response = await self.client.describe_cluster(ClusterArn=cluster_arn)
        cluster_info = response.get("ClusterInfo", {})

        logger.info(f"Successfully fetched MSK cluster details for {cluster_arn}")

        return {
            "ClusterArn": cluster_info.get("ClusterArn", ""),
            "ClusterName": cluster_info.get("ClusterName", ""),
            "State": cluster_info.get("State"),
            "CreationTime": cluster_info.get("CreationTime"),
            "CurrentVersion": cluster_info.get("CurrentVersion"),
            "BrokerNodeGroupInfo": cluster_info.get("BrokerNodeGroupInfo"),
            "ClientAuthentication": cluster_info.get("ClientAuthentication"),
            "EncryptionInfo": cluster_info.get("EncryptionInfo"),
            "CurrentBrokerSoftwareInfo": cluster_info.get("CurrentBrokerSoftwareInfo"),
            "LoggingInfo": cluster_info.get("LoggingInfo"),
            "OpenMonitoring": cluster_info.get("OpenMonitoring"),
            "NumberOfBrokerNodes": cluster_info.get("NumberOfBrokerNodes"),
            "EnhancedMonitoring": cluster_info.get("EnhancedMonitoring"),
            "StorageMode": cluster_info.get("StorageMode"),
            "ZookeeperConnectString": cluster_info.get("ZookeeperConnectString"),
            "ZookeeperConnectStringTls": cluster_info.get("ZookeeperConnectStringTls"),
            "Tags": cluster_info.get("Tags", {}),
        }


class MskClusterActionsMap(ActionMap):
    """Groups all actions for MSK cluster resources."""

    defaults: List[Type[Action]] = [
        ListClustersAction,
        DescribeClustersAction,
    ]
    options: List[Type[Action]] = []
