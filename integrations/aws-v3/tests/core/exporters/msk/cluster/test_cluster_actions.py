from typing import Any
from unittest.mock import AsyncMock

import pytest

from aws.core.exporters.msk.cluster.actions import (
    DescribeClusterAction,
    MskClusterActionsMap,
)
from aws.core.interfaces.action import Action


class TestDescribeClusterAction:

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def action(self, mock_client: AsyncMock) -> DescribeClusterAction:
        return DescribeClusterAction(mock_client)

    def test_inheritance(self, action: DescribeClusterAction) -> None:
        assert isinstance(action, Action)

    @pytest.mark.asyncio
    async def test_execute_success(self, action: DescribeClusterAction) -> None:
        cluster_arns = [
            "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc123",
            "arn:aws:kafka:us-west-2:123456789012:cluster/prod-cluster/def456",
        ]

        def mock_describe_cluster(ClusterArn: str) -> dict[str, Any]:
            cluster_name = ClusterArn.split("/")[1]
            return {
                "ClusterInfo": {
                    "ClusterArn": ClusterArn,
                    "ClusterName": cluster_name,
                    "State": "ACTIVE",
                    "NumberOfBrokerNodes": 3,
                    "BrokerNodeGroupInfo": {"InstanceType": "kafka.m5.large"},
                    "ClientAuthentication": {"Sasl": {"Iam": {"Enabled": True}}},
                    "Tags": {},
                }
            }

        action.client.describe_cluster.side_effect = mock_describe_cluster

        result = await action.execute(cluster_arns)

        assert len(result) == 2
        assert result[0]["ClusterName"] == "test-cluster"
        assert result[0]["State"] == "ACTIVE"
        assert result[1]["ClusterName"] == "prod-cluster"

    @pytest.mark.asyncio
    async def test_execute_empty_list(self, action: DescribeClusterAction) -> None:
        result = await action.execute([])
        assert result == []

    @pytest.mark.asyncio
    async def test_execute_with_recoverable_exception(
        self, action: DescribeClusterAction
    ) -> None:
        cluster_arns = [
            "arn:aws:kafka:us-west-2:123456789012:cluster/failing-cluster/fail123",
        ]

        error_response = {"Error": {"Code": "AccessDeniedException"}}
        action.client.describe_cluster.side_effect = type(
            "ClientError", (Exception,), {"response": error_response}
        )()

        result = await action.execute(cluster_arns)

        assert len(result) == 0

    @pytest.mark.asyncio
    async def test_execute_with_non_recoverable_exception(
        self, action: DescribeClusterAction
    ) -> None:
        cluster_arns = [
            "arn:aws:kafka:us-west-2:123456789012:cluster/failing-cluster/fail123",
        ]

        action.client.describe_cluster.side_effect = RuntimeError("Unexpected error")

        with pytest.raises(RuntimeError, match="Unexpected error"):
            await action.execute(cluster_arns)


class TestMskClusterActionsMap:

    def test_defaults(self) -> None:
        assert DescribeClusterAction in MskClusterActionsMap.defaults

    def test_options(self) -> None:
        assert MskClusterActionsMap.options == []
