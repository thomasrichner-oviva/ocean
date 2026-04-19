from unittest.mock import AsyncMock
import pytest

from aws.core.exporters.msk.cluster.actions import (
    ListClustersAction,
    DescribeClustersAction,
    MskClusterActionsMap,
)
from aws.core.interfaces.action import Action


class TestListClustersAction:

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        return AsyncMock()

    @pytest.fixture
    def action(self, mock_client: AsyncMock) -> ListClustersAction:
        return ListClustersAction(mock_client)

    def test_inheritance(self, action: ListClustersAction) -> None:
        assert isinstance(action, Action)

    @pytest.mark.asyncio
    async def test_execute_success(self, action: ListClustersAction) -> None:
        clusters = [
            {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc123",
                "ClusterName": "test-cluster",
                "State": "ACTIVE",
                "CreationTime": "2024-01-01T00:00:00Z",
            },
            {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/prod-cluster/def456",
                "ClusterName": "prod-cluster",
                "State": "ACTIVE",
                "CreationTime": "2024-02-01T00:00:00Z",
            },
        ]

        result = await action.execute(clusters)

        assert len(result) == 2
        assert result[0]["ClusterArn"] == clusters[0]["ClusterArn"]
        assert result[0]["ClusterName"] == "test-cluster"
        assert result[1]["ClusterName"] == "prod-cluster"

    @pytest.mark.asyncio
    async def test_execute_empty_list(self, action: ListClustersAction) -> None:
        result = await action.execute([])
        assert result == []


class TestDescribeClustersAction:

    @pytest.fixture
    def mock_client(self) -> AsyncMock:
        mock_client = AsyncMock()
        return mock_client

    @pytest.fixture
    def action(self, mock_client: AsyncMock) -> DescribeClustersAction:
        return DescribeClustersAction(mock_client)

    def test_inheritance(self, action: DescribeClustersAction) -> None:
        assert isinstance(action, Action)

    @pytest.mark.asyncio
    async def test_execute_success(self, action: DescribeClustersAction) -> None:
        clusters = [
            {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc123",
            },
        ]

        action.client.describe_cluster.return_value = {
            "ClusterInfo": {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc123",
                "ClusterName": "test-cluster",
                "State": "ACTIVE",
                "NumberOfBrokerNodes": 3,
                "BrokerNodeGroupInfo": {
                    "InstanceType": "kafka.m5.large",
                    "ClientSubnets": ["subnet-1", "subnet-2"],
                },
                "ClientAuthentication": {
                    "Sasl": {"Iam": {"Enabled": True}},
                },
                "CurrentBrokerSoftwareInfo": {
                    "KafkaVersion": "2.8.1",
                },
                "Tags": {"Environment": "test"},
            }
        }

        result = await action.execute(clusters)

        assert len(result) == 1
        assert result[0]["ClusterName"] == "test-cluster"
        assert result[0]["State"] == "ACTIVE"
        assert result[0]["NumberOfBrokerNodes"] == 3
        assert result[0]["BrokerNodeGroupInfo"]["InstanceType"] == "kafka.m5.large"
        assert result[0]["Tags"] == {"Environment": "test"}

        action.client.describe_cluster.assert_called_once_with(
            ClusterArn="arn:aws:kafka:us-west-2:123456789012:cluster/test-cluster/abc123"
        )

    @pytest.mark.asyncio
    async def test_execute_with_minimal_fields(
        self, action: DescribeClustersAction
    ) -> None:
        clusters = [
            {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/minimal-cluster/xyz789",
            },
        ]

        action.client.describe_cluster.return_value = {
            "ClusterInfo": {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/minimal-cluster/xyz789",
                "ClusterName": "minimal-cluster",
            }
        }

        result = await action.execute(clusters)

        assert len(result) == 1
        assert result[0]["ClusterName"] == "minimal-cluster"
        assert result[0]["Tags"] == {}

    @pytest.mark.asyncio
    async def test_execute_empty_list(self, action: DescribeClustersAction) -> None:
        result = await action.execute([])
        assert result == []

    @pytest.mark.asyncio
    async def test_execute_with_exception(self, action: DescribeClustersAction) -> None:
        clusters = [
            {
                "ClusterArn": "arn:aws:kafka:us-west-2:123456789012:cluster/failing-cluster/fail123",
            },
        ]

        action.client.describe_cluster.side_effect = Exception("Access denied")

        result = await action.execute(clusters)

        assert len(result) == 0


class TestMskClusterActionsMap:

    def test_defaults(self) -> None:
        assert ListClustersAction in MskClusterActionsMap.defaults
        assert DescribeClustersAction in MskClusterActionsMap.defaults

    def test_options(self) -> None:
        assert MskClusterActionsMap.options == []
