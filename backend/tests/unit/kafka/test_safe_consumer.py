"""
Unit tests for SafeKafkaConsumer - Strong Consistency Guarantees.

Tests the critical safety features:
1. isolation.level=read_committed enforcement
2. Rebalance callback handling
3. Proper offset management
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestSafeKafkaConsumerConfig:
    """Test SafeKafkaConsumer configuration enforcement."""

    def test_enforced_settings_cannot_be_overridden(self) -> None:
        """ENFORCED_SETTINGS should not allow override via extra_config."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        # Verify ENFORCED_SETTINGS contains critical settings
        assert SafeKafkaConsumer.ENFORCED_SETTINGS["isolation.level"] == "read_committed"
        assert SafeKafkaConsumer.ENFORCED_SETTINGS["enable.auto.commit"] is False

    def test_isolation_level_read_committed(self) -> None:
        """SafeKafkaConsumer should always use read_committed isolation."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        assert SafeKafkaConsumer.ENFORCED_SETTINGS["isolation.level"] == "read_committed"

    def test_auto_commit_disabled(self) -> None:
        """SafeKafkaConsumer should always disable auto-commit."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        assert SafeKafkaConsumer.ENFORCED_SETTINGS["enable.auto.commit"] is False

    def test_validate_consumer_config_rejects_uncommitted(self) -> None:
        """validate_consumer_config should reject read_uncommitted."""
        from shared.services.kafka.safe_consumer import validate_consumer_config

        with pytest.raises(ValueError, match="isolation.level must be 'read_committed'"):
            validate_consumer_config({"isolation.level": "read_uncommitted"})

    def test_validate_consumer_config_rejects_auto_commit(self) -> None:
        """validate_consumer_config should reject auto-commit."""
        from shared.services.kafka.safe_consumer import validate_consumer_config

        with pytest.raises(ValueError, match="enable.auto.commit must be False"):
            validate_consumer_config({
                "isolation.level": "read_committed",
                "enable.auto.commit": True,
            })

    def test_validate_consumer_config_accepts_valid(self) -> None:
        """validate_consumer_config should accept valid config."""
        from shared.services.kafka.safe_consumer import validate_consumer_config

        # Should not raise
        validate_consumer_config({
            "isolation.level": "read_committed",
            "enable.auto.commit": False,
        })


class TestConsumerState:
    """Test ConsumerState enum."""

    def test_consumer_state_values(self) -> None:
        """ConsumerState should have all lifecycle states."""
        from shared.services.kafka.safe_consumer import ConsumerState

        assert ConsumerState.CREATED.value == "created"
        assert ConsumerState.RUNNING.value == "running"
        assert ConsumerState.REBALANCING.value == "rebalancing"
        assert ConsumerState.PAUSED.value == "paused"
        assert ConsumerState.CLOSED.value == "closed"


class TestPartitionState:
    """Test PartitionState dataclass."""

    def test_partition_state_creation(self) -> None:
        """PartitionState should track partition info."""
        from shared.services.kafka.safe_consumer import PartitionState

        state = PartitionState(
            topic="test-topic",
            partition=0,
            current_offset=100,
        )

        assert state.topic == "test-topic"
        assert state.partition == 0
        assert state.current_offset == 100
        assert state.pending_offset is None
        assert state.processing is False


class TestRebalanceHandler:
    """Test RebalanceHandler callbacks."""

    def test_rebalance_handler_on_revoke_marks_rebalancing(self) -> None:
        """on_revoke should set consumer state to REBALANCING."""
        from shared.services.kafka.safe_consumer import (
            RebalanceHandler,
            SafeKafkaConsumer,
            ConsumerState,
        )

        # Create mock consumer
        mock_consumer = MagicMock(spec=SafeKafkaConsumer)
        mock_consumer._state = ConsumerState.RUNNING
        mock_consumer._partition_states = {}

        handler = RebalanceHandler(consumer=mock_consumer)

        # Mock confluent_kafka Consumer
        mock_kafka_consumer = MagicMock()

        # Mock TopicPartition
        mock_tp = MagicMock()
        mock_tp.topic = "test"
        mock_tp.partition = 0

        # Call on_revoke
        handler.on_revoke(mock_kafka_consumer, [mock_tp])

        # Should set state to REBALANCING
        assert mock_consumer._state == ConsumerState.REBALANCING

    def test_rebalance_handler_on_assign_marks_running(self) -> None:
        """on_assign should set consumer state to RUNNING."""
        from shared.services.kafka.safe_consumer import (
            RebalanceHandler,
            SafeKafkaConsumer,
            ConsumerState,
        )

        # Create mock consumer
        mock_consumer = MagicMock(spec=SafeKafkaConsumer)
        mock_consumer._state = ConsumerState.REBALANCING
        mock_consumer._partition_states = {}

        handler = RebalanceHandler(consumer=mock_consumer)

        # Mock TopicPartition
        mock_tp = MagicMock()
        mock_tp.topic = "test"
        mock_tp.partition = 0
        mock_tp.offset = 100

        # Call on_assign
        handler.on_assign(MagicMock(), [mock_tp])

        # Should set state to RUNNING
        assert mock_consumer._state == ConsumerState.RUNNING

    def test_rebalance_handler_calls_user_callbacks(self) -> None:
        """RebalanceHandler should call user-provided callbacks."""
        from shared.services.kafka.safe_consumer import (
            RebalanceHandler,
            SafeKafkaConsumer,
            ConsumerState,
        )

        mock_consumer = MagicMock(spec=SafeKafkaConsumer)
        mock_consumer._state = ConsumerState.RUNNING
        mock_consumer._partition_states = {}

        on_revoke_called = []
        on_assign_called = []

        def user_on_revoke(partitions):
            on_revoke_called.append(partitions)

        def user_on_assign(partitions):
            on_assign_called.append(partitions)

        handler = RebalanceHandler(
            consumer=mock_consumer,
            on_revoke_callback=user_on_revoke,
            on_assign_callback=user_on_assign,
        )

        mock_tp = MagicMock()
        mock_tp.topic = "test"
        mock_tp.partition = 0
        mock_tp.offset = 100

        handler.on_revoke(MagicMock(), [mock_tp])
        handler.on_assign(MagicMock(), [mock_tp])

        assert len(on_revoke_called) == 1
        assert len(on_assign_called) == 1


class TestSafeKafkaConsumerIntegration:
    """Integration tests (with mocked Kafka client)."""

    @patch("shared.services.kafka.safe_consumer.Consumer")
    @patch("shared.services.kafka.safe_consumer.get_settings")
    def test_consumer_creation_with_enforced_settings(
        self, mock_settings, mock_consumer_class
    ) -> None:
        """SafeKafkaConsumer should create consumer with enforced settings."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        # Mock settings
        mock_settings.return_value.database.kafka_servers = "localhost:9092"

        # Create consumer
        SafeKafkaConsumer(
            group_id="test-group",
            topics=["test-topic"],
            service_name="test-service",
        )

        # Verify Consumer was called with correct config
        call_args = mock_consumer_class.call_args[0][0]
        assert call_args["isolation.level"] == "read_committed"
        assert call_args["enable.auto.commit"] is False
        assert call_args["group.id"] == "test-group"

    @patch("shared.services.kafka.safe_consumer.Consumer")
    @patch("shared.services.kafka.safe_consumer.get_settings")
    def test_extra_config_cannot_override_enforced(
        self, mock_settings, mock_consumer_class
    ) -> None:
        """extra_config should not override ENFORCED_SETTINGS."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer

        mock_settings.return_value.database.kafka_servers = "localhost:9092"

        # Try to override enforced settings
        SafeKafkaConsumer(
            group_id="test-group",
            topics=["test-topic"],
            service_name="test-service",
            extra_config={
                "isolation.level": "read_uncommitted",  # Should be ignored
                "enable.auto.commit": True,  # Should be ignored
                "fetch.min.bytes": 1024,  # Should be applied
            },
        )

        call_args = mock_consumer_class.call_args[0][0]
        # Enforced settings should remain
        assert call_args["isolation.level"] == "read_committed"
        assert call_args["enable.auto.commit"] is False
        # Extra config should be applied
        assert call_args["fetch.min.bytes"] == 1024

    @patch("shared.services.kafka.safe_consumer.Consumer")
    @patch("shared.services.kafka.safe_consumer.get_settings")
    def test_is_rebalancing_property(self, mock_settings, mock_consumer_class) -> None:
        """is_rebalancing should reflect REBALANCING state."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer, ConsumerState

        mock_settings.return_value.database.kafka_servers = "localhost:9092"

        consumer = SafeKafkaConsumer(
            group_id="test-group",
            topics=["test-topic"],
            service_name="test-service",
        )

        assert consumer.is_rebalancing is False
        consumer._state = ConsumerState.REBALANCING
        assert consumer.is_rebalancing is True

    @patch("shared.services.kafka.safe_consumer.Consumer")
    @patch("shared.services.kafka.safe_consumer.get_settings")
    def test_commit_message_marks_partition_processed(self, mock_settings, mock_consumer_class) -> None:
        """commit(message=...) should clear processing state (rebalance-safe bookkeeping)."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer, PartitionState

        mock_settings.return_value.database.kafka_servers = "localhost:9092"
        mock_kafka_consumer = MagicMock()
        mock_consumer_class.return_value = mock_kafka_consumer

        consumer = SafeKafkaConsumer(
            group_id="test-group",
            topics=["test-topic"],
            service_name="test-service",
        )

        key = ("test-topic", 0)
        consumer._partition_states[key] = PartitionState(topic="test-topic", partition=0, pending_offset=123, processing=True)

        msg = MagicMock()
        msg.topic.return_value = "test-topic"
        msg.partition.return_value = 0
        msg.offset.return_value = 123

        consumer.commit(message=msg, asynchronous=False)

        mock_kafka_consumer.commit.assert_called_with(message=msg, asynchronous=False)
        state = consumer._partition_states[key]
        assert state.processing is False
        assert state.current_offset == 123
        assert state.last_committed == 123

    @patch("shared.services.kafka.safe_consumer.Consumer")
    @patch("shared.services.kafka.safe_consumer.get_settings")
    def test_seek_delegates_and_clears_inflight_state(self, mock_settings, mock_consumer_class) -> None:
        """seek() should delegate and clear in-flight state to avoid stale bookkeeping."""
        from shared.services.kafka.safe_consumer import SafeKafkaConsumer, PartitionState
        from confluent_kafka import TopicPartition

        mock_settings.return_value.database.kafka_servers = "localhost:9092"
        mock_kafka_consumer = MagicMock()
        mock_consumer_class.return_value = mock_kafka_consumer

        consumer = SafeKafkaConsumer(
            group_id="test-group",
            topics=["test-topic"],
            service_name="test-service",
        )

        key = ("test-topic", 0)
        consumer._partition_states[key] = PartitionState(topic="test-topic", partition=0, pending_offset=123, processing=True)

        tp = TopicPartition("test-topic", 0, 123)
        consumer.seek(tp)

        mock_kafka_consumer.seek.assert_called_with(tp)
        state = consumer._partition_states[key]
        assert state.processing is False
        assert state.pending_offset is None
