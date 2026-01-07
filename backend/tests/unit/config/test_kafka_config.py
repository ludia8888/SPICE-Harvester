from __future__ import annotations

import pytest

from shared.config.kafka_config import KafkaEOSConfig, TransactionalProducer


class _FakeProducer:
    def __init__(self) -> None:
        self.calls = []
        self.produced = []
        self.flushed = False

    def init_transactions(self, timeout):
        self.calls.append(("init", timeout))

    def begin_transaction(self):
        self.calls.append("begin")

    def commit_transaction(self, timeout):
        self.calls.append(("commit", timeout))

    def abort_transaction(self, timeout):
        self.calls.append(("abort", timeout))

    def produce(self, *, topic, value, key=None):
        self.produced.append({"topic": topic, "value": value, "key": key})

    def flush(self):
        self.flushed = True


def test_kafka_eos_producer_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    config = KafkaEOSConfig.get_producer_config("service", instance_id="abc123", enable_transactions=True)

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["client.id"] == "service-producer"
    assert config["enable.idempotence"] is True
    assert config["transactional.id"].startswith("service-")


def test_kafka_eos_consumer_config(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")

    config = KafkaEOSConfig.get_consumer_config("svc", group_id="group", read_committed=False, auto_commit=True)

    assert config["bootstrap.servers"] == "localhost:9092"
    assert config["client.id"] == "svc-consumer"
    assert config["group.id"] == "group"
    assert config["isolation.level"] == "read_uncommitted"
    assert config["enable.auto.commit"] is True


def test_transactional_producer_batch_success() -> None:
    producer = _FakeProducer()
    wrapper = TransactionalProducer(producer, enable_transactions=True)

    wrapper.init_transactions()
    wrapper.init_transactions()

    messages = ["one", "two"]
    result = wrapper.send_transactional_batch(messages, "topic", key_extractor=lambda m: "k")

    assert result is True
    assert ("init", 30.0) in producer.calls
    assert "begin" in producer.calls
    assert ("commit", 30.0) in producer.calls
    assert len(producer.produced) == 2
    assert producer.produced[0]["key"] == b"k"


def test_transactional_producer_batch_no_transactions() -> None:
    producer = _FakeProducer()
    wrapper = TransactionalProducer(producer, enable_transactions=False)

    messages = ["one"]
    result = wrapper.send_transactional_batch(messages, "topic")

    assert result is True
    assert producer.flushed is True
    assert producer.produced[0]["value"] == b"one"
