"""
Kafka producer factory (Factory Method).

Centralizes common confluent-kafka Producer configuration used across workers,
especially DLQ publishers, to reduce duplication and keep defaults consistent.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Mapping, Optional, TypeVar

from confluent_kafka import Producer

DEFAULT_ACKS = "all"
DEFAULT_COMPRESSION_TYPE = "snappy"

ProducerT = TypeVar("ProducerT")


def create_kafka_producer(
    *,
    bootstrap_servers: str,
    client_id: str,
    acks: str = DEFAULT_ACKS,
    retries: int = 3,
    compression_type: str = DEFAULT_COMPRESSION_TYPE,
    retry_backoff_ms: Optional[int] = None,
    linger_ms: Optional[int] = None,
    enable_idempotence: Optional[bool] = None,
    max_in_flight_requests_per_connection: Optional[int] = None,
    extra_config: Optional[Mapping[str, Any]] = None,
    producer_ctor: Callable[[dict[str, Any]], ProducerT] = Producer,
) -> ProducerT:
    config: dict[str, Any] = {
        "bootstrap.servers": str(bootstrap_servers),
        "client.id": str(client_id),
        "acks": str(acks),
        "retries": int(retries),
        "compression.type": str(compression_type),
    }

    if retry_backoff_ms is not None:
        config["retry.backoff.ms"] = int(retry_backoff_ms)
    if linger_ms is not None:
        config["linger.ms"] = int(linger_ms)
    if enable_idempotence is not None:
        config["enable.idempotence"] = bool(enable_idempotence)
    if max_in_flight_requests_per_connection is not None:
        config["max.in.flight.requests.per.connection"] = int(max_in_flight_requests_per_connection)
    if extra_config:
        config.update(dict(extra_config))

    return producer_ctor(config)


def create_kafka_dlq_producer(
    *,
    bootstrap_servers: str,
    client_id: str,
    acks: str = DEFAULT_ACKS,
    retries: int = 3,
    retry_backoff_ms: int = 100,
    linger_ms: int = 20,
    compression_type: str = DEFAULT_COMPRESSION_TYPE,
    enable_idempotence: Optional[bool] = None,
    max_in_flight_requests_per_connection: Optional[int] = None,
    extra_config: Optional[Mapping[str, Any]] = None,
    producer_ctor: Callable[[dict[str, Any]], ProducerT] = Producer,
) -> ProducerT:
    return create_kafka_producer(
        bootstrap_servers=bootstrap_servers,
        client_id=client_id,
        acks=acks,
        retries=retries,
        compression_type=compression_type,
        retry_backoff_ms=retry_backoff_ms,
        linger_ms=linger_ms,
        enable_idempotence=enable_idempotence,
        max_in_flight_requests_per_connection=max_in_flight_requests_per_connection,
        extra_config=extra_config,
        producer_ctor=producer_ctor,
    )
