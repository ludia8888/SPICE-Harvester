from __future__ import annotations

import pytest

from shared.services.kafka.safe_consumer import ConsumerState, SafeKafkaConsumer


class _FailingConsumer:
    def __init__(self, exc: Exception) -> None:
        self._exc = exc

    def poll(self, timeout: float):  # noqa: ANN201
        _ = timeout
        raise self._exc

    def assignment(self):  # noqa: ANN201
        return []


class _EventuallyAssignedConsumer:
    def __init__(self) -> None:
        self._calls = 0

    def poll(self, timeout: float):  # noqa: ANN201
        _ = timeout
        self._calls += 1
        return None

    def assignment(self):  # noqa: ANN201
        return [object()] if self._calls >= 2 else []


@pytest.mark.unit
def test_wait_for_assignment_raises_after_repeated_poll_failures() -> None:
    consumer = SafeKafkaConsumer.__new__(SafeKafkaConsumer)
    consumer._state = ConsumerState.RUNNING  # type: ignore[attr-defined]
    consumer._consumer = _FailingConsumer(RuntimeError("broker auth failed"))  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="poll failed repeatedly"):
        consumer.wait_for_assignment(timeout_seconds=1.0)


@pytest.mark.unit
def test_wait_for_assignment_raises_when_consumer_is_closed() -> None:
    consumer = SafeKafkaConsumer.__new__(SafeKafkaConsumer)
    consumer._state = ConsumerState.CLOSED  # type: ignore[attr-defined]
    consumer._consumer = _FailingConsumer(RuntimeError("closed"))  # type: ignore[attr-defined]

    with pytest.raises(RuntimeError, match="closed while waiting"):
        consumer.wait_for_assignment(timeout_seconds=0.1)


@pytest.mark.unit
def test_wait_for_assignment_returns_true_when_assignment_arrives() -> None:
    consumer = SafeKafkaConsumer.__new__(SafeKafkaConsumer)
    consumer._state = ConsumerState.RUNNING  # type: ignore[attr-defined]
    consumer._consumer = _EventuallyAssignedConsumer()  # type: ignore[attr-defined]

    assert consumer.wait_for_assignment(timeout_seconds=1.0) is True
