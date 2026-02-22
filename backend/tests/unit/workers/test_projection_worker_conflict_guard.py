from __future__ import annotations

from typing import Any, Dict, Optional

import pytest

from projection_worker.main import ProjectionWorker


class _FakeElasticSearch:
    def __init__(self, *, document: Optional[Dict[str, Any]]) -> None:
        self._document = document
        self.calls: list[tuple[str, str]] = []

    async def get_document(self, index_name: str, doc_id: str) -> Optional[Dict[str, Any]]:
        self.calls.append((index_name, doc_id))
        return self._document


def _build_worker(*, document: Optional[Dict[str, Any]]) -> tuple[ProjectionWorker, list[dict[str, Any]]]:
    worker = object.__new__(ProjectionWorker)
    worker.elasticsearch_service = _FakeElasticSearch(document=document)
    side_effects: list[dict[str, Any]] = []

    async def _record_es_side_effect(**kwargs: Any) -> None:
        side_effects.append(dict(kwargs))

    worker._record_es_side_effect = _record_es_side_effect  # type: ignore[method-assign]
    return worker, side_effects


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_es_version_conflict_confirms_stale_sequence() -> None:
    worker, side_effects = _build_worker(document={"event_sequence": 12})

    await worker._handle_es_version_conflict(
        event_id="evt-1",
        event_data={"event_type": "OntologyClassUpdated", "sequence_number": 10},
        db_name="demo",
        index_name="demo_ontologies",
        doc_id="class-1",
        operation="index",
        incoming_seq=10,
        skip_reason="stale_version_conflict",
        conflict_message="stale version conflict",
    )

    assert side_effects
    payload = side_effects[-1]
    assert payload["status"] == "success"
    assert payload["skip_reason"] == "stale_version_conflict"
    assert payload["extra_metadata"]["incoming_sequence"] == 10
    assert payload["extra_metadata"]["existing_sequence"] == 12
    assert payload["extra_metadata"]["conflict_confirmed"] is True


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_es_version_conflict_requires_verifiable_ordering() -> None:
    worker, side_effects = _build_worker(document={"event_sequence": 9})

    with pytest.raises(RuntimeError):
        await worker._handle_es_version_conflict(
            event_id="evt-2",
            event_data={"event_type": "OntologyClassUpdated", "sequence_number": 10},
            db_name="demo",
            index_name="demo_ontologies",
            doc_id="class-1",
            operation="index",
            incoming_seq=10,
            skip_reason="stale_version_conflict",
            conflict_message="stale version conflict",
        )

    assert side_effects
    payload = side_effects[-1]
    assert payload["status"] == "failure"
    assert payload["skip_reason"] == "version_conflict_unverified"
    assert payload["extra_metadata"]["conflict_confirmed"] is False


@pytest.mark.unit
@pytest.mark.asyncio
async def test_handle_es_version_conflict_accepts_unsequenced_duplicate_only_when_document_exists() -> None:
    worker, side_effects = _build_worker(document={"class_id": "class-1"})

    await worker._handle_es_version_conflict(
        event_id="evt-3",
        event_data={"event_type": "OntologyClassCreated"},
        db_name="demo",
        index_name="demo_ontologies",
        doc_id="class-1",
        operation="index",
        incoming_seq=None,
        skip_reason="es_create_conflict",
        conflict_message="duplicate create conflict",
    )

    assert side_effects[-1]["status"] == "success"
    assert side_effects[-1]["skip_reason"] == "es_create_conflict"
    assert side_effects[-1]["extra_metadata"]["conflict_confirmed"] is True
