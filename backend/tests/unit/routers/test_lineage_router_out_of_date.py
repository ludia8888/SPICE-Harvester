from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List

import pytest

from bff.routers.lineage import (
    _artifact_latest_writer_state,
    _chunked,
    _edge_cause_payload,
    _enrich_artifacts_with_latest_writer,
    _event_node_id_from_latest_writer,
    _extract_impacted_artifacts_from_edges,
    _freshness_status,
    _get_latest_edges_for_projections_batched,
    _get_latest_edges_from_batched,
    _get_latest_edges_to_batched,
    _normalize_window,
    _out_of_date_scope,
    _projection_name_from_latest_writer,
    _status_rank,
    _staleness_reason_with_scope,
    _summarize_run_rows,
    _upstream_gap_minutes,
    _update_type,
    _writer_code_sha,
)


def test_freshness_status_healthy_warning_critical() -> None:
    as_of = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

    healthy, healthy_age = _freshness_status(
        last_occurred_at=as_of - timedelta(minutes=30),
        as_of=as_of,
        freshness_slo_minutes=60,
    )
    warning, warning_age = _freshness_status(
        last_occurred_at=as_of - timedelta(minutes=100),
        as_of=as_of,
        freshness_slo_minutes=60,
    )
    critical, critical_age = _freshness_status(
        last_occurred_at=as_of - timedelta(minutes=250),
        as_of=as_of,
        freshness_slo_minutes=60,
    )

    assert healthy == "healthy"
    assert healthy_age == 30.0
    assert warning == "warning"
    assert warning_age == 100.0
    assert critical == "critical"
    assert critical_age == 250.0


def test_freshness_status_no_data() -> None:
    as_of = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)

    status, age = _freshness_status(last_occurred_at=None, as_of=as_of, freshness_slo_minutes=60)

    assert status == "no_data"
    assert age is None


def test_status_rank_orders_for_triage() -> None:
    ranked = sorted(["healthy", "warning", "critical", "no_data"], key=_status_rank)

    assert ranked == ["critical", "warning", "no_data", "healthy"]


def test_normalize_window_defaults_and_order() -> None:
    since_ts, until_ts = _normalize_window(since=None, until=None, default_hours=2)

    assert since_ts < until_ts
    assert round((until_ts - since_ts).total_seconds() / 3600.0) == 2


def test_extract_impacted_artifacts_from_edges_keeps_latest_per_artifact() -> None:
    edges = [
        {
            "to_node_id": "artifact:es:demo:main:Order/o1",
            "occurred_at": "2026-02-15T10:00:00+00:00",
        },
        {
            "to_node_id": "artifact:es:demo:main:Order/o1",
            "occurred_at": "2026-02-15T10:10:00+00:00",
        },
        {
            "to_node_id": "artifact:graph:demo:main:Order/o1",
            "occurred_at": "2026-02-15T09:00:00+00:00",
        },
        {"to_node_id": "agg:Order:o1", "occurred_at": "2026-02-15T09:00:00+00:00"},
    ]

    impacted, total = _extract_impacted_artifacts_from_edges(edges=edges, limit=10)

    assert total == 2
    assert len(impacted) == 2
    assert impacted[0]["node_id"] == "artifact:es:demo:main:Order/o1"
    assert impacted[0]["last_occurred_at"] == "2026-02-15T10:10:00+00:00"


def test_summarize_run_rows_assigns_status_counts() -> None:
    as_of = datetime(2026, 2, 15, 12, 0, 0, tzinfo=timezone.utc)
    runs, counts = _summarize_run_rows(
        run_rows=[
            {
                "run_id": "run-healthy",
                "first_occurred_at": as_of - timedelta(minutes=10),
                "last_occurred_at": as_of - timedelta(minutes=5),
                "edge_count": 10,
                "impacted_artifact_count": 3,
                "impacted_projection_count": 1,
            },
            {
                "run_id": "run-critical",
                "first_occurred_at": as_of - timedelta(hours=10),
                "last_occurred_at": as_of - timedelta(hours=8),
                "edge_count": 2,
                "impacted_artifact_count": 1,
                "impacted_projection_count": 1,
            },
        ],
        as_of=as_of,
        freshness_slo_minutes=60,
    )

    assert [run["run_id"] for run in runs] == ["run-critical", "run-healthy"]
    assert counts == {"healthy": 1, "warning": 0, "critical": 1, "no_data": 0}


def test_edge_cause_payload_extracts_latest_writer_context() -> None:
    payload = _edge_cause_payload(
        {
            "event_id": "evt-1",
            "from_node_id": "event:evt-1",
            "edge_type": "event_materialized_es_document",
            "projection_name": "orders_projection",
            "occurred_at": "2026-02-15T12:00:00+00:00",
            "run_id": None,
            "metadata": {
                "producer_run_id": "run-42",
                "producer_service": "projection-worker",
                "producer_code_sha": "abc123",
            },
        }
    )

    assert payload is not None
    assert payload["event_id"] == "evt-1"
    assert payload["run_id"] == "run-42"
    assert payload["producer_service"] == "projection-worker"
    assert payload["producer_code_sha"] == "abc123"


def test_edge_cause_payload_returns_none_for_invalid_input() -> None:
    assert _edge_cause_payload(None) is None
    assert _edge_cause_payload({}) is not None


def test_upstream_gap_minutes() -> None:
    last_occurred_at = datetime(2026, 2, 15, 10, 0, 0, tzinfo=timezone.utc)
    upstream_newer = datetime(2026, 2, 15, 10, 30, 0, tzinfo=timezone.utc)
    upstream_older = datetime(2026, 2, 15, 9, 30, 0, tzinfo=timezone.utc)

    assert _upstream_gap_minutes(last_occurred_at=last_occurred_at, latest_upstream_occurred_at=upstream_newer) == 30.0
    assert _upstream_gap_minutes(last_occurred_at=last_occurred_at, latest_upstream_occurred_at=upstream_older) == 0.0
    assert _upstream_gap_minutes(last_occurred_at=None, latest_upstream_occurred_at=upstream_newer) is None


def test_chunked_helper() -> None:
    chunks = list(_chunked(["a", "b", "c", "d", "e"], 2))
    assert chunks == [["a", "b"], ["c", "d"], ["e"]]


class _FakeLineageStoreForBatch:
    def __init__(self) -> None:
        self.to_calls: List[List[str]] = []
        self.from_calls: List[List[str]] = []
        self.projection_calls: List[List[str]] = []

    async def get_latest_edges_to(self, *, to_node_ids: List[str], **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        self.to_calls.append(list(to_node_ids))
        return {node_id: {"to_node_id": node_id} for node_id in to_node_ids}

    async def get_latest_edges_from(self, *, from_node_ids: List[str], **kwargs: Any) -> Dict[str, Dict[str, Any]]:
        self.from_calls.append(list(from_node_ids))
        return {node_id: {"from_node_id": node_id} for node_id in from_node_ids}

    async def get_latest_edges_for_projections(
        self,
        *,
        projection_names: List[str],
        **kwargs: Any,
    ) -> Dict[str, Dict[str, Any]]:
        self.projection_calls.append(list(projection_names))
        return {name: {"projection_name": name} for name in projection_names}


@pytest.mark.asyncio
async def test_batched_latest_edges_helpers_chunk_large_inputs(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bff.routers.lineage._lineage_lookup_batch_size", lambda: 2)
    store = _FakeLineageStoreForBatch()

    to_result = await _get_latest_edges_to_batched(
        lineage_store=store,
        to_node_ids=["n1", "n2", "n3", "n4", "n5"],
        db_name="demo",
        branch="main",
    )
    from_result = await _get_latest_edges_from_batched(
        lineage_store=store,
        from_node_ids=["a1", "a2", "a3"],
        edge_type="aggregate_emitted_event",
        db_name="demo",
        branch="main",
    )
    projection_result = await _get_latest_edges_for_projections_batched(
        lineage_store=store,
        projection_names=["p1", "p2", "p3", "p4"],
        db_name="demo",
        branch="main",
    )

    assert sorted(to_result.keys()) == ["n1", "n2", "n3", "n4", "n5"]
    assert sorted(from_result.keys()) == ["a1", "a2", "a3"]
    assert sorted(projection_result.keys()) == ["p1", "p2", "p3", "p4"]
    assert store.to_calls == [["n1", "n2"], ["n3", "n4"], ["n5"]]
    assert store.from_calls == [["a1", "a2"], ["a3"]]
    assert store.projection_calls == [["p1", "p2"], ["p3", "p4"]]


@pytest.mark.asyncio
async def test_enrich_artifacts_with_latest_writer(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("bff.routers.lineage._lineage_lookup_batch_size", lambda: 2)
    store = _FakeLineageStoreForBatch()
    artifacts = [
        {"node_id": "artifact:es:demo:main:Order/o1"},
        {"node_id": "artifact:es:demo:main:Order/o2"},
    ]

    with_cause_count = await _enrich_artifacts_with_latest_writer(
        lineage_store=store,
        artifacts=artifacts,
        db_name="demo",
        branch="main",
    )

    assert with_cause_count == 2
    assert isinstance(artifacts[0].get("latest_writer"), dict)
    assert store.to_calls == [["artifact:es:demo:main:Order/o1", "artifact:es:demo:main:Order/o2"]]


def test_artifact_latest_writer_state() -> None:
    assert _artifact_latest_writer_state(run_id="run-1", latest_writer={"run_id": "run-1"}) == "current"
    assert _artifact_latest_writer_state(run_id="run-1", latest_writer={"run_id": "run-2"}) == "superseded"
    assert _artifact_latest_writer_state(run_id="run-1", latest_writer={"run_id": ""}) == "unknown"
    assert _artifact_latest_writer_state(run_id="run-1", latest_writer=None) == "unknown"


def test_event_node_id_from_latest_writer() -> None:
    assert _event_node_id_from_latest_writer({"from_node_id": "event:evt-1"}) == "event:evt-1"
    assert _event_node_id_from_latest_writer({"event_id": "evt-2"}) == "event:evt-2"
    assert _event_node_id_from_latest_writer({"from_node_id": "agg:Order:o1"}) is None
    assert _event_node_id_from_latest_writer(None) is None


def test_projection_and_writer_fields_from_latest_writer() -> None:
    latest_writer = {"projection_name": "orders_projection", "producer_code_sha": "sha-1"}

    assert _projection_name_from_latest_writer(latest_writer) == "orders_projection"
    assert _writer_code_sha(latest_writer) == "sha-1"
    assert _projection_name_from_latest_writer({"projection_name": "   "}) is None
    assert _writer_code_sha({"producer_code_sha": "   "}) is None
    assert _projection_name_from_latest_writer(None) is None
    assert _writer_code_sha(None) is None


def test_out_of_date_scope_resolution() -> None:
    last_occurred_at = datetime(2026, 2, 15, 10, 0, 0, tzinfo=timezone.utc)
    parent_newer = datetime(2026, 2, 15, 10, 10, 0, tzinfo=timezone.utc)
    upstream_newer = datetime(2026, 2, 15, 10, 30, 0, tzinfo=timezone.utc)

    assert (
        _out_of_date_scope(
            status_name="healthy",
            last_occurred_at=last_occurred_at,
            parent_latest_occurred_at=parent_newer,
            latest_upstream_occurred_at=upstream_newer,
        )
        == "none"
    )
    assert (
        _out_of_date_scope(
            status_name="critical",
            last_occurred_at=last_occurred_at,
            parent_latest_occurred_at=parent_newer,
            latest_upstream_occurred_at=upstream_newer,
        )
        == "parent"
    )
    assert (
        _out_of_date_scope(
            status_name="warning",
            last_occurred_at=last_occurred_at,
            parent_latest_occurred_at=last_occurred_at,
            latest_upstream_occurred_at=upstream_newer,
        )
        == "ancestor"
    )
    assert (
        _out_of_date_scope(
            status_name="warning",
            last_occurred_at=last_occurred_at,
            parent_latest_occurred_at=last_occurred_at,
            latest_upstream_occurred_at=last_occurred_at,
        )
        == "none"
    )


def test_staleness_reason_with_scope_resolution() -> None:
    assert _staleness_reason_with_scope(status_name="healthy", out_of_date_scope="ancestor") == "healthy"
    assert _staleness_reason_with_scope(status_name="no_data", out_of_date_scope="parent") == "no_lineage_data"
    assert _staleness_reason_with_scope(status_name="warning", out_of_date_scope="parent") == "parent_has_newer_events"
    assert _staleness_reason_with_scope(status_name="critical", out_of_date_scope="ancestor") == "ancestor_has_newer_events"
    assert _staleness_reason_with_scope(status_name="warning", out_of_date_scope="none") == "freshness_slo_breached"


def test_update_type_resolution() -> None:
    last_occurred_at = datetime(2026, 2, 15, 10, 0, 0, tzinfo=timezone.utc)
    latest_projection_at = datetime(2026, 2, 15, 10, 15, 0, tzinfo=timezone.utc)

    assert (
        _update_type(
            status_name="healthy",
            out_of_date_scope="ancestor",
            last_occurred_at=last_occurred_at,
            latest_projection_occurred_at=latest_projection_at,
            current_code_sha="sha-a",
            latest_projection_code_sha="sha-b",
        )
        == "none"
    )
    assert (
        _update_type(
            status_name="warning",
            out_of_date_scope="ancestor",
            last_occurred_at=last_occurred_at,
            latest_projection_occurred_at=latest_projection_at,
            current_code_sha="sha-a",
            latest_projection_code_sha="sha-b",
        )
        == "logic"
    )
    assert (
        _update_type(
            status_name="warning",
            out_of_date_scope="ancestor",
            last_occurred_at=last_occurred_at,
            latest_projection_occurred_at=latest_projection_at,
            current_code_sha="sha-a",
            latest_projection_code_sha="sha-a",
        )
        == "data"
    )
    assert (
        _update_type(
            status_name="warning",
            out_of_date_scope="none",
            last_occurred_at=last_occurred_at,
            latest_projection_occurred_at=last_occurred_at,
            current_code_sha="sha-a",
            latest_projection_code_sha="sha-a",
        )
        == "none"
    )
