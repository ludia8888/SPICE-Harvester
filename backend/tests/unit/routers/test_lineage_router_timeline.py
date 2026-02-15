from __future__ import annotations

from datetime import datetime, timezone

from bff.routers.lineage import _bucket_start, _build_timeline_summary, _compact_edge_metadata


def test_bucket_start_floors_to_interval() -> None:
    ts = datetime(2026, 2, 15, 10, 17, 30, tzinfo=timezone.utc)

    bucket = _bucket_start(ts, bucket_minutes=15)

    assert bucket == datetime(2026, 2, 15, 10, 15, 0, tzinfo=timezone.utc)


def test_build_timeline_summary_counts_and_spikes() -> None:
    edges = [
        {
            "edge_type": "aggregate_emitted_event",
            "projection_name": "orders_projection",
            "occurred_at": datetime(2026, 2, 15, 10, 0, 0, tzinfo=timezone.utc),
            "metadata": {"producer_service": "action-worker"},
        },
        {
            "edge_type": "aggregate_emitted_event",
            "projection_name": "orders_projection",
            "occurred_at": datetime(2026, 2, 15, 10, 15, 0, tzinfo=timezone.utc),
            "metadata": {"producer_service": "action-worker"},
        },
    ]
    for idx in range(10):
        edges.append(
            {
                "edge_type": "event_materialized_es_document",
                "projection_name": "orders_projection",
                "occurred_at": datetime(2026, 2, 15, 10, 30, idx, tzinfo=timezone.utc),
                "metadata": {"producer_service": "projection-worker"},
            }
        )

    summary = _build_timeline_summary(edges=edges, bucket_minutes=15)

    assert summary["total_edges"] == 12
    assert summary["edge_type_counts"]["event_materialized_es_document"] == 10
    assert summary["edge_type_counts"]["aggregate_emitted_event"] == 2
    assert summary["projection_counts"]["orders_projection"] == 12
    assert summary["producer_service_counts"]["projection-worker"] == 10
    assert len(summary["buckets"]) == 3
    assert len(summary["spikes"]) == 1
    assert summary["spikes"][0]["bucket_start"] == datetime(2026, 2, 15, 10, 30, 0, tzinfo=timezone.utc).isoformat()


def test_compact_edge_metadata_whitelists_keys() -> None:
    metadata = {
        "db_name": "demo",
        "branch": "main",
        "producer_service": "projection-worker",
        "unexpected": "drop-me",
    }

    compact = _compact_edge_metadata(metadata)

    assert compact == {
        "db_name": "demo",
        "branch": "main",
        "producer_service": "projection-worker",
    }
