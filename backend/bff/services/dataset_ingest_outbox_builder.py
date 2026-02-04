"""Dataset ingest outbox helpers (BFF).

Shared between multiple ingest workflows (uploads/manual versions) to avoid
duplicating event payload shapes and lineage edge construction.

This is a small Builder-style helper: callers compose the outbox list by
invoking intent-revealing methods rather than re-assembling dicts inline.
"""

from dataclasses import dataclass
from typing import Any, Optional

from shared.utils.s3_uri import parse_s3_uri
from shared.utils.time_utils import utcnow


@dataclass(frozen=True)
class DatasetIngestOutboxBuilder:
    build_dataset_event_payload: Any
    lineage_store: Any

    def dataset_created(
        self,
        *,
        dataset_id: str,
        db_name: str,
        name: str,
        actor: Optional[str],
        transaction_id: Optional[str],
        extra_data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        data = {"dataset_id": dataset_id, "db_name": db_name, "name": name, "transaction_id": transaction_id}
        if extra_data:
            data.update(extra_data)
        return {
            "kind": "eventstore",
            "payload": self.build_dataset_event_payload(
                event_id=dataset_id,
                event_type="DATASET_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset_id,
                command_type="CREATE_DATASET",
                actor=actor,
                data=data,
            ),
        }

    def version_created(
        self,
        *,
        event_id: str,
        dataset_id: str,
        db_name: str,
        name: str,
        actor: Optional[str],
        command_type: str,
        lakefs_commit_id: Optional[str],
        artifact_key: Optional[str],
        transaction_id: Optional[str],
        extra_data: Optional[dict[str, Any]] = None,
    ) -> dict[str, Any]:
        data = {
            "dataset_id": dataset_id,
            "db_name": db_name,
            "name": name,
            "lakefs_commit_id": lakefs_commit_id,
            "artifact_key": artifact_key,
            "transaction_id": transaction_id,
        }
        if extra_data:
            data.update(extra_data)
        return {
            "kind": "eventstore",
            "payload": self.build_dataset_event_payload(
                event_id=event_id,
                event_type="DATASET_VERSION_CREATED",
                aggregate_type="Dataset",
                aggregate_id=dataset_id,
                command_type=command_type,
                actor=actor,
                data=data,
            ),
        }

    def artifact_stored_lineage(
        self,
        *,
        version_event_id: str,
        artifact_key: Optional[str],
        db_name: str,
        from_label: str,
        edge_metadata: dict[str, Any],
        to_label: Optional[str] = None,
    ) -> Optional[dict[str, Any]]:
        if not self.lineage_store or not artifact_key:
            return None
        parsed = parse_s3_uri(artifact_key)
        if not parsed:
            return None
        bucket, key = parsed
        return {
            "kind": "lineage",
            "payload": {
                "from_node_id": self.lineage_store.node_event(str(version_event_id)),
                "to_node_id": self.lineage_store.node_artifact("s3", bucket, key),
                "edge_type": "dataset_artifact_stored",
                "occurred_at": utcnow(),
                "from_label": from_label,
                "to_label": to_label or artifact_key,
                "db_name": db_name,
                "edge_metadata": dict(edge_metadata or {}, bucket=bucket, key=key),
            },
        }
