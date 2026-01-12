from __future__ import annotations


def writeback_patchset_key(action_log_id: str) -> str:
    return f"writeback_patchsets/actions/{action_log_id}/patchset.json"


def writeback_patchset_metadata_key(action_log_id: str) -> str:
    return f"writeback_patchsets/actions/{action_log_id}/metadata.json"


def snapshot_manifest_key(snapshot_id: str) -> str:
    sid = (snapshot_id or "").strip().strip("/")
    return f"writeback_merged_snapshot/snapshots/{sid}/manifest.json"


def snapshot_object_key(
    *,
    snapshot_id: str,
    object_type: str,
    instance_id: str,
    lifecycle_id: str,
) -> str:
    sid = (snapshot_id or "").strip().strip("/")
    ot = (object_type or "").strip().strip("/")
    iid = (instance_id or "").strip().strip("/")
    lc = (lifecycle_id or "").strip().strip("/")
    return f"writeback_merged_snapshot/snapshots/{sid}/objects/{ot}/{iid}/{lc}.json"


def snapshot_latest_pointer_key() -> str:
    return "writeback_merged_snapshot/snapshots/vlatest.json"


def queue_compaction_marker_key() -> str:
    return "writeback_edits_queue/compaction/compacted_until.json"


def queue_entry_key(
    *,
    object_type: str,
    instance_id: str,
    lifecycle_id: str,
    action_applied_seq: int,
    action_log_id: str,
) -> str:
    return (
        "writeback_edits_queue/queue/by_object/"
        f"{object_type}/{instance_id}/{lifecycle_id}/{action_applied_seq}_{action_log_id}.json"
    )


def queue_entry_prefix(
    *,
    object_type: str,
    instance_id: str,
    lifecycle_id: str,
) -> str:
    object_type_norm = (object_type or "").strip().strip("/")
    instance_id_norm = (instance_id or "").strip().strip("/")
    lifecycle_norm = (lifecycle_id or "").strip().strip("/")
    return (
        "writeback_edits_queue/queue/by_object/"
        f"{object_type_norm}/{instance_id_norm}/{lifecycle_norm}/"
    )


def ref_key(ref: str, key: str) -> str:
    ref_norm = (ref or "").strip().strip("/")
    key_norm = (key or "").strip().lstrip("/")
    if not ref_norm:
        return key_norm
    return f"{ref_norm}/{key_norm}"
