from shared.utils.writeback_paths import (
    queue_compaction_marker_key,
    queue_entry_prefix,
    snapshot_latest_pointer_key,
    snapshot_manifest_key,
    snapshot_object_key,
)


def test_queue_entry_prefix_builds_expected_path():
    assert (
        queue_entry_prefix(object_type="Ticket", instance_id="t1", lifecycle_id="lc-1")
        == "writeback_edits_queue/queue/by_object/Ticket/t1/lc-1/"
    )


def test_snapshot_keys_match_design_layout():
    assert snapshot_manifest_key("snap-1") == "writeback_merged_snapshot/snapshots/snap-1/manifest.json"
    assert (
        snapshot_object_key(snapshot_id="snap-1", object_type="Ticket", instance_id="t1", lifecycle_id="lc-1")
        == "writeback_merged_snapshot/snapshots/snap-1/objects/Ticket/t1/lc-1.json"
    )
    assert snapshot_latest_pointer_key() == "writeback_merged_snapshot/snapshots/vlatest.json"
    assert queue_compaction_marker_key() == "writeback_edits_queue/compaction/compacted_until.json"

