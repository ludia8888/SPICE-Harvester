from __future__ import annotations

import os

from pipeline_worker.main import (
    _collect_input_commit_map,
    _collect_watermark_keys_from_snapshots,
    _inputs_diff_empty,
    _is_sensitive_conf_key,
    _max_watermark_from_snapshots,
    _resolve_code_version,
    _resolve_lakefs_repository,
    _resolve_output_format,
    _resolve_partition_columns,
    _resolve_watermark_column,
    _watermark_values_match,
)


def test_resolve_code_version_and_sensitive_keys(monkeypatch) -> None:
    monkeypatch.setenv("CODE_SHA", "abc")
    assert _resolve_code_version() == "abc"
    assert _is_sensitive_conf_key("aws_secret") is True
    assert _is_sensitive_conf_key("regular") is False


def test_resolve_lakefs_repository(monkeypatch) -> None:
    monkeypatch.setenv("LAKEFS_ARTIFACTS_REPOSITORY", "repo")
    assert _resolve_lakefs_repository() == "repo"


def test_watermark_snapshot_helpers() -> None:
    snapshots = [
        {"node_id": "n1", "lakefs_commit_id": "c1", "diff_requested": True, "diff_ok": True, "diff_empty": True},
        {"node_id": "n2", "lakefs_commit_id": "c2"},
    ]
    assert _collect_input_commit_map(snapshots) == {"n1": "c1", "n2": "c2"}
    assert _inputs_diff_empty(snapshots) is True

    wm = _max_watermark_from_snapshots(
        [
            {"watermark_column": "ts", "watermark_max": 1},
            {"watermark_column": "ts", "watermark_max": 2},
        ],
        watermark_column="ts",
    )
    assert wm == 2

    keys = _collect_watermark_keys_from_snapshots(
        [{"watermark_column": "ts", "watermark_max": 1, "watermark_keys": ["a"]}],
        watermark_column="ts",
        watermark_value=1,
    )
    assert keys == ["a"]
    assert _watermark_values_match("1", 1.0) is True


def test_resolve_output_format_and_partitions() -> None:
    definition = {"settings": {"outputFormat": "json"}}
    output_metadata = {}
    assert _resolve_output_format(definition=definition, output_metadata=output_metadata) == "json"

    partitions = _resolve_partition_columns(
        definition={"partitionBy": "a,b"},
        output_metadata={},
    )
    assert partitions == ["a", "b"]

    assert _resolve_watermark_column(incremental={"watermark": "ts"}, metadata={}) == "ts"
