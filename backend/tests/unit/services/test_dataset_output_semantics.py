from __future__ import annotations

import pytest

from shared.services.pipeline.dataset_output_semantics import (
    DatasetWriteMode,
    normalize_dataset_output_metadata,
    resolve_dataset_write_policy,
    validate_dataset_output_metadata,
)


@pytest.mark.unit
def test_normalize_dataset_output_metadata_legacy_aliases() -> None:
    normalized = normalize_dataset_output_metadata(
        definition={"settings": {"outputFormat": "json"}},
        output_metadata={
            "pkSemantics": "append_state",
            "pkColumns": ["id"],
            "removeColumn": "is_deleted",
            "partitionBy": "ds,region",
        },
    )
    assert normalized.metadata["write_mode"] == "append_only_new_rows"
    assert normalized.metadata["primary_key_columns"] == ["id"]
    assert normalized.metadata["post_filtering_column"] == "is_deleted"
    assert normalized.metadata["output_format"] == "json"
    assert normalized.metadata["partition_by"] == ["ds", "region"]
    assert normalized.warnings


@pytest.mark.unit
def test_resolve_dataset_write_policy_default_snapshot() -> None:
    policy = resolve_dataset_write_policy(
        definition={},
        output_metadata={},
        execution_semantics="snapshot",
    )
    assert policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE
    assert policy.runtime_write_mode == "overwrite"


@pytest.mark.unit
def test_resolve_dataset_write_policy_default_incremental_with_pk() -> None:
    policy = resolve_dataset_write_policy(
        definition={},
        output_metadata={"primary_key_columns": ["id"]},
        execution_semantics="incremental",
    )
    assert policy.resolved_write_mode == DatasetWriteMode.APPEND_ONLY_NEW_ROWS
    assert policy.runtime_write_mode == "append"


@pytest.mark.unit
def test_resolve_dataset_write_policy_default_incremental_without_pk() -> None:
    policy = resolve_dataset_write_policy(
        definition={},
        output_metadata={},
        execution_semantics="incremental",
    )
    assert policy.resolved_write_mode == DatasetWriteMode.ALWAYS_APPEND
    assert any("always_append" in warning for warning in policy.warnings)


@pytest.mark.unit
def test_validate_dataset_output_metadata_requires_pk_and_post_filtering() -> None:
    errors = validate_dataset_output_metadata(
        definition={},
        output_metadata={"write_mode": "snapshot_replace_and_remove", "primary_key_columns": []},
        execution_semantics="snapshot",
    )
    assert "write_mode=snapshot_replace_and_remove requires primary_key_columns" in errors
    assert "write_mode=snapshot_replace_and_remove requires post_filtering_column" in errors


@pytest.mark.unit
def test_validate_dataset_output_metadata_checks_available_columns() -> None:
    errors = validate_dataset_output_metadata(
        definition={},
        output_metadata={
            "write_mode": "append_only_new_rows",
            "primary_key_columns": ["id", "tenant_id"],
            "partition_by": ["ds"],
            "output_format": "parquet",
        },
        execution_semantics="incremental",
        available_columns={"id", "ds"},
    )
    assert "primary_key_columns missing in output schema: tenant_id" in errors


@pytest.mark.unit
def test_dataset_write_policy_hash_stable() -> None:
    policy_a = resolve_dataset_write_policy(
        definition={},
        output_metadata={
            "write_mode": "append_only_new_rows",
            "primary_key_columns": ["id"],
            "output_format": "orc",
        },
        execution_semantics="incremental",
    )
    policy_b = resolve_dataset_write_policy(
        definition={},
        output_metadata={
            "primary_key_columns": ["id"],
            "output_format": "orc",
            "write_mode": "append_only_new_rows",
        },
        execution_semantics="incremental",
    )
    assert policy_a.policy_hash == policy_b.policy_hash
