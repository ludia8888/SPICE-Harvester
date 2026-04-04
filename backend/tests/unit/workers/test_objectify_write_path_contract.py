from __future__ import annotations

import pytest

from objectify_worker.job_processing import _build_objectify_write_path_contract
from shared.services.core.write_path_contract import followup_completed, followup_degraded


@pytest.mark.unit
def test_objectify_write_path_contract_uses_postgres_registry_as_authoritative_store() -> None:
    contract = _build_objectify_write_path_contract(
        job_id="job-1",
        execution_mode="incremental",
        indexed_instances=12,
        write_path_report={"stale_prune": {"executed": False, "reason": "execution_mode_not_full"}},
        command_ids=["cmd-1", "cmd-2"],
        instance_event_files_written=12,
        instance_event_file_failures=0,
        lineage_limit=100,
        lineage_remaining=88,
        lineage_enabled=True,
        watermark_followup=followup_completed(
            "watermark_update",
            details={"new_watermark": "2026-04-04T00:00:00Z"},
        ),
    )

    assert contract["authoritative_store"] == "postgres.objectify_registry"
    assert contract["authoritative_write"] == "objectify_job_commit"
    assert contract["recovery_path"]["reference"] == "objectify_job:job-1"
    followups = {item["name"]: item for item in contract["derived_side_effects"]}
    assert followups["elasticsearch_index"]["status"] == "completed"
    assert followups["instance_event_files"]["status"] == "completed"
    assert followups["lineage"]["status"] == "completed"
    assert followups["watermark_update"]["status"] == "completed"


@pytest.mark.unit
def test_objectify_write_path_contract_marks_degraded_instance_event_files() -> None:
    contract = _build_objectify_write_path_contract(
        job_id="job-2",
        execution_mode="full",
        indexed_instances=3,
        write_path_report={},
        command_ids=["cmd-1"],
        instance_event_files_written=2,
        instance_event_file_failures=1,
        lineage_limit=0,
        lineage_remaining=0,
        lineage_enabled=False,
        watermark_followup=followup_degraded(
            "watermark_update",
            error="watermark store unavailable",
        ),
    )

    followups = {item["name"]: item for item in contract["derived_side_effects"]}
    assert followups["instance_event_files"]["status"] == "degraded"
    assert followups["lineage"]["status"] == "skipped"
    assert followups["watermark_update"]["status"] == "degraded"
