from __future__ import annotations

import pytest

from objectify_worker.job_processing import (
    _build_objectify_completion_report,
    _build_objectify_write_path_contract,
)
from shared.services.core.write_path_contract import followup_completed, followup_degraded


@pytest.mark.unit
def test_objectify_write_path_contract_uses_postgres_registry_as_authoritative_store() -> None:
    contract = _build_objectify_write_path_contract(
        job_id="job-1",
        execution_mode="incremental",
        indexed_instances=12,
        write_path_report={"stale_prune": {"executed": False, "reason": "execution_mode_not_full"}},
        command_ids_sample=["cmd-1", "cmd-2"],
        instance_event_files_written=12,
        instance_event_file_failures=0,
        lineage_followup=followup_completed("lineage", details={"recorded_links": 12}),
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
        command_ids_sample=["cmd-1"],
        instance_event_files_written=2,
        instance_event_file_failures=1,
        lineage_followup=followup_degraded("lineage", error="lineage store unavailable"),
        watermark_followup=followup_degraded(
            "watermark_update",
            error="watermark store unavailable",
        ),
    )

    followups = {item["name"]: item for item in contract["derived_side_effects"]}
    assert followups["instance_event_files"]["status"] == "degraded"
    assert followups["lineage"]["status"] == "degraded"
    assert followups["watermark_update"]["status"] == "degraded"


@pytest.mark.unit
def test_objectify_completion_report_keeps_sample_count_consistent_with_reported_ids() -> None:
    report = _build_objectify_completion_report(
        total_rows=50,
        prepared_instances=50,
        warnings=[],
        errors=[],
        error_rows=[],
        command_ids_sample=[f"cmd-{idx}" for idx in range(40)],
        instance_ids_sample=[f"instance-{idx}" for idx in range(20)],
        indexed_instances=50,
        write_path_report={"stale_prune": {"executed": False}},
        write_path_contract={"authoritative_store": "postgres.objectify_registry"},
        ontology_version={"ref": "branch:main"},
        instance_event_files_written=50,
        instance_event_file_failures=2,
    )

    assert len(report["command_ids"]) == 25
    assert report["command_id_count"] == len(report["command_ids"])
    assert report["command_ids_truncated"] is True
    assert report["instance_event_files_written"] == 50
    assert report["instance_event_file_failures"] == 2
