from __future__ import annotations

from shared.foundry.payload_ids import extract_build_rid, extract_pipeline_id, extract_pipeline_job_id


def test_extract_pipeline_job_id_prefers_explicit_job_id() -> None:
    payload = {
        "job_id": "job-root",
        "data": {"jobId": "job-data"},
        "result": {"rid": "ri.foundry.main.build.job-result"},
    }

    assert extract_pipeline_job_id(payload) == "job-root"


def test_extract_pipeline_job_id_reads_build_refs_across_nested_payloads() -> None:
    assert extract_pipeline_job_id({"rid": "build://job-a"}) == "job-a"
    assert extract_pipeline_job_id({"data": {"buildRid": "ri.foundry.main.build.job-b"}}) == "job-b"
    assert extract_pipeline_job_id({"result": {"rid": "ri.foundry.main.build.job-c"}}) == "job-c"


def test_extract_pipeline_id_and_build_rid_support_data_and_result_shapes() -> None:
    payload = {
        "data": {"pipelineId": "pipe-1"},
        "result": {"rid": "build://job-1"},
    }

    assert extract_pipeline_id(payload) == "pipe-1"
    assert extract_build_rid(payload) == "ri.foundry.main.build.job-1"
