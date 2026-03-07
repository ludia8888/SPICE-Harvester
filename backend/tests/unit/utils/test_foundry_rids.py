from __future__ import annotations

from shared.foundry.rids import build_rid, extract_build_job_id, rid_id_for_kind


def test_extract_build_job_id_accepts_foundry_rid_and_build_uri() -> None:
    assert extract_build_job_id(build_rid("build", "build-job-123")) == "build-job-123"
    assert extract_build_job_id("build://build-job-456") == "build-job-456"


def test_rid_id_for_kind_returns_none_for_wrong_kind_or_invalid_value() -> None:
    assert rid_id_for_kind(build_rid("pipeline", "pipe-1"), "build") is None
    assert rid_id_for_kind("not-a-rid", "build") is None
