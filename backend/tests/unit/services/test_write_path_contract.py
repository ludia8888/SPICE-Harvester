from __future__ import annotations

import pytest

from shared.services.core.write_path_contract import (
    WritePathContractError,
    build_followup_status,
    build_write_path_contract,
    followup_completed,
)


@pytest.mark.unit
def test_build_write_path_contract_exposes_authoritative_store_and_recovery_path() -> None:
    contract = build_write_path_contract(
        authoritative_write="pipeline_create",
        followups=[followup_completed("pipeline_audit")],
    )

    assert contract["authoritative_state"] == "committed"
    assert contract["authoritative_store"] == "postgres.pipeline_registry"
    assert contract["authoritative_write"] == "pipeline_create"
    assert contract["recovery_path"]["strategy"] == "reconcile"
    assert contract["recovery_path"]["owner"] == "bff.pipeline_catalog"


@pytest.mark.unit
def test_build_write_path_contract_rejects_derived_only_authoritative_store() -> None:
    with pytest.raises(WritePathContractError, match="elasticsearch is derived-only"):
        build_write_path_contract(
            authoritative_write="pipeline_create",
            authoritative_store="elasticsearch",
            followups=[followup_completed("pipeline_audit")],
        )


@pytest.mark.unit
def test_build_write_path_contract_rejects_unknown_authoritative_write() -> None:
    with pytest.raises(WritePathContractError, match="not registered"):
        build_write_path_contract(
            authoritative_write="custom_write_that_is_not_registered",
            authoritative_store="postgres.pipeline_registry",
            recovery_path={"strategy": "reconcile", "owner": "unit", "reference": "x"},
        )


@pytest.mark.unit
def test_build_followup_status_rejects_invalid_status() -> None:
    with pytest.raises(WritePathContractError, match="derived_side_effects\\[\\]\\.status"):
        build_followup_status("pipeline_audit", status="healthy")
