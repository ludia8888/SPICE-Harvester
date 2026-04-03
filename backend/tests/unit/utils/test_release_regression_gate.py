from __future__ import annotations

from types import SimpleNamespace

import pytest

from scripts import release_regression_gate as gate


@pytest.mark.unit
def test_load_manifest_exposes_expected_blocking_targets() -> None:
    gates = gate.load_manifest()

    assert [item.make_target for item in gates] == [
        "backend-runtime-ddl-audit",
        "backend-error-taxonomy",
        "backend-unit",
        "backend-boundary-smoke",
        "backend-release-smoke",
        "backend-infra-tier",
    ]
    assert all(item.blocking for item in gates)


@pytest.mark.unit
def test_evaluate_ci_results_fails_when_required_job_missing() -> None:
    gates = gate.load_manifest()

    report = gate.evaluate_ci_results(
        gates=gates,
        job_results={
            "backend-runtime-ddl-audit": "success",
            "backend-error-taxonomy": "success",
            "backend-unit": "success",
            "backend-boundary-smoke": "success",
            "backend-release-smoke": "success",
        },
    )

    assert report["success"] is False
    failing = {item["ci_job"]: item["result"] for item in report["gates"] if not item["success"]}
    assert failing == {"backend-infra-tier": "missing"}


@pytest.mark.unit
def test_execute_release_gate_records_failure_and_stops(monkeypatch: pytest.MonkeyPatch) -> None:
    gates = gate.load_manifest()[:3]
    calls: list[str] = []

    def _fake_run(command, cwd, check):  # noqa: ANN001
        calls.append(command[-1])
        return SimpleNamespace(returncode=0 if command[-1] != "backend-unit" else 2)

    monkeypatch.setattr(gate.subprocess, "run", _fake_run)

    report = gate.execute_release_gate(gates=gates, stop_on_failure=True)

    assert report["success"] is False
    assert calls == ["backend-runtime-ddl-audit", "backend-error-taxonomy", "backend-unit"]
    assert report["executed_gates"][-1]["make_target"] == "backend-unit"
    assert report["executed_gates"][-1]["success"] is False
