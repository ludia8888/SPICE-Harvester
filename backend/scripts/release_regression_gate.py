from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable


SCRIPT_PATH = Path(__file__).resolve()
REPO_ROOT = SCRIPT_PATH.parents[2]
DEFAULT_MANIFEST_PATH = REPO_ROOT / "backend" / "release_regression_gate.json"
DEFAULT_REPORT_PATH = REPO_ROOT / "backend" / ".artifacts" / "release_regression_gate_report.json"


@dataclass(frozen=True)
class ReleaseGate:
    name: str
    make_target: str
    ci_job: str
    stage: str
    blocking: bool
    requires_stack: bool
    description: str


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_manifest(manifest_path: Path = DEFAULT_MANIFEST_PATH) -> list[ReleaseGate]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    schema = str(payload.get("schema") or "").strip()
    if schema != "release_regression_gate.v1":
        raise ValueError(f"Unsupported release gate manifest schema: {schema!r}")

    gates: list[ReleaseGate] = []
    for raw in payload.get("gates") or []:
        gates.append(
            ReleaseGate(
                name=str(raw["name"]),
                make_target=str(raw["make_target"]),
                ci_job=str(raw["ci_job"]),
                stage=str(raw["stage"]),
                blocking=bool(raw.get("blocking", True)),
                requires_stack=bool(raw.get("requires_stack", False)),
                description=str(raw.get("description") or "").strip(),
            )
        )
    if not gates:
        raise ValueError("Release gate manifest has no gates")
    return gates


def _filter_gates(gates: Iterable[ReleaseGate], selected_targets: set[str] | None) -> list[ReleaseGate]:
    if not selected_targets:
        return list(gates)
    return [gate for gate in gates if gate.make_target in selected_targets or gate.name in selected_targets]


def execute_release_gate(
    *,
    gates: list[ReleaseGate],
    repo_root: Path = REPO_ROOT,
    stop_on_failure: bool = True,
) -> dict[str, Any]:
    started_at = _utc_now()
    records: list[dict[str, Any]] = []
    overall_success = True

    for gate in gates:
        command = ["make", gate.make_target]
        started = time.monotonic()
        result = subprocess.run(command, cwd=repo_root, check=False)
        duration_seconds = round(time.monotonic() - started, 3)
        success = result.returncode == 0
        records.append(
            {
                "name": gate.name,
                "make_target": gate.make_target,
                "ci_job": gate.ci_job,
                "stage": gate.stage,
                "blocking": gate.blocking,
                "requires_stack": gate.requires_stack,
                "description": gate.description,
                "success": success,
                "return_code": result.returncode,
                "duration_seconds": duration_seconds,
            }
        )
        if gate.blocking and not success:
            overall_success = False
            if stop_on_failure:
                break

    finished_at = _utc_now()
    return {
        "schema": "release_regression_gate_report.v1",
        "started_at": started_at,
        "finished_at": finished_at,
        "repo_root": str(repo_root),
        "success": overall_success,
        "executed_gates": records,
    }


def evaluate_ci_results(*, gates: list[ReleaseGate], job_results: dict[str, str]) -> dict[str, Any]:
    records: list[dict[str, Any]] = []
    overall_success = True
    for gate in gates:
        result = str(job_results.get(gate.ci_job) or "missing").strip().lower() or "missing"
        success = result == "success"
        records.append(
            {
                "name": gate.name,
                "ci_job": gate.ci_job,
                "make_target": gate.make_target,
                "stage": gate.stage,
                "blocking": gate.blocking,
                "requires_stack": gate.requires_stack,
                "result": result,
                "success": success,
                "description": gate.description,
            }
        )
        if gate.blocking and not success:
            overall_success = False

    return {
        "schema": "release_regression_gate_ci.v1",
        "evaluated_at": _utc_now(),
        "success": overall_success,
        "gates": records,
    }


def _write_report(report: dict[str, Any], report_path: Path) -> None:
    report_path.parent.mkdir(parents=True, exist_ok=True)
    report_path.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _print_summary(report: dict[str, Any]) -> None:
    if report.get("schema") == "release_regression_gate_report.v1":
        print(f"Release gate success: {report['success']}")
        for item in report.get("executed_gates") or []:
            mark = "PASS" if item.get("success") else "FAIL"
            print(f"- [{mark}] {item['make_target']} ({item['stage']}, {item['duration_seconds']}s)")
        return
    if report.get("schema") == "release_regression_gate_ci.v1":
        print(f"Release gate CI success: {report['success']}")
        for item in report.get("gates") or []:
            mark = "PASS" if item.get("success") else "FAIL"
            print(f"- [{mark}] {item['ci_job']} -> {item['result']}")
        return
    print(json.dumps(report, indent=2, sort_keys=True))


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run or evaluate the backend release regression gate.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST_PATH, help="Path to release gate manifest JSON")
    parser.add_argument("--report-path", type=Path, default=DEFAULT_REPORT_PATH, help="Where to write execution/evaluation report JSON")
    parser.add_argument("--execute", action="store_true", help="Execute make targets listed in the manifest")
    parser.add_argument("--ci-results-json", type=Path, help="Evaluate CI job results from a JSON mapping of ci_job -> result")
    parser.add_argument("--target", action="append", dest="targets", help="Limit execution/evaluation to specific gate names or make targets")
    parser.add_argument("--no-stop-on-failure", action="store_true", help="Continue executing remaining targets after a blocking failure")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    gates = load_manifest(args.manifest)
    selected = set(args.targets or [])
    filtered_gates = _filter_gates(gates, selected_targets=selected if selected else None)

    if args.execute:
        report = execute_release_gate(
            gates=filtered_gates,
            stop_on_failure=not args.no_stop_on_failure,
        )
    elif args.ci_results_json:
        job_results = json.loads(args.ci_results_json.read_text(encoding="utf-8"))
        if not isinstance(job_results, dict):
            raise ValueError("CI results JSON must be a mapping of ci_job -> result")
        report = evaluate_ci_results(gates=filtered_gates, job_results={str(k): str(v) for k, v in job_results.items()})
    else:
        report = {
            "schema": "release_regression_gate_manifest.v1",
            "validated_at": _utc_now(),
            "success": True,
            "gates": [gate.__dict__ for gate in filtered_gates],
        }

    _write_report(report, args.report_path)
    _print_summary(report)
    return 0 if report.get("success") else 1


if __name__ == "__main__":
    raise SystemExit(main())
