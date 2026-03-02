#!/usr/bin/env python3
"""CLI for OpenAPI duplicate/legacy audit."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any

try:
    from .analysis import attach_phase2_results, analyze_inventory, merge_phase1_results, run_runtime_validation
    from .inventory import collect_inventory, parse_services, repo_root
    from .report import render_markdown, write_json, write_markdown
except ImportError:  # pragma: no cover
    from analysis import attach_phase2_results, analyze_inventory, merge_phase1_results, run_runtime_validation  # type: ignore
    from inventory import collect_inventory, parse_services, repo_root  # type: ignore
    from report import render_markdown, write_json, write_markdown  # type: ignore


def _default_output_paths() -> tuple[Path, Path, Path]:
    base = repo_root() / "docs" / "reference" / "_generated"
    return (
        base / "API_DUPLICATE_LEGACY_AUDIT.md",
        base / "API_DUPLICATE_LEGACY_INVENTORY.json",
        base / "API_DUPLICATE_LEGACY_THRESHOLDS.json",
    )


def parse_args() -> argparse.Namespace:
    default_md, default_json, default_thresholds = _default_output_paths()
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "-m",
        "--mode",
        choices=("phase1", "phase2"),
        default="phase1",
        help="phase1=static analysis, phase2=runtime validation on high-risk endpoints",
    )
    parser.add_argument(
        "--services",
        default="bff,oms,agent",
        help="Comma-separated service names (bff,oms,agent)",
    )
    parser.add_argument(
        "--include-hidden",
        action="store_true",
        help="Include routes with include_in_schema=false in the inventory",
    )
    parser.add_argument(
        "--output-md",
        type=Path,
        default=default_md,
        help="Markdown report output path",
    )
    parser.add_argument(
        "--output-json",
        type=Path,
        default=default_json,
        help="Machine-readable inventory JSON output path",
    )
    parser.add_argument(
        "--output-thresholds",
        type=Path,
        default=default_thresholds,
        help="Threshold calibration JSON output path",
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=default_json,
        help="Input inventory JSON for phase2",
    )
    parser.add_argument(
        "--risk",
        choices=("low", "medium", "high"),
        default="high",
        help="Phase2 minimum risk class",
    )
    parser.add_argument(
        "--runtime-timeout",
        type=float,
        default=5.0,
        help="Phase2 HTTP request timeout in seconds",
    )
    parser.add_argument(
        "--strict",
        action="store_true",
        help="Exit non-zero when acceptance criteria fail",
    )
    parser.add_argument(
        "--strict-contract",
        action="store_true",
        help="With --strict, also fail when phase2 has contract mismatches (even if fallback 4xx accepted).",
    )
    return parser.parse_args()


def _phase1_strict_failures(payload: dict[str, Any]) -> list[str]:
    failures: list[str] = []
    coverage = payload.get("coverage", {})
    if int(coverage.get("hidden_unclassified_count", 0)) != 0:
        failures.append("hidden_unclassified_count must be 0")
    if int(coverage.get("legacy_unclassified_count", 0)) != 0:
        failures.append("legacy_unclassified_count must be 0")
    if int(coverage.get("sink_missing_reason_count", 0)) != 0:
        failures.append("sink_missing_reason_count must be 0")

    service_diffs = payload.get("service_diffs", {})
    for service, diff in sorted(service_diffs.items()):
        if not isinstance(diff, dict):
            continue
        runtime_available = bool(diff.get("runtime_available"))
        if runtime_available and int(diff.get("static_only_count", 0)) != 0:
            failures.append(f"{service}: static_only_count must be 0 when runtime is available")
        if runtime_available and int(diff.get("runtime_only_count", 0)) != 0:
            failures.append(f"{service}: runtime_only_count must be 0 when runtime is available")

    endpoints = payload.get("endpoints", [])
    if isinstance(endpoints, list):
        missing_sink_or_reason = [
            endpoint.get("endpoint_id")
            for endpoint in endpoints
            if isinstance(endpoint, dict)
            and not endpoint.get("sink_categories")
            and not endpoint.get("no_sink_reason")
        ]
        if missing_sink_or_reason:
            failures.append(f"{len(missing_sink_or_reason)} endpoints have no sink evidence and no no_sink_reason")

    return failures


def _phase2_strict_failures(payload: dict[str, Any], *, strict_contract: bool) -> list[str]:
    failures: list[str] = []
    phase2 = payload.get("phase2", {})
    if not isinstance(phase2, dict):
        failures.append("phase2 report is missing")
        return failures
    if phase2.get("pending_reason"):
        return failures
    summary = phase2.get("summary", {})
    if int(summary.get("total", 0)) != int(summary.get("target_count", 0)):
        failures.append("phase2 total checks must match selected target_count")
    if int(summary.get("server_errors", 0)) > 0:
        failures.append("phase2 has server_errors > 0")
    if int(summary.get("failed", 0)) > 0:
        failures.append("phase2 has failed checks > 0")
    if strict_contract and int(summary.get("contract_mismatch_count", 0)) > 0:
        failures.append("phase2 has contract_mismatch_count > 0 with --strict-contract")
    return failures


def _print_failures(failures: list[str]) -> None:
    if not failures:
        return
    print("Acceptance criteria failed:")
    for failure in failures:
        print(f" - {failure}")


def run_phase1(args: argparse.Namespace) -> int:
    services = parse_services(args.services)
    collection = collect_inventory(services=services, include_hidden=bool(args.include_hidden))
    analysis = analyze_inventory(collection)
    payload = merge_phase1_results(collection, analysis)

    write_json(args.output_json, payload)
    write_markdown(args.output_md, render_markdown(payload))
    write_json(args.output_thresholds, payload.get("thresholds", {}))

    print(f"[phase1] endpoints={payload['summary']['endpoint_count']} clusters={payload['summary']['cluster_count']}")
    print(f"[phase1] markdown={args.output_md}")
    print(f"[phase1] inventory={args.output_json}")
    print(f"[phase1] thresholds={args.output_thresholds}")

    if args.strict:
        failures = _phase1_strict_failures(payload)
        _print_failures(failures)
        if failures:
            return 1
    return 0


def run_phase2(args: argparse.Namespace) -> int:
    if not args.input.exists():
        raise FileNotFoundError(f"Phase2 input JSON not found: {args.input}")

    payload = json.loads(args.input.read_text(encoding="utf-8"))
    runtime_report = run_runtime_validation(
        payload,
        risk_filter=str(args.risk),
        timeout_seconds=float(args.runtime_timeout),
    )
    updated_payload = attach_phase2_results(payload, runtime_report)

    write_json(args.output_json, updated_payload)
    write_markdown(args.output_md, render_markdown(updated_payload))

    phase2_summary = runtime_report.to_dict()["summary"]
    print(
        f"[phase2] checks={phase2_summary['total']}/{phase2_summary['target_count']} "
        f"passed={phase2_summary['passed']} failed={phase2_summary['failed']} "
        f"server_errors={phase2_summary['server_errors']} "
        f"contract_mismatch={phase2_summary['contract_mismatch_count']} "
        f"fallback_4xx={phase2_summary['fallback_4xx_count']}"
    )
    print(f"[phase2] markdown={args.output_md}")
    print(f"[phase2] inventory={args.output_json}")
    if runtime_report.pending_reason:
        print(f"[phase2] pending={runtime_report.pending_reason}")

    if args.strict:
        failures = _phase2_strict_failures(updated_payload, strict_contract=bool(args.strict_contract))
        _print_failures(failures)
        if failures:
            return 1
    return 0


def main() -> int:
    args = parse_args()
    if args.mode == "phase1":
        return run_phase1(args)
    if args.mode == "phase2":
        return run_phase2(args)
    raise ValueError(f"Unsupported mode: {args.mode}")


if __name__ == "__main__":
    raise SystemExit(main())
