from __future__ import annotations

from pathlib import Path

import yaml


def _as_list(value: object) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return [str(value)]


def main() -> int:
    root = Path(__file__).resolve().parents[1]
    matrix_path = root / "docs/foundry_checklist/FOUNDARY_CHECKLIST_MATRIX.yml"
    report_path = root / "docs/foundry_checklist/VERIFICATION_REPORT.md"

    if not matrix_path.exists():
        raise SystemExit(f"Missing matrix: {matrix_path}")

    entries = yaml.safe_load(matrix_path.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("Matrix must be a YAML list")

    counts: dict[str, dict[str, int]] = {"P0": {"total": 0, "pass": 0}, "P1": {"total": 0, "pass": 0}, "P2": {"total": 0, "pass": 0}}
    for entry in entries:
        if not isinstance(entry, dict):
            continue
        priority = str(entry.get("priority") or "").strip()
        if priority not in counts:
            continue
        counts[priority]["total"] += 1
        if str(entry.get("status") or "").strip().upper() == "PASS":
            counts[priority]["pass"] += 1

    lines: list[str] = []
    lines.append("# Foundry Checklist Verification Report")
    lines.append("")
    lines.append("- Checklist: `docs/PipelineBuilder_checklist.md`")
    lines.append("- Matrix: `docs/foundry_checklist/FOUNDARY_CHECKLIST_MATRIX.yml`")
    lines.append("- Evidence dir: `docs/foundry_checklist/evidence/`")
    lines.append("")
    lines.append("## Current Status")
    lines.append(f"- P0: {counts['P0']['pass']}/{counts['P0']['total']} PASS")
    lines.append(f"- P1: {counts['P1']['pass']}/{counts['P1']['total']} PASS")
    lines.append(f"- P2: {counts['P2']['pass']}/{counts['P2']['total']} PASS")
    lines.append("")
    lines.append("## How to Reproduce")
    lines.append("```bash")
    lines.append("./scripts/verify_foundry_checklist.sh")
    lines.append("```")
    lines.append("")
    lines.append("## Per-Item Results")
    lines.append("")

    for entry in entries:
        if not isinstance(entry, dict):
            continue
        cl_id = str(entry.get("id") or "").strip()
        if not cl_id:
            continue
        priority = str(entry.get("priority") or "").strip()
        status = str(entry.get("status") or "FAIL").strip().upper()
        original = str(entry.get("original_text") or "").strip()

        verification = entry.get("verification") if isinstance(entry.get("verification"), dict) else {}
        vtype = str(verification.get("type") or "").strip() if isinstance(verification, dict) else ""
        commands = _as_list(verification.get("commands")) if isinstance(verification, dict) else []
        tests = _as_list(verification.get("tests")) if isinstance(verification, dict) else []
        artifacts = _as_list(verification.get("artifacts")) if isinstance(verification, dict) else []
        evidence_links = _as_list(entry.get("evidence_links"))

        lines.append(f"### {cl_id} ({priority}) — {status}")
        lines.append("")
        lines.append(f"- Original: {original}")
        if vtype:
            lines.append(f"- Verification type: `{vtype}`")
        if tests:
            lines.append("- Tests:")
            for test in tests:
                lines.append(f"  - `{test}`")
        if commands:
            lines.append("- Commands:")
            for cmd in commands:
                lines.append(f"  - `{cmd}`")
        if artifacts:
            lines.append("- Artifacts:")
            for artifact in artifacts:
                lines.append(f"  - `{artifact}`")
        if evidence_links:
            lines.append("- Evidence links:")
            for link in evidence_links:
                lines.append(f"  - `{link}`")
        lines.append("")

    report_path.write_text("\n".join(lines).rstrip() + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

