from __future__ import annotations

import re
from pathlib import Path

import pytest


_CHECKLIST_LABELS = (
    "계층 간 누수",
    "의존성 튐(패키지 순환)",
    "I/O와 Core 직접 연결",
    "모듈 결합도 과다",
    "파일 응집도 저하",
    "파일 단일 책임 위반",
    "함수 단일 책임 위반",
    "연속 상속 깊이(>=3)",
    "복잡도 과다(CC>=15)",
    "롱메서드(len>=80)",
)

_ROW_PATTERN = re.compile(
    r"^\|\s*(\d+)\s*\|\s*(.*?)\s*\|\s*(\d+/\d+\s+\(\d+\.\d+%\))\s*\|\s*<=\s*\d+\.\d+%\s*\|\s*\*\*(PASS|FAIL)\*\*\s*\|",
    re.MULTILINE,
)

_POPULATION_PATTERN = re.compile(
    r"Population:\s*files\s*\*\*\d+\*\*,\s*functions\s*\*\*\d+\*\*,\s*classes\s*\*\*\d+\*\*,\s*internal cross-imports\s*\*\*\d+\*\*"
)


def _checklist_section(text: str) -> str:
    heading = "## Architecture Quality Checklist (Auto-Computed)"
    pos = text.find(heading)
    assert pos >= 0, "Architecture quality checklist section is missing"
    return text[pos:]


@pytest.mark.unit
@pytest.mark.parametrize(
    "doc_path",
    [
        Path(__file__).resolve().parents[4] / "docs" / "ARCHITECTURE.md",
        Path(__file__).resolve().parents[4] / "docs-portal" / "docs" / "architecture" / "auto-architecture.md",
    ],
)
def test_architecture_quality_checklist_is_auto_rendered_with_ratios(doc_path: Path) -> None:
    text = doc_path.read_text(encoding="utf-8")
    section = _checklist_section(text)

    assert _POPULATION_PATTERN.search(section), f"Population summary line missing in {doc_path}"

    rows = _ROW_PATTERN.findall(section)
    assert len(rows) == 10, f"Expected 10 checklist rows in {doc_path}, got {len(rows)}"

    indices = [int(index) for index, _label, _ratio, _status in rows]
    assert indices == list(range(1, 11))

    labels = [label.strip() for _index, label, _ratio, _status in rows]
    assert labels == list(_CHECKLIST_LABELS)
