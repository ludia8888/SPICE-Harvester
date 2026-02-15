from __future__ import annotations

import pytest

from shared.security.input_sanitizer import SecurityViolationError, validate_branch_name


def test_validate_branch_name_accepts_foundry_branch_rid() -> None:
    branch_rid = "ri.ontology.main.branch.809f45f2-8f80-4f18-ba5e-34725fb85f65"
    assert validate_branch_name(branch_rid) == branch_rid


def test_validate_branch_name_rejects_reserved_head() -> None:
    with pytest.raises(SecurityViolationError):
        validate_branch_name("HEAD")
