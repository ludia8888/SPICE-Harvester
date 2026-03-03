from __future__ import annotations

import pytest

from shared.config.settings import OntologySettings, PipelineSettings, _normalize_base_branch
from shared.security.input_sanitizer import SecurityViolationError, validate_branch_name


def test_normalize_base_branch_defaults_to_main() -> None:
    assert _normalize_base_branch(None) == "main"
    assert _normalize_base_branch("") == "main"


def test_validate_branch_name_rejects_master() -> None:
    with pytest.raises(SecurityViolationError):
        validate_branch_name("master")


def test_pipeline_branch_defaults_are_main_only() -> None:
    settings = PipelineSettings(protected_branches="", fallback_branches="")
    assert settings.protected_branches_set == {"main"}
    assert settings.fallback_branches_list == ["main"]


def test_ontology_protected_branches_default_excludes_master() -> None:
    settings = OntologySettings(protected_branches="")
    assert "main" in settings.protected_branches_set
    assert "master" not in settings.protected_branches_set
