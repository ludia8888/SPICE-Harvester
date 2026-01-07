from __future__ import annotations

import os

from shared.models.ontology import Property
from shared.services.ontology_linter import OntologyLinterConfig, lint_ontology_create


def _reset_env(monkeypatch) -> None:
    for key in (
        "ENVIRONMENT",
        "APP_ENV",
        "APP_ENVIRONMENT",
        "ONTOLOGY_REQUIRE_PRIMARY_KEY",
        "ONTOLOGY_ALLOW_IMPLICIT_PRIMARY_KEY",
        "ONTOLOGY_REQUIRE_TITLE_KEY",
        "ONTOLOGY_ALLOW_IMPLICIT_TITLE_KEY",
        "ONTOLOGY_REQUIRE_PROPOSALS",
        "ONTOLOGY_PROTECTED_BRANCHES",
    ):
        monkeypatch.delenv(key, raising=False)


def _make_properties() -> list[Property]:
    return [
        Property(
            name="order_id",
            type="xsd:string",
            label="Order ID",
        ),
        Property(
            name="name",
            type="xsd:string",
            label="Order Name",
            title_key=True,
        ),
    ]


def test_linter_allows_implicit_pk_on_dev_branch(monkeypatch) -> None:
    _reset_env(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("ONTOLOGY_REQUIRE_PROPOSALS", "true")
    monkeypatch.setenv("ONTOLOGY_PROTECTED_BRANCHES", "main")

    config = OntologyLinterConfig.from_env(branch="dev")
    report = lint_ontology_create(
        class_id="order",
        label="Order",
        abstract=False,
        properties=_make_properties(),
        relationships=[],
        config=config,
    )

    assert not report.errors
    assert any(issue.rule_id == "ONT002" for issue in report.warnings)


def test_linter_blocks_implicit_pk_on_protected_branch(monkeypatch) -> None:
    _reset_env(monkeypatch)
    monkeypatch.setenv("ENVIRONMENT", "dev")
    monkeypatch.setenv("ONTOLOGY_REQUIRE_PROPOSALS", "true")
    monkeypatch.setenv("ONTOLOGY_PROTECTED_BRANCHES", "main")

    config = OntologyLinterConfig.from_env(branch="main")
    report = lint_ontology_create(
        class_id="order",
        label="Order",
        abstract=False,
        properties=_make_properties(),
        relationships=[],
        config=config,
    )

    assert any(issue.rule_id == "ONT001" for issue in report.errors)


def test_linter_requires_explicit_title_key_when_disabled(monkeypatch) -> None:
    _reset_env(monkeypatch)
    monkeypatch.setenv("ONTOLOGY_REQUIRE_TITLE_KEY", "true")
    monkeypatch.setenv("ONTOLOGY_ALLOW_IMPLICIT_TITLE_KEY", "false")

    config = OntologyLinterConfig.from_env(branch="main")
    report = lint_ontology_create(
        class_id="order",
        label="Order",
        abstract=False,
        properties=[
            Property(name="order_id", type="xsd:string", label="Order ID", primary_key=True),
        ],
        relationships=[],
        config=config,
    )

    assert any(
        issue.rule_id == "ONT003" and "title_key" in issue.message for issue in report.errors
    )
