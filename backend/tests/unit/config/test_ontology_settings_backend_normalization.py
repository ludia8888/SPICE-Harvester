from __future__ import annotations

from shared.config.settings import OntologySettings


def test_ontology_settings_backend_normalization_forces_postgres() -> None:
    assert OntologySettings(resource_storage_backend="postgres").resource_storage_backend == "postgres"
    assert OntologySettings(resource_storage_backend="terminus").resource_storage_backend == "postgres"
    assert OntologySettings(resource_storage_backend="hybrid").resource_storage_backend == "postgres"
    assert OntologySettings(resource_storage_backend="legacy-anything").resource_storage_backend == "postgres"
