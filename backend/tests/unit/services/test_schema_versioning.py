from __future__ import annotations

import pytest

from shared.services.schema_versioning import (
    SchemaMigration,
    SchemaRegistry,
    SchemaVersion,
    SchemaVersioningService,
)


def test_schema_version_parsing_and_comparison() -> None:
    with pytest.raises(ValueError):
        SchemaVersion("1.0")

    v1 = SchemaVersion("1.2.3")
    v2 = SchemaVersion("1.2.4")
    v3 = SchemaVersion("2.0.0")

    assert str(v1) == "1.2.3"
    assert v1 < v2
    assert v2 < v3
    assert v2.is_backward_compatible(SchemaVersion("1.0.0"))
    assert not v3.is_backward_compatible(v1)


def test_schema_registry_register_and_migrate() -> None:
    registry = SchemaRegistry()
    registry.register_schema("Widget", "1.0.0", {"id": {"type": "string"}})
    registry.register_schema("Widget", "1.1.0", {"id": {"type": "string"}, "flag": {"type": "bool"}})

    def _add_flag(data: dict[str, object]) -> dict[str, object]:
        return {**data, "flag": True}

    registry.register_migration(
        SchemaMigration(
            from_version="1.0.0",
            to_version="1.1.0",
            entity_type="Widget",
            migration_func=_add_flag,
        )
    )

    path = registry.get_migration_path("Widget", "1.0.0")
    assert len(path) == 1
    assert str(path[0].to_version) == "1.1.0"

    migrated = registry.migrate_data({"id": "w1", "schema_version": "1.0.0"}, "Widget")
    assert migrated["flag"] is True
    assert migrated["schema_version"] == "1.1.0"


def test_schema_versioning_service_event_helpers() -> None:
    service = SchemaVersioningService()
    event = {"event_id": "evt-1", "event_type": "CREATED", "occurred_at": "2024-01-01T00:00:00Z", "data": {}}

    versioned = service.version_event(dict(event))
    assert "schema_version" in versioned

    migrated = service.migrate_event({**event, "schema_version": "1.0.0"})
    assert migrated["schema_version"] == "1.2.0"
    assert migrated["sequence_number"] == 0
    assert migrated["aggregate_id"] is None

    assert service.is_compatible({"schema_version": "1.2.0"}, "Event")
    assert not service.is_compatible({"schema_version": "2.0.0"}, "Event")
