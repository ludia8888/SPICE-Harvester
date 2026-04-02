from __future__ import annotations

import pytest

from data_connector.adapters.factory import (
    ConnectorAdapterFactory,
    connection_source_type_for_kind,
    connector_kind_from_source_type,
    file_import_source_type_for_kind,
    import_config_key_for_source_type,
    table_import_source_type_for_kind,
    virtual_table_source_type_for_kind,
)


def test_connector_kind_from_source_type_known_values() -> None:
    assert connector_kind_from_source_type("google_sheets_connection", strict=True) == "google_sheets"
    assert connector_kind_from_source_type("postgresql_table_import", strict=True) == "postgresql"
    assert connector_kind_from_source_type("mysql_file_import", strict=True) == "mysql"
    assert connector_kind_from_source_type("oracle_virtual_table", strict=True) == "oracle"


def test_connector_kind_from_source_type_unknown_raises_in_strict_mode() -> None:
    with pytest.raises(ValueError, match="Unknown connector source_type"):
        connector_kind_from_source_type("legacy_connector_source_type", strict=True)


def test_import_config_key_for_source_type_known_values() -> None:
    assert import_config_key_for_source_type("postgresql_table_import", strict=True) == "table_import_config"
    assert import_config_key_for_source_type("postgresql_file_import", strict=True) == "file_import_config"
    assert import_config_key_for_source_type("postgresql_virtual_table", strict=True) == "virtual_table_config"


def test_import_config_key_for_source_type_unknown_raises_in_strict_mode() -> None:
    with pytest.raises(ValueError, match="Unknown connector source_type"):
        import_config_key_for_source_type("legacy_connector_source_type", strict=True)


def test_source_type_helpers_reject_unknown_connector_kind() -> None:
    with pytest.raises(ValueError, match="Unknown connector kind"):
        connection_source_type_for_kind("legacy")
    with pytest.raises(ValueError, match="Unknown connector kind"):
        table_import_source_type_for_kind("legacy")
    with pytest.raises(ValueError, match="Unknown connector kind"):
        file_import_source_type_for_kind("legacy")
    with pytest.raises(ValueError, match="Unknown connector kind"):
        virtual_table_source_type_for_kind("legacy")


def test_adapter_factory_rejects_unknown_connector_kind() -> None:
    factory = ConnectorAdapterFactory(google_sheets_service=object())  # type: ignore[arg-type]

    with pytest.raises(ValueError, match="Unknown connector kind"):
        factory.get_adapter("legacy")
