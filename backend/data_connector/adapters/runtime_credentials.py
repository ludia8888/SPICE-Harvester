from __future__ import annotations

from typing import Any

from data_connector.adapters.factory import connection_source_type_for_kind, connector_kind_from_source_type
from shared.services.registries.connector_registry import ConnectorRegistry


async def resolve_source_runtime_credentials(
    *,
    connector_registry: ConnectorRegistry | None,
    source_type: str,
    source_config: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Resolve runtime connector config + secrets via parent connection link.

    Source config remains authoritative (merged over parent connection config).
    """
    source_cfg = dict(source_config or {})
    if connector_registry is None:
        return source_cfg, {}

    connection_id = str(source_cfg.get("connection_id") or "").strip() or None
    if not connection_id:
        return source_cfg, {}

    connector_kind = connector_kind_from_source_type(source_type)
    connection_source = await connector_registry.get_source(
        source_type=connection_source_type_for_kind(connector_kind),
        source_id=connection_id,
    )
    if connection_source is None:
        return source_cfg, {}

    connection_cfg = dict(connection_source.config_json or {})
    secrets = await connector_registry.get_connection_secrets(
        source_type=connection_source.source_type,
        source_id=connection_source.source_id,
    )
    merged_cfg = dict(connection_cfg)
    merged_cfg.update(source_cfg)
    return merged_cfg, secrets
