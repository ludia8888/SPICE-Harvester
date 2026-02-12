"""
Backing Source Adapter — OMS-first mapping spec resolution.

Foundry stores property_mappings inside object_type.spec.backing_source.
This adapter reads that data and presents it in the same shape as
OntologyMappingSpecRecord so that consumers (pipeline_worker, objectify_worker)
can switch transparently.

Two main entry points:
  * get_mapping_from_oms()  — class_id → BackingSourceMappingSpec
  * find_class_id_by_dataset() — dataset_id → class_id (reverse lookup)

MappingSpecResolver combines OMS lookup with PostgreSQL fallback.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import httpx

logger = logging.getLogger(__name__)

# Prefix used to distinguish OMS-sourced mapping specs from PostgreSQL ones.
OMS_MAPPING_SPEC_PREFIX = "oms:"


@dataclass(frozen=True)
class BackingSourceMappingSpec:
    """OMS backing_source에서 추출된 mapping spec (PostgreSQL MappingSpecRecord과 호환)."""

    mapping_spec_id: str  # "oms:{class_id}"
    dataset_id: str
    dataset_branch: str
    artifact_output_name: Optional[str]
    schema_hash: Optional[str]
    target_class_id: str
    mappings: List[Dict[str, str]]
    target_field_types: Dict[str, str]
    auto_sync: bool
    version: int  # mapping_version from backing_source
    status: str = "ACTIVE"
    backing_datasource_id: Optional[str] = None
    backing_datasource_version_id: Optional[str] = None
    options: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))


def _normalize_oms_response(payload: Any) -> Dict[str, Any]:
    """OMS responses may be wrapped in {"data": {...}} or {"response": {...}}."""
    if isinstance(payload, dict):
        if isinstance(payload.get("data"), dict):
            return payload["data"]
        if isinstance(payload.get("response"), dict):
            return payload["response"]
    return payload if isinstance(payload, dict) else {}


async def get_mapping_from_oms(
    http_client: httpx.AsyncClient,
    *,
    oms_base_url: str,
    db_name: str,
    target_class_id: str,
    branch: str = "main",
    admin_token: Optional[str] = None,
) -> Optional[BackingSourceMappingSpec]:
    """
    Fetch an object_type resource from OMS and extract backing_source mapping spec.

    Returns None if:
      - object_type does not exist
      - backing_source has no property_mappings
    """
    url = f"{oms_base_url}/api/v1/database/{db_name}/ontology/resources/object_type/{target_class_id}"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if admin_token:
        headers["X-Admin-Token"] = admin_token

    try:
        resp = await http_client.get(url, params={"branch": branch}, headers=headers)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        raw = resp.json() if resp.text else {}
    except Exception as exc:
        logger.warning("get_mapping_from_oms failed for %s/%s: %s", db_name, target_class_id, exc)
        return None

    resource = _normalize_oms_response(raw)
    spec = resource.get("spec") or {}
    backing = spec.get("backing_source") or {}

    property_mappings = backing.get("property_mappings")
    if not property_mappings or not isinstance(property_mappings, list):
        return None

    dataset_id = str(backing.get("dataset_id") or "").strip()
    if not dataset_id:
        return None

    return BackingSourceMappingSpec(
        mapping_spec_id=f"{OMS_MAPPING_SPEC_PREFIX}{target_class_id}",
        dataset_id=dataset_id,
        dataset_branch=str(backing.get("dataset_branch") or branch),
        artifact_output_name=backing.get("artifact_output_name"),
        schema_hash=backing.get("schema_hash"),
        target_class_id=target_class_id,
        mappings=[
            {"source_field": str(m.get("source_field", "")), "target_field": str(m.get("target_field", ""))}
            for m in property_mappings
            if isinstance(m, dict)
        ],
        target_field_types=backing.get("target_field_types") or {},
        auto_sync=backing.get("auto_sync", True),
        version=int(backing.get("mapping_version") or 1),
        status=str(spec.get("status") or "ACTIVE"),
        backing_datasource_id=str(backing.get("backing_datasource_id") or "") or None,
        backing_datasource_version_id=str(backing.get("version_id") or "") or None,
    )


async def find_class_id_by_dataset(
    http_client: httpx.AsyncClient,
    *,
    oms_base_url: str,
    db_name: str,
    dataset_id: str,
    branch: str = "main",
    admin_token: Optional[str] = None,
) -> Optional[str]:
    """
    Reverse lookup: find the object_type whose backing_source.dataset_id matches.

    Scans all object_type resources (typically < 100 per database).
    Returns the first matching class_id or None.
    """
    url = f"{oms_base_url}/api/v1/database/{db_name}/ontology/resources/object_type"
    headers: Dict[str, str] = {"Accept": "application/json"}
    if admin_token:
        headers["X-Admin-Token"] = admin_token

    try:
        resp = await http_client.get(url, params={"branch": branch}, headers=headers)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        raw = resp.json() if resp.text else {}
    except Exception as exc:
        logger.warning("find_class_id_by_dataset failed for %s/%s: %s", db_name, dataset_id, exc)
        return None

    data = _normalize_oms_response(raw)
    resources = data.get("resources") or (data.get("items") if isinstance(data.get("items"), list) else None) or []

    # If list_resources_by_type returns a flat list
    if isinstance(data, list):
        resources = data

    for resource in resources:
        if not isinstance(resource, dict):
            continue
        spec = resource.get("spec") or {}
        backing = spec.get("backing_source") or {}
        res_dataset_id = str(backing.get("dataset_id") or "").strip()
        if res_dataset_id == dataset_id:
            return str(resource.get("resource_id") or resource.get("id") or "").strip() or None

    return None


class MappingSpecResolver:
    """
    OMS-first, PostgreSQL-fallback mapping spec resolver.

    Usage::

        resolver = MappingSpecResolver(
            http_client=...,
            oms_base_url="http://oms:8000",
            admin_token="...",
            objectify_registry=registry,  # optional PostgreSQL fallback
        )
        spec = await resolver.resolve(db_name="mydb", dataset_id="uuid-...", branch="main")
    """

    def __init__(
        self,
        *,
        http_client: httpx.AsyncClient,
        oms_base_url: str,
        admin_token: Optional[str] = None,
        objectify_registry: Any = None,
    ):
        self._http = http_client
        self._oms_base_url = oms_base_url
        self._admin_token = admin_token
        self._pg_registry = objectify_registry

    async def resolve(
        self,
        *,
        db_name: str,
        dataset_id: str,
        branch: str = "main",
        schema_hash: Optional[str] = None,
        target_class_id: Optional[str] = None,
        artifact_output_name: Optional[str] = None,
    ) -> Optional[BackingSourceMappingSpec]:
        """
        Resolve mapping spec with OMS priority:
          1) If target_class_id given → direct OMS lookup
          2) Else → reverse-lookup dataset_id → class_id → OMS lookup
          3) Fallback: PostgreSQL objectify_registry
        """

        # --- Strategy 1: Direct OMS lookup by class_id ---
        if target_class_id:
            oms_spec = await get_mapping_from_oms(
                self._http,
                oms_base_url=self._oms_base_url,
                db_name=db_name,
                target_class_id=target_class_id,
                branch=branch,
                admin_token=self._admin_token,
            )
            if oms_spec:
                logger.debug("OMS mapping resolved for class_id=%s", target_class_id)
                return oms_spec

        # --- Strategy 2: Reverse lookup dataset_id → class_id → OMS ---
        if not target_class_id:
            found_class_id = await find_class_id_by_dataset(
                self._http,
                oms_base_url=self._oms_base_url,
                db_name=db_name,
                dataset_id=dataset_id,
                branch=branch,
                admin_token=self._admin_token,
            )
            if found_class_id:
                oms_spec = await get_mapping_from_oms(
                    self._http,
                    oms_base_url=self._oms_base_url,
                    db_name=db_name,
                    target_class_id=found_class_id,
                    branch=branch,
                    admin_token=self._admin_token,
                )
                if oms_spec:
                    logger.debug("OMS mapping resolved via reverse-lookup dataset=%s → class=%s", dataset_id, found_class_id)
                    return oms_spec

        # --- Strategy 3: PostgreSQL fallback ---
        if self._pg_registry:
            try:
                pg_spec = await self._pg_registry.get_active_mapping_spec(
                    dataset_id=dataset_id,
                    dataset_branch=branch,
                    target_class_id=target_class_id,
                    artifact_output_name=artifact_output_name,
                    schema_hash=schema_hash,
                )
                if pg_spec:
                    logger.debug("PostgreSQL fallback mapping resolved for dataset=%s", dataset_id)
                    # Wrap PostgreSQL record into BackingSourceMappingSpec for uniform interface
                    return BackingSourceMappingSpec(
                        mapping_spec_id=pg_spec.mapping_spec_id,
                        dataset_id=pg_spec.dataset_id,
                        dataset_branch=pg_spec.dataset_branch,
                        artifact_output_name=pg_spec.artifact_output_name,
                        schema_hash=pg_spec.schema_hash,
                        target_class_id=pg_spec.target_class_id,
                        mappings=pg_spec.mappings,
                        target_field_types=pg_spec.target_field_types,
                        auto_sync=pg_spec.auto_sync,
                        version=pg_spec.version,
                        status=pg_spec.status,
                        backing_datasource_id=getattr(pg_spec, "backing_datasource_id", None),
                        backing_datasource_version_id=getattr(pg_spec, "backing_datasource_version_id", None),
                        options=getattr(pg_spec, "options", {}),
                        created_at=getattr(pg_spec, "created_at", datetime.now(timezone.utc)),
                        updated_at=getattr(pg_spec, "updated_at", datetime.now(timezone.utc)),
                    )
            except Exception as exc:
                logger.warning("PostgreSQL fallback failed for dataset=%s: %s", dataset_id, exc)

        return None

    async def resolve_by_class_id(
        self,
        *,
        db_name: str,
        target_class_id: str,
        branch: str = "main",
    ) -> Optional[BackingSourceMappingSpec]:
        """Convenience: resolve directly by class_id (OMS-only, no PG fallback)."""
        return await get_mapping_from_oms(
            self._http,
            oms_base_url=self._oms_base_url,
            db_name=db_name,
            target_class_id=target_class_id,
            branch=branch,
            admin_token=self._admin_token,
        )


def is_oms_mapping_spec(mapping_spec_id: str) -> bool:
    """Check if a mapping_spec_id originates from OMS backing_source."""
    return bool(mapping_spec_id and mapping_spec_id.startswith(OMS_MAPPING_SPEC_PREFIX))


def extract_class_id_from_oms_spec_id(mapping_spec_id: str) -> str:
    """Extract class_id from an OMS mapping_spec_id like 'oms:Customer'."""
    return mapping_spec_id[len(OMS_MAPPING_SPEC_PREFIX):]
