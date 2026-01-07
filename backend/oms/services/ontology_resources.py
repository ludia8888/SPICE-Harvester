"""
Ontology resource storage service (shared properties, value types, interfaces, groups, functions, action types).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from oms.exceptions import DatabaseError, OntologyNotFoundError
from oms.services.async_terminus import AsyncTerminusService

logger = logging.getLogger(__name__)


RESOURCE_CLASS_ID = "__ontology_resource"
RESOURCE_DOC_PREFIX = "__ontology_resource"

RESOURCE_TYPE_ALIASES = {
    "object_type": "object_type",
    "object_types": "object_type",
    "object-type": "object_type",
    "object-types": "object_type",
    "link_type": "link_type",
    "link_types": "link_type",
    "link-type": "link_type",
    "link-types": "link_type",
    "shared_property": "shared_property",
    "shared_properties": "shared_property",
    "shared-property": "shared_property",
    "shared-properties": "shared_property",
    "value_type": "value_type",
    "value_types": "value_type",
    "value-type": "value_type",
    "value-types": "value_type",
    "interface": "interface",
    "interfaces": "interface",
    "group": "group",
    "groups": "group",
    "function": "function",
    "functions": "function",
    "action_type": "action_type",
    "action_types": "action_type",
    "action-type": "action_type",
    "action-types": "action_type",
}


def normalize_resource_type(value: str) -> str:
    raw = (value or "").strip().lower()
    if not raw:
        raise ValueError("resource_type is required")
    normalized = RESOURCE_TYPE_ALIASES.get(raw)
    if not normalized:
        raise ValueError(f"Unsupported resource_type: {value}")
    return normalized


def _resource_doc_id(resource_type: str, resource_id: str) -> str:
    # TerminusDB expects instance IDs to be under the class prefix path.
    return f"{RESOURCE_DOC_PREFIX}/{resource_type}:{resource_id}"


def _localized_to_string(value: Any) -> Tuple[Optional[str], Optional[Dict[str, str]]]:
    if value is None:
        return None, None
    if isinstance(value, str):
        text = value.strip()
        return text or None, None
    if isinstance(value, dict):
        lang_map: Dict[str, str] = {}
        for k, v in value.items():
            if v is None:
                continue
            v_str = str(v).strip()
            if not v_str:
                continue
            lang_map[str(k)] = v_str
        if not lang_map:
            return None, None
        for key in ("en", "ko"):
            if key in lang_map:
                return lang_map[key], lang_map
        return next(iter(lang_map.values())), lang_map
    text = str(value).strip()
    return text or None, None


class OntologyResourceService:
    """CRUD for ontology resource instances stored in TerminusDB."""

    def __init__(self, terminus: AsyncTerminusService):
        self.terminus = terminus
        self.documents = terminus.document_service

    async def ensure_resource_schema(self, db_name: str, *, branch: str) -> None:
        exists = await self.documents.document_exists(
            db_name, RESOURCE_CLASS_ID, graph_type="schema", branch=branch
        )
        if exists:
            return

        schema_doc = {
            "@type": "Class",
            "@id": RESOURCE_CLASS_ID,
            "@key": {"@type": "Random"},
            "@documentation": {
                "@label": "Ontology Resource",
                "@comment": "Internal ontology resource registry",
                "@internal": True,
            },
            "resource_type": "xsd:string",
            "resource_id": "xsd:string",
            "label": "xsd:string",
            "description": {"@type": "Optional", "@class": "xsd:string"},
            "label_i18n": {"@type": "Optional", "@class": "sys:JSON"},
            "description_i18n": {"@type": "Optional", "@class": "sys:JSON"},
            "spec": {"@type": "Optional", "@class": "sys:JSON"},
            "metadata": {"@type": "Optional", "@class": "sys:JSON"},
            "created_at": {"@type": "Optional", "@class": "xsd:dateTime"},
            "updated_at": {"@type": "Optional", "@class": "xsd:dateTime"},
        }

        await self.documents.create_document(
            db_name,
            schema_doc,
            graph_type="schema",
            branch=branch,
            author=self.terminus.connection_info.user or "system",
            message="Create ontology resource schema",
        )

    async def create_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        await self.ensure_resource_schema(db_name, branch=branch)
        doc_id = _resource_doc_id(resource_type, resource_id)

        if await self.documents.document_exists(db_name, doc_id, branch=branch):
            raise DatabaseError(f"Resource '{resource_id}' already exists")

        doc = self._payload_to_document(resource_type, resource_id, payload, doc_id=doc_id, is_create=True)

        await self.documents.create_document(
            db_name,
            doc,
            graph_type="instance",
            branch=branch,
            author=self.terminus.connection_info.user or "system",
            message=f"Create ontology resource {resource_type}:{resource_id}",
        )

        return self._document_to_payload(doc)

    async def update_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
    ) -> Dict[str, Any]:
        await self.ensure_resource_schema(db_name, branch=branch)
        doc_id = _resource_doc_id(resource_type, resource_id)

        existing = await self.documents.get_document(db_name, doc_id, branch=branch)
        if not existing:
            raise OntologyNotFoundError(f"Resource '{resource_id}' not found")

        doc = self._payload_to_document(
            resource_type,
            resource_id,
            payload,
            doc_id=doc_id,
            is_create=False,
            existing=existing,
        )

        await self.documents.update_document(
            db_name,
            doc_id,
            doc,
            graph_type="instance",
            branch=branch,
            author=self.terminus.connection_info.user or "system",
            message=f"Update ontology resource {resource_type}:{resource_id}",
        )

        return self._document_to_payload(doc)

    async def delete_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> None:
        doc_id = _resource_doc_id(resource_type, resource_id)
        await self.documents.delete_document(
            db_name,
            doc_id,
            graph_type="instance",
            branch=branch,
            author=self.terminus.connection_info.user or "system",
            message=f"Delete ontology resource {resource_type}:{resource_id}",
        )

    async def get_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ) -> Optional[Dict[str, Any]]:
        doc_id = _resource_doc_id(resource_type, resource_id)
        doc = await self.documents.get_document(db_name, doc_id, branch=branch)
        if not doc:
            return None
        return self._document_to_payload(doc)

    async def list_resources(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: Optional[str] = None,
        limit: int = 200,
        offset: int = 0,
    ) -> List[Dict[str, Any]]:
        await self.ensure_resource_schema(db_name, branch=branch)
        docs = await self.documents.list_documents(
            db_name,
            graph_type="instance",
            branch=branch,
            doc_type=RESOURCE_CLASS_ID,
            limit=limit,
            offset=offset,
        )

        out: List[Dict[str, Any]] = []
        for doc in docs or []:
            if not isinstance(doc, dict):
                continue
            if resource_type and doc.get("resource_type") != resource_type:
                continue
            out.append(self._document_to_payload(doc))
        return out

    def _payload_to_document(
        self,
        resource_type: str,
        resource_id: str,
        payload: Dict[str, Any],
        *,
        doc_id: str,
        is_create: bool,
        existing: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        base_payload = dict(payload or {})

        label_raw = base_payload.pop("label", None)
        description_raw = base_payload.pop("description", None)

        label, label_i18n = _localized_to_string(label_raw)
        description, description_i18n = _localized_to_string(description_raw)

        spec = base_payload.pop("spec", None)
        metadata = base_payload.pop("metadata", None)

        spec_payload = spec if isinstance(spec, dict) else {}
        metadata_payload = metadata if isinstance(metadata, dict) else {}

        if base_payload:
            spec_payload = {**base_payload, **spec_payload}

        now = datetime.now(timezone.utc)
        created_at = (existing or {}).get("created_at") if not is_create else None
        if not created_at:
            created_at = now.isoformat()

        doc = {
            "@id": doc_id,
            "@type": RESOURCE_CLASS_ID,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "label": label or resource_id,
            "description": description,
            "label_i18n": label_i18n,
            "description_i18n": description_i18n,
            "spec": spec_payload or None,
            "metadata": metadata_payload or None,
            "created_at": created_at,
            "updated_at": now.isoformat(),
        }

        return {k: v for k, v in doc.items() if v is not None}

    def _document_to_payload(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        label_i18n = doc.get("label_i18n")
        description_i18n = doc.get("description_i18n")
        label = label_i18n if isinstance(label_i18n, dict) and label_i18n else doc.get("label")
        description = (
            description_i18n
            if isinstance(description_i18n, dict) and description_i18n
            else doc.get("description")
        )
        return {
            "resource_type": doc.get("resource_type"),
            "id": doc.get("resource_id"),
            "label": label,
            "description": description,
            "spec": doc.get("spec") or {},
            "metadata": doc.get("metadata") or {},
            "created_at": doc.get("created_at"),
            "updated_at": doc.get("updated_at"),
        }
