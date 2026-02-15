from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

from shared.utils.ontology_type_normalization import normalize_ontology_base_type


_INTERFACE_METADATA_KEYS = ("interfaces", "interface_refs", "interfaceRefs", "interfaceRef", "interface")


@dataclass(frozen=True)
class ActionTargetRuntimeContract:
    interfaces: List[str]
    field_types: Dict[str, str]


def strip_interface_prefix(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    for prefix in ("interface:", "interfaces:"):
        if text.startswith(prefix):
            return text[len(prefix) :].strip()
    return text


def extract_interfaces_from_metadata(metadata: Mapping[str, Any] | None) -> List[str]:
    if not isinstance(metadata, Mapping):
        return []
    refs: List[str] = []
    for key in _INTERFACE_METADATA_KEYS:
        raw = metadata.get(key)
        if isinstance(raw, str) and raw.strip():
            refs.append(raw.strip())
        elif isinstance(raw, list):
            for item in raw:
                if isinstance(item, str) and item.strip():
                    refs.append(item.strip())
    seen = set()
    out: List[str] = []
    for ref in refs:
        normalized = strip_interface_prefix(ref)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def extract_required_action_interfaces(action_spec: Mapping[str, Any] | None) -> List[str]:
    if not isinstance(action_spec, Mapping):
        return []

    refs: List[str] = []

    def _collect(value: Any) -> None:
        if isinstance(value, str):
            text = value.strip()
            if text:
                refs.append(text)
            return
        if isinstance(value, list):
            for item in value:
                if isinstance(item, str) and item.strip():
                    refs.append(item.strip())

    for key in (
        "target_interfaces",
        "target_interface_refs",
        "targetInterfaceRefs",
        "interface_refs",
        "interfaceRefs",
    ):
        _collect(action_spec.get(key))

    applies_to = action_spec.get("applies_to") or action_spec.get("appliesTo")
    if isinstance(applies_to, Mapping):
        for key in ("interfaces", "interface_refs", "interfaceRefs", "interface"):
            _collect(applies_to.get(key))

    seen = set()
    out: List[str] = []
    for ref in refs:
        normalized = strip_interface_prefix(ref)
        if normalized and normalized not in seen:
            seen.add(normalized)
            out.append(normalized)
    return out


def build_property_type_map_from_properties(properties: Any) -> Dict[str, str]:
    if not isinstance(properties, list):
        return {}
    out: Dict[str, str] = {}
    for item in properties:
        if isinstance(item, Mapping):
            name = str(item.get("name") or "").strip()
            raw_type = item.get("type")
        else:
            name = str(getattr(item, "name", "") or "").strip()
            raw_type = getattr(item, "type", None)
        if not name:
            continue
        normalized = normalize_ontology_base_type(raw_type)
        if normalized:
            out[name] = normalized
    return out


def _pick_properties_from_record(record: Any) -> Any:
    if isinstance(record, Mapping):
        direct = record.get("properties")
        if isinstance(direct, list):
            return direct
        spec = record.get("spec")
        if isinstance(spec, Mapping):
            for key in ("properties", "required_properties", "fields"):
                raw = spec.get(key)
                if isinstance(raw, list):
                    return raw
        return []

    direct = getattr(record, "properties", None)
    if isinstance(direct, list):
        return direct
    spec = getattr(record, "spec", None)
    if isinstance(spec, Mapping):
        for key in ("properties", "required_properties", "fields"):
            raw = spec.get(key)
            if isinstance(raw, list):
                return raw
    return []


def _pick_interface_metadata_from_record(record: Any) -> Mapping[str, Any]:
    if isinstance(record, Mapping):
        metadata = record.get("metadata")
        if isinstance(metadata, Mapping):
            interfaces = extract_interfaces_from_metadata(metadata)
            if interfaces:
                return metadata
        spec = record.get("spec")
        if isinstance(spec, Mapping):
            interfaces = extract_interfaces_from_metadata(spec)
            if interfaces:
                return spec
        return metadata if isinstance(metadata, Mapping) else {}

    metadata = getattr(record, "metadata", None)
    if isinstance(metadata, Mapping):
        interfaces = extract_interfaces_from_metadata(metadata)
        if interfaces:
            return metadata

    spec = getattr(record, "spec", None)
    if isinstance(spec, Mapping):
        interfaces = extract_interfaces_from_metadata(spec)
        if interfaces:
            return spec
    return metadata if isinstance(metadata, Mapping) else {}


def _build_contract_from_record(record: Any) -> ActionTargetRuntimeContract:
    metadata = _pick_interface_metadata_from_record(record)
    properties = _pick_properties_from_record(record)
    return ActionTargetRuntimeContract(
        interfaces=extract_interfaces_from_metadata(metadata),
        field_types=build_property_type_map_from_properties(properties),
    )


async def load_action_target_runtime_contract(
    *,
    db_name: str,
    class_id: str,
    branch: str,
    resources: Any | None = None,
) -> Optional[ActionTargetRuntimeContract]:
    if resources is not None:
        object_resource = await resources.get_resource(
            db_name,
            branch=branch,
            resource_type="object_type",
            resource_id=class_id,
        )
        if object_resource is None:
            return None
        return _build_contract_from_record(object_resource)

    return None
