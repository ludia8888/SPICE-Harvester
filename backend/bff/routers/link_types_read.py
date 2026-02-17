"""Helpers for normalizing link-type resources into Foundry v2 shapes."""


def _unwrap_data(payload):
    if not isinstance(payload, dict):
        return {}
    data = payload.get("data")
    if isinstance(data, dict):
        return data
    return payload


def _extract_resources(payload):
    data = _unwrap_data(payload)
    resources = data.get("resources") if isinstance(data, dict) else None
    if not isinstance(resources, list):
        return []
    return [entry for entry in resources if isinstance(entry, dict)]


def _normalize_object_ref(raw):
    if not isinstance(raw, str):
        return None
    value = raw.strip()
    if not value:
        return None
    for prefix in ("object_type:", "object:", "class:"):
        if value.startswith(prefix):
            value = value[len(prefix) :].strip()
            break
    if "@" in value:
        value = value.split("@", 1)[0].strip()
    return value or None


def _localized_text(value):
    if isinstance(value, str):
        text = value.strip()
        return text or None
    if isinstance(value, dict):
        for key in ("en", "ko"):
            key_value = value.get(key)
            if isinstance(key_value, str) and key_value.strip():
                return key_value.strip()
        for key_value in value.values():
            if isinstance(key_value, str) and key_value.strip():
                return key_value.strip()
    return None


def _map_cardinality(raw):
    value = str(raw or "").strip().lower()
    if value in {"1:1", "n:1", "one"}:
        return "ONE"
    if value in {"1:n", "n:1+", "n:m", "n:n", "many"}:
        return "MANY"
    if value == "m:1":
        return "ONE"
    return None


def _to_foundry_outgoing_link_type(resource, *, source_object_type):
    spec = resource.get("spec") if isinstance(resource.get("spec"), dict) else {}
    from_ref = _normalize_object_ref(spec.get("from") if isinstance(spec, dict) else None)
    if from_ref is None:
        from_ref = _normalize_object_ref(resource.get("from"))
    if from_ref != source_object_type:
        return None

    to_ref = _normalize_object_ref(spec.get("to") if isinstance(spec, dict) else None)
    if to_ref is None:
        to_ref = _normalize_object_ref(resource.get("to"))

    relationship_spec = spec.get("relationship_spec") if isinstance(spec.get("relationship_spec"), dict) else {}
    if not to_ref and isinstance(relationship_spec, dict):
        to_ref = _normalize_object_ref(relationship_spec.get("target_object_type"))

    link_type_api_name = str(resource.get("id") or "").strip()
    if not link_type_api_name:
        return None

    out = {"apiName": link_type_api_name}
    if to_ref:
        out["objectTypeApiName"] = to_ref

    display_name = _localized_text(resource.get("label"))
    if display_name:
        out["displayName"] = display_name

    status_value = str(spec.get("status") or resource.get("status") or "ACTIVE").strip().upper()
    out["status"] = status_value or "ACTIVE"

    cardinality = _map_cardinality(spec.get("cardinality") if isinstance(spec, dict) else None)
    if cardinality is None:
        cardinality = _map_cardinality(resource.get("cardinality"))
    if cardinality:
        out["cardinality"] = cardinality

    foreign_key_property = None
    if isinstance(relationship_spec, dict):
        foreign_key_property = relationship_spec.get("fk_column") or relationship_spec.get("source_key_column")
    if isinstance(foreign_key_property, str) and foreign_key_property.strip():
        out["foreignKeyPropertyApiName"] = foreign_key_property.strip()

    link_type_rid = str(resource.get("rid") or "").strip()
    if link_type_rid:
        out["linkTypeRid"] = link_type_rid
    return out


__all__ = [
    "_extract_resources",
    "_normalize_object_ref",
    "_to_foundry_outgoing_link_type",
]
