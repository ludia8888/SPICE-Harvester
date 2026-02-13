from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Protocol, Sequence


OUTPUT_KIND_DATASET = "dataset"
OUTPUT_KIND_GEOTEMPORAL = "geotemporal"
OUTPUT_KIND_MEDIA = "media"
OUTPUT_KIND_VIRTUAL = "virtual"
OUTPUT_KIND_ONTOLOGY = "ontology"

OUTPUT_KIND_ALIASES: Dict[str, str] = {
    "unknown": OUTPUT_KIND_DATASET,
    "object": OUTPUT_KIND_ONTOLOGY,
    "link": OUTPUT_KIND_ONTOLOGY,
}

SUPPORTED_OUTPUT_KINDS = frozenset(
    {
        OUTPUT_KIND_DATASET,
        OUTPUT_KIND_GEOTEMPORAL,
        OUTPUT_KIND_MEDIA,
        OUTPUT_KIND_VIRTUAL,
        OUTPUT_KIND_ONTOLOGY,
    }
)


class OutputPlugin(Protocol):
    kind: str

    def validate(self, payload: Mapping[str, Any]) -> list[str]: ...


@dataclass(frozen=True)
class _RequiredFieldsPlugin:
    kind: str
    required_fields: Sequence[str]

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        missing = [field for field in self.required_fields if not str(payload.get(field) or "").strip()]
        if not missing:
            return []
        return [f"missing required metadata ({', '.join(missing)})"]


class _OntologyPlugin:
    kind = OUTPUT_KIND_ONTOLOGY

    _link_required_fields = (
        "link_type_id",
        "source_class_id",
        "target_class_id",
        "predicate",
        "cardinality",
        "source_key_column",
        "target_key_column",
        "relationship_spec_type",
    )

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        relationship_spec_type = str(payload.get("relationship_spec_type") or "").strip().lower()
        has_link_hints = any(
            str(payload.get(field) or "").strip()
            for field in (
                "link_type_id",
                "source_class_id",
                "predicate",
                "cardinality",
                "source_key_column",
                "target_key_column",
            )
        )
        if relationship_spec_type in {"link", "relationship", "edge"} or has_link_hints:
            missing = [field for field in self._link_required_fields if not str(payload.get(field) or "").strip()]
            if missing:
                return [f"missing required link metadata ({', '.join(missing)})"]
            return []

        if not str(payload.get("target_class_id") or "").strip():
            return ["target_class_id is required"]
        return []


_PLUGINS: Dict[str, OutputPlugin] = {
    OUTPUT_KIND_DATASET: _RequiredFieldsPlugin(kind=OUTPUT_KIND_DATASET, required_fields=()),
    OUTPUT_KIND_GEOTEMPORAL: _RequiredFieldsPlugin(kind=OUTPUT_KIND_GEOTEMPORAL, required_fields=()),
    OUTPUT_KIND_MEDIA: _RequiredFieldsPlugin(kind=OUTPUT_KIND_MEDIA, required_fields=()),
    OUTPUT_KIND_VIRTUAL: _RequiredFieldsPlugin(kind=OUTPUT_KIND_VIRTUAL, required_fields=()),
    OUTPUT_KIND_ONTOLOGY: _OntologyPlugin(),
}


def normalize_output_kind(value: Any) -> str:
    raw = str(value or OUTPUT_KIND_DATASET).strip().lower() or OUTPUT_KIND_DATASET
    mapped = OUTPUT_KIND_ALIASES.get(raw, raw)
    if mapped not in SUPPORTED_OUTPUT_KINDS:
        raise ValueError(
            "output_kind must be one of: " + "|".join(sorted(SUPPORTED_OUTPUT_KINDS))
        )
    return mapped


def get_output_plugin(kind: str) -> OutputPlugin:
    normalized = normalize_output_kind(kind)
    return _PLUGINS[normalized]


def validate_output_payload(*, kind: Any, payload: Mapping[str, Any]) -> list[str]:
    plugin = get_output_plugin(str(kind or OUTPUT_KIND_DATASET))
    return plugin.validate(payload)
