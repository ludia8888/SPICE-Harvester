from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Protocol, Sequence

from shared.services.pipeline.dataset_output_semantics import validate_dataset_output_metadata


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
class ResolvedOutputKind:
    raw_kind: str
    normalized_kind: str
    used_alias: bool


def _text(payload: Mapping[str, Any], *keys: str) -> str:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _camel_case(value: str) -> str:
    parts = [part for part in str(value or "").split("_") if part]
    if not parts:
        return ""
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


class _DatasetPlugin:
    kind = OUTPUT_KIND_DATASET

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        return validate_dataset_output_metadata(
            definition={},
            output_metadata=payload,
        )


@dataclass(frozen=True)
class _RequiredMetadataPlugin:
    kind: str
    required_fields: Sequence[tuple[str, tuple[str, ...]]]

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        missing = [canonical for canonical, aliases in self.required_fields if not _text(payload, *aliases)]
        if not missing:
            return []
        return [f"missing required metadata ({', '.join(missing)})"]


class _GeotemporalPlugin(_RequiredMetadataPlugin):
    kind = OUTPUT_KIND_GEOTEMPORAL

    def __init__(self) -> None:
        super().__init__(
            kind=OUTPUT_KIND_GEOTEMPORAL,
            required_fields=(
                ("time_column", ("time_column", "timeColumn")),
                ("geometry_column", ("geometry_column", "geometryColumn")),
                ("geometry_format", ("geometry_format", "geometryFormat")),
            ),
        )

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        errors = super().validate(payload)
        if errors:
            return errors
        geometry_format = _text(payload, "geometry_format", "geometryFormat").lower()
        if geometry_format not in {"wkt", "geojson"}:
            return ["geometry_format must be one of: wkt|geojson"]
        return []


class _MediaPlugin(_RequiredMetadataPlugin):
    kind = OUTPUT_KIND_MEDIA

    def __init__(self) -> None:
        super().__init__(
            kind=OUTPUT_KIND_MEDIA,
            required_fields=(
                ("media_uri_column", ("media_uri_column", "mediaUriColumn")),
                ("media_type", ("media_type", "mediaType")),
            ),
        )

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        errors = super().validate(payload)
        if errors:
            return errors
        media_type = _text(payload, "media_type", "mediaType").lower()
        if media_type not in {"image", "video", "audio", "document"}:
            return ["media_type must be one of: image|video|audio|document"]
        return []


class _VirtualPlugin(_RequiredMetadataPlugin):
    kind = OUTPUT_KIND_VIRTUAL

    def __init__(self) -> None:
        super().__init__(
            kind=OUTPUT_KIND_VIRTUAL,
            required_fields=(
                ("query_sql", ("query_sql", "querySql")),
                ("refresh_mode", ("refresh_mode", "refreshMode")),
            ),
        )

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        errors = super().validate(payload)
        if errors:
            return errors
        refresh_mode = _text(payload, "refresh_mode", "refreshMode").lower()
        if refresh_mode not in {"on_read", "scheduled"}:
            return ["refresh_mode must be one of: on_read|scheduled"]
        return []


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
        relationship_spec_type = _text(payload, "relationship_spec_type", "relationshipSpecType").lower()
        has_link_hints = any(
            _text(payload, field, _camel_case(field))
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
            missing = [field for field in self._link_required_fields if not _text(payload, field, _camel_case(field))]
            if missing:
                return [f"missing required link metadata ({', '.join(missing)})"]
            return []

        if not _text(payload, "target_class_id", "targetClassId"):
            return ["target_class_id is required"]
        return []


_PLUGINS: Dict[str, OutputPlugin] = {
    OUTPUT_KIND_DATASET: _DatasetPlugin(),
    OUTPUT_KIND_GEOTEMPORAL: _GeotemporalPlugin(),
    OUTPUT_KIND_MEDIA: _MediaPlugin(),
    OUTPUT_KIND_VIRTUAL: _VirtualPlugin(),
    OUTPUT_KIND_ONTOLOGY: _OntologyPlugin(),
}


def resolve_output_kind(value: Any) -> ResolvedOutputKind:
    raw = str(value or OUTPUT_KIND_DATASET).strip().lower() or OUTPUT_KIND_DATASET
    mapped = OUTPUT_KIND_ALIASES.get(raw, raw)
    if mapped not in SUPPORTED_OUTPUT_KINDS:
        raise ValueError(
            "output_kind must be one of: " + "|".join(sorted(SUPPORTED_OUTPUT_KINDS))
        )
    return ResolvedOutputKind(
        raw_kind=raw,
        normalized_kind=mapped,
        used_alias=raw in OUTPUT_KIND_ALIASES,
    )


def normalize_output_kind(value: Any) -> str:
    return resolve_output_kind(value).normalized_kind


def get_output_plugin(kind: str) -> OutputPlugin:
    normalized = normalize_output_kind(kind)
    return _PLUGINS[normalized]


def validate_output_payload(*, kind: Any, payload: Mapping[str, Any]) -> list[str]:
    plugin = get_output_plugin(str(kind or OUTPUT_KIND_DATASET))
    return plugin.validate(payload)
