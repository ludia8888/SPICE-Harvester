from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Protocol, Sequence

from shared.services.pipeline.dataset_output_semantics import validate_dataset_output_metadata
from shared.utils.mapping_text import first_mapping_text


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
def _camel_case(value: str) -> str:
    parts = [part for part in str(value or "").split("_") if part]
    if not parts:
        return ""
    return parts[0] + "".join(part.capitalize() for part in parts[1:])


def find_virtual_dataset_style_settings(payload: Mapping[str, Any]) -> list[str]:
    unsupported: List[str] = []
    for aliases in _VirtualPlugin._UNSUPPORTED_DATASET_STYLE_KEYS:
        value = first_mapping_text(keys=aliases, sources=(payload,))
        if value:
            unsupported.append(aliases[0])
            continue
        raw = payload.get(aliases[0])
        if isinstance(raw, list) and raw:
            unsupported.append(aliases[0])
    return sorted(set(unsupported))


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
        missing = [
            canonical
            for canonical, aliases in self.required_fields
            if not first_mapping_text(keys=aliases, sources=(payload,))
        ]
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
        geometry_format = first_mapping_text(
            keys=("geometry_format", "geometryFormat"),
            sources=(payload,),
        ).lower()
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
        media_type = first_mapping_text(
            keys=("media_type", "mediaType"),
            sources=(payload,),
        ).lower()
        if media_type not in {"image", "video", "audio", "document"}:
            return ["media_type must be one of: image|video|audio|document"]
        return []


class _VirtualPlugin(_RequiredMetadataPlugin):
    kind = OUTPUT_KIND_VIRTUAL
    _UNSUPPORTED_DATASET_STYLE_KEYS = (
        ("write_mode", "writeMode"),
        ("primary_key_columns", "primaryKeyColumns"),
        ("post_filtering_column", "postFilteringColumn"),
        ("output_format", "outputFormat", "format"),
        ("partition_by", "partitionBy"),
    )

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
        refresh_mode = first_mapping_text(
            keys=("refresh_mode", "refreshMode"),
            sources=(payload,),
        ).lower()
        if refresh_mode not in {"on_read", "scheduled"}:
            return ["refresh_mode must be one of: on_read|scheduled"]

        unsupported = find_virtual_dataset_style_settings(payload)
        if unsupported:
            return [
                "virtual output does not support dataset write settings: "
                + ", ".join(unsupported)
            ]
        return []


@dataclass(frozen=True)
class OntologyOutputSemantics:
    relationship_spec_type: str
    required_columns: tuple[str, ...]

_ONTOLOGY_LINK_REQUIRED_FIELDS = (
    "link_type_id",
    "source_class_id",
    "target_class_id",
    "predicate",
    "cardinality",
    "source_key_column",
    "target_key_column",
    "relationship_spec_type",
)
_ONTOLOGY_LINK_SPEC_TYPES = frozenset({"link", "relationship", "edge"})
_ONTOLOGY_OBJECT_SPEC_TYPES = frozenset({"object", "object_type", "node"})
_SUPPORTED_ONTOLOGY_SPEC_TYPES = frozenset(set(_ONTOLOGY_LINK_SPEC_TYPES) | set(_ONTOLOGY_OBJECT_SPEC_TYPES))


def resolve_ontology_output_semantics(payload: Mapping[str, Any]) -> OntologyOutputSemantics:
    relationship_spec_type = first_mapping_text(
        keys=("relationship_spec_type", "relationshipSpecType"),
        sources=(payload,),
    ).lower()
    if relationship_spec_type and relationship_spec_type not in _SUPPORTED_ONTOLOGY_SPEC_TYPES:
        allowed = "|".join(sorted(_SUPPORTED_ONTOLOGY_SPEC_TYPES))
        raise ValueError(f"relationship_spec_type must be one of: {allowed}")

    has_link_hints = any(
        first_mapping_text(keys=(field, _camel_case(field)), sources=(payload,))
        for field in (
            "link_type_id",
            "source_class_id",
            "predicate",
            "cardinality",
            "source_key_column",
            "target_key_column",
        )
    )
    if relationship_spec_type in _ONTOLOGY_LINK_SPEC_TYPES or has_link_hints:
        missing = [
            field
            for field in _ONTOLOGY_LINK_REQUIRED_FIELDS
            if not first_mapping_text(keys=(field, _camel_case(field)), sources=(payload,))
        ]
        if missing:
            raise ValueError(f"missing required link metadata ({', '.join(missing)})")
        source_key_column = first_mapping_text(
            keys=("source_key_column", "sourceKeyColumn"),
            sources=(payload,),
        )
        target_key_column = first_mapping_text(
            keys=("target_key_column", "targetKeyColumn"),
            sources=(payload,),
        )
        return OntologyOutputSemantics(
            relationship_spec_type="link",
            required_columns=tuple(column for column in (source_key_column, target_key_column) if column),
        )

    if not first_mapping_text(keys=("target_class_id", "targetClassId"), sources=(payload,)):
        raise ValueError("target_class_id is required")

    source_key_column = first_mapping_text(
        keys=("source_key_column", "sourceKeyColumn"),
        sources=(payload,),
    )
    target_key_column = first_mapping_text(
        keys=("target_key_column", "targetKeyColumn"),
        sources=(payload,),
    )
    return OntologyOutputSemantics(
        relationship_spec_type="object",
        required_columns=tuple(column for column in (source_key_column, target_key_column) if column),
    )


class _OntologyPlugin:
    kind = OUTPUT_KIND_ONTOLOGY

    def validate(self, payload: Mapping[str, Any]) -> list[str]:
        try:
            resolve_ontology_output_semantics(payload)
        except ValueError as exc:
            return [str(exc)]
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
