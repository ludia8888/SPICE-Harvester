"""Link type + relationship spec helpers (BFF).

This module is a thin Facade that re-exports link-type helper logic from the
service layer. It exists to support router composition and to keep backwards
compatibility with existing imports/tests.
"""

from bff.services.link_types_mapping_service import (
    build_join_schema as _build_join_schema,
    build_mapping_request as _build_mapping_request,
    compute_schema_hash as _compute_schema_hash,
    ensure_join_dataset as _ensure_join_dataset,
    extract_ontology_properties as _extract_ontology_properties,
    extract_ontology_relationships as _extract_ontology_relationships,
    extract_schema_columns as _extract_schema_columns,
    extract_schema_types as _extract_schema_types,
    normalize_pk_fields as _normalize_pk_fields,
    normalize_policy as _normalize_policy,
    normalize_spec_type as _normalize_spec_type,
    resolve_dataset_and_version as _resolve_dataset_and_version,
    resolve_object_type_contract as _resolve_object_type_contract,
    resolve_property_type as _resolve_property_type,
)

__all__ = [
    "_build_join_schema",
    "_build_mapping_request",
    "_compute_schema_hash",
    "_ensure_join_dataset",
    "_extract_ontology_properties",
    "_extract_ontology_relationships",
    "_extract_schema_columns",
    "_extract_schema_types",
    "_normalize_pk_fields",
    "_normalize_policy",
    "_normalize_spec_type",
    "_resolve_dataset_and_version",
    "_resolve_object_type_contract",
    "_resolve_property_type",
]

