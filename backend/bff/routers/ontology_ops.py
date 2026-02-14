"""
Ontology router helpers (BFF).

This module acts as a small Facade over repeated normalization / transformation
logic used by multiple ontology subrouters.
"""

from bff.services.ontology_ops_service import (
    _build_sample_data_from_preview,
    _build_source_schema_from_preview,
    _build_target_schema_from_ontology,
    _extract_target_field_types,
    _extract_target_field_types_from_import_schema,
    _localized_to_string,
    _normalize_import_target_type,
    _normalize_mapping_type,
    _normalize_target_schema_for_mapping,
    _transform_properties_for_oms,
)

__all__ = [
    "_build_sample_data_from_preview",
    "_build_source_schema_from_preview",
    "_build_target_schema_from_ontology",
    "_extract_target_field_types",
    "_extract_target_field_types_from_import_schema",
    "_localized_to_string",
    "_normalize_import_target_type",
    "_normalize_mapping_type",
    "_normalize_target_schema_for_mapping",
    "_transform_properties_for_oms",
]
