"""
Ontology router composition (BFF).

This module composes ontology endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers and request
schemas live in dedicated modules (Facade) and are re-exported here for
backwards compatibility.
"""

from __future__ import annotations

from fastapi import APIRouter

from bff.routers.ontology_crud import router as crud_router
from bff.routers.ontology_imports import router as imports_router
from bff.routers.ontology_metadata import router as metadata_router
from bff.routers.ontology_relationships import router as relationships_router
from bff.routers.ontology_suggestions import router as suggestions_router
from bff.routers.ontology_ops import (
    _build_sample_data_from_preview,
    _build_source_schema_from_preview,
    _extract_target_field_types_from_import_schema,
    _localized_to_string,
    _normalize_mapping_type,
    _transform_properties_for_oms,
)
from bff.schemas.ontology_requests import (
    ImportFieldMapping,
    ImportFromGoogleSheetsRequest,
    ImportTargetField,
    MappingFromGoogleSheetsRequest,
    MappingSuggestionRequest,
    SchemaFromDataRequest,
    SchemaFromGoogleSheetsRequest,
)

router = APIRouter(tags=["Ontology Management"])

_BASE_PREFIX = "/databases/{db_name}"

router.include_router(crud_router, prefix=_BASE_PREFIX)
router.include_router(relationships_router, prefix=_BASE_PREFIX)
router.include_router(suggestions_router, prefix=_BASE_PREFIX)
router.include_router(imports_router, prefix=_BASE_PREFIX)
router.include_router(metadata_router, prefix=_BASE_PREFIX)

