"""Link types router composition (BFF).

This module composes link-type endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers, dependencies,
and request schemas live in dedicated modules (Facade) and are re-exported here
for backwards compatibility with existing tests/imports.
"""


from fastapi import APIRouter

from bff.routers import link_types_edits, link_types_read, link_types_write
from bff.routers.link_types_deps import get_dataset_registry
from bff.routers.link_types_edits import create_link_edit, list_link_edits
from bff.routers.link_types_read import (
    get_outgoing_link_type,
    get_link_type,
    list_link_types,
    list_outgoing_link_types,
)
from bff.routers.link_types_write import create_link_type, reindex_link_type, update_link_type
from bff.services.link_types_mapping_service import (
    build_mapping_request as _build_mapping_request,
    compute_schema_hash as _compute_schema_hash,
    ensure_join_dataset as _ensure_join_dataset,
)
from shared.security.database_access import enforce_database_role

# Re-export endpoint functions used directly by tests.

# Re-export helper functions used directly by tests.

# Patch point used by tests.

router = APIRouter(tags=["Ontology Link Types"])

_BASE_PREFIX = "/databases/{db_name}/ontology"

router.include_router(link_types_read.router, prefix=_BASE_PREFIX)
router.include_router(link_types_edits.router, prefix=_BASE_PREFIX)
router.include_router(link_types_write.router, prefix=_BASE_PREFIX)

__all__ = [
    "router",
    "list_link_types",
    "list_outgoing_link_types",
    "get_outgoing_link_type",
    "get_link_type",
    "list_link_edits",
    "create_link_edit",
    "create_link_type",
    "update_link_type",
    "reindex_link_type",
    "get_dataset_registry",
    "enforce_database_role",
    "_build_mapping_request",
    "_ensure_join_dataset",
    "_compute_schema_hash",
]
