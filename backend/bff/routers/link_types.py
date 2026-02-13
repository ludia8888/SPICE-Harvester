"""Link types router composition (BFF).

This module composes link-type endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers, dependencies,
and request schemas live in dedicated modules (Facade) and are re-exported here
for backwards compatibility with existing tests/imports.
"""


from fastapi import APIRouter

from bff.routers import link_types_edits, link_types_read, link_types_write
from bff.routers.link_types_deps import get_dataset_registry, get_objectify_registry

# Re-export endpoint functions used directly by tests.
from bff.routers.link_types_read import get_link_type, list_link_types

# Re-export helper functions used directly by tests.
from bff.routers.link_types_ops import _build_mapping_request, _compute_schema_hash, _ensure_join_dataset

# Patch point used by tests.
from shared.security.database_access import enforce_database_role

router = APIRouter(tags=["Ontology Link Types"])

_BASE_PREFIX = "/databases/{db_name}/ontology"

router.include_router(link_types_read.router, prefix=_BASE_PREFIX)
router.include_router(link_types_edits.router, prefix=_BASE_PREFIX)
router.include_router(link_types_write.router, prefix=_BASE_PREFIX)
