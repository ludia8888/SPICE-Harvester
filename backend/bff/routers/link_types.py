"""Link types router composition (BFF).

This module composes link-type endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers, dependencies,
and request schemas live in dedicated modules (Facade) and are re-exported here
for backwards compatibility with existing tests/imports.
"""


from fastapi import APIRouter

from bff.routers import link_types_edits, link_types_read, link_types_write

# Re-export endpoint functions used directly by tests.

# Re-export helper functions used directly by tests.

# Patch point used by tests.

router = APIRouter(tags=["Ontology Link Types"])

_BASE_PREFIX = "/databases/{db_name}/ontology"

router.include_router(link_types_read.router, prefix=_BASE_PREFIX)
router.include_router(link_types_edits.router, prefix=_BASE_PREFIX)
router.include_router(link_types_write.router, prefix=_BASE_PREFIX)
