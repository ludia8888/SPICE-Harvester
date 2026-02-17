"""
Ontology router composition (BFF).

This module composes ontology endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers and request
schemas live in dedicated modules (Facade).
"""


from fastapi import APIRouter

from bff.routers.ontology_crud import router as crud_router

router = APIRouter(tags=["Ontology Management"])

_BASE_PREFIX = "/databases/{db_name}"

router.include_router(crud_router, prefix=_BASE_PREFIX)
