"""
Ontology router composition (BFF).

This module composes ontology endpoints using router composition (Composite
pattern) to keep each router small and focused. Shared helpers and request
schemas live in dedicated modules (Facade).
"""


from fastapi import APIRouter

router = APIRouter(tags=["Ontology Management"])
