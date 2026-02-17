"""Ontology extension endpoints (BFF).

This module provides fixed subpaths under `/ontology/*` that must be registered
before generic ontology routes (see `bff.main` include order).

Routers are kept thin: OMS orchestration + error handling lives in
`bff.services.ontology_extensions_service` (Facade), and request schemas live in
`bff.schemas.ontology_extensions_requests`.
"""

from fastapi import APIRouter

router = APIRouter(prefix="/databases/{db_name}/ontology", tags=["Ontology Extensions"])

# Foundry-aligned public surface: legacy v1 ontology extension routes are code-deleted.
# Keep an empty router object to avoid import churn in composition/bootstrap tests.
