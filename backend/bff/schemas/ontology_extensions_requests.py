"""Ontology extension request schemas (BFF re-export shim).

Canonical definitions live in ``shared.schemas.ontology_extension_requests``.
"""

from shared.schemas.ontology_extension_requests import (  # noqa: F401
    OntologyDeploymentRecordRequest,
    OntologyResourceRequest,
)

__all__ = ["OntologyResourceRequest", "OntologyDeploymentRecordRequest"]
