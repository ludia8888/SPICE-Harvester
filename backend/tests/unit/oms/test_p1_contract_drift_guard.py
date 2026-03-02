"""Guard test: OMS internal-only paths must NOT appear in OMS OpenAPI.

P1 contract drift consolidation — BFF paths are the canonical public contract.
OMS paths are internal (BFF proxies via OMSClient) and hidden from OpenAPI.
"""

from __future__ import annotations

import pytest

from oms.main import app

# OMS paths that are internal-only.  BFF exposes the public contract.
# Group A: /database/* singular  (BFF canonical: /databases/* plural)
# Group B: /instances/*/async    (BFF canonical: /databases/*/instances/*)
OMS_INTERNAL_ONLY_PATHS: tuple[str, ...] = (
    # --- Group A: database singular ------------------------------------------
    "/api/v1/database/list",
    "/api/v1/database/create",
    "/api/v1/database/{db_name}",
    "/api/v1/database/exists/{db_name}",
    # --- Group A: ontology under /database (singular) ------------------------
    "/api/v1/database/{db_name}/ontology",
    "/api/v1/database/{db_name}/ontology/validate",
    "/api/v1/database/{db_name}/ontology/{class_id}",
    "/api/v1/database/{db_name}/ontology/resources",
    "/api/v1/database/{db_name}/ontology/resources/{resource_type}",
    "/api/v1/database/{db_name}/ontology/resources/{resource_type}/{resource_id}",
    "/api/v1/database/{db_name}/ontology/records/deployments",
    # --- Group B: instance async under /instances ----------------------------
    "/api/v1/instances/{db_name}/async/{class_id}/create",
    "/api/v1/instances/{db_name}/async/{class_id}/bulk-create",
    "/api/v1/instances/{db_name}/async/{class_id}/bulk-create-tracked",
    "/api/v1/instances/{db_name}/async/{class_id}/bulk-update",
    "/api/v1/instances/{db_name}/async/{class_id}/{instance_id}/update",
    "/api/v1/instances/{db_name}/async/{class_id}/{instance_id}/delete",
    "/api/v1/instances/{db_name}/async/command/{command_id}/status",
)


@pytest.mark.unit
def test_oms_internal_paths_hidden_from_openapi() -> None:
    """Internal-only OMS paths must not appear in the OMS OpenAPI schema."""
    paths = set((app.openapi() or {}).get("paths", {}).keys())
    for path in OMS_INTERNAL_ONLY_PATHS:
        assert path not in paths, (
            f"Internal-only OMS path still exposed in OpenAPI: {path}"
        )
