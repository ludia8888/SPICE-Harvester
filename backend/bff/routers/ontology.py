"""Ontology router composition (BFF)."""

from fastapi import APIRouter, Query, Request, status

from bff.dependencies import LabelMapper, LabelMapperDep, OMSClientDep
from bff.services.database_role_guard import enforce_database_role_or_http_error
from bff.services.input_validation_service import enforce_db_scope_or_403
from bff.services import ontology_crud_service
from bff.services.oms_client import OMSClient
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.models.ontology import OntologyCreateRequestBFF
from shared.observability.tracing import trace_endpoint
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role

router = APIRouter(tags=["Ontology Management"])


async def _require_domain_role(request: Request, *, db_name: str) -> None:
    enforce_db_scope_or_403(request, db_name=db_name)
    await enforce_database_role_or_http_error(
        headers=request.headers,
        db_name=db_name,
        required_roles=DOMAIN_MODEL_ROLES,
        enforce_fn=enforce_database_role,
    )


@router.post(
    "/databases/{db_name}/ontology",
    status_code=status.HTTP_202_ACCEPTED,
    deprecated=True,
    include_in_schema=False,
)
@trace_endpoint("bff.ontology.create_ontology_compat")
async def create_ontology_compat(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    request: Request,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    oms_client: OMSClient = OMSClientDep,
):
    await _require_domain_role(request, db_name=db_name)
    return await ontology_crud_service.create_ontology(
        db_name=db_name,
        body=ontology,
        branch=branch,
        mapper=mapper,
        oms_client=oms_client,
    )
