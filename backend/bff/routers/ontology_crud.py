"""Ontology CRUD endpoints (BFF).

Composed by `bff.routers.ontology` via router composition (Composite pattern).

Business logic lives in `bff.services.ontology_crud_service` (Facade).
"""

from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Query, status

from bff.dependencies import LabelMapper, LabelMapperDep, OMSClientDep
from bff.services import ontology_crud_service
from bff.services.oms_client import OMSClient
from shared.models.ontology import OntologyCreateRequestBFF
from shared.models.responses import ApiResponse

router = APIRouter(tags=["Ontology Management"])


@router.post(
    "/ontology",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
@trace_endpoint("bff.ontology.create_ontology")
async def create_ontology(
    db_name: str,
    ontology: OntologyCreateRequestBFF,
    branch: str = Query("main", description="Target branch (default: main)"),
    mapper: LabelMapper = LabelMapperDep,
    oms_client: OMSClient = OMSClientDep,
):
    return await ontology_crud_service.create_ontology(
        db_name=db_name,
        body=ontology,
        branch=branch,
        mapper=mapper,
        oms_client=oms_client,
    )

