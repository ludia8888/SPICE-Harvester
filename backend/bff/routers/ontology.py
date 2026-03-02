"""Ontology router composition (BFF)."""

from fastapi import APIRouter, Query, status

from bff.dependencies import LabelMapper, LabelMapperDep, OMSClientDep
from bff.services import ontology_crud_service
from bff.services.oms_client import OMSClient
from shared.models.ontology import OntologyCreateRequestBFF
from shared.observability.tracing import trace_endpoint

router = APIRouter(tags=["Ontology Management"])


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
    branch: str = Query("master", description="Target branch (default: master)"),
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
