"""
레이블 매핑 관리 라우터
레이블 매핑 내보내기/가져오기를 담당
"""

import logging
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, File, UploadFile

from bff.dependencies import LabelMapper, OMSClient, get_label_mapper, get_oms_client
from bff.services import label_mapping_service
from shared.models.requests import ApiResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/mappings", tags=["Label Mappings"])


@router.post("/export")
@trace_endpoint("bff.mapping.export_mappings")
async def export_mappings(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.export_mappings(db_name=db_name, mapper=mapper)


@router.post("/import", response_model=ApiResponse)
@trace_endpoint("bff.mapping.import_mappings")
async def import_mappings(
    db_name: str,
    file: UploadFile = File(...),
    mapper: LabelMapper = Depends(get_label_mapper),
    oms_client: OMSClient = Depends(get_oms_client),
):
    return await label_mapping_service.import_mappings(
        db_name=db_name,
        file=file,
        mapper=mapper,
        oms_client=oms_client,
    )


@router.post("/validate", response_model=ApiResponse)
@trace_endpoint("bff.mapping.validate_mappings")
async def validate_mappings(
    db_name: str,
    file: UploadFile = File(...),
    mapper: LabelMapper = Depends(get_label_mapper),
    oms_client: OMSClient = Depends(get_oms_client),
):
    return await label_mapping_service.validate_mappings(
        db_name=db_name,
        file=file,
        mapper=mapper,
        oms_client=oms_client,
    )


@router.get("/")
@trace_endpoint("bff.mapping.get_mappings_summary")
async def get_mappings_summary(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.get_mappings_summary(db_name=db_name, mapper=mapper)


@router.delete("/")
@trace_endpoint("bff.mapping.clear_mappings")
async def clear_mappings(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.clear_mappings(db_name=db_name, mapper=mapper)

