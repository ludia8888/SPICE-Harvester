"""
레이블 매핑 관리 라우터
레이블 매핑 내보내기/가져오기를 담당
"""

import logging

from fastapi import APIRouter, Depends, File, UploadFile

from bff.dependencies import LabelMapper, OMSClient, get_label_mapper, get_oms_client
from bff.services import label_mapping_service
from shared.models.requests import ApiResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}/mappings", tags=["Label Mappings"])


@router.post("/export")
async def export_mappings(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.export_mappings(db_name=db_name, mapper=mapper)


@router.post("/import", response_model=ApiResponse)
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
async def get_mappings_summary(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.get_mappings_summary(db_name=db_name, mapper=mapper)


@router.delete("/")
async def clear_mappings(db_name: str, mapper: LabelMapper = Depends(get_label_mapper)):
    return await label_mapping_service.clear_mappings(db_name=db_name, mapper=mapper)

