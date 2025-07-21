"""
OMS Dependencies
서비스 의존성 관리 모듈
"""

from fastapi import HTTPException, status

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

# shared imports
from shared.utils.jsonld import JSONToJSONLDConverter

# Import shared label mapper
from shared.utils.label_mapper import LabelMapper

# 전역 서비스 인스턴스
terminus_service = None
jsonld_converter = None
label_mapper = None


def set_services(terminus: AsyncTerminusService, converter: JSONToJSONLDConverter):
    """서비스 인스턴스 설정"""
    global terminus_service, jsonld_converter, label_mapper
    terminus_service = terminus
    jsonld_converter = converter
    label_mapper = LabelMapper()


def get_terminus_service() -> AsyncTerminusService:
    """TerminusDB 서비스 의존성"""
    if not terminus_service:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="TerminusDB 서비스가 초기화되지 않았습니다",
        )
    return terminus_service


def get_jsonld_converter() -> JSONToJSONLDConverter:
    """JSON-LD 변환기 의존성"""
    if not jsonld_converter:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="JSON-LD 변환기가 초기화되지 않았습니다",
        )
    return jsonld_converter


def get_label_mapper() -> LabelMapper:
    """레이블 매퍼 의존성"""
    if not label_mapper:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="레이블 매퍼가 초기화되지 않았습니다",
        )
    return label_mapper
