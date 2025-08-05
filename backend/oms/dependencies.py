"""
OMS Dependencies
서비스 의존성 관리 모듈
"""

from fastapi import HTTPException, status, Path, Depends
from typing import Annotated

from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

# shared imports
from shared.utils.jsonld import JSONToJSONLDConverter

# Import shared label mapper
from shared.utils.label_mapper import LabelMapper

# Import validation functions
from shared.security.input_sanitizer import validate_db_name, validate_class_id

# 전역 서비스 인스턴스
terminus_service = None
jsonld_converter = None
label_mapper = None
outbox_service = None


def set_services(terminus: AsyncTerminusService, converter: JSONToJSONLDConverter, outbox=None):
    """서비스 인스턴스 설정"""
    global terminus_service, jsonld_converter, label_mapper, outbox_service
    terminus_service = terminus
    jsonld_converter = converter
    label_mapper = LabelMapper()
    outbox_service = outbox


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


def get_outbox_service():
    """Outbox 서비스 의존성"""
    if not outbox_service:
        # Outbox 서비스가 없어도 에러를 발생시키지 않고 None 반환
        # 이벤트 발행은 선택적 기능이므로
        return None
    return outbox_service


# Validation Dependencies
def ValidatedDatabaseName(db_name: str = Path(..., description="데이터베이스 이름")) -> str:
    """데이터베이스 이름 검증 의존성"""
    try:
        return validate_db_name(db_name)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"잘못된 데이터베이스 이름: {str(e)}"
        )


def ValidatedClassId(class_id: str = Path(..., description="클래스 ID")) -> str:
    """클래스 ID 검증 의존성"""
    try:
        return validate_class_id(class_id)
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"잘못된 클래스 ID: {str(e)}"
        )


# Combined validation for database existence check
async def ensure_database_exists(
    db_name: Annotated[str, ValidatedDatabaseName],
    terminus: AsyncTerminusService = Depends(get_terminus_service)
) -> str:
    """데이터베이스 존재 확인 및 검증된 이름 반환"""
    try:
        dbs = await terminus.list_databases()
        # 올바른 방식: 딕셔너리 리스트에서 name 필드 확인
        db_exists = any(db.get('name') == db_name for db in dbs if isinstance(db, dict))
        if not db_exists:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'이(가) 존재하지 않습니다"
            )
        return db_name
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"데이터베이스 확인 실패: {str(e)}"
        )
