"""
Instance Management Router
효율적인 인스턴스 데이터 조회를 위한 전용 API

N+1 Query 문제를 해결하기 위해 설계된 최적화된 엔드포인트
"""

import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status

from oms.dependencies import ValidatedDatabaseName, get_terminus_service
from shared.services.terminus_service import AsyncTerminusService
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_class_id,
    validate_instance_id,
)

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instance/{db_name}", tags=["Instance Management"])


@router.get("/class/{class_id}/instances")
async def get_class_instances(
    db_name: str = Depends(ValidatedDatabaseName),
    class_id: str = ...,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None, description="Search query"),
    terminus: AsyncTerminusService = Depends(get_terminus_service),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 목록을 효율적으로 조회
    
    최적화된 쿼리를 사용하여 N+1 Query 문제를 방지하고,
    완전히 조립된 인스턴스 객체 배열을 반환합니다.
    
    Args:
        db_name: 데이터베이스 이름
        class_id: 클래스 ID
        limit: 최대 결과 수
        offset: 시작 위치
        search: 검색 쿼리 (선택사항)
        
    Returns:
        완전히 조립된 인스턴스 객체 배열
    """
    try:
        # 입력 검증
        class_id = validate_class_id(class_id)
        
        logger.info(f"Getting instances for class {class_id} in database {db_name}")
        
        # 최적화된 인스턴스 조회 (TerminusService의 새 메서드 사용)
        result = await terminus.get_class_instances_optimized(
            db_name=db_name,
            class_id=class_id,
            limit=limit,
            offset=offset,
            search=search
        )
        
        return {
            "status": "success",
            "class_id": class_id,
            "total": result.get("total", 0),
            "limit": limit,
            "offset": offset,
            "search": search,
            "instances": result.get("instances", []),
            "source": "terminus_optimized"
        }
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_class_instances: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get class instances: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 목록 조회 실패: {str(e)}"
        )


@router.get("/instance/{instance_id}")
async def get_instance(
    db_name: str = Depends(ValidatedDatabaseName),
    instance_id: str = ...,
    class_id: Optional[str] = Query(default=None, description="Optional class ID for validation"),
    terminus: AsyncTerminusService = Depends(get_terminus_service),
) -> Dict[str, Any]:
    """
    개별 인스턴스를 효율적으로 조회
    
    모든 속성을 포함한 완전한 인스턴스 객체를 반환합니다.
    
    Args:
        db_name: 데이터베이스 이름
        instance_id: 인스턴스 ID
        class_id: 클래스 ID (선택사항, 검증용)
        
    Returns:
        완전한 인스턴스 객체
    """
    try:
        # 입력 검증
        instance_id = validate_instance_id(instance_id)
        if class_id:
            class_id = validate_class_id(class_id)
        
        logger.info(f"Getting instance {instance_id} from database {db_name}")
        
        # 최적화된 개별 인스턴스 조회
        instance = await terminus.get_instance_optimized(
            db_name=db_name,
            instance_id=instance_id,
            class_id=class_id
        )
        
        if not instance:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다"
            )
        
        return {
            "status": "success",
            "data": instance,
            "source": "terminus_optimized"
        }
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_instance: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다"
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get instance {instance_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 조회 실패: {str(e)}"
        )


@router.get("/class/{class_id}/count")
async def get_class_instance_count(
    db_name: str = Depends(ValidatedDatabaseName),
    class_id: str = ...,
    terminus: AsyncTerminusService = Depends(get_terminus_service),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 개수를 효율적으로 조회
    
    COUNT 쿼리를 사용하여 빠르게 개수만 반환합니다.
    
    Args:
        db_name: 데이터베이스 이름
        class_id: 클래스 ID
        
    Returns:
        인스턴스 개수
    """
    try:
        # 입력 검증
        class_id = validate_class_id(class_id)
        
        logger.info(f"Counting instances for class {class_id} in database {db_name}")
        
        # 인스턴스 개수 조회
        count = await terminus.count_class_instances(
            db_name=db_name,
            class_id=class_id
        )
        
        return {
            "status": "success",
            "class_id": class_id,
            "count": count
        }
        
    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_class_instance_count: {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="입력 데이터에 보안 위반이 감지되었습니다"
        )
    except Exception as e:
        logger.error(f"Failed to count instances: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 개수 조회 실패: {str(e)}"
        )


@router.post("/sparql")
async def execute_sparql_query(
    db_name: str = Depends(ValidatedDatabaseName),
    query: str = ...,
    limit: Optional[int] = Query(default=None, le=10000),
    offset: Optional[int] = Query(default=None, ge=0),
    terminus: AsyncTerminusService = Depends(get_terminus_service),
) -> Dict[str, Any]:
    """
    SPARQL 쿼리 직접 실행
    
    기존 SPARQL 기반 코드와의 호환성을 위한 엔드포인트입니다.
    
    Args:
        db_name: 데이터베이스 이름
        query: SPARQL 쿼리 문자열
        limit: 최대 결과 수 (선택사항)
        offset: 시작 위치 (선택사항)
        
    Returns:
        SPARQL 쿼리 결과
    """
    try:
        logger.info(f"Executing SPARQL query on database {db_name}")
        
        # SPARQL 쿼리 실행
        result = await terminus.execute_sparql(
            db_name=db_name,
            sparql_query=query,
            limit=limit,
            offset=offset
        )
        
        return {
            "status": "success",
            "results": result.get("results", []),
            "total": result.get("total", 0)
        }
        
    except Exception as e:
        logger.error(f"Failed to execute SPARQL query: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"SPARQL 쿼리 실행 실패: {str(e)}"
        )