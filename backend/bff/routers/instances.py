"""
인스턴스 관련 API 라우터
온톨로지 클래스의 실제 인스턴스 데이터 조회
"""

import logging
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status
from bff.dependencies import (
    TerminusService,
    get_terminus_service,
    get_oms_client,
)
from bff.services.oms_client import OMSClient
from shared.security.input_sanitizer import validate_db_name, validate_class_id

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}", tags=["Instance Management"])


@router.get("/class/{class_id}/instances")
async def get_class_instances(
    db_name: str,
    class_id: str,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None, description="Search query for filtering instances"),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 샘플 조회
    
    매핑 제안을 위한 값 분포 분석에 사용됩니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        
        # 검색어 보안 검증 - SPARQL injection 방지
        if search:
            # 특수 문자 이스케이프
            search = search.replace('"', '\\"').replace("'", "\\'").replace("\\", "\\\\")
            # 길이 제한
            if len(search) > 100:
                search = search[:100]
        
        # OMS를 통해 인스턴스 조회
        # 쿼리 구성
        if search:
            # 검색어가 있는 경우, 문자열 값에서 검색
            query = f"""
            SELECT DISTINCT ?instance ?property ?value
            WHERE {{
                ?instance a <{class_id}> .
                ?instance ?property ?value .
                FILTER(
                    isLiteral(?value) && 
                    regex(str(?value), "{search}", "i")
                )
            }}
            LIMIT {limit}
            OFFSET {offset}
            """
        else:
            # 기본 쿼리
            query = f"""
            SELECT * 
            WHERE {{
                ?instance a <{class_id}> .
                ?instance ?property ?value .
            }}
            LIMIT {limit}
            OFFSET {offset}
            """
        
        logger.info(f"Querying instances for class {class_id} in database {db_name}")
        
        # OMS API 호출
        result = await oms_client.query_ontologies(db_name, query)
        
        # 결과 포맷팅
        instances = []
        instance_data = {}
        
        if result and 'results' in result:
            for row in result['results']:
                instance_id = row.get('instance', '')
                property_name = row.get('property', '').split('/')[-1]  # Extract property name
                value = row.get('value', '')
                
                if instance_id not in instance_data:
                    instance_data[instance_id] = {'id': instance_id}
                
                instance_data[instance_id][property_name] = value
            
            instances = list(instance_data.values())
        
        return {
            "class_id": class_id,
            "total": len(instances),
            "limit": limit,
            "offset": offset,
            "search": search,
            "instances": instances
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get class instances: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 조회 실패: {str(e)}"
        )


@router.get("/class/{class_id}/sample-values")
async def get_class_sample_values(
    db_name: str,
    class_id: str,
    property_name: Optional[str] = None,
    limit: int = Query(default=200, le=500),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    """
    특정 클래스/속성의 샘플 값 조회
    
    값 분포 분석을 위한 간단한 값 목록만 반환합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        
        # 특정 속성만 조회하는 쿼리
        if property_name:
            query = f"""
            SELECT ?value
            WHERE {{
                ?instance a <{class_id}> .
                ?instance <{property_name}> ?value .
            }}
            LIMIT {limit}
            """
        else:
            # 모든 속성 조회
            query = f"""
            SELECT ?property ?value
            WHERE {{
                ?instance a <{class_id}> .
                ?instance ?property ?value .
            }}
            LIMIT {limit}
            """
        
        logger.info(f"Querying sample values for class {class_id} in database {db_name}")
        
        # OMS API 호출
        result = await oms_client.query_ontologies(db_name, query)
        
        # 결과 포맷팅
        if property_name:
            # 단일 속성 값들
            values = []
            if result and 'results' in result:
                values = [row.get('value', '') for row in result['results']]
            
            return {
                "class_id": class_id,
                "property": property_name,
                "total": len(values),
                "values": values
            }
        else:
            # 속성별 값들
            property_values = {}
            if result and 'results' in result:
                for row in result['results']:
                    prop = row.get('property', '').split('/')[-1]
                    value = row.get('value', '')
                    
                    if prop not in property_values:
                        property_values[prop] = []
                    property_values[prop].append(value)
            
            return {
                "class_id": class_id,
                "total": len(property_values),
                "property_values": property_values
            }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get sample values: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"샘플 값 조회 실패: {str(e)}"
        )