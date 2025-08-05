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
    get_elasticsearch_service,
)
from bff.services.oms_client import OMSClient
from shared.security.input_sanitizer import validate_db_name, validate_class_id, validate_instance_id
from shared.services import ElasticsearchService

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/database/{db_name}", tags=["Instance Management"])


@router.get("/class/{class_id}/instances")
async def get_class_instances(
    db_name: str,
    class_id: str,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None, description="Search query for filtering instances"),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    oms_client: OMSClient = Depends(get_oms_client),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 목록 조회 (Elasticsearch 사용)
    
    Event Sourcing + CQRS 패턴에 따라 Elasticsearch에서 
    미리 계산된 최신 상태를 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        
        # 검색어 보안 검증
        if search and len(search) > 100:
            search = search[:100]
        
        # Elasticsearch에서 인스턴스 목록 조회
        index_name = f"{db_name.lower()}_instances"
        
        # 쿼리 구성
        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": class_id}}
                ]
            }
        }
        
        # 검색어가 있는 경우 추가
        if search:
            query["bool"]["must"].append({
                "multi_match": {
                    "query": search,
                    "fields": ["*"],
                    "type": "phrase_prefix"
                }
            })
        
        logger.info(f"Querying instances for class {class_id} from Elasticsearch")
        
        try:
            result = await elasticsearch_service.search(
                index=index_name,
                query=query,
                size=limit,
                from_=offset,
                sort=[{"event_timestamp": {"order": "desc"}}]
            )
            
            # 결과 처리
            instances = []
            total = 0
            
            if result:
                total = result.get("hits", {}).get("total", {}).get("value", 0)
                hits = result.get("hits", {}).get("hits", [])
                
                for hit in hits:
                    instance_data = hit["_source"]
                    instances.append(instance_data)
            
            return {
                "class_id": class_id,
                "total": total,
                "limit": limit,
                "offset": offset,
                "search": search,
                "instances": instances,
                "source": "elasticsearch"
            }
            
        except Exception as es_error:
            logger.error(f"Elasticsearch query failed: {es_error}")
            logger.info("Falling back to TerminusDB query")
            
            # Elasticsearch 장애 시 TerminusDB로 fallback
            # 기존 SPARQL 쿼리 로직 사용
            if search:
                # 특수 문자 이스케이프
                search_escaped = search.replace('"', '\\"').replace("'", "\\'").replace("\\", "\\\\")
                query = f"""
                SELECT DISTINCT ?instance ?property ?value
                WHERE {{
                    ?instance a <{class_id}> .
                    ?instance ?property ?value .
                    FILTER(
                        isLiteral(?value) && 
                        regex(str(?value), "{search_escaped}", "i")
                    )
                }}
                LIMIT {limit}
                OFFSET {offset}
                """
            else:
                query = f"""
                SELECT * 
                WHERE {{
                    ?instance a <{class_id}> .
                    ?instance ?property ?value .
                }}
                LIMIT {limit}
                OFFSET {offset}
                """
            
            # OMS API 호출
            result = await oms_client.query_ontologies(db_name, query)
            
            # 결과 포맷팅
            instances = []
            instance_data = {}
            
            if result and 'results' in result:
                for row in result['results']:
                    instance_id = row.get('instance', '')
                    property_name = row.get('property', '').split('/')[-1]
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
                "instances": instances,
                "source": "terminus_fallback"
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


@router.get("/class/{class_id}/instance/{instance_id}")
async def get_instance(
    db_name: str,
    class_id: str,
    instance_id: str,
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
) -> Dict[str, Any]:
    """
    개별 인스턴스 조회 (Elasticsearch 사용)
    
    Event Sourcing + CQRS 패턴에 따라 Elasticsearch에서 
    미리 계산된 최신 상태를 조회합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        instance_id = validate_instance_id(instance_id)
        
        # Elasticsearch에서 인스턴스 조회
        index_name = f"{db_name.lower()}_instances"
        
        # 특정 인스턴스 ID로 문서 조회
        query = {
            "bool": {
                "must": [
                    {"term": {"instance_id": instance_id}},
                    {"term": {"class_id": class_id}}
                ]
            }
        }
        
        logger.info(f"Querying instance {instance_id} of class {class_id} from Elasticsearch")
        
        result = await elasticsearch_service.search(
            index=index_name,
            query=query,
            size=1
        )
        
        # 결과 처리
        if result and result.get("hits", {}).get("total", {}).get("value", 0) > 0:
            hits = result["hits"]["hits"]
            if hits:
                instance_data = hits[0]["_source"]
                return {
                    "status": "success",
                    "data": instance_data
                }
        
        # 인스턴스를 찾을 수 없는 경우
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get instance from Elasticsearch: {e}")
        
        # Elasticsearch 장애 시 TerminusDB로 fallback
        try:
            logger.info("Falling back to TerminusDB query")
            oms_client = get_oms_client()
            
            # TerminusDB에서 직접 조회
            query = f"""
            SELECT *
            WHERE {{
                <{instance_id}> a <{class_id}> .
                <{instance_id}> ?property ?value .
            }}
            """
            
            result = await oms_client.query_ontologies(db_name, query)
            
            if result and 'results' in result:
                instance_data = {"id": instance_id, "type": class_id}
                for row in result['results']:
                    property_name = row.get('property', '').split('/')[-1]
                    value = row.get('value', '')
                    instance_data[property_name] = value
                
                return {
                    "status": "success",
                    "data": instance_data,
                    "source": "terminus_fallback"
                }
            
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다"
            )
            
        except Exception as fallback_error:
            logger.error(f"Fallback to TerminusDB also failed: {fallback_error}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"인스턴스 조회 실패: {str(e)}"
            )