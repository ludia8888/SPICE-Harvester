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
from shared.security.input_sanitizer import validate_db_name, validate_class_id, validate_instance_id, sanitize_es_query
from shared.services.elasticsearch_service import ElasticsearchService
from shared.config.search_config import get_instances_index_name
from elasticsearch.exceptions import ConnectionError as ESConnectionError, NotFoundError, RequestError

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
        
        # 검색어 보안 검증 및 정제 (엄격한 보안 검사 적용)
        sanitized_search = None
        if search:
            # 1. 길이 제한 (자르지 말고 거부)
            if len(search) > 100:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Search query too long (max 100 characters)"
                )
            
            # 2. 포괄적 보안 검증 (SQL injection, XSS, NoSQL injection 등)
            try:
                from shared.security.input_sanitizer import input_sanitizer
                # 모든 보안 패턴 검사 적용
                validated_search = input_sanitizer.sanitize_string(search, max_length=100)
                # 3. Elasticsearch 전용 정제 추가 적용
                sanitized_search = sanitize_es_query(validated_search)
            except Exception as security_error:
                logger.warning(f"Search query security violation: {security_error}")
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid search query format"
                )
        
        # Elasticsearch에서 인스턴스 목록 조회
        index_name = get_instances_index_name(db_name)
        
        # 쿼리 구성
        query = {
            "bool": {
                "must": [
                    {"term": {"class_id": class_id}}
                ]
            }
        }
        
        # 정제된 검색어가 있는 경우 추가
        if sanitized_search:
            query["bool"]["must"].append({
                "simple_query_string": {
                    "query": sanitized_search,
                    "fields": ["*"],
                    "default_operator": "AND",
                    "analyze_wildcard": False,
                    "allow_leading_wildcard": False
                }
            })
        
        logger.info(f"Querying instances for class {class_id} from Elasticsearch")
        
        # Elasticsearch 조회 시도
        es_result = None
        es_error = None
        try:
            es_result = await elasticsearch_service.search(
                index=index_name,
                query=query,
                size=limit,
                from_=offset,
                sort=[{"event_timestamp": {"order": "desc"}}]
            )
        except (ESConnectionError, ConnectionRefusedError, TimeoutError) as e:
            logger.warning(f"Elasticsearch connection failed, falling back to TerminusDB: {e}")
            es_error = "connection"
        except RequestError as e:
            logger.error(f"Elasticsearch query error: {e}")
            es_error = "query"
        except Exception as e:
            logger.error(f"Unexpected Elasticsearch error, falling back to TerminusDB: {e}")
            es_error = "unknown"
        
        # Elasticsearch 결과 처리
        if es_result and not es_error:
            # 결과 처리
            instances = []
            total = 0
            
            if es_result:
                total = es_result.get("hits", {}).get("total", {}).get("value", 0)
                hits = es_result.get("hits", {}).get("hits", [])
                
                for hit in hits:
                    instance_data = hit["_source"]
                    instances.append(instance_data)
            
            return {
                "class_id": class_id,
                "total": total,
                "limit": limit,
                "offset": offset,
                "search": search,
                "instances": instances
            }
        
        # Fallback to TerminusDB
        if es_error:
            logger.info(f"Elasticsearch unavailable ({es_error}), using TerminusDB fallback for class {class_id} instances")
            
            # Elasticsearch 장애 시 최적화된 OMS Instance API로 fallback
            try:
                result = await oms_client.get_class_instances(
                    db_name=db_name,
                    class_id=class_id,
                    limit=limit,
                    offset=offset,
                    search=search
                )
                
                # API 응답에서 데이터 추출
                if result and result.get("status") == "success":
                    return {
                        "class_id": class_id,
                        "total": result.get("total", 0),
                        "limit": limit,
                        "offset": offset,
                        "search": search,
                        "instances": result.get("instances", [])
                    }
                else:
                    # API 응답이 실패인 경우
                    logger.error(f"OMS Instance API returned error: {result}")
                    return {
                        "class_id": class_id,
                        "total": 0,
                        "limit": limit,
                        "offset": offset,
                        "search": search,
                        "instances": []
                    }
                    
            except Exception as e:
                logger.error(f"Failed to query instances from OMS: {e}")
                return {
                    "class_id": class_id,
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                    "search": search,
                    "instances": []
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

        logger.info(f"Collecting sample values for class {class_id} in database {db_name}")

        # Use optimized OMS instance listing (Document API) and compute samples in-process.
        payload = await oms_client.get_class_instances(db_name, class_id, limit=limit, offset=0)
        instances = payload.get("instances", []) if isinstance(payload, dict) else []
        if not isinstance(instances, list):
            instances = []

        def _normalize_field(name: str) -> str:
            if not name:
                return name
            if "/" in name:
                name = name.rsplit("/", 1)[-1]
            if "#" in name:
                name = name.rsplit("#", 1)[-1]
            return name

        if property_name:
            key = _normalize_field(property_name)
            values: List[Any] = []
            for inst in instances:
                if not isinstance(inst, dict):
                    continue
                if key not in inst:
                    continue
                v = inst.get(key)
                if isinstance(v, list):
                    values.extend(v)
                else:
                    values.append(v)

            return {
                "class_id": class_id,
                "property": property_name,
                "total": len(values),
                "values": values,
            }

        property_values: Dict[str, List[Any]] = {}
        exclude_keys = {"@id", "@type", "class_id", "instance_id"}
        for inst in instances:
            if not isinstance(inst, dict):
                continue
            for k, v in inst.items():
                if k in exclude_keys:
                    continue
                if v is None:
                    continue
                bucket = property_values.setdefault(k, [])
                if isinstance(v, list):
                    bucket.extend(v)
                else:
                    bucket.append(v)

        return {
            "class_id": class_id,
            "total": len(property_values),
            "property_values": property_values,
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
    oms_client: OMSClient = Depends(get_oms_client),  # 의존성 정상 주입
) -> Dict[str, Any]:
    """
    개별 인스턴스 조회 (Elasticsearch 우선, TerminusDB fallback)
    
    Event Sourcing + CQRS 패턴에 따라:
    1. Elasticsearch에서 미리 계산된 최신 상태 조회
    2. 없으면 TerminusDB에서 직접 조회 (Projection 지연 대응)
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        instance_id = validate_instance_id(instance_id)
        
        # 1. Elasticsearch (읽기 모델) 우선 조회
        index_name = get_instances_index_name(db_name)
        
        query = {
            "bool": {
                "must": [
                    {"term": {"instance_id": instance_id}},
                    {"term": {"class_id": class_id}}
                ]
            }
        }
        
        logger.info(f"Querying instance {instance_id} of class {class_id} from Elasticsearch")
        
        # Elasticsearch 조회 시도
        es_found = False
        try:
            result = await elasticsearch_service.search(
                index=index_name,
                query=query,
                size=1
            )
            
            if result and result.get("hits", {}).get("total", {}).get("value", 0) > 0:
                hits = result["hits"]["hits"]
                if hits:
                    instance_data = hits[0]["_source"]
                    return {
                        "status": "success",
                        "data": instance_data
                    }
            
            # ES에 문서가 없는 경우 - TerminusDB fallback
            logger.info(f"Instance {instance_id} not in Elasticsearch index, querying TerminusDB directly for latest state")
            
        except (ESConnectionError, ConnectionRefusedError, TimeoutError) as e:
            # ES 연결 장애 - TerminusDB fallback
            logger.warning(f"Elasticsearch connection error: {e}. Falling back to TerminusDB.")
        except RequestError as e:
            # ES 쿼리 에러 (인덱스가 없는 경우 등)
            logger.warning(f"Elasticsearch request error: {e}. Falling back to TerminusDB.")
        except Exception as e:
            # 예상치 못한 ES 에러
            logger.error(f"Unexpected Elasticsearch error: {e}. Falling back to TerminusDB.")
        
        # 2. Fallback: 최적화된 OMS Instance API 사용
        try:
            result = await oms_client.get_instance(
                db_name=db_name,
                instance_id=instance_id,
                class_id=class_id
            )
            
            if result and result.get("status") == "success":
                instance_data = result.get("data", {})
                if instance_data:
                    return {
                        "status": "success",
                        "data": instance_data
                    }
        except Exception as e:
            logger.error(f"Failed to get instance from OMS: {e}")
        
        # 두 곳 모두에서 찾을 수 없는 경우
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다"
        )
        
    except HTTPException:
        raise
    except Exception as e:
        # 예기치 못한 에러 처리
        logger.error(f"Failed to get instance {instance_id}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"인스턴스 조회 실패: {str(e)}"
        )
