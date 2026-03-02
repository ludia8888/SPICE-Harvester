"""
Instance Management Router
Foundry Object Storage v2 read mapping for instance data

Dataset-primary indexing: objectify_worker/instance_worker가 ES에 직접 write한 데이터를 조회.
"""

import logging
from typing import Any, Dict, Optional
from fastapi import APIRouter, Depends, HTTPException, Query, status

from oms.dependencies import ValidatedDatabaseName
from shared.config.search_config import get_instances_index_name
from shared.dependencies.providers import ElasticsearchServiceDep
from shared.security.input_sanitizer import (
    SecurityViolationError,
    validate_branch_name,
    validate_class_id,
    validate_instance_id,
)
from shared.errors.error_types import ErrorCode, classified_http_exception
from shared.observability.tracing import trace_endpoint

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/instance/{db_name}", tags=["Instance Management"])


@router.get("/class/{class_id}/instances", include_in_schema=False)
@trace_endpoint("oms.instance.get_class_instances")
async def get_class_instances(
    es: ElasticsearchServiceDep,
    db_name: str = Depends(ValidatedDatabaseName),
    class_id: str = ...,
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    branch: str = Query(default="master"),
    search: Optional[str] = Query(default=None, description="Search query"),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 목록을 효율적으로 조회

    Elasticsearch에서 직접 쿼리하여 완전히 조립된 인스턴스 객체 배열을 반환합니다.
    """
    try:
        class_id = validate_class_id(class_id)
        branch = validate_branch_name(branch)

        validated_search = None
        if search:
            if len(search) > 100:
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Search query too long (max 100 characters)",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )
            try:
                from shared.security.input_sanitizer import input_sanitizer
                validated_search = input_sanitizer.sanitize_string(search, max_length=100)
            except Exception as security_error:
                logger.warning(f"Search query security violation in OMS: {security_error}")
                raise classified_http_exception(
                    status.HTTP_400_BAD_REQUEST,
                    "Invalid search query format",
                    code=ErrorCode.REQUEST_VALIDATION_FAILED,
                )

        index_name = get_instances_index_name(db_name, branch=branch)

        query: Dict[str, Any]
        if validated_search:
            query = {
                "bool": {
                    "must": [
                        {"term": {"class_id": class_id}},
                        {"simple_query_string": {
                            "query": validated_search,
                            "fields": ["properties.value", "class_label"],
                            "default_operator": "AND",
                        }},
                    ]
                }
            }
        else:
            query = {"term": {"class_id": class_id}}

        result = await es.search(
            index=index_name,
            query=query,
            size=limit,
            from_=offset,
            sort=[{"instance_id": {"order": "asc"}}],
        )

        return {
            "status": "success",
            "class_id": class_id,
            "total": result.get("total", 0),
            "limit": limit,
            "offset": offset,
            "search": validated_search,
            "branch": branch,
            "instances": result.get("hits", []),
            "source": "elasticsearch"
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_class_instances: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get class instances: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"인스턴스 목록 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/instance/{instance_id}", include_in_schema=False)
@trace_endpoint("oms.instance.get")
async def get_instance(
    es: ElasticsearchServiceDep,
    db_name: str = Depends(ValidatedDatabaseName),
    instance_id: str = ...,
    class_id: Optional[str] = Query(default=None, description="Optional class ID for validation"),
    branch: str = Query(default="master"),
) -> Dict[str, Any]:
    """
    개별 인스턴스를 효율적으로 조회

    Elasticsearch에서 document ID로 직접 조회합니다.
    """
    try:
        instance_id = validate_instance_id(instance_id)
        if class_id:
            class_id = validate_class_id(class_id)
        branch = validate_branch_name(branch)

        index_name = get_instances_index_name(db_name, branch=branch)
        instance = await es.get_document(index=index_name, doc_id=instance_id)

        if not instance:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        if class_id and instance.get("class_id") != class_id:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"인스턴스 '{instance_id}'가 클래스 '{class_id}'에 속하지 않습니다",
                code=ErrorCode.RESOURCE_NOT_FOUND,
            )

        return {
            "status": "success",
            "data": instance,
            "source": "elasticsearch"
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_instance: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get instance {instance_id}: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"인스턴스 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/class/{class_id}/count", include_in_schema=False)
@trace_endpoint("oms.instance.get_class_count")
async def get_class_instance_count(
    es: ElasticsearchServiceDep,
    db_name: str = Depends(ValidatedDatabaseName),
    class_id: str = ...,
    branch: str = Query(default="master"),
) -> Dict[str, Any]:
    """
    특정 클래스의 인스턴스 개수를 효율적으로 조회
    """
    try:
        class_id = validate_class_id(class_id)
        branch = validate_branch_name(branch)

        index_name = get_instances_index_name(db_name, branch=branch)
        count = await es.count(
            index=index_name,
            query={"term": {"class_id": class_id}},
        )

        return {
            "status": "success",
            "class_id": class_id,
            "count": count
        }

    except SecurityViolationError as e:
        logger.warning(f"Security violation in get_class_instance_count: {e}")
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "입력 데이터에 보안 위반이 감지되었습니다",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        )
    except Exception as e:
        logger.error(f"Failed to count instances: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"인스턴스 개수 조회 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )
