"""
쿼리 라우터
Foundry Search Objects v2 기반 데이터 쿼리를 담당
"""

import logging
from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter, Depends, HTTPException, Request, Response, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.dependencies import (
    FoundryQueryService,
    LabelMapper,
    get_foundry_query_service,
    get_label_mapper,
)
from bff.routers.registry_deps import get_dataset_registry
from bff.utils.deprecation_headers import apply_v1_to_v2_deprecation_headers
from shared.models.ontology import QueryInput, QueryResponse
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.access_policy import apply_access_policy

# Security validation imports
from shared.security.input_sanitizer import validate_db_name

# Add shared path for common utilities
from shared.utils.language import get_accept_language

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}", tags=["Query"])


@router.post("/query", response_model=QueryResponse)
@trace_endpoint("bff.query.execute_query")
async def execute_query(
    db_name: str,
    query: QueryInput,
    request: Request,
    response: Response = None,
    mapper: LabelMapper = Depends(get_label_mapper),
    query_service: FoundryQueryService = Depends(get_foundry_query_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    """
    온톨로지 쿼리 실행

    레이블 기반 쿼리를 실행하여 온톨로지 데이터를 조회합니다.

    예시:
    ```json
    {
        "class_label": "제품",
        "filters": [
            {"field_label": "가격", "operator": ">=", "value": 10000},
            {"field_label": "카테고리", "operator": "=", "value": "전자제품"}
        ],
        "select": ["이름", "가격", "설명"],
        "order_by": "가격",
        "order_direction": "desc",
        "limit": 10
    }
    ```
    """
    lang = get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        if response is not None:
            apply_v1_to_v2_deprecation_headers(
                response,
                successor_path="/api/v2/ontologies/{ontology}/objects/{objectType}/search",
            )
        # 쿼리 입력을 딕셔너리로 변환
        query_dict = query.model_dump(exclude_unset=True)

        # 레이블 기반 쿼리를 내부 ID 기반으로 변환
        internal_query = await mapper.convert_query_to_internal(db_name, query_dict, lang)

        # 쿼리 실행 (OMS를 통해)
        result = await query_service.query_database(db_name, internal_query)

        raw_results = result.get("data", []) if isinstance(result, dict) else []
        access_filtered = False
        if isinstance(raw_results, list):
            class_id = str(internal_query.get("class_id") or "").strip()
            if class_id:
                policy = await dataset_registry.get_access_policy(
                    db_name=db_name,
                    scope="data_access",
                    subject_type="object_type",
                    subject_id=class_id,
                )
                if policy:
                    raw_results, _ = apply_access_policy(raw_results, policy=policy.policy)
                    access_filtered = True
        labeled_results = await mapper.convert_to_display_batch(db_name, raw_results, lang)

        return {
            "results": labeled_results,
            "total": len(labeled_results)
            if access_filtered
            else ((result.get("count") if isinstance(result, dict) else None) or len(labeled_results)),
            "query": query_dict,
        }

    except ValueError as e:
        # 레이블을 찾을 수 없는 경우
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(e), code=ErrorCode.REQUEST_VALIDATION_FAILED)
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR, f"쿼리 실행 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )


@router.get("/query/builder")
@trace_endpoint("bff.query.query_builder_info")
async def query_builder_info():
    """
    쿼리 빌더 정보

    쿼리 작성을 위한 사용 가능한 연산자와 옵션을 반환합니다.
    """
    # 보안 검증: 이 엔드포인트는 정적 정보만 반환하므로 validate_db_name 불필요
    # 공개 정보이므로 추가 입력 검증이 필요하지 않음
    
    return {
        "dsl": "SearchJsonQueryV2",
        "operators": [
            "eq",
            "gt",
            "lt",
            "gte",
            "lte",
            "isNull",
            "contains",
            "containsAnyTerm",
            "containsAllTerms",
            "containsAllTermsInOrder",
            "containsAllTermsInOrderPrefixLastTerm",
            "and",
            "or",
            "not",
        ],
        "aliases": {
            "startsWith": "containsAllTermsInOrderPrefixLastTerm",
        },
        "notes": [
            "startsWith is supported as a deprecated alias for containsAllTermsInOrderPrefixLastTerm.",
        ],
        "pagination": {
            "pageSize": "1..1000",
            "pageToken": "opaque token from previous response (expires)",
            "invariants": "Reuse pageToken only with identical pageSize and request parameters",
        },
        "request_fields": [
            "where",
            "orderBy",
            "pageSize",
            "pageToken",
            "select",
            "selectV2",
            "excludeRid",
            "snapshot",
        ],
        "examples": {
            "simple_eq": {
                "where": {"type": "eq", "field": "status", "value": "ACTIVE"},
                "select": ["customer_id", "status"],
                "orderBy": {"orderType": "fields", "fields": [{"field": "customer_id", "direction": "asc"}]},
                "pageSize": 25,
            },
            "compound": {
                "where": {
                    "type": "and",
                    "value": [
                        {"type": "gte", "field": "amount", "value": 1000},
                        {
                            "type": "or",
                            "value": [
                                {
                                    "type": "containsAllTermsInOrderPrefixLastTerm",
                                    "field": "customer_name",
                                    "value": "Kim",
                                },
                                {"type": "containsAnyTerm", "field": "customer_name", "value": "Park"},
                            ],
                        },
                    ],
                },
                "pageSize": 50,
                "pageToken": "opaque-page-token",
            },
        },
    }
