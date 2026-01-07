"""
쿼리 라우터
온톨로지 데이터 쿼리를 담당
"""

import logging
from typing import Any, Dict

from fastapi import APIRouter, Depends, HTTPException, Request, status

from bff.dependencies import LabelMapper, TerminusService, get_label_mapper, get_terminus_service
from shared.models.ontology import QueryInput, QueryResponse
from shared.services.dataset_registry import DatasetRegistry
from shared.utils.access_policy import apply_access_policy

# Security validation imports
from shared.security.input_sanitizer import sanitize_input, validate_db_name

# Add shared path for common utilities
from shared.utils.language import get_accept_language

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}", tags=["Query"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


@router.post("/query", response_model=QueryResponse)
async def execute_query(
    db_name: str,
    query: QueryInput,
    request: Request,
    mapper: LabelMapper = Depends(get_label_mapper),
    terminus: TerminusService = Depends(get_terminus_service),
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
        # 쿼리 입력을 딕셔너리로 변환
        query_dict = query.model_dump(exclude_unset=True)

        # 레이블 기반 쿼리를 내부 ID 기반으로 변환
        internal_query = await mapper.convert_query_to_internal(db_name, query_dict, lang)

        # 쿼리 실행 (OMS를 통해)
        result = await terminus.query_database(db_name, internal_query)

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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except Exception as e:
        logger.error(f"Failed to execute query: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=f"쿼리 실행 실패: {str(e)}"
        )


@router.post("/query/raw")
async def execute_raw_query(
    db_name: str,
    query: Dict[str, Any],
    terminus: TerminusService = Depends(get_terminus_service),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    """
    원시 쿼리 실행 (제한적 접근)

    내부 ID 기반의 원시 쿼리를 실행합니다.
    보안상 매우 제한적인 쿼리만 허용됩니다.
    """
    try:
        # 데이터베이스 이름 검증
        validated_db_name = validate_db_name(db_name)

        # 쿼리 검증 - 허용된 쿼리 타입만 허용
        allowed_query_types = ["select", "count", "exists"]
        query_type = query.get("type", "").lower()

        if query_type not in allowed_query_types:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"허용되지 않은 쿼리 타입: {query_type}. 허용된 타입: {allowed_query_types}",
            )

        # 쿼리 파라미터 정화
        sanitized_query = sanitize_input(query)

        # 쿼리 실행 (OMS를 통해)
        result = await terminus.query_database(validated_db_name, sanitized_query)

        if isinstance(result, dict) and isinstance(result.get("data"), list):
            class_id = str(sanitized_query.get("class_id") or "").strip()
            if class_id:
                policy = await dataset_registry.get_access_policy(
                    db_name=validated_db_name,
                    scope="data_access",
                    subject_type="object_type",
                    subject_id=class_id,
                )
                if policy:
                    filtered, _ = apply_access_policy(result["data"], policy=policy.policy)
                    result["data"] = filtered

        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to execute raw query: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"원시 쿼리 실행 실패: {str(e)}",
        )


@router.get("/query/builder")
async def query_builder_info():
    """
    쿼리 빌더 정보

    쿼리 작성을 위한 사용 가능한 연산자와 옵션을 반환합니다.
    """
    # 보안 검증: 이 엔드포인트는 정적 정보만 반환하므로 validate_db_name 불필요
    # 공개 정보이므로 추가 입력 검증이 필요하지 않음
    
    return {
        "operators": {
            "comparison": ["=", "!=", ">", ">=", "<", "<="],
            "string": ["LIKE", "NOT_LIKE", "STARTS_WITH", "ENDS_WITH", "CONTAINS"],
            "array": ["IN", "NOT_IN"],
            "null": ["IS_NULL", "IS_NOT_NULL"],
        },
        "order_directions": ["asc", "desc"],
        "special_fields": {
            "@id": "문서 ID",
            "@type": "문서 타입",
            "@created": "생성 시간",
            "@modified": "수정 시간",
        },
        "examples": {
            "simple": {
                "class_label": "제품",
                "filters": [{"field_label": "가격", "operator": ">", "value": 10000}],
                "limit": 10,
            },
            "complex": {
                "class_label": "주문",
                "filters": [
                    {
                        "field_label": "상태",
                        "operator": "IN",
                        "value_labels": ["배송중", "배송완료"],
                    },
                    {"field_label": "총액", "operator": ">=", "value": 50000},
                ],
                "select": ["주문번호", "고객명", "총액", "상태"],
                "order_by": "주문일시",
                "order_direction": "desc",
                "limit": 20,
                "offset": 0,
            },
        },
    }
