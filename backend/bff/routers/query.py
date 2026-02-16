"""
쿼리 라우터
Foundry Search Objects v2 기반 데이터 쿼리를 담당
"""

from shared.observability.tracing import trace_endpoint

from fastapi import APIRouter

router = APIRouter(prefix="/databases/{db_name}", tags=["Query"])


@router.get("/query/builder")
@trace_endpoint("bff.query.query_builder_info")
async def query_builder_info():
    """
    쿼리 빌더 정보

    쿼리 작성을 위한 사용 가능한 연산자와 옵션을 반환합니다.
    """
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
