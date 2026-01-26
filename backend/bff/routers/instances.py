"""
인스턴스 관련 API 라우터
온톨로지 클래스의 실제 인스턴스 데이터 조회
"""

import logging
from typing import Any, Dict, List, Optional
import asyncio
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, Request, status
from bff.dependencies import (
    TerminusService,
    get_terminus_service,
    get_oms_client,
    get_elasticsearch_service,
)
from bff.services.oms_client import OMSClient
from shared.security.input_sanitizer import (
    validate_db_name,
    validate_class_id,
    validate_instance_id,
    validate_branch_name,
    sanitize_es_query,
)
from shared.security.database_access import DOMAIN_MODEL_ROLES, enforce_database_role
from shared.services.storage.elasticsearch_service import ElasticsearchService
from shared.config.search_config import get_instances_index_name
from shared.config.app_config import AppConfig
from shared.config.settings import get_settings
from shared.services.registries.action_log_registry import ActionLogRecord, ActionLogRegistry
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.services.storage.lakefs_storage_service import create_lakefs_storage_service
from shared.services.storage.storage_service import create_storage_service
from shared.utils.access_policy import apply_access_policy
from shared.utils.writeback_lifecycle import derive_lifecycle_id, overlay_doc_id
from shared.services.core.writeback_merge_service import WritebackMergeService
from elasticsearch.exceptions import ConnectionError as ESConnectionError, NotFoundError, RequestError

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases/{db_name}", tags=["Instance Management"])

_ACTION_LOG_CLASS_ID = "ActionLog"
_action_log_registry_singleton: Optional[ActionLogRegistry] = None
_action_log_registry_lock = asyncio.Lock()

async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


async def _apply_access_policy_to_instances(
    *,
    dataset_registry: DatasetRegistry,
    db_name: str,
    class_id: str,
    instances: List[Dict[str, Any]],
) -> tuple[List[Dict[str, Any]], bool]:
    if not instances:
        return instances, False
    policy = await dataset_registry.get_access_policy(
        db_name=db_name,
        scope="data_access",
        subject_type="object_type",
        subject_id=class_id,
    )
    if not policy:
        return instances, False
    filtered, _ = apply_access_policy(instances, policy=policy.policy)
    return filtered, True

def _normalize_es_search_result(result: Any) -> tuple[int, List[Dict[str, Any]]]:
    """
    Normalize Elasticsearch search results across return shapes.

    Supported shapes:
    1) elasticsearch-py raw: {"hits": {"total": {"value": int}, "hits": [{"_source": {...}}]}}
    2) shared ElasticsearchService.search(): {"total": int, "hits": [{...}], "aggregations": {...}}
    """
    if not result or not isinstance(result, dict):
        return 0, []

    # shared ElasticsearchService.search() shape
    hits = result.get("hits")
    total = result.get("total")
    if isinstance(hits, list):
        try:
            total_value = int(total or 0)
        except (TypeError, ValueError):
            total_value = len(hits)
        return total_value, [h for h in hits if isinstance(h, dict)]

    # raw elasticsearch-py shape
    if isinstance(hits, dict):
        total_obj = hits.get("total") if isinstance(hits.get("total"), dict) else None
        total_value_raw = (
            total_obj.get("value") if isinstance(total_obj, dict) else hits.get("total")
        )
        try:
            total_value = int(total_value_raw or 0)
        except (TypeError, ValueError):
            total_value = 0

        sources: List[Dict[str, Any]] = []
        for hit in hits.get("hits") or []:
            if not isinstance(hit, dict):
                continue
            source = hit.get("_source")
            if isinstance(source, dict):
                sources.append(source)
        return total_value, sources

    return 0, []

def _is_action_log_class_id(class_id: str) -> bool:
    return str(class_id or "").strip().lower() == _ACTION_LOG_CLASS_ID.lower()


async def _maybe_get_action_log_registry(
    class_id: str,
) -> Optional[ActionLogRegistry]:
    if not _is_action_log_class_id(class_id):
        return None
    global _action_log_registry_singleton
    if _action_log_registry_singleton is not None:
        return _action_log_registry_singleton
    async with _action_log_registry_lock:
        if _action_log_registry_singleton is not None:
            return _action_log_registry_singleton
        registry = ActionLogRegistry()
        await registry.connect()
        _action_log_registry_singleton = registry
        return _action_log_registry_singleton


def _dt_iso(value: Any) -> Optional[str]:
    if value is None:
        return None
    try:
        return value.isoformat()
    except Exception:
        return str(value)


def _action_log_as_instance(record: ActionLogRecord) -> Dict[str, Any]:
    return {
        "class_id": _ACTION_LOG_CLASS_ID,
        "instance_id": record.action_log_id,
        "rid": f"action_log:{record.action_log_id}",
        "action_log_id": record.action_log_id,
        "db_name": record.db_name,
        "action_type_id": record.action_type_id,
        "action_type_rid": record.action_type_rid,
        "resource_rid": record.resource_rid,
        "ontology_commit_id": record.ontology_commit_id,
        "input": record.input,
        "status": record.status,
        "result": record.result,
        "correlation_id": record.correlation_id,
        "submitted_by": record.submitted_by,
        "submitted_at": _dt_iso(record.submitted_at),
        "finished_at": _dt_iso(record.finished_at),
        "writeback_target": record.writeback_target,
        "writeback_commit_id": record.writeback_commit_id,
        "action_applied_event_id": record.action_applied_event_id,
        "action_applied_seq": record.action_applied_seq,
        "metadata": record.metadata,
        "updated_at": _dt_iso(record.updated_at),
    }


@router.get("/class/{class_id}/instances")
async def get_class_instances(
    db_name: str,
    class_id: str,
    http_request: Request,
    base_branch: str = Query(default="main", description="Base branch (Terminus/ES base index)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
    limit: int = Query(default=100, le=1000),
    offset: int = Query(default=0, ge=0),
    search: Optional[str] = Query(default=None, description="Search query for filtering instances"),
    status_filter: Optional[List[str]] = Query(default=None, alias="status"),
    action_type_id: Optional[str] = Query(default=None),
    submitted_by: Optional[str] = Query(default=None),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    oms_client: OMSClient = Depends(get_oms_client),
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    action_logs: Optional[ActionLogRegistry] = Depends(_maybe_get_action_log_registry),
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
        resolved_base_branch = validate_branch_name(branch or base_branch or "main")

        if _is_action_log_class_id(class_id):
            try:
                await enforce_database_role(
                    headers=http_request.headers,
                    db_name=db_name,
                    required_roles=DOMAIN_MODEL_ROLES,
                )
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

            if search:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="search is not supported for ActionLog instances",
                )

            if not action_logs:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="ActionLogRegistry not available",
                )

            records = await action_logs.list_logs(
                db_name=db_name,
                statuses=status_filter,
                action_type_id=action_type_id,
                submitted_by=submitted_by,
                limit=limit,
                offset=offset,
            )

            return {
                "class_id": class_id,
                "total": len(records),
                "limit": limit,
                "offset": offset,
                "search": search,
                "base_branch": resolved_base_branch,
                "overlay_branch": None,
                "overlay_status": "DISABLED",
                "writeback_enabled": False,
                "writeback_edits_present": None,
                "instances": [_action_log_as_instance(rec) for rec in records],
            }

        writeback_enabled = bool(
            AppConfig.WRITEBACK_READ_OVERLAY and AppConfig.is_writeback_enabled_object_type(class_id)
        )
        resolved_overlay_branch = None
        requested_overlay_branch = str(overlay_branch).strip() if overlay_branch else None
        if requested_overlay_branch:
            resolved_overlay_branch = requested_overlay_branch
        elif writeback_enabled:
            resolved_overlay_branch = AppConfig.get_ontology_writeback_branch(db_name)
        overlay_required = writeback_enabled or bool(requested_overlay_branch)
        
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
        base_index_name = get_instances_index_name(db_name, branch=resolved_base_branch)
        overlay_index_name = (
            get_instances_index_name(db_name, branch=resolved_overlay_branch)
            if resolved_overlay_branch
            else None
        )
        
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
        overlay_result = None
        overlay_error = None
        overlay_status = "DISABLED" if not resolved_overlay_branch else "ACTIVE"
        try:
            es_result = await elasticsearch_service.search(
                index=base_index_name,
                query=query,
                size=limit,
                from_=offset,
                sort=[{"event_timestamp": {"order": "desc"}}]
            )
        except (ESConnectionError, ConnectionRefusedError, TimeoutError) as e:
            logger.warning(f"Elasticsearch connection failed, falling back to TerminusDB: {e}")
            es_error = "connection"
        except NotFoundError as e:
            logger.warning(f"Elasticsearch index not found (base index). Falling back to TerminusDB: {e}")
            es_error = "not_found"
        except RequestError as e:
            logger.error(f"Elasticsearch query error: {e}")
            es_error = "query"
        except Exception as e:
            logger.error(f"Unexpected Elasticsearch error, falling back to TerminusDB: {e}")
            es_error = "unknown"

        if overlay_index_name and not es_error:
            try:
                overlay_result = await elasticsearch_service.search(
                    index=overlay_index_name,
                    query=query,
                    size=limit,
                    from_=offset,
                    sort=[{"event_timestamp": {"order": "desc"}}],
                )
            except NotFoundError:
                # Treat a missing overlay index as an empty overlay (no edits yet).
                overlay_result = None
            except (ESConnectionError, ConnectionRefusedError, TimeoutError):
                overlay_error = "connection"
                overlay_status = "DEGRADED"
            except RequestError:
                overlay_error = "query"
                overlay_status = "DEGRADED"
            except Exception:
                overlay_error = "unknown"
                overlay_status = "DEGRADED"

        if overlay_required and overlay_status == "DEGRADED":
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail={
                    "error": "overlay_degraded",
                    "message": "Overlay index unavailable; cannot serve authoritative view.",
                    "class_id": class_id,
                    "base_branch": resolved_base_branch,
                    "overlay_branch": resolved_overlay_branch,
                    "overlay_status": "DEGRADED",
                    "writeback_enabled": writeback_enabled,
                    "writeback_edits_present": None,
                },
            )
        
        # Elasticsearch 결과 처리
        if es_result and not es_error:
            total, instances = _normalize_es_search_result(es_result)
            if overlay_result and not overlay_error:
                _overlay_total, overlay_hits = _normalize_es_search_result(overlay_result)

                def _doc_key(doc: Dict[str, Any]) -> Optional[str]:
                    if not isinstance(doc, dict):
                        return None
                    iid = doc.get("instance_id")
                    if iid is None:
                        return None
                    iid_str = str(iid).strip()
                    if not iid_str:
                        return None
                    lifecycle = str(doc.get("lifecycle_id") or "").strip()
                    if not lifecycle:
                        payload = doc.get("data")
                        if isinstance(payload, dict):
                            lifecycle = derive_lifecycle_id(payload)
                    lifecycle = lifecycle or "lc-0"
                    return overlay_doc_id(instance_id=iid_str, lifecycle_id=lifecycle)

                overlay_by_key: Dict[str, Dict[str, Any]] = {}
                for hit in overlay_hits:
                    key = _doc_key(hit) if isinstance(hit, dict) else None
                    if key:
                        overlay_by_key[key] = hit
                merged: List[Dict[str, Any]] = []
                for inst in instances:
                    key = _doc_key(inst) if isinstance(inst, dict) else None
                    if key and key in overlay_by_key:
                        inst = overlay_by_key[key]
                    if isinstance(inst, dict) and inst.get("overlay_tombstone") is True:
                        continue
                    merged.append(inst)
                # Include overlay-only docs that weren't in the base page (best-effort).
                base_keys = {_doc_key(inst) for inst in merged if isinstance(inst, dict)}
                for key, inst in overlay_by_key.items():
                    if key not in base_keys and isinstance(inst, dict) and inst.get("overlay_tombstone") is not True:
                        merged.append(inst)
                instances = merged

            instances, access_filtered = await _apply_access_policy_to_instances(
                dataset_registry=dataset_registry,
                db_name=db_name,
                class_id=class_id,
                instances=instances,
            )
            if access_filtered:
                total = len(instances)
            return {
                "class_id": class_id,
                "total": total,
                "limit": limit,
                "offset": offset,
                "search": search,
                "base_branch": resolved_base_branch,
                "overlay_branch": resolved_overlay_branch,
                "overlay_status": overlay_status,
                "writeback_enabled": writeback_enabled,
                "writeback_edits_present": None,
                "instances": instances
            }
        
        # Fallback to TerminusDB
        if es_error:
            if overlay_required:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail={
                        "error": "overlay_degraded",
                        "message": "Overlay index unavailable; cannot serve authoritative view.",
                        "class_id": class_id,
                        "base_branch": resolved_base_branch,
                        "overlay_branch": resolved_overlay_branch,
                        "overlay_status": "DEGRADED",
                        "writeback_enabled": writeback_enabled,
                        "writeback_edits_present": None,
                    },
                )
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
                # OMS 응답은 환경/버전에 따라 형태가 다를 수 있음:
                # - 최신: {"status":"success","total":..,"instances":[...]}
                # - 레거시: [{"...":...}, ...]
                if isinstance(result, list):
                    instances, _ = await _apply_access_policy_to_instances(
                        dataset_registry=dataset_registry,
                        db_name=db_name,
                        class_id=class_id,
                        instances=[inst for inst in result if isinstance(inst, dict)],
                    )
                    return {
                        "class_id": class_id,
                        "total": len(instances),
                        "limit": limit,
                        "offset": offset,
                        "search": search,
                        "instances": instances,
                    }
                if isinstance(result, dict):
                    if result.get("status") == "success":
                        instances = result.get("instances", [])
                        if not isinstance(instances, list):
                            instances = []
                        instances, access_filtered = await _apply_access_policy_to_instances(
                            dataset_registry=dataset_registry,
                            db_name=db_name,
                            class_id=class_id,
                            instances=[inst for inst in instances if isinstance(inst, dict)],
                        )
                        total = len(instances) if access_filtered else result.get("total", 0)
                        return {
                            "class_id": class_id,
                            "total": total,
                            "limit": limit,
                            "offset": offset,
                            "search": search,
                            "instances": instances,
                        }
                    instances = result.get("instances")
                    if isinstance(instances, list):
                        instances, access_filtered = await _apply_access_policy_to_instances(
                            dataset_registry=dataset_registry,
                            db_name=db_name,
                            class_id=class_id,
                            instances=[inst for inst in instances if isinstance(inst, dict)],
                        )
                        total = len(instances) if access_filtered else result.get("total")
                        return {
                            "class_id": class_id,
                            "total": int(total) if isinstance(total, int) else len(instances),
                            "limit": limit,
                            "offset": offset,
                            "search": search,
                            "instances": instances,
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
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
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
        instances, _ = await _apply_access_policy_to_instances(
            dataset_registry=dataset_registry,
            db_name=db_name,
            class_id=class_id,
            instances=[inst for inst in instances if isinstance(inst, dict)],
        )

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
    http_request: Request,
    base_branch: str = Query(default="main", description="Base branch (Terminus/ES base index)"),
    overlay_branch: Optional[str] = Query(default=None, description="ES overlay branch (writeback)"),
    branch: Optional[str] = Query(default=None, description="Deprecated alias for base_branch"),
    elasticsearch_service: ElasticsearchService = Depends(get_elasticsearch_service),
    oms_client: OMSClient = Depends(get_oms_client),  # 의존성 정상 주입
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
    action_logs: Optional[ActionLogRegistry] = Depends(_maybe_get_action_log_registry),
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
        resolved_base_branch = validate_branch_name(branch or base_branch or "main")

        if _is_action_log_class_id(class_id):
            try:
                await enforce_database_role(
                    headers=http_request.headers,
                    db_name=db_name,
                    required_roles=DOMAIN_MODEL_ROLES,
                )
            except ValueError as exc:
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc

            try:
                action_log_uuid = UUID(str(instance_id))
            except Exception as exc:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Invalid ActionLog UUID",
                ) from exc

            if not action_logs:
                raise HTTPException(
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    detail="ActionLogRegistry not available",
                )

            record = await action_logs.get_log(action_log_id=str(action_log_uuid))
            if not record or record.db_name != db_name:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="ActionLog not found")

            return {"status": "success", "data": _action_log_as_instance(record)}

        writeback_enabled = bool(
            AppConfig.WRITEBACK_READ_OVERLAY and AppConfig.is_writeback_enabled_object_type(class_id)
        )
        resolved_overlay_branch = None
        requested_overlay_branch = str(overlay_branch).strip() if overlay_branch else None
        if requested_overlay_branch:
            resolved_overlay_branch = requested_overlay_branch
        elif writeback_enabled:
            resolved_overlay_branch = AppConfig.get_ontology_writeback_branch(db_name)
        overlay_required = writeback_enabled or bool(requested_overlay_branch)
        
        # 1. Elasticsearch (읽기 모델) 우선 조회
        base_index_name = get_instances_index_name(db_name, branch=resolved_base_branch)
        overlay_index_name = (
            get_instances_index_name(db_name, branch=resolved_overlay_branch)
            if resolved_overlay_branch
            else None
        )
        
        query = {
            "bool": {
                "must": [
                    {"term": {"instance_id": instance_id}},
                    {"term": {"class_id": class_id}}
                ]
            }
        }
        
        logger.info(f"Querying instance {instance_id} of class {class_id} from Elasticsearch")
        
        overlay_status = "DISABLED" if not resolved_overlay_branch else "ACTIVE"

        async def _server_merge_fallback() -> Dict[str, Any]:
            settings = get_settings()
            base_storage = create_storage_service(settings)
            lakefs_storage = create_lakefs_storage_service(settings)
            if not base_storage or not lakefs_storage:
                raise RuntimeError("server_merge_unavailable")

            writeback_repo = AppConfig.ONTOLOGY_WRITEBACK_REPO
            writeback_branch = resolved_overlay_branch or AppConfig.get_ontology_writeback_branch(db_name)
            merger = WritebackMergeService(base_storage=base_storage, lakefs_storage=lakefs_storage)
            merged = await merger.merge_instance(
                db_name=db_name,
                base_branch=resolved_base_branch,
                overlay_branch=resolved_overlay_branch or writeback_branch,
                class_id=class_id,
                instance_id=instance_id,
                writeback_repo=writeback_repo,
                writeback_branch=writeback_branch,
            )

            filtered, _ = await _apply_access_policy_to_instances(
                dataset_registry=dataset_registry,
                db_name=db_name,
                class_id=class_id,
                instances=[merged.document],
            )
            if not filtered:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다")

            if merged.overlay_tombstone:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail={
                        "code": "RESOURCE_NOT_FOUND",
                        "message": f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                        "base_branch": resolved_base_branch,
                        "overlay_branch": resolved_overlay_branch,
                        "overlay_status": "DEGRADED",
                        "writeback_enabled": writeback_enabled,
                        "writeback_edits_present": merged.writeback_edits_present,
                    },
                )

            return {
                "status": "success",
                "base_branch": resolved_base_branch,
                "overlay_branch": resolved_overlay_branch,
                "overlay_status": "DEGRADED",
                "writeback_enabled": writeback_enabled,
                "writeback_edits_present": merged.writeback_edits_present,
                "data": filtered[0],
            }

        try:
            result = await elasticsearch_service.search(
                index=base_index_name,
                query=query,
                size=1,
            )
            
            _total, sources = _normalize_es_search_result(result)
            if sources:
                base_doc = sources[0]
                lifecycle_id = str(base_doc.get("lifecycle_id") or "").strip() if isinstance(base_doc, dict) else ""
                if not lifecycle_id and isinstance(base_doc, dict) and isinstance(base_doc.get("data"), dict):
                    lifecycle_id = derive_lifecycle_id(base_doc["data"])
                lifecycle_id = lifecycle_id or "lc-0"

                if overlay_index_name:
                    overlay_id = overlay_doc_id(instance_id=instance_id, lifecycle_id=lifecycle_id)
                    overlay_doc = await elasticsearch_service.get_document(overlay_index_name, overlay_id)
                    if overlay_doc is None:
                        # Back-compat: older overlay docs used `_id == instance_id`.
                        try:
                            overlay_result = await elasticsearch_service.search(
                                index=overlay_index_name,
                                query={
                                    "bool": {
                                        "must": [
                                            {"term": {"instance_id": instance_id}},
                                            {"term": {"class_id": class_id}},
                                            {"term": {"lifecycle_id": lifecycle_id}},
                                        ]
                                    }
                                },
                                size=1,
                            )
                            _overlay_total, overlay_sources = _normalize_es_search_result(overlay_result)
                            if overlay_sources:
                                overlay_doc = overlay_sources[0]
                        except NotFoundError:
                            overlay_doc = None
                    if isinstance(overlay_doc, dict) and overlay_doc.get("overlay_tombstone") is True:
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail={
                                "code": "RESOURCE_NOT_FOUND",
                                "message": f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                                "base_branch": resolved_base_branch,
                                "overlay_branch": resolved_overlay_branch,
                                "overlay_status": overlay_status,
                                "writeback_enabled": writeback_enabled,
                                "writeback_edits_present": None,
                            },
                        )
                    if isinstance(overlay_doc, dict):
                        filtered_overlay, _ = await _apply_access_policy_to_instances(
                            dataset_registry=dataset_registry,
                            db_name=db_name,
                            class_id=class_id,
                            instances=[overlay_doc],
                        )
                        if filtered_overlay:
                            return {
                                "status": "success",
                                "base_branch": resolved_base_branch,
                                "overlay_branch": resolved_overlay_branch,
                                "overlay_status": overlay_status,
                                "writeback_enabled": writeback_enabled,
                                "writeback_edits_present": None,
                                "data": filtered_overlay[0],
                            }

                filtered, _ = await _apply_access_policy_to_instances(
                    dataset_registry=dataset_registry,
                    db_name=db_name,
                    class_id=class_id,
                    instances=[base_doc],
                )
                if not filtered:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    )
                return {
                    "status": "success",
                    "base_branch": resolved_base_branch,
                    "overlay_branch": resolved_overlay_branch,
                    "overlay_status": overlay_status,
                    "writeback_enabled": writeback_enabled,
                    "writeback_edits_present": None,
                    "data": filtered[0],
                }
            
            if writeback_enabled:
                try:
                    return await _server_merge_fallback()
                except FileNotFoundError:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail={
                            "code": "RESOURCE_NOT_FOUND",
                            "message": f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": resolved_overlay_branch,
                            "overlay_status": "DEGRADED",
                            "writeback_enabled": writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    )
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "overlay_degraded",
                            "message": "Base projection missing; cannot serve authoritative view for writeback-enabled types.",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": resolved_overlay_branch,
                            "overlay_status": "DEGRADED",
                            "writeback_enabled": writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    )

            # ES에 문서가 없는 경우 - TerminusDB fallback
            logger.info(f"Instance {instance_id} not in Elasticsearch index, querying TerminusDB directly for latest state")
            
        except HTTPException:
            raise
        except (ESConnectionError, ConnectionRefusedError, TimeoutError) as e:
            if overlay_required:
                try:
                    return await _server_merge_fallback()
                except FileNotFoundError:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    ) from e
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "overlay_degraded",
                            "message": "Overlay index unavailable; cannot serve authoritative view.",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": resolved_overlay_branch,
                            "overlay_status": "DEGRADED",
                            "writeback_enabled": writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    ) from e
            logger.warning(f"Elasticsearch connection error: {e}. Falling back to TerminusDB.")
        except RequestError as e:
            if overlay_required:
                try:
                    return await _server_merge_fallback()
                except FileNotFoundError:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    ) from e
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "overlay_degraded",
                            "message": "Overlay index unavailable; cannot serve authoritative view.",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": resolved_overlay_branch,
                            "overlay_status": "DEGRADED",
                            "writeback_enabled": writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    ) from e
            logger.warning(f"Elasticsearch request error: {e}. Falling back to TerminusDB.")
        except Exception as e:
            if overlay_required:
                try:
                    return await _server_merge_fallback()
                except FileNotFoundError:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                    ) from e
                except HTTPException:
                    raise
                except Exception:
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "overlay_degraded",
                            "message": "Overlay index unavailable; cannot serve authoritative view.",
                            "base_branch": resolved_base_branch,
                            "overlay_branch": resolved_overlay_branch,
                            "overlay_status": "DEGRADED",
                            "writeback_enabled": writeback_enabled,
                            "writeback_edits_present": None,
                        },
                    ) from e
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
                    filtered, _ = await _apply_access_policy_to_instances(
                        dataset_registry=dataset_registry,
                        db_name=db_name,
                        class_id=class_id,
                        instances=[instance_data],
                    )
                    if not filtered:
                        raise HTTPException(
                            status_code=status.HTTP_404_NOT_FOUND,
                            detail=f"인스턴스 '{instance_id}'를 찾을 수 없습니다",
                        )
                    return {
                        "status": "success",
                        "data": filtered[0]
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
