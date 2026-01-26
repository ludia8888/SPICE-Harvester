"""
Database management router for BFF
Handles database creation, deletion, and listing
"""

import logging
import re
from typing import Any, Dict, List, Optional, DefaultDict
from collections import defaultdict

import httpx
import asyncpg
from fastapi import APIRouter, Depends, HTTPException, Request, status, Query
from fastapi.responses import JSONResponse

# Modernized dependency injection imports
from bff.dependencies import get_oms_client, OMSClientDep
from bff.services.oms_client import OMSClient
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.models.requests import ApiResponse, DatabaseCreateRequest
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.utils.branch_utils import protected_branch_write_message
from shared.config.settings import get_settings

# Add shared path for common utilities
from shared.utils.language import get_accept_language

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/databases", tags=["Database Management"])


async def get_dataset_registry() -> DatasetRegistry:
    from bff.main import get_dataset_registry as _get_dataset_registry

    return await _get_dataset_registry()


def _is_dev_mode() -> bool:
    return not get_settings().is_production


async def _get_expected_seq_for_database(db_name: str) -> int:
    seq_settings = get_settings().event_sourcing
    schema = seq_settings.event_store_sequence_schema
    if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
        raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")

    prefix = str(seq_settings.event_store_sequence_handler_prefix or "write_side").strip() or "write_side"
    handler = f"{prefix}:Database"

    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        value = await conn.fetchval(
            f"""
            SELECT last_sequence
            FROM {schema}.aggregate_versions
            WHERE handler = $1 AND aggregate_id = $2
            """,
            handler,
            db_name,
        )
        return int(value or 0)
    finally:
        await conn.close()


def _coerce_db_entry(entry: Any) -> Dict[str, Any]:
    if isinstance(entry, dict):
        payload = dict(entry)
    elif isinstance(entry, str):
        payload = {"name": entry}
    else:
        payload = {}

    name = payload.get("name") or payload.get("db_name") or payload.get("id") or payload.get("label")
    if name and "name" not in payload:
        payload["name"] = name
    return payload


async def _fetch_database_access(db_names: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    if not db_names:
        return {}
    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            rows = await conn.fetch(
                """
                SELECT db_name, principal_type, principal_id, principal_name, role
                FROM database_access
                WHERE db_name = ANY($1)
                """,
                db_names,
            )
        except asyncpg.UndefinedTableError:
            # Treat missing table as "unconfigured" (migrations not applied yet).
            rows = []
    finally:
        await conn.close()

    grouped: DefaultDict[str, List[Dict[str, Any]]] = defaultdict(list)
    for row in rows or []:
        grouped[str(row["db_name"])].append(
            {
                "principal_type": str(row["principal_type"]),
                "principal_id": str(row["principal_id"]),
                "principal_name": str(row["principal_name"]) if row["principal_name"] else None,
                "role": str(row["role"]),
            }
        )
    return dict(grouped)


async def _upsert_database_owner(
    *,
    db_name: str,
    principal_type: str,
    principal_id: str,
    principal_name: str,
) -> None:
    if not principal_id:
        return
    conn = await asyncpg.connect(get_settings().database.postgres_url)
    try:
        try:
            await conn.execute(
                """
                INSERT INTO database_access (
                    db_name, principal_type, principal_id, principal_name, role, created_at, updated_at
                ) VALUES ($1, $2, $3, $4, 'Owner', NOW(), NOW())
                ON CONFLICT (db_name, principal_type, principal_id)
                DO UPDATE SET
                    principal_name = EXCLUDED.principal_name,
                    role = EXCLUDED.role,
                    updated_at = NOW()
                """,
                db_name,
                principal_type,
                principal_id,
                principal_name,
            )
        except asyncpg.UndefinedTableError:
            # Treat missing table as "unconfigured" (migrations not applied yet).
            return
    finally:
        await conn.close()


def _resolve_actor(request: Request) -> tuple[str, str, str]:
    principal_type = (request.headers.get("X-User-Type") or "user").strip().lower() or "user"
    principal_id = (
        request.headers.get("X-User-ID")
        or request.headers.get("X-User")
        or request.headers.get("X-User-Id")
        or ""
    ).strip() or "system"
    principal_name = (request.headers.get("X-User-Name") or "").strip() or principal_id
    return principal_type, principal_id, principal_name


def _enrich_db_entry(
    entry: Any,
    *,
    actor_type: str,
    actor_id: str,
    actor_name: str,
    access_rows: Dict[str, List[Dict[str, Any]]],
) -> Dict[str, Any]:
    payload = _coerce_db_entry(entry)
    db_name = payload.get("name") or payload.get("db_name") or payload.get("id")
    rows = access_rows.get(db_name, []) if db_name else []

    owner_row = next((row for row in rows if row.get("role") == "Owner"), None)
    actor_row = next(
        (row for row in rows if row.get("principal_type") == actor_type and row.get("principal_id") == actor_id),
        None,
    )

    owner_id = owner_row["principal_id"] if owner_row else payload.get("owner_id") or actor_id
    owner_name = owner_row.get("principal_name") if owner_row else payload.get("owner_name") or actor_name
    role = actor_row["role"] if actor_row else payload.get("role")

    shared_with = [row["principal_id"] for row in rows if row.get("role") != "Owner"]
    shared = bool(shared_with)

    if not rows:
        shared_with = []
        shared = False
        role = role or ("Owner" if actor_id == owner_id else "Viewer")
        owner_id = owner_id or actor_id
        owner_name = owner_name or actor_name
    else:
        role = role or "Viewer"

    payload.update(
        {
            "owner_id": owner_id,
            "owner_name": owner_name,
            "role": role,
            "shared": shared,
            "shared_with": shared_with,
        }
    )
    return payload


@router.get("", response_model=ApiResponse)
async def list_databases(
    request: Request,
    oms: OMSClient = OMSClientDep,
    dataset_registry: DatasetRegistry = Depends(get_dataset_registry),
):
    """데이터베이스 목록 조회"""
    try:
        # 기본 보안 검증 (관리자 권한 필요한 작업)
        # TODO: 실제 환경에서는 사용자 권한 확인 필요
        # OMS를 통해 데이터베이스 목록 조회
        result = await oms.list_databases()

        databases = result.get("data", {}).get("databases", [])
        actor_type, actor_id, actor_name = _resolve_actor(request)
        db_names = [entry.get("name") for entry in map(_coerce_db_entry, databases) if entry.get("name")]
        access_rows = await _fetch_database_access(db_names)
        dataset_counts: Dict[str, int] = {}
        try:
            dataset_counts = await dataset_registry.count_datasets_by_db_names(db_names=db_names)
        except Exception as exc:
            logger.warning("Failed to load dataset counts: %s", exc)
        is_dev = _is_dev_mode()
        enriched = []
        for entry in databases:
            payload = _enrich_db_entry(
                entry,
                actor_type=actor_type,
                actor_id=actor_id,
                actor_name=actor_name,
                access_rows=access_rows,
            )
            if is_dev:
                payload["role"] = "Owner"
            db_key = payload.get("name") or payload.get("db_name") or payload.get("id")
            if db_key and db_key in dataset_counts:
                payload["dataset_count"] = dataset_counts[db_key]
            entry_rows = access_rows.get(payload.get("name") or payload.get("db_name") or payload.get("id"), [])
            actor_has_access = any(
                row.get("principal_type") == actor_type and row.get("principal_id") == actor_id for row in entry_rows
            )
            if entry_rows and not actor_has_access and not is_dev:
                continue
            enriched.append(payload)

        return ApiResponse.success(
            message=f"데이터베이스 목록 조회 완료 ({len(enriched)}개)",
            data={"databases": enriched, "count": len(enriched)},
        ).to_dict()
    except Exception as e:
        logger.error(f"Failed to list databases: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post(
    "",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_201_CREATED: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "Conflict (already exists / OCC)"},
    },
)
async def create_database(request: DatabaseCreateRequest, http_request: Request, oms: OMSClient = OMSClientDep):
    """데이터베이스 생성"""
    logger.info(f"🔥 BFF: Database creation request received - name: {request.name}, description: {request.description}")
    
    try:
        # 입력 데이터 보안 검증
        logger.info(f"🔒 BFF: Validating database name: {request.name}")
        validated_name = validate_db_name(request.name)
        logger.info(f"✅ BFF: Database name validated: {validated_name}")
        
        if request.description:
            sanitized_description = sanitize_input(request.description)
            logger.info(f"✅ BFF: Description sanitized")
        
        actor_type, actor_id, actor_name = _resolve_actor(http_request)

        # OMS를 통해 데이터베이스 생성
        logger.info(f"📡 BFF: Calling OMS to create database - URL: {oms.base_url}")
        result = await oms.create_database(request.name, request.description)
        logger.info(f"✅ BFF: OMS response received: {result}")

        # Event Sourcing mode: pass through async contract (202 + command_id)
        if isinstance(result, dict) and result.get("status") == "accepted":
            await _upsert_database_owner(
                db_name=request.name,
                principal_type=actor_type,
                principal_id=actor_id,
                principal_name=actor_name,
            )
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        # 자동 커밋: 데이터베이스 생성 기록
        try:
            commit_message = f"Create database: {request.name}"
            if request.description:
                commit_message += f"\n\nDescription: {request.description}"
            
            await oms.commit_database_change(
                db_name=request.name,
                message=commit_message,
                author="system"
            )
            logger.info(f"Auto-committed database creation: {request.name}")
        except Exception as commit_error:
            # 커밋 실패해도 데이터베이스 생성은 성공으로 처리
            logger.warning(f"Failed to auto-commit database creation for '{request.name}': {commit_error}")

        await _upsert_database_owner(
            db_name=request.name,
            principal_type=actor_type,
            principal_id=actor_id,
            principal_name=actor_name,
        )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=ApiResponse.created(
                message=f"데이터베이스 '{request.name}'가 생성되었습니다",
                data={"name": request.name, "result": result, "mode": "direct"},
            ).to_dict(),
        )
    except httpx.HTTPStatusError as e:
        status_code = e.response.status_code
        detail: Any
        try:
            detail = e.response.json()
        except Exception:
            detail = e.response.text or str(e)

        if status_code == status.HTTP_409_CONFLICT:
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            if not isinstance(detail, str) or not detail.strip():
                detail = f"데이터베이스 '{request.name}'이(가) 이미 존재합니다"
        raise HTTPException(status_code=status_code, detail=detail)
    except SecurityViolationError as e:
        # 보안 검증 실패는 400 Bad Request로 처리
        logger.warning(f"Security validation failed for database '{request.name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create database '{request.name}': {e}")

        # 중복 데이터베이스 체크
        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"데이터베이스 '{request.name}'이(가) 이미 존재합니다",
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.delete(
    "/{db_name}",
    response_model=ApiResponse,
    status_code=status.HTTP_202_ACCEPTED,
    responses={
        status.HTTP_200_OK: {"model": ApiResponse, "description": "Direct mode (legacy)"},
        status.HTTP_202_ACCEPTED: {"model": ApiResponse, "description": "Event-sourcing mode (async)"},
        status.HTTP_409_CONFLICT: {"description": "OCC conflict"},
    },
)
async def delete_database(
    db_name: str,
    http_request: Request,
    expected_seq: Optional[int] = Query(
        default=None,
        ge=0,
        description="Expected current aggregate sequence (OCC). When omitted, server resolves the current value.",
    ),
    oms: OMSClient = OMSClientDep,
):
    """데이터베이스 삭제"""
    try:
        # 입력 데이터 보안 검증
        validated_db_name = validate_db_name(db_name)

        # 시스템 데이터베이스 보호
        protected_dbs = ["_system", "_meta"]
        if validated_db_name in protected_dbs:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"시스템 데이터베이스 '{validated_db_name}'은(는) 삭제할 수 없습니다",
            )

        actor_type, actor_id, _actor_name = _resolve_actor(http_request)
        if not _is_dev_mode():
            access_rows = await _fetch_database_access([validated_db_name])
            db_access = access_rows.get(validated_db_name, [])
            actor_row = next(
                (
                    row
                    for row in db_access
                    if row.get("principal_type") == actor_type and row.get("principal_id") == actor_id
                ),
                None,
            )
            actor_role = (actor_row or {}).get("role", "")
            if not actor_role or actor_role.lower() != "owner":
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Only owners can delete projects.",
                )

        if expected_seq is None:
            expected_seq = await _get_expected_seq_for_database(validated_db_name)

        # OMS를 통해 데이터베이스 삭제
        try:
            result = await oms.delete_database(validated_db_name, expected_seq=expected_seq)
        except httpx.HTTPStatusError as e:
            status_code = e.response.status_code
            detail: Any
            try:
                detail = e.response.json()
            except Exception:
                detail = e.response.text or str(e)
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            raise HTTPException(status_code=status_code, detail=detail)
        except httpx.RequestError as e:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(e))

        # Event Sourcing mode: pass through async contract (202 + command_id)
        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        # 자동 커밋: 데이터베이스 삭제 기록
        # 참고: 데이터베이스가 삭제되었으므로 메타데이터나 로그 시스템에 기록
        try:
            # 다른 데이터베이스(보통 _system 또는 메인 데이터베이스)에 기록
            await oms.commit_system_change(
                message=f"Delete database: {validated_db_name}",
                author="system",
                operation="database_delete",
                target=validated_db_name
            )
            logger.info(f"Auto-committed database deletion: {validated_db_name}")
        except Exception as commit_error:
            # 커밋 실패해도 데이터베이스 삭제는 성공으로 처리
            logger.warning(f"Failed to auto-commit database deletion for '{validated_db_name}': {commit_error}")

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"데이터베이스 '{validated_db_name}'이(가) 삭제되었습니다",
                data={"database_name": validated_db_name, "mode": "direct"},
            ).to_dict(),
        )
    except SecurityViolationError as e:
        # 보안 검증 실패는 400 Bad Request로 처리
        logger.warning(f"Security validation failed for database '{db_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete database '{db_name}': {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{db_name}/branches/{branch_name:path}")
async def get_branch_info(db_name: str, branch_name: str, oms: OMSClient = OMSClientDep) -> Dict[str, Any]:
    """브랜치 정보 조회 (프론트엔드용 BFF 래핑)"""
    try:
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        return await oms.get(f"/api/v1/branch/{db_name}/branch/{branch_name}/info")
    except SecurityViolationError as e:
        logger.warning(f"Security validation failed for branch info ({db_name}/{branch_name}): {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except httpx.HTTPStatusError as e:
        resp = getattr(e, "response", None)
        if resp is not None and resp.status_code == 404:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="브랜치를 찾을 수 없습니다") from e
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 조회 실패") from e
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 조회 실패") from e
    except Exception as e:
        logger.error(f"Failed to get branch info ({db_name}/{branch_name}): {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@router.delete("/{db_name}/branches/{branch_name:path}")
async def delete_branch(
    db_name: str,
    branch_name: str,
    force: bool = False,
    oms: OMSClient = OMSClientDep,
) -> Dict[str, Any]:
    """브랜치 삭제 (프론트엔드용 BFF 래핑)"""
    try:
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        return await oms.delete(
            f"/api/v1/branch/{db_name}/branch/{branch_name}",
            params={"force": force},
        )
    except SecurityViolationError as e:
        logger.warning(f"Security validation failed for branch delete ({db_name}/{branch_name}): {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except httpx.HTTPStatusError as e:
        resp = getattr(e, "response", None)
        if resp is not None:
            if resp.status_code == 404:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="브랜치를 찾을 수 없습니다") from e
            if resp.status_code == 403:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=protected_branch_write_message(),
                ) from e
            if resp.status_code == 400:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="현재 브랜치는 삭제할 수 없습니다") from e
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 삭제 실패") from e
    except httpx.HTTPError as e:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 삭제 실패") from e
    except Exception as e:
        logger.error(f"Failed to delete branch ({db_name}/{branch_name}): {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@router.get("/{db_name}")
async def get_database(db_name: str, oms: OMSClient = OMSClientDep):
    """데이터베이스 정보 조회"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # OMS를 통해 데이터베이스 정보 조회
        result = await oms.get_database(db_name)

        return {"status": "success", "data": result}
    except SecurityViolationError as e:
        # 보안 검증 실패는 400 Bad Request로 처리
        logger.warning(f"Security validation failed for database '{db_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get database '{db_name}': {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{db_name}/expected-seq", response_model=ApiResponse)
async def get_database_expected_seq(
    db_name: str,
):
    """
    Resolve the current `expected_seq` for database (aggregate) operations.

    Frontend policy: OCC tokens should be treated as resource versions, not user input.
    """
    try:
        db_name = validate_db_name(db_name)
        expected_seq = await _get_expected_seq_for_database(db_name)
        return ApiResponse.success(
            message="Database expected_seq fetched",
            data={"db_name": db_name, "expected_seq": expected_seq},
        ).to_dict()
    except SecurityViolationError as e:
        logger.warning(f"Security validation failed for expected-seq ({db_name}): {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e))
    except ValueError as e:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except Exception as e:
        logger.error(f"Failed to get expected_seq for database '{db_name}': {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e)) from e


@router.get("/{db_name}/classes")
async def list_classes(
    db_name: str,
    type: Optional[str] = "Class",
    limit: Optional[int] = None,
    oms: OMSClient = OMSClientDep,
):
    """데이터베이스의 클래스 목록 조회"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # OMS를 통해 클래스 목록 조회
        result = await oms.list_ontologies(db_name)

        classes = result.get("data", {}).get("ontologies", [])
        if not isinstance(classes, list):
            classes = []

        # Virtual/system ontology object types (control plane).
        action_log_id = "ActionLog"
        if not any(
            isinstance(item, dict) and str(item.get("id") or "").strip() == action_log_id
            for item in classes
        ):
            classes.append(
                {
                    "id": action_log_id,
                    "label": "Action Log",
                    "description": "Action-only writeback audit log (Postgres-backed; first-class ontology object)",
                    "metadata": {
                        "system": True,
                        "backing_store": "postgres",
                        "table": "spice_action_logs.ontology_action_logs",
                    },
                }
            )

        return {"classes": classes, "count": len(classes)}
    except SecurityViolationError as e:
        # 보안 검증 실패는 400 Bad Request로 처리
        logger.warning(f"Security validation failed for database '{db_name}': {e}")
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(e)
        )
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to list classes for database '{db_name}': {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.post("/{db_name}/classes")
async def create_class(
    db_name: str, class_data: Dict[str, Any], oms: OMSClient = OMSClientDep
):
    """데이터베이스에 새 클래스 생성"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_data = sanitize_input(class_data)

        # 요청 데이터 검증
        if not class_data.get("@id"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="클래스 ID (@id)가 필요합니다"
            )

        # Convert @id to id for OMS (OMS expects 'id', not '@id')
        oms_data = class_data.copy()
        oms_data["id"] = oms_data.pop("@id", None)

        # OMS를 통해 클래스 생성
        result = await oms.create_ontology(db_name, oms_data)

        return {"status": "success", "@id": class_data.get("@id"), "data": result}
    except SecurityViolationError as e:
        logger.warning(f"Security validation failed for create_class ({db_name}): {e}")
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(e)) from e
    except httpx.HTTPStatusError as e:
        # Preserve OMS semantics (e.g., protected-branch proposal workflow = 409).
        upstream_status = int(getattr(e.response, "status_code", 502) or 502)
        try:
            upstream_body: Any = e.response.json()
        except Exception:  # pragma: no cover (depends on upstream)
            upstream_body = getattr(e.response, "text", str(e))

        if upstream_status in {400, 401, 403, 404, 409, 422}:
            raise HTTPException(status_code=upstream_status, detail=upstream_body) from e

        # Unknown upstream failure -> treat as a gateway error (not a BFF 500).
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=upstream_body) from e
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create class in database '{db_name}': {e}")

        if "already exists" in str(e).lower() or "duplicate" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"클래스 '{class_data.get('@id')}'이(가) 이미 존재합니다",
            )

        if "invalid" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail=f"잘못된 클래스 데이터: {str(e)}"
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{db_name}/classes/{class_id}")
async def get_class(
    db_name: str, class_id: str, request: Request, oms: OMSClient = OMSClientDep
):
    """특정 클래스 조회"""
    get_accept_language(request)

    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = sanitize_input(class_id)

        if str(class_id or "").strip() == "ActionLog":
            return {
                "status": "success",
                "message": "Virtual ontology class",
                "data": {
                    "id": "ActionLog",
                    "label": "Action Log",
                    "description": "Action-only writeback audit log (Postgres-backed; first-class ontology object).",
                    "properties": [
                        {"name": "action_log_id", "type": "uuid"},
                        {"name": "db_name", "type": "string"},
                        {"name": "action_type_id", "type": "string"},
                        {"name": "action_type_rid", "type": "string"},
                        {"name": "ontology_commit_id", "type": "string"},
                        {"name": "status", "type": "string"},
                        {"name": "input", "type": "object"},
                        {"name": "result", "type": "object"},
                        {"name": "submitted_by", "type": "string"},
                        {"name": "submitted_at", "type": "datetime"},
                        {"name": "finished_at", "type": "datetime"},
                        {"name": "writeback_target", "type": "object"},
                        {"name": "writeback_commit_id", "type": "string"},
                        {"name": "action_applied_event_id", "type": "string"},
                        {"name": "action_applied_seq", "type": "integer"},
                        {"name": "metadata", "type": "object"},
                        {"name": "updated_at", "type": "datetime"},
                    ],
                    "metadata": {
                        "system": True,
                        "backing_store": "postgres",
                        "table": "spice_action_logs.ontology_action_logs",
                    },
                },
            }

        # OMS를 통해 클래스 조회
        result = await oms.get_ontology(db_name, class_id)

        return result
    except Exception as e:
        logger.error(f"Failed to get class '{class_id}' from database '{db_name}': {e}")

        if "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_id}'을(를) 찾을 수 없습니다",
            )

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(e))


@router.get("/{db_name}/branches")
async def list_branches(db_name: str, oms: OMSClient = OMSClientDep):
    """브랜치 목록 조회"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # OMS를 통해 브랜치 목록 조회
        result = await oms.list_branches(db_name)

        branches = result.get("data", {}).get("branches", [])

        return {"branches": branches, "count": len(branches)}
    except Exception as e:
        logger.error(f"Failed to list branches for database '{db_name}': {e}")

        # 🔥 REAL IMPLEMENTATION! 브랜치 기능은 완전히 구현되어 있음
        # 실제 에러 상황 처리
        if "database not found" in str(e).lower() or "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        elif "access denied" in str(e).lower() or "unauthorized" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="브랜치 목록 조회 권한이 없습니다"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail=f"브랜치 목록 조회 실패: {str(e)}"
            )


@router.post("/{db_name}/branches")
async def create_branch(
    db_name: str, branch_data: Dict[str, Any], oms: OMSClient = OMSClientDep
):
    """새 브랜치 생성"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        branch_data = sanitize_input(branch_data)

        # 요청 데이터 검증
        branch_name = branch_data.get("name")
        if not branch_name:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST, detail="브랜치 이름이 필요합니다"
            )
        branch_name = validate_branch_name(branch_name)

        from_branch = branch_data.get("from_branch", "main")
        if from_branch:
            from_branch = validate_branch_name(from_branch)

        # 🔥 ROOT CAUSE FIX: OMS가 기대하는 필드명으로 변환
        oms_branch_data = {
            "branch_name": branch_name,  # 'name' -> 'branch_name'
            "from_branch": from_branch,
        }

        # OMS를 통해 브랜치 생성
        result = await oms.create_branch(db_name, oms_branch_data)

        return {"status": "success", "name": branch_name, "data": result}
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create branch in database '{db_name}': {e}")

        # 🔥 REAL IMPLEMENTATION! 브랜치 기능은 완전히 구현되어 있음
        # 더미 메시지 제거하고 실제 에러 처리
        if "branch already exists" in str(e).lower() or "already exists" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"브랜치 '{branch_data.get('name')}'이(가) 이미 존재합니다"
            )
        elif "database not found" in str(e).lower() or "not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"브랜치 생성 실패: {str(e)}"
            )


@router.get("/{db_name}/versions")
async def get_versions(db_name: str, oms: OMSClient = OMSClientDep):
    """버전 히스토리 조회"""
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)

        # OMS를 통해 버전 히스토리 조회
        result = await oms.get_version_history(db_name)

        versions = result.get("data", {}).get("versions", [])

        return {"versions": versions, "count": len(versions)}
    except Exception as e:
        logger.error(f"Failed to get versions for database '{db_name}': {e}")

        # 🔥 REAL IMPLEMENTATION! 버전 관리 기능은 완전히 구현되어 있음
        # 실제 에러 상황 처리
        if "database not found" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다"
            )
        elif "access denied" in str(e).lower() or "unauthorized" in str(e).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="버전 히스토리 조회 권한이 없습니다"
            )
        elif "no commits" in str(e).lower() or "empty history" in str(e).lower():
            # 실제로 커밋이 없는 경우 - 빈 목록 반환 (정상 상황)
            return {"versions": [], "count": 0}
        else:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, 
                detail=f"버전 히스토리 조회 실패: {str(e)}"
            )
