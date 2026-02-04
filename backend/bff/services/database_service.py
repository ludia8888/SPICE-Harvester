"""
Database domain logic (BFF).

Extracted from `bff.routers.database` to keep routers thin and to centralize
validation/error-mapping around OMS + database_access enrichment.
"""

import logging
import re
from typing import Any, Dict, List, Optional

import asyncpg
import httpx
from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from bff.services.oms_client import OMSClient
from shared.config.settings import get_settings
from shared.models.requests import ApiResponse, DatabaseCreateRequest
from shared.security.database_access import (
    fetch_database_access_entries,
    get_database_access_role,
    resolve_database_actor_with_name,
    upsert_database_owner,
)
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.branch_utils import protected_branch_write_message

logger = logging.getLogger(__name__)

_ACTION_LOG_CLASS_ID = "ActionLog"


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


async def list_databases(
    *,
    request: Request,
    oms: OMSClient,
    dataset_registry: DatasetRegistry,
) -> Dict[str, Any]:
    """데이터베이스 목록 조회"""
    try:
        result = await oms.list_databases()

        databases = result.get("data", {}).get("databases", [])
        actor_type, actor_id, actor_name = resolve_database_actor_with_name(request.headers)
        db_names = [entry.get("name") for entry in map(_coerce_db_entry, databases) if entry.get("name")]
        access_rows = await fetch_database_access_entries(db_names=db_names)
        dataset_counts: Dict[str, int] = {}
        try:
            dataset_counts = await dataset_registry.count_datasets_by_db_names(db_names=db_names)
        except Exception as exc:
            logger.warning("Failed to load dataset counts: %s", exc)
        is_dev = _is_dev_mode()

        enriched: List[Dict[str, Any]] = []
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

            entry_rows = access_rows.get(db_key, []) if db_key else []
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
    except Exception as exc:
        logger.error("Failed to list databases: %s", exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def create_database(
    *,
    body: DatabaseCreateRequest,
    http_request: Request,
    oms: OMSClient,
) -> JSONResponse:
    """데이터베이스 생성"""
    logger.info(
        "BFF: Database creation request received - name=%s, description=%s",
        body.name,
        body.description,
    )

    try:
        validated_name = validate_db_name(body.name)
        if body.description:
            sanitize_input(body.description)

        actor_type, actor_id, actor_name = resolve_database_actor_with_name(http_request.headers)

        result = await oms.create_database(validated_name, body.description)

        # Event Sourcing mode: pass through async contract (202 + command_id)
        if isinstance(result, dict) and result.get("status") == "accepted":
            await upsert_database_owner(
                db_name=validated_name,
                principal_type=actor_type,
                principal_id=actor_id,
                principal_name=actor_name,
            )
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        try:
            commit_message = f"Create database: {validated_name}"
            if body.description:
                commit_message += f"\n\nDescription: {body.description}"

            await oms.commit_database_change(
                db_name=validated_name,
                message=commit_message,
                author="system",
            )
            logger.info("Auto-committed database creation: %s", validated_name)
        except Exception as commit_error:
            logger.warning("Failed to auto-commit database creation for %r: %s", validated_name, commit_error)

        await upsert_database_owner(
            db_name=validated_name,
            principal_type=actor_type,
            principal_id=actor_id,
            principal_name=actor_name,
        )

        return JSONResponse(
            status_code=status.HTTP_201_CREATED,
            content=ApiResponse.created(
                message=f"데이터베이스 '{validated_name}'가 생성되었습니다",
                data={"name": validated_name, "result": result, "mode": "direct"},
            ).to_dict(),
        )
    except httpx.HTTPStatusError as exc:
        status_code = exc.response.status_code
        try:
            detail: Any = exc.response.json()
        except Exception:
            detail = exc.response.text or str(exc)

        if status_code == status.HTTP_409_CONFLICT:
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            if not isinstance(detail, str) or not detail.strip():
                detail = f"데이터베이스 '{body.name}'이(가) 이미 존재합니다"
        raise HTTPException(status_code=status_code, detail=detail) from exc
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", body.name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create database %r: %s", body.name, exc)

        if "already exists" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"데이터베이스 '{body.name}'이(가) 이미 존재합니다",
            ) from exc

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def delete_database(
    *,
    db_name: str,
    http_request: Request,
    expected_seq: Optional[int],
    oms: OMSClient,
) -> JSONResponse:
    """데이터베이스 삭제"""
    try:
        validated_db_name = validate_db_name(db_name)

        protected_dbs = ["_system", "_meta"]
        if validated_db_name in protected_dbs:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"시스템 데이터베이스 '{validated_db_name}'은(는) 삭제할 수 없습니다",
            )

        actor_type, actor_id, _actor_name = resolve_database_actor_with_name(http_request.headers)
        if not _is_dev_mode():
            role = await get_database_access_role(
                db_name=validated_db_name,
                principal_type=actor_type,
                principal_id=actor_id,
            )
            if not role or role.lower() != "owner":
                raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Only owners can delete projects.")

        if expected_seq is None:
            expected_seq = await _get_expected_seq_for_database(validated_db_name)

        try:
            result = await oms.delete_database(validated_db_name, expected_seq=expected_seq)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            try:
                detail: Any = exc.response.json()
            except Exception:
                detail = exc.response.text or str(exc)
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            raise HTTPException(status_code=status_code, detail=detail) from exc
        except httpx.RequestError as exc:
            raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail=str(exc)) from exc

        # Event Sourcing mode: pass through async contract (202 + command_id)
        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        try:
            await oms.commit_system_change(
                message=f"Delete database: {validated_db_name}",
                author="system",
                operation="database_delete",
                target=validated_db_name,
            )
            logger.info("Auto-committed database deletion: %s", validated_db_name)
        except Exception as commit_error:
            logger.warning("Failed to auto-commit database deletion for %r: %s", validated_db_name, commit_error)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"데이터베이스 '{validated_db_name}'이(가) 삭제되었습니다",
                data={"database_name": validated_db_name, "mode": "direct"},
            ).to_dict(),
        )
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to delete database %r: %s", db_name, exc)

        if "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_branch_info(*, db_name: str, branch_name: str, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 정보 조회 (프론트엔드용 BFF 래핑)"""
    try:
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        return await oms.get(f"/api/v1/branch/{db_name}/branch/{branch_name}/info")
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for branch info (%s/%s): %s", db_name, branch_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        resp = getattr(exc, "response", None)
        if resp is not None and resp.status_code == 404:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="브랜치를 찾을 수 없습니다") from exc
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 조회 실패") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 조회 실패") from exc
    except Exception as exc:
        logger.error("Failed to get branch info (%s/%s): %s", db_name, branch_name, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def delete_branch(*, db_name: str, branch_name: str, force: bool, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 삭제 (프론트엔드용 BFF 래핑)"""
    try:
        db_name = validate_db_name(db_name)
        branch_name = validate_branch_name(branch_name)

        return await oms.delete(
            f"/api/v1/branch/{db_name}/branch/{branch_name}",
            params={"force": force},
        )
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for branch delete (%s/%s): %s", db_name, branch_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        resp = getattr(exc, "response", None)
        if resp is not None:
            if resp.status_code == 404:
                raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="브랜치를 찾을 수 없습니다") from exc
            if resp.status_code == 403:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=protected_branch_write_message(),
                ) from exc
            if resp.status_code == 400:
                raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="현재 브랜치는 삭제할 수 없습니다") from exc
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 삭제 실패") from exc
    except httpx.HTTPError as exc:
        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail="OMS 브랜치 삭제 실패") from exc
    except Exception as exc:
        logger.error("Failed to delete branch (%s/%s): %s", db_name, branch_name, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_database(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """데이터베이스 정보 조회"""
    try:
        db_name = validate_db_name(db_name)
        result = await oms.get_database(db_name)
        return {"status": "success", "data": result}
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to get database %r: %s", db_name, exc)
        if "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_database_expected_seq(*, db_name: str) -> Dict[str, Any]:
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
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for expected-seq (%s): %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except Exception as exc:
        logger.error("Failed to get expected_seq for database %r: %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def list_classes(
    *,
    db_name: str,
    type: Optional[str],  # noqa: A002 - Keep API surface (legacy query param).
    limit: Optional[int],
    oms: OMSClient,
) -> Dict[str, Any]:
    """데이터베이스의 클래스 목록 조회"""
    try:
        db_name = validate_db_name(db_name)
        _ = type
        _ = limit

        result = await oms.list_ontologies(db_name)

        classes = result.get("data", {}).get("ontologies", [])
        if not isinstance(classes, list):
            classes = []

        if not any(isinstance(item, dict) and str(item.get("id") or "").strip() == _ACTION_LOG_CLASS_ID for item in classes):
            classes.append(
                {
                    "id": _ACTION_LOG_CLASS_ID,
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
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to list classes for database %r: %s", db_name, exc)
        if "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def create_class(*, db_name: str, class_data: Dict[str, Any], oms: OMSClient) -> Dict[str, Any]:
    """데이터베이스에 새 클래스 생성"""
    try:
        db_name = validate_db_name(db_name)
        class_data = sanitize_input(class_data)

        if not class_data.get("@id"):
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="클래스 ID (@id)가 필요합니다")

        oms_data = class_data.copy()
        oms_data["id"] = oms_data.pop("@id", None)

        result = await oms.create_ontology(db_name, oms_data)

        return {"status": "success", "@id": class_data.get("@id"), "data": result}
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for create_class (%s): %s", db_name, exc)
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except httpx.HTTPStatusError as exc:
        upstream_status = int(getattr(exc.response, "status_code", 502) or 502)
        try:
            upstream_body: Any = exc.response.json()
        except Exception:  # pragma: no cover (depends on upstream)
            upstream_body = getattr(exc.response, "text", str(exc))

        if upstream_status in {400, 401, 403, 404, 409, 422}:
            raise HTTPException(status_code=upstream_status, detail=upstream_body) from exc

        raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY, detail=upstream_body) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create class in database %r: %s", db_name, exc)

        if "already exists" in str(exc).lower() or "duplicate" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"클래스 '{class_data.get('@id')}'이(가) 이미 존재합니다",
            ) from exc

        if "invalid" in str(exc).lower():
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=f"잘못된 클래스 데이터: {str(exc)}") from exc

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_class(*, db_name: str, class_id: str, oms: OMSClient) -> Dict[str, Any]:
    """특정 클래스 조회"""
    try:
        db_name = validate_db_name(db_name)
        class_id = sanitize_input(class_id)

        if str(class_id or "").strip() == _ACTION_LOG_CLASS_ID:
            return {
                "status": "success",
                "message": "Virtual ontology class",
                "data": {
                    "id": _ACTION_LOG_CLASS_ID,
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

        result = await oms.get_ontology(db_name, class_id)
        return result
    except Exception as exc:
        logger.error("Failed to get class %r from database %r: %s", class_id, db_name, exc)

        if "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"클래스 '{class_id}'을(를) 찾을 수 없습니다",
            ) from exc

        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def list_branches(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 목록 조회"""
    try:
        db_name = validate_db_name(db_name)

        result = await oms.list_branches(db_name)
        branches = result.get("data", {}).get("branches", [])

        return {"branches": branches, "count": len(branches)}
    except Exception as exc:
        logger.error("Failed to list branches for database %r: %s", db_name, exc)

        if "database not found" in str(exc).lower() or "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc
        if "access denied" in str(exc).lower() or "unauthorized" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="브랜치 목록 조회 권한이 없습니다",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"브랜치 목록 조회 실패: {str(exc)}",
        ) from exc


async def create_branch(*, db_name: str, branch_data: Dict[str, Any], oms: OMSClient) -> Dict[str, Any]:
    """새 브랜치 생성"""
    try:
        db_name = validate_db_name(db_name)
        branch_data = sanitize_input(branch_data)

        branch_name = branch_data.get("name")
        if not branch_name:
            raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="브랜치 이름이 필요합니다")
        branch_name = validate_branch_name(branch_name)

        from_branch = branch_data.get("from_branch", "main")
        if from_branch:
            from_branch = validate_branch_name(from_branch)

        oms_branch_data = {"branch_name": branch_name, "from_branch": from_branch}

        result = await oms.create_branch(db_name, oms_branch_data)

        return {"status": "success", "name": branch_name, "data": result}
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Failed to create branch in database %r: %s", db_name, exc)

        if "branch already exists" in str(exc).lower() or "already exists" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"브랜치 '{branch_data.get('name')}'이(가) 이미 존재합니다",
            ) from exc
        if "database not found" in str(exc).lower() or "not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"브랜치 생성 실패: {str(exc)}",
        ) from exc


async def get_versions(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """버전 히스토리 조회"""
    try:
        db_name = validate_db_name(db_name)

        result = await oms.get_version_history(db_name)
        versions = result.get("data", {}).get("versions", [])

        return {"versions": versions, "count": len(versions)}
    except Exception as exc:
        logger.error("Failed to get versions for database %r: %s", db_name, exc)

        if "database not found" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            ) from exc
        if "access denied" in str(exc).lower() or "unauthorized" in str(exc).lower():
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="버전 히스토리 조회 권한이 없습니다",
            ) from exc
        if "no commits" in str(exc).lower() or "empty history" in str(exc).lower():
            return {"versions": [], "count": 0}
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"버전 히스토리 조회 실패: {str(exc)}",
        ) from exc

