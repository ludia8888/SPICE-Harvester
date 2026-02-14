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

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.services.database_error_policy import MessageErrorPolicy, apply_message_error_policies
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
from shared.observability.tracing import trace_external_call, trace_db_operation

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


def _validate_db_branch_pair(*, db_name: str, branch_name: str) -> tuple[str, str]:
    return validate_db_name(db_name), validate_branch_name(branch_name)


def _database_not_found_policy(*, db_name: str) -> MessageErrorPolicy:
    return MessageErrorPolicy(
        patterns=("database not found", "not found"),
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
    )


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


@trace_external_call("bff.database.list_databases")
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
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.database.create_database")
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
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:265", exc_info=True)
            detail = exc.response.text or str(exc)

        if status_code == status.HTTP_409_CONFLICT:
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            if not isinstance(detail, str) or not detail.strip():
                detail = f"데이터베이스 '{body.name}'이(가) 이미 존재합니다"
        raise classified_http_exception(status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", body.name, exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:279", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to create database {body.name!r}: %s",
            policies=(
                MessageErrorPolicy(
                    patterns=("already exists", "duplicate"),
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"데이터베이스 '{body.name}'이(가) 이미 존재합니다",
                ),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_external_call("bff.database.delete_database")
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
            raise classified_http_exception(
                status.HTTP_403_FORBIDDEN,
                f"시스템 데이터베이스 '{validated_db_name}'은(는) 삭제할 수 없습니다",
                code=ErrorCode.PERMISSION_DENIED,
            )

        actor_type, actor_id, _actor_name = resolve_database_actor_with_name(http_request.headers)
        if not _is_dev_mode():
            role = await get_database_access_role(
                db_name=validated_db_name,
                principal_type=actor_type,
                principal_id=actor_id,
            )
            if not role or role.lower() != "owner":
                raise classified_http_exception(status.HTTP_403_FORBIDDEN, "Only owners can delete projects.", code=ErrorCode.PERMISSION_DENIED)

        if expected_seq is None:
            expected_seq = await _get_expected_seq_for_database(validated_db_name)

        try:
            result = await oms.delete_database(validated_db_name, expected_seq=expected_seq)
        except httpx.HTTPStatusError as exc:
            status_code = exc.response.status_code
            try:
                detail: Any = exc.response.json()
            except Exception:
                logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:335", exc_info=True)
                detail = exc.response.text or str(exc)
            if isinstance(detail, dict):
                detail = detail.get("detail") or detail.get("message") or detail.get("error") or detail
            raise classified_http_exception(status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from exc
        except httpx.RequestError as exc:
            raise classified_http_exception(status.HTTP_503_SERVICE_UNAVAILABLE, str(exc), code=ErrorCode.UPSTREAM_UNAVAILABLE) from exc

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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:370", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to delete database {db_name!r}: %s",
            policies=(_database_not_found_policy(db_name=db_name),),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_external_call("bff.database.get_branch_info")
async def get_branch_info(*, db_name: str, branch_name: str, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 정보 조회 (프론트엔드용 BFF 래핑)"""
    try:
        db_name, branch_name = _validate_db_branch_pair(db_name=db_name, branch_name=branch_name)
        return await oms.get_branch_info(db_name, branch_name)
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for branch info (%s/%s): %s", db_name, branch_name, exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except httpx.HTTPStatusError as exc:
        resp = getattr(exc, "response", None)
        if resp is not None and resp.status_code == 404:
            raise classified_http_exception(status.HTTP_404_NOT_FOUND, "브랜치를 찾을 수 없습니다", code=ErrorCode.RESOURCE_NOT_FOUND) from exc
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, "OMS 브랜치 조회 실패", code=ErrorCode.UPSTREAM_ERROR) from exc
    except httpx.HTTPError as exc:
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, "OMS 브랜치 조회 실패", code=ErrorCode.UPSTREAM_ERROR) from exc
    except Exception as exc:
        logger.error("Failed to get branch info (%s/%s): %s", db_name, branch_name, exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.database.delete_branch")
async def delete_branch(*, db_name: str, branch_name: str, force: bool, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 삭제 (프론트엔드용 BFF 래핑)"""
    try:
        db_name, branch_name = _validate_db_branch_pair(db_name=db_name, branch_name=branch_name)
        return await oms.delete_branch(db_name, branch_name, force=force)
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for branch delete (%s/%s): %s", db_name, branch_name, exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except httpx.HTTPStatusError as exc:
        resp = getattr(exc, "response", None)
        if resp is not None:
            if resp.status_code == 404:
                raise classified_http_exception(status.HTTP_404_NOT_FOUND, "브랜치를 찾을 수 없습니다", code=ErrorCode.RESOURCE_NOT_FOUND) from exc
            if resp.status_code == 403:
                raise classified_http_exception(
                    status.HTTP_403_FORBIDDEN,
                    protected_branch_write_message(),
                    code=ErrorCode.PERMISSION_DENIED,
                ) from exc
            if resp.status_code == 400:
                raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "현재 브랜치는 삭제할 수 없습니다", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, "OMS 브랜치 삭제 실패", code=ErrorCode.UPSTREAM_ERROR) from exc
    except httpx.HTTPError as exc:
        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, "OMS 브랜치 삭제 실패", code=ErrorCode.UPSTREAM_ERROR) from exc
    except Exception as exc:
        logger.error("Failed to delete branch (%s/%s): %s", db_name, branch_name, exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.database.get_database")
async def get_database(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """데이터베이스 정보 조회"""
    try:
        db_name = validate_db_name(db_name)
        result = await oms.get_database(db_name)
        return {"status": "success", "data": result}
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for database %r: %s", db_name, exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:444", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to get database {db_name!r}: %s",
            policies=(_database_not_found_policy(db_name=db_name),),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_db_operation("bff.database.get_database_expected_seq")
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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except ValueError as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except Exception as exc:
        logger.error("Failed to get expected_seq for database %r: %s", db_name, exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, str(exc), code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.database.list_classes")
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
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:519", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to list classes for database {db_name!r}: %s",
            policies=(_database_not_found_policy(db_name=db_name),),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_external_call("bff.database.create_class")
async def create_class(*, db_name: str, class_data: Dict[str, Any], oms: OMSClient) -> Dict[str, Any]:
    """데이터베이스에 새 클래스 생성"""
    try:
        db_name = validate_db_name(db_name)
        class_data = sanitize_input(class_data)

        if not class_data.get("@id"):
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "클래스 ID (@id)가 필요합니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        oms_data = class_data.copy()
        oms_data["id"] = oms_data.pop("@id", None)

        result = await oms.create_ontology(db_name, oms_data)

        return {"status": "success", "@id": class_data.get("@id"), "data": result}
    except SecurityViolationError as exc:
        logger.warning("Security validation failed for create_class (%s): %s", db_name, exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
    except httpx.HTTPStatusError as exc:
        upstream_status = int(getattr(exc.response, "status_code", 502) or 502)
        try:
            upstream_body: Any = exc.response.json()
        except Exception:  # pragma: no cover (depends on upstream)
            logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:553", exc_info=True)
            upstream_body = getattr(exc.response, "text", str(exc))

        if upstream_status in {400, 401, 403, 404, 409, 422}:
            code_by_status = {
                400: ErrorCode.REQUEST_VALIDATION_FAILED,
                401: ErrorCode.AUTH_REQUIRED,
                403: ErrorCode.PERMISSION_DENIED,
                404: ErrorCode.RESOURCE_NOT_FOUND,
                409: ErrorCode.CONFLICT,
                422: ErrorCode.REQUEST_VALIDATION_FAILED,
            }
            raise classified_http_exception(
                upstream_status,
                str(upstream_body),
                code=code_by_status.get(upstream_status, ErrorCode.UPSTREAM_ERROR),
            ) from exc

        raise classified_http_exception(status.HTTP_502_BAD_GATEWAY, str(upstream_body) if not isinstance(upstream_body, str) else upstream_body, code=ErrorCode.UPSTREAM_ERROR) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:574", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to create class in database {db_name!r}: %s",
            policies=(
                MessageErrorPolicy(
                    patterns=("already exists", "duplicate"),
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"클래스 '{class_data.get('@id')}'이(가) 이미 존재합니다",
                ),
                MessageErrorPolicy(
                    patterns=("invalid",),
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"잘못된 클래스 데이터: {str(exc)}",
                ),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_external_call("bff.database.get_class")
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
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:640", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to get class {class_id!r} from database {db_name!r}: %s",
            policies=(
                MessageErrorPolicy(
                    patterns=("not found",),
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"클래스 '{class_id}'을(를) 찾을 수 없습니다",
                ),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=str(exc),
        )


@trace_external_call("bff.database.list_branches")
async def list_branches(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """브랜치 목록 조회"""
    try:
        db_name = validate_db_name(db_name)

        result = await oms.list_branches(db_name)
        branches = result.get("data", {}).get("branches", [])

        return {"branches": branches, "count": len(branches)}
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:667", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to list branches for database {db_name!r}: %s",
            policies=(
                _database_not_found_policy(db_name=db_name),
                MessageErrorPolicy(
                    patterns=("access denied", "unauthorized"),
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="브랜치 목록 조회 권한이 없습니다",
                ),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=f"브랜치 목록 조회 실패: {str(exc)}",
        )


@trace_external_call("bff.database.create_branch")
async def create_branch(*, db_name: str, branch_data: Dict[str, Any], oms: OMSClient) -> Dict[str, Any]:
    """새 브랜치 생성"""
    try:
        db_name = validate_db_name(db_name)
        branch_data = sanitize_input(branch_data)

        branch_name = branch_data.get("name")
        if not branch_name:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "브랜치 이름이 필요합니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)
        branch_name = validate_branch_name(branch_name)

        from_branch = branch_data.get("from_branch", "main")
        if from_branch:
            from_branch = validate_branch_name(from_branch)

        result = await oms.create_branch(db_name, branch_name, from_branch=from_branch)

        return {"status": "success", "name": branch_name, "data": result}
    except HTTPException:
        raise
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:706", exc_info=True)
        apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to create branch in database {db_name!r}: %s",
            policies=(
                MessageErrorPolicy(
                    patterns=("branch already exists", "already exists"),
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"브랜치 '{branch_data.get('name')}'이(가) 이미 존재합니다",
                ),
                _database_not_found_policy(db_name=db_name),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=f"브랜치 생성 실패: {str(exc)}",
        )


@trace_external_call("bff.database.get_versions")
async def get_versions(*, db_name: str, oms: OMSClient) -> Dict[str, Any]:
    """버전 히스토리 조회"""
    try:
        db_name = validate_db_name(db_name)

        result = await oms.get_version_history(db_name)
        versions = result.get("data", {}).get("versions", [])

        return {"versions": versions, "count": len(versions)}
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/database_service.py:734", exc_info=True)
        return apply_message_error_policies(
            exc=exc,
            logger=logger,
            log_message=f"Failed to get versions for database {db_name!r}: %s",
            policies=(
                _database_not_found_policy(db_name=db_name),
                MessageErrorPolicy(
                    patterns=("access denied", "unauthorized"),
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="버전 히스토리 조회 권한이 없습니다",
                ),
                MessageErrorPolicy(
                    patterns=("no commits", "empty history"),
                    status_code=status.HTTP_200_OK,
                    detail="",
                    fallback_return={"versions": [], "count": 0},
                ),
            ),
            default_status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            default_detail=f"버전 히스토리 조회 실패: {str(exc)}",
        )
