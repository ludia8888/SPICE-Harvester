"""Async instance service (BFF).

Extracted from `bff.routers.instance_async` to keep routers thin and to
deduplicate label->id conversion + OMS forwarding (Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException, Request, status
from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.services.oms_client import OMSClient
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.models.commands import CommandResult
from shared.security.input_sanitizer import (
    SecurityViolationError,
    sanitize_label_input,
    input_sanitizer,
    validate_branch_name,
    validate_class_id,
    validate_db_name,
    validate_instance_id,
)
from shared.utils.label_mapper import LabelMapper
from shared.utils.language import get_accept_language
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


def _metadata_dict(value: Any) -> Dict[str, Any]:
    if isinstance(value, dict):
        return dict(value)
    return {}


def _fallback_langs(primary: str) -> List[str]:
    langs = [primary, "ko", "en"]
    out: List[str] = []
    for item in langs:
        item = str(item or "").strip()
        if item and item not in out:
            out.append(item)
    return out


@trace_external_call("bff.instance_async.resolve_class_id")
async def resolve_class_id(
    *,
    db_name: str,
    class_label: str,
    label_mapper: LabelMapper,
    lang: str,
) -> str:
    for candidate_lang in _fallback_langs(lang):
        class_id = await label_mapper.get_class_id(db_name, class_label, candidate_lang)
        if class_id:
            return str(class_id)
    return validate_class_id(class_label)


@trace_external_call("bff.instance_async.convert_labels_to_ids")
async def convert_labels_to_ids(
    data: Dict[str, Any],
    db_name: str,
    class_id: str,
    label_mapper: LabelMapper,
    *,
    lang: str,
) -> Dict[str, Any]:
    """
    Label 기반 데이터를 ID 기반으로 변환.

    - property label -> property_id via LabelMapper
    - if no mapping exists, allow safe internal property_id keys
    """

    converted: Dict[str, Any] = {}
    unknown_labels: List[str] = []

    for label, value in (data or {}).items():
        property_id: Optional[str] = None
        for candidate_lang in _fallback_langs(lang):
            property_id = await label_mapper.get_property_id(db_name, class_id, label, candidate_lang)
            if property_id:
                break

        if property_id:
            converted[property_id] = value
            continue

        # Allow advanced clients to pass internal property_id directly (must be a safe field name).
        try:
            input_sanitizer.sanitize_field_name(label)
            converted[label] = value
        except Exception:
            logging.getLogger(__name__).warning("Exception fallback at bff/services/instance_async_service.py:100", exc_info=True)
            unknown_labels.append(label)

    if unknown_labels:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "Unknown label keys",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
            extra={
                "error": "unknown_label_keys",
                "class_id": class_id,
                "labels": unknown_labels,
                "hint": "Use existing property labels (as defined in ontology) or pass internal property_id keys.",
            },
        )

    return converted


async def _handle_command_errors(fn, *args, op_message: str, **kwargs):  # noqa: ANN001
    try:
        return await fn(*args, **kwargs)
    except HTTPException:
        raise
    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
        raise  # pragma: no cover
    except httpx.HTTPError as exc:
        raise classified_http_exception(502, "OMS 요청 실패", code=ErrorCode.UPSTREAM_ERROR) from exc
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(400, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except Exception as exc:
        logger.error("%s: %s", op_message, exc, exc_info=True)
        raise classified_http_exception(500, f"{op_message}: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.instance_async.create_instance_async")
async def create_instance_async(
    *,
    db_name: str,
    class_label: str,
    data: Dict[str, Any],
    metadata: Any,
    http_request: Request,
    branch: str,
    oms_client: OMSClient,
    label_mapper: LabelMapper,
    user_id: Optional[str],
) -> CommandResult:
    async def _impl() -> CommandResult:
        validated_db = validate_db_name(db_name)
        validated_branch = validate_branch_name(branch)

        lang = get_accept_language(http_request)
        class_id = await resolve_class_id(
            db_name=validated_db,
            class_label=class_label,
            label_mapper=label_mapper,
            lang=lang,
        )

        sanitized_data = sanitize_label_input(data)
        converted = await convert_labels_to_ids(
            sanitized_data,
            validated_db,
            class_id,
            label_mapper,
            lang=lang,
        )

        oms_request = {
            "data": converted,
            "metadata": {
                **_metadata_dict(metadata),
                "original_class_label": class_label,
                "user_id": user_id,
            },
        }

        response = await oms_client.post(
            f"/api/v1/instances/{validated_db}/async/{class_id}/create",
            params={"branch": validated_branch},
            json=oms_request,
        )
        return CommandResult(**response)

    return await _handle_command_errors(_impl, op_message="인스턴스 생성 명령 실패")


@trace_external_call("bff.instance_async.update_instance_async")
async def update_instance_async(
    *,
    db_name: str,
    class_label: str,
    instance_id: str,
    data: Dict[str, Any],
    metadata: Any,
    http_request: Request,
    expected_seq: int,
    branch: str,
    oms_client: OMSClient,
    label_mapper: LabelMapper,
    user_id: Optional[str],
) -> CommandResult:
    async def _impl() -> CommandResult:
        validated_db = validate_db_name(db_name)
        validate_instance_id(instance_id)
        validated_branch = validate_branch_name(branch)

        lang = get_accept_language(http_request)
        class_id = await resolve_class_id(
            db_name=validated_db,
            class_label=class_label,
            label_mapper=label_mapper,
            lang=lang,
        )

        sanitized_data = sanitize_label_input(data)
        converted = await convert_labels_to_ids(
            sanitized_data,
            validated_db,
            class_id,
            label_mapper,
            lang=lang,
        )

        oms_request = {
            "data": converted,
            "metadata": {
                **_metadata_dict(metadata),
                "original_class_label": class_label,
                "user_id": user_id,
            },
        }

        response = await oms_client.put(
            f"/api/v1/instances/{validated_db}/async/{class_id}/{instance_id}/update",
            params={"expected_seq": int(expected_seq), "branch": validated_branch},
            json=oms_request,
        )
        return CommandResult(**response)

    return await _handle_command_errors(_impl, op_message="인스턴스 수정 명령 실패")


@trace_external_call("bff.instance_async.delete_instance_async")
async def delete_instance_async(
    *,
    db_name: str,
    class_label: str,
    instance_id: str,
    http_request: Request,
    branch: str,
    expected_seq: int,
    oms_client: OMSClient,
    label_mapper: LabelMapper,
    user_id: Optional[str],
) -> CommandResult:
    async def _impl() -> CommandResult:
        validated_db = validate_db_name(db_name)
        validate_instance_id(instance_id)
        validated_branch = validate_branch_name(branch)

        lang = get_accept_language(http_request)
        class_id = await resolve_class_id(
            db_name=validated_db,
            class_label=class_label,
            label_mapper=label_mapper,
            lang=lang,
        )

        response = await oms_client.delete(
            f"/api/v1/instances/{validated_db}/async/{class_id}/{instance_id}/delete",
            params={"expected_seq": int(expected_seq), "branch": validated_branch},
        )
        return CommandResult(**response)

    _ = user_id
    return await _handle_command_errors(_impl, op_message="인스턴스 삭제 명령 실패")


@trace_external_call("bff.instance_async.bulk_create_instances_async")
async def bulk_create_instances_async(
    *,
    db_name: str,
    class_label: str,
    instances: List[Dict[str, Any]],
    metadata: Any,
    http_request: Request,
    branch: str,
    oms_client: OMSClient,
    label_mapper: LabelMapper,
    user_id: Optional[str],
) -> CommandResult:
    async def _impl() -> CommandResult:
        validated_db = validate_db_name(db_name)
        validated_branch = validate_branch_name(branch)

        lang = get_accept_language(http_request)
        class_id = await resolve_class_id(
            db_name=validated_db,
            class_label=class_label,
            label_mapper=label_mapper,
            lang=lang,
        )

        sanitized_instances = [sanitize_label_input(instance) for instance in (instances or [])]
        converted_instances: List[Dict[str, Any]] = []
        for instance_data in sanitized_instances:
            converted = await convert_labels_to_ids(
                instance_data,
                validated_db,
                class_id,
                label_mapper,
                lang=lang,
            )
            converted_instances.append(converted)

        oms_request = {
            "instances": converted_instances,
            "metadata": {
                **_metadata_dict(metadata),
                "original_class_label": class_label,
                "user_id": user_id,
            },
        }

        response = await oms_client.post(
            f"/api/v1/instances/{validated_db}/async/{class_id}/bulk-create",
            params={"branch": validated_branch},
            json=oms_request,
        )
        return CommandResult(**response)

    return await _handle_command_errors(_impl, op_message="대량 인스턴스 생성 명령 실패")
