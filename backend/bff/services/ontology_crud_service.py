"""Ontology CRUD service (BFF).

Extracted from `bff.routers.ontology_crud` to keep routers thin and to
deduplicate common CRUD/control-flow (Facade pattern).
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional, Sequence

from fastapi import HTTPException, Request, status
from fastapi.responses import JSONResponse

from shared.errors.error_types import ErrorCode, ErrorCategory, classified_http_exception
from bff.routers.ontology_ops import _transform_properties_for_oms
from bff.services.ontology_class_id_service import resolve_or_generate_class_id
from bff.services.ontology_label_mapper_service import register_ontology_label_mappings
from bff.services.oms_error_policy import raise_oms_boundary_exception
from bff.services.oms_client import OMSClient
from bff.utils.request_headers import extract_forward_headers
from shared.models.ontology import OntologyCreateRequestBFF, OntologyResponse, OntologyUpdateInput
from shared.models.responses import ApiResponse
from shared.security.input_sanitizer import (
    sanitize_input,
    validate_branch_name,
    validate_db_name,
)
from shared.utils.label_mapper import LabelMapper
from shared.utils.language import get_accept_language
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


def _as_string(value: Any, *, lang: str, fallback: str) -> str:
    if isinstance(value, dict):
        selected = value.get(lang) or value.get("en") or value.get("ko")
        if selected is None:
            return fallback
        value = selected
    if value is None:
        return fallback
    if isinstance(value, str):
        return value
    return str(value)


async def _resolve_class_id(*, db_name: str, class_label: str, lang: str, mapper: LabelMapper) -> str:
    mapped = await mapper.get_class_id(db_name, class_label, lang)
    return mapped or class_label


@trace_external_call("bff.ontology_crud.create_ontology")
async def create_ontology(
    *,
    db_name: str,
    body: OntologyCreateRequestBFF,
    branch: str,
    mapper: LabelMapper,
    oms_client: OMSClient,
) -> JSONResponse:
    """
    온톨로지 생성

    새로운 온톨로지 클래스를 생성합니다.
    레이블 기반으로 ID가 자동 생성됩니다.
    """
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        ontology_dict = sanitize_input(body.model_dump(exclude_unset=True))

        class_id = resolve_or_generate_class_id(ontology_dict)

        ontology_dict["id"] = class_id
        _transform_properties_for_oms(ontology_dict, log_conversions=True)

        result = await oms_client.create_ontology(db_name, ontology_dict, branch=branch)
        await register_ontology_label_mappings(
            mapper=mapper,
            db_name=db_name,
            class_id=class_id,
            ontology_dict=ontology_dict,
        )

        if isinstance(result, dict) and result.get("status") == "accepted" and "data" in result:
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        if isinstance(result, dict) and "status" in result and "message" in result:
            return JSONResponse(status_code=status.HTTP_200_OK, content=result)

        if isinstance(result, dict) and "id" in result:
            class_id = result.get("id") or class_id

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_id}'이(가) 생성되었습니다",
                data={"ontology_id": class_id, "ontology": result, "mode": "direct"},
            ).to_dict(),
        )

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:105", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 생성", logger=logger)


@trace_external_call("bff.ontology_crud.list_ontologies")
async def list_ontologies(
    *,
    db_name: str,
    request: Request,
    branch: str,
    class_type: str,
    limit: Optional[int],
    offset: int,
    mapper: LabelMapper,
    terminus: Any,
) -> Dict[str, Any]:
    """
    온톨로지 목록 조회

    데이터베이스의 모든 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)

        allowed_class_types = {"sys:Class", "owl:Class", "rdfs:Class"}
        if class_type not in allowed_class_types:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Invalid class_type. Allowed values: {', '.join(allowed_class_types)}",
                code=ErrorCode.ONTOLOGY_VALIDATION_FAILED,
            )

        ontologies = await terminus.list_classes(db_name, branch=branch)
        if not isinstance(ontologies, list):
            ontologies = []

        labeled_ontologies = await mapper.convert_to_display_batch(db_name, ontologies, lang)
        if not isinstance(labeled_ontologies, list):
            labeled_ontologies = []

        total = len(labeled_ontologies)
        page = labeled_ontologies
        if offset:
            page = page[max(0, int(offset)) :]
        if limit is not None:
            page = page[: max(0, int(limit))]

        return {
            "total": total,
            "ontologies": page,
            "offset": offset,
            "limit": limit,
            "branch": branch,
        }

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:162", exc_info=True)
        raise_oms_boundary_exception(
            exc=exc,
            action="온톨로지 목록 조회",
            logger=logger,
            custom_http_status_details={
                status.HTTP_404_NOT_FOUND: f"데이터베이스 '{db_name}'을(를) 찾을 수 없습니다",
            },
        )


@trace_external_call("bff.ontology_crud.get_ontology")
async def get_ontology(
    *,
    db_name: str,
    class_label: str,
    request: Request,
    branch: str,
    mapper: LabelMapper,
    terminus: Any,
) -> OntologyResponse:
    """
    온톨로지 조회

    레이블 또는 ID로 온톨로지를 조회합니다.
    """
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        branch = validate_branch_name(branch)

        class_id = await _resolve_class_id(db_name=db_name, class_label=class_label, lang=lang, mapper=mapper)
        result = await terminus.get_class(db_name, class_id, branch=branch)
        if not result:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_label}'을(를) 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        ontology_data = result if isinstance(result, dict) else {}
        ontology_data = await mapper.convert_to_display(db_name, ontology_data, lang)

        if not ontology_data.get("id"):
            ontology_data["id"] = class_id
        if not ontology_data.get("label"):
            label = await mapper.get_class_label(db_name, class_id, lang)
            ontology_data["label"] = label or class_label

        label_value = _as_string(ontology_data.get("label"), lang=lang, fallback=class_label)
        desc_value = _as_string(ontology_data.get("description"), lang=lang, fallback="")

        ontology_base_data = {
            "id": ontology_data.get("id"),
            "label": label_value,
            "description": desc_value,
            "properties": ontology_data.get("properties", []),
            "relationships": ontology_data.get("relationships", []),
            "parent_class": ontology_data.get("parent_class"),
            "abstract": ontology_data.get("abstract", False),
            "metadata": {**(ontology_data.get("metadata", {}) or {}), "branch": branch},
        }
        return OntologyResponse(**ontology_base_data)

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:227", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 조회", logger=logger)


@trace_external_call("bff.ontology_crud.validate_ontology_create")
async def validate_ontology_create(
    *,
    db_name: str,
    body: OntologyCreateRequestBFF,
    branch: str,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """온톨로지 생성 검증 (no write) - OMS proxy."""
    try:
        db_name = validate_db_name(db_name)
        branch = validate_branch_name(branch)
        payload = sanitize_input(body.model_dump(exclude_unset=True))
        return await oms_client.validate_ontology_create(db_name, payload, branch=branch)
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:245", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 생성 검증", logger=logger)


@trace_external_call("bff.ontology_crud.validate_ontology_update")
async def validate_ontology_update(
    *,
    db_name: str,
    class_label: str,
    body: OntologyUpdateInput,
    request: Request,
    branch: str,
    mapper: LabelMapper,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """온톨로지 업데이트 검증 (no write) - OMS proxy."""
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        branch = validate_branch_name(branch)

        class_id = await _resolve_class_id(db_name=db_name, class_label=class_label, lang=lang, mapper=mapper)
        payload = sanitize_input(body.model_dump(exclude_unset=True))
        return await oms_client.validate_ontology_update(db_name, class_id, payload, branch=branch)
    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:270", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 업데이트 검증", logger=logger)


@trace_external_call("bff.ontology_crud.update_ontology")
async def update_ontology(
    *,
    db_name: str,
    class_label: str,
    body: OntologyUpdateInput,
    request: Request,
    expected_seq: int,
    branch: str,
    mapper: LabelMapper,
    terminus: Any,
) -> JSONResponse:
    """
    온톨로지 수정

    기존 온톨로지를 수정합니다.
    """
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        branch = validate_branch_name(branch)

        class_id = await _resolve_class_id(db_name=db_name, class_label=class_label, lang=lang, mapper=mapper)

        update_data = body.model_dump(exclude_unset=True)
        update_data["id"] = class_id

        forward_headers = extract_forward_headers(request)
        result = await terminus.update_class(
            db_name,
            class_id,
            update_data,
            expected_seq=expected_seq,
            branch=branch,
            headers=forward_headers or None,
        )

        await mapper.update_mappings(db_name, update_data)

        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_label}'이(가) 수정되었습니다",
                data={"ontology_id": class_id, "updated_fields": list(update_data.keys()), "mode": "direct"},
            ).to_dict(),
        )

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:325", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 수정", logger=logger)


@trace_external_call("bff.ontology_crud.delete_ontology")
async def delete_ontology(
    *,
    db_name: str,
    class_label: str,
    request: Request,
    expected_seq: int,
    branch: str,
    mapper: LabelMapper,
    terminus: Any,
) -> JSONResponse:
    """
    온톨로지 삭제

    온톨로지를 삭제합니다. (OCC protected)
    """
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_label = sanitize_input(class_label)
        branch = validate_branch_name(branch)

        class_id = await _resolve_class_id(db_name=db_name, class_label=class_label, lang=lang, mapper=mapper)

        forward_headers = extract_forward_headers(request)
        result = await terminus.delete_class(
            db_name,
            class_id,
            expected_seq=expected_seq,
            branch=branch,
            headers=forward_headers or None,
        )

        await mapper.remove_class(db_name, class_id)

        if isinstance(result, dict) and result.get("status") == "accepted":
            return JSONResponse(status_code=status.HTTP_202_ACCEPTED, content=result)

        return JSONResponse(
            status_code=status.HTTP_200_OK,
            content=ApiResponse.success(
                message=f"온톨로지 '{class_label}'이(가) 삭제되었습니다",
                data={"ontology_id": class_id, "mode": "direct"},
            ).to_dict(),
        )

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:375", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 삭제", logger=logger)


@trace_external_call("bff.ontology_crud.get_ontology_schema")
async def get_ontology_schema(
    *,
    db_name: str,
    class_id: str,
    request: Request,
    format: str,
    branch: str,
    mapper: LabelMapper,
    terminus: Any,
    jsonld_conv: Any,
) -> Any:
    """
    온톨로지 스키마 조회

    온톨로지의 스키마를 다양한 형식으로 조회합니다.
    """
    lang = get_accept_language(request)
    try:
        db_name = validate_db_name(db_name)
        class_id = sanitize_input(class_id)
        branch = validate_branch_name(branch)

        allowed_formats = {"json", "jsonld", "owl"}
        if format not in allowed_formats:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                f"Invalid format. Allowed values: {', '.join(allowed_formats)}",
                code=ErrorCode.ONTOLOGY_VALIDATION_FAILED,
            )

        actual_id = await _resolve_class_id(db_name=db_name, class_label=class_id, lang=lang, mapper=mapper)
        ontology = await terminus.get_class(db_name, actual_id, branch=branch)
        if not ontology:
            raise classified_http_exception(
                status.HTTP_404_NOT_FOUND,
                f"온톨로지 '{class_id}'을(를) 찾을 수 없습니다",
                code=ErrorCode.ONTOLOGY_NOT_FOUND,
            )

        if format == "jsonld":
            return jsonld_conv.convert_to_jsonld(ontology, db_name)
        if format == "owl":
            schema = jsonld_conv.convert_to_jsonld(ontology, db_name)
            if isinstance(schema, dict):
                schema["@comment"] = "This is a JSON-LD representation compatible with OWL"
                schema["@owl:versionInfo"] = "1.0"
            return schema
        return ontology

    except Exception as exc:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/services/ontology_crud_service.py:429", exc_info=True)
        raise_oms_boundary_exception(exc=exc, action="온톨로지 스키마 조회", logger=logger)
