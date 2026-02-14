"""
Label mapping domain logic (BFF).

Extracted from `bff.routers.mapping` to keep routers thin and to deduplicate
common file-import workflows (Template Method).
"""

from __future__ import annotations

import hashlib
import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, Union

from fastapi import HTTPException, UploadFile, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from bff.services.oms_client import OMSClient
from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode, classified_http_exception
from shared.observability.request_context import get_correlation_id, get_request_id
from shared.security.input_sanitizer import SecurityViolationError, sanitize_input
from shared.utils.label_mapper import LabelMapper

logger = logging.getLogger(__name__)

SERVICE_NAME = "BFF"


class MappingImportPayload(BaseModel):
    """
    Label mapping bundle file schema.

    This matches the shape produced by `LabelMapper.export_mappings()`:
    - db_name
    - classes/properties/relationships arrays (each item is a dict row)
    """

    db_name: Optional[str] = None
    classes: List[Dict[str, Any]] = Field(default_factory=list)
    properties: List[Dict[str, Any]] = Field(default_factory=list)
    relationships: List[Dict[str, Any]] = Field(default_factory=list)


@dataclass(frozen=True)
class MappingBundleContext:
    db_name: str
    content_hash: str
    mapping_request: MappingImportPayload
    sanitized_mappings: Dict[str, Any]
    total_mappings: int


def _validation_details_template() -> Dict[str, Any]:
    return {"unmapped_classes": [], "unmapped_properties": [], "conflicts": [], "duplicate_labels": []}


async def export_mappings(*, db_name: str, mapper: LabelMapper) -> JSONResponse:
    """
    레이블 매핑 내보내기

    데이터베이스의 모든 레이블 매핑을 내보냅니다.
    """
    try:
        mappings = await mapper.export_mappings(db_name)
        return JSONResponse(
            content=mappings,
            headers={"Content-Disposition": f"attachment; filename={db_name}_mappings.json"},
        )
    except Exception as exc:
        logger.error("Failed to export mappings: %s", exc)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"매핑 내보내기 실패: {str(exc)}",
            code=ErrorCode.INTERNAL_ERROR,
        ) from exc


async def _validate_file_upload(file: UploadFile) -> None:
    """Validate uploaded file size, type, and extension."""
    if file.size is not None and file.size > 10 * 1024 * 1024:  # 10MB
        raise classified_http_exception(
            status.HTTP_413_REQUEST_ENTITY_TOO_LARGE,
            "파일 크기가 너무 큽니다 (최대 10MB)",
            code=ErrorCode.PAYLOAD_TOO_LARGE,
        )

    if file.size is not None and file.size == 0:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "빈 파일입니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    if file.content_type and not file.content_type.startswith("application/json"):
        logger.warning("Suspicious content type: %s", file.content_type)

    if not file.filename or not any(file.filename.lower().endswith(ext) for ext in [".json", ".txt"]):
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "JSON 파일만 지원됩니다 (.json 또는 .txt)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )


async def _read_and_parse_file(file: UploadFile) -> Tuple[str, Dict[str, Any]]:
    """Read file content and parse JSON."""
    try:
        content = await file.read()
    except Exception as exc:
        logger.error("Failed to read uploaded file: %s", exc)
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, f"파일을 읽을 수 없습니다: {str(exc)}", code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc

    if content is None:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "파일 내용을 읽을 수 없습니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)
    if not isinstance(content, (bytes, bytearray)):
        content = str(content).encode("utf-8", errors="replace")
    if len(content) == 0:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "빈 파일입니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    try:
        decoded = bytes(content).decode("utf-8")
    except UnicodeDecodeError as exc:
        logger.error("File encoding error: %s", exc)
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "파일 인코딩이 올바르지 않습니다 (UTF-8 필요)",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        ) from exc

    if not decoded.strip():
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "파일이 비어있거나 공백만 포함되어 있습니다",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    content_hash = hashlib.sha256(bytes(content)).hexdigest()
    logger.info("Processing mapping import with hash: %s...", content_hash[:16])

    try:
        raw_mappings = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            f"잘못된 JSON 형식입니다: {str(exc)}",
            code=ErrorCode.JSON_DECODE_ERROR,
        ) from exc

    if not isinstance(raw_mappings, dict):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "매핑 데이터는 JSON 객체여야 합니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    return content_hash, raw_mappings


def _sanitize_and_validate_schema(raw_mappings: Dict[str, Any], db_name: str) -> Tuple[MappingImportPayload, Dict[str, Any]]:
    """Sanitize input and validate schema."""
    try:
        sanitized_mappings = sanitize_input(raw_mappings)
    except SecurityViolationError as exc:
        logger.warning("Security violation detected in mapping import: %s", exc)
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            "보안 위반이 감지되었습니다. 파일 내용을 확인해주세요.",
            code=ErrorCode.INPUT_SANITIZATION_FAILED,
        ) from exc

    try:
        mapping_request = MappingImportPayload.model_validate(
            {
                "db_name": db_name,
                "classes": sanitized_mappings.get("classes", []),
                "properties": sanitized_mappings.get("properties", []),
                "relationships": sanitized_mappings.get("relationships", []),
            }
        )
    except Exception as exc:
        logger.error("Schema validation failed: %s", exc)
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            f"매핑 데이터 스키마가 올바르지 않습니다: {str(exc)}",
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        ) from exc

    return mapping_request, sanitized_mappings


def _validate_business_logic(mapping_request: MappingImportPayload, sanitized_mappings: Dict[str, Any], db_name: str) -> int:
    """Validate business logic and data consistency."""
    file_db_name = sanitized_mappings.get("db_name")
    if file_db_name and file_db_name != db_name:
        raise classified_http_exception(
            status.HTTP_400_BAD_REQUEST,
            (
                "매핑 데이터의 데이터베이스 이름이 일치하지 않습니다: "
                f"예상: {db_name}, 실제: {file_db_name}"
            ),
            code=ErrorCode.REQUEST_VALIDATION_FAILED,
        )

    total_mappings = len(mapping_request.classes) + len(mapping_request.properties) + len(mapping_request.relationships)
    if total_mappings == 0:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "가져올 매핑 데이터가 없습니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    for cls in mapping_request.classes:
        if not cls.get("class_id") or not cls.get("label"):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "클래스 매핑에 필수 필드가 누락되었습니다 (class_id, label)",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

    for prop in mapping_request.properties:
        if not prop.get("property_id") or not prop.get("label"):
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "속성 매핑에 필수 필드가 누락되었습니다 (property_id, label)",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
            )

    class_ids = [cls.get("class_id") for cls in mapping_request.classes]
    if len(class_ids) != len(set(class_ids)):
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "중복된 클래스 ID가 있습니다", code=ErrorCode.REQUEST_VALIDATION_FAILED)

    return total_mappings


async def _load_ontology_ids(*, oms_client: OMSClient, db_name: str) -> Tuple[set[str], set[str]]:
    ontologies = await oms_client.get_ontologies(db_name)
    existing_class_ids = {ont.id for ont in ontologies if ont.type == "Class"}
    existing_property_ids = {ont.id for ont in ontologies if ont.type == "Property"}
    return existing_class_ids, existing_property_ids


async def _load_existing_label_map(*, mapper: LabelMapper, db_name: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    existing_mappings = await mapper.export_mappings(db_name)
    class_labels = {cls.get("label"): cls.get("class_id") for cls in existing_mappings.get("classes", [])}
    prop_labels = {prop.get("label"): prop.get("property_id") for prop in existing_mappings.get("properties", [])}
    return class_labels, prop_labels


def _compute_validation_details(
    *,
    mapping_request: MappingImportPayload,
    existing_class_ids: set[str],
    existing_property_ids: set[str],
    existing_class_labels: Dict[str, str],
    existing_prop_labels: Dict[str, str],
) -> Dict[str, Any]:
    details = _validation_details_template()

    for cls_mapping in mapping_request.classes:
        class_id = cls_mapping.get("class_id")
        if class_id not in existing_class_ids:
            details["unmapped_classes"].append(
                {
                    "class_id": class_id,
                    "label": cls_mapping.get("label"),
                    "issue": "클래스 ID가 데이터베이스에 존재하지 않음",
                }
            )

    for prop_mapping in mapping_request.properties:
        property_id = prop_mapping.get("property_id")
        if property_id not in existing_property_ids:
            details["unmapped_properties"].append(
                {
                    "property_id": property_id,
                    "label": prop_mapping.get("label"),
                    "issue": "속성 ID가 데이터베이스에 존재하지 않음",
                }
            )

    for cls_mapping in mapping_request.classes:
        label = cls_mapping.get("label")
        class_id = cls_mapping.get("class_id")
        if label in existing_class_labels and existing_class_labels[label] != class_id:
            details["conflicts"].append(
                {
                    "type": "class",
                    "label": label,
                    "new_id": class_id,
                    "existing_id": existing_class_labels[label],
                    "issue": "동일한 레이블이 다른 클래스 ID에 이미 매핑됨",
                }
            )

    for prop_mapping in mapping_request.properties:
        label = prop_mapping.get("label")
        property_id = prop_mapping.get("property_id")
        if label in existing_prop_labels and existing_prop_labels[label] != property_id:
            details["conflicts"].append(
                {
                    "type": "property",
                    "label": label,
                    "new_id": property_id,
                    "existing_id": existing_prop_labels[label],
                    "issue": "동일한 레이블이 다른 속성 ID에 이미 매핑됨",
                }
            )

    return details


def _validation_passed(details: Dict[str, Any]) -> bool:
    return not details["unmapped_classes"] and not details["unmapped_properties"] and not details["conflicts"]


async def _perform_mapping_import(*, mapper: LabelMapper, validated_mappings: Dict[str, Any], db_name: str) -> Tuple[Dict[str, Any], float, datetime]:
    start_time = datetime.now()
    backup: Optional[Dict[str, Any]] = None

    try:
        backup = await mapper.export_mappings(db_name)
        await mapper.import_mappings(validated_mappings)
        processing_time = (datetime.now() - start_time).total_seconds()
        return backup, processing_time, start_time

    except Exception as import_error:
        try:
            if backup:
                await mapper.import_mappings(backup)
                logger.info("Restored backup mappings for %s after import failure", db_name)
        except Exception as restore_error:
            logger.error("Failed to restore backup: %s", restore_error)

        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"매핑 가져오기 중 오류가 발생했습니다: {str(import_error)}",
            code=ErrorCode.INTERNAL_ERROR,
        ) from import_error


class _MappingBundleProcessor(ABC):
    """Template Method for mapping bundle file processing."""

    async def process(
        self,
        *,
        db_name: str,
        file: UploadFile,
        mapper: LabelMapper,
        oms_client: OMSClient,
    ) -> Any:
        await _validate_file_upload(file)
        content_hash, raw_mappings = await _read_and_parse_file(file)
        mapping_request, sanitized_mappings = _sanitize_and_validate_schema(raw_mappings, db_name)
        total_mappings = _validate_business_logic(mapping_request, sanitized_mappings, db_name)
        ctx = MappingBundleContext(
            db_name=db_name,
            content_hash=content_hash,
            mapping_request=mapping_request,
            sanitized_mappings=sanitized_mappings,
            total_mappings=total_mappings,
        )
        return await self._handle(ctx=ctx, mapper=mapper, oms_client=oms_client)

    @abstractmethod
    async def _handle(self, *, ctx: MappingBundleContext, mapper: LabelMapper, oms_client: OMSClient) -> Any: ...


class _ImportProcessor(_MappingBundleProcessor):
    async def _handle(self, *, ctx: MappingBundleContext, mapper: LabelMapper, oms_client: OMSClient) -> Dict[str, Any]:
        try:
            validation_passed = False
            try:
                db_exists = await oms_client.database_exists(ctx.db_name)
                if db_exists:
                    existing_class_ids, existing_property_ids = await _load_ontology_ids(oms_client=oms_client, db_name=ctx.db_name)
                    existing_class_labels, existing_prop_labels = await _load_existing_label_map(mapper=mapper, db_name=ctx.db_name)
                    details = _compute_validation_details(
                        mapping_request=ctx.mapping_request,
                        existing_class_ids=existing_class_ids,
                        existing_property_ids=existing_property_ids,
                        existing_class_labels=existing_class_labels,
                        existing_prop_labels=existing_prop_labels,
                    )
                    validation_passed = _validation_passed(details)
            except Exception as exc:
                logger.error("Validation failed with error: %s", exc)
                validation_passed = False

            start_time = datetime.now()
            validated_mappings = {
                "db_name": ctx.db_name,
                "classes": ctx.mapping_request.classes,
                "properties": ctx.mapping_request.properties,
                "relationships": ctx.mapping_request.relationships,
                "imported_at": start_time.isoformat(),
                "import_hash": ctx.content_hash,
                "validation_passed": validation_passed,
            }

            _backup, processing_time, imported_at = await _perform_mapping_import(
                mapper=mapper,
                validated_mappings=validated_mappings,
                db_name=ctx.db_name,
            )

            logger.info(
                "Successfully imported %s mappings for %s in %.2fs",
                ctx.total_mappings,
                ctx.db_name,
                processing_time,
            )

            return {
                "status": "success",
                "message": "레이블 매핑을 성공적으로 가져왔습니다",
                "data": {
                    "database": ctx.db_name,
                    "stats": {
                        "classes": len(ctx.mapping_request.classes),
                        "properties": len(ctx.mapping_request.properties),
                        "relationships": len(ctx.mapping_request.relationships),
                        "total": ctx.total_mappings,
                    },
                    "processing_time_seconds": processing_time,
                    "content_hash": ctx.content_hash[:16],
                    "imported_at": imported_at.isoformat(),
                },
            }
        except HTTPException:
            raise
        except SecurityViolationError as exc:
            logger.warning("Security violation in mapping import: %s", exc)
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "보안 정책 위반이 감지되었습니다", code=ErrorCode.INPUT_SANITIZATION_FAILED) from exc
        except Exception as exc:
            logger.error("Unexpected error in mapping import: %s", exc)
            raise classified_http_exception(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                "매핑 가져오기 실패: 서버 오류가 발생했습니다",
                code=ErrorCode.INTERNAL_ERROR,
            ) from exc


class _ValidateProcessor(_MappingBundleProcessor):
    async def _handle(
        self,
        *,
        ctx: MappingBundleContext,
        mapper: LabelMapper,
        oms_client: OMSClient,
    ) -> Union[Dict[str, Any], JSONResponse]:
        try:
            validation_details = _validation_details_template()

            try:
                db_exists = await oms_client.database_exists(ctx.db_name)
                if not db_exists:
                    payload = build_error_envelope(
                        service_name=SERVICE_NAME,
                        message=f"데이터베이스 '{ctx.db_name}'이 존재하지 않습니다",
                        detail="Database not found",
                        code=ErrorCode.RESOURCE_NOT_FOUND,
                        category=ErrorCategory.RESOURCE,
                        status_code=status.HTTP_404_NOT_FOUND,
                        context={"validation_passed": False, "details": validation_details, "db_name": ctx.db_name},
                        request_id=get_request_id(),
                        correlation_id=get_correlation_id(),
                    )
                    return JSONResponse(
                        content=payload,
                        status_code=payload.get("http_status", status.HTTP_404_NOT_FOUND),
                    )
            except Exception as exc:
                logger.error("Failed to check database existence: %s", exc)
                payload = build_error_envelope(
                    service_name=SERVICE_NAME,
                    message="데이터베이스 연결 실패",
                    detail=str(exc),
                    code=ErrorCode.OMS_UNAVAILABLE,
                    category=ErrorCategory.UPSTREAM,
                    status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                    context={"validation_passed": False, "details": validation_details, "db_name": ctx.db_name},
                    request_id=get_request_id(),
                    correlation_id=get_correlation_id(),
                )
                return JSONResponse(
                    content=payload,
                    status_code=payload.get("http_status", status.HTTP_503_SERVICE_UNAVAILABLE),
                )

            try:
                existing_class_ids, existing_property_ids = await _load_ontology_ids(oms_client=oms_client, db_name=ctx.db_name)
            except Exception as exc:
                logger.error("Failed to get ontologies: %s", exc)
                payload = build_error_envelope(
                    service_name=SERVICE_NAME,
                    message="온톨로지 데이터 조회 실패",
                    detail=str(exc),
                    code=ErrorCode.UPSTREAM_ERROR,
                    category=ErrorCategory.UPSTREAM,
                    status_code=status.HTTP_502_BAD_GATEWAY,
                    context={"validation_passed": False, "details": validation_details, "db_name": ctx.db_name},
                    request_id=get_request_id(),
                    correlation_id=get_correlation_id(),
                )
                return JSONResponse(
                    content=payload,
                    status_code=payload.get("http_status", status.HTTP_502_BAD_GATEWAY),
                )

            existing_class_labels, existing_prop_labels = await _load_existing_label_map(mapper=mapper, db_name=ctx.db_name)
            validation_details = _compute_validation_details(
                mapping_request=ctx.mapping_request,
                existing_class_ids=existing_class_ids,
                existing_property_ids=existing_property_ids,
                existing_class_labels=existing_class_labels,
                existing_prop_labels=existing_prop_labels,
            )

            validation_passed = _validation_passed(validation_details)
            result_status = "success" if validation_passed else "warning"
            message = "매핑 검증 완료" if validation_passed else "매핑 검증 중 문제 발견"

            return {
                "status": result_status,
                "message": message,
                "data": {
                    "database": ctx.db_name,
                    "validation_passed": validation_passed,
                    "stats": {
                        "classes": len(ctx.mapping_request.classes),
                        "properties": len(ctx.mapping_request.properties),
                        "relationships": len(ctx.mapping_request.relationships),
                        "total": ctx.total_mappings,
                    },
                    "details": validation_details,
                    "content_hash": ctx.content_hash[:16],
                },
            }

        except HTTPException:
            raise
        except Exception as exc:
            logger.error("Validation error: %s", exc)
            raise classified_http_exception(
                status.HTTP_500_INTERNAL_SERVER_ERROR,
                f"매핑 검증 실패: {str(exc)}",
                code=ErrorCode.INTERNAL_ERROR,
            ) from exc


async def import_mappings(
    *,
    db_name: str,
    file: UploadFile,
    mapper: LabelMapper,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """
    Import label mappings from JSON file with enhanced security validation.
    """
    return await _ImportProcessor().process(db_name=db_name, file=file, mapper=mapper, oms_client=oms_client)


async def validate_mappings(
    *,
    db_name: str,
    file: UploadFile,
    mapper: LabelMapper,
    oms_client: OMSClient,
) -> Union[Dict[str, Any], JSONResponse]:
    """
    Validate mappings without importing.
    """
    return await _ValidateProcessor().process(db_name=db_name, file=file, mapper=mapper, oms_client=oms_client)


async def get_mappings_summary(*, db_name: str, mapper: LabelMapper) -> Dict[str, Any]:
    """
    레이블 매핑 요약 조회

    데이터베이스의 레이블 매핑 통계를 조회합니다.
    """
    try:
        mappings = await mapper.export_mappings(db_name)
        lang_stats: Dict[str, Dict[str, int]] = {}

        for cls in mappings.get("classes", []):
            lang = cls.get("label_lang", "ko")
            lang_stats.setdefault(lang, {"classes": 0, "properties": 0, "relationships": 0})
            lang_stats[lang]["classes"] += 1

        for prop in mappings.get("properties", []):
            lang = prop.get("label_lang", "ko")
            lang_stats.setdefault(lang, {"classes": 0, "properties": 0, "relationships": 0})
            lang_stats[lang]["properties"] += 1

        for rel in mappings.get("relationships", []):
            lang = rel.get("label_lang", "ko")
            lang_stats.setdefault(lang, {"classes": 0, "properties": 0, "relationships": 0})
            lang_stats[lang]["relationships"] += 1

        return {
            "database": db_name,
            "total": {
                "classes": len(mappings.get("classes", [])),
                "properties": len(mappings.get("properties", [])),
                "relationships": len(mappings.get("relationships", [])),
            },
            "by_language": lang_stats,
            "last_exported": mappings.get("exported_at"),
        }

    except Exception as exc:
        logger.error("Failed to get mappings summary: %s", exc)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"매핑 요약 조회 실패: {str(exc)}",
            code=ErrorCode.INTERNAL_ERROR,
        ) from exc


async def clear_mappings(*, db_name: str, mapper: LabelMapper) -> Dict[str, Any]:
    """
    레이블 매핑 초기화

    데이터베이스의 모든 레이블 매핑을 삭제합니다.
    주의: 이 작업은 되돌릴 수 없습니다!
    """
    try:
        backup = await mapper.export_mappings(db_name)

        for cls in backup.get("classes", []):
            await mapper.remove_class(db_name, cls["class_id"])

        return {
            "message": "레이블 매핑이 초기화되었습니다",
            "database": db_name,
            "deleted": {
                "classes": len(backup.get("classes", [])),
                "properties": len(backup.get("properties", [])),
                "relationships": len(backup.get("relationships", [])),
            },
        }

    except Exception as exc:
        logger.error("Failed to clear mappings: %s", exc)
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"매핑 초기화 실패: {str(exc)}",
            code=ErrorCode.INTERNAL_ERROR,
        ) from exc

