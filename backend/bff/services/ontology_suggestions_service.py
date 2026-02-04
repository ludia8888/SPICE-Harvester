"""Ontology suggestion service (BFF).

Extracted from `bff.routers.ontology_suggestions` to keep routers thin and to
deduplicate FunnelClient/mapping-suggestion glue code (Facade pattern).
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

import httpx
from fastapi import HTTPException, UploadFile, status

from bff.routers.ontology_ops import (
    _build_sample_data_from_preview,
    _build_source_schema_from_preview,
    _normalize_target_schema_for_mapping,
)
from bff.schemas.ontology_requests import (
    MappingFromGoogleSheetsRequest,
    MappingSuggestionRequest,
    SchemaFromDataRequest,
    SchemaFromGoogleSheetsRequest,
)
from bff.services.sheet_import_parsing import parse_table_bbox, parse_target_schema_json, read_excel_upload
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.security.input_sanitizer import validate_class_id, validate_db_name

logger = logging.getLogger(__name__)

_MAPPING_CONFIG_PATH = Path(__file__).resolve().parents[1] / "config" / "mapping_config.json"


def _load_mapping_config() -> Optional[Dict[str, Any]]:
    try:
        if not _MAPPING_CONFIG_PATH.exists():
            return None
        config = json.loads(_MAPPING_CONFIG_PATH.read_text(encoding="utf-8"))
        if isinstance(config, dict):
            logger.info("Loaded mapping config from %s", _MAPPING_CONFIG_PATH)
            return config
    except Exception as exc:
        logger.warning("Failed to load mapping config: %s, using defaults", exc)
    return None


def _apply_semantic_hints(config: Optional[Dict[str, Any]], *, enabled: bool) -> Dict[str, Any]:
    base: Dict[str, Any] = dict(config) if isinstance(config, dict) else {}
    features = dict(base.get("features") or {})
    features["semantic_match"] = bool(enabled)
    base["features"] = features
    return base


def _format_mapping_suggestion(*, suggestion: Any, source_schema: List[Dict[str, Any]], target_schema: List[Dict[str, Any]]) -> Dict[str, Any]:
    return {
        "message": (
            f"Found {len(suggestion.mappings)} mapping suggestions with "
            f"{suggestion.overall_confidence:.2f} overall confidence"
        ),
        "mappings": [
            {
                "source_field": m.source_field,
                "target_field": m.target_field,
                "confidence": m.confidence,
                "match_type": m.match_type,
                "reasons": m.reasons,
            }
            for m in suggestion.mappings
        ],
        "unmapped_source_fields": suggestion.unmapped_source_fields,
        "unmapped_target_fields": suggestion.unmapped_target_fields,
        "overall_confidence": suggestion.overall_confidence,
        "statistics": {
            "total_source_fields": len(source_schema),
            "total_target_fields": len(target_schema),
            "mapped_fields": len(suggestion.mappings),
            "high_confidence_mappings": len([m for m in suggestion.mappings if m.confidence >= 0.8]),
            "medium_confidence_mappings": len([m for m in suggestion.mappings if 0.6 <= m.confidence < 0.8]),
        },
    }


async def suggest_schema_from_data(*, db_name: str, body: SchemaFromDataRequest) -> Dict[str, Any]:
    """
    🔥 데이터에서 스키마 자동 제안

    제공된 데이터를 분석하여 OMS 온톨로지 스키마를 자동으로 생성합니다.
    """
    try:
        db_name = validate_db_name(db_name)
        _ = db_name  # Currently used as a namespacing guard only.

        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            result = await funnel_client.analyze_and_suggest_schema(
                data=body.data,
                columns=body.columns,
                class_name=body.class_name,
                include_complex_types=body.include_complex_types,
            )

        analysis = result.get("analysis", {}) if isinstance(result, dict) else {}
        schema_suggestion = result.get("schema_suggestion", {}) if isinstance(result, dict) else {}

        analyzed_columns = analysis.get("columns", [])
        if not isinstance(analyzed_columns, list):
            analyzed_columns = []

        high_confidence_types = [
            col for col in analyzed_columns if col.get("inferred_type", {}).get("confidence", 0) >= 0.7
        ]

        avg_conf = 0
        if analyzed_columns:
            avg_conf = round(
                sum(col.get("inferred_type", {}).get("confidence", 0) for col in analyzed_columns) / len(analyzed_columns),
                2,
            )

        detected_types = list(
            set(col.get("inferred_type", {}).get("type", "unknown") for col in analyzed_columns)
        )

        return {
            "status": "success",
            "message": (
                "스키마 제안이 완료되었습니다. "
                f"{len(high_confidence_types)}/{len(analyzed_columns)} 컬럼에 대해 높은 신뢰도 타입 추론 성공"
            ),
            "suggested_schema": schema_suggestion,
            "analysis_summary": {
                "total_columns": len(analyzed_columns),
                "high_confidence_columns": len(high_confidence_types),
                "average_confidence": avg_conf,
                "detected_types": detected_types,
            },
            "detailed_analysis": analysis,
        }

    except Exception as exc:
        logger.error("Schema suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"스키마 제안 실패: {str(exc)}",
        ) from exc


async def suggest_mappings(*, db_name: str, body: MappingSuggestionRequest) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        _ = db_name

        from bff.services.mapping_suggestion_service import MappingSuggestionService

        config = _load_mapping_config()
        suggestion_service = MappingSuggestionService(config=config)

        if not body.target_sample_data:
            logger.info("No target sample data provided - value distribution matching will be skipped")

        suggestion = suggestion_service.suggest_mappings(
            source_schema=body.source_schema,
            target_schema=body.target_schema,
            sample_data=body.sample_data,
            target_sample_data=body.target_sample_data,
        )

        formatted = _format_mapping_suggestion(
            suggestion=suggestion,
            source_schema=body.source_schema,
            target_schema=body.target_schema,
        )

        return {
            "status": "success",
            **formatted,
        }

    except Exception as exc:
        logger.error("Mapping suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"매핑 제안 실패: {str(exc)}",
        ) from exc


async def suggest_mappings_from_google_sheets(*, db_name: str, body: MappingFromGoogleSheetsRequest) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(body.target_class_id)
        target_schema = _normalize_target_schema_for_mapping(
            body.target_schema,
            include_relationships=bool(body.include_relationships),
        )
        if not target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema is required for import (field types)",
            )

        from bff.services.funnel_client import FunnelClient
        from bff.services.mapping_suggestion_service import MappingSuggestionService

        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_structure_preview(
                sheet_url=body.sheet_url,
                worksheet_name=body.worksheet_name,
                api_key=body.api_key,
                connection_id=body.connection_id,
                table_id=body.table_id,
                table_bbox=body.table_bbox.model_dump() if body.table_bbox is not None else None,
                include_complex_types=True,
            )

        preview = (result.get("preview") or {}) if isinstance(result, dict) else {}
        structure = result.get("structure") if isinstance(result, dict) else None
        table = result.get("table") or {} if isinstance(result, dict) else {}

        source_schema = _build_source_schema_from_preview(preview)
        sample_data = _build_sample_data_from_preview(preview)

        config = _apply_semantic_hints(_load_mapping_config(), enabled=bool(body.enable_semantic_hints))
        suggestion_service = MappingSuggestionService(config=config)
        suggestion = suggestion_service.suggest_mappings(
            source_schema=source_schema,
            target_schema=target_schema,
            sample_data=sample_data,
            target_sample_data=None,
        )

        formatted = _format_mapping_suggestion(
            suggestion=suggestion,
            source_schema=source_schema,
            target_schema=target_schema,
        )

        return {
            "status": "success",
            "source_info": {
                "sheet_url": body.sheet_url,
                "worksheet_name": body.worksheet_name,
                "table_id": table.get("id"),
                "table_mode": table.get("mode"),
                "table_bbox": table.get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "source_schema": source_schema,
            "target_schema": target_schema,
            "preview_data": preview,
            "structure": structure,
            **formatted,
        }

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Google Sheets mapping suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 매핑 제안 실패: {str(exc)}",
        ) from exc


async def suggest_mappings_from_excel(
    *,
    db_name: str,
    target_class_id: str,
    file: UploadFile,
    target_schema_json: Optional[str],
    sheet_name: Optional[str],
    table_id: Optional[str],
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
    include_relationships: bool,
    enable_semantic_hints: bool,
    max_tables: int,
    max_rows: Optional[int],
    max_cols: Optional[int],
) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(target_class_id)

        filename, content = await read_excel_upload(file)
        target_schema_raw = parse_target_schema_json(target_schema_json)
        table_bbox = parse_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        from bff.services.funnel_client import FunnelClient
        from bff.services.mapping_suggestion_service import MappingSuggestionService

        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_structure_preview(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=True,
                max_tables=max_tables,
                max_rows=max_rows,
                max_cols=max_cols,
            )

        preview = (result.get("preview") or {}) if isinstance(result, dict) else {}
        structure = result.get("structure") if isinstance(result, dict) else None
        table = result.get("table") or {} if isinstance(result, dict) else {}

        source_schema = _build_source_schema_from_preview(preview)
        sample_data = _build_sample_data_from_preview(preview)
        target_schema = _normalize_target_schema_for_mapping(
            target_schema_raw,
            include_relationships=bool(include_relationships),
        )
        if not target_schema:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="target_schema_json produced an empty schema",
            )

        config = _apply_semantic_hints(_load_mapping_config(), enabled=bool(enable_semantic_hints))
        suggestion_service = MappingSuggestionService(config=config)
        suggestion = suggestion_service.suggest_mappings(
            source_schema=source_schema,
            target_schema=target_schema,
            sample_data=sample_data,
            target_sample_data=None,
        )

        formatted = _format_mapping_suggestion(
            suggestion=suggestion,
            source_schema=source_schema,
            target_schema=target_schema,
        )

        return {
            "status": "success",
            "source_info": {
                "file_name": filename,
                "sheet_name": sheet_name,
                "table_id": table.get("id"),
                "table_mode": table.get("mode"),
                "table_bbox": table.get("bbox"),
            },
            "target_info": {"class_id": target_class_id},
            "source_schema": source_schema,
            "target_schema": target_schema,
            "preview_data": preview,
            "structure": structure,
            **formatted,
        }

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Excel mapping suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel 매핑 제안 실패: {str(exc)}",
        ) from exc


async def suggest_schema_from_google_sheets(*, db_name: str, body: SchemaFromGoogleSheetsRequest) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        _ = db_name

        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            result = await funnel_client.google_sheets_to_schema(
                sheet_url=body.sheet_url,
                worksheet_name=body.worksheet_name,
                class_name=body.class_name,
                api_key=body.api_key,
                connection_id=body.connection_id,
                table_id=body.table_id,
                table_bbox=body.table_bbox.model_dump() if body.table_bbox is not None else None,
            )

        preview = result.get("preview", {}) if isinstance(result, dict) else {}
        schema_suggestion = result.get("schema_suggestion", {}) if isinstance(result, dict) else {}
        structure = result.get("structure") if isinstance(result, dict) else None

        total_rows = preview.get("total_rows", 0)
        preview_rows = preview.get("preview_rows", 0)
        columns = preview.get("columns", [])

        return {
            "status": "success",
            "message": f"Google Sheets 스키마 제안이 완료되었습니다. {total_rows}행 중 {preview_rows}행 분석됨",
            "suggested_schema": schema_suggestion,
            "source_info": {
                "sheet_url": body.sheet_url,
                "worksheet_name": body.worksheet_name,
                "total_rows": total_rows,
                "preview_rows": preview_rows,
                "columns": columns,
            },
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Google Sheets schema suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Google Sheets 스키마 제안 실패: {str(exc)}",
        ) from exc


async def suggest_schema_from_excel(
    *,
    db_name: str,
    file: UploadFile,
    sheet_name: Optional[str],
    class_name: Optional[str],
    table_id: Optional[str],
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
    include_complex_types: bool,
    max_tables: int,
    max_rows: Optional[int],
    max_cols: Optional[int],
) -> Dict[str, Any]:
    try:
        db_name = validate_db_name(db_name)
        _ = db_name

        filename, content = await read_excel_upload(file)
        table_bbox = parse_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            result = await funnel_client.excel_to_schema(
                xlsx_bytes=content,
                filename=filename,
                sheet_name=sheet_name,
                class_name=class_name,
                table_id=table_id,
                table_bbox=table_bbox,
                include_complex_types=include_complex_types,
                max_tables=max_tables,
                max_rows=max_rows,
                max_cols=max_cols,
            )

        preview = result.get("preview", {}) if isinstance(result, dict) else {}
        schema_suggestion = result.get("schema_suggestion", {}) if isinstance(result, dict) else {}
        structure = result.get("structure") if isinstance(result, dict) else None

        total_rows = preview.get("total_rows", 0)
        preview_rows = preview.get("preview_rows", 0)
        columns = preview.get("columns", [])

        return {
            "status": "success",
            "message": f"Excel 스키마 제안이 완료되었습니다. (추정) {total_rows}행 중 {preview_rows}행 샘플 분석됨",
            "suggested_schema": schema_suggestion,
            "source_info": {
                "file_name": filename,
                "sheet_name": sheet_name,
                "total_rows": total_rows,
                "preview_rows": preview_rows,
                "columns": columns,
            },
            "preview_data": preview,
            "structure": structure,
        }

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except ValueError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Excel schema suggestion failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Excel 스키마 제안 실패: {str(exc)}",
        ) from exc
