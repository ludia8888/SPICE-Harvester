"""Ontology metadata endpoints (BFF).

Composed by `bff.routers.ontology` via router composition (Composite pattern).
"""

from shared.observability.tracing import trace_endpoint

import logging
from datetime import datetime, timezone
from typing import Any, Dict

import httpx
from fastapi import APIRouter, HTTPException, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.dependencies import LabelMapper, LabelMapperDep, OMSClientDep
from bff.services.oms_client import OMSClient
from shared.security.input_sanitizer import sanitize_input, validate_class_id, validate_db_name

logger = logging.getLogger(__name__)

router = APIRouter(tags=["Ontology Management"])

@router.post("/ontology/{class_id}/mapping-metadata")
@trace_endpoint("bff.ontology.save_mapping_metadata")
async def save_mapping_metadata(
    db_name: str,
    class_id: str,
    metadata: Dict[str, Any],
    oms: OMSClient = OMSClientDep,
    mapper: LabelMapper = LabelMapperDep,
):
    """
    매핑 메타데이터를 온톨로지 클래스에 저장
    
    데이터 매핑 이력과 통계를 ClassMetadata에 저장합니다.
    """
    try:
        # 입력 데이터 보안 검증
        db_name = validate_db_name(db_name)
        class_id = validate_class_id(class_id)
        sanitized_metadata = sanitize_input(metadata)

        # Fetch current ontology to merge metadata safely.
        ontology_resp = await oms.get_ontology(db_name, class_id)
        class_data = (ontology_resp.get("data") or {}) if isinstance(ontology_resp, dict) else {}
        existing_meta = class_data.get("metadata") if isinstance(class_data, dict) else None
        if not isinstance(existing_meta, dict):
            existing_meta = {}

        # Back-compat: older code paths may have stored mapping fields at top-level.
        for k in ("mapping_history", "last_mapping_date", "total_mappings", "mapping_sources"):
            if k in class_data and k not in existing_meta:
                existing_meta[k] = class_data.get(k)

        mapping_history = existing_meta.get("mapping_history", []) or []
        if not isinstance(mapping_history, list):
            mapping_history = []
        
        # 새 매핑 정보 추가
        new_mapping_entry = {
            "timestamp": sanitized_metadata.get("timestamp", datetime.now(timezone.utc).isoformat()),
            "source_file": sanitized_metadata.get("sourceFile", "unknown"),
            "mappings_count": sanitized_metadata.get("mappingsCount", 0),
            "average_confidence": sanitized_metadata.get("averageConfidence", 0),
            "mapping_details": sanitized_metadata.get("mappingDetails", [])
        }
        
        mapping_history.append(new_mapping_entry)
        
        # 최근 10개의 매핑 이력만 유지
        if len(mapping_history) > 10:
            mapping_history = mapping_history[-10:]
        
        # 메타데이터 업데이트
        updated_metadata = {
            **existing_meta,
            "mapping_history": mapping_history,
            "last_mapping_date": new_mapping_entry["timestamp"],
            "total_mappings": sum(entry.get("mappings_count", 0) for entry in mapping_history),
            "mapping_sources": list(set(
                entry.get("source_file", "unknown") 
                for entry in mapping_history 
                if entry.get("source_file") != "unknown"
            ))
        }
        
        async def _get_expected_seq() -> int:
            import asyncpg
            import re

            from shared.config.settings import get_settings

            settings = get_settings()
            schema = settings.event_sourcing.event_store_sequence_schema
            if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", schema):
                raise ValueError(f"Invalid EVENT_STORE_SEQUENCE_SCHEMA: {schema!r}")

            prefix = (settings.event_sourcing.event_store_sequence_handler_prefix or "write_side").strip()
            handler = f"{prefix}:OntologyClass"
            aggregate_id = f"{db_name}:main:{class_id}"

            conn = await asyncpg.connect(settings.database.postgres_url)
            try:
                value = await conn.fetchval(
                    f"""
                    SELECT last_sequence
                    FROM {schema}.aggregate_versions
                    WHERE handler = $1 AND aggregate_id = $2
                    """,
                    handler,
                    aggregate_id,
                )
                return int(value or 0)
            finally:
                await conn.close()

        # Persist metadata via the official ontology update endpoint (async command) with OCC.
        # We auto-resolve expected_seq from Postgres to keep this endpoint usable without extra client plumbing.
        update_result = None
        for attempt in range(3):
            expected_seq = await _get_expected_seq()
            try:
                update_result = await oms.update_ontology(
                    db_name,
                    class_id,
                    {"metadata": updated_metadata},
                    expected_seq=expected_seq,
                    branch="main",
                )
                break
            except httpx.HTTPStatusError as e:
                status_code = getattr(e.response, "status_code", 500)
                if status_code == 409 and attempt < 2:
                    continue
                detail: Any = e.response.text
                try:
                    detail_json = e.response.json()
                    if isinstance(detail_json, dict):
                        detail = detail_json.get("detail") or detail_json
                except ValueError:
                    logging.getLogger(__name__).warning("Failed to parse ontology metadata error payload as JSON", exc_info=True)
                raise classified_http_exception(status_code, str(detail), code=ErrorCode.UPSTREAM_ERROR) from e
        
        logger.info(f"Saved mapping metadata for class {class_id}: {new_mapping_entry['mappings_count']} mappings from {new_mapping_entry['source_file']}")
        
        return {
            "status": "success",
            "message": "매핑 메타데이터가 저장되었습니다",
            "data": {
                "class_id": class_id,
                "mapping_entry": new_mapping_entry,
                "total_history_entries": len(mapping_history),
                "update": update_result,
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to save mapping metadata: {e}")
        raise classified_http_exception(
            status.HTTP_500_INTERNAL_SERVER_ERROR,
            f"매핑 메타데이터 저장 실패: {str(e)}",
            code=ErrorCode.INTERNAL_ERROR,
        )
