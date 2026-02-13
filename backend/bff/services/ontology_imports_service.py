"""Ontology import service (BFF).

Extracted from `bff.routers.ontology_imports` to keep routers thin and to
deduplicate tabular import flows (Template Method + Strategy).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Protocol, Sequence

import httpx
from fastapi import HTTPException, UploadFile, status

from shared.errors.error_types import ErrorCode, classified_http_exception

from bff.routers.ontology_ops import _extract_target_field_types_from_import_schema
from bff.schemas.ontology_requests import ImportFieldMapping, ImportFromGoogleSheetsRequest, ImportTargetField
from bff.services.oms_client import OMSClient
from bff.services.sheet_import_parsing import parse_json_array, parse_json_object, parse_table_bbox, read_excel_upload
from bff.services.sheet_import_service import FieldMapping, SheetImportService
from bff.utils.httpx_exceptions import raise_httpx_as_http_exception
from shared.security.input_sanitizer import SecurityViolationError, validate_class_id, validate_db_name
from shared.observability.tracing import trace_external_call

logger = logging.getLogger(__name__)


class _ImportSourceStrategy(Protocol):
    source: str

    async def fetch_structure_preview(self, *, options: Dict[str, Any]) -> Dict[str, Any]: ...

    def dry_run_source_info(self, *, table: Dict[str, Any]) -> Dict[str, Any]: ...

    def commit_base_metadata(self, *, table: Dict[str, Any], mappings: Sequence[ImportFieldMapping]) -> Dict[str, Any]: ...


@dataclass(frozen=True)
class _GoogleSheetsSource:
    sheet_url: str
    worksheet_name: Optional[str]
    api_key: Optional[str]
    connection_id: Optional[str]
    table_id: Optional[str]
    table_bbox: Optional[Dict[str, int]]
    max_tables: int
    max_rows: Optional[int]
    max_cols: Optional[int]
    trim_trailing_empty: bool

    source: str = "google_sheets"

    async def fetch_structure_preview(self, *, options: Dict[str, Any]) -> Dict[str, Any]:
        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            return await funnel_client.google_sheets_to_structure_preview(
                sheet_url=self.sheet_url,
                worksheet_name=self.worksheet_name,
                api_key=self.api_key,
                connection_id=self.connection_id,
                table_id=self.table_id,
                table_bbox=self.table_bbox,
                include_complex_types=True,
                max_tables=int(self.max_tables or 5),
                max_rows=self.max_rows,
                max_cols=self.max_cols,
                trim_trailing_empty=bool(self.trim_trailing_empty),
                options=options,
            )

    def dry_run_source_info(self, *, table: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "sheet_url": self.sheet_url,
            "worksheet_name": self.worksheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
        }

    def commit_base_metadata(self, *, table: Dict[str, Any], mappings: Sequence[ImportFieldMapping]) -> Dict[str, Any]:
        return {
            "source": self.source,
            "sheet_url": self.sheet_url,
            "worksheet_name": self.worksheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
            "mappings": [m.model_dump() for m in mappings],
        }


@dataclass(frozen=True)
class _ExcelSource:
    filename: str
    xlsx_bytes: bytes
    sheet_name: Optional[str]
    table_id: Optional[str]
    table_bbox: Optional[Dict[str, int]]
    max_tables: int
    max_rows: Optional[int]
    max_cols: Optional[int]

    source: str = "excel"

    async def fetch_structure_preview(self, *, options: Dict[str, Any]) -> Dict[str, Any]:
        from bff.services.funnel_client import FunnelClient

        async with FunnelClient() as funnel_client:
            return await funnel_client.excel_to_structure_preview(
                xlsx_bytes=self.xlsx_bytes,
                filename=self.filename,
                sheet_name=self.sheet_name,
                table_id=self.table_id,
                table_bbox=self.table_bbox,
                include_complex_types=True,
                max_tables=int(self.max_tables or 5),
                max_rows=self.max_rows,
                max_cols=self.max_cols,
                options=options,
            )

    def dry_run_source_info(self, *, table: Dict[str, Any]) -> Dict[str, Any]:
        return {
            "file_name": self.filename,
            "sheet_name": self.sheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
        }

    def commit_base_metadata(self, *, table: Dict[str, Any], mappings: Sequence[ImportFieldMapping]) -> Dict[str, Any]:
        return {
            "source": self.source,
            "file_name": self.filename,
            "sheet_name": self.sheet_name,
            "table_id": table.get("id"),
            "table_mode": table.get("mode"),
            "table_bbox": table.get("bbox"),
            "mappings": [m.model_dump() for m in mappings],
        }


@dataclass(frozen=True)
class _PreparedImport:
    table: Dict[str, Any]
    preview: Dict[str, Any]
    structure: Any
    build: Dict[str, Any]


class _ImportProcessorTemplate:
    def __init__(self, source: _ImportSourceStrategy) -> None:
        self._source = source

    def _compute_sample_row_limit(self, *, max_import_rows: Optional[int]) -> int:
        raise NotImplementedError

    async def _prepare(
        self,
        *,
        target_schema: List[ImportTargetField],
        mappings: Sequence[ImportFieldMapping],
        max_import_rows: Optional[int],
        options: Dict[str, Any],
    ) -> _PreparedImport:
        sample_row_limit = self._compute_sample_row_limit(max_import_rows=max_import_rows)
        preview_options = {**(options or {}), "sample_row_limit": sample_row_limit}

        result = await self._source.fetch_structure_preview(options=preview_options)

        table = (result.get("table") or {}) if isinstance(result, dict) else {}
        if table.get("mode") == "property":
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "Property-form tables are not supported for record import yet", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        preview = (result.get("preview") or {}) if isinstance(result, dict) else {}
        structure = result.get("structure") if isinstance(result, dict) else None

        target_field_types = _extract_target_field_types_from_import_schema(target_schema)
        build = SheetImportService.build_instances(
            columns=preview.get("columns") or [],
            rows=preview.get("sample_data") or [],
            mappings=[FieldMapping(source_field=m.source_field, target_field=m.target_field) for m in mappings],
            target_field_types=target_field_types,
        )
        return _PreparedImport(
            table=table,
            preview=preview,
            structure=structure,
            build=build,
        )


class _DryRunImportProcessor(_ImportProcessorTemplate):
    def __init__(self, source: _ImportSourceStrategy, *, dry_run_rows: int) -> None:
        super().__init__(source)
        self._dry_run_rows = dry_run_rows

    def _compute_sample_row_limit(self, *, max_import_rows: Optional[int]) -> int:
        dry_run_rows = max(1, min(int(self._dry_run_rows or 100), 5000))
        sample_row_limit = dry_run_rows
        if max_import_rows is not None:
            sample_row_limit = min(sample_row_limit, int(max_import_rows))
        return sample_row_limit

    async def run(
        self,
        *,
        target_class_id: str,
        target_schema: List[ImportTargetField],
        mappings: List[ImportFieldMapping],
        max_import_rows: Optional[int],
        options: Dict[str, Any],
    ) -> Dict[str, Any]:
        prepared = await self._prepare(
            target_schema=target_schema,
            mappings=mappings,
            max_import_rows=max_import_rows,
            options=options,
        )

        instances = prepared.build.get("instances") or []
        errors = prepared.build.get("errors") or []

        return {
            "status": "success",
            "message": "Dry-run import completed",
            "source_info": self._source.dry_run_source_info(table=prepared.table),
            "target_info": {"class_id": target_class_id},
            "mapping_summary": {
                "mappings": [m.model_dump() for m in mappings],
                "mapping_count": len(mappings),
            },
            "stats": prepared.build.get("stats") or {},
            "warnings": prepared.build.get("warnings") or [],
            "errors": errors[:200],
            "sample_instances": instances[:5],
            "preview_data": prepared.preview,
            "structure": prepared.structure,
        }


class _CommitImportProcessor(_ImportProcessorTemplate):
    def _compute_sample_row_limit(self, *, max_import_rows: Optional[int]) -> int:
        if max_import_rows is None:
            return -1  # full
        return max(0, int(max_import_rows))

    async def run(
        self,
        *,
        db_name: str,
        target_class_id: str,
        target_schema: List[ImportTargetField],
        mappings: List[ImportFieldMapping],
        max_import_rows: Optional[int],
        options: Dict[str, Any],
        allow_partial: bool,
        batch_size: int,
        return_instances: bool,
        max_return_instances: int,
        oms_client: OMSClient,
    ) -> Dict[str, Any]:
        prepared = await self._prepare(
            target_schema=target_schema,
            mappings=mappings,
            max_import_rows=max_import_rows,
            options=options,
        )

        errors = prepared.build.get("errors") or []
        if errors and not allow_partial:
            raise classified_http_exception(
                status.HTTP_400_BAD_REQUEST,
                "Import validation failed. Fix errors or set allow_partial=true to skip bad rows.",
                code=ErrorCode.REQUEST_VALIDATION_FAILED,
                extra={
                    "stats": prepared.build.get("stats") or {},
                    "errors": errors[:200],
                },
            )

        instances = prepared.build.get("instances") or []
        instance_row_indices = prepared.build.get("instance_row_indices") or []
        error_row_indices = set(prepared.build.get("error_row_indices") or [])

        if error_row_indices:
            filtered_instances = []
            for inst, row_idx in zip(instances, instance_row_indices):
                if row_idx in error_row_indices:
                    continue
                filtered_instances.append(inst)
            instances = filtered_instances

        if not instances:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "No valid instances to import", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        batch_size = max(1, min(int(batch_size or 500), 5000))
        prepared_batches = [
            {"batch_index": i // batch_size, "count": len(instances[i : i + batch_size])}
            for i in range(0, len(instances), batch_size)
        ]

        base_metadata = self._source.commit_base_metadata(table=prepared.table, mappings=mappings)

        returned_instances: List[Dict[str, Any]] = []
        has_more_instances = False
        if return_instances:
            max_return = max(0, min(int(max_return_instances or 1000), 50000))
            returned_instances = instances[:max_return]
            has_more_instances = len(instances) > max_return

        submitted_commands: List[Dict[str, Any]] = []
        for batch in prepared_batches:
            idx = int(batch["batch_index"])
            start = idx * batch_size
            end = start + batch_size
            batch_instances = instances[start:end]

            resp = await oms_client.post(
                f"/api/v1/instances/{db_name}/async/{target_class_id}/bulk-create",
                params={"branch": "main"},
                json={
                    "instances": batch_instances,
                    "metadata": {
                        "import": base_metadata,
                        "import_batch": {"index": idx, "count": len(batch_instances)},
                    },
                },
            )

            command_id = resp.get("command_id") if isinstance(resp, dict) else None
            submitted_commands.append(
                {
                    "batch_index": idx,
                    "count": len(batch_instances),
                    "command": resp,
                    "status_url": f"/api/v1/commands/{command_id}/status" if command_id else None,
                }
            )

        return {
            "status": "success",
            "message": "Import accepted and submitted to OMS (async write pipeline started)",
            "source_info": base_metadata,
            "target_info": {"class_id": target_class_id},
            "stats": {
                **(prepared.build.get("stats") or {}),
                "prepared_instances": len(instances),
                "skipped_error_rows": len(error_row_indices),
                "batches": len(prepared_batches),
                "submitted_commands": len(submitted_commands),
            },
            "warnings": prepared.build.get("warnings") or [],
            "errors": errors[:200],
            "prepared": {
                "batch_size": batch_size,
                "batches": prepared_batches,
                "instances": returned_instances,
                "has_more_instances": has_more_instances,
            },
            "write": {
                "branch": "main",
                "commands": submitted_commands,
            },
            "structure": prepared.structure,
        }


@trace_external_call("bff.ontology_imports.dry_run_import_from_google_sheets")
async def dry_run_import_from_google_sheets(*, db_name: str, body: ImportFromGoogleSheetsRequest) -> Dict[str, Any]:
    """
    Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
    """
    try:
        db_name = validate_db_name(db_name)
        _ = db_name  # Currently a namespacing guard only.

        target_class_id = validate_class_id(body.target_class_id)
        if not body.target_schema:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "target_schema is required for import (field types)", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        source = _GoogleSheetsSource(
            sheet_url=body.sheet_url,
            worksheet_name=body.worksheet_name,
            api_key=body.api_key,
            connection_id=body.connection_id,
            table_id=body.table_id,
            table_bbox=body.table_bbox.model_dump() if body.table_bbox is not None else None,
            max_tables=body.max_tables,
            max_rows=body.max_rows,
            max_cols=body.max_cols,
            trim_trailing_empty=bool(body.trim_trailing_empty),
        )

        processor = _DryRunImportProcessor(source, dry_run_rows=body.dry_run_rows)
        return await processor.run(
            target_class_id=target_class_id,
            target_schema=body.target_schema,
            mappings=body.mappings,
            max_import_rows=body.max_import_rows,
            options=body.options,
        )

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Google Sheets dry-run import failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Google Sheets dry-run import 실패: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.ontology_imports.commit_import_from_google_sheets")
async def commit_import_from_google_sheets(
    *,
    db_name: str,
    body: ImportFromGoogleSheetsRequest,
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """
    Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(body.target_class_id)
        if not body.target_schema:
            raise classified_http_exception(status.HTTP_400_BAD_REQUEST, "target_schema is required for import (field types)", code=ErrorCode.REQUEST_VALIDATION_FAILED)

        source = _GoogleSheetsSource(
            sheet_url=body.sheet_url,
            worksheet_name=body.worksheet_name,
            api_key=body.api_key,
            connection_id=body.connection_id,
            table_id=body.table_id,
            table_bbox=body.table_bbox.model_dump() if body.table_bbox is not None else None,
            max_tables=body.max_tables,
            max_rows=body.max_rows,
            max_cols=body.max_cols,
            trim_trailing_empty=bool(body.trim_trailing_empty),
        )

        processor = _CommitImportProcessor(source)
        return await processor.run(
            db_name=db_name,
            target_class_id=target_class_id,
            target_schema=body.target_schema,
            mappings=body.mappings,
            max_import_rows=body.max_import_rows,
            options=body.options,
            allow_partial=bool(body.allow_partial),
            batch_size=int(body.batch_size),
            return_instances=bool(body.return_instances),
            max_return_instances=int(body.max_return_instances),
            oms_client=oms_client,
        )

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Google Sheets import commit failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Google Sheets import 실패: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.ontology_imports.dry_run_import_from_excel")
async def dry_run_import_from_excel(
    *,
    db_name: str,
    file: UploadFile,
    target_class_id: str,
    target_schema_json: Optional[str],
    mappings_json: str,
    sheet_name: Optional[str],
    table_id: Optional[str],
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
    max_tables: int,
    max_rows: Optional[int],
    max_cols: Optional[int],
    dry_run_rows: int,
    max_import_rows: Optional[int],
    options_json: Optional[str],
) -> Dict[str, Any]:
    """
    Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
    """
    try:
        db_name = validate_db_name(db_name)
        _ = db_name
        target_class_id = validate_class_id(target_class_id)

        filename, content = await read_excel_upload(file)

        mappings_raw = parse_json_array(
            mappings_json,
            field_name="mappings_json",
            type_error_message="mappings_json must be a JSON array",
        )
        mappings = [ImportFieldMapping(**m) for m in mappings_raw]

        options = parse_json_object(
            options_json,
            field_name="options_json",
            default={},
            treat_blank_as_missing=True,
            type_error_message="options_json must be an object",
        )

        target_schema_raw = parse_json_array(
            target_schema_json,
            field_name="target_schema_json",
            required_message="target_schema_json is required for import (field types)",
            treat_blank_as_missing=True,
            type_error_message="target_schema_json must be a JSON array",
        )
        target_schema = [ImportTargetField(**f) for f in target_schema_raw]

        table_bbox = parse_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        source = _ExcelSource(
            filename=filename,
            xlsx_bytes=content,
            sheet_name=sheet_name,
            table_id=table_id,
            table_bbox=table_bbox,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
        )

        processor = _DryRunImportProcessor(source, dry_run_rows=dry_run_rows)
        return await processor.run(
            target_class_id=target_class_id,
            target_schema=target_schema,
            mappings=mappings,
            max_import_rows=max_import_rows,
            options=options,
        )

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Excel dry-run import failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Excel dry-run import 실패: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc


@trace_external_call("bff.ontology_imports.commit_import_from_excel")
async def commit_import_from_excel(
    *,
    db_name: str,
    file: UploadFile,
    target_class_id: str,
    target_schema_json: Optional[str],
    mappings_json: str,
    sheet_name: Optional[str],
    table_id: Optional[str],
    table_top: Optional[int],
    table_left: Optional[int],
    table_bottom: Optional[int],
    table_right: Optional[int],
    max_tables: int,
    max_rows: Optional[int],
    max_cols: Optional[int],
    allow_partial: bool,
    max_import_rows: Optional[int],
    batch_size: int,
    return_instances: bool,
    max_return_instances: int,
    options_json: Optional[str],
    oms_client: OMSClient,
) -> Dict[str, Any]:
    """
    Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
    """
    try:
        db_name = validate_db_name(db_name)
        target_class_id = validate_class_id(target_class_id)

        filename, content = await read_excel_upload(file)

        mappings_raw = parse_json_array(
            mappings_json,
            field_name="mappings_json",
            type_error_message="mappings_json must be a JSON array",
        )
        mappings = [ImportFieldMapping(**m) for m in mappings_raw]

        options = parse_json_object(
            options_json,
            field_name="options_json",
            default={},
            treat_blank_as_missing=True,
            type_error_message="options_json must be an object",
        )

        target_schema_raw = parse_json_array(
            target_schema_json,
            field_name="target_schema_json",
            required_message="target_schema_json is required for import (field types)",
            treat_blank_as_missing=True,
            type_error_message="target_schema_json must be a JSON array",
        )
        target_schema = [ImportTargetField(**f) for f in target_schema_raw]

        table_bbox = parse_table_bbox(
            table_top=table_top,
            table_left=table_left,
            table_bottom=table_bottom,
            table_right=table_right,
        )

        source = _ExcelSource(
            filename=filename,
            xlsx_bytes=content,
            sheet_name=sheet_name,
            table_id=table_id,
            table_bbox=table_bbox,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
        )

        processor = _CommitImportProcessor(source)
        return await processor.run(
            db_name=db_name,
            target_class_id=target_class_id,
            target_schema=target_schema,
            mappings=mappings,
            max_import_rows=max_import_rows,
            options=options,
            allow_partial=bool(allow_partial),
            batch_size=int(batch_size),
            return_instances=bool(return_instances),
            max_return_instances=int(max_return_instances),
            oms_client=oms_client,
        )

    except httpx.HTTPStatusError as exc:
        raise_httpx_as_http_exception(exc)
    except (SecurityViolationError, ValueError) as exc:
        raise classified_http_exception(status.HTTP_400_BAD_REQUEST, str(exc), code=ErrorCode.REQUEST_VALIDATION_FAILED) from exc
    except HTTPException:
        raise
    except Exception as exc:
        logger.error("Excel import commit failed: %s", exc)
        raise classified_http_exception(status.HTTP_500_INTERNAL_SERVER_ERROR, f"Excel import 실패: {str(exc)}", code=ErrorCode.INTERNAL_ERROR) from exc
