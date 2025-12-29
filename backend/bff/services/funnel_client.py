"""
Funnel Service 클라이언트
BFF에서 Funnel 마이크로서비스와 통신하기 위한 HTTP 클라이언트
"""

import hashlib
import io
import logging
import os
from typing import Any, Dict, List, Optional

import httpx

from shared.config.service_config import ServiceConfig

logger = logging.getLogger(__name__)


class FunnelClient:
    """Funnel HTTP 클라이언트"""

    def __init__(self, base_url: str = None):
        self.base_url = base_url or ServiceConfig.get_funnel_url()

        # SSL 설정 가져오기
        ssl_config = ServiceConfig.get_client_ssl_config()
        self.excel_timeout_seconds = self._resolve_excel_timeout_seconds()

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers={"Accept": "application/json"},
            verify=ssl_config.get("verify", True),
        )

        logger.info(f"Funnel Client initialized with base URL: {self.base_url}")

    @staticmethod
    def _resolve_excel_timeout_seconds() -> float:
        raw = (os.getenv("FUNNEL_EXCEL_TIMEOUT_SECONDS") or os.getenv("FUNNEL_CLIENT_TIMEOUT_SECONDS") or "").strip()
        if not raw:
            return 120.0
        try:
            return max(5.0, float(raw))
        except ValueError:
            return 120.0

    async def close(self):
        """클라이언트 연결 종료"""
        await self.client.aclose()

    async def check_health(self) -> bool:
        """Funnel 서비스 상태 확인"""
        try:
            response = await self.client.get("/health")
            response.raise_for_status()
            data = response.json()
            return data.get("status") == "healthy"
        except Exception as e:
            logger.error(f"Funnel 헬스 체크 실패: {e}")
            return False

    async def analyze_dataset(self, request_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        데이터셋 타입 분석

        Args:
            request_data: DatasetAnalysisRequest 형태의 데이터

        Returns:
            DatasetAnalysisResponse 형태의 분석 결과
        """
        try:
            response = await self.client.post("/api/v1/funnel/analyze", json=request_data)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"데이터셋 분석 실패: {e}")
            raise

    async def suggest_schema(
        self, analysis_results: Dict[str, Any], class_name: str = None
    ) -> Dict[str, Any]:
        """
        분석 결과를 기반으로 OMS 스키마 제안

        Args:
            analysis_results: DatasetAnalysisResponse 형태의 분석 결과
            class_name: 선택적 클래스 이름

        Returns:
            OMS 호환 스키마 제안
        """
        try:
            params = {}
            if class_name:
                params["class_name"] = class_name

            response = await self.client.post(
                "/api/v1/funnel/suggest-schema", json=analysis_results, params=params
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"스키마 제안 실패: {e}")
            raise

    async def preview_google_sheets(
        self,
        sheet_url: str,
        worksheet_name: str = None,
        api_key: str = None,
        connection_id: Optional[str] = None,
        infer_types: bool = True,
        include_complex_types: bool = False,
    ) -> Dict[str, Any]:
        """
        Google Sheets 데이터 미리보기와 타입 추론

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름 (선택사항)
            api_key: Google Sheets API 키 (선택사항)
            infer_types: 타입 추론 여부
            include_complex_types: 복합 타입 포함 여부

        Returns:
            FunnelPreviewResponse 형태의 미리보기 결과
        """
        try:
            params = {
                "sheet_url": sheet_url,
                "infer_types": infer_types,
                "include_complex_types": include_complex_types,
            }

            if worksheet_name:
                params["worksheet_name"] = worksheet_name
            if api_key:
                params["api_key"] = api_key
            if connection_id:
                params["connection_id"] = connection_id

            response = await self.client.post("/api/v1/funnel/preview/google-sheets", params=params)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"Google Sheets 미리보기 실패: {e}")
            raise

    async def analyze_and_suggest_schema(
        self,
        data: List[List[Any]],
        columns: List[str],
        class_name: str = None,
        sample_size: int = 1000,
        include_complex_types: bool = False,
    ) -> Dict[str, Any]:
        """
        데이터 분석과 스키마 제안을 한 번에 실행

        Args:
            data: 분석할 데이터 행들
            columns: 컬럼 이름들
            class_name: 선택적 클래스 이름
            sample_size: 분석할 샘플 크기
            include_complex_types: 복합 타입 포함 여부

        Returns:
            스키마 제안 결과
        """
        try:
            # 1. 데이터 분석
            analysis_request = {
                "data": data,
                "columns": columns,
                "sample_size": sample_size,
                "include_complex_types": include_complex_types,
            }

            analysis_result = await self.analyze_dataset(analysis_request)

            # 2. 스키마 제안
            schema_suggestion = await self.suggest_schema(analysis_result, class_name)

            return {"analysis": analysis_result, "schema_suggestion": schema_suggestion}
        except Exception as e:
            logger.error(f"분석 및 스키마 제안 실패: {e}")
            raise

    async def google_sheets_to_schema(
        self,
        sheet_url: str,
        worksheet_name: str = None,
        class_name: str = None,
        api_key: str = None,
        connection_id: Optional[str] = None,
        table_id: Optional[str] = None,
        table_bbox: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Google Sheets에서 직접 스키마 생성

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름
            class_name: 생성할 클래스 이름
            api_key: Google Sheets API 키

        Returns:
            스키마 제안 결과
        """
        try:
            # Preferred path: structure analysis (handles multi-table, transposed, key-value forms)
            try:
                structure = await self.analyze_google_sheets_structure(
                    sheet_url=sheet_url,
                    worksheet_name=worksheet_name,
                    api_key=api_key,
                    connection_id=connection_id,
                    include_complex_types=True,
                )
                table = self._select_requested_table(
                    structure,
                    table_id=table_id,
                    table_bbox=table_bbox,
                )
                preview_result = self._structure_table_to_preview(
                    structure=structure,
                    table=table,
                    sheet_url=sheet_url,
                    worksheet_name=worksheet_name,
                )

                analysis_result = {
                    "columns": preview_result.get("inferred_schema") or [],
                    "analysis_metadata": {
                        "source": "google_sheets",
                        "sheet_url": sheet_url,
                        "worksheet_name": worksheet_name,
                        "table_id": table.get("id"),
                        "table_mode": table.get("mode"),
                        "table_bbox": table.get("bbox"),
                        "total_rows": preview_result.get("total_rows", 0),
                    },
                }
                schema_suggestion = await self.suggest_schema(analysis_result, class_name)
                return {"preview": preview_result, "schema_suggestion": schema_suggestion, "structure": structure}
            except Exception as e:
                if table_id or table_bbox:
                    raise
                logger.warning(f"Structure analysis path failed; falling back to simple preview: {e}")

            # Fallback path: legacy preview (assumes header-row table)
            preview_result = await self.preview_google_sheets(
                sheet_url=sheet_url,
                worksheet_name=worksheet_name,
                api_key=api_key,
                connection_id=connection_id,
                infer_types=True,
                include_complex_types=True,
            )

            if not preview_result.get("inferred_schema"):
                raise ValueError("타입 추론에 실패했습니다.")

            analysis_result = {
                "columns": preview_result["inferred_schema"],
                "analysis_metadata": {
                    "source": "google_sheets",
                    "sheet_url": sheet_url,
                    "worksheet_name": worksheet_name,
                    "total_rows": preview_result.get("total_rows", 0),
                },
            }
            schema_suggestion = await self.suggest_schema(analysis_result, class_name)
            return {"preview": preview_result, "schema_suggestion": schema_suggestion}

        except Exception as e:
            logger.error(f"Google Sheets 스키마 생성 실패: {e}")
            raise

    async def google_sheets_to_structure_preview(
        self,
        *,
        sheet_url: str,
        worksheet_name: Optional[str] = None,
        api_key: Optional[str] = None,
        connection_id: Optional[str] = None,
        table_id: Optional[str] = None,
        table_bbox: Optional[Dict[str, Any]] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        trim_trailing_empty: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Google Sheets URL → (grid/merged_cells) → structure analysis → selected table preview.

        Notes:
        - This method does NOT call `/suggest-schema`; it only returns preview + structure for downstream steps
          (e.g., mapping suggestions, UI preview).
        """
        structure = await self.analyze_google_sheets_structure(
            sheet_url=sheet_url,
            worksheet_name=worksheet_name,
            api_key=api_key,
            connection_id=connection_id,
            include_complex_types=include_complex_types,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
            trim_trailing_empty=trim_trailing_empty,
            options=options,
        )
        table = self._select_requested_table(structure, table_id=table_id, table_bbox=table_bbox)
        preview = self._structure_table_to_preview(
            structure=structure,
            table=table,
            sheet_url=sheet_url,
            worksheet_name=worksheet_name,
        )
        return {"preview": preview, "structure": structure, "table": table}

    async def analyze_google_sheets_structure(
        self,
        *,
        sheet_url: str,
        worksheet_name: Optional[str] = None,
        api_key: Optional[str] = None,
        connection_id: Optional[str] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        trim_trailing_empty: bool = True,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Analyze sheet structure via Funnel (Google Sheets URL → grid/merged_cells → structure analysis).
        """
        payload: Dict[str, Any] = {
            "sheet_url": sheet_url,
            "worksheet_name": worksheet_name,
            "api_key": api_key,
            "connection_id": connection_id,
            "include_complex_types": include_complex_types,
            "max_tables": max_tables,
            "max_rows": max_rows,
            "max_cols": max_cols,
            "trim_trailing_empty": trim_trailing_empty,
            "options": options or {},
        }
        # Drop nulls to keep request minimal
        payload = {k: v for k, v in payload.items() if v is not None}

        response = await self.client.post("/api/v1/funnel/structure/analyze/google-sheets", json=payload)
        response.raise_for_status()
        return response.json()

    async def excel_to_structure_preview(
        self,
        *,
        xlsx_bytes: bytes,
        filename: str,
        sheet_name: Optional[str] = None,
        table_id: Optional[str] = None,
        table_bbox: Optional[Dict[str, Any]] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Excel bytes → (grid/merged_cells) → structure analysis → selected table preview.

        Notes:
        - This method does NOT call `/suggest-schema`; it only returns preview + structure for downstream steps
          (e.g., mapping suggestions, UI preview).
        """
        structure = await self.analyze_excel_structure(
            xlsx_bytes=xlsx_bytes,
            filename=filename,
            sheet_name=sheet_name,
            include_complex_types=include_complex_types,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
            options=options,
        )

        table = self._select_requested_table(structure, table_id=table_id, table_bbox=table_bbox)
        preview = self._structure_table_to_excel_preview(
            structure=structure,
            table=table,
            file_name=filename,
            sheet_name=sheet_name,
        )
        return {"preview": preview, "structure": structure, "table": table}

    async def excel_to_structure_preview_stream(
        self,
        *,
        fileobj: Any,
        filename: str,
        sheet_name: Optional[str] = None,
        table_id: Optional[str] = None,
        table_bbox: Optional[Dict[str, Any]] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], str]:
        """
        Excel stream → (grid/merged_cells) → structure analysis → selected table preview.
        """
        structure, checksum = await self.analyze_excel_structure_stream(
            fileobj=fileobj,
            filename=filename,
            sheet_name=sheet_name,
            include_complex_types=include_complex_types,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
            options=options,
        )

        table = self._select_requested_table(structure, table_id=table_id, table_bbox=table_bbox)
        preview = self._structure_table_to_excel_preview(
            structure=structure,
            table=table,
            file_name=filename,
            sheet_name=sheet_name,
        )
        return {"preview": preview, "structure": structure, "table": table}, checksum

    async def analyze_excel_structure(
        self,
        *,
        xlsx_bytes: bytes,
        filename: str,
        sheet_name: Optional[str] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Analyze sheet structure via Funnel (Excel bytes → grid/merged_cells → structure analysis).
        """
        import json

        params: Dict[str, Any] = {
            "sheet_name": sheet_name,
            "include_complex_types": include_complex_types,
            "max_tables": max_tables,
            "max_rows": max_rows,
            "max_cols": max_cols,
        }
        if options is not None:
            params["options_json"] = json.dumps(options, ensure_ascii=False)
        params = {k: v for k, v in params.items() if v is not None}

        files = {
            "file": (
                filename,
                xlsx_bytes,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        }

        response = await self.client.post(
            "/api/v1/funnel/structure/analyze/excel",
            params=params,
            files=files,
            timeout=self.excel_timeout_seconds,
        )
        response.raise_for_status()
        return response.json()

    async def analyze_excel_structure_stream(
        self,
        *,
        fileobj: Any,
        filename: str,
        sheet_name: Optional[str] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> tuple[Dict[str, Any], str]:
        """
        Analyze sheet structure via Funnel (streaming Excel upload).
        """
        import json

        if not hasattr(fileobj, "read") or not hasattr(fileobj, "seek"):
            raise ValueError("fileobj must be a seekable file-like object")

        params: Dict[str, Any] = {
            "sheet_name": sheet_name,
            "include_complex_types": include_complex_types,
            "max_tables": max_tables,
            "max_rows": max_rows,
            "max_cols": max_cols,
        }
        if options is not None:
            params["options_json"] = json.dumps(options, ensure_ascii=False)
        params = {k: v for k, v in params.items() if v is not None}

        hasher = hashlib.sha256()

        class _HashingReader(io.RawIOBase):
            def __init__(self, raw: Any, digest) -> None:
                self._raw = raw
                self._digest = digest

            def read(self, size: int = -1) -> bytes:
                chunk = self._raw.read(size)
                if chunk:
                    self._digest.update(chunk)
                return chunk

            def readable(self) -> bool:
                return True

            def seekable(self) -> bool:
                return hasattr(self._raw, "seek")

            def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
                return self._raw.seek(offset, whence)

            def tell(self) -> int:
                return self._raw.tell()

            def close(self) -> None:
                return None

        fileobj.seek(0)
        stream = _HashingReader(fileobj, hasher)
        files = {
            "file": (
                filename,
                stream,
                "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
        }

        response = await self.client.post(
            "/api/v1/funnel/structure/analyze/excel",
            params=params,
            files=files,
            timeout=self.excel_timeout_seconds,
        )
        response.raise_for_status()
        try:
            fileobj.seek(0)
        except Exception:
            pass
        return response.json(), hasher.hexdigest()
    @staticmethod
    def _select_primary_table(structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Choose a single "primary" table for schema suggestion.

        Heuristic (domain-neutral):
        - Prefer record-like tables (mode=table/transposed) over property forms.
        - Then prefer higher confidence.
        - Tie-break by area, column count, sample rows.
        """
        tables = structure.get("tables") or []
        if not tables:
            raise ValueError("No tables detected in sheet")

        def area(t: Dict[str, Any]) -> int:
            bbox = t.get("bbox") or {}
            try:
                return int(bbox.get("bottom", 0) - bbox.get("top", 0) + 1) * int(
                    bbox.get("right", 0) - bbox.get("left", 0) + 1
                )
            except Exception:
                return 0

        def rank(t: Dict[str, Any]) -> tuple:
            mode = t.get("mode")
            is_record_table = mode in {"table", "transposed"}
            conf = float(t.get("confidence") or 0.0)
            headers = t.get("headers") or []
            rows = t.get("sample_rows") or []
            return (is_record_table, conf, area(t), len(headers), len(rows))

        return max(tables, key=rank)

    @classmethod
    def _select_requested_table(
        cls,
        structure: Dict[str, Any],
        *,
        table_id: Optional[str],
        table_bbox: Optional[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Select a table from structure analysis output.

        When neither table_id nor table_bbox are provided, falls back to the primary-table heuristic.
        """
        if not table_id and not table_bbox:
            return cls._select_primary_table(structure)

        tables = structure.get("tables") or []
        if not tables:
            raise ValueError("No tables detected in sheet")

        if table_id:
            by_id = next((t for t in tables if str(t.get("id")) == str(table_id)), None)
            if by_id is None:
                available = [t.get("id") for t in tables]
                raise ValueError(f"Unknown table_id '{table_id}'. Available: {available}")

            if table_bbox:
                want = cls._normalize_bbox_dict(table_bbox)
                got = cls._normalize_bbox_dict(by_id.get("bbox") or {})
                if want and got and want != got:
                    raise ValueError(
                        f"table_id '{table_id}' bbox mismatch: expected {want}, got {got}"
                    )
            return by_id

        want = cls._normalize_bbox_dict(table_bbox or {})
        if not want:
            return cls._select_primary_table(structure)

        by_bbox = next(
            (t for t in tables if cls._normalize_bbox_dict(t.get("bbox") or {}) == want),
            None,
        )
        if by_bbox is None:
            available = [cls._normalize_bbox_dict(t.get("bbox") or {}) for t in tables]
            raise ValueError(f"Unknown table_bbox {want}. Available: {available}")
        return by_bbox

    @staticmethod
    def _normalize_bbox_dict(bbox: Dict[str, Any]) -> Optional[Dict[str, int]]:
        try:
            keys = ("top", "left", "bottom", "right")
            if not all(k in bbox for k in keys):
                return None
            return {k: int(bbox[k]) for k in keys}
        except Exception:
            return None

    @staticmethod
    def _structure_table_to_preview(
        *,
        structure: Dict[str, Any],
        table: Dict[str, Any],
        sheet_url: str,
        worksheet_name: Optional[str],
    ) -> Dict[str, Any]:
        meta = structure.get("metadata") or {}
        sheet_id = meta.get("sheet_id")
        sheet_title = meta.get("sheet_title")
        worksheet_title = meta.get("worksheet_title") or worksheet_name

        headers = table.get("headers") or []
        rows = table.get("sample_rows") or []

        bbox = table.get("bbox") or {}
        mode = table.get("mode")
        header_rows = int(table.get("header_rows") or 0)
        header_cols = int(table.get("header_cols") or 0)

        total_rows_est = 0
        try:
            height = int(bbox.get("bottom") - bbox.get("top") + 1)
            width = int(bbox.get("right") - bbox.get("left") + 1)
            if mode == "table":
                total_rows_est = max(0, height - max(1, header_rows))
            elif mode == "transposed":
                total_rows_est = max(0, width - max(1, header_cols))
            elif mode == "property":
                total_rows_est = len(table.get("key_values") or [])
        except Exception:
            total_rows_est = 0

        return {
            "source_metadata": {
                "type": "google_sheets",
                "sheet_id": sheet_id,
                "sheet_title": sheet_title,
                "worksheet_title": worksheet_title,
                "sheet_url": sheet_url,
                "table_id": table.get("id"),
                "table_mode": mode,
                "table_bbox": bbox,
            },
            "columns": headers,
            "sample_data": rows,
            "inferred_schema": table.get("inferred_schema"),
            "total_rows": total_rows_est,
            "preview_rows": len(rows),
        }

    @staticmethod
    def _structure_table_to_excel_preview(
        *,
        structure: Dict[str, Any],
        table: Dict[str, Any],
        file_name: str,
        sheet_name: Optional[str],
    ) -> Dict[str, Any]:
        meta = structure.get("metadata") or {}
        headers = table.get("headers") or []
        rows = table.get("sample_rows") or []

        bbox = table.get("bbox") or {}
        mode = table.get("mode")
        header_rows = int(table.get("header_rows") or 0)
        header_cols = int(table.get("header_cols") or 0)

        total_rows_est = 0
        try:
            height = int(bbox.get("bottom") - bbox.get("top") + 1)
            width = int(bbox.get("right") - bbox.get("left") + 1)
            if mode == "table":
                total_rows_est = max(0, height - max(1, header_rows))
            elif mode == "transposed":
                total_rows_est = max(0, width - max(1, header_cols))
            elif mode == "property":
                total_rows_est = len(table.get("key_values") or [])
        except Exception:
            total_rows_est = 0

        return {
            "source_metadata": {
                "type": "excel",
                "file_name": file_name,
                "sheet_name": sheet_name or meta.get("sheet_name"),
                "table_id": table.get("id"),
                "table_mode": mode,
                "table_bbox": bbox,
            },
            "columns": headers,
            "sample_data": rows,
            "inferred_schema": table.get("inferred_schema"),
            "total_rows": total_rows_est,
            "preview_rows": len(rows),
        }

    async def excel_to_schema(
        self,
        *,
        xlsx_bytes: bytes,
        filename: str,
        sheet_name: Optional[str] = None,
        class_name: Optional[str] = None,
        table_id: Optional[str] = None,
        table_bbox: Optional[Dict[str, Any]] = None,
        include_complex_types: bool = True,
        max_tables: int = 5,
        max_rows: Optional[int] = None,
        max_cols: Optional[int] = None,
        options: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Excel 업로드에서 직접 스키마 생성 (구조 분석 기반).

        Notes:
        - 멀티 테이블/전치/폼 문서도 처리 가능하도록 구조 분석 결과에서 "대표 테이블"을 선택합니다.
        """
        structure = await self.analyze_excel_structure(
            xlsx_bytes=xlsx_bytes,
            filename=filename,
            sheet_name=sheet_name,
            include_complex_types=include_complex_types,
            max_tables=max_tables,
            max_rows=max_rows,
            max_cols=max_cols,
            options=options,
        )

        table = self._select_requested_table(
            structure,
            table_id=table_id,
            table_bbox=table_bbox,
        )
        preview_result = self._structure_table_to_excel_preview(
            structure=structure,
            table=table,
            file_name=filename,
            sheet_name=sheet_name,
        )

        analysis_result = {
            "columns": preview_result.get("inferred_schema") or [],
            "analysis_metadata": {
                "source": "excel",
                "file_name": filename,
                "sheet_name": sheet_name,
                "table_id": table.get("id"),
                "table_mode": table.get("mode"),
                "table_bbox": table.get("bbox"),
                "total_rows": preview_result.get("total_rows", 0),
            },
        }
        schema_suggestion = await self.suggest_schema(analysis_result, class_name)
        return {"preview": preview_result, "schema_suggestion": schema_suggestion, "structure": structure}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
