"""In-process Funnel runtime client.

BFF에서 Funnel 분석 런타임을 in-process ASGI transport로 호출한다.
외부 Funnel HTTP 서비스 주소/모드는 지원하지 않는다.
Internal runtime routes are non-versioned and mounted under `/internal/funnel/*`.
"""

import hashlib
import io
import importlib
import logging
from typing import Any, Dict, List, Optional

import httpx

from shared.config.settings import get_settings
from bff.services.base_http_client import ManagedAsyncClient

logger = logging.getLogger(__name__)


class FunnelClient(ManagedAsyncClient):
    """Funnel HTTP 클라이언트"""

    def __init__(self):
        self.runtime_mode = "internal"
        self.excel_timeout_seconds = self._resolve_excel_timeout_seconds()
        self.client = self._build_internal_client()
        logger.info("Funnel Client initialized in internal mode (in-process ASGI)")

    def _build_internal_client(self) -> httpx.AsyncClient:
        # Import lazily to avoid app import side effects unless internal mode is enabled.
        # Compose/runtime image layouts may differ:
        # - local/backend test runner: `funnel.main`
        # - compose image (package under /app/backend): `backend.funnel.main`
        funnel_app = None
        import_errors: list[str] = []
        for module_name in ("funnel.main", "backend.funnel.main"):
            try:
                module = importlib.import_module(module_name)
                funnel_app = getattr(module, "app", None)
                if funnel_app is not None:
                    break
            except ImportError as exc:
                import_errors.append(f"{module_name}: {exc}")

        if funnel_app is None:
            raise ModuleNotFoundError(
                "Failed to import Funnel ASGI app. Tried: "
                + ", ".join(import_errors or ["funnel.main", "backend.funnel.main"])
            )

        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=funnel_app),
            base_url="http://funnel-internal",
            timeout=30.0,
            headers={"Accept": "application/json"},
        )

    @staticmethod
    def _resolve_excel_timeout_seconds() -> float:
        return float(get_settings().services.funnel_excel_timeout_seconds)

    async def check_health(self) -> bool:
        """Funnel 서비스 상태 확인"""
        try:
            response = await self.client.get("/health")
            response.raise_for_status()
            data = response.json()
            if not isinstance(data, dict):
                return False
            # Support canonical runtime payloads, legacy healthy payloads, and wrapped ApiResponse payloads.
            if data.get("ready") is True:
                return True
            if data.get("status") in {"healthy", "ready"}:
                return True
            if data.get("status") == "success":
                inner = data.get("data") or {}
                if not isinstance(inner, dict):
                    return False
                return inner.get("ready") is True or inner.get("status") in {"healthy", "ready"}
            return False
        except (httpx.HTTPError, ValueError) as e:
            logger.error(f"Funnel 헬스 체크 실패: {e}")
            return False

    # NOTE: Legacy type inference methods (analyze_dataset, suggest_schema,
    # preview_google_sheets, analyze_and_suggest_schema) have been removed.
    # Palantir Foundry style: all columns default to xsd:string.
    # Type coercion happens at objectify/write time via coerce_value().

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

        response = await self.client.post("/internal/funnel/structure/analyze/google-sheets", json=payload)
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
            "/internal/funnel/structure/analyze/excel",
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
            "/internal/funnel/structure/analyze/excel",
            params=params,
            files=files,
            timeout=self.excel_timeout_seconds,
        )
        response.raise_for_status()
        try:
            fileobj.seek(0)
        except (AttributeError, OSError, ValueError):
            logging.getLogger(__name__).warning("Failed to rewind file stream after funnel analysis", exc_info=True)
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
                logging.getLogger(__name__).warning("Exception fallback at bff/services/funnel_client.py:592", exc_info=True)
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
        except (TypeError, ValueError, KeyError):
            logging.getLogger(__name__).warning("Exception fallback at bff/services/funnel_client.py:660", exc_info=True)
            return None

    @staticmethod
    def _estimate_total_rows(table: Dict[str, Any]) -> int:
        """Estimate the total data rows from a table structure's bounding box."""
        mode = table.get("mode")
        header_rows = int(table.get("header_rows") or 0)
        header_cols = int(table.get("header_cols") or 0)
        if mode == "property":
            return len(table.get("key_values") or [])

        bbox = FunnelClient._normalize_bbox_dict(table.get("bbox") or {})
        if not bbox:
            return 0
        try:
            height = int(bbox["bottom"] - bbox["top"] + 1)
            width = int(bbox["right"] - bbox["left"] + 1)
            if mode == "table":
                return max(0, height - max(1, header_rows))
            elif mode == "transposed":
                return max(0, width - max(1, header_cols))
        except (TypeError, ValueError, KeyError):
            pass
        return 0

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
        total_rows_est = FunnelClient._estimate_total_rows(table)

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
        total_rows_est = FunnelClient._estimate_total_rows(table)

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

    # NOTE: Legacy excel_to_schema() method removed.
    # Palantir Foundry style: all columns default to xsd:string.
    # Type coercion happens at objectify/write time via coerce_value().

    async def __aenter__(self):
        return self

    async def __aexit__(self, _exc_type, _exc_val, _exc_tb):
        await self.close()
