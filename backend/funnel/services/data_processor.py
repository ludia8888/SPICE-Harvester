"""
🔥 THINK ULTRA! Funnel Data Processor Service
Data Connector와 OMS/BFF 사이의 데이터 처리 레이어
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data_connector.google_sheets.auth import GoogleOAuth2Client
from data_connector.google_sheets.service import GoogleSheetsService
from shared.models.google_sheets import GoogleSheetPreviewRequest, GoogleSheetPreviewResponse

from funnel.services.risk_assessor import assess_dataset_risks
from funnel.services.schema_utils import normalize_property_name
from funnel.services.type_inference import FunnelTypeInferenceService
from shared.models.type_inference import (
    ColumnAnalysisResult,
    DatasetAnalysisRequest,
    DatasetAnalysisResponse,
    TabularPreviewResponse,
)
from shared.services.registries.connector_registry import ConnectorRegistry


class FunnelDataProcessor:
    """
    🔥 THINK ULTRA! 데이터 처리 파이프라인

    Architecture Flow:
    1. Data Connector (Google Sheets, CSV, etc.) → Raw Data
    2. Funnel → Type Inference & Data Processing
    3. OMS/BFF → Schema Generation & Storage
    """

    def __init__(self, *, connector_registry: Optional[ConnectorRegistry] = None):
        self.type_inference_service = FunnelTypeInferenceService
        self._connector_registry = connector_registry
        self._owns_connector_registry = connector_registry is None
        self._connector_registry_connected = False

    async def initialize(self) -> None:
        if self._connector_registry is None:
            self._connector_registry = ConnectorRegistry()
            self._owns_connector_registry = True
        if self._owns_connector_registry and not self._connector_registry_connected:
            await self._connector_registry.connect()
            self._connector_registry_connected = True

    async def close(self) -> None:
        if self._owns_connector_registry and self._connector_registry and self._connector_registry_connected:
            await self._connector_registry.close()
            self._connector_registry_connected = False

    async def _get_connector_registry(self) -> ConnectorRegistry:
        await self.initialize()
        assert self._connector_registry is not None
        return self._connector_registry

    async def resolve_optional_access_token(
        self,
        *,
        connection_id: Optional[str],
    ) -> Optional[str]:
        resolved_connection_id = str(connection_id or "").strip()
        if not resolved_connection_id:
            return None

        connector_registry = await self._get_connector_registry()
        source = await connector_registry.get_source(
            source_type="google_sheets_connection",
            source_id=resolved_connection_id,
        )
        if not source or not source.enabled:
            raise ValueError("Connection not found")

        config = dict(source.config_json or {})
        token = str(config.get("access_token") or "").strip() or None
        expires_at = config.get("expires_at")
        refresh_token = config.get("refresh_token")
        oauth_client = GoogleOAuth2Client()

        if token and expires_at:
            try:
                expires_at_float = float(expires_at)
            except (TypeError, ValueError):
                expires_at_float = None
            if expires_at_float and oauth_client.is_token_expired(expires_at_float):
                token = None

        if not token and refresh_token:
            refreshed = await oauth_client.refresh_access_token(str(refresh_token))
            config.update(
                {
                    "access_token": refreshed.get("access_token"),
                    "expires_at": refreshed.get("expires_at"),
                    "refresh_token": refreshed.get("refresh_token", refresh_token),
                }
            )
            await connector_registry.upsert_source(
                source_type=source.source_type,
                source_id=source.source_id,
                enabled=True,
                config_json=config,
            )
            token = str(config.get("access_token") or "").strip() or None

        return token

    @staticmethod
    def _normalize_preview_response(preview: Any) -> GoogleSheetPreviewResponse:
        columns = [str(column) for column in (preview.columns or [])]
        sample_rows = [
            ["" if value is None else str(value) for value in row]
            for row in (preview.sample_rows or [])
            if isinstance(row, list)
        ]
        return GoogleSheetPreviewResponse(
            sheet_id=str(preview.sheet_id),
            sheet_title=str(preview.sheet_title),
            worksheet_title=str(preview.worksheet_title or ""),
            columns=columns,
            sample_rows=sample_rows,
            total_rows=int(preview.total_rows or 0),
            total_columns=int(preview.total_columns or len(columns)),
        )

    async def process_google_sheets_preview(
        self,
        sheet_url: str,
        worksheet_name: Optional[str] = None,
        api_key: Optional[str] = None,
        connection_id: Optional[str] = None,
        infer_types: bool = True,
        include_complex_types: bool = False,
    ) -> TabularPreviewResponse:
        """
        Google Sheets 데이터를 처리하고 타입을 추론합니다.

        Args:
            sheet_url: Google Sheets URL
            worksheet_name: 워크시트 이름
            api_key: Google API 키
            infer_types: 타입 추론 여부
            include_complex_types: 복합 타입 검사 여부

        Returns:
            타입이 추론된 preview 응답
        """
        # 1. Google Sheets connector를 통해 데이터 가져오기
        preview_request = GoogleSheetPreviewRequest(
            sheet_url=sheet_url,
            worksheet_name=worksheet_name,
            api_key=api_key,
            connection_id=connection_id,
        )
        access_token = await self.resolve_optional_access_token(
            connection_id=preview_request.connection_id,
        )
        google_sheets_service = GoogleSheetsService(api_key=preview_request.api_key)
        connector_preview = await google_sheets_service.preview_sheet(
            str(preview_request.sheet_url),
            worksheet_name=preview_request.worksheet_name,
            api_key=preview_request.api_key,
            access_token=access_token,
        )
        sheets_preview = self._normalize_preview_response(connector_preview)

        # 2. 타입 추론 수행
        inferred_schema = None
        risk_summary: List[Any] = []
        if infer_types and sheets_preview.sample_rows:
            analysis_results = self.type_inference_service.analyze_dataset(
                data=sheets_preview.sample_rows,
                columns=sheets_preview.columns,
                include_complex_types=include_complex_types,
            )
            risk_summary, column_risks, column_profiles = assess_dataset_risks(
                sheets_preview.sample_rows,
                sheets_preview.columns,
                analysis_results,
            )
            inferred_schema = _attach_risks_and_profiles(
                analysis_results, column_risks, column_profiles
            )

        # 3. Funnel response 생성
        return TabularPreviewResponse(
            source_metadata={
                "type": "google_sheets",
                "sheet_id": sheets_preview.sheet_id,
                "sheet_title": sheets_preview.sheet_title,
                "worksheet_title": sheets_preview.worksheet_title,
            },
            columns=sheets_preview.columns,
            sample_data=sheets_preview.sample_rows,
            inferred_schema=inferred_schema,
            total_rows=sheets_preview.total_rows,
            preview_rows=len(sheets_preview.sample_rows),
            risk_summary=risk_summary,
        )

    async def analyze_dataset(self, request: DatasetAnalysisRequest) -> DatasetAnalysisResponse:
        """
        데이터셋을 분석하고 타입을 추론합니다.

        Args:
            request: 데이터셋 분석 요청

        Returns:
            분석 결과
        """
        # 타입 추론 수행
        analysis_results = self.type_inference_service.analyze_dataset(
            data=request.data,
            columns=request.columns,
            sample_size=request.sample_size,
            include_complex_types=request.include_complex_types,
        )

        risk_summary, column_risks, column_profiles = assess_dataset_risks(
            request.data, request.columns, analysis_results
        )
        analysis_results = _attach_risks_and_profiles(
            analysis_results, column_risks, column_profiles
        )

        # 메타데이터 생성
        metadata = {
            "total_columns": len(request.columns),
            "analyzed_rows": min(len(request.data), request.sample_size or len(request.data)),
            "include_complex_types": request.include_complex_types,
            "analysis_version": "1.0",
        }

        return DatasetAnalysisResponse(
            columns=analysis_results,
            analysis_metadata=metadata,
            timestamp=datetime.now(timezone.utc),
            risk_summary=risk_summary,
        )

    def generate_schema_suggestion(
        self, analysis_results: List[ColumnAnalysisResult], class_name: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        분석 결과를 기반으로 스키마를 제안합니다.

        Args:
            analysis_results: 컬럼 분석 결과
            class_name: 클래스 이름 (선택사항)

        Returns:
            OMS에서 사용할 수 있는 스키마 제안
        """
        properties = []

        for result in analysis_results:
            # 높은 confidence의 타입만 사용
            if result.inferred_type.confidence >= 0.7:
                data_type = result.inferred_type.type
            else:
                data_type = "xsd:string"  # 기본값

            property_def = {
                "name": normalize_property_name(result.column_name),
                "type": data_type,
                "label": {"ko": result.column_name},
                "description": {
                    "ko": f"{result.inferred_type.reason} (신뢰도: {result.inferred_type.confidence:.0%})"
                },
                # 데이터가 없는 경우(required 판단 불가)는 False로 둠
                "required": result.total_count > 0 and result.null_count == 0,
                "metadata": {
                    "inferred_confidence": result.inferred_type.confidence,
                    "total_count": result.total_count,
                    "non_empty_count": result.non_empty_count,
                    "unique_count": result.unique_count,
                    "null_count": result.null_count,
                    "unique_ratio": result.unique_ratio,
                    "null_ratio": result.null_ratio,
                },
            }

            # 타입별 추가 제약조건
            if (
                data_type == "xsd:string"
                and result.non_empty_count > 0
                and result.unique_count == result.non_empty_count
            ):
                property_def["constraints"] = {"unique": True}
            elif data_type in ["xsd:integer", "xsd:decimal"]:
                # Prefer engine-provided numeric stats when available
                meta = result.inferred_type.metadata or {}
                min_val = meta.get("min")
                max_val = meta.get("max")

                if min_val is None or max_val is None:
                    # Fallback to sample values (limited) when metadata is missing
                    numeric_values = []
                    for val in result.sample_values:
                        try:
                            numeric_values.append(float(val))
                        except (ValueError, TypeError):
                            continue
                    if numeric_values:
                        min_val = min(numeric_values)
                        max_val = max(numeric_values)

                if min_val is not None and max_val is not None:
                    property_def["constraints"] = {"min": min_val, "max": max_val}

            # 복합 타입(예: enum/phone 등)의 추론 메타데이터에서 제약조건 제안이 있으면 반영
            suggested = (result.inferred_type.metadata or {}).get("suggested_constraints")
            if isinstance(suggested, dict) and suggested:
                property_def.setdefault("constraints", {})
                property_def["constraints"].update(suggested)

            properties.append(property_def)

        # 클래스 이름 생성
        if not class_name:
            class_name = "GeneratedClass" + datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")

        return {
            "id": self._generate_class_id(class_name),
            "label": {"ko": class_name},
            "description": {
                "ko": f"Funnel 서비스에서 자동 생성된 스키마 (컬럼 {len(properties)}개)"
            },
            "properties": properties,
            "metadata": {
                "generated_by": "funnel",
                "generation_date": datetime.now(timezone.utc).isoformat(),
                "confidence_scores": {
                    result.column_name: result.inferred_type.confidence
                    for result in analysis_results
                },
            },
        }

    def _normalize_property_name(self, column_name: str) -> str:
        """컬럼 이름을 속성 이름으로 정규화"""
        return normalize_property_name(column_name)

    def _generate_class_id(self, class_name: str) -> str:
        """클래스 ID 생성"""
        # CamelCase로 변환
        parts = class_name.replace("_", " ").replace("-", " ").split()
        return "".join(word.capitalize() for word in parts)


def _attach_risks_and_profiles(
    results: List[ColumnAnalysisResult],
    column_risks: Dict[str, List[Any]],
    column_profiles: Dict[str, Any],
) -> List[ColumnAnalysisResult]:
    enriched: List[ColumnAnalysisResult] = []
    for result in results:
        update: Dict[str, Any] = {}
        if result.column_name in column_profiles:
            update["profile"] = column_profiles[result.column_name]
        if result.column_name in column_risks:
            update["risk_flags"] = column_risks[result.column_name]
        if update:
            enriched.append(_copy_model(result, update))
        else:
            enriched.append(result)
    return enriched


def _copy_model(model: Any, update: Dict[str, Any]) -> Any:
    if hasattr(model, "model_copy"):
        return model.model_copy(update=update)
    return model.copy(update=update)
