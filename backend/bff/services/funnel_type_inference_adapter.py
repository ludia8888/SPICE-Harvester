"""Type inference adapter backed by the in-process Funnel runtime."""

import logging
from typing import Any, Dict, List, Optional

from shared.interfaces.type_inference import ColumnAnalysisResult as InterfaceColumnResult
from shared.interfaces.type_inference import TypeInferenceInterface
from shared.interfaces.type_inference import TypeInferenceResult as InterfaceTypeResult

from bff.services.funnel_client import FunnelClient

logger = logging.getLogger(__name__)


class InProcessTypeInferenceAdapter(TypeInferenceInterface):
    """In-process Funnel 런타임을 TypeInferenceInterface로 어댑트한다."""

    def __init__(self, funnel_client: FunnelClient = None):
        self.funnel_client = funnel_client or FunnelClient()

    async def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
        context_columns: Optional[Dict[str, List[Any]]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> InterfaceColumnResult:
        """
        단일 컬럼 데이터의 타입을 추론합니다.
        """
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)

        # 분석 요청을 dataset 형태로 구성한다.
        data = [[value] for value in column_data]
        headers = [column_name or "column_0"]

        result = await self._analyze_single_column(data, headers, include_complex_types)

        if result and len(result) > 0:
            return result[0]
        else:
            # 기본값 반환
            total_count = len(column_data)
            non_empty_values = [v for v in column_data if v is not None and str(v).strip() != ""]
            non_empty_count = len(non_empty_values)
            null_count = total_count - non_empty_count
            unique_count = len(set(str(v) for v in non_empty_values))
            null_ratio = null_count / total_count if total_count > 0 else 0.0
            unique_ratio = unique_count / non_empty_count if non_empty_count > 0 else 0.0

            return InterfaceColumnResult(
                column_name=column_name or "column_0",
                inferred_type=InterfaceTypeResult(
                    type="xsd:string", confidence=0.5, reason="Default type inference failed"
                ),
                total_count=total_count,
                non_empty_count=non_empty_count,
                sample_values=non_empty_values[:10],
                null_count=null_count,
                unique_count=unique_count,
                null_ratio=null_ratio,
                unique_ratio=unique_ratio,
            )

    async def analyze_dataset(
        self,
        data: List[List[Any]],
        columns: List[str],
        sample_size: int = 1000,
        include_complex_types: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> List[InterfaceColumnResult]:
        """
        전체 데이터셋을 분석하여 모든 컬럼의 타입을 추론합니다.
        """
        if metadata:
            include_complex_types = metadata.get("include_complex_types", include_complex_types)
            sample_size = metadata.get("sample_size", sample_size)

        return await self._analyze_dataset_async(data, columns, include_complex_types, sample_size)

    async def infer_type_with_confidence(
        self, values: List[Any], check_complex: bool = False
    ) -> InterfaceTypeResult:
        """
        값 리스트에서 타입을 추론하고 신뢰도를 반환합니다.
        """
        column_result = await self.infer_column_type(
            values, column_name="temp", include_complex_types=check_complex
        )

        return column_result.inferred_type

    async def infer_single_value_type(
        self, value: Any, context: Optional[Dict[str, Any]] = None
    ) -> InterfaceTypeResult:
        """
        단일 값의 타입을 추론합니다.
        
        Args:
            value: 분석할 값
            context: 추론을 위한 선택적 컨텍스트
            
        Returns:
            타입 추론 결과 (타입, 신뢰도 포함)
        """
        analysis = await self.infer_column_type(
            [value],
            column_name=context.get("column_name") if context else "single_value",
            include_complex_types=context.get("include_complex_types", True) if context else True,
            metadata=context,
        )
        return analysis.inferred_type

    async def _analyze_single_column(
        self, data: List[List[Any]], headers: List[str], include_complex_types: bool = False
    ) -> List[InterfaceColumnResult]:
        """
        단일 컬럼 분석을 위한 비동기 헬퍼 메서드
        """
        return await self._analyze_with_fallback(
            data=data,
            headers=headers,
            include_complex_types=include_complex_types,
            sample_size=len(data),
        )

    async def _analyze_dataset_async(
        self,
        data: List[List[Any]],
        headers: List[str],
        include_complex_types: bool = False,
        sample_size: Optional[int] = None,
    ) -> List[InterfaceColumnResult]:
        """
        데이터셋 분석을 위한 비동기 헬퍼 메서드
        """
        return await self._analyze_with_fallback(
            data=data,
            headers=headers,
            include_complex_types=include_complex_types,
            sample_size=sample_size or 1000,
        )

    @staticmethod
    def _build_analysis_request(
        *,
        data: List[List[Any]],
        headers: List[str],
        include_complex_types: bool,
        sample_size: int,
    ) -> Dict[str, Any]:
        return {
            "data": data,
            "columns": headers,
            "sample_size": sample_size,
            "include_complex_types": include_complex_types,
        }

    async def _analyze_with_fallback(
        self,
        *,
        data: List[List[Any]],
        headers: List[str],
        include_complex_types: bool,
        sample_size: int,
    ) -> List[InterfaceColumnResult]:
        try:
            from shared.config.settings import get_settings

            request_data = self._build_analysis_request(
                data=data,
                headers=headers,
                sample_size=sample_size,
                include_complex_types=include_complex_types,
            )

            settings = get_settings()
            response = await self.funnel_client.analyze_dataset(
                request_data,
                timeout_seconds=float(settings.services.funnel_infer_timeout_seconds),
            )

            return [self._convert_funnel_column_result(col) for col in response.get("columns", [])]
        except Exception as e:
            logger.error(f"Funnel 서비스 분석 실패: {e}")
            from shared.services.pipeline.pipeline_funnel_fallback import build_tabular_analysis_fallback

            fallback = build_tabular_analysis_fallback(
                columns=headers,
                rows=data,
                include_complex_types=include_complex_types,
                error=str(e),
                stage="bff",
            )
            return [InterfaceColumnResult(**col) for col in (fallback.get("columns") or [])]

    def _convert_funnel_column_result(self, funnel_result: Dict[str, Any]) -> InterfaceColumnResult:
        """
        Funnel 서비스 응답을 Interface 형식으로 변환
        """
        # Funnel 응답은 shared.models.type_inference.ColumnAnalysisResult와 호환되는 형태를 반환
        # (새 필드가 추가되어도 Pydantic이 기본값/ignore로 처리)
        inferred_type = funnel_result.get("inferred_type") or {}
        funnel_result["inferred_type"] = InterfaceTypeResult(
            type=inferred_type.get("type", "xsd:string"),
            confidence=inferred_type.get("confidence", 0.5),
            reason=inferred_type.get("reason", "No reason provided"),
            metadata=inferred_type.get("metadata"),
        )

        return InterfaceColumnResult(**funnel_result)

    async def close(self):
        """
        클라이언트 연결 종료
        """
        if self.funnel_client:
            await self.funnel_client.close()
