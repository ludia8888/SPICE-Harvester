"""
🔥 THINK ULTRA! Funnel HTTP Type Inference Service Adapter
HTTP 기반 Funnel 마이크로서비스를 TypeInferenceInterface로 adapting
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional

from shared.interfaces.type_inference import ColumnAnalysisResult as InterfaceColumnResult
from shared.interfaces.type_inference import TypeInferenceInterface
from shared.interfaces.type_inference import TypeInferenceResult as InterfaceTypeResult

from bff.services.funnel_client import FunnelClient

logger = logging.getLogger(__name__)


class FunnelHTTPTypeInferenceAdapter(TypeInferenceInterface):
    """
    HTTP 기반 Funnel 마이크로서비스를 TypeInferenceInterface로 adapting하는 클래스.

    FunnelClient를 사용하여 Funnel 서비스와 HTTP 통신하며,
    결과를 TypeInferenceInterface 형식으로 변환합니다.
    """

    def __init__(self, funnel_client: FunnelClient = None):
        self.funnel_client = funnel_client or FunnelClient()

    def infer_column_type(
        self,
        column_data: List[Any],
        column_name: Optional[str] = None,
        include_complex_types: bool = False,
    ) -> InterfaceColumnResult:
        """
        단일 컬럼 데이터의 타입을 추론합니다.
        """
        # HTTP 기반 분석을 위해 데이터를 dataset 형태로 변환
        data = [[value] for value in column_data]
        headers = [column_name or "column_0"]

        # 동기 함수에서 비동기 호출
        result = asyncio.run(self._analyze_single_column(data, headers, include_complex_types))

        if result and len(result) > 0:
            return result[0]
        else:
            # 기본값 반환
            return InterfaceColumnResult(
                column_name=column_name or "column_0",
                inferred_type=InterfaceTypeResult(
                    type="xsd:string", confidence=0.5, reason="Default type inference failed"
                ),
                non_empty_count=len([v for v in column_data if v is not None]),
                unique_count=len(set(column_data)),
                null_count=len([v for v in column_data if v is None]),
                sample_values=column_data[:10],
                complex_type_info=None,
            )

    def analyze_dataset(
        self,
        data: List[List[Any]],
        headers: Optional[List[str]] = None,
        include_complex_types: bool = False,
        sample_size: Optional[int] = None,
    ) -> Dict[str, InterfaceColumnResult]:
        """
        전체 데이터셋을 분석하여 모든 컬럼의 타입을 추론합니다.
        """
        if headers is None:
            headers = [f"column_{i}" for i in range(len(data[0]) if data else 0)]

        # 동기 함수에서 비동기 호출
        results = asyncio.run(
            self._analyze_dataset_async(data, headers, include_complex_types, sample_size)
        )

        # 결과를 딕셔너리로 변환
        converted_results = {}
        for result in results:
            converted_results[result.column_name] = result

        return converted_results

    def infer_type_with_confidence(
        self, values: List[Any], check_complex: bool = False
    ) -> InterfaceTypeResult:
        """
        값 리스트에서 타입을 추론하고 신뢰도를 반환합니다.
        """
        column_result = self.infer_column_type(
            values, column_name="temp", include_complex_types=check_complex
        )

        return column_result.inferred_type

    def infer_single_value_type(
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
        # 단일 값을 column_data로 변환하여 기존 메서드 사용
        column_result = self.infer_column_type(
            [value], 
            column_name="single_value",
            include_complex_types=True
        )
        
        return column_result.inferred_type

    async def _analyze_single_column(
        self, data: List[List[Any]], headers: List[str], include_complex_types: bool = False
    ) -> List[InterfaceColumnResult]:
        """
        단일 컬럼 분석을 위한 비동기 헬퍼 메서드
        """
        try:
            request_data = {
                "data": data,
                "columns": headers,
                "sample_size": len(data),
                "include_complex_types": include_complex_types,
            }

            response = await self.funnel_client.analyze_dataset(request_data)

            # Funnel 응답을 Interface 형식으로 변환
            return [self._convert_funnel_column_result(col) for col in response.get("columns", [])]

        except Exception as e:
            logger.error(f"Funnel 서비스 분석 실패: {e}")
            # 기본값 반환
            return []

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
        try:
            request_data = {
                "data": data,
                "columns": headers,
                "sample_size": sample_size or 1000,
                "include_complex_types": include_complex_types,
            }

            response = await self.funnel_client.analyze_dataset(request_data)

            # Funnel 응답을 Interface 형식으로 변환
            return [self._convert_funnel_column_result(col) for col in response.get("columns", [])]

        except Exception as e:
            logger.error(f"Funnel 서비스 분석 실패: {e}")
            # 기본값 반환
            return []

    def _convert_funnel_column_result(self, funnel_result: Dict[str, Any]) -> InterfaceColumnResult:
        """
        Funnel 서비스 응답을 Interface 형식으로 변환
        """
        inferred_type = funnel_result.get("inferred_type", {})

        # TypeInferenceResult 변환
        type_result = InterfaceTypeResult(
            type=inferred_type.get("type", "xsd:string"),
            confidence=inferred_type.get("confidence", 0.5),
            reason=inferred_type.get("reason", "No reason provided"),
        )

        # ColumnAnalysisResult 변환
        return InterfaceColumnResult(
            column_name=funnel_result.get("column_name", "unknown"),
            inferred_type=type_result,
            non_empty_count=funnel_result.get("unique_count", 0)
            + funnel_result.get("null_count", 0)
            - funnel_result.get("null_count", 0),
            unique_count=funnel_result.get("unique_count", 0),
            null_count=funnel_result.get("null_count", 0),
            sample_values=funnel_result.get("sample_values", []),
            complex_type_info=inferred_type.get("metadata", None),
        )

    async def close(self):
        """
        클라이언트 연결 종료
        """
        if self.funnel_client:
            await self.funnel_client.close()

    def __del__(self):
        """
        소멸자에서 클라이언트 정리
        """
        try:
            asyncio.run(self.close())
        except Exception:
            pass  # 소멸자에서는 예외를 무시
