"""
Funnel Service 클라이언트
BFF에서 Funnel 마이크로서비스와 통신하기 위한 HTTP 클라이언트
"""

import logging
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

        self.client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=30.0,
            headers={"Content-Type": "application/json", "Accept": "application/json"},
            verify=ssl_config.get("verify", True),
        )

        logger.info(f"Funnel Client initialized with base URL: {self.base_url}")

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
            # 1. Google Sheets 미리보기 및 타입 추론
            preview_result = await self.preview_google_sheets(
                sheet_url=sheet_url,
                worksheet_name=worksheet_name,
                api_key=api_key,
                infer_types=True,
                include_complex_types=True,
            )

            # 2. 추론된 스키마를 기반으로 제안 생성
            if preview_result.get("inferred_schema"):
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
            else:
                raise ValueError("타입 추론에 실패했습니다.")

        except Exception as e:
            logger.error(f"Google Sheets 스키마 생성 실패: {e}")
            raise

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()
