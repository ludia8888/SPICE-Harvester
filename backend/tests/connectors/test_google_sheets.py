"""
Google Sheets Connector 테스트
"""

import pytest
import httpx
from datetime import datetime
from unittest.mock import Mock, patch, AsyncMock

# No need for sys.path.insert - using proper spice_harvester package imports
from data_connector.google_sheets.service import GoogleSheetsService
from data_connector.google_sheets.models import (
    GoogleSheetPreviewRequest,
    GoogleSheetRegisterRequest,
    GoogleSheetPreviewResponse,
    GoogleSheetRegisterResponse
)
from data_connector.google_sheets.utils import (
    extract_sheet_id,
    extract_gid,
    normalize_sheet_data,
    calculate_data_hash
)

class TestGoogleSheetsUtils:
    """유틸리티 함수 테스트"""
    
    def test_extract_sheet_id(self):
        """Sheet ID 추출 테스트"""
        # 정상적인 URL
        url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
        assert extract_sheet_id(url) == "1abc123XYZ"
        
        # 다양한 형식
        url2 = "https://docs.google.com/spreadsheets/d/1-_aB3c/edit"
        assert extract_sheet_id(url2) == "1-_aB3c"
        
        # 잘못된 URL
        with pytest.raises(ValueError):
            extract_sheet_id("https://google.com")
    
    def test_extract_gid(self):
        """GID 추출 테스트"""
        # Fragment에서 추출
        url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=123"
        assert extract_gid(url) == "123"
        
        # Query parameter에서 추출
        url2 = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit?gid=456"
        assert extract_gid(url2) == "456"
        
        # GID 없음
        url3 = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit"
        assert extract_gid(url3) is None
    
    def test_normalize_sheet_data(self):
        """시트 데이터 정규화 테스트"""
        # 정상 데이터
        raw_data = [
            ["Name", "Age", "City"],
            ["Alice", 25, "Seoul"],
            ["Bob", 30, "Busan"],
            ["Charlie", 35]  # 불완전한 행
        ]
        
        columns, rows = normalize_sheet_data(raw_data)
        
        assert columns == ["Name", "Age", "City"]
        assert len(rows) == 3
        assert rows[0] == ["Alice", "25", "Seoul"]
        assert rows[2] == ["Charlie", "35", ""]  # 빈 셀 채움
        
        # 빈 데이터
        columns2, rows2 = normalize_sheet_data([])
        assert columns2 == []
        assert rows2 == []

class TestGoogleSheetsService:
    """Google Sheets 서비스 테스트"""
    
    @pytest.fixture
    def service(self):
        """서비스 인스턴스"""
        return GoogleSheetsService(api_key="test_api_key")
    
    @pytest.fixture
    def mock_sheet_metadata(self):
        """모의 시트 메타데이터"""
        return {
            "properties": {
                "title": "테스트 스프레드시트",
                "locale": "ko_KR",
                "timeZone": "Asia/Seoul"
            },
            "sheets": [
                {
                    "properties": {
                        "sheetId": 0,
                        "title": "Sheet1",
                        "index": 0
                    }
                }
            ]
        }
    
    @pytest.fixture
    def mock_sheet_data(self):
        """모의 시트 데이터"""
        return {
            "values": [
                ["상품명", "카테고리", "가격", "재고"],
                ["티셔츠", "상의", "19000", "45"],
                ["바지", "하의", "29000", "21"],
                ["신발", "잡화", "49000", "12"]
            ]
        }
    
    @pytest.mark.asyncio
    async def test_preview_sheet_success(self, service, mock_sheet_metadata, mock_sheet_data):
        """시트 미리보기 성공 테스트"""
        # HTTP 응답 모의
        with patch.object(service, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_get_client.return_value = mock_client
            
            # 메타데이터 응답
            mock_metadata_response = AsyncMock()
            mock_metadata_response.json.return_value = mock_sheet_metadata
            mock_metadata_response.raise_for_status = Mock()
            
            # 데이터 응답
            mock_data_response = AsyncMock()
            mock_data_response.json.return_value = mock_sheet_data
            mock_data_response.raise_for_status = Mock()
            
            # 응답 순서 설정
            mock_client.get.side_effect = [mock_metadata_response, mock_data_response]
            
            # 테스트 실행
            url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
            result = await service.preview_sheet(url)
            
            # 검증
            assert isinstance(result, GoogleSheetPreviewResponse)
            assert result.sheet_id == "1abc123XYZ"
            assert result.sheet_title == "테스트 스프레드시트"
            assert result.columns == ["상품명", "카테고리", "가격", "재고"]
            assert len(result.sample_rows) == 3
            assert result.total_rows == 3
            assert result.total_columns == 4
    
    @pytest.mark.asyncio
    async def test_preview_sheet_access_denied(self, service):
        """접근 거부 시트 테스트"""
        with patch.object(service, '_get_client') as mock_get_client:
            mock_client = AsyncMock()
            mock_get_client.return_value = mock_client
            
            # 403 에러 응답
            mock_error_response = Mock()
            mock_error_response.status_code = 403
            mock_error = httpx.HTTPStatusError("Forbidden", request=Mock(), response=mock_error_response)
            
            mock_client.get.side_effect = mock_error
            
            # 테스트 실행
            url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
            
            with pytest.raises(ValueError, match="Cannot access"):
                await service.preview_sheet(url)
    
    @pytest.mark.asyncio
    async def test_register_sheet(self, service, mock_sheet_metadata):
        """시트 등록 테스트"""
        with patch.object(service, '_get_sheet_metadata') as mock_get_metadata:
            mock_get_metadata.return_value = AsyncMock(
                title="테스트 스프레드시트",
                sheets=[{"properties": {"title": "Sheet1"}}]
            )
            
            # 폴링 태스크 모의
            with patch('asyncio.create_task'):
                url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit"
                result = await service.register_sheet(url, polling_interval=300)
                
                # 검증
                assert isinstance(result, GoogleSheetRegisterResponse)
                assert result.sheet_id == "1abc123XYZ"
                assert result.worksheet_name == "Sheet1"
                assert result.polling_interval == 300
                assert result.status == "registered"
    
    def test_data_hash_calculation(self):
        """데이터 해시 계산 테스트"""
        data1 = [["A", "B"], ["1", "2"]]
        data2 = [["A", "B"], ["1", "2"]]
        data3 = [["A", "B"], ["1", "3"]]  # 다른 데이터
        
        hash1 = calculate_data_hash(data1)
        hash2 = calculate_data_hash(data2)
        hash3 = calculate_data_hash(data3)
        
        # 같은 데이터는 같은 해시
        assert hash1 == hash2
        # 다른 데이터는 다른 해시
        assert hash1 != hash3

class TestGoogleSheetsRouter:
    """API 라우터 테스트"""
    
    @pytest.fixture
    def client(self):
        """테스트 클라이언트"""
        from fastapi.testclient import TestClient
        from data_connector.google_sheets.router import router
        from fastapi import FastAPI
        
        app = FastAPI()
        app.include_router(router)
        
        return TestClient(app)
    
    def test_health_check(self, client):
        """헬스 체크 엔드포인트 테스트"""
        response = client.get("/connectors/google/health")
        assert response.status_code == 200
        
        data = response.json()
        assert data["status"] == "healthy"
        assert data["service"] == "google_sheets_connector"
    
    @pytest.mark.asyncio
    async def test_preview_endpoint(self, client):
        """미리보기 엔드포인트 테스트"""
        # 서비스 모의
        with patch('connectors.google_sheets.router.sheets_service') as mock_service:
            mock_preview_response = GoogleSheetPreviewResponse(
                sheet_id="1abc123XYZ",
                sheet_title="테스트",
                worksheet_title="Sheet1",
                columns=["A", "B"],
                sample_rows=[["1", "2"]],
                total_rows=1,
                total_columns=2
            )
            
            mock_service.preview_sheet = AsyncMock(return_value=mock_preview_response)
            
            # 요청
            response = client.post(
                "/connectors/google/preview",
                json={"sheet_url": "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit"}
            )
            
            # 검증
            assert response.status_code == 200
            data = response.json()
            assert data["sheet_id"] == "1abc123XYZ"
            assert data["sheet_title"] == "테스트"