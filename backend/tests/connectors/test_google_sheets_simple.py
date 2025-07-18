"""
Google Sheets Connector 간단한 테스트
"""

import pytest
import sys
import os

# 절대 경로로 backend 디렉토리 추가
backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, backend_path)

# 이제 import
from data_connector.google_sheets.utils import (
    extract_sheet_id,
    extract_gid,
    normalize_sheet_data,
    calculate_data_hash
)


def test_extract_sheet_id():
    """Sheet ID 추출 테스트"""
    # 정상적인 URL
    url = "https://docs.google.com/spreadsheets/d/1abc123XYZ/edit#gid=0"
    assert extract_sheet_id(url) == "1abc123XYZ"
    
    # 다양한 형식
    url2 = "https://docs.google.com/spreadsheets/d/1-_aB3c/edit"
    assert extract_sheet_id(url2) == "1-_aB3c"
    
    print("✅ Sheet ID 추출 테스트 통과")


def test_extract_gid():
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
    
    print("✅ GID 추출 테스트 통과")


def test_normalize_sheet_data():
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
    
    print("✅ 데이터 정규화 테스트 통과")


def test_data_hash():
    """데이터 해시 테스트"""
    data1 = [["A", "B"], ["1", "2"]]
    data2 = [["A", "B"], ["1", "2"]]
    data3 = [["A", "B"], ["1", "3"]]
    
    hash1 = calculate_data_hash(data1)
    hash2 = calculate_data_hash(data2)
    hash3 = calculate_data_hash(data3)
    
    # 같은 데이터는 같은 해시
    assert hash1 == hash2
    # 다른 데이터는 다른 해시
    assert hash1 != hash3
    
    print("✅ 데이터 해시 테스트 통과")


if __name__ == "__main__":
    print("🧪 Google Sheets Connector 유틸리티 테스트 시작")
    print(f"📁 Backend 경로: {backend_path}")
    
    try:
        test_extract_sheet_id()
        test_extract_gid()
        test_normalize_sheet_data()
        test_data_hash()
        
        print("\n✅ 모든 테스트 통과!")
    except AssertionError as e:
        print(f"\n❌ 테스트 실패: {e}")
    except Exception as e:
        print(f"\n❌ 예상치 못한 오류: {e}")
        import traceback
        traceback.print_exc()