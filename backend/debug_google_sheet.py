"""
Google Sheet 접근 문제 상세 디버깅
"""

import httpx
import asyncio
import json
import sys
import os

# Add backend to path
backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '.'))
sys.path.insert(0, backend_path)

from data_connector.google_sheets.utils import extract_sheet_id, extract_gid


async def direct_api_test(sheet_id: str):
    """Google Sheets API 직접 호출 테스트"""
    print("\n🔍 Google Sheets API 직접 호출 테스트")
    print("=" * 50)
    
    # API endpoints
    metadata_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
    data_url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}/values/Sheet1"
    
    async with httpx.AsyncClient() as client:
        # 1. 메타데이터 조회 (API 키 없이)
        print(f"\n1️⃣ 메타데이터 조회 (API 키 없이)")
        print(f"   URL: {metadata_url}")
        
        try:
            response = await client.get(metadata_url)
            print(f"   상태 코드: {response.status_code}")
            
            if response.status_code == 403:
                error_data = response.json()
                print(f"   ❌ 접근 거부")
                print(f"   오류: {json.dumps(error_data, indent=2, ensure_ascii=False)}")
                
                # 오류 상세 분석
                if 'error' in error_data:
                    error = error_data['error']
                    print(f"\n   📋 오류 분석:")
                    print(f"   - 코드: {error.get('code')}")
                    print(f"   - 메시지: {error.get('message')}")
                    print(f"   - 상태: {error.get('status')}")
                    
                    # 에러가 API 키 관련인지 확인
                    if "API key" in error.get('message', ''):
                        print(f"\n   💡 API 키가 필요합니다!")
                        return False
                    elif "permission" in error.get('message', '').lower():
                        print(f"\n   💡 시트 권한 문제입니다!")
                        return False
                        
            elif response.status_code == 200:
                print(f"   ✅ 성공!")
                data = response.json()
                print(f"   시트 제목: {data.get('properties', {}).get('title')}")
                return True
                
        except Exception as e:
            print(f"   ❌ 오류: {e}")
            return False
        
        # 2. 데이터 조회 시도
        print(f"\n2️⃣ 데이터 조회 (API 키 없이)")
        print(f"   URL: {data_url}")
        
        try:
            response = await client.get(data_url)
            print(f"   상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                print(f"   ✅ 데이터 접근 성공!")
                data = response.json()
                values = data.get('values', [])
                if values:
                    print(f"   행 수: {len(values)}")
                    print(f"   첫 번째 행: {values[0][:5]}...")
                return True
            else:
                print(f"   ❌ 데이터 접근 실패")
                
        except Exception as e:
            print(f"   ❌ 오류: {e}")
            
    return False


async def test_with_bff_service(sheet_url: str):
    """BFF 서비스를 통한 테스트"""
    print("\n🔧 BFF 서비스를 통한 테스트")
    print("=" * 50)
    
    async with httpx.AsyncClient() as client:
        # 상세 로깅을 위한 헤더
        headers = {
            "Content-Type": "application/json",
            "X-Debug": "true"
        }
        
        try:
            print(f"   URL: {sheet_url}")
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/preview",
                json={"sheet_url": sheet_url},
                headers=headers,
                timeout=30.0
            )
            
            print(f"   상태 코드: {response.status_code}")
            print(f"   응답 헤더: {dict(response.headers)}")
            
            if response.status_code != 200:
                print(f"\n   ❌ 오류 응답:")
                print(f"   {json.dumps(response.json(), indent=2, ensure_ascii=False)}")
            else:
                print(f"\n   ✅ 성공!")
                data = response.json()
                print(f"   시트 제목: {data.get('sheet_title')}")
                print(f"   컬럼 수: {data.get('total_columns')}")
                
        except Exception as e:
            print(f"   ❌ 예외 발생: {type(e).__name__}: {e}")


async def analyze_service_code():
    """서비스 코드 문제 분석"""
    print("\n🔍 서비스 코드 분석")
    print("=" * 50)
    
    # 서비스 파일 읽기
    service_path = os.path.join(backend_path, "connectors/google_sheets/service.py")
    
    print(f"1. service.py의 오류 처리 부분 확인")
    
    # 실제 오류가 발생하는 부분 찾기
    with open(service_path, 'r') as f:
        content = f.read()
        
    # _get_sheet_metadata 메서드 찾기
    if "_get_sheet_metadata" in content:
        print("   ✅ _get_sheet_metadata 메서드 존재")
        
        # 403 에러 처리 부분 찾기
        if "403" in content and "Cannot access the Google Sheet" in content:
            print("   ✅ 403 에러 처리 로직 존재")
            print("\n   💡 문제 분석:")
            print("   - 서비스가 403 에러를 받으면 즉시 'Cannot access' 메시지 반환")
            print("   - 실제로는 API 키가 필요한 경우일 수 있음")
            print("   - 공개 시트라도 API 키 없이는 접근 불가능할 수 있음")


async def test_public_sample_sheet():
    """공개된 샘플 시트로 테스트"""
    print("\n📊 공개 샘플 시트 테스트")
    print("=" * 50)
    
    # Google의 공식 샘플 시트
    sample_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
    
    print(f"샘플 URL: {sample_url}")
    
    await test_with_bff_service(sample_url)


async def main():
    """메인 디버깅 함수"""
    print("🔥 THINK ULTRA - Google Sheets 접근 문제 심층 분석")
    print("=" * 70)
    
    # 사용자 시트 정보
    user_sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?usp=sharing"
    sheet_id = extract_sheet_id(user_sheet_url)
    
    print(f"📋 사용자 시트 정보:")
    print(f"   URL: {user_sheet_url}")
    print(f"   Sheet ID: {sheet_id}")
    print(f"   공유 파라미터: usp=sharing ✅")
    
    # 1. 직접 API 테스트
    api_accessible = await direct_api_test(sheet_id)
    
    # 2. BFF 서비스 테스트
    if not api_accessible:
        print("\n⚠️  직접 API 호출도 실패했습니다.")
        print("💡 해결 방안:")
        print("   1. Google API Key 설정이 필요할 수 있습니다")
        print("   2. 시트가 '링크가 있는 모든 사용자에게 공개'로 설정되어야 합니다")
        
        # API 키 설정 확인
        print(f"\n📌 현재 API 키 설정: {bool(os.getenv('GOOGLE_API_KEY'))}")
        
    # 3. 서비스 테스트
    await test_with_bff_service(user_sheet_url)
    
    # 4. 코드 분석
    await analyze_service_code()
    
    # 5. 공개 샘플 시트 테스트
    await test_public_sample_sheet()
    
    print("\n" + "=" * 70)
    print("📊 분석 결과:")
    print("\n1. Google Sheets API는 공개 시트라도 API 키가 필요할 수 있습니다")
    print("2. 현재 서비스는 403 에러를 모두 '권한 없음'으로 처리합니다")
    print("3. API 키 설정이 필요합니다:")
    print("\n   export GOOGLE_API_KEY='your-api-key-here'")
    print("\n4. Google Cloud Console에서:")
    print("   - Google Sheets API 활성화")
    print("   - API 키 생성")
    print("   - API 키에 Google Sheets API 권한 부여")


if __name__ == "__main__":
    asyncio.run(main())