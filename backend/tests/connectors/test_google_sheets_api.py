"""
Google Sheets Connector API 통합 테스트
"""

import httpx
import asyncio
import json
import sys
import os

# Add backend to path
backend_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.insert(0, backend_path)


async def test_health_check():
    """헬스 체크 엔드포인트 테스트"""
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get("http://localhost:8002/api/v1/connectors/google/health")
            
            print(f"📊 Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health Check 성공!")
                print(f"   - Status: {data.get('status')}")
                print(f"   - Service: {data.get('service')}")
                print(f"   - API Key 설정: {data.get('api_key_configured')}")
                return True
            else:
                print(f"❌ Health Check 실패: {response.status_code}")
                return False
                
        except httpx.ConnectError:
            print("❌ BFF 서비스에 연결할 수 없습니다. 서비스가 실행 중인지 확인하세요.")
            print("   실행 명령: cd backend && python backend-for-frontend/main.py")
            return False
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            return False


async def test_preview_public_sheet():
    """공개 Google Sheet 미리보기 테스트"""
    # 테스트용 공개 Google Sheet URL (예시)
    test_sheet_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
    
    async with httpx.AsyncClient() as client:
        try:
            payload = {
                "sheet_url": test_sheet_url
            }
            
            print(f"\n📋 Google Sheet 미리보기 테스트")
            print(f"   URL: {test_sheet_url}")
            
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/preview",
                json=payload,
                timeout=30.0
            )
            
            print(f"📊 Status Code: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ 미리보기 성공!")
                print(f"   - Sheet ID: {data.get('sheet_id')}")
                print(f"   - Title: {data.get('sheet_title')}")
                print(f"   - Columns: {data.get('columns', [])[:5]}...")  # 처음 5개만
                print(f"   - Total Rows: {data.get('total_rows')}")
                print(f"   - Total Columns: {data.get('total_columns')}")
                
                # 샘플 데이터 출력
                sample_rows = data.get('sample_rows', [])
                if sample_rows:
                    print(f"   - Sample Data (first 3 rows):")
                    for i, row in enumerate(sample_rows[:3]):
                        print(f"     Row {i+1}: {row[:5]}...")  # 처음 5개 컬럼만
                
                return True
                
            elif response.status_code == 403:
                print("❌ 접근 거부: Google Sheet가 공개되지 않았습니다.")
                print("   Sheet를 'Anyone with the link can view'로 설정하세요.")
                return False
            else:
                print(f"❌ 미리보기 실패: {response.status_code}")
                print(f"   응답: {response.text}")
                return False
                
        except httpx.ConnectError:
            print("❌ BFF 서비스에 연결할 수 없습니다.")
            return False
        except httpx.TimeoutException:
            print("❌ 요청 시간 초과 (30초)")
            return False
        except Exception as e:
            print(f"❌ 예상치 못한 오류: {e}")
            import traceback
            traceback.print_exc()
            return False


async def test_invalid_url():
    """잘못된 URL 테스트"""
    invalid_urls = [
        "https://google.com",
        "https://docs.google.com/document/d/123/edit",
        "not-a-url"
    ]
    
    async with httpx.AsyncClient() as client:
        print(f"\n🧪 잘못된 URL 테스트")
        
        for url in invalid_urls:
            try:
                response = await client.post(
                    "http://localhost:8002/api/v1/connectors/google/preview",
                    json={"sheet_url": url}
                )
                
                if response.status_code == 422:  # Validation error
                    print(f"✅ 예상대로 거부됨: {url}")
                else:
                    print(f"❌ 예상치 못한 응답 ({response.status_code}): {url}")
                    
            except Exception as e:
                print(f"❌ 오류 발생: {e}")


async def test_register_sheet():
    """시트 등록 테스트"""
    test_sheet_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
    
    async with httpx.AsyncClient() as client:
        try:
            print(f"\n📝 Google Sheet 등록 테스트")
            
            payload = {
                "sheet_url": test_sheet_url,
                "worksheet_name": "Sheet1",
                "polling_interval": 60  # 1분
            }
            
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/register",
                json=payload
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ 등록 성공!")
                print(f"   - Sheet ID: {data.get('sheet_id')}")
                print(f"   - Worksheet: {data.get('worksheet_name')}")
                print(f"   - Polling Interval: {data.get('polling_interval')}초")
                print(f"   - Registered At: {data.get('registered_at')}")
                
                # 등록된 시트 목록 확인
                list_response = await client.get(
                    "http://localhost:8002/api/v1/connectors/google/registered"
                )
                
                if list_response.status_code == 200:
                    sheets = list_response.json()
                    print(f"\n📋 등록된 시트 수: {len(sheets)}")
                
                return data.get('sheet_id')
                
            else:
                print(f"❌ 등록 실패: {response.status_code}")
                return None
                
        except Exception as e:
            print(f"❌ 오류 발생: {e}")
            return None


async def test_unregister_sheet(sheet_id: str):
    """시트 등록 해제 테스트"""
    if not sheet_id:
        print("\n⚠️  등록 해제할 sheet_id가 없습니다.")
        return
    
    async with httpx.AsyncClient() as client:
        try:
            print(f"\n🗑️  Google Sheet 등록 해제 테스트")
            print(f"   Sheet ID: {sheet_id}")
            
            response = await client.delete(
                f"http://localhost:8002/api/v1/connectors/google/register/{sheet_id}"
            )
            
            if response.status_code == 200:
                data = response.json()
                print(f"✅ 등록 해제 성공!")
                print(f"   - Message: {data.get('message')}")
            else:
                print(f"❌ 등록 해제 실패: {response.status_code}")
                
        except Exception as e:
            print(f"❌ 오류 발생: {e}")


async def main():
    """모든 테스트 실행"""
    print("🧪 Google Sheets Connector API 통합 테스트")
    print("=" * 50)
    
    # 1. 헬스 체크
    health_ok = await test_health_check()
    if not health_ok:
        print("\n⚠️  BFF 서비스가 실행되지 않았습니다. 테스트를 중단합니다.")
        print("실행 방법:")
        print("  cd /Users/isihyeon/Desktop/SPICE\\ HARVESTER/backend")
        print("  python backend-for-frontend/main.py")
        return
    
    # 2. 공개 시트 미리보기
    await test_preview_public_sheet()
    
    # 3. 잘못된 URL 테스트
    await test_invalid_url()
    
    # 4. 시트 등록/해제 테스트
    sheet_id = await test_register_sheet()
    if sheet_id:
        await asyncio.sleep(1)  # 잠시 대기
        await test_unregister_sheet(sheet_id)
    
    print("\n" + "=" * 50)
    print("✅ 모든 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main())