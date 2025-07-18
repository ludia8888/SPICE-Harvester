"""
Google Sheets 커넥터 빠른 테스트
"""

import httpx
import asyncio
import subprocess
import time
import os
import signal


def start_bff_service():
    """BFF 서비스 시작"""
    print("🚀 BFF 서비스 시작 중...")
    process = subprocess.Popen(
        ["python", "backend-for-frontend/main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/Users/isihyeon/Desktop/SPICE HARVESTER/backend"
    )
    return process


async def wait_for_service(url: str, timeout: int = 10):
    """서비스가 준비될 때까지 대기"""
    print(f"⏳ 서비스 준비 대기 중... (최대 {timeout}초)")
    start_time = time.time()
    
    async with httpx.AsyncClient() as client:
        while time.time() - start_time < timeout:
            try:
                response = await client.get(url)
                if response.status_code in [200, 404]:  # 서비스가 응답하면 OK
                    print("✅ 서비스가 준비되었습니다!")
                    return True
            except:
                pass
            await asyncio.sleep(0.5)
    
    print("❌ 서비스 시작 시간 초과")
    return False


async def test_google_sheets_endpoints():
    """Google Sheets 엔드포인트 테스트"""
    base_url = "http://localhost:8002/api/v1"
    
    async with httpx.AsyncClient() as client:
        # 1. Health Check
        print("\n📋 1. Google Sheets 커넥터 헬스 체크")
        try:
            response = await client.get(f"{base_url}/connectors/google/health")
            print(f"   Status: {response.status_code}")
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ 상태: {data.get('status')}")
                print(f"   서비스: {data.get('service')}")
                print(f"   API Key 설정: {data.get('api_key_configured')}")
            else:
                print(f"   ❌ 응답: {response.text}")
        except Exception as e:
            print(f"   ❌ 오류: {e}")
        
        # 2. Preview Test (with sample public sheet)
        print("\n📋 2. Google Sheet 미리보기 테스트")
        sample_url = "https://docs.google.com/spreadsheets/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/edit"
        
        try:
            response = await client.post(
                f"{base_url}/connectors/google/preview",
                json={"sheet_url": sample_url},
                timeout=30.0
            )
            print(f"   Status: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print(f"   ✅ Sheet ID: {data.get('sheet_id')}")
                print(f"   제목: {data.get('sheet_title')}")
                print(f"   컬럼 수: {data.get('total_columns')}")
                print(f"   행 수: {data.get('total_rows')}")
                
                # 첫 번째 행 데이터 표시
                if data.get('sample_rows'):
                    print(f"   샘플 데이터: {data['sample_rows'][0][:3]}...")
            else:
                print(f"   ❌ 응답: {response.text[:200]}...")
                
        except httpx.TimeoutException:
            print("   ❌ 요청 시간 초과 (30초)")
        except Exception as e:
            print(f"   ❌ 오류: {e}")
        
        # 3. Invalid URL Test
        print("\n📋 3. 잘못된 URL 검증 테스트")
        try:
            response = await client.post(
                f"{base_url}/connectors/google/preview",
                json={"sheet_url": "https://google.com"}
            )
            print(f"   Status: {response.status_code}")
            if response.status_code == 422:
                print("   ✅ 예상대로 유효성 검사 실패")
            else:
                print(f"   ❌ 예상치 못한 응답: {response.text[:100]}...")
        except Exception as e:
            print(f"   ❌ 오류: {e}")


async def main():
    """메인 함수"""
    print("🧪 Google Sheets 커넥터 빠른 테스트")
    print("=" * 50)
    
    # BFF 서비스 시작
    bff_process = start_bff_service()
    
    try:
        # 서비스 준비 대기
        service_ready = await wait_for_service("http://localhost:8002/", timeout=15)
        
        if service_ready:
            # 테스트 실행
            await test_google_sheets_endpoints()
        else:
            print("❌ BFF 서비스를 시작할 수 없습니다.")
            
            # 로그 확인
            if bff_process.poll() is not None:
                stdout, stderr = bff_process.communicate()
                print("\n📋 BFF 오류 로그:")
                print(stderr.decode('utf-8')[:500])
    
    finally:
        # BFF 서비스 종료
        print("\n🛑 BFF 서비스 종료 중...")
        try:
            bff_process.terminate()
            bff_process.wait(timeout=5)
        except:
            bff_process.kill()
        
        print("✅ 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main())