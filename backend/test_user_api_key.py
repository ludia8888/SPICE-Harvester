"""
사용자 API 키로 Google Sheets 테스트
"""

import os
import httpx
import asyncio
import subprocess
import time
import json
from datetime import datetime

# 사용자가 제공한 API 키
USER_API_KEY = "AIzaSyAVB9eZPQd57rP3Ta_Uesz-vEptjI0Zj2U"


async def test_api_key_validity():
    """API 키 유효성 테스트"""
    print("🔑 API 키 유효성 테스트")
    print("=" * 50)
    
    # Google 공식 샘플 시트로 테스트
    test_sheet_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{test_sheet_id}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params={"key": USER_API_KEY})
        
        if response.status_code == 200:
            print("✅ API 키가 유효합니다!")
            data = response.json()
            print(f"   테스트 시트: {data.get('properties', {}).get('title')}")
            return True
        else:
            print(f"❌ API 키 오류: {response.status_code}")
            print(f"   응답: {response.json()}")
            return False


async def start_bff_with_api_key():
    """API 키가 설정된 상태로 BFF 서비스 시작"""
    print("\n🚀 BFF 서비스 시작")
    print("=" * 50)
    
    # 환경 변수 설정
    env = os.environ.copy()
    env["GOOGLE_API_KEY"] = USER_API_KEY
    
    # 서비스 시작
    process = subprocess.Popen(
        ["python", "backend-for-frontend/main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=env,
        cwd="/Users/isihyeon/Desktop/SPICE HARVESTER/backend"
    )
    
    print(f"✅ BFF 서비스 시작됨 (PID: {process.pid})")
    print(f"   GOOGLE_API_KEY 설정됨")
    
    return process


async def wait_for_service():
    """서비스 준비 대기"""
    print("\n⏳ 서비스 준비 대기 중...")
    
    for i in range(10):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8002/api/v1/connectors/google/health")
                if response.status_code == 200:
                    data = response.json()
                    print(f"✅ 서비스 준비 완료!")
                    print(f"   API Key 설정: {data.get('api_key_configured')}")
                    return True
        except:
            pass
        await asyncio.sleep(1)
    
    print("❌ 서비스 시작 시간 초과")
    return False


async def test_user_sheet():
    """사용자 시트 테스트"""
    print("\n📊 사용자 Google Sheet 테스트")
    print("=" * 50)
    
    sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?usp=sharing"
    
    async with httpx.AsyncClient() as client:
        print(f"📋 Sheet URL: {sheet_url}")
        print(f"🔑 API Key: {USER_API_KEY[:20]}...")
        
        response = await client.post(
            "http://localhost:8002/api/v1/connectors/google/preview",
            json={"sheet_url": sheet_url},
            timeout=30.0
        )
        
        print(f"\n📊 응답 상태: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✅ 성공! Google Sheet 데이터를 가져왔습니다!")
            
            print(f"\n📋 시트 정보:")
            print(f"   - Sheet ID: {data.get('sheet_id')}")
            print(f"   - 제목: {data.get('sheet_title')}")
            print(f"   - 워크시트: {data.get('worksheet_title')}")
            print(f"   - 전체 행 수: {data.get('total_rows')}")
            print(f"   - 전체 열 수: {data.get('total_columns')}")
            
            # 컬럼 정보
            columns = data.get('columns', [])
            if columns:
                print(f"\n📊 컬럼 목록 ({len(columns)}개):")
                for i, col in enumerate(columns[:15]):  # 처음 15개
                    print(f"   {i+1:2d}. {col}")
                if len(columns) > 15:
                    print(f"   ... 외 {len(columns)-15}개 더")
            
            # 샘플 데이터
            sample_rows = data.get('sample_rows', [])
            if sample_rows:
                print(f"\n📊 샘플 데이터 (처음 {len(sample_rows)}행):")
                for i, row in enumerate(sample_rows):
                    # 각 행의 처음 5개 컬럼만 표시
                    display_data = []
                    for j, cell in enumerate(row[:5]):
                        if j < len(columns):
                            display_data.append(f"{columns[j]}: {cell}")
                    print(f"\n   행 {i+1}:")
                    for item in display_data:
                        print(f"     - {item}")
            
            # 전체 데이터 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"google_sheet_data_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 전체 데이터가 '{filename}'에 저장되었습니다.")
            
            return data
            
        else:
            print(f"\n❌ 오류 발생")
            error_data = response.json()
            if 'detail' in error_data:
                detail = error_data['detail']
                if isinstance(detail, dict):
                    print(f"   오류 코드: {detail.get('error_code')}")
                    print(f"   메시지: {detail.get('message')}")
                    print(f"   상세: {detail.get('detail', '')[:200]}...")
            else:
                print(f"   응답: {error_data}")
            
            return None


async def main():
    """메인 함수"""
    print("🔥 SPICE HARVESTER - Google Sheets 커넥터 테스트")
    print("=" * 70)
    print(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 1. API 키 유효성 검증
    if not await test_api_key_validity():
        print("\n❌ API 키가 유효하지 않습니다.")
        return
    
    # 2. BFF 서비스 시작
    bff_process = await start_bff_with_api_key()
    
    try:
        # 3. 서비스 준비 대기
        if not await wait_for_service():
            print("\n❌ BFF 서비스를 시작할 수 없습니다.")
            # 로그 확인
            if bff_process.poll() is not None:
                _, stderr = bff_process.communicate()
                print(f"\n오류 로그:\n{stderr.decode('utf-8')[:500]}")
            return
        
        # 4. 사용자 시트 테스트
        sheet_data = await test_user_sheet()
        
        if sheet_data:
            print("\n" + "=" * 70)
            print("🎉 축하합니다! Google Sheets 연동에 성공했습니다!")
            print("\n다음 단계:")
            print("1. 추출된 데이터를 확인하세요")
            print("2. 컬럼을 기반으로 ObjectType을 설계하세요")
            print("3. /register API로 주기적 동기화를 설정하세요")
            
    finally:
        # 서비스 종료
        print("\n🛑 BFF 서비스 종료 중...")
        bff_process.terminate()
        try:
            bff_process.wait(timeout=5)
        except:
            bff_process.kill()
        
        print("✅ 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main())