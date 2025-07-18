"""
API Key를 사용한 Google Sheets 테스트
"""

import os
import httpx
import asyncio
import subprocess
import time


# 테스트용 API 키 (제한된 할당량)
# 주의: 프로덕션에서는 자신의 API 키를 사용하세요!
TEST_API_KEY = "AIzaSyDjQCv7MGlYYLmEWnNpS0R-K_r-gKDu1ro"  # 공개 테스트용 키


async def test_user_sheet_with_key():
    """API 키를 사용하여 사용자 시트 테스트"""
    
    # 환경 변수 설정
    os.environ["GOOGLE_API_KEY"] = TEST_API_KEY
    
    print("🔑 Google Sheets API 키 테스트")
    print("=" * 50)
    print(f"📌 테스트 API 키 설정됨")
    print(f"   (프로덕션에서는 자신의 키를 사용하세요!)")
    
    # BFF 서비스 재시작
    print("\n🔄 BFF 서비스 재시작 중...")
    
    # 기존 프로세스 종료
    subprocess.run(["pkill", "-f", "python.*backend-for-frontend/main.py"], 
                   capture_output=True)
    time.sleep(2)
    
    # 새로 시작
    process = subprocess.Popen(
        ["python", "backend-for-frontend/main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env=os.environ.copy()
    )
    
    # 서비스 시작 대기
    print("⏳ 서비스 시작 대기 중...")
    await asyncio.sleep(5)
    
    # 사용자 시트 테스트
    user_sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?usp=sharing"
    
    async with httpx.AsyncClient() as client:
        print(f"\n📋 사용자 시트 테스트")
        print(f"   URL: {user_sheet_url}")
        
        try:
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/preview",
                json={"sheet_url": user_sheet_url},
                timeout=30.0
            )
            
            print(f"\n📊 응답 상태: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("\n✅ 성공! 시트 데이터를 가져왔습니다!")
                print(f"\n📋 시트 정보:")
                print(f"   - ID: {data.get('sheet_id')}")
                print(f"   - 제목: {data.get('sheet_title')}")
                print(f"   - 워크시트: {data.get('worksheet_title')}")
                print(f"   - 행 수: {data.get('total_rows')}")
                print(f"   - 열 수: {data.get('total_columns')}")
                
                # 컬럼 정보
                columns = data.get('columns', [])
                if columns:
                    print(f"\n📊 컬럼 ({len(columns)}개):")
                    for i, col in enumerate(columns[:10]):
                        print(f"   {i+1}. {col}")
                
                # 샘플 데이터
                sample_rows = data.get('sample_rows', [])
                if sample_rows:
                    print(f"\n📊 샘플 데이터:")
                    for i, row in enumerate(sample_rows[:3]):
                        print(f"   행 {i+1}: {row[:5]}...")
                
                # 파일로 저장
                import json
                with open('user_sheet_data.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"\n💾 전체 데이터가 'user_sheet_data.json'에 저장되었습니다.")
                
            else:
                print(f"\n❌ 오류: {response.status_code}")
                print(f"   응답: {response.text}")
                
        except Exception as e:
            print(f"\n❌ 예외 발생: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            # 서비스 종료
            print("\n🛑 BFF 서비스 종료 중...")
            process.terminate()
            process.wait()


async def test_direct_api():
    """직접 API 호출 테스트"""
    print("\n🔍 직접 API 호출 테스트")
    print("=" * 50)
    
    sheet_id = "1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw"
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params={"key": TEST_API_KEY})
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ 직접 API 호출 성공!")
            print(f"   시트 제목: {data.get('properties', {}).get('title')}")
            
            # 워크시트 목록
            sheets = data.get('sheets', [])
            if sheets:
                print(f"\n📑 워크시트 목록:")
                for sheet in sheets:
                    props = sheet.get('properties', {})
                    print(f"   - {props.get('title')} (GID: {props.get('sheetId')})")
        else:
            print(f"❌ 실패: {response.status_code}")
            print(f"   응답: {response.text[:200]}...")


async def main():
    """메인 함수"""
    print("🚀 Google Sheets API Key 테스트")
    print("=" * 70)
    
    # 1. 직접 API 테스트
    await test_direct_api()
    
    # 2. BFF 서비스를 통한 테스트
    await test_user_sheet_with_key()
    
    print("\n" + "=" * 70)
    print("✅ 테스트 완료!")
    print("\n📌 자신의 API 키 설정하기:")
    print("   python setup_google_api_key.py")


if __name__ == "__main__":
    asyncio.run(main())