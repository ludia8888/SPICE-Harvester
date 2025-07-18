"""
최종 Google Sheet 테스트 - 올바른 워크시트로
"""

import os
import httpx
import asyncio
import subprocess
import time
import json
from datetime import datetime

# API 키 설정
os.environ["GOOGLE_API_KEY"] = "AIzaSyAVB9eZPQd57rP3Ta_Uesz-vEptjI0Zj2U"


async def start_bff_service():
    """BFF 서비스 시작"""
    print("🚀 BFF 서비스 시작 중...")
    
    # 기존 프로세스 종료
    subprocess.run(["pkill", "-f", "python.*backend-for-frontend/main.py"], capture_output=True)
    time.sleep(2)
    
    # 새로 시작
    process = subprocess.Popen(
        ["python", "backend-for-frontend/main.py"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd="/Users/isihyeon/Desktop/SPICE HARVESTER/backend"
    )
    
    print(f"✅ 서비스 시작됨 (PID: {process.pid})")
    return process


async def wait_for_service():
    """서비스 준비 대기"""
    print("⏳ 서비스 준비 대기 중...")
    
    for i in range(10):
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get("http://localhost:8002/api/v1/connectors/google/health")
                if response.status_code == 200:
                    print("✅ 서비스 준비 완료!")
                    return True
        except:
            pass
        await asyncio.sleep(1)
    
    return False


async def test_correct_worksheet():
    """올바른 워크시트로 테스트"""
    print("\n📊 '상품리스트 양식변환' 워크시트 테스트")
    print("=" * 50)
    
    # GID가 포함된 URL 사용
    sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit#gid=46521583"
    
    async with httpx.AsyncClient() as client:
        print(f"📋 URL: {sheet_url}")
        print(f"   GID: 46521583 (상품리스트 양식변환)")
        
        response = await client.post(
            "http://localhost:8002/api/v1/connectors/google/preview",
            json={"sheet_url": sheet_url},
            timeout=30.0
        )
        
        print(f"\n📊 응답 상태: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            print("\n✅ 성공! 데이터를 가져왔습니다!")
            
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
                for i, col in enumerate(columns):
                    print(f"   {i+1}. {col if col else '(빈 컬럼)'}")
            
            # 샘플 데이터
            sample_rows = data.get('sample_rows', [])
            if sample_rows:
                print(f"\n📊 샘플 데이터 (처음 {len(sample_rows)}행):")
                
                # 의미 있는 컬럼 찾기 (빈 컬럼 제외)
                meaningful_cols = []
                for i, col in enumerate(columns):
                    if col and col.strip():
                        meaningful_cols.append((i, col))
                
                for row_idx, row in enumerate(sample_rows[:3]):
                    print(f"\n   행 {row_idx + 1}:")
                    
                    # 의미 있는 데이터만 표시
                    has_data = False
                    for col_idx, col_name in meaningful_cols[:5]:  # 처음 5개 의미있는 컬럼
                        if col_idx < len(row) and row[col_idx]:
                            print(f"     {col_name}: {row[col_idx]}")
                            has_data = True
                    
                    if not has_data:
                        # 빈 컬럼이라도 데이터가 있으면 표시
                        for i, cell in enumerate(row[:5]):
                            if cell:
                                print(f"     컬럼{i+1}: {cell}")
            
            # 전체 데이터 저장
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"product_list_data_{timestamp}.json"
            
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            
            print(f"\n💾 전체 데이터가 '{filename}'에 저장되었습니다.")
            
            # 데이터 분석
            print("\n📊 데이터 분석:")
            print(f"   - 상품 코드 컬럼: 1번째")
            print(f"   - 상품명 컬럼: 2번째")
            print(f"   - 색상 컬럼: 3번째")
            print(f"   - 사이즈 컬럼: 4번째")
            print(f"   - 플랜제이용 옵션: 5번째")
            print(f"   - 클메용 옵션: 6번째")
            
            return data
            
        else:
            print(f"\n❌ 오류 발생")
            print(f"   응답: {response.json()}")
            return None


async def main():
    """메인 함수"""
    print("🔥 SPICE HARVESTER - Google Sheets 최종 테스트")
    print("=" * 70)
    
    # BFF 서비스 시작
    bff_process = await start_bff_service()
    
    try:
        # 서비스 준비 대기
        if not await wait_for_service():
            print("❌ 서비스를 시작할 수 없습니다.")
            return
        
        # 올바른 워크시트로 테스트
        sheet_data = await test_correct_worksheet()
        
        if sheet_data:
            print("\n" + "=" * 70)
            print("🎉 완벽한 성공!")
            print("\n📋 다음 단계:")
            print("1. 추출된 상품 데이터를 확인하세요")
            print("2. 다음과 같은 ObjectType을 생성할 수 있습니다:")
            print("   - Product (상품)")
            print("     - code: 상품 코드")
            print("     - name: 상품명")
            print("     - color: 색상")
            print("     - size: 사이즈")
            print("     - option_planjay: 플랜제이용 옵션")
            print("     - option_klme: 클메용 옵션")
            print("\n3. 주기적 동기화 설정:")
            print("   POST /api/v1/connectors/google/register")
            
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