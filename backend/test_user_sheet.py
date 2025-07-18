"""
사용자 제공 Google Sheet 테스트
"""

import httpx
import asyncio
import json


async def test_user_sheet():
    """사용자가 제공한 Google Sheet 테스트"""
    
    # 사용자가 제공한 URL
    sheet_url = "https://docs.google.com/spreadsheets/d/1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw/edit?gid=46521583#gid=46521583"
    
    print("🧪 사용자 Google Sheet 테스트")
    print("=" * 50)
    print(f"📋 Sheet URL: {sheet_url}")
    print(f"📊 Sheet ID: 1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw")
    print(f"📑 Worksheet GID: 46521583")
    
    async with httpx.AsyncClient() as client:
        # 1. BFF 서비스 상태 확인
        print("\n1️⃣ BFF 서비스 상태 확인...")
        try:
            health_response = await client.get("http://localhost:8002/api/v1/connectors/google/health")
            if health_response.status_code == 200:
                print("✅ Google Sheets 커넥터가 정상 작동 중입니다.")
            else:
                print("❌ Google Sheets 커넥터에 문제가 있습니다.")
                print("   BFF 서비스를 시작해주세요:")
                print("   cd backend && python backend-for-frontend/main.py")
                return
        except httpx.ConnectError:
            print("❌ BFF 서비스에 연결할 수 없습니다.")
            print("   서비스를 시작해주세요:")
            print("   cd /Users/isihyeon/Desktop/SPICE\\ HARVESTER/backend")
            print("   python backend-for-frontend/main.py")
            return
        
        # 2. Sheet 미리보기
        print("\n2️⃣ Google Sheet 미리보기 시도...")
        try:
            response = await client.post(
                "http://localhost:8002/api/v1/connectors/google/preview",
                json={"sheet_url": sheet_url},
                timeout=30.0
            )
            
            print(f"\n📊 응답 상태 코드: {response.status_code}")
            
            if response.status_code == 200:
                data = response.json()
                print("\n✅ 미리보기 성공!")
                print(f"📋 시트 정보:")
                print(f"   - Sheet ID: {data.get('sheet_id')}")
                print(f"   - 제목: {data.get('sheet_title')}")
                print(f"   - 워크시트: {data.get('worksheet_title')}")
                print(f"   - 전체 행 수: {data.get('total_rows')}")
                print(f"   - 전체 열 수: {data.get('total_columns')}")
                
                # 컬럼 정보
                columns = data.get('columns', [])
                if columns:
                    print(f"\n📊 컬럼 목록 ({len(columns)}개):")
                    for i, col in enumerate(columns[:10]):  # 처음 10개만
                        print(f"   {i+1}. {col}")
                    if len(columns) > 10:
                        print(f"   ... 외 {len(columns)-10}개 더")
                
                # 샘플 데이터
                sample_rows = data.get('sample_rows', [])
                if sample_rows:
                    print(f"\n📊 샘플 데이터 (처음 {len(sample_rows)}행):")
                    for i, row in enumerate(sample_rows):
                        # 처음 5개 컬럼만 표시
                        display_row = row[:5]
                        if len(row) > 5:
                            display_row.append(f"... {len(row)-5}개 더")
                        print(f"   행 {i+1}: {display_row}")
                
                # JSON 전체 저장
                with open('user_sheet_preview.json', 'w', encoding='utf-8') as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                print(f"\n💾 전체 응답이 'user_sheet_preview.json'에 저장되었습니다.")
                
            elif response.status_code == 403:
                print("\n❌ 접근 거부됨")
                print("   Google Sheet가 '링크가 있는 모든 사용자'로 공유되어 있는지 확인하세요.")
                print("   설정 방법:")
                print("   1. Google Sheets에서 '공유' 버튼 클릭")
                print("   2. '링크 가져오기' 클릭")
                print("   3. '제한됨' → '링크가 있는 모든 사용자' 변경")
                print("   4. '뷰어' 권한 확인")
                
            elif response.status_code == 400:
                error_data = response.json()
                print("\n❌ 잘못된 요청")
                print(f"   오류: {error_data}")
                
                # 상세 오류 정보
                if 'detail' in error_data:
                    detail = error_data['detail']
                    if isinstance(detail, dict):
                        print(f"   오류 코드: {detail.get('error_code')}")
                        print(f"   메시지: {detail.get('message')}")
                        print(f"   상세: {detail.get('detail')}")
                        
            else:
                print(f"\n❌ 예상치 못한 응답: {response.status_code}")
                print(f"   응답 내용: {response.text[:500]}...")
                
        except httpx.TimeoutException:
            print("\n❌ 요청 시간 초과 (30초)")
            print("   - 네트워크 연결을 확인하세요")
            print("   - Google Sheets가 너무 크거나 복잡할 수 있습니다")
            
        except Exception as e:
            print(f"\n❌ 예상치 못한 오류: {type(e).__name__}")
            print(f"   {str(e)}")
            import traceback
            traceback.print_exc()


async def main():
    """메인 함수"""
    await test_user_sheet()
    
    print("\n" + "=" * 50)
    print("✅ 테스트 완료!")
    print("\n💡 추가 작업:")
    print("1. Sheet가 공개되어 있지 않다면 공유 설정을 변경하세요")
    print("2. 성공했다면 '/register' API로 주기적 동기화를 설정할 수 있습니다")
    print("3. 미리보기 데이터를 바탕으로 ObjectType을 생성하세요")


if __name__ == "__main__":
    asyncio.run(main())