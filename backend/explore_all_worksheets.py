"""
Google Sheet의 모든 워크시트 탐색
"""

import httpx
import asyncio
import json
import os

# API 키 설정
API_KEY = "AIzaSyAVB9eZPQd57rP3Ta_Uesz-vEptjI0Zj2U"
SHEET_ID = "1dniTdsPGWbah3NY_m3sMpuyYCR0UVbCYl9TaZJAvZEw"


async def get_all_worksheets():
    """모든 워크시트 정보 가져오기"""
    print("📊 Google Sheet 워크시트 탐색")
    print("=" * 50)
    
    # 메타데이터 API 호출
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params={"key": API_KEY})
        
        if response.status_code == 200:
            data = response.json()
            
            print(f"📋 스프레드시트: {data.get('properties', {}).get('title')}")
            print(f"   Locale: {data.get('properties', {}).get('locale')}")
            print(f"   Time Zone: {data.get('properties', {}).get('timeZone')}")
            
            sheets = data.get('sheets', [])
            print(f"\n📑 워크시트 목록 ({len(sheets)}개):")
            
            worksheet_info = []
            
            for i, sheet in enumerate(sheets):
                props = sheet.get('properties', {})
                sheet_id = props.get('sheetId')
                title = props.get('title')
                row_count = props.get('gridProperties', {}).get('rowCount', 0)
                col_count = props.get('gridProperties', {}).get('columnCount', 0)
                
                print(f"\n{i+1}. {title}")
                print(f"   - GID: {sheet_id}")
                print(f"   - 크기: {row_count}행 x {col_count}열")
                print(f"   - 인덱스: {props.get('index')}")
                
                worksheet_info.append({
                    'title': title,
                    'gid': sheet_id,
                    'rows': row_count,
                    'cols': col_count
                })
            
            return worksheet_info
        else:
            print(f"❌ 오류: {response.status_code}")
            print(response.json())
            return []


async def preview_worksheet(worksheet_name: str):
    """특정 워크시트 미리보기"""
    print(f"\n📊 워크시트 '{worksheet_name}' 미리보기")
    print("-" * 50)
    
    # A1 notation으로 범위 지정
    range_name = f"'{worksheet_name}'!A1:Z10"  # 처음 10행, A-Z 열
    url = f"https://sheets.googleapis.com/v4/spreadsheets/{SHEET_ID}/values/{range_name}"
    
    async with httpx.AsyncClient() as client:
        response = await client.get(url, params={"key": API_KEY})
        
        if response.status_code == 200:
            data = response.json()
            values = data.get('values', [])
            
            if values:
                print(f"✅ 데이터 발견! ({len(values)}행)")
                
                # 첫 번째 행 (헤더로 추정)
                if values:
                    headers = values[0]
                    print(f"\n컬럼 ({len(headers)}개):")
                    for i, header in enumerate(headers[:10]):
                        print(f"   {i+1}. {header}")
                    if len(headers) > 10:
                        print(f"   ... 외 {len(headers)-10}개 더")
                
                # 샘플 데이터
                if len(values) > 1:
                    print(f"\n샘플 데이터:")
                    for i, row in enumerate(values[1:4]):  # 2-4번째 행
                        print(f"\n   행 {i+2}:")
                        for j, cell in enumerate(row[:5]):  # 처음 5개 컬럼
                            if j < len(headers):
                                print(f"     {headers[j]}: {cell}")
            else:
                print("   (데이터 없음)")
                
        elif response.status_code == 400:
            print(f"   ❌ 워크시트를 찾을 수 없습니다")
        else:
            print(f"   ❌ 오류: {response.status_code}")


async def test_with_bff(worksheet_name: str):
    """BFF 서비스로 특정 워크시트 테스트"""
    print(f"\n🔧 BFF 서비스로 '{worksheet_name}' 워크시트 테스트")
    print("-" * 50)
    
    # URL에 워크시트 이름 추가 (인코딩 필요)
    import urllib.parse
    encoded_name = urllib.parse.quote(worksheet_name)
    sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}/edit#gid=0&range={encoded_name}"
    
    async with httpx.AsyncClient() as client:
        response = await client.post(
            "http://localhost:8002/api/v1/connectors/google/preview",
            json={"sheet_url": sheet_url},
            timeout=30.0
        )
        
        if response.status_code == 200:
            data = response.json()
            print(f"✅ 성공!")
            print(f"   워크시트: {data.get('worksheet_title')}")
            print(f"   행/열: {data.get('total_rows')} x {data.get('total_columns')}")
            
            # 저장
            filename = f"worksheet_{worksheet_name.replace(' ', '_')}.json"
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
            print(f"   💾 '{filename}'에 저장됨")
        else:
            print(f"❌ 오류: {response.status_code}")


async def main():
    """메인 함수"""
    print("🔍 Google Sheet 전체 워크시트 탐색")
    print("=" * 70)
    
    # 1. 모든 워크시트 정보 가져오기
    worksheets = await get_all_worksheets()
    
    if not worksheets:
        print("❌ 워크시트 정보를 가져올 수 없습니다.")
        return
    
    # 2. 각 워크시트 미리보기
    print("\n" + "=" * 70)
    print("📊 각 워크시트 데이터 미리보기")
    
    for ws in worksheets:
        if ws['rows'] > 0 and ws['cols'] > 0:  # 비어있지 않은 워크시트만
            await preview_worksheet(ws['title'])
    
    # 3. 사용자가 원하는 GID의 워크시트 찾기
    target_gid = 46521583  # 사용자가 제공한 GID
    target_worksheet = None
    
    for ws in worksheets:
        if ws['gid'] == target_gid:
            target_worksheet = ws
            break
    
    if target_worksheet:
        print(f"\n" + "=" * 70)
        print(f"🎯 사용자가 지정한 워크시트 (GID: {target_gid})")
        print(f"   이름: {target_worksheet['title']}")
        
        # BFF 서비스가 실행 중이면 테스트
        try:
            await test_with_bff(target_worksheet['title'])
        except:
            print("   (BFF 서비스가 실행되지 않음)")
    
    print("\n✅ 탐색 완료!")


if __name__ == "__main__":
    asyncio.run(main())