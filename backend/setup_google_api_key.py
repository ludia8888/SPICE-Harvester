"""
Google API Key 설정 도우미
"""

import os
import sys


def setup_api_key():
    """Google API Key 설정"""
    print("🔑 Google API Key 설정 도우미")
    print("=" * 50)
    
    print("\n📋 Google API Key를 얻는 방법:")
    print("\n1. Google Cloud Console 접속")
    print("   https://console.cloud.google.com/")
    
    print("\n2. 프로젝트 생성 또는 선택")
    print("   - 상단의 프로젝트 선택 드롭다운 클릭")
    print("   - '새 프로젝트' 클릭")
    print("   - 프로젝트 이름 입력 (예: 'spice-harvester')")
    
    print("\n3. Google Sheets API 활성화")
    print("   - 메뉴 > API 및 서비스 > 라이브러리")
    print("   - 'Google Sheets API' 검색")
    print("   - '사용' 버튼 클릭")
    
    print("\n4. API 키 생성")
    print("   - 메뉴 > API 및 서비스 > 사용자 인증 정보")
    print("   - '+ 사용자 인증 정보 만들기' 클릭")
    print("   - 'API 키' 선택")
    print("   - 생성된 API 키 복사")
    
    print("\n5. (선택) API 키 제한")
    print("   - API 키 클릭하여 설정 페이지 진입")
    print("   - 'API 제한사항' > 'Google Sheets API'만 선택")
    print("   - 'HTTP 참조자' 또는 'IP 주소'로 제한 가능")
    
    print("\n" + "=" * 50)
    
    # 기존 API 키 확인
    current_key = os.getenv("GOOGLE_API_KEY", "")
    if current_key:
        print(f"\n✅ 현재 설정된 API 키가 있습니다: {current_key[:10]}...")
        change = input("변경하시겠습니까? (y/N): ")
        if change.lower() != 'y':
            return current_key
    
    # 새 API 키 입력
    print("\n🔑 Google API Key를 입력하세요:")
    api_key = input("API Key: ").strip()
    
    if not api_key:
        print("❌ API 키가 입력되지 않았습니다.")
        return None
    
    # 환경 변수 설정 명령 생성
    print("\n✅ API 키가 입력되었습니다!")
    print("\n다음 명령어를 실행하여 환경 변수를 설정하세요:")
    print(f"\nexport GOOGLE_API_KEY='{api_key}'")
    
    # .env 파일 생성 옵션
    save_env = input("\n.env 파일에 저장하시겠습니까? (y/N): ")
    if save_env.lower() == 'y':
        env_path = os.path.join(os.path.dirname(__file__), '.env')
        
        # 기존 .env 파일 읽기
        env_content = []
        if os.path.exists(env_path):
            with open(env_path, 'r') as f:
                env_content = [line for line in f.readlines() if not line.startswith('GOOGLE_API_KEY=')]
        
        # 새 내용 추가
        env_content.append(f"GOOGLE_API_KEY={api_key}\n")
        
        # 파일 쓰기
        with open(env_path, 'w') as f:
            f.writelines(env_content)
        
        print(f"✅ .env 파일에 저장되었습니다: {env_path}")
    
    return api_key


def test_api_key(api_key: str):
    """API 키 테스트"""
    import httpx
    import asyncio
    
    async def test():
        # Google의 공식 샘플 시트로 테스트
        sheet_id = "1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
        url = f"https://sheets.googleapis.com/v4/spreadsheets/{sheet_id}"
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, params={"key": api_key})
            
            if response.status_code == 200:
                data = response.json()
                print(f"\n✅ API 키 테스트 성공!")
                print(f"   시트 제목: {data.get('properties', {}).get('title')}")
                return True
            else:
                print(f"\n❌ API 키 테스트 실패: {response.status_code}")
                print(f"   응답: {response.text[:200]}...")
                return False
    
    return asyncio.run(test())


if __name__ == "__main__":
    # API 키 설정
    api_key = setup_api_key()
    
    if api_key:
        # API 키 테스트
        print("\n🧪 API 키 테스트 중...")
        if test_api_key(api_key):
            print("\n🎉 모든 설정이 완료되었습니다!")
            print("\n다음 단계:")
            print("1. 터미널에서 환경 변수 설정:")
            print(f"   export GOOGLE_API_KEY='{api_key}'")
            print("\n2. BFF 서비스 재시작:")
            print("   pkill -f 'python.*backend-for-frontend/main.py'")
            print("   python backend-for-frontend/main.py")
            print("\n3. Google Sheets 커넥터 사용!")
        else:
            print("\n⚠️  API 키가 유효하지 않습니다.")
            print("Google Cloud Console에서 다시 확인해주세요.")