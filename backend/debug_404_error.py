"""
404 에러 디버깅
"""

import asyncio
import httpx

async def debug_404():
    """404 에러 원인 파악"""
    print("🔍 404 에러 디버깅 시작\n")
    
    async with httpx.AsyncClient() as client:
        # 1. 데이터베이스 존재 확인
        print("1. 데이터베이스 목록 확인")
        response = await client.get("http://localhost:8000/api/v1/database/list")
        print(f"   상태: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            databases = data.get("data", {}).get("databases", [])
            print(f"   데이터베이스: {databases}")
        
        # 2. load_test_db 생성
        print("\n2. load_test_db 생성 시도")
        response = await client.post("http://localhost:8000/api/v1/database/create",
                                   json={"name": "load_test_db", "description": "테스트"})
        print(f"   상태: {response.status_code}")
        print(f"   응답: {response.text[:200]}")
        
        # 3. 다시 데이터베이스 목록 확인
        print("\n3. 생성 후 데이터베이스 목록")
        response = await client.get("http://localhost:8000/api/v1/database/list")
        if response.status_code == 200:
            data = response.json()
            databases = data.get("data", {}).get("databases", [])
            print(f"   데이터베이스: {databases}")
        
        # 4. 온톨로지 생성 테스트
        print("\n4. 온톨로지 생성 테스트")
        response = await client.post(
            "http://localhost:8000/api/v1/ontology/load_test_db/create",
            json={
                "id": "TestClass",
                "label": "테스트 클래스",
                "description": "테스트"
            }
        )
        print(f"   상태: {response.status_code}")
        if response.status_code != 200:
            print(f"   에러: {response.text[:200]}")
        
        # 5. BFF 데이터베이스 목록 조회
        print("\n5. BFF 데이터베이스 목록 조회")
        response = await client.get("http://localhost:8002/api/v1/databases")
        print(f"   상태: {response.status_code}")
        if response.status_code != 200:
            print(f"   에러: {response.text[:200]}")
        
        # 6. 서비스 로그 확인
        print("\n6. 서비스 로그에서 404 에러 확인")
        import subprocess
        
        # OMS 로그
        print("\n   OMS 로그 (최근 404):")
        result = subprocess.run("tail -50 oms.log | grep '404'", 
                              shell=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout[:500])
        
        # BFF 로그  
        print("\n   BFF 로그 (최근 404):")
        result = subprocess.run("tail -50 bff.log | grep '404'", 
                              shell=True, capture_output=True, text=True)
        if result.stdout:
            print(result.stdout[:500])


if __name__ == "__main__":
    asyncio.run(debug_404())