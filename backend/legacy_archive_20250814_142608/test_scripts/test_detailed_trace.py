"""
상세 추적: async_terminus.py에서 result가 None일 때의 처리
"""

import asyncio
import httpx

async def test_detailed_trace():
    print("🔍 상세 추적 테스트\n")
    
    # OMS 로그 레벨을 DEBUG로 설정하여 더 많은 정보 확인
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 테스트 DB 생성
        db_name = "trace_test_db"
        await client.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": db_name, "description": "추적 테스트"}
        )
        
        print("1️⃣ 존재하는 온톨로지 생성")
        response = await client.post(
            f"http://localhost:8000/api/v1/ontology/{db_name}/create",
            json={
                "id": "ExistingClass",
                "label": {"ko": "존재하는 클래스"},
                "properties": []
            }
        )
        print(f"   생성 결과: {response.status_code}")
        
        print("\n2️⃣ 존재하는 온톨로지 조회")
        response = await client.get(
            f"http://localhost:8000/api/v1/ontology/{db_name}/ExistingClass"
        )
        print(f"   조회 결과: {response.status_code}")
        if response.status_code == 200:
            data = response.json()
            print(f"   데이터: ID={data.get('data', {}).get('id')}")
        
        print("\n3️⃣ 존재하지 않는 온톨로지 조회 (NonExistentClassABC)")
        response = await client.get(
            f"http://localhost:8000/api/v1/ontology/{db_name}/NonExistentClassABC"
        )
        print(f"   조회 결과: {response.status_code}")
        print(f"   응답: {response.text}")
        
        # 정리
        try:
            await client.delete(f"http://localhost:8000/api/v1/database/{db_name}")
        except:
            pass
        
        print("\n✅ 추적 완료!")

if __name__ == "__main__":
    asyncio.run(test_detailed_trace())