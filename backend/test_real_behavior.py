"""
실제 동작 검증: 존재하지 않는 온톨로지 조회 시 어떤 예외가 발생하는지 확인
"""

import asyncio
import httpx
import json
import traceback

async def test_real_behavior():
    print("🔍 실제 동작 검증 테스트\n")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. 직접 OMS의 async_terminus 동작 확인
        print("1️⃣ OMS에 직접 존재하지 않는 온톨로지 요청")
        
        try:
            response = await client.get(
                "http://localhost:8000/api/v1/ontology/test_db/NonExistentClass999"
            )
            print(f"   상태 코드: {response.status_code}")
            print(f"   응답: {response.text[:200]}")
        except Exception as e:
            print(f"   예외 발생: {type(e).__name__}: {e}")
            traceback.print_exc()
        
        # 2. BFF를 통한 요청
        print("\n2️⃣ BFF를 통한 존재하지 않는 온톨로지 요청")
        
        try:
            response = await client.get(
                "http://localhost:8002/api/v1/database/test_db/ontology/NonExistentClass999"
            )
            print(f"   상태 코드: {response.status_code}")
            print(f"   응답: {response.text[:200]}")
        except Exception as e:
            print(f"   예외 발생: {type(e).__name__}: {e}")
            traceback.print_exc()
        
        # 3. async_terminus.py의 실제 동작 확인을 위한 디버깅
        print("\n3️⃣ async_terminus 내부 동작 확인")
        
        # 테스트 DB 생성
        db_name = "test_real_behavior_db"
        await client.post(
            "http://localhost:8000/api/v1/database/create",
            json={"name": db_name, "description": "실제 동작 검증"}
        )
        
        # 존재하지 않는 온톨로지 조회
        print(f"\n   데이터베이스 '{db_name}'에서 NonExistentClass 조회...")
        try:
            response = await client.get(
                f"http://localhost:8000/api/v1/ontology/{db_name}/NonExistentClassXYZ"
            )
            print(f"   상태 코드: {response.status_code}")
            data = response.json()
            print(f"   응답 타입: {data.get('status')}")
            print(f"   메시지: {data.get('message')}")
            print(f"   세부사항: {data.get('detail')}")
        except httpx.HTTPStatusError as e:
            print(f"   HTTP 에러: {e.response.status_code}")
            print(f"   응답 내용: {e.response.text}")
        except Exception as e:
            print(f"   예외: {type(e).__name__}: {e}")
        
        # 정리
        try:
            await client.delete(f"http://localhost:8000/api/v1/database/{db_name}")
        except:
            pass
        
        print("\n✅ 실제 동작 검증 완료!")

if __name__ == "__main__":
    asyncio.run(test_real_behavior())