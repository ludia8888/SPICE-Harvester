"""
서비스 간 통신 테스트
BFF -> OMS 통신 검증
"""

import asyncio
import httpx
import time
import json

async def test_service_communication():
    print("🔍 서비스 간 통신 테스트")
    
    # 테스트용 데이터베이스 생성
    db_name = f"comm_test_db_{int(time.time())}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. BFF를 통한 데이터베이스 생성 (BFF -> OMS)
        print(f"\n1️⃣ BFF를 통한 데이터베이스 생성: {db_name}")
        
        response = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "서비스 간 통신 테스트"}
        )
        
        if response.status_code in [200, 201]:
            print("✅ BFF → OMS 데이터베이스 생성 성공")
        else:
            print(f"❌ 실패: {response.status_code} - {response.text}")
            return False
        
        # 2. BFF를 통한 데이터베이스 목록 조회
        print("\n2️⃣ BFF를 통한 데이터베이스 목록 조회")
        
        response = await client.get("http://localhost:8002/api/v1/databases")
        
        if response.status_code == 200:
            data = response.json()
            db_count = len(data.get("data", {}).get("databases", []))
            print(f"✅ BFF → OMS 목록 조회 성공: {db_count}개 데이터베이스")
            
            # 생성된 DB 확인
            db_names = [db["name"] for db in data.get("data", {}).get("databases", [])]
            if db_name in db_names:
                print(f"✅ 생성된 데이터베이스 '{db_name}' 확인됨")
            else:
                print(f"⚠️ 생성된 데이터베이스 '{db_name}'를 찾을 수 없음")
        else:
            print(f"❌ 실패: {response.status_code}")
        
        # 3. BFF를 통한 온톨로지 생성 (라벨 기반)
        print("\n3️⃣ BFF를 통한 온톨로지 생성 (라벨 기반)")
        
        response = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json={
                "label": "통신 테스트 클래스",
                "description": "BFF-OMS 통신 검증용",
                "properties": [
                    {
                        "label": "테스트 속성",
                        "type": "STRING",
                        "required": True
                    }
                ]
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            # BFF는 직접 온톨로지 객체를 반환
            ontology_id = data.get("id")
            print(f"✅ BFF → OMS 온톨로지 생성 성공: ID={ontology_id}")
        else:
            print(f"❌ 실패: {response.status_code} - {response.text[:200]}")
            ontology_id = None
        
        # 4. BFF를 통한 온톨로지 목록 조회
        print("\n4️⃣ BFF를 통한 온톨로지 목록 조회")
        
        response = await client.get(f"http://localhost:8002/api/v1/database/{db_name}/ontology/list")
        
        if response.status_code == 200:
            data = response.json()
            onto_count = len(data.get("data", {}).get("ontologies", []))
            print(f"✅ BFF → OMS 온톨로지 목록 조회 성공: {onto_count}개")
        else:
            print(f"❌ 실패: {response.status_code}")
        
        # 5. 직접 OMS 호출과 BFF 경유 비교
        print("\n5️⃣ 직접 OMS vs BFF 경유 성능 비교")
        
        # 직접 OMS 호출
        start_time = time.time()
        for _ in range(10):
            response = await client.get("http://localhost:8000/api/v1/database/list")
        oms_direct_time = time.time() - start_time
        
        # BFF 경유
        start_time = time.time()
        for _ in range(10):
            response = await client.get("http://localhost:8002/api/v1/databases")
        bff_time = time.time() - start_time
        
        print(f"  → 직접 OMS 호출: {oms_direct_time:.3f}초 (10회)")
        print(f"  → BFF 경유: {bff_time:.3f}초 (10회)")
        print(f"  → 오버헤드: {((bff_time - oms_direct_time) / oms_direct_time * 100):.1f}%")
        
        # 6. 에러 전파 테스트
        print("\n6️⃣ 에러 전파 테스트")
        
        # 존재하지 않는 데이터베이스에 대한 요청
        response = await client.get(f"http://localhost:8002/api/v1/database/non_existent_db/ontology/list")
        
        if response.status_code == 404:
            print("✅ 404 에러가 올바르게 전파됨")
        else:
            print(f"⚠️ 예상치 못한 상태 코드: {response.status_code}")
        
        # 7. 동시 요청 처리 테스트
        print("\n7️⃣ 동시 요청 처리 테스트 (20개 동시 요청)")
        
        async def concurrent_request(i):
            try:
                response = await client.get(f"http://localhost:8002/api/v1/databases")
                return i, response.status_code, None
            except Exception as e:
                return i, None, str(e)
        
        start_time = time.time()
        tasks = [concurrent_request(i) for i in range(20)]
        results = await asyncio.gather(*tasks)
        elapsed = time.time() - start_time
        
        success_count = sum(1 for _, status, _ in results if status == 200)
        print(f"✅ 동시 요청 처리: {success_count}/20 성공 (소요시간: {elapsed:.2f}초)")
        
        # 8. 정리
        print("\n8️⃣ 테스트 데이터베이스 정리")
        try:
            response = await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
            if response.status_code == 200:
                print("✅ BFF를 통한 데이터베이스 삭제 성공")
            else:
                print(f"⚠️ 삭제 실패: {response.status_code}")
        except:
            pass
        
        print("\n✅ 서비스 간 통신 테스트 완료!")
        return True

if __name__ == "__main__":
    asyncio.run(test_service_communication())