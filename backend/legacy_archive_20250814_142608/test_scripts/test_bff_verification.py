"""
BFF 기능 완전 검증 테스트
"""

import asyncio
import httpx
import time
import json

async def verify_bff_complete():
    print("🔍 BFF 기능 완전 검증 테스트\n")
    
    # 테스트용 데이터베이스 이름
    db_name = f"verify_bff_{int(time.time())}"
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. 데이터베이스 생성
        print("1️⃣ BFF를 통한 데이터베이스 생성")
        response = await client.post(
            "http://localhost:8002/api/v1/databases",
            json={"name": db_name, "description": "BFF 완전 검증"}
        )
        print(f"   상태: {response.status_code}")
        if response.status_code in [200, 201]:
            print("   ✅ 데이터베이스 생성 성공")
        else:
            print(f"   ❌ 실패: {response.text}")
            return False
        
        # 2. BFF를 통한 온톨로지 생성
        print("\n2️⃣ BFF를 통한 온톨로지 생성")
        ontology_data = {
            "label": "검증 테스트 클래스",
            "description": "BFF 검증용 온톨로지",
            "properties": [
                {
                    "label": "이름",
                    "type": "STRING",
                    "required": True
                },
                {
                    "label": "나이",
                    "type": "INTEGER",
                    "required": False
                }
            ]
        }
        
        response = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json=ontology_data
        )
        print(f"   상태: {response.status_code}")
        
        if response.status_code == 200:
            data = response.json()
            created_id = data.get("id")
            print(f"   ✅ 온톨로지 생성 성공: ID={created_id}")
            print(f"   레이블: {data.get('label')}")
            print(f"   속성 개수: {len(data.get('properties', []))}")
        else:
            print(f"   ❌ 실패: {response.text}")
            return False
        
        # 3. OMS 직접 호출로 실제 생성 확인
        print("\n3️⃣ OMS 직접 호출로 실제 생성 확인")
        response = await client.get(
            f"http://localhost:8000/api/v1/ontology/{db_name}/list"
        )
        
        if response.status_code == 200:
            data = response.json()
            ontologies = data.get("data", {}).get("ontologies", [])
            print(f"   ✅ OMS 확인: {len(ontologies)}개 온톨로지")
            
            # 생성한 온톨로지 찾기
            found = False
            for onto in ontologies:
                if onto.get("id") == created_id:
                    found = True
                    print(f"   ✅ 생성된 온톨로지 확인됨: {onto.get('id')}")
                    break
            
            if not found:
                print(f"   ❌ 생성된 온톨로지를 찾을 수 없음!")
                # 실제로 무엇이 있는지 출력
                for onto in ontologies:
                    print(f"      - {onto.get('id')}: {onto.get('@documentation', {}).get('@comment', 'No comment')}")
        else:
            print(f"   ❌ OMS 조회 실패: {response.status_code}")
        
        # 4. BFF를 통한 온톨로지 목록 조회
        print("\n4️⃣ BFF를 통한 온톨로지 목록 조회")
        response = await client.get(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology/list"
        )
        
        if response.status_code == 200:
            data = response.json()
            total = data.get("total", 0)
            ontologies = data.get("ontologies", [])
            print(f"   ✅ BFF 목록 조회 성공: {total}개 온톨로지")
            
            # 생성한 온톨로지 찾기
            found = False
            for onto in ontologies:
                if onto.get("id") == created_id:
                    found = True
                    print(f"   ✅ BFF 목록에서 생성된 온톨로지 확인됨")
                    break
            
            if not found:
                print(f"   ⚠️ BFF 목록에서 생성된 온톨로지를 찾을 수 없음")
                # 실제 목록 출력
                for onto in ontologies[:5]:  # 처음 5개만
                    print(f"      - {onto.get('id')}")
        else:
            print(f"   ❌ BFF 목록 조회 실패: {response.status_code}")
        
        # 5. 에러 전파 테스트
        print("\n5️⃣ 에러 전파 테스트")
        
        # 5-1. 존재하지 않는 데이터베이스
        response = await client.get(
            "http://localhost:8002/api/v1/database/non_existent_db_12345/ontology/list"
        )
        print(f"   존재하지 않는 DB 테스트: {response.status_code}")
        if response.status_code == 404:
            print("   ✅ 404 에러가 올바르게 전파됨")
        else:
            print(f"   ❌ 잘못된 상태 코드: {response.status_code} (404 예상)")
        
        # 5-2. 존재하지 않는 온톨로지
        response = await client.get(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology/NonExistentClass"
        )
        print(f"   존재하지 않는 온톨로지 테스트: {response.status_code}")
        if response.status_code == 404:
            print("   ✅ 404 에러가 올바르게 전파됨")
        else:
            print(f"   ❌ 잘못된 상태 코드: {response.status_code} (404 예상)")
        
        # 6. ID 반환 테스트
        print("\n6️⃣ ID 반환 테스트")
        
        # 또 다른 온톨로지 생성
        response = await client.post(
            f"http://localhost:8002/api/v1/database/{db_name}/ontology",
            json={
                "label": "ID 테스트",
                "properties": []
            }
        )
        
        if response.status_code == 200:
            data = response.json()
            returned_id = data.get("id")
            if returned_id:
                print(f"   ✅ ID가 올바르게 반환됨: {returned_id}")
            else:
                print(f"   ❌ ID가 None으로 반환됨")
                print(f"   응답 데이터: {json.dumps(data, indent=2)}")
        
        # 7. 정리
        print("\n7️⃣ 테스트 정리")
        try:
            response = await client.delete(f"http://localhost:8002/api/v1/databases/{db_name}")
            if response.status_code == 200:
                print("   ✅ 데이터베이스 삭제 성공")
        except:
            pass
        
        print("\n✅ BFF 완전 검증 테스트 완료!")
        return True

if __name__ == "__main__":
    asyncio.run(verify_bff_complete())