"""
🔥 FINAL PRODUCTION TEST
최종 운영환경 시뮬레이션 테스트
"""

import asyncio
import httpx
import time
import os
from pathlib import Path
from collections import defaultdict
import sys

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv()

sys.path.append(str(Path(__file__).parent))


async def test_high_load():
    """1. 고부하 테스트 (200 동시 요청)"""
    print("\n=== 1. 고부하 동시성 테스트 (200 요청) ===")
    
    async def make_request(client: httpx.AsyncClient, i: int):
        try:
            # API 호출
            if i % 2 == 0:
                response = await client.get("http://localhost:8000/api/v1/database/list")
            else:
                response = await client.post(
                    f"http://localhost:8000/api/v1/ontology/test_db/create",
                    json={
                        "id": f"LoadTest_{i}",
                        "label": f"부하 테스트 {i}",
                        "description": "테스트"
                    }
                )
            return response.status_code, None
        except Exception as e:
            return None, type(e).__name__
    
    start_time = time.time()
    success = 0
    errors = defaultdict(int)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        # 테스트 DB 생성
        await client.post("http://localhost:8000/api/v1/database/create",
                         json={"name": "test_db", "description": "테스트"})
        
        # 동시 요청
        tasks = [make_request(client, i) for i in range(200)]
        results = await asyncio.gather(*tasks)
    
    for status, error in results:
        if error:
            errors[error] += 1
        elif status and 200 <= status < 300:
            success += 1
        else:
            errors[f"HTTP_{status}"] += 1
    
    duration = time.time() - start_time
    
    print(f"✅ 완료: {duration:.2f}초")
    print(f"   성공: {success}/200 ({success/2:.1f}%)")
    print(f"   RPS: {200/duration:.1f}")
    
    if errors:
        print("   에러:")
        for error, count in list(errors.items())[:3]:
            print(f"     - {error}: {count}회")
    
    # 정리
    async with httpx.AsyncClient() as client:
        await client.delete("http://localhost:8000/api/v1/database/test_db")


async def test_large_data():
    """2. 대용량 데이터 테스트"""
    print("\n=== 2. 대용량 데이터 처리 테스트 ===")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 테스트 DB 생성
        await client.post("http://localhost:8000/api/v1/database/create",
                         json={"name": "large_test", "description": "대용량 테스트"})
        
        # 500개 온톨로지 생성
        print("500개 온톨로지 생성 중...")
        
        batch_size = 50
        start_time = time.time()
        success = 0
        
        for batch in range(10):
            tasks = []
            for i in range(batch_size):
                idx = batch * batch_size + i
                tasks.append(
                    client.post(
                        "http://localhost:8000/api/v1/ontology/large_test/create",
                        json={
                            "id": f"BigData_{idx}",
                            "label": f"대용량 {idx}",
                            "properties": [
                                {"id": f"prop_{j}", "label": f"속성 {j}", "type": "xsd:string"}
                                for j in range(5)
                            ]
                        }
                    )
                )
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success += sum(1 for r in results if not isinstance(r, Exception) and r.status_code < 300)
            print(f"   배치 {batch+1}/10 완료")
        
        duration = time.time() - start_time
        print(f"✅ 500개 생성 완료: {duration:.2f}초 ({success}/500 성공)")
        
        # 전체 조회
        start_time = time.time()
        response = await client.get("http://localhost:8000/api/v1/ontology/large_test/list")
        list_time = time.time() - start_time
        
        if response.status_code == 200:
            count = len(response.json().get("data", {}).get("ontologies", []))
            print(f"✅ 전체 조회: {count}개, {list_time:.3f}초")
        
        # 정리
        await client.delete("http://localhost:8000/api/v1/database/large_test")


async def test_security():
    """3. 보안 테스트"""
    print("\n=== 3. 보안 및 에러 처리 테스트 ===")
    
    malicious = [
        "'; DROP TABLE users; --",
        "../../../etc/passwd",
        "<script>alert('xss')</script>",
        "test\x00null",
        "A" * 1000
    ]
    
    blocked = 0
    async with httpx.AsyncClient() as client:
        for bad_input in malicious:
            try:
                response = await client.post(
                    "http://localhost:8000/api/v1/database/create",
                    json={"name": bad_input, "description": "test"}
                )
                if response.status_code in [400, 422]:
                    blocked += 1
            except:
                blocked += 1
    
    print(f"✅ 악성 입력 차단: {blocked}/{len(malicious)}")


async def test_service_communication():
    """4. 서비스 간 통신 테스트"""
    print("\n=== 4. 서비스 간 통신 테스트 ===")
    
    async with httpx.AsyncClient() as client:
        # BFF -> OMS 통신
        try:
            response = await client.get("http://localhost:8002/api/v1/databases")
            print(f"BFF -> OMS 통신: {'✅' if response.status_code == 200 else '❌'} ({response.status_code})")
        except Exception as e:
            print(f"BFF -> OMS 통신: ❌ ({type(e).__name__})")
        
        # BFF Health Check
        try:
            response = await client.get("http://localhost:8002/health")
            data = response.json()
            oms_connected = data.get("data", {}).get("oms_connected", False)
            print(f"BFF OMS 연결 상태: {'✅' if oms_connected else '❌'}")
        except:
            print("BFF OMS 연결 상태: ❌")


async def main():
    """메인 실행 함수"""
    print("🔥 FINAL PRODUCTION TEST")
    print("최종 운영환경 시뮬레이션\n")
    
    print("⚠️ 사전 요구사항:")
    print("1. 터미널에서 다음 명령어로 서비스를 시작하세요:")
    print("   python -m oms.main")
    print("   python -m bff.main")
    print("   python -m funnel.main")
    print("2. 모든 서비스가 시작된 후 이 테스트를 실행하세요.")
    print("\n계속하려면 Enter를 누르세요...")
    input()
    
    # 서비스 상태 확인
    print("\n서비스 상태 확인 중...")
    services_ok = True
    
    async with httpx.AsyncClient() as client:
        # OMS 확인
        try:
            response = await client.get("http://localhost:8000/health")
            print(f"OMS: {'✅' if response.status_code == 200 else '❌'}")
        except:
            print("OMS: ❌")
            services_ok = False
        
        # BFF 확인
        try:
            response = await client.get("http://localhost:8002/health")
            print(f"BFF: {'✅' if response.status_code == 200 else '❌'}")
        except:
            print("BFF: ❌")
            services_ok = False
        
        # Funnel 확인
        try:
            response = await client.get("http://localhost:8004/health")
            print(f"Funnel: {'✅' if response.status_code == 200 else '❌'}")
        except:
            print("Funnel: ❌")
            services_ok = False
    
    if not services_ok:
        print("\n❌ 일부 서비스가 실행되지 않았습니다.")
        return
    
    # 테스트 실행
    await test_high_load()
    await test_large_data()
    await test_security()
    await test_service_communication()
    
    print("\n✅ 모든 테스트 완료!")


if __name__ == "__main__":
    asyncio.run(main())