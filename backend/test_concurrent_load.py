"""
고부하 동시성 테스트
"""

import asyncio
import httpx
import time
from collections import defaultdict
import json

async def high_load_test():
    """고부하 동시성 테스트 (1000 요청)"""
    print("🔥 고부하 동시성 테스트 시작 (1000 동시 요청)")
    
    # 테스트 DB 생성
    async with httpx.AsyncClient() as client:
        try:
            await client.post("http://localhost:8000/api/v1/database/create",
                            json={"name": "load_test_db", "description": "부하 테스트"})
        except:
            pass
    
    async def make_request(client: httpx.AsyncClient, i: int):
        try:
            start = time.time()
            
            if i % 3 == 0:
                # 데이터베이스 목록 조회 (READ)
                response = await client.get("http://localhost:8000/api/v1/database/list")
            elif i % 3 == 1:
                # 온톨로지 생성 (WRITE)
                response = await client.post(
                    f"http://localhost:8000/api/v1/ontology/load_test_db/create",
                    json={
                        "id": f"LoadTest_{int(time.time() * 1000)}_{i}",
                        "label": f"부하 테스트 {i}",
                        "description": "고부하 테스트를 위한 온톨로지",
                        "properties": [
                            {"name": f"prop_{j}", "label": f"속성 {j}", "type": "xsd:string"}
                            for j in range(3)
                        ]
                    }
                )
            else:
                # BFF를 통한 데이터베이스 목록 조회
                response = await client.get("http://localhost:8002/api/v1/databases")
            
            elapsed = time.time() - start
            return response.status_code, elapsed, None
        except httpx.TimeoutException:
            return None, time.time() - start, "Timeout"
        except Exception as e:
            return None, time.time() - start, type(e).__name__
    
    print("\n테스트 진행 중...")
    start_time = time.time()
    
    # 동시 요청 수행
    async with httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=200)) as client:
        tasks = [make_request(client, i) for i in range(1000)]
        results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    
    # 결과 분석
    success_count = 0
    errors = defaultdict(int)
    response_times = []
    status_codes = defaultdict(int)
    
    for status, elapsed, error in results:
        if error:
            errors[error] += 1
        elif status:
            if 200 <= status < 300:
                success_count += 1
            status_codes[status] += 1
            response_times.append(elapsed)
    
    # 통계 계산
    if response_times:
        response_times.sort()
        avg_time = sum(response_times) / len(response_times)
        p50 = response_times[int(len(response_times) * 0.5)]
        p95 = response_times[int(len(response_times) * 0.95)]
        p99 = response_times[int(len(response_times) * 0.99)]
        max_time = response_times[-1]
        min_time = response_times[0]
    else:
        avg_time = p50 = p95 = p99 = max_time = min_time = 0
    
    # 결과 출력
    print(f"\n📊 테스트 결과")
    print(f"총 소요 시간: {total_time:.2f}초")
    print(f"총 요청 수: 1000")
    print(f"성공 요청: {success_count} ({success_count/10:.1f}%)")
    print(f"실패 요청: {1000 - success_count}")
    print(f"RPS (Requests Per Second): {1000/total_time:.1f}")
    
    print(f"\n⏱️ 응답 시간 통계")
    print(f"평균: {avg_time:.3f}초")
    print(f"최소: {min_time:.3f}초")
    print(f"최대: {max_time:.3f}초")
    print(f"P50: {p50:.3f}초")
    print(f"P95: {p95:.3f}초")
    print(f"P99: {p99:.3f}초")
    
    print(f"\n📈 상태 코드 분포")
    for status, count in sorted(status_codes.items()):
        print(f"HTTP {status}: {count}회")
    
    if errors:
        print(f"\n❌ 에러 분석")
        for error, count in sorted(errors.items(), key=lambda x: x[1], reverse=True):
            print(f"{error}: {count}회")
    
    # 서비스 로그 확인
    print("\n📋 서비스 로그 확인")
    import subprocess
    
    # OMS 로그에서 에러 확인
    oms_errors = subprocess.run(
        "tail -100 oms.log | grep -i error | wc -l",
        shell=True, capture_output=True, text=True
    ).stdout.strip()
    print(f"OMS 에러 로그: {oms_errors}개")
    
    # 메모리 사용량 확인
    print("\n💾 메모리 사용량")
    for service, pid in [("OMS", "78213"), ("BFF", "78356"), ("Funnel", "78430")]:
        try:
            mem_info = subprocess.run(
                f"ps -o pid,rss,vsz,%mem -p {pid}",
                shell=True, capture_output=True, text=True
            ).stdout.strip().split('\n')[-1].split()
            if len(mem_info) >= 4:
                print(f"{service}: RSS {int(mem_info[1])/1024:.1f}MB, 메모리 {mem_info[3]}%")
        except:
            pass
    
    # 테스트 DB 정리
    async with httpx.AsyncClient() as client:
        try:
            await client.delete("http://localhost:8000/api/v1/database/load_test_db")
        except:
            pass
    
    # 문제 분석
    if success_count < 900:  # 90% 미만 성공
        print("\n⚠️ 성공률이 90% 미만입니다. 문제 분석이 필요합니다.")
        return False
    
    if p99 > 5.0:  # P99가 5초 초과
        print("\n⚠️ P99 응답시간이 5초를 초과합니다. 성능 최적화가 필요합니다.")
        return False
    
    print("\n✅ 고부하 테스트 통과!")
    return True


if __name__ == "__main__":
    asyncio.run(high_load_test())