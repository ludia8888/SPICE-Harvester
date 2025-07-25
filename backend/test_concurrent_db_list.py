"""
동시 데이터베이스 목록 조회 테스트
OMS endpoint 성능 이슈 재현
"""

import asyncio
import httpx
import time
from collections import defaultdict

async def test_concurrent_db_list():
    """동시 데이터베이스 목록 조회 테스트"""
    print("🔍 OMS 데이터베이스 목록 동시 조회 테스트")
    
    async def make_db_list_request(client: httpx.AsyncClient, i: int):
        try:
            start = time.time()
            response = await client.get("http://localhost:8000/api/v1/database/list")
            elapsed = time.time() - start
            return response.status_code, elapsed, None, i
        except httpx.TimeoutException:
            return None, time.time() - start, "Timeout", i
        except Exception as e:
            return None, time.time() - start, type(e).__name__, i
    
    # 50개 동시 요청으로 문제 재현 시도
    print("\n📊 50개 동시 요청 실행...")
    start_time = time.time()
    
    async with httpx.AsyncClient(timeout=30.0, limits=httpx.Limits(max_connections=50)) as client:
        tasks = [make_db_list_request(client, i) for i in range(50)]
        results = await asyncio.gather(*tasks)
    
    total_time = time.time() - start_time
    
    # 결과 분석
    success_count = 0
    errors = defaultdict(int)
    response_times = []
    status_codes = defaultdict(int)
    slow_requests = []
    
    for status, elapsed, error, req_id in results:
        if error:
            errors[error] += 1
        elif status:
            if 200 <= status < 300:
                success_count += 1
            status_codes[status] += 1
            response_times.append(elapsed)
            
            # 1초 이상 걸린 요청들 기록
            if elapsed > 1.0:
                slow_requests.append((req_id, elapsed, status))
    
    # 통계 계산
    if response_times:
        response_times.sort()
        avg_time = sum(response_times) / len(response_times)
        p95 = response_times[int(len(response_times) * 0.95)]
        p99 = response_times[int(len(response_times) * 0.99)]
        max_time = response_times[-1]
        min_time = response_times[0]
    else:
        avg_time = p95 = p99 = max_time = min_time = 0
    
    # 결과 출력
    print(f"\n📊 테스트 결과")
    print(f"총 소요 시간: {total_time:.2f}초")
    print(f"총 요청 수: 50")
    print(f"성공 요청: {success_count} ({success_count*2:.0f}%)")
    print(f"실패 요청: {50 - success_count}")
    
    print(f"\n⏱️ 응답 시간 통계")
    print(f"평균: {avg_time:.3f}초")
    print(f"최소: {min_time:.3f}초")
    print(f"최대: {max_time:.3f}초")
    print(f"P95: {p95:.3f}초")
    print(f"P99: {p99:.3f}초")
    
    print(f"\n📈 상태 코드 분포")
    for status, count in sorted(status_codes.items()):
        print(f"HTTP {status}: {count}회")
    
    if errors:
        print(f"\n❌ 에러 분석")
        for error, count in sorted(errors.items(), key=lambda x: x[1], reverse=True):
            print(f"{error}: {count}회")
    
    if slow_requests:
        print(f"\n🐌 느린 요청들 (1초 이상)")
        for req_id, elapsed, status in slow_requests:
            print(f"요청 #{req_id}: {elapsed:.3f}초 (HTTP {status})")
    
    # 성능 이슈 감지
    if max_time > 10.0:
        print(f"\n⚠️ 성능 이슈 감지: 최대 응답시간 {max_time:.3f}초")
        return False
    
    if success_count < 45:  # 90% 미만 성공
        print(f"\n⚠️ 성공률 이슈 감지: {success_count*2:.0f}%")
        return False
    
    print(f"\n✅ 동시 접근 테스트 통과!")
    return True

if __name__ == "__main__":
    asyncio.run(test_concurrent_db_list())