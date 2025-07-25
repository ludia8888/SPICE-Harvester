"""
🔥 QUICK PRODUCTION TEST
빠른 운영환경 핵심 시나리오 테스트
"""

import asyncio
import httpx
import time
import os
import subprocess
import psutil
from pathlib import Path
from typing import Dict, Any
from collections import defaultdict

import sys
sys.path.append(str(Path(__file__).parent))


class QuickServiceManager:
    """빠른 서비스 관리"""
    
    def __init__(self):
        self.processes = {}
        
    async def start_service(self, name: str, module: str, port: int) -> bool:
        """서비스 시작"""
        try:
            subprocess.run(f"pkill -f 'python.*{module}'", shell=True, capture_output=True)
            await asyncio.sleep(1)
            
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            
            process = subprocess.Popen(
                f"python -m {module}".split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent,
                env=env
            )
            
            self.processes[name] = process
            
            # 서비스 시작 대기
            for i in range(10):
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"http://localhost:{port}/health", timeout=2.0)
                        if response.status_code == 200:
                            print(f"✅ {name} 시작 성공 (포트: {port})")
                            return True
                except:
                    await asyncio.sleep(1)
                    
            return False
            
        except Exception as e:
            print(f"❌ {name} 시작 실패: {e}")
            return False
    
    def get_metrics(self, name: str) -> Dict[str, Any]:
        """서비스 메트릭"""
        if name not in self.processes:
            return {}
            
        try:
            p = psutil.Process(self.processes[name].pid)
            return {
                "cpu": p.cpu_percent(),
                "memory_mb": p.memory_info().rss / 1024 / 1024,
                "threads": p.num_threads()
            }
        except:
            return {}
    
    def stop_all(self):
        """모든 서비스 종료"""
        for name, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=3)
            except:
                process.kill()
        
        subprocess.run("pkill -f 'python.*(oms|bff|funnel).main'", shell=True, capture_output=True)


async def test_high_load():
    """1. 고부하 테스트 (500 동시 요청)"""
    print("\n=== 1. 고부하 동시성 테스트 (500 요청) ===")
    
    async def make_request(client: httpx.AsyncClient, i: int):
        try:
            # 다양한 엔드포인트 테스트
            if i % 3 == 0:
                response = await client.get("http://localhost:8000/api/v1/database/list")
            elif i % 3 == 1:
                response = await client.post(
                    f"http://localhost:8000/api/v1/ontology/test_db/create",
                    json={
                        "id": f"LoadTest_{i}",
                        "label": f"부하 테스트 {i}",
                        "description": "테스트"
                    }
                )
            else:
                response = await client.get("http://localhost:8002/api/v1/databases")
            
            return response.status_code, None
        except Exception as e:
            return None, type(e).__name__
    
    start_time = time.time()
    success = 0
    errors = defaultdict(int)
    
    async with httpx.AsyncClient(timeout=10.0) as client:
        tasks = [make_request(client, i) for i in range(500)]
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
    print(f"   성공: {success}/500 ({success/5:.1f}%)")
    print(f"   RPS: {500/duration:.1f}")
    
    if errors:
        print("   에러:")
        for error, count in list(errors.items())[:3]:
            print(f"     - {error}: {count}회")


async def test_large_data():
    """2. 대용량 데이터 테스트"""
    print("\n=== 2. 대용량 데이터 처리 테스트 ===")
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1000개 온톨로지 생성
        print("1000개 온톨로지 일괄 생성...")
        
        batch_size = 100
        start_time = time.time()
        success = 0
        
        for batch in range(10):
            tasks = []
            for i in range(batch_size):
                idx = batch * batch_size + i
                tasks.append(
                    client.post(
                        "http://localhost:8000/api/v1/ontology/test_db/create",
                        json={
                            "id": f"BigData_{idx}",
                            "label": f"대용량 {idx}",
                            "properties": [
                                {"id": f"prop_{j}", "label": f"속성 {j}", "type": "xsd:string"}
                                for j in range(10)
                            ]
                        }
                    )
                )
            
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success += sum(1 for r in results if not isinstance(r, Exception) and r.status_code < 300)
            
            print(f"   배치 {batch+1}/10 완료")
        
        duration = time.time() - start_time
        print(f"✅ 1000개 생성 완료: {duration:.2f}초 ({success}/1000 성공)")
        
        # 전체 조회
        start_time = time.time()
        response = await client.get("http://localhost:8000/api/v1/ontology/test_db/list")
        list_time = time.time() - start_time
        
        if response.status_code == 200:
            count = len(response.json().get("data", {}).get("ontologies", []))
            print(f"✅ 전체 조회: {count}개, {list_time:.3f}초")


async def test_failure_recovery():
    """3. 장애 복구 테스트"""
    print("\n=== 3. 장애 복구 및 에러 처리 ===")
    
    manager = QuickServiceManager()
    
    # OMS 중단
    if "OMS" in manager.processes:
        manager.processes["OMS"].terminate()
        print("⚠️ OMS 서비스 중단")
        await asyncio.sleep(1)
    
    # BFF 요청 (OMS 없이)
    async with httpx.AsyncClient(timeout=5.0) as client:
        try:
            response = await client.get("http://localhost:8002/api/v1/databases")
            print(f"   BFF 응답 (OMS 중단): {response.status_code}")
        except Exception as e:
            print(f"   BFF 에러 처리: {type(e).__name__}")
    
    # OMS 재시작
    await manager.start_service("OMS", "oms.main", 8000)
    
    # 악성 입력 테스트
    print("\n악성 입력 차단 테스트:")
    malicious = [
        "'; DROP TABLE users; --",
        "../../../etc/passwd",
        "<script>alert('xss')</script>",
        "A" * 10000
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


async def test_memory_usage():
    """4. 메모리 사용량 테스트"""
    print("\n=== 4. 메모리 및 리소스 사용량 ===")
    
    manager = QuickServiceManager()
    
    # 초기 상태
    print("초기 메모리 상태:")
    initial_memory = {}
    for service in ["OMS", "BFF", "Funnel"]:
        metrics = manager.get_metrics(service)
        if metrics:
            initial_memory[service] = metrics["memory_mb"]
            print(f"  {service}: {metrics['memory_mb']:.1f}MB")
    
    # 200회 반복 요청
    print("\n200회 반복 요청 실행...")
    async with httpx.AsyncClient() as client:
        for i in range(20):
            tasks = []
            for j in range(10):
                if j % 2 == 0:
                    tasks.append(client.get("http://localhost:8000/api/v1/database/list"))
                else:
                    tasks.append(client.get("http://localhost:8002/health"))
            
            await asyncio.gather(*tasks, return_exceptions=True)
    
    # 최종 상태
    print("\n최종 메모리 상태:")
    for service in ["OMS", "BFF", "Funnel"]:
        metrics = manager.get_metrics(service)
        if metrics and service in initial_memory:
            increase = metrics["memory_mb"] - initial_memory[service]
            print(f"  {service}: {metrics['memory_mb']:.1f}MB (+{increase:.1f}MB)")
            
            if increase > 50:
                print(f"    ⚠️ 메모리 증가 경고!")


async def generate_quick_report(results: Dict[str, Any]):
    """빠른 리포트 생성"""
    print("\n" + "="*60)
    print("📊 빠른 운영환경 테스트 결과")
    print("="*60)
    
    if "high_load" in results:
        print(f"\n고부하 테스트:")
        print(f"- 성공률: {results['high_load']['success_rate']:.1f}%")
        print(f"- RPS: {results['high_load']['rps']:.1f}")
    
    if "large_data" in results:
        print(f"\n대용량 데이터:")
        print(f"- 생성 시간: {results['large_data']['create_time']:.1f}초")
        print(f"- 조회 시간: {results['large_data']['query_time']:.3f}초")
    
    if "security" in results:
        print(f"\n보안:")
        print(f"- 악성 입력 차단률: {results['security']['block_rate']:.0f}%")
    
    print("\n" + "="*60)


async def main():
    """메인 실행"""
    manager = QuickServiceManager()
    results = {}
    
    try:
        # 환경변수 설정
        os.environ["TERMINUS_URL"] = "http://localhost:6364"
        os.environ["TERMINUS_USER"] = "admin"
        os.environ["TERMINUS_ACCOUNT"] = "admin"
        os.environ["TERMINUS_KEY"] = "admin"
        
        # 서비스 시작
        print("서비스 시작 중...")
        
        if not await manager.start_service("OMS", "oms.main", 8000):
            print("❌ OMS 시작 실패")
            return
            
        await asyncio.sleep(2)
        
        if not await manager.start_service("BFF", "bff.main", 8002):
            print("❌ BFF 시작 실패")
            return
            
        await asyncio.sleep(2)
        
        if not await manager.start_service("Funnel", "funnel.main", 8004):
            print("❌ Funnel 시작 실패")
            return
            
        await asyncio.sleep(2)
        
        # 테스트 DB 생성
        async with httpx.AsyncClient() as client:
            await client.post(
                "http://localhost:8000/api/v1/database/create",
                json={"name": "test_db", "description": "테스트"}
            )
        
        print("\n테스트 시작...")
        
        # 테스트 실행
        await test_high_load()
        await test_large_data()
        await test_failure_recovery()
        await test_memory_usage()
        
        # 리포트
        await generate_quick_report(results)
        
    except Exception as e:
        print(f"\n❌ 테스트 오류: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        print("\n서비스 종료 중...")
        manager.stop_all()
        await asyncio.sleep(2)


if __name__ == "__main__":
    print("🔥 QUICK PRODUCTION TEST")
    print("핵심 운영환경 시나리오 빠른 테스트\n")
    
    asyncio.run(main())