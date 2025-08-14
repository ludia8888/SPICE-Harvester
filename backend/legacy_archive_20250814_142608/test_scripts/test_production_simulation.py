"""
🔥 ULTRA PRODUCTION SIMULATION TEST
실제 운영환경 시뮬레이션 및 고난이도 시나리오 기반 종합 성능 테스트
"""

import asyncio
import httpx
import json
import sys
import time
import os
import random
import tracemalloc
import psutil
import subprocess
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass
from collections import defaultdict
import statistics

# 프로젝트 루트 경로 추가
sys.path.append(str(Path(__file__).parent))


@dataclass
class TestResult:
    """테스트 결과 저장 클래스"""
    test_name: str
    success_count: int
    failure_count: int
    avg_response_time: float
    max_response_time: float
    min_response_time: float
    p95_response_time: float
    p99_response_time: float
    error_types: Dict[str, int]
    start_time: datetime
    end_time: datetime
    
    @property
    def total_requests(self) -> int:
        return self.success_count + self.failure_count
    
    @property
    def success_rate(self) -> float:
        if self.total_requests == 0:
            return 0.0
        return (self.success_count / self.total_requests) * 100
    
    @property
    def duration(self) -> float:
        return (self.end_time - self.start_time).total_seconds()
    
    @property
    def requests_per_second(self) -> float:
        if self.duration == 0:
            return 0.0
        return self.total_requests / self.duration


class ServiceManager:
    """서비스 프로세스 관리"""
    
    def __init__(self):
        self.processes = {}
        self.start_times = {}
        
    async def start_service(self, name: str, module: str, port: int) -> bool:
        """서비스 시작"""
        try:
            # 기존 프로세스 종료
            subprocess.run(f"pkill -f 'python.*{module}'", shell=True, capture_output=True)
            await asyncio.sleep(1)
            
            # 새 프로세스 시작
            env = os.environ.copy()
            env["PYTHONUNBUFFERED"] = "1"
            
            cmd = f"python -m {module}"
            process = subprocess.Popen(
                cmd.split(),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                cwd=Path(__file__).parent,
                env=env
            )
            
            self.processes[name] = process
            self.start_times[name] = time.time()
            
            # 서비스가 시작될 때까지 대기
            for i in range(20):
                try:
                    async with httpx.AsyncClient() as client:
                        response = await client.get(f"http://localhost:{port}/health", timeout=2.0)
                        if response.status_code == 200:
                            print(f"✅ {name} 서비스 시작 성공 (포트: {port}, 시작 시간: {i+1}초)")
                            return True
                except:
                    await asyncio.sleep(1)
                    
            print(f"❌ {name} 서비스 시작 실패")
            return False
            
        except Exception as e:
            print(f"❌ {name} 서비스 시작 중 오류: {e}")
            return False
    
    def get_service_metrics(self, name: str) -> Dict[str, Any]:
        """서비스 메트릭 수집"""
        if name not in self.processes:
            return {}
            
        process = self.processes[name]
        try:
            p = psutil.Process(process.pid)
            return {
                "cpu_percent": p.cpu_percent(),
                "memory_mb": p.memory_info().rss / 1024 / 1024,
                "num_threads": p.num_threads(),
                "uptime_seconds": time.time() - self.start_times[name]
            }
        except:
            return {}
    
    def stop_all(self):
        """모든 서비스 종료"""
        for name, process in self.processes.items():
            try:
                process.terminate()
                process.wait(timeout=5)
                print(f"✅ {name} 서비스 종료")
            except:
                process.kill()
                print(f"⚠️ {name} 서비스 강제 종료")
        
        # 추가 정리
        subprocess.run("pkill -f 'python.*oms.main'", shell=True, capture_output=True)
        subprocess.run("pkill -f 'python.*bff.main'", shell=True, capture_output=True)
        subprocess.run("pkill -f 'python.*funnel.main'", shell=True, capture_output=True)


class ProductionSimulator:
    """운영환경 시뮬레이터"""
    
    def __init__(self):
        self.manager = ServiceManager()
        self.results = []
        self.test_data = self._generate_test_data()
        
    def _generate_test_data(self) -> Dict[str, Any]:
        """테스트 데이터 생성"""
        return {
            "databases": [f"test_db_{i}" for i in range(10)],
            "ontologies": [
                {
                    "id": f"Class_{i}",
                    "label": f"테스트 클래스 {i}",
                    "description": f"테스트 설명 {i}" * 10,
                    "properties": [
                        {
                            "id": f"prop_{j}",
                            "label": f"속성 {j}",
                            "type": random.choice(["xsd:string", "xsd:integer", "xsd:decimal", "xsd:boolean"])
                        } for j in range(5)
                    ],
                    "relationships": [
                        {
                            "type": "subClassOf",
                            "target": f"Class_{(i+1)%100}"
                        }
                    ]
                } for i in range(100)
            ],
            "malicious_inputs": [
                "'; DROP TABLE users; --",
                "../../../etc/passwd",
                "<script>alert('xss')</script>",
                "test\x00null",
                "test%00null",
                "A" * 10000,  # 매우 긴 문자열
                {"@id": "x" * 1000},  # 긴 ID
                {"nested": {"nested": {"nested": {"nested": {}}}}},  # 깊은 중첩
            ]
        }
    
    async def measure_request(self, client: httpx.AsyncClient, method: str, url: str, 
                            json_data: Optional[Dict] = None) -> Tuple[float, Optional[int], Optional[str]]:
        """단일 요청 측정"""
        start_time = time.time()
        try:
            if method == "GET":
                response = await client.get(url, timeout=30.0)
            elif method == "POST":
                response = await client.post(url, json=json_data, timeout=30.0)
            elif method == "DELETE":
                response = await client.delete(url, timeout=30.0)
            else:
                raise ValueError(f"Unsupported method: {method}")
                
            response_time = time.time() - start_time
            return response_time, response.status_code, None
            
        except httpx.TimeoutException:
            return time.time() - start_time, None, "Timeout"
        except Exception as e:
            return time.time() - start_time, None, str(type(e).__name__)
    
    async def run_concurrent_load(self, num_requests: int, num_workers: int, 
                                test_func) -> TestResult:
        """동시성 부하 테스트 실행"""
        print(f"\n동시 요청 수: {num_requests}, 워커 수: {num_workers}")
        
        response_times = []
        errors = defaultdict(int)
        success_count = 0
        failure_count = 0
        start_time = datetime.now()
        
        # 워커별 작업 분배
        tasks_per_worker = num_requests // num_workers
        remaining_tasks = num_requests % num_workers
        
        async def worker(worker_id: int, num_tasks: int):
            nonlocal success_count, failure_count
            
            async with httpx.AsyncClient(timeout=30.0) as client:
                for i in range(num_tasks):
                    response_time, status_code, error = await test_func(client, worker_id, i)
                    response_times.append(response_time)
                    
                    if error:
                        failure_count += 1
                        errors[error] += 1
                    elif status_code and 200 <= status_code < 300:
                        success_count += 1
                    else:
                        failure_count += 1
                        errors[f"HTTP_{status_code}"] += 1
        
        # 모든 워커 동시 실행
        workers = []
        for i in range(num_workers):
            tasks = tasks_per_worker + (1 if i < remaining_tasks else 0)
            workers.append(worker(i, tasks))
        
        await asyncio.gather(*workers)
        
        end_time = datetime.now()
        
        # 통계 계산
        if response_times:
            response_times.sort()
            avg_time = statistics.mean(response_times)
            p95_time = response_times[int(len(response_times) * 0.95)]
            p99_time = response_times[int(len(response_times) * 0.99)]
        else:
            avg_time = p95_time = p99_time = 0.0
        
        return TestResult(
            test_name="Concurrent Load Test",
            success_count=success_count,
            failure_count=failure_count,
            avg_response_time=avg_time,
            max_response_time=max(response_times) if response_times else 0.0,
            min_response_time=min(response_times) if response_times else 0.0,
            p95_response_time=p95_time,
            p99_response_time=p99_time,
            error_types=dict(errors),
            start_time=start_time,
            end_time=end_time
        )
    
    async def test_high_concurrency(self):
        """1. 고부하 동시성 테스트"""
        print("\n=== 1. 고부하 동시성 테스트 ===")
        
        async def ontology_create_test(client: httpx.AsyncClient, worker_id: int, task_id: int):
            """온톨로지 생성 테스트"""
            db_name = f"concurrent_db_{worker_id % 5}"
            ontology_data = self.test_data["ontologies"][task_id % len(self.test_data["ontologies"])].copy()
            ontology_data["id"] = f"{ontology_data['id']}_w{worker_id}_t{task_id}"
            
            return await self.measure_request(
                client, "POST",
                f"http://localhost:8000/api/v1/ontology/{db_name}/create",
                ontology_data
            )
        
        # 점진적 부하 증가
        for num_requests, num_workers in [(100, 10), (500, 50), (1000, 100)]:
            result = await self.run_concurrent_load(num_requests, num_workers, ontology_create_test)
            self.results.append(result)
            
            print(f"✅ {num_requests} 요청 완료:")
            print(f"   - 성공률: {result.success_rate:.2f}%")
            print(f"   - 평균 응답시간: {result.avg_response_time:.3f}초")
            print(f"   - P95 응답시간: {result.p95_response_time:.3f}초")
            print(f"   - P99 응답시간: {result.p99_response_time:.3f}초")
            print(f"   - RPS: {result.requests_per_second:.2f}")
            
            # 서비스 메트릭 출력
            for service in ["OMS", "BFF", "Funnel"]:
                metrics = self.manager.get_service_metrics(service)
                if metrics:
                    print(f"   - {service}: CPU {metrics['cpu_percent']:.1f}%, "
                          f"메모리 {metrics['memory_mb']:.1f}MB, "
                          f"스레드 {metrics['num_threads']}")
    
    async def test_large_data_processing(self):
        """2. 대용량 데이터 처리 테스트"""
        print("\n=== 2. 대용량 데이터 처리 테스트 ===")
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            # 테스트 DB 생성
            await client.post("http://localhost:8000/api/v1/database/create",
                            json={"name": "large_data_test", "description": "대용량 테스트"})
            
            # 10,000개 온톨로지 생성
            print("10,000개 온톨로지 생성 중...")
            batch_size = 100
            total_time = 0
            
            for batch in range(100):
                batch_start = time.time()
                tasks = []
                
                for i in range(batch_size):
                    idx = batch * batch_size + i
                    ontology = {
                        "id": f"LargeClass_{idx}",
                        "label": f"대용량 클래스 {idx}",
                        "description": f"설명 {idx}" * 50,
                        "properties": [
                            {
                                "id": f"large_prop_{j}",
                                "label": f"대용량 속성 {j}",
                                "type": "xsd:string"
                            } for j in range(20)
                        ]
                    }
                    
                    tasks.append(
                        self.measure_request(
                            client, "POST",
                            "http://localhost:8000/api/v1/ontology/large_data_test/create",
                            ontology
                        )
                    )
                
                results = await asyncio.gather(*tasks)
                batch_time = time.time() - batch_start
                total_time += batch_time
                
                success = sum(1 for _, status, error in results if status and 200 <= status < 300)
                print(f"   배치 {batch+1}/100 완료: {success}/{batch_size} 성공, {batch_time:.2f}초")
            
            print(f"\n✅ 10,000개 온톨로지 생성 완료: 총 {total_time:.2f}초")
            
            # 대용량 쿼리 테스트
            print("\n대용량 데이터 쿼리 테스트...")
            
            # 전체 목록 조회
            start_time = time.time()
            response = await client.get("http://localhost:8000/api/v1/ontology/large_data_test/list")
            list_time = time.time() - start_time
            
            if response.status_code == 200:
                data = response.json()
                ontology_count = len(data.get("data", {}).get("ontologies", []))
                print(f"✅ 전체 목록 조회: {ontology_count}개, {list_time:.3f}초")
            
            # 복잡한 쿼리
            complex_query = {
                "filters": {
                    "label": {"contains": "대용량"},
                    "properties": {"size": {"gte": 10}}
                },
                "limit": 1000
            }
            
            start_time = time.time()
            response = await client.post(
                "http://localhost:8000/api/v1/ontology/large_data_test/query",
                json=complex_query
            )
            query_time = time.time() - start_time
            print(f"✅ 복잡한 쿼리 실행: {query_time:.3f}초")
            
            # 정리
            await client.delete("http://localhost:8000/api/v1/database/large_data_test")
    
    async def test_failure_recovery(self):
        """3. 장애 복구 및 에러 처리 테스트"""
        print("\n=== 3. 장애 복구 및 에러 처리 테스트 ===")
        
        async with httpx.AsyncClient(timeout=10.0) as client:
            # 3-1. 서비스 재시작 테스트
            print("\n3-1. 서비스 재시작 중 요청 처리")
            
            # OMS 서비스 중단
            if "OMS" in self.manager.processes:
                process = self.manager.processes["OMS"]
                process.terminate()
                print("⚠️ OMS 서비스 중단됨")
            
            # 중단된 상태에서 요청
            failed_requests = 0
            for i in range(5):
                try:
                    response = await client.get("http://localhost:8000/health", timeout=2.0)
                    if response.status_code != 200:
                        failed_requests += 1
                except:
                    failed_requests += 1
                await asyncio.sleep(0.5)
            
            print(f"   서비스 중단 중 실패한 요청: {failed_requests}/5")
            
            # OMS 재시작
            await self.manager.start_service("OMS", "oms.main", 8000)
            
            # 재시작 후 요청
            success_requests = 0
            for i in range(5):
                try:
                    response = await client.get("http://localhost:8000/health", timeout=2.0)
                    if response.status_code == 200:
                        success_requests += 1
                except:
                    pass
                await asyncio.sleep(0.5)
            
            print(f"   서비스 재시작 후 성공한 요청: {success_requests}/5")
            
            # 3-2. 악성 입력 처리
            print("\n3-2. 악성 입력 처리 테스트")
            
            blocked_count = 0
            for malicious_input in self.test_data["malicious_inputs"]:
                try:
                    if isinstance(malicious_input, str):
                        # 데이터베이스 이름으로 악성 입력
                        response = await client.post(
                            "http://localhost:8000/api/v1/database/create",
                            json={"name": malicious_input, "description": "test"}
                        )
                    else:
                        # 온톨로지 데이터로 악성 입력
                        response = await client.post(
                            "http://localhost:8000/api/v1/ontology/test_db/create",
                            json=malicious_input
                        )
                    
                    if response.status_code in [400, 422]:
                        blocked_count += 1
                except:
                    blocked_count += 1
            
            print(f"✅ 악성 입력 차단: {blocked_count}/{len(self.test_data['malicious_inputs'])}")
            
            # 3-3. 타임아웃 처리
            print("\n3-3. 타임아웃 처리 테스트")
            
            # 매우 큰 데이터로 타임아웃 유도
            huge_data = {
                "id": "TimeoutTest",
                "label": "타임아웃 테스트",
                "description": "A" * 1000000,  # 1MB 텍스트
                "properties": [
                    {"id": f"prop_{i}", "label": f"속성 {i}", "type": "xsd:string"}
                    for i in range(1000)
                ]
            }
            
            timeout_count = 0
            for i in range(3):
                try:
                    response = await client.post(
                        "http://localhost:8000/api/v1/ontology/test_db/create",
                        json=huge_data,
                        timeout=2.0  # 2초 타임아웃
                    )
                except httpx.TimeoutException:
                    timeout_count += 1
            
            print(f"✅ 타임아웃 처리: {timeout_count}/3 요청이 타임아웃됨")
    
    async def test_memory_and_resources(self):
        """4. 메모리 누수 및 리소스 관리 테스트"""
        print("\n=== 4. 메모리 누수 및 리소스 관리 테스트 ===")
        
        # 메모리 추적 시작
        tracemalloc.start()
        initial_memory = {}
        
        # 초기 메모리 상태
        for service in ["OMS", "BFF", "Funnel"]:
            metrics = self.manager.get_service_metrics(service)
            if metrics:
                initial_memory[service] = metrics["memory_mb"]
                print(f"{service} 초기 메모리: {metrics['memory_mb']:.1f}MB")
        
        async with httpx.AsyncClient() as client:
            # 1000회 반복 요청으로 메모리 누수 확인
            print("\n1000회 반복 요청 실행 중...")
            
            for i in range(10):
                tasks = []
                for j in range(100):
                    # 다양한 엔드포인트 호출
                    if j % 3 == 0:
                        tasks.append(client.get("http://localhost:8000/api/v1/database/list"))
                    elif j % 3 == 1:
                        tasks.append(client.get("http://localhost:8002/api/v1/databases"))
                    else:
                        tasks.append(client.get("http://localhost:8003/health"))
                
                await asyncio.gather(*tasks, return_exceptions=True)
                
                if (i + 1) % 2 == 0:
                    print(f"   {(i + 1) * 100}회 완료")
                    
                    # 메모리 체크
                    for service in ["OMS", "BFF", "Funnel"]:
                        metrics = self.manager.get_service_metrics(service)
                        if metrics and service in initial_memory:
                            memory_increase = metrics["memory_mb"] - initial_memory[service]
                            print(f"   {service}: {metrics['memory_mb']:.1f}MB "
                                  f"(+{memory_increase:.1f}MB)")
        
        # 최종 메모리 상태
        print("\n최종 메모리 상태:")
        memory_leaks = []
        
        for service in ["OMS", "BFF", "Funnel"]:
            metrics = self.manager.get_service_metrics(service)
            if metrics and service in initial_memory:
                memory_increase = metrics["memory_mb"] - initial_memory[service]
                print(f"{service}: {metrics['memory_mb']:.1f}MB (+{memory_increase:.1f}MB)")
                
                # 50MB 이상 증가시 메모리 누수 의심
                if memory_increase > 50:
                    memory_leaks.append(f"{service} (+{memory_increase:.1f}MB)")
        
        if memory_leaks:
            print(f"\n⚠️ 메모리 누수 의심: {', '.join(memory_leaks)}")
        else:
            print("\n✅ 메모리 누수 없음")
        
        # 메모리 스냅샷
        snapshot = tracemalloc.take_snapshot()
        top_stats = snapshot.statistics('lineno')[:5]
        
        print("\n메모리 사용 상위 5개:")
        for stat in top_stats:
            print(f"   {stat}")
        
        tracemalloc.stop()
    
    async def test_network_issues(self):
        """5. 네트워크 지연 및 타임아웃 시나리오"""
        print("\n=== 5. 네트워크 지연 및 타임아웃 시나리오 ===")
        
        # 다양한 타임아웃 설정으로 테스트
        timeout_scenarios = [
            (0.5, "극도로 짧은 타임아웃"),
            (1.0, "짧은 타임아웃"),
            (5.0, "보통 타임아웃"),
            (30.0, "긴 타임아웃")
        ]
        
        for timeout, description in timeout_scenarios:
            print(f"\n{description} ({timeout}초) 테스트:")
            
            success_count = 0
            timeout_count = 0
            
            async with httpx.AsyncClient(timeout=timeout) as client:
                for i in range(10):
                    try:
                        # 복잡한 쿼리로 처리 시간 증가
                        response = await client.post(
                            "http://localhost:8000/api/v1/ontology/test_db/query",
                            json={
                                "filters": {"label": {"contains": "test"}},
                                "limit": 1000
                            }
                        )
                        if response.status_code == 200:
                            success_count += 1
                    except httpx.TimeoutException:
                        timeout_count += 1
                    except Exception:
                        pass
            
            print(f"   성공: {success_count}/10, 타임아웃: {timeout_count}/10")
        
        # 네트워크 지연 시뮬레이션 (프록시 사용)
        print("\n네트워크 지연 시뮬레이션:")
        
        # 인위적 지연 추가
        async def delayed_request(delay: float):
            await asyncio.sleep(delay)
            async with httpx.AsyncClient() as client:
                return await client.get("http://localhost:8000/health")
        
        delays = [0.1, 0.5, 1.0, 2.0]
        for delay in delays:
            start_time = time.time()
            try:
                response = await delayed_request(delay)
                total_time = time.time() - start_time
                print(f"   {delay}초 지연: 총 {total_time:.2f}초, 상태 {response.status_code}")
            except Exception as e:
                print(f"   {delay}초 지연: 실패 - {type(e).__name__}")
    
    async def test_service_communication(self):
        """6. 서비스 간 통신 장애 시나리오"""
        print("\n=== 6. 서비스 간 통신 장애 시나리오 ===")
        
        async with httpx.AsyncClient() as client:
            # 6-1. OMS 중단 시 BFF 동작
            print("\n6-1. OMS 중단 시 BFF 응답:")
            
            # OMS 중단
            if "OMS" in self.manager.processes:
                self.manager.processes["OMS"].terminate()
                await asyncio.sleep(2)
            
            # BFF 요청
            try:
                response = await client.get("http://localhost:8002/api/v1/databases")
                print(f"   BFF 응답: {response.status_code}")
                if response.status_code >= 500:
                    print("   ✅ BFF가 OMS 장애를 적절히 처리함")
            except Exception as e:
                print(f"   BFF 오류: {type(e).__name__}")
            
            # OMS 재시작
            await self.manager.start_service("OMS", "oms.main", 8000)
            
            # 6-2. Funnel 중단 시 BFF 동작
            print("\n6-2. Funnel 중단 시 BFF 타입 추론:")
            
            # Funnel 중단
            if "Funnel" in self.manager.processes:
                self.manager.processes["Funnel"].terminate()
                await asyncio.sleep(2)
            
            # BFF에서 타입 추론 요청
            try:
                response = await client.post(
                    "http://localhost:8002/api/v1/type-inference/infer",
                    json={"data": ["2024-01-01", "2024-01-02", "2024-01-03"]}
                )
                print(f"   타입 추론 응답: {response.status_code}")
            except Exception as e:
                print(f"   ✅ Funnel 장애 시 적절한 에러: {type(e).__name__}")
            
            # Funnel 재시작
            await self.manager.start_service("Funnel", "funnel.main", 8003)
    
    async def test_database_pool_exhaustion(self):
        """7. 데이터베이스 연결 풀 고갈 테스트"""
        print("\n=== 7. 데이터베이스 연결 풀 고갈 테스트 ===")
        
        # 동시에 많은 DB 작업 요청
        async def db_heavy_operation(client: httpx.AsyncClient, op_id: int):
            operations = [
                # DB 생성
                ("POST", "http://localhost:8000/api/v1/database/create", 
                 {"name": f"pool_test_{op_id}", "description": "풀 테스트"}),
                # 온톨로지 생성
                ("POST", f"http://localhost:8000/api/v1/ontology/pool_test_{op_id}/create",
                 {"id": f"PoolClass_{op_id}", "label": "풀 클래스"}),
                # 쿼리
                ("POST", f"http://localhost:8000/api/v1/ontology/pool_test_{op_id}/query",
                 {"filters": {}, "limit": 100}),
                # 삭제
                ("DELETE", f"http://localhost:8000/api/v1/database/pool_test_{op_id}", None)
            ]
            
            results = []
            for method, url, data in operations:
                response_time, status, error = await self.measure_request(client, method, url, data)
                results.append((response_time, status, error))
            
            return results
        
        # 100개 동시 DB 작업
        print("100개 동시 DB 작업 실행 중...")
        
        start_time = time.time()
        async with httpx.AsyncClient(timeout=30.0) as client:
            tasks = [db_heavy_operation(client, i) for i in range(100)]
            results = await asyncio.gather(*tasks, return_exceptions=True)
        
        total_time = time.time() - start_time
        
        # 결과 분석
        success_count = 0
        error_count = 0
        timeout_count = 0
        
        for result in results:
            if isinstance(result, Exception):
                error_count += 1
            else:
                for _, status, error in result:
                    if error == "Timeout":
                        timeout_count += 1
                    elif error:
                        error_count += 1
                    elif status and 200 <= status < 300:
                        success_count += 1
        
        print(f"\n✅ DB 풀 테스트 완료 ({total_time:.2f}초):")
        print(f"   성공: {success_count}")
        print(f"   오류: {error_count}")
        print(f"   타임아웃: {timeout_count}")
    
    async def generate_report(self):
        """종합 성능 리포트 생성"""
        print("\n" + "="*80)
        print("📊 종합 성능 테스트 리포트")
        print("="*80)
        
        # 전체 테스트 요약
        total_requests = sum(r.total_requests for r in self.results)
        total_success = sum(r.success_count for r in self.results)
        overall_success_rate = (total_success / total_requests * 100) if total_requests > 0 else 0
        
        print(f"\n전체 요약:")
        print(f"- 총 요청 수: {total_requests:,}")
        print(f"- 성공 요청: {total_success:,}")
        print(f"- 전체 성공률: {overall_success_rate:.2f}%")
        
        # 응답 시간 통계
        all_response_times = []
        for r in self.results:
            if r.avg_response_time > 0:
                all_response_times.extend([r.avg_response_time] * r.total_requests)
        
        if all_response_times:
            print(f"\n응답 시간 통계:")
            print(f"- 평균: {statistics.mean(all_response_times):.3f}초")
            print(f"- 중앙값: {statistics.median(all_response_times):.3f}초")
            print(f"- 표준편차: {statistics.stdev(all_response_times):.3f}초")
        
        # 서비스별 최종 상태
        print(f"\n서비스 상태:")
        for service in ["OMS", "BFF", "Funnel"]:
            metrics = self.manager.get_service_metrics(service)
            if metrics:
                print(f"- {service}:")
                print(f"  - CPU: {metrics['cpu_percent']:.1f}%")
                print(f"  - 메모리: {metrics['memory_mb']:.1f}MB")
                print(f"  - 스레드: {metrics['num_threads']}")
                print(f"  - 가동시간: {metrics['uptime_seconds']:.0f}초")
        
        # 주요 발견사항
        print(f"\n주요 발견사항:")
        
        # 성능 이슈 체크
        performance_issues = []
        for r in self.results:
            if r.p99_response_time > 5.0:
                performance_issues.append(f"P99 응답시간 {r.p99_response_time:.1f}초")
            if r.success_rate < 95.0:
                performance_issues.append(f"성공률 {r.success_rate:.1f}%")
        
        if performance_issues:
            print("⚠️ 성능 이슈:")
            for issue in performance_issues[:5]:
                print(f"  - {issue}")
        else:
            print("✅ 심각한 성능 이슈 없음")
        
        # 에러 분석
        all_errors = defaultdict(int)
        for r in self.results:
            for error_type, count in r.error_types.items():
                all_errors[error_type] += count
        
        if all_errors:
            print("\n⚠️ 발생한 에러 유형:")
            for error_type, count in sorted(all_errors.items(), key=lambda x: x[1], reverse=True)[:5]:
                print(f"  - {error_type}: {count}회")
        
        print("\n" + "="*80)
    
    async def run_all_tests(self):
        """모든 테스트 실행"""
        try:
            # 서비스 시작
            print("서비스 시작 중...")
            
            # 데이터베이스 초기화를 위한 환경변수 설정
            os.environ["TERMINUS_URL"] = "http://localhost:6364"
            os.environ["TERMINUS_USER"] = "admin"
            os.environ["TERMINUS_ACCOUNT"] = "admin"
            os.environ["TERMINUS_KEY"] = "admin"
            
            # 서비스 시작
            if not await self.manager.start_service("OMS", "oms.main", 8000):
                print("❌ OMS 시작 실패. 테스트 중단.")
                return
            
            await asyncio.sleep(2)
            
            if not await self.manager.start_service("BFF", "bff.main", 8002):
                print("❌ BFF 시작 실패. 테스트 중단.")
                return
                
            await asyncio.sleep(2)
            
            if not await self.manager.start_service("Funnel", "funnel.main", 8004):
                print("❌ Funnel 시작 실패. 테스트 중단.")
                return
                
            await asyncio.sleep(2)
            
            # 테스트 DB 준비
            async with httpx.AsyncClient() as client:
                for db_name in ["test_db", "concurrent_db_0", "concurrent_db_1", 
                               "concurrent_db_2", "concurrent_db_3", "concurrent_db_4"]:
                    try:
                        await client.post(
                            "http://localhost:8000/api/v1/database/create",
                            json={"name": db_name, "description": "테스트 DB"}
                        )
                    except:
                        pass
            
            print("\n테스트 시작...\n")
            
            # 모든 테스트 실행
            await self.test_high_concurrency()
            await self.test_large_data_processing()
            await self.test_failure_recovery()
            await self.test_memory_and_resources()
            await self.test_network_issues()
            await self.test_service_communication()
            await self.test_database_pool_exhaustion()
            
            # 리포트 생성
            await self.generate_report()
            
        except Exception as e:
            print(f"\n❌ 테스트 중 오류 발생: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            print("\n서비스 종료 중...")
            self.manager.stop_all()
            await asyncio.sleep(2)


async def main():
    """메인 실행 함수"""
    simulator = ProductionSimulator()
    await simulator.run_all_tests()


if __name__ == "__main__":
    print("🔥 ULTRA PRODUCTION SIMULATION TEST 시작")
    print("실제 운영환경을 시뮬레이션하는 고난이도 시나리오 테스트\n")
    
    asyncio.run(main())