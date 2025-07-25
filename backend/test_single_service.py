"""
빠른 단일 서비스 테스트
환경 설정 문제 해결을 위한 최소한의 테스트
"""

import asyncio
import httpx
import os
import time
from pathlib import Path
import sys

# .env 파일 로드
from dotenv import load_dotenv
load_dotenv()

sys.path.append(str(Path(__file__).parent))


async def test_terminus_connection():
    """TerminusDB 연결 테스트"""
    print("\n=== TerminusDB 연결 테스트 ===")
    
    url = os.getenv("TERMINUS_SERVER_URL", "http://localhost:6364")
    user = os.getenv("TERMINUS_USER", "admin")
    password = os.getenv("TERMINUS_KEY", "admin")
    
    print(f"URL: {url}")
    print(f"User: {user}")
    print(f"Password: {password}")
    
    try:
        # AsyncTerminusService 직접 테스트
        from oms.services.async_terminus import AsyncTerminusService
        from shared.models.config import ConnectionConfig
        
        config = ConnectionConfig(
            server_url=url,
            user=user,
            key=password,
            account=user
        )
        
        terminus = AsyncTerminusService(connection_info=config)
        
        # 연결 테스트
        result = await terminus.check_connection()
        print(f"✅ TerminusDB 연결 성공: {result}")
        
        # 데이터베이스 목록 조회
        databases = await terminus.list_databases()
        print(f"✅ 데이터베이스 목록: {databases}")
        
        return True
        
    except Exception as e:
        print(f"❌ TerminusDB 연결 실패: {e}")
        return False


async def test_oms_service():
    """OMS 서비스 테스트"""
    print("\n=== OMS 서비스 테스트 ===")
    
    # OMS 서버 시작 (백그라운드)
    import subprocess
    
    env = os.environ.copy()
    process = subprocess.Popen(
        ["python", "-m", "oms.main"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        cwd=Path(__file__).parent,
        env=env
    )
    
    print("OMS 시작 중...")
    
    # 시작 대기
    await asyncio.sleep(3)
    
    try:
        async with httpx.AsyncClient() as client:
            # Health check
            response = await client.get("http://localhost:8000/health")
            print(f"Health check: {response.status_code}")
            
            if response.status_code == 200:
                print("✅ OMS 서비스 정상 작동")
                print(f"응답: {response.json()}")
                return True
            else:
                print(f"❌ OMS 서비스 응답 오류: {response.status_code}")
                return False
                
    except Exception as e:
        print(f"❌ OMS 서비스 접속 실패: {e}")
        return False
        
    finally:
        # 프로세스 종료
        process.terminate()
        process.wait()
        print("OMS 서비스 종료")


async def test_all_services():
    """모든 서비스 통합 테스트"""
    print("\n=== 모든 서비스 통합 테스트 ===")
    
    import subprocess
    
    services = {
        "OMS": {"module": "oms.main", "port": 8000},
        "BFF": {"module": "bff.main", "port": 8002},
        "Funnel": {"module": "funnel.main", "port": 8004}
    }
    
    processes = {}
    
    # 서비스 시작
    for name, info in services.items():
        env = os.environ.copy()
        process = subprocess.Popen(
            ["python", "-m", info["module"]],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            cwd=Path(__file__).parent,
            env=env
        )
        processes[name] = process
        print(f"{name} 시작 중...")
    
    # 시작 대기
    await asyncio.sleep(5)
    
    # 각 서비스 테스트
    results = {}
    async with httpx.AsyncClient() as client:
        for name, info in services.items():
            try:
                response = await client.get(f"http://localhost:{info['port']}/health")
                results[name] = response.status_code == 200
                print(f"{name}: {'✅' if results[name] else '❌'} ({response.status_code})")
            except Exception as e:
                results[name] = False
                print(f"{name}: ❌ ({type(e).__name__})")
    
    # 프로세스 종료
    for name, process in processes.items():
        process.terminate()
        process.wait()
    
    print("\n서비스 종료 완료")
    
    # 결과 요약
    success_count = sum(1 for v in results.values() if v)
    print(f"\n결과: {success_count}/{len(services)} 서비스 정상")
    
    return success_count == len(services)


async def main():
    """메인 실행 함수"""
    print("🔧 환경 설정 테스트")
    
    # 1. TerminusDB 연결 테스트
    terminus_ok = await test_terminus_connection()
    
    if not terminus_ok:
        print("\n⚠️ TerminusDB 연결 실패. 설정을 확인하세요.")
        return
    
    # 2. OMS 단독 테스트
    oms_ok = await test_oms_service()
    
    if not oms_ok:
        print("\n⚠️ OMS 서비스 실행 실패.")
        return
    
    # 3. 모든 서비스 테스트
    all_ok = await test_all_services()
    
    if all_ok:
        print("\n✅ 모든 서비스가 정상적으로 작동합니다!")
    else:
        print("\n⚠️ 일부 서비스에 문제가 있습니다.")


if __name__ == "__main__":
    asyncio.run(main())