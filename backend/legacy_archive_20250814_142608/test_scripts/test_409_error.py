"""
409 에러 처리 검증 테스트
"""

import httpx
import asyncio

async def test_409_error():
    print('🔍 409 에러 처리 검증 테스트')
    
    async with httpx.AsyncClient(timeout=30.0) as client:
        # 1. TestDuplicate 온톨로지 생성
        print('\n1️⃣ TestDuplicate 온톨로지 첫 번째 생성 시도...')
        response1 = await client.post(
            'http://localhost:8000/api/v1/ontology/test_error_db/create',
            json={
                'id': 'TestDuplicate',
                'label': '중복 테스트',
                'description': '409 에러 검증용'
            }
        )
        print(f'첫 번째 생성 결과: HTTP {response1.status_code}')
        if response1.status_code != 200:
            print(f'응답: {response1.text}')
        
        # 2. 동일한 ID로 다시 생성 시도
        print('\n2️⃣ 동일한 ID로 두 번째 생성 시도...')
        response2 = await client.post(
            'http://localhost:8000/api/v1/ontology/test_error_db/create',
            json={
                'id': 'TestDuplicate',
                'label': '중복 테스트 2',
                'description': '409 에러가 반환되어야 함'
            }
        )
        print(f'두 번째 생성 결과: HTTP {response2.status_code}')
        print(f'응답: {response2.text}')
        
        # 3. 결과 검증
        print('\n📊 검증 결과:')
        if response2.status_code == 409:
            print('✅ 성공! DocumentIdAlreadyExists 에러가 올바르게 HTTP 409로 반환됨')
            return True
        elif response2.status_code == 500:
            print('❌ 실패! 여전히 HTTP 500으로 반환됨')
            return False
        else:
            print(f'⚠️ 예상치 못한 상태 코드: {response2.status_code}')
            return False

if __name__ == "__main__":
    asyncio.run(test_409_error())