"""
메타데이터 스키마 동시성 테스트
"""

import asyncio
import httpx
import time

async def test_metadata_concurrent():
    print('🔍 메타데이터 스키마 동시성 테스트')
    
    # 새 데이터베이스 생성
    async with httpx.AsyncClient(timeout=30.0) as client:
        db_name = f"meta_test_{int(time.time())}"
        print(f'\n1️⃣ 테스트 데이터베이스 생성: {db_name}')
        
        response = await client.post(
            'http://localhost:8000/api/v1/database/create',
            json={'name': db_name, 'description': '메타데이터 동시성 테스트'}
        )
        if response.status_code != 201:
            print(f'❌ 데이터베이스 생성 실패: {response.text}')
            return False
        
        # 동시에 여러 온톨로지 생성 (메타데이터 스키마 동시 생성 유발)
        print('\n2️⃣ 10개 온톨로지 동시 생성...')
        
        async def create_ontology(i: int):
            try:
                response = await client.post(
                    f'http://localhost:8000/api/v1/ontology/{db_name}/create',
                    json={
                        'id': f'MetaTest_{i}',
                        'label': f'메타 테스트 {i}',
                        'description': '메타데이터 스키마 동시성 테스트'
                    }
                )
                return i, response.status_code, response.text if response.status_code != 200 else None
            except Exception as e:
                return i, None, str(e)
        
        # 10개 동시 요청
        tasks = [create_ontology(i) for i in range(10)]
        results = await asyncio.gather(*tasks)
        
        # 결과 분석
        success_count = 0
        errors = []
        
        for i, status, error in results:
            if status == 200:
                success_count += 1
                print(f'✅ 온톨로지 {i}: 성공')
            else:
                errors.append((i, status, error))
                print(f'❌ 온톨로지 {i}: 실패 (HTTP {status})')
                if error and 'DocumentIdAlreadyExists' in error and 'FieldMetadata' in error:
                    print(f'   → FieldMetadata 중복 에러 발생!')
        
        print(f'\n📊 결과: {success_count}/10 성공')
        
        # 메타데이터 관련 오류 확인
        metadata_errors = [e for e in errors if e[2] and ('FieldMetadata' in e[2] or 'ClassMetadata' in e[2])]
        if metadata_errors:
            print(f'\n⚠️ 메타데이터 스키마 관련 오류 {len(metadata_errors)}개 발생:')
            for i, status, error in metadata_errors[:3]:  # 처음 3개만 표시
                print(f'   - 온톨로지 {i}: HTTP {status}')
            return False
        
        # 정리
        try:
            await client.delete(f'http://localhost:8000/api/v1/database/{db_name}')
        except:
            pass
        
        if success_count >= 8:  # 80% 이상 성공
            print('\n✅ 메타데이터 스키마 동시성 테스트 통과!')
            return True
        else:
            print('\n❌ 성공률이 낮습니다.')
            return False

if __name__ == "__main__":
    asyncio.run(test_metadata_concurrent())