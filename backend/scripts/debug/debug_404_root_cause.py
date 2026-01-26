#!/usr/bin/env python3
"""
Deep dive into 404 error root cause
GOD MODE ON - Find the EXACT problem
"""

import asyncio
import aiohttp
from datetime import datetime

async def debug_ontology_404():
    print('🔍 DEEP DIVE: 온톨로지 404 오류 ROOT CAUSE 분석')
    print('=' * 60)
    
    test_db = f'debug_404_{datetime.now().strftime("%H%M%S")}'
    
    async with aiohttp.ClientSession() as session:
        # Step 1: Create database
        print(f'1. 데이터베이스 생성: {test_db}')
        resp = await session.post(
            'http://localhost:8000/api/v1/database/create',
            json={'name': test_db, 'description': 'Debug 404'}
        )
        result = await resp.json()
        print(f'   Status: {resp.status}')
        print(f'   Command ID: {result.get("command_id")}')
        
        # Step 2: IMMEDIATELY check if DB exists (no wait)
        print(f'\n2. 즉시 DB 존재 확인 (대기 없음)')
        resp = await session.get(f'http://localhost:8000/api/v1/database/exists/{test_db}')
        if resp.status == 200:
            data = await resp.json()
            exists = data.get('data', {}).get('exists', False)
            print(f'   Exists: {exists}')
        else:
            print(f'   Status: {resp.status}')
        
        # Step 3: Try ontology creation immediately
        print(f'\n3. 온톨로지 생성 시도 (대기 없음)')
        ontology = {
            'id': 'TestClass',
            'label': 'Test',
            'properties': [
                {'name': 'id', 'type': 'string', 'label': 'ID', 'required': True}
            ]
        }
        resp = await session.post(
            f'http://localhost:8000/api/v1/database/{test_db}/ontology',
            json=ontology
        )
        print(f'   Status: {resp.status}')
        if resp.status != 200:
            error = await resp.text()
            print(f'   Error: {error[:200]}')
        
        # Step 4: Wait and retry
        total_wait = 0
        for wait_time in [1, 2, 3, 5, 10]:
            print(f'\n4. {wait_time}초 대기 후 재시도...')
            await asyncio.sleep(wait_time)
            total_wait += wait_time
            
            # Check existence
            resp = await session.get(f'http://localhost:8000/api/v1/database/exists/{test_db}')
            if resp.status == 200:
                data = await resp.json()
                exists = data.get('data', {}).get('exists', False)
                print(f'   DB Exists: {exists}')
                
                if exists:
                    # Try ontology creation
                    resp = await session.post(
                        f'http://localhost:8000/api/v1/database/{test_db}/ontology',
                        json=ontology
                    )
                    print(f'   Ontology creation: {resp.status}')
                    if resp.status in [200, 201]:
                        print(f'   ✅ 성공! DB 생성에 총 {total_wait}초 필요했음')
                        break
                    else:
                        error = await resp.text()
                        print(f'   Still failing: {error[:100]}')
        
        # Step 5: Check actual TerminusDB
        print(f'\n5. TerminusDB 직접 확인')
        resp = await session.get('http://localhost:8000/api/v1/database/list')
        if resp.status == 200:
            data = await resp.json()
            dbs = data.get('data', [])
            found = any(db.get('name') == test_db for db in dbs)
            print(f'   Database in list: {found}')
            if found:
                print(f'   ✅ DB는 TerminusDB에 존재함')
                
                # Check the DB details
                for db in dbs:
                    if db.get('name') == test_db:
                        print(f'   DB 상세: {db}')
        
        # Step 6: Check message relay logs
        print(f'\n6. Message Relay 및 Worker 상태 확인 필요')
        print('   - message_relay가 Kafka에서 메시지를 가져오고 있는가?')
        print('   - ontology_worker가 CREATE_DATABASE 이벤트를 처리하고 있는가?')
        
        # Cleanup
        print(f'\n7. 정리: {test_db} 삭제')
        resp = await session.delete(f'http://localhost:8000/api/v1/database/{test_db}')
        print(f'   Delete status: {resp.status}')

if __name__ == "__main__":
    asyncio.run(debug_ontology_404())