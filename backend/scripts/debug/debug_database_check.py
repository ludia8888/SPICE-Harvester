"""
데이터베이스 목록 형식 확인
"""

import asyncio
from oms.services.async_terminus import AsyncTerminusService
from shared.models.config import ConnectionConfig

async def debug_database_list():
    config = ConnectionConfig(
        server_url="http://localhost:6364",
        user="admin",
        key="admin",
        account="admin"
    )
    
    terminus = AsyncTerminusService(connection_info=config)
    
    print("1. list_databases() 결과 확인")
    dbs = await terminus.list_databases()
    print(f"   타입: {type(dbs)}")
    print(f"   내용: {dbs}")
    
    print("\n2. 각 DB 항목 구조 확인")
    for i, db in enumerate(dbs):
        print(f"   DB {i}: {type(db)} - {db}")
    
    print("\n3. load_test_db 존재 확인")
    db_name = "load_test_db"
    
    # 현재 방식 (잘못된 방식)
    exists_wrong = db_name in dbs
    print(f"   잘못된 방식 (db_name in dbs): {exists_wrong}")
    
    # 올바른 방식
    exists_right = any(db.get('name') == db_name for db in dbs if isinstance(db, dict))
    print(f"   올바른 방식 (name 필드 확인): {exists_right}")

if __name__ == "__main__":
    asyncio.run(debug_database_list())