#!/usr/bin/env python3
"""
OutboxService 초기화 직접 테스트
"""

import asyncio
import os
import sys
import logging

# 환경 설정 - 로컬 개발 환경으로 설정
os.environ.pop('DOCKER_CONTAINER', None)  # Docker 환경 변수 제거
os.environ['POSTGRES_HOST'] = 'localhost'  # PostgreSQL 호스트를 localhost로 명시적 설정
os.environ['POSTGRES_PORT'] = '5433'  # PostgreSQL 포트를 5433으로 설정 (spice-foundry-postgres)
sys.path.insert(0, '/Users/isihyeon/Desktop/SPICE HARVESTER/backend')

# 로깅 설정
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

async def test_outbox_service():
    """OutboxService 초기화 직접 테스트"""
    print("🔥 OutboxService 초기화 직접 테스트")
    print("=" * 50)
    
    try:
        # 1. PostgreSQL 연결 테스트
        print("1️⃣ PostgreSQL 연결 테스트...")
        from shared.config.service_config import ServiceConfig
        
        postgres_url = ServiceConfig.get_postgres_url()
        print(f"   PostgreSQL URL: {postgres_url}")
        
        # 2. PostgresDatabase 초기화 테스트
        print("2️⃣ PostgresDatabase 초기화 테스트...")
        from oms.database.postgres import PostgresDatabase
        
        postgres_db = PostgresDatabase()
        await postgres_db.connect()
        print("   ✅ PostgresDatabase 연결 성공")
        
        # 3. OutboxService 직접 생성 테스트
        print("3️⃣ OutboxService 직접 생성 테스트...")
        from oms.database.outbox import OutboxService
        
        outbox_service = OutboxService(postgres_db)
        print("   ✅ OutboxService 생성 성공")
        
        # 4. 테이블 존재 확인
        print("4️⃣ Outbox 테이블 존재 확인...")
        async with postgres_db.pool.acquire() as conn:
            # PostgreSQL 스키마 확인
            result = await conn.fetch("""
                SELECT EXISTS(
                    SELECT FROM information_schema.tables 
                    WHERE table_schema = 'spice_outbox' 
                    AND table_name = 'outbox'
                );
            """)
            table_exists = result[0]['exists'] if result else False
            print(f"   spice_outbox.outbox 테이블 존재: {table_exists}")
            
            # 직접 접근 테스트
            try:
                count_result = await conn.fetch("SELECT COUNT(*) as count FROM spice_outbox.outbox")
                count = count_result[0]['count']
                print(f"   ✅ 테이블 직접 접근 성공: {count}개 레코드")
                table_exists = True
            except Exception as e:
                print(f"   ❌ 테이블 직접 접근 실패: {e}")
            
            if not table_exists:
                print("   ❌ Outbox 테이블이 존재하지 않습니다!")
                print("   💡 Migration을 실행해야 합니다.")
                return False
        
        # 5. 테스트 메시지 발행
        print("5️⃣ 테스트 메시지 발행...")
        from shared.models.commands import DatabaseCommand, CommandType
        
        test_command = DatabaseCommand(
            command_type=CommandType.CREATE_DATABASE,
            aggregate_id="test_outbox_db",
            payload={"database_name": "test_outbox_db", "description": "OutboxService test"},
            metadata={"source": "test", "user": "test"}
        )
        
        async with postgres_db.transaction() as conn:
            await outbox_service.publish_command(conn, test_command)
            print(f"   ✅ 테스트 커맨드 발행 성공: {test_command.command_id}")
        
        # 6. 메시지 조회 확인
        print("6️⃣ 발행된 메시지 조회...")
        async with postgres_db.pool.acquire() as conn:
            messages = await conn.fetch("""
                SELECT id, message_type, aggregate_id, payload
                FROM spice_outbox.outbox 
                WHERE processed_at IS NULL
                ORDER BY created_at DESC
                LIMIT 5
            """)
            
            print(f"   📊 미처리 메시지 수: {len(messages)}")
            for msg in messages:
                print(f"      🔸 {msg['message_type']} - {msg['aggregate_id']}")
        
        await postgres_db.disconnect()
        print("\n🎉 OutboxService 초기화 및 기능 테스트 완료!")
        return True
        
    except Exception as e:
        print(f"\n❌ OutboxService 테스트 실패: {type(e).__name__}: {e}")
        import traceback
        print(f"Full traceback:\n{traceback.format_exc()}")
        return False

if __name__ == "__main__":
    success = asyncio.run(test_outbox_service())
    sys.exit(0 if success else 1)