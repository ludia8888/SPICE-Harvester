#!/usr/bin/env python3
"""
Event Sourcing 파이프라인 복구 스크립트
OutboxService 초기화 문제 진단 및 해결
"""

import asyncio
import asyncpg
import logging
import os
from typing import Optional

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class OutboxDiagnostics:
    """Outbox 패턴 진단 및 복구 도구"""
    
    def __init__(self):
        self.conn: Optional[asyncpg.Connection] = None
        self.postgres_url = self._get_postgres_url()
        
    def _get_postgres_url(self) -> str:
        """PostgreSQL 연결 URL 생성"""
        host = os.getenv("POSTGRES_HOST", "localhost")
        port = os.getenv("POSTGRES_PORT", "5433")
        user = os.getenv("POSTGRES_USER", "spiceadmin")
        password = os.getenv("POSTGRES_PASSWORD", "spicepass123")
        database = os.getenv("POSTGRES_DB", "spicedb")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    async def test_connection(self) -> bool:
        """PostgreSQL 연결 테스트"""
        logger.info("🔍 PostgreSQL 연결 테스트 시작...")
        try:
            self.conn = await asyncpg.connect(self.postgres_url)
            version = await self.conn.fetchval("SELECT version()")
            logger.info(f"✅ PostgreSQL 연결 성공!")
            logger.info(f"   버전: {version[:50]}...")
            return True
        except Exception as e:
            logger.error(f"❌ PostgreSQL 연결 실패: {e}")
            return False
    
    async def check_schema(self) -> bool:
        """spice_outbox 스키마 존재 확인"""
        logger.info("🔍 spice_outbox 스키마 확인...")
        try:
            exists = await self.conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.schemata 
                    WHERE schema_name = 'spice_outbox'
                )
                """
            )
            if exists:
                logger.info("✅ spice_outbox 스키마 존재함")
            else:
                logger.warning("⚠️  spice_outbox 스키마가 없습니다")
            return exists
        except Exception as e:
            logger.error(f"❌ 스키마 확인 실패: {e}")
            return False
    
    async def check_table(self) -> bool:
        """outbox 테이블 존재 및 구조 확인"""
        logger.info("🔍 outbox 테이블 확인...")
        try:
            # 테이블 존재 확인
            exists = await self.conn.fetchval(
                """
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_schema = 'spice_outbox' 
                    AND table_name = 'outbox'
                )
                """
            )
            
            if not exists:
                logger.warning("⚠️  outbox 테이블이 없습니다")
                return False
            
            logger.info("✅ outbox 테이블 존재함")
            
            # 컬럼 확인
            columns = await self.conn.fetch(
                """
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_schema = 'spice_outbox' 
                AND table_name = 'outbox'
                ORDER BY ordinal_position
                """
            )
            
            logger.info("   테이블 구조:")
            required_columns = ['id', 'message_type', 'aggregate_type', 'aggregate_id', 'topic', 'payload']
            found_columns = [col['column_name'] for col in columns]
            
            for col in columns:
                logger.info(f"   - {col['column_name']}: {col['data_type']} (nullable: {col['is_nullable']})")
            
            # 필수 컬럼 확인
            missing_columns = set(required_columns) - set(found_columns)
            if missing_columns:
                logger.error(f"❌ 필수 컬럼 누락: {missing_columns}")
                return False
            
            # 데이터 개수 확인
            count = await self.conn.fetchval("SELECT COUNT(*) FROM spice_outbox.outbox")
            logger.info(f"   현재 레코드 수: {count}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 테이블 확인 실패: {e}")
            return False
    
    async def apply_migrations(self) -> bool:
        """마이그레이션 스크립트 실행"""
        logger.info("🔧 마이그레이션 적용 시작...")
        
        migration_files = [
            "/Users/isihyeon/Desktop/SPICE HARVESTER/backend/database/migrations/001_create_outbox_table.sql",
            "/Users/isihyeon/Desktop/SPICE HARVESTER/backend/database/migrations/002_add_message_type_to_outbox.sql"
        ]
        
        try:
            for migration_file in migration_files:
                if os.path.exists(migration_file):
                    logger.info(f"   마이그레이션 실행: {os.path.basename(migration_file)}")
                    
                    with open(migration_file, 'r') as f:
                        sql = f.read()
                    
                    # 트랜잭션으로 실행
                    async with self.conn.transaction():
                        await self.conn.execute(sql)
                    
                    logger.info(f"   ✅ {os.path.basename(migration_file)} 적용 완료")
                else:
                    logger.warning(f"   ⚠️  파일 없음: {migration_file}")
            
            return True
            
        except Exception as e:
            logger.error(f"❌ 마이그레이션 실패: {e}")
            return False
    
    async def test_outbox_service(self) -> bool:
        """OutboxService 초기화 테스트"""
        logger.info("🔍 OutboxService 초기화 테스트...")
        
        try:
            # sys.path에 backend 디렉토리 추가
            import sys
            sys.path.insert(0, "/Users/isihyeon/Desktop/SPICE HARVESTER/backend")
            
            from oms.database.postgres import PostgresDatabase
            from oms.database.outbox import OutboxService
            
            # PostgresDatabase 초기화
            db = PostgresDatabase()
            await db.connect()
            logger.info("   ✅ PostgresDatabase 연결 성공")
            
            # OutboxService 초기화
            outbox_service = OutboxService(db)
            logger.info("   ✅ OutboxService 초기화 성공")
            
            # 테스트 Command 생성
            from shared.models.commands import InstanceCommand, CommandType
            from datetime import datetime
            
            test_command = InstanceCommand(
                command_type=CommandType.CREATE_INSTANCE,
                db_name="test_db",
                class_id="TestClass",
                payload={"test": "data"},
                created_by="test_script"
            )
            
            # 트랜잭션으로 테스트 Command 저장
            async with db.transaction() as conn:
                message_id = await outbox_service.publish_command(conn, test_command)
                logger.info(f"   ✅ 테스트 Command 저장 성공: {message_id}")
            
            # 저장된 메시지 확인
            messages = await outbox_service.get_unprocessed_messages(limit=1)
            if messages:
                logger.info(f"   ✅ 저장된 메시지 확인: {len(messages)}개")
                
                # 테스트 메시지 처리 완료 표시
                await outbox_service.mark_as_processed(messages[0].id)
                logger.info("   ✅ 메시지 처리 완료 표시 성공")
            
            await db.disconnect()
            return True
            
        except Exception as e:
            logger.error(f"❌ OutboxService 테스트 실패: {e}")
            import traceback
            logger.error(f"   상세 오류:\n{traceback.format_exc()}")
            return False
    
    async def diagnose_and_fix(self):
        """전체 진단 및 복구 프로세스"""
        logger.info("=" * 60)
        logger.info("🚀 Event Sourcing 파이프라인 진단 시작")
        logger.info("=" * 60)
        
        # 1. PostgreSQL 연결 테스트
        if not await self.test_connection():
            logger.error("💀 PostgreSQL 연결 실패 - 프로세스 중단")
            return
        
        # 2. 스키마 확인
        schema_exists = await self.check_schema()
        
        # 3. 테이블 확인
        table_exists = False
        if schema_exists:
            table_exists = await self.check_table()
        
        # 4. 마이그레이션 필요시 적용
        if not schema_exists or not table_exists:
            logger.info("🔧 마이그레이션이 필요합니다...")
            if await self.apply_migrations():
                # 재확인
                schema_exists = await self.check_schema()
                table_exists = await self.check_table()
        
        # 5. OutboxService 테스트
        if schema_exists and table_exists:
            await self.test_outbox_service()
        
        # 6. 연결 종료
        if self.conn:
            await self.conn.close()
        
        logger.info("=" * 60)
        logger.info("🏁 진단 완료")
        logger.info("=" * 60)
        
        # 요약
        logger.info("\n📊 진단 결과 요약:")
        logger.info(f"   - PostgreSQL 연결: ✅")
        logger.info(f"   - spice_outbox 스키마: {'✅' if schema_exists else '❌'}")
        logger.info(f"   - outbox 테이블: {'✅' if table_exists else '❌'}")
        
        if schema_exists and table_exists:
            logger.info("\n✨ Event Sourcing 인프라가 준비되었습니다!")
            logger.info("   다음 단계:")
            logger.info("   1. OMS 서비스 재시작: docker-compose restart oms")
            logger.info("   2. API 테스트로 Command 생성 확인")
            logger.info("   3. Outbox 테이블에 데이터 저장 확인")
        else:
            logger.error("\n⚠️  문제가 해결되지 않았습니다. 수동 개입이 필요합니다.")


async def main():
    diagnostics = OutboxDiagnostics()
    await diagnostics.diagnose_and_fix()


if __name__ == "__main__":
    asyncio.run(main())