"""
PostgreSQL database connection and session management for OMS
Outbox Pattern을 위한 데이터베이스 연결 관리
Enhanced with MVCC support for better concurrency control
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from asyncpg import Connection, Pool

from shared.config.service_config import ServiceConfig
from .mvcc import MVCCTransactionManager, IsolationLevel
from .decorators import with_deadlock_retry

logger = logging.getLogger(__name__)


class PostgresDatabase:
    """
    PostgreSQL database connection manager with MVCC support.
    
    Enhanced with MVCCTransactionManager for better concurrency control
    and automatic retry logic for deadlock/serialization failures.
    """
    
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.mvcc_manager: Optional[MVCCTransactionManager] = None
        self.connection_url = ServiceConfig.get_postgres_url()
        # Default to REPEATABLE_READ for better consistency
        self.default_isolation = IsolationLevel.REPEATABLE_READ
        
    async def connect(self) -> None:
        """Create connection pool to PostgreSQL with MVCC support"""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_url,
                min_size=10,
                max_size=20,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                command_timeout=60
            )
            
            # Initialize MVCC manager with the pool
            self.mvcc_manager = MVCCTransactionManager(
                pool=self.pool,
                default_isolation=self.default_isolation
            )
            
            logger.info(
                f"PostgreSQL connection pool created with MVCC support "
                f"(default isolation: {self.default_isolation.value})"
            )
            
            # Ensure outbox table exists
            await self.ensure_outbox_table()
            
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise
    
    async def ensure_outbox_table(self) -> None:
        """
        🔥 THINK ULTRA! Ensure outbox table and schema exist for Event Sourcing
        
        This method creates the spice_outbox schema and outbox table if they don't exist.
        This ensures the application works in both Docker and local environments.
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized")
        
        try:
            async with self.pool.acquire() as connection:
                # Create schema if not exists
                await connection.execute("""
                    CREATE SCHEMA IF NOT EXISTS spice_outbox
                """)
                
                # Create outbox table if not exists
                await connection.execute("""
                    CREATE TABLE IF NOT EXISTS spice_outbox.outbox (
                        id VARCHAR(255) PRIMARY KEY,
                        message_type VARCHAR(50) NOT NULL,
                        aggregate_type VARCHAR(255) NOT NULL,
                        aggregate_id VARCHAR(255) NOT NULL,
                        topic VARCHAR(255) NOT NULL,
                        payload TEXT NOT NULL,
                        created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                        processed_at TIMESTAMP WITH TIME ZONE,
                        retry_count INTEGER DEFAULT 0,
                        last_retry_at TIMESTAMP WITH TIME ZONE,
                        entity_version INTEGER
                    )
                """)
                
                # Create indexes if not exists
                await connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_unprocessed 
                    ON spice_outbox.outbox(created_at) 
                    WHERE processed_at IS NULL
                """)
                
                await connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_aggregate 
                    ON spice_outbox.outbox(aggregate_type, aggregate_id)
                """)
                
                await connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_message_type 
                    ON spice_outbox.outbox(message_type, processed_at) 
                    WHERE processed_at IS NULL
                """)
                
                await connection.execute("""
                    CREATE INDEX IF NOT EXISTS idx_outbox_topic 
                    ON spice_outbox.outbox(topic)
                """)
                
                logger.info("✅ Outbox table and indexes ensured in spice_outbox schema")
                
                # Verify table exists
                table_exists = await connection.fetchval("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_schema = 'spice_outbox' 
                        AND table_name = 'outbox'
                    )
                """)
                
                if table_exists:
                    # Get row count for logging
                    count = await connection.fetchval("""
                        SELECT COUNT(*) FROM spice_outbox.outbox
                    """)
                    logger.info(f"📊 Outbox table verified: {count} existing records")
                else:
                    logger.error("❌ Failed to create outbox table")
                    raise RuntimeError("Outbox table creation failed")
                    
        except Exception as e:
            logger.error(f"Failed to ensure outbox table: {e}")
            raise
            
    async def disconnect(self) -> None:
        """Close all connections in the pool"""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
            
    @asynccontextmanager
    async def transaction(
        self,
        isolation_level: Optional[IsolationLevel] = None,
        read_only: bool = False,
        with_retry: bool = True
    ):
        """
        Provide a transactional database connection with MVCC support.
        
        Args:
            isolation_level: Transaction isolation level (uses default if None)
            read_only: Whether transaction is read-only
            with_retry: Whether to enable automatic retry on deadlock/serialization failures
        
        Usage:
            async with db.transaction() as conn:
                await conn.execute(...)
                
            # With specific isolation level
            async with db.transaction(IsolationLevel.SERIALIZABLE) as conn:
                await conn.execute(...)
        """
        if not self.mvcc_manager:
            raise RuntimeError("MVCC manager not initialized. Call connect() first.")
        
        if with_retry:
            # Use MVCC manager with retry logic
            max_retries = 3
            for attempt in range(max_retries):
                try:
                    async with self.mvcc_manager.transaction(
                        isolation_level=isolation_level,
                        read_only=read_only
                    ) as connection:
                        yield connection
                        break  # Success, exit retry loop
                except (asyncpg.DeadlockDetectedError, asyncpg.SerializationError) as e:
                    if attempt < max_retries - 1:
                        # Calculate backoff delay
                        delay = 0.1 * (2 ** attempt)
                        logger.warning(
                            f"Transaction failed with {e.__class__.__name__}, "
                            f"retrying in {delay}s (attempt {attempt + 1}/{max_retries})"
                        )
                        await asyncio.sleep(delay)
                    else:
                        logger.error(f"Transaction failed after {max_retries} attempts")
                        raise
        else:
            # Use MVCC manager without retry
            async with self.mvcc_manager.transaction(
                isolation_level=isolation_level,
                read_only=read_only
            ) as connection:
                yield connection
                
    @with_deadlock_retry()
    async def execute(self, query: str, *args):
        """Execute a query without returning results with automatic retry on deadlock"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        async with self.pool.acquire() as connection:
            return await connection.execute(query, *args)
            
    async def fetch(self, query: str, *args):
        """Fetch multiple rows"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        async with self.pool.acquire() as connection:
            return await connection.fetch(query, *args)
            
    async def fetchrow(self, query: str, *args):
        """Fetch a single row"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        async with self.pool.acquire() as connection:
            return await connection.fetchrow(query, *args)
            
    async def fetchval(self, query: str, *args):
        """Fetch a single value"""
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        async with self.pool.acquire() as connection:
            return await connection.fetchval(query, *args)


# Global database instance
db = PostgresDatabase()


async def get_db() -> PostgresDatabase:
    """Dependency to get database instance"""
    return db