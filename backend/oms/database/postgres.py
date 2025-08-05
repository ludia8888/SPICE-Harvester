"""
PostgreSQL database connection and session management for OMS
Outbox Pattern을 위한 데이터베이스 연결 관리
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Optional

import asyncpg
from asyncpg import Connection, Pool

from shared.config.service_config import ServiceConfig

logger = logging.getLogger(__name__)


class PostgresDatabase:
    """PostgreSQL database connection manager"""
    
    def __init__(self):
        self.pool: Optional[Pool] = None
        self.connection_url = ServiceConfig.get_postgres_url()
        
    async def connect(self) -> None:
        """Create connection pool to PostgreSQL"""
        try:
            self.pool = await asyncpg.create_pool(
                self.connection_url,
                min_size=10,
                max_size=20,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                command_timeout=60
            )
            logger.info("PostgreSQL connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create PostgreSQL connection pool: {e}")
            raise
            
    async def disconnect(self) -> None:
        """Close all connections in the pool"""
        if self.pool:
            await self.pool.close()
            logger.info("PostgreSQL connection pool closed")
            
    @asynccontextmanager
    async def transaction(self):
        """
        Provide a transactional database connection
        
        Usage:
            async with db.transaction() as conn:
                await conn.execute(...)
        """
        if not self.pool:
            raise RuntimeError("Database pool not initialized. Call connect() first.")
            
        async with self.pool.acquire() as connection:
            async with connection.transaction():
                yield connection
                
    async def execute(self, query: str, *args):
        """Execute a query without returning results"""
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