"""
MVCC Transaction Manager for PostgreSQL
Implements Multi-Version Concurrency Control with configurable isolation levels
"""

import asyncio
import logging
from contextlib import asynccontextmanager
from enum import Enum
from typing import Optional, AsyncIterator

import asyncpg
from asyncpg import Connection, Pool

logger = logging.getLogger(__name__)


class IsolationLevel(str, Enum):
    """
    PostgreSQL Transaction Isolation Levels
    
    From least to most strict:
    - READ_UNCOMMITTED: Allows dirty reads (PostgreSQL treats as READ_COMMITTED)
    - READ_COMMITTED: Default PostgreSQL level, prevents dirty reads
    - REPEATABLE_READ: Prevents dirty & non-repeatable reads
    - SERIALIZABLE: Full isolation, prevents all phenomena
    """
    READ_UNCOMMITTED = "READ UNCOMMITTED"
    READ_COMMITTED = "READ COMMITTED"
    REPEATABLE_READ = "REPEATABLE READ"
    SERIALIZABLE = "SERIALIZABLE"
    
    @classmethod
    def get_default(cls) -> 'IsolationLevel':
        """Get default isolation level for MVCC operations"""
        return cls.REPEATABLE_READ


class MVCCTransactionManager:
    """
    Transaction manager with MVCC support.
    
    Follows SOLID principles:
    - SRP: Only manages MVCC transactions
    - OCP: Can be extended for new isolation strategies
    - DIP: Depends on asyncpg abstraction, not concrete implementation
    """
    
    def __init__(self, pool: Pool, default_isolation: IsolationLevel = None):
        """
        Initialize MVCC Transaction Manager.
        
        Args:
            pool: AsyncPG connection pool
            default_isolation: Default isolation level (defaults to REPEATABLE_READ)
        """
        self.pool = pool
        self.default_isolation = default_isolation or IsolationLevel.get_default()
        self._active_transactions = set()
        
    @asynccontextmanager
    async def transaction(
        self,
        isolation_level: Optional[IsolationLevel] = None,
        read_only: bool = False,
        deferrable: bool = False
    ) -> AsyncIterator[Connection]:
        """
        Create a transaction with specified isolation level.
        
        Args:
            isolation_level: Transaction isolation level (uses default if None)
            read_only: Whether transaction is read-only (can optimize performance)
            deferrable: For SERIALIZABLE read-only, can delay to avoid conflicts
            
        Yields:
            Database connection with active transaction
            
        Example:
            async with mvcc_manager.transaction(IsolationLevel.SERIALIZABLE) as conn:
                await conn.execute("UPDATE ...")
        """
        isolation = isolation_level or self.default_isolation
        connection = None
        transaction_id = id(asyncio.current_task())
        
        try:
            # Acquire connection from pool
            connection = await self.pool.acquire()
            self._active_transactions.add(transaction_id)
            
            # Set isolation level before starting transaction
            await connection.execute(f"SET TRANSACTION ISOLATION LEVEL {isolation.value}")
            
            # Set transaction characteristics
            characteristics = []
            if read_only:
                characteristics.append("READ ONLY")
            else:
                characteristics.append("READ WRITE")
                
            if deferrable and read_only and isolation == IsolationLevel.SERIALIZABLE:
                characteristics.append("DEFERRABLE")
                
            if characteristics:
                await connection.execute(f"SET TRANSACTION {', '.join(characteristics)}")
            
            # Start transaction
            transaction = connection.transaction()
            await transaction.start()
            
            logger.debug(
                f"Started MVCC transaction {transaction_id} with isolation: {isolation.value}, "
                f"read_only: {read_only}, deferrable: {deferrable}"
            )
            
            try:
                yield connection
                await transaction.commit()
                logger.debug(f"Committed MVCC transaction {transaction_id}")
                
            except asyncpg.SerializationError as e:
                # Serialization failure - should retry at application level
                await transaction.rollback()
                logger.warning(f"Serialization failure in transaction {transaction_id}: {e}")
                raise MVCCSerializationError(
                    "Transaction failed due to concurrent update. Please retry."
                ) from e
                
            except asyncpg.DeadlockDetectedError as e:
                # Deadlock detected - should retry with backoff
                await transaction.rollback()
                logger.warning(f"Deadlock detected in transaction {transaction_id}: {e}")
                raise MVCCDeadlockError(
                    "Transaction deadlocked with another transaction. Please retry."
                ) from e
                
            except Exception as e:
                # Other errors - rollback and re-raise
                await transaction.rollback()
                logger.error(f"Error in transaction {transaction_id}: {e}")
                raise
                
        finally:
            # Clean up
            self._active_transactions.discard(transaction_id)
            if connection:
                await self.pool.release(connection)
                
    async def get_active_transaction_count(self) -> int:
        """Get count of currently active transactions"""
        return len(self._active_transactions)
    
    async def execute_with_retry(
        self,
        query: str,
        *args,
        max_retries: int = 3,
        isolation_level: Optional[IsolationLevel] = None,
        backoff_base: float = 0.1
    ):
        """
        Execute a query with automatic retry on serialization/deadlock errors.
        
        Args:
            query: SQL query to execute
            *args: Query parameters
            max_retries: Maximum number of retry attempts
            isolation_level: Transaction isolation level
            backoff_base: Base delay for exponential backoff (seconds)
            
        Returns:
            Query result
            
        Raises:
            MVCCError: After max retries exceeded
        """
        last_error = None
        
        for attempt in range(max_retries):
            try:
                async with self.transaction(isolation_level) as conn:
                    result = await conn.execute(query, *args)
                    return result
                    
            except (MVCCSerializationError, MVCCDeadlockError) as e:
                last_error = e
                if attempt < max_retries - 1:
                    # Exponential backoff with jitter
                    delay = backoff_base * (2 ** attempt)
                    jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
                    await asyncio.sleep(delay + jitter)
                    logger.info(
                        f"Retrying query after {e.__class__.__name__} "
                        f"(attempt {attempt + 1}/{max_retries})"
                    )
                else:
                    logger.error(f"Max retries ({max_retries}) exceeded for query")
                    
        raise MVCCMaxRetriesError(
            f"Query failed after {max_retries} attempts"
        ) from last_error


class MVCCError(Exception):
    """Base exception for MVCC-related errors"""
    pass


class MVCCSerializationError(MVCCError):
    """Raised when a serialization failure occurs in SERIALIZABLE isolation"""
    pass


class MVCCDeadlockError(MVCCError):
    """Raised when a deadlock is detected"""
    pass


class MVCCMaxRetriesError(MVCCError):
    """Raised when maximum retry attempts are exceeded"""
    pass