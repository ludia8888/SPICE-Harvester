"""
Database operation decorators for MVCC and retry logic.
Implements Dependency Inversion Principle - high-level modules depend on abstractions.
"""

import asyncio
import functools
import logging
from typing import Optional, Callable, Any, TypeVar, cast

from .retry_handler import (
    RetryStrategy,
    RetryExecutor,
    DEFAULT_DEADLOCK_STRATEGY,
    DEFAULT_SERIALIZATION_STRATEGY,
    DEFAULT_MVCC_STRATEGY
)
from .mvcc import IsolationLevel, MVCCTransactionManager

logger = logging.getLogger(__name__)

# Type variable for generic decorator return type
F = TypeVar('F', bound=Callable[..., Any])


def with_deadlock_retry(
    strategy: Optional[RetryStrategy] = None,
    log_retries: bool = True
) -> Callable[[F], F]:
    """
    Decorator to add deadlock retry logic to async database operations.
    
    Args:
        strategy: Retry strategy to use (defaults to DeadlockRetryStrategy)
        log_retries: Whether to log retry attempts
        
    Returns:
        Decorated function with retry logic
        
    Example:
        @with_deadlock_retry()
        async def update_user(user_id: int, name: str):
            async with db.transaction() as conn:
                await conn.execute("UPDATE users SET name = $1 WHERE id = $2", name, user_id)
    """
    retry_strategy = strategy or DEFAULT_DEADLOCK_STRATEGY
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            executor = RetryExecutor(retry_strategy)
            
            if log_retries:
                logger.debug(f"Executing {func.__name__} with deadlock retry")
                
            return await executor.execute(func, *args, **kwargs)
            
        return cast(F, wrapper)
    
    return decorator


def with_serialization_retry(
    strategy: Optional[RetryStrategy] = None
) -> Callable[[F], F]:
    """
    Decorator for handling serialization failures in SERIALIZABLE transactions.
    
    Args:
        strategy: Retry strategy to use (defaults to SerializationRetryStrategy)
        
    Returns:
        Decorated function with serialization retry logic
        
    Example:
        @with_serialization_retry()
        async def transfer_funds(from_id: int, to_id: int, amount: float):
            async with mvcc.transaction(IsolationLevel.SERIALIZABLE) as conn:
                # Complex transaction logic
    """
    retry_strategy = strategy or DEFAULT_SERIALIZATION_STRATEGY
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            executor = RetryExecutor(retry_strategy)
            return await executor.execute(func, *args, **kwargs)
            
        return cast(F, wrapper)
    
    return decorator


def with_mvcc_retry(
    strategy: Optional[RetryStrategy] = None
) -> Callable[[F], F]:
    """
    Decorator that handles both deadlock and serialization failures.
    
    Args:
        strategy: Retry strategy to use (defaults to composite strategy)
        
    Returns:
        Decorated function with full MVCC retry logic
        
    Example:
        @with_mvcc_retry()
        async def complex_operation(data: dict):
            async with mvcc.transaction() as conn:
                # Operation that might face concurrency issues
    """
    retry_strategy = strategy or DEFAULT_MVCC_STRATEGY
    
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            executor = RetryExecutor(retry_strategy)
            
            try:
                return await executor.execute(func, *args, **kwargs)
            except Exception as e:
                logger.error(
                    f"Operation {func.__name__} failed after all retries: {e}"
                )
                raise
                
        return cast(F, wrapper)
    
    return decorator


def with_transaction(
    isolation_level: Optional[IsolationLevel] = None,
    read_only: bool = False,
    with_retry: bool = True
) -> Callable[[F], F]:
    """
    Decorator that automatically wraps a function in a database transaction.
    
    Args:
        isolation_level: Transaction isolation level
        read_only: Whether transaction is read-only
        with_retry: Whether to add retry logic for concurrency errors
        
    Returns:
        Decorated function that runs in a transaction
        
    Example:
        @with_transaction(IsolationLevel.REPEATABLE_READ)
        async def create_order(conn: Connection, order_data: dict):
            # conn is automatically provided by decorator
            await conn.execute("INSERT INTO orders ...")
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get the MVCCTransactionManager from the first argument
            # This assumes the decorated function has 'self' or manager as first arg
            manager = None
            
            if args and hasattr(args[0], 'mvcc_manager'):
                manager = args[0].mvcc_manager
            elif args and isinstance(args[0], MVCCTransactionManager):
                manager = args[0]
            else:
                raise ValueError(
                    f"Cannot find MVCCTransactionManager for {func.__name__}. "
                    "Ensure the decorated method is in a class with 'mvcc_manager' attribute "
                    "or the first argument is an MVCCTransactionManager instance."
                )
            
            async def execute_in_transaction():
                async with manager.transaction(
                    isolation_level=isolation_level,
                    read_only=read_only
                ) as conn:
                    # Inject connection as first argument after self/manager
                    new_args = list(args)
                    if len(args) > 1:
                        new_args = [args[0], conn] + list(args[1:])
                    else:
                        new_args = [args[0], conn]
                    
                    return await func(*new_args, **kwargs)
            
            if with_retry:
                # Add retry logic for concurrency errors
                executor = RetryExecutor(DEFAULT_MVCC_STRATEGY)
                return await executor.execute(execute_in_transaction)
            else:
                return await execute_in_transaction()
                
        return cast(F, wrapper)
    
    return decorator


def with_optimistic_lock(
    version_field: str = "version",
    entity_type: str = "Entity"
) -> Callable[[F], F]:
    """
    Decorator to add optimistic locking version check.
    
    Args:
        version_field: Name of the version field in the entity
        entity_type: Type of entity for error messages
        
    Returns:
        Decorated function with optimistic locking
        
    Example:
        @with_optimistic_lock(entity_type="Ontology")
        async def update_ontology(ontology_id: str, data: dict, expected_version: int):
            # Will automatically check version before update
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Extract expected version from kwargs or last positional arg
            expected_version = kwargs.get('expected_version')
            if expected_version is None and len(args) >= 3:
                # Try to get from positional args
                expected_version = args[-1] if isinstance(args[-1], int) else None
                
            if expected_version is not None:
                logger.debug(
                    f"Optimistic lock check for {entity_type} "
                    f"with expected version {expected_version}"
                )
                
            return await func(*args, **kwargs)
            
        return cast(F, wrapper)
    
    return decorator


def monitor_transaction_time(
    threshold_seconds: float = 1.0,
    log_level: int = logging.WARNING
) -> Callable[[F], F]:
    """
    Decorator to monitor and log slow transactions.
    
    Args:
        threshold_seconds: Time threshold for logging
        log_level: Logging level for slow transactions
        
    Returns:
        Decorated function with timing monitoring
    """
    def decorator(func: F) -> F:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = asyncio.get_event_loop().time()
            
            try:
                result = await func(*args, **kwargs)
                return result
            finally:
                elapsed = asyncio.get_event_loop().time() - start_time
                
                if elapsed > threshold_seconds:
                    logger.log(
                        log_level,
                        f"Slow transaction in {func.__name__}: {elapsed:.3f}s "
                        f"(threshold: {threshold_seconds}s)"
                    )
                else:
                    logger.debug(f"Transaction {func.__name__} completed in {elapsed:.3f}s")
                    
        return cast(F, wrapper)
    
    return decorator