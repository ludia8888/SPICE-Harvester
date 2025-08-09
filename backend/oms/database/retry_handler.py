"""
Retry Strategy Pattern Implementation for Database Operations
Follows Interface Segregation Principle (ISP) and Strategy Pattern
"""

import asyncio
import logging
import random
from abc import ABC, abstractmethod
from typing import Optional, Callable, Any
from enum import Enum

import asyncpg

logger = logging.getLogger(__name__)


class RetryableError(Enum):
    """Enumeration of retryable error types"""
    DEADLOCK = "deadlock"
    SERIALIZATION = "serialization"
    CONNECTION = "connection"
    TIMEOUT = "timeout"
    LOCK_TIMEOUT = "lock_timeout"


class RetryStrategy(ABC):
    """
    Abstract base class for retry strategies.
    
    Follows ISP - clients only depend on methods they use.
    """
    
    @abstractmethod
    async def should_retry(self, error: Exception, attempt: int) -> bool:
        """
        Determine if the operation should be retried.
        
        Args:
            error: The exception that occurred
            attempt: Current attempt number (0-based)
            
        Returns:
            True if should retry, False otherwise
        """
        pass
    
    @abstractmethod
    def get_delay(self, attempt: int) -> float:
        """
        Calculate delay before next retry.
        
        Args:
            attempt: Current attempt number (0-based)
            
        Returns:
            Delay in seconds
        """
        pass
    
    @abstractmethod
    def get_max_attempts(self) -> int:
        """
        Get maximum number of retry attempts.
        
        Returns:
            Maximum attempts (including initial attempt)
        """
        pass


class DeadlockRetryStrategy(RetryStrategy):
    """
    Retry strategy specifically for database deadlocks.
    
    Uses exponential backoff with jitter to avoid thundering herd.
    """
    
    def __init__(
        self,
        max_attempts: int = 5,
        base_delay: float = 0.1,
        max_delay: float = 5.0,
        jitter_factor: float = 0.1
    ):
        """
        Initialize deadlock retry strategy.
        
        Args:
            max_attempts: Maximum retry attempts
            base_delay: Base delay for exponential backoff (seconds)
            max_delay: Maximum delay between retries (seconds)
            jitter_factor: Randomization factor (0.0 to 1.0)
        """
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.jitter_factor = jitter_factor
        
    async def should_retry(self, error: Exception, attempt: int) -> bool:
        """Check if error is a deadlock and we haven't exceeded max attempts"""
        if attempt >= self.max_attempts - 1:
            return False
            
        # Check for PostgreSQL deadlock error
        if isinstance(error, asyncpg.DeadlockDetectedError):
            logger.info(f"Deadlock detected, will retry (attempt {attempt + 1}/{self.max_attempts})")
            return True
            
        # Check error message for deadlock indicators
        error_msg = str(error).lower()
        if "deadlock" in error_msg or "40p01" in error_msg:
            logger.info(f"Deadlock pattern detected in error, will retry")
            return True
            
        return False
    
    def get_delay(self, attempt: int) -> float:
        """Calculate exponential backoff with jitter"""
        # Exponential backoff: base * 2^attempt
        delay = min(self.base_delay * (2 ** attempt), self.max_delay)
        
        # Add jitter to prevent thundering herd
        if self.jitter_factor > 0:
            jitter = delay * self.jitter_factor * (2 * random.random() - 1)
            delay = max(0, delay + jitter)
            
        logger.debug(f"Calculated retry delay: {delay:.3f}s for attempt {attempt}")
        return delay
    
    def get_max_attempts(self) -> int:
        """Get maximum retry attempts"""
        return self.max_attempts


class SerializationRetryStrategy(RetryStrategy):
    """
    Retry strategy for serialization failures in SERIALIZABLE isolation.
    
    More aggressive than deadlock retry as serialization failures are expected
    in high-concurrency scenarios.
    """
    
    def __init__(
        self,
        max_attempts: int = 10,
        base_delay: float = 0.05,
        max_delay: float = 2.0
    ):
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        
    async def should_retry(self, error: Exception, attempt: int) -> bool:
        """Check if error is a serialization failure"""
        if attempt >= self.max_attempts - 1:
            return False
            
        # Check for PostgreSQL serialization error
        if isinstance(error, asyncpg.SerializationError):
            logger.info(f"Serialization failure, will retry (attempt {attempt + 1}/{self.max_attempts})")
            return True
            
        # Check error codes
        error_msg = str(error).lower()
        if "serialization" in error_msg or "40001" in error_msg:
            logger.info(f"Serialization pattern detected in error, will retry")
            return True
            
        return False
    
    def get_delay(self, attempt: int) -> float:
        """Linear backoff for serialization failures"""
        delay = min(self.base_delay * (attempt + 1), self.max_delay)
        # Small random jitter
        delay += random.uniform(0, 0.01)
        return delay
    
    def get_max_attempts(self) -> int:
        return self.max_attempts


class CompositeRetryStrategy(RetryStrategy):
    """
    Combines multiple retry strategies.
    
    Useful for handling multiple error types with different strategies.
    """
    
    def __init__(self, strategies: list[RetryStrategy]):
        """
        Initialize composite strategy.
        
        Args:
            strategies: List of retry strategies to combine
        """
        if not strategies:
            raise ValueError("At least one strategy must be provided")
        self.strategies = strategies
        
    async def should_retry(self, error: Exception, attempt: int) -> bool:
        """Check if any strategy says we should retry"""
        for strategy in self.strategies:
            if await strategy.should_retry(error, attempt):
                return True
        return False
    
    def get_delay(self, attempt: int) -> float:
        """Use the maximum delay from all strategies"""
        delays = [s.get_delay(attempt) for s in self.strategies]
        return max(delays)
    
    def get_max_attempts(self) -> int:
        """Use the maximum attempts from all strategies"""
        return max(s.get_max_attempts() for s in self.strategies)


class RetryExecutor:
    """
    Executes operations with retry logic.
    
    Follows SRP - only responsible for retry execution logic.
    """
    
    def __init__(self, strategy: RetryStrategy):
        """
        Initialize retry executor.
        
        Args:
            strategy: Retry strategy to use
        """
        self.strategy = strategy
        
    async def execute(
        self,
        operation: Callable[..., Any],
        *args,
        **kwargs
    ) -> Any:
        """
        Execute an operation with retry logic.
        
        Args:
            operation: Async function to execute
            *args: Positional arguments for operation
            **kwargs: Keyword arguments for operation
            
        Returns:
            Result from successful operation
            
        Raises:
            Last exception if all retries exhausted
        """
        last_error = None
        
        for attempt in range(self.strategy.get_max_attempts()):
            try:
                # Execute the operation
                result = await operation(*args, **kwargs)
                
                if attempt > 0:
                    logger.info(f"Operation succeeded after {attempt} retries")
                    
                return result
                
            except Exception as e:
                last_error = e
                
                # Check if we should retry
                if await self.strategy.should_retry(e, attempt):
                    delay = self.strategy.get_delay(attempt)
                    logger.info(
                        f"Operation failed with {e.__class__.__name__}, "
                        f"retrying in {delay:.3f}s (attempt {attempt + 1}/{self.strategy.get_max_attempts()})"
                    )
                    await asyncio.sleep(delay)
                else:
                    # Don't retry this error
                    logger.error(f"Operation failed with non-retryable error: {e}")
                    raise
                    
        # All retries exhausted
        logger.error(
            f"Operation failed after {self.strategy.get_max_attempts()} attempts"
        )
        raise last_error


# Pre-configured strategies for common use cases
DEFAULT_DEADLOCK_STRATEGY = DeadlockRetryStrategy()
DEFAULT_SERIALIZATION_STRATEGY = SerializationRetryStrategy()
DEFAULT_MVCC_STRATEGY = CompositeRetryStrategy([
    DEFAULT_DEADLOCK_STRATEGY,
    DEFAULT_SERIALIZATION_STRATEGY
])