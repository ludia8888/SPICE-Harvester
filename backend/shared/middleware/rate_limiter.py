"""
Rate Limiting Middleware for API Protection
Implements Token Bucket algorithm with Redis backend
Based on Context7 recommendations for API security
"""

import asyncio
import hashlib
import json
import time
from typing import Optional, Dict, Any, Tuple
from functools import wraps
from datetime import datetime, timedelta

from fastapi import Request, HTTPException, status
from fastapi.responses import JSONResponse
import redis.asyncio as redis

from shared.config.service_config import ServiceConfig
from shared.utils.app_logger import get_logger

logger = get_logger(__name__)


class TokenBucket:
    """
    Token Bucket algorithm implementation for rate limiting
    
    Based on Context7 pattern recommendations:
    - Smooth rate limiting without bursts
    - Configurable refill rate
    - Redis-backed for distributed systems
    """
    
    def __init__(
        self,
        redis_client: redis.Redis,
        capacity: int,
        refill_rate: float,
        key_prefix: str = "rate_limit"
    ):
        """
        Initialize Token Bucket
        
        Args:
            redis_client: Redis client for state storage
            capacity: Maximum number of tokens (requests)
            refill_rate: Tokens added per second
            key_prefix: Redis key prefix for namespacing
        """
        self.redis = redis_client
        self.capacity = capacity
        self.refill_rate = refill_rate
        self.key_prefix = key_prefix
        
    async def consume(self, key: str, tokens: int = 1) -> Tuple[bool, Dict[str, Any]]:
        """
        Try to consume tokens from the bucket
        
        Args:
            key: Unique identifier (user_id, IP, etc.)
            tokens: Number of tokens to consume
            
        Returns:
            Tuple of (success: bool, info: dict with remaining tokens and reset time)
        """
        bucket_key = f"{self.key_prefix}:{key}"
        
        # Lua script for atomic token bucket operations
        lua_script = """
        local key = KEYS[1]
        local capacity = tonumber(ARGV[1])
        local refill_rate = tonumber(ARGV[2])
        local tokens_requested = tonumber(ARGV[3])
        local now = tonumber(ARGV[4])
        
        -- Get current bucket state
        local bucket = redis.call('HGETALL', key)
        local tokens = capacity
        local last_refill = now
        
        if #bucket > 0 then
            tokens = tonumber(bucket[2]) or capacity
            last_refill = tonumber(bucket[4]) or now
        end
        
        -- Calculate tokens to add based on time passed
        local time_passed = now - last_refill
        local tokens_to_add = time_passed * refill_rate
        tokens = math.min(capacity, tokens + tokens_to_add)
        
        -- Check if we can consume the requested tokens
        if tokens >= tokens_requested then
            tokens = tokens - tokens_requested
            redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
            redis.call('EXPIRE', key, 3600)  -- Expire after 1 hour
            
            return {1, tokens, capacity}
        else
            -- Update refill time even if request is denied
            redis.call('HSET', key, 'tokens', tokens, 'last_refill', now)
            redis.call('EXPIRE', key, 3600)
            
            -- Calculate reset time
            local tokens_needed = tokens_requested - tokens
            local reset_time = tokens_needed / refill_rate
            
            return {0, tokens, capacity, reset_time}
        end
        """
        
        # Execute Lua script
        try:
            result = await self.redis.eval(
                lua_script,
                1,
                bucket_key,
                self.capacity,
                self.refill_rate,
                tokens,
                time.time()
            )
            
            success = result[0] == 1
            remaining = int(result[1])
            capacity = int(result[2])
            reset_time = result[3] if len(result) > 3 else 0
            
            return success, {
                "remaining": remaining,
                "capacity": capacity,
                "reset_in": reset_time,
                "refill_rate": self.refill_rate
            }
            
        except Exception as e:
            logger.error(f"Rate limiter error: {e}")
            # On error, allow the request but log it
            return True, {"remaining": -1, "capacity": self.capacity}


class RateLimiter:
    """
    Rate limiting middleware for FastAPI
    Supports multiple strategies based on Context7 recommendations
    """
    
    def __init__(self, redis_url: Optional[str] = None):
        """
        Initialize rate limiter
        
        Args:
            redis_url: Redis connection URL
        """
        self.redis_url = redis_url or ServiceConfig.get_redis_url()
        self.redis_client: Optional[redis.Redis] = None
        self.buckets: Dict[str, TokenBucket] = {}
        
    async def initialize(self):
        """Initialize Redis connection"""
        if not self.redis_client:
            self.redis_client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            await self.redis_client.ping()
            logger.info("Rate limiter Redis connection established")
    
    async def close(self):
        """Close Redis connection"""
        if self.redis_client:
            await self.redis_client.close()
            
    def get_bucket(self, bucket_type: str, capacity: int, refill_rate: float) -> TokenBucket:
        """
        Get or create a token bucket
        
        Args:
            bucket_type: Type of bucket (user, ip, api_key)
            capacity: Maximum tokens
            refill_rate: Tokens per second
            
        Returns:
            TokenBucket instance
        """
        bucket_key = f"{bucket_type}:{capacity}:{refill_rate}"
        
        if bucket_key not in self.buckets:
            self.buckets[bucket_key] = TokenBucket(
                self.redis_client,
                capacity,
                refill_rate,
                key_prefix=f"bucket:{bucket_type}"
            )
            
        return self.buckets[bucket_key]
    
    def get_client_id(self, request: Request, strategy: str = "ip") -> str:
        """
        Get client identifier based on strategy
        
        Args:
            request: FastAPI request
            strategy: Identification strategy (ip, user, api_key)
            
        Returns:
            Client identifier string
        """
        if strategy == "user":
            # Try to get user ID from various sources
            user_id = request.headers.get("X-User-ID")
            if not user_id and hasattr(request.state, "user"):
                user_id = getattr(request.state.user, "id", None)
            return f"user:{user_id}" if user_id else self.get_client_id(request, "ip")
            
        elif strategy == "api_key":
            api_key = request.headers.get("X-API-Key")
            if api_key:
                # Hash API key for privacy
                hashed = hashlib.sha256(api_key.encode()).hexdigest()[:16]
                return f"api:{hashed}"
            return self.get_client_id(request, "ip")
            
        else:  # Default to IP
            # Get real IP considering proxies
            forwarded = request.headers.get("X-Forwarded-For")
            if forwarded:
                ip = forwarded.split(",")[0].strip()
            else:
                ip = request.client.host if request.client else "unknown"
            return f"ip:{ip}"
    
    async def check_rate_limit(
        self,
        request: Request,
        capacity: int = 60,
        refill_rate: float = 1.0,
        strategy: str = "ip",
        tokens: int = 1
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request should be rate limited
        
        Args:
            request: FastAPI request
            capacity: Maximum requests in window
            refill_rate: Requests per second refill
            strategy: Client identification strategy
            tokens: Tokens to consume for this request
            
        Returns:
            Tuple of (allowed: bool, info: dict)
        """
        if not self.redis_client:
            await self.initialize()
            
        client_id = self.get_client_id(request, strategy)
        bucket = self.get_bucket(strategy, capacity, refill_rate)
        
        return await bucket.consume(client_id, tokens)


def rate_limit(
    requests: int = 60,
    window: int = 60,
    strategy: str = "ip",
    cost: int = 1
):
    """
    Rate limiting decorator for FastAPI endpoints
    
    Based on Context7 recommendations for API protection
    
    Args:
        requests: Number of requests allowed
        window: Time window in seconds
        strategy: Client identification (ip, user, api_key)
        cost: Request cost in tokens (for weighted rate limiting)
        
    Example:
        @router.get("/api/resource")
        @rate_limit(requests=100, window=60)
        async def get_resource():
            return {"data": "resource"}
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            # Get or create rate limiter
            if not hasattr(request.app.state, "rate_limiter"):
                request.app.state.rate_limiter = RateLimiter()
                await request.app.state.rate_limiter.initialize()
            
            limiter = request.app.state.rate_limiter
            
            # Calculate refill rate
            refill_rate = requests / window
            
            # Check rate limit
            allowed, info = await limiter.check_rate_limit(
                request,
                capacity=requests,
                refill_rate=refill_rate,
                strategy=strategy,
                tokens=cost
            )
            
            # Add rate limit headers
            headers = {
                "X-RateLimit-Limit": str(requests),
                "X-RateLimit-Remaining": str(max(0, info.get("remaining", 0))),
                "X-RateLimit-Reset": str(int(time.time() + info.get("reset_in", 0)))
            }
            
            if not allowed:
                # Rate limit exceeded
                retry_after = int(info.get("reset_in", 1))
                headers["Retry-After"] = str(retry_after)
                
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail={
                        "error": "Rate limit exceeded",
                        "retry_after": retry_after,
                        "limit": requests,
                        "window": window
                    },
                    headers=headers
                )
            
            # Add headers to response
            response = await func(request, *args, **kwargs)
            if isinstance(response, JSONResponse):
                for key, value in headers.items():
                    response.headers[key] = value
                    
            return response
            
        return wrapper
    return decorator


# Preset rate limit configurations based on Context7 patterns
class RateLimitPresets:
    """Common rate limit configurations"""
    
    # Strict: For sensitive operations
    STRICT = {"requests": 10, "window": 60}
    
    # Standard: For normal API usage
    STANDARD = {"requests": 60, "window": 60}
    
    # Relaxed: For read-heavy operations
    RELAXED = {"requests": 200, "window": 60}
    
    # Burst: Allow short bursts
    BURST = {"requests": 20, "window": 1}
    
    # Search: For search endpoints
    SEARCH = {"requests": 30, "window": 60, "cost": 2}
    
    # Write: For write operations
    WRITE = {"requests": 20, "window": 60, "cost": 3}


# Global rate limiter instance
_rate_limiter: Optional[RateLimiter] = None


async def get_rate_limiter() -> RateLimiter:
    """Get or create global rate limiter instance"""
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = RateLimiter()
        await _rate_limiter.initialize()
    return _rate_limiter