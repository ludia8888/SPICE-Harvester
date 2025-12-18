"""
Rate Limiting Middleware for API Protection
Implements Token Bucket algorithm with Redis backend
Based on Context7 recommendations for API security
"""

import hashlib
import os
import time
from typing import Optional, Dict, Any, Tuple
from functools import wraps
from fastapi import FastAPI, Request, HTTPException, status
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
        key_prefix: str = "rate_limit",
        fail_open: bool = False,
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
        self.fail_open = fail_open
        
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
        
        -- Get current bucket state (avoid HGETALL ordering assumptions)
        local tokens = tonumber(redis.call('HGET', key, 'tokens')) or capacity
        local last_refill = tonumber(redis.call('HGET', key, 'last_refill')) or now
        
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
            if self.fail_open:
                return True, {
                    "remaining": self.capacity,
                    "capacity": self.capacity,
                    "reset_in": 0,
                    "refill_rate": self.refill_rate,
                    "disabled": True,
                    "error": str(e),
                }
            return False, {
                "remaining": 0,
                "capacity": self.capacity,
                "reset_in": 1,
                "refill_rate": self.refill_rate,
                "disabled": True,
                "error": str(e),
            }


class LocalTokenBucket:
    """In-memory token bucket for degraded mode when Redis is unavailable."""

    def __init__(self, capacity: int, refill_rate: float, max_entries: int) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate
        self._max_entries = max_entries
        # key -> (tokens, last_refill_ts, last_seen_ts)
        self._state: Dict[str, Tuple[float, float, float]] = {}

    def _evict_if_needed(self) -> None:
        if len(self._state) <= self._max_entries:
            return
        # Drop oldest entries by last_seen.
        overflow = len(self._state) - self._max_entries
        for key, _ in sorted(self._state.items(), key=lambda item: item[1][2])[:overflow]:
            self._state.pop(key, None)

    async def consume(self, key: str, tokens: int = 1) -> Tuple[bool, Dict[str, Any]]:
        now = time.time()
        current = self._state.get(key)
        if current is None:
            available = float(self.capacity)
            last_refill = now
        else:
            available, last_refill, _ = current
            elapsed = max(0.0, now - last_refill)
            available = min(float(self.capacity), available + (elapsed * self.refill_rate))
            last_refill = now

        if available >= tokens:
            available -= tokens
            allowed = True
            reset_in = 0
        else:
            allowed = False
            needed = tokens - available
            reset_in = (needed / self.refill_rate) if self.refill_rate > 0 else 1

        self._state[key] = (available, last_refill, now)
        self._evict_if_needed()

        return allowed, {
            "remaining": int(available),
            "capacity": self.capacity,
            "reset_in": reset_in,
            "refill_rate": self.refill_rate,
            "mode": "local",
            "degraded": True,
        }


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
        self._local_buckets: Dict[str, LocalTokenBucket] = {}
        self.fail_open = os.getenv("RATE_LIMIT_FAIL_OPEN", "false").lower() in ("true", "1", "yes", "on")
        self._local_max_entries = int(os.getenv("RATE_LIMIT_LOCAL_MAX_ENTRIES", "10000"))
        # Redis reconnect guard.
        self._next_retry_at: float = 0.0
        self._last_init_error: Optional[str] = None
        
    async def initialize(self):
        """Initialize Redis connection"""
        if self.redis_client:
            return

        now = time.time()
        if self._next_retry_at and now < self._next_retry_at:
            return

        # Throttle reconnect attempts to avoid log spam during outages.
        self._next_retry_at = now + 5.0

        try:
            self.redis_client = redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True,
            )
            await self.redis_client.ping()
            self._last_init_error = None
            self._next_retry_at = 0.0
            logger.info("Rate limiter Redis connection established")
        except Exception as e:
            self.redis_client = None
            self._last_init_error = str(e)
            mode = "fail-open" if self.fail_open else "fail-closed"
            logger.warning(f"Rate limiter Redis unavailable ({mode}): {e}")

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
                key_prefix=f"bucket:{bucket_type}",
                fail_open=self.fail_open,
            )
            
        return self.buckets[bucket_key]

    def get_local_bucket(self, bucket_type: str, capacity: int, refill_rate: float) -> LocalTokenBucket:
        bucket_key = f"{bucket_type}:{capacity}:{refill_rate}:local"
        if bucket_key not in self._local_buckets:
            self._local_buckets[bucket_key] = LocalTokenBucket(
                capacity=capacity,
                refill_rate=refill_rate,
                max_entries=self._local_max_entries,
            )
        return self._local_buckets[bucket_key]
    
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
        if not self.redis_client:
            if self.fail_open:
                return True, {
                    "remaining": capacity,
                    "capacity": capacity,
                    "reset_in": 1,
                    "refill_rate": refill_rate,
                    "disabled": True,
                    "error": self._last_init_error,
                }
            client_id = self.get_client_id(request, strategy)
            bucket = self.get_local_bucket(strategy, capacity, refill_rate)
            allowed, info = await bucket.consume(client_id, tokens)
            info["error"] = self._last_init_error
            return allowed, info
            
        client_id = self.get_client_id(request, strategy)
        bucket = self.get_bucket(strategy, capacity, refill_rate)

        allowed, info = await bucket.consume(client_id, tokens)
        if info.get("disabled") and not self.fail_open:
            local_bucket = self.get_local_bucket(strategy, capacity, refill_rate)
            allowed, local_info = await local_bucket.consume(client_id, tokens)
            local_info["error"] = info.get("error")
            return allowed, local_info

        return allowed, info


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
        async def get_resource(request: Request = Depends()):
            return {"data": "resource"}
    """
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            # 🔥 FIX! Find the Request object in function arguments
            request = None
            
            # Look for Request object in positional args
            for arg in args:
                if isinstance(arg, Request):
                    request = arg
                    break
            
            # Look for Request object in keyword args
            if request is None:
                for key, value in kwargs.items():
                    if isinstance(value, Request):
                        request = value
                        break
            
            # If no Request found, skip rate limiting (for non-HTTP contexts)
            if request is None:
                return await func(*args, **kwargs)
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

            # Observability: record rate limit checks/rejections (best-effort).
            metrics = getattr(getattr(request, "app", None), "state", None)
            metrics = getattr(metrics, "metrics_collector", None) if metrics else None
            if metrics is not None and hasattr(metrics, "record_rate_limit"):
                try:
                    metrics.record_rate_limit(
                        endpoint=request.url.path,
                        rejected=not allowed,
                        strategy=strategy,
                    )
                except Exception:
                    pass
            
            # Add rate limit headers
            headers = {
                "X-RateLimit-Limit": str(requests),
                "X-RateLimit-Remaining": str(max(0, info.get("remaining", 0))),
                "X-RateLimit-Reset": str(int(time.time() + info.get("reset_in", 0)))
            }
            mode = info.get("mode")
            if mode:
                headers["X-RateLimit-Mode"] = str(mode)
            if info.get("degraded"):
                headers["X-RateLimit-Degraded"] = "true"
            if info.get("disabled"):
                headers["X-RateLimit-Disabled"] = "true"
            request.state.rate_limit_headers = headers
            
            if not allowed:
                # Rate limit exceeded
                retry_after = int(info.get("reset_in", 1))
                headers["Retry-After"] = str(retry_after)
                if info.get("disabled"):
                    raise HTTPException(
                        status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                        detail={
                            "error": "rate_limiter_unavailable",
                            "retry_after": retry_after,
                            "limit": requests,
                            "window": window,
                        },
                        headers=headers,
                    )

                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail={
                        "error": "Rate limit exceeded",
                        "retry_after": retry_after,
                        "limit": requests,
                        "window": window,
                    },
                    headers=headers,
                )
            
            # Call endpoint normally (do not inject Request twice).
            response = await func(*args, **kwargs)
            if hasattr(response, "headers"):
                for key, value in headers.items():
                    response.headers[key] = value
                    
            return response
            
        return wrapper
    return decorator


def install_rate_limit_headers_middleware(app: FastAPI) -> None:
    @app.middleware("http")
    async def _rate_limit_headers_middleware(request: Request, call_next):
        response = await call_next(request)
        headers = getattr(request.state, "rate_limit_headers", None)
        if headers:
            for key, value in headers.items():
                if key not in response.headers:
                    response.headers[key] = value
        return response


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
