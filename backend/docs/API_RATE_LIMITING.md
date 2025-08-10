# API Rate Limiting Implementation

## Overview

Based on Context7 analysis recommendations, we've implemented a comprehensive API rate limiting system for SPICE HARVESTER to protect against abuse and ensure fair resource usage.

## Architecture

### Components

1. **Rate Limiter Middleware** (`shared/middleware/rate_limiter.py`)
   - Token Bucket algorithm implementation
   - Redis-backed for distributed systems
   - Configurable per endpoint

2. **Rate Limit Configuration** (`shared/config/rate_limit_config.py`)
   - Centralized configuration for all endpoints
   - Category-based default limits
   - User tier support

3. **Service Integration**
   - BFF Service: Rate limiter initialized in service container
   - OMS Service: Rate limiter initialized in service container
   - Automatic app.state injection for decorators

## Token Bucket Algorithm

The implementation uses the Token Bucket algorithm, which provides:
- Smooth rate limiting without hard cutoffs
- Configurable refill rates
- Burst handling capabilities
- Redis-backed for distributed consistency

### How it Works

1. Each client (IP/User/API Key) has a bucket with a maximum capacity
2. Tokens are added to the bucket at a constant rate (refill_rate)
3. Each request consumes tokens from the bucket
4. If not enough tokens are available, the request is rejected

## Configuration

### Default Limits by Category

| Category | Requests/Min | Strategy | Cost |
|----------|-------------|----------|------|
| Public Read | 100 | IP | 1 |
| Public Write | 10 | IP | 5 |
| Authenticated Read | 300 | User | 1 |
| Authenticated Write | 60 | User | 3 |
| Admin | 1000 | API Key | 1 |
| Search | 30 | IP | 2 |
| Bulk Operations | 5 | User | 10 |

### Rate Limiting Strategies

1. **IP-based**: Limits based on client IP address
2. **User-based**: Limits based on authenticated user ID
3. **API Key-based**: Limits based on API key
4. **Combined**: Can combine multiple strategies

### User Tiers

Different user tiers get different rate limit multipliers:
- Free: 1.0x
- Basic: 2.0x
- Pro: 5.0x
- Enterprise: 10.0x
- Unlimited: No limits

## Usage

### Using Decorators

```python
from shared.middleware.rate_limiter import rate_limit, RateLimitPresets

# Using presets
@app.get("/api/resource")
@rate_limit(**RateLimitPresets.STANDARD)
async def get_resource():
    return {"data": "resource"}

# Custom configuration
@app.post("/api/create")
@rate_limit(requests=10, window=60, strategy="user", cost=5)
async def create_resource():
    return {"status": "created"}
```

### Rate Limit Headers

All rate-limited endpoints return these headers:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Remaining requests in current window
- `X-RateLimit-Reset`: Unix timestamp when the limit resets
- `Retry-After`: Seconds to wait before retrying (on 429 responses)

### Error Response

When rate limit is exceeded:
```json
{
    "error": "Rate limit exceeded",
    "retry_after": 15,
    "limit": 60,
    "window": 60
}
```
Status Code: 429 Too Many Requests

## Testing

Run the test script to verify rate limiting:

```bash
cd backend
python test_rate_limiting.py
```

The test script will:
1. Test different endpoints with various limits
2. Verify rate limit headers
3. Test burst behavior
4. Test different identification strategies

## Redis Requirements

Rate limiting requires Redis to be running:

```bash
# Check Redis connection
redis-cli ping

# Start Redis if needed
redis-server
```

## Monitoring

### Metrics to Track

1. **Rate Limit Hits**: Number of requests rejected
2. **Token Consumption**: Average tokens consumed per endpoint
3. **Client Distribution**: Top clients by request volume
4. **Error Rates**: 429 response rates

### Logging

Rate limiter logs important events:
- Initialization success/failure
- Redis connection issues
- Rate limit violations (with client info)

## Configuration Examples

### Endpoint-Specific Limits

```python
# In rate_limit_config.py
ENDPOINT_LIMITS = {
    "/api/v1/expensive-operation": RateLimitRule(
        requests=5,
        window=300,  # 5 minutes
        strategy=RateLimitStrategy.USER,
        cost=10
    ),
    "/api/v1/public-search": RateLimitRule(
        requests=20,
        window=60,
        strategy=RateLimitStrategy.IP,
        cost=2
    )
}
```

### Whitelisting

Add trusted IPs to bypass rate limiting:

```python
# In rate_limit_config.py
IP_WHITELIST = [
    "127.0.0.1",
    "10.0.0.0/8",  # Internal network
    "trusted-partner.com"
]
```

## Performance Considerations

1. **Redis Latency**: Rate limiting adds ~1-2ms per request
2. **Memory Usage**: Each client uses ~100 bytes in Redis
3. **TTL**: Buckets expire after 1 hour of inactivity

## Security Benefits

1. **DDoS Protection**: Prevents overwhelming the system
2. **Brute Force Prevention**: Limits authentication attempts
3. **Resource Protection**: Prevents expensive operations abuse
4. **Fair Usage**: Ensures equitable resource distribution

## Future Enhancements

1. **Dynamic Limits**: Adjust limits based on system load
2. **Geographical Rate Limiting**: Different limits by region
3. **Advanced Analytics**: Real-time rate limit dashboards
4. **ML-based Detection**: Identify and block anomalous patterns
5. **Cost-based Billing**: Track API usage for billing

## Rollback Plan

If issues arise with rate limiting:

1. **Disable Globally**: Set environment variable `DISABLE_RATE_LIMITING=true`
2. **Increase Limits**: Modify `rate_limit_config.py` and restart
3. **Remove Decorators**: Comment out `@rate_limit()` decorators
4. **Redis Fallback**: If Redis fails, requests are allowed through

## References

- [Token Bucket Algorithm](https://en.wikipedia.org/wiki/Token_bucket)
- [Redis Rate Limiting Patterns](https://redis.io/docs/manual/patterns/rate-limiting/)
- [API Rate Limiting Best Practices](https://cloud.google.com/architecture/rate-limiting-strategies-techniques)

---

*Implementation based on Context7 analysis and recommendations for SPICE HARVESTER system improvements.*