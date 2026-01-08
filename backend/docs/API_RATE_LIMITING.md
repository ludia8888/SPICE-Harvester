# API Rate Limiting (Code-Backed)

> Updated: 2026-01-08

## Overview

- Token bucket rate limiter with Redis backing.
- Local fallback buckets are used when Redis is unavailable.
- Middleware is installed in BFF/OMS via `shared/services/service_factory.py`.

## Key Files

- `backend/shared/middleware/rate_limiter.py`
- `backend/shared/config/rate_limit_config.py`

## Identification Strategies

- **IP**: default strategy (`client_ip`).
- **User**: uses `X-User-ID` header (or `request.state.user.id` if provided).
- **API Key**: uses `X-API-Key` header (hashed).

If no user/api key is present, the limiter falls back to IP-based limits.

## Headers

- `X-RateLimit-Limit`
- `X-RateLimit-Remaining`
- `X-RateLimit-Reset`
- Optional:
  - `X-RateLimit-Mode`
  - `X-RateLimit-Degraded`
  - `X-RateLimit-Disabled`

## Failure Modes

- Redis outage:
  - `RATE_LIMIT_FAIL_OPEN=true` → allow requests (fail-open)
  - otherwise fail-closed for strict endpoints
- Limiter returns standard error envelope with `rate_limiter_unavailable` when it cannot enforce limits.

## Notes

- Category limits and endpoint overrides are defined in `rate_limit_config.py`.
- User tier multipliers and API key tiers are **configured but not automatically assigned**; you must supply user/api key context.
