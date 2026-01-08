# CORS Configuration Guide (Code-Backed)

> Updated: 2026-01-08

## Overview

CORS is configured via `shared/config/service_config.py` and applies to BFF/OMS/Funnel.

## Environment Variables

```bash
CORS_ENABLED=true
# JSON array of allowed origins
CORS_ORIGINS=["http://localhost:5173","http://localhost:3000"]
```

Notes:
- `CORS_ORIGINS` **must** be a JSON array string.
- In production, wildcard `*` is rejected.

## Defaults

If `CORS_ORIGINS` is not set, development defaults include common localhost ports (Vite/React/Vue/Angular).

## Debug

Enable debug endpoints:

```bash
export ENABLE_DEBUG_ENDPOINTS=true
```

Then inspect:

```bash
curl http://localhost:8002/debug/cors  # BFF
curl http://localhost:8000/debug/cors  # OMS
curl http://localhost:8003/debug/cors  # Funnel
```
