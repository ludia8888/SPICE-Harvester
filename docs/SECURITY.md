# SPICE HARVESTER Security (Code-Backed)

> Updated: 2026-01-15

## 1) Scope

This document describes **what is implemented today** in this repo. It does not assume external gateways, SSO, or enterprise IAM unless you add them.

## 2) Authentication (BFF/OMS + Agent)

Auth is **required by default** (fail-closed) unless explicitly disabled for local/dev with an additional allow flag.

### 2.1 Service/admin tokens (shared token auth)

- Token source: `X-Admin-Token` or `Authorization: Bearer <token>`
- BFF tokens: `BFF_ADMIN_TOKEN`, `BFF_WRITE_TOKEN` (fallback `ADMIN_API_KEY`, `ADMIN_TOKEN`)
- OMS tokens: `OMS_ADMIN_TOKEN`, `OMS_WRITE_TOKEN` (fallback `ADMIN_API_KEY`, `ADMIN_TOKEN`)
- Rotation: the token env vars accept **comma-separated lists** (e.g., `new,old`) and any configured token is accepted.

Disable only with explicit override:
- `BFF_REQUIRE_AUTH=false` + `ALLOW_INSECURE_BFF_AUTH_DISABLE=true` (or `ALLOW_INSECURE_AUTH_DISABLE=true`)
- `OMS_REQUIRE_AUTH=false` + `ALLOW_INSECURE_OMS_AUTH_DISABLE=true` (or `ALLOW_INSECURE_AUTH_DISABLE=true`)

### 2.2 End-user JWT (delegated auth for Agent Sessions)

When `USER_JWT_ENABLED=true`, BFF can verify end-user JWTs and attach a verified principal to the request context:
- Verification options (pick one for production):
  - `USER_JWT_JWKS_URL` (recommended; supports rotation at the IdP)
  - `USER_JWT_PUBLIC_KEY` (PEM)
  - `USER_JWT_HS256_SECRET` (dev/local; supports comma-separated rotation)
- Optional claim constraints: `USER_JWT_ISSUER`, `USER_JWT_AUDIENCE`, `USER_JWT_ALGORITHMS`

### 2.3 Internal agent boundary (no privilege escalation)

The Agent service calls BFF tools using `BFF_AGENT_TOKEN`. Those calls are only accepted when:
- `X-Admin-Token` matches `BFF_AGENT_TOKEN` (supports comma-separated rotation), **and**
- `X-Delegated-Authorization: Bearer <user_jwt>` is present and verifies successfully (when `USER_JWT_ENABLED=true`)

This prevents “agent/service account” from bypassing end-user policy (AUTH-001/003).

## 3) Authorization / Governance

### 3.1 Tool + model policies (central policy, enforced at boundary)

- Tool allowlist + constraints are enforced at BFF boundary before execution:
  - `backend/shared/services/agent_tool_registry.py`
  - `backend/shared/services/agent_policy_registry.py`
  - `backend/bff/middleware/auth.py`
- Model allowlist + quotas are enforced via:
  - `backend/shared/services/agent_model_registry.py`
  - `backend/shared/services/llm_quota.py`

### 3.2 Database RBAC boundary (optional hard enforcement)

- Per-db principal roles are stored in Postgres table `database_access`.
- Enforcement entrypoint: `backend/shared/security/database_access.py`
- Gate: `BFF_REQUIRE_DB_ACCESS=true` (fail-closed when configured)

## 4) Multi-tenancy (tenant boundary)

- Verified principals can carry `tenant_id` / `org_id` (from JWT claims).
- Control-plane registries and audit partitions are namespaced by tenant id to prevent cross-tenant drift/leakage:
  - `backend/shared/services/agent_session_registry.py`
  - `backend/shared/services/agent_registry.py`
  - `backend/shared/services/audit_log_store.py`

## 5) Input Sanitization

- Centralized sanitization in `backend/shared/security/input_sanitizer.py`.
- Applied at BFF/OMS request boundaries to reduce SQL/XSS/path injection risk.
- DB names, class IDs, and filenames are normalized/validated.

## 6) Rate Limiting

- Token bucket rate limiter backed by Redis with local fallback.
- Headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.
- Config in `backend/shared/config/rate_limit_config.py`.

## 7) Transport Security

- App does **not** terminate TLS; run behind a TLS proxy/load balancer.
- Use network policies to restrict access to OMS/TerminusDB/Kafka/MinIO/ES.

## 8) Data Protection / Key Rotation

- Event Store and dataset artifacts are in S3/MinIO; encryption at rest depends on storage configuration.
- Postgres holds idempotency registry, lineage, audit logs, and control-plane data.
- Stored sensitive fields can be encrypted at rest with `DATA_ENCRYPTION_KEYS` (AESGCM keyring; encrypt with first key, decrypt with any key for rotation): `backend/shared/security/data_encryption.py`

### 8.1 Rotation playbooks (code-backed + tested)

Service/shared tokens (BFF/OMS):
- Rotation is done by setting comma-separated lists (e.g. `ADMIN_TOKEN=new,old`).
- After all clients are updated to `new`, remove `old`.

End-user JWT:
- JWKS (`USER_JWT_JWKS_URL`) is preferred; rotation is handled upstream by your IdP via `kid`.
- HS256 (`USER_JWT_HS256_SECRET`) is supported for local/dev and supports comma-separated rotation.

Data encryption keyring:
- Rotate by setting `DATA_ENCRYPTION_KEYS=new,old` (encrypt uses first key; decrypt tries all keys).
- Do not remove `old` until you have re-encrypted/rewritten old ciphertexts or confirmed they’re no longer needed.

## 9) Audit & Lineage

- Audit logs: `backend/shared/services/audit_log_store.py` (hash-chain per partition).
- Lineage: `backend/shared/services/lineage_store.py` (node/edge graph in Postgres).
- 상세 설계/조회 API: `docs/AUDIT_LOGS.md`

## 10) Operational Hardening Checklist

- Put BFF/OMS behind an auth gateway (OIDC/SSO) if needed.
- Enable TLS and rotate shared tokens.
- Restrict network access to internal services.
- Back up Postgres + MinIO + lakeFS metadata.
- Monitor `/api/v1/monitoring/*` and `/metrics`.
