# SPICE HARVESTER Security (Code-Backed)

> Updated: 2026-01-08

## 1) Scope

This document describes **what is implemented today** in this repo. It does not assume external gateways, SSO, or enterprise IAM unless you add them.

## 2) Authentication & Authorization

- **Shared-token auth** for BFF/OMS.
- Token is supplied via `X-Admin-Token` or `Authorization: Bearer <token>`.
- Default behavior: auth **required** unless explicitly disabled for local/dev.

Config:
- BFF: `BFF_REQUIRE_AUTH` (default true), `BFF_ADMIN_TOKEN`/`BFF_WRITE_TOKEN`
- OMS: `OMS_REQUIRE_AUTH` (default true), `OMS_ADMIN_TOKEN`/`OMS_WRITE_TOKEN`
- Disable only with explicit override:
  - `ALLOW_INSECURE_BFF_AUTH_DISABLE=true`
  - `ALLOW_INSECURE_OMS_AUTH_DISABLE=true`

**Not implemented in code**:
- User accounts, roles, multi-tenant ACLs, MFA, JWT issuance/rotation.

## 3) Input Sanitization

- Centralized sanitization in `backend/shared/security/input_sanitizer.py`.
- Applied at BFF/OMS request boundaries to reduce SQL/XSS/path injection risk.
- DB names, class IDs, and filenames are normalized/validated.

## 4) Rate Limiting

- Token bucket rate limiter backed by Redis with local fallback.
- Headers: `X-RateLimit-Limit`, `X-RateLimit-Remaining`, `X-RateLimit-Reset`.
- Config in `backend/shared/config/rate_limit_config.py`.

## 5) Transport Security

- App does **not** terminate TLS; run behind a TLS proxy/load balancer.
- Use network policies to restrict access to OMS/TerminusDB/Kafka/MinIO/ES.

## 6) Data Protection

- Event Store and dataset artifacts are in S3/MinIO; encryption at rest depends on storage configuration.
- Postgres holds idempotency registry, lineage, audit logs, and control-plane data.

## 7) Audit & Lineage

- Audit logs: `backend/shared/services/audit_log_store.py` (hash-chain per partition).
- Lineage: `backend/shared/services/lineage_store.py` (node/edge graph in Postgres).

## 8) Operational Hardening Checklist

- Put BFF/OMS behind an auth gateway (OIDC/SSO) if needed.
- Enable TLS and rotate shared tokens.
- Restrict network access to internal services.
- Back up Postgres + MinIO + lakeFS metadata.
- Monitor `/api/v1/monitoring/*` and `/metrics`.
