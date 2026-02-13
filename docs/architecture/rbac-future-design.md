# RBAC Future Design (BFF)

## Background
`/backend/bff/middleware/rbac.py` was removed from runtime because it had no active imports/usages and contained only prototype-level code/comments. This document preserves the design intent for a future production implementation without keeping dead code on the execution path.

## Roles
- `ADMIN`: full permissions.
- `MAINTAINER`: protected branch and merge-oriented permissions.
- `DEVELOPER`: standard development permissions.
- `VIEWER`: read-only permissions.

## Permission Model
Example permission domains that were proposed:
- Ontology: `ontology:read`, `ontology:create`, `ontology:update`, `ontology:delete`
- Branch: `branch:create`, `branch:delete`, `branch:merge`, `branch:protect`
- Commit: `commit:create`, `commit:rollback`
- Database: `database:create`, `database:delete`

Design goals:
- Keep role-to-permission mapping explicit and auditable.
- Support resource-level checks (ownership, branch pattern, tenant scope).
- Keep policy evaluation deterministic and side-effect free.

## Branch Protection Concept
Proposed rule fields:
- `branch_pattern`
- `required_roles`
- `require_review`
- `min_reviewers`
- `dismiss_stale_reviews`
- `require_up_to_date`
- `restrict_push`
- `allowed_push_users`

Planned behavior:
- Protected branches (`main`, `production`, `release/*`) require stronger roles.
- Direct push may be blocked except explicit allowlist.
- Branch checks should compose with global permission checks.

## Runtime Integration Plan
1. Authentication boundary
- Extract principal from verified token and build immutable user context.

2. Authorization boundary
- Provide dependency-based permission checks for FastAPI routes.
- Enforce branch-aware authorization for write/merge operations.

3. Audit logging
- Record permission decision, principal, action, resource, and decision reason.
- Keep structured logs suitable for SIEM/search pipelines.

## Extensibility Roadmap
- Conditional policies (time/IP/state-based)
- Temporary delegated permissions (with expiry)
- Multi-tenant role partitioning
- External identity/role sync (OIDC/LDAP/SAML)
- Permission cache with bounded TTL and explicit invalidation

## Testing Strategy
- Role matrix tests (`VIEWER`, `DEVELOPER`, `MAINTAINER`, `ADMIN`)
- Protected branch behavior tests
- Authorization bypass attempts
- Audit-log completeness and integrity checks
- Performance tests for high-volume permission evaluations

## Reintroduction Rules
If RBAC is reintroduced into runtime:
- Implement in a new module with production-ready tests.
- Wire only through explicit imports from active app startup paths.
- Prohibit large commented example blocks in runtime modules.
- Require error taxonomy integration (`ErrorCode.PERMISSION_DENIED`) and structured envelope responses.
