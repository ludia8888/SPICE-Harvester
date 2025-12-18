# TODO — Critical Fix Tracker

> 기준: **실제 통합 테스트 통과** 시에만 완료 처리

## 🔴 Critical
- [x] i18n 미들웨어가 1MB 초과 JSON 응답을 절단 → 큰 JSON 응답에서도 파싱 가능해야 함  
  - 검증: `pytest backend/tests/unit/middleware/test_middleware_fixes.py::test_i18n_large_json_not_truncated`
- [x] RBAC/인증 미들웨어가 런타임 경로에 적용되어 무권한 쓰기 차단  
  - 검증: `pytest backend/tests/unit/middleware/test_middleware_fixes.py::test_bff_auth_middleware_blocks_unsafe_methods`
- [x] worker S3 동기 호출로 event-loop 블로킹 → heartbeat 누락 위험 제거  
  - 검증: `pytest backend/tests/unit/workers/test_instance_worker_s3.py::test_s3_call_does_not_block_event_loop`
- [x] OMS 쓰기 API 인증/인가 강제 (서비스 토큰 없으면 부팅 실패)  
  - 검증: `pytest backend/tests/test_auth_hardening_e2e.py::test_oms_write_requires_auth`
- [x] Kafka 소비 루프 동기 poll/commit 제거 (async executor 단일 스레드)  
  - 검증: `pytest backend/tests/test_worker_lease_safety_e2e.py::test_heartbeat_not_blocked_by_poll`

## 🟠 High Risk
- [x] Config Monitoring 엔드포인트 500 (key_path 오타) 해결  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_config_monitor_current_returns_payload`
- [x] OTLP exporter 기본값이 “켜짐 + collector 없음” → 기본 off  
  - 검증: `pytest backend/tests/unit/observability/test_tracing_config.py::test_otlp_export_disabled_when_no_endpoint`
- [x] mark_failed() owner 상실 시 침묵 종료 → 명시적 예외  
  - 검증: `pytest backend/tests/test_idempotency_chaos.py::test_registry_mark_failed_owner_mismatch_raises`
- [x] 핵심 idempotency/seq tests가 환경 불일치 시 skip → fail-fast로 전환  
  - 검증: `POSTGRES_URL=... pytest backend/tests/test_idempotency_chaos.py backend/tests/test_sequence_allocator.py`
- [x] Command Status: Redis/Postgres 동시 장애 시 503, 단일 장애는 degraded 상태로 200  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_command_status_dual_outage_returns_503 backend/tests/test_critical_fixes_e2e.py::test_redis_down_rate_limit_and_command_status_fallback`
- [x] BFF GET 엔드포인트(모니터링/설정/관리) 인증 강제  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_bff_sensitive_get_requires_auth`
- [x] BFF/OMS auth 기본값 fail-closed + 명시적 disable만 허용  
  - 검증: `pytest backend/tests/test_auth_hardening_e2e.py::test_auth_disabled_requires_explicit_allow`
- [x] heartbeat 간격 vs lease timeout 구성 검증 (잘못된 설정 시 부팅 실패)  
  - 검증: `pytest backend/tests/test_worker_lease_safety_e2e.py::test_invalid_lease_settings_fail_fast`
- [x] ENABLE_PROCESSED_EVENT_REGISTRY=false 차단 (안전 모드 강제)  
  - 검증: `pytest backend/tests/test_worker_lease_safety_e2e.py::test_registry_disable_rejected`

## 🟡 Latent
- [x] rate_limit 데코레이터가 정상 응답에 헤더 누락 → 전역 미들웨어로 항상 첨부  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_rate_limit_headers_present_on_success`
- [x] i18n이 message/detail/errors만 번역 → description/error 등도 번역  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_i18n_translates_health_description`
- [x] WebSocket 인증 강제 (token 없으면 연결 거부)  
  - 검증: `pytest backend/tests/test_websocket_auth_e2e.py::test_ws_requires_token backend/tests/test_websocket_auth_e2e.py::test_ws_allows_token`
- [x] Event Store TLS 구성 자동화(https면 TLS) + prod에서 http 차단 옵션  
  - 검증: `pytest backend/tests/test_event_store_tls_guard.py::test_event_store_tls_requirement`
- [x] Command Status TTL 구성 가능 + 무기한 보존 옵션  
  - 검증: `pytest backend/tests/test_command_status_ttl_e2e.py::test_command_status_ttl_configurable`

## 🔵 Design Risk
- [x] WIP(Projections) API가 OpenAPI에 노출 → schema 제외  
  - 검증: `pytest backend/tests/test_openapi_contract_smoke.py::test_openapi_stable_contract_smoke`
- [x] Redis 장애 시 rate limiting 로컬 토큰버킷 fallback (트래픽 제한 유지)  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_redis_down_rate_limit_and_command_status_fallback`
