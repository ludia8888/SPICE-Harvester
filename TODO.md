# TODO — Critical Fix Tracker

> 기준: **실제 통합 테스트 통과** 시에만 완료 처리

## 🔴 Critical
- [x] i18n 미들웨어가 1MB 초과 JSON 응답을 절단 → 큰 JSON 응답에서도 파싱 가능해야 함  
  - 검증: `pytest backend/tests/unit/middleware/test_middleware_fixes.py::test_i18n_large_json_not_truncated`
- [x] RBAC/인증 미들웨어가 런타임 경로에 적용되어 무권한 쓰기 차단  
  - 검증: `pytest backend/tests/unit/middleware/test_middleware_fixes.py::test_bff_auth_middleware_blocks_unsafe_methods`
- [x] worker S3 동기 호출로 event-loop 블로킹 → heartbeat 누락 위험 제거  
  - 검증: `pytest backend/tests/unit/workers/test_instance_worker_s3.py::test_s3_call_does_not_block_event_loop`

## 🟠 High Risk
- [x] Config Monitoring 엔드포인트 500 (key_path 오타) 해결  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_config_monitor_current_returns_payload`
- [x] OTLP exporter 기본값이 “켜짐 + collector 없음” → 기본 off  
  - 검증: `pytest backend/tests/unit/observability/test_tracing_config.py::test_otlp_export_disabled_when_no_endpoint`
- [x] mark_failed() owner 상실 시 침묵 종료 → 명시적 예외  
  - 검증: `pytest backend/tests/test_idempotency_chaos.py::test_registry_mark_failed_owner_mismatch_raises`
- [x] 핵심 idempotency/seq tests가 환경 불일치 시 skip → fail-fast로 전환  
  - 검증: `POSTGRES_URL=... pytest backend/tests/test_idempotency_chaos.py backend/tests/test_sequence_allocator.py`
- [x] Command Status가 Redis 장애 시 503 → fallback으로 200 응답  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_redis_down_rate_limit_and_command_status_fallback`

## 🟡 Latent
- [x] rate_limit 데코레이터가 정상 응답에 헤더 누락 → 전역 미들웨어로 항상 첨부  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_rate_limit_headers_present_on_success`
- [x] i18n이 message/detail/errors만 번역 → description/error 등도 번역  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_i18n_translates_health_description`

## 🔵 Design Risk
- [x] WIP(Projections) API가 OpenAPI에 노출 → schema 제외  
  - 검증: `pytest backend/tests/test_openapi_contract_smoke.py::test_openapi_stable_contract_smoke`
- [x] Redis 장애 시 rate limiting fail-open → fail-closed(503)  
  - 검증: `pytest backend/tests/test_critical_fixes_e2e.py::test_redis_down_rate_limit_and_command_status_fallback`
