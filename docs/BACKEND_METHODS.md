# Backend Method Index

> Generated: 2026-02-14T10:15:49+09:00
> Scope: backend/**/*.py (including scripts and tests, excluding __pycache__)

## action_outbox_worker

### `backend/action_outbox_worker/__init__.py`

### `backend/action_outbox_worker/main.py`
- **Functions**
  - `_resolve_overlay_branch(log)` (line 48): no docstring
  - `async main()` (line 424): no docstring
- **Classes**
  - `ActionOutboxWorker` (line 62): no docstring
    - `__init__(self)` (line 63): no docstring
    - `async initialize(self)` (line 72): no docstring
    - `async shutdown(self)` (line 83): no docstring
    - `async _get_event_seq(self, event_id)` (line 89): no docstring
    - `async _emit_action_applied(self, log)` (line 96): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 150): no docstring
    - `async _append_queue_entries(self, log)` (line 153): no docstring
    - `async _reconcile_log(self, log)` (line 292): no docstring
    - `async run(self)` (line 332): no docstring

## action_worker

### `backend/action_worker/__init__.py`

### `backend/action_worker/main.py`
- **Functions**
  - `async main()` (line 1854): no docstring
- **Classes**
  - `_ActionCommandPayload` (line 112): no docstring
  - `_ActionCommandParseError` (line 119): no docstring
  - `_ActionRejected` (line 123): Used to short-circuit retries when the ActionLog is already finalized with a rejection result.
  - `ActionWorker` (line 127): no docstring
    - `__init__(self)` (line 128): no docstring
    - `async initialize(self)` (line 165): no docstring
    - `async shutdown(self)` (line 219): no docstring
    - `_parse_payload(self, payload)` (line 237): no docstring
    - `_fallback_metadata(self, payload)` (line 290): no docstring
    - `_registry_key(self, payload)` (line 293): no docstring
    - `async _process_payload(self, payload)` (line 304): no docstring
    - `_span_name(self, payload)` (line 339): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 342): no docstring
    - `_metric_event_name(self, payload)` (line 363): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 366): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 397): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 413): no docstring
    - `_is_retryable_error(exc, payload)` (line 429): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 438): no docstring
    - `async _send_to_dlq(self, msg, error, attempt_count, payload, raw_payload, stage, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 466): no docstring
    - `async run(self)` (line 497): no docstring
    - `async _enforce_permission(self, db_name, submitted_by, submitted_by_type, action_spec)` (line 501): no docstring
    - `async _check_writeback_dataset_acl_alignment(self, db_name, submitted_by, submitted_by_type, actor_role, ontology_commit_id, resources, class_ids)` (line 533): no docstring
    - `async _execute_action(self, db_name, action_log_id, command, envelope)` (line 760): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 1683): no docstring
    - `async _write_patchset_commit(self, repository, branch, action_log_id, patchset, metadata_doc)` (line 1686): no docstring
    - `async _append_queue_entries(self, repository, branch, patchset_commit_id, action_log_id, action_applied_seq)` (line 1739): no docstring

## agent

### `backend/agent/__init__.py`

### `backend/agent/main.py`
- **Functions**
  - `async lifespan(app)` (line 25): Initialize dependencies.

### `backend/agent/models.py`
- **Classes**
  - `AgentToolCall` (line 9): no docstring
  - `AgentRunRequest` (line 27): no docstring
  - `AgentRunResponse` (line 35): no docstring
  - `AgentRunSummary` (line 42): no docstring

### `backend/agent/models_pipeline.py`

### `backend/agent/routers/__init__.py`

### `backend/agent/routers/agent.py`
- **Functions**
  - `_resolve_principal(request)` (line 24): no docstring
  - `_resolve_tenant_id(request)` (line 29): no docstring
  - `_actor_label(principal_type, principal_id)` (line 36): no docstring
  - `_request_meta(request, body)` (line 40): no docstring
  - `_step_id(index)` (line 48): no docstring
  - `_resolve_tool_id(tool_call)` (line 52): no docstring
  - `_extract_plan_id(context)` (line 61): no docstring
  - `_extract_plan_snapshot(context)` (line 75): no docstring
  - `_extract_risk_level(context)` (line 84): no docstring
  - `async _record_run_start(agent_registry, run_id, tenant_id, actor, requester, delegated_actor, body, request_meta)` (line 91): no docstring
  - `async _execute_agent_run(runtime, state, request_id, agent_registry, tenant_id)` (line 139): no docstring
  - `async create_agent_run(request, body)` (line 239): no docstring
  - `async get_agent_run(request, run_id, include_events, limit)` (line 330): no docstring
  - `async list_agent_run_events(request, run_id, limit, offset)` (line 425): no docstring

### `backend/agent/services/__init__.py`

### `backend/agent/services/agent_policy.py`
- **Functions**
  - `_stable_unit_interval(seed)` (line 19): no docstring
  - `compute_backoff_s(seed, attempt, base_delay_s, max_delay_s)` (line 25): no docstring
  - `compute_retry_delay_s(seed, attempt, base_delay_ms, max_delay_ms, jitter_strategy)` (line 34): no docstring
  - `_enterprise_field(enterprise, key)` (line 52): no docstring
  - `_normalize_code(value)` (line 61): no docstring
  - `_enterprise_int(enterprise, key)` (line 65): no docstring
  - `_enterprise_bool(enterprise, key)` (line 75): no docstring
  - `_method_is_safe_to_retry(method)` (line 82): no docstring
  - `decide_policy(tool_call, result, context)` (line 86): no docstring
- **Classes**
  - `AgentPolicyDecision` (line 11): no docstring

### `backend/agent/services/agent_run_loop.py`
- **Functions**
  - `async _run_single_step(runtime, semaphore, run_id, actor, step_index, tool_call, attempts, context, dry_run, request_headers, request_id)` (line 41): Execute exactly one tool-call step, applying deterministic enterprise retry policy.
  - `async run_agent_steps(runtime, initial_state)` (line 206): Execute the provided steps in order, stopping at the first failure.
- **Classes**
  - `AgentState` (line 21): State envelope for deterministic single-threaded agent step execution.

### `backend/agent/services/agent_runtime.py`
- **Functions**
  - `_clean_url(value)` (line 61): no docstring
  - `_safe_json(obj)` (line 65): no docstring
  - `_normalize_error_code(value)` (line 74): no docstring
  - `_extract_error_message(payload)` (line 86): no docstring
  - `_compute_tool_run_id(run_id, step_id, step_index, attempt)` (line 118): Deterministic-ish tool run id for observability.
  - `_extract_retry_after_ms(headers)` (line 132): no docstring
  - `_is_agent_proxy_path(path)` (line 157): no docstring
  - `_resolve_template_token(token, context)` (line 162): no docstring
  - `_resolve_template_string(value, context)` (line 206): no docstring
  - `_resolve_templates(obj, context)` (line 231): no docstring
  - `_artifact_value_from_response(payload)` (line 243): Choose the most useful artifact value from a tool response payload.
  - `_compact_tool_payload(payload)` (line 257): Best-effort compaction for large tool payloads.
  - `_compact_artifact_value(obj, depth)` (line 285): no docstring
  - `_walk_json_for_key(obj, wanted_keys, max_depth, max_nodes)` (line 310): no docstring
  - `_coerce_scalar(value)` (line 338): no docstring
  - `_extract_step_outputs(payload)` (line 348): no docstring
  - `_extract_scope(context)` (line 366): no docstring
  - `_normalize_status(value)` (line 386): no docstring
  - `_extract_command_id_from_url(url)` (line 393): no docstring
  - `_extract_command_id(payload)` (line 400): no docstring
  - `_extract_command_status(payload)` (line 426): no docstring
  - `_extract_pipeline_job_id(payload)` (line 445): no docstring
  - `_extract_pipeline_id(payload)` (line 461): no docstring
  - `_extract_pipeline_id_from_path(path)` (line 477): no docstring
  - `_extract_status_url(payload)` (line 484): no docstring
  - `_extract_progress(payload)` (line 498): no docstring
  - `_method_is_write(method)` (line 542): no docstring
  - `_tool_call_expects_pipeline_job(tool_id, path)` (line 546): no docstring
  - `_extract_overlay_status(payload)` (line 556): no docstring
  - `_extract_enterprise_legacy_code(payload)` (line 581): no docstring
  - `_iter_error_candidates(payload)` (line 593): no docstring
  - `_extract_error_key(payload)` (line 604): no docstring
  - `_extract_enterprise(payload)` (line 617): no docstring
  - `_extract_api_code(payload)` (line 625): no docstring
  - `_extract_api_category(payload)` (line 633): no docstring
  - `_extract_retryable(payload)` (line 641): no docstring
  - `_extract_action_log_signals(payload)` (line 654): no docstring
  - `_extract_action_simulation_signals(payload)` (line 675): Extract minimal, policy-relevant signals from ActionSimulation responses.
  - `_extract_action_simulation_rejection(payload)` (line 736): If the payload is an ActionSimulation response and the effective scenario is REJECTED,
- **Classes**
  - `AgentRuntimeConfig` (line 783): no docstring
  - `AgentRuntime` (line 805): no docstring
    - `__init__(self, event_store, audit_store, config)` (line 806): no docstring
    - `from_env(cls, event_store, audit_store)` (line 818): no docstring
    - `_resolve_base_url(self, tool_call)` (line 856): no docstring
    - `_preview_payload(self, obj)` (line 864): no docstring
    - `_payload_size(self, obj)` (line 869): no docstring
    - `_forward_headers(self, headers, actor)` (line 876): no docstring
    - `_resolve_status_url(self, command_id, payload)` (line 921): no docstring
    - `_resolve_ws_token(self, request_headers)` (line 931): no docstring
    - `_resolve_ws_url(self, command_id, token)` (line 947): no docstring
    - `_update_progress_context(self, context, command_id, status, progress_payload)` (line 960): no docstring
    - `async _fetch_command_status(self, status_url, actor, request_headers)` (line 979): no docstring
    - `async _wait_for_command_completion(self, run_id, actor, step_index, attempt, command_id, initial_payload, request_id, request_headers, context)` (line 1001): no docstring
    - `_update_pipeline_progress_context(self, context, pipeline_id, job_id, status, run_payload)` (line 1140): no docstring
    - `async _fetch_pipeline_runs(self, pipeline_id, actor, request_headers, limit)` (line 1164): no docstring
    - `async _wait_for_pipeline_run_completion(self, run_id, actor, step_index, attempt, pipeline_id, job_id, initial_payload, request_id, request_headers, context)` (line 1191): no docstring
    - `async record_event(self, event_type, run_id, actor, status, data, request_id, step_index, resource_type, resource_id, error)` (line 1301): no docstring
    - `async execute_tool_call(self, run_id, actor, step_index, attempt, tool_call, context, dry_run, request_headers, request_id)` (line 1351): no docstring

## analysis

### `backend/analysis/system_improvement_analysis.py`
- **Functions**
  - `async main()` (line 318): Run system analysis
- **Classes**
  - `SystemAnalyzer` (line 15): Analyzes system using Context7-like intelligence
    - `async analyze_current_system(self)` (line 18): Comprehensive system analysis
    - `async prioritize_improvements(self, analysis)` (line 167): Prioritize improvements based on impact and effort
    - `generate_implementation_plan(self, improvement)` (line 189): Generate detailed implementation plan for an improvement

## bff

### `backend/bff/__init__.py`

### `backend/bff/dependencies.py`
- **Functions**
  - `async get_terminus_service(oms_client)` (line 551): Get TerminusService with modern dependency injection
  - `async check_bff_dependencies_health(container)` (line 577): Check health of all BFF dependencies
- **Classes**
  - `BFFDependencyProvider` (line 50): Modern dependency provider for BFF services
    - `async get_oms_client(container)` (line 59): Get OMS client from container
    - `async get_action_log_registry(container)` (line 84): Get ActionLogRegistry (Postgres-backed) from container.
  - `TerminusService` (line 115): OMS client wrapper for TerminusService compatibility - Modernized version
    - `__init__(self, oms_client)` (line 123): Initialize with OMS client dependency
    - `async list_databases(self)` (line 133): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 144): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 149): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 154): 데이터베이스 정보 조회
    - `async list_classes(self, db_name, branch)` (line 159): 클래스 목록 조회
    - `async create_class(self, db_name, class_data, branch, headers)` (line 167): 클래스 생성
    - `async get_class(self, db_name, class_id, branch)` (line 182): 클래스 조회
    - `async update_class(self, db_name, class_id, class_data, expected_seq, branch, headers)` (line 207): 클래스 업데이트
    - `async delete_class(self, db_name, class_id, expected_seq, branch, headers)` (line 223): 클래스 삭제
    - `async query_database(self, db_name, query)` (line 238): 데이터베이스 쿼리
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 244): 브랜치 생성 - 실제 OMS API 호출
    - `async delete_branch(self, db_name, branch_name)` (line 255): 브랜치 삭제 - 실제 OMS API 호출
    - `async checkout(self, db_name, target, target_type)` (line 264): 체크아웃 - 실제 OMS API 호출
    - `async commit_changes(self, db_name, message, author, branch)` (line 276): 변경사항 커밋 - 실제 OMS API 호출
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 293): 커밋 히스토리 조회 - 실제 OMS API 호출
    - `async get_diff(self, db_name, base, compare)` (line 300): 차이 비교 - 실제 OMS API 호출
    - `async merge_branches(self, db_name, source, target, strategy, message, author)` (line 310): 브랜치 병합 - 실제 OMS API 호출
    - `async rollback(self, db_name, target_commit, create_branch, branch_name)` (line 333): 롤백 - 실제 OMS API 호출
    - `async get_branch_info(self, db_name, branch_name)` (line 354): 브랜치 정보 조회 - 실제 OMS API 호출
    - `async simulate_merge(self, db_name, source_branch, target_branch, strategy)` (line 366): 병합 시뮬레이션 - 충돌 감지 without 실제 병합
    - `async resolve_merge_conflicts(self, db_name, source_branch, target_branch, resolutions, strategy, message, author)` (line 384): 수동 충돌 해결 및 병합 실행
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references, headers)` (line 416): 고급 관계 관리 기능을 포함한 온톨로지 생성 - OMS API 호출
    - `async validate_relationships(self, db_name, ontology_data, branch, headers)` (line 451): 온톨로지 관계 검증 - OMS API 호출 (no write).
    - `async detect_circular_references(self, db_name, branch, new_ontology, headers)` (line 475): 순환 참조 탐지 - OMS API 호출 (no write).
    - `async analyze_relationship_network(self, db_name, branch, headers)` (line 499): 관계 네트워크 분석 - OMS API 호출 (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch, headers)` (line 521): 관계 경로 탐색 - OMS API 호출 (no write).

### `backend/bff/main.py`
- **Functions**
  - `async lifespan(app)` (line 652): Modern application lifecycle management
  - `async get_oms_client()` (line 897): Get OMS client from BFF container
  - `async get_label_mapper()` (line 908): Get label mapper from BFF container
  - `async get_google_sheets_service()` (line 919): Get Google Sheets service from BFF container
  - `async get_connector_registry()` (line 930): Get ConnectorRegistry from BFF container
  - `async get_dataset_registry()` (line 941): Get DatasetRegistry from BFF container
  - `async get_dataset_profile_registry()` (line 952): Get DatasetProfileRegistry from BFF container
  - `async get_pipeline_registry()` (line 963): Get PipelineRegistry from BFF container
  - `async get_pipeline_plan_registry()` (line 981): Get PipelinePlanRegistry from BFF container
  - `async get_objectify_registry()` (line 992): no docstring
  - `async get_agent_registry()` (line 997): no docstring
  - `async get_agent_session_registry()` (line 1003): no docstring
  - `async get_agent_policy_registry()` (line 1009): no docstring
  - `async get_pipeline_executor()` (line 1015): Get PipelineExecutor from BFF container
- **Classes**
  - `BFFServiceContainer` (line 132): BFF-specific service container to manage BFF services
    - `__init__(self, container, settings)` (line 140): no docstring
    - `async initialize_bff_services(self)` (line 145): Initialize BFF-specific services
    - `async _initialize_oms_client(self)` (line 202): Initialize OMS client with health check
    - `async _initialize_label_mapper(self)` (line 223): Initialize label mapper
    - `async _initialize_type_inference(self)` (line 229): Initialize type inference service
    - `async _initialize_websocket_service(self)` (line 243): Initialize WebSocket notification service
    - `async _initialize_rate_limiter(self)` (line 265): Initialize rate limiting service
    - `async _initialize_connector_registry(self)` (line 281): Initialize Postgres-backed connector registry.
    - `async _initialize_dataset_registry(self)` (line 293): Initialize Postgres-backed dataset registry.
    - `async _initialize_dataset_profile_registry(self)` (line 304): Initialize Postgres-backed dataset profile registry.
    - `async _initialize_pipeline_registry(self)` (line 315): Initialize Postgres-backed pipeline registry.
    - `async _initialize_pipeline_plan_registry(self)` (line 326): Initialize Postgres-backed pipeline plan registry.
    - `async _initialize_objectify_registry(self)` (line 337): Initialize Postgres-backed objectify registry.
    - `async _initialize_agent_registry(self)` (line 348): Initialize Postgres-backed agent registry.
    - `async _initialize_agent_session_registry(self)` (line 359): Initialize Postgres-backed agent session registry.
    - `async _initialize_agent_policy_registry(self)` (line 370): Initialize Postgres-backed agent policy registry.
    - `async _initialize_agent_tool_registry(self)` (line 381): Initialize Postgres-backed agent tool registry (internal allowlist/policy).
    - `async _initialize_pipeline_executor(self)` (line 400): Initialize pipeline executor (preview/build engine).
    - `async _initialize_google_sheets_service(self)` (line 418): Initialize Google Sheets service (connector library)
    - `async shutdown_bff_services(self)` (line 437): Shutdown BFF-specific services
    - `get_oms_client(self)` (line 562): Get OMS client instance
    - `get_label_mapper(self)` (line 568): Get label mapper instance
    - `get_google_sheets_service(self)` (line 574): Get Google Sheets service instance
    - `get_connector_registry(self)` (line 580): Get connector registry instance
    - `get_dataset_registry(self)` (line 586): Get dataset registry instance
    - `get_dataset_profile_registry(self)` (line 592): Get dataset profile registry instance
    - `get_pipeline_registry(self)` (line 598): Get pipeline registry instance
    - `get_pipeline_plan_registry(self)` (line 604): Get pipeline plan registry instance
    - `get_objectify_registry(self)` (line 610): Get objectify registry instance
    - `get_agent_registry(self)` (line 616): Get agent registry instance
    - `get_agent_session_registry(self)` (line 622): Get agent session registry instance
    - `get_agent_policy_registry(self)` (line 628): Get agent policy registry instance
    - `get_agent_tool_registry(self)` (line 634): Get agent tool registry instance
    - `get_pipeline_executor(self)` (line 640): Get pipeline executor instance

### `backend/bff/middleware/__init__.py`

### `backend/bff/middleware/auth.py`
- **Functions**
  - `_dev_master_auth_enabled()` (line 56): no docstring
  - `_attach_dev_master_principal(request)` (line 64): no docstring
  - `_approx_token_count(payload)` (line 81): no docstring
  - `_resolve_agent_tool_run_id(request)` (line 85): no docstring
  - `_error_response(request, status_code, message, code, category, detail, context, headers)` (line 94): no docstring
  - `_internal_error_payload(request, message, detail)` (line 126): no docstring
  - `_set_scope_header(request, name, value)` (line 148): Attach a trusted header into the ASGI scope so downstream code that still
  - `_attach_verified_principal(request, principal)` (line 163): no docstring
  - `_compile_agent_tool_path(pattern)` (line 181): no docstring
  - `_path_matches_tool_policy(policy, request_path)` (line 195): no docstring
  - `_compile_agent_tool_path_params(pattern)` (line 202): no docstring
  - `_extract_agent_tool_path_params(policy, request_path)` (line 231): no docstring
  - `_resolve_agent_tool_id(request)` (line 241): no docstring
  - `async _capture_agent_tool_request(request)` (line 246): no docstring
  - `async _maybe_start_session_tool_call(request)` (line 278): no docstring
  - `async _finalize_session_tool_call(request, response, terminal_status)` (line 365): no docstring
  - `_resolve_agent_tool_registry(request)` (line 446): no docstring
  - `_resolve_agent_session_registry(request)` (line 457): no docstring
  - `_resolve_agent_policy_registry(request)` (line 468): no docstring
  - `_resolve_agent_registry(request)` (line 479): no docstring
  - `async _get_cached_tenant_policy(request, tenant_id)` (line 490): no docstring
  - `async _enforce_internal_agent_tool_policy(request)` (line 504): no docstring
  - `async _compute_agent_tool_idempotency_digest(request, tool_id)` (line 799): no docstring
  - `async _wait_for_tool_idempotency(registry, tenant_id, idempotency_key, timeout_s, interval_s)` (line 831): no docstring
  - `async _maybe_replay_or_start_tool_idempotency(request)` (line 852): no docstring
  - `async _finalize_tool_idempotency(request, response)` (line 964): no docstring
  - `async _finalize_tool_idempotency_error(request, exc)` (line 1033): no docstring
  - `ensure_bff_auth_configured()` (line 1058): no docstring
  - `async _bff_auth_handle_missing_token(request, call_next, ctx)` (line 1094): no docstring
  - `async _bff_auth_handle_agent_token(request, call_next, ctx)` (line 1110): no docstring
  - `async _bff_auth_handle_expected_token(request, call_next, ctx)` (line 1230): no docstring
  - `async _bff_auth_handle_user_jwt(request, call_next, ctx)` (line 1280): no docstring
  - `async _bff_auth_handle_no_token_configured(request, call_next, ctx)` (line 1303): no docstring
  - `async _bff_auth_handle_invalid_credentials(request, call_next, ctx)` (line 1317): no docstring
  - `install_bff_auth_middleware(app)` (line 1329): no docstring
  - `async enforce_bff_websocket_auth(websocket, token)` (line 1366): no docstring
- **Classes**
  - `_BffAuthContext` (line 1085): no docstring

### `backend/bff/routers/__init__.py`

### `backend/bff/routers/actions.py`
- **Functions**
  - `async submit_action(db_name, action_type_id, request, http_request, base_branch, overlay_branch, oms_client)` (line 29): no docstring
  - `async simulate_action(db_name, action_type_id, request, http_request, oms_client)` (line 55): Action writeback simulation (dry-run) surface.
  - `async get_action_log(db_name, action_log_id, http_request, action_logs)` (line 79): no docstring
  - `async list_action_logs(db_name, http_request, status_filter, action_type_id, submitted_by, limit, offset, action_logs)` (line 96): no docstring
  - `async list_action_simulations(db_name, http_request, action_type_id, limit, offset)` (line 121): no docstring
  - `async get_action_simulation(db_name, simulation_id, http_request, include_versions, version_limit)` (line 140): no docstring
  - `async list_action_simulation_versions(db_name, simulation_id, http_request, limit, offset)` (line 159): no docstring
  - `async get_action_simulation_version(db_name, simulation_id, version, http_request)` (line 178): no docstring

### `backend/bff/routers/admin.py`

### `backend/bff/routers/admin_deps.py`
- **Functions**
  - `async require_admin(request)` (line 25): Minimal admin guard for operational endpoints.

### `backend/bff/routers/admin_instance_rebuild.py`
- **Functions**
  - `async rebuild_instance_index_endpoint(db_name, background_tasks, branch, task_manager, redis_service, elasticsearch_service)` (line 34): Trigger an async instance index rebuild for a database.
  - `async get_rebuild_status(db_name, task_id, task_manager, redis_service)` (line 106): Check the status of a rebuild task.

### `backend/bff/routers/admin_lakefs.py`
- **Functions**
  - `async list_lakefs_credentials(registry)` (line 32): no docstring
  - `async upsert_lakefs_credentials(payload, request, registry)` (line 41): no docstring
- **Classes**
  - `LakeFSCredentialsUpsertRequest` (line 21): Upsert request for lakeFS credentials stored in Postgres (encrypted).

### `backend/bff/routers/admin_recompute_projection.py`
- **Functions**
  - `async recompute_projection(http_request, request, background_tasks, task_manager, redis_service, audit_store, lineage_store, elasticsearch_service)` (line 33): no docstring
  - `async get_recompute_projection_result(task_id, task_manager, redis_service)` (line 58): no docstring
  - `async reindex_instances_endpoint(db_name, branch, delete_index_first, elasticsearch_service)` (line 78): Rebuild the ES instances index by re-running all active objectify jobs.

### `backend/bff/routers/admin_replay.py`
- **Functions**
  - `async replay_instance_state(request, background_tasks, storage_service, task_manager, redis_service)` (line 30): no docstring
  - `async get_replay_result(task_id, task_manager, redis_service)` (line 49): no docstring
  - `async get_replay_trace(task_id, command_id, include_audit, audit_limit, include_lineage, lineage_direction, lineage_max_depth, lineage_max_nodes, lineage_max_edges, timeline_limit, task_manager, redis_service, audit_store, lineage_store)` (line 63): no docstring
  - `async cleanup_old_replay_results(older_than_hours, redis_service)` (line 103): no docstring

### `backend/bff/routers/admin_system.py`
- **Functions**
  - `async get_system_health(task_manager, redis_service)` (line 22): no docstring

### `backend/bff/routers/admin_task_monitor.py`
- **Functions**
  - `async monitor_admin_task(task_id, task_manager)` (line 19): no docstring

### `backend/bff/routers/agent_proxy.py`
- **Functions**
  - `async create_pipeline_run(request, body, llm, redis_service, audit_store, dataset_registry, pipeline_registry, plan_registry)` (line 49): Pipeline agent runs are handled in the BFF as a single autonomous loop + MCP tools.
  - `async stream_pipeline_run(request, body, llm, redis_service, audit_store, dataset_registry, pipeline_registry, plan_registry)` (line 140): SSE 스트리밍 버전의 Pipeline Agent API.

### `backend/bff/routers/ai.py`
- **Functions**
  - `async ai_intent(body, request, llm, redis_service, audit_store, sessions, dataset_registry)` (line 26): no docstring
  - `async translate_query_plan(db_name, body, request, llm, redis_service, audit_store, oms, sessions, dataset_registry)` (line 50): no docstring
  - `async ai_query(db_name, body, request, llm, redis_service, audit_store, lineage_store, oms, mapper, terminus, sessions, dataset_registry)` (line 77): no docstring

### `backend/bff/routers/audit.py`
- **Functions**
  - `async list_audit_logs(partition_key, action, status_filter, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset, audit_store)` (line 24): no docstring
  - `async get_chain_head(partition_key, audit_store)` (line 67): no docstring

### `backend/bff/routers/ci_webhooks.py`
- **Functions**
  - `async ingest_ci_result(body, request, sessions)` (line 55): no docstring
- **Classes**
  - `AgentSessionCIResultIngestRequest` (line 35): no docstring

### `backend/bff/routers/command_status.py`
- **Functions**
  - `async get_command_status(command_id, oms)` (line 28): Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

### `backend/bff/routers/context7.py`
- **Functions**
  - `_context7_unavailable_exc()` (line 38): no docstring
  - `async get_context7_client()` (line 49): no docstring
  - `async search_context7(request, client)` (line 64): no docstring
  - `async get_entity_context(entity_id, client)` (line 70): no docstring
  - `async add_knowledge(request, client)` (line 76): no docstring
  - `async create_entity_link(request, client)` (line 82): no docstring
  - `async analyze_ontology(request, client, oms_client)` (line 88): no docstring
  - `async get_ontology_suggestions(db_name, class_id, client)` (line 98): no docstring
  - `async check_context7_health(client)` (line 108): no docstring

### `backend/bff/routers/context_tools.py`
- **Functions**
  - `_policy_set(value)` (line 25): no docstring
  - `_enforce_policy_value(allowed, value)` (line 35): no docstring
  - `_filter_dataset_ids(dataset_ids, allowed_dataset_ids)` (line 52): no docstring
  - `async describe_datasets(body, request, dataset_registry, policy_registry)` (line 69): no docstring
  - `async snapshot_ontology(body, request, oms_client, policy_registry)` (line 129): no docstring
- **Classes**
  - `OntologySnapshotRequest` (line 40): no docstring
  - `DatasetDescribeRequest` (line 46): no docstring

### `backend/bff/routers/data_connector.py`

### `backend/bff/routers/data_connector_browse.py`
- **Functions**
  - `async list_google_sheets_spreadsheets(http_request, connection_id, query, limit, connector_registry, google_sheets_service)` (line 33): no docstring
  - `async list_google_sheets_worksheets(sheet_id, http_request, connection_id, connector_registry, google_sheets_service)` (line 61): no docstring

### `backend/bff/routers/data_connector_connections.py`
- **Functions**
  - `async list_google_sheets_connections(http_request, connector_registry)` (line 32): no docstring
  - `async delete_google_sheets_connection(connection_id, http_request, connector_registry)` (line 57): no docstring

### `backend/bff/routers/data_connector_deps.py`
- **Functions**
  - `async get_google_sheets_service()` (line 23): Import here to avoid circular dependency.
  - `async get_connector_registry()` (line 30): Import here to avoid circular dependency.

### `backend/bff/routers/data_connector_oauth.py`
- **Functions**
  - `async start_google_sheets_oauth(payload, http_request)` (line 40): no docstring
  - `async google_sheets_oauth_callback(request, code, state, connector_registry)` (line 77): no docstring

### `backend/bff/routers/data_connector_ops.py`
- **Functions**
  - `_build_google_oauth_client()` (line 23): no docstring
  - `_connector_oauth_enabled(oauth_client)` (line 27): no docstring
  - `_append_query_param(url, key, value)` (line 31): no docstring
  - `async _resolve_google_connection(connector_registry, oauth_client, connection_id)` (line 38): no docstring
  - `async _resolve_optional_access_token(connector_registry, connection_id)` (line 89): no docstring

### `backend/bff/routers/data_connector_pipelining.py`
- **Functions**
  - `async start_pipelining_google_sheet(sheet_id, payload, http_request, google_sheets_service, connector_registry, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 43): no docstring

### `backend/bff/routers/data_connector_registration.py`
- **Functions**
  - `async register_google_sheet(sheet_data, http_request, google_sheets_service, connector_registry, dataset_registry, lineage_store)` (line 34): Register a Google Sheet for data monitoring and automatic import.
  - `async preview_google_sheet(sheet_id, http_request, worksheet_name, limit, google_sheets_service, connector_registry)` (line 69): Preview data from a registered Google Sheet.
  - `async list_registered_sheets(http_request, database_name, connector_registry)` (line 95): List all registered Google Sheets.
  - `async unregister_google_sheet(sheet_id, http_request, connector_registry)` (line 115): Unregister a Google Sheet from monitoring.

### `backend/bff/routers/data_connector_sheet_tools.py`
- **Functions**
  - `async extract_google_sheet_grid(request, http_request, google_sheets_service, connector_registry)` (line 35): no docstring
  - `async preview_google_sheet_for_funnel(request, http_request, limit, google_sheets_service, connector_registry)` (line 92): no docstring

### `backend/bff/routers/database.py`
- **Functions**
  - `async list_databases(request, oms, dataset_registry)` (line 23): 데이터베이스 목록 조회
  - `async create_database(request, http_request, oms)` (line 43): 데이터베이스 생성
  - `async delete_database(db_name, http_request, expected_seq, oms)` (line 59): 데이터베이스 삭제
  - `async get_branch_info(db_name, branch_name, oms)` (line 80): 브랜치 정보 조회 (프론트엔드용 BFF 래핑)
  - `async delete_branch(db_name, branch_name, force, oms)` (line 87): 브랜치 삭제 (프론트엔드용 BFF 래핑)
  - `async get_database(db_name, oms)` (line 99): 데이터베이스 정보 조회
  - `async get_database_expected_seq(db_name)` (line 106): Resolve the current `expected_seq` for database (aggregate) operations.
  - `async list_classes(db_name, type, limit, oms)` (line 119): 데이터베이스의 클래스 목록 조회
  - `async create_class(db_name, class_data, oms)` (line 131): 데이터베이스에 새 클래스 생성
  - `async get_class(db_name, class_id, request, oms)` (line 140): 특정 클래스 조회
  - `async list_branches(db_name, oms)` (line 150): 브랜치 목록 조회
  - `async create_branch(db_name, branch_data, oms)` (line 157): 새 브랜치 생성
  - `async get_versions(db_name, oms)` (line 166): 버전 히스토리 조회

### `backend/bff/routers/document_bundles.py`
- **Functions**
  - `_enforce_bundle_access(tenant_policy, bundle_id)` (line 23): no docstring
  - `async search_document_bundle(bundle_id, body, request, policy_registry, client)` (line 41): no docstring
- **Classes**
  - `DocumentBundleSearchRequest` (line 33): no docstring

### `backend/bff/routers/governance.py`
- **Functions**
  - `async create_backing_datasource(body, request, dataset_registry)` (line 31): no docstring
  - `async list_backing_datasources(request, dataset_id, db_name, branch, dataset_registry)` (line 46): no docstring
  - `async get_backing_datasource(backing_id, request, dataset_registry)` (line 65): no docstring
  - `async create_backing_datasource_version(backing_id, body, request, dataset_registry)` (line 80): no docstring
  - `async list_backing_datasource_versions(backing_id, request, dataset_registry)` (line 97): no docstring
  - `async get_backing_datasource_version(version_id, request, dataset_registry)` (line 112): no docstring
  - `async create_key_spec(body, request, dataset_registry)` (line 127): no docstring
  - `async list_key_specs(request, dataset_id, dataset_registry)` (line 142): no docstring
  - `async get_key_spec(key_spec_id, request, dataset_registry)` (line 157): no docstring
  - `async list_schema_migration_plans(request, db_name, subject_type, subject_id, status_value, dataset_registry)` (line 172): no docstring
  - `async upsert_gate_policy(body, dataset_registry)` (line 193): no docstring
  - `async list_gate_policies(scope, dataset_registry)` (line 206): no docstring
  - `async list_gate_results(scope, subject_type, subject_id, dataset_registry)` (line 219): no docstring
  - `async upsert_access_policy(body, request, dataset_registry)` (line 236): no docstring
  - `async list_access_policies(request, db_name, scope, subject_type, subject_id, policy_status, dataset_registry)` (line 251): no docstring

### `backend/bff/routers/graph.py`
- **Functions**
  - `async execute_graph_query(db_name, query, request, lineage_store, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 38): no docstring
  - `async execute_simple_graph_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 64): no docstring
  - `async execute_multi_hop_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 88): no docstring
  - `async find_relationship_paths(db_name, source_class, target_class, max_depth, graph_service, branch)` (line 112): no docstring
  - `async graph_service_health(graph_service)` (line 132): no docstring
  - `async register_projection(db_name, request, graph_service)` (line 160): no docstring
  - `async query_projection(db_name, request, graph_service)` (line 190): no docstring
  - `async list_projections(db_name, graph_service)` (line 220): no docstring
- **Classes**
  - `ProjectionRegistrationRequest` (line 139): no docstring
  - `ProjectionQueryRequest` (line 147): no docstring

### `backend/bff/routers/health.py`
- **Functions**
  - `async root()` (line 21): 루트 엔드포인트
  - `async health_check(oms_client)` (line 36): 헬스체크 엔드포인트

### `backend/bff/routers/instance_async.py`
- **Functions**
  - `async create_instance_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 30): 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
  - `async update_instance_async(db_name, class_label, instance_id, request, http_request, expected_seq, branch, oms_client, label_mapper, user_id)` (line 65): 인스턴스 수정 명령을 비동기로 처리 (Label 기반)
  - `async delete_instance_async(db_name, class_label, instance_id, http_request, branch, expected_seq, oms_client, label_mapper, user_id)` (line 101): 인스턴스 삭제 명령을 비동기로 처리 (Label 기반)
  - `async bulk_create_instances_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 134): 대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)

### `backend/bff/routers/instances.py`
- **Functions**
  - `async _maybe_get_action_log_registry(class_id)` (line 33): no docstring
  - `async get_class_instances(db_name, class_id, http_request, base_branch, overlay_branch, branch, limit, offset, search, status_filter, action_type_id, submitted_by, elasticsearch_service, dataset_registry, action_logs)` (line 47): no docstring
  - `async get_class_sample_values(db_name, class_id, property_name, base_branch, branch, limit, elasticsearch_service, dataset_registry)` (line 95): no docstring
  - `async get_instance(db_name, class_id, instance_id, http_request, base_branch, overlay_branch, branch, elasticsearch_service, dataset_registry, action_logs)` (line 129): no docstring

### `backend/bff/routers/lineage.py`
- **Functions**
  - `_parse_artifact_node_id(node_id)` (line 33): Parse artifact node id: artifact:<kind>:<...>
  - `_suggest_remediation_actions(artifacts)` (line 51): Recommend safe operational actions.
  - `async get_lineage_graph(root, db_name, direction, max_depth, max_nodes, max_edges, lineage_store)` (line 105): no docstring
  - `async get_lineage_impact(root, db_name, direction, max_depth, artifact_kind, max_nodes, max_edges, lineage_store)` (line 138): no docstring
  - `async get_lineage_metrics(db_name, window_minutes, lineage_store, audit_store)` (line 200): Operational lineage metrics.

### `backend/bff/routers/link_types.py`

### `backend/bff/routers/link_types_deps.py`

### `backend/bff/routers/link_types_edits.py`
- **Functions**
  - `async list_link_edits(db_name, link_type_id, branch, dataset_registry)` (line 25): no docstring
  - `async create_link_edit(db_name, link_type_id, body, request, dataset_registry)` (line 67): no docstring

### `backend/bff/routers/link_types_ops.py`

### `backend/bff/routers/link_types_read.py`
- **Functions**
  - `async list_link_types(db_name, branch, oms_client, dataset_registry)` (line 33): no docstring
  - `async get_link_type(db_name, link_type_id, request, branch, oms_client, dataset_registry, _)` (line 71): no docstring

### `backend/bff/routers/link_types_write.py`
- **Functions**
  - `async create_link_type(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 31): no docstring
  - `async update_link_type(db_name, link_type_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 61): no docstring
  - `async reindex_link_type(db_name, link_type_id, request, dataset_version_id, dataset_registry, objectify_registry)` (line 93): no docstring

### `backend/bff/routers/mapping.py`
- **Functions**
  - `async export_mappings(db_name, mapper)` (line 22): no docstring
  - `async import_mappings(db_name, file, mapper, oms_client)` (line 28): no docstring
  - `async validate_mappings(db_name, file, mapper, oms_client)` (line 44): no docstring
  - `async get_mappings_summary(db_name, mapper)` (line 60): no docstring
  - `async clear_mappings(db_name, mapper)` (line 66): no docstring

### `backend/bff/routers/merge_conflict.py`
- **Functions**
  - `async simulate_merge(db_name, request, oms_client)` (line 22): no docstring
  - `async resolve_merge_conflicts(db_name, request, oms_client)` (line 32): no docstring

### `backend/bff/routers/object_types.py`
- **Functions**
  - `async _require_domain_role(request, db_name)` (line 32): no docstring
  - `async create_object_type_contract(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 41): no docstring
  - `async get_object_type_contract(db_name, class_id, request, branch, oms_client, dataset_registry, objectify_registry)` (line 70): no docstring
  - `async update_object_type_contract(db_name, class_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 94): no docstring

### `backend/bff/routers/object_types_deps.py`

### `backend/bff/routers/objectify.py`

### `backend/bff/routers/objectify_changelog.py`
- **Functions**
  - `_get_changelog_store()` (line 19): Dependency placeholder — wired by objectify router composition.
  - `async list_changelogs(db_name, branch, target_class_id, limit, offset)` (line 30): List recent objectify changelogs for a database.
  - `async get_changelog(changelog_id)` (line 68): Get a single objectify changelog entry.

### `backend/bff/routers/objectify_dag.py`
- **Functions**
  - `async run_objectify_dag(db_name, body, request, dataset_registry, objectify_registry, job_queue, oms_client)` (line 28): no docstring

### `backend/bff/routers/objectify_deps.py`
- **Functions**
  - `async _require_db_role(request, db_name, roles)` (line 27): no docstring

### `backend/bff/routers/objectify_enterprise.py`
- **Functions**
  - `async detect_relationships(db_name, dataset_id, request, body, branch, dataset_registry)` (line 32): Detect potential FK relationships in a dataset.

### `backend/bff/routers/objectify_incremental.py`
- **Functions**
  - `async trigger_incremental_objectify(mapping_spec_id, request, body, branch, dataset_registry, objectify_registry, job_queue)` (line 36): Trigger objectify with incremental execution mode.
  - `async get_mapping_spec_watermark(mapping_spec_id, request, branch, objectify_registry)` (line 129): Get watermark state for incremental objectify.

### `backend/bff/routers/objectify_job_ops.py`
- **Functions**
  - `async enqueue_objectify_job_for_mapping_spec(objectify_registry, mapping_spec_id, mapping_spec_version, dataset_registry, dataset_id, dataset_version_id, dataset, version, mapping_spec_record, options_override, options_defaults, strict_dataset_match)` (line 19): no docstring

### `backend/bff/routers/objectify_job_queue_deps.py`
- **Functions**
  - `async get_objectify_job_queue(objectify_registry)` (line 12): no docstring

### `backend/bff/routers/objectify_mapping_specs.py`
- **Functions**
  - `async create_mapping_spec(body, request, dataset_registry, objectify_registry, oms_client)` (line 29): no docstring
  - `async list_mapping_specs(dataset_id, include_inactive, objectify_registry)` (line 47): no docstring

### `backend/bff/routers/objectify_ops.py`

### `backend/bff/routers/objectify_reconcile.py`
- **Functions**
  - `async reconcile_relationships(db_name, branch, oms_client, es_service, objectify_registry)` (line 36): no docstring

### `backend/bff/routers/objectify_runs.py`
- **Functions**
  - `async run_objectify(dataset_id, body, request, dataset_registry, objectify_registry, job_queue, pipeline_registry, oms_client)` (line 34): no docstring

### `backend/bff/routers/ontology.py`

### `backend/bff/routers/ontology_agent.py`
- **Functions**
  - `_get_tenant_id(request)` (line 50): Extract tenant ID from request headers.
  - `_get_user_id(request)` (line 60): Extract user ID from request headers.
  - `_get_actor(request)` (line 66): Extract actor from request headers.
  - `async run_ontology_agent(request, body)` (line 93): Run the autonomous ontology agent by delegating to Pipeline Agent.
- **Classes**
  - `OntologyAgentRunRequest` (line 35): Request body for ontology agent runs.

### `backend/bff/routers/ontology_crud.py`
- **Functions**
  - `async create_ontology(db_name, ontology, branch, mapper, oms_client)` (line 42): no docstring
  - `async list_ontologies(db_name, request, branch, class_type, limit, offset, mapper, terminus)` (line 60): no docstring
  - `async get_ontology(db_name, class_label, request, branch, mapper, terminus)` (line 84): no docstring
  - `async validate_ontology_create_bff(db_name, ontology, request, branch, oms_client)` (line 104): no docstring
  - `async validate_ontology_update_bff(db_name, class_label, ontology, request, branch, mapper, oms_client)` (line 122): no docstring
  - `async update_ontology(db_name, class_label, ontology, request, expected_seq, branch, mapper, terminus)` (line 153): no docstring
  - `async delete_ontology(db_name, class_label, request, expected_seq, branch, mapper, terminus)` (line 186): no docstring
  - `async get_ontology_schema(db_name, class_id, request, format, branch, mapper, terminus, jsonld_conv)` (line 208): no docstring

### `backend/bff/routers/ontology_extensions.py`
- **Functions**
  - `_resource_routes(resource_type)` (line 30): no docstring
  - `async list_ontology_branches(db_name, oms_client)` (line 141): no docstring
  - `async create_ontology_branch(db_name, request, oms_client)` (line 150): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, oms_client)` (line 160): no docstring
  - `async create_ontology_proposal(db_name, request, oms_client)` (line 176): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, oms_client)` (line 190): no docstring
  - `async deploy_ontology(db_name, request, oms_client)` (line 206): no docstring
  - `async ontology_health(db_name, branch, oms_client)` (line 220): no docstring

### `backend/bff/routers/ontology_imports.py`
- **Functions**
  - `async dry_run_import_from_google_sheets(db_name, request)` (line 26): no docstring
  - `async commit_import_from_google_sheets(db_name, request, oms_client)` (line 35): no docstring
  - `async dry_run_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, dry_run_rows, max_import_rows, options_json)` (line 49): no docstring
  - `async commit_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, allow_partial, max_import_rows, batch_size, return_instances, max_return_instances, options_json, oms_client)` (line 91): no docstring

### `backend/bff/routers/ontology_metadata.py`
- **Functions**
  - `async save_mapping_metadata(db_name, class_id, metadata, oms, mapper)` (line 27): 매핑 메타데이터를 온톨로지 클래스에 저장

### `backend/bff/routers/ontology_ops.py`

### `backend/bff/routers/ontology_relationships.py`
- **Functions**
  - `async create_ontology_with_relationship_validation(db_name, ontology, request, branch, auto_generate_inverse, validate_relationships, check_circular_references, mapper, terminus)` (line 34): no docstring
  - `async validate_ontology_relationships_bff(db_name, ontology, request, branch, mapper, terminus)` (line 60): no docstring
  - `async check_circular_references_bff(db_name, request, ontology, branch, mapper, terminus)` (line 80): no docstring
  - `async analyze_relationship_network_bff(db_name, request, terminus, mapper)` (line 100): no docstring
  - `async find_relationship_paths_bff(request, db_name, start_entity, end_entity, max_depth, path_type, terminus, mapper)` (line 116): no docstring

### `backend/bff/routers/ontology_suggestions.py`
- **Functions**
  - `async suggest_schema_from_data(db_name, request, terminus)` (line 30): no docstring
  - `async suggest_mappings(db_name, request)` (line 41): no docstring
  - `async suggest_mappings_from_google_sheets(db_name, request)` (line 50): no docstring
  - `async suggest_mappings_from_excel(db_name, target_class_id, file, target_schema_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, include_relationships, enable_semantic_hints, max_tables, max_rows, max_cols)` (line 59): no docstring
  - `async suggest_schema_from_google_sheets(db_name, request, terminus)` (line 97): no docstring
  - `async suggest_schema_from_excel(db_name, file, sheet_name, class_name, table_id, table_top, table_left, table_bottom, table_right, include_complex_types, max_tables, max_rows, max_cols)` (line 108): no docstring

### `backend/bff/routers/ops.py`
- **Functions**
  - `async ops_status(dataset_registry, objectify_registry)` (line 20): no docstring

### `backend/bff/routers/pipeline.py`

### `backend/bff/routers/pipeline_branches.py`
- **Functions**
  - `async list_pipeline_branches(db_name, pipeline_registry)` (line 30): no docstring
  - `async archive_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 50): no docstring
  - `async restore_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 92): no docstring
  - `async create_pipeline_branch(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 132): no docstring

### `backend/bff/routers/pipeline_catalog.py`
- **Functions**
  - `async list_pipelines(db_name, branch, pipeline_registry, request)` (line 26): no docstring
  - `async create_pipeline(payload, audit_store, pipeline_registry, dataset_registry, request)` (line 42): no docstring

### `backend/bff/routers/pipeline_datasets.py`

### `backend/bff/routers/pipeline_datasets_catalog.py`
- **Functions**
  - `async list_datasets(db_name, branch, dataset_registry)` (line 33): no docstring
  - `async get_dataset_raw_file(dataset_id, file_name, file_index, request, pipeline_registry, dataset_registry)` (line 54): no docstring

### `backend/bff/routers/pipeline_datasets_deps.py`

### `backend/bff/routers/pipeline_datasets_ingest.py`
- **Functions**
  - `async get_dataset_ingest_request(ingest_request_id, request, dataset_registry)` (line 30): no docstring
  - `async approve_dataset_schema(ingest_request_id, payload, request, dataset_registry)` (line 62): no docstring

### `backend/bff/routers/pipeline_datasets_ops.py`

### `backend/bff/routers/pipeline_datasets_ops_funnel.py`
- **Functions**
  - `_normalize_inferred_type(type_value)` (line 19): no docstring
  - `_build_schema_columns(columns, inferred_schema)` (line 43): no docstring
  - `_columns_from_schema(schema_columns)` (line 67): no docstring
  - `_rows_from_preview(columns, sample_rows)` (line 75): no docstring
  - `_build_funnel_analysis_payload(analysis, inferred_schema)` (line 90): no docstring
  - `_extract_sample_columns(sample_json)` (line 103): no docstring
  - `_extract_sample_rows(sample_json, columns)` (line 134): no docstring
  - `async _compute_funnel_analysis_from_sample(sample_json)` (line 164): no docstring
  - `_select_sample_row(sample_json, filename, file_index)` (line 200): no docstring

### `backend/bff/routers/pipeline_datasets_ops_ingest.py`
- **Functions**
  - `async _ensure_ingest_transaction(dataset_registry, ingest_request_id)` (line 17): no docstring
  - `_build_ingest_request_fingerprint(payload)` (line 32): no docstring
  - `_ingest_staging_prefix(prefix, ingest_request_id)` (line 41): no docstring
  - `_sanitize_s3_metadata(metadata)` (line 46): no docstring
  - `_dataset_artifact_prefix(db_name, dataset_id, dataset_name)` (line 62): no docstring

### `backend/bff/routers/pipeline_datasets_ops_lakefs.py`
- **Functions**
  - `async _acquire_lakefs_commit_lock(repository, branch, job_id)` (line 26): no docstring
  - `async _release_lakefs_commit_lock(redis_service, lock_key, token)` (line 64): no docstring
  - `_lakefs_commit_retry_delay(attempt)` (line 77): no docstring
  - `async _resolve_lakefs_commit_from_head(lakefs_client, lakefs_storage_service, repository, branch, object_key, expected_checksum, attempts)` (line 81): no docstring
  - `_resolve_lakefs_raw_repository()` (line 126): no docstring
  - `async _ensure_lakefs_branch_exists(lakefs_client, repository, branch, source_branch)` (line 131): no docstring
  - `_extract_lakefs_ref_from_artifact_key(artifact_key)` (line 152): no docstring
  - `async _commit_lakefs_with_predicate_fallback(lakefs_client, lakefs_storage_service, repository, branch, message, metadata, object_key, expected_checksum)` (line 163): no docstring

### `backend/bff/routers/pipeline_datasets_ops_objectify.py`
- **Functions**
  - `async _maybe_enqueue_objectify_job(dataset, version, objectify_registry, job_queue, dataset_registry, actor_user_id)` (line 20): no docstring

### `backend/bff/routers/pipeline_datasets_ops_parsing.py`
- **Functions**
  - `_default_dataset_name(filename)` (line 17): no docstring
  - `_convert_xls_to_xlsx_bytes(xls_bytes)` (line 26): no docstring
  - `_normalize_table_bbox(table_top, table_left, table_bottom, table_right)` (line 74): no docstring
  - `_detect_csv_delimiter(sample)` (line 98): no docstring
  - `_parse_csv_rows(reader, has_header, preview_limit)` (line 108): no docstring
  - `_parse_csv_file(file_obj, delimiter, has_header, preview_limit)` (line 138): no docstring
  - `_parse_csv_content(content, delimiter, has_header, preview_limit)` (line 208): no docstring

### `backend/bff/routers/pipeline_datasets_uploads.py`

### `backend/bff/routers/pipeline_datasets_uploads_csv.py`
- **Functions**
  - `async upload_csv_dataset(db_name, branch, file, dataset_name, description, delimiter, has_header, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 42): no docstring

### `backend/bff/routers/pipeline_datasets_uploads_excel.py`
- **Functions**
  - `async upload_excel_dataset(db_name, branch, file, dataset_name, description, sheet_name, table_id, table_top, table_left, table_bottom, table_right, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 43): no docstring

### `backend/bff/routers/pipeline_datasets_uploads_media.py`
- **Functions**
  - `async upload_media_dataset(db_name, branch, files, dataset_name, description, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 27): no docstring

### `backend/bff/routers/pipeline_datasets_versions.py`
- **Functions**
  - `async create_dataset(payload, dataset_registry)` (line 40): no docstring
  - `async create_dataset_version(dataset_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 103): no docstring
  - `async reanalyze_dataset_version(dataset_id, version_id, request, dataset_registry)` (line 132): no docstring

### `backend/bff/routers/pipeline_deps.py`
- **Functions**
  - `async get_pipeline_executor()` (line 23): no docstring
  - `async get_pipeline_job_queue()` (line 29): no docstring

### `backend/bff/routers/pipeline_detail.py`
- **Functions**
  - `async get_pipeline(pipeline_id, pipeline_registry, branch, preview_node_id, request)` (line 29): no docstring
  - `async get_pipeline_readiness(pipeline_id, branch, pipeline_registry, dataset_registry, request)` (line 47): no docstring
  - `async update_pipeline(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, request)` (line 65): no docstring

### `backend/bff/routers/pipeline_execution.py`
- **Functions**
  - `async preview_pipeline(pipeline_id, payload, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, request)` (line 40): no docstring
  - `async build_pipeline(pipeline_id, payload, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, oms_client, request)` (line 63): no docstring
  - `async deploy_pipeline(pipeline_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, oms_client, lineage_store, audit_store)` (line 88): no docstring

### `backend/bff/routers/pipeline_history.py`
- **Functions**
  - `async list_pipeline_runs(pipeline_id, limit, pipeline_registry, request)` (line 27): no docstring
  - `async list_pipeline_artifacts(pipeline_id, mode, limit, pipeline_registry, request)` (line 54): no docstring
  - `async get_pipeline_artifact(pipeline_id, artifact_id, pipeline_registry, request)` (line 84): no docstring

### `backend/bff/routers/pipeline_ops.py`

### `backend/bff/routers/pipeline_ops_augmentation.py`

### `backend/bff/routers/pipeline_ops_augmentation_casts.py`

### `backend/bff/routers/pipeline_ops_augmentation_contract.py`

### `backend/bff/routers/pipeline_ops_definition.py`
- **Functions**
  - `_stable_definition_hash(definition_json)` (line 18): no docstring
  - `_resolve_definition_commit_id(definition_json, latest_version, definition_hash)` (line 27): no docstring
  - `_normalize_location(location)` (line 42): no docstring
  - `_extract_node_ids(definition_json)` (line 55): no docstring
  - `_extract_edge_ids(definition_json)` (line 71): no docstring
  - `_definition_diff(previous, current)` (line 93): no docstring

### `backend/bff/routers/pipeline_ops_dependencies.py`
- **Functions**
  - `_normalize_dependencies_payload(raw)` (line 20): no docstring
  - `async _validate_dependency_targets(pipeline_registry, db_name, pipeline_id, dependencies)` (line 48): no docstring
  - `_format_dependencies_for_api(dependencies)` (line 72): no docstring

### `backend/bff/routers/pipeline_ops_locks.py`
- **Functions**
  - `async _acquire_pipeline_publish_lock(pipeline_id, branch, job_id)` (line 24): no docstring
  - `async _release_pipeline_publish_lock(redis_service, lock_key, token)` (line 62): no docstring

### `backend/bff/routers/pipeline_ops_policy.py`
- **Functions**
  - `_resolve_pipeline_protected_branches()` (line 10): no docstring
  - `_pipeline_requires_proposal(branch)` (line 14): no docstring

### `backend/bff/routers/pipeline_ops_preflight.py`
- **Functions**
  - `_iter_udf_nodes(definition_json)` (line 26): no docstring
  - `_udf_issue(kind, node_id, message)` (line 50): no docstring
  - `async _validate_udf_preflight_references(definition_json, db_name, pipeline_registry)` (line 59): no docstring
  - `async _run_pipeline_preflight(definition_json, db_name, branch, dataset_registry, pipeline_registry)` (line 191): no docstring
  - `_validate_pipeline_definition(definition_json, require_output)` (line 246): no docstring

### `backend/bff/routers/pipeline_ops_schema.py`
- **Functions**
  - `_normalize_schema_column_type(value)` (line 15): no docstring
  - `_coerce_schema_columns(raw)` (line 19): no docstring
  - `_detect_breaking_schema_changes(previous_schema, next_columns)` (line 37): no docstring
  - `_resolve_output_pk_columns(definition_json, node_id, output_name)` (line 58): no docstring

### `backend/bff/routers/pipeline_plans.py`

### `backend/bff/routers/pipeline_plans_compile.py`
- **Functions**
  - `async compile_plan(body, request, llm, redis_service, audit_store, dataset_registry, pipeline_registry, plan_registry)` (line 31): no docstring

### `backend/bff/routers/pipeline_plans_deps.py`
- **Functions**
  - `async get_dataset_profile_registry()` (line 6): no docstring
  - `async get_pipeline_plan_registry()` (line 12): no docstring

### `backend/bff/routers/pipeline_plans_ops.py`

### `backend/bff/routers/pipeline_plans_preview.py`
- **Functions**
  - `async preview_plan(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 29): no docstring
  - `async inspect_plan_preview(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 49): no docstring
  - `async evaluate_joins(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 69): no docstring

### `backend/bff/routers/pipeline_plans_read.py`
- **Functions**
  - `async get_plan(plan_id, request, plan_registry)` (line 15): no docstring

### `backend/bff/routers/pipeline_proposals.py`
- **Functions**
  - `async list_pipeline_proposals(db_name, branch, status_filter, pipeline_registry, request)` (line 27): no docstring
  - `async submit_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, objectify_registry, request)` (line 45): no docstring
  - `async approve_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 67): no docstring
  - `async reject_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 85): no docstring

### `backend/bff/routers/pipeline_shared.py`
- **Functions**
  - `_resolve_principal(request)` (line 24): no docstring
  - `_actor_label(principal_type, principal_id)` (line 49): no docstring
  - `async _filter_pipeline_records_for_read_access(pipeline_registry, records, request, required_role, pipeline_id_keys)` (line 53): no docstring
  - `async _ensure_pipeline_permission(pipeline_registry, pipeline_id, request, required_role)` (line 94): no docstring
  - `async _log_pipeline_audit(audit_store, request, action, status, pipeline_id, db_name, metadata, error)` (line 129): no docstring
  - `_require_idempotency_key(request)` (line 155): no docstring
  - `_require_pipeline_idempotency_key(request, operation)` (line 159): Require idempotency key for pipeline mutation operations.

### `backend/bff/routers/pipeline_simulation.py`
- **Functions**
  - `async simulate_pipeline_definition(payload, dataset_registry, pipeline_registry, request)` (line 36): no docstring

### `backend/bff/routers/pipeline_udfs.py`
- **Functions**
  - `async create_udf(body, db_name, pipeline_registry)` (line 33): no docstring
  - `async list_udfs(db_name, pipeline_registry)` (line 49): no docstring
  - `async get_udf(udf_id, pipeline_registry)` (line 61): no docstring
  - `async create_udf_version(body, udf_id, pipeline_registry)` (line 73): no docstring
  - `async get_udf_version(udf_id, version, pipeline_registry)` (line 87): no docstring
- **Classes**
  - `UdfCreateRequest` (line 21): no docstring
  - `UdfVersionCreateRequest` (line 27): no docstring

### `backend/bff/routers/query.py`
- **Functions**
  - `async execute_query(db_name, query, request, mapper, terminus, dataset_registry)` (line 33): 온톨로지 쿼리 실행
  - `async execute_raw_query(db_name, query, terminus, dataset_registry)` (line 112): 원시 쿼리 실행 (제한적 접근)
  - `async query_builder_info()` (line 173): 쿼리 빌더 정보

### `backend/bff/routers/registry_deps.py`
- **Functions**
  - `async get_dataset_registry()` (line 20): no docstring
  - `async get_objectify_registry()` (line 26): no docstring
  - `async get_pipeline_registry()` (line 32): no docstring
  - `async get_agent_policy_registry()` (line 38): no docstring
  - `async get_agent_session_registry()` (line 44): no docstring

### `backend/bff/routers/role_deps.py`
- **Functions**
  - `async enforce_required_database_role(request, db_name, roles)` (line 19): no docstring
  - `require_database_role(roles)` (line 26): no docstring

### `backend/bff/routers/schema_changes.py`
- **Functions**
  - `async list_schema_changes(db_name, subject_type, subject_id, severity, since, limit, offset)` (line 50): List schema drift history for a database.
  - `async acknowledge_drift(drift_id, request)` (line 80): Acknowledge a schema drift.
  - `async list_subscriptions(request, db_name, status_filter, limit)` (line 100): List schema change subscriptions for the current user.
  - `async create_subscription(request, body)` (line 124): Create a new schema change subscription.
  - `async delete_subscription(request, subscription_id)` (line 150): Delete a schema change subscription.
  - `async check_mapping_compatibility(mapping_spec_id, db_name, dataset_version_id)` (line 170): Check if a mapping spec is compatible with the current dataset schema.
  - `async get_schema_change_stats(db_name, days)` (line 195): Get schema change statistics for a database.

### `backend/bff/routers/summary.py`
- **Functions**
  - `async get_summary(db, branch, oms, redis_service, es_service)` (line 29): Summarize context + cross-service health for UI.

### `backend/bff/routers/tasks.py`
- **Functions**
  - `async get_task_status(task_id, task_manager)` (line 34): Get current status of a background task.
  - `async list_tasks(status, task_type, limit, task_manager)` (line 49): List background tasks with optional filtering.
  - `async cancel_task(task_id, task_manager)` (line 72): Cancel a running background task.
  - `async get_task_metrics(task_manager)` (line 87): Get aggregated metrics for all background tasks.
  - `async retry_task(task_id, task_manager)` (line 101): Retry a failed task.
  - `async get_task_result(task_id, task_manager)` (line 140): Get the result of a completed task.

### `backend/bff/routers/websocket.py`
- **Functions**
  - `get_ws_manager()` (line 19): WebSocket 연결 관리자 의존성
  - `async websocket_command_updates(websocket, command_id, client_id, user_id, token, manager)` (line 25): 특정 Command의 실시간 상태 업데이트 구독
  - `async websocket_user_commands(websocket, user_id, client_id, token, manager)` (line 52): 사용자의 모든 Command 실시간 업데이트 구독

### `backend/bff/schemas/__init__.py`

### `backend/bff/schemas/actions_requests.py`
- **Classes**
  - `ActionSubmitRequest` (line 16): no docstring
  - `ActionSimulateScenarioRequest` (line 22): no docstring
  - `ActionSimulateStatePatch` (line 30): Patch-like state override for decision simulation (what-if).
  - `ActionSimulateObservedBaseOverrides` (line 41): Override observed_base snapshot fields/links to simulate stale reads.
  - `ActionSimulateTargetAssumption` (line 48): no docstring
  - `ActionSimulateAssumptions` (line 55): no docstring
  - `ActionSimulateRequest` (line 62): no docstring

### `backend/bff/schemas/admin_projection_requests.py`
- **Classes**
  - `RecomputeProjectionRequest` (line 11): Request model for projection recompute (Versioning + Recompute).
  - `RecomputeProjectionResponse` (line 37): Response model for projection recompute.

### `backend/bff/schemas/admin_replay_requests.py`
- **Classes**
  - `ReplayInstanceStateRequest` (line 8): Request model for instance state replay.
  - `ReplayInstanceStateResponse` (line 18): Response model for instance state replay.

### `backend/bff/schemas/context7_requests.py`
- **Classes**
  - `SearchRequest` (line 10): Context7 search request.
  - `KnowledgeRequest` (line 18): Request to add knowledge to Context7.
  - `EntityLinkRequest` (line 27): Request to create entity relationship.
  - `OntologyAnalysisRequest` (line 36): Request to analyze ontology with Context7.

### `backend/bff/schemas/governance_requests.py`
- **Classes**
  - `CreateBackingDatasourceRequest` (line 13): no docstring
  - `CreateBackingDatasourceVersionRequest` (line 19): no docstring
  - `CreateKeySpecRequest` (line 25): no docstring
  - `GatePolicyRequest` (line 35): no docstring
  - `AccessPolicyRequest` (line 43): no docstring

### `backend/bff/schemas/instance_async_requests.py`
- **Classes**
  - `InstanceCreateRequest` (line 14): 인스턴스 생성 요청 (Label 기반)
  - `InstanceUpdateRequest` (line 21): 인스턴스 수정 요청 (Label 기반)
  - `BulkInstanceCreateRequest` (line 28): 대량 인스턴스 생성 요청 (Label 기반)

### `backend/bff/schemas/label_mapping_schema.py`
- **Functions**
  - `get_label_mapping_schema()` (line 9): LabelMapping 클래스 스키마 반환
  - `get_label_mapping_properties()` (line 28): LabelMapping 클래스의 속성들 반환
  - `get_label_mapping_ontology()` (line 120): LabelMapping 전체 온톨로지 반환

### `backend/bff/schemas/link_types_requests.py`
- **Classes**
  - `ForeignKeyRelationshipSpec` (line 10): no docstring
  - `JoinTableRelationshipSpec` (line 21): no docstring
  - `ObjectBackedRelationshipSpec` (line 37): no docstring
  - `LinkTypeRequest` (line 51): no docstring
  - `LinkTypeUpdateRequest` (line 65): no docstring
  - `LinkEditRequest` (line 74): no docstring

### `backend/bff/schemas/object_types_requests.py`
- **Classes**
  - `ObjectTypeContractRequest` (line 10): no docstring
  - `ObjectTypeContractUpdate` (line 25): no docstring

### `backend/bff/schemas/objectify_requests.py`
- **Classes**
  - `MappingSpecField` (line 15): no docstring
  - `CreateMappingSpecRequest` (line 20): no docstring
  - `TriggerObjectifyRequest` (line 37): no docstring
  - `RunObjectifyDAGRequest` (line 53): Topologically enqueue objectify jobs based on mapping-spec relationship dependencies.
  - `DetectRelationshipsRequest` (line 67): no docstring
  - `DetectRelationshipsResponse` (line 72): no docstring
  - `TriggerIncrementalRequest` (line 77): no docstring

### `backend/bff/schemas/ontology_extensions_requests.py`
- **Classes**
  - `OntologyResourceRequest` (line 15): no docstring
  - `OntologyProposalRequest` (line 25): no docstring
  - `OntologyApproveRequest` (line 33): no docstring
  - `OntologyDeployRequest` (line 38): no docstring

### `backend/bff/schemas/ontology_requests.py`
- **Classes**
  - `SchemaFromDataRequest` (line 18): Request model for schema suggestion from data.
  - `SchemaFromGoogleSheetsRequest` (line 27): Request model for schema suggestion from Google Sheets.
  - `MappingSuggestionRequest` (line 39): Request model for mapping suggestions between schemas.
  - `MappingFromGoogleSheetsRequest` (line 48): Request model for mapping suggestions from Google Sheets → existing ontology class.
  - `ImportFieldMapping` (line 67): Field mapping for import (source column → target property).
  - `ImportTargetField` (line 74): Target field definition for import (name + type).
  - `ImportFromGoogleSheetsRequest` (line 81): Request model for dry-run/commit import from Google Sheets.

### `backend/bff/schemas/pipeline_datasets.py`
- **Classes**
  - `FunnelAnalysisData` (line 18): no docstring
  - `FunnelAnalysisApiResponse` (line 25): no docstring

### `backend/bff/schemas/pipeline_plans_requests.py`
- **Classes**
  - `PipelinePlanCompileRequest` (line 10): no docstring
  - `PipelinePlanPreviewRequest` (line 18): no docstring
  - `PipelinePlanInspectPreviewRequest` (line 25): no docstring
  - `PipelinePlanEvaluateJoinsRequest` (line 31): no docstring

### `backend/bff/schemas/schema_changes_requests.py`
- **Classes**
  - `SchemaChangeItem` (line 15): no docstring
  - `SubscriptionCreateRequest` (line 30): no docstring
  - `SubscriptionResponse` (line 38): no docstring
  - `CompatibilityCheckRequest` (line 50): no docstring
  - `CompatibilityCheckResponse` (line 54): no docstring
  - `AcknowledgeRequest` (line 63): no docstring

### `backend/bff/schemas/tasks_requests.py`
- **Classes**
  - `TaskStatusResponse` (line 18): Task status response model.
  - `TaskListResponse` (line 33): Task list response model.
  - `TaskMetricsResponse` (line 40): Task metrics response model.

### `backend/bff/services/__init__.py`

### `backend/bff/services/actions_service.py`
- **Functions**
  - `_require_action_type_id(action_type_id)` (line 31): no docstring
  - `async _enforce_domain_model_role(http_request, db_name, enforce_role)` (line 38): no docstring
  - `_parse_uuid(value)` (line 50): no docstring
  - `_oms_metadata(request_metadata, principal_type, principal_id)` (line 57): no docstring
  - `async _oms_post(oms_client, path, payload)` (line 70): no docstring
  - `async _action_simulation_registry()` (line 81): no docstring
  - `_serialize_action_simulation(record)` (line 90): no docstring
  - `_serialize_action_simulation_version(record)` (line 104): no docstring
  - `async submit_action(db_name, action_type_id, body, http_request, base_branch, overlay_branch, oms_client, enforce_role)` (line 126): no docstring
  - `async simulate_action(db_name, action_type_id, body, http_request, oms_client, enforce_role)` (line 163): no docstring
  - `async get_action_log(db_name, action_log_id, http_request, action_logs, enforce_role)` (line 205): no docstring
  - `async list_action_logs(db_name, http_request, status_filter, action_type_id, submitted_by, limit, offset, action_logs, enforce_role)` (line 226): no docstring
  - `async list_action_simulations(db_name, http_request, action_type_id, limit, offset, enforce_role)` (line 254): no docstring
  - `async get_action_simulation(db_name, simulation_id, http_request, include_versions, version_limit, enforce_role)` (line 277): no docstring
  - `async list_action_simulation_versions(db_name, simulation_id, http_request, limit, offset, enforce_role)` (line 303): no docstring
  - `async get_action_simulation_version(db_name, simulation_id, version, http_request, enforce_role)` (line 330): no docstring

### `backend/bff/services/admin_recompute_projection_service.py`
- **Functions**
  - `_normalize_dt(dt)` (line 40): no docstring
  - `_load_projection_mapping(projection)` (line 46): no docstring
  - `async _ensure_es_connected(es)` (line 54): no docstring
  - `_versioning_kwargs(seq)` (line 59): no docstring
  - `_validate_recompute_projection_mode(projection)` (line 65): no docstring
  - `_strategy_for_projection(projection)` (line 290): no docstring
  - `async start_recompute_projection(http_request, request, background_tasks, task_manager, redis_service, audit_store, lineage_store, elasticsearch_service)` (line 299): no docstring
  - `async get_recompute_projection_result(task_id, task_manager, redis_service)` (line 349): no docstring
  - `async _log_audit_safe(audit_store, action, db_name, resource_id, metadata, occurred_at)` (line 375): no docstring
  - `async _maybe_record_lineage(lineage_store, envelope, db_name, index_name, doc_id, seq, event_ts, ontology_ref, ontology_commit)` (line 398): no docstring
  - `async _promote_alias_to_index(elasticsearch_service, base_index, new_index, allow_delete_base_index)` (line 428): Thin wrapper around shared.promote_alias_to_index for backward compatibility.
  - `async recompute_projection_task(task_id, request, elasticsearch_service, redis_service, audit_store, lineage_store, requested_by, request_ip)` (line 445): no docstring
- **Classes**
  - `IndexDecision` (line 82): no docstring
  - `ProjectionStrategy` (line 89): no docstring
    - `event_types(self)` (line 93): no docstring
    - `base_index(self, db_name, branch)` (line 96): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 99): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 102): no docstring
  - `InstancesProjectionStrategy` (line 120): no docstring
    - `base_index(self, db_name, branch)` (line 124): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 127): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 130): no docstring
  - `OntologiesProjectionStrategy` (line 205): no docstring
    - `base_index(self, db_name, branch)` (line 209): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 212): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 215): no docstring

### `backend/bff/services/admin_reindex_instances_service.py`
- **Functions**
  - `async reindex_all_instances(db_name, branch, objectify_registry, dataset_registry, job_queue, delete_index_first, elasticsearch_service)` (line 27): Re-index all instances for a database by re-running objectify jobs.

### `backend/bff/services/admin_replay_service.py`
- **Functions**
  - `async start_replay_instance_state(request, background_tasks, storage_service, task_manager, redis_service)` (line 33): no docstring
  - `async get_replay_result(task_id, task_manager, redis_service)` (line 70): no docstring
  - `async get_replay_trace(task_id, command_id, include_audit, audit_limit, include_lineage, lineage_direction, lineage_max_depth, lineage_max_nodes, lineage_max_edges, timeline_limit, task_manager, redis_service, audit_store, lineage_store)` (line 95): no docstring
  - `async cleanup_old_replay_results(older_than_hours, redis_service)` (line 193): no docstring
  - `async replay_instance_state_task(task_id, request, storage_service, redis_service)` (line 238): no docstring
  - `async _require_completed_task(task_id, task_manager)` (line 290): no docstring
  - `async _get_replay_payload(task_id, redis_service)` (line 302): no docstring
  - `_extract_command_history(instance_state)` (line 315): no docstring
  - `_select_command(command_history, command_id)` (line 330): no docstring
  - `_resolve_target(task, instance_state)` (line 343): no docstring
  - `_build_timeline(command_history, timeline_limit, db_name)` (line 356): no docstring
  - `async _load_audit_logs(audit_store, db_name, command_id, limit)` (line 384): no docstring
  - `_parse_iso_datetime(raw)` (line 408): no docstring

### `backend/bff/services/ai_service.py`
- **Functions**
  - `_trim_text(value, max_chars)` (line 56): no docstring
  - `_resolve_optional_principal(request)` (line 65): no docstring
  - `_ensure_session_owner(record, user_id)` (line 75): no docstring
  - `async _load_session_context(session_id, request, sessions, max_messages)` (line 87): no docstring
  - `async _record_session_message(session_id, request, sessions, role, content, metadata)` (line 145): no docstring
  - `_log_ai_event(event, payload, max_chars)` (line 197): no docstring
  - `_build_intent_prompts(question, lang, context)` (line 205): no docstring
  - `_build_intent_repair_prompts(question, lang, context, draft, issue)` (line 234): no docstring
  - `async ai_intent(body, request, llm, redis_service, audit_store, sessions, dataset_registry)` (line 263): no docstring
  - `_now_iso()` (line 531): no docstring
  - `_cap_int(value, lo, hi)` (line 535): no docstring
  - `async _load_schema_context(db_name, oms, redis_service, cache_ttl_s, max_classes, max_properties_per_class, max_relationships_per_class)` (line 539): Build a minimal, LLM-friendly schema context.
  - `_build_plan_prompts(question, schema_context, mode, branch, limit_cap, conversation_context, dataset_inventory)` (line 636): no docstring
  - `_build_plan_repair_prompts(question, schema_context, mode, branch, limit_cap, conversation_context, dataset_inventory, prior_plan, issue)` (line 734): no docstring
  - `_build_answer_prompts(question, grounding)` (line 835): no docstring
  - `_validate_and_cap_plan(plan, limit_cap)` (line 871): Enforce server-side caps regardless of what the LLM produced.
  - `async _execute_label_query(db_name, query_dict, lang, mapper, terminus)` (line 908): Execute label query by reusing the same deterministic pipeline as /database/{db_name}/query.
  - `_ground_label_query_result(execution, max_rows)` (line 933): no docstring
  - `_ground_graph_query_result(execution, max_nodes, max_edges)` (line 945): no docstring
  - `_dataset_item_min(item)` (line 1016): no docstring
  - `_ground_dataset_list_result(datasets, total_count, max_items)` (line 1036): no docstring
  - `_build_dataset_inventory(datasets, max_items)` (line 1045): no docstring
  - `async translate_query_plan(db_name, body, request, llm, redis_service, audit_store, oms, sessions, dataset_registry)` (line 1057): Natural language → constrained query plan JSON.
  - `async ai_query(db_name, body, request, llm, redis_service, audit_store, lineage_store, oms, mapper, terminus, sessions, dataset_registry)` (line 1206): End-to-end natural language query:

### `backend/bff/services/base_http_client.py`
- **Classes**
  - `ManagedAsyncClient` (line 8): no docstring
    - `async close(self)` (line 11): no docstring

### `backend/bff/services/context7_service.py`
- **Functions**
  - `async _call_context7(action, func)` (line 25): no docstring
  - `async search_context7(request, client)` (line 36): no docstring
  - `async get_entity_context(entity_id, client)` (line 45): no docstring
  - `async add_knowledge(request, client)` (line 51): no docstring
  - `async create_entity_link(request, client)` (line 65): no docstring
  - `async analyze_ontology(request, client, oms_client)` (line 83): no docstring
  - `async get_ontology_suggestions(db_name, class_id, client)` (line 112): no docstring
  - `async check_context7_health(client)` (line 124): no docstring

### `backend/bff/services/data_connector_pipelining_service.py`
- **Functions**
  - `async start_pipelining_google_sheet(sheet_id, payload, http_request, google_sheets_service, connector_registry, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 37): no docstring

### `backend/bff/services/data_connector_registration_service.py`
- **Functions**
  - `_as_int_or_none(value)` (line 31): no docstring
  - `async _resolve_tokens(connector_registry, connection_id)` (line 35): no docstring
  - `async register_google_sheet(sheet_data, google_sheets_service, connector_registry, dataset_registry, lineage_store)` (line 56): no docstring
  - `async preview_google_sheet(sheet_id, worksheet_name, limit, google_sheets_service, connector_registry)` (line 209): no docstring
  - `async list_registered_sheets(database_name, connector_registry)` (line 261): no docstring
  - `async unregister_google_sheet(sheet_id, connector_registry)` (line 316): no docstring

### `backend/bff/services/database_error_policy.py`
- **Functions**
  - `_code_for_status(status_code)` (line 15): no docstring
  - `apply_message_error_policies(exc, logger, log_message, policies, default_status_code, default_detail)` (line 44): no docstring
- **Classes**
  - `MessageErrorPolicy` (line 34): no docstring
    - `matches(self, normalized_message)` (line 40): no docstring

### `backend/bff/services/database_service.py`
- **Functions**
  - `_is_dev_mode()` (line 44): no docstring
  - `async _get_expected_seq_for_database(db_name)` (line 48): no docstring
  - `_coerce_db_entry(entry)` (line 73): no docstring
  - `_validate_db_branch_pair(db_name, branch_name)` (line 87): no docstring
  - `_database_not_found_policy(db_name)` (line 91): no docstring
  - `_enrich_db_entry(entry, actor_type, actor_id, actor_name, access_rows)` (line 99): no docstring
  - `async list_databases(request, oms, dataset_registry)` (line 146): 데이터베이스 목록 조회
  - `async create_database(body, http_request, oms)` (line 201): 데이터베이스 생성
  - `async delete_database(db_name, http_request, expected_seq, oms)` (line 299): 데이터베이스 삭제
  - `async get_branch_info(db_name, branch_name, oms)` (line 386): 브랜치 정보 조회 (프론트엔드용 BFF 래핑)
  - `async delete_branch(db_name, branch_name, force, oms)` (line 407): 브랜치 삭제 (프론트엔드용 BFF 래핑)
  - `async get_database(db_name, oms)` (line 437): 데이터베이스 정보 조회
  - `async get_database_expected_seq(db_name)` (line 461): Resolve the current `expected_seq` for database (aggregate) operations.
  - `async list_classes(db_name, type, limit, oms)` (line 485): 데이터베이스의 클래스 목록 조회
  - `async create_class(db_name, class_data, oms)` (line 537): 데이터베이스에 새 클래스 생성
  - `async get_class(db_name, class_id, oms)` (line 605): 특정 클래스 조회
  - `async list_branches(db_name, oms)` (line 667): 브랜치 목록 조회
  - `async create_branch(db_name, branch_data, oms)` (line 696): 새 브랜치 생성
  - `async get_versions(db_name, oms)` (line 736): 버전 히스토리 조회

### `backend/bff/services/dataset_ingest_commit_service.py`
- **Functions**
  - `_normalize_text(value)` (line 24): no docstring
  - `async ensure_lakefs_commit_artifact(ingest_request, lakefs_client, lakefs_storage_service, repository, branch, commit_message, commit_metadata, object_key, expected_checksum, source_branch, artifact_key_builder, initial_commit_id, initial_artifact_key)` (line 29): no docstring
  - `async persist_ingest_commit_state(dataset_registry, ingest_request, ingest_transaction, commit_id, artifact_key, force)` (line 85): no docstring
- **Classes**
  - `LakeFSCommitArtifact` (line 18): no docstring

### `backend/bff/services/dataset_ingest_failures.py`
- **Functions**
  - `async mark_ingest_failed(dataset_registry, ingest_request, error, stage)` (line 13): no docstring

### `backend/bff/services/dataset_ingest_idempotency.py`
- **Functions**
  - `async resolve_existing_version_or_raise(dataset_registry, ingest_request, expected_dataset_id, request_fingerprint)` (line 16): Enforce idempotency invariants for an ingest request.

### `backend/bff/services/dataset_ingest_outbox_builder.py`
- **Classes**
  - `DatasetIngestOutboxBuilder` (line 19): no docstring
    - `dataset_created(self, dataset_id, db_name, name, actor, transaction_id, extra_data)` (line 23): no docstring
    - `version_created(self, event_id, dataset_id, db_name, name, actor, command_type, lakefs_commit_id, artifact_key, transaction_id, extra_data)` (line 49): no docstring
    - `artifact_stored_lineage(self, version_event_id, artifact_key, db_name, from_label, edge_metadata, to_label)` (line 86): no docstring

### `backend/bff/services/dataset_ingest_outbox_flusher.py`
- **Functions**
  - `_dataset_ingest_outbox_worker_enabled()` (line 12): no docstring
  - `async maybe_flush_dataset_ingest_outbox_inline(dataset_registry, lineage_store, flush_dataset_ingest_outbox, batch_size)` (line 17): Best-effort inline flush of the dataset ingest outbox.

### `backend/bff/services/funnel_client.py`
- **Classes**
  - `FunnelClient` (line 19): Funnel HTTP 클라이언트
    - `__init__(self, base_url)` (line 22): no docstring
    - `_resolve_excel_timeout_seconds()` (line 39): no docstring
    - `async check_health(self)` (line 42): Funnel 서비스 상태 확인
    - `async analyze_dataset(self, request_data, timeout_seconds)` (line 59): 데이터셋 타입 분석
    - `async suggest_schema(self, analysis_results, class_name)` (line 86): 분석 결과를 기반으로 OMS 스키마 제안
    - `async preview_google_sheets(self, sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types)` (line 113): Google Sheets 데이터 미리보기와 타입 추론
    - `async analyze_and_suggest_schema(self, data, columns, class_name, sample_size, include_complex_types)` (line 156): 데이터 분석과 스키마 제안을 한 번에 실행
    - `async google_sheets_to_schema(self, sheet_url, worksheet_name, class_name, api_key, connection_id, table_id, table_bbox)` (line 196): Google Sheets에서 직접 스키마 생성
    - `async google_sheets_to_structure_preview(self, sheet_url, worksheet_name, api_key, connection_id, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 288): Google Sheets URL → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_google_sheets_structure(self, sheet_url, worksheet_name, api_key, connection_id, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 332): Analyze sheet structure via Funnel (Google Sheets URL → grid/merged_cells → structure analysis).
    - `async excel_to_structure_preview(self, xlsx_bytes, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 368): Excel bytes → (grid/merged_cells) → structure analysis → selected table preview.
    - `async excel_to_structure_preview_stream(self, fileobj, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 409): Excel stream → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_excel_structure(self, xlsx_bytes, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 446): Analyze sheet structure via Funnel (Excel bytes → grid/merged_cells → structure analysis).
    - `async analyze_excel_structure_stream(self, fileobj, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 491): Analyze sheet structure via Funnel (streaming Excel upload).
    - `_select_primary_table(structure)` (line 574): Choose a single "primary" table for schema suggestion.
    - `_select_requested_table(cls, structure, table_id, table_bbox)` (line 608): Select a table from structure analysis output.
    - `_normalize_bbox_dict(bbox)` (line 656): no docstring
    - `_structure_table_to_preview(structure, table, sheet_url, worksheet_name)` (line 667): no docstring
    - `_structure_table_to_excel_preview(structure, table, file_name, sheet_name)` (line 720): no docstring
    - `async excel_to_schema(self, xlsx_bytes, filename, sheet_name, class_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 766): Excel 업로드에서 직접 스키마 생성 (구조 분석 기반).
    - `async __aenter__(self)` (line 825): no docstring
    - `async __aexit__(self, _exc_type, _exc_val, _exc_tb)` (line 828): no docstring

### `backend/bff/services/funnel_type_inference_adapter.py`
- **Classes**
  - `FunnelHTTPTypeInferenceAdapter` (line 18): HTTP 기반 Funnel 마이크로서비스를 TypeInferenceInterface로 adapting하는 클래스.
    - `__init__(self, funnel_client)` (line 26): no docstring
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 29): 단일 컬럼 데이터의 타입을 추론합니다.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 75): 전체 데이터셋을 분석하여 모든 컬럼의 타입을 추론합니다.
    - `async infer_type_with_confidence(self, values, check_complex)` (line 92): 값 리스트에서 타입을 추론하고 신뢰도를 반환합니다.
    - `async infer_single_value_type(self, value, context)` (line 104): 단일 값의 타입을 추론합니다.
    - `async _analyze_single_column(self, data, headers, include_complex_types)` (line 125): 단일 컬럼 분석을 위한 비동기 헬퍼 메서드
    - `async _analyze_dataset_async(self, data, headers, include_complex_types, sample_size)` (line 138): 데이터셋 분석을 위한 비동기 헬퍼 메서드
    - `_build_analysis_request(data, headers, include_complex_types, sample_size)` (line 156): no docstring
    - `async _analyze_with_fallback(self, data, headers, include_complex_types, sample_size)` (line 170): no docstring
    - `_convert_funnel_column_result(self, funnel_result)` (line 208): Funnel 서비스 응답을 Interface 형식으로 변환
    - `async close(self)` (line 224): 클라이언트 연결 종료

### `backend/bff/services/governance_service.py`
- **Functions**
  - `async create_backing_datasource(body, request, dataset_registry)` (line 41): no docstring
  - `async list_backing_datasources(request, dataset_id, db_name, branch, dataset_registry)` (line 84): no docstring
  - `async get_backing_datasource(backing_id, request, dataset_registry)` (line 116): no docstring
  - `async create_backing_datasource_version(backing_id, body, request, dataset_registry)` (line 131): no docstring
  - `async list_backing_datasource_versions(backing_id, request, dataset_registry)` (line 165): no docstring
  - `async get_backing_datasource_version(version_id, request, dataset_registry)` (line 184): no docstring
  - `async create_key_spec(body, request, dataset_registry)` (line 204): no docstring
  - `async list_key_specs(request, dataset_id, dataset_registry)` (line 259): no docstring
  - `async get_key_spec(key_spec_id, request, dataset_registry)` (line 276): no docstring
  - `async list_schema_migration_plans(request, db_name, subject_type, subject_id, status_value, dataset_registry)` (line 293): no docstring
  - `async upsert_gate_policy(body, dataset_registry)` (line 318): no docstring
  - `async list_gate_policies(scope, dataset_registry)` (line 339): no docstring
  - `async list_gate_results(scope, subject_type, subject_id, dataset_registry)` (line 349): no docstring
  - `async upsert_access_policy(body, request, dataset_registry)` (line 361): no docstring
  - `async list_access_policies(request, db_name, scope, subject_type, subject_id, policy_status, dataset_registry)` (line 402): no docstring
  - `async handle_request_errors(fn, *args, **kwargs)` (line 427): Helper to wrap governance service calls with consistent error mapping.

### `backend/bff/services/graph_federation_provider.py`
- **Functions**
  - `_build_graph_federation_service(settings)` (line 20): no docstring
  - `async _get_from_container(container)` (line 31): no docstring
  - `async get_graph_federation_service()` (line 39): FastAPI dependency to get a GraphFederationServiceES instance.

### `backend/bff/services/graph_query_service.py`
- **Functions**
  - `_resolve_graph_branches(db_name, base_branch, overlay_branch, branch, include_documents, classes_in_query)` (line 51): no docstring
  - `_raise_overlay_degraded(ctx)` (line 103): no docstring
  - `async _merge_fallback_for_degraded(db_name, ctx, documents)` (line 121): Server-side merge fallback when ES overlay index is unavailable (DEGRADED).
  - `async _load_access_policies(dataset_registry, db_name, class_ids)` (line 198): no docstring
  - `_apply_access_policies_to_nodes(nodes, policies)` (line 217): no docstring
  - `_apply_access_policies_to_documents(documents, policy)` (line 248): no docstring
  - `_collect_class_ids_from_hops(hops)` (line 272): no docstring
  - `async execute_graph_query(db_name, query, request, lineage_store, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 286): Execute multi-hop graph query with ES federation (TerminusDB + Elasticsearch).
  - `async execute_simple_graph_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 591): Execute simple single-class graph query.
  - `async execute_multi_hop_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 708): Execute multi-hop graph query (legacy dict payload).
  - `async find_relationship_paths(db_name, source_class, target_class, max_depth, graph_service, branch)` (line 854): no docstring
  - `async graph_service_health(graph_service)` (line 892): Check health of graph federation service (ES-only, no TerminusDB).
- **Classes**
  - `GraphBranchContext` (line 40): no docstring

### `backend/bff/services/http_idempotency.py`
- **Functions**
  - `require_idempotency_key(request)` (line 14): no docstring
  - `get_idempotency_key(request)` (line 23): no docstring

### `backend/bff/services/input_validation_service.py`
- **Functions**
  - `_to_bad_request(exc)` (line 23): no docstring
  - `validated_db_name(db_name)` (line 27): no docstring
  - `validated_branch_name(branch)` (line 34): no docstring
  - `sanitized_payload(payload)` (line 41): no docstring
  - `enforce_db_scope_or_403(request, db_name)` (line 48): no docstring

### `backend/bff/services/instance_async_service.py`
- **Functions**
  - `_metadata_dict(value)` (line 35): no docstring
  - `_fallback_langs(primary)` (line 41): no docstring
  - `async resolve_class_id(db_name, class_label, label_mapper, lang)` (line 52): no docstring
  - `async convert_labels_to_ids(data, db_name, class_id, label_mapper, lang)` (line 67): Label 기반 데이터를 ID 기반으로 변환.
  - `async _handle_command_errors(fn, *args, op_message, **kwargs)` (line 120): no docstring
  - `async create_instance_async(db_name, class_label, data, metadata, http_request, branch, oms_client, label_mapper, user_id)` (line 138): no docstring
  - `async update_instance_async(db_name, class_label, instance_id, data, metadata, http_request, expected_seq, branch, oms_client, label_mapper, user_id)` (line 191): no docstring
  - `async delete_instance_async(db_name, class_label, instance_id, http_request, branch, expected_seq, oms_client, label_mapper, user_id)` (line 247): no docstring
  - `async bulk_create_instances_async(db_name, class_label, instances, metadata, http_request, branch, oms_client, label_mapper, user_id)` (line 283): no docstring

### `backend/bff/services/instances_service.py`
- **Functions**
  - `_is_action_log_class_id(class_id)` (line 50): no docstring
  - `_action_log_as_instance(record)` (line 54): no docstring
  - `_projection_unavailable_detail(message, base_branch, overlay_branch, writeback_enabled, class_id, instance_id)` (line 58): no docstring
  - `async _apply_access_policy_to_instances(dataset_registry, db_name, class_id, instances)` (line 83): no docstring
  - `_normalize_es_search_result(result)` (line 104): Normalize Elasticsearch search results across return shapes.
  - `_resolve_overlay_context(db_name, class_id, overlay_branch)` (line 146): no docstring
  - `_sanitize_search_query(search)` (line 170): no docstring
  - `_overlay_key_for_doc(doc)` (line 193): no docstring
  - `_merge_overlay_instances(base_instances, overlay_instances)` (line 211): no docstring
  - `async list_class_instances(db_name, class_id, request_headers, base_branch, overlay_branch, branch, limit, offset, search, status_filter, action_type_id, submitted_by, elasticsearch_service, dataset_registry, action_logs)` (line 240): no docstring
  - `async get_class_sample_values(db_name, class_id, property_name, base_branch, branch, limit, elasticsearch_service, dataset_registry)` (line 463): no docstring
  - `async _server_merge_fallback(db_name, class_id, instance_id, resolved_base_branch, resolved_overlay_branch, writeback_enabled, dataset_registry)` (line 577): no docstring
  - `async get_instance_detail(db_name, class_id, instance_id, request_headers, base_branch, overlay_branch, branch, elasticsearch_service, dataset_registry, action_logs)` (line 645): no docstring
- **Classes**
  - `OverlayContext` (line 44): no docstring

### `backend/bff/services/label_mapping_service.py`
- **Functions**
  - `_validation_details_template()` (line 58): no docstring
  - `async export_mappings(db_name, mapper)` (line 62): 레이블 매핑 내보내기
  - `async _validate_file_upload(file)` (line 83): Validate uploaded file size, type, and extension.
  - `async _read_and_parse_file(file)` (line 106): Read file content and parse JSON.
  - `_sanitize_and_validate_schema(raw_mappings, db_name)` (line 156): Sanitize input and validate schema.
  - `_validate_business_logic(mapping_request, sanitized_mappings, db_name)` (line 188): Validate business logic and data consistency.
  - `async _load_ontology_ids(oms_client, db_name)` (line 228): no docstring
  - `async _load_existing_label_map(mapper, db_name)` (line 235): no docstring
  - `_compute_validation_details(mapping_request, existing_class_ids, existing_property_ids, existing_class_labels, existing_prop_labels)` (line 242): no docstring
  - `_validation_passed(details)` (line 305): no docstring
  - `async _perform_mapping_import(mapper, validated_mappings, db_name)` (line 309): no docstring
  - `async import_mappings(db_name, file, mapper, oms_client)` (line 545): Import label mappings from JSON file with enhanced security validation.
  - `async validate_mappings(db_name, file, mapper, oms_client)` (line 558): Validate mappings without importing.
  - `async get_mappings_summary(db_name, mapper)` (line 571): 레이블 매핑 요약 조회
  - `async clear_mappings(db_name, mapper)` (line 616): 레이블 매핑 초기화
- **Classes**
  - `MappingImportPayload` (line 34): Label mapping bundle file schema.
  - `MappingBundleContext` (line 50): no docstring
  - `_MappingBundleProcessor` (line 334): Template Method for mapping bundle file processing.
    - `async process(self, db_name, file, mapper, oms_client)` (line 337): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 359): no docstring
  - `_ImportProcessor` (line 362): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 363): no docstring
  - `_ValidateProcessor` (line 437): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 438): no docstring

### `backend/bff/services/link_types_mapping_service.py`
- **Functions**
  - `extract_schema_columns(schema)` (line 44): no docstring
  - `extract_schema_types(schema)` (line 48): no docstring
  - `compute_schema_hash(schema)` (line 52): no docstring
  - `build_join_schema(source_key_column, target_key_column, source_key_type, target_key_type)` (line 56): no docstring
  - `extract_ontology_properties(payload)` (line 71): no docstring
  - `extract_ontology_relationships(payload)` (line 85): no docstring
  - `normalize_spec_type(value)` (line 99): no docstring
  - `normalize_policy(value, default)` (line 103): no docstring
  - `normalize_pk_fields(value)` (line 110): no docstring
  - `async resolve_object_type_contract(oms_client, db_name, class_id, branch)` (line 115): no docstring
  - `async resolve_dataset_and_version(dataset_registry, dataset_id, dataset_version_id)` (line 136): no docstring
  - `async ensure_join_dataset(dataset_registry, request, db_name, join_dataset_id, join_dataset_version_id, join_dataset_name, join_dataset_branch, auto_create, default_name, source_key_column, target_key_column, source_key_type, target_key_type)` (line 161): no docstring
  - `resolve_property_type(prop_map, field)` (line 246): no docstring
  - `_extract_pk_fields(contract, props)` (line 254): no docstring
  - `async build_mapping_request(db_name, request, oms_client, dataset_registry, relationship_spec_id, link_type_id, source_class, target_class, predicate, cardinality, branch, source_props, target_props, source_contract, target_contract, spec_payload)` (line 589): no docstring
- **Classes**
  - `_MappingContext` (line 260): no docstring
  - `_MappingResult` (line 279): no docstring
  - `_ForeignKeyMappingStrategy` (line 286): no docstring
    - `async build(self, ctx, fk_spec, source_pk_fields, target_pk_fields)` (line 287): no docstring
  - `_JoinTableMappingStrategy` (line 409): no docstring
    - `async build(self, ctx, join_spec, source_pk_fields, target_pk_fields, spec_type, relationship_kind, relationship_object_type)` (line 410): no docstring
  - `_ObjectBackedMappingStrategy` (line 525): no docstring
    - `__init__(self, join_strategy)` (line 526): no docstring
    - `async build(self, ctx, object_backed_spec, source_pk_fields, target_pk_fields)` (line 529): no docstring

### `backend/bff/services/link_types_write_service.py`
- **Functions**
  - `async _require_role(request, db_name, roles, enforce_role)` (line 48): no docstring
  - `_require_non_empty(value, detail)` (line 58): no docstring
  - `_extract_mapping_payload(mapping_response)` (line 65): no docstring
  - `async _enqueue_link_index_job(enqueue_job, dataset_registry, objectify_registry, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id)` (line 72): no docstring
  - `async create_link_type(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry, enforce_role, create_mapping_spec, enqueue_job)` (line 93): no docstring
  - `async update_link_type(db_name, link_type_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry, enforce_role, create_mapping_spec, enqueue_job)` (line 296): no docstring
  - `async reindex_link_type(db_name, link_type_id, request, dataset_version_id, dataset_registry, objectify_registry, enforce_role, enqueue_job)` (line 515): no docstring

### `backend/bff/services/mapping_suggestion_service.py`
- **Classes**
  - `MappingCandidate` (line 21): 매핑 후보
  - `MappingSuggestion` (line 31): 매핑 제안 결과
  - `MappingSuggestionService` (line 39): 스키마 간 매핑을 자동으로 제안하는 서비스
    - `__init__(self, config)` (line 53): no docstring
    - `suggest_mappings(self, source_schema, target_schema, sample_data, target_sample_data)` (line 120): 소스 스키마를 타겟 스키마에 매핑하는 제안 생성
    - `_field_name_candidates(field)` (line 242): Return candidate strings for name matching.
    - `_check_exact_match(self, source_field, target_field)` (line 284): 정확한 이름 매칭 검사
    - `_check_token_match(self, source_field, target_field)` (line 311): 토큰 기반 이름 매칭 검사
    - `_check_fuzzy_match(self, source_field, target_field)` (line 358): 퍼지 이름 매칭 검사
    - `_check_semantic_match(self, source_field, target_field)` (line 404): 의미론적 매칭 검사 (옵션: 별칭/동의어 그룹 기반)
    - `_check_type_match(self, source_field, target_field)` (line 444): 타입 기반 매칭 검사
    - `_check_pattern_match(self, source_field, target_field, sample_data)` (line 495): 값 패턴 기반 매칭 검사
    - `_normalize_field_name(self, name)` (line 557): 필드 이름 정규화
    - `_is_abbreviation(self, short, long)` (line 569): 약어 관계 검사
    - `_analyze_value_patterns(self, values)` (line 589): 값 패턴 분석
    - `_resolve_conflicts(self, candidates)` (line 632): 매핑 충돌 해결 (하나의 타겟에 여러 소스가 매핑되는 경우)
    - `_levenshtein_similarity(self, s1, s2)` (line 658): Calculate Levenshtein similarity between two strings
    - `_tokenize_field_name(self, name)` (line 698): 필드 이름을 토큰으로 분리 (스톱워드 제거)
    - `_token_similarity(self, source_name, target_name)` (line 715): 토큰 기반 이름 유사도 계산
    - `_normalize_text_advanced(self, text)` (line 741): 고급 텍스트 정규화 (NFKC, 공백, 특수문자)
    - `_distribution_similarity(self, source_values, target_values, source_type, target_type)` (line 758): 값 분포 유사도 계산
    - `_numeric_distribution_similarity(self, source_values, target_values)` (line 798): 수치형 분포 유사도 (KS-test 기반)
    - `_categorical_distribution_similarity(self, source_values, target_values)` (line 849): 범주형 분포 유사도 (Jaccard/Overlap)
    - `_string_distribution_similarity(self, source_values, target_values)` (line 872): 문자열 분포 유사도 (n-gram 기반)
    - `_temporal_distribution_similarity(self, source_values, target_values)` (line 918): 시간형 분포 유사도
    - `_is_numeric(self, value)` (line 935): Check if string represents a number
    - `_is_date_pattern(self, value)` (line 943): Check if string matches common date patterns
    - `_detect_date_format(self, date_str)` (line 958): Detect date format pattern
    - `_check_distribution_match(self, source_field, target_field, sample_data, target_sample_data)` (line 970): 값 분포 기반 매칭 검사

### `backend/bff/services/merge_conflict_service.py`
- **Functions**
  - `_validate_inputs(db_name, source_branch, target_branch)` (line 31): no docstring
  - `async simulate_merge(db_name, request, oms_client)` (line 39): Simulate a merge (no write) and return UI-friendly conflict format.
  - `async resolve_merge_conflicts(db_name, request, oms_client)` (line 150): Resolve merge conflicts using user-provided resolutions, then perform merge.
  - `async _detect_merge_conflicts(source_changes, target_changes, common_ancestor, db_name, oms_client)` (line 205): no docstring
  - `async _convert_resolution_to_terminus_format(resolution)` (line 279): no docstring

### `backend/bff/services/object_type_contract_service.py`
- **Functions**
  - `_extract_resource_payload(response)` (line 40): no docstring
  - `_schema_hash_from_version(sample_json, schema_json)` (line 49): no docstring
  - `async _resolve_backing(db_name, request, dataset_registry, backing_dataset_id, backing_datasource_id, backing_datasource_version_id, dataset_version_id, schema_hash)` (line 61): no docstring
  - `_extract_ontology_property_names(payload)` (line 156): no docstring
  - `_normalize_and_validate_pk_spec(raw_pk_spec, ontology_property_names)` (line 172): no docstring
  - `async create_object_type_contract(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 191): no docstring
  - `async get_object_type_contract(db_name, class_id, request, branch, oms_client, dataset_registry, objectify_registry)` (line 383): no docstring
  - `_normalize_field_list(raw_value)` (line 461): no docstring
  - `_normalize_field_moves(raw_value)` (line 471): no docstring
  - `async update_object_type_contract(db_name, class_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 489): no docstring

### `backend/bff/services/objectify_dag_service.py`
- **Functions**
  - `async run_objectify_dag(db_name, body, request, dataset_registry, objectify_registry, job_queue, oms_client)` (line 603): Enterprise helper: enqueue multiple objectify jobs in dependency order.
- **Classes**
  - `_DagPlanItem` (line 41): no docstring
    - `to_dict(self)` (line 49): no docstring
  - `_ObjectifyDagOrchestrator` (line 60): no docstring
    - `__init__(self, db_name, body, dataset_registry, objectify_registry, job_queue, oms_client)` (line 61): no docstring
    - `async _fetch_object_type_contract(self, class_id)` (line 91): no docstring
    - `async _resolve_mapping_spec_for_object_type(self, class_id, backing_source)` (line 112): no docstring
    - `async _fetch_relationship_targets(self, class_id)` (line 155): no docstring
    - `async _load_class_info(self, class_id)` (line 173): no docstring
    - `async _build_dependency_closure(self, start)` (line 219): no docstring
    - `_toposort(self, start)` (line 268): no docstring
    - `_build_plan(self, ordered)` (line 320): no docstring
    - `async compute_plan(self, class_ids)` (line 337): no docstring
    - `async _enqueue_class_job(self, class_id, run_id, start)` (line 345): no docstring
    - `_extract_command_status(payload)` (line 407): no docstring
    - `_is_dataset_primary_mode(report)` (line 417): no docstring
    - `async _wait_for_objectify_submitted(self, job_id, timeout_seconds)` (line 424): no docstring
    - `async _wait_for_command_terminal(self, command_id, timeout_seconds)` (line 461): no docstring
    - `async _wait_for_class_ready(self, class_id)` (line 487): no docstring
    - `async _run_ready_orchestrator(self, ordered, initial_classes, start)` (line 495): no docstring
    - `async enqueue_plan(self, ordered, start)` (line 542): no docstring

### `backend/bff/services/objectify_mapping_spec_service.py`
- **Functions**
  - `async create_mapping_spec(body, request, dataset_registry, objectify_registry, oms_client)` (line 45): Create an objectify mapping spec with full validation.

### `backend/bff/services/objectify_ops_service.py`
- **Functions**
  - `_match_output_name(output, name)` (line 34): no docstring
  - `_compute_schema_hash_from_sample(sample_json)` (line 38): no docstring
  - `_extract_schema_columns(schema)` (line 42): no docstring
  - `_extract_schema_types(schema)` (line 46): no docstring
  - `_normalize_mapping_pair(item)` (line 50): no docstring
  - `_build_mapping_change_summary(previous_mappings, new_mappings)` (line 60): no docstring
  - `_extract_ontology_fields(payload)` (line 113): no docstring
  - `_resolve_import_type(raw_type)` (line 117): no docstring
  - `_is_type_compatible(source_type, target_type)` (line 121): no docstring
  - `_unwrap_data_payload(payload)` (line 125): no docstring

### `backend/bff/services/objectify_run_service.py`
- **Functions**
  - `async run_objectify(dataset_id, body, request, dataset_registry, objectify_registry, job_queue, pipeline_registry, oms_client)` (line 41): no docstring

### `backend/bff/services/oms_client.py`
- **Classes**
  - `OntologyRef` (line 26): Minimal ontology reference used by BFF validation flows.
  - `OMSClient` (line 33): OMS HTTP 클라이언트
    - `__init__(self, base_url)` (line 36): no docstring
    - `_get_auth_token()` (line 61): no docstring
    - `_branch_base_path(db_name)` (line 66): no docstring
    - `async get(self, path, **kwargs)` (line 73): Low-level GET helper (returns JSON dict).
    - `async post(self, path, **kwargs)` (line 81): Low-level POST helper (returns JSON dict).
    - `async put(self, path, **kwargs)` (line 89): Low-level PUT helper (returns JSON dict).
    - `async delete(self, path, **kwargs)` (line 97): Low-level DELETE helper (returns JSON dict when available).
    - `async check_health(self)` (line 105): OMS 서비스 상태 확인
    - `async list_databases(self)` (line 117): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 127): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 150): 데이터베이스 삭제
    - `async get_database(self, db_name)` (line 163): 데이터베이스 정보 조회
    - `async create_ontology(self, db_name, ontology_data, branch, headers)` (line 173): 온톨로지 생성
    - `async validate_ontology_create(self, db_name, ontology_data, branch)` (line 201): 온톨로지 생성 검증 (no write).
    - `async get_ontology(self, db_name, class_id, branch)` (line 221): 온톨로지 조회
    - `async list_ontologies(self, db_name, branch)` (line 234): 온톨로지 목록 조회
    - `async get_ontologies(self, db_name, branch)` (line 247): Compatibility helper for BFF callers that need a flattened view of:
    - `async list_branches(self, db_name)` (line 292): 브랜치 목록 조회
    - `async get_branch_info(self, db_name, branch_name)` (line 302): no docstring
    - `async list_ontology_resources(self, db_name, resource_type, branch, limit, offset)` (line 313): 온톨로지 리소스 목록 조회
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 341): 단일 온톨로지 리소스 조회
    - `async create_ontology_resource(self, db_name, resource_type, payload, branch, expected_head_commit)` (line 361): 온톨로지 리소스 생성
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 386): 온톨로지 리소스 업데이트
    - `async delete_ontology_resource(self, db_name, resource_type, resource_id, branch, expected_head_commit)` (line 412): 온톨로지 리소스 삭제
    - `async list_ontology_branches(self, db_name)` (line 436): 온톨로지 브랜치 목록 조회
    - `async create_ontology_branch(self, db_name, payload)` (line 446): 온톨로지 브랜치 생성
    - `async list_ontology_proposals(self, db_name, status_filter, limit)` (line 459): 온톨로지 제안 목록 조회
    - `async create_ontology_proposal(self, db_name, payload)` (line 477): 온톨로지 제안 생성
    - `async approve_ontology_proposal(self, db_name, proposal_id, payload)` (line 490): 온톨로지 제안 승인
    - `async deploy_ontology(self, db_name, payload)` (line 505): 온톨로지 배포(승격)
    - `async get_ontology_health(self, db_name, branch)` (line 518): 온톨로지 헬스 체크
    - `async create_branch(self, db_name, branch_data, from_branch)` (line 531): 브랜치 생성
    - `async delete_branch(self, db_name, branch_name, force)` (line 552): no docstring
    - `async get_version_history(self, db_name)` (line 566): 버전 히스토리 조회
    - `async get_version_head(self, db_name, branch)` (line 576): 브랜치 head 커밋 ID 조회 (deploy gate).
    - `async update_ontology(self, db_name, class_id, update_data, expected_seq, branch, headers)` (line 589): 온톨로지 업데이트
    - `async validate_ontology_update(self, db_name, class_id, update_data, branch)` (line 613): 온톨로지 업데이트 검증 (no write).
    - `async delete_ontology(self, db_name, class_id, expected_seq, branch, headers)` (line 634): 온톨로지 삭제
    - `async query_ontologies(self, db_name, query)` (line 661): 온톨로지 쿼리
    - `async database_exists(self, db_name)` (line 671): 데이터베이스 존재 여부 확인
    - `async commit_database_change(self, db_name, message, author)` (line 682): 데이터베이스 변경사항 자동 커밋
    - `async commit_system_change(self, message, author, operation, target)` (line 709): 시스템 레벨 변경사항 커밋 (데이터베이스 생성/삭제 등)
    - `async get_class_metadata(self, db_name, class_id)` (line 735): 클래스의 메타데이터 가져오기
    - `async update_class_metadata(self, db_name, class_id, metadata)` (line 758): 클래스의 메타데이터 업데이트
    - `async get_class_instances(self, db_name, class_id, limit, offset, search)` (line 801): 특정 클래스의 인스턴스 목록을 효율적으로 조회 (N+1 Query 최적화)
    - `async get_instance(self, db_name, instance_id, class_id)` (line 840): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id)` (line 872): 특정 클래스의 인스턴스 개수 조회
    - `async execute_sparql(self, db_name, query, limit, offset)` (line 897): SPARQL 쿼리 실행
    - `async __aenter__(self)` (line 935): no docstring
    - `async __aexit__(self, _exc_type, _exc_val, _exc_tb)` (line 938): no docstring

### `backend/bff/services/oms_error_policy.py`
- **Functions**
  - `_code_for_status(status_code)` (line 16): no docstring
  - `raise_oms_boundary_exception(exc, action, logger, custom_http_status_details)` (line 34): no docstring

### `backend/bff/services/ontology_class_id_service.py`
- **Functions**
  - `resolve_or_generate_class_id(payload)` (line 17): no docstring

### `backend/bff/services/ontology_crud_service.py`
- **Functions**
  - `_as_string(value, lang, fallback)` (line 36): no docstring
  - `async _resolve_class_id(db_name, class_label, lang, mapper)` (line 49): no docstring
  - `async create_ontology(db_name, body, branch, mapper, oms_client)` (line 55): 온톨로지 생성
  - `async list_ontologies(db_name, request, branch, class_type, limit, offset, mapper, terminus)` (line 111): 온톨로지 목록 조회
  - `async get_ontology(db_name, class_label, request, branch, mapper, terminus)` (line 176): 온톨로지 조회
  - `async validate_ontology_create(db_name, body, branch, oms_client)` (line 235): 온톨로지 생성 검증 (no write) - OMS proxy.
  - `async validate_ontology_update(db_name, class_label, body, request, branch, mapper, oms_client)` (line 254): 온톨로지 업데이트 검증 (no write) - OMS proxy.
  - `async update_ontology(db_name, class_label, body, request, expected_seq, branch, mapper, terminus)` (line 280): 온톨로지 수정
  - `async delete_ontology(db_name, class_label, request, expected_seq, branch, mapper, terminus)` (line 336): 온톨로지 삭제
  - `async get_ontology_schema(db_name, class_id, request, format, branch, mapper, terminus, jsonld_conv)` (line 387): 온톨로지 스키마 조회

### `backend/bff/services/ontology_extensions_service.py`
- **Functions**
  - `async _call_oms(action, func)` (line 36): no docstring
  - `async list_resources(oms_client, db_name, resource_type, branch, limit, offset)` (line 53): no docstring
  - `async create_resource(oms_client, db_name, resource_type, payload, branch, expected_head_commit)` (line 75): no docstring
  - `async update_resource(oms_client, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 107): no docstring
  - `async get_resource(oms_client, db_name, resource_type, resource_id, branch)` (line 141): no docstring
  - `async delete_resource(oms_client, db_name, resource_type, resource_id, branch, expected_head_commit)` (line 161): no docstring
  - `async list_ontology_branches(oms_client, db_name)` (line 193): no docstring
  - `async create_ontology_branch(oms_client, db_name, request)` (line 201): no docstring
  - `async list_ontology_proposals(oms_client, db_name, status_filter, limit)` (line 212): no docstring
  - `async create_ontology_proposal(oms_client, db_name, request)` (line 230): no docstring
  - `async approve_ontology_proposal(oms_client, db_name, proposal_id, request)` (line 246): no docstring
  - `async deploy_ontology(oms_client, db_name, request)` (line 264): no docstring
  - `async ontology_health(oms_client, db_name, branch)` (line 280): no docstring

### `backend/bff/services/ontology_imports_service.py`
- **Functions**
  - `async dry_run_import_from_google_sheets(db_name, body)` (line 373): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_google_sheets(db_name, body, oms_client)` (line 419): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
  - `async dry_run_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, dry_run_rows, max_import_rows, options_json)` (line 474): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, allow_partial, max_import_rows, batch_size, return_instances, max_return_instances, options_json, oms_client)` (line 567): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
- **Classes**
  - `_ImportSourceStrategy` (line 30): no docstring
    - `async fetch_structure_preview(self, options)` (line 33): no docstring
    - `dry_run_source_info(self, table)` (line 35): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 37): no docstring
  - `_GoogleSheetsSource` (line 41): no docstring
    - `async fetch_structure_preview(self, options)` (line 55): no docstring
    - `dry_run_source_info(self, table)` (line 74): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 83): no docstring
  - `_ExcelSource` (line 96): no docstring
    - `async fetch_structure_preview(self, options)` (line 108): no docstring
    - `dry_run_source_info(self, table)` (line 125): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 134): no docstring
  - `_PreparedImport` (line 147): no docstring
  - `_ImportProcessorTemplate` (line 154): no docstring
    - `__init__(self, source)` (line 155): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 158): no docstring
    - `async _prepare(self, target_schema, mappings, max_import_rows, options)` (line 161): no docstring
  - `_DryRunImportProcessor` (line 196): no docstring
    - `__init__(self, source, dry_run_rows)` (line 197): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 201): no docstring
    - `async run(self, target_class_id, target_schema, mappings, max_import_rows, options)` (line 208): no docstring
  - `_CommitImportProcessor` (line 245): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 246): no docstring
    - `async run(self, db_name, target_class_id, target_schema, mappings, max_import_rows, options, allow_partial, batch_size, return_instances, max_return_instances, oms_client)` (line 251): no docstring

### `backend/bff/services/ontology_label_mapper_service.py`
- **Functions**
  - `async map_relationship_targets(mapper, db_name, ontology_dict, lang)` (line 16): Convert relationship targets from labels → class ids when mappings exist.
  - `async register_ontology_label_mappings(mapper, db_name, class_id, ontology_dict)` (line 39): Register class/property/relationship label mappings for a created ontology.

### `backend/bff/services/ontology_occ_guard_service.py`
- **Functions**
  - `_normalize_ref(value)` (line 20): no docstring
  - `_extract_head_commit_id(head_payload)` (line 25): no docstring
  - `async fetch_branch_head_commit_id(oms_client, db_name, branch)` (line 39): no docstring
  - `async resolve_expected_head_commit(oms_client, db_name, branch, expected_head_commit, allow_none, unresolved_detail)` (line 50): no docstring
  - `async resolve_branch_head_commit_with_bootstrap(oms_client, db_name, branch, source_branch, max_attempts, initial_backoff_seconds, max_backoff_seconds, warning_logger)` (line 76): no docstring

### `backend/bff/services/ontology_ops_service.py`
- **Functions**
  - `_localized_to_string(value)` (line 18): no docstring
  - `_transform_properties_for_oms(data, log_conversions)` (line 31): no docstring
  - `_normalize_mapping_type(type_value)` (line 78): no docstring
  - `_build_source_schema_from_preview(preview)` (line 106): no docstring
  - `_build_sample_data_from_preview(preview)` (line 143): no docstring
  - `_build_target_schema_from_ontology(ontology, include_relationships)` (line 156): no docstring
  - `_normalize_target_schema_for_mapping(target_schema, include_relationships)` (line 208): Normalize a client-provided target schema to the shape expected by MappingSuggestionService.
  - `_extract_target_field_types(ontology)` (line 244): no docstring
  - `_normalize_import_target_type(type_value)` (line 266): Normalize user-provided target field type for import.
  - `_extract_target_field_types_from_import_schema(target_schema)` (line 277): no docstring

### `backend/bff/services/ontology_relationships_service.py`
- **Functions**
  - `async create_ontology_with_relationship_validation(db_name, ontology, request, branch, auto_generate_inverse, validate_relationships, check_circular_references, mapper, terminus)` (line 43): Create ontology with advanced relationship validation (BFF).
  - `async validate_ontology_relationships(db_name, ontology, request, branch, mapper, terminus)` (line 121): Validate ontology relationships (no write).
  - `async check_circular_references(db_name, request, ontology, branch, mapper, terminus)` (line 156): Detect circular references in ontology graph (no write).
  - `async analyze_relationship_network(db_name, request, terminus)` (line 189): Analyze ontology relationship network and return user-friendly metrics.
  - `async find_relationship_paths(request, db_name, start_entity, end_entity, max_depth, path_type, terminus, mapper)` (line 278): Find relationship paths between ontology entities.

### `backend/bff/services/ontology_suggestions_service.py`
- **Functions**
  - `_load_mapping_config()` (line 40): no docstring
  - `_apply_semantic_hints(config, enabled)` (line 53): no docstring
  - `_format_mapping_suggestion(suggestion, source_schema, target_schema)` (line 61): no docstring
  - `async suggest_schema_from_data(db_name, body)` (line 91): 🔥 데이터에서 스키마 자동 제안
  - `async suggest_mappings(db_name, body)` (line 155): no docstring
  - `async suggest_mappings_from_google_sheets(db_name, body)` (line 192): no docstring
  - `async suggest_mappings_from_excel(db_name, target_class_id, file, target_schema_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, include_relationships, enable_semantic_hints, max_tables, max_rows, max_cols)` (line 268): no docstring
  - `async suggest_schema_from_google_sheets(db_name, body)` (line 372): no docstring
  - `async suggest_schema_from_excel(db_name, file, sheet_name, class_name, table_id, table_top, table_left, table_bottom, table_right, include_complex_types, max_tables, max_rows, max_cols)` (line 425): no docstring

### `backend/bff/services/pipeline_agent_autonomous_loop.py`
- **Functions**
  - `_tool_alias(tool_name)` (line 142): Convert a tool name into a short alias for intra-batch reference resolution.
  - `_resolve_ref_path(value, path)` (line 158): no docstring
  - `_drop_none_values(value)` (line 168): MCP tools validate inputs via JSON schema where `null` is frequently invalid for optional
  - `_resolve_batch_placeholders(value, last, last_by_alias)` (line 182): Resolve lightweight placeholders inside a *single batch* of tool calls.
  - `_build_agent_context_snapshot(state)` (line 264): Serialize key agent state for persistence across sessions (clarification resume).
  - `_extract_knowledge(tool_name, observation, ledger)` (line 314): Extract key structural facts from tool results into the knowledge ledger.
  - `_summarize_tool_output(tool_name, observation)` (line 388): Summarize large tool outputs before appending to prompt_items.
  - `_deduplicate_tool_call(state, tool_name, args, step_idx)` (line 465): Return the step index of a previous identical call, or None if unique.
  - `_dedup_evict_on_failure(state, tool_name, args)` (line 484): Remove a tool call from the dedup cache so the LLM can retry it.
  - `_check_knowledge_cache(ledger, tool_name, args)` (line 509): Return a synthetic cached observation if the knowledge ledger already has
  - `_calculate_trim_limit(total_count, default_limit, min_limit)` (line 668): Dynamically calculate trim limit based on data size.
  - `_trim_null_report(report)` (line 682): no docstring
  - `_trim_key_inference(value)` (line 713): no docstring
  - `_trim_type_inference(value)` (line 750): no docstring
  - `_trim_join_plan(value)` (line 788): no docstring
  - `_summarize_plan(plan_obj)` (line 800): no docstring
  - `_summarize_pipeline_progress(state)` (line 894): no docstring
  - `_record_pipeline_event(state, tool_name, args, observation)` (line 928): no docstring
  - `_pipeline_has_unresolved_status(state)` (line 1002): If pipeline execution was attempted, ensure we don't incorrectly report success while
  - `_plan_status(plan_obj)` (line 1025): no docstring
  - `_build_system_prompt(allowed_tools)` (line 1048): no docstring
  - `_prompt_text(items)` (line 1329): no docstring
  - `_build_prompt_header(state, answers, planner_hints, task_spec)` (line 1336): no docstring
  - `_summarize_ontology(ontology)` (line 1365): Create a summary of the working ontology for compaction.
  - `_build_compaction_snapshot(state, answers, planner_hints, task_spec)` (line 1387): no docstring
  - `_get_item_type(item_json)` (line 1424): Fast extraction of item type from serialized JSON without full parse.
  - `_reshrink_tool_output(item_json, max_chars)` (line 1435): Re-summarize a tool_output item to fit within max_chars.
  - `_progressive_compress_prompt_items(state, answers, planner_hints, task_spec, target_chars, preserve_recent_n)` (line 1458): Five-level graduated compression with importance scoring.
  - `_compute_prompt_hash(text)` (line 1570): Compute a short hash of prompt text for identification.
  - `async _log_pre_compression_state(run_id, prompt_items, compression_reason, audit_store, event_store)` (line 1576): Save pre-compression state for debugging.
  - `async _maybe_compact_prompt_items(state, answers, planner_hints, task_spec, max_chars, run_id, audit_store, event_store)` (line 1673): Improved compaction with progressive compression and pre-compression logging.
  - `_mask_tool_observation(payload)` (line 1730): no docstring
  - `_is_internal_budget_clarification(questions)` (line 1735): no docstring
  - `async _call_mcp_tool(mcp_manager, server, tool, arguments)` (line 1747): Call an MCP tool on *server* ("pipeline" or "ontology") and normalise the response.
  - `_build_llm_meta_dict(llm_meta)` (line 1815): Convert an LLMCallMeta to a plain dict (or None). Used in every return block.
  - `async _execute_tool_call(tool_name, args, state, mcp_manager, allowed_tools, planner_hints, tool_errors, tool_warnings, step_idx)` (line 1831): Execute a single MCP tool call and mutate *state*.
  - `async _run_agent_core(goal, data_scope, answers, planner_hints, task_spec, resume_plan_id, persist_plan, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, event_store, dataset_registry, pipeline_registry, plan_registry, streaming)` (line 2331): Single autonomous loop that serves both streaming and non-streaming callers.
  - `async run_pipeline_agent_mcp_autonomous(goal, data_scope, answers, planner_hints, task_spec, resume_plan_id, persist_plan, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, event_store, dataset_registry, pipeline_registry, plan_registry)` (line 3255): Non-streaming wrapper: collects the terminal event from the unified core loop.
  - `async run_pipeline_agent_streaming(goal, data_scope, answers, planner_hints, task_spec, resume_plan_id, persist_plan, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, event_store, dataset_registry, pipeline_registry, plan_registry)` (line 3298): SSE streaming wrapper: yields all events from the unified core loop.
- **Classes**
  - `AutonomousPipelineAgentToolCall` (line 49): no docstring
    - `_coerce_args(cls, v)` (line 55): no docstring
  - `AutonomousPipelineAgentDecision` (line 60): no docstring
    - `_coerce_args(cls, v)` (line 82): no docstring
    - `_coerce_tool_calls(cls, v)` (line 88): no docstring
    - `_coerce_questions(cls, v)` (line 94): no docstring
    - `_coerce_lists(cls, v)` (line 119): no docstring
    - `_validate_action(self)` (line 124): no docstring
  - `_AgentState` (line 227): no docstring
  - `StreamEvent` (line 2312): SSE event container. ``event_type`` maps to the SSE ``event:`` field.

### `backend/bff/services/pipeline_catalog_service.py`
- **Functions**
  - `async list_pipelines(db_name, branch, pipeline_registry, request)` (line 40): no docstring
  - `async create_pipeline(payload, audit_store, pipeline_registry, dataset_registry, event_store, request)` (line 67): no docstring

### `backend/bff/services/pipeline_cleansing_utils.py`
- **Functions**
  - `_transform_columns(transform)` (line 8): no docstring
  - `_columns_available(schema_by_node, node_id, columns)` (line 28): no docstring
  - `_build_outgoing(edges)` (line 45): no docstring
  - `_reachable_outputs(nodes, edges, output_nodes)` (line 56): no docstring
  - `_select_anchor_groups(nodes, edges, output_nodes, required_columns, schema_by_node)` (line 72): no docstring
  - `_insert_chain(source_id, target_ids, transforms, nodes, edges, existing_ids)` (line 111): no docstring
  - `_apply_transforms_to_outputs(output_ids, transforms, nodes, edges, existing_ids, warnings)` (line 138): no docstring
  - `apply_cleansing_transforms(definition_json, transforms, schema_by_node)` (line 176): no docstring

### `backend/bff/services/pipeline_dataset_media_upload_service.py`
- **Functions**
  - `async upload_media_dataset(db_name, branch, files, dataset_name, description, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, flush_dataset_ingest_outbox, build_dataset_event_payload)` (line 34): no docstring

### `backend/bff/services/pipeline_dataset_upload_context.py`
- **Functions**
  - `async _prepare_dataset_upload_context(request, db_name, branch, pipeline_registry)` (line 35): no docstring
  - `_build_dataset_upload_response(result, preview, source, funnel_analysis, schema_json)` (line 68): no docstring
- **Classes**
  - `_DatasetUploadContext` (line 26): no docstring

### `backend/bff/services/pipeline_dataset_upload_service.py`
- **Functions**
  - `async _save_artifact(lakefs_storage_service, repo, key, fileobj, content_type, metadata, checksum)` (line 65): no docstring
  - `async upload_tabular_dataset(inputs, lakefs_client, lakefs_storage_service, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, build_dataset_event_payload, flush_dataset_ingest_outbox)` (line 108): no docstring
- **Classes**
  - `TabularDatasetUploadInput` (line 33): no docstring
  - `TabularDatasetUploadResult` (line 57): no docstring

### `backend/bff/services/pipeline_dataset_version_service.py`
- **Functions**
  - `async create_dataset_version(dataset_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, flush_dataset_ingest_outbox, build_dataset_event_payload)` (line 34): no docstring

### `backend/bff/services/pipeline_detail_service.py`
- **Functions**
  - `async get_pipeline(pipeline_id, pipeline_registry, branch, preview_node_id, request)` (line 42): no docstring
  - `async get_pipeline_readiness(pipeline_id, branch, pipeline_registry, dataset_registry, request)` (line 96): no docstring
  - `async update_pipeline(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, request, event_store)` (line 277): no docstring

### `backend/bff/services/pipeline_execution_service.py`
- **Functions**
  - `_parse_optional_bool(value)` (line 60): no docstring
  - `_resolve_output_contract_from_definition(definition_json, node_id, output_name)` (line 73): no docstring
  - `async preview_pipeline(pipeline_id, payload, request, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, event_store)` (line 139): no docstring
  - `async build_pipeline(pipeline_id, payload, request, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, oms_client, emit_pipeline_control_plane_event)` (line 393): no docstring
  - `async deploy_pipeline(pipeline_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, oms_client, lineage_store, audit_store, emit_pipeline_control_plane_event, _acquire_pipeline_publish_lock, _release_pipeline_publish_lock)` (line 614): no docstring

### `backend/bff/services/pipeline_join_evaluator.py`
- **Functions**
  - `_count_matches(left, right, left_keys, right_keys)` (line 46): no docstring
  - `_coerce_table(payload)` (line 87): no docstring
  - `_coerce_tables(payload)` (line 106): no docstring
  - `_choose_join_inputs(inputs, tables, left_key, right_key, left_keys, right_keys)` (line 122): no docstring
  - `async evaluate_pipeline_joins(definition_json, db_name, dataset_registry, node_filter, run_tables, storage_service)` (line 176): no docstring
- **Classes**
  - `JoinEvaluation` (line 22): no docstring

### `backend/bff/services/pipeline_plan_autonomous_compiler.py`
- **Functions**
  - `_coerce_questions(raw)` (line 30): no docstring
  - `_coerce_llm_meta(raw)` (line 44): no docstring
  - `_coerce_planner_fields(payload)` (line 60): no docstring
  - `async compile_pipeline_plan_mcp_autonomous(goal, data_scope, answers, planner_hints, task_spec, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, dataset_registry, pipeline_registry, plan_registry)` (line 75): Compile a pipeline plan using the single autonomous loop runtime.

### `backend/bff/services/pipeline_plan_models.py`
- **Classes**
  - `PipelineClarificationQuestion` (line 13): no docstring
    - `_accept_key_as_id(cls, data)` (line 23): LLM sometimes returns ``key`` or ``name`` instead of ``id``,
  - `PipelinePlanCompileResult` (line 48): no docstring

### `backend/bff/services/pipeline_plan_preview_service.py`
- **Functions**
  - `async _try_get_lakefs_storage(pipeline_registry, request, purpose)` (line 47): no docstring
  - `async _validate_or_warn(plan, dataset_registry, pipeline_registry, db_name, branch, require_output)` (line 58): no docstring
  - `async preview_plan(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 88): no docstring
  - `async inspect_plan_preview(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 208): no docstring
  - `async evaluate_joins(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 275): no docstring

### `backend/bff/services/pipeline_plan_preview_utils.py`
- **Functions**
  - `_definition_has_join(definition_json)` (line 25): no docstring
  - `_definition_join_count(definition_json)` (line 42): no docstring
  - `_serialize_run_tables(run_tables, limit)` (line 60): no docstring
  - `_normalize_definition_for_digest(definition_json)` (line 79): no docstring
  - `_definition_digest(definition_json)` (line 122): no docstring
  - `_sanitize_label_dict_with_limits(payload)` (line 127): no docstring
  - `_sanitize_preview_columns(columns)` (line 137): no docstring
  - `_sanitize_preview_rows(rows)` (line 155): no docstring
  - `_sanitize_preview_table(table)` (line 171): no docstring
  - `_sanitize_preview_tables(payload)` (line 194): no docstring

### `backend/bff/services/pipeline_plan_scoping_service.py`
- **Functions**
  - `_parse_plan_id(plan_id)` (line 22): no docstring
  - `async _get_plan_record_or_404(plan_id, request, plan_registry)` (line 29): no docstring
  - `_load_plan_or_400(record)` (line 40): no docstring
  - `_extract_db_name_or_400(plan)` (line 47): no docstring
  - `async _load_scoped_plan(plan_id, request, plan_registry)` (line 65): no docstring
- **Classes**
  - `PipelinePlanRequestContext` (line 57): no docstring

### `backend/bff/services/pipeline_plan_tenant_service.py`
- **Functions**
  - `resolve_tenant_id(request)` (line 20): no docstring
  - `require_verified_user(request)` (line 32): no docstring
  - `resolve_verified_tenant_user(request)` (line 39): no docstring
  - `resolve_actor(request)` (line 46): no docstring
  - `async resolve_tenant_policy(request)` (line 51): no docstring

### `backend/bff/services/pipeline_plan_validation.py`
- **Functions**
  - `_is_acyclic(nodes, edges)` (line 40): no docstring
  - `async validate_pipeline_plan(plan, dataset_registry, pipeline_registry, db_name, branch, require_output, task_spec)` (line 46): no docstring
- **Classes**
  - `PipelinePlanValidationResult` (line 32): no docstring

### `backend/bff/services/pipeline_proposal_service.py`
- **Functions**
  - `normalize_mapping_spec_ids(raw)` (line 31): no docstring
  - `async _build_proposal_bundle(pipeline, build_job_id, mapping_spec_ids, pipeline_registry, dataset_registry, objectify_registry)` (line 58): no docstring
  - `async list_pipeline_proposals(db_name, branch, status_filter, pipeline_registry, request)` (line 224): no docstring
  - `async submit_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, objectify_registry, request)` (line 252): no docstring
  - `async approve_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 329): no docstring
  - `async reject_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 409): no docstring

### `backend/bff/services/pipeline_tabular_upload_facade.py`
- **Functions**
  - `async finalize_tabular_upload(ctx, dataset_name, description, source_type, source_ref, request_fingerprint_payload, schema_json, sample_json, row_count, source_metadata, artifact_fileobj, artifact_basename, artifact_content_type, content_sha256, commit_message, commit_metadata_extra, lineage_label, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, preview_payload, funnel_analysis, success_message)` (line 13): no docstring

### `backend/bff/services/pipeline_udf_service.py`
- **Functions**
  - `_parse_uuid_or_404(value, detail)` (line 20): no docstring
  - `async create_udf(db_name, name, code, description, pipeline_registry)` (line 28): no docstring
  - `async list_udfs(db_name, pipeline_registry)` (line 64): no docstring
  - `async get_udf(udf_id, pipeline_registry)` (line 97): no docstring
  - `async create_udf_version(udf_id, code, pipeline_registry)` (line 130): no docstring
  - `async get_udf_version(udf_id, version, pipeline_registry)` (line 164): no docstring

### `backend/bff/services/relationship_reconciler_service.py`
- **Functions**
  - `async reconcile_relationships(db_name, branch, oms_client, es_service, objectify_registry)` (line 36): Reconcile all FK-based relationships for a database.
  - `async _load_class_defs(oms_client, db_name, branch)` (line 167): Load all class definitions from OMS keyed by class_id.
  - `_collect_relationships(class_defs)` (line 206): Collect all relationship declarations from class definitions.
  - `async _detect_fk_field(source_class, target_class, index_name, es_service, objectify_registry, db_name)` (line 238): Detect the FK field in target class instances that references the source class PK.
  - `async _sample_instance(index_name, class_id, es_service)` (line 323): Get a single instance document for a class.
  - `async _get_class_instance_ids(index_name, class_id, es_service, limit)` (line 342): Get a sample of instance IDs for a class.
  - `async _scan_and_group_fk(index_name, target_class, fk_field, es_service)` (line 364): Scan all instances of target_class, grouping by their FK field value.
  - `async _bulk_update_relationships(index_name, source_class, predicate, target_class, fk_groups, es_service)` (line 419): Bulk update source class instances with relationship references.

### `backend/bff/services/schema_changes_service.py`
- **Functions**
  - `async list_schema_changes(pool, db_name, subject_type, subject_id, severity, since, limit, offset)` (line 23): no docstring
  - `async acknowledge_drift(pool, drift_id, acknowledged_by)` (line 105): no docstring
  - `async list_subscriptions(pool, user_id, db_name, status_filter, limit)` (line 138): no docstring
  - `async create_subscription(pool, user_id, subject_type, subject_id, db_name, severity_filter, notification_channels)` (line 203): no docstring
  - `async delete_subscription(pool, user_id, subscription_id)` (line 263): no docstring
  - `async check_mapping_compatibility(mapping_spec_id, db_name, dataset_version_id, dataset_registry, objectify_registry, detector)` (line 296): no docstring
  - `async get_schema_change_stats(pool, db_name, days)` (line 404): no docstring

### `backend/bff/services/sheet_import_parsing.py`
- **Functions**
  - `async read_excel_upload(file)` (line 18): no docstring
  - `parse_table_bbox(table_top, table_left, table_bottom, table_right)` (line 29): no docstring
  - `parse_json_array(value, field_name, required_message, treat_blank_as_missing, type_error_message)` (line 49): no docstring
  - `parse_json_object(value, field_name, default, treat_blank_as_missing, type_error_message)` (line 74): no docstring
  - `parse_target_schema_json(value)` (line 96): no docstring

### `backend/bff/services/sheet_import_service.py`

### `backend/bff/services/tasks_service.py`
- **Functions**
  - `_to_status_response(task)` (line 20): no docstring
  - `async get_task_status(task_id, task_manager)` (line 36): no docstring
  - `async list_tasks(status_filter, task_type, limit, task_manager)` (line 44): no docstring
  - `async cancel_task(task_id, task_manager)` (line 57): no docstring
  - `async get_task_metrics(task_manager)` (line 68): no docstring
  - `async get_task_result(task_id, task_manager)` (line 74): no docstring

### `backend/bff/services/websocket_service.py`
- **Functions**
  - `_is_valid_identifier(value)` (line 28): no docstring
  - `async _send_json(websocket, payload)` (line 36): no docstring
  - `async _send_error(websocket, message)` (line 40): no docstring
  - `async _send_connection_established(websocket, client_id, command_id, user_id)` (line 44): no docstring
  - `async handle_client_message(websocket, client_id, message, manager)` (line 73): Handle a message received from the client.
  - `async _run_session(websocket, client_id, user_id, token, manager, command_id)` (line 144): no docstring
  - `async run_command_updates(websocket, command_id, client_id, user_id, token, manager)` (line 201): no docstring
  - `async run_user_updates(websocket, user_id, client_id, token, manager)` (line 233): no docstring

### `backend/bff/tests/test_actions_submit_actor_metadata.py`
- **Functions**
  - `test_action_submit_forwards_actor_identity_from_headers()` (line 9): no docstring

### `backend/bff/tests/test_ai_query_router.py`
- **Functions**
  - `client()` (line 82): no docstring
  - `_install_common_overrides(llm_gateway)` (line 86): no docstring
  - `test_translate_query_plan_returns_plan(client)` (line 113): no docstring
  - `test_translate_query_plan_returns_graph_query_with_paths(client)` (line 136): no docstring
  - `test_ai_query_unsupported_returns_guidance_templates(client)` (line 172): no docstring
  - `test_ai_query_label_query_executes_and_answers(client)` (line 196): no docstring
  - `test_ai_query_dataset_list_executes_and_answers(client, monkeypatch)` (line 252): no docstring
  - `test_ai_intent_passes_through_llm_response(client)` (line 314): no docstring
  - `test_ai_intent_does_not_override_greeting_route(client)` (line 334): no docstring
- **Classes**
  - `_FakeOMSClient` (line 22): no docstring
    - `async list_ontologies(self, db_name)` (line 23): no docstring
  - `_FakeGateway` (line 41): no docstring
    - `__init__(self, plan, answer, intent)` (line 42): no docstring
    - `async complete_json(self, task, response_model, **kwargs)` (line 53): no docstring

### `backend/bff/tests/test_ci_webhooks_router.py`
- **Functions**
  - `async test_ci_webhook_ingest_records_ci_result()` (line 54): no docstring
- **Classes**
  - `_Request` (line 12): no docstring
    - `__init__(self)` (line 13): no docstring
  - `_FakeSessions` (line 18): no docstring
    - `__init__(self, session)` (line 19): no docstring
    - `async get_session(self, session_id, tenant_id)` (line 24): no docstring
    - `async record_ci_result(self, **kwargs)` (line 29): no docstring
    - `async append_event(self, **kwargs)` (line 48): no docstring

### `backend/bff/tests/test_command_status_router.py`
- **Functions**
  - `async test_command_status_proxies_to_api_v1_commands_status_path()` (line 22): no docstring
- **Classes**
  - `DummyOMSClient` (line 6): no docstring
    - `__init__(self)` (line 7): no docstring
    - `async get(self, path, **kwargs)` (line 10): no docstring

### `backend/bff/tests/test_conflict_converter_unit.py`
- **Classes**
  - `TestConflictConverter` (line 20): ConflictConverter 단위 테스트
    - `converter(self)` (line 24): ConflictConverter 인스턴스
    - `sample_terminus_conflict(self)` (line 29): 샘플 TerminusDB 충돌
    - `async test_namespace_splitting(self, converter)` (line 47): 네임스페이스 분리 테스트
    - `async test_path_type_determination(self, converter)` (line 66): 경로 타입 결정 테스트
    - `async test_human_readable_conversion(self, converter)` (line 86): 사람이 읽기 쉬운 형태 변환 테스트
    - `async test_jsonld_path_analysis(self, converter)` (line 104): JSON-LD 경로 분석 테스트
    - `async test_conflict_type_determination(self, converter)` (line 119): 충돌 타입 결정 테스트
    - `async test_severity_assessment(self, converter)` (line 147): 충돌 심각도 평가 테스트
    - `async test_auto_resolvability_assessment(self, converter)` (line 179): 자동 해결 가능성 평가 테스트
    - `async test_value_extraction_and_typing(self, converter)` (line 197): 값 추출 및 타입 지정 테스트
    - `async test_value_preview_generation(self, converter)` (line 214): 값 미리보기 생성 테스트
    - `async test_resolution_options_generation(self, converter)` (line 237): 해결 옵션 생성 테스트
    - `async test_complete_conflict_conversion(self, converter, sample_terminus_conflict)` (line 261): 완전한 충돌 변환 테스트
    - `async test_fallback_conflict_creation(self, converter)` (line 303): 폴백 충돌 생성 테스트
    - `async test_korean_property_mappings(self, converter)` (line 320): 한국어 속성 매핑 테스트
    - `async test_impact_analysis(self, converter)` (line 335): 영향 분석 테스트
    - `async test_multiple_conflicts_conversion(self, converter)` (line 358): 여러 충돌 변환 테스트

### `backend/bff/tests/test_context_tools_router.py`
- **Functions**
  - `_principal(tenant_id)` (line 18): no docstring
  - `async test_context_tools_dataset_describe_enforces_db_policy()` (line 75): no docstring
  - `async test_context_tools_dataset_describe_constrains_allowed_dataset_ids()` (line 91): no docstring
  - `async test_context_tools_ontology_snapshot_enforces_policy()` (line 109): no docstring
- **Classes**
  - `_Request` (line 13): no docstring
    - `__init__(self, principal)` (line 14): no docstring
  - `_FakePolicyRegistry` (line 22): no docstring
    - `__init__(self, data_policies)` (line 23): no docstring
    - `async get_tenant_policy(self, tenant_id)` (line 26): no docstring
  - `_FakeDatasetRegistry` (line 31): no docstring
    - `async list_datasets(self, db_name, branch)` (line 32): no docstring
  - `_FakeOMSClient` (line 65): no docstring
    - `__init__(self)` (line 66): no docstring
    - `async get(self, path, params)` (line 69): no docstring

### `backend/bff/tests/test_dataset_ingest_idempotency.py`
- **Functions**
  - `async _noop_flush_outbox(*args, **kwargs)` (line 264): no docstring
  - `_build_upload(content)` (line 268): no docstring
  - `async test_csv_upload_idempotency_key_reuses_version(monkeypatch)` (line 277): no docstring
- **Classes**
  - `_FakeLakeFSStorage` (line 16): no docstring
    - `async save_bytes(self, *args, **kwargs)` (line 17): no docstring
    - `async save_json(self, *args, **kwargs)` (line 20): no docstring
  - `_FakeLakeFSClient` (line 24): no docstring
    - `__init__(self)` (line 25): no docstring
    - `async commit(self, *args, **kwargs)` (line 28): no docstring
    - `async create_branch(self, *args, **kwargs)` (line 32): no docstring
  - `_FakePipelineRegistry` (line 36): no docstring
    - `__init__(self)` (line 37): no docstring
    - `async get_lakefs_storage(self, *args, **kwargs)` (line 41): no docstring
    - `async get_lakefs_client(self, *args, **kwargs)` (line 44): no docstring
  - `_Request` (line 49): no docstring
  - `_Dataset` (line 54): no docstring
  - `_IngestRequest` (line 63): no docstring
  - `_IngestTransaction` (line 84): no docstring
  - `_DatasetVersion` (line 94): no docstring
  - `_FakeDatasetRegistry` (line 105): no docstring
    - `__init__(self)` (line 106): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 113): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 116): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 137): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 174): no docstring
    - `async create_ingest_transaction(self, ingest_request_id)` (line 177): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 185): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 197): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 211): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, apply_schema, outbox_entries)` (line 214): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 251): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 257): no docstring

### `backend/bff/tests/test_document_bundles_router.py`
- **Functions**
  - `_principal(tenant_id)` (line 40): no docstring
  - `async test_document_bundle_search_enforces_allowed_bundle_ids()` (line 45): no docstring
  - `async test_document_bundle_search_returns_citations()` (line 62): no docstring
- **Classes**
  - `_Request` (line 12): no docstring
    - `__init__(self, principal)` (line 13): no docstring
  - `_FakePolicyRegistry` (line 17): no docstring
    - `__init__(self, allowed_bundle_ids)` (line 18): no docstring
    - `async get_tenant_policy(self, tenant_id)` (line 21): no docstring
  - `_FakeContext7` (line 28): no docstring
    - `__init__(self)` (line 29): no docstring
    - `async search(self, query, limit, filters)` (line 32): no docstring

### `backend/bff/tests/test_funnel_client_structure_selection.py`
- **Classes**
  - `TestFunnelClientStructureSelection` (line 4): no docstring
    - `test_select_primary_table_prefers_record_table_over_property(self)` (line 5): no docstring
    - `test_select_primary_table_prefers_higher_confidence(self)` (line 34): no docstring
    - `test_structure_table_to_preview_estimates_total_rows(self)` (line 64): no docstring
    - `test_select_requested_table_by_id(self)` (line 91): no docstring
    - `test_select_requested_table_by_bbox(self)` (line 102): no docstring
    - `test_select_requested_table_unknown_id_raises(self)` (line 117): no docstring

### `backend/bff/tests/test_i18n_language_selection.py`
- **Functions**
  - `test_bff_health_message_localizes_by_lang_param()` (line 11): no docstring
  - `test_bff_http_exception_detail_localizes_by_lang_param()` (line 30): no docstring

### `backend/bff/tests/test_import_commit_wiring.py`
- **Functions**
  - `test_google_sheets_import_commit_submits_to_oms(monkeypatch)` (line 27): no docstring
  - `test_excel_import_commit_submits_to_oms(monkeypatch)` (line 74): no docstring
- **Classes**
  - `_FakeFunnelClient` (line 10): no docstring
    - `async __aenter__(self)` (line 14): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 17): no docstring
    - `async google_sheets_to_structure_preview(self, **kwargs)` (line 20): no docstring
    - `async excel_to_structure_preview(self, **kwargs)` (line 23): no docstring

### `backend/bff/tests/test_instance_async_label_payload.py`
- **Functions**
  - `test_instance_create_allows_label_keys_with_spaces()` (line 22): no docstring
- **Classes**
  - `_FakeLabelMapper` (line 9): no docstring
    - `async get_class_id(self, db_name, label, lang)` (line 10): no docstring
    - `async get_property_id(self, db_name, class_id, label, lang)` (line 15): no docstring

### `backend/bff/tests/test_instances_access_policy.py`
- **Functions**
  - `_client_with_overrides(registry, es, oms)` (line 33): no docstring
  - `test_instances_list_masks_fields_with_access_policy()` (line 41): no docstring
  - `test_instance_get_hides_rows_blocked_by_access_policy()` (line 55): no docstring
- **Classes**
  - `_FakeElasticsearch` (line 10): no docstring
    - `__init__(self, hits)` (line 11): no docstring
    - `async search(self, index, query, size, from_, sort)` (line 14): no docstring
  - `_FakeDatasetRegistry` (line 18): no docstring
    - `__init__(self, policy)` (line 19): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 22): no docstring
  - `_FakeOMSClient` (line 28): no docstring
    - `async get_instance(self, db_name, instance_id, class_id)` (line 29): no docstring

### `backend/bff/tests/test_link_types_auto_join_table.py`
- **Functions**
  - `async test_ensure_join_dataset_auto_creates_dataset_and_version()` (line 79): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 9): no docstring
    - `__init__(self)` (line 10): no docstring
    - `async get_dataset(self, dataset_id)` (line 16): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 19): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 22): no docstring
    - `async get_version(self, version_id)` (line 47): no docstring
    - `async get_latest_version(self, dataset_id)` (line 53): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json)` (line 56): no docstring

### `backend/bff/tests/test_link_types_fk_validation.py`
- **Functions**
  - `async test_fk_type_mismatch_is_rejected()` (line 72): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 10): no docstring
    - `__init__(self, dataset, version)` (line 11): no docstring
    - `async get_dataset(self, dataset_id)` (line 35): no docstring
    - `async get_version(self, version_id)` (line 40): no docstring
    - `async get_latest_version(self, dataset_id)` (line 45): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 50): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 55): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 59): no docstring

### `backend/bff/tests/test_link_types_join_table_validation.py`
- **Functions**
  - `async test_join_table_missing_target_column_is_rejected()` (line 32): no docstring
  - `async test_join_table_source_type_mismatch_is_rejected()` (line 87): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 10): no docstring
    - `__init__(self, dataset, version)` (line 11): no docstring
    - `async get_dataset(self, dataset_id)` (line 15): no docstring
    - `async get_version(self, version_id)` (line 20): no docstring
    - `async get_latest_version(self, dataset_id)` (line 25): no docstring

### `backend/bff/tests/test_link_types_link_edits.py`
- **Functions**
  - `_post_link_edit(registry, payload)` (line 30): no docstring
  - `test_link_edit_rejected_when_disabled()` (line 42): no docstring
  - `test_link_edit_records_when_enabled()` (line 57): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 9): no docstring
    - `__init__(self, spec)` (line 10): no docstring
    - `async get_relationship_spec(self, link_type_id)` (line 14): no docstring
    - `async record_link_edit(self, **kwargs)` (line 25): no docstring

### `backend/bff/tests/test_link_types_retrieval.py`
- **Functions**
  - `async test_list_link_types_includes_relationship_spec_status()` (line 36): no docstring
  - `async test_get_link_type_includes_relationship_spec_status()` (line 58): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 9): no docstring
    - `__init__(self, spec)` (line 10): no docstring
    - `async get_relationship_spec(self, link_type_id)` (line 13): no docstring
  - `_FakeOMSClient` (line 19): no docstring
    - `__init__(self, resources)` (line 20): no docstring
    - `async list_ontology_resources(self, db_name, resource_type, branch)` (line 23): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 27): no docstring

### `backend/bff/tests/test_mapping_suggestion_service.py`
- **Functions**
  - `_pairs(suggestion)` (line 4): no docstring
  - `test_mapping_suggestion_is_deterministic()` (line 8): no docstring

### `backend/bff/tests/test_mapping_suggestion_service_domain_neutral.py`
- **Functions**
  - `test_semantic_match_disabled_by_default()` (line 4): no docstring
  - `test_semantic_match_is_opt_in_and_domain_neutral()` (line 9): no docstring
  - `test_label_is_used_for_matching_but_id_is_returned()` (line 24): no docstring

### `backend/bff/tests/test_merge_conflict_integration.py`
- **Classes**
  - `TestMergeConflictIntegration` (line 18): 병합 충돌 해결 통합 테스트
    - `client(self)` (line 22): FastAPI 테스트 클라이언트
    - `mock_oms_client(self)` (line 27): Mock OMS Client
    - `sample_conflict_data(self)` (line 34): 샘플 충돌 데이터
    - `expected_ui_conflict(self)` (line 66): 예상되는 UI/클라이언트 친화 충돌
    - `async test_merge_simulation_success(self, client, mock_oms_client, sample_conflict_data)` (line 95): 병합 시뮬레이션 성공 테스트
    - `async test_merge_simulation_with_conflicts(self, client, mock_oms_client, sample_conflict_data)` (line 164): 충돌이 있는 병합 시뮬레이션 테스트
    - `async test_conflict_resolution_success(self, client, mock_oms_client)` (line 234): 충돌 해결 성공 테스트
    - `async test_conflict_converter_integration(self)` (line 283): 충돌 변환기 통합 테스트
    - `async test_path_mapping_system(self)` (line 314): JSON-LD 경로 매핑 시스템 테스트
    - `async test_error_handling(self, client, mock_oms_client)` (line 342): 에러 처리 테스트
    - `async test_invalid_input_validation(self, client, mock_oms_client)` (line 365): 입력 검증 테스트
    - `async test_bff_dependencies_integration(self)` (line 386): BFF Dependencies 통합 테스트
    - `test_api_documentation_completeness(self, client)` (line 422): API 문서화 완성도 테스트
  - `TestFullStackMergeConflictFlow` (line 448): 전체 스택 병합 충돌 플로우 테스트
    - `async test_complete_conflict_resolution_workflow(self)` (line 452): 완전한 충돌 해결 워크플로우 테스트

### `backend/bff/tests/test_object_types_backing_retrieval.py`
- **Functions**
  - `async _noop_require_domain_role(request, db_name)` (line 57): no docstring
  - `async test_object_type_retrieval_includes_backing_datasource()` (line 63): no docstring
- **Classes**
  - `_FakeOMSClient` (line 9): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 10): no docstring
  - `_FakeDatasetRegistry` (line 22): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 23): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 38): no docstring
  - `_FakeObjectifyRegistry` (line 51): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 52): no docstring

### `backend/bff/tests/test_object_types_edit_migration.py`
- **Functions**
  - `_base_spec(status)` (line 85): no docstring
  - `async test_edit_policy_moves_drops_invalidates_are_applied_and_recorded()` (line 95): no docstring
  - `async test_pk_change_with_id_remap_records_plan()` (line 138): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 9): no docstring
    - `__init__(self, edit_count, edit_impact)` (line 10): no docstring
    - `async record_gate_result(self, **kwargs)` (line 20): no docstring
    - `async create_schema_migration_plan(self, **kwargs)` (line 23): no docstring
    - `async count_instance_edits(self, **kwargs)` (line 26): no docstring
    - `async get_instance_edit_field_stats(self, **kwargs)` (line 29): no docstring
    - `async apply_instance_edit_field_moves(self, **kwargs)` (line 32): no docstring
    - `async update_instance_edit_status_by_fields(self, **kwargs)` (line 36): no docstring
    - `async remap_instance_edits(self, **kwargs)` (line 44): no docstring
    - `async clear_instance_edits(self, **kwargs)` (line 48): no docstring
  - `_FakeOMSClient` (line 53): no docstring
    - `__init__(self, existing_spec, properties)` (line 54): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 58): no docstring
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 62): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 74): no docstring
  - `_FakeObjectifyRegistry` (line 79): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 80): no docstring

### `backend/bff/tests/test_object_types_key_spec_required.py`
- **Functions**
  - `_build_registry()` (line 81): no docstring
  - `async test_object_type_requires_primary_key()` (line 127): no docstring
  - `async test_object_type_requires_title_key()` (line 165): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 10): no docstring
    - `__init__(self, dataset, version, backing, backing_version)` (line 11): no docstring
    - `async get_dataset(self, dataset_id)` (line 17): no docstring
    - `async get_latest_version(self, dataset_id)` (line 22): no docstring
    - `async get_version(self, version_id)` (line 27): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 32): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 37): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 42): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 46): no docstring
  - `_FakeOMSClient` (line 58): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 59): no docstring
    - `async create_ontology_resource(self, db_name, resource_type, payload, branch, expected_head_commit)` (line 63): no docstring
  - `_FakeObjectifyRegistry` (line 75): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 76): no docstring

### `backend/bff/tests/test_object_types_migration_gate.py`
- **Functions**
  - `_build_context(status_value)` (line 103): no docstring
  - `async test_object_type_migration_requires_approval()` (line 143): no docstring
  - `async test_object_type_migration_plan_is_recorded()` (line 171): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 10): no docstring
    - `__init__(self, dataset, backing, backing_version, version)` (line 11): no docstring
    - `async get_dataset(self, dataset_id)` (line 19): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 24): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 29): no docstring
    - `async get_latest_version(self, dataset_id)` (line 34): no docstring
    - `async get_version(self, version_id)` (line 39): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 44): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 48): no docstring
    - `async record_gate_result(self, **kwargs)` (line 59): no docstring
    - `async create_schema_migration_plan(self, **kwargs)` (line 62): no docstring
    - `async count_instance_edits(self, **kwargs)` (line 65): no docstring
    - `async get_instance_edit_field_stats(self, **kwargs)` (line 68): no docstring
  - `_FakeOMSClient` (line 72): no docstring
    - `__init__(self, existing_spec)` (line 73): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 76): no docstring
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 80): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 92): no docstring
  - `_FakeObjectifyRegistry` (line 97): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 98): no docstring

### `backend/bff/tests/test_object_types_swap_reindex.py`
- **Functions**
  - `async test_object_type_swap_enqueues_reindex()` (line 105): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 9): no docstring
    - `__init__(self, dataset, backing, backing_version, version)` (line 10): no docstring
    - `async get_dataset(self, dataset_id)` (line 16): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 21): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 26): no docstring
    - `async get_latest_version(self, dataset_id)` (line 31): no docstring
    - `async get_version(self, version_id)` (line 36): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 41): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 45): no docstring
    - `async record_gate_result(self, **kwargs)` (line 49): no docstring
    - `async create_schema_migration_plan(self, **kwargs)` (line 52): no docstring
  - `_FakeOMSClient` (line 56): no docstring
    - `__init__(self, existing_spec)` (line 57): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 60): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 64): no docstring
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 68): no docstring
  - `_FakeObjectifyRegistry` (line 73): no docstring
    - `__init__(self, mapping_spec)` (line 74): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 78): no docstring
    - `build_dedupe_key(self, dataset_id, dataset_branch, mapping_spec_id, mapping_spec_version, dataset_version_id, artifact_id, artifact_output_name)` (line 83): no docstring
    - `async get_objectify_job_by_dedupe_key(self, dedupe_key)` (line 96): no docstring
    - `async enqueue_objectify_job(self, job)` (line 100): no docstring

### `backend/bff/tests/test_objectify_mapping_spec_preflight.py`
- **Functions**
  - `_make_dataset(schema_columns, schema_types)` (line 133): no docstring
  - `_make_latest(schema_columns, schema_types)` (line 151): no docstring
  - `_build_object_type_resource(ontology_payload)` (line 162): no docstring
  - `_post_mapping_spec(body, schema_columns, ontology_payload, key_spec, object_type_resource, schema_types, objectify_registry)` (line 187): no docstring
  - `_base_payload(mappings, target_field_types, options)` (line 225): no docstring
  - `test_mapping_spec_source_missing_is_rejected()` (line 239): no docstring
  - `test_mapping_spec_target_unknown_is_rejected()` (line 254): no docstring
  - `test_mapping_spec_relationship_target_is_rejected()` (line 269): no docstring
  - `test_mapping_spec_dataset_pk_target_mismatch_is_rejected()` (line 287): no docstring
  - `test_mapping_spec_required_missing_is_rejected()` (line 316): no docstring
  - `test_mapping_spec_primary_key_missing_is_rejected()` (line 337): no docstring
  - `test_mapping_spec_unsupported_type_is_rejected()` (line 358): no docstring
  - `test_mapping_spec_target_type_mismatch_is_rejected()` (line 384): no docstring
  - `test_mapping_spec_source_type_incompatible_is_rejected()` (line 403): no docstring
  - `test_mapping_spec_source_type_unsupported_is_rejected()` (line 433): no docstring
  - `test_mapping_spec_change_summary_is_recorded()` (line 456): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 12): no docstring
    - `__init__(self, dataset, latest_version, key_spec)` (line 13): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_latest_version(self, dataset_id)` (line 35): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 38): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 43): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 48): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 51): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 73): no docstring
    - `async record_gate_result(self, **kwargs)` (line 76): no docstring
  - `_FakeObjectifyRegistry` (line 80): no docstring
    - `async get_active_mapping_spec(self, **kwargs)` (line 81): no docstring
    - `async create_mapping_spec(self, **kwargs)` (line 84): no docstring
  - `_CapturingObjectifyRegistry` (line 88): no docstring
    - `__init__(self, active_spec)` (line 89): no docstring
    - `async get_active_mapping_spec(self, **kwargs)` (line 93): no docstring
    - `async create_mapping_spec(self, **kwargs)` (line 96): no docstring
  - `_FakeOMSClient` (line 119): no docstring
    - `__init__(self, payload, object_type_resource)` (line 120): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 124): no docstring
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 127): no docstring

### `backend/bff/tests/test_oms_client_http_helpers.py`
- **Functions**
  - `async test_oms_client_http_helpers_roundtrip_json()` (line 8): no docstring

### `backend/bff/tests/test_ontology_router_helpers.py`
- **Functions**
  - `test_localized_to_string()` (line 6): no docstring
  - `test_transform_properties_for_oms()` (line 12): no docstring
  - `test_build_source_schema_and_samples()` (line 30): no docstring
  - `test_normalize_mapping_type_and_import_target()` (line 48): no docstring

### `backend/bff/tests/test_ontology_validate_proxy.py`
- **Functions**
  - `test_ontology_validate_create_proxies_to_oms()` (line 19): no docstring
  - `test_ontology_validate_update_resolves_label_and_proxies_to_oms()` (line 47): no docstring
- **Classes**
  - `_FakeLabelMapper` (line 9): no docstring
    - `async get_class_id(self, db_name, label, lang)` (line 10): no docstring
    - `async update_mappings(self, db_name, update_data)` (line 15): no docstring

### `backend/bff/tests/test_pipeline_audit_logging.py`
- **Functions**
  - `async test_pipeline_update_writes_audit_log(monkeypatch)` (line 77): no docstring
- **Classes**
  - `_Request` (line 15): no docstring
  - `_Pipeline` (line 20): no docstring
  - `_Version` (line 28): no docstring
  - `_PipelineRegistry` (line 32): no docstring
    - `__init__(self, pipeline)` (line 33): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 36): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 39): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 42): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 47): no docstring
    - `async get_pipeline_branch(self, db_name, branch)` (line 50): no docstring
    - `async update_pipeline(self, **kwargs)` (line 53): no docstring
    - `async list_dependencies(self, pipeline_id)` (line 56): no docstring
  - `_EventStore` (line 60): no docstring
    - `async connect(self)` (line 61): no docstring
    - `async append_event(self, event)` (line 64): no docstring
  - `_AuditStore` (line 68): no docstring
    - `__init__(self)` (line 69): no docstring
    - `async log(self, **kwargs)` (line 72): no docstring

### `backend/bff/tests/test_pipeline_dataset_version_materialization.py`
- **Functions**
  - `async test_create_dataset_version_materializes_manual_sample_to_artifact(monkeypatch)` (line 105): no docstring
- **Classes**
  - `_Request` (line 13): no docstring
  - `_Dataset` (line 18): no docstring
  - `_DatasetRegistry` (line 26): no docstring
    - `__init__(self, dataset)` (line 27): no docstring
    - `async get_dataset(self, dataset_id)` (line 34): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, promoted_from_artifact_id)` (line 39): no docstring
  - `_LakeFSStorageService` (line 58): no docstring
    - `__init__(self)` (line 59): no docstring
    - `async save_json(self, bucket, key, data, metadata)` (line 62): no docstring
  - `_PipelineRegistry` (line 79): no docstring
    - `__init__(self, storage, client)` (line 80): no docstring
    - `async get_lakefs_storage(self, user_id)` (line 84): no docstring
    - `async get_lakefs_client(self, user_id)` (line 87): no docstring
  - `_LineageStore` (line 91): no docstring
    - `node_event(event_id)` (line 93): no docstring
    - `node_artifact(kind, *parts)` (line 97): no docstring
    - `async record_link(self, **kwargs)` (line 100): no docstring

### `backend/bff/tests/test_pipeline_ontology_gate.py`
- **Functions**
  - `async test_promote_build_rejects_missing_ontology_commit()` (line 113): no docstring
  - `async test_promote_build_rejects_ontology_commit_mismatch()` (line 146): no docstring
  - `async test_promote_build_returns_503_when_ontology_gate_unavailable()` (line 182): no docstring
- **Classes**
  - `_Request` (line 15): no docstring
  - `_Pipeline` (line 20): no docstring
  - `_PipelineRegistry` (line 27): no docstring
    - `__init__(self, pipeline, build_run)` (line 28): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 32): no docstring
    - `async get_run(self, pipeline_id, job_id)` (line 37): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 42): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 45): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 48): no docstring
    - `async get_artifact_by_job(self, pipeline_id, job_id, mode)` (line 55): no docstring
    - `async record_promotion_manifest(self, **kwargs)` (line 58): no docstring
  - `_PipelineJobQueue` (line 62): no docstring
    - `async publish(self, job)` (line 63): no docstring
  - `_DatasetRegistry` (line 67): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 68): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 71): no docstring
    - `async create_key_spec(self, **kwargs)` (line 74): no docstring
  - `_ObjectifyRegistry` (line 78): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 79): no docstring
  - `_AuditStore` (line 91): no docstring
    - `__init__(self)` (line 92): no docstring
    - `async log(self, **kwargs)` (line 95): no docstring
  - `_OMSClient` (line 99): no docstring
    - `__init__(self, head_commit_id)` (line 100): no docstring
    - `async get_version_head(self, db_name, branch)` (line 103): no docstring
  - `_FailingOMSClient` (line 107): no docstring
    - `async get_version_head(self, db_name, branch)` (line 108): no docstring

### `backend/bff/tests/test_pipeline_permissions_enforced.py`
- **Functions**
  - `async test_get_pipeline_requires_read_permission()` (line 73): no docstring
  - `async test_get_pipeline_bootstraps_permissions_when_missing()` (line 89): no docstring
- **Classes**
  - `_Request` (line 15): no docstring
  - `_Pipeline` (line 20): no docstring
  - `_Version` (line 28): no docstring
  - `_PipelineRegistryDenied` (line 34): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 35): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 38): no docstring
  - `_PipelineRegistryBootstrap` (line 42): no docstring
    - `__init__(self)` (line 43): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 47): no docstring
    - `async grant_permission(self, pipeline_id, principal_type, principal_id, role)` (line 50): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 60): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 65): no docstring
    - `async list_dependencies(self, pipeline_id)` (line 68): no docstring

### `backend/bff/tests/test_pipeline_promotion_semantics.py`
- **Functions**
  - `async _noop_publish_lock(*args, **kwargs)` (line 46): no docstring
  - `async _noop_emit_event(*args, **kwargs)` (line 50): no docstring
  - `lakefs_merge_stub(monkeypatch)` (line 278): no docstring
  - `async test_build_enqueues_job_and_records_run()` (line 283): no docstring
  - `async test_preview_enqueues_job_with_node_id_and_records_preview_and_run(monkeypatch)` (line 326): no docstring
  - `async test_promote_build_merges_build_branch_to_main_and_registers_version(lakefs_merge_stub, monkeypatch)` (line 382): no docstring
  - `async test_promote_build_rejects_non_staged_artifact_key()` (line 454): no docstring
  - `async test_promote_build_surfaces_build_errors_when_build_failed()` (line 506): no docstring
  - `async test_promote_build_blocks_deploy_when_expectations_failed()` (line 544): no docstring
  - `async test_promote_build_requires_replay_for_breaking_schema_changes(lakefs_merge_stub)` (line 581): no docstring
  - `async test_promote_build_allows_breaking_schema_changes_with_replay_flag(lakefs_merge_stub, monkeypatch)` (line 644): no docstring
- **Classes**
  - `_Request` (line 16): no docstring
  - `_Pipeline` (line 21): no docstring
  - `_Dataset` (line 30): no docstring
  - `_DatasetVersion` (line 39): no docstring
  - `_PipelineRegistry` (line 54): no docstring
    - `__init__(self, pipeline, build_run, lakefs_merge_calls)` (line 55): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 72): no docstring
    - `async get_pipeline_branch(self, db_name, branch)` (line 77): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 80): no docstring
    - `async get_run(self, pipeline_id, job_id)` (line 87): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 92): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 95): no docstring
    - `async record_run(self, pipeline_id, job_id, mode, status, output_json, sample_json, finished_at, **kwargs)` (line 98): no docstring
    - `async record_build(self, pipeline_id, status, output_json, deployed_commit_id, **kwargs)` (line 122): no docstring
    - `async record_preview(self, **kwargs)` (line 140): no docstring
    - `async replace_dependencies(self, **kwargs)` (line 144): no docstring
    - `async update_pipeline(self, **kwargs)` (line 147): no docstring
    - `async get_lakefs_client(self, user_id)` (line 150): no docstring
    - `async get_artifact_by_job(self, pipeline_id, job_id, mode)` (line 160): no docstring
    - `async record_promotion_manifest(self, **kwargs)` (line 163): no docstring
  - `_PipelineJobQueue` (line 168): no docstring
    - `__init__(self)` (line 169): no docstring
    - `async publish(self, job, **kwargs)` (line 172): no docstring
  - `_DatasetRegistry` (line 177): no docstring
    - `__init__(self)` (line 178): no docstring
    - `async get_dataset(self, dataset_id)` (line 182): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 188): no docstring
    - `async get_latest_version(self, dataset_id)` (line 191): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 202): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, promoted_from_artifact_id)` (line 213): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 241): no docstring
    - `async create_key_spec(self, **kwargs)` (line 244): no docstring
  - `_ObjectifyRegistry` (line 248): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 249): no docstring
  - `_OMSClient` (line 261): no docstring
    - `__init__(self, head_commit_id)` (line 262): no docstring
    - `async get_version_head(self, db_name, branch)` (line 265): no docstring
  - `_AuditStore` (line 269): no docstring
    - `__init__(self)` (line 270): no docstring
    - `async log(self, **kwargs)` (line 273): no docstring

### `backend/bff/tests/test_pipeline_proposal_governance.py`
- **Functions**
  - `async test_pipeline_proposal_submit_and_approve_flow()` (line 171): no docstring
  - `async test_pipeline_proposal_requires_approve_role()` (line 232): no docstring
  - `async test_pipeline_proposal_requires_pending_status()` (line 270): no docstring
- **Classes**
  - `_Request` (line 14): no docstring
  - `_ProposalRecord` (line 19): no docstring
  - `_PipelineRecord` (line 28): no docstring
  - `_FakeAuditStore` (line 43): no docstring
    - `__init__(self)` (line 44): no docstring
    - `async log(self, **kwargs)` (line 47): no docstring
  - `_FakePipelineRegistry` (line 51): no docstring
    - `__init__(self)` (line 52): no docstring
    - `async create_pipeline(self, db_name, name, description, pipeline_type, location, status, branch, proposal_status, proposal_title)` (line 56): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 85): no docstring
    - `async grant_permission(self, pipeline_id, principal_type, principal_id, role)` (line 88): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 98): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 101): no docstring
    - `async submit_proposal(self, pipeline_id, title, description, proposal_bundle)` (line 115): no docstring
    - `async review_proposal(self, pipeline_id, status, review_comment)` (line 136): no docstring
    - `async merge_branch(self, pipeline_id, from_branch, to_branch)` (line 156): no docstring
  - `_FakeDatasetRegistry` (line 162): no docstring
  - `_FakeObjectifyRegistry` (line 166): no docstring

### `backend/bff/tests/test_pipeline_router_helpers.py`
- **Functions**
  - `test_pipeline_protected_branches(monkeypatch)` (line 12): no docstring
  - `test_normalize_mapping_spec_ids()` (line 20): no docstring
  - `test_schema_change_detection()` (line 26): no docstring
  - `test_dependency_payload_normalization()` (line 33): no docstring
  - `test_format_dependencies_for_api()` (line 42): no docstring
  - `test_resolve_principal_and_actor_label()` (line 48): no docstring
  - `test_location_and_dataset_name_helpers()` (line 59): no docstring
  - `test_definition_diff_and_bbox()` (line 65): no docstring
  - `test_csv_helpers()` (line 78): no docstring
  - `test_idempotency_key_required()` (line 88): no docstring

### `backend/bff/tests/test_pipeline_router_uploads.py`
- **Functions**
  - `_build_request(headers)` (line 359): no docstring
  - `test_pipeline_helpers_normalize_inputs(monkeypatch)` (line 369): no docstring
  - `async test_upload_csv_dataset_creates_version(monkeypatch)` (line 398): no docstring
  - `async test_upload_csv_dataset_funnel_failure_uses_fallback(monkeypatch)` (line 444): no docstring
  - `async test_upload_excel_dataset_commits_preview(monkeypatch)` (line 496): no docstring
  - `async test_approve_dataset_schema_updates_dataset()` (line 544): no docstring
  - `async test_get_ingest_request_includes_funnel_analysis(monkeypatch)` (line 582): no docstring
  - `async test_get_ingest_request_funnel_failure_uses_fallback(monkeypatch)` (line 622): no docstring
  - `async test_reanalyze_dataset_version_returns_funnel_analysis(monkeypatch)` (line 668): no docstring
  - `async test_upload_media_dataset_stores_files(monkeypatch)` (line 706): no docstring
  - `async test_maybe_enqueue_objectify_job()` (line 746): no docstring
- **Classes**
  - `_FakeLakeFSStorage` (line 19): no docstring
    - `__init__(self)` (line 20): no docstring
    - `async save_fileobj(self, repo, key, fileobj, content_type, metadata, checksum)` (line 23): no docstring
    - `async save_bytes(self, repo, key, content, content_type, metadata)` (line 36): no docstring
  - `_FakeLakeFSClient` (line 48): no docstring
    - `__init__(self)` (line 49): no docstring
    - `async commit(self, repository, branch, message, metadata)` (line 53): no docstring
    - `async create_branch(self, repository, name, source)` (line 66): no docstring
  - `_FakePipelineRegistry` (line 70): no docstring
    - `__init__(self)` (line 71): no docstring
    - `async get_lakefs_storage(self, user_id)` (line 75): no docstring
    - `async get_lakefs_client(self, user_id)` (line 78): no docstring
  - `_FakeDatasetRegistry` (line 82): no docstring
    - `__init__(self, ingest_status)` (line 83): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 91): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 94): no docstring
    - `async get_dataset(self, dataset_id)` (line 112): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 115): no docstring
    - `async get_ingest_request(self, ingest_request_id)` (line 154): no docstring
    - `async approve_ingest_schema(self, ingest_request_id, schema_json, approved_by)` (line 157): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 182): no docstring
    - `async get_version(self, version_id)` (line 185): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 191): no docstring
    - `async create_ingest_transaction(self, ingest_request_id)` (line 194): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 211): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 217): no docstring
    - `async update_ingest_request_payload(self, ingest_request_id, sample_json, row_count)` (line 223): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, apply_schema, outbox_entries)` (line 229): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, version_id, ingest_request_id, promoted_from_artifact_id)` (line 265): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 298): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 303): no docstring
  - `_FakeFunnelClient` (line 309): no docstring
    - `async __aenter__(self)` (line 310): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 313): no docstring
    - `async analyze_dataset(self, payload, timeout_seconds)` (line 316): no docstring
    - `async excel_to_structure_preview_stream(self, *args, **kwargs)` (line 337): no docstring
  - `_FailingFunnelClient` (line 348): no docstring
    - `async __aenter__(self)` (line 349): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 352): no docstring
    - `async analyze_dataset(self, payload, timeout_seconds)` (line 355): no docstring

### `backend/bff/tests/test_security_information_leakage.py`
- **Classes**
  - `_StubDatasetRegistry` (line 19): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 20): no docstring
  - `TestInformationLeakagePrevention` (line 24): no docstring
    - `client(self)` (line 26): no docstring
    - `async test_get_instance_es_success_no_source_leak(self, client)` (line 30): no docstring
    - `async test_get_instance_es_failure_fails_closed_without_source_leak(self, client)` (line 66): no docstring
    - `async test_get_class_instances_es_success_no_source_leak(self, client)` (line 86): no docstring
    - `async test_get_class_instances_es_failure_fails_closed_without_source_leak(self, client)` (line 117): no docstring
    - `async test_not_found_response_no_internal_info_leak(self, client)` (line 138): no docstring
    - `test_response_structure_consistency(self)` (line 158): no docstring
    - `test_forbidden_fields_in_responses(self)` (line 169): no docstring

### `backend/bff/tests/test_sheet_import_service.py`
- **Functions**
  - `test_coerce_integer_with_currency_suffix()` (line 4): no docstring
  - `test_coerce_decimal_with_currency_symbol()` (line 15): no docstring
  - `test_coerce_date_accepts_common_separators()` (line 26): no docstring
  - `test_boolean_parsing()` (line 37): no docstring
  - `test_error_rows_are_reported_and_can_be_filtered()` (line 48): no docstring

### `backend/bff/utils/__init__.py`

### `backend/bff/utils/action_log_serialization.py`
- **Functions**
  - `dt_iso(value)` (line 16): no docstring
  - `serialize_action_log_record(record)` (line 26): no docstring

### `backend/bff/utils/conflict_converter.py`
- **Classes**
  - `ConflictSeverity` (line 16): 충돌 심각도
  - `PathType` (line 25): JSON-LD 경로 타입
  - `JsonLdPath` (line 37): JSON-LD 경로 분석 결과
  - `ConflictAnalysis` (line 49): 충돌 분석 결과
  - `ConflictConverter` (line 59): TerminusDB 충돌을 UI/클라이언트 친화적 포맷으로 변환하는 클래스
    - `__init__(self)` (line 62): no docstring
    - `async convert_conflicts_to_ui_format(self, terminus_conflicts, db_name, source_branch, target_branch)` (line 88): TerminusDB 충돌을 UI/클라이언트 친화적 포맷으로 변환
    - `async _convert_single_conflict(self, conflict, conflict_id, db_name, source_branch, target_branch)` (line 124): 단일 충돌을 UI 포맷으로 변환
    - `async _analyze_jsonld_path(self, path)` (line 199): JSON-LD 경로 분석
    - `_split_namespace_and_property(self, path)` (line 240): 네임스페이스와 속성명 분리
    - `_determine_path_type(self, full_path, property_name)` (line 266): 경로 타입 결정
    - `_convert_to_human_readable(self, namespace, property_name)` (line 288): 사람이 읽기 쉬운 형태로 변환
    - `async _analyze_conflict(self, conflict, path_info)` (line 312): 충돌 분석 수행
    - `_determine_conflict_type(self, source_change, target_change, path_info)` (line 349): 충돌 타입 결정
    - `_assess_severity(self, conflict_type, path_info, source_change, target_change)` (line 370): 충돌 심각도 평가
    - `_assess_auto_resolvability(self, conflict_type, source_change, target_change)` (line 394): 자동 해결 가능성 평가
    - `_suggest_resolution_strategy(self, conflict_type, source_change, target_change, auto_resolvable)` (line 412): 해결 방법 제안
    - `_analyze_impact(self, conflict_type, path_info, source_change, target_change)` (line 433): 영향 분석
    - `_extract_value_and_type(self, change)` (line 453): 변경사항에서 값과 타입 추출
    - `_generate_value_preview(self, value, value_type)` (line 471): 값 미리보기 생성
    - `_generate_conflict_description(self, path_info, source_change, target_change)` (line 483): 충돌 설명 생성
    - `_generate_resolution_options(self, source_value, target_value, analysis)` (line 494): 해결 옵션 생성
    - `_create_fallback_conflict(self, conflict, conflict_id)` (line 530): 변환 실패 시 기본 충돌 정보 생성
    - `_get_current_timestamp(self)` (line 561): 현재 타임스탬프 반환

### `backend/bff/utils/httpx_exceptions.py`
- **Functions**
  - `extract_httpx_detail(exc)` (line 17): no docstring
  - `raise_httpx_as_http_exception(exc)` (line 33): no docstring

### `backend/bff/utils/request_headers.py`
- **Functions**
  - `extract_forward_headers(request, keys)` (line 21): no docstring

### `backend/bff/verify_implementation.py`
- **Functions**
  - `test_core_conflict_system()` (line 11): 핵심 충돌 시스템 검증 (UI/클라이언트 친화 포맷)
  - `test_real_world_scenario()` (line 272): 실제 시나리오 시뮬레이션

## conftest.py

### `backend/conftest.py`
- **Functions**
  - `_ensure_test_env()` (line 9): no docstring

## connector_sync_worker

### `backend/connector_sync_worker/__init__.py`

### `backend/connector_sync_worker/main.py`
- **Classes**
  - `ConnectorSyncWorker` (line 56): no docstring
    - `__init__(self)` (line 57): no docstring
    - `async initialize(self)` (line 94): no docstring
    - `async close(self)` (line 151): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 177): no docstring
    - `async _process_payload(self, payload)` (line 198): no docstring
    - `_span_name(self, payload)` (line 201): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 204): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 207): no docstring
    - `_should_seek_on_in_progress(self, claim, payload)` (line 210): no docstring
    - `_should_seek_on_retry(self, attempt_count, payload)` (line 213): no docstring
    - `_should_mark_done_after_dlq(self, payload, error)` (line 216): no docstring
    - `async _on_success(self, payload, result, duration_s)` (line 219): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 232): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 254): no docstring
    - `_bff_scope_headers(self, db_name)` (line 276): no docstring
    - `async _fetch_ontology_schema(self, db_name, class_label, branch)` (line 282): no docstring
    - `async _target_field_types(self, db_name, class_label, branch)` (line 295): no docstring
    - `async _process_google_sheets_update(self, envelope)` (line 312): no docstring
    - `async _handle_envelope(self, envelope)` (line 505): no docstring
    - `async run(self)` (line 515): no docstring

## connector_trigger_service

### `backend/connector_trigger_service/__init__.py`

### `backend/connector_trigger_service/main.py`
- **Classes**
  - `ConnectorTriggerService` (line 45): no docstring
    - `__init__(self)` (line 46): no docstring
    - `async initialize(self)` (line 64): no docstring
    - `async close(self)` (line 94): no docstring
    - `_get_producer_ops(self)` (line 122): no docstring
    - `async _is_due(self, source)` (line 133): no docstring
    - `async _poll_google_sheets(self, source)` (line 144): no docstring
    - `async _poll_source(self, source, sem)` (line 210): no docstring
    - `async _poll_loop(self)` (line 225): no docstring
    - `async _publish_outbox_loop(self)` (line 244): no docstring
    - `async run(self)` (line 336): no docstring

## data_connector

### `backend/data_connector/__init__.py`

### `backend/data_connector/google_sheets/__init__.py`

### `backend/data_connector/google_sheets/auth.py`
- **Classes**
  - `GoogleOAuth2Client` (line 17): Google OAuth2 인증 클라이언트 (향후 확장용)
    - `__init__(self, client_id, client_secret, redirect_uri)` (line 36): 초기화
    - `get_authorization_url(self, state)` (line 58): OAuth2 인증 URL 생성
    - `async exchange_code_for_token(self, code)` (line 81): Authorization code를 access token으로 교환
    - `async refresh_access_token(self, refresh_token)` (line 113): Refresh token으로 새 access token 획득
    - `async revoke_token(self, token)` (line 148): Token 취소
    - `store_user_token(self, user_id, token_data)` (line 164): 사용자 토큰 저장
    - `get_user_token(self, user_id)` (line 175): 사용자 토큰 조회
    - `async get_valid_access_token(self, user_id)` (line 187): 유효한 access token 조회 (필요시 refresh)
    - `remove_user_token(self, user_id)` (line 222): 사용자 토큰 삭제
  - `APIKeyAuth` (line 239): API Key 기반 인증 (현재 사용 중)
    - `__init__(self, api_key)` (line 244): 초기화
    - `get_auth_params(self)` (line 253): API 요청용 인증 파라미터 반환
    - `is_configured(self)` (line 264): API Key 설정 여부 확인

### `backend/data_connector/google_sheets/models.py`
- **Classes**
  - `RegisteredSheet` (line 12): 등록된 Google Sheet 정보
  - `SheetMetadata` (line 34): Google Sheet 메타데이터
  - `GoogleSheetPreviewRequest` (line 46): Google Sheet 미리보기 요청
  - `GoogleSheetPreviewResponse` (line 53): Google Sheet 미리보기 응답
  - `GoogleSheetRegisterRequest` (line 69): Google Sheet 등록 요청
  - `GoogleSheetRegisterResponse` (line 83): Google Sheet 등록 응답

### `backend/data_connector/google_sheets/service.py`
- **Classes**
  - `GoogleSheetsService` (line 33): Google Sheets API client (read-only).
    - `__init__(self, api_key)` (line 36): no docstring
    - `async _get_client(self)` (line 43): no docstring
    - `async fetch_sheet_values(self, sheet_url, worksheet_name, api_key, access_token)` (line 51): Fetch raw values + metadata for a Google Sheet URL.
    - `async preview_sheet(self, sheet_url, worksheet_name, limit, api_key, access_token)` (line 112): no docstring
    - `async get_sheet_metadata(self, sheet_id, api_key, access_token)` (line 145): no docstring
    - `async list_spreadsheets(self, access_token, query, page_size)` (line 154): no docstring
    - `async _get_sheet_metadata(self, sheet_id, api_key, access_token)` (line 199): no docstring
    - `async _get_sheet_data(self, sheet_id, range_name, api_key, access_token)` (line 238): no docstring
    - `async close(self)` (line 278): no docstring

### `backend/data_connector/google_sheets/utils.py`
- **Functions**
  - `extract_sheet_id(sheet_url)` (line 13): Google Sheets URL에서 Sheet ID 추출
  - `extract_gid(sheet_url)` (line 34): Google Sheets URL에서 GID (worksheet ID) 추출
  - `build_sheets_api_url(sheet_id, range_name)` (line 60): Google Sheets API v4 URL 생성
  - `build_sheets_metadata_url(sheet_id)` (line 75): Google Sheets 메타데이터 API URL 생성
  - `calculate_data_hash(data)` (line 89): 데이터의 해시값 계산 (변경 감지용)
  - `normalize_sheet_data(raw_data)` (line 104): Google Sheets 원시 데이터 정규화
  - `validate_api_key(api_key)` (line 135): Google API 키 형식 검증
  - `format_datetime_iso(dt)` (line 151): datetime을 ISO 8601 형식으로 변환
  - `parse_range_notation(range_str)` (line 168): A1 notation 범위 파싱
  - `convert_column_letter_to_index(letter)` (line 185): Excel 컬럼 문자를 인덱스로 변환 (A=0, B=1, ..., Z=25, AA=26, ...)
  - `convert_index_to_column_letter(index)` (line 201): 인덱스를 Excel 컬럼 문자로 변환
  - `sanitize_worksheet_name(name)` (line 222): 워크시트 이름 정규화 (특수문자 제거)
  - `estimate_data_size(rows)` (line 239): 데이터 크기 추정

## examples

### `backend/examples/kafka_consumer/consumer_example.py`
- **Functions**
  - `main()` (line 146): 메인 진입점
- **Classes**
  - `OntologyEventConsumer` (line 23): 온톨로지 이벤트 컨슈머 예제
    - `__init__(self, consumer_group)` (line 26): no docstring
    - `initialize(self)` (line 32): 컨슈머 초기화
    - `process_event(self, event)` (line 48): 이벤트 처리 로직
    - `handle_class_created(self, class_id, data)` (line 66): 온톨로지 클래스 생성 이벤트 처리
    - `handle_class_updated(self, class_id, data)` (line 76): 온톨로지 클래스 업데이트 이벤트 처리
    - `handle_class_deleted(self, class_id, data)` (line 83): 온톨로지 클래스 삭제 이벤트 처리
    - `run(self)` (line 89): 메인 실행 루프
    - `shutdown(self)` (line 135): 컨슈머 종료

## funnel

### `backend/funnel/__init__.py`

### `backend/funnel/main.py`
- **Functions**
  - `async lifespan(app)` (line 26): 애플리케이션 시작/종료 이벤트
  - `async root()` (line 63): 루트 엔드포인트
  - `async health_check()` (line 81): 서비스 상태 확인

### `backend/funnel/routers/__init__.py`

### `backend/funnel/routers/type_inference_router.py`
- **Functions**
  - `get_data_processor()` (line 34): 데이터 프로세서 의존성
  - `async analyze_dataset(request, processor)` (line 40): 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.
  - `async analyze_sheet_structure(request)` (line 71): Raw sheet grid(엑셀/스프레드시트)의 구조를 분석합니다.
  - `async analyze_excel_structure(file, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options_json)` (line 120): Excel(.xlsx/.xlsm) 파일을 업로드 받아 grid + merged_cells로 파싱한 뒤,
  - `async analyze_google_sheets_structure(request)` (line 234): Google Sheets URL → (BFF에서 values+metadata(merges) 가져오기) → grid/merged_cells → 구조 분석
  - `async upsert_structure_patch(patch)` (line 321): Store/update a structure-analysis patch for a given sheet_signature.
  - `async get_structure_patch(sheet_signature)` (line 333): no docstring
  - `async delete_structure_patch(sheet_signature)` (line 345): no docstring
  - `async preview_google_sheets_with_inference(sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types, processor)` (line 351): Google Sheets 데이터를 미리보기하고 타입을 추론합니다.
  - `async suggest_schema(analysis_results, class_name, processor)` (line 394): 분석 결과를 기반으로 OMS 스키마를 제안합니다.
  - `async health_check()` (line 425): Funnel 서비스 상태 확인

### `backend/funnel/services/__init__.py`

### `backend/funnel/services/data_processor.py`
- **Functions**
  - `_attach_risks_and_profiles(results, column_risks, column_profiles)` (line 266): no docstring
  - `_copy_model(model, update)` (line 285): no docstring
- **Classes**
  - `FunnelDataProcessor` (line 25): 🔥 THINK ULTRA! 데이터 처리 파이프라인
    - `__init__(self)` (line 35): no docstring
    - `async process_google_sheets_preview(self, sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types)` (line 38): Google Sheets 데이터를 처리하고 타입을 추론합니다.
    - `async analyze_dataset(self, request)` (line 116): 데이터셋을 분석하고 타입을 추론합니다.
    - `generate_schema_suggestion(self, analysis_results, class_name)` (line 156): 분석 결과를 기반으로 스키마를 제안합니다.
    - `_normalize_property_name(self, column_name)` (line 255): 컬럼 이름을 속성 이름으로 정규화
    - `_generate_class_id(self, class_name)` (line 259): 클래스 ID 생성

### `backend/funnel/services/risk_assessor.py`
- **Functions**
  - `assess_dataset_risks(data, columns, analysis_results)` (line 24): no docstring
  - `_build_column_data_map(data, columns)` (line 78): no docstring
  - `_append_name_collision_risks(columns, risks)` (line 89): no docstring
  - `_assess_column_risks(column_name, result)` (line 112): no docstring
  - `_build_column_profile(values, result)` (line 257): no docstring
  - `_compute_length_stats(values)` (line 275): no docstring
  - `_compute_numeric_stats(values, metadata)` (line 289): no docstring
  - `_extract_format_stats(metadata)` (line 333): no docstring
  - `_is_key_like(name)` (line 339): no docstring

### `backend/funnel/services/schema_utils.py`
- **Functions**
  - `normalize_property_name(column_name)` (line 8): Normalize raw column names into schema-safe property identifiers.

### `backend/funnel/services/structure_analysis.py`
- **Classes**
  - `_CellInfo` (line 49): no docstring
  - `FunnelStructureAnalyzer` (line 63): Structure analyzer for sheet-like 2D grids.
    - `_is_blank(value)` (line 82): no docstring
    - `_cache_get(cls, key)` (line 90): no docstring
    - `_cache_set(cls, key, payload, ttl_seconds, max_entries)` (line 102): no docstring
    - `_safe_json_dumps(value)` (line 134): no docstring
    - `_hash_grid(cls, grid)` (line 142): no docstring
    - `_hash_style_hints(cls, style_hints)` (line 157): no docstring
    - `_hash_merges(cls, merged_cells)` (line 174): no docstring
    - `_make_cache_key(cls, grid, merged_cells, style_hints, include_complex_types, max_tables, options)` (line 187): no docstring
    - `_compute_sheet_signature(cls, grid, merged_cells, style_hints, opts)` (line 219): Compute a "sheet_signature" designed to be stable across repeated uploads of the same template.
    - `_compute_coarse_strides(rows, cols, target_cells)` (line 312): Choose downsampling strides so that coarse_rows * coarse_cols ~= target_cells.
    - `_downsample_grid(cls, grid, row_stride, col_stride)` (line 332): no docstring
    - `_map_coarse_bbox_to_full(cls, coarse, row_stride, col_stride, rows, cols, margin_rows, margin_cols)` (line 353): no docstring
    - `_score_cells_in_bbox(cls, grid, bbox, include_complex_types, style_hints)` (line 382): Score only cells inside a bbox (used by coarse-to-fine mode).
    - `_analyze_coarse_to_fine(cls, grid, style_hints, include_complex_types, merged_cells, max_tables, opts)` (line 457): no docstring
    - `analyze(cls, grid, include_complex_types, merged_cells, cell_style_hints, max_tables, options)` (line 576): no docstring
    - `analyze_bbox(cls, grid, bbox, include_complex_types, merged_cells, cell_style_hints, options, table_id, override_mode, override_header_rows, override_header_cols)` (line 763): Analyze a single bbox (used for patch re-evaluation / UI corrections).
    - `_detect_data_islands(cls, grid, cell_map, row_stats, max_tables, opts, style_hints)` (line 938): no docstring
    - `_bbox_quality_score(cls, grid, bbox, cell_map)` (line 1054): no docstring
    - `_split_bbox_by_row_separators(cls, grid, bbox, cell_map, opts, style_hints)` (line 1076): Split a bbox into multiple bboxes when internal separator rows exist.
    - `_split_bbox_by_row_profile(cls, grid, bbox, opts)` (line 1204): Split hybrid blocks where top rows are narrow (key-value) and bottom rows are wide (table).
    - `_expand_bbox_to_dense_region(cls, grid, bbox, cell_map, expand_threshold, max_header_scan)` (line 1254): Expand a bbox derived from "core" cells to cover adjacent string/header cells that
    - `_analyze_island(cls, grid, cell_map, bbox, include_complex_types, table_id, opts, merged_cells)` (line 1340): no docstring
    - `_detect_preamble_skip(cls, grid, bbox, cell_map, opts)` (line 1516): Detect leading "title/description" rows inside a detected bbox.
    - `_rank_header_row_candidates(cls, sub, bbox, cell_map, max_k, include_complex_types, merged_cells, opts)` (line 1619): no docstring
    - `_rank_header_col_candidates(cls, sub, bbox, cell_map, max_k, include_complex_types, merged_cells, opts)` (line 1809): no docstring
    - `_score_property_mode(cls, sub, bbox, cell_map)` (line 1972): no docstring
    - `_best_header_row_candidate(cls, sub, bbox, cell_map, max_k, include_complex_types)` (line 2045): no docstring
    - `_best_header_col_candidate(cls, sub, bbox, cell_map, max_k, include_complex_types)` (line 2071): no docstring
    - `_header_row_score(cls, sub, bbox, cell_map, header_rows)` (line 2097): no docstring
    - `_header_col_score(cls, sub, bbox, cell_map, header_cols)` (line 2130): no docstring
    - `_axis_type_consistency(cls, sequences, include_complex_types)` (line 2165): no docstring
    - `_string_sequence_consistency(cls, values)` (line 2190): Estimate how "table-like" a string-only sequence is.
    - `_flatten_merged_cells(cls, grid, merged_cells, include_complex_types, fill_boxes)` (line 2241): no docstring
    - `_should_fill_merge(cls, value, mr, include_complex_types)` (line 2267): no docstring
    - `_bboxes_intersect(a, b)` (line 2288): no docstring
    - `_extract_key_values(cls, grid, cell_map, exclude_boxes, include_complex_types, opts)` (line 2301): no docstring
    - `_looks_like_kv_label(cls, text)` (line 2423): no docstring
    - `_looks_like_explicit_kv_label(cls, text)` (line 2437): Strict label detector used to avoid pairing label-to-label in KV extraction.
    - `_looks_like_data_value_text(cls, text)` (line 2448): Heuristic: some strings are much more likely to be data values than headers/labels.
    - `_find_nearest_label(cls, cell_map, row, col, radius)` (line 2465): no docstring
    - `_extract_property_table_kv(cls, sub, bbox, cell_map)` (line 2502): no docstring
    - `_pivot_transposed(cls, sub, header_cols)` (line 2541): no docstring
    - `_build_table_column_provenance(cls, headers, bbox, header_rows)` (line 2577): no docstring
    - `_build_transposed_column_provenance(cls, headers, bbox, header_cols, field_row_offsets)` (line 2602): no docstring
    - `_extract_table(cls, sub, header_rows)` (line 2631): no docstring
    - `_build_header_tree(cls, header_grid)` (line 2667): Build a hierarchical header tree from a multi-row header grid.
    - `_collect_cell_evidence(cls, cell_map, bbox, limit)` (line 2722): Collect a small sample of "evidence" cells for explainability.
    - `_infer_schema(cls, headers, rows, include_complex_types)` (line 2770): no docstring
    - `_dedupe_headers(cls, headers)` (line 2789): no docstring
    - `_normalize_grid(cls, grid)` (line 2805): no docstring
    - `_normalize_style_hints(cls, style_hints, rows, cols)` (line 2810): no docstring
    - `_score_cells(cls, grid, include_complex_types, style_hints)` (line 2829): no docstring
    - `_infer_single_value_type(cls, text, include_complex_types)` (line 2900): no docstring
    - `_cell_score(cls, text, inferred_type, row, non_empty_in_row)` (line 2907): no docstring
    - `_is_label_like_text(cls, text)` (line 2937): no docstring
    - `_is_header_like_text(cls, text)` (line 2951): no docstring
    - `_connected_components(points)` (line 2962): no docstring
    - `_bbox_for_points(points)` (line 2984): no docstring
    - `_tighten_bbox(grid, bbox)` (line 2990): no docstring
    - `_bbox_area(bbox)` (line 3012): no docstring
    - `_count_non_empty_in_bbox(cls, grid, bbox)` (line 3016): no docstring
    - `_slice_bbox(grid, bbox)` (line 3026): no docstring
    - `_extract_columns_from_sub(rows)` (line 3030): no docstring
    - `_extract_rows_from_sub(rows, start_col)` (line 3041): no docstring
    - `_ensure_row_len(grid, row, length)` (line 3048): no docstring
    - `_get_cell(grid, row, col)` (line 3055): no docstring

### `backend/funnel/services/structure_patch.py`
- **Functions**
  - `_resolve_table_index(tables, op)` (line 16): no docstring
  - `apply_structure_patch(analysis, patch, grid, merged_cells, cell_style_hints, include_complex_types, options)` (line 35): Apply a stored patch to an analysis result.

### `backend/funnel/services/structure_patch_store.py`
- **Functions**
  - `get_patch(sheet_signature)` (line 19): no docstring
  - `upsert_patch(patch)` (line 26): no docstring
  - `delete_patch(sheet_signature)` (line 31): no docstring

### `backend/funnel/services/type_inference.py`
- **Classes**
  - `PatternBasedTypeDetector` (line 23): 🔥 THINK ULTRA! Pattern-Based Type Detection Service
    - `infer_column_type(cls, column_data, column_name, include_complex_types, context_columns)` (line 97): 🔥 패턴 매칭과 통계 분석으로 컬럼 데이터를 분석하여 타입을 추론합니다.
    - `_infer_type_advanced(cls, values, column_name, include_complex_types, context_columns, sample_size)` (line 169): 🔥 Pattern-Based Type Detection Engine
    - `_get_column_name_hint_scores(cls, column_name)` (line 260): Return type -> hint strength (0.0~1.0) based on column name.
    - `_extract_unit_from_values(cls, values)` (line 343): Best-effort unit extraction from sample values.
    - `_infer_semantic_label_and_unit(cls, values, column_name, inferred)` (line 384): Derive a semantic label (meaning) + unit from type + hints.
    - `_min_confidence_for_type(cls, type_id, thresholds, name_hints)` (line 475): Minimum acceptance confidence for a type (name hints can lower it).
    - `_type_priority(cls, type_id)` (line 501): Tie-break priority (lower is preferred).
    - `_select_best_candidate(cls, candidates, thresholds, name_hints)` (line 523): no docstring
    - `_summarize_candidates(cls, candidates, thresholds, name_hints)` (line 573): no docstring
    - `_check_complex_types_enhanced(cls, values, thresholds, column_name, name_hints)` (line 604): Evaluate complex/specialized types via validators and heuristics.
    - `_check_validator_type(cls, values, type_id, sample_limit, hint_reason, constraints)` (line 651): Check values against ComplexTypeValidator for a given type.
    - `_derive_money_constraints_from_samples(cls, values)` (line 735): Derive money constraints (allowedCurrencies) from explicit tokens in samples.
    - `_check_enum_enhanced(cls, values, thresholds, name_hints)` (line 766): Detect enum-like categorical strings and propose constraints.
    - `_check_boolean(cls, values)` (line 806): Check if values are boolean
    - `_check_integer(cls, values)` (line 830): Check if values are integers
    - `_check_decimal(cls, values)` (line 867): Check if values are decimal numbers
    - `_check_date(cls, values)` (line 914): Check if values are dates
    - `_check_datetime(cls, values)` (line 954): Check if values are datetime
    - `_check_column_name_hints(cls, column_name)` (line 996): Check column name for type hints
    - `_calculate_adaptive_thresholds(cls, values, sample_size)` (line 1072): 🔥 Adaptive Thresholds: tune acceptance based on sample size.
    - `_analyze_context(cls, column_name, context_columns)` (line 1104): 🔥 Contextual Analysis: Analyze surrounding columns for type hints
    - `_check_column_name_hints_enhanced(cls, column_name)` (line 1134): 🔥 Enhanced Column Name Hints with Multilingual Support
    - `_check_boolean_enhanced(cls, values, thresholds)` (line 1181): 🔥 Enhanced Boolean Detection with Fuzzy Matching
    - `_check_integer_enhanced(cls, values, thresholds)` (line 1213): 🔥 Enhanced Integer Detection with Statistical Analysis
    - `_check_decimal_enhanced(cls, values, thresholds)` (line 1296): 🔥 Enhanced Decimal Detection with Distribution Analysis
    - `_check_date_enhanced(cls, values, thresholds)` (line 1418): 🔥 Enhanced Date Detection with strict parsing and ambiguity handling.
    - `_check_datetime_enhanced(cls, values, thresholds)` (line 1566): 🔥 Enhanced DateTime Detection with Advanced Parsing
    - `analyze_dataset(cls, data, columns, sample_size, include_complex_types)` (line 1636): 전체 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.
    - `_check_phone_enhanced(cls, values, thresholds, column_name)` (line 1703): 🔥 Enhanced Phone Number Detection with Global Patterns

### `backend/funnel/services/type_inference_adapter.py`
- **Classes**
  - `FunnelTypeInferenceAdapter` (line 15): Adapter that wraps FunnelTypeInferenceService to implement TypeInferenceInterface.
    - `__init__(self)` (line 23): no docstring
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 30): Analyze a column of data and infer its type.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 49): Analyze an entire dataset and infer types for all columns.

### `backend/funnel/tests/__init__.py`

### `backend/funnel/tests/test_data_processor.py`
- **Functions**
  - `async test_data_processor_analyze_dataset_metadata()` (line 43): no docstring
  - `test_generate_schema_suggestion_handles_confidence()` (line 58): no docstring
  - `async test_process_google_sheets_preview_success(monkeypatch)` (line 92): no docstring
  - `async test_process_google_sheets_preview_failure(monkeypatch)` (line 121): no docstring
- **Classes**
  - `_FakeResponse` (line 10): no docstring
    - `__init__(self, payload, status_code)` (line 11): no docstring
    - `json(self)` (line 16): no docstring
    - `raise_for_status(self)` (line 19): no docstring
  - `_FakeClient` (line 26): no docstring
    - `__init__(self, response)` (line 27): no docstring
    - `async __aenter__(self)` (line 31): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 34): no docstring
    - `async post(self, url, json)` (line 37): no docstring

### `backend/funnel/tests/test_funnel_main.py`
- **Functions**
  - `async test_funnel_root_and_health()` (line 21): no docstring
  - `async test_funnel_lifespan_initializes_rate_limiter(monkeypatch)` (line 32): no docstring
- **Classes**
  - `_FakeRateLimiter` (line 8): no docstring
    - `__init__(self)` (line 9): no docstring
    - `async initialize(self)` (line 13): no docstring
    - `async close(self)` (line 16): no docstring

### `backend/funnel/tests/test_risk_assessor.py`
- **Functions**
  - `test_assess_dataset_risks_name_collision()` (line 7): no docstring
  - `test_assess_dataset_risks_low_confidence_and_nulls()` (line 44): no docstring

### `backend/funnel/tests/test_sheet_grid_parser.py`
- **Classes**
  - `TestSheetGridParser` (line 10): no docstring
    - `test_google_values_normalize_and_trim_trailing(self)` (line 11): no docstring
    - `test_google_merges_from_metadata(self)` (line 27): no docstring
    - `test_google_merges_are_clipped_to_grid(self)` (line 47): no docstring
    - `test_excel_parser_optional_dependency(self)` (line 57): no docstring
    - `test_excel_parser_extracts_merges_and_currency_format(self)` (line 68): no docstring

### `backend/funnel/tests/test_structure_analysis.py`
- **Classes**
  - `TestStructureAnalysis` (line 15): no docstring
    - `test_detect_data_island_with_offset_title(self)` (line 16): no docstring
    - `test_detect_multi_tables_split(self)` (line 46): no docstring
    - `test_split_tables_with_memo_row_no_blank_gap(self)` (line 65): 표 사이에 메모 텍스트가 끼어 있어도 테이블을 분리해야 함
    - `test_detect_transposed_table_and_pivot(self)` (line 82): no docstring
    - `test_detect_property_table(self)` (line 98): no docstring
    - `test_hybrid_invoice_property_plus_line_items_no_blank_gap(self)` (line 117): 하이브리드 문서: 상단은 Key-Value 폼, 하단은 라인아이템 테이블인데
    - `test_merged_cell_flattening_forward_fill(self)` (line 146): no docstring
    - `test_text_only_table_detection(self)` (line 162): 숫자/날짜가 거의 없는 텍스트 표도 데이터 섬으로 잡혀야 함
    - `test_text_only_table_detected_even_when_typed_cells_exist_elsewhere(self)` (line 178): 타입이 강한 셀이 다른 곳에 있어도, 텍스트-only 표를 놓치지 않아야 함
    - `test_multi_header_table(self)` (line 196): 2단 헤더(그룹 헤더 + 필드명) 합성 지원

### `backend/funnel/tests/test_type_inference.py`
- **Functions**
  - `test_parametrized_type_detection(test_input, expected_type)` (line 396): 파라미터화된 타입 감지 테스트
- **Classes**
  - `TestTypeInference` (line 14): 타입 추론 테스트
    - `test_integer_detection(self)` (line 17): 정수 타입 감지 테스트
    - `test_decimal_detection(self)` (line 32): 소수 타입 감지 테스트
    - `test_boolean_detection(self)` (line 48): 불리언 타입 감지 테스트
    - `test_date_detection_iso_format(self)` (line 59): ISO 날짜 형식 감지 테스트
    - `test_date_detection_us_format(self)` (line 70): 미국식 날짜 형식 감지 테스트
    - `test_date_detection_korean_format(self)` (line 80): 한국식 날짜 형식 감지 테스트
    - `test_datetime_detection(self)` (line 90): 날짜시간 타입 감지 테스트
    - `test_mixed_data_string_fallback(self)` (line 100): 혼합 데이터 - 문자열로 폴백
    - `test_null_handling(self)` (line 110): Null 값 처리 테스트
    - `test_column_name_hint_email(self)` (line 122): 컬럼 이름 힌트 - 이메일
    - `test_column_name_hint_phone(self)` (line 136): 컬럼 이름 힌트 - 전화번호
    - `test_dataset_analysis(self)` (line 149): 전체 데이터셋 분석 테스트
    - `test_large_dataset_sampling(self)` (line 178): 대용량 데이터셋 샘플링 테스트
    - `test_empty_dataset(self)` (line 193): 빈 데이터셋 처리
    - `test_confidence_scores(self)` (line 207): 신뢰도 점수 테스트
    - `test_decimal_detection_european_format(self)` (line 219): 유럽식 숫자 형식(1.234,56) 감지 테스트
    - `test_money_detection_with_symbols(self)` (line 231): 통화 기호 기반 money 타입 감지 테스트
    - `test_semantic_label_qty_from_column_name(self)` (line 247): 의미 라벨(QTY) - 컬럼명 힌트 기반
    - `test_money_detection_with_asian_currency_formats(self)` (line 258): 아시아권 통화 표기(¥/RMB/원) 기반 money 타입 감지 테스트
    - `test_enum_detection_and_constraints(self)` (line 276): 열거형(enum) 후보 감지 및 제약조건 제안 테스트
    - `test_uuid_detection(self)` (line 291): UUID 타입 감지 테스트
    - `test_ip_detection(self)` (line 306): IP 주소 타입 감지 테스트
    - `test_uri_detection(self)` (line 317): URI/URL 타입 감지 테스트
    - `test_json_array_object_detection(self)` (line 332): JSON array/object 타입 감지 테스트
    - `test_coordinate_detection(self)` (line 349): 좌표(coordinate) 타입 감지 테스트
    - `test_phone_suggested_region(self)` (line 360): 전화번호 기본 지역 제안(defaultRegion) 테스트
    - `test_ambiguous_date_detection_sets_metadata(self)` (line 374): 모호한 날짜(DD/MM vs MM/DD) 감지 시 메타데이터/신뢰도 페널티 테스트

### `backend/funnel/tests/test_type_inference_adapter.py`
- **Functions**
  - `async test_infer_column_type_respects_metadata_override()` (line 10): no docstring
  - `async test_analyze_dataset_uses_metadata_sample_size()` (line 27): no docstring
  - `async test_infer_single_value_type_returns_type()` (line 44): no docstring

### `backend/funnel/tests/test_type_inference_router.py`
- **Functions**
  - `async test_analyze_dataset_success_and_error()` (line 64): no docstring
  - `async test_analyze_sheet_structure_applies_patch(monkeypatch)` (line 74): no docstring
  - `async test_analyze_excel_structure_happy_path(monkeypatch)` (line 101): no docstring
  - `async test_analyze_excel_structure_errors()` (line 124): no docstring
  - `async test_analyze_google_sheets_structure(monkeypatch)` (line 135): no docstring
  - `async test_structure_patch_endpoints(monkeypatch)` (line 192): no docstring
  - `async test_preview_and_suggest_schema()` (line 212): no docstring
  - `async test_router_health_check()` (line 242): no docstring
- **Classes**
  - `_FakeProcessor` (line 23): no docstring
    - `async analyze_dataset(self, request)` (line 24): no docstring
    - `async process_google_sheets_preview(self, **kwargs)` (line 38): no docstring
    - `generate_schema_suggestion(self, analysis_results, class_name)` (line 48): no docstring
  - `_FailingProcessor` (line 52): no docstring
    - `async analyze_dataset(self, request)` (line 53): no docstring
    - `async process_google_sheets_preview(self, **kwargs)` (line 56): no docstring
    - `generate_schema_suggestion(self, analysis_results, class_name)` (line 59): no docstring

## ingest_reconciler_worker

### `backend/ingest_reconciler_worker/__init__.py`

### `backend/ingest_reconciler_worker/main.py`
- **Functions**
  - `async lifespan(app)` (line 207): no docstring
  - `main()` (line 234): no docstring
- **Classes**
  - `IngestReconcilerWorker` (line 34): no docstring
    - `__init__(self)` (line 35): no docstring
    - `async initialize(self)` (line 54): no docstring
    - `async close(self)` (line 60): no docstring
    - `_record_metrics(self, result)` (line 68): no docstring
    - `_record_error_metric(self)` (line 79): no docstring
    - `_record_alert_metric(self)` (line 82): no docstring
    - `_record_alert_failure_metric(self)` (line 85): no docstring
    - `_should_alert(self, result)` (line 88): no docstring
    - `_alert_allowed(self)` (line 95): no docstring
    - `async _emit_alert(self, payload)` (line 104): no docstring
    - `async run(self, stop_event)` (line 117): no docstring

## instance_worker

### `backend/instance_worker/__init__.py`

### `backend/instance_worker/main.py`
- **Functions**
  - `async main()` (line 2570): Main entry point
- **Classes**
  - `_InstanceCommandPayload` (line 81): no docstring
  - `_InstanceCommandParseError` (line 86): no docstring
  - `StrictInstanceWorker` (line 90): STRICT Lightweight Instance Worker
    - `__init__(self)` (line 102): no docstring
    - `_is_ingest_metadata(metadata)` (line 169): no docstring
    - `_writeback_guard_blocks(cls, command)` (line 175): no docstring
    - `async initialize(self)` (line 196): Initialize all connections
    - `async _ensure_instances_index(self, db_name, branch)` (line 331): Ensure the ES instances index exists (create if needed, update mapping if exists).
    - `async _fetch_class_schema_via_oms(self, db_name, class_id, branch)` (line 363): Fetch ontology schema from OMS HTTP API (replaces TerminusDB get_ontology).
    - `async _resolve_ontology_version_via_oms(self, db_name, branch)` (line 382): Resolve ontology version from OMS HTTP API (replaces TerminusDB version_control).
    - `_build_es_document(db_name, branch, class_id, instance_id, payload, ontology_version, event_sequence, relationships, now_iso, created_at_override, command_id)` (line 403): Build ES document in canonical format matching objectify_worker write_paths._build_document.
    - `async _s3_call(self, func, *args, **kwargs)` (line 463): no docstring
    - `async _s3_read_body(self, body)` (line 466): no docstring
    - `_extract_payload_from_message(self, message)` (line 469): Unwrap a command from the canonical EventEnvelope message.
    - `async extract_payload_from_message(self, message)` (line 498): no docstring
    - `async _stamp_ontology_version(self, command, db_name, branch)` (line 501): Ensure commands carry an ontology ref/commit stamp for reproducibility.
    - `get_primary_key_value(self, class_id, payload, allow_generate)` (line 536): Extract primary key value dynamically based on class naming convention
    - `_is_objectify_command(command)` (line 567): no docstring
    - `async extract_relationships(self, db_name, class_id, payload, branch, allow_pattern_fallback, strict_schema)` (line 576): Extract ONLY relationship fields from payload via shared library.
    - `async _apply_create_instance_side_effects(self, command_id, db_name, class_id, branch, payload, instance_id, command_log, ontology_version, created_by, allow_pattern_fallback)` (line 622): Apply the create-instance side-effects without touching command status.
    - `async process_create_instance(self, command)` (line 848): Process CREATE_INSTANCE command - strict lightweight mode.
    - `async process_bulk_create_instances(self, command)` (line 966): Process BULK_CREATE_INSTANCES command (idempotent per event_id; no sequence-guard).
    - `async process_bulk_update_instances(self, command)` (line 1151): Process BULK_UPDATE_INSTANCES command (updates multiple instances).
    - `async process_update_instance(self, command, skip_status)` (line 1245): Process UPDATE_INSTANCE command (idempotent + ordered via registry claim).
    - `async process_delete_instance(self, command)` (line 1735): Process DELETE_INSTANCE command (idempotent delete).
    - `async _record_instance_edit(self, db_name, class_id, instance_id, edit_type, fields, metadata)` (line 1991): no docstring
    - `async _resolve_instance_payload(self, db_name, branch, class_id, instance_id)` (line 2015): no docstring
    - `async _enqueue_link_reindex(self, db_name, link_type_id)` (line 2085): no docstring
    - `async _apply_relationship_object_link_edits(self, db_name, branch, class_id, instance_id, current_payload, previous_payload)` (line 2096): no docstring
    - `async set_command_status(self, command_id, status, result)` (line 2170): Set command status using CommandStatusService (preserves history + pubsub).
    - `_is_retryable_error_impl(exc)` (line 2239): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 2249): no docstring
    - `_parse_payload(self, payload)` (line 2278): no docstring
    - `_fallback_metadata(self, payload)` (line 2327): no docstring
    - `_registry_key(self, payload)` (line 2330): no docstring
    - `async _process_payload(self, payload)` (line 2358): no docstring
    - `_span_name(self, payload)` (line 2409): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 2412): no docstring
    - `_metric_event_name(self, payload)` (line 2435): no docstring
    - `_is_retryable_error(exc, payload)` (line 2442): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 2447): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 2455): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 2470): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 2503): no docstring
    - `async run(self)` (line 2530): Main processing loop
    - `async shutdown(self)` (line 2542): Graceful shutdown

## mcp_servers

### `backend/mcp_servers/__init__.py`

### `backend/mcp_servers/bff_auth.py`
- **Functions**
  - `bff_api_base_url()` (line 8): no docstring
  - `bff_admin_token()` (line 13): no docstring

### `backend/mcp_servers/context7_development.py`
- **Functions**
  - `get_context7_developer()` (line 368): Get or create Context7 developer instance
  - `async analyze_feature(name, description)` (line 377): Quick analysis before implementing a feature
  - `async validate_code(feature, details, files)` (line 383): Quick validation of implementation
  - `async document_feature(name, description, details, lessons)` (line 389): Quick documentation of implemented feature
- **Classes**
  - `Context7Developer` (line 15): Development helper that integrates Context7 for code analysis and suggestions
    - `__init__(self)` (line 20): no docstring
    - `async analyze_before_implementation(self, feature_name, description, related_files)` (line 24): Analyze codebase before implementing a new feature
    - `async validate_implementation(self, feature_name, implementation_details, files_modified)` (line 103): Validate implementation against best practices
    - `async document_implementation(self, feature_name, description, technical_details, lessons_learned)` (line 161): Document implementation in Context7 knowledge base
    - `async get_coding_suggestions(self, code_snippet, language, context)` (line 233): Get coding suggestions from Context7
    - `_generate_recommendations(self, patterns, insights)` (line 265): Generate recommendations based on patterns and insights
    - `async _get_improvement_suggestions(self, feature_name, files_modified)` (line 295): Get improvement suggestions for the implementation
    - `async _check_code_smells(self, implementation_details)` (line 313): Check for potential code smells
    - `_calculate_quality_score(self, validation_results, code_smells)` (line 333): Calculate overall quality score
    - `_format_technical_details(self, details)` (line 350): Format technical details for documentation
    - `_format_lessons(self, lessons)` (line 357): Format lessons learned

### `backend/mcp_servers/mcp_client.py`
- **Functions**
  - `get_mcp_manager()` (line 397): Get or create MCP manager singleton
  - `get_context7_client()` (line 405): Get Context7 client
- **Classes**
  - `MCPServerConfig` (line 31): Configuration for an MCP server
  - `MCPClientManager` (line 40): Manager for multiple MCP client connections
    - `__init__(self, config_path)` (line 46): no docstring
    - `_resolve_config_path(config_path)` (line 54): no docstring
    - `_load_config(self)` (line 72): Load MCP configuration from file
    - `async connect_server(self, server_name)` (line 100): Connect to a specific MCP server
    - `async disconnect_server(self, server_name)` (line 150): Disconnect from a specific MCP server.
    - `async reconnect_server(self, server_name)` (line 168): Reconnect to a server to refresh the tool list cache.
    - `async call_tool(self, server_name, tool_name, arguments, _retries)` (line 179): Call a tool on a specific MCP server.
    - `async list_tools(self, server_name)` (line 233): List available tools from a server.
  - `Context7Client` (line 262): Specialized client for Context7 MCP server
    - `__init__(self, mcp_manager)` (line 268): no docstring
    - `async search(self, query, limit, filters)` (line 272): Search Context7 knowledge base
    - `async get_context(self, entity_id)` (line 300): Get context for a specific entity
    - `async add_knowledge(self, title, content, metadata)` (line 316): Add knowledge to Context7
    - `async link_entities(self, source_id, target_id, relationship, properties)` (line 343): Create relationship between entities
    - `async analyze_ontology(self, ontology_data)` (line 376): Analyze ontology with Context7

### `backend/mcp_servers/ontology_mcp_server.py`
- **Functions**
  - `_normalize_string(value)` (line 37): Normalize a value to a trimmed string.
  - `_normalize_string_list(value)` (line 42): Normalize to a list of strings.
  - `_mask_observation(payload)` (line 55): Mask PII in tool observations.
  - `_build_error_response(tool_name, error, hint)` (line 60): Build a structured error response.
  - `async main()` (line 1409): no docstring
- **Classes**
  - `OntologyMCPServer` (line 76): MCP server for ontology building tools.
    - `__init__(self)` (line 79): no docstring
    - `_get_or_create_ontology(self, session_id)` (line 84): Get or create a working ontology for a session.
    - `_setup_handlers(self)` (line 99): no docstring
    - `async _handle_tool_call(self, name, arguments)` (line 459): Route tool calls to implementations (Command pattern via naming convention).
    - `async _tool_ontology_new(self, args)` (line 468): Create a new empty ontology in memory.
    - `async _tool_ontology_load(self, args)` (line 505): Load an existing ontology class into memory.
    - `async _tool_ontology_reset(self, args)` (line 551): Reset working ontology to empty state.
    - `async _tool_ontology_set_class_meta(self, args)` (line 562): Set class metadata.
    - `async _tool_ontology_set_abstract(self, args)` (line 591): Set whether class is abstract.
    - `async _tool_ontology_add_property(self, args)` (line 602): Add a property to the ontology.
    - `async _tool_ontology_update_property(self, args)` (line 666): Update an existing property.
    - `async _tool_ontology_remove_property(self, args)` (line 725): Remove a property.
    - `async _tool_ontology_set_primary_key(self, args)` (line 743): Set a property as primary key.
    - `async _tool_ontology_add_relationship(self, args)` (line 783): Add a relationship to the ontology.
    - `async _tool_ontology_update_relationship(self, args)` (line 836): Update an existing relationship.
    - `async _tool_ontology_remove_relationship(self, args)` (line 867): Remove a relationship.
    - `async _tool_ontology_infer_schema_from_data(self, args)` (line 885): Infer schema from sample data using FunnelClient.
    - `async _tool_ontology_suggest_mappings(self, args)` (line 911): Suggest mappings between source schema and target ontology.
    - `async _tool_ontology_validate(self, args)` (line 974): Validate the working ontology structure.
    - `async _tool_ontology_check_relationships(self, args)` (line 1029): Check if relationship targets exist.
    - `async _tool_ontology_check_circular_refs(self, args)` (line 1070): Check for circular references in parent class chain.
    - `async _tool_ontology_list_classes(self, args)` (line 1117): List all ontology classes.
    - `async _tool_ontology_get_class(self, args)` (line 1159): Get details of a specific ontology class.
    - `async _tool_ontology_search_classes(self, args)` (line 1193): Search ontology classes by label or property names.
    - `async _tool_ontology_create(self, args)` (line 1253): Create the ontology class in the database.
    - `async _tool_ontology_update(self, args)` (line 1324): Update an existing ontology class.
    - `async _tool_ontology_preview(self, args)` (line 1390): Preview the working ontology without saving.

### `backend/mcp_servers/pipeline_mcp_errors.py`
- **Functions**
  - `_tool_error_fingerprint(operation, code, message, detail, context)` (line 12): no docstring
  - `tool_error(message, detail, status_code, code, category, external_code, context, operation, diagnostics, fingerprint)` (line 31): no docstring
  - `missing_required_params(tool_name, required, arguments)` (line 87): no docstring

### `backend/mcp_servers/pipeline_mcp_helpers.py`
- **Functions**
  - `normalize_string_list(value)` (line 6): no docstring
  - `normalize_aggregates(value)` (line 18): Normalize aggregates list and return (aggregates, warnings).
  - `extract_spark_error_details(run)` (line 43): Extract error details from a pipeline run's output_json.
  - `trim_preview_payload(preview, max_rows)` (line 92): no docstring
  - `trim_build_output(output_json, max_rows)` (line 102): no docstring

### `backend/mcp_servers/pipeline_mcp_http.py`
- **Functions**
  - `async http_json(method, url, headers, json_body, params, timeout_seconds, error_prefix, error_path)` (line 14): no docstring
  - `bff_headers(db_name, principal_id, principal_type)` (line 44): no docstring
  - `async bff_json(method, path, db_name, principal_id, principal_type, json_body, params, timeout_seconds)` (line 75): no docstring
  - `oms_api_base_url()` (line 104): Get OMS API base URL from environment.
  - `_oms_admin_token()` (line 109): Resolve OMS admin token from environment (same fallback as OMSClient).
  - `async oms_json(method, path, params, json_body, timeout_seconds)` (line 118): Make an HTTP request to OMS API and return JSON response.

### `backend/mcp_servers/pipeline_mcp_server.py`
- **Functions**
  - `_build_tool_error_response(tool_name, error, arguments, hint)` (line 116): Enterprise Enhancement: Build a helpful error response for MCP tool failures.
  - `async main()` (line 1551): no docstring
- **Classes**
  - `_ToolCallRateLimiter` (line 56): Simple rate limiter for MCP tool calls to prevent runaway Agent loops.
    - `__init__(self, max_calls_per_minute, max_calls_per_tool_per_minute)` (line 62): no docstring
    - `check_and_record(self, tool_name)` (line 75): Check if the call is allowed and record it.
  - `PipelineMCPServer` (line 159): no docstring
    - `__init__(self)` (line 160): no docstring
    - `async _ensure_registries(self)` (line 169): no docstring
    - `async _ensure_pipeline_registry(self)` (line 178): no docstring
    - `async _ensure_objectify_registry(self)` (line 184): no docstring
    - `async _ensure_websocket_service(self)` (line 190): Lazy-init WebSocket service for schema drift broadcasts.
    - `_setup_handlers(self)` (line 202): no docstring
    - `async run(self)` (line 1540): no docstring

### `backend/mcp_servers/pipeline_tools/__init__.py`

### `backend/mcp_servers/pipeline_tools/dataset_tools.py`
- **Functions**
  - `_parse_csv_bytes(raw_bytes, max_rows)` (line 33): Parse CSV payload into row dicts (standalone version of PipelineExecutor helper).
  - `_parse_excel_bytes(raw_bytes, max_rows)` (line 67): no docstring
  - `_parse_json_bytes(raw_bytes, max_rows)` (line 78): no docstring
  - `_extract_sample_rows(sample)` (line 107): Extract sample rows from a version's sample_json payload.
  - `_extract_schema_column_names(schema_json)` (line 127): Extract column names from a schema JSON payload.
  - `async _load_artifact_rows(storage_service, artifact_key, max_rows)` (line 143): Load rows from an S3/lakeFS artifact, parsing CSV/Excel/JSON.
  - `async _load_sample_rows(server, dataset_id, limit)` (line 208): Load sample rows from a dataset, trying multiple sources: profile cache → artifact → sample_json.
  - `_filter_columns(rows, columns_filter)` (line 246): Filter rows to only include specified columns.
  - `async _dataset_get_by_name(server, arguments)` (line 260): no docstring
  - `async _dataset_get_latest_version(server, arguments)` (line 293): no docstring
  - `async _dataset_validate_columns(server, arguments)` (line 317): no docstring
  - `async _dataset_list(server, arguments)` (line 374): List all datasets in a given database/project, with schema summaries.
  - `async _dataset_sample(server, arguments)` (line 404): Return a small sample of actual data rows from a dataset (PII-masked).
  - `async _dataset_profile(server, arguments)` (line 456): Get column-level statistics for a dataset: null ratio, distinct count, top values, histogram.
  - `async _data_query(server, arguments)` (line 531): Execute ad-hoc SQL query on dataset sample rows using DuckDB.
  - `build_dataset_tool_handlers()` (line 638): no docstring

### `backend/mcp_servers/pipeline_tools/debug_tools.py`
- **Functions**
  - `async _debug_inspect_node(_server, arguments)` (line 14): no docstring
  - `async _debug_dry_run(_server, arguments)` (line 59): no docstring
  - `build_debug_tool_handlers()` (line 138): no docstring

### `backend/mcp_servers/pipeline_tools/objectify_tools.py`
- **Functions**
  - `async _objectify_suggest_mapping(server, arguments)` (line 20): no docstring
  - `async _objectify_create_mapping_spec(server, arguments)` (line 125): no docstring
  - `async _objectify_list_mapping_specs(server, arguments)` (line 281): no docstring
  - `async _objectify_run(server, arguments)` (line 310): no docstring
  - `async _objectify_get_status(server, arguments)` (line 478): no docstring
  - `async _objectify_wait(server, arguments)` (line 514): no docstring
  - `async _trigger_incremental_objectify(server, arguments)` (line 575): no docstring
  - `async _get_objectify_watermark(server, arguments)` (line 643): no docstring
  - `async _reconcile_relationships(server, arguments)` (line 675): Auto-populate relationships on ES instances by detecting FK references.
  - `build_objectify_tool_handlers()` (line 741): no docstring

### `backend/mcp_servers/pipeline_tools/ontology_tools.py`
- **Functions**
  - `async _ontology_register_object_type(_server, arguments)` (line 19): no docstring
  - `async _ontology_query_instances(_server, arguments)` (line 188): no docstring
  - `async _detect_foreign_keys(server, arguments)` (line 257): no docstring
  - `async _create_link_type_from_fk(_server, arguments)` (line 358): no docstring
  - `build_ontology_tool_handlers()` (line 442): no docstring

### `backend/mcp_servers/pipeline_tools/pipeline_tools.py`
- **Functions**
  - `_run_status(run)` (line 27): no docstring
  - `async _pipeline_wait_for_mode(server, pipeline_id, mode, enqueue_path, output_builder, enqueue_job_id_extractor, timeout_message, arguments)` (line 31): no docstring
  - `_preview_job_id(resp)` (line 167): no docstring
  - `_build_job_id(resp)` (line 175): no docstring
  - `_preview_output(selected_run, job_id, reused_existing)` (line 180): no docstring
  - `_build_output(selected_run, job_id, reused_existing)` (line 195): no docstring
  - `async _preview_inspect(_server, arguments)` (line 213): no docstring
  - `async _pipeline_create_from_plan(_server, arguments)` (line 228): no docstring
  - `async _pipeline_update_from_plan(_server, arguments)` (line 378): no docstring
  - `async _pipeline_preview_wait(server, arguments)` (line 442): no docstring
  - `async _pipeline_build_wait(server, arguments)` (line 465): no docstring
  - `async _pipeline_deploy_promote_build(server, arguments)` (line 488): no docstring
  - `build_pipeline_tool_handlers()` (line 652): no docstring

### `backend/mcp_servers/pipeline_tools/plan_tools.py`
- **Functions**
  - `async _plan_new(_server, arguments)` (line 69): no docstring
  - `async _plan_reset(_server, arguments)` (line 80): no docstring
  - `async _plan_add_input(_server, arguments)` (line 87): no docstring
  - `async _plan_add_external_input(_server, arguments)` (line 101): no docstring
  - `async _plan_configure_input_read(_server, arguments)` (line 116): no docstring
  - `async _plan_add_join(_server, arguments)` (line 151): no docstring
  - `async _plan_add_group_by(_server, arguments)` (line 218): no docstring
  - `async _plan_add_group_by_expr(_server, arguments)` (line 242): no docstring
  - `async _plan_add_window(_server, arguments)` (line 262): no docstring
  - `async _plan_add_window_expr(_server, arguments)` (line 282): no docstring
  - `async _plan_add_transform(_server, arguments)` (line 296): no docstring
  - `async _plan_add_sort(_server, arguments)` (line 331): no docstring
  - `async _plan_add_explode(_server, arguments)` (line 346): no docstring
  - `async _plan_add_union(_server, arguments)` (line 360): no docstring
  - `async _plan_add_pivot(_server, arguments)` (line 373): no docstring
  - `async _plan_add_filter(_server, arguments)` (line 404): no docstring
  - `async _plan_add_compute(_server, arguments)` (line 416): no docstring
  - `async _plan_add_udf(_server, arguments)` (line 492): no docstring
  - `async _plan_add_compute_column(_server, arguments)` (line 507): no docstring
  - `async _plan_add_compute_assignments(_server, arguments)` (line 520): no docstring
  - `async _plan_add_cast(_server, arguments)` (line 532): no docstring
  - `async _plan_add_rename(_server, arguments)` (line 544): no docstring
  - `async _plan_add_select(_server, arguments)` (line 598): no docstring
  - `async _plan_add_select_expr(_server, arguments)` (line 610): no docstring
  - `async _plan_add_drop(_server, arguments)` (line 622): no docstring
  - `async _plan_add_dedupe(_server, arguments)` (line 634): no docstring
  - `async _plan_add_normalize(_server, arguments)` (line 646): no docstring
  - `async _plan_add_regex_replace(_server, arguments)` (line 663): no docstring
  - `async _plan_add_split(_server, arguments)` (line 675): no docstring
  - `async _plan_add_geospatial(_server, arguments)` (line 688): no docstring
  - `async _plan_add_pattern_mining(_server, arguments)` (line 724): no docstring
  - `async _plan_add_stream_join(_server, arguments)` (line 739): no docstring
  - `async _plan_add_output(_server, arguments)` (line 772): no docstring
  - `async _plan_add_edge(_server, arguments)` (line 813): no docstring
  - `async _plan_delete_edge(_server, arguments)` (line 824): no docstring
  - `async _plan_set_node_inputs(_server, arguments)` (line 835): no docstring
  - `async _plan_update_node_metadata(_server, arguments)` (line 846): no docstring
  - `async _plan_update_settings(_server, arguments)` (line 869): no docstring
  - `async _plan_delete_node(_server, arguments)` (line 886): no docstring
  - `async _plan_update_output(_server, arguments)` (line 896): no docstring
  - `async _plan_validate_structure(_server, arguments)` (line 930): no docstring
  - `async _plan_validate(server, arguments)` (line 937): no docstring
  - `async _plan_preview(server, arguments)` (line 981): no docstring
  - `async _plan_refute_claims(server, arguments)` (line 1091): no docstring
  - `async _plan_evaluate_joins(server, arguments)` (line 1141): no docstring
  - `build_plan_tool_handlers()` (line 1229): no docstring

### `backend/mcp_servers/pipeline_tools/registry.py`
- **Functions**
  - `merge_tool_handlers(*maps)` (line 8): no docstring

### `backend/mcp_servers/pipeline_tools/schema_tools.py`
- **Functions**
  - `async _check_schema_drift(server, arguments)` (line 17): no docstring
  - `async _list_schema_changes(server, arguments)` (line 121): no docstring
  - `build_schema_tool_handlers()` (line 202): no docstring

### `backend/mcp_servers/terminus_mcp_server.py`
- **Functions**
  - `async main()` (line 308): Main entry point
- **Classes**
  - `TerminusDBMCPServer` (line 36): MCP Server for TerminusDB operations
    - `__init__(self)` (line 42): no docstring
    - `_setup_handlers(self)` (line 47): Setup MCP request handlers
    - `async _connect_terminus(self)` (line 286): Connect to TerminusDB
    - `async run(self)` (line 294): Run the MCP server

## message_relay

### `backend/message_relay/__init__.py`

### `backend/message_relay/main.py`
- **Functions**
  - `async main()` (line 770): 메인 진입점
- **Classes**
  - `_RecentPublishedEventIds` (line 48): Best-effort in-memory dedup window (publisher is still at-least-once).
    - `__init__(self, max_events)` (line 51): no docstring
    - `mark_published(self, event_id)` (line 55): no docstring
    - `was_published(self, event_id)` (line 65): no docstring
    - `load(self, event_ids)` (line 73): no docstring
    - `snapshot(self, max_events)` (line 84): no docstring
  - `EventPublisher` (line 92): S3/MinIO Event Store -> Kafka publisher.
    - `__init__(self)` (line 95): no docstring
    - `_s3_client_kwargs(self)` (line 137): no docstring
    - `async initialize(self)` (line 147): 서비스 초기화
    - `async ensure_kafka_topics(self)` (line 201): 필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성
    - `async _load_checkpoint(self)` (line 258): no docstring
    - `async _save_checkpoint(self, checkpoint)` (line 270): no docstring
    - `_log_metrics_if_due(self)` (line 284): no docstring
    - `_flush_producer(self, timeout_s)` (line 307): no docstring
    - `_initial_checkpoint(self)` (line 317): no docstring
    - `_advance_checkpoint(checkpoint, ts_ms, idx_key)` (line 331): Advance the durable checkpoint monotonically (never move backwards).
    - `async _list_next_index_keys(self, checkpoint)` (line 355): no docstring
    - `async process_events(self)` (line 430): Tail S3 by-date index and publish to Kafka.
    - `async run(self)` (line 736): 메인 실행 루프
    - `async shutdown(self)` (line 756): 서비스 종료

## monitoring

### `backend/monitoring/s3_event_store_dashboard.py`
- **Functions**
  - `async main()` (line 357): Run dashboard in CLI mode
- **Classes**
  - `S3EventStoreDashboard` (line 91): S3/MinIO Event Store Monitoring Dashboard
    - `__init__(self)` (line 94): no docstring
    - `async connect(self)` (line 102): Initialize connection to S3/MinIO
    - `async collect_storage_metrics(self)` (line 107): Collect storage usage metrics
    - `async collect_performance_metrics(self)` (line 156): Collect performance metrics from recent operations
    - `async collect_publisher_checkpoint_metrics(self)` (line 178): Collect EventPublisher checkpoint metrics from S3/MinIO.
    - `async collect_health_metrics(self)` (line 243): Collect health and availability metrics
    - `async generate_dashboard(self)` (line 291): Generate complete dashboard data
    - `async start_monitoring(self, interval_seconds)` (line 319): Start continuous monitoring
    - `get_prometheus_metrics(self)` (line 346): Export metrics in Prometheus format
    - `async get_json_dashboard(self)` (line 350): Get dashboard data as JSON

## objectify_worker

### `backend/objectify_worker/__init__.py`

### `backend/objectify_worker/main.py`
- **Functions**
  - `_auto_detect_watermark_column(columns, options)` (line 91): Auto-detect a watermark column from dataset schema or options.
  - `async _compute_lakefs_delta(job, delta_computer, storage)` (line 116): Compute row-level delta between two LakeFS commits using the diff API.
  - `_extract_instance_relationships(instance, rel_map)` (line 175): Thin wrapper: extract relationships for a single instance using pre-parsed rel_map.
  - `async main()` (line 4091): no docstring
- **Classes**
  - `ObjectifyNonRetryableError` (line 190): Raised for objectify failures that should not be retried.
  - `ObjectifyWorker` (line 194): no docstring
    - `__init__(self)` (line 223): no docstring
    - `_build_error_report(self, error, report, job, message, context)` (line 271): no docstring
    - `async _record_gate_result(self, job, status, details)` (line 310): no docstring
    - `async _emit_objectify_completed_event(self, job, total_rows, prepared_instances, indexed_instances, execution_mode, ontology_version)` (line 338): Emit OBJECTIFY_COMPLETED event via pipeline control-plane.
    - `async _update_object_type_active_version(self, job, mapping_spec)` (line 396): no docstring
    - `_normalize_ontology_payload(payload)` (line 455): no docstring
    - `_extract_ontology_fields(cls, payload)` (line 463): no docstring
    - `_is_blank(value)` (line 490): no docstring
    - `_normalize_relationship_ref(value, target_class)` (line 494): no docstring
    - `_normalize_constraints(constraints, raw_type)` (line 518): no docstring
    - `_resolve_import_type(raw_type)` (line 562): no docstring
    - `_validate_value_constraints(self, value, constraints, raw_type)` (line 565): no docstring
    - `_validate_value_constraints_single(self, value, constraints, raw_type)` (line 584): no docstring
    - `_map_mappings_by_target(mappings)` (line 649): no docstring
    - `_has_p0_errors(self, errors)` (line 659): no docstring
    - `async initialize(self)` (line 666): no docstring
    - `async close(self)` (line 746): no docstring
    - `async run(self)` (line 778): no docstring
    - `_parse_payload(self, payload)` (line 789): no docstring
    - `_registry_key(self, payload)` (line 792): no docstring
    - `async _process_payload(self, payload)` (line 799): no docstring
    - `_fallback_metadata(self, payload)` (line 814): no docstring
    - `_span_name(self, payload)` (line 828): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 831): no docstring
    - `_metric_event_name(self, payload)` (line 852): no docstring
    - `_heartbeat_options(self)` (line 855): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 860): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 869): no docstring
    - `async _persist_objectify_failure_status(self, job, status, error, attempt_count, retryable, completed_at)` (line 878): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 911): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 936): no docstring
    - `async _process_job(self, job)` (line 958): no docstring
    - `async _bulk_update_instances(self, job, updates, ontology_version)` (line 2172): no docstring
    - `async _iter_class_instance_ids(self, db_name, class_id, branch, limit)` (line 2208): no docstring
    - `async _resolve_artifact_output(self, job)` (line 2247): no docstring
    - `async _fetch_target_field_types(self, job)` (line 2292): no docstring
    - `async _fetch_class_schema(self, job)` (line 2316): no docstring
    - `async _fetch_object_type_contract(self, job)` (line 2327): no docstring
    - `async _fetch_value_type_defs(self, job, value_type_refs)` (line 2340): no docstring
    - `async _fetch_ontology_version(self, job)` (line 2375): no docstring
    - `async _fetch_ontology_head_commit(self, job)` (line 2395): no docstring
    - `_normalize_pk_fields(value)` (line 2413): no docstring
    - `_hash_payload(payload)` (line 2417): no docstring
    - `_derive_row_key(self, columns, col_index, row, instance, pk_fields, pk_targets)` (line 2421): no docstring
    - `_derive_unique_key(self, instance, key_fields)` (line 2451): no docstring
    - `async _iter_dataset_batches(self, job, options, row_batch_size, max_rows)` (line 2459): no docstring
    - `async _iter_csv_batches(self, bucket, key, delimiter, has_header, row_batch_size, max_rows)` (line 2499): no docstring
    - `async _iter_json_part_batches(self, bucket, prefix, row_batch_size, max_rows)` (line 2592): no docstring
    - `async _iter_dataset_batches_incremental(self, job, options, row_batch_size, max_rows, mapping_spec)` (line 2649): Iterate dataset batches with incremental filtering.
    - `async _update_watermark_after_job(self, job, new_watermark)` (line 2754): Update watermark in registry after successful incremental job.
    - `_build_instances_with_validation(self, columns, rows, row_offset, mappings, relationship_mappings, relationship_meta, target_field_types, mapping_sources, sources_by_target, required_targets, pk_targets, pk_fields, field_constraints, field_raw_types, seen_row_keys)` (line 2783): no docstring
    - `async _run_link_index_job(self, job, mapping_spec, options, mappings, mapping_sources, mapping_targets, sources_by_target, prop_map, rel_map, relationship_mappings, stable_seed, row_batch_size, max_rows)` (line 3011): no docstring
    - `async _validate_batches(self, job, options, mappings, relationship_mappings, relationship_meta, target_field_types, mapping_sources, sources_by_target, required_targets, pk_targets, pk_fields, field_constraints, field_raw_types, row_batch_size, max_rows)` (line 3619): no docstring
    - `async _scan_key_constraints(self, job, options, mappings, relationship_meta, target_field_types, sources_by_target, required_targets, pk_targets, pk_fields, unique_keys, row_batch_size, max_rows)` (line 3687): no docstring
    - `_ensure_instance_ids(self, instances, class_id, stable_seed, mapping_spec_version, row_keys, instance_id_field)` (line 3809): no docstring
    - `async _record_lineage_header(self, job, mapping_spec, ontology_version, input_type, artifact_output_name)` (line 3843): no docstring
    - `async _record_instance_lineage(self, job, job_node_id, instance_ids, mapping_spec_id, mapping_spec_version, ontology_version, limit_remaining, input_type, artifact_output_name)` (line 3943): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 4043): no docstring
    - `_is_retryable_error_impl(exc)` (line 4078): no docstring

### `backend/objectify_worker/validation_codes.py`
- **Classes**
  - `ObjectifyValidationCode` (line 9): Validation codes used in objectify job failure reports (Kafka, not HTTP).

### `backend/objectify_worker/write_paths.py`
- **Classes**
  - `ObjectifyWriteBatchResult` (line 31): Result for a single write batch.
  - `ObjectifyWritePath` (line 38): Port for objectify write-side strategies.
    - `async write_instances(self, job, instances, ontology_version, objectify_pk_fields, objectify_instance_id_field, instance_relationships, target_field_types)` (line 41): no docstring
    - `async finalize_job(self, job, execution_mode, indexed_instance_ids)` (line 55): no docstring
  - `DatasetPrimaryIndexWritePath` (line 65): Foundry-style path: dataset rows are indexed directly into Elasticsearch.
    - `__init__(self, elasticsearch_service, storage_service, instance_bucket, chunk_size, refresh, prune_stale_on_full)` (line 121): no docstring
    - `async write_instances(self, job, instances, ontology_version, objectify_pk_fields, objectify_instance_id_field, instance_relationships, target_field_types)` (line 139): no docstring
    - `async _write_instance_commands_to_s3(self, job, instances, indexed_instance_ids, branch, now_iso, batch_sequence)` (line 217): Write BULK_CREATE_INSTANCES command files to instance-events S3 for action writeback.
    - `async finalize_job(self, job, execution_mode, indexed_instance_ids)` (line 288): no docstring
    - `async _ensure_instances_index(self, db_name, branch)` (line 331): no docstring
    - `async _find_stale_instance_ids(self, index_name, class_id, active_instance_ids)` (line 357): no docstring
    - `_build_document(job, instance, instance_id, branch, ontology_version, now_iso, event_sequence, relationships, target_field_types)` (line 414): no docstring

## oms

### `backend/oms/__init__.py`

### `backend/oms/database/__init__.py`

### `backend/oms/database/decorators.py`
- **Functions**
  - `with_deadlock_retry(strategy, log_retries)` (line 26): Decorator to add deadlock retry logic to async database operations.
  - `with_serialization_retry(strategy)` (line 63): Decorator for handling serialization failures in SERIALIZABLE transactions.
  - `with_mvcc_retry(strategy)` (line 94): Decorator that handles both deadlock and serialization failures.
  - `with_transaction(isolation_level, read_only, with_retry)` (line 132): Decorator that automatically wraps a function in a database transaction.
  - `with_optimistic_lock(version_field, entity_type)` (line 198): Decorator to add optimistic locking version check.
  - `monitor_transaction_time(threshold_seconds, log_level)` (line 239): Decorator to monitor and log slow transactions.

### `backend/oms/database/mvcc.py`
- **Classes**
  - `IsolationLevel` (line 18): PostgreSQL Transaction Isolation Levels
    - `get_default(cls)` (line 34): Get default isolation level for MVCC operations
  - `MVCCTransactionManager` (line 39): Transaction manager with MVCC support.
    - `__init__(self, pool, default_isolation)` (line 49): Initialize MVCC Transaction Manager.
    - `async transaction(self, isolation_level, read_only, deferrable)` (line 62): Create a transaction with specified isolation level.
    - `async get_active_transaction_count(self)` (line 150): Get count of currently active transactions
    - `async execute_with_retry(self, query, *args, max_retries, isolation_level, backoff_base)` (line 154): Execute a query with automatic retry on serialization/deadlock errors.
  - `MVCCError` (line 205): Base exception for MVCC-related errors
  - `MVCCSerializationError` (line 210): Raised when a serialization failure occurs in SERIALIZABLE isolation
  - `MVCCDeadlockError` (line 215): Raised when a deadlock is detected
  - `MVCCMaxRetriesError` (line 220): Raised when maximum retry attempts are exceeded

### `backend/oms/database/postgres.py`
- **Functions**
  - `async get_db()` (line 168): Dependency to get database instance
- **Classes**
  - `PostgresDatabase` (line 25): PostgreSQL database connection manager with MVCC support.
    - `__init__(self)` (line 33): no docstring
    - `async connect(self)` (line 40): Create connection pool to PostgreSQL with MVCC support
    - `async disconnect(self)` (line 67): Close all connections in the pool
    - `async transaction(self, isolation_level, read_only, with_retry)` (line 74): Provide a transactional database connection with MVCC support.
    - `async execute(self, query, *args)` (line 131): Execute a query without returning results with automatic retry on deadlock
    - `async fetch(self, query, *args)` (line 139): Fetch multiple rows
    - `async fetchrow(self, query, *args)` (line 147): Fetch a single row
    - `async fetchval(self, query, *args)` (line 155): Fetch a single value

### `backend/oms/database/retry_handler.py`
- **Classes**
  - `RetryableError` (line 18): Enumeration of retryable error types
  - `RetryStrategy` (line 27): Abstract base class for retry strategies.
    - `async should_retry(self, error, attempt)` (line 35): Determine if the operation should be retried.
    - `get_delay(self, attempt)` (line 49): Calculate delay before next retry.
    - `get_max_attempts(self)` (line 62): Get maximum number of retry attempts.
  - `DeadlockRetryStrategy` (line 72): Retry strategy specifically for database deadlocks.
    - `__init__(self, max_attempts, base_delay, max_delay, jitter_factor)` (line 79): Initialize deadlock retry strategy.
    - `async should_retry(self, error, attempt)` (line 100): Check if error is a deadlock and we haven't exceeded max attempts
    - `get_delay(self, attempt)` (line 118): Calculate exponential backoff with jitter
    - `get_max_attempts(self)` (line 131): Get maximum retry attempts
  - `SerializationRetryStrategy` (line 136): Retry strategy for serialization failures in SERIALIZABLE isolation.
    - `__init__(self, max_attempts, base_delay, max_delay)` (line 144): no docstring
    - `async should_retry(self, error, attempt)` (line 154): Check if error is a serialization failure
    - `get_delay(self, attempt)` (line 172): Linear backoff for serialization failures
    - `get_max_attempts(self)` (line 179): no docstring
  - `CompositeRetryStrategy` (line 183): Combines multiple retry strategies.
    - `__init__(self, strategies)` (line 190): Initialize composite strategy.
    - `async should_retry(self, error, attempt)` (line 201): Check if any strategy says we should retry
    - `get_delay(self, attempt)` (line 208): Use the maximum delay from all strategies
    - `get_max_attempts(self)` (line 213): Use the maximum attempts from all strategies
  - `RetryExecutor` (line 218): Executes operations with retry logic.
    - `__init__(self, strategy)` (line 225): Initialize retry executor.
    - `async execute(self, operation, *args, **kwargs)` (line 234): Execute an operation with retry logic.

### `backend/oms/dependencies.py`
- **Functions**
  - `ValidatedDatabaseName(db_name)` (line 198): 데이터베이스 이름 검증 의존성 - Modernized version
  - `ValidatedClassId(class_id)` (line 210): 클래스 ID 검증 의존성 - Modernized version
  - `async ensure_database_exists(db_name, terminus)` (line 223): 데이터베이스 존재 확인 및 검증된 이름 반환 - Modernized version
  - `async check_oms_dependencies_health(container)` (line 262): Check health of all OMS dependencies
- **Classes**
  - `OMSDependencyProvider` (line 54): Modern dependency provider for OMS services
    - `async get_terminus_service(container)` (line 63): Get AsyncTerminusService from container
    - `async get_event_store(container)` (line 94): Get S3/MinIO Event Store - The Single Source of Truth.
    - `async get_command_status_service(container)` (line 104): Get command status service from container
    - `async get_processed_event_registry(container)` (line 165): no docstring

### `backend/oms/entities/__init__.py`

### `backend/oms/entities/label_mapping.py`
- **Classes**
  - `LabelMapping` (line 14): 레이블 매핑 엔티티
    - `to_terminusdb_document(self)` (line 42): TerminusDB 문서 형식으로 변환
    - `from_terminusdb_document(cls, doc)` (line 64): TerminusDB 문서에서 LabelMapping 엔티티 생성
    - `update_timestamp(self)` (line 95): 업데이트 타임스탬프 갱신
    - `generate_id(db_name, mapping_type, target_id, language, class_id)` (line 100): 고유 ID 생성
    - `__eq__(self, other)` (line 125): 동등성 비교
    - `__hash__(self)` (line 137): 해시 계산

### `backend/oms/entities/ontology.py`
- **Classes**
  - `Property` (line 19): 속성 엔티티
  - `Relationship` (line 31): 관계 엔티티
  - `Ontology` (line 43): 온톨로지 엔티티
    - `__post_init__(self)` (line 57): 초기화 후 처리
    - `add_property(self, property)` (line 64): 속성 추가
    - `remove_property(self, property_name)` (line 71): 속성 제거
    - `add_relationship(self, relationship)` (line 80): 관계 추가
    - `validate(self)` (line 89): 엔티티 유효성 검증
    - `to_dict(self)` (line 111): 딕셔너리로 변환

### `backend/oms/exceptions.py`
- **Classes**
  - `OmsBaseException` (line 6): Base exception for OMS
    - `__init__(self, message, details)` (line 9): no docstring
  - `OntologyNotFoundError` (line 15): Raised when ontology is not found
  - `DuplicateOntologyError` (line 21): Raised when trying to create duplicate ontology
  - `OntologyValidationError` (line 27): Raised when ontology validation fails
  - `ConnectionError` (line 33): Raised when connection to database fails
  - `DatabaseNotFoundError` (line 39): Raised when database is not found
  - `DatabaseError` (line 45): General database error
  - `AtomicUpdateError` (line 52): Base exception for atomic update operations
  - `PatchUpdateError` (line 58): Raised when patch-based atomic update fails
  - `TransactionUpdateError` (line 64): Raised when transaction-based atomic update fails
  - `WOQLUpdateError` (line 70): Raised when WOQL-based atomic update fails
  - `BackupCreationError` (line 76): Raised when backup creation fails
  - `BackupRestoreError` (line 82): Raised when backup restore fails
  - `CriticalDataLossRisk` (line 88): Raised when there's a critical risk of data loss
  - `RelationshipError` (line 95): Base exception for relationship operations
  - `CircularReferenceError` (line 101): Raised when circular reference is detected
  - `InvalidRelationshipError` (line 107): Raised when relationship is invalid
  - `RelationshipValidationError` (line 113): Raised when relationship validation fails

### `backend/oms/main.py`
- **Functions**
  - `async lifespan(app)` (line 339): Modern application lifecycle management
  - `async _oms_domain_exception_handler(request, exc)` (line 456): Map OMS domain exceptions to enterprise error envelope responses.
  - `async get_terminus_service()` (line 488): Get TerminusDB service from OMS container
  - `async root()` (line 497): 루트 엔드포인트 - Modernized version
  - `async health_check()` (line 517): 헬스 체크 - Modernized version
  - `async container_health_check()` (line 603): Health check for the modernized container system
- **Classes**
  - `OMSServiceContainer` (line 82): OMS-specific service container to manage OMS services
    - `__init__(self, container, settings)` (line 90): no docstring
    - `async initialize_oms_services(self)` (line 95): Initialize OMS-specific services
    - `async _initialize_event_store(self)` (line 125): Initialize S3/MinIO Event Store - The Single Source of Truth.
    - `async _initialize_terminus_service(self)` (line 145): Initialize TerminusDB service with health check
    - `async _initialize_postgres(self)` (line 171): Initialize Postgres MVCC pool (required for pull requests/proposals).
    - `async _initialize_jsonld_converter(self)` (line 181): Initialize JSON-LD converter
    - `async _initialize_label_mapper(self)` (line 187): Initialize label mapper
    - `async _initialize_redis_and_command_status(self)` (line 193): Initialize Redis service and Command Status service
    - `async _initialize_elasticsearch(self)` (line 218): Initialize Elasticsearch service
    - `async _initialize_rate_limiter(self)` (line 237): Initialize rate limiting service
    - `async shutdown_oms_services(self)` (line 253): Shutdown OMS-specific services
    - `get_terminus_service(self)` (line 303): Get TerminusDB service instance
    - `get_jsonld_converter(self)` (line 309): Get JSON-LD converter instance
    - `get_label_mapper(self)` (line 315): Get label mapper instance
    - `get_redis_service(self)` (line 321): Get Redis service instance (can be None)
    - `get_command_status_service(self)` (line 325): Get command status service instance (can be None)
    - `get_elasticsearch_service(self)` (line 329): Get Elasticsearch service instance (can be None)

### `backend/oms/middleware/__init__.py`

### `backend/oms/middleware/auth.py`
- **Functions**
  - `ensure_oms_auth_configured()` (line 14): no docstring
  - `install_oms_auth_middleware(app)` (line 30): no docstring

### `backend/oms/routers/__init__.py`

### `backend/oms/routers/_event_sourcing.py`
- **Functions**
  - `build_command_status_metadata(command, extra)` (line 29): no docstring
  - `async append_event_sourcing_command(event_store, command, actor, kafka_topic, envelope_metadata, command_status_service, command_status_metadata)` (line 41): no docstring

### `backend/oms/routers/action_async.py`
- **Functions**
  - `_resolve_writeback_target(db_name, raw_target)` (line 167): no docstring
  - `async submit_action_async(db_name, action_type_id, request, base_branch, terminus, event_store)` (line 189): Submit an Action for async execution.
  - `async simulate_action_async(db_name, action_type_id, request, terminus)` (line 555): Simulate an Action writeback (dry-run).
- **Classes**
  - `ActionSubmitRequest` (line 79): no docstring
  - `ActionSubmitResponse` (line 90): no docstring
  - `ActionSimulateScenarioRequest` (line 101): no docstring
  - `ActionSimulateStatePatch` (line 109): Patch-like state override for decision simulation (what-if).
  - `ActionSimulateObservedBaseOverrides` (line 120): Override observed_base snapshot fields/links to simulate stale reads.
  - `ActionSimulateTargetAssumption` (line 127): no docstring
  - `ActionSimulateAssumptions` (line 134): no docstring
  - `ActionSimulateRequest` (line 141): no docstring

### `backend/oms/routers/branch.py`
- **Functions**
  - `async list_branches(db_name, terminus)` (line 33): 브랜치 목록 조회
  - `async create_branch(db_name, request, terminus, elasticsearch_service)` (line 79): 새 브랜치 생성
  - `async delete_branch(db_name, branch_name, force, terminus, elasticsearch_service)` (line 167): 브랜치 삭제
  - `async checkout(db_name, request, terminus)` (line 253): 브랜치 또는 커밋 체크아웃
  - `async get_branch_info(db_name, branch_name, terminus)` (line 335): 브랜치 정보 조회
  - `async commit_changes(db_name, request, terminus)` (line 392): 브랜치에 변경사항 커밋
- **Classes**
  - `CommitRequest` (line 381): 커밋 요청 모델

### `backend/oms/routers/command_status.py`
- **Functions**
  - `async _fallback_from_registry(command_uuid, registry)` (line 30): no docstring
  - `async get_command_status(command_id, command_status_service, processed_event_registry, event_store)` (line 75): Get command execution status/result.

### `backend/oms/routers/database.py`
- **Functions**
  - `async list_databases(terminus_service)` (line 30): 데이터베이스 목록 조회
  - `async create_database(request, event_store, command_status_service, terminus_service)` (line 53): 새 데이터베이스 생성
  - `async delete_database(db_name, expected_seq, event_store, command_status_service)` (line 187): 데이터베이스 삭제
  - `async database_exists(db_name, terminus_service)` (line 266): 데이터베이스 존재 여부 확인

### `backend/oms/routers/instance.py`
- **Functions**
  - `async get_class_instances(es, db_name, class_id, limit, offset, branch, search)` (line 31): 특정 클래스의 인스턴스 목록을 효율적으로 조회
  - `async get_instance(es, db_name, instance_id, class_id, branch)` (line 127): 개별 인스턴스를 효율적으로 조회
  - `async get_class_instance_count(es, db_name, class_id, branch)` (line 188): 특정 클래스의 인스턴스 개수를 효율적으로 조회
  - `async execute_sparql_query(db_name)` (line 231): SPARQL 엔드포인트 — deprecated (TerminusDB 제거됨).

### `backend/oms/routers/instance_async.py`
- **Functions**
  - `_enforce_ingest_only_if_writeback_enabled(class_id, metadata)` (line 44): no docstring
  - `async _fallback_from_registry(command_uuid, registry)` (line 70): no docstring
  - `_derive_ordering_key(command)` (line 110): no docstring
  - `async _publish_instance_command(command, event_store, command_status_service, actor, status_extra)` (line 119): Append an InstanceCommand to the Event Store + best-effort Redis status.
  - `_derive_instance_id(class_id, payload)` (line 160): Derive a stable instance_id for CREATE_INSTANCE so command/event aggregate_id matches.
  - `async create_instance_async(db_name, class_id, branch, request, terminus, command_status_service, event_store, user_id)` (line 214): 인스턴스 생성 명령을 비동기로 처리
  - `async update_instance_async(db_name, class_id, instance_id, branch, expected_seq, request, terminus, command_status_service, event_store, user_id)` (line 302): 인스턴스 수정 명령을 비동기로 처리
  - `async delete_instance_async(db_name, class_id, instance_id, branch, expected_seq, request, terminus, command_status_service, event_store, user_id)` (line 390): 인스턴스 삭제 명령을 비동기로 처리
  - `async bulk_create_instances_async(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 477): 대량 인스턴스 생성 명령을 비동기로 처리
  - `async bulk_update_instances_async(db_name, class_id, branch, request, terminus, command_status_service, event_store, user_id)` (line 574): 대량 인스턴스 수정 명령을 비동기로 처리
  - `async get_instance_command_status(db_name, command_id, command_status_service, processed_event_registry, event_store)` (line 665): 인스턴스 명령의 상태 조회
  - `async _track_bulk_create_progress(command_id, total_instances, command_status_service)` (line 760): Track progress of bulk create operation in background.
  - `async bulk_create_instances_with_tracking(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 827): Enhanced bulk instance creation with proper background task tracking.
  - `async _process_bulk_create_in_background(task_id, db_name, class_id, branch, instances, metadata, user_id, ontology_version, event_store, command_status_service)` (line 882): Process bulk create operation in background with proper error handling.
- **Classes**
  - `InstanceCreateRequest` (line 182): 인스턴스 생성 요청
  - `InstanceUpdateRequest` (line 188): 인스턴스 수정 요청
  - `InstanceDeleteRequest` (line 194): 인스턴스 삭제 요청 (optional body for metadata/ingest marker).
  - `BulkInstanceCreateRequest` (line 200): 대량 인스턴스 생성 요청
  - `BulkInstanceUpdateRequest` (line 206): 대량 인스턴스 수정 요청

### `backend/oms/routers/ontology.py`
- **Functions**
  - `_is_protected_branch(branch)` (line 85): no docstring
  - `_require_proposal_for_branch(branch)` (line 89): no docstring
  - `_reject_direct_write_if_required(branch)` (line 95): no docstring
  - `_admin_authorized(request)` (line 107): no docstring
  - `_extract_change_reason(request)` (line 117): no docstring
  - `_extract_actor(request)` (line 122): no docstring
  - `async _collect_interface_issues(terminus, db_name, branch, ontology_id, metadata, properties, relationships, resource_service)` (line 127): no docstring
  - `_extract_shared_property_refs(metadata)` (line 166): no docstring
  - `_extract_group_refs(metadata)` (line 192): no docstring
  - `async _validate_group_refs(terminus, db_name, branch, metadata, resource_service)` (line 219): no docstring
  - `async _apply_shared_properties(terminus, db_name, branch, properties, metadata, resource_service)` (line 244): no docstring
  - `async _validate_value_type_refs(terminus, db_name, branch, properties, resource_service)` (line 312): no docstring
  - `_is_internal_ontology(ontology)` (line 369): no docstring
  - `_localized_to_string(value, lang)` (line 378): no docstring
  - `_merge_lint_reports(*reports)` (line 400): no docstring
  - `_relationship_validation_enabled(flag)` (line 422): no docstring
  - `async _validate_relationships_gate(terminus, db_name, branch, ontology_payload, enabled)` (line 429): no docstring
  - `async _ensure_database_exists(db_name, terminus)` (line 455): 데이터베이스 존재 여부 확인 후 404 예외 발생
  - `async create_ontology(ontology_request, request, db_name, branch, terminus, event_store, command_status_service)` (line 485): 내부 ID 기반 온톨로지 생성
  - `async validate_ontology_create(ontology_request, request, db_name, branch, terminus)` (line 800): 온톨로지 생성 검증 (no write).
  - `async validate_ontology_update(ontology_data, request, db_name, class_id, branch, terminus)` (line 906): 온톨로지 업데이트 검증 (no write).
  - `async list_ontologies(db_name, branch, class_type, limit, offset, terminus, label_mapper)` (line 1026): 내부 ID 기반 온톨로지 목록 조회
  - `async analyze_relationship_network(db_name, terminus)` (line 1107): 🔥 관계 네트워크 종합 분석 엔드포인트
  - `async get_ontology(request, db_name, class_id, branch, terminus, converter, label_mapper)` (line 1140): 내부 ID 기반 온톨로지 조회
  - `async update_ontology(ontology_data, request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 1231): 내부 ID 기반 온톨로지 업데이트
  - `async delete_ontology(request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 1569): 내부 ID 기반 온톨로지 삭제
  - `async query_ontologies(query, db_name, branch, terminus)` (line 1688): 내부 ID 기반 온톨로지 쿼리
  - `async create_ontology_with_advanced_relationships(ontology_request, request, db_name, branch, auto_generate_inverse, validate_relationships, check_circular_references, terminus, event_store, command_status_service)` (line 1791): 🔥 고급 관계 관리 기능을 포함한 온톨로지 생성
  - `async validate_ontology_relationships(request, db_name, terminus)` (line 2065): 🔥 온톨로지 관계 검증 전용 엔드포인트
  - `async detect_circular_references(db_name, new_ontology, terminus)` (line 2113): 🔥 순환 참조 탐지 전용 엔드포인트
  - `async find_relationship_paths(start_entity, db_name, end_entity, max_depth, path_type, terminus)` (line 2163): 🔥 관계 경로 탐색 엔드포인트
  - `async get_reachable_entities(start_entity, db_name, max_depth, terminus)` (line 2225): 🔥 도달 가능한 엔티티 조회 엔드포인트

### `backend/oms/routers/ontology_extensions.py`
- **Functions**
  - `async _get_pr_service()` (line 100): no docstring
  - `_normalize_resource_payload(payload)` (line 110): no docstring
  - `_extract_value_type_spec(payload)` (line 130): no docstring
  - `_validate_value_type_immutability(existing, incoming)` (line 138): no docstring
  - `_resource_validation_strict()` (line 157): no docstring
  - `_require_health_gate(branch)` (line 161): no docstring
  - `_ensure_branch_writable(branch)` (line 167): no docstring
  - `async _assert_expected_head_commit(terminus, db_name, branch, expected_head_commit)` (line 179): no docstring
  - `async _compute_ontology_health(db_name, branch, terminus)` (line 219): no docstring
  - `async list_resources(db_name, resource_type, branch, limit, offset, terminus)` (line 472): no docstring
  - `async list_resources_by_type(db_name, resource_type, branch, limit, offset, terminus)` (line 511): no docstring
  - `async create_resource(db_name, resource_type, payload, branch, expected_head_commit, terminus)` (line 531): no docstring
  - `async get_resource(db_name, resource_type, resource_id, branch, terminus)` (line 605): no docstring
  - `async update_resource(db_name, resource_type, resource_id, payload, branch, expected_head_commit, terminus)` (line 647): no docstring
  - `async delete_resource(db_name, resource_type, resource_id, branch, expected_head_commit, terminus)` (line 725): no docstring
  - `async list_ontology_branches(db_name, terminus)` (line 772): no docstring
  - `async create_ontology_branch(db_name, request, terminus)` (line 792): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, pr_service)` (line 818): no docstring
  - `async create_ontology_proposal(db_name, request, pr_service)` (line 842): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, pr_service, terminus)` (line 875): no docstring
  - `async deploy_ontology(db_name, request, pr_service, terminus)` (line 965): no docstring
  - `async ontology_health(db_name, branch, terminus)` (line 1079): no docstring
  - `_validation_result_to_issue(result)` (line 1108): no docstring
  - `_resource_is_referenced(resource_type, resource_id, references)` (line 1118): no docstring
  - `_resource_ref(resource_type, resource_id)` (line 1135): no docstring
  - `_build_issue(code, severity, resource_ref, details, suggested_fix, message, source)` (line 1139): no docstring
  - `_normalize_issue(issue, source)` (line 1160): no docstring
  - `_resolve_relationship_resource_ref(result, class_ids)` (line 1181): no docstring
- **Classes**
  - `OntologyResourceRequest` (line 68): no docstring
  - `OntologyProposalRequest` (line 78): no docstring
  - `OntologyDeployRequest` (line 86): no docstring
  - `OntologyApproveRequest` (line 94): no docstring

### `backend/oms/routers/pull_request.py`
- **Functions**
  - `async get_pr_service()` (line 73): Get PullRequestService instance with MVCC support
  - `async create_pull_request(db_name, request, pr_service)` (line 88): Create a new pull request
  - `async get_pull_request(db_name, pr_id, pr_service)` (line 144): Get pull request details
  - `async list_pull_requests(db_name, status, limit, pr_service)` (line 194): List pull requests for a database
  - `async merge_pull_request(db_name, pr_id, request, pr_service)` (line 249): Merge a pull request
  - `async close_pull_request(db_name, pr_id, request, pr_service)` (line 303): Close a pull request without merging
  - `async get_pull_request_diff(db_name, pr_id, refresh, pr_service)` (line 348): Get diff for a pull request
- **Classes**
  - `PRCreateRequest` (line 31): Pull Request creation request model
  - `PRMergeRequest` (line 52): Pull Request merge request model
  - `PRCloseRequest` (line 67): Pull Request close request model

### `backend/oms/routers/query.py`
- **Functions**
  - `async get_elasticsearch()` (line 42): Get Elasticsearch client
  - `async execute_simple_query(db_name, query, es_client)` (line 66): Execute simple SQL-like query against Elasticsearch
  - `async execute_woql_query(db_name, query, terminus, es_client)` (line 176): Execute WOQL query for graph analysis
  - `async list_instances(db_name, class_id, limit, offset, es_client)` (line 216): List instances of a class from Elasticsearch
- **Classes**
  - `SimpleQuery` (line 31): Simple SQL-like query
  - `WOQLQuery` (line 36): WOQL query for graph analysis

### `backend/oms/routers/tasks.py`
- **Functions**
  - `async get_internal_task_status(task_id, task_manager)` (line 24): Get internal task status for monitoring.
  - `async get_active_tasks(task_manager)` (line 55): Get all active (running) tasks.
  - `async cleanup_old_tasks(older_than_days, task_manager, redis_service)` (line 96): Clean up old completed tasks.
  - `async task_service_health(task_manager)` (line 131): Get task service health status.

### `backend/oms/routers/version.py`
- **Functions**
  - `_rollback_enabled()` (line 79): Rollback is effectively a "force-push/reset" of the ontology graph.
  - `async get_branch_head_commit(db_name, branch, terminus)` (line 95): 브랜치 HEAD 커밋 ID 조회
  - `async create_commit(db_name, request, terminus)` (line 149): 변경사항 커밋
  - `async get_commit_history(db_name, branch, limit, offset, terminus)` (line 211): 커밋 히스토리 조회
  - `async get_diff(db_name, from_ref, to_ref, terminus)` (line 283): 차이점 조회
  - `async merge_branches(db_name, request, terminus)` (line 343): 브랜치 머지
  - `async rollback(db_name, request, audit_store, branch, terminus)` (line 436): 변경사항 롤백
  - `async rebase_branch(db_name, onto, branch, terminus)` (line 621): 브랜치 리베이스
  - `async get_common_ancestor(db_name, branch1, branch2, terminus)` (line 691): 두 브랜치의 공통 조상 찾기
- **Classes**
  - `CommitRequest` (line 34): 커밋 요청
  - `MergeRequest` (line 45): 머지 요청
  - `RollbackRequest` (line 63): 롤백 요청

### `backend/oms/services/__init__.py`

### `backend/oms/services/action_simulation_service.py`
- **Functions**
  - `_enterprise_payload_for_error(error_key)` (line 69): no docstring
  - `_attach_enterprise(payload)` (line 94): no docstring
  - `_assumption_is_forbidden_field(field)` (line 118): no docstring
  - `_extract_link_fields(ops)` (line 127): no docstring
  - `_extract_patch_field_lists(patch)` (line 147): no docstring
  - `_apply_assumption_patch(scope, base_state, patch)` (line 170): no docstring
  - `_apply_observed_base_overrides(observed_base, overrides)` (line 223): no docstring
  - `_coerce_overlay_branch(db_name, writeback_target, overlay_branch)` (line 346): no docstring
  - `async enforce_action_permission(db_name, submitted_by, submitted_by_type, action_spec)` (line 356): no docstring
  - `async _check_writeback_dataset_acl_alignment(db_name, submitted_by, submitted_by_type, actor_role, ontology_commit_id, resources, dataset_registry, class_ids)` (line 422): no docstring
  - `async preflight_action_writeback(terminus, base_storage, dataset_registry, db_name, action_type_id, ontology_commit_id, action_spec, action_type_rid, input_payload, assumptions, submitted_by, submitted_by_type, actor_role, permission_profile, base_branch, overlay_branch)` (line 673): no docstring
  - `build_patchset_for_scenario(preflight, action_log_id, conflict_policy_override)` (line 1258): no docstring
  - `async simulate_effects_for_patchset(base_storage, lakefs_storage, db_name, base_branch, overlay_branch, writeback_repo, writeback_branch, action_log_id, patchset_id, targets, base_overrides_by_target)` (line 1347): no docstring
- **Classes**
  - `ActionSimulationRejected` (line 62): no docstring
    - `__init__(self, payload, status_code)` (line 63): no docstring
  - `ActionSimulationScenario` (line 306): no docstring
  - `TargetPreflight` (line 312): no docstring
  - `ActionPreflight` (line 330): no docstring

### `backend/oms/services/async_terminus.py`
- **Classes**
  - `AtomicUpdateError` (line 51): Base exception for atomic update operations
  - `PatchUpdateError` (line 55): Exception for PATCH-based update failures
  - `TransactionUpdateError` (line 59): Exception for transaction-based update failures
  - `WOQLUpdateError` (line 63): Exception for WOQL-based update failures
  - `BackupCreationError` (line 67): Exception for backup creation failures
  - `RestoreError` (line 71): Exception for restore operation failures
  - `BackupRestoreError` (line 75): Exception for backup and restore operation failures
  - `AsyncTerminusService` (line 94): 비동기 TerminusDB 서비스 클래스 - Clean Facade Pattern
    - `__init__(self, connection_info)` (line 109): 초기화
    - `async check_connection(self)` (line 152): 연결 상태 확인
    - `async connect(self)` (line 166): 연결 설정
    - `async disconnect(self)` (line 172): 연결 해제
    - `async close(self)` (line 176): 모든 서비스 종료
    - `async create_database(self, db_name, description)` (line 203): 데이터베이스 생성
    - `async database_exists(self, db_name)` (line 216): 데이터베이스 존재 여부 확인
    - `async list_databases(self)` (line 221): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 227): 데이터베이스 삭제
    - `async execute_query(self, db_name, query_dict, branch)` (line 236): Query execution (query-spec or raw WOQL passthrough).
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 243): SPARQL 쿼리 직접 실행
    - `async get_class_instances_optimized(self, db_name, class_id, branch, limit, offset, filter_conditions)` (line 258): 특정 클래스의 모든 인스턴스를 효율적으로 조회
    - `async get_instance_optimized(self, db_name, instance_id, branch, class_id)` (line 283): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id, branch, filter_conditions)` (line 299): 특정 클래스의 인스턴스 개수를 효율적으로 조회
    - `async get_ontology(self, db_name, class_id, raise_if_missing, branch)` (line 319): 온톨로지 조회 (branch-aware).
    - `async create_ontology(self, db_name, ontology_data, branch)` (line 341): 온톨로지 생성
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references)` (line 355): no docstring
    - `async update_ontology(self, db_name, class_id, ontology_data, branch)` (line 427): 온톨로지 업데이트 - Atomic 버전
    - `async delete_ontology(self, db_name, class_id, branch)` (line 439): 온톨로지 삭제
    - `async list_ontology_classes(self, db_name)` (line 444): 데이터베이스의 모든 온톨로지 목록 조회
    - `_is_protected_branch_name(branch_name)` (line 454): no docstring
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 458): 브랜치 생성
    - `async list_branches(self, db_name)` (line 464): 브랜치 목록 조회
    - `async get_branch_info(self, db_name, branch_name)` (line 472): no docstring
    - `async get_current_branch(self, db_name)` (line 483): 현재 브랜치 (best-effort, 기본값: main)
    - `async delete_branch(self, db_name, branch_name)` (line 488): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 493): 브랜치 체크아웃
    - `async checkout(self, db_name, target, target_type)` (line 500): Router 호환 checkout (branch/commit).
    - `async merge_branches(self, db_name, source_branch, target_branch, message, author)` (line 508): 브랜치 병합
    - `async commit(self, db_name, message, author, branch)` (line 526): 커밋 생성
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 537): 커밋 히스토리 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 548): 차이점 조회
    - `async merge(self, db_name, source_branch, target_branch, strategy)` (line 553): Router 호환 merge API.
    - `async rebase(self, db_name, onto, branch, message)` (line 570): Router 호환 rebase API (branch -> onto).
    - `async rollback(self, db_name, target)` (line 582): Router 호환 rollback API (reset current branch to target).
    - `async find_common_ancestor(self, db_name, branch1, branch2)` (line 587): 공통 조상 찾기 (현재는 best-effort 미구현).
    - `async create_instance(self, db_name, class_id, instance_data, branch)` (line 598): 인스턴스 생성
    - `async update_instance(self, db_name, class_id, instance_id, update_data, branch)` (line 610): 인스턴스 업데이트
    - `async delete_instance(self, db_name, class_id, instance_id, branch)` (line 625): 인스턴스 삭제
    - `async validate_relationships(self, db_name, ontology_data, branch, fix_issues)` (line 641): Validate ontology relationships against current schema (no write).
    - `async detect_circular_references(self, db_name, branch, include_new_ontology, max_cycle_depth)` (line 719): Detect circular references across ontology relationship graph (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch)` (line 792): Find relationship paths between entities in the ontology graph (no write).
    - `async analyze_relationship_network(self, db_name, branch)` (line 870): Analyze relationship network health and statistics (no write).
    - `convert_properties_to_relationships(self, ontology)` (line 965): 속성을 관계로 변환
    - `clear_cache(self, db_name)` (line 976): 캐시 초기화
    - `async ping(self)` (line 984): 서버 연결 상태 확인
    - `get_connection_info(self)` (line 994): 현재 연결 정보 반환
    - `async __aenter__(self)` (line 998): Async context manager entry
    - `async __aexit__(self, _exc_type, _exc_val, _exc_tb)` (line 1002): Async context manager exit

### `backend/oms/services/event_store.py`

### `backend/oms/services/ontology_deploy_outbox.py`
- **Functions**
  - `async run_ontology_deploy_outbox_worker(registry, poll_interval_seconds, batch_size, stop_event)` (line 104): no docstring
- **Classes**
  - `OntologyDeployOutboxPublisher` (line 24): no docstring
    - `__init__(self, registry, batch_size)` (line 25): no docstring
    - `async flush_once(self)` (line 51): no docstring
    - `async maybe_purge(self)` (line 90): no docstring

### `backend/oms/services/ontology_deploy_outbox_store.py`
- **Classes**
  - `OntologyDeployOutboxItem` (line 12): no docstring
  - `OntologyDeployOutboxTableSpec` (line 27): no docstring
  - `OntologyDeployOutboxStore` (line 33): Outbox operations (Strategy via table/column spec).
    - `__init__(self, table)` (line 42): no docstring
    - `async claim_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 46): no docstring
    - `async mark_published(self, outbox_id)` (line 123): no docstring
    - `async mark_failed(self, outbox_id, error, next_attempt_at)` (line 141): no docstring

### `backend/oms/services/ontology_deployment_registry.py`
- **Classes**
  - `OntologyDeploymentRegistry` (line 31): Record ontology deployments in Postgres.
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, occurred_at)` (line 71): no docstring
    - `async record_deployment(self, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, status, metadata)` (line 104): no docstring

### `backend/oms/services/ontology_deployment_registry_base.py`
- **Classes**
  - `BaseOntologyDeploymentRegistry` (line 29): no docstring
    - `async ensure_schema(self)` (line 36): no docstring
    - `async _ensure_indexes(self)` (line 42): no docstring
    - `build_common_event_payload(deployment_id, target_branch, ontology_commit_id, deployed_by, data, occurred_at)` (line 55): no docstring
    - `async claim_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 85): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 102): no docstring
    - `async mark_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 106): no docstring
    - `async purge_outbox(self, retention_days, limit)` (line 116): no docstring
    - `_normalize_claimed_payload(self, payload)` (line 141): no docstring
    - `_require_schema_configuration(self)` (line 144): no docstring
    - `_require_outbox_store(self)` (line 150): no docstring

### `backend/oms/services/ontology_deployment_registry_v2.py`
- **Classes**
  - `OntologyDeploymentRegistryV2` (line 33): Record ontology deployments in Postgres (v2 schema).
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, target_branch, ontology_commit_id, snapshot_rid, deployed_by, gate_policy, health_summary, occurred_at)` (line 74): no docstring
    - `async record_deployment(self, db_name, target_branch, ontology_commit_id, snapshot_rid, proposal_id, status, gate_policy, health_summary, deployed_by, error, metadata)` (line 107): no docstring
    - `_normalize_claimed_payload(self, payload)` (line 194): no docstring
    - `async get_latest_deployed_commit(self, db_name, target_branch)` (line 199): Return the latest succeeded deployment record for a db/branch.

### `backend/oms/services/ontology_health_issue_registry.py`
- **Functions**
  - `build_object_type_ref(object_id)` (line 101): no docstring
  - `build_link_type_ref(link_id)` (line 105): no docstring
  - `build_ontology_resource_ref(resource_type, resource_id)` (line 109): no docstring
  - `normalize_issue_code(code, source)` (line 117): no docstring
  - `normalize_severity(value)` (line 127): no docstring
  - `normalize_issue(code, severity, resource_ref, details, suggested_fix, message, source)` (line 135): no docstring
  - `_build_resource_ref(kind, resource_id)` (line 166): no docstring
  - `_enforce_details_schema(code, details)` (line 171): no docstring

### `backend/oms/services/ontology_interface_contract.py`
- **Functions**
  - `extract_interface_refs(metadata)` (line 13): no docstring
  - `strip_interface_prefix(value)` (line 34): no docstring
  - `collect_interface_contract_issues(ontology_id, metadata, properties, relationships, interface_index)` (line 43): no docstring
  - `extract_required_entries(items, name_keys)` (line 182): no docstring
  - `extract_entry_value(entry, keys)` (line 204): no docstring
  - `build_property_map(items)` (line 214): no docstring
  - `build_relationship_map(items)` (line 227): no docstring
  - `extract_property_type(item)` (line 240): no docstring
  - `extract_relationship_target(item)` (line 250): no docstring
  - `normalize_reference_value(value)` (line 260): no docstring

### `backend/oms/services/ontology_resource_validator.py`
- **Functions**
  - `_normalize_spec(spec)` (line 123): no docstring
  - `_merge_payload_spec(payload)` (line 136): no docstring
  - `_extract_reference_values(value, keys, parent_is_ref)` (line 147): no docstring
  - `collect_reference_values(spec)` (line 160): no docstring
  - `check_required_fields(resource_type, spec)` (line 164): no docstring
  - `async find_missing_references(db_name, resource_type, payload, terminus, branch)` (line 170): no docstring
  - `_canonicalize_ref(raw)` (line 206): no docstring
  - `_is_primitive_reference(value)` (line 216): no docstring
  - `_strip_object_ref(raw)` (line 230): no docstring
  - `_collect_link_type_issues(spec)` (line 245): no docstring
  - `_collect_relationship_spec_issues(spec)` (line 268): no docstring
  - `async _find_missing_link_type_refs(terminus, db_name, branch, spec)` (line 340): no docstring
  - `_validate_required_fields(resource_type, spec)` (line 359): no docstring
  - `_collect_required_field_issues(resource_type, spec)` (line 365): no docstring
  - `_collect_required_items_issues(items, item_name, name_keys)` (line 810): no docstring
  - `_collect_permission_policy_issues(policy)` (line 848): no docstring
  - `_validate_string_list(value, field_name)` (line 940): no docstring
  - `_append_spec_issue(issues, message, missing_fields, invalid_fields)` (line 950): no docstring
  - `async _reference_exists(terminus, resources, db_name, branch, ref_type, ref)` (line 969): no docstring
  - `async validate_resource(db_name, resource_type, payload, terminus, branch, expected_head_commit, strict)` (line 1012): no docstring
- **Classes**
  - `ResourceSpecError` (line 27): Raised when resource spec is invalid or missing required fields.
  - `ResourceReferenceError` (line 31): Raised when resource spec references missing entities.

### `backend/oms/services/ontology_resources.py`
- **Functions**
  - `normalize_resource_type(value)` (line 51): no docstring
  - `_resource_doc_id(resource_type, resource_id)` (line 61): no docstring
  - `_localized_to_string(value)` (line 66): no docstring
- **Classes**
  - `OntologyResourceService` (line 91): CRUD for ontology resource instances stored in TerminusDB.
    - `__init__(self, terminus)` (line 94): no docstring
    - `async ensure_resource_schema(self, db_name, branch)` (line 99): no docstring
    - `async create_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 137): no docstring
    - `async update_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 166): no docstring
    - `async delete_resource(self, db_name, branch, resource_type, resource_id)` (line 204): no docstring
    - `async get_resource(self, db_name, branch, resource_type, resource_id)` (line 223): no docstring
    - `async list_resources(self, db_name, branch, resource_type, limit, offset)` (line 238): no docstring
    - `_payload_to_document(self, resource_type, resource_id, payload, doc_id, is_create, existing)` (line 266): no docstring
    - `_document_to_payload(self, doc)` (line 331): no docstring

### `backend/oms/services/property_to_relationship_converter.py`
- **Classes**
  - `PropertyToRelationshipConverter` (line 14): Property를 Relationship으로 자동 변환하는 컨버터
    - `__init__(self)` (line 22): no docstring
    - `process_class_data(self, class_data)` (line 25): 클래스 데이터를 처리하여 property를 relationship으로 자동 변환
    - `detect_class_references(self, properties)` (line 109): 속성 목록에서 클래스 참조를 감지
    - `validate_class_references(self, class_data, existing_classes)` (line 125): 클래스 참조의 유효성 검증
    - `generate_inverse_relationships(self, class_data)` (line 155): 자동 변환된 관계에 대한 역관계 생성 정보
    - `_inverse_cardinality(self, cardinality)` (line 181): 카디널리티 역변환

### `backend/oms/services/pull_request_service.py`
- **Classes**
  - `PullRequestStatus` (line 34): PR status constants
  - `PullRequestService` (line 41): Pull Request management service following SRP
    - `__init__(self, mvcc_manager, *args, **kwargs)` (line 57): Initialize PullRequestService
    - `async create_pull_request(self, db_name, source_branch, target_branch, title, description, author)` (line 72): Create a new pull request
    - `async get_branch_diff(self, db_name, source_branch, target_branch)` (line 176): Get diff between two branches using TerminusDB diff API
    - `async check_merge_conflicts(self, db_name, source_branch, target_branch)` (line 205): Check for potential merge conflicts
    - `async merge_pull_request(self, pr_id, merge_message, author)` (line 255): Merge a pull request using rebase strategy
    - `async get_pull_request(self, pr_id)` (line 359): Get pull request details
    - `async list_pull_requests(self, db_name, status, limit)` (line 409): List pull requests with optional filters
    - `async close_pull_request(self, pr_id, reason)` (line 463): Close a pull request without merging

### `backend/oms/services/relationship_manager.py`
- **Classes**
  - `RelationshipPair` (line 16): 관계 쌍 (정방향 + 역방향)
  - `RelationshipManager` (line 24): 🔥 THINK ULTRA! 고급 관계 관리자
    - `__init__(self)` (line 35): no docstring
    - `create_bidirectional_relationship(self, source_class, relationship, auto_generate_inverse)` (line 51): 양방향 관계 생성
    - `_generate_inverse_relationship(self, source_class, target_class, forward_relationship)` (line 89): 역관계 자동 생성
    - `_get_inverse_cardinality(self, cardinality)` (line 125): 카디널리티의 역관계 계산
    - `_generate_inverse_predicate(self, predicate)` (line 142): predicate의 역관계명 자동 생성
    - `_generate_inverse_label(self, explicit_inverse_label, forward_label, predicate)` (line 187): 역관계 레이블 생성
    - `_invert_label_text(self, text)` (line 201): 레이블 텍스트의 역관계 표현 생성
    - `_generate_inverse_description(self, forward_description, source_class, target_class)` (line 235): 역관계 설명 생성
    - `_validate_and_normalize_relationship(self, relationship)` (line 246): 관계 검증 및 정규화
    - `_normalize_cardinality(self, cardinality)` (line 269): 카디널리티 정규화
    - `_validate_relationship_pair(self, forward, inverse)` (line 279): 관계 쌍 검증
    - `detect_relationship_conflicts(self, relationships)` (line 307): 관계 충돌 감지
    - `generate_relationship_summary(self, relationships)` (line 325): 관계 요약 정보 생성

### `backend/oms/services/terminus/__init__.py`

### `backend/oms/services/terminus/base.py`
- **Classes**
  - `BaseTerminusService` (line 31): TerminusDB 기본 서비스
    - `__init__(self, connection_info)` (line 39): TerminusDB 서비스 초기화
    - `async _get_client(self)` (line 63): HTTP 클라이언트 가져오기 (lazy initialization)
    - `_branch_descriptor(self, branch)` (line 93): Build the TerminusDB descriptor path for a branch (or commit).
    - `async _authenticate(self)` (line 125): TerminusDB 인증 토큰 획득
    - `async _make_request(self, method, endpoint, data, params, headers, **kwargs)` (line 142): HTTP 요청 실행
    - `async connect(self, db_name)` (line 269): TerminusDB 연결
    - `async disconnect(self)` (line 286): 연결 종료
    - `async check_connection(self)` (line 295): 연결 상태 확인
    - `async __aenter__(self)` (line 304): 비동기 컨텍스트 매니저 진입
    - `async __aexit__(self, _exc_type, _exc_val, _exc_tb)` (line 309): 비동기 컨텍스트 매니저 종료
    - `is_connected(self)` (line 313): 연결 상태 반환

### `backend/oms/services/terminus/database.py`
- **Classes**
  - `DatabaseService` (line 20): TerminusDB 데이터베이스 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 27): no docstring
    - `async database_exists(self, db_name)` (line 32): 데이터베이스 존재 여부 확인
    - `async ensure_db_exists(self, db_name, description)` (line 45): 데이터베이스 존재 확인 및 생성
    - `async create_database(self, db_name, description)` (line 69): 새 데이터베이스 생성
    - `async list_databases(self)` (line 110): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 202): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 234): 데이터베이스 상세 정보 조회
    - `clear_cache(self)` (line 263): 데이터베이스 캐시 초기화

### `backend/oms/services/terminus/db_backed.py`
- **Classes**
  - `DatabaseBackedTerminusService` (line 14): Base class for Terminus services that compose a nested DatabaseService.
    - `__init__(self, *args, **kwargs)` (line 17): no docstring
    - `async disconnect(self)` (line 21): no docstring

### `backend/oms/services/terminus/document.py`
- **Classes**
  - `DocumentService` (line 19): TerminusDB 문서(인스턴스) 관리 서비스
    - `async create_document(self, db_name, document, graph_type, branch, author, message)` (line 26): 새 문서 생성
    - `async update_document(self, db_name, doc_id, document, graph_type, branch, author, message)` (line 97): 문서 업데이트
    - `async delete_document(self, db_name, doc_id, graph_type, branch, author, message)` (line 162): 문서 삭제
    - `async get_document(self, db_name, doc_id, graph_type, branch)` (line 217): 특정 문서 조회
    - `async list_documents(self, db_name, graph_type, branch, doc_type, limit, offset)` (line 261): 문서 목록 조회
    - `async document_exists(self, db_name, doc_id, graph_type, branch)` (line 318): 문서 존재 여부 확인
    - `async bulk_create_documents(self, db_name, documents, graph_type, branch, author, message)` (line 351): 여러 문서를 한 번에 생성
    - `async bulk_update_documents(self, db_name, documents, graph_type, branch, author, message)` (line 409): 여러 문서를 한 번에 업데이트
    - `async bulk_delete_documents(self, db_name, doc_ids, graph_type, branch, author, message)` (line 460): 여러 문서를 한 번에 삭제
    - `async search_documents(self, db_name, search_query, graph_type, branch, doc_type, limit)` (line 508): 문서 검색
    - `async create_instance(self, db_name, class_id, instance_data, branch)` (line 579): Create an instance document (alias for create_document)
    - `async update_instance(self, db_name, class_id, instance_id, update_data, branch)` (line 614): Update an instance document (alias for update_document)
    - `async delete_instance(self, db_name, class_id, instance_id, branch)` (line 648): Delete an instance document (alias for delete_document)

### `backend/oms/services/terminus/instance.py`
- **Classes**
  - `InstanceService` (line 16): TerminusDB 인스턴스 관리 서비스
    - `async get_class_instances_optimized(self, db_name, class_id, branch, limit, offset, search)` (line 23): 특정 클래스의 인스턴스 목록을 효율적으로 조회
    - `async get_instance_optimized(self, db_name, instance_id, branch, class_id)` (line 137): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id, branch, filter_conditions)` (line 237): 특정 클래스의 인스턴스 개수를 효율적으로 조회

### `backend/oms/services/terminus/ontology.py`
- **Classes**
  - `OntologyService` (line 24): TerminusDB 온톨로지 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 31): no docstring
    - `async disconnect(self)` (line 35): no docstring
    - `async _overlay_key_spec(self, db_name, branch, ontology)` (line 44): Overlay ordered key spec from Postgres registry into an ontology response.
    - `async create_ontology(self, db_name, ontology, branch)` (line 97): 새 온톨로지(클래스) 생성
    - `async update_ontology(self, db_name, ontology_id, ontology, branch)` (line 264): 온톨로지 업데이트
    - `async delete_ontology(self, db_name, ontology_id, branch)` (line 299): 온톨로지 삭제
    - `async get_ontology(self, db_name, ontology_id, branch)` (line 342): 특정 온톨로지 조회
    - `async list_ontologies(self, db_name, branch, limit, offset)` (line 385): 데이터베이스의 모든 온톨로지 목록 조회
    - `async ontology_exists(self, db_name, ontology_id, branch)` (line 491): 온톨로지 존재 여부 확인
    - `_create_property_schema(self, prop)` (line 521): 속성 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_create_relationship_schema(self, rel)` (line 576): 관계 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_map_datatype_to_terminus(self, datatype)` (line 641): DataType을 TerminusDB 타입으로 매핑
    - `_parse_ontology_document(self, doc)` (line 710): TerminusDB 문서를 OntologyResponse로 파싱
    - `_map_terminus_to_datatype(self, terminus_type)` (line 881): TerminusDB 타입을 DataType으로 매핑

### `backend/oms/services/terminus/query.py`
- **Classes**
  - `QueryService` (line 18): TerminusDB 쿼리 실행 서비스
    - `_normalize_field_name(field)` (line 26): no docstring
    - `_coerce_scalar(value)` (line 36): no docstring
    - `_try_float(value)` (line 42): no docstring
    - `_matches_filters(self, doc, filters)` (line 54): no docstring
    - `async execute_query(self, db_name, query_dict, branch)` (line 146): Execute a query against instance documents.
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 306): SPARQL 쿼리 실행
    - `convert_to_woql(self, query_dict)` (line 377): 간단한 쿼리 딕셔너리를 WOQL 형식으로 변환

### `backend/oms/services/terminus/version_control.py`
- **Classes**
  - `VersionControlService` (line 20): TerminusDB 버전 관리 서비스
    - `_is_origin_branch_missing_error(exc)` (line 31): TerminusDB can transiently report `OriginBranchDoesNotExist` right after DB creation.
    - `_is_branch_already_exists_error(exc)` (line 40): no docstring
    - `_is_transient_request_handler_error(exc)` (line 49): TerminusDB occasionally returns a 500 "Unexpected failure in request handler" right after DB creation.
    - `_is_merge_endpoint_missing_error(exc)` (line 64): no docstring
    - `async _get_branch_head(self, db_name, branch_name)` (line 75): no docstring
    - `async _get_branch_head_with_retries(self, db_name, branch_name, max_attempts)` (line 92): no docstring
    - `async list_branches(self, db_name)` (line 102): 데이터베이스의 모든 브랜치 목록 조회
    - `async create_branch(self, db_name, branch_name, source_branch, empty)` (line 150): 새 브랜치 생성
    - `async delete_branch(self, db_name, branch_name)` (line 225): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 261): TerminusDB는 stateless HTTP API이므로 'checkout'은 서버 상태를 바꾸지 않습니다.
    - `async reset_branch(self, db_name, branch_name, commit_id)` (line 271): 브랜치를 특정 커밋으로 리셋
    - `async get_commits(self, db_name, branch_name, limit, offset)` (line 320): 브랜치의 커밋 이력 조회
    - `async create_commit(self, db_name, branch_name, message, author)` (line 392): 새 커밋 생성
    - `async rebase_branch(self, db_name, branch_name, target_branch, message)` (line 470): 브랜치 리베이스
    - `async rebase(self, db_name, branch, onto, message)` (line 520): Router 호환 rebase API (branch -> onto).
    - `async squash_commits(self, db_name, branch_name, commit_id, message)` (line 531): 커밋 스쿼시 (여러 커밋을 하나로 합치기)
    - `async get_diff(self, db_name, from_ref, to_ref)` (line 575): 두 참조(브랜치/커밋) 간의 차이점 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 625): Router 호환 diff API: changes(리스트/딕트)만 반환.
    - `async commit(self, db_name, message, author, branch)` (line 632): Router 호환 commit API: commit_id 문자열 반환.
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 644): Router 호환 commit history API.
    - `async merge(self, db_name, source_branch, target_branch, strategy, author, message)` (line 654): Router 호환 merge API.
    - `_parse_commit_info(self, commit_data)` (line 732): 커밋 정보 파싱

### `backend/oms/utils/__init__.py`

### `backend/oms/utils/cardinality_utils.py`
- **Functions**
  - `inverse_cardinality(cardinality)` (line 14): no docstring

### `backend/oms/utils/circular_reference_detector.py`
- **Classes**
  - `CycleType` (line 18): 순환 참조 유형
  - `CycleInfo` (line 28): 순환 참조 정보
  - `RelationshipEdge` (line 41): 관계 그래프의 엣지
  - `CircularReferenceDetector` (line 51): 🔥 THINK ULTRA! 순환 참조 탐지기
    - `__init__(self, max_cycle_depth)` (line 64): 초기화
    - `build_relationship_graph(self, ontologies)` (line 94): 온톨로지들로부터 관계 그래프 구축
    - `detect_all_cycles(self)` (line 142): 모든 순환 참조 탐지
    - `detect_cycle_for_new_relationship(self, source, target, predicate)` (line 169): 새로운 관계 추가 시 발생할 수 있는 순환 참조 탐지
    - `_detect_self_references(self)` (line 232): 자기 참조 탐지
    - `_detect_direct_cycles(self)` (line 255): 직접 순환 탐지 (A -> B -> A)
    - `_detect_indirect_cycles(self)` (line 285): 간접 순환 탐지 (A -> B -> C -> A)
    - `_detect_complex_cycles(self)` (line 309): 복잡한 다중 경로 순환 탐지
    - `_dfs_cycle_detection(self, current, start, visited, path_stack, predicate_stack, depth)` (line 336): DFS를 사용한 순환 탐지
    - `_find_strongly_connected_components(self)` (line 389): Tarjan 알고리즘을 사용한 강하게 연결된 컴포넌트 탐지
    - `_find_representative_cycle(self, scc)` (line 430): SCC 내의 대표 순환 경로 찾기
    - `_find_paths_between(self, start, end, max_depth)` (line 465): 두 노드 간의 모든 경로 찾기 (최대 깊이 제한)
    - `_get_path_edges(self, path)` (line 491): 경로의 엣지 목록 반환
    - `_assess_self_reference_severity(self, predicate)` (line 503): 자기 참조의 심각도 평가
    - `_assess_direct_cycle_severity(self, edge1, edge2)` (line 513): 직접 순환의 심각도 평가
    - `_assess_cycle_severity(self, path, predicates)` (line 526): 순환의 심각도 평가
    - `_can_break_cycle(self, predicates)` (line 537): 순환을 끊을 수 있는지 판별
    - `_deduplicate_cycles(self, cycles)` (line 543): 중복 순환 제거
    - `_sort_cycles_by_severity(self, cycles)` (line 558): 심각도별로 순환 정렬
    - `suggest_cycle_resolution(self, cycle)` (line 567): 순환 해결 방안 제안
    - `get_cycle_analysis_report(self, cycles)` (line 593): 순환 분석 보고서 생성
    - `_generate_recommendations(self, cycles)` (line 613): 전체 권장사항 생성

### `backend/oms/utils/command_status_utils.py`
- **Functions**
  - `map_registry_status(status_value)` (line 6): no docstring

### `backend/oms/utils/constraint_extractor.py`
- **Functions**
  - `extract_property_constraints(property_data)` (line 364): 속성 제약조건 추출 편의 함수
  - `extract_relationship_constraints(relationship_data)` (line 370): 관계 제약조건 추출 편의 함수
  - `extract_all_constraints(class_data)` (line 376): 모든 제약조건 추출 편의 함수
- **Classes**
  - `ConstraintType` (line 16): 제약조건 타입들
  - `DefaultValueType` (line 52): 기본값 타입들
  - `ConstraintExtractor` (line 63): 제약조건 및 기본값 추출기
    - `__init__(self)` (line 66): no docstring
    - `extract_property_constraints(self, property_data)` (line 69): 속성에서 제약조건 추출
    - `extract_relationship_constraints(self, relationship_data)` (line 142): 관계에서 제약조건 추출
    - `extract_default_value(self, field_data)` (line 182): 필드에서 기본값 정보 추출
    - `validate_constraint_compatibility(self, constraints, field_type)` (line 215): 제약조건과 필드 타입의 호환성 검증
    - `extract_all_constraints(self, class_data)` (line 278): 클래스 데이터에서 모든 제약조건 추출
    - `generate_constraint_summary(self, all_constraints)` (line 319): 제약조건 요약 생성

### `backend/oms/utils/deprecation.py`
- **Functions**
  - `deprecated(reason, version, alternative, removal_version)` (line 13): Decorator to mark functions/methods as deprecated.
  - `_issue_deprecation_warning(func, reason, version, alternative, removal_version)` (line 78): Issue a deprecation warning with detailed information.
  - `legacy_api(reason)` (line 103): Decorator for legacy APIs that are kept for backward compatibility.
  - `experimental(feature)` (line 128): Decorator for experimental features that may change.

### `backend/oms/utils/ontology_stamp.py`
- **Functions**
  - `merge_ontology_stamp(existing, resolved)` (line 8): no docstring

### `backend/oms/utils/relationship_path_tracker.py`
- **Classes**
  - `PathType` (line 19): 경로 유형
  - `TraversalDirection` (line 28): 탐색 방향
  - `RelationshipHop` (line 37): 관계 홉 (한 단계 관계)
  - `RelationshipPath` (line 50): 관계 경로
    - `entities(self)` (line 63): 경로상의 모든 엔티티 반환
    - `predicates(self)` (line 74): 경로상의 모든 predicate 반환
    - `to_readable_string(self)` (line 78): 읽기 쉬운 경로 문자열 반환
  - `PathQuery` (line 92): 경로 탐색 쿼리
  - `RelationshipPathTracker` (line 107): 🔥 THINK ULTRA! 관계 경로 추적기
    - `__init__(self)` (line 120): no docstring
    - `build_graph(self, ontologies)` (line 160): 온톨로지들로부터 관계 그래프 구축
    - `find_paths(self, query)` (line 232): 경로 탐색 (쿼리 기반)
    - `find_shortest_path(self, start, end, max_depth)` (line 263): 최단 경로 탐색 (단순 버전)
    - `find_all_reachable_entities(self, start, max_depth)` (line 275): 시작 엔티티에서 도달 가능한 모든 엔티티와 경로
    - `find_connecting_entities(self, entity1, entity2, max_depth)` (line 312): 두 엔티티를 연결하는 중간 엔티티들 탐색
    - `_find_shortest_paths(self, query)` (line 326): Dijkstra 알고리즘으로 최단 경로 탐색
    - `_find_all_paths(self, query)` (line 372): DFS로 모든 경로 탐색
    - `_find_weighted_paths(self, query)` (line 413): 가중치 기반 최적 경로 탐색
    - `_find_semantic_paths(self, query)` (line 428): 의미적 관련성 기반 경로 탐색
    - `_is_hop_allowed(self, hop, query)` (line 440): 홉이 쿼리 조건에 맞는지 확인
    - `_calculate_hop_weight(self, hop, query)` (line 461): 홉의 가중치 계산
    - `_calculate_semantic_score(self, path, query)` (line 476): 경로의 의미적 점수 계산
    - `get_path_statistics(self, paths)` (line 505): 경로 통계 정보
    - `_find_common_predicates(self, paths)` (line 523): 경로들에서 공통으로 사용되는 predicate 찾기
    - `visualize_path(self, path, format)` (line 539): 경로 시각화
    - `export_graph_summary(self)` (line 570): 그래프 요약 정보 내보내기
    - `_get_cardinality_distribution(self)` (line 588): 카디널리티 분포 통계

### `backend/oms/utils/terminus_retry.py`
- **Functions**
  - `build_async_retry(retry_exceptions, backoff, logger, on_failure)` (line 14): no docstring

### `backend/oms/utils/terminus_schema_types.py`
- **Functions**
  - `create_basic_class_schema(class_id, key_type)` (line 445): 기본 클래스 스키마 빌더 생성
  - `create_subdocument_schema(class_id)` (line 450): 서브문서 스키마 빌더 생성
  - `convert_simple_schema(class_data)` (line 455): 간단한 스키마 데이터를 TerminusDB 형식으로 변환
- **Classes**
  - `TerminusSchemaType` (line 16): TerminusDB v11.x 지원 스키마 타입들
  - `TerminusSchemaBuilder` (line 56): TerminusDB 스키마 구조를 생성하는 빌더 클래스
    - `__init__(self)` (line 59): no docstring
    - `set_class(self, class_id, key_type)` (line 62): 기본 클래스 설정
    - `set_subdocument(self)` (line 71): 서브 도큐먼트로 설정
    - `add_string_property(self, name, optional)` (line 76): 문자열 속성 추가
    - `add_integer_property(self, name, optional)` (line 84): 정수 속성 추가
    - `add_boolean_property(self, name, optional)` (line 92): 불리언 속성 추가
    - `add_datetime_property(self, name, optional)` (line 100): 날짜시간 속성 추가
    - `add_date_property(self, name, optional)` (line 108): 날짜 속성 추가
    - `add_list_property(self, name, element_type, optional)` (line 116): 리스트 속성 추가
    - `add_set_property(self, name, element_type, optional)` (line 124): 셋 속성 추가
    - `add_array_property(self, name, element_type, dimensions, optional)` (line 134): 배열 속성 추가
    - `add_class_reference(self, name, target_class, optional)` (line 147): 다른 클래스 참조 추가
    - `add_enum_property(self, name, enum_values, optional)` (line 155): Enum 속성 추가
    - `add_foreign_property(self, name, foreign_type, optional)` (line 163): Foreign 키 속성 추가
    - `add_one_of_type(self, name, type_options, optional)` (line 171): OneOfType 속성 추가 (Union type)
    - `add_geopoint_property(self, name, optional)` (line 185): 지리적 좌표 속성 추가
    - `add_documentation(self, comment, description)` (line 195): 문서화 정보 추가
    - `build(self)` (line 206): 완성된 스키마 반환
  - `TerminusSchemaConverter` (line 211): 기존 스키마 데이터를 TerminusDB 형식으로 변환하는 클래스
    - `convert_property_type(prop_type, constraints)` (line 215): 속성 타입을 TerminusDB 형식으로 변환
    - `convert_relationship_cardinality(cardinality)` (line 330): 관계 카디널리티를 TerminusDB 형식으로 변환
    - `convert_complex_type(type_config)` (line 348): 복잡한 타입 구성을 변환
  - `TerminusConstraintProcessor` (line 395): TerminusDB 제약조건 처리 클래스
    - `extract_constraints_for_validation(constraints)` (line 399): 스키마 제약조건에서 런타임 검증용 제약조건 추출
    - `apply_schema_level_constraints(schema, constraints)` (line 426): 스키마 레벨에서 적용 가능한 제약조건 적용

### `backend/oms/validation_codes.py`
- **Classes**
  - `OntologyValidationCode` (line 6): Issue codes used by OMS ontology validation flows.

### `backend/oms/validators/__init__.py`

### `backend/oms/validators/relationship_validator.py`
- **Classes**
  - `ValidationSeverity` (line 16): 검증 결과 심각도
  - `ValidationResult` (line 25): 검증 결과
  - `RelationshipValidator` (line 35): 🔥 THINK ULTRA! 고급 관계 검증기
    - `__init__(self, existing_ontologies)` (line 47): no docstring
    - `validate_relationship(self, relationship, source_class)` (line 77): 단일 관계 검증
    - `validate_relationship_pair(self, forward, inverse, source_class, target_class)` (line 104): 관계 쌍 검증 (정방향 + 역방향)
    - `validate_ontology_relationships(self, ontology)` (line 128): 온톨로지 전체 관계 검증
    - `validate_multiple_ontologies(self, ontologies)` (line 148): 다중 온톨로지 간 관계 검증
    - `_validate_basic_fields(self, relationship)` (line 167): 기본 필드 검증
    - `_validate_predicate(self, predicate)` (line 204): predicate 명명 규칙 검증
    - `_validate_cardinality(self, cardinality)` (line 259): 카디널리티 검증
    - `_validate_target_class(self, target)` (line 293): 타겟 클래스 검증
    - `_validate_self_reference(self, relationship, source_class)` (line 330): 자기 참조 검증
    - `_validate_labels(self, relationship)` (line 362): 레이블 검증
    - `_validate_cardinality_consistency(self, forward, inverse)` (line 381): 카디널리티 일관성 검증
    - `_validate_mutual_reference(self, forward, inverse)` (line 427): 상호 참조 검증
    - `_validate_target_consistency(self, forward, inverse, source_class, target_class)` (line 460): 타겟 일관성 검증
    - `_validate_relationship_conflicts(self, relationships)` (line 493): 관계 간 충돌 검증
    - `_validate_predicate_uniqueness(self, relationships)` (line 518): predicate 유일성 검증
    - `_validate_relationship_network(self, ontology)` (line 541): 관계 네트워크 검증
    - `_validate_cross_ontology_relationships(self, ontologies)` (line 564): 온톨로지 간 관계 검증
    - `_validate_global_relationship_consistency(self, ontologies)` (line 589): 전역 관계 일관성 검증
    - `get_validation_summary(self, results)` (line 617): 검증 결과 요약

## ontology_worker

### `backend/ontology_worker/__init__.py`

### `backend/ontology_worker/main.py`
- **Classes**
  - `_OntologyCommandPayload` (line 68): no docstring
  - `_OntologyCommandParseError` (line 73): no docstring
  - `OntologyWorker` (line 77): 온톨로지 Command를 처리하는 워커
    - `__init__(self)` (line 84): no docstring
    - `_extract_key_spec_from_payload(payload)` (line 135): Extract ordered primary/title keys from an ontology payload.
    - `async _wait_for_database_exists(self, db_name, expected, timeout_seconds)` (line 172): no docstring
    - `async initialize(self)` (line 188): 워커 초기화
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 265): no docstring
    - `async process_command(self, command_data)` (line 293): Command 처리
    - `async handle_create_ontology(self, command_data)` (line 338): 온톨로지 생성 처리
    - `async handle_update_ontology(self, command_data)` (line 532): 온톨로지 업데이트 처리
    - `async handle_delete_ontology(self, command_data)` (line 718): 온톨로지 삭제 처리
    - `async handle_create_database(self, command_data)` (line 880): 데이터베이스 생성 처리
    - `async handle_delete_database(self, command_data)` (line 948): 데이터베이스 삭제 처리
    - `_to_domain_envelope(self, event, kafka_topic)` (line 999): no docstring
    - `async publish_event(self, event)` (line 1047): 이벤트 발행 (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka).
    - `async publish_failure_event(self, command_data, error)` (line 1059): 실패 이벤트 발행
    - `_parse_payload(self, payload)` (line 1081): no docstring
    - `_fallback_metadata(self, payload)` (line 1157): no docstring
    - `_registry_key(self, payload)` (line 1160): no docstring
    - `async _process_payload(self, payload)` (line 1181): no docstring
    - `_span_name(self, payload)` (line 1185): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 1188): no docstring
    - `_is_retryable_error(exc, payload)` (line 1211): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 1214): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 1248): no docstring
    - `async run(self)` (line 1276): 메인 실행 루프
    - `async shutdown(self)` (line 1289): 워커 종료

## perf

### `backend/perf/cleanup_perf_databases.py`
- **Functions**
  - `_postgres_dsn_candidates()` (line 29): no docstring
  - `_bff_base_url()` (line 34): no docstring
  - `_admin_token()` (line 38): no docstring
  - `async _connect_postgres()` (line 44): no docstring
  - `async _fetch_db_expected_seq(conn, db_name)` (line 57): no docstring
  - `_matches_any_prefix(name, prefixes)` (line 75): no docstring
  - `async main()` (line 82): no docstring

## pipeline_scheduler

### `backend/pipeline_scheduler/main.py`
- **Functions**
  - `async main()` (line 20): no docstring

## pipeline_worker

### `backend/pipeline_worker/__init__.py`

### `backend/pipeline_worker/main.py`
- **Functions**
  - `_resolve_declared_output_kind(declared_outputs, output_node_id, output_name)` (line 183): no docstring
  - `_validate_output_kind_metadata(output_kind, output_metadata, node_id)` (line 238): no docstring
  - `async main()` (line 5597): no docstring
- **Classes**
  - `_PipelinePayloadParseError` (line 167): no docstring
    - `__init__(self, stage, payload_text, payload_obj, cause)` (line 168): no docstring
  - `PipelineWorker` (line 250): no docstring
    - `__init__(self)` (line 251): no docstring
    - `_build_error_payload(self, message, errors, code, category, status_code, external_code, stage, job, pipeline_id, node_id, mode, context)` (line 334): no docstring
    - `async initialize(self)` (line 380): no docstring
    - `async close(self)` (line 481): no docstring
    - `_create_spark_session(self)` (line 525): no docstring
    - `_extract_job_settings(self, definition)` (line 553): no docstring
    - `_extract_job_spark_conf(self, definition)` (line 557): no docstring
    - `_apply_job_overrides(self, definition)` (line 573): Apply per-job Spark/cast overrides from definition.settings.
    - `_restart_spark_session(self)` (line 613): no docstring
    - `_is_spark_gateway_error(exc)` (line 647): no docstring
    - `async _run_spark(self, fn, label)` (line 664): Run a blocking Spark action off the main event loop.
    - `async run(self)` (line 683): no docstring
    - `_service_name(self)` (line 693): no docstring
    - `_cancel_inflight_on_revoke(self)` (line 696): no docstring
    - `_buffer_messages(self)` (line 702): no docstring
    - `_pending_log_thresholds(self)` (line 705): no docstring
    - `_uses_commit_state(self)` (line 708): no docstring
    - `_parse_payload(self, payload)` (line 711): no docstring
    - `_registry_key(self, payload)` (line 750): no docstring
    - `async _process_payload(self, payload)` (line 758): no docstring
    - `_fallback_metadata(self, payload)` (line 808): no docstring
    - `_span_name(self, payload)` (line 817): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 820): no docstring
    - `_metric_event_name(self, payload)` (line 839): no docstring
    - `_heartbeat_options(self)` (line 842): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 847): no docstring
    - `async _mark_retryable_failure(self, payload, registry_key, handler, error)` (line 850): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 864): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 904): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 921): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 935): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, payload_text, payload_obj, job, attempt_count)` (line 954): no docstring
    - `async _best_effort_record_invalid_job(self, payload, error)` (line 995): no docstring
    - `async _resolve_pipeline_id_from_fields(self, db_name, pipeline_id, branch)` (line 1042): no docstring
    - `async _execute_job(self, job)` (line 1060): no docstring
    - `async _maybe_enqueue_objectify_job(self, dataset, version)` (line 2951): no docstring
    - `async _maybe_enqueue_relationship_jobs(self, dataset, version)` (line 3084): no docstring
    - `async _materialize_output_by_kind(self, output_kind, output_metadata, df, artifact_bucket, prefix, db_name, branch, dataset_name, execution_semantics, incremental_inputs_have_additive_updates, write_mode, file_prefix, file_format, partition_cols, base_row_count)` (line 3198): no docstring
    - `async _materialize_dataset_output(self, output_metadata, df, artifact_bucket, prefix, db_name, branch, dataset_name, execution_semantics, incremental_inputs_have_additive_updates, write_mode, file_prefix, file_format, partition_cols, base_row_count)` (line 3296): no docstring
    - `async _materialize_geotemporal_output(self, output_metadata, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 3465): no docstring
    - `async _materialize_media_output(self, output_metadata, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 3499): no docstring
    - `async _materialize_virtual_output(self, output_metadata, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols, row_count_hint)` (line 3532): no docstring
    - `async _materialize_ontology_output(self, output_metadata, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 3617): no docstring
    - `_ensure_output_columns_present(self, df, required_columns, output_kind)` (line 3647): no docstring
    - `async _load_existing_output_dataset(self, db_name, branch, dataset_name)` (line 3663): no docstring
    - `_align_columns(self, df, columns)` (line 3699): no docstring
    - `_select_new_or_changed_rows(self, input_df, existing_df, pk_columns, dedupe_input)` (line 3709): no docstring
    - `_post_filter_false_expr(self, post_filtering_column)` (line 3737): no docstring
    - `async _materialize_output_dataframe(self, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 3741): no docstring
    - `_row_hash_expr(self, df)` (line 3853): no docstring
    - `_apply_watermark_filter(self, df, watermark_column, watermark_after, watermark_keys)` (line 3866): no docstring
    - `_collect_watermark_keys(self, df, watermark_column, watermark_value)` (line 3889): no docstring
    - `async _load_input_dataframe(self, db_name, metadata, temp_dirs, branch, node_id, input_snapshots, previous_commit_id, use_lakefs_diff, watermark_column, watermark_after, watermark_keys)` (line 3916): no docstring
    - `_preview_sampling_seed(self, job_id)` (line 4197): no docstring
    - `_resolve_sampling_strategy(self, metadata, preview_meta)` (line 4201): no docstring
    - `_attach_sampling_snapshot(self, input_snapshots, node_id, sampling_strategy)` (line 4217): no docstring
    - `_normalize_sampling_fraction(self, value, field)` (line 4229): no docstring
    - `_apply_sampling_strategy(self, df, sampling_strategy, node_id, seed)` (line 4238): no docstring
    - `_strip_commit_prefix(self, key, commit_id)` (line 4283): no docstring
    - `async _list_lakefs_diff_paths(self, repository, ref, since, prefix, node_id)` (line 4289): no docstring
    - `async _load_parquet_keys_dataframe(self, bucket, keys, temp_dirs, prefix)` (line 4335): no docstring
    - `async _load_media_prefix_dataframe(self, bucket, key, node_id)` (line 4369): Treat the artifact_key as an unstructured/media prefix.
    - `async _resolve_pipeline_id(self, job)` (line 4420): no docstring
    - `_collect_spark_conf(self)` (line 4439): no docstring
    - `_build_input_commit_payload(self, input_snapshots)` (line 4455): no docstring
    - `async _acquire_pipeline_lock(self, job)` (line 4479): no docstring
    - `_validate_required_subgraph(self, nodes, incoming, required_node_ids)` (line 4510): no docstring
    - `_validate_definition(self, definition, require_output)` (line 4526): no docstring
    - `_build_table_ops(self, df)` (line 4604): no docstring
    - `_sql_ident(self, name)` (line 4675): no docstring
    - `_clean_string_column(self, column)` (line 4678): no docstring
    - `_try_cast_column(self, column, spark_type)` (line 4682): no docstring
    - `_safe_cast_column(self, column, target_type)` (line 4708): no docstring
    - `_apply_casts(self, df, casts)` (line 4719): no docstring
    - `_apply_schema_casts(self, df, dataset, version)` (line 4731): no docstring
    - `_parse_fk_expectation(self, expectation, default_branch)` (line 4740): no docstring
    - `async _load_fk_reference_dataframe(self, db_name, dataset_id, dataset_name, branch, temp_dirs)` (line 4788): no docstring
    - `async _evaluate_fk_expectations(self, expectations, output_df, db_name, branch, temp_dirs)` (line 4819): no docstring
    - `_normalize_read_options(self, read_config)` (line 4888): no docstring
    - `_mask_sensitive_options(self, options)` (line 4924): no docstring
    - `_schema_ddl_from_read_config(self, read_config)` (line 4945): no docstring
    - `_resolve_read_format(self, path, read_config)` (line 4968): no docstring
    - `_resolve_streaming_checkpoint_location(self, read_config, node_id)` (line 4987): no docstring
    - `_resolve_kafka_value_format(self, read_config)` (line 5002): no docstring
    - `_resolve_kafka_schema_registry_headers(self, read_config)` (line 5017): no docstring
    - `_resolve_kafka_avro_schema(self, read_config, node_id)` (line 5063): no docstring
    - `_apply_kafka_value_parsing(self, df, read_config, node_id)` (line 5095): no docstring
    - `_load_external_streaming_dataframe(self, read_config, node_id, fmt, temp_dirs)` (line 5146): no docstring
    - `_load_external_input_dataframe(self, read_config, node_id, temp_dirs)` (line 5247): Load an input DataFrame directly from Spark using metadata.read (no DatasetRegistry artifact).
    - `async _load_artifact_dataframe(self, bucket, key, temp_dirs, read_config)` (line 5347): no docstring
    - `async _load_prefix_dataframe(self, bucket, prefix, temp_dirs, read_config)` (line 5369): no docstring
    - `async _download_object_to_path(self, bucket, key, local_path)` (line 5459): no docstring
    - `async _download_object(self, bucket, key, temp_dirs, temp_dir)` (line 5472): no docstring
    - `_read_local_file(self, path, read_config)` (line 5488): no docstring
    - `_strip_bom_headers(self, df)` (line 5515): Normalize UTF-8 BOM artifacts in CSV headers.
    - `_load_excel_path(self, path)` (line 5555): no docstring
    - `_load_json_path(self, path, reader)` (line 5561): no docstring
    - `_empty_dataframe(self)` (line 5578): no docstring
    - `_apply_transform(self, metadata, inputs, parameters)` (line 5581): no docstring

### `backend/pipeline_worker/spark_schema_helpers.py`
- **Functions**
  - `_is_data_object(key)` (line 21): no docstring
  - `_schema_from_dataframe(frame)` (line 32): no docstring
  - `_hash_schema_columns(columns)` (line 40): no docstring
  - `_schema_columns_map(columns)` (line 44): no docstring
  - `_schema_diff(current_columns, expected_columns)` (line 57): no docstring
  - `_list_part_files(path, extensions)` (line 81): no docstring

### `backend/pipeline_worker/spark_transform_engine.py`
- **Functions**
  - `apply_spark_transform(metadata, inputs, parameters, apply_casts)` (line 57): no docstring
  - `_clean_col_name(value)` (line 84): no docstring
  - `_clean_expr(value, parameters)` (line 88): no docstring
  - `_regex_inline_flags(value)` (line 92): no docstring
  - `_encode_geohash(lat, lon, precision)` (line 104): no docstring
  - `_resolve_join_col(df, col_name)` (line 146): Resolve join column names defensively (handles UTF-8 BOM artifacts).
  - `_resolve_join_keys(metadata)` (line 159): no docstring
  - `_resolve_stream_join_right_column(joined, left_col, right_col)` (line 170): no docstring
  - `_apply_join(ctx)` (line 188): no docstring
  - `_apply_stream_join(ctx)` (line 275): no docstring
  - `_apply_filter(ctx)` (line 457): no docstring
  - `_apply_split(ctx)` (line 464): no docstring
  - `_apply_compute(ctx)` (line 474): no docstring
  - `_apply_normalize(ctx)` (line 519): no docstring
  - `_apply_explode(ctx)` (line 554): no docstring
  - `_apply_select(ctx)` (line 564): no docstring
  - `_apply_drop(ctx)` (line 577): no docstring
  - `_apply_rename(ctx)` (line 587): no docstring
  - `_apply_cast(ctx)` (line 626): no docstring
  - `_apply_regex_replace(ctx)` (line 633): no docstring
  - `_apply_dedupe(ctx)` (line 669): no docstring
  - `_parse_sort_specs(items)` (line 677): no docstring
  - `_apply_sort(ctx)` (line 707): no docstring
  - `_apply_union(ctx)` (line 715): no docstring
  - `_apply_geospatial(ctx)` (line 750): no docstring
  - `_apply_pattern_mining(ctx)` (line 803): no docstring
  - `_apply_group_by(ctx)` (line 831): no docstring
  - `_apply_pivot(ctx)` (line 901): no docstring
  - `_apply_window(ctx)` (line 926): no docstring
  - `_apply_udf(ctx)` (line 964): no docstring
- **Classes**
  - `_SparkTransformContext` (line 45): no docstring

### `backend/pipeline_worker/worker_helpers.py`
- **Functions**
  - `_resolve_code_version()` (line 34): no docstring
  - `_is_sensitive_conf_key(key)` (line 38): no docstring
  - `_resolve_lakefs_repository()` (line 43): no docstring
  - `_resolve_watermark_column(incremental, metadata)` (line 48): no docstring
  - `_max_watermark_from_snapshots(input_snapshots, watermark_column)` (line 55): no docstring
  - `_watermark_values_match(left, right)` (line 87): no docstring
  - `_collect_watermark_keys_from_snapshots(input_snapshots, watermark_column, watermark_value)` (line 97): no docstring
  - `_collect_input_commit_map(input_snapshots)` (line 129): no docstring
  - `_inputs_diff_empty(input_snapshots)` (line 141): no docstring
  - `_resolve_execution_semantics(job, definition)` (line 152): no docstring
  - `_resolve_output_format(definition, output_metadata)` (line 156): no docstring
  - `_resolve_partition_columns(definition, output_metadata)` (line 171): no docstring
  - `_resolve_external_read_mode(read_config)` (line 203): no docstring
  - `_resolve_streaming_trigger_mode(read_config, default_mode)` (line 215): no docstring
  - `_resolve_streaming_timeout_seconds(read_config, default_seconds)` (line 245): no docstring

## projection_worker

### `backend/projection_worker/__init__.py`

### `backend/projection_worker/main.py`
- **Classes**
  - `ProjectionWorker` (line 62): Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커
    - `__init__(self)` (line 71): no docstring
    - `_is_es_version_conflict(error)` (line 142): no docstring
    - `_parse_sequence(value)` (line 150): no docstring
    - `_normalize_localized_field(value, default_lang)` (line 154): no docstring
    - `_normalize_ontology_properties(self, properties, default_lang)` (line 164): no docstring
    - `_normalize_ontology_relationships(self, relationships, default_lang)` (line 201): no docstring
    - `_extract_envelope_metadata(event_data)` (line 250): no docstring
    - `async _record_es_side_effect(self, event_id, event_data, db_name, index_name, doc_id, operation, status, record_lineage, skip_reason, error, extra_metadata)` (line 268): Record projection side-effects for provenance (lineage) + audit.
    - `async initialize(self)` (line 356): 워커 초기화
    - `async _setup_indices(self)` (line 426): 매핑 파일 로드 (인덱스는 DB별로 동적 생성)
    - `async _ensure_index_exists(self, db_name, index_type, branch)` (line 438): 특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성
    - `async _load_mapping(self, filename)` (line 535): 매핑 파일 로드
    - `async run(self)` (line 549): 메인 실행 루프
    - `_span_name(self, payload)` (line 561): no docstring
    - `_registry_handler(self, msg, payload)` (line 564): no docstring
    - `_registry_key(self, payload)` (line 567): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 574): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 579): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 591): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 603): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 614): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 644): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 658): no docstring
    - `async _commit(self, msg)` (line 690): no docstring
    - `async _publish_to_dlq(self, msg, error, attempt_count, payload_text, kafka_headers, fallback_metadata)` (line 696): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 732): no docstring
    - `async _process_payload(self, payload)` (line 762): no docstring
    - `async _handle_ontology_event(self, event_data)` (line 790): 온톨로지 이벤트 처리
    - `async _handle_action_event(self, event_data)` (line 814): Action writeback events -> overlay projection.
    - `async _handle_action_applied(self, action_data, event_id, event_data)` (line 829): no docstring
    - `async _handle_ontology_class_created(self, ontology_data, event_id, event_data)` (line 987): 온톨로지 클래스 생성 이벤트 처리
    - `async _handle_ontology_class_updated(self, ontology_data, event_id, event_data)` (line 1170): 온톨로지 클래스 업데이트 이벤트 처리
    - `async _handle_ontology_class_deleted(self, ontology_data, event_id, event_data)` (line 1369): 온톨로지 클래스 삭제 이벤트 처리
    - `async _handle_database_created(self, db_data, event_id, event_data)` (line 1624): 데이터베이스 생성 이벤트 처리
    - `async _handle_database_deleted(self, db_data, event_id, event_data)` (line 1680): 데이터베이스 삭제 이벤트 처리
    - `async _get_class_label(self, class_id, db_name, branch)` (line 1760): Redis에서 클래스 라벨 조회 (Cache Stampede 방지)
    - `async _get_class_label_fallback(self, class_id, db_name, branch)` (line 1866): 락 획득 실패 시 fallback 조회 (성능보다 안정성 우선)
    - `get_cache_efficiency_metrics(self)` (line 1902): 캐시 효율성 및 락 경합 메트릭 반환
    - `log_cache_metrics(self)` (line 1955): 캐시 메트릭을 로그로 출력
    - `async _cache_class_label(self, class_id, label, db_name, branch)` (line 1974): 클래스 라벨을 Redis에 캐싱
    - `_normalize_properties(self, properties)` (line 1989): 속성을 검색 최적화된 형태로 정규화
    - `_is_transient_infra_error(error)` (line 2001): Return True for errors that are expected to recover via retry (e.g. ES outage).
    - `async _shutdown(self)` (line 2025): 워커 종료

## scripts

### `backend/scripts/backfill_lineage.py`
- **Functions**
  - `_parse_dt(value)` (line 22): no docstring
  - `async _run_queue(limit, db_name)` (line 32): no docstring
  - `async _run_replay(from_dt, to_dt, limit)` (line 68): no docstring
  - `async main()` (line 103): no docstring

### `backend/scripts/dependency_parsing.py`
- **Functions**
  - `_normalize_package_name(raw_name)` (line 15): no docstring
  - `_split_dependency_spec(spec)` (line 22): no docstring
  - `parse_requirements_txt(file_path)` (line 40): no docstring
  - `parse_pyproject_toml(file_path)` (line 55): no docstring

### `backend/scripts/ghost_dependency_audit.py`
- **Functions**
  - `check_service_imports(service_dir)` (line 24): Check what external libraries a service actually imports
  - `audit_service(service_name, service_dir)` (line 52): Comprehensive audit of a single service
  - `main()` (line 107): Main audit execution

### `backend/scripts/import_performance_test.py`
- **Functions**
  - `measure_import_performance(import_func, description)` (line 21): Import 성능과 메모리 사용량 측정
  - `test_direct_import()` (line 66): 직접 경로 import 테스트 (개선된 방식)
  - `test_bulk_import_simulation()` (line 71): Bulk import 시뮬레이션 (이전 방식)
  - `test_single_service_need()` (line 96): 실제 서비스에서 ElasticsearchService 하나만 필요한 경우
  - `main()` (line 109): 메인 테스트 실행

### `backend/scripts/migrations/add_lightweight_system_fields.py`
- **Functions**
  - `async add_system_fields_to_ontology(db_name, class_id, terminus_service)` (line 75): Add lightweight system fields to a specific ontology class
  - `async add_system_fields_to_all_ontologies(db_name)` (line 168): Add lightweight system fields to all ontologies in a database
  - `async verify_lightweight_architecture(db_name)` (line 259): Verify that the lightweight graph architecture is properly configured

### `backend/scripts/migrations/add_system_fields_direct.py`
- **Functions**
  - `async update_ontology_with_system_fields()` (line 10): Update ontologies to include system fields

### `backend/scripts/migrations/add_system_fields_to_schema.py`
- **Functions**
  - `async add_system_fields_to_class(terminus_url, db_name, class_name, auth)` (line 11): Add system fields to a single class
  - `async get_all_classes(terminus_url, db_name, auth)` (line 80): Get all classes in the database
  - `async main()` (line 120): Main function to add system fields to all classes

### `backend/scripts/migrations/clean_terminus_reference.py`
- **Functions**
  - `async clean_stale_reference()` (line 15): no docstring

### `backend/scripts/migrations/create_db_and_schema_direct.py`
- **Functions**
  - `async create_database_direct(db_name)` (line 11): Create database directly in TerminusDB
  - `async create_schema_with_system_fields(db_name)` (line 45): Create schema classes with system fields directly
  - `async verify_schema(db_name)` (line 132): Verify the schema was created correctly
  - `async main()` (line 229): Main function

### `backend/scripts/migrations/create_integration_schema.py`
- **Functions**
  - `async create_schema()` (line 15): no docstring

### `backend/scripts/migrations/create_minio_bucket.py`

### `backend/scripts/migrations/create_test_schema.py`
- **Functions**
  - `async create_test_environment()` (line 10): Create test database with proper schema including system fields

### `backend/scripts/migrations/fix_all_datetime_utc.py`
- **Functions**
  - `fix_file(filepath)` (line 10): Fix datetime.UTC to timezone.utc in a file
  - `main()` (line 41): Fix all Python files in OMS directory

### `backend/scripts/migrations/fix_datetime_deprecation.py`
- **Functions**
  - `fix_datetime_in_file(filepath)` (line 11): Fix datetime deprecation in a single file
  - `main()` (line 53): Fix all Python files with datetime deprecation

### `backend/scripts/migrations/fix_terminus_schema.py`
- **Functions**
  - `async fix_schemas()` (line 11): no docstring

### `backend/scripts/migrations/implement_lightweight_schema.py`
- **Functions**
  - `async implement_lightweight_architecture()` (line 12): no docstring

### `backend/scripts/migrations/migrate_es_to_terminus_lightweight.py`
- **Functions**
  - `async verify_migration(db_name)` (line 263): Verify the migration by comparing counts
  - `async main()` (line 320): Main execution
- **Classes**
  - `ESToTerminusMigrator` (line 37): Migrates ES documents to TerminusDB lightweight nodes
    - `__init__(self, db_name)` (line 40): no docstring
    - `async initialize(self)` (line 52): Initialize services
    - `async fetch_all_documents(self)` (line 66): Fetch all documents from Elasticsearch
    - `async create_lightweight_node(self, doc)` (line 116): Create a lightweight node in TerminusDB
    - `get_relationship_fields(self, class_id)` (line 175): Get relationship fields for a class
    - `get_key_fields(self, class_id)` (line 190): Get key identifier fields for a class
    - `async migrate(self)` (line 205): Run the migration

### `backend/scripts/migrations/update_imports.py`
- **Functions**
  - `update_imports_in_file(file_path)` (line 11): Update imports in a single file
  - `find_python_files(root_dir)` (line 35): Find all Python files excluding backup directories
  - `main()` (line 47): no docstring

### `backend/scripts/processed_event_registry_smoke.py`
- **Functions**
  - `async _main()` (line 19): no docstring

### `backend/scripts/run_coverage_report.py`
- **Functions**
  - `main()` (line 487): Main CLI interface
- **Classes**
  - `CoverageReporter` (line 20): 🔥 THINK ULTRA! Comprehensive coverage reporter with enhanced analysis
    - `__init__(self, project_root)` (line 23): no docstring
    - `run_coverage_analysis(self, test_pattern, include_integration, include_performance)` (line 46): Run comprehensive coverage analysis
    - `_parse_coverage_results(self, result, analysis_time)` (line 112): Parse coverage results from subprocess output
    - `_parse_xml_coverage(self, xml_path)` (line 153): Parse detailed coverage data from XML report
    - `_parse_test_results(self, stdout)` (line 216): Parse test execution results
    - `_generate_detailed_reports(self, coverage_data)` (line 255): Generate detailed coverage reports
    - `_generate_summary_report(self, coverage_data)` (line 288): Generate summary coverage report
    - `_generate_markdown_report(self, coverage_data)` (line 358): Generate markdown coverage report
    - `_generate_csv_report(self, coverage_data)` (line 433): Generate CSV report for coverage tracking
    - `_print_coverage_summary(self, coverage_data)` (line 454): Print coverage summary to console

### `backend/scripts/run_message_relay_local.py`
- **Functions**
  - `async main()` (line 25): EventPublisher 로컬 실행

### `backend/scripts/single_source_of_truth_audit.py`
- **Functions**
  - `check_duplicate_dependencies()` (line 23): Check for duplicate dependency declarations across files
  - `check_version_consistency()` (line 69): Check for version inconsistencies across files
  - `check_single_source_compliance()` (line 105): Verify that all services use only -e ../shared in requirements.txt
  - `main()` (line 141): Main audit execution

### `backend/scripts/start_services.py`
- **Functions**
  - `start_service(name, path, command, port, protocol, verify_ssl, health_path)` (line 22): Start a service and verify it's running
  - `stop_services(processes)` (line 73): Stop all services
  - `main()` (line 87): no docstring

### `backend/scripts/sync_agent_tool_allowlist.py`
- **Functions**
  - `_build_parser()` (line 19): no docstring
  - `async _run(args)` (line 34): no docstring
  - `main()` (line 49): no docstring

### `backend/scripts/validate_environment.py`
- **Functions**
  - `_redact_url(url)` (line 30): no docstring
  - `_redact_secret(value, show)` (line 49): no docstring
  - `async main()` (line 362): no docstring
- **Classes**
  - `EnvironmentValidator` (line 60): no docstring
    - `__init__(self)` (line 61): no docstring
    - `print_header(self, title)` (line 66): no docstring
    - `check_result(self, name, success, message)` (line 71): no docstring
    - `validate_effective_settings(self)` (line 80): no docstring
    - `async check_postgresql(self)` (line 111): no docstring
    - `async check_redis(self)` (line 156): no docstring
    - `async check_elasticsearch(self)` (line 172): no docstring
    - `check_kafka(self)` (line 195): no docstring
    - `async check_terminus(self)` (line 227): no docstring
    - `async check_services(self)` (line 264): no docstring
    - `check_docker_config(self)` (line 286): no docstring
    - `async check_workers(self)` (line 306): no docstring
    - `async run_all_checks(self)` (line 330): no docstring

### `backend/scripts/verify-imports.py`
- **Functions**
  - `extract_imports(file_path)` (line 45): Extract all imports from a Python file.
  - `verify_import(module_name)` (line 60): Verify that a module can be imported.
  - `check_conditional_imports(file_path)` (line 69): Check for conditional imports (try/except ImportError patterns).
  - `verify_service(service_dir)` (line 109): Verify all imports in a service directory.
  - `main()` (line 157): Main entry point.
- **Classes**
  - `ImportChecker` (line 24): AST visitor to extract all import statements.
    - `__init__(self)` (line 27): no docstring
    - `visit_Import(self, node)` (line 31): Visit import statements.
    - `visit_ImportFrom(self, node)` (line 37): Visit from ... import statements.

## shared

### `backend/shared/__init__.py`

### `backend/shared/config/__init__.py`
- **Functions**
  - `__getattr__(name)` (line 177): Lazy, drift-free compatibility exports.
- **Classes**
  - `Config` (line 34): 통합 설정 클래스
    - `get_postgres_url()` (line 46): PostgreSQL 연결 URL
    - `get_redis_url()` (line 51): Redis 연결 URL
    - `get_elasticsearch_url()` (line 56): Elasticsearch 연결 URL
    - `get_kafka_bootstrap_servers()` (line 61): Kafka Bootstrap Servers
    - `get_terminus_url()` (line 66): TerminusDB 연결 URL
    - `get_minio_url()` (line 71): MinIO 연결 URL
    - `get_instances_index_name(db_name, version)` (line 80): 인스턴스 Elasticsearch 인덱스 이름
    - `get_ontologies_index_name(db_name, version)` (line 85): 온톨로지 Elasticsearch 인덱스 이름
    - `sanitize_index_name(name)` (line 90): Elasticsearch 인덱스 이름 정제
    - `get_default_index_settings()` (line 95): 기본 인덱스 설정
    - `validate_all_config(cls)` (line 104): 모든 설정의 유효성 검증
    - `get_full_config_summary(cls)` (line 134): 전체 시스템 설정 요약 반환

### `backend/shared/config/app_config.py`
- **Classes**
  - `_SettingsValue` (line 14): Descriptor that resolves values from the current settings instance (SSoT).
    - `__init__(self, path)` (line 17): no docstring
    - `__get__(self, instance, owner)` (line 21): no docstring
  - `AppConfig` (line 25): SPICE HARVESTER 애플리케이션 전체 설정 중앙 관리 클래스
    - `get_instance_command_key(db_name, command_id)` (line 77): 인스턴스 Command S3 키 생성
    - `get_instance_latest_key(db_name, instance_id)` (line 82): 인스턴스 최신 상태 S3 키 생성 (deprecated - 순수 append-only로 변경됨)
    - `get_command_status_key(command_id)` (line 90): Command 상태 Redis 키 생성
    - `get_command_result_key(command_id)` (line 95): Command 결과 Redis 키 생성
    - `get_command_status_pattern()` (line 100): 모든 Command 상태 키 패턴
    - `get_class_label_key(db_name, class_id, branch)` (line 105): 클래스 라벨 캐시 Redis 키 생성 (branch-aware).
    - `get_user_session_key(user_id)` (line 112): 사용자 세션 Redis 키 생성
    - `get_websocket_connection_key(client_id)` (line 117): WebSocket 연결 Redis 키 생성
    - `get_instances_index_name(db_name, version)` (line 126): 인스턴스 Elasticsearch 인덱스 이름 생성
    - `get_ontologies_index_name(db_name, version)` (line 132): 온톨로지 Elasticsearch 인덱스 이름 생성
    - `get_oms_url()` (line 141): OMS 서비스 URL
    - `get_bff_url()` (line 146): BFF 서비스 URL
    - `get_funnel_url()` (line 151): Funnel 서비스 URL
    - `_normalize_object_type_id(value)` (line 187): no docstring
    - `get_writeback_enabled_object_types(cls)` (line 201): no docstring
    - `is_writeback_enabled_object_type(cls, class_id)` (line 210): no docstring
    - `get_ontology_writeback_branch(cls, db_name)` (line 220): Return a lakeFS-compatible writeback branch id.
    - `sanitize_lakefs_branch_id(value)` (line 235): no docstring
    - `validate_config(cls)` (line 289): 설정값들의 유효성 검증
    - `get_all_topics(cls)` (line 318): 모든 Kafka 토픽 목록 반환
    - `get_config_summary(cls)` (line 351): 현재 설정 요약 반환 (디버깅용)

### `backend/shared/config/kafka_config.py`
- **Functions**
  - `create_eos_producer(service_name, instance_id)` (line 273): Create a Kafka producer with EOS v2 configuration
  - `create_eos_consumer(service_name, group_id)` (line 295): Create a Kafka consumer with EOS v2 configuration
- **Classes**
  - `KafkaEOSConfig` (line 15): Kafka Exactly-Once Semantics v2 Configuration
    - `get_producer_config(service_name, instance_id, enable_transactions)` (line 27): Get producer configuration with EOS v2 support
    - `get_consumer_config(service_name, group_id, read_committed, auto_commit)` (line 79): Get consumer configuration with EOS v2 support
    - `get_admin_config()` (line 128): Get admin client configuration for topic management
    - `get_topic_config(retention_ms, min_insync_replicas, replication_factor)` (line 142): Get topic configuration for durability and performance
  - `TransactionalProducer` (line 169): Helper class for transactional message production
    - `__init__(self, producer, enable_transactions)` (line 177): Initialize transactional producer wrapper
    - `init_transactions(self, timeout)` (line 189): Initialize transactions (must be called once before any transaction)
    - `begin_transaction(self)` (line 200): Begin a new transaction
    - `commit_transaction(self, timeout)` (line 205): Commit the current transaction
    - `abort_transaction(self, timeout)` (line 215): Abort the current transaction
    - `send_transactional_batch(self, messages, topic, key_extractor)` (line 225): Send a batch of messages in a single transaction

### `backend/shared/config/model_context_limits.py`
- **Functions**
  - `get_model_context_config(model_id, registry_record)` (line 99): Resolve context config with priority:
- **Classes**
  - `ModelContextConfig` (line 13): Configuration for a specific LLM model's context capabilities.
    - `safe_prompt_chars(self)` (line 21): Calculate safe prompt character limit.
  - `PromptBudget` (line 61): Sectioned budget allocation for agent prompt context.
    - `working_state(self)` (line 71): Plan, analysis summaries, ontology state (15%).
    - `knowledge_ledger(self)` (line 76): Persistent facts extracted from tool results (15%).
    - `tool_history(self)` (line 81): Historical tool calls and observations (30%).
    - `recent_context(self)` (line 86): Last N items — must not be compressed (40%).

### `backend/shared/config/rate_limit_config.py`
- **Classes**
  - `RateLimitStrategy` (line 13): Rate limiting identification strategy
  - `EndpointCategory` (line 21): Categories of endpoints for rate limiting
  - `RateLimitRule` (line 33): Rate limit rule configuration
  - `RateLimitConfig` (line 43): Centralized rate limiting configuration
    - `get_endpoint_rule(cls, endpoint, method)` (line 199): Get rate limit rule for a specific endpoint
    - `_match_pattern(endpoint, pattern)` (line 228): Check if endpoint matches a pattern with wildcards
    - `get_user_limit(cls, base_rule, user_tier)` (line 246): Adjust rate limit based on user tier
    - `is_whitelisted(cls, ip)` (line 278): Check if IP is whitelisted

### `backend/shared/config/search_config.py`
- **Functions**
  - `sanitize_index_name(name)` (line 13): Elasticsearch 인덱스 이름 규칙에 맞게 문자열을 정제합니다.
  - `_branch_overlay_token(branch)` (line 61): Stable, collision-resistant token for ES branch overlay indices.
  - `get_instances_index_name(db_name, version, branch)` (line 76): 인스턴스 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
  - `get_ontologies_index_name(db_name, version, branch)` (line 109): 온톨로지 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
  - `get_index_alias_name(index_name)` (line 143): 인덱스의 별칭(alias) 이름을 생성합니다.
  - `get_default_index_settings()` (line 169): Return default ES index settings with dev-safe replica defaults.
  - `__getattr__(name)` (line 195): no docstring

### `backend/shared/config/service_config.py`
- **Functions**
  - `get_oms_url()` (line 485): Get OMS URL - convenience function.
  - `get_bff_url()` (line 490): Get BFF URL - convenience function.
  - `get_funnel_url()` (line 495): Get Funnel URL - convenience function.
  - `get_agent_url()` (line 500): Get Agent URL - convenience function.
- **Classes**
  - `_SettingsProxy` (line 28): no docstring
    - `__getattr__(self, name)` (line 29): no docstring
  - `ServiceConfig` (line 36): Centralized service configuration management.
    - `get_oms_port()` (line 53): Get OMS (Ontology Management Service) port from environment or default.
    - `get_bff_port()` (line 58): Get BFF (Backend for Frontend) port from environment or default.
    - `get_funnel_port()` (line 63): Get Funnel service port from environment or default.
    - `get_agent_port()` (line 68): Get Agent service port from environment or default.
    - `get_oms_host()` (line 73): Get OMS host from environment or default.
    - `get_bff_host()` (line 78): Get BFF host from environment or default.
    - `get_funnel_host()` (line 83): Get Funnel host from environment or default.
    - `get_agent_host()` (line 88): Get Agent host from environment or default.
    - `get_oms_url()` (line 93): Get complete OMS URL from environment or construct from host/port.
    - `get_bff_url()` (line 105): Get complete BFF URL from environment or construct from host/port.
    - `get_funnel_url()` (line 117): Get complete Funnel URL from environment or construct from host/port.
    - `get_agent_url()` (line 129): Get complete Agent URL from environment or construct from host/port.
    - `get_terminus_url()` (line 141): Get TerminusDB URL from environment or default.
    - `get_postgres_url()` (line 146): Get PostgreSQL connection URL from environment or default.
    - `get_kafka_bootstrap_servers()` (line 151): Get Kafka bootstrap servers from environment or default.
    - `get_redis_host()` (line 156): Get Redis host from environment or default.
    - `get_redis_port()` (line 161): Get Redis port from environment or default.
    - `get_redis_url()` (line 166): Get Redis connection URL from environment or construct from host/port.
    - `get_elasticsearch_host()` (line 171): Get Elasticsearch host from environment or default.
    - `get_elasticsearch_port()` (line 176): Get Elasticsearch port from environment or default.
    - `get_elasticsearch_url()` (line 181): Get Elasticsearch base URL from environment or construct from host/port.
    - `is_docker_environment()` (line 186): Check if running in Docker environment.
    - `get_minio_endpoint()` (line 195): Get MinIO/S3 endpoint URL.
    - `get_minio_access_key()` (line 200): Get MinIO/S3 access key.
    - `get_minio_secret_key()` (line 205): Get MinIO/S3 secret key.
    - `get_event_store_bucket()` (line 210): Get S3/MinIO bucket name for immutable event store.
    - `get_lakefs_api_url()` (line 215): Get lakeFS API base URL.
    - `get_lakefs_s3_endpoint()` (line 226): Get lakeFS S3 Gateway endpoint URL.
    - `get_service_url(service_name)` (line 235): Get URL for a specific service by name.
    - `get_all_service_urls()` (line 262): Get all service URLs as a dictionary.
    - `validate_configuration()` (line 273): Validate that all required configuration is present.
    - `use_https()` (line 290): Check if HTTPS should be used for service communication.
    - `is_production()` (line 300): Check if running in production environment.
    - `is_debug_endpoints_enabled()` (line 305): Enable opt-in debug endpoints (never on by default).
    - `get_ssl_cert_path()` (line 310): Get SSL certificate path from environment.
    - `get_ssl_key_path()` (line 322): Get SSL key path from environment.
    - `get_ssl_ca_path()` (line 334): Get SSL CA certificate path from environment.
    - `verify_ssl()` (line 346): Check if SSL certificate verification should be enabled.
    - `get_protocol()` (line 361): Get the protocol to use for service communication.
    - `get_ssl_config()` (line 371): Get complete SSL configuration as a dictionary.
    - `get_client_ssl_config()` (line 381): Get SSL configuration for HTTP clients (requests, httpx).
    - `get_cors_origins()` (line 393): Get CORS allowed origins from environment variables.
    - `_get_environment_default_origins()` (line 408): Get environment-based default CORS origins.
    - `_get_dev_cors_origins()` (line 427): Get development CORS origins for common frontend ports.
    - `get_cors_config()` (line 454): Get complete CORS configuration for FastAPI middleware.
    - `is_cors_enabled()` (line 464): Check if CORS should be enabled.
    - `get_cors_debug_info()` (line 474): Get CORS configuration debug information.

### `backend/shared/config/settings.py`
- **Functions**
  - `_is_docker_environment()` (line 27): no docstring
  - `_clamp_int(raw, default, min_value, max_value)` (line 36): no docstring
  - `_parse_boolish(raw)` (line 46): no docstring
  - `_strip_optional_text(raw)` (line 57): no docstring
  - `_strip_text_if_not_none(raw)` (line 64): no docstring
  - `_normalize_base_branch(raw)` (line 70): no docstring
  - `_clamp_flush_timeout_seconds(raw, default)` (line 74): no docstring
  - `_env_truthy(name)` (line 82): no docstring
  - `_should_load_dotenv()` (line 87): Whether settings should read from a local `.env` file.
  - `get_settings()` (line 4291): Get the global settings instance
  - `reload_settings()` (line 4305): Reload settings from environment (useful for testing)
  - `build_client_ssl_config(settings)` (line 4317): SSL config for HTTP clients (httpx/requests/etc).
  - `build_server_ssl_config(settings)` (line 4338): SSL config for uvicorn (server-side TLS).
  - `_get_dev_cors_origins()` (line 4368): no docstring
  - `_get_environment_default_origins(settings)` (line 4384): no docstring
  - `resolve_cors_origins(settings)` (line 4394): Resolve CORS origins with production safety.
  - `build_cors_middleware_config(settings)` (line 4436): no docstring
  - `get_cors_debug_info(settings)` (line 4466): no docstring
- **Classes**
  - `Environment` (line 107): Application environment types
  - `DatabaseSettings` (line 114): Database configuration settings
    - `get_terminus_url(cls, v)` (line 146): no docstring
    - `get_terminus_user(cls, v)` (line 151): no docstring
    - `get_terminus_password(cls, v)` (line 156): no docstring
    - `get_terminus_account(cls, v)` (line 161): no docstring
    - `clamp_elasticsearch_default_shards(cls, v)` (line 304): no docstring
    - `clamp_elasticsearch_default_replicas(cls, v)` (line 309): no docstring
    - `postgres_url(self)` (line 318): Construct PostgreSQL connection URL
    - `kafka_servers(self)` (line 325): Get Kafka bootstrap servers
    - `elasticsearch_url(self)` (line 332): Construct Elasticsearch URL with authentication
    - `redis_url(self)` (line 341): Construct Redis URL
  - `ServiceSettings` (line 350): Service configuration settings
    - `get_oms_base_url_override(cls, v)` (line 456): no docstring
    - `get_bff_base_url_override(cls, v)` (line 464): no docstring
    - `get_funnel_base_url_override(cls, v)` (line 472): no docstring
    - `get_agent_base_url_override(cls, v)` (line 480): no docstring
    - `resolve_funnel_excel_timeout(cls, v)` (line 488): no docstring
    - `clamp_funnel_excel_timeout(cls, v)` (line 496): no docstring
    - `resolve_funnel_infer_timeout(cls, v)` (line 505): no docstring
    - `clamp_funnel_infer_timeout(cls, v)` (line 513): no docstring
    - `oms_base_url(self)` (line 521): Construct OMS base URL
    - `bff_base_url(self)` (line 529): Construct BFF base URL
    - `funnel_base_url(self)` (line 537): Construct Funnel base URL
    - `agent_base_url(self)` (line 545): Construct Agent base URL
    - `cors_origins_list(self)` (line 553): Parse CORS origins from JSON string
  - `LLMSettings` (line 565): LLM gateway settings (shared across services).
    - `fallback_anthropic_api_key(cls, v)` (line 652): no docstring
    - `fallback_google_api_key(cls, v)` (line 660): no docstring
    - `fallback_openai_api_key(cls, v)` (line 668): no docstring
    - `fallback_openai_base_url(cls, v)` (line 682): no docstring
    - `fallback_openai_model(cls, v)` (line 696): no docstring
    - `anthropic_api_key_effective(self)` (line 709): no docstring
    - `google_api_key_effective(self)` (line 713): no docstring
    - `mock_json_for_task(self, task)` (line 716): Resolve the mock provider JSON payload for a task.
  - `ObservabilitySettings` (line 759): Logging/observability settings (shared across services/workers).
    - `normalize_log_level(cls, v)` (line 873): no docstring
    - `fallback_run_id(cls, v)` (line 890): no docstring
    - `fallback_code_sha(cls, v)` (line 901): no docstring
    - `normalize_otel_service_version(cls, v)` (line 912): no docstring
    - `normalize_jaeger_endpoint(cls, v)` (line 918): no docstring
    - `clamp_trace_sample_rate(cls, v)` (line 924): no docstring
    - `parse_otel_export_otlp(cls, v)` (line 937): no docstring
    - `clamp_metric_export_interval_seconds(cls, v)` (line 942): no docstring
    - `service_name_effective(self)` (line 950): no docstring
    - `lineage_required_effective(self)` (line 958): no docstring
    - `enterprise_catalog_ref_effective(self)` (line 966): no docstring
    - `otel_export_otlp_effective(self)` (line 976): no docstring
  - `GraphQuerySettings` (line 982): Graph query guardrails (graph federation).
    - `clamp_max_hops(cls, v)` (line 1000): no docstring
    - `clamp_max_limit(cls, v)` (line 1005): no docstring
    - `clamp_max_paths(cls, v)` (line 1010): no docstring
  - `FeatureFlagsSettings` (line 1014): Feature flags / opt-in endpoints.
  - `PipelineSettings` (line 1042): Pipeline Builder + pipeline worker settings.
    - `fallback_publish_lock_timeout(cls, v)` (line 1233): no docstring
    - `clamp_jobs_max_retries(cls, v)` (line 1242): no docstring
    - `clamp_job_queue_flush_timeout_seconds(cls, v)` (line 1247): no docstring
    - `clamp_jobs_backoff_base_seconds(cls, v)` (line 1256): no docstring
    - `clamp_jobs_backoff_max_seconds(cls, v)` (line 1261): no docstring
    - `clamp_spark_executor_threads(cls, v)` (line 1266): no docstring
    - `clamp_spark_shuffle_partitions(cls, v)` (line 1271): no docstring
    - `clamp_spark_streaming_await_timeout_seconds(cls, v)` (line 1276): no docstring
    - `clamp_kafka_schema_registry_timeout_seconds(cls, v)` (line 1281): no docstring
    - `normalize_spark_streaming_default_trigger(cls, v)` (line 1286): no docstring
    - `clamp_lock_ttl_seconds(cls, v)` (line 1296): no docstring
    - `clamp_lock_renew_seconds(cls, v)` (line 1301): no docstring
    - `clamp_lock_retry_seconds(cls, v)` (line 1306): no docstring
    - `clamp_lock_acquire_timeout_seconds(cls, v)` (line 1311): no docstring
    - `clamp_publish_lock_acquire_timeout_seconds(cls, v)` (line 1316): no docstring
    - `clamp_scheduler_poll_seconds(cls, v)` (line 1321): no docstring
    - `protected_branches_set(self)` (line 1325): no docstring
    - `fallback_branches_list(self)` (line 1332): no docstring
  - `OntologySettings` (line 1340): Ontology API + linter governance settings.
    - `strip_protected_branches(cls, v)` (line 1406): no docstring
    - `parse_optional_bool(cls, v)` (line 1411): no docstring
    - `protected_branches_set(self)` (line 1415): no docstring
    - `allow_implicit_primary_key_effective(self, is_production, branch)` (line 1420): no docstring
    - `allow_implicit_title_key_effective(self, is_production, branch)` (line 1429): no docstring
  - `AgentRuntimeSettings` (line 1439): Agent runtime settings (agent service tool runner).
    - `clamp_context_upload_max_bytes(cls, v)` (line 1565): no docstring
    - `clamp_context_upload_max_text_chars(cls, v)` (line 1570): no docstring
    - `clamp_context_upload_clamav_port(cls, v)` (line 1580): no docstring
    - `fallback_bff_token(cls, v)` (line 1585): no docstring
    - `fallback_command_timeout(cls, v)` (line 1593): no docstring
  - `AgentPlanSettings` (line 1603): LLM-native control plane settings (planner + allowlist bootstrap).
  - `PipelinePlanSettings` (line 1633): Pipeline plan planner settings (LLM-backed pipeline definition proposals).
  - `ClientSettings` (line 1650): Internal service-to-service client settings (BFF/OMS/etc).
    - `clamp_agent_proxy_timeout(cls, v)` (line 1683): no docstring
    - `fallback_oms_client_token(cls, v)` (line 1692): no docstring
    - `fallback_bff_admin_token(cls, v)` (line 1703): no docstring
  - `MCPSettings` (line 1713): MCP integration settings (BFF/agent).
  - `AuthSettings` (line 1735): Service auth configuration (BFF/OMS).
    - `bff_auth_disable_allowed(self)` (line 1890): no docstring
    - `oms_auth_disable_allowed(self)` (line 1894): no docstring
    - `_split_tokens(raw)` (line 1898): no docstring
    - `_tokens_from_values(cls, *values)` (line 1905): no docstring
    - `bff_expected_tokens(self)` (line 1912): no docstring
    - `bff_agent_tokens(self)` (line 1916): no docstring
    - `oms_expected_tokens(self)` (line 1920): no docstring
    - `bff_expected_token(self)` (line 1924): no docstring
    - `bff_admin_only_token(self)` (line 1934): no docstring
    - `oms_expected_token(self)` (line 1939): no docstring
    - `admin_bypass_tokens(self)` (line 1944): no docstring
    - `is_bff_auth_required(self, allow_pytest, default_required)` (line 1955): no docstring
    - `is_oms_auth_required(self, default_required)` (line 1964): no docstring
    - `_parse_exempt_paths(raw, defaults)` (line 1972): no docstring
    - `resolve_bff_exempt_paths(self, defaults)` (line 1979): no docstring
    - `resolve_oms_exempt_paths(self, defaults)` (line 1982): no docstring
    - `dev_master_role_set(self)` (line 1986): no docstring
  - `RateLimitSettings` (line 1990): Rate limiter runtime configuration.
    - `clamp_local_max_entries(cls, v)` (line 2012): no docstring
  - `MessagingSettings` (line 2016): Kafka topic/group configuration settings
  - `StorageSettings` (line 2132): Storage configuration settings
    - `normalize_minio_endpoint_url(cls, v)` (line 2187): no docstring
    - `clamp_lakefs_client_timeout(cls, v)` (line 2257): no docstring
    - `normalize_lakefs_credentials_source(cls, v)` (line 2265): no docstring
    - `use_ssl(self)` (line 2276): Determine if SSL should be used based on endpoint URL
    - `lakefs_api_url_effective(self)` (line 2281): Return lakeFS API base URL (without /api/v1).
    - `lakefs_s3_endpoint_effective(self)` (line 2289): Return lakeFS S3 Gateway endpoint URL.
  - `CacheSettings` (line 2296): Cache and TTL configuration settings
    - `clamp_command_status_ttl_seconds(cls, v)` (line 2334): no docstring
  - `SecuritySettings` (line 2338): Security configuration settings
  - `PerformanceSettings` (line 2405): Performance and optimization settings
    - `clamp_pg_pool_min(cls, v)` (line 2533): no docstring
    - `clamp_pg_pool_max(cls, v)` (line 2548): no docstring
    - `clamp_pg_command_timeout_seconds(cls, v)` (line 2563): no docstring
    - `clamp_lineage_latest_edges_max_ids(cls, v)` (line 2568): no docstring
  - `EventSourcingSettings` (line 2572): Event sourcing / CQRS tuning settings
    - `normalize_event_store_strings(cls, v)` (line 2674): no docstring
  - `BranchVirtualizationSettings` (line 2680): Branch virtualization defaults (OCC seeding).
  - `InstanceWorkerSettings` (line 2698): Instance worker runtime settings.
    - `fallback_allow_pk_generation(cls, v)` (line 2736): no docstring
    - `fallback_relationship_strict(cls, v)` (line 2742): no docstring
    - `clamp_max_retry_attempts(cls, v)` (line 2748): no docstring
    - `clamp_untyped_ref_max_retry_attempts(cls, v)` (line 2753): no docstring
    - `clamp_untyped_ref_backoff_max_seconds(cls, v)` (line 2758): no docstring
  - `OntologyWorkerSettings` (line 2766): Ontology worker runtime settings.
    - `clamp_max_retry_attempts(cls, v)` (line 2792): no docstring
  - `ProjectionWorkerSettings` (line 2796): Projection worker runtime settings.
    - `clamp_max_retries(cls, v)` (line 2818): no docstring
  - `ActionWorkerSettings` (line 2822): Action worker runtime settings.
    - `clamp_dlq_retries(cls, v)` (line 2848): no docstring
    - `clamp_max_retry_attempts(cls, v)` (line 2853): no docstring
  - `ActionOutboxSettings` (line 2857): Action outbox worker settings.
    - `clamp_batch_size(cls, v)` (line 2879): no docstring
  - `OntologyDeployOutboxSettings` (line 2883): Ontology deployment outbox worker settings (OMS embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 2951): no docstring
    - `clamp_batch_size(cls, v)` (line 2956): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 2961): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 2966): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 2971): no docstring
    - `clamp_retention_days(cls, v)` (line 2976): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 2981): no docstring
    - `clamp_purge_limit(cls, v)` (line 2986): no docstring
  - `ConnectorSyncSettings` (line 2990): Connector sync worker settings.
    - `clamp_max_retries(cls, v)` (line 3030): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3035): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3040): no docstring
  - `ConnectorTriggerSettings` (line 3044): Connector trigger service settings.
    - `strip_source_type(cls, v)` (line 3074): no docstring
    - `clamp_tick_seconds(cls, v)` (line 3079): no docstring
    - `clamp_poll_concurrency(cls, v)` (line 3084): no docstring
    - `clamp_outbox_batch(cls, v)` (line 3089): no docstring
  - `ObjectifySettings` (line 3093): Objectify worker settings.
    - `strip_worker_handler(cls, v)` (line 3169): no docstring
    - `clamp_batch_size(cls, v)` (line 3174): no docstring
    - `clamp_row_batch_size(cls, v)` (line 3179): no docstring
    - `clamp_bulk_update_batch_size(cls, v)` (line 3184): no docstring
    - `clamp_list_page_size(cls, v)` (line 3191): no docstring
    - `clamp_max_rows(cls, v)` (line 3196): no docstring
    - `clamp_lineage_max_links(cls, v)` (line 3201): no docstring
    - `clamp_max_retries(cls, v)` (line 3206): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3211): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3216): no docstring
    - `normalize_ontology_pk_validation_mode(cls, v)` (line 3221): no docstring
    - `clamp_dataset_primary_index_chunk_size(cls, v)` (line 3235): no docstring
    - `bulk_update_batch_size_effective(self)` (line 3239): no docstring
  - `IngestReconcilerSettings` (line 3243): Dataset ingest reconciler worker settings.
    - `fallback_alert_webhook_url(cls, v)` (line 3306): no docstring
    - `clamp_poll_seconds(cls, v)` (line 3315): no docstring
    - `clamp_stale_seconds(cls, v)` (line 3320): no docstring
    - `clamp_limit(cls, v)` (line 3325): no docstring
    - `clamp_lock_key(cls, v)` (line 3330): no docstring
    - `clamp_alert_published_threshold(cls, v)` (line 3335): no docstring
    - `clamp_alert_aborted_threshold(cls, v)` (line 3340): no docstring
    - `clamp_alert_cooldown_seconds(cls, v)` (line 3345): no docstring
  - `DatasetIngestOutboxSettings` (line 3349): Dataset ingest outbox worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3442): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3452): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3457): no docstring
    - `clamp_max_retries(cls, v)` (line 3462): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 3467): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 3472): no docstring
    - `clamp_retention_days(cls, v)` (line 3477): no docstring
    - `clamp_purge_limit(cls, v)` (line 3482): no docstring
    - `clamp_dlq_max_in_flight(cls, v)` (line 3487): no docstring
    - `clamp_dlq_delivery_timeout_ms(cls, v)` (line 3492): no docstring
    - `clamp_dlq_request_timeout_ms(cls, v)` (line 3497): no docstring
    - `clamp_dlq_retries(cls, v)` (line 3502): no docstring
  - `ObjectifyOutboxWorkerSettings` (line 3506): Objectify outbox worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3594): no docstring
    - `clamp_batch_size(cls, v)` (line 3599): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3609): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3614): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 3619): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 3624): no docstring
    - `clamp_retention_days(cls, v)` (line 3629): no docstring
    - `clamp_purge_limit(cls, v)` (line 3634): no docstring
    - `clamp_producer_max_in_flight(cls, v)` (line 3639): no docstring
    - `clamp_producer_delivery_timeout_ms(cls, v)` (line 3644): no docstring
    - `clamp_producer_request_timeout_ms(cls, v)` (line 3649): no docstring
    - `clamp_producer_retries(cls, v)` (line 3654): no docstring
  - `ObjectifyReconcilerSettings` (line 3658): Objectify reconciler worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3696): no docstring
    - `clamp_stale_after_seconds(cls, v)` (line 3701): no docstring
    - `clamp_enqueued_stale_seconds(cls, v)` (line 3706): no docstring
    - `clamp_lock_key(cls, v)` (line 3711): no docstring
    - `enqueued_stale_seconds_effective(self)` (line 3715): no docstring
  - `WritebackMaterializerSettings` (line 3720): Writeback materializer worker settings.
    - `db_names_list(self)` (line 3752): no docstring
  - `EventPublisherSettings` (line 3759): Event publisher (message relay) settings.
    - `fallback_poll_interval(cls, v)` (line 3821): no docstring
    - `fallback_batch_size(cls, v)` (line 3829): no docstring
    - `fallback_topic_bootstrap_timeout(cls, v)` (line 3837): no docstring
    - `clamp_poll_interval_seconds(cls, v)` (line 3843): no docstring
    - `clamp_batch_size(cls, v)` (line 3848): no docstring
    - `clamp_kafka_flush_batch_size(cls, v)` (line 3853): no docstring
    - `clamp_metrics_log_interval_seconds(cls, v)` (line 3860): no docstring
    - `clamp_lookback_seconds(cls, v)` (line 3865): no docstring
    - `clamp_lookback_max_keys(cls, v)` (line 3870): no docstring
    - `clamp_dedup_max_events(cls, v)` (line 3875): no docstring
    - `clamp_dedup_checkpoint_max_events(cls, v)` (line 3880): no docstring
    - `clamp_topic_bootstrap_timeout_seconds(cls, v)` (line 3885): no docstring
    - `kafka_flush_batch_size_effective(self)` (line 3889): no docstring
  - `AgentRetentionWorkerSettings` (line 3893): Agent session retention worker settings (SEC-005).
    - `clamp_poll_seconds(cls, v)` (line 3915): no docstring
    - `clamp_retention_days(cls, v)` (line 3920): no docstring
    - `normalize_action(cls, v)` (line 3925): no docstring
  - `SchemaChangeMonitorSettings` (line 3936): Schema change monitor settings for proactive drift detection.
    - `clamp_check_interval(cls, v)` (line 3962): no docstring
    - `clamp_cooldown(cls, v)` (line 3967): no docstring
  - `ChaosSettings` (line 3971): Chaos/fault injection settings (test-only).
    - `coerce_enabled(cls, v)` (line 4002): no docstring
    - `coerce_crash_once(cls, v)` (line 4013): no docstring
    - `clamp_crash_exit_code(cls, v)` (line 4024): no docstring
  - `WorkersSettings` (line 4028): Workers/services runtime settings.
  - `WritebackSettings` (line 4058): Ontology writeback + read overlay settings
  - `TestSettings` (line 4128): Test environment configuration
  - `GoogleSheetsSettings` (line 4152): Google Sheets integration settings
    - `fallback_google_api_key(cls, v)` (line 4188): no docstring
  - `ApplicationSettings` (line 4195): Main application settings - aggregates all other settings
    - `normalize_environment(cls, v)` (line 4247): no docstring
    - `is_development(self)` (line 4268): Check if running in development mode
    - `is_production(self)` (line 4273): Check if running in production mode
    - `is_test(self)` (line 4278): Check if running in test mode
    - `is_pytest(self)` (line 4283): Check if running under pytest (PYTEST_CURRENT_TEST set).

### `backend/shared/dependencies/__init__.py`

### `backend/shared/dependencies/container.py`
- **Functions**
  - `async get_container()` (line 335): Get the global service container
  - `async initialize_container(settings)` (line 353): Initialize the global service container (thread-safe)
  - `async shutdown_container()` (line 379): Shutdown the global service container
  - `async container_lifespan(settings)` (line 394): Async context manager for container lifecycle
  - `get_settings_from_container()` (line 416): Get settings from the global container (synchronous)
- **Classes**
  - `ServiceLifecycle` (line 29): Protocol for services that have lifecycle management
    - `async initialize(self)` (line 32): Initialize the service
    - `async health_check(self)` (line 36): Check if the service is healthy
    - `async shutdown(self)` (line 40): Shutdown the service gracefully
  - `ServiceFactory` (line 45): Protocol for service factory functions
    - `__call__(self, settings)` (line 48): Create a service instance from settings
  - `ServiceRegistration` (line 54): Service registration information
  - `ServiceContainer` (line 63): Modern dependency injection container
    - `__init__(self, settings)` (line 71): Initialize the service container
    - `is_initialized(self)` (line 84): Check if container is initialized
    - `register_singleton(self, service_type, factory)` (line 88): Register a singleton service with a factory function
    - `register_instance(self, service_type, instance)` (line 111): Register a service instance directly
    - `async get(self, service_type)` (line 131): Get a service instance (thread-safe)
    - `get_sync(self, service_type)` (line 183): Get a service instance synchronously (for use in factory functions)
    - `has(self, service_type)` (line 227): Check if a service is registered
    - `is_created(self, service_type)` (line 239): Check if a service instance has been created
    - `async health_check_all(self)` (line 254): Perform health check on all created services
    - `async shutdown_all(self)` (line 280): Shutdown all created services gracefully
    - `get_service_info(self)` (line 305): Get information about registered services
    - `async initialize_container(self)` (line 322): Initialize the container and mark as ready

### `backend/shared/dependencies/providers.py`
- **Functions**
  - `async get_settings_dependency()` (line 42): FastAPI dependency to get application settings
  - `async get_label_mapper(container)` (line 52): no docstring
  - `async get_jsonld_converter(container)` (line 62): no docstring
  - `async get_storage_service(container)` (line 72): FastAPI dependency to get StorageService instance
  - `async get_lakefs_storage_service(container)` (line 95): FastAPI dependency to get LakeFSStorageService instance (S3 gateway via lakeFS).
  - `async get_redis_service(container)` (line 109): FastAPI dependency to get RedisService instance
  - `async get_elasticsearch_service(container)` (line 128): FastAPI dependency to get ElasticsearchService instance
  - `async get_lineage_store(container)` (line 146): FastAPI dependency to get LineageStore instance.
  - `async get_audit_log_store(container)` (line 155): FastAPI dependency to get AuditLogStore instance.
  - `async get_llm_gateway(container)` (line 163): FastAPI dependency to get LLMGateway instance.
  - `async get_background_task_manager(container)` (line 171): FastAPI dependency to get BackgroundTaskManager instance.
  - `async get_initialized_background_task_manager(container)` (line 194): FastAPI dependency to get an already-initialized BackgroundTaskManager, if present.
  - `register_core_services(container)` (line 222): Register all core services with the container
  - `async health_check_core_services(container)` (line 246): Perform health check on all core services

### `backend/shared/dependencies/type_inference.py`
- **Functions**
  - `configure_type_inference_service(service)` (line 14): Configure the type inference service implementation.
  - `get_type_inference_service()` (line 28): Get the configured type inference service.
  - `type_inference_dependency()` (line 49): FastAPI dependency function for type inference service.
  - `reset_type_inference_service()` (line 63): Reset the type inference service (mainly for testing).

### `backend/shared/errors/__init__.py`

### `backend/shared/errors/enterprise_catalog.py`
- **Functions**
  - `_normalize_subsystem(service_name)` (line 183): no docstring
  - `is_external_code(value)` (line 2421): no docstring
  - `_resolve_http_status(spec, status_code, prefer_status_code)` (line 2427): no docstring
  - `_resolve_retryable(spec, retryable_hint)` (line 2440): no docstring
  - `_resolve_http_status_hint(spec, status_code)` (line 2452): no docstring
  - `_resolve_default_retry_policy(spec)` (line 2458): no docstring
  - `_resolve_human_required(spec)` (line 2464): no docstring
  - `_resolve_max_attempts(spec, retry_policy)` (line 2470): no docstring
  - `_resolve_base_delay_ms(spec, retry_policy)` (line 2479): no docstring
  - `_resolve_max_delay_ms(spec, retry_policy)` (line 2488): no docstring
  - `_resolve_jitter_strategy(spec, retry_policy)` (line 2497): no docstring
  - `_resolve_retry_after_header_respect(spec)` (line 2503): no docstring
  - `enterprise_catalog_fingerprint()` (line 2512): no docstring
  - `_resolve_runbook_ref(spec, legacy_code)` (line 2552): no docstring
  - `_resolve_safe_next_actions(spec, legacy_code, retry_policy, human_required)` (line 2558): no docstring
  - `_resolve_action(spec)` (line 2587): no docstring
  - `_resolve_owner(spec)` (line 2591): no docstring
  - `resolve_enterprise_error(service_name, code, category, status_code, external_code, retryable_hint, prefer_status_code)` (line 2595): no docstring
  - `resolve_objectify_error(error)` (line 2662): no docstring
  - `_normalize_objectify_error_key(error)` (line 2715): no docstring
- **Classes**
  - `EnterpriseSeverity` (line 14): no docstring
  - `EnterpriseDomain` (line 21): no docstring
  - `EnterpriseClass` (line 37): no docstring
  - `EnterpriseAction` (line 52): no docstring
  - `EnterpriseRetryPolicy` (line 65): no docstring
  - `EnterpriseJitterStrategy` (line 72): no docstring
  - `EnterpriseSafeNextAction` (line 77): no docstring
  - `EnterpriseOwner` (line 85): no docstring
  - `EnterpriseSubsystem` (line 91): no docstring
  - `EnterpriseErrorSpec` (line 104): no docstring
  - `EnterpriseError` (line 126): no docstring
    - `to_dict(self)` (line 150): no docstring

### `backend/shared/errors/error_envelope.py`
- **Functions**
  - `_normalize_origin(service_name, origin)` (line 32): no docstring
  - `_derive_category_code(enterprise)` (line 45): no docstring
  - `_runbook_uri(runbook_ref)` (line 51): no docstring
  - `_build_diagnostics(service_name, code, category, http_status, enterprise, enterprise_payload, origin, request_id, correlation_id, trace_id, span_id)` (line 58): no docstring
  - `build_error_envelope(service_name, message, detail, code, category, status_code, errors, context, external_code, objectify_error, enterprise, origin, request_id, correlation_id, trace_id, prefer_status_code)` (line 116): no docstring

### `backend/shared/errors/error_response.py`
- **Functions**
  - `_get_request_id(request)` (line 49): no docstring
  - `_get_correlation_id(request)` (line 58): no docstring
  - `_get_origin(request, service_name)` (line 67): no docstring
  - `_normalize_message(detail)` (line 80): no docstring
  - `_extract_upstream_metadata(body)` (line 92): no docstring
  - `_extract_external_code(detail)` (line 107): no docstring
  - `_classify_upstream_url(url, status_code)` (line 130): no docstring
  - `_classify_db_error(exc)` (line 152): no docstring
  - `_build_payload(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 173): no docstring
  - `_sanitize_header_value(value, max_len)` (line 203): no docstring
  - `_build_error_headers(payload)` (line 216): no docstring
  - `_build_response(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 239): no docstring
  - `_resolve_validation_error(exc)` (line 272): no docstring
  - `install_error_handlers(app, service_name, validation_status)` (line 279): no docstring

### `backend/shared/errors/error_types.py`
- **Functions**
  - `classified_http_exception(status_code, detail, code, category, external_code, extra, headers)` (line 102): Create an HTTPException that carries enterprise error classification.
- **Classes**
  - `ErrorCategory` (line 6): no docstring
  - `ErrorCode` (line 17): no docstring

### `backend/shared/errors/legacy_codes.py`
- **Classes**
  - `LegacyErrorCode` (line 10): no docstring

### `backend/shared/errors/runtime_exception_policy.py`
- **Functions**
  - `_current_trace_id()` (line 56): no docstring
  - `_safe_text(value)` (line 77): no docstring
  - `_exception_fingerprint(zone, operation, exc, code)` (line 84): no docstring
  - `log_exception_rate_limited(logger, zone, operation, exc, code, category, warn_interval_seconds, context)` (line 103): no docstring
  - `fallback_value(policy, exc, logger, context)` (line 168): no docstring
  - `preserve_primary_exception(primary_exc, cleanup_exc, logger, operation, zone, code, category, warn_interval_seconds, context)` (line 191): no docstring
  - `assert_lineage_available(lineage_store, required, logger, operation, zone, context, warn_interval_seconds)` (line 226): no docstring
  - `async record_lineage_or_raise(lineage_store, required, record_call, logger, operation, zone, context, warn_interval_seconds)` (line 256): no docstring
- **Classes**
  - `RuntimeZone` (line 18): no docstring
  - `FallbackPolicy` (line 36): no docstring
  - `_RateState` (line 47): no docstring
  - `LineageUnavailableError` (line 218): Raised when lineage is required but no lineage store is available.
  - `LineageRecordError` (line 222): Raised when lineage recording fails in fail-closed mode.

### `backend/shared/i18n/__init__.py`

### `backend/shared/i18n/context.py`
- **Functions**
  - `set_language(lang)` (line 11): no docstring
  - `reset_language(token)` (line 15): no docstring
  - `get_language()` (line 19): no docstring

### `backend/shared/i18n/middleware.py`
- **Functions**
  - `install_i18n_middleware(app, max_body_bytes)` (line 15): Install request-scoped language + best-effort response localization.
  - `_rewrite_payload(payload, target_lang, status_code, api_status, is_root)` (line 99): no docstring

### `backend/shared/i18n/translator.py`
- **Functions**
  - `m(en, ko, lang, **params)` (line 10): Inline bilingual message helper.
  - `_generic_http_detail(status_code, lang)` (line 29): no docstring
  - `_generic_api_message(api_status, lang)` (line 50): no docstring
  - `_translate_known(text, target_lang)` (line 94): Small curated dictionary for common phrases.
  - `_translate_ko_to_en(text)` (line 141): no docstring
  - `localize_free_text(text, target_lang, status_code, api_status)` (line 162): Best-effort localization for existing free-text messages.

### `backend/shared/interfaces/__init__.py`

### `backend/shared/interfaces/type_inference.py`
- **Functions**
  - `get_production_type_inference_service()` (line 153): 🔥 Get REAL production type inference service!
  - `get_mock_type_inference_service()` (line 165): Legacy helper kept for backward compatibility.
- **Classes**
  - `TypeInferenceInterface` (line 13): Abstract interface for type inference services.
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 22): Analyze a single column and infer its type.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 46): Analyze an entire dataset and infer types for all columns.
    - `async infer_single_value_type(self, value, context)` (line 69): Infer the type of a single value.
  - `RealTypeInferenceService` (line 92): 🔥 REAL IMPLEMENTATION! Production-ready type inference service.
    - `__init__(self)` (line 100): Initialize with real pattern-based type detection service.
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 107): 🔥 REAL implementation using Funnel service algorithms.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 130): 🔥 REAL implementation using Funnel service algorithms.

### `backend/shared/middleware/__init__.py`

### `backend/shared/middleware/rate_limiter.py`
- **Functions**
  - `_is_valid_admin_bypass_token(headers)` (line 24): no docstring
  - `rate_limit(requests, window, strategy, cost)` (line 394): Rate limiting decorator for FastAPI endpoints
  - `install_rate_limit_headers_middleware(app)` (line 555): no docstring
  - `async get_rate_limiter()` (line 594): Get or create global rate limiter instance
- **Classes**
  - `TokenBucket` (line 35): Token Bucket algorithm implementation for rate limiting
    - `__init__(self, redis_client, capacity, refill_rate, key_prefix, fail_open)` (line 45): Initialize Token Bucket
    - `async consume(self, key, tokens)` (line 68): Try to consume tokens from the bucket
  - `LocalTokenBucket` (line 163): In-memory token bucket for degraded mode when Redis is unavailable.
    - `__init__(self, capacity, refill_rate, max_entries)` (line 166): no docstring
    - `_evict_if_needed(self)` (line 173): no docstring
    - `async consume(self, key, tokens)` (line 181): no docstring
  - `RateLimiter` (line 215): Rate limiting middleware for FastAPI
    - `__init__(self, redis_url)` (line 221): Initialize rate limiter
    - `async initialize(self)` (line 239): Initialize Redis connection
    - `async close(self)` (line 267): Close Redis connection
    - `get_bucket(self, bucket_type, capacity, refill_rate)` (line 272): Get or create a token bucket
    - `get_local_bucket(self, bucket_type, capacity, refill_rate)` (line 297): no docstring
    - `get_client_id(self, request, strategy)` (line 307): Get client identifier based on strategy
    - `async check_rate_limit(self, request, capacity, refill_rate, strategy, tokens)` (line 342): Check if request should be rate limited
  - `RateLimitPresets` (line 568): Common rate limit configurations

### `backend/shared/models/__init__.py`

### `backend/shared/models/agent_plan_report.py`
- **Classes**
  - `PlanDiagnosticSeverity` (line 17): no docstring
  - `PlanPatchOp` (line 22): RFC6902-like JSON patch operation.
    - `_validate_op(cls, value)` (line 37): no docstring
    - `_validate_path(cls, value)` (line 46): no docstring
    - `_validate_shape(self)` (line 53): no docstring
  - `PlanPatchProposal` (line 63): no docstring
  - `PlanDiagnostic` (line 74): no docstring
  - `PlanRequiredControl` (line 86): no docstring
  - `PlanPolicySnapshot` (line 95): no docstring
  - `PlanCompilationReport` (line 101): Machine-readable compilation report used by clients and agent UIs.

### `backend/shared/models/ai.py`
- **Classes**
  - `AIQueryMode` (line 20): no docstring
  - `AIQueryTool` (line 27): no docstring
  - `AIIntentType` (line 34): no docstring
  - `AIIntentRoute` (line 43): no docstring
  - `AIIntentRequest` (line 50): no docstring
  - `AIIntentDraft` (line 60): no docstring
  - `AIIntentResponse` (line 72): no docstring
    - `_validate_reply(self)` (line 83): no docstring
  - `AIQueryRequest` (line 89): no docstring
  - `AIAnswer` (line 99): no docstring
  - `DatasetListQuery` (line 106): no docstring
  - `AIQueryPlan` (line 112): LLM-produced query plan.
    - `_validate_shape(self)` (line 130): no docstring
  - `AIQueryResponse` (line 142): no docstring

### `backend/shared/models/audit_log.py`
- **Classes**
  - `AuditLogEntry` (line 20): no docstring

### `backend/shared/models/background_task.py`
- **Classes**
  - `TaskStatus` (line 15): Background task execution status.
  - `TaskProgress` (line 25): Task progress information for long-running operations.
  - `TaskResult` (line 34): Task execution result.
  - `BackgroundTask` (line 45): Complete background task representation.
    - `duration(self)` (line 83): Calculate task duration in seconds.
    - `is_running(self)` (line 90): Check if task is currently running.
    - `is_complete(self)` (line 95): Check if task has completed (successfully or not).
    - `is_successful(self)` (line 100): Check if task completed successfully.
  - `TaskMetrics` (line 105): Aggregated metrics for background tasks.
    - `active_tasks(self)` (line 118): Get number of active (running) tasks.
    - `finished_tasks(self)` (line 123): Get number of finished tasks.
  - `TaskFilter` (line 128): Filter criteria for querying tasks.
  - `TaskUpdateNotification` (line 138): Real-time task update notification model.

### `backend/shared/models/base.py`
- **Classes**
  - `VersionedModelMixin` (line 10): Mixin for adding optimistic locking version support to models.
    - `increment_version(self)` (line 28): Increment the version number for optimistic locking.
    - `check_version_conflict(self, expected_version)` (line 35): Check if there's a version conflict for optimistic locking.
    - `get_version_for_update(self)` (line 47): Get the current version for use in update operations.
  - `OptimisticLockError` (line 58): Exception raised when optimistic locking version conflict is detected.
    - `__init__(self, entity_type, entity_id, expected_version, actual_version)` (line 69): no docstring
  - `ConcurrencyControl` (line 89): Utility class for concurrency control operations.
    - `validate_version_for_update(current_version, provided_version)` (line 96): Validate version for update operation.
    - `get_next_version(current_version)` (line 119): Calculate next version number.

### `backend/shared/models/commands.py`
- **Classes**
  - `CommandType` (line 14): 명령 유형
  - `CommandStatus` (line 53): 명령 상태
  - `BaseCommand` (line 63): 기본 명령 모델
  - `OntologyCommand` (line 82): 온톨로지 관련 명령
    - `__init__(self, **data)` (line 87): no docstring
  - `PropertyCommand` (line 93): 속성 관련 명령
    - `__init__(self, **data)` (line 98): no docstring
  - `RelationshipCommand` (line 104): 관계 관련 명령
    - `__init__(self, **data)` (line 110): no docstring
  - `DatabaseCommand` (line 116): 데이터베이스 관련 명령
    - `__init__(self, **data)` (line 118): no docstring
  - `BranchCommand` (line 124): 브랜치 관련 명령
    - `__init__(self, **data)` (line 128): no docstring
  - `InstanceCommand` (line 134): 인스턴스 관련 명령
    - `__init__(self, **data)` (line 141): no docstring
  - `ActionCommand` (line 154): Action execution command (intent-only writeback).
    - `__init__(self, **data)` (line 165): no docstring
  - `CommandResult` (line 177): 명령 실행 결과

### `backend/shared/models/common.py`
- **Functions**
  - `_emit_base_response_deprecation_once()` (line 307): no docstring
- **Classes**
  - `DataType` (line 16): Data type enumeration
    - `from_python_type(cls, py_type)` (line 65): Convert Python type to DataType
    - `is_numeric(cls, data_type)` (line 76): Check if data type is numeric
    - `is_temporal(cls, data_type)` (line 94): Check if data type is temporal
    - `validate_value(self, value)` (line 109): Validate if value matches this data type
    - `is_complex_type(cls, data_type)` (line 133): Check if data type is complex
    - `get_base_type(cls, data_type)` (line 157): Get base type for complex types
  - `Cardinality` (line 214): Cardinality enumeration
    - `is_valid(cls, value)` (line 225): Check if value is a valid cardinality
  - `QueryOperator` (line 231): Query operator definition
  - `BaseResponse` (line 317): Deprecated compatibility wrapper for ApiResponse.
    - `__init__(self, status, message, data, errors)` (line 320): no docstring

### `backend/shared/models/config.py`
- **Classes**
  - `ConnectionConfig` (line 13): Database connection configuration
    - `__post_init__(self)` (line 27): Post-initialization validation
    - `from_settings(cls)` (line 46): Create ConnectionConfig from centralized ApplicationSettings.
    - `from_env(cls)` (line 63): Backward-compatible alias for from_settings().
    - `to_dict(self)` (line 67): Convert to dictionary
  - `AsyncConnectionInfo` (line 84): Async connection information
    - `__post_init__(self)` (line 93): Post-initialization setup
    - `mark_used(self)` (line 100): Mark connection as used
    - `can_create_connection(self)` (line 104): Check if new connection can be created
    - `to_dict(self)` (line 108): Convert to dictionary

### `backend/shared/models/event_envelope.py`
- **Classes**
  - `EventEnvelope` (line 23): Canonical event envelope.
    - `_normalize_datetime(value)` (line 51): no docstring
    - `from_command(cls, command, actor, event_type, kafka_topic, metadata)` (line 57): no docstring
    - `from_base_event(cls, event, kafka_topic, metadata)` (line 113): no docstring
    - `from_connector_update(cls, source_type, source_id, cursor, previous_cursor, sequence_number, occurred_at, event_type, actor, kafka_topic, data, metadata)` (line 149): Build a canonical connector update envelope.
    - `as_kafka_key(self)` (line 222): no docstring
    - `as_json(self)` (line 226): no docstring

### `backend/shared/models/events.py`
- **Classes**
  - `EventType` (line 14): 이벤트 유형
  - `BaseEvent` (line 58): 기본 이벤트 모델
  - `OntologyEvent` (line 76): 온톨로지 관련 이벤트
    - `__init__(self, **data)` (line 82): no docstring
  - `PropertyEvent` (line 91): 속성 관련 이벤트
    - `__init__(self, **data)` (line 97): no docstring
  - `RelationshipEvent` (line 105): 관계 관련 이벤트
    - `__init__(self, **data)` (line 112): no docstring
  - `DatabaseEvent` (line 120): 데이터베이스 관련 이벤트
    - `__init__(self, **data)` (line 124): no docstring
  - `BranchEvent` (line 132): 브랜치 관련 이벤트
    - `__init__(self, **data)` (line 137): no docstring
  - `InstanceEvent` (line 145): 인스턴스 관련 이벤트
    - `__init__(self, **data)` (line 154): no docstring
  - `ActionAppliedEvent` (line 166): Action applied (writeback patchset commit) event.
    - `__init__(self, **data)` (line 175): no docstring
  - `CommandFailedEvent` (line 185): 명령 실패 이벤트
    - `__init__(self, **data)` (line 192): no docstring

### `backend/shared/models/google_sheets.py`
- **Functions**
  - `_validate_google_sheet_url_field(value)` (line 12): no docstring
- **Classes**
  - `GoogleSheetUrlValidatedModel` (line 23): no docstring
    - `validate_google_sheet_url(cls, v)` (line 26): no docstring
  - `GoogleSheetPreviewRequest` (line 30): Google Sheet preview request model
  - `GoogleSheetPreviewResponse` (line 47): Google Sheet preview response model
    - `validate_sheet_id(cls, v)` (line 60): Validate sheet ID format
    - `validate_columns(cls, v)` (line 68): Validate columns
    - `validate_sample_rows(cls, v)` (line 76): Validate sample rows
    - `validate_total_rows(cls, v)` (line 84): Validate total rows
    - `validate_total_columns(cls, v)` (line 92): Validate total columns
  - `GoogleSheetError` (line 99): Google Sheet error response model
  - `GoogleSheetRegisterRequest` (line 117): Google Sheet registration request model
  - `GoogleSheetRegisterResponse` (line 133): Google Sheet registration response model

### `backend/shared/models/graph_query.py`
- **Classes**
  - `GraphHop` (line 16): Represents a single hop in a graph traversal.
  - `GraphQueryRequest` (line 24): Request model for multi-hop graph queries.
  - `GraphNode` (line 45): Graph node with ES document reference.
  - `GraphEdge` (line 60): Graph edge between nodes.
  - `GraphQueryResponse` (line 69): Response model for graph queries.
  - `SimpleGraphQueryRequest` (line 87): Request for simple single-class queries.

### `backend/shared/models/i18n.py`

### `backend/shared/models/lineage.py`
- **Classes**
  - `LineageNode` (line 20): no docstring
  - `LineageEdge` (line 31): no docstring
  - `LineageGraph` (line 43): no docstring

### `backend/shared/models/objectify_job.py`
- **Classes**
  - `ObjectifyJob` (line 16): no docstring
    - `_validate_inputs(self)` (line 58): no docstring

### `backend/shared/models/ontology.py`
- **Functions**
  - `_validate_localized_required(value, field_name)` (line 29): no docstring
  - `_validate_localized_optional(value, field_name)` (line 60): no docstring
  - `_validate_unique_property_list(value)` (line 66): no docstring
  - `_validate_unique_relationship_list(value)` (line 74): no docstring
- **Classes**
  - `Cardinality` (line 18): Cardinality enumeration
  - `QueryOperator` (line 82): Query operator definition
  - `OntologyBase` (line 90): Base ontology model with MVCC support through version field.
    - `validate_id(cls, v)` (line 111): Validate ID format
    - `validate_label(cls, v)` (line 119): no docstring
    - `validate_description(cls, v)` (line 124): no docstring
    - `set_timestamps(cls, values)` (line 129): Set timestamps if not provided
  - `Relationship` (line 149): Relationship model
    - `validate_label(cls, v)` (line 162): no docstring
    - `validate_description(cls, v)` (line 167): no docstring
    - `validate_inverse_label(cls, v)` (line 172): no docstring
    - `validate_cardinality(cls, v)` (line 177): Validate cardinality format
  - `Property` (line 187): Property model with class reference support
    - `validate_name(cls, v)` (line 231): Validate property name
    - `validate_type(cls, v)` (line 239): Validate property type
    - `validate_label(cls, v)` (line 247): no docstring
    - `validate_description(cls, v)` (line 252): no docstring
    - `is_class_reference(self)` (line 255): Check if this property is a class reference (ObjectProperty)
    - `to_relationship(self)` (line 296): Convert property to relationship format
  - `OntologyCreateRequest` (line 325): Request model for creating ontology
    - `validate_id(cls, v)` (line 341): Validate ID format (optional).
    - `validate_label(cls, v)` (line 351): Validate label is not empty (string or language map).
    - `validate_properties(cls, v)` (line 365): no docstring
    - `validate_relationships(cls, v)` (line 370): no docstring
  - `OntologyUpdateRequest` (line 374): Request model for updating ontology
    - `validate_label(cls, v)` (line 387): no docstring
    - `validate_description(cls, v)` (line 392): no docstring
    - `validate_properties(cls, v)` (line 397): no docstring
    - `validate_relationships(cls, v)` (line 402): no docstring
    - `has_changes(self)` (line 405): Check if request has any changes
  - `OntologyResponse` (line 420): Response model for ontology operations
    - `validate_structure(self)` (line 431): Validate ontology structure
  - `QueryFilter` (line 455): Query filter model
    - `validate_field(cls, v)` (line 464): Validate field name
    - `validate_operator(cls, v)` (line 472): Validate operator
  - `QueryInput` (line 492): Query input model
    - `validate_limit(cls, v)` (line 506): Validate limit
    - `validate_offset(cls, v)` (line 514): Validate offset
    - `validate_order_direction(cls, v)` (line 522): Validate order direction
    - `validate_class_identifier(self)` (line 529): Validate that either class_label or class_id is provided

### `backend/shared/models/ontology_lint.py`
- **Classes**
  - `LintSeverity` (line 14): no docstring
  - `LintIssue` (line 20): no docstring
  - `LintReport` (line 33): no docstring

### `backend/shared/models/ontology_resources.py`
- **Classes**
  - `OntologyResourceBase` (line 16): Base fields shared by ontology resource definitions.
    - `_validate_id(cls, value)` (line 29): no docstring
    - `_set_timestamps(cls, values)` (line 36): no docstring
  - `SharedPropertyDefinition` (line 52): Reusable property templates for object types.
  - `ValueTypeDefinition` (line 58): Semantic value type definition (e.g., Money, GeoPoint).
  - `InterfaceDefinition` (line 67): Interface-style contract shared across object types.
  - `LinkTypeDefinition` (line 74): Link type definition between object types.
  - `GroupDefinition` (line 86): Grouping / module metadata for ontology resources.
  - `FunctionDefinition` (line 92): Derived field/function definition.
  - `ActionTypeDefinition` (line 101): Action template definition.
  - `OntologyResourceRecord` (line 116): Standardized API shape for ontology resources.

### `backend/shared/models/ontology_validation_mixin.py`
- **Classes**
  - `CardinalityValidationMixin` (line 10): no docstring
    - `is_valid_cardinality(self)` (line 13): no docstring
  - `PropertyValueValidationMixin` (line 17): no docstring
    - `validate_value(self, value)` (line 23): no docstring

### `backend/shared/models/pipeline_agent.py`
- **Classes**
  - `PipelineOutputBinding` (line 10): no docstring
  - `PipelineAgentRunRequest` (line 26): no docstring

### `backend/shared/models/pipeline_job.py`
- **Classes**
  - `PipelineJob` (line 16): no docstring
    - `_compute_dedupe_key(self)` (line 37): Auto-compute dedupe_key if not provided.
    - `build_dedupe_key(pipeline_id, mode, branch, definition_hash, idempotency_key, node_id)` (line 63): Build dedupe key from components (for external use).

### `backend/shared/models/pipeline_plan.py`
- **Classes**
  - `PipelinePlanOutputKind` (line 29): no docstring
  - `PipelinePlanDataScope` (line 37): no docstring
  - `PipelinePlanOutput` (line 45): no docstring
    - `_normalize_output_kind(cls, value)` (line 61): no docstring
  - `PipelinePlanAssociation` (line 65): no docstring
    - `_normalize_fields(cls, data)` (line 80): no docstring
  - `PipelinePlan` (line 156): no docstring
    - `_validate_definition(cls, value)` (line 171): no docstring

### `backend/shared/models/pipeline_task_spec.py`
- **Classes**
  - `PipelineTaskScope` (line 17): no docstring
  - `PipelineTaskIntent` (line 22): no docstring
  - `PipelineTaskSpec` (line 31): Typed task spec returned by the task classifier.

### `backend/shared/models/query_operator_mixin.py`
- **Classes**
  - `QueryOperatorApplicabilityMixin` (line 6): no docstring
    - `can_apply_to(self, data_type)` (line 9): no docstring

### `backend/shared/models/requests.py`
- **Classes**
  - `BranchCreateRequest` (line 14): Request model for creating a branch
  - `CheckoutRequest` (line 23): Request model for checking out a branch or commit
  - `CommitRequest` (line 30): Request model for creating a commit
  - `MergeRequest` (line 38): Request model for merging branches
  - `RollbackRequest` (line 47): Request model for rolling back changes
  - `DatabaseCreateRequest` (line 54): Request model for creating a database
  - `MappingImportRequest` (line 62): Request model for importing mappings

### `backend/shared/models/responses.py`
- **Classes**
  - `ApiResponse` (line 13): Standardized API response model for all SPICE HARVESTER services
    - `to_dict(self)` (line 26): Convert to dictionary for JSON response
    - `success(cls, message, data)` (line 36): Create success response (200 OK)
    - `created(cls, message, data)` (line 41): Create resource created response (201 Created)
    - `accepted(cls, message, data)` (line 46): Create accepted response (202 Accepted)
    - `no_content(cls, message)` (line 51): Create no content response (204 No Content)
    - `error(cls, message, errors)` (line 56): Create error response (4xx/5xx status codes)
    - `warning(cls, message, data)` (line 61): Create warning response (successful but with warnings)
    - `partial(cls, message, data, errors)` (line 66): Create partial success response (some operations succeeded, some failed)
    - `health_check(cls, service_name, version, description)` (line 73): Create standardized health check response
    - `is_success(self)` (line 83): Check if response indicates success
    - `is_error(self)` (line 87): Check if response indicates error
    - `is_warning(self)` (line 91): Check if response has warnings

### `backend/shared/models/sheet_grid.py`
- **Classes**
  - `GoogleSheetGridRequest` (line 18): Request for extracting a full grid (values + merges) from a Google Sheet URL.
  - `GoogleSheetStructureAnalysisRequest` (line 35): Request for end-to-end Google Sheets structure analysis via grid extraction.
  - `SheetGrid` (line 45): Normalized sheet representation (0-based coordinates).

### `backend/shared/models/structure_analysis.py`
- **Classes**
  - `CellAddress` (line 17): 0-based cell address (row, col).
  - `BoundingBox` (line 26): 0-based inclusive bounding box.
  - `MergeRange` (line 37): Merged cell range (inclusive).
  - `KeyValueItem` (line 43): Extracted key-value item from a sheet-like grid.
  - `HeaderTreeNode` (line 57): Hierarchical header node for multi-row/grouped headers.
  - `CellEvidence` (line 68): Small evidence sample for explainability/debugging.
  - `DetectedTable` (line 80): Detected table-like block.
  - `ColumnProvenance` (line 129): Lineage hook: where a field came from in the source grid.
  - `SheetStructureAnalysisRequest` (line 140): Request for structure analysis on a raw 2D grid.
  - `SheetStructureAnalysisResponse` (line 159): Structure analysis output: table blocks + key-value metadata.

### `backend/shared/models/structure_patch.py`
- **Classes**
  - `SheetStructurePatchOp` (line 27): Single patch operation (applied in order).
  - `SheetStructurePatch` (line 48): Patch bundle stored per sheet_signature.

### `backend/shared/models/sync_wrapper.py`
- **Classes**
  - `SyncOptions` (line 11): 동기 API 실행 옵션
  - `SyncResult` (line 46): 동기 API 실행 결과
  - `TimeoutError` (line 86): Command 실행 타임아웃 에러
    - `__init__(self, command_id, timeout, last_status)` (line 89): no docstring

### `backend/shared/models/type_inference.py`
- **Functions**
  - `_default_risk_policy()` (line 12): no docstring
- **Classes**
  - `TypeInferenceResult` (line 16): Type inference result with confidence and reasoning
  - `FunnelRiskItem` (line 25): Risk signal emitted by Funnel (suggestion-only).
  - `ColumnProfile` (line 40): Lightweight column profiling summary (sample-based).
  - `ColumnAnalysisResult` (line 48): Analysis result for a single column
  - `FunnelAnalysisPayload` (line 92): Funnel analysis payload (suggestion-only).
  - `DatasetAnalysisRequest` (line 100): Request for dataset type analysis
  - `DatasetAnalysisResponse` (line 109): Response for dataset type analysis
  - `SchemaGenerationRequest` (line 123): Request for schema generation based on analysis
  - `SchemaGenerationResponse` (line 133): Generated schema based on type analysis
  - `FunnelPreviewRequest` (line 142): Request for data preview with type inference
  - `FunnelPreviewResponse` (line 151): Preview response with inferred types
  - `TypeMappingRequest` (line 168): Request for mapping inferred types to target schema
  - `TypeMappingResponse` (line 176): Response with mapped types for target system

### `backend/shared/observability/__init__.py`

### `backend/shared/observability/config_monitor.py`
- **Classes**
  - `ConfigChangeType` (line 28): Types of configuration changes
  - `ConfigSeverity` (line 37): Severity levels for configuration issues
  - `ConfigChange` (line 46): Represents a configuration change
    - `to_dict(self)` (line 58): Convert to dictionary for serialization
    - `_sanitize_value(self, value)` (line 72): Sanitize sensitive values for logging
    - `_is_sensitive_key(self, key_path)` (line 78): Check if a key path contains sensitive information
  - `ConfigValidationRule` (line 88): Configuration validation rule
  - `ConfigSecurityAudit` (line 99): Security audit result for configuration
    - `to_dict(self)` (line 107): Convert to dictionary for serialization
  - `ConfigurationMonitor` (line 118): Monitors configuration changes and provides observability
    - `__init__(self, settings)` (line 126): no docstring
    - `_initialize_default_validation_rules(self)` (line 139): Initialize default configuration validation rules
    - `add_validation_rule(self, rule)` (line 210): Add custom validation rule
    - `add_change_callback(self, callback)` (line 215): Add callback for configuration changes
    - `get_config_snapshot(self)` (line 219): Get current configuration snapshot
    - `calculate_config_hash(self, config_dict)` (line 255): Calculate hash of configuration for change detection
    - `detect_changes(self)` (line 260): Detect configuration changes since last check
    - `_compare_snapshots(self, old_snapshot, new_snapshot)` (line 291): Compare two configuration snapshots and return changes
    - `_analyze_change_impact(self, change)` (line 351): Analyze the impact of a configuration change
    - `validate_configuration(self)` (line 384): Validate current configuration against rules
    - `perform_security_audit(self)` (line 410): Perform comprehensive security audit of configuration
    - `_check_debug_mode_in_production(self, config)` (line 448): Check if debug mode is enabled in production
    - `_check_default_passwords(self, config)` (line 467): Check for default or weak passwords
    - `_check_insecure_connections(self, config)` (line 491): Check for insecure connection configurations
    - `_check_exposed_sensitive_data(self, config)` (line 513): Check for potentially exposed sensitive data
    - `_check_weak_authentication(self, config)` (line 524): Check for weak authentication configurations
    - `_get_nested_value(self, data, key_path)` (line 546): Get nested value from dictionary using dot notation
    - `_sanitize_sensitive_value(self, key_path, value)` (line 559): Sanitize sensitive values for display
    - `_count_total_settings(self, config)` (line 566): Count total number of configuration settings
    - `get_change_history(self, limit)` (line 576): Get recent configuration change history
    - `get_configuration_report(self)` (line 581): Get comprehensive configuration report

### `backend/shared/observability/context_propagation.py`
- **Functions**
  - `_to_text(value)` (line 62): no docstring
  - `_encode_baggage_value(value)` (line 78): no docstring
  - `_merge_baggage(existing, request_id, correlation_id, db_name)` (line 82): no docstring
  - `carrier_from_kafka_headers(kafka_headers)` (line 111): Extract a W3C carrier dict from confluent_kafka headers.
  - `carrier_from_envelope_metadata(payload_or_metadata)` (line 130): Extract a W3C carrier dict from an EventEnvelope.metadata-like dict.
  - `kafka_headers_from_carrier(carrier)` (line 179): no docstring
  - `kafka_headers_from_envelope_metadata(payload_or_metadata)` (line 193): no docstring
  - `kafka_headers_with_dedup(payload_or_metadata, dedup_id, event_id, aggregate_id)` (line 198): Build Kafka headers with deduplication ID for idempotent message processing.
  - `kafka_headers_from_current_context()` (line 237): Build Kafka headers (W3C Trace Context + baggage) from the current OTel context.
  - `enrich_metadata_with_current_trace(metadata)` (line 261): Mutate a metadata/payload dict by adding W3C trace context keys.
  - `attach_context_from_carrier(carrier, service_name)` (line 333): Attach an extracted context for the duration of the `with` block.
  - `attach_context_from_kafka(kafka_headers, fallback_metadata, service_name)` (line 378): Attach trace context from Kafka headers (preferred) or fallback metadata.

### `backend/shared/observability/logging.py`
- **Functions**
  - `install_trace_context_record_factory()` (line 40): Install a global LogRecord factory that always provides `trace_id`/`span_id`.
  - `install_trace_context_filter(logger)` (line 174): Install TraceContextFilter on the given logger (default: root logger).
- **Classes**
  - `TraceContextFilter` (line 113): Attach `trace_id` and `span_id` fields to every LogRecord.
    - `filter(self, record)` (line 120): no docstring

### `backend/shared/observability/metrics.py`
- **Functions**
  - `_log_no_op_once(reason)` (line 67): no docstring
  - `initialize_metrics_provider(service_name)` (line 75): Configure a global MeterProvider so OTel metrics are actually exported.
  - `_prom_counter(name, description, labelnames)` (line 145): no docstring
  - `_prom_histogram(name, description, labelnames)` (line 153): no docstring
  - `_prom_gauge(name, description, labelnames)` (line 161): no docstring
  - `measure_time(metric_name, collector)` (line 666): Decorator for measuring function execution time
  - `prometheus_latest()` (line 779): Render Prometheus metrics for `/metrics`.
  - `get_metrics_collector(service_name)` (line 793): Get or create global metrics collector
- **Classes**
  - `_SettingsValue` (line 42): no docstring
    - `__init__(self, getter)` (line 43): no docstring
    - `__get__(self, instance, owner)` (line 46): no docstring
  - `OpenTelemetryMetricsConfig` (line 50): no docstring
  - `MetricsCollector` (line 169): Centralized metrics collection based on Context7 patterns
    - `__init__(self, service_name)` (line 174): Initialize metrics collector
    - `_initialize_metrics(self)` (line 187): Initialize all metrics
    - `record_request(self, method, endpoint, status_code, duration, request_size, response_size)` (line 415): Record HTTP request metrics
    - `record_db_query(self, operation, table, duration, success)` (line 464): Record database query metrics
    - `record_cache_access(self, hit, cache_name)` (line 503): Record cache access
    - `record_event(self, event_type, action, duration)` (line 528): Record event sourcing metrics
    - `record_rate_limit(self, endpoint, rejected, strategy)` (line 566): Record rate limiting metrics
    - `record_business_metric(self, metric_name, value, attributes)` (line 603): Record custom business metrics
    - `timer(self, metric_name, attributes)` (line 639): Context manager for timing operations
  - `RequestMetricsMiddleware` (line 720): FastAPI middleware for automatic request metrics collection
    - `__init__(self, metrics_collector)` (line 725): Initialize middleware
    - `async __call__(self, request, call_next)` (line 735): Process request and collect metrics

### `backend/shared/observability/request_context.py`
- **Functions**
  - `_norm(value)` (line 46): no docstring
  - `generate_request_id()` (line 50): no docstring
  - `get_request_id()` (line 54): no docstring
  - `get_correlation_id()` (line 58): no docstring
  - `get_db_name()` (line 62): no docstring
  - `get_principal()` (line 66): no docstring
  - `parse_baggage_header(header_value)` (line 70): Best-effort parser for W3C `baggage` header.
  - `context_from_headers(headers)` (line 104): no docstring
  - `context_from_metadata(metadata)` (line 121): no docstring
  - `request_context(request_id, correlation_id, db_name, principal, inject_baggage)` (line 144): Attach request/debug context for the duration of a `with` block.

### `backend/shared/observability/tracing.py`
- **Functions**
  - `_sanitize_span_attributes(attributes)` (line 29): Ensure span attributes satisfy OpenTelemetry type constraints.
  - `get_tracing_service(service_name)` (line 525): no docstring
  - `trace_endpoint(name)` (line 534): Lazily create a tracing decorator for request handlers.
  - `trace_db_operation(name)` (line 547): no docstring
  - `trace_external_call(name)` (line 553): no docstring
  - `trace_storage_operation(name, system)` (line 558): Trace storage operations (S3/MinIO, Elasticsearch, Redis, etc.).
  - `trace_kafka_operation(name)` (line 565): Trace Kafka produce/consume operations.
  - `_lazy_trace(name, kind, attributes)` (line 572): no docstring
- **Classes**
  - `_SettingsValue` (line 174): no docstring
    - `__init__(self, getter)` (line 175): no docstring
    - `__get__(self, instance, owner)` (line 178): no docstring
  - `OpenTelemetryConfig` (line 182): no docstring
  - `TracingService` (line 207): Distributed tracing facade.
    - `__init__(self, service_name)` (line 215): no docstring
    - `_log_no_op_once(self, reason)` (line 224): no docstring
    - `initialize(self)` (line 230): no docstring
    - `instrument_fastapi(self, app)` (line 323): no docstring
    - `instrument_clients(self)` (line 339): no docstring
    - `span(self, name, kind, attributes)` (line 423): no docstring
    - `trace(self, name, kind, attributes)` (line 445): no docstring
    - `get_current_span(self)` (line 465): no docstring
    - `record_exception(self, exception)` (line 470): no docstring
    - `set_span_attribute(self, key, value)` (line 477): no docstring
    - `get_trace_id(self)` (line 483): no docstring
    - `get_span_id(self)` (line 492): no docstring
    - `inject_trace_context(self, headers)` (line 501): no docstring
    - `extract_trace_context(self, headers)` (line 511): no docstring

### `backend/shared/routers/__init__.py`

### `backend/shared/routers/config_monitoring.py`
- **Functions**
  - `async get_config_monitor(settings)` (line 35): Get or create configuration monitor
  - `async get_current_configuration(include_validation, monitor)` (line 57): Get current application configuration
  - `async get_configuration_changes(limit, severity, change_type, since, monitor)` (line 91): Get configuration change history
  - `async validate_configuration(monitor)` (line 180): Validate current configuration
  - `async perform_security_audit(monitor)` (line 224): Perform security audit of configuration
  - `async get_configuration_report(monitor)` (line 269): Get comprehensive configuration report
  - `async check_configuration_changes(background_tasks, monitor)` (line 309): Manually trigger configuration change detection
  - `async analyze_environment_drift(compare_environment, monitor)` (line 339): Analyze configuration drift between environments
  - `async analyze_configuration_health_impact(monitor)` (line 446): Analyze configuration health impact
  - `async get_monitoring_status(monitor)` (line 539): Get configuration monitoring system status

### `backend/shared/routers/monitoring.py`
- **Functions**
  - `_resolve_service_name(request)` (line 33): no docstring
  - `async _check_service_instance(instance)` (line 41): Best-effort, runtime-validated service health check.
  - `async basic_health_check()` (line 87): Basic health check endpoint
  - `async detailed_health_check(include_metrics, settings, container)` (line 103): Detailed health check with comprehensive service information
  - `async readiness_probe(container)` (line 167): Kubernetes readiness probe
  - `async liveness_probe(container)` (line 189): Kubernetes liveness probe
  - `async get_service_metrics(service_name)` (line 224): Get comprehensive service metrics
  - `async get_service_status(container)` (line 251): Get current status of all services
  - `async get_configuration_overview(include_sensitive, settings)` (line 278): Get current application configuration
  - `async restart_service(service_name, _)` (line 348): Restart a specific service
  - `async get_service_dependencies(_)` (line 369): Get service dependency information
  - `async get_background_task_metrics(request, task_manager)` (line 388): Get background task execution metrics
  - `async get_active_background_tasks(request, limit, task_manager)` (line 453): Get list of all active background tasks
  - `async get_background_task_health(request, task_manager)` (line 531): Get health status of background task processing system

### `backend/shared/security/__init__.py`

### `backend/shared/security/auth_utils.py`
- **Functions**
  - `get_expected_token(env_keys)` (line 17): no docstring
  - `extract_presented_token(headers)` (line 41): no docstring
  - `auth_disable_allowed(allow_disable_env_keys)` (line 51): no docstring
  - `auth_required(require_env_key, token_env_keys, default_required, allow_pytest, pytest_env_key)` (line 64): no docstring
  - `get_exempt_paths(env_key, defaults)` (line 87): no docstring
  - `is_exempt_path(path, exempt_paths)` (line 102): no docstring
  - `get_db_scope(headers)` (line 106): no docstring
  - `enforce_db_scope(headers, db_name, require_env_key)` (line 114): no docstring

### `backend/shared/security/data_encryption.py`
- **Functions**
  - `_strip_prefix(value, prefix)` (line 17): no docstring
  - `_b64decode(value)` (line 24): no docstring
  - `_b64encode(value)` (line 32): no docstring
  - `parse_encryption_keys(raw)` (line 36): Parse a comma-separated list of keys from env/settings.
  - `is_encrypted_text(value)` (line 156): no docstring
  - `is_encrypted_json(value)` (line 160): no docstring
  - `is_encrypted_bytes(value)` (line 164): no docstring
  - `encryptor_from_keys(raw_keys)` (line 168): no docstring
- **Classes**
  - `DataEncryptor` (line 69): no docstring
    - `_aesgcm(self, key)` (line 72): no docstring
    - `encrypt_text(self, plaintext, aad)` (line 75): no docstring
    - `decrypt_text(self, ciphertext, aad)` (line 85): no docstring
    - `encrypt_bytes(self, plaintext, aad)` (line 107): no docstring
    - `decrypt_bytes(self, ciphertext, aad)` (line 116): no docstring
    - `encrypt_json(self, value, aad)` (line 136): no docstring
    - `decrypt_json(self, value, aad)` (line 146): no docstring

### `backend/shared/security/database_access.py`
- **Functions**
  - `resolve_database_actor(headers)` (line 39): no docstring
  - `normalize_database_role(value)` (line 50): no docstring
  - `async fetch_database_access_entries(db_names)` (line 60): no docstring
  - `async upsert_database_access_entry(db_name, principal_type, principal_id, principal_name, role)` (line 97): no docstring
  - `async upsert_database_owner(db_name, principal_type, principal_id, principal_name)` (line 136): no docstring
  - `resolve_database_actor_with_name(headers)` (line 152): no docstring
  - `async ensure_database_access_table(conn)` (line 158): no docstring
  - `async get_database_access_role(db_name, principal_type, principal_id)` (line 195): no docstring
  - `async has_database_access_config(db_name)` (line 225): no docstring
  - `async enforce_database_role(headers, db_name, required_roles, allow_if_unconfigured, require_env_key)` (line 240): no docstring

### `backend/shared/security/input_sanitizer.py`
- **Functions**
  - `sanitize_input(data)` (line 815): 전역 입력 정화 함수
  - `sanitize_label_input(data)` (line 827): Sanitize a label-keyed payload (BFF).
  - `validate_db_name(db_name)` (line 846): 데이터베이스 이름 검증 함수
  - `validate_class_id(class_id)` (line 851): 클래스 ID 검증 함수
  - `validate_branch_name(branch_name)` (line 856): 브랜치 이름 검증 함수
  - `validate_instance_id(instance_id)` (line 861): 인스턴스 ID 검증 함수
  - `sanitize_es_query(query)` (line 866): Elasticsearch 쿼리 문자열 정제
- **Classes**
  - `SecurityViolationError` (line 16): 보안 위반 시 발생하는 예외
  - `InputSanitizer` (line 22): 포괄적인 입력 데이터 보안 검증 및 정화 클래스
    - `__init__(self)` (line 129): no docstring
    - `detect_sql_injection(self, value)` (line 156): SQL Injection 패턴 탐지
    - `detect_xss(self, value)` (line 163): XSS 패턴 탐지
    - `detect_path_traversal(self, value)` (line 170): Path Traversal 패턴 탐지
    - `detect_command_injection(self, value, is_shell_context)` (line 177): Command Injection 패턴 탐지
    - `detect_nosql_injection(self, value)` (line 197): NoSQL Injection 패턴 탐지
    - `detect_ldap_injection(self, value)` (line 204): LDAP Injection 패턴 탐지
    - `sanitize_string(self, value, max_length)` (line 211): 문자열 정화 처리
    - `sanitize_field_name(self, value)` (line 259): 필드명 정화 (id, name 등 일반적인 필드명 허용)
    - `sanitize_label_key(self, value)` (line 289): Label-key sanitizer for "label-based" payloads (BFF).
    - `sanitize_label_dict(self, data, max_depth, current_depth)` (line 311): Sanitize a dict whose keys are *labels* (human-facing), not internal field names.
    - `sanitize_map_key(self, value, max_length)` (line 344): Sanitize keys for *free-form* key/value maps embedded in payloads.
    - `sanitize_identifier_mapping(self, data, max_depth, current_depth)` (line 373): Sanitize a mapping whose keys AND values are identifiers (e.g., column rename maps).
    - `sanitize_description(self, value)` (line 408): 설명 텍스트 정화 (command injection 체크 안함)
    - `sanitize_sql_expression(self, value, max_length)` (line 432): Sanitize a SQL *expression* (Spark SQL / ETL compute predicate / select expr).
    - `sanitize_shell_command(self, value)` (line 452): Shell 명령어 컨텍스트의 문자열 정화 (모든 보안 체크 적용)
    - `sanitize_pipeline_definition(self, value, max_depth, current_depth)` (line 484): Recursively sanitize a pipeline definition subtree.
    - `sanitize_dict(self, data, max_depth, current_depth)` (line 571): 딕셔너리 재귀적 정화 처리
    - `sanitize_list(self, data, max_depth, current_depth)` (line 672): 리스트 정화 처리
    - `sanitize_any(self, value, max_depth, current_depth)` (line 694): 모든 타입의 데이터 정화 처리
    - `validate_database_name(self, db_name)` (line 722): 데이터베이스 이름 검증 - 엄격한 규칙 적용
    - `validate_class_id(self, class_id)` (line 759): 클래스 ID 검증
    - `validate_branch_name(self, branch_name)` (line 773): 브랜치 이름 검증
    - `validate_instance_id(self, instance_id)` (line 792): 인스턴스 ID 검증

### `backend/shared/security/principal_utils.py`
- **Functions**
  - `resolve_principal_from_headers(headers, principal_id_headers, principal_type_headers, default_principal_type, default_principal_id, allowed_principal_types)` (line 23): no docstring
  - `actor_label(principal_type, principal_id)` (line 55): no docstring

### `backend/shared/security/user_context.py`
- **Functions**
  - `extract_bearer_token(value)` (line 40): no docstring
  - `_parse_algorithms(raw)` (line 49): no docstring
  - `_claim_str(claims, keys)` (line 57): no docstring
  - `_claim_list_str(claims, keys)` (line 68): no docstring
  - `async _fetch_jwks(url)` (line 85): no docstring
  - `_select_jwk(jwks, kid)` (line 102): no docstring
  - `_decode_user_claims(token, key, algorithms, issuer, audience)` (line 119): no docstring
  - `async verify_user_token(token, jwt_enabled, jwt_issuer, jwt_audience, jwt_jwks_url, jwt_public_key, jwt_hs256_secret, jwt_algorithms)` (line 141): Verify a user JWT and return a structured principal.
- **Classes**
  - `UserTokenError` (line 12): Raised when a user/delegated JWT cannot be verified.
  - `UserPrincipal` (line 17): Verified (or explicitly unverified) principal extracted from an auth token.

### `backend/shared/serializers/__init__.py`

### `backend/shared/serializers/complex_type_serializer.py`
- **Classes**
  - `ComplexTypeSerializer` (line 14): Complex type serializer for converting between internal and external representations
    - `serialize(value, data_type, constraints)` (line 18): Serialize a complex type value to string representation
    - `deserialize(serialized_value, data_type, metadata)` (line 64): Deserialize a string representation back to complex type value
    - `_serialize_array(value, constraints)` (line 105): Serialize array value
    - `_serialize_object(value, constraints)` (line 117): Serialize object value
    - `_serialize_enum(value, constraints)` (line 128): Serialize enum value
    - `_serialize_money(value, constraints)` (line 139): Serialize money value
    - `_serialize_phone(value, constraints)` (line 160): Serialize phone value
    - `_serialize_email(value, constraints)` (line 181): Serialize email value
    - `_serialize_coordinate(value, constraints)` (line 202): Serialize coordinate value
    - `_serialize_address(value, constraints)` (line 225): Serialize address value
    - `_serialize_image(value, constraints)` (line 245): Serialize image value
    - `_serialize_file(value, constraints)` (line 266): Serialize file value
    - `_deserialize_array(serialized_value, metadata)` (line 285): Deserialize array value
    - `_deserialize_object(serialized_value, metadata)` (line 293): Deserialize object value
    - `_deserialize_enum(serialized_value, metadata)` (line 301): Deserialize enum value
    - `_deserialize_money(serialized_value, metadata)` (line 319): Deserialize money value
    - `_deserialize_phone(serialized_value, metadata)` (line 340): Deserialize phone value
    - `_deserialize_email(serialized_value, metadata)` (line 351): Deserialize email value
    - `_deserialize_coordinate(serialized_value, metadata)` (line 362): Deserialize coordinate value
    - `_deserialize_address(serialized_value, metadata)` (line 383): Deserialize address value
    - `_deserialize_image(serialized_value, metadata)` (line 394): Deserialize image value
    - `_deserialize_file(serialized_value, metadata)` (line 405): Deserialize file value

### `backend/shared/services/__init__.py`

### `backend/shared/services/agent/__init__.py`

### `backend/shared/services/agent/agent_retention_worker.py`
- **Functions**
  - `async run_agent_session_retention_worker(session_registry, poll_interval_seconds, retention_days, stop_event, tenant_id, action, storage_service, delete_file_upload_objects, retention_policy_json)` (line 17): Background retention worker for agent session data (SEC-005).
  - `_parse_retention_policy(raw)` (line 94): no docstring
  - `_policy_days_action(policy, key, default_days, default_action)` (line 143): no docstring
  - `async _apply_policy(session_registry, storage_service, tenant_id, delete_uploads, default_days, default_action, now, policy)` (line 167): no docstring

### `backend/shared/services/agent/agent_tool_allowlist.py`
- **Functions**
  - `_derive_resource_scopes(path)` (line 10): no docstring
  - `_derive_tool_type(tool_id, method, risk_level)` (line 28): no docstring
  - `_default_allowlist_bundle_path()` (line 52): no docstring
  - `load_agent_tool_allowlist_bundle(bundle_path)` (line 56): no docstring
  - `async bootstrap_agent_tool_allowlist(tool_registry, bundle_path, only_if_empty)` (line 76): no docstring

### `backend/shared/services/agent/llm_gateway.py`
- **Functions**
  - `_load_pricing_table(raw)` (line 83): no docstring
  - `_estimate_cost(model, prompt_tokens, completion_tokens, pricing_json)` (line 117): no docstring
  - `_parse_provider_policies(raw)` (line 129): no docstring
  - `_coerce_provider_extra_headers(value)` (line 151): no docstring
  - `_coerce_provider_extra_body(value)` (line 166): no docstring
  - `_filter_provider_extra_body(provider, extra_body)` (line 178): no docstring
  - `_build_provider_send_overrides(provider, provider_policy, audit_partition_key, audit_actor)` (line 189): Build provider send overrides (SEC-002).
  - `_extract_json_object(text)` (line 241): Best-effort JSON object extraction.
  - `_tool_parameters_from_model(model)` (line 284): Build an OpenAI tool/function `parameters` schema from a Pydantic model.
  - `_openai_max_tokens_params(model, max_tokens)` (line 313): OpenAI compatibility: newer models (ex: gpt-5) require max_completion_tokens.
  - `_openai_temperature_params(model, temperature)` (line 323): OpenAI compatibility: gpt-5 only supports the default temperature (1).
  - `_openai_reasoning_params(model)` (line 333): OpenAI responses API: gpt-5 needs low reasoning effort to emit output tokens.
  - `_use_openai_responses_api(model)` (line 343): no docstring
  - `_extract_openai_responses_text(data)` (line 348): no docstring
  - `_mask_payload(value, max_chars)` (line 377): no docstring
  - `_log_llm_event(event, payload, max_chars)` (line 387): no docstring
  - `create_llm_gateway(settings)` (line 1453): no docstring
- **Classes**
  - `LLMUnavailableError` (line 45): no docstring
  - `LLMRequestError` (line 49): no docstring
  - `LLMOutputValidationError` (line 53): no docstring
  - `LLMPolicyError` (line 57): Raised when a model/tool/policy guard blocks an LLM call.
  - `LLMHTTPStatusError` (line 61): no docstring
    - `__init__(self, status_code, body_preview)` (line 62): no docstring
  - `LLMCallMeta` (line 69): no docstring
  - `LLMGateway` (line 395): A thin, safe wrapper around an LLM provider.
    - `__init__(self, settings)` (line 404): no docstring
    - `is_enabled(self)` (line 449): no docstring
    - `_circuit_key(self, provider, model)` (line 462): no docstring
    - `_is_circuit_open(self, circuit_key)` (line 465): no docstring
    - `_record_circuit_success(self, circuit_key)` (line 470): no docstring
    - `_record_circuit_failure(self, circuit_key)` (line 474): no docstring
    - `_retry_delay_s(self, prompt_hash, attempt)` (line 482): no docstring
    - `_should_retry(self, exc)` (line 492): no docstring
    - `async _call_openai_compat_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 499): no docstring
    - `async _call_openai_compat_tool_call(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, tool_name, tool_parameters, extra_headers, extra_body)` (line 603): no docstring
    - `async _call_openai_compat_responses_json(self, task, system_prompt, user_prompt, model, max_tokens, prompt_hash, tool_parameters, schema_name, extra_headers, extra_body)` (line 729): no docstring
    - `async _call_anthropic_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 906): no docstring
    - `async _call_google_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 961): no docstring
    - `async complete_json(self, task, system_prompt, user_prompt, response_model, model, allowed_models, use_native_tool_calling, redis_service, audit_store, audit_partition_key, audit_actor, audit_resource_id, audit_metadata, temperature, max_tokens)` (line 1020): no docstring

### `backend/shared/services/agent/llm_quota.py`
- **Functions**
  - `_sanitize_key_part(value)` (line 27): no docstring
  - `_extract_quota_spec(data_policies, model_id)` (line 34): no docstring
  - `async enforce_llm_quota(redis_service, tenant_id, user_id, model_id, system_prompt, user_prompt, data_policies, now)` (line 75): Best-effort quota enforcement (NFR-004) using Redis counters.
- **Classes**
  - `LLMQuotaSpec` (line 13): no docstring
  - `LLMQuotaExceededError` (line 19): no docstring
    - `__init__(self, message, spec, used_calls, used_tokens)` (line 20): no docstring

### `backend/shared/services/core/__init__.py`

### `backend/shared/services/core/async_terminus.py`
- **Classes**
  - `AsyncTerminusService` (line 13): Lightweight TerminusDB service for BFF
    - `__init__(self, connection_info)` (line 16): no docstring
    - `async connect(self)` (line 20): Establish connection to TerminusDB
    - `async ping(self)` (line 29): Check if TerminusDB is accessible
    - `async close(self)` (line 40): Close the connection

### `backend/shared/services/core/audit_log_store.py`
- **Functions**
  - `create_audit_log_store(settings)` (line 401): no docstring
- **Classes**
  - `AuditLogStore` (line 24): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 25): no docstring
    - `async _ensure_tables(self, conn)` (line 44): no docstring
    - `_canonical_json(value)` (line 99): no docstring
    - `_compute_hash(cls, prev_hash, payload)` (line 103): no docstring
    - `async append(self, entry, partition_key)` (line 108): no docstring
    - `async log(self, partition_key, actor, action, status, resource_type, resource_id, event_id, command_id, trace_id, correlation_id, metadata, error, occurred_at)` (line 211): no docstring
    - `async list_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset)` (line 244): no docstring
    - `async count_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until)` (line 327): no docstring
    - `async get_chain_head(self, partition_key)` (line 376): no docstring

### `backend/shared/services/core/background_task_manager.py`
- **Functions**
  - `create_background_task_manager(redis_service, websocket_service)` (line 664): Create a BackgroundTaskManager instance.
- **Classes**
  - `TaskPriority` (line 41): Task execution priority levels.
  - `BackgroundTaskManager` (line 48): Centralized background task management service.
    - `__init__(self, redis_service, websocket_service)` (line 56): Initialize the background task manager.
    - `async start(self)` (line 83): Start the background task manager.
    - `async stop(self)` (line 90): Stop the background task manager and cancel all tasks.
    - `async create_task(self, func, *args, task_id, task_name, task_type, priority, metadata, **kwargs)` (line 108): Create and track a new background task.
    - `async run_with_tracking(self, task_id, func, args, kwargs)` (line 200): Run a function with full tracking (for use with FastAPI BackgroundTasks).
    - `async _execute_task(self, task_id, func, args, kwargs)` (line 222): Execute a task with full error handling and status tracking.
    - `async update_progress(self, task_id, current, total, message, metadata)` (line 320): Update task progress for long-running operations.
    - `async get_task_status(self, task_id)` (line 369): Get current task status and details.
    - `async get_all_tasks(self, status, task_type, limit)` (line 373): Get all tasks matching the criteria.
    - `async cancel_task(self, task_id)` (line 413): Cancel a running task.
    - `async add_task_callback(self, task_id, callback)` (line 447): Add a callback to be called when task completes.
    - `async get_task_metrics(self)` (line 465): Get overall task execution metrics.
    - `async _save_task(self, task)` (line 485): Save task to Redis.
    - `async _get_task(self, task_id)` (line 499): Get task from Redis.
    - `async _handle_task_done(self, task_id, asyncio_task)` (line 507): Handle task completion callback.
    - `_handle_cleanup_task_done(self, task)` (line 525): Handle cleanup task completion.
    - `async _cleanup_dead_tasks(self)` (line 539): Periodically clean up dead tasks.
    - `_calculate_average_duration(self, tasks)` (line 571): Calculate average task duration in seconds.
    - `_calculate_success_rate(self, tasks)` (line 581): Calculate task success rate as percentage.
    - `async _notify_task_created(self, task)` (line 595): Notify about task creation.
    - `async _notify_task_status_changed(self, task)` (line 606): Notify about task status change.
    - `async _notify_task_progress(self, task)` (line 615): Notify about task progress update.
    - `async _notify_task_completed(self, task)` (line 624): Notify about task completion.
    - `async _notify_task_failed(self, task)` (line 633): Notify about task failure.
    - `async _notify_task_cancelled(self, task)` (line 643): Notify about task cancellation.
    - `async _notify_task_retrying(self, task)` (line 651): Notify about task retry.

### `backend/shared/services/core/command_status_service.py`
- **Classes**
  - `CommandStatus` (line 19): Command execution status enumeration.
  - `CommandStatusService` (line 29): Service for tracking command execution status.
    - `__init__(self, redis_service)` (line 41): no docstring
    - `async create_command_status(self, command_id, command_type, aggregate_id, payload, user_id)` (line 45): Create initial command status entry.
    - `async update_status(self, command_id, status, message, error, progress)` (line 87): Update command status.
    - `async start_processing(self, command_id, worker_id)` (line 166): Mark command as being processed.
    - `async complete_command(self, command_id, result, message)` (line 191): Mark command as completed with result.
    - `async fail_command(self, command_id, error, retry_count)` (line 219): Mark command as failed.
    - `async cancel_command(self, command_id, reason)` (line 247): Cancel a pending or processing command.
    - `async get_command_details(self, command_id)` (line 282): Get complete command details including status and result.
    - `async list_user_commands(self, user_id, status_filter, limit)` (line 315): List commands for a specific user.
    - `async cleanup_old_commands(self, days)` (line 362): Clean up commands older than specified days.
    - `async set_command_status(self, command_id, status, metadata)` (line 378): Legacy compatibility method for setting command status.
    - `async get_command_status(self, command_id)` (line 421): Legacy compatibility method for getting command status.
    - `async get_command_result(self, command_id)` (line 448): Legacy compatibility method for getting command result.

### `backend/shared/services/core/consistency_checker.py`
- **Functions**
  - `async run_consistency_check(db_name, class_id)` (line 412): Run complete consistency check
- **Classes**
  - `ConsistencyChecker` (line 28): Real-time consistency verification for Event Sourcing + CQRS
    - `__init__(self, es_url, tdb_url, redis_url, s3_endpoint, s3_access_key, s3_secret_key)` (line 34): no docstring
    - `async check_all_invariants(self, db_name, class_id)` (line 62): Check all 6 production invariants
    - `async check_es_tdb_function(self, db_name, class_id)` (line 93): Invariant 1: ES projection = TDB partial function
    - `async check_idempotency(self, db_name, class_id)` (line 144): Invariant 2: Idempotency (exactly-once processing)
    - `async check_ordering(self, db_name, class_id)` (line 183): Invariant 3: Ordering guarantee (per-aggregate)
    - `async check_schema_composition(self, db_name, class_id)` (line 245): Invariant 4: Schema-projection composition preservation
    - `async check_replay_determinism(self, db_name, class_id)` (line 301): Invariant 5: Replay determinism
    - `async check_read_your_writes(self, db_name, class_id)` (line 368): Invariant 6: Read-your-writes guarantee

### `backend/shared/services/core/consistency_token.py`
- **Functions**
  - `async demo_consistency_token()` (line 357): Demo the consistency token functionality
- **Classes**
  - `ConsistencyToken` (line 21): Token containing sufficient information to ensure read-your-writes consistency
    - `to_string(self)` (line 32): Encode token as a compact string
    - `from_string(cls, token_str)` (line 51): Decode token from string
  - `ConsistencyTokenService` (line 83): Service for managing consistency tokens
    - `__init__(self, redis_url)` (line 89): no docstring
    - `async connect(self)` (line 93): Initialize Redis connection
    - `async disconnect(self)` (line 97): Close Redis connection
    - `async create_token(self, command_id, aggregate_id, sequence_number, version)` (line 102): Create a new consistency token after a write operation
    - `async _estimate_projection_lag(self)` (line 138): Estimate current projection lag in milliseconds
    - `async _store_token_metadata(self, token)` (line 155): Store token metadata for validation
    - `async wait_for_consistency(self, token, es_client, max_wait_ms)` (line 180): Wait until the write represented by the token is visible
    - `async _check_write_visible(self, token, es_client)` (line 223): Check if a write is visible in Elasticsearch
    - `async validate_token(self, token_str)` (line 253): Validate a consistency token
    - `async get_read_timestamp(self, token)` (line 282): Get the minimum timestamp that guarantees consistency
    - `async update_projection_lag(self, actual_lag_ms)` (line 295): Update the estimated projection lag based on actual measurements
  - `CommandResponseWithToken` (line 320): Enhanced command response that includes consistency token
    - `__init__(self, command_id, status, result, consistency_token)` (line 325): no docstring
    - `to_dict(self)` (line 337): Convert to dictionary for API response

### `backend/shared/services/core/graph_federation_service_es.py`
- **Classes**
  - `GraphFederationServiceES` (line 42): ES-native graph traversal — Search Arounds without TerminusDB.
    - `__init__(self, es_service, oms_base_url)` (line 45): no docstring
    - `async _ensure_connected(self)` (line 54): Lazily connect the ES client on first use.
    - `async multi_hop_query(self, db_name, start_class, hops, base_branch, overlay_branch, terminus_branch, strict_overlay, filters, limit, offset, max_nodes, max_edges, include_paths, max_paths, no_cycles, include_documents, include_audit)` (line 65): Execute multi-hop graph query entirely within Elasticsearch.
    - `async simple_graph_query(self, db_name, class_name, base_branch, overlay_branch, terminus_branch, strict_overlay, filters, include_documents, include_audit)` (line 220): Single-class ES search — no hops.
    - `async find_relationship_paths(self, db_name, source_class, target_class, branch, max_depth)` (line 268): Discover relationship paths between two classes by sampling ES docs.
    - `async _search_start_class(self, index_name, class_id, filters, limit, offset)` (line 319): Hop 0 — search for start class instances with optional filters.
    - `async _hop_forward(self, index_name, source_docs, predicate, target_class, max_fan_out)` (line 364): Forward hop: extract relationships.{predicate} → fetch targets via mget.
    - `async _hop_reverse(self, index_name, source_docs, predicate, owner_class, source_class, max_fan_out)` (line 408): Reverse hop: find owner_class docs whose relationships.{predicate}
    - `async _mget_instances(self, index_name, class_id, instance_ids)` (line 477): Batch-fetch instance documents by ID (instance_id == ES _id).
    - `async _discover_class_adjacency(self, index_name)` (line 503): Sample ES docs to discover class → [(predicate, target_class)] adjacency.
    - `_normalize_hops(hops)` (line 555): Normalize hop specs into (predicate, target_class, reverse) tuples.
    - `_node_id(doc)` (line 579): Build canonical node ID: 'Class/instance_id'.
    - `_make_node(doc)` (line 586): Build a graph node dict from an ES document.
    - `_class_of(docs)` (line 600): Return the class_id of the first doc (all should be same class).
    - `_get_relationship_refs(doc, predicate)` (line 607): Extract relationship references for a given predicate from a doc.
    - `_parse_ref(ref)` (line 620): Parse 'TargetClass/instance_id' → (target_class, instance_id).
    - `_empty_result()` (line 628): no docstring

### `backend/shared/services/core/health_check.py`
- **Classes**
  - `HealthStatus` (line 27): Health status enumeration
  - `ServiceType` (line 35): Service type classification
  - `HealthCheckResult` (line 43): Result of a health check operation
    - `__post_init__(self)` (line 54): no docstring
    - `to_dict(self)` (line 60): Convert to dictionary for serialization
  - `HealthCheckInterface` (line 74): Abstract interface for health checks
    - `service_name(self)` (line 79): Return the service name
    - `service_type(self)` (line 85): Return the service type classification
    - `async health_check(self)` (line 90): Perform health check and return result
    - `async health_check_with_timeout(self, timeout_seconds)` (line 94): Perform health check with timeout
  - `DatabaseHealthCheck` (line 117): Health check for database connections
    - `__init__(self, db_connection, service_name)` (line 120): no docstring
    - `service_name(self)` (line 125): no docstring
    - `service_type(self)` (line 129): no docstring
    - `async health_check(self)` (line 132): Check database connectivity and performance
  - `RedisHealthCheck` (line 174): Health check for Redis connections
    - `__init__(self, redis_service, service_name)` (line 177): no docstring
    - `service_name(self)` (line 182): no docstring
    - `service_type(self)` (line 186): no docstring
    - `async health_check(self)` (line 189): Check Redis connectivity and performance
  - `ElasticsearchHealthCheck` (line 240): Health check for Elasticsearch connections
    - `__init__(self, elasticsearch_service, service_name)` (line 243): no docstring
    - `service_name(self)` (line 248): no docstring
    - `service_type(self)` (line 252): no docstring
    - `async health_check(self)` (line 255): Check Elasticsearch connectivity and cluster health
  - `StorageHealthCheck` (line 309): Health check for storage services (S3, etc.)
    - `__init__(self, storage_service, service_name)` (line 312): no docstring
    - `service_name(self)` (line 317): no docstring
    - `service_type(self)` (line 321): no docstring
    - `async health_check(self)` (line 324): Check storage service connectivity
  - `TerminusDBHealthCheck` (line 375): Health check for TerminusDB connections
    - `__init__(self, terminus_service, service_name)` (line 378): no docstring
    - `service_name(self)` (line 383): no docstring
    - `service_type(self)` (line 387): no docstring
    - `async health_check(self)` (line 390): Check TerminusDB connectivity and basic operations
  - `AggregatedHealthStatus` (line 453): Aggregated health status for the entire system
    - `to_dict(self)` (line 460): Convert to dictionary for serialization
  - `HealthCheckAggregator` (line 470): Aggregates health checks from multiple services
    - `__init__(self)` (line 473): no docstring
    - `register_health_checker(self, health_checker)` (line 476): Register a health checker
    - `async check_all_services(self, timeout_seconds)` (line 480): Check all registered services and aggregate results
    - `_determine_overall_status(self, results)` (line 523): Determine overall system health based on individual service results
    - `_generate_summary(self, results)` (line 545): Generate summary statistics

### `backend/shared/services/core/instance_index_rebuild_service.py`
- **Functions**
  - `async rebuild_instance_index(request, elasticsearch_service, task_id)` (line 61): Rebuild the instances index via ES reindex + Blue-Green alias swap.
  - `async _resolve_alias_targets(es, alias_name)` (line 189): Return concrete index names behind *alias_name*, or empty list.
  - `async _reindex_from_source(es, source_index, dest_index)` (line 205): Run ES ``_reindex`` API and return the number of documents reindexed.
  - `async _get_class_counts(es, index_name)` (line 227): Return ``{class_id: count}`` from a terms aggregation.
- **Classes**
  - `RebuildIndexRequest` (line 33): Parameters for an index rebuild operation.
  - `RebuildClassResult` (line 42): no docstring
  - `RebuildIndexResult` (line 49): no docstring

### `backend/shared/services/core/object_type_meta_resolver.py`
- **Functions**
  - `build_object_type_meta_resolver(resources, db_name, branch)` (line 15): no docstring
- **Classes**
  - `ObjectTypeMeta` (line 10): no docstring

### `backend/shared/services/core/ontology_linter.py`
- **Functions**
  - `_is_snake_case(value)` (line 58): no docstring
  - `_tokenize(value)` (line 62): no docstring
  - `_event_like_triggers(value)` (line 69): Conservative "event/state/log-like" hint detector.
  - `_issue(severity, rule_id, message, path, suggestion, rationale, metadata)` (line 115): no docstring
  - `compute_risk_score(errors, warnings, infos)` (line 136): no docstring
  - `risk_level(score)` (line 148): no docstring
  - `lint_ontology_create(class_id, label, abstract, properties, relationships, config)` (line 158): Lint a create payload (no IO).
  - `lint_ontology_update(existing_properties, existing_relationships, updated_properties, updated_relationships, config)` (line 422): Lint an update as a diff (no IO).
- **Classes**
  - `OntologyLinterConfig` (line 26): Controls strictness (domain-neutral).
    - `from_env(cls, branch)` (line 37): no docstring

### `backend/shared/services/core/projection_position_tracker.py`
- **Classes**
  - `ProjectionPositionTracker` (line 19): CRUD operations for projection_positions table.
    - `__init__(self, pool)` (line 22): pool: asyncpg connection pool.
    - `async get_position(self, projection_name, db_name, branch)` (line 26): Get the current position for a projection.
    - `async update_position(self, projection_name, db_name, branch, last_sequence, last_event_id, last_job_id)` (line 56): Update (upsert) the position for a projection.
    - `async reset_position(self, projection_name, db_name, branch)` (line 96): Reset position to zero (for full rebuild).
    - `async list_positions(self, db_name, limit)` (line 124): List all projection positions, optionally filtered by db_name.
    - `async compute_lag(self, projection_name, db_name, branch, current_sequence)` (line 158): Compute the lag between a projection position and current head.

### `backend/shared/services/core/relationship_extractor.py`
- **Functions**
  - `_normalize_ref(ref)` (line 19): Normalize a relationship reference to TargetClass/instance_id format.
  - `_expects_many(cardinality)` (line 38): Determine if a relationship cardinality expects multiple values.
  - `_extract_from_relationship_list(rel_list, payload, relationships, known_fields)` (line 52): Extract relationships from a list of relationship definitions (OntologyResponse or OMS dict).
  - `_extract_from_terminus_schema(schema, payload, relationships, known_fields)` (line 96): Extract relationships from TerminusDB-style schema (properties with @class).
  - `_pattern_fallback(payload, relationships)` (line 130): Fallback: detect relationships by field name patterns.
  - `extract_relationships(payload, ontology_data, rel_map, allow_pattern_fallback)` (line 153): Extract relationship fields from an instance payload.

### `backend/shared/services/core/schema_change_monitor.py`
- **Classes**
  - `MonitorConfig` (line 33): Configuration for schema change monitoring
    - `from_dict(cls, config_dict)` (line 51): no docstring
  - `SchemaChangeMonitor` (line 61): Monitors schema changes for active mapping specs and broadcasts notifications.
    - `__init__(self, drift_detector, dataset_registry, objectify_registry, websocket_service, config)` (line 72): Args:
    - `add_drift_callback(self, callback)` (line 104): Add a callback to be invoked when drift is detected
    - `async start(self)` (line 111): Start the schema change monitor.
    - `async stop(self)` (line 131): Stop the schema change monitor
    - `_on_task_done(self, task)` (line 144): Handle task completion
    - `async _monitoring_loop(self)` (line 153): Main monitoring loop
    - `async _check_all_active_mappings(self)` (line 167): Check all active mapping specs for schema drift
    - `async _check_mapping_spec(self, spec)` (line 192): Check a single mapping spec for schema drift
    - `_should_notify(self, drift, subject_id)` (line 269): Determine if notification should be sent
    - `async _notify_drift(self, drift, impacted_mappings)` (line 288): Send drift notification
    - `async check_mapping_spec_compatibility(self, mapping_spec_id)` (line 315): On-demand compatibility check for a specific mapping spec.
    - `get_status(self)` (line 415): Get monitor status

### `backend/shared/services/core/schema_drift_detector.py`
- **Classes**
  - `SchemaChange` (line 22): Individual schema change detail
  - `SchemaDrift` (line 32): Detected schema drift between two versions
    - `is_breaking(self)` (line 45): no docstring
    - `change_summary(self)` (line 49): Count of changes by type
  - `ImpactedMapping` (line 58): Mapping spec impacted by schema drift
  - `SchemaDriftConfig` (line 68): Configuration for schema drift detection
    - `from_dict(cls, config_dict)` (line 89): no docstring
  - `SchemaDriftDetector` (line 97): Detects schema drift and analyzes impact on dependent resources.
    - `__init__(self, config)` (line 108): no docstring
    - `detect_drift(self, subject_type, subject_id, db_name, current_schema, previous_schema, previous_hash)` (line 114): Detect schema drift between previous and current schema.
    - `_detect_changes(self, old_schema, new_schema)` (line 180): Detect individual schema changes
    - `_detect_renames(self, removed_cols, added_cols)` (line 263): Detect potential column renames
    - `_is_critical_column(self, column_name)` (line 304): Check if column is critical (e.g., primary key)
    - `_assess_type_change_impact(self, old_type, new_type)` (line 313): Assess impact of type change
    - `_classify_drift_type(self, changes)` (line 324): Classify the primary drift type
    - `_classify_severity(self, changes)` (line 335): Classify overall severity of drift
    - `async analyze_impact(self, drift, mapping_specs)` (line 348): Analyze impact of schema drift on mapping specs.
    - `to_notification_payload(self, drift, impacted_mappings)` (line 431): Convert drift to notification payload for WebSocket broadcast

### `backend/shared/services/core/schema_versioning.py`
- **Classes**
  - `SchemaVersion` (line 16): Represents a schema version with comparison capabilities.
    - `__init__(self, version_string)` (line 22): Initialize schema version from string.
    - `__str__(self)` (line 38): no docstring
    - `__eq__(self, other)` (line 41): no docstring
    - `__lt__(self, other)` (line 48): no docstring
    - `__le__(self, other)` (line 57): no docstring
    - `is_backward_compatible(self, other)` (line 60): Check if this version is backward compatible with another.
  - `MigrationStrategy` (line 79): Migration strategies for schema changes
  - `SchemaMigration` (line 89): Represents a migration from one schema version to another.
    - `__init__(self, from_version, to_version, entity_type, migration_func, description)` (line 94): Initialize schema migration.
    - `apply(self, data)` (line 118): Apply migration to data.
  - `SchemaRegistry` (line 137): Central registry for schema versions and migrations.
    - `__init__(self)` (line 142): Initialize schema registry
    - `register_schema(self, entity_type, version, schema)` (line 148): Register a schema version.
    - `register_migration(self, migration)` (line 175): Register a migration between versions.
    - `get_migration_path(self, entity_type, from_version, to_version)` (line 193): Find migration path between versions.
    - `migrate_data(self, data, entity_type, target_version)` (line 250): Migrate data to target version.
  - `SchemaVersioningService` (line 291): High-level service for schema versioning operations.
    - `__init__(self, registry)` (line 296): Initialize schema versioning service.
    - `_initialize_default_schemas(self)` (line 306): Initialize default schemas and migrations
    - `version_event(self, event)` (line 376): Add or update schema version for an event.
    - `migrate_event(self, event, target_version)` (line 397): Migrate event to target version.
    - `is_compatible(self, data, entity_type, required_version)` (line 414): Check if data version is compatible with required version.

### `backend/shared/services/core/sequence_service.py`
- **Classes**
  - `SequenceService` (line 16): Provides sequence number generation for aggregates.
    - `__init__(self, redis_client, namespace)` (line 24): Initialize sequence service.
    - `_get_key(self, aggregate_id)` (line 40): Get Redis key for aggregate sequence.
    - `async get_next_sequence(self, aggregate_id)` (line 52): Get next sequence number for aggregate.
    - `async get_current_sequence(self, aggregate_id)` (line 73): Get current sequence number without incrementing.
    - `async set_sequence(self, aggregate_id, sequence)` (line 98): Set sequence number for aggregate (used for recovery/replay).
    - `async reset_aggregate(self, aggregate_id)` (line 132): Reset sequence for an aggregate (dangerous - use carefully).
    - `async get_batch_sequences(self, aggregate_id, count)` (line 152): Reserve a batch of sequence numbers for bulk operations.
    - `async get_all_sequences(self, pattern)` (line 183): Get all current sequences (for monitoring/debugging).
    - `async cleanup_old_sequences(self, ttl_seconds)` (line 205): Set TTL on sequence keys for inactive aggregates.
  - `SequenceValidator` (line 230): Validates event sequences for consistency.
    - `__init__(self, sequence_service)` (line 235): Initialize sequence validator.
    - `async validate_sequence(self, aggregate_id, sequence, allow_gaps)` (line 245): Validate sequence number for aggregate.
    - `reset_expectations(self)` (line 281): Reset local sequence expectations

### `backend/shared/services/core/service_factory.py`
- **Functions**
  - `create_fastapi_service(service_info, custom_lifespan, include_health_check, include_logging_middleware, custom_tags, include_error_handlers, validation_error_status)` (line 60): Create a standardized FastAPI application with common configurations.
  - `_install_openapi_language_contract(app)` (line 154): Add `?lang=en|ko` and `Accept-Language` to OpenAPI so clients discover i18n support.
  - `_configure_cors(app)` (line 204): Configure CORS middleware based on environment variables
  - `_add_logging_middleware(app)` (line 219): Add request logging middleware
  - `_add_health_check(app, service_info)` (line 263): Add standardized health check endpoints
  - `_add_debug_endpoints(app)` (line 287): Add debug endpoints for development environment
  - `_install_observability(app, service_info)` (line 296): Install tracing + metrics in a way that is:
  - `create_uvicorn_config(service_info, reload)` (line 339): Create standardized uvicorn configuration.
  - `_get_logging_config(service_name)` (line 372): Get standardized logging configuration for uvicorn
  - `run_service(app, service_info, app_module_path, reload)` (line 417): Run the service with standardized uvicorn configuration.
  - `get_bff_service_info()` (line 436): no docstring
  - `get_oms_service_info()` (line 468): no docstring
  - `get_funnel_service_info()` (line 486): no docstring
  - `get_agent_service_info()` (line 502): no docstring
- **Classes**
  - `ServiceInfo` (line 38): Service configuration container
    - `__init__(self, name, title, description, version, port, host, tags)` (line 41): no docstring

### `backend/shared/services/core/sheet_grid_parser.py`
- **Classes**
  - `SheetGridParseOptions` (line 29): Options shared across parsers.
  - `SheetGridParser` (line 45): Parsers for Excel and Google Sheets into SheetGrid.
    - `from_google_sheets_values(cls, values, merged_cells, sheet_name, options, metadata)` (line 53): Build SheetGrid from Google Sheets "values" matrix (A1-anchored).
    - `merged_cells_from_google_metadata(cls, sheets_metadata, worksheet_name, sheet_id)` (line 92): Extract merged ranges from Google Sheets "spreadsheets.get" metadata JSON.
    - `from_excel_bytes(cls, xlsx_bytes, sheet_name, options, metadata)` (line 134): Parse an .xlsx file into SheetGrid.
    - `_normalize_grid(cls, grid, max_rows, max_cols)` (line 340): no docstring
    - `_json_safe_cell(value)` (line 360): no docstring
    - `_trim_trailing_empty(cls, grid, min_rows, min_cols)` (line 375): no docstring
    - `_clip_merge_ranges(cls, merges, rows, cols)` (line 409): no docstring
    - `_excel_cell_to_display_value(cls, cell, fallback_to_formula)` (line 431): Best-effort conversion of an openpyxl cell into a display-like value.
    - `_format_excel_number(cls, value, fmt)` (line 468): no docstring
    - `_detect_currency_affixes(cls, fmt)` (line 506): no docstring
    - `_infer_decimal_places_from_format(fmt)` (line 532): no docstring

### `backend/shared/services/core/sheet_import_service.py`
- **Classes**
  - `FieldMapping` (line 30): no docstring
  - `SheetImportService` (line 35): Pure helpers; no network/IO.
    - `build_column_index(columns)` (line 39): no docstring
    - `_is_blank(value)` (line 52): no docstring
    - `_strip_numeric_affixes(raw)` (line 56): Remove common affixes around numeric strings (currency symbols/units/codes, percent).
    - `coerce_value(cls, value, target_type)` (line 114): Coerce a cell value into a JSON-serializable value compatible with target_type.
    - `build_instances(cls, columns, rows, mappings, target_field_types, max_rows)` (line 240): Apply mappings and type coercion to build target instances.

### `backend/shared/services/core/sync_wrapper_service.py`
- **Classes**
  - `SyncWrapperService` (line 20): 비동기 Command API를 동기적으로 래핑하는 서비스.
    - `__init__(self, command_status_service)` (line 27): no docstring
    - `async wait_for_command(self, command_id, options)` (line 30): Command가 완료될 때까지 기다리고 결과를 반환합니다.
    - `async _poll_until_complete(self, command_id, options, progress_history, start_time)` (line 112): Command가 완료될 때까지 주기적으로 상태를 확인합니다.
    - `async execute_sync(self, async_func, request_data, options)` (line 176): 비동기 함수를 실행하고 결과를 기다립니다.
    - `async get_command_progress(self, command_id)` (line 216): Command의 현재 진행 상태를 조회합니다.

### `backend/shared/services/core/watermark_monitor.py`
- **Functions**
  - `async create_watermark_monitor(kafka_config, redis_url, consumer_groups, topics)` (line 430): Create and start a watermark monitor
- **Classes**
  - `PartitionWatermark` (line 26): Watermark information for a single partition
    - `progress_percentage(self)` (line 37): Calculate progress as percentage
  - `GlobalWatermark` (line 47): Aggregated watermark across all partitions
    - `is_healthy(self)` (line 60): Check if lag is within acceptable limits
    - `estimated_catch_up_time_ms(self)` (line 66): Estimate time to catch up based on processing rate
  - `WatermarkMonitor` (line 76): Monitor Kafka consumer lag and watermarks across all partitions
    - `__init__(self, kafka_config, redis_client, consumer_groups, topics, alert_threshold_ms)` (line 88): Initialize watermark monitor
    - `async start_monitoring(self, interval_seconds)` (line 126): Start monitoring watermarks
    - `async stop_monitoring(self)` (line 143): Stop monitoring watermarks
    - `async _monitor_loop(self, interval_seconds)` (line 160): Main monitoring loop
    - `async update_watermarks(self, consumer_group)` (line 187): Update watermarks for a consumer group
    - `calculate_global_watermark(self)` (line 245): Calculate global watermark across all partitions
    - `async store_metrics(self)` (line 283): Store metrics in Redis for historical tracking
    - `async check_alerts(self)` (line 327): Check for lag alerts and trigger notifications
    - `async export_prometheus_metrics(self)` (line 353): Export metrics in Prometheus format
    - `async get_current_lag(self)` (line 379): Get current lag information
    - `async get_partition_details(self, topic)` (line 403): Get detailed lag information for a specific topic

### `backend/shared/services/core/websocket_service.py`
- **Functions**
  - `utc_now()` (line 23): no docstring
  - `get_connection_manager()` (line 625): WebSocket 연결 관리자 싱글톤 인스턴스 반환
  - `get_notification_service(redis_service)` (line 633): WebSocket 알림 서비스 싱글톤 인스턴스 반환
- **Classes**
  - `WebSocketConnection` (line 28): WebSocket 연결 정보
  - `WebSocketConnectionManager` (line 39): WebSocket 연결 관리자
    - `__init__(self)` (line 48): no docstring
    - `async connect(self, websocket, client_id, user_id)` (line 59): 새로운 WebSocket 연결 수락
    - `async disconnect(self, client_id)` (line 84): WebSocket 연결 해제
    - `async subscribe_command(self, client_id, command_id)` (line 110): 특정 Command에 대한 업데이트 구독
    - `async unsubscribe_command(self, client_id, command_id)` (line 125): Command 구독 해제
    - `async send_to_client(self, client_id, message)` (line 141): 특정 클라이언트에게 메시지 전송
    - `async broadcast_command_update(self, command_id, update_data)` (line 159): Command 업데이트를 구독 중인 클라이언트들에게 브로드캐스트
    - `async send_to_user(self, user_id, message)` (line 192): 특정 사용자의 모든 연결에 메시지 전송
    - `async broadcast_to_all(self, message)` (line 209): Broadcast a message to all connected clients.
    - `async ping_all_clients(self)` (line 229): 모든 클라이언트에 ping 전송 (연결 상태 확인)
    - `get_connection_stats(self)` (line 252): 연결 통계 반환
    - `_build_schema_subject_key(self, db_name, subject_type, subject_id)` (line 269): Build schema subscription key.
    - `async subscribe_schema_changes(self, client_id, db_name, subject_type, subject_id, severity_filter)` (line 280): Subscribe to schema change notifications.
    - `async unsubscribe_schema_changes(self, client_id, subject_key)` (line 316): Unsubscribe from schema change notifications.
    - `async broadcast_schema_drift(self, db_name, drift_payload)` (line 336): Broadcast schema drift notification to subscribed clients.
    - `async get_schema_subscription_info(self, client_id)` (line 411): Get schema subscription info for a client.
  - `WebSocketNotificationService` (line 423): WebSocket 알림 서비스 with proper task tracking
    - `__init__(self, redis_service, connection_manager, task_manager)` (line 431): no docstring
    - `async start(self)` (line 444): 알림 서비스 시작 with proper task tracking
    - `async stop(self)` (line 466): 알림 서비스 중지 with proper cleanup
    - `_handle_pubsub_task_done(self, task)` (line 485): Handle completion of pubsub task.
    - `async _restart_pubsub_listener(self)` (line 498): Restart the pubsub listener after a failure.
    - `async _listen_redis_updates(self)` (line 505): Redis Pub/Sub 채널을 수신하여 WebSocket으로 전달 with improved error handling
    - `async notify_task_update(self, update_data)` (line 566): Send task update notification to all connected clients.
    - `async publish_schema_drift(self, db_name, drift_payload)` (line 578): Publish schema drift notification via Redis Pub/Sub.
    - `async notify_schema_drift_direct(self, db_name, drift_payload)` (line 600): Send schema drift notification directly to WebSocket clients.

### `backend/shared/services/core/worker_stores.py`
- **Functions**
  - `async initialize_worker_stores(enable_lineage, enable_audit_logs, logger)` (line 82): no docstring
- **Classes**
  - `WorkerStores` (line 33): no docstring
  - `WorkerObservability` (line 41): Facade over optional provenance stores used by many workers.
    - `async record_link(self, **kwargs)` (line 55): no docstring
    - `async audit_log(self, **kwargs)` (line 73): no docstring

### `backend/shared/services/core/writeback_merge_service.py`
- **Functions**
  - `_parse_queue_entry_seq(key)` (line 21): no docstring
  - `_coerce_object_type(resource_rid, fallback)` (line 33): no docstring
- **Classes**
  - `WritebackMergedInstance` (line 43): no docstring
  - `WritebackMergeService` (line 53): Authoritative server-side merge path for Action writeback.
    - `__init__(self, base_storage, lakefs_storage)` (line 60): no docstring
    - `async merge_instance(self, db_name, base_branch, overlay_branch, class_id, instance_id, writeback_repo, writeback_branch)` (line 69): no docstring

### `backend/shared/services/events/__init__.py`

### `backend/shared/services/events/aggregate_sequence_allocator.py`
- **Classes**
  - `OptimisticConcurrencyError` (line 22): Raised when the aggregate's current sequence doesn't match the caller's expectation.
    - `__init__(self, handler, aggregate_id, expected_last_sequence, actual_last_sequence)` (line 25): no docstring
  - `AggregateSequenceAllocator` (line 43): Atomic per-aggregate sequence allocator.
    - `__init__(self, dsn, schema, handler_prefix)` (line 56): no docstring
    - `async connect(self)` (line 73): no docstring
    - `async close(self)` (line 86): no docstring
    - `async ensure_schema(self)` (line 91): no docstring
    - `handler_for(self, aggregate_type)` (line 109): no docstring
    - `async try_reserve_next_sequence(self, handler, aggregate_id)` (line 114): Fast path: reserve the next seq if the allocator row already exists.
    - `async reserve_next_sequence(self, handler, aggregate_id, seed_last_sequence)` (line 145): Reserve the next seq, initializing/catching-up using `seed_last_sequence`.
    - `async try_reserve_next_sequence_if_expected(self, handler, aggregate_id, expected_last_sequence)` (line 183): OCC fast path: reserve the next seq only if current last_sequence matches `expected_last_sequence`.
    - `async reserve_next_sequence_if_expected(self, handler, aggregate_id, seed_last_sequence, expected_last_sequence)` (line 247): OCC reserve with seeding: ensure allocator is at least `seed_last_sequence`, then reserve next seq

### `backend/shared/services/events/dataset_ingest_outbox.py`
- **Functions**
  - `async flush_dataset_ingest_outbox(dataset_registry, lineage_store, batch_size)` (line 255): no docstring
  - `async run_dataset_ingest_outbox_worker(dataset_registry, lineage_store, poll_interval_seconds, batch_size, stop_event)` (line 272): no docstring
  - `build_dataset_event_payload(event_id, event_type, aggregate_type, aggregate_id, command_type, actor, data)` (line 294): no docstring
- **Classes**
  - `DatasetIngestOutboxPublisher` (line 35): no docstring
    - `__init__(self, dataset_registry, lineage_store, batch_size)` (line 36): no docstring
    - `async close(self)` (line 96): no docstring
    - `async _send_to_dlq(self, item, error, attempts)` (line 105): no docstring
    - `async _handle_failure(self, item, error)` (line 147): no docstring
    - `async _publish_item(self, item)` (line 174): no docstring
    - `async flush_once(self)` (line 224): no docstring
    - `async maybe_purge(self)` (line 241): no docstring

### `backend/shared/services/events/dataset_ingest_reconciler.py`
- **Functions**
  - `async run_dataset_ingest_reconciler(dataset_registry, poll_interval_seconds, stale_after_seconds, stop_event)` (line 12): no docstring

### `backend/shared/services/events/dlq_handler_fixed.py`
- **Classes**
  - `RetryStrategy` (line 29): Retry strategies for failed messages
  - `RetryPolicy` (line 38): Configuration for retry behavior
  - `FailedMessage` (line 49): Representation of a failed message
    - `is_poison(self)` (line 65): Check if message should be considered poison
    - `age_hours(self)` (line 77): Get age of the message in hours
    - `calculate_next_retry_time(self, policy)` (line 82): Calculate when this message should be retried
  - `DLQHandlerFixed` (line 102): FIXED: Handles Dead Letter Queue processing with intelligent retry
    - `__init__(self, dlq_topic, kafka_config, redis_client, retry_policy, poison_topic, consumer_group)` (line 114): Initialize DLQ handler
    - `register_processor(self, topic, processor)` (line 155): Register a message processor for a specific topic
    - `async start_processing(self)` (line 160): Start processing DLQ messages
    - `async stop_processing(self)` (line 202): Stop processing DLQ messages
    - `async _process_loop(self)` (line 223): Main DLQ processing loop - FIXED to not block event loop
    - `async _process_dlq_message(self, msg)` (line 253): Process a message from the DLQ
    - `async _retry_scheduler(self)` (line 301): Background task to retry messages when their time comes
    - `async _retry_message(self, failed_msg)` (line 328): Retry a failed message
    - `async _add_to_retry_queue(self, failed_msg)` (line 372): Add message to retry queue
    - `async _move_to_poison_queue(self, failed_msg)` (line 398): Move message to poison queue
    - `async _record_recovery(self, failed_msg)` (line 434): Record successful recovery metrics
    - `_generate_message_id(self, value)` (line 450): Generate unique ID for a message
    - `async get_metrics(self)` (line 454): Get current metrics

### `backend/shared/services/events/event_replay.py`
- **Functions**
  - `async demo_replay()` (line 335): Demo the replay functionality
- **Classes**
  - `EventReplayService` (line 19): Deterministic event replay from S3 storage
    - `__init__(self, s3_endpoint, s3_access_key, s3_secret_key, bucket_name, s3_client)` (line 25): no docstring
    - `async replay_aggregate(self, db_name, class_id, aggregate_id, up_to_sequence, up_to_timestamp)` (line 44): Replay all events for a specific aggregate
    - `_apply_event(self, state, event)` (line 150): Apply a single event to the current state
    - `async replay_all_aggregates(self, db_name, class_id, limit)` (line 186): Replay all aggregates of a specific class
    - `async point_in_time_replay(self, db_name, class_id, aggregate_id, target_time)` (line 237): Replay aggregate state at a specific point in time
    - `async verify_replay_determinism(self, db_name, class_id, aggregate_id)` (line 258): Verify that replaying produces deterministic results
    - `async get_aggregate_history(self, db_name, class_id, aggregate_id)` (line 287): Get complete event history for an aggregate

### `backend/shared/services/events/idempotency_service.py`
- **Classes**
  - `IdempotencyService` (line 19): Provides idempotency guarantees for event processing.
    - `__init__(self, redis_client, ttl_seconds, namespace)` (line 27): Initialize idempotency service.
    - `_generate_key(self, event_id, aggregate_id)` (line 45): Generate Redis key for idempotency check.
    - `_generate_event_hash(self, event_data)` (line 60): Generate deterministic hash of event data.
    - `async is_duplicate(self, event_id, event_data, aggregate_id)` (line 74): Check if event is duplicate and acquire processing lock.
    - `async mark_processed(self, event_id, result, aggregate_id)` (line 133): Mark event as successfully processed with optional result.
    - `async mark_failed(self, event_id, error, aggregate_id, retry_after)` (line 175): Mark event as failed with error details.
    - `async get_processing_status(self, event_id, aggregate_id)` (line 224): Get current processing status of an event.
    - `async cleanup_expired(self, pattern)` (line 246): Clean up expired idempotency keys (Redis handles this automatically).
  - `IdempotentEventProcessor` (line 272): Wrapper for idempotent event processing.
    - `__init__(self, idempotency_service)` (line 280): Initialize idempotent processor.
    - `async process_event(self, event_id, event_data, processor_func, aggregate_id)` (line 289): Process event with idempotency guarantee.

### `backend/shared/services/events/objectify_job_queue.py`
- **Classes**
  - `ObjectifyJobQueue` (line 16): no docstring
    - `__init__(self, objectify_registry)` (line 17): no docstring
    - `async _get_registry(self)` (line 21): no docstring
    - `async close(self)` (line 28): no docstring
    - `async publish(self, job, require_delivery)` (line 34): no docstring

### `backend/shared/services/events/objectify_outbox.py`
- **Functions**
  - `async run_objectify_outbox_worker(objectify_registry, poll_interval_seconds, batch_size, stop_event)` (line 250): no docstring
- **Classes**
  - `ObjectifyOutboxPublisher` (line 30): no docstring
    - `__init__(self, objectify_registry, topic, batch_size)` (line 31): no docstring
    - `async close(self)` (line 81): no docstring
    - `async _publish_batch(self, batch)` (line 87): Publish a batch of outbox items with atomic delivery tracking.
    - `async flush_once(self)` (line 226): no docstring
    - `async maybe_purge(self)` (line 236): no docstring

### `backend/shared/services/events/objectify_reconciler.py`
- **Functions**
  - `async _build_objectify_payload(job, dataset_registry, objectify_registry, pipeline_registry)` (line 18): no docstring
  - `async reconcile_objectify_jobs(objectify_registry, dataset_registry, pipeline_registry, stale_after_seconds, enqueued_stale_seconds, limit, use_lock, lock_key)` (line 95): no docstring
  - `async run_objectify_reconciler(objectify_registry, dataset_registry, pipeline_registry, poll_interval_seconds, stale_after_seconds, enqueued_stale_seconds, stop_event)` (line 197): no docstring

### `backend/shared/services/events/outbox_runtime.py`
- **Functions**
  - `build_outbox_worker_id(configured_worker_id, service_name, hostname, default_service_name)` (line 13): no docstring
  - `async maybe_purge_with_interval(retention_days, purge_interval_seconds, purge_limit, last_purge, purge_call, info_logger, warning_logger, success_message, failure_message)` (line 28): no docstring
  - `async run_outbox_poll_loop(publisher, poll_interval_seconds, stop_event, warning_logger, failure_message, adaptive, min_poll_interval, max_poll_interval)` (line 55): Run outbox poll loop with optional adaptive polling.
  - `async flush_outbox_until_empty(publisher, is_empty)` (line 122): no docstring

### `backend/shared/services/kafka/__init__.py`

### `backend/shared/services/kafka/consumer_ops.py`
- **Classes**
  - `KafkaConsumerOps` (line 28): Strategy interface for executing consumer operations.
    - `async poll(self, timeout)` (line 32): no docstring
    - `async commit_sync(self, msg)` (line 36): no docstring
    - `async seek(self, tp)` (line 40): no docstring
    - `async pause(self, partitions)` (line 44): no docstring
    - `async resume(self, partitions)` (line 48): no docstring
    - `async close(self)` (line 52): no docstring
    - `async call(self, fn, *args, **kwargs)` (line 56): Execute an arbitrary call in the consumer's execution context.
  - `InlineKafkaConsumerOps` (line 66): Execute consumer operations inline on the event-loop thread.
    - `async poll(self, timeout)` (line 72): no docstring
    - `async commit_sync(self, msg)` (line 76): no docstring
    - `async seek(self, tp)` (line 80): no docstring
    - `async pause(self, partitions)` (line 84): no docstring
    - `async resume(self, partitions)` (line 88): no docstring
    - `async close(self)` (line 92): no docstring
    - `async call(self, fn, *args, **kwargs)` (line 95): no docstring
  - `ExecutorKafkaConsumerOps` (line 99): Execute all consumer operations on a dedicated single thread.
    - `__init__(self, consumer, thread_name_prefix, executor)` (line 107): no docstring
    - `async poll(self, timeout)` (line 122): no docstring
    - `async commit_sync(self, msg)` (line 126): no docstring
    - `async seek(self, tp)` (line 130): no docstring
    - `async pause(self, partitions)` (line 134): no docstring
    - `async resume(self, partitions)` (line 138): no docstring
    - `async close(self)` (line 142): no docstring
    - `async call(self, fn, *args, **kwargs)` (line 152): no docstring

### `backend/shared/services/kafka/dlq_publisher.py`
- **Functions**
  - `_safe_decode(value)` (line 33): no docstring
  - `build_standard_dlq_payload(msg, worker, stage, error, attempt_count, payload_text, payload_obj, error_max_chars, extra, enterprise_error)` (line 44): no docstring
  - `default_dlq_span_attributes(dlq_topic, msg)` (line 138): no docstring
  - `build_envelope_dlq_event(envelope, spec, error, attempt_count, extra_metadata)` (line 148): no docstring
  - `default_envelope_dlq_span_attributes(spec, envelope)` (line 173): no docstring
  - `async publish_envelope_dlq(producer_ops, spec, envelope, tracing, metrics, key, span_attributes)` (line 185): no docstring
  - `async publish_standard_dlq(producer, msg, worker, dlq_spec, error, attempt_count, stage, payload_text, payload_obj, extra, tracing, metrics, kafka_headers, fallback_metadata, lock, error_max_chars)` (line 235): no docstring
  - `async publish_dlq_json(producer, spec, msg, payload, message_key, tracing, metrics, kafka_headers, fallback_metadata, lock, span_attributes)` (line 279): no docstring
  - `async publish_contextual_dlq_json(producer, spec, payload, message_key, msg, source_topic, source_partition, source_offset, tracing, metrics, kafka_headers, fallback_metadata, lock, span_attributes)` (line 342): no docstring
- **Classes**
  - `DlqPublishSpec` (line 102): no docstring
  - `EnvelopeDlqSpec` (line 112): no docstring
  - `_SyntheticKafkaMessage` (line 123): no docstring
    - `topic(self)` (line 128): no docstring
    - `partition(self)` (line 131): no docstring
    - `offset(self)` (line 134): no docstring

### `backend/shared/services/kafka/processed_event_worker.py`
- **Classes**
  - `RegistryKey` (line 57): no docstring
  - `HeartbeatOptions` (line 64): no docstring
  - `ParseErrorContext` (line 72): no docstring
  - `CommandParseError` (line 80): no docstring
    - `__init__(self, stage, payload_text, payload_obj, fallback_metadata, cause)` (line 81): no docstring
  - `WorkerRuntimeConfig` (line 99): no docstring
  - `ProcessedEventKafkaWorker` (line 110): Template Method for Kafka workers guarded by ProcessedEventRegistry.
    - `_bootstrap_worker_runtime(self, config, tracing, metrics)` (line 136): no docstring
    - `_parse_payload(self, payload)` (line 163): Parse msg.value() into a domain payload (may raise).
    - `_registry_key(self, payload)` (line 167): Extract the ProcessedEventRegistry key for this payload.
    - `async _process_payload(self, payload)` (line 171): Perform the worker side-effect for a claimed payload.
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count, stage, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 174): Publish a DLQ record for a terminal failure.
    - `_service_name(self)` (line 208): no docstring
    - `_fallback_metadata(self, payload)` (line 211): no docstring
    - `_span_name(self, payload)` (line 214): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 218): no docstring
    - `_metric_event_name(self, payload)` (line 229): no docstring
    - `_registry_handler(self, msg, payload)` (line 232): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 235): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 239): no docstring
    - `_should_seek_on_in_progress(self, claim, payload)` (line 242): no docstring
    - `_should_seek_on_retry(self, attempt_count, payload)` (line 245): no docstring
    - `_should_mark_done_after_dlq(self, payload, error)` (line 248): no docstring
    - `_cancel_inflight_on_revoke(self)` (line 251): Whether to cancel in-flight partition tasks on Kafka rebalance revoke.
    - `_backoff_seconds(self, attempt_count, payload)` (line 263): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 266): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 269): no docstring
    - `async _send_parse_error_via_publish_to_dlq(self, msg, stage, cause_text, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 280): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 305): no docstring
    - `_parse_error_context(self, raw_payload, error)` (line 345): no docstring
    - `_safe_message_headers(self, msg)` (line 364): no docstring
    - `_fallback_metadata_from_raw_payload(self, raw_payload)` (line 371): no docstring
    - `async _publish_parse_error_to_dlq(self, msg, raw_payload, error, dlq_sender, publish_failure_message, invalid_payload_message, raise_on_publish_failure, logger_instance)` (line 385): no docstring
    - `_normalize_dlq_publish_inputs(self, msg, stage, default_stage, raw_payload, payload_text, payload_obj, kafka_headers, fallback_metadata, inferred_payload_obj, inferred_metadata, inferred_stage)` (line 418): no docstring
    - `async _publish_standard_dlq_record(self, producer, msg, worker, dlq_spec, error, attempt_count, stage, payload_text, payload_obj, extra, kafka_headers, fallback_metadata, tracing, metrics, lock, raise_on_missing_producer, missing_producer_message)` (line 454): no docstring
    - `async _send_standard_dlq_record(self, msg, error, attempt_count, stage, default_stage, raw_payload, payload_text, payload_obj, kafka_headers, fallback_metadata, publisher, inferred_payload_obj, inferred_metadata, inferred_stage)` (line 500): no docstring
    - `async _send_command_payload_dlq_record(self, msg, error, attempt_count, payload, raw_payload, stage, default_stage, payload_text, payload_obj, kafka_headers, fallback_metadata, publisher)` (line 545): no docstring
    - `async _send_default_command_payload_dlq(self, msg, error, attempt_count, payload, raw_payload, publisher)` (line 587): no docstring
    - `async _on_success(self, payload, result, duration_s)` (line 615): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 625): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 636): no docstring
    - `async _mark_retryable_failure(self, payload, registry_key, handler, error)` (line 646): no docstring
    - `async _commit(self, msg)` (line 660): no docstring
    - `async _seek(self, topic, partition, offset)` (line 667): no docstring
    - `_heartbeat_options(self)` (line 674): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 677): no docstring
    - `async _poll_message(self, timeout)` (line 690): no docstring
    - `_initialize_safe_consumer_runtime(self, group_id, topics, service_name, thread_name_prefix, max_poll_interval_ms, session_timeout_ms, reset_partition_state)` (line 699): no docstring
    - `_get_consumer_ops(self)` (line 731): no docstring
    - `_get_consumer_runtime(self)` (line 742): no docstring
    - `async _close_consumer_runtime(self)` (line 756): no docstring
    - `_is_partition_eof(self, msg)` (line 788): no docstring
    - `_loop_label(self)` (line 800): no docstring
    - `async _on_poll_exception(self, exc)` (line 803): no docstring
    - `async _on_kafka_message_error(self, msg)` (line 806): no docstring
    - `async _on_unexpected_message_error(self, exc, msg)` (line 812): no docstring
    - `async _seek_on_unexpected_error(self, msg)` (line 815): no docstring
    - `_init_partition_state(self, reset)` (line 825): no docstring
    - `_buffer_messages(self)` (line 840): no docstring
    - `_pending_log_thresholds(self)` (line 843): no docstring
    - `_uses_commit_state(self)` (line 846): no docstring
    - `_busy_partition_sleep_seconds(self)` (line 849): no docstring
    - `_partition_task_name(self, msg)` (line 852): no docstring
    - `_partition_key(self, msg)` (line 856): no docstring
    - `_log_buffered_message(self, msg, pending_count)` (line 859): no docstring
    - `async _pause_partition(self, topic, partition)` (line 869): no docstring
    - `async _resume_partition(self, topic, partition)` (line 874): no docstring
    - `_log_background_task_exception(self, task)` (line 879): no docstring
    - `_run_on_event_loop_thread(self, fn)` (line 890): no docstring
    - `_handle_partitions_revoked(self, partitions, clear_pending)` (line 907): no docstring
    - `_handle_partitions_assigned(self, partitions, resume)` (line 923): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 932): Default SafeKafkaConsumer rebalance revoke callback.
    - `_on_partitions_assigned(self, partitions)` (line 949): Default SafeKafkaConsumer rebalance assign callback.
    - `async _handle_busy_partition_message(self, msg)` (line 964): no docstring
    - `async _start_partition_task(self, msg)` (line 981): no docstring
    - `async _handle_partition_message(self, msg)` (line 996): no docstring
    - `async _cancel_inflight_tasks(self)` (line 1055): no docstring
    - `async _poll_for_message(self, poll_timeout, idle_sleep, missing_consumer_sleep, poll_exception_sleep)` (line 1062): Shared polling template for worker loops.
    - `async run_loop(self, poll_timeout, idle_sleep, missing_consumer_sleep, poll_exception_sleep, unexpected_error_sleep, post_seek_sleep, seek_on_error, catch_exceptions)` (line 1106): no docstring
    - `async run_partitioned_loop(self, poll_timeout, idle_sleep, missing_consumer_sleep, poll_exception_sleep)` (line 1148): no docstring
    - `async handle_message(self, msg)` (line 1191): Handle a single Kafka message (msg.value()).
    - `async _handle_claimed(self, msg, payload, registry_key, topic, partition, offset, raw_text, start)` (line 1282): no docstring
  - `StrictHeartbeatPolicyMixin` (line 1434): Shared strict heartbeat policy for ProcessedEventRegistry-backed workers.
    - `_heartbeat_options(self)` (line 1442): no docstring
  - `StrictHeartbeatKafkaWorker` (line 1449): ProcessedEventKafkaWorker with strict heartbeat behavior.
  - `EventEnvelopeKafkaWorker` (line 1457): Specialization of ProcessedEventKafkaWorker for EventEnvelope payloads.
    - `_parse_payload(self, payload)` (line 1467): no docstring
    - `_registry_key(self, payload)` (line 1481): no docstring
    - `_fallback_metadata(self, payload)` (line 1488): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 1491): no docstring
    - `_metric_event_name(self, payload)` (line 1503): no docstring
    - `async _publish_envelope_failure_to_dlq(self, msg, payload, error, attempt_count, producer_ops, spec, key, missing_producer_message)` (line 1507): no docstring
    - `async _send_envelope_failure_to_dlq(self, msg, payload, error, attempt_count, producer_ops, spec, key_fallback, missing_producer_message)` (line 1543): no docstring
  - `StrictHeartbeatEventEnvelopeKafkaWorker` (line 1569): EventEnvelopeKafkaWorker with strict heartbeat behavior.

### `backend/shared/services/kafka/producer_factory.py`
- **Functions**
  - `create_kafka_producer(bootstrap_servers, client_id, acks, retries, compression_type, retry_backoff_ms, linger_ms, enable_idempotence, max_in_flight_requests_per_connection, extra_config, producer_ctor)` (line 21): no docstring
  - `create_kafka_dlq_producer(bootstrap_servers, client_id, acks, retries, retry_backoff_ms, linger_ms, compression_type, enable_idempotence, max_in_flight_requests_per_connection, extra_config, producer_ctor)` (line 57): no docstring

### `backend/shared/services/kafka/producer_ops.py`
- **Functions**
  - `async close_kafka_producer(producer, producer_ops, timeout_s, warning_logger, warning_message)` (line 104): no docstring
- **Classes**
  - `KafkaProducerOps` (line 25): Strategy interface for executing producer operations.
    - `async produce(self, **kwargs)` (line 29): no docstring
    - `async flush(self, timeout_s)` (line 33): no docstring
    - `async close(self, timeout_s)` (line 37): no docstring
  - `InlineKafkaProducerOps` (line 42): Execute producer operations inline on the event-loop thread.
    - `async produce(self, **kwargs)` (line 48): no docstring
    - `async flush(self, timeout_s)` (line 52): no docstring
    - `async close(self, timeout_s)` (line 56): no docstring
  - `ExecutorKafkaProducerOps` (line 60): Execute all producer operations on a dedicated single thread.
    - `__init__(self, producer, thread_name_prefix, executor)` (line 68): no docstring
    - `async produce(self, **kwargs)` (line 83): no docstring
    - `async flush(self, timeout_s)` (line 87): no docstring
    - `async close(self, timeout_s)` (line 92): no docstring

### `backend/shared/services/kafka/retry_classifier.py`
- **Functions**
  - `normalize_error_message(exc)` (line 22): no docstring
  - `contains_marker(message, markers)` (line 26): no docstring
  - `classify_retryable_by_markers(exc, non_retryable_markers, retryable_markers, default_retryable)` (line 34): no docstring
  - `_normalize_markers(markers)` (line 49): no docstring
  - `create_retry_policy_profile(non_retryable_markers, retryable_markers, default_retryable)` (line 58): no docstring
  - `classify_retryable_with_profile(exc, profile)` (line 71): no docstring
  - `classify_retryable_by_error_code(code)` (line 227): Classify retryability based on a structured ErrorCode.
- **Classes**
  - `RetryPolicyProfile` (line 16): no docstring

### `backend/shared/services/kafka/safe_consumer.py`
- **Functions**
  - `create_safe_consumer(group_id, topics, service_name, **kwargs)` (line 517): Factory function to create a SafeKafkaConsumer.
  - `validate_consumer_config(config)` (line 546): Validate that a consumer config meets safety requirements.
- **Classes**
  - `ConsumerState` (line 30): Consumer lifecycle states.
  - `PartitionState` (line 40): Track state for each assigned partition.
  - `RebalanceHandler` (line 51): Handle consumer group rebalancing events.
    - `__init__(self, consumer, on_revoke_callback, on_assign_callback)` (line 58): no docstring
    - `on_revoke(self, consumer, partitions)` (line 69): Called before partitions are revoked.
    - `on_assign(self, consumer, partitions)` (line 121): Called after partitions are assigned.
  - `SafeKafkaConsumer` (line 153): Production-hardened Kafka consumer with strong consistency guarantees.
    - `__init__(self, group_id, topics, service_name, extra_config, on_revoke, on_assign, subscribe, session_timeout_ms, max_poll_interval_ms, heartbeat_interval_ms)` (line 186): Create a safe Kafka consumer.
    - `state(self)` (line 276): Current consumer state.
    - `is_rebalancing(self)` (line 281): Check if consumer is currently rebalancing.
    - `poll(self, timeout)` (line 286): Poll for a message with rebalance awareness.
    - `wait_for_assignment(self, timeout_seconds)` (line 318): Block until partitions are assigned (or timeout).
    - `mark_processed(self, msg)` (line 344): Mark a message as successfully processed.
    - `commit(self, message, offsets, asynchronous, msg)` (line 362): Commit offsets.
    - `commit_sync(self, msg)` (line 432): Synchronously commit a specific message offset.
    - `seek(self, partition)` (line 436): Seek to a specific offset for a partition.
    - `close(self, timeout)` (line 455): Gracefully close the consumer.
    - `__enter__(self)` (line 477): no docstring
    - `__exit__(self, _exc_type, _exc_val, _exc_tb)` (line 480): no docstring
    - `list_topics(self, topic, timeout)` (line 484): List available topics.
    - `get_watermark_offsets(self, partition, timeout)` (line 488): Return (low, high) offsets for a partition.
    - `committed(self, partitions, timeout)` (line 492): Return committed offsets for partitions in this consumer group.
    - `assignment(self)` (line 496): Get current partition assignment.
    - `position(self, partitions)` (line 500): Get current position for partitions.
    - `pause(self, partitions)` (line 504): Pause fetching from the provided partitions (backpressure).
    - `resume(self, partitions)` (line 510): Resume fetching from the provided partitions (backpressure).

### `backend/shared/services/kafka/worker_consumer_runtime.py`
- **Classes**
  - `WorkerConsumerRuntime` (line 33): no docstring
    - `async poll_message(self, timeout, poller)` (line 41): no docstring
    - `async commit(self, msg)` (line 47): no docstring
    - `async seek(self, topic, partition, offset)` (line 76): no docstring
    - `async pause_partition(self, topic, partition)` (line 102): no docstring
    - `async resume_partition(self, topic, partition)` (line 115): no docstring

### `backend/shared/services/pipeline/__init__.py`

### `backend/shared/services/pipeline/dataset_output_semantics.py`
- **Functions**
  - `_text(value)` (line 143): no docstring
  - `_first_text(output_metadata, settings, definition, keys)` (line 147): no docstring
  - `_first_raw(output_metadata, settings, definition, keys)` (line 162): no docstring
  - `_normalize_partition_by(value)` (line 176): no docstring
  - `_normalize_primary_key_columns(value)` (line 195): no docstring
  - `_normalize_write_mode(value)` (line 210): no docstring
  - `_mode_requires_primary_key(mode)` (line 217): no docstring
  - `normalize_dataset_output_metadata(definition, output_metadata)` (line 226): no docstring
  - `_resolve_default_runtime_write_mode(execution_semantics, has_incremental_input, incremental_inputs_have_additive_updates, warnings)` (line 302): no docstring
  - `_normalize_bool(value)` (line 326): no docstring
  - `_runtime_write_mode(mode)` (line 339): no docstring
  - `validate_dataset_output_format_constraints(output_format, partition_by)` (line 349): no docstring
  - `_policy_hash_payload(policy)` (line 365): no docstring
  - `_compute_policy_hash(payload)` (line 380): no docstring
  - `resolve_dataset_write_policy(definition, output_metadata, execution_semantics, has_incremental_input, incremental_inputs_have_additive_updates)` (line 385): no docstring
  - `validate_dataset_output_metadata(definition, output_metadata, execution_semantics, has_incremental_input, incremental_inputs_have_additive_updates, available_columns)` (line 464): no docstring
- **Classes**
  - `DatasetWriteMode` (line 12): no docstring
  - `NormalizedDatasetOutputMetadata` (line 106): no docstring
  - `ResolvedDatasetWritePolicy` (line 113): no docstring
    - `requires_primary_key(self)` (line 130): no docstring
    - `append_mode(self)` (line 139): no docstring

### `backend/shared/services/pipeline/fk_pattern_detector.py`
- **Classes**
  - `ForeignKeyPattern` (line 19): Detected foreign key pattern
  - `TargetCandidate` (line 32): Potential FK target (dataset or object_type)
  - `FKDetectionConfig` (line 43): Configuration for FK pattern detection - no hardcoding
    - `from_dict(cls, config_dict)` (line 83): Create config from dictionary (for settings injection)
  - `ForeignKeyPatternDetector` (line 95): Detects foreign key patterns in dataset schemas using configurable rules.
    - `__init__(self, config)` (line 106): no docstring
    - `_compile_patterns(self)` (line 112): Pre-compile regex patterns for performance
    - `detect_patterns(self, source_dataset_id, source_schema, source_sample, target_candidates)` (line 123): Detect potential FK relationships in a dataset.
    - `_is_excluded(self, column_name)` (line 191): Check if column should be excluded from FK detection
    - `_is_fk_compatible_type(self, column_type)` (line 195): Check if column type is compatible with FK references
    - `_detect_by_naming(self, source_dataset_id, column_name, target_candidates)` (line 203): Detect FK by naming convention patterns
    - `_extract_target_name(self, column_name, pattern)` (line 255): Extract potential target name from FK column name
    - `_find_matching_target(self, potential_name, candidates)` (line 263): Find a target candidate that matches the potential name
    - `_detect_by_value_overlap(self, source_dataset_id, column_name, source_sample, target_candidates)` (line 290): Detect FK by value overlap with target primary keys
    - `_extract_column_values(self, sample, column_name)` (line 343): Extract column values from sample data
    - `_calculate_overlap_confidence(self, source_set, target_set)` (line 356): Calculate FK confidence based on value overlap.
    - `suggest_link_type(self, fk_pattern, source_object_type)` (line 389): Generate a link_type suggestion from a detected FK pattern.
    - `_generate_predicate(self, column_name)` (line 425): Generate a predicate name from FK column name

### `backend/shared/services/pipeline/objectify_delta_utils.py`
- **Functions**
  - `create_delta_computer_for_mapping_spec(mapping_spec)` (line 362): Factory function to create a delta computer from mapping spec configuration.
- **Classes**
  - `DeltaResult` (line 19): Result of delta computation between two dataset versions
    - `__post_init__(self)` (line 26): no docstring
    - `has_changes(self)` (line 36): no docstring
  - `WatermarkState` (line 41): Watermark state for incremental processing
    - `to_dict(self)` (line 50): no docstring
    - `from_dict(cls, data)` (line 61): no docstring
  - `ObjectifyDeltaComputer` (line 78): Computes deltas for incremental objectify processing.
    - `__init__(self, pk_columns, batch_size)` (line 92): Args:
    - `compute_row_key(self, row)` (line 105): Compute a unique key for a row based on primary key columns
    - `compute_row_hash(self, row)` (line 110): Compute a content hash for a row (for change detection)
    - `async compute_delta_from_watermark(self, rows_iterator, watermark_column, previous_watermark)` (line 116): Compute delta using watermark-based filtering.
    - `_compare_watermarks(self, a, b)` (line 158): Compare two watermark values.
    - `async compute_delta_from_snapshots(self, old_rows_iterator, new_rows_iterator)` (line 183): Compute delta by comparing two full snapshots.
    - `async compute_delta_from_lakefs_diff(self, lakefs_client, repository, base_ref, target_ref, path, file_reader)` (line 230): Compute delta using LakeFS diff API.
    - `async _get_lakefs_diff(self, lakefs_client, repository, base_ref, target_ref, path)` (line 310): Get diff entries from LakeFS
    - `filter_rows_by_watermark(self, rows, watermark_column, previous_watermark)` (line 332): Synchronous version of watermark filtering.

### `backend/shared/services/pipeline/output_plugins.py`
- **Functions**
  - `_text(payload, *keys)` (line 45): no docstring
  - `_camel_case(value)` (line 56): no docstring
  - `resolve_ontology_output_semantics(payload)` (line 194): no docstring
  - `resolve_output_kind(value)` (line 253): no docstring
  - `normalize_output_kind(value)` (line 267): no docstring
  - `get_output_plugin(kind)` (line 271): no docstring
  - `validate_output_payload(kind, payload)` (line 276): no docstring
- **Classes**
  - `OutputPlugin` (line 32): no docstring
    - `validate(self, payload)` (line 35): no docstring
  - `ResolvedOutputKind` (line 39): no docstring
  - `_DatasetPlugin` (line 63): no docstring
    - `validate(self, payload)` (line 66): no docstring
  - `_RequiredMetadataPlugin` (line 74): no docstring
    - `validate(self, payload)` (line 78): no docstring
  - `_GeotemporalPlugin` (line 85): no docstring
    - `__init__(self)` (line 88): no docstring
    - `validate(self, payload)` (line 98): no docstring
  - `_MediaPlugin` (line 108): no docstring
    - `__init__(self)` (line 111): no docstring
    - `validate(self, payload)` (line 120): no docstring
  - `_VirtualPlugin` (line 130): no docstring
    - `__init__(self)` (line 140): no docstring
    - `validate(self, payload)` (line 149): no docstring
  - `OntologyOutputSemantics` (line 175): no docstring
  - `_OntologyPlugin` (line 233): no docstring
    - `validate(self, payload)` (line 236): no docstring

### `backend/shared/services/pipeline/pipeline_artifact_store.py`
- **Classes**
  - `PipelineArtifactStore` (line 16): no docstring
    - `__init__(self, storage_service, bucket)` (line 17): no docstring
    - `async save_table(self, dataset_name, columns, rows, db_name, pipeline_id)` (line 21): no docstring

### `backend/shared/services/pipeline/pipeline_claim_refuter.py`
- **Functions**
  - `_normalize_string_list(value)` (line 37): no docstring
  - `_normalize_claim_kind(value)` (line 51): no docstring
  - `_normalize_claim_severity(value)` (line 79): no docstring
  - `_find_similar_claim_kinds(target, cutoff)` (line 110): Find claim kinds similar to the target for helpful error messages.
  - `_claim_spec(claim)` (line 130): no docstring
  - `_claim_target_node_id(claim)` (line 135): no docstring
  - `_row_ref(row, row_index, include_columns)` (line 145): no docstring
  - `_row_digest_for_columns(row, columns)` (line 160): no docstring
  - `_bag_counts_for_table(table, columns)` (line 165): no docstring
  - `_first_row_by_digest(table, columns, digest)` (line 173): no docstring
  - `_value_is_null(value, treat_empty_as_null, treat_whitespace_as_null)` (line 180): no docstring
  - `_refute_filter_only_nulls(input_table, output_table, column, treat_empty_as_null, treat_whitespace_as_null)` (line 192): Refute the claim that a filter only removes rows where target column is NULL.
  - `_refute_filter_min_retain_rate(input_table, output_table, min_rate)` (line 288): no docstring
  - `_refute_union_row_lossless(left, right, output)` (line 317): no docstring
  - `_refute_pk(table, key_cols, require_not_null)` (line 363): no docstring
  - `_refute_join_functional_right(left, right, left_keys, right_keys)` (line 409): Refute the claim that the join is functional on the right side (N:1 relationship).
  - `_refute_cast_success(table, casts, only_columns)` (line 476): no docstring
  - `_normalize_allowed_normalization(value)` (line 539): no docstring
  - `_serialize_canonical_decimal(value)` (line 573): Deterministic, non-scientific canonical form with bounded precision.
  - `_serialize_roundtrip(value, target_type)` (line 624): no docstring
  - `_apply_lossless_normalization(text, rules)` (line 668): no docstring
  - `_refute_cast_lossless(input_table, output_table, casts, allowed_normalization, only_columns)` (line 720): no docstring
  - `_extract_claims(definition_json)` (line 819): no docstring
  - `async refute_pipeline_plan_claims(plan, dataset_registry, run_tables, sample_limit, max_output_rows, max_hard_failures, max_soft_warnings)` (line 852): Returns:

### `backend/shared/services/pipeline/pipeline_control_plane_events.py`
- **Functions**
  - `pipeline_control_plane_events_enabled()` (line 15): no docstring
  - `sanitize_event_id(value)` (line 23): no docstring
  - `async emit_pipeline_control_plane_event(event_type, pipeline_id, event_id, data, actor, kind)` (line 27): no docstring

### `backend/shared/services/pipeline/pipeline_dataset_utils.py`
- **Functions**
  - `resolve_fallback_branches(fallback_raw)` (line 29): no docstring
  - `build_branch_candidates(requested_branch, fallback_branches, fallback_raw)` (line 41): no docstring
  - `normalize_dataset_selection(metadata, default_branch, fallback_raw)` (line 56): no docstring
  - `async resolve_dataset_version(dataset_registry, db_name, selection)` (line 81): no docstring
- **Classes**
  - `DatasetSelection` (line 10): no docstring
  - `DatasetResolution` (line 18): no docstring

### `backend/shared/services/pipeline/pipeline_definition_augmentation.py`
- **Functions**
  - `_clone_definition_graph(definition_json)` (line 42): no docstring
  - `_output_name_for_node(node_id, node)` (line 56): no docstring
  - `_is_canonical_output(node_id, node, canonical_output_names)` (line 61): no docstring
  - `_sql_ident(name)` (line 74): no docstring
  - `_concat_expr(columns, null_if_any)` (line 78): no docstring
  - `_merge_schema_checks(existing, additions)` (line 87): no docstring
  - `_sys_compute_expressions(output_name)` (line 112): no docstring
  - `_refresh_sys_compute_nodes(node_by_id, output_id, output_name)` (line 126): no docstring
  - `_inject_sys_columns_chain(edges, nodes, node_by_id, existing_ids, source_id, output_id, output_name)` (line 150): no docstring
  - `_inject_fk_join_check(edges, nodes, node_by_id, existing_ids, left_node_id, output_id, fk_index, fk_spec, default_branch)` (line 183): no docstring
  - `augment_definition_with_canonical_contract(definition_json, branch, canonical_output_names)` (line 401): no docstring
  - `_is_cast_transform(node)` (line 497): no docstring
  - `async augment_definition_with_casts(definition_json, db_name, branch, dataset_registry)` (line 509): no docstring

### `backend/shared/services/pipeline/pipeline_definition_utils.py`
- **Functions**
  - `split_expectation_columns(column)` (line 8): no docstring
  - `normalize_expectation_columns(value)` (line 12): no docstring
  - `resolve_execution_semantics(definition, pipeline_type)` (line 18): no docstring
  - `resolve_incremental_config(definition)` (line 55): no docstring
  - `resolve_incremental_watermark_column(definition, metadata)` (line 60): no docstring
  - `is_truthy(value)` (line 75): no docstring
  - `normalize_pk_semantics(value)` (line 85): no docstring
  - `resolve_pk_semantics(execution_semantics, definition, output_metadata)` (line 102): no docstring
  - `resolve_delete_column(definition, output_metadata)` (line 136): no docstring
  - `coerce_pk_columns(value)` (line 159): no docstring
  - `collect_pk_columns(*candidates)` (line 200): no docstring
  - `match_output_declaration(output, node_id, output_name)` (line 211): no docstring
  - `resolve_pk_columns(definition, output_metadata, output_name, output_node_id, declared_outputs)` (line 234): no docstring
  - `build_expectations_with_pk(definition, output_metadata, output_name, output_node_id, declared_outputs, pk_semantics, delete_column, pk_columns, available_columns)` (line 280): no docstring
  - `validate_pk_semantics(available_columns, pk_semantics, pk_columns, delete_column)` (line 334): no docstring

### `backend/shared/services/pipeline/pipeline_definition_validator.py`
- **Functions**
  - `_resolve_output_kind_for_node(definition_json, node_id, output_name, metadata)` (line 24): no docstring
  - `_merged_output_payload_for_node(definition_json, node_id, output_name, metadata)` (line 57): no docstring
  - `normalize_transform_metadata(metadata)` (line 89): no docstring
  - `validate_pipeline_definition(definition_json, policy)` (line 151): no docstring
- **Classes**
  - `PipelineDefinitionValidationPolicy` (line 134): no docstring
  - `PipelineDefinitionValidationResult` (line 144): no docstring

### `backend/shared/services/pipeline/pipeline_dependency_utils.py`
- **Functions**
  - `normalize_dependency_entries(raw, strict)` (line 6): no docstring

### `backend/shared/services/pipeline/pipeline_executor.py`
- **Functions**
  - `_extract_schema_columns(schema)` (line 777): no docstring
  - `_extract_schema_types(schema)` (line 795): no docstring
  - `_extract_sample_rows(sample)` (line 818): no docstring
  - `_fallback_columns(node)` (line 837): no docstring
  - `_build_sample_rows(columns, count)` (line 847): no docstring
  - `_group_by_table(table, group_by, aggregates)` (line 857): no docstring
  - `_pivot_table(table, pivot_meta)` (line 930): no docstring
  - `_nulls_last_sort_key(row, column)` (line 973): no docstring
  - `_window_table(table, window_meta)` (line 978): no docstring
  - `_select_columns(table, columns)` (line 1033): no docstring
  - `_drop_columns(table, columns)` (line 1039): no docstring
  - `_rename_columns(table, rename_map)` (line 1046): no docstring
  - `_record_cast_stat(cast_stats, column, attempted, failed)` (line 1058): no docstring
  - `_cast_value_with_status(value, target, cast_mode)` (line 1073): no docstring
  - `_cast_value(value, target, cast_mode)` (line 1125): no docstring
  - `_apply_schema_casts(rows, schema_types, cast_mode, cast_stats)` (line 1130): no docstring
  - `_cast_columns(table, casts, cast_mode, cast_stats)` (line 1152): no docstring
  - `_dedupe_table(table, columns)` (line 1178): no docstring
  - `_sort_table(table, columns)` (line 1191): no docstring
  - `_union_tables(left, right, union_mode)` (line 1231): no docstring
  - `_join_tables(left, right, join_type, left_key, right_key, join_key, left_keys, right_keys, allow_cross_join, max_output_rows)` (line 1269): no docstring
  - `_build_join_output_layout(left_columns, right_columns)` (line 1381): no docstring
  - `_stream_join_tables(left, right, metadata, max_output_rows)` (line 1408): no docstring
  - `_right_latest_snapshot_table(table, right_keys, right_event_time_column)` (line 1596): no docstring
  - `_resolve_stream_join_right_column(columns, left_col, right_col)` (line 1618): no docstring
  - `_to_epoch_seconds(value)` (line 1635): no docstring
  - `_merge_rows(left, right, right_column_map)` (line 1658): no docstring
  - `_find_similar_columns(target, available, cutoff)` (line 1675): Enterprise Enhancement (2026-01):
  - `_filter_table(table, expression, parameters)` (line 1703): Filter table rows based on expression.
  - `_parse_filter(expression, parameters)` (line 1751): no docstring
  - `_compare(left, op, right)` (line 1767): no docstring
  - `_compute_assignment_table(table, target, expression, parameters)` (line 1791): no docstring
  - `_compute_table(table, expression, parameters)` (line 1813): no docstring
  - `_explode_table(table, column)` (line 1843): no docstring
  - `_parse_assignment(expression)` (line 1866): no docstring
  - `_safe_eval(expression, row, parameters)` (line 1873): no docstring
  - `_is_safe_ast(node)` (line 1909): no docstring
  - `_eval_ast(node, variables)` (line 1925): no docstring
  - `_parse_literal(raw)` (line 2008): no docstring
  - `_parse_timestamp_literal(raw)` (line 2026): no docstring
  - `_normalize_table(table, columns, trim, empty_to_null, whitespace_to_null, lowercase, uppercase)` (line 2045): no docstring
  - `_geospatial_table(table, metadata, parameters)` (line 2083): no docstring
  - `_pattern_mining_table(table, metadata, parameters)` (line 2141): no docstring
  - `_encode_geohash_text(lat, lon, precision)` (line 2175): no docstring
  - `_regex_flags(raw)` (line 2211): no docstring
  - `_normalize_regex_rules(metadata)` (line 2225): no docstring
  - `_regex_replace_table(table, rules)` (line 2264): no docstring
  - `_parse_csv_bytes(raw_bytes, max_rows)` (line 2292): Parse a CSV payload into row dicts.
  - `_parse_excel_bytes(raw_bytes, max_rows)` (line 2343): no docstring
  - `_parse_json_bytes(raw_bytes, max_rows)` (line 2355): no docstring
  - `_infer_column_types(table)` (line 2390): no docstring
  - `_build_table_ops(table)` (line 2399): no docstring
- **Classes**
  - `PipelineExpectationError` (line 69): no docstring
  - `PipelineTable` (line 74): no docstring
    - `limited_rows(self, limit)` (line 78): no docstring
  - `PipelineRunResult` (line 85): no docstring
  - `PipelineArtifactStore` (line 90): no docstring
    - `__init__(self, base_path)` (line 91): no docstring
    - `save_table(self, table, dataset_name)` (line 96): no docstring
  - `PipelineExecutor` (line 108): no docstring
    - `__init__(self, dataset_registry, pipeline_registry, artifact_store, storage_service)` (line 109): no docstring
    - `async preview(self, definition, db_name, node_id, limit, input_overrides)` (line 130): no docstring
    - `async deploy(self, definition, db_name, node_id, dataset_name, store_local, input_overrides)` (line 143): no docstring
    - `async run(self, definition, db_name, input_overrides)` (line 163): no docstring
    - `async _load_input(self, node, db_name, branch, sample_limit)` (line 299): no docstring
    - `async _load_rows_from_artifact(self, artifact_key, max_rows)` (line 347): no docstring
    - `async _load_fk_reference_rows(self, db_name, dataset_id, dataset_name, branch)` (line 420): no docstring
    - `async _evaluate_fk_expectations(self, expectations, output_table, db_name, branch)` (line 450): no docstring
    - `async _apply_transform(self, metadata, inputs, parameters)` (line 555): no docstring
    - `async _apply_udf_transform(self, table, metadata)` (line 713): no docstring
    - `_table_to_sample(self, table, limit)` (line 739): no docstring
    - `_summarize_cast_stats(self, columns)` (line 751): no docstring
    - `_select_table(self, result, node_id)` (line 767): no docstring

### `backend/shared/services/pipeline/pipeline_funnel_fallback.py`
- **Functions**
  - `_is_non_empty(value)` (line 45): no docstring
  - `_sample_non_empty(values, max_samples)` (line 53): no docstring
  - `_phone_ratio(values)` (line 65): no docstring
  - `_email_ratio(values)` (line 78): no docstring
  - `_bool_ratio(values)` (line 91): no docstring
  - `_int_ratio(values)` (line 104): no docstring
  - `_decimal_ratio(values)` (line 119): no docstring
  - `_date_datetime_ratios(values)` (line 134): no docstring
  - `infer_type_fallback(values, column_name, include_complex_types)` (line 171): Deterministic, dependency-free(ish) inference used when Funnel is unavailable.
  - `build_funnel_analysis_fallback(columns, rows, include_complex_types, error, stage)` (line 283): Build a Funnel-compatible analysis payload from sample rows without calling Funnel.

### `backend/shared/services/pipeline/pipeline_graph_utils.py`
- **Functions**
  - `unique_node_id(base, existing, start_index)` (line 7): Return a node id that is unique within `existing`.
  - `normalize_nodes(nodes_raw)` (line 30): no docstring
  - `normalize_edges(edges_raw)` (line 44): no docstring
  - `build_incoming(edges)` (line 59): no docstring
  - `topological_sort(nodes, edges, include_unordered)` (line 66): no docstring

### `backend/shared/services/pipeline/pipeline_job_queue.py`
- **Classes**
  - `PipelineJobQueue` (line 25): no docstring
    - `__init__(self)` (line 26): no docstring
    - `_producer_instance(self)` (line 31): no docstring
    - `async publish(self, job, require_delivery)` (line 44): no docstring

### `backend/shared/services/pipeline/pipeline_join_keys.py`
- **Functions**
  - `normalize_join_key_list(value)` (line 6): no docstring

### `backend/shared/services/pipeline/pipeline_kafka_avro.py`
- **Functions**
  - `_text(value)` (line 26): no docstring
  - `_first_text(keys, sources)` (line 30): no docstring
  - `_mapping(value)` (line 40): no docstring
  - `_parse_schema_registry_version(version_raw, prefix)` (line 46): no docstring
  - `resolve_inline_avro_schema(read_config)` (line 65): no docstring
  - `_extract_schema_registry_reference_parts(read_config)` (line 72): no docstring
  - `validate_kafka_avro_schema_config(read_config, node_id)` (line 110): no docstring
  - `resolve_kafka_avro_schema_registry_reference(read_config, node_id)` (line 140): no docstring
  - `parse_kafka_avro_schema_registry_response(payload)` (line 164): no docstring
  - `fetch_kafka_avro_schema_from_registry(reference, timeout_seconds, headers)` (line 176): no docstring
- **Classes**
  - `KafkaAvroSchemaRegistryReference` (line 11): no docstring
    - `cache_key(self)` (line 17): no docstring
    - `request_url(self)` (line 21): no docstring

### `backend/shared/services/pipeline/pipeline_lock.py`
- **Classes**
  - `PipelineLockError` (line 11): no docstring
  - `PipelineLock` (line 15): no docstring
    - `__init__(self, redis_client, key, token, ttl_seconds, renew_seconds)` (line 16): no docstring
    - `async start(self)` (line 33): no docstring
    - `raise_if_lost(self)` (line 40): no docstring
    - `async release(self)` (line 44): no docstring
    - `async _renew_loop(self)` (line 61): no docstring
    - `async _extend(self)` (line 77): no docstring

### `backend/shared/services/pipeline/pipeline_math_utils.py`
- **Functions**
  - `safe_ratio(numerator, denominator)` (line 4): no docstring

### `backend/shared/services/pipeline/pipeline_parameter_utils.py`
- **Functions**
  - `normalize_parameters(parameters_raw)` (line 6): no docstring
  - `apply_parameters(expression, parameters)` (line 22): no docstring

### `backend/shared/services/pipeline/pipeline_plan_builder.py`
- **Functions**
  - `_ensure_dict(value, name)` (line 31): no docstring
  - `_ensure_list(value, name)` (line 37): no docstring
  - `_ensure_str(value, name)` (line 45): no docstring
  - `_ensure_string_list(value, name)` (line 52): no docstring
  - `_looks_like_spark_expr(text)` (line 69): Detect obvious Spark SQL expressions passed where a *column name* is required.
  - `_is_sql_expression(key)` (line 110): Detect if a join key is a SQL expression rather than a simple column name.
  - `_definition(plan)` (line 144): no docstring
  - `update_settings(plan, set_fields, unset_fields, replace)` (line 157): Patch plan.definition_json.settings (merge by default, replace if requested).
  - `_node_ids(definition)` (line 182): no docstring
  - `_find_node(definition, node_id)` (line 192): no docstring
  - `_incoming_sources_in_order(definition, node_id)` (line 204): Return incoming edge sources for `node_id` in the order they appear in definition_json.edges.
  - `new_plan(goal, db_name, branch, dataset_ids)` (line 227): no docstring
  - `reset_plan(plan)` (line 252): Reset a plan's definition to empty while preserving goal + data_scope.
  - `add_input(plan, dataset_id, dataset_name, dataset_branch, read, node_id)` (line 280): no docstring
  - `add_external_input(plan, read, source_name, node_id)` (line 330): Add an input node that is NOT backed by a DatasetRegistry artifact.
  - `configure_input_read(plan, node_id, read, replace)` (line 371): Patch input node read configuration.
  - `add_transform(plan, operation, input_node_ids, metadata, node_id)` (line 405): no docstring
  - `add_join(plan, left_node_id, right_node_id, left_keys, right_keys, join_type, join_hints, broadcast_left, broadcast_right, node_id)` (line 452): no docstring
  - `add_filter(plan, input_node_id, expression, node_id)` (line 515): no docstring
  - `add_compute(plan, input_node_id, expression, node_id)` (line 527): no docstring
  - `add_udf(plan, input_node_id, udf_id, udf_version, node_id)` (line 539): no docstring
  - `add_compute_column(plan, input_node_id, target_column, formula, node_id)` (line 570): Add a compute transform that writes a single column.
  - `add_compute_assignments(plan, input_node_id, assignments, node_id)` (line 595): Add a compute transform that writes multiple columns (assignments).
  - `add_sort(plan, input_node_id, columns, node_id)` (line 624): Add a sort transform node.
  - `add_explode(plan, input_node_id, column, node_id)` (line 667): Add an explode transform node for an array/map-like column.
  - `add_union(plan, left_node_id, right_node_id, union_mode, node_id)` (line 686): Add a union transform node for two inputs.
  - `add_pivot(plan, input_node_id, index, columns, values, agg, node_id)` (line 718): Add a pivot transform node.
  - `add_cast(plan, input_node_id, casts, node_id)` (line 749): no docstring
  - `add_rename(plan, input_node_id, rename, node_id)` (line 778): no docstring
  - `add_select(plan, input_node_id, columns, node_id)` (line 804): no docstring
  - `add_select_expr(plan, input_node_id, expressions, node_id)` (line 828): Add a select transform using Spark SQL selectExpr-style expressions.
  - `add_drop(plan, input_node_id, columns, node_id)` (line 846): no docstring
  - `add_dedupe(plan, input_node_id, columns, node_id)` (line 864): no docstring
  - `add_group_by_expr(plan, input_node_id, group_by, aggregate_expressions, operation, node_id)` (line 882): Add a groupBy/aggregate node using Spark SQL aggregate expressions.
  - `add_window_expr(plan, input_node_id, expressions, node_id)` (line 931): Add a window transform that computes one or more Spark SQL window expressions.
  - `add_normalize(plan, input_node_id, columns, trim, empty_to_null, whitespace_to_null, lowercase, uppercase, node_id)` (line 960): no docstring
  - `add_regex_replace(plan, input_node_id, rules, node_id)` (line 990): no docstring
  - `add_split(plan, input_node_id, expression, true_node_id, false_node_id)` (line 1026): Macro expansion for split semantics.
  - `add_geospatial(plan, input_node_id, mode, config, node_id)` (line 1083): no docstring
  - `add_pattern_mining(plan, input_node_id, source_column, pattern, output_column, match_mode, node_id)` (line 1104): no docstring
  - `add_stream_join(plan, left_node_id, right_node_id, left_keys, right_keys, join_type, strategy, left_event_time_column, right_event_time_column, allowed_lateness_seconds, left_cache_expiration_seconds, right_cache_expiration_seconds, time_direction, stream_join_metadata, node_id)` (line 1137): no docstring
  - `add_output(plan, input_node_id, output_name, output_kind, node_id, output_metadata)` (line 1331): no docstring
  - `validate_structure(plan)` (line 1395): Lightweight structural validation for plan.definition_json.
  - `add_edge(plan, from_node_id, to_node_id)` (line 1610): Add a graph edge (idempotent).
  - `delete_edge(plan, from_node_id, to_node_id)` (line 1640): Delete all matching edges from->to (no-op if not found).
  - `set_node_inputs(plan, node_id, input_node_ids)` (line 1668): Replace all incoming edges to node_id with input_node_ids (in order).
  - `update_node_metadata(plan, node_id, set_fields, unset_fields, replace)` (line 1706): Patch node.metadata (merge by default, replace if requested).
  - `delete_node(plan, node_id)` (line 1753): Delete a node and any incident edges; also removes outputs[] entry for output nodes.
  - `update_output(plan, output_name, set_fields, unset_fields, replace)` (line 1787): Patch an outputs[] entry by output_name; keeps output node metadata.outputName in sync if renamed.
- **Classes**
  - `PipelinePlanBuilderError` (line 20): no docstring
  - `PlanMutation` (line 25): no docstring

### `backend/shared/services/pipeline/pipeline_preflight_utils.py`
- **Functions**
  - `_normalize_column_list(raw)` (line 45): no docstring
  - `_extract_schema_columns(schema)` (line 62): no docstring
  - `_extract_sample_rows(sample)` (line 76): no docstring
  - `_output_metadata_text(payload, *keys)` (line 131): no docstring
  - `_infer_types_from_rows(rows, columns)` (line 142): no docstring
  - `_merge_types(base, extra)` (line 152): no docstring
  - `_schema_for_input(dataset, version)` (line 163): no docstring
  - `_schema_for_external_input_read(read_config)` (line 188): no docstring
  - `_apply_select(schema, columns)` (line 216): no docstring
  - `_apply_drop(schema, columns)` (line 222): no docstring
  - `_apply_rename(schema, rename_map)` (line 229): no docstring
  - `_apply_cast(schema, casts)` (line 241): no docstring
  - `_apply_compute(schema, metadata)` (line 253): Compute transforms can be represented in a few deterministic forms:
  - `_apply_geospatial(schema, metadata)` (line 315): no docstring
  - `_apply_pattern_mining(schema, metadata)` (line 337): no docstring
  - `_apply_group_by(schema, group_by, aggregates)` (line 356): no docstring
  - `_parse_sql_alias(expr)` (line 380): Best-effort alias extraction for Spark SQL expressions.
  - `_apply_group_by_expr(schema, group_by, expressions)` (line 400): Schema propagation for Spark `groupBy(...).agg(expr(...).alias(...))` via `aggregateExpressions`.
  - `_apply_window(schema, metadata)` (line 440): Window transforms can either be declarative metadata (partition/order + row_number)
  - `_apply_join(left, right, left_keys, right_keys)` (line 483): Apply the deterministic join output naming policy.
  - `_apply_select_expr(schema, expressions)` (line 540): Best-effort schema propagation for Spark `selectExpr`.
  - `_apply_union(left, right, mode)` (line 641): no docstring
  - `_schema_empty(schema)` (line 667): no docstring
  - `_column_type(schema, column)` (line 671): no docstring
  - `async compute_pipeline_preflight(definition, db_name, dataset_registry, branch)` (line 684): no docstring
  - `async compute_schema_by_node(definition, db_name, dataset_registry, branch)` (line 1686): no docstring
- **Classes**
  - `SchemaInfo` (line 39): no docstring

### `backend/shared/services/pipeline/pipeline_preview_inspector.py`
- **Functions**
  - `_iter_values(rows, column)` (line 48): no docstring
  - `_is_bool(value)` (line 64): no docstring
  - `_is_int(value)` (line 68): no docstring
  - `_is_decimal(value)` (line 72): no docstring
  - `_is_datetime(value)` (line 76): no docstring
  - `_looks_like_id(name)` (line 80): no docstring
  - `_domain_hint(name)` (line 93): no docstring
  - `_should_preserve_whitespace(column_name)` (line 106): Enterprise Enhancement (2026-01):
  - `_is_event_or_log_table(table_name, column_names)` (line 118): Enterprise Enhancement (2026-01):
  - `_row_key(row, columns)` (line 153): no docstring
  - `_case_variation_ratio(values)` (line 157): no docstring
  - `_parseability(values)` (line 169): no docstring
  - `_column_type_map(columns)` (line 186): no docstring
  - `inspect_preview(preview, table_name)` (line 201): Analyze preview data and suggest cleansing operations.

### `backend/shared/services/pipeline/pipeline_preview_policy.py`
- **Functions**
  - `_level_from_issues(issues)` (line 45): no docstring
  - `_count_comparison_ops(expression)` (line 67): no docstring
  - `_is_simple_filter_expression(expression)` (line 71): no docstring
  - `_is_safe_python_ast(tree)` (line 112): no docstring
  - `_is_timestamp_literal_arg(text)` (line 123): no docstring
  - `_is_preview_safe_compute_expression(expression)` (line 134): no docstring
  - `_select_expr_item_requires_spark(expr)` (line 158): no docstring
  - `evaluate_preview_policy(definition_json)` (line 181): Enterprise hard-gating for plan_preview.
- **Classes**
  - `PreviewPolicyIssue` (line 17): no docstring
    - `to_dict(self)` (line 24): no docstring

### `backend/shared/services/pipeline/pipeline_profiler.py`
- **Functions**
  - `_safe_stringify(value)` (line 16): no docstring
  - `_coerce_float(value)` (line 30): no docstring
  - `_compute_histogram(values, bins)` (line 49): no docstring
  - `_normalize_columns(columns)` (line 82): no docstring
  - `compute_column_stats(rows, columns, max_top_values)` (line 96): Returns a dict payload that is safe to JSON serialize:

### `backend/shared/services/pipeline/pipeline_scheduler.py`
- **Functions**
  - `_should_run_schedule(now, last_run, interval, cron)` (line 391): no docstring
  - `_cron_matches(now, expression)` (line 408): no docstring
  - `_cron_field_matches(field, value)` (line 423): no docstring
  - `_normalize_dependencies(raw)` (line 457): no docstring
  - `async _dependencies_satisfied(registry, dependencies)` (line 461): no docstring
  - `async _evaluate_dependencies(registry, dependencies)` (line 469): no docstring
  - `_is_valid_cron_expression(expression)` (line 504): no docstring
  - `_is_valid_cron_field(field)` (line 511): no docstring
- **Classes**
  - `ScheduledPipeline` (line 28): no docstring
  - `DependencyEvaluation` (line 37): no docstring
  - `PipelineScheduler` (line 43): no docstring
    - `__init__(self, registry, queue, poll_seconds, tracing, metrics)` (line 44): no docstring
    - `async run(self)` (line 60): no docstring
    - `async stop(self)` (line 92): no docstring
    - `async _tick(self)` (line 95): no docstring
    - `async _record_scheduler_config_error(self, pipeline_id, now, error_key, detail, extra)` (line 300): no docstring
    - `async _record_scheduler_ignored(self, pipeline_id, now, reason, detail, extra)` (line 344): no docstring

### `backend/shared/services/pipeline/pipeline_schema_casts.py`
- **Functions**
  - `extract_schema_casts(schema_json)` (line 8): no docstring

### `backend/shared/services/pipeline/pipeline_schema_utils.py`
- **Functions**
  - `normalize_schema_type(value)` (line 28): no docstring
  - `normalize_schema_checks(checks)` (line 70): no docstring
  - `normalize_expectations(expectations)` (line 86): no docstring
  - `normalize_schema_contract(contract)` (line 107): no docstring
  - `normalize_value_list(value)` (line 124): no docstring
  - `normalize_number(value)` (line 134): no docstring
- **Classes**
  - `SchemaCheckSpec` (line 8): no docstring
  - `ExpectationSpec` (line 15): no docstring
  - `SchemaContractSpec` (line 22): no docstring

### `backend/shared/services/pipeline/pipeline_task_spec_policy.py`
- **Functions**
  - `clamp_task_spec(spec, dataset_count)` (line 30): Clamp a TaskSpec to safe defaults.
  - `normalize_task_spec(raw, dataset_count)` (line 54): no docstring
  - `_iter_transform_ops(definition_json)` (line 65): no docstring
  - `validate_plan_against_task_spec(plan, task_spec)` (line 84): Validate a plan against TaskSpec policy (overreach guardrails).

### `backend/shared/services/pipeline/pipeline_transform_spec.py`
- **Functions**
  - `normalize_operation(value)` (line 57): no docstring
  - `resolve_join_spec(metadata)` (line 61): no docstring
  - `resolve_stream_join_spec(metadata)` (line 102): no docstring
  - `resolve_stream_join_effective_join_type(strategy, requested_join_type)` (line 186): no docstring
  - `resolve_input_read_mode(node)` (line 198): no docstring
  - `resolve_input_read_format(node)` (line 211): no docstring
  - `is_stream_like_input_node(node)` (line 224): no docstring
  - `normalize_union_mode(metadata)` (line 234): no docstring
- **Classes**
  - `JoinSpec` (line 37): no docstring
  - `StreamJoinSpec` (line 47): no docstring

### `backend/shared/services/pipeline/pipeline_type_inference.py`
- **Functions**
  - `_is_bool(value)` (line 24): no docstring
  - `_is_datetime(value)` (line 28): no docstring
  - `_is_int(value)` (line 32): no docstring
  - `_is_decimal(value)` (line 36): no docstring
  - `_sample_non_null(values, max_samples)` (line 40): no docstring
  - `infer_xsd_type_with_confidence(values, max_samples)` (line 62): Best-effort inference with a confidence score based on parse rates.
  - `common_join_key_type(left, right)` (line 107): Pick a safe common type for join keys.
  - `normalize_declared_type(value)` (line 122): no docstring
- **Classes**
  - `TypeInferenceResult` (line 55): no docstring

### `backend/shared/services/pipeline/pipeline_type_utils.py`
- **Functions**
  - `_normalize_numeric_text(text)` (line 65): Normalize numeric text for parsing. Returns (cleaned_string, is_negative).
  - `parse_decimal_text_with_ambiguity(text)` (line 114): Enterprise Enhancement (2026-01):
  - `parse_int_text(text)` (line 224): no docstring
  - `parse_decimal_text(text)` (line 235): no docstring
  - `parse_datetime_text(text, allow_ambiguous)` (line 246): Parse datetime text, returning the primary interpretation.
  - `parse_datetime_text_with_ambiguity(text, allow_ambiguous)` (line 252): Enterprise Enhancement (2026-01):
  - `infer_xsd_type_from_values(values)` (line 374): no docstring
  - `normalize_cast_target(target)` (line 397): no docstring
  - `normalize_cast_mode(value)` (line 401): no docstring
  - `xsd_to_spark_type(value)` (line 408): no docstring
  - `spark_type_to_xsd(data_type)` (line 413): no docstring
- **Classes**
  - `NumericParseResult` (line 26): Enterprise Enhancement (2026-01):
  - `DateParseResult` (line 46): Enterprise Enhancement (2026-01):

### `backend/shared/services/pipeline/pipeline_udf_runtime.py`
- **Functions**
  - `_validate_udf_ast(tree)` (line 101): no docstring
  - `_code_hash(code)` (line 124): no docstring
  - `build_udf_cache_key(udf_id, version, code)` (line 128): no docstring
  - `_parse_udf_version(value)` (line 132): no docstring
  - `async resolve_udf_reference(metadata, pipeline_registry, require_reference, require_version_pinning, code_cache)` (line 147): no docstring
  - `validate_udf_output_schema(payload)` (line 202): no docstring
  - `normalize_udf_output_rows(payload)` (line 214): no docstring
  - `compile_udf(code)` (line 229): no docstring
  - `_normalize_udf_source(code)` (line 233): no docstring
  - `compile_row_udf(code)` (line 248): Compile a Python UDF for row-level transforms.
- **Classes**
  - `PipelineUdfError` (line 9): no docstring
  - `PipelineUdfRegistry` (line 13): no docstring
    - `async get_udf_latest_version(self, udf_id)` (line 14): no docstring
    - `async get_udf_version(self, udf_id, version)` (line 16): no docstring
  - `ResolvedPipelineUdf` (line 20): no docstring
  - `_UdfAstValidator` (line 86): no docstring
    - `generic_visit(self, node)` (line 87): no docstring
    - `visit_Attribute(self, node)` (line 93): no docstring

### `backend/shared/services/pipeline/pipeline_unit_test_runner.py`
- **Functions**
  - `_stable_row_key(row)` (line 20): no docstring
  - `_normalize_columns(spec)` (line 24): no docstring
  - `_normalize_rows(spec, columns)` (line 45): no docstring
  - `_table_from_spec(spec)` (line 60): no docstring
  - `_diff_tables(actual, expected, max_rows)` (line 66): no docstring
  - `_select_table(result, node_id)` (line 112): no docstring
  - `async run_unit_tests(executor, definition, db_name, unit_tests)` (line 127): no docstring
- **Classes**
  - `PipelineUnitTestResult` (line 13): no docstring

### `backend/shared/services/pipeline/pipeline_validation_utils.py`
- **Functions**
  - `_format_error(prefix, message)` (line 31): no docstring
  - `validate_schema_checks(ops, checks, error_prefix)` (line 35): no docstring
  - `validate_expectations(ops, expectations)` (line 91): no docstring
  - `validate_schema_contract(ops, contract)` (line 158): no docstring
- **Classes**
  - `TableOps` (line 19): no docstring

### `backend/shared/services/pipeline/pipeline_value_predicates.py`
- **Functions**
  - `is_bool_like(value)` (line 14): no docstring
  - `is_int_like(value)` (line 22): no docstring
  - `is_decimal_like(value, include_int)` (line 32): no docstring
  - `is_datetime_like(value, iso_only, allow_ambiguous)` (line 44): no docstring

### `backend/shared/services/registries/__init__.py`

### `backend/shared/services/registries/action_log_registry.py`
- **Functions**
  - `_coerce_dt(value)` (line 57): no docstring
  - `_jsonb_to_dict(value)` (line 65): no docstring
  - `_jsonb_to_optional_dict(value)` (line 82): no docstring
  - `_row_to_record(row)` (line 99): no docstring
- **Classes**
  - `ActionLogStatus` (line 26): no docstring
  - `ActionLogRecord` (line 35): no docstring
  - `ActionLogRegistry` (line 125): Postgres-backed Action log registry.
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 134): no docstring
    - `_jsonb_param(value)` (line 154): asyncpg expects JSON/JSONB bind params as strings by default.
    - `async _ensure_tables(self, conn)` (line 170): no docstring
    - `async create_log(self, action_log_id, db_name, action_type_id, action_type_rid, resource_rid, ontology_commit_id, input_payload, correlation_id, submitted_by, writeback_target, metadata)` (line 234): no docstring
    - `async get_log(self, action_log_id)` (line 290): no docstring
    - `async list_logs(self, db_name, statuses, action_type_id, submitted_by, limit, offset)` (line 300): no docstring
    - `async list_outbox_candidates(self, limit, statuses, skip_locked)` (line 348): List action log records that need outbox processing.
    - `async release_outbox_lock(self, action_log_id)` (line 401): Release the advisory lock for a given action_log_id after processing.
    - `async mark_commit_written(self, action_log_id, writeback_commit_id, result)` (line 411): no docstring
    - `async mark_event_emitted(self, action_log_id, action_applied_event_id, action_applied_seq)` (line 438): no docstring
    - `async mark_succeeded(self, action_log_id, result, finished_at)` (line 465): no docstring
    - `async mark_failed(self, action_log_id, result, finished_at)` (line 495): no docstring

### `backend/shared/services/registries/action_simulation_registry.py`
- **Classes**
  - `ActionSimulationRecord` (line 23): no docstring
  - `ActionSimulationVersionRecord` (line 36): no docstring
  - `ActionSimulationRegistry` (line 55): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 56): no docstring
    - `async connect(self)` (line 70): no docstring
    - `async initialize(self)` (line 81): no docstring
    - `async close(self)` (line 84): no docstring
    - `async shutdown(self)` (line 89): no docstring
    - `async ensure_schema(self)` (line 92): no docstring
    - `_row_to_simulation(self, row)` (line 156): no docstring
    - `_row_to_version(self, row)` (line 169): no docstring
    - `async create_simulation(self, simulation_id, db_name, action_type_id, created_by, created_by_type, title, description)` (line 193): no docstring
    - `async get_simulation(self, simulation_id)` (line 227): no docstring
    - `async list_simulations(self, db_name, action_type_id, limit, offset)` (line 244): no docstring
    - `async next_version(self, simulation_id)` (line 288): no docstring
    - `async create_version(self, simulation_id, version, status, base_branch, overlay_branch, ontology_commit_id, action_type_rid, preview_action_log_id, input_payload, assumptions, scenarios, result, error, created_by, created_by_type)` (line 299): no docstring
    - `async list_versions(self, simulation_id, limit, offset)` (line 367): no docstring
    - `async get_version(self, simulation_id, version)` (line 390): no docstring

### `backend/shared/services/registries/agent_function_registry.py`
- **Classes**
  - `AgentFunctionRecord` (line 26): no docstring
  - `AgentFunctionRegistry` (line 40): no docstring
    - `async _ensure_tables(self, conn)` (line 41): no docstring
    - `_row_to_record(self, row)` (line 67): no docstring
    - `async upsert_function(self, function_id, version, status, handler, tags, roles, input_schema, output_schema, metadata)` (line 82): no docstring
    - `async get_function(self, function_id, version, status)` (line 142): no docstring
    - `async list_functions(self, function_id, status, limit, offset)` (line 173): no docstring

### `backend/shared/services/registries/agent_model_registry.py`
- **Classes**
  - `AgentModelRecord` (line 22): no docstring
  - `AgentModelRegistry` (line 38): no docstring
    - `async _ensure_tables(self, conn)` (line 39): no docstring
    - `_row_to_model(self, row)` (line 72): no docstring
    - `async upsert_model(self, model_id, provider, display_name, status, supports_json_mode, supports_native_tool_calling, max_context_tokens, max_output_tokens, prompt_per_1k, completion_per_1k, metadata)` (line 89): no docstring
    - `async get_model(self, model_id)` (line 160): no docstring
    - `async list_models(self, status, provider, limit, offset)` (line 181): no docstring

### `backend/shared/services/registries/agent_policy_registry.py`
- **Classes**
  - `AgentTenantPolicyRecord` (line 23): no docstring
  - `AgentPolicyRegistry` (line 34): no docstring
    - `async _ensure_tables(self, conn)` (line 35): no docstring
    - `_row_to_policy(self, row)` (line 54): no docstring
    - `async upsert_tenant_policy(self, tenant_id, allowed_models, allowed_tools, default_model, auto_approve_rules, data_policies)` (line 66): no docstring
    - `async get_tenant_policy(self, tenant_id)` (line 114): no docstring
    - `async list_tenant_policies(self, limit, offset)` (line 130): no docstring

### `backend/shared/services/registries/agent_registry.py`
- **Classes**
  - `AgentRunRecord` (line 21): no docstring
  - `AgentStepRecord` (line 38): no docstring
  - `AgentApprovalRecord` (line 57): no docstring
  - `AgentApprovalRequestRecord` (line 71): no docstring
  - `AgentToolIdempotencyRecord` (line 92): no docstring
  - `AgentRegistry` (line 107): no docstring
    - `async _ensure_tables(self, conn)` (line 108): no docstring
    - `_row_to_run(self, row)` (line 257): no docstring
    - `_row_to_step(self, row)` (line 274): no docstring
    - `_row_to_approval(self, row)` (line 293): no docstring
    - `_row_to_approval_request(self, row)` (line 307): no docstring
    - `_row_to_tool_idempotency(self, row)` (line 328): no docstring
    - `async create_run(self, run_id, tenant_id, plan_id, status, risk_level, requester, delegated_actor, context, plan_snapshot, started_at)` (line 344): no docstring
    - `async update_run_status(self, run_id, tenant_id, status, finished_at)` (line 387): no docstring
    - `async get_run(self, run_id, tenant_id)` (line 416): no docstring
    - `async list_runs(self, tenant_id, plan_id, status, limit)` (line 433): no docstring
    - `async create_step(self, run_id, step_id, tenant_id, tool_id, status, command_id, task_id, input_digest, output_digest, error, started_at, metadata)` (line 468): no docstring
    - `async update_step_status(self, run_id, step_id, tenant_id, status, output_digest, error, finished_at)` (line 514): no docstring
    - `async list_steps(self, run_id, tenant_id)` (line 551): no docstring
    - `async create_approval(self, approval_id, plan_id, tenant_id, step_id, decision, approved_by, approved_at, comment, metadata)` (line 569): no docstring
    - `async list_approvals(self, plan_id, tenant_id)` (line 608): no docstring
    - `async create_approval_request(self, approval_request_id, plan_id, tenant_id, session_id, job_id, status, risk_level, requested_by, requested_at, request_payload, metadata)` (line 626): no docstring
    - `async get_approval_request(self, approval_request_id, tenant_id)` (line 679): no docstring
    - `async list_approval_requests(self, tenant_id, session_id, job_id, plan_id, status, limit, offset)` (line 700): no docstring
    - `async decide_approval_request(self, approval_request_id, tenant_id, decision, decided_by, decided_at, comment, status, metadata)` (line 747): no docstring
    - `async get_tool_idempotency(self, tenant_id, idempotency_key)` (line 796): no docstring
    - `async begin_tool_idempotency(self, tenant_id, idempotency_key, tool_id, request_digest, started_at)` (line 822): Create or fetch a tool idempotency record.
    - `async finalize_tool_idempotency(self, tenant_id, idempotency_key, tool_id, request_digest, response_status, response_body, error, finished_at)` (line 878): no docstring

### `backend/shared/services/registries/agent_session_registry.py`
- **Functions**
  - `_wrap_json_object(value)` (line 26): no docstring
  - `_get_encryptor()` (line 104): no docstring
  - `_aad_for_session(session_id)` (line 115): no docstring
  - `validate_session_status_transition(current_status, next_status)` (line 119): no docstring
- **Classes**
  - `AgentSessionRecord` (line 132): no docstring
  - `AgentSessionMessageRecord` (line 148): no docstring
  - `AgentSessionJobRecord` (line 166): no docstring
  - `AgentSessionContextItemRecord` (line 180): no docstring
  - `AgentSessionEventRecord` (line 193): no docstring
  - `AgentSessionCIResultRecord` (line 206): no docstring
  - `AgentSessionToolCallRecord` (line 223): no docstring
  - `AgentSessionLLMCallRecord` (line 255): no docstring
  - `AgentSessionLLMUsageAggregateRecord` (line 276): no docstring
  - `AgentSessionRegistry` (line 289): no docstring
    - `async _ensure_tables(self, conn)` (line 290): no docstring
    - `_row_to_session(self, row)` (line 543): no docstring
    - `_row_to_message(self, row)` (line 562): no docstring
    - `_row_to_job(self, row)` (line 591): no docstring
    - `_row_to_context_item(self, row)` (line 605): no docstring
    - `_row_to_event(self, row)` (line 618): no docstring
    - `_row_to_ci_result(self, row)` (line 631): no docstring
    - `_row_to_tool_call(self, row)` (line 650): no docstring
    - `_row_to_llm_call(self, row)` (line 702): no docstring
    - `async create_session(self, session_id, tenant_id, created_by, status, selected_model, enabled_tools, metadata, started_at)` (line 723): no docstring
    - `async get_session(self, session_id, tenant_id)` (line 763): no docstring
    - `async list_sessions(self, tenant_id, created_by, status, limit, offset)` (line 779): no docstring
    - `async update_session(self, session_id, tenant_id, status, selected_model, enabled_tools, summary, metadata, terminated_at)` (line 815): no docstring
    - `async add_message(self, message_id, session_id, tenant_id, role, content, content_digest, token_count, cost_estimate, latency_ms, metadata, created_at)` (line 870): no docstring
    - `async list_messages(self, session_id, tenant_id, limit, offset, include_removed)` (line 924): no docstring
    - `async list_recent_messages(self, session_id, tenant_id, limit, include_removed)` (line 961): no docstring
    - `async get_messages_by_ids(self, session_id, tenant_id, message_ids, include_removed)` (line 996): no docstring
    - `async mark_messages_removed(self, session_id, tenant_id, message_ids, removed_by, removed_reason, removed_at, placeholder)` (line 1036): no docstring
    - `async create_job(self, job_id, session_id, tenant_id, plan_id, run_id, status, error, metadata, created_at)` (line 1086): no docstring
    - `async update_job(self, job_id, tenant_id, status, run_id, error, metadata, finished_at)` (line 1154): no docstring
    - `async get_job(self, job_id, tenant_id)` (line 1196): no docstring
    - `async list_jobs(self, session_id, tenant_id, limit, offset)` (line 1215): no docstring
    - `async add_context_item(self, item_id, session_id, tenant_id, item_type, include_mode, ref, token_count, metadata, created_at)` (line 1245): no docstring
    - `async list_context_items(self, session_id, tenant_id, limit, offset)` (line 1291): no docstring
    - `async remove_context_item(self, session_id, tenant_id, item_id)` (line 1320): no docstring
    - `async append_event(self, event_id, session_id, tenant_id, event_type, data, occurred_at, trace_id, correlation_id, created_at)` (line 1353): no docstring
    - `async list_session_events(self, session_id, tenant_id, limit, after)` (line 1396): no docstring
    - `async start_tool_call(self, tool_run_id, session_id, tenant_id, tool_id, method, path, query, request_body, request_digest, request_token_count, idempotency_key, job_id, plan_id, run_id, step_id, started_at)` (line 1429): no docstring
    - `async finish_tool_call(self, tool_run_id, tenant_id, status, response_status, response_body, response_digest, response_token_count, error_code, error_message, side_effect_summary, latency_ms, finished_at)` (line 1517): no docstring
    - `async list_tool_calls(self, session_id, tenant_id, limit, offset)` (line 1593): no docstring
    - `async record_llm_call(self, llm_call_id, session_id, tenant_id, call_type, provider, model_id, cache_hit, latency_ms, prompt_tokens, completion_tokens, total_tokens, cost_estimate, input_digest, output_digest, job_id, plan_id, created_at)` (line 1627): no docstring
    - `async list_llm_calls(self, session_id, tenant_id, limit, offset)` (line 1693): no docstring
    - `async record_ci_result(self, ci_result_id, session_id, tenant_id, job_id, plan_id, run_id, provider, status, details_url, summary, checks, raw, created_at)` (line 1726): no docstring
    - `async list_ci_results(self, session_id, tenant_id, limit, offset)` (line 1788): no docstring
    - `async list_expired_file_uploads(self, cutoff, tenant_id, limit)` (line 1823): Return `file_upload` context items older than `cutoff` that still contain bucket/key refs.
    - `async aggregate_llm_usage(self, tenant_id, group_by, created_by, start_time, end_time, limit, offset)` (line 1888): Aggregate LLM usage/cost across sessions for a tenant (OBS-005).
    - `async apply_retention(self, cutoff, tenant_id, action, message_placeholder, removed_by, removed_reason, include_messages, include_tool_calls, include_context_items, include_ci_results, include_events, include_llm_calls, context_item_type, exclude_context_item_types)` (line 1996): Apply retention to agent session data (SEC-005).

### `backend/shared/services/registries/agent_tool_registry.py`
- **Classes**
  - `AgentToolPolicyRecord` (line 20): no docstring
  - `AgentToolRegistry` (line 42): no docstring
    - `async _ensure_tables(self, conn)` (line 43): no docstring
    - `_row_to_policy(self, row)` (line 103): no docstring
    - `async upsert_tool_policy(self, tool_id, method, path, risk_level, requires_approval, requires_idempotency_key, status, roles, max_payload_bytes, version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata)` (line 126): no docstring
    - `async get_tool_policy(self, tool_id)` (line 207): no docstring
    - `async list_tool_policies(self, status, limit)` (line 223): no docstring

### `backend/shared/services/registries/backing_source_adapter.py`
- **Functions**
  - `_normalize_oms_response(payload)` (line 53): OMS responses may be wrapped in {"data": {...}} or {"response": {...}}.
  - `async get_mapping_from_oms(http_client, oms_base_url, db_name, target_class_id, branch, admin_token)` (line 63): Fetch an object_type resource from OMS and extract backing_source mapping spec.
  - `async find_class_id_by_dataset(http_client, oms_base_url, db_name, dataset_id, branch, admin_token)` (line 127): Reverse lookup: find the object_type whose backing_source.dataset_id matches.
  - `is_oms_mapping_spec(mapping_spec_id)` (line 312): Check if a mapping_spec_id originates from OMS backing_source.
  - `extract_class_id_from_oms_spec_id(mapping_spec_id)` (line 317): Extract class_id from an OMS mapping_spec_id like 'oms:Customer'.
- **Classes**
  - `BackingSourceMappingSpec` (line 32): OMS backing_source에서 추출된 mapping spec (PostgreSQL MappingSpecRecord과 호환).
  - `MappingSpecResolver` (line 176): OMS-first, PostgreSQL-fallback mapping spec resolver.
    - `__init__(self, http_client, oms_base_url, admin_token, objectify_registry)` (line 191): no docstring
    - `async resolve(self, db_name, dataset_id, branch, schema_hash, target_class_id, artifact_output_name)` (line 204): Resolve mapping spec with OMS priority:
    - `async resolve_by_class_id(self, db_name, target_class_id, branch)` (line 294): Convenience: resolve directly by class_id (OMS-only, no PG fallback).

### `backend/shared/services/registries/changelog_store.py`
- **Classes**
  - `ChangelogStore` (line 19): CRUD operations for objectify_changelog table.
    - `__init__(self, pool)` (line 22): pool: asyncpg connection pool.
    - `async record_changelog(self, job_id, db_name, branch, target_class_id, execution_mode, mapping_spec_id, added_count, modified_count, deleted_count, total_instances, delta_summary, delta_s3_key, lakefs_base_commit, lakefs_target_commit, watermark_before, watermark_after, duration_ms)` (line 26): Record a changelog entry for an objectify job execution.
    - `async list_changelogs(self, db_name, branch, target_class_id, limit, offset)` (line 106): List recent changelogs for a database.
    - `async get_changelog(self, changelog_id)` (line 150): Get a single changelog entry by ID.

### `backend/shared/services/registries/connector_registry.py`
- **Functions**
  - `_parse_json_list_of_dicts(value)` (line 31): no docstring
- **Classes**
  - `ConnectorSource` (line 37): no docstring
  - `ConnectorMapping` (line 47): no docstring
  - `SyncState` (line 62): no docstring
  - `OutboxItem` (line 79): no docstring
  - `ConnectorRegistry` (line 93): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 96): no docstring
    - `async _ensure_tables(self, conn)` (line 115): no docstring
    - `async upsert_source(self, source_type, source_id, config_json, enabled)` (line 211): no docstring
    - `async set_source_enabled(self, source_type, source_id, enabled)` (line 258): no docstring
    - `async get_source(self, source_type, source_id)` (line 276): no docstring
    - `async list_sources(self, source_type, enabled, limit)` (line 302): no docstring
    - `_deterministic_mapping_id(self, source_type, source_id)` (line 341): no docstring
    - `async upsert_mapping(self, source_type, source_id, enabled, status, target_db_name, target_branch, target_class_label, field_mappings)` (line 346): no docstring
    - `async get_mapping(self, source_type, source_id)` (line 418): no docstring
    - `async record_poll_result(self, source_type, source_id, current_cursor, kafka_topic)` (line 454): Record a poll result, and enqueue a connector update event when the cursor changed.
    - `async claim_outbox_batch(self, limit)` (line 579): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 631): no docstring
    - `async mark_outbox_failed(self, outbox_id, error)` (line 646): no docstring
    - `async record_sync_outcome(self, source_type, source_id, success, command_id, error, next_retry_at, rate_limit_until)` (line 664): no docstring
    - `async get_sync_state(self, source_type, source_id)` (line 734): no docstring

### `backend/shared/services/registries/dataset_profile_registry.py`
- **Classes**
  - `DatasetProfileRecord` (line 25): no docstring
  - `DatasetProfileRegistry` (line 39): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 40): no docstring
    - `async _ensure_tables(self, conn)` (line 59): no docstring
    - `_row_to_profile(self, row)` (line 96): no docstring
    - `async upsert_profile(self, dataset_id, dataset_version_id, db_name, branch, schema_hash, profile)` (line 111): no docstring
    - `async get_latest_profile(self, dataset_id, dataset_version_id)` (line 160): no docstring

### `backend/shared/services/registries/dataset_registry.py`
- **Functions**
  - `_inject_dataset_version(outbox_entries, dataset_version_id)` (line 270): Ensure dataset_version_id is propagated into outbox payloads that depend on it.
- **Classes**
  - `DatasetRecord` (line 28): no docstring
  - `DatasetVersionRecord` (line 42): no docstring
  - `DatasetIngestRequestRecord` (line 55): no docstring
  - `DatasetIngestTransactionRecord` (line 79): no docstring
  - `DatasetIngestOutboxItem` (line 93): no docstring
  - `BackingDatasourceRecord` (line 111): no docstring
  - `BackingDatasourceVersionRecord` (line 126): no docstring
  - `KeySpecRecord` (line 138): no docstring
  - `GatePolicyRecord` (line 149): no docstring
  - `GateResultRecord` (line 161): no docstring
  - `AccessPolicyRecord` (line 173): no docstring
  - `InstanceEditRecord` (line 186): no docstring
  - `RelationshipSpecRecord` (line 199): no docstring
  - `RelationshipIndexResultRecord` (line 225): no docstring
  - `LinkEditRecord` (line 242): no docstring
  - `SchemaMigrationPlanRecord` (line 259): no docstring
  - `DatasetRegistry` (line 304): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 305): no docstring
    - `_row_to_backing(row)` (line 325): no docstring
    - `_row_to_backing_version(row)` (line 341): no docstring
    - `_row_to_key_spec(row)` (line 354): no docstring
    - `_row_to_gate_policy(row)` (line 366): no docstring
    - `_row_to_gate_result(row)` (line 379): no docstring
    - `_row_to_access_policy(row)` (line 392): no docstring
    - `_row_to_instance_edit(row)` (line 406): no docstring
    - `_row_to_relationship_spec(row)` (line 423): no docstring
    - `_row_to_relationship_index_result(row)` (line 456): no docstring
    - `_row_to_link_edit(row)` (line 474): no docstring
    - `_row_to_schema_migration_plan(row)` (line 492): no docstring
    - `async _ensure_tables(self, conn)` (line 504): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch, dataset_id)` (line 1191): no docstring
    - `async list_datasets(self, db_name, branch)` (line 1243): no docstring
    - `async count_datasets_by_db_names(self, db_names, branch)` (line 1298): no docstring
    - `async get_dataset(self, dataset_id)` (line 1330): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 1358): no docstring
    - `async get_dataset_by_source_ref(self, db_name, source_type, source_ref, branch)` (line 1394): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, version_id, ingest_request_id, promoted_from_artifact_id)` (line 1432): no docstring
    - `async get_latest_version(self, dataset_id)` (line 1526): no docstring
    - `async get_version(self, version_id)` (line 1557): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 1586): no docstring
    - `async create_backing_datasource(self, dataset_id, db_name, name, branch, description, source_type, source_ref, backing_id)` (line 1619): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 1657): no docstring
    - `async get_backing_datasource_by_dataset(self, dataset_id, branch)` (line 1674): no docstring
    - `async list_backing_datasources(self, db_name, branch, limit)` (line 1700): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 1726): no docstring
    - `async create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 1749): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 1795): no docstring
    - `async get_backing_datasource_version_by_dataset_version(self, dataset_version_id)` (line 1816): no docstring
    - `async list_backing_datasource_versions(self, backing_id, limit)` (line 1839): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 1862): no docstring
    - `async create_key_spec(self, dataset_id, spec, dataset_version_id, status, key_spec_id)` (line 1882): no docstring
    - `async get_key_spec(self, key_spec_id)` (line 1913): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 1929): no docstring
    - `async list_key_specs(self, dataset_id, limit)` (line 1970): no docstring
    - `async upsert_gate_policy(self, scope, name, description, rules, status)` (line 1992): no docstring
    - `async get_gate_policy(self, scope, name)` (line 2028): no docstring
    - `async list_gate_policies(self, scope, limit)` (line 2050): no docstring
    - `async record_gate_result(self, scope, subject_type, subject_id, status, details, policy_name)` (line 2072): no docstring
    - `async list_gate_results(self, scope, subject_type, subject_id, limit)` (line 2114): no docstring
    - `async upsert_access_policy(self, db_name, scope, subject_type, subject_id, policy, status)` (line 2142): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id, status)` (line 2179): no docstring
    - `async list_access_policies(self, db_name, scope, subject_type, subject_id, status, limit)` (line 2208): no docstring
    - `async record_instance_edit(self, db_name, class_id, instance_id, edit_type, metadata, status, fields)` (line 2242): no docstring
    - `async count_instance_edits(self, db_name, class_id, status)` (line 2282): no docstring
    - `async list_instance_edits(self, db_name, class_id, instance_id, status, limit)` (line 2305): no docstring
    - `async clear_instance_edits(self, db_name, class_id)` (line 2336): no docstring
    - `async remap_instance_edits(self, db_name, class_id, id_map, status)` (line 2359): no docstring
    - `async get_instance_edit_field_stats(self, db_name, class_id, fields, status)` (line 2397): no docstring
    - `async apply_instance_edit_field_moves(self, db_name, class_id, field_moves, status)` (line 2471): no docstring
    - `async update_instance_edit_status_by_fields(self, db_name, class_id, fields, new_status, status, metadata_note)` (line 2535): no docstring
    - `async create_relationship_spec(self, link_type_id, db_name, source_object_type, target_object_type, predicate, spec_type, dataset_id, mapping_spec_id, mapping_spec_version, spec, dataset_version_id, status, auto_sync, relationship_spec_id)` (line 2592): no docstring
    - `async update_relationship_spec(self, relationship_spec_id, status, spec, auto_sync, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version)` (line 2647): no docstring
    - `async record_relationship_index_result(self, relationship_spec_id, status, stats, errors, dataset_version_id, mapping_spec_version, lineage, indexed_at)` (line 2705): no docstring
    - `async get_relationship_spec(self, relationship_spec_id, link_type_id)` (line 2789): no docstring
    - `async list_relationship_specs(self, db_name, dataset_id, status, limit)` (line 2820): no docstring
    - `async list_relationship_specs_by_relationship_object_type(self, db_name, relationship_object_type, status, limit)` (line 2852): no docstring
    - `async list_relationship_index_results(self, relationship_spec_id, link_type_id, db_name, status, limit)` (line 2885): no docstring
    - `async record_link_edit(self, db_name, link_type_id, branch, source_object_type, target_object_type, predicate, source_instance_id, target_instance_id, edit_type, status, metadata, edit_id)` (line 2917): no docstring
    - `async list_link_edits(self, db_name, link_type_id, branch, status, source_instance_id, target_instance_id, limit)` (line 2967): no docstring
    - `async clear_link_edits(self, db_name, link_type_id, branch)` (line 3005): no docstring
    - `async create_schema_migration_plan(self, db_name, subject_type, subject_id, plan, status, plan_id)` (line 3031): no docstring
    - `async list_schema_migration_plans(self, db_name, subject_type, subject_id, status, limit)` (line 3064): no docstring
    - `async get_ingest_request_by_key(self, idempotency_key)` (line 3095): no docstring
    - `async get_ingest_request(self, ingest_request_id)` (line 3139): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 3183): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 3258): no docstring
    - `async create_ingest_transaction(self, ingest_request_id, status)` (line 3290): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 3329): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 3370): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 3408): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 3473): no docstring
    - `async update_ingest_request_payload(self, ingest_request_id, schema_json, sample_json, row_count, source_metadata)` (line 3506): no docstring
    - `async approve_ingest_schema(self, ingest_request_id, schema_json, approved_by)` (line 3538): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, apply_schema, outbox_entries)` (line 3648): no docstring
    - `async claim_ingest_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 3917): no docstring
    - `async mark_ingest_outbox_published(self, outbox_id)` (line 3994): no docstring
    - `async mark_ingest_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 4013): no docstring
    - `async mark_ingest_outbox_dead(self, outbox_id, error)` (line 4040): no docstring
    - `async purge_ingest_outbox(self, retention_days, limit)` (line 4060): no docstring
    - `async get_ingest_outbox_metrics(self)` (line 4090): no docstring
    - `async reconcile_ingest_state(self, stale_after_seconds, limit, use_lock, lock_key)` (line 4132): Best-effort reconciliation for ingest atomicity.

### `backend/shared/services/registries/lineage_store.py`
- **Functions**
  - `create_lineage_store(settings)` (line 1133): no docstring
- **Classes**
  - `LineageStore` (line 27): Postgres-backed lineage store.
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 37): no docstring
    - `async _ensure_tables(self, conn)` (line 56): no docstring
    - `node_event(event_id)` (line 263): no docstring
    - `node_aggregate(aggregate_type, aggregate_id)` (line 267): no docstring
    - `node_artifact(kind, *parts)` (line 271): no docstring
    - `_infer_node_type(node_id)` (line 276): no docstring
    - `_parse_node_id(node_id)` (line 288): Decompose node_id into queryable columns (best-effort).
    - `_run_context(cls)` (line 348): no docstring
    - `_deterministic_edge_id(*parts)` (line 353): no docstring
    - `_coerce_metadata(value)` (line 358): Coerce a Postgres JSONB value into a Python dict.
    - `async upsert_node(self, node_id, node_type, label, metadata, created_at, recorded_at, db_name, run_id, code_sha, schema_version)` (line 393): no docstring
    - `async insert_edge(self, from_node_id, to_node_id, edge_type, occurred_at, metadata, projection_name, recorded_at, db_name, run_id, code_sha, schema_version, edge_id)` (line 458): no docstring
    - `async enqueue_backfill(self, envelope, s3_bucket, s3_key, error)` (line 522): Best-effort enqueue for eventual lineage recovery.
    - `async mark_backfill_done(self, event_id)` (line 571): no docstring
    - `async mark_backfill_failed(self, event_id, error)` (line 585): no docstring
    - `async claim_backfill_batch(self, limit, db_name)` (line 601): Claim a batch of pending backfill rows (best-effort).
    - `async get_backfill_metrics(self, db_name)` (line 657): no docstring
    - `async count_edges(self, edge_type, db_name, since, until)` (line 694): no docstring
    - `async get_latest_edges_to(self, to_node_ids, edge_type, projection_name, db_name)` (line 725): Fetch the latest edge (by occurred_at) for each `to_node_id`.
    - `async record_link(self, from_node_id, to_node_id, edge_type, occurred_at, edge_metadata, from_label, to_label, from_type, to_type, from_metadata, to_metadata, db_name, projection_name, run_id, code_sha, schema_version, edge_id)` (line 801): no docstring
    - `async record_event_envelope(self, envelope, s3_bucket, s3_key)` (line 877): Record the core lineage relationships for an EventEnvelope:
    - `normalize_root(root)` (line 974): no docstring
    - `async get_graph(self, root, direction, max_depth, max_nodes, max_edges, db_name)` (line 987): no docstring

### `backend/shared/services/registries/objectify_registry.py`
- **Classes**
  - `OCCConflictError` (line 26): Raised when optimistic concurrency control check fails.
    - `__init__(self, table, record_id, expected_version, actual_version)` (line 29): no docstring
  - `OntologyMappingSpecRecord` (line 48): no docstring
  - `ObjectifyJobRecord` (line 69): no docstring
  - `ObjectifyOutboxItem` (line 91): no docstring
  - `ObjectifyRegistry` (line 105): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 106): no docstring
    - `async _ensure_tables(self, conn)` (line 125): no docstring
    - `_normalize_optional(value)` (line 323): no docstring
    - `_row_to_objectify_job(row)` (line 330): no docstring
    - `build_dedupe_key(dataset_id, dataset_branch, mapping_spec_id, mapping_spec_version, dataset_version_id, artifact_id, artifact_output_name)` (line 354): no docstring
    - `_validate_objectify_inputs(self, dataset_version_id, artifact_id, artifact_output_name)` (line 378): no docstring
    - `async create_mapping_spec(self, dataset_id, dataset_branch, artifact_output_name, schema_hash, backing_datasource_id, backing_datasource_version_id, target_class_id, mappings, target_field_types, status, auto_sync, options)` (line 392): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 517): no docstring
    - `async list_mapping_specs(self, dataset_id, include_inactive, limit)` (line 555): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 620): no docstring
    - `async create_objectify_job(self, job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id, artifact_id, artifact_output_name, dataset_branch, target_class_id, status, outbox_payload, dedupe_key)` (line 683): no docstring
    - `async get_objectify_metrics(self)` (line 775): no docstring
    - `async enqueue_objectify_job(self, job)` (line 821): no docstring
    - `async enqueue_outbox_for_job(self, job_id, payload)` (line 849): no docstring
    - `async has_outbox_for_job(self, job_id, statuses)` (line 871): no docstring
    - `async claim_objectify_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 896): no docstring
    - `async mark_objectify_outbox_published(self, outbox_id, job_id)` (line 970): no docstring
    - `async mark_objectify_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 999): no docstring
    - `async purge_objectify_outbox(self, retention_days, limit)` (line 1025): no docstring
    - `async list_objectify_jobs(self, statuses, older_than, limit)` (line 1055): no docstring
    - `async get_objectify_job(self, job_id)` (line 1087): no docstring
    - `async get_objectify_job_by_dedupe_key(self, dedupe_key)` (line 1106): no docstring
    - `async find_objectify_job(self, dataset_version_id, mapping_spec_id, mapping_spec_version, statuses)` (line 1130): no docstring
    - `async find_objectify_job_for_artifact(self, artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, statuses)` (line 1166): no docstring
    - `async update_objectify_job_status(self, job_id, status, command_id, error, report, completed_at, expected_version)` (line 1207): Update objectify job status with optional OCC.
    - `async get_watermark(self, mapping_spec_id, target_class_id, dataset_branch)` (line 1312): Get the watermark state for a mapping spec or target class.
    - `async update_watermark(self, mapping_spec_id, dataset_branch, watermark_column, watermark_value, dataset_version_id, lakefs_commit_id, rows_processed)` (line 1369): Update or create watermark state for a mapping spec.
    - `async delete_watermark(self, mapping_spec_id, dataset_branch)` (line 1430): Delete watermark state (for full refresh reset).
    - `async get_all_watermarks(self, mapping_spec_id, limit)` (line 1455): Get all watermarks, optionally filtered by mapping spec.

### `backend/shared/services/registries/ontology_key_spec_registry.py`
- **Functions**
  - `_normalize_str_list(values)` (line 28): no docstring
- **Classes**
  - `OntologyKeySpec` (line 51): no docstring
  - `OntologyKeySpecRegistry` (line 61): Persist ordered ontology key specs in Postgres.
    - `__init__(self, postgres_url, pool_min, pool_max)` (line 70): no docstring
    - `async _ensure_pool(self)` (line 82): no docstring
    - `async close(self)` (line 93): no docstring
    - `async ensure_schema(self)` (line 98): no docstring
    - `async upsert_key_spec(self, db_name, branch, class_id, primary_key, title_key)` (line 119): no docstring
    - `async get_key_spec(self, db_name, branch, class_id)` (line 152): no docstring
    - `async delete_key_spec(self, db_name, branch, class_id)` (line 174): no docstring
    - `async get_key_spec_index(self, db_name, branch, class_ids)` (line 191): Fetch many key specs in a single query.
    - `_row_to_model(row)` (line 227): no docstring

### `backend/shared/services/registries/pipeline_plan_registry.py`
- **Classes**
  - `PipelinePlanRecord` (line 22): no docstring
  - `PipelinePlanRegistry` (line 36): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 37): no docstring
    - `async _ensure_tables(self, conn)` (line 56): no docstring
    - `_row_to_plan(self, row)` (line 84): no docstring
    - `async upsert_plan(self, plan_id, tenant_id, status, goal, db_name, branch, plan, created_by)` (line 99): no docstring
    - `async get_plan(self, plan_id, tenant_id)` (line 152): no docstring
    - `async list_plans(self, tenant_id, status, limit, offset)` (line 169): no docstring

### `backend/shared/services/registries/pipeline_registry.py`
- **Functions**
  - `_ensure_json_string(value)` (line 66): no docstring
  - `_normalize_output_list(value, field_name)` (line 72): no docstring
  - `_is_production_env()` (line 85): no docstring
  - `_lakefs_credentials_source()` (line 89): no docstring
  - `_lakefs_service_principal()` (line 97): no docstring
  - `_lakefs_fernet()` (line 111): no docstring
  - `_encrypt_secret(secret_access_key)` (line 124): no docstring
  - `_decrypt_secret(encrypted)` (line 131): no docstring
  - `_normalize_pipeline_role(value)` (line 149): no docstring
  - `_normalize_principal_type(value)` (line 158): no docstring
  - `_role_allows(required, assigned)` (line 165): no docstring
  - `_definition_object_key(db_name, pipeline_name)` (line 171): no docstring
  - `_row_to_pipeline_record(row)` (line 189): no docstring
  - `_row_to_pipeline_artifact(row)` (line 342): no docstring
- **Classes**
  - `PipelineMergeNotSupportedError` (line 34): no docstring
  - `PipelineOCCConflictError` (line 38): Raised when optimistic concurrency control detects a version conflict.
    - `__init__(self, pipeline_id, expected_version, actual_version)` (line 41): no docstring
  - `PipelineAlreadyExistsError` (line 58): no docstring
    - `__init__(self, db_name, name, branch)` (line 59): no docstring
  - `LakeFSCredentials` (line 179): no docstring
  - `PipelineRecord` (line 229): no docstring
  - `PipelineVersionRecord` (line 267): no docstring
  - `PipelineUdfRecord` (line 277): no docstring
  - `PipelineUdfVersionRecord` (line 288): no docstring
  - `PipelineArtifactRecord` (line 297): no docstring
  - `PromotionManifestRecord` (line 321): no docstring
  - `PipelineRegistry` (line 367): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 368): no docstring
    - `async _get_lakefs_credentials(self, principal_type, principal_id)` (line 387): no docstring
    - `async upsert_lakefs_credentials(self, principal_type, principal_id, access_key_id, secret_access_key, created_by)` (line 425): no docstring
    - `async list_lakefs_credentials(self)` (line 470): no docstring
    - `async resolve_lakefs_credentials(self, user_id)` (line 493): no docstring
    - `async get_lakefs_client(self, user_id)` (line 530): no docstring
    - `async get_lakefs_storage(self, user_id)` (line 541): no docstring
    - `async ensure_lakefs_branch(self, repository, branch, source, user_id)` (line 551): Ensure the target lakeFS branch exists before attempting S3-gateway writes.
    - `_resolve_repository(self, pipeline)` (line 585): no docstring
    - `async _ensure_tables(self, conn)` (line 592): no docstring
    - `async list_dependencies(self, pipeline_id)` (line 1120): no docstring
    - `async replace_dependencies(self, pipeline_id, dependencies)` (line 1139): no docstring
    - `async grant_permission(self, pipeline_id, principal_type, principal_id, role)` (line 1177): no docstring
    - `async revoke_permission(self, pipeline_id, principal_type, principal_id)` (line 1208): no docstring
    - `async list_permissions(self, pipeline_id)` (line 1233): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 1258): no docstring
    - `async get_permission_role(self, pipeline_id, principal_type, principal_id)` (line 1274): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 1303): no docstring
    - `async create_pipeline(self, db_name, name, description, pipeline_type, location, status, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle, schedule_interval_seconds, schedule_cron, pipeline_id)` (line 1320): no docstring
    - `async list_pipelines(self, db_name, branch)` (line 1407): no docstring
    - `async list_proposals(self, db_name, branch, status)` (line 1510): no docstring
    - `async submit_proposal(self, pipeline_id, title, description, proposal_bundle)` (line 1563): no docstring
    - `async review_proposal(self, pipeline_id, status, review_comment)` (line 1607): no docstring
    - `async merge_branch(self, pipeline_id, from_branch, to_branch, user_id)` (line 1645): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 1741): no docstring
    - `async get_pipeline_by_name(self, db_name, name, branch)` (line 1766): no docstring
    - `async update_pipeline(self, pipeline_id, name, description, location, status, schedule_interval_seconds, schedule_cron, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle, expected_version)` (line 1799): Update pipeline with Optimistic Concurrency Control.
    - `async add_version(self, pipeline_id, branch, definition_json, version_id, user_id)` (line 1918): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 2040): no docstring
    - `async get_version(self, pipeline_id, lakefs_commit_id, branch)` (line 2078): no docstring
    - `async record_preview(self, pipeline_id, status, row_count, sample_json, job_id, node_id)` (line 2115): no docstring
    - `async record_run(self, pipeline_id, job_id, mode, status, node_id, row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash, input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version, started_at, finished_at)` (line 2159): no docstring
    - `async upsert_artifact(self, pipeline_id, job_id, mode, status, run_id, artifact_id, definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id, inputs, lakefs_repository, lakefs_branch, lakefs_commit_id, outputs, declared_outputs, sampling_strategy, error)` (line 2268): no docstring
    - `async get_artifact(self, artifact_id)` (line 2386): no docstring
    - `async get_artifact_by_job(self, pipeline_id, job_id, mode)` (line 2407): no docstring
    - `async list_artifacts(self, pipeline_id, limit, mode)` (line 2443): no docstring
    - `async list_runs(self, pipeline_id, limit)` (line 2477): no docstring
    - `async get_run(self, pipeline_id, job_id)` (line 2526): no docstring
    - `async get_watermarks(self, pipeline_id, branch)` (line 2572): no docstring
    - `async upsert_watermarks(self, pipeline_id, branch, watermarks)` (line 2592): no docstring
    - `async record_build(self, pipeline_id, status, output_json, deployed_commit_id)` (line 2627): no docstring
    - `async record_promotion_manifest(self, pipeline_id, db_name, build_job_id, artifact_id, definition_hash, lakefs_repository, lakefs_commit_id, ontology_commit_id, mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id, promoted_dataset_version_id, promoted_dataset_name, target_branch, promoted_by, metadata, manifest_id, promoted_at)` (line 2659): no docstring
    - `async list_scheduled_pipelines(self)` (line 2780): no docstring
    - `async record_schedule_tick(self, pipeline_id, scheduled_at)` (line 2834): no docstring
    - `async list_pipeline_branches(self, db_name)` (line 2849): no docstring
    - `async get_pipeline_branch(self, db_name, branch)` (line 2876): no docstring
    - `async archive_pipeline_branch(self, db_name, branch)` (line 2902): no docstring
    - `async restore_pipeline_branch(self, db_name, branch)` (line 2939): no docstring
    - `async create_branch(self, pipeline_id, new_branch, user_id)` (line 2976): no docstring
    - `async create_udf(self, db_name, name, code, description)` (line 3060): no docstring
    - `async create_udf_version(self, udf_id, code)` (line 3119): no docstring
    - `async list_udfs(self, db_name)` (line 3180): List all UDFs for a database.
    - `async get_udf(self, udf_id)` (line 3202): no docstring
    - `async get_udf_version(self, udf_id, version)` (line 3222): no docstring
    - `async get_udf_latest_version(self, udf_id)` (line 3249): no docstring

### `backend/shared/services/registries/postgres_schema_registry.py`
- **Classes**
  - `PostgresSchemaRegistry` (line 23): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max, command_timeout)` (line 24): no docstring
    - `async initialize(self)` (line 40): no docstring
    - `async connect(self)` (line 43): no docstring
    - `async close(self)` (line 54): no docstring
    - `async shutdown(self)` (line 59): no docstring
    - `async health_check(self)` (line 62): no docstring
    - `async ensure_schema(self)` (line 73): no docstring
    - `async _ensure_tables(self, conn)` (line 82): no docstring

### `backend/shared/services/registries/processed_event_heartbeat.py`
- **Functions**
  - `async run_processed_event_heartbeat_loop(registry, handler, event_id, interval_seconds, stop_when_false, continue_on_exception, logger, warning_message)` (line 13): Keep a ProcessedEventRegistry lease alive while processing an event.

### `backend/shared/services/registries/processed_event_registry.py`
- **Functions**
  - `validate_lease_settings(lease_timeout_seconds, heartbeat_interval_seconds)` (line 584): no docstring
  - `validate_registry_enabled()` (line 614): no docstring
- **Classes**
  - `ClaimDecision` (line 24): no docstring
  - `ClaimResult` (line 32): no docstring
  - `ProcessedEventRegistry` (line 38): Postgres-backed idempotency + ordering guard.
    - `__init__(self, dsn, schema, lease_timeout_seconds)` (line 47): no docstring
    - `async connect(self)` (line 71): no docstring
    - `async initialize(self)` (line 84): no docstring
    - `async close(self)` (line 87): no docstring
    - `async shutdown(self)` (line 92): no docstring
    - `async ensure_schema(self)` (line 95): no docstring
    - `async claim(self, handler, event_id, aggregate_id, sequence_number)` (line 168): Try to claim an event for processing.
    - `async heartbeat(self, handler, event_id)` (line 347): Extend processing lease for a claimed event (owner-scoped).
    - `async get_event_record(self, event_id)` (line 369): no docstring
    - `async mark_done(self, handler, event_id, aggregate_id, sequence_number)` (line 408): no docstring
    - `async mark_failed(self, handler, event_id, error)` (line 476): no docstring
    - `async mark_retrying(self, handler, event_id, error)` (line 527): Mark a claimed event as retrying without finalizing it as failed.

### `backend/shared/services/registries/processed_event_registry_factory.py`
- **Functions**
  - `async create_processed_event_registry(lease_timeout_seconds, dsn, schema, validate)` (line 19): no docstring

### `backend/shared/services/storage/__init__.py`

### `backend/shared/services/storage/connectivity.py`
- **Classes**
  - `AsyncClientPingMixin` (line 8): no docstring
    - `client(self)` (line 12): no docstring
    - `async ping(self)` (line 16): no docstring

### `backend/shared/services/storage/elasticsearch_service.py`
- **Functions**
  - `create_elasticsearch_service(settings)` (line 614): Elasticsearch 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_elasticsearch_service_legacy(host, port, username, password)` (line 637): 레거시 Elasticsearch 서비스 팩토리 함수 (하위 호환성)
  - `async promote_alias_to_index(elasticsearch_service, base_index, new_index, allow_delete_base_index)` (line 671): Atomically promote *new_index* behind the *base_index* alias.
- **Classes**
  - `ElasticsearchService` (line 29): Async Elasticsearch client service with connection pooling and error handling.
    - `__init__(self, host, port, username, password, use_ssl, verify_certs, request_timeout, max_retries, retry_on_timeout)` (line 45): no docstring
    - `async connect(self)` (line 81): Initialize Elasticsearch connection.
    - `async disconnect(self)` (line 96): Close Elasticsearch connection.
    - `client(self)` (line 106): Get Elasticsearch client instance.
    - `async get_cluster_health(self)` (line 113): Get Elasticsearch cluster health status.
    - `async create_index(self, index, mappings, settings, aliases)` (line 126): Create an index with optional mappings, settings, and aliases.
    - `async delete_index(self, index)` (line 165): Delete an index.
    - `async index_exists(self, index)` (line 187): Check if index exists.
    - `async update_mapping(self, index, properties)` (line 196): Update index mapping.
    - `async index_document(self, index, document, doc_id, refresh, version, version_type, op_type)` (line 225): Index a single document.
    - `async get_document(self, index, doc_id, source_includes, source_excludes)` (line 269): Get a document by ID.
    - `async update_document(self, index, doc_id, doc, script, upsert, refresh)` (line 303): Update a document.
    - `async delete_document(self, index, doc_id, refresh, version, version_type)` (line 352): Delete a document.
    - `async bulk_index(self, index, documents, chunk_size, refresh)` (line 390): Bulk index documents.
    - `async search(self, index, query, size, from_, sort, source_includes, source_excludes, aggregations)` (line 440): Search documents.
    - `async count(self, index, query)` (line 494): Count documents matching query.
    - `async create_alias(self, index, alias, filter)` (line 523): Create an alias for an index with optional filter.
    - `async delete_alias(self, index, alias)` (line 557): Delete an alias.
    - `async update_aliases(self, actions)` (line 580): Perform multiple alias operations atomically.
    - `async refresh_index(self, index)` (line 604): Force refresh an index to make changes searchable.

### `backend/shared/services/storage/event_store.py`
- **Functions**
  - `async get_event_store()` (line 1246): Dependency to get the Event Store instance
- **Classes**
  - `EventStore` (line 55): The REAL Event Store using S3/MinIO as Single Source of Truth.
    - `__init__(self)` (line 70): no docstring
    - `_sequence_mode(self)` (line 84): no docstring
    - `_lineage_enabled(self)` (line 89): no docstring
    - `_audit_enabled(self)` (line 93): no docstring
    - `_s3_client_kwargs(self)` (line 96): no docstring
    - `async connect(self)` (line 124): Initialize S3/MinIO connection
    - `async _initialize_lineage_and_audit(self)` (line 164): no docstring
    - `_partition_key_for_envelope(envelope)` (line 193): no docstring
    - `async _record_lineage_and_audit(self, envelope, s3_key, audit_action)` (line 202): no docstring
    - `async _record_audit_failure(self, envelope, error)` (line 321): no docstring
    - `async _get_sequence_allocator(self)` (line 358): no docstring
    - `async _ensure_sequence_number(self, event)` (line 371): Ensure `event.sequence_number` is set using an atomic write-side allocator.
    - `async append_event(self, event)` (line 509): Append an immutable event to S3/MinIO.
    - `_enforce_idempotency_contract(self, existing, incoming, source)` (line 690): Detect event_id reuse with conflicting contents.
    - `_is_command_envelope(env)` (line 725): no docstring
    - `_normalize_for_idempotency_compare(env)` (line 731): no docstring
    - `_stable_hash(payload)` (line 746): no docstring
    - `async _get_existing_key_by_event_id(self, s3, event_id)` (line 750): no docstring
    - `async _get_existing_key_by_aggregate_index(self, s3, aggregate_type, aggregate_id, event_id)` (line 763): no docstring
    - `async _read_event_object(self, s3, key)` (line 785): no docstring
    - `async get_event_object_key(self, event_id)` (line 791): Resolve an event_id to its S3 object key using the by-event-id index.
    - `async read_event_by_key(self, key)` (line 804): Read an event envelope from S3/MinIO by object key.
    - `async get_events(self, aggregate_type, aggregate_id, from_version, to_version)` (line 813): Retrieve all events for an aggregate from S3/MinIO.
    - `_dedup_events(events)` (line 903): Best-effort dedup to hide historical duplicates during migration.
    - `_dedup_key(event)` (line 916): no docstring
    - `async replay_events(self, from_timestamp, to_timestamp, event_types)` (line 928): Replay events from S3/MinIO for a time range.
    - `async get_aggregate_version(self, aggregate_type, aggregate_id, allow_full_scan)` (line 1021): Get the current version of an aggregate (max sequence_number).
    - `async _update_indexes(self, event, key, s3)` (line 1065): Update various indexes for efficient querying.
    - `async _write_index_entries(self, s3, aggregate_index_key, date_index_key, event_id_index_key, payload)` (line 1133): no docstring
    - `async get_snapshot(self, aggregate_type, aggregate_id, version)` (line 1163): Get a snapshot of an aggregate at a specific version.
    - `async save_snapshot(self, aggregate_type, aggregate_id, version, state)` (line 1192): Save a snapshot for performance optimization.

### `backend/shared/services/storage/lakefs_branch_utils.py`
- **Functions**
  - `async ensure_lakefs_branch(lakefs_client, repository, branch, source)` (line 10): no docstring

### `backend/shared/services/storage/lakefs_client.py`
- **Functions**
  - `_extract_commit_id(payload)` (line 60): no docstring
  - `_normalize_metadata(metadata)` (line 69): lakeFS commit/merge metadata is stored as string key-value pairs.
- **Classes**
  - `LakeFSError` (line 21): no docstring
  - `LakeFSAuthError` (line 25): no docstring
  - `LakeFSNotFoundError` (line 29): no docstring
  - `LakeFSConflictError` (line 33): no docstring
  - `LakeFSConfig` (line 38): no docstring
    - `from_env()` (line 44): no docstring
  - `LakeFSClient` (line 96): Minimal async lakeFS REST client.
    - `__init__(self, config, timeout_seconds)` (line 105): no docstring
    - `_client(self)` (line 111): no docstring
    - `async create_branch(self, repository, name, source)` (line 119): no docstring
    - `async delete_branch(self, repository, name)` (line 142): no docstring
    - `async commit(self, repository, branch, message, metadata)` (line 161): no docstring
    - `async get_branch_head_commit_id(self, repository, branch)` (line 195): no docstring
    - `async list_diff_objects(self, repository, ref, since, prefix, amount)` (line 218): no docstring
    - `async merge(self, repository, source_ref, destination_branch, message, metadata, allow_empty)` (line 294): no docstring

### `backend/shared/services/storage/lakefs_storage_service.py`
- **Functions**
  - `create_lakefs_storage_service(settings)` (line 30): no docstring
- **Classes**
  - `LakeFSStorageService` (line 18): no docstring
    - `async create_bucket(self, bucket_name)` (line 20): no docstring
    - `async bucket_exists(self, bucket_name)` (line 25): no docstring

### `backend/shared/services/storage/redis_service.py`
- **Functions**
  - `create_redis_service(settings)` (line 454): Redis 서비스 팩토리 함수 (Anti-pattern 13 해결)
- **Classes**
  - `RedisService` (line 27): Async Redis client service with connection pooling and error handling.
    - `__init__(self, host, port, password, db, decode_responses, max_connections, socket_timeout, connection_timeout, retry_on_timeout)` (line 41): no docstring
    - `async connect(self)` (line 76): Initialize Redis connection.
    - `async initialize(self)` (line 87): ServiceContainer-compatible initialization method.
    - `async disconnect(self)` (line 92): Close Redis connection and pool.
    - `client(self)` (line 111): Get Redis client instance.
    - `async set_command_status(self, command_id, status, data, ttl)` (line 120): Set command status with optional data.
    - `async get_command_status(self, command_id)` (line 154): Get command status and data.
    - `async update_command_progress(self, command_id, progress, message)` (line 173): Update command execution progress.
    - `async set_command_result(self, command_id, result, ttl)` (line 200): Store command execution result.
    - `async get_command_result(self, command_id)` (line 223): Get command execution result.
    - `async publish_command_update(self, command_id, data)` (line 244): Publish command status update to subscribers.
    - `async subscribe_command_updates(self, command_id, callback, task_manager)` (line 260): Subscribe to command status updates with proper task tracking.
    - `async _listen_for_updates(self, pubsub, callback, command_id)` (line 309): Listen for pub/sub updates with improved error handling.
    - `_handle_listener_done(self, task)` (line 346): Handle completion of a listener task.
    - `async set_json(self, key, value, ttl)` (line 358): Set JSON value with optional TTL.
    - `async get_json(self, key)` (line 371): Get JSON value.
    - `async set(self, key, value, ttl)` (line 379): Set key-value pair with optional TTL.
    - `async get(self, key)` (line 386): Get value for key.
    - `async delete(self, key)` (line 391): Delete key.
    - `async exists(self, key)` (line 396): Check if key exists.
    - `async expire(self, key, seconds)` (line 401): Set expiration on key.
    - `async keys(self, pattern)` (line 406): Get keys matching pattern.
    - `async cleanup_listeners(self)` (line 411): Clean up all active pub/sub listeners.
    - `async scan_keys(self, pattern, count)` (line 430): Scan keys matching pattern without blocking.

### `backend/shared/services/storage/s3_client_config.py`
- **Functions**
  - `build_s3_client_config(endpoint_url, addressing_style, extra_path_style_hosts)` (line 28): Return a botocore Config for S3 addressing style, or None when not needed.

### `backend/shared/services/storage/storage_service.py`
- **Functions**
  - `create_storage_service(settings)` (line 901): 스토리지 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_storage_service_legacy(endpoint_url, access_key, secret_key)` (line 935): 레거시 스토리지 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `StorageService` (line 33): S3/MinIO 스토리지 서비스 - Event Sourcing 지원
    - `__init__(self, endpoint_url, access_key, secret_key, region, use_ssl, ssl_verify)` (line 57): 스토리지 서비스 초기화
    - `async create_bucket(self, bucket_name)` (line 95): 버킷 생성
    - `async bucket_exists(self, bucket_name)` (line 117): 버킷 존재 여부 확인
    - `async save_json(self, bucket, key, data, metadata)` (line 137): JSON 데이터를 S3에 저장하고 체크섬 반환
    - `async save_bytes(self, bucket, key, data, content_type, metadata)` (line 185): Raw bytes를 S3에 저장하고 체크섬 반환
    - `async save_fileobj(self, bucket, key, fileobj, content_type, metadata, checksum)` (line 233): Stream a file-like object into S3 and return a checksum.
    - `async load_json(self, bucket, key)` (line 290): S3에서 JSON 데이터 로드
    - `async load_bytes(self, bucket, key)` (line 312): S3에서 Raw bytes 로드
    - `async load_bytes_lines(self, bucket, key, max_lines, max_bytes)` (line 332): Load up to `max_lines` newline-delimited lines from the start of an object.
    - `async verify_checksum(self, bucket, key, expected_checksum)` (line 386): 저장된 파일의 체크섬 검증
    - `async delete_object(self, bucket, key)` (line 412): S3 객체 삭제
    - `async delete_prefix(self, bucket, prefix)` (line 430): Delete all objects under a prefix.
    - `async list_objects(self, bucket, prefix, max_keys)` (line 473): 버킷의 객체 목록 조회
    - `async list_objects_paginated(self, bucket, prefix, max_keys, continuation_token)` (line 502): Paginated object listing (returns next continuation token if more).
    - `async iter_objects(self, bucket, prefix, max_keys)` (line 524): Async iterator over all objects under prefix (pagination-aware).
    - `async get_object_metadata(self, bucket, key)` (line 548): 객체 메타데이터 조회
    - `generate_instance_path(self, db_name, class_id, instance_id, command_id)` (line 576): 인스턴스 이벤트 저장 경로 생성
    - `async get_all_commands_for_instance(self, bucket, db_name, class_id, instance_id)` (line 598): 특정 인스턴스의 모든 Command 파일 목록 조회
    - `async list_command_files(self, bucket, prefix)` (line 662): List command JSON objects under a prefix (pagination-aware, sorted by LastModified).
    - `async replay_instance_state(self, bucket, command_files)` (line 703): Command 파일들을 순차적으로 읽어 인스턴스의 최종 상태 재구성
    - `is_instance_deleted(self, instance_state)` (line 863): 인스턴스가 삭제된 상태인지 확인
    - `get_deletion_info(self, instance_state)` (line 877): 삭제된 인스턴스의 삭제 정보 반환

### `backend/shared/setup.py`

### `backend/shared/testing/__init__.py`

### `backend/shared/testing/config_fixtures.py`
- **Functions**
  - `create_mock_storage_service()` (line 177): Create mock storage service for testing
  - `create_mock_redis_service()` (line 187): Create mock Redis service for testing
  - `create_mock_elasticsearch_service()` (line 199): Create mock Elasticsearch service for testing
  - `create_mock_label_mapper()` (line 210): Create mock label mapper for testing
  - `create_mock_jsonld_converter()` (line 218): Create mock JSON-LD converter for testing
  - `test_settings()` (line 227): Pytest fixture for test application settings
  - `test_settings_with_overrides()` (line 233): Pytest fixture factory for test settings with custom overrides
  - `async mock_container(test_settings)` (line 241): Pytest fixture for mock service container
  - `mock_command_status_service()` (line 259): Pytest fixture for mock command status service
  - `async isolated_test_environment(**config_overrides)` (line 298): Async context manager for completely isolated test environment
  - `setup_test_database_config(**overrides)` (line 326): Create test settings with database configuration
  - `setup_test_service_config(**overrides)` (line 340): Create test settings with service configuration
- **Classes**
  - `TestApplicationSettings` (line 34): Test-specific application settings that provide safe defaults for testing
    - `__init__(self, **overrides)` (line 42): no docstring
  - `MockServiceContainer` (line 78): Mock service container for testing
    - `__init__(self, settings)` (line 88): no docstring
    - `register_singleton(self, service_type, factory)` (line 94): Register a service factory
    - `register_mock(self, service_type, mock_instance)` (line 98): Register a mock service instance directly
    - `has(self, service_type)` (line 102): Check if service type is registered
    - `async get(self, service_type)` (line 106): Get service instance (async)
    - `get_sync(self, service_type)` (line 122): Get service instance (sync)
    - `async health_check_all(self)` (line 135): Mock health check for all services
    - `async shutdown(self)` (line 154): Shutdown all services
  - `ConfigOverride` (line 268): Context manager for temporary configuration overrides
    - `__init__(self, **overrides)` (line 276): no docstring
    - `__enter__(self)` (line 280): no docstring
    - `__exit__(self, _exc_type, _exc_val, _exc_tb)` (line 288): no docstring

### `backend/shared/tools/__init__.py`

### `backend/shared/tools/bff_admin_api.py`
- **Functions**
  - `normalize_base_url(base_url)` (line 10): no docstring
  - `extract_command_id(payload)` (line 17): Extract a command id from common BFF response shapes.
  - `async wait_for_command(client, base_url, command_id, timeout_seconds, poll_interval_seconds)` (line 55): no docstring
  - `async list_databases(client, base_url)` (line 79): no docstring
  - `async delete_database(client, base_url, db_name, expected_seq, allow_missing, timeout_seconds)` (line 100): no docstring

### `backend/shared/tools/error_taxonomy_audit.py`
- **Functions**
  - `_attr_chain(node)` (line 69): no docstring
  - `_extract_error_code(node)` (line 80): no docstring
  - `_extract_status(node)` (line 88): no docstring
  - `_iter_python_files(root)` (line 100): no docstring
  - `_iter_runtime_files(root, runtime_scope_glob)` (line 107): no docstring
  - `_is_exception_type(node)` (line 120): no docstring
  - `_is_suppress_call(func)` (line 130): no docstring
  - `_handler_contains_log_call(handler)` (line 138): no docstring
  - `_handler_contains_raise(handler)` (line 155): no docstring
  - `_handler_contains_runtime_policy_call(handler)` (line 162): no docstring
  - `_try_contains_lineage_calls(try_node)` (line 180): no docstring
  - `_count_runtime_guard_patterns(root, runtime_scope_glob)` (line 192): no docstring
  - `_find_commented_exports(root, runtime_scope_glob)` (line 337): no docstring
  - `_find_doc_only_modules(root, runtime_scope_glob)` (line 360): no docstring
  - `_normalize_route_path(*parts)` (line 400): no docstring
  - `_extract_apirouter_prefix(tree)` (line 410): no docstring
  - `_iter_router_routes(tree)` (line 426): no docstring
  - `_collect_app_route_collisions(root)` (line 446): no docstring
  - `_find_duplicate_symbols(root, runtime_scope_glob)` (line 525): no docstring
  - `_run_vulture_high_confidence(root)` (line 564): no docstring
  - `_parse_catalog_specs(catalog_path)` (line 620): no docstring
  - `_audit_classified(root, specs)` (line 669): no docstring
  - `_dict_has_code_key(node)` (line 733): no docstring
  - `_extract_code_literal(node)` (line 742): no docstring
  - `_extract_dict_code(node)` (line 753): no docstring
  - `_count_raw_http_exception(root)` (line 766): no docstring
  - `_audit_raw_http_status_codes(calls, specs)` (line 820): no docstring
  - `_count_raw_string_codes(root)` (line 854): no docstring
  - `_print_counter(label, rows, top_n)` (line 869): no docstring
  - `main()` (line 879): no docstring
- **Classes**
  - `CodeSpec` (line 43): no docstring
  - `Mismatch` (line 50): no docstring
  - `RawHttpCall` (line 60): no docstring

### `backend/shared/tools/foundry_functions_compat.py`
- **Functions**
  - `_parse_scalar(value)` (line 18): no docstring
  - `_load_yaml_payload(path)` (line 27): no docstring
  - `default_snapshot_path()` (line 103): no docstring
  - `_parse_status(raw, field, fn_name)` (line 107): no docstring
  - `load_foundry_functions_snapshot(snapshot_path)` (line 117): no docstring
  - `load_default_foundry_functions_snapshot()` (line 167): no docstring
  - `filter_functions(entries, engine, status)` (line 171): no docstring
- **Classes**
  - `FunctionCompatibility` (line 87): no docstring
    - `status_for_engine(self, engine)` (line 94): no docstring

### `backend/shared/tools/registry_cleanup.py`
- **Functions**
  - `_env_list(key)` (line 60): no docstring
  - `_is_safe_endpoint(base_url)` (line 67): no docstring
  - `_ensure_dev_only(base_url)` (line 77): no docstring
  - `_resolve_bff_base_url()` (line 84): no docstring
  - `_resolve_admin_token()` (line 88): no docstring
  - `_matches_any_prefix(name, prefixes)` (line 96): no docstring
  - `_matches_any_name(name, names)` (line 100): no docstring
  - `async _connect_postgres()` (line 104): no docstring
  - `_sequence_schema()` (line 108): no docstring
  - `_database_handler()` (line 115): no docstring
  - `async _fetch_updated_at_map(conn, db_names)` (line 120): no docstring
  - `_build_plan(args)` (line 156): no docstring
  - `async _run_once(plan, base_url, token)` (line 188): no docstring
  - `build_parser()` (line 265): no docstring
  - `async _async_main(args)` (line 279): no docstring
  - `main()` (line 305): no docstring
- **Classes**
  - `CleanupPlan` (line 145): no docstring

### `backend/shared/utils/__init__.py`

### `backend/shared/utils/access_policy.py`
- **Functions**
  - `_coerce_bool(value)` (line 27): no docstring
  - `_match_rule(value, op, expected)` (line 33): no docstring
  - `_match_filters(row, filters, operator)` (line 66): no docstring
  - `_apply_mask(row, columns, mask_value)` (line 97): no docstring
  - `apply_access_policy(rows, policy)` (line 115): Apply a row/column policy to result rows.

### `backend/shared/utils/action_audit_policy.py`
- **Functions**
  - `_json_size_bytes(value)` (line 24): no docstring
  - `_as_str_set(values)` (line 29): no docstring
  - `normalize_audit_policy(raw)` (line 43): no docstring
  - `_redact_recursive(value, redact_keys, redact_value)` (line 78): no docstring
  - `_summarize_large_list(values, max_items)` (line 95): no docstring
  - `_summarize_known_arrays(value, max_changes)` (line 107): no docstring
  - `audit_action_log_input(payload, audit_policy)` (line 124): no docstring
  - `audit_action_log_result(payload, audit_policy)` (line 138): no docstring
- **Classes**
  - `ActionAuditPolicyError` (line 9): no docstring
  - `NormalizedAuditPolicy` (line 14): no docstring

### `backend/shared/utils/action_data_access.py`
- **Functions**
  - `_target_text(payload, key)` (line 36): no docstring
  - `_extract_changes(target)` (line 44): no docstring
  - `_extract_changed_fields(changes)` (line 49): no docstring
  - `_has_link_changes(changes)` (line 66): no docstring
  - `_changed_attachment_fields(changed_fields, field_types)` (line 72): no docstring
  - `_changed_object_set_fields(changed_fields, field_types, link_changed)` (line 85): no docstring
  - `async _load_object_type_policy(dataset_registry, db_name, scope, class_id, cache)` (line 101): no docstring
  - `async evaluate_action_target_data_access(dataset_registry, db_name, targets, scope, enforce_data_access_policy, principal_tags, enforce_object_edit_policy, enforce_attachment_edit_policy, enforce_object_set_edit_policy, fail_on_missing_edit_policy, object_edit_scope, attachment_edit_scope, object_set_edit_scope)` (line 143): no docstring
- **Classes**
  - `_DatasetRegistryLike` (line 17): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 18): no docstring
  - `ActionTargetDataAccessReport` (line 29): no docstring

### `backend/shared/utils/action_input_schema.py`
- **Functions**
  - `_json_size_bytes(value)` (line 49): no docstring
  - `_walk_keys(value)` (line 58): no docstring
  - `_require_public_key(name)` (line 71): no docstring
  - `_normalize_field_spec(raw)` (line 78): no docstring
  - `normalize_input_schema(input_schema)` (line 150): Normalize an ActionType input_schema definition.
  - `validate_action_input(input_schema, payload, max_total_bytes)` (line 183): Validate and normalize an Action submission payload against ActionType.input_schema.
  - `_validate_value(field, value)` (line 243): no docstring
- **Classes**
  - `ActionInputSchemaError` (line 16): no docstring
  - `ActionInputValidationError` (line 20): no docstring
  - `_FieldSpec` (line 35): no docstring

### `backend/shared/utils/action_permission_profile.py`
- **Functions**
  - `_coerce_bool(value, field)` (line 40): no docstring
  - `resolve_action_permission_profile(action_spec)` (line 46): no docstring
  - `requires_action_data_access_enforcement(profile, global_enforcement)` (line 78): no docstring
- **Classes**
  - `ActionPermissionProfileError` (line 20): no docstring
    - `__init__(self, message, field)` (line 21): no docstring
  - `ActionPermissionProfile` (line 27): no docstring
    - `requires_permission_policy(self)` (line 32): no docstring
    - `requires_data_access_enforcement(self)` (line 36): no docstring

### `backend/shared/utils/action_runtime_contracts.py`
- **Functions**
  - `strip_interface_prefix(value)` (line 18): no docstring
  - `extract_interfaces_from_metadata(metadata)` (line 28): no docstring
  - `extract_required_action_interfaces(action_spec)` (line 50): no docstring
  - `build_property_type_map_from_properties(properties)` (line 91): no docstring
  - `async load_action_target_runtime_contract(terminus, db_name, class_id, branch)` (line 110): no docstring
- **Classes**
  - `ActionTargetRuntimeContract` (line 13): no docstring

### `backend/shared/utils/action_simulation_utils.py`
- **Functions**
  - `reject_simulation_delete_flag(value)` (line 6): no docstring

### `backend/shared/utils/action_template_engine.py`
- **Functions**
  - `_detect_mustache_syntax(value, label)` (line 28): Reject mustache/handlebars syntax in template values.
  - `_is_non_empty_str(value)` (line 51): no docstring
  - `_require_public_identifier(value, label)` (line 55): no docstring
  - `_split_dotted_path(path, label)` (line 66): no docstring
  - `_get_by_path(obj, path, label)` (line 81): no docstring
  - `_normalize_object_ref(value, label)` (line 92): no docstring
  - `_coerce_link_value(value, label)` (line 103): no docstring
  - `_is_ref_object(value)` (line 112): no docstring
  - `_resolve_ref_object(value, input_payload, user, target, now)` (line 116): no docstring
  - `_resolve_value(value, input_payload, user, target, now)` (line 141): no docstring
  - `_extract_link_field_names(raw_ops)` (line 175): no docstring
  - `_compile_link_ops(raw_ops, input_payload, user, target, now, label)` (line 195): no docstring
  - `_normalize_unset_list(value, label)` (line 231): no docstring
  - `_normalize_set_ops(value, label)` (line 243): no docstring
  - `_is_noop_change_spec(changes)` (line 256): no docstring
  - `_merge_change_specs(existing, incoming)` (line 267): no docstring
  - `_validate_template_v1(implementation)` (line 292): no docstring
  - `validate_template_v1_definition(implementation)` (line 315): Validate that an ActionType.implementation is executable (P0).
  - `compile_template_v1_change_shape(implementation, input_payload)` (line 359): Compile a template_v1 into a per-target "change shape" (keys only) for submission-time observed_base snapshots.
  - `compile_template_v1(implementation, input_payload, user, target_docs, now)` (line 434): Compile a template_v1 into concrete per-target changes.
- **Classes**
  - `ActionImplementationError` (line 17): no docstring
  - `CompiledTarget` (line 309): no docstring

### `backend/shared/utils/action_writeback.py`
- **Functions**
  - `action_applied_event_id(action_log_id)` (line 9): no docstring
  - `safe_str(value)` (line 13): no docstring
  - `is_noop_changes(changes)` (line 19): no docstring

### `backend/shared/utils/app_logger.py`
- **Functions**
  - `_normalize_log_level(level)` (line 18): no docstring
  - `get_logger(name, level)` (line 26): Get a configured logger instance.
  - `configure_logging(level)` (line 79): Configure global logging settings.
  - `get_funnel_logger(name)` (line 107): Get Funnel service logger.
  - `get_bff_logger(name)` (line 112): Get BFF service logger.
  - `get_oms_logger(name)` (line 117): Get OMS service logger.

### `backend/shared/utils/async_utils.py`
- **Functions**
  - `async await_if_needed(value)` (line 7): no docstring
  - `async raise_for_status_async(response)` (line 13): no docstring
  - `async response_json_async(response)` (line 17): no docstring
  - `async aclose_if_present(resource)` (line 21): no docstring

### `backend/shared/utils/backoff_utils.py`
- **Functions**
  - `exponential_backoff_seconds(attempts, base_seconds, max_seconds)` (line 6): no docstring
  - `next_exponential_backoff_at(attempts, base_seconds, max_seconds)` (line 21): no docstring

### `backend/shared/utils/blank_utils.py`
- **Functions**
  - `strip_to_none(value)` (line 6): no docstring
  - `is_blank_value(value)` (line 13): no docstring

### `backend/shared/utils/branch_utils.py`
- **Functions**
  - `protected_branch_write_message()` (line 12): no docstring
  - `get_protected_branches(env_key, defaults)` (line 16): no docstring

### `backend/shared/utils/canonical_json.py`
- **Functions**
  - `_default(value)` (line 11): no docstring
  - `canonical_json_dumps(value)` (line 20): Deterministic JSON serialization (fixed rules, versioned).
  - `sha256_canonical_json(value)` (line 39): no docstring
  - `sha256_canonical_json_prefixed(value)` (line 44): no docstring

### `backend/shared/utils/chaos.py`
- **Functions**
  - `chaos_enabled()` (line 29): no docstring
  - `_sanitize_marker(point)` (line 33): no docstring
  - `maybe_crash(point, logger)` (line 39): Crash the current process if CHAOS_CRASH_POINT matches.

### `backend/shared/utils/collection_utils.py`
- **Functions**
  - `ensure_list(value)` (line 6): no docstring

### `backend/shared/utils/commit_utils.py`
- **Functions**
  - `coerce_commit_id(value)` (line 6): no docstring

### `backend/shared/utils/deterministic_ids.py`
- **Functions**
  - `deterministic_uuid5(key)` (line 11): no docstring
  - `deterministic_uuid5_str(key)` (line 15): no docstring
  - `deterministic_uuid5_hex_prefix(key, length)` (line 19): no docstring

### `backend/shared/utils/diff_utils.py`
- **Functions**
  - `_looks_like_change(value)` (line 8): no docstring
  - `normalize_diff_changes(raw)` (line 15): no docstring
  - `_classify_change(change)` (line 42): no docstring
  - `summarize_diff_changes(changes)` (line 58): no docstring
  - `normalize_diff_response(from_ref, to_ref, raw)` (line 69): no docstring

### `backend/shared/utils/env_utils.py`
- **Functions**
  - `parse_bool(raw)` (line 8): no docstring
  - `parse_bool_env(name, default)` (line 17): no docstring
  - `parse_int_env(name, default, min_value, max_value)` (line 22): no docstring

### `backend/shared/utils/event_utils.py`
- **Functions**
  - `build_command_event(event_type, aggregate_type, aggregate_id, data, command_type, actor, event_id)` (line 10): no docstring

### `backend/shared/utils/executor_utils.py`
- **Functions**
  - `async call_in_executor(executor, func, *args, **kwargs)` (line 11): no docstring

### `backend/shared/utils/id_generator.py`
- **Functions**
  - `_normalize_korean_to_roman(text)` (line 21): 한국어를 로마자로 변환 (간단한 매핑)
  - `_extract_text_from_label(label)` (line 47): 레이블에서 텍스트 추출
  - `_clean_and_format_id(text, preserve_camel_case)` (line 94): 텍스트를 ID 형식으로 정리
  - `_generate_timestamp()` (line 123): 고유성을 위한 타임스탬프 생성
  - `_generate_short_timestamp()` (line 128): 짧은 타임스탬프 생성
  - `generate_ontology_id(label, preserve_camel_case, handle_korean, default_fallback)` (line 133): 온톨로지 ID 생성 (고급 옵션)
  - `generate_simple_id(label, use_timestamp_for_korean, default_fallback)` (line 187): 간단한 ID 생성 (타임스탬프 포함)
  - `generate_unique_id(label, prefix, suffix, max_length, force_unique)` (line 249): 고유 ID 생성 (확장 가능한 버전)
  - `generate_class_id(label)` (line 303): 클래스 ID 생성
  - `generate_property_id(label)` (line 308): 속성 ID 생성
  - `generate_relationship_id(label)` (line 313): 관계 ID 생성
  - `generate_instance_id(class_id, label)` (line 318): 인스턴스 ID 생성
  - `validate_generated_id(id_string)` (line 348): 생성된 ID의 유효성 검증
- **Classes**
  - `IDGenerationError` (line 16): ID 생성 관련 예외

### `backend/shared/utils/import_type_normalization.py`
- **Functions**
  - `normalize_import_target_type(type_value)` (line 48): Normalize a target field type for import.
  - `resolve_import_type(raw_type)` (line 98): Resolve a raw type hint to an importable target type.

### `backend/shared/utils/json_patch.py`
- **Functions**
  - `_decode_pointer_token(token)` (line 23): no docstring
  - `_iter_pointer(path)` (line 27): no docstring
  - `_resolve_parent(doc, tokens)` (line 35): no docstring
  - `apply_json_patch(document, operations)` (line 61): no docstring
- **Classes**
  - `JsonPatchError` (line 19): no docstring

### `backend/shared/utils/json_utils.py`
- **Functions**
  - `json_default(value)` (line 14): no docstring
  - `maybe_decode_json(value)` (line 20): no docstring
  - `normalize_json_payload(value, default_handler)` (line 40): no docstring
  - `coerce_json_dataset(value)` (line 51): no docstring
  - `coerce_json_pipeline(value)` (line 95): no docstring
  - `coerce_json_list(value, allow_wrapped_value, wrap_dict)` (line 129): no docstring
  - `coerce_json_dict(value, parsed_fallback_key)` (line 190): no docstring
  - `coerce_json_strict(value)` (line 240): no docstring

### `backend/shared/utils/jsonld.py`
- **Functions**
  - `get_default_converter()` (line 229): Get default JSON-LD converter.
- **Classes**
  - `JSONToJSONLDConverter` (line 12): Converter for transforming JSON data to JSON-LD format.
    - `__init__(self, context)` (line 17): Initialize converter with optional context.
    - `_get_default_context(self)` (line 26): Get default JSON-LD context.
    - `convert_to_jsonld(self, data)` (line 41): Convert JSON data to JSON-LD format.
    - `convert_from_jsonld(self, jsonld_data)` (line 75): Convert JSON-LD data back to regular JSON format.
    - `validate_jsonld(self, data)` (line 113): Validate if data is valid JSON-LD.
    - `expand_jsonld(self, data)` (line 141): Expand JSON-LD data by resolving context.
    - `compact_jsonld(self, data)` (line 167): Compact JSON-LD data using context.
    - `to_json_string(self, data, indent)` (line 201): Convert data to JSON-LD string.
    - `from_json_string(self, json_string)` (line 215): Parse JSON-LD string to data.

### `backend/shared/utils/key_spec.py`
- **Functions**
  - `_dedupe(values)` (line 34): no docstring
  - `normalize_key_columns(value)` (line 45): no docstring
  - `normalize_unique_keys(value)` (line 69): no docstring
  - `normalize_key_spec(spec, columns)` (line 92): no docstring

### `backend/shared/utils/label_mapper.py`
- **Classes**
  - `LabelMapper` (line 24): 레이블과 ID 간의 매핑을 관리하는 클래스
    - `_resolve_database_path(db_path)` (line 31): 데이터베이스 파일 경로를 안전하게 해결합니다.
    - `__init__(self, db_path)` (line 61): 초기화
    - `_ensure_directory(self)` (line 74): 데이터베이스 디렉토리 생성
    - `async _init_database(self)` (line 78): 데이터베이스 초기화 및 테이블 생성 (thread-safe)
    - `async _get_connection(self)` (line 169): 데이터베이스 연결 컨텍스트 매니저 (with connection pooling)
    - `async register_class(self, db_name, class_id, label, description)` (line 183): 클래스 레이블 매핑 등록
    - `async get_class_labels_in_batch(self, db_name, class_ids, lang)` (line 244): 여러 클래스의 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `async get_property_labels_in_batch(self, db_name, class_id, property_ids, lang)` (line 292): 특정 클래스의 여러 속성 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `async get_all_property_labels_in_batch(self, db_name, class_property_pairs, lang)` (line 341): 여러 클래스의 여러 속성 레이블을 한 번의 쿼리로 조회 (N+1 쿼리 문제 완전 해결)
    - `async get_relationship_labels_in_batch(self, db_name, predicates, lang)` (line 392): 여러 관계의 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `_extract_ids_from_data_list(self, data_list)` (line 440): Extract class IDs, property IDs, and predicates from data list.
    - `_extract_property_ids_from_data(self, data, property_ids)` (line 468): Extract property IDs from a single data item.
    - `_extract_class_property_pairs(self, data_list)` (line 487): Extract all (class_id, property_id) pairs from data list.
    - `_convert_properties_to_display(self, properties, class_id, property_labels)` (line 506): Convert properties to display format with labels.
    - `_convert_relationships_to_display(self, relationships, relationship_labels)` (line 538): Convert relationships to display format with labels.
    - `_convert_data_item_to_display(self, data, class_labels, property_labels, relationship_labels)` (line 546): Convert a single data item to display format.
    - `async convert_to_display_batch(self, db_name, data_list, lang)` (line 576): Convert multiple data items to label-based format in batch (solves N+1 query problem)
    - `async register_property(self, db_name, class_id, property_id, label)` (line 645): 속성 레이블 매핑 등록
    - `async register_relationship(self, db_name, predicate, label)` (line 681): 관계 레이블 매핑 등록
    - `async get_class_id(self, db_name, label, lang)` (line 713): 레이블로 클래스 ID 조회
    - `async get_class_label(self, db_name, class_id, lang)` (line 743): 클래스 ID로 레이블 조회
    - `async get_property_id(self, db_name, class_id, label, lang)` (line 773): 레이블로 속성 ID 조회
    - `async get_predicate(self, db_name, label, lang)` (line 806): 레이블로 관계 술어 조회
    - `async convert_query_to_internal(self, db_name, query, lang)` (line 836): 레이블 기반 쿼리를 내부 ID 기반으로 변환
    - `async convert_to_display(self, db_name, data, lang)` (line 928): 내부 ID 기반 데이터를 레이블 기반으로 변환
    - `async get_property_label(self, db_name, class_id, property_id, lang)` (line 949): 속성 ID로 레이블 조회 (공개 메서드)
    - `async _get_property_label(self, db_name, class_id, property_id, lang)` (line 966): 속성 ID로 레이블 조회 (내부 메서드)
    - `async _get_relationship_label(self, db_name, predicate, lang)` (line 988): 관계 술어로 레이블 조회
    - `_extract_labels(self, label)` (line 1010): 레이블에서 언어별 텍스트 추출
    - `async update_mappings(self, db_name, ontology_data)` (line 1046): 온톨로지 데이터로부터 모든 매핑 업데이트
    - `async remove_class(self, db_name, class_id)` (line 1075): 클래스 관련 모든 매핑 제거
    - `async export_mappings(self, db_name)` (line 1106): 특정 데이터베이스의 모든 매핑 내보내기
    - `async import_mappings(self, data)` (line 1176): 매핑 데이터 가져오기

### `backend/shared/utils/language.py`
- **Functions**
  - `get_supported_languages()` (line 16): Get list of supported languages.
  - `get_default_language()` (line 26): Get default language.
  - `is_supported_language(lang)` (line 36): Check if language is supported.
  - `normalize_language(lang)` (line 49): Normalize language code.
  - `_parse_accept_language_header(value)` (line 85): Parse Accept-Language into a list of language codes ordered by preference.
  - `_normalize_language_map_key(key)` (line 116): Strict normalization for language-map keys.
  - `get_accept_language(request)` (line 135): Get the preferred language from the request.
  - `get_language_name(lang)` (line 158): Get human-readable name for language code.
  - `detect_language_from_text(text)` (line 173): Simple language detection from text.
  - `fallback_languages(lang)` (line 196): Languages to try in order when a translation is missing.
  - `coerce_localized_text(value, default_lang)` (line 208): Coerce a LocalizedText-like value into a normalized language map.
  - `select_localized_text(value, lang)` (line 252): Choose the best string for the requested language from a LocalizedText-like input.
- **Classes**
  - `MultilingualText` (line 264): Utility class for handling multilingual text.
    - `__init__(self, **kwargs)` (line 269): Initialize with language-specific text.
    - `get(self, language, fallback)` (line 278): Get text for specific language.
    - `set(self, language, text)` (line 308): Set text for specific language.
    - `has_language(self, language)` (line 318): Check if text exists for language.
    - `get_languages(self)` (line 330): Get list of available languages.
    - `to_dict(self)` (line 339): Convert to dictionary.

### `backend/shared/utils/llm_safety.py`
- **Functions**
  - `sha256_hex(value)` (line 39): no docstring
  - `stable_json_dumps(obj)` (line 44): no docstring
  - `digest_for_audit(obj)` (line 48): no docstring
  - `truncate_text(text, max_chars)` (line 52): no docstring
  - `_mask_email(text)` (line 60): no docstring
  - `_mask_long_digits(text)` (line 69): no docstring
  - `mask_pii_text(text, max_chars)` (line 99): no docstring
  - `mask_pii(obj, max_string_chars)` (line 108): Recursively mask likely PII in a JSON-like structure.
  - `sample_items(items, max_items)` (line 131): no docstring
  - `build_agent_error_response(error_message, error_code, recoverable, hint, suggested_action, context)` (line 137): Build a standard error response format for the Agent.
  - `detect_value_pattern(value)` (line 192): Best-effort value pattern detection for raw-data reasoning.
  - `extract_column_value_patterns(values, max_samples)` (line 251): Summarize observed value patterns for a column.
  - `build_column_semantic_observations(column_name, values)` (line 322): Produce higher-level semantic hints for a column based on:
  - `_normalize_value_for_overlap(value)` (line 381): no docstring
  - `build_relationship_observations(left_column, right_column, left_values, right_values, max_examples)` (line 388): Compare two columns' values to produce FK/relationship hints.

### `backend/shared/utils/log_rotation.py`
- **Functions**
  - `create_default_rotation_manager(log_dir)` (line 314): Create log rotation manager with sensible defaults for test services
- **Classes**
  - `LogRotationManager` (line 17): 🔥 THINK ULTRA! Professional log rotation with compression and cleanup
    - `__init__(self, log_dir, max_size_mb, max_files, compress_after_days, delete_after_days)` (line 20): no docstring
    - `get_file_size(self, file_path)` (line 37): Get file size in bytes, handling errors gracefully
    - `get_file_age_days(self, file_path)` (line 44): Get file age in days
    - `should_rotate(self, log_file)` (line 53): Check if log file should be rotated based on size
    - `rotate_log_file(self, log_file, service_name)` (line 61): Rotate a log file by renaming it with timestamp and creating new one
    - `compress_old_logs(self)` (line 94): Compress log files older than compress_after_days
    - `cleanup_old_logs(self)` (line 133): Remove log files older than delete_after_days
    - `limit_rotated_files(self, service_name)` (line 172): Ensure we don't exceed max_files limit for rotated logs
    - `perform_maintenance(self, service_logs)` (line 211): Perform complete log maintenance: rotation, compression, cleanup
    - `get_log_directory_info(self)` (line 287): Get information about the log directory

### `backend/shared/utils/number_utils.py`
- **Functions**
  - `to_int_or_none(value)` (line 7): no docstring

### `backend/shared/utils/objectify_outputs.py`
- **Functions**
  - `match_output_name(output, name)` (line 8): no docstring

### `backend/shared/utils/ontology_type_normalization.py`
- **Functions**
  - `normalize_ontology_base_type(value)` (line 10): Normalize ontology/base type identifiers to a canonical token.

### `backend/shared/utils/ontology_version.py`
- **Functions**
  - `normalize_ontology_version(value)` (line 17): Normalize an ontology version payload.
  - `build_ontology_version(branch, commit)` (line 47): no docstring
  - `extract_ontology_version(envelope_metadata, envelope_data)` (line 55): Extract ontology_version stamp from either:
  - `split_ref_commit(value)` (line 74): no docstring
  - `async resolve_ontology_version(terminus, db_name, branch, logger)` (line 81): Best-effort ontology semantic contract stamp (ref + commit).

### `backend/shared/utils/path_utils.py`
- **Functions**
  - `safe_lakefs_ref(value)` (line 4): no docstring
  - `safe_path_segment(value)` (line 21): no docstring

### `backend/shared/utils/payload_utils.py`
- **Functions**
  - `unwrap_data_payload(payload)` (line 8): Return the inner data dict when payloads wrap data under `data`.

### `backend/shared/utils/principal_policy.py`
- **Functions**
  - `build_principal_tags(principal_type, principal_id, user_id, role)` (line 13): no docstring
  - `policy_allows(policy, principal_tags)` (line 37): Evaluate a minimal principal policy:
- **Classes**
  - `PrincipalPolicyError` (line 6): no docstring

### `backend/shared/utils/pythonpath_setup.py`
- **Functions**
  - `detect_backend_directory()` (line 12): Dynamically detect the backend directory by looking for characteristic files/directories.
  - `setup_pythonpath(backend_dir)` (line 45): Setup PYTHONPATH to include the backend directory.
  - `validate_pythonpath()` (line 95): Validate that PYTHONPATH is correctly configured by testing imports.
  - `configure_python_environment(backend_dir, verbose)` (line 113): Complete Python environment configuration including PYTHONPATH setup and validation.
  - `ensure_backend_in_path()` (line 162): Convenience function to ensure backend directory is in Python path.

### `backend/shared/utils/repo_dotenv.py`
- **Functions**
  - `load_repo_dotenv(keys)` (line 7): Load key/value pairs from the repo-root `.env`.

### `backend/shared/utils/resource_rid.py`
- **Functions**
  - `parse_metadata_rev(metadata)` (line 6): no docstring
  - `format_resource_rid(resource_type, resource_id, rev)` (line 17): no docstring
  - `strip_rid_revision(resource_rid)` (line 28): Extract the logical identifier portion from a RID that may include a revision suffix.

### `backend/shared/utils/s3_uri.py`
- **Functions**
  - `is_s3_uri(value)` (line 11): no docstring
  - `build_s3_uri(bucket, key)` (line 15): no docstring
  - `parse_s3_uri(uri)` (line 25): no docstring
  - `normalize_s3_uri(value, bucket)` (line 36): no docstring

### `backend/shared/utils/safe_bool_expression.py`
- **Functions**
  - `safe_eval_bool_expression(expression, variables, max_nodes)` (line 49): no docstring
  - `validate_bool_expression_syntax(expression, max_nodes)` (line 71): Validate a boolean expression for safety and syntax without evaluating it.
  - `_validate_bool_expression_ast(tree, max_nodes)` (line 89): no docstring
  - `_eval_bool_expression_node(node, variables)` (line 115): no docstring
  - `_apply_compare(left, op, right)` (line 202): no docstring
- **Classes**
  - `BoolExpressionError` (line 7): no docstring
  - `UnsafeBoolExpressionError` (line 11): no docstring
  - `BoolExpressionEvaluationError` (line 15): no docstring

### `backend/shared/utils/schema_columns.py`
- **Functions**
  - `extract_schema_columns(schema, strip_bom, dedupe)` (line 8): Extract a normalized list of column definitions from a schema payload.
  - `extract_schema_column_names(schema, strip_bom, dedupe)` (line 111): no docstring
  - `extract_schema_type_map(schema, strip_bom, dedupe, normalizer)` (line 125): no docstring

### `backend/shared/utils/schema_hash.py`
- **Functions**
  - `compute_schema_hash(columns)` (line 14): Produce a stable hash for a list of column definitions.
  - `compute_schema_hash_from_sample(sample_json)` (line 25): no docstring
  - `compute_schema_hash_from_payload(payload)` (line 34): no docstring

### `backend/shared/utils/schema_type_compatibility.py`
- **Functions**
  - `is_type_compatible(source_type, target_type)` (line 21): no docstring

### `backend/shared/utils/spice_event_ids.py`
- **Functions**
  - `spice_event_id(command_id, event_type, aggregate_id)` (line 6): Deterministic domain event id derived from a command id.

### `backend/shared/utils/sql_filter_builder.py`
- **Classes**
  - `SqlFilterBuilder` (line 8): no docstring
    - `add(self, condition_template, value)` (line 12): no docstring
    - `where(self, joiner, prefix)` (line 16): no docstring
    - `join(self, joiner)` (line 19): no docstring

### `backend/shared/utils/string_list_utils.py`
- **Functions**
  - `normalize_string_list(value)` (line 6): no docstring

### `backend/shared/utils/submission_criteria_diagnostics.py`
- **Functions**
  - `infer_submission_criteria_failure_reason(expression)` (line 7): Best-effort heuristics for classifying `submission_criteria` failures.

### `backend/shared/utils/terminus_branch.py`
- **Functions**
  - `encode_branch_name(branch_name)` (line 22): no docstring
  - `decode_branch_name(branch_name)` (line 31): no docstring

### `backend/shared/utils/time_utils.py`
- **Functions**
  - `utcnow()` (line 6): no docstring

### `backend/shared/utils/token_count.py`
- **Functions**
  - `approx_token_count(payload, empty_collections_as_zero)` (line 8): no docstring
  - `approx_token_count_json(payload, empty_collections_as_zero)` (line 20): no docstring

### `backend/shared/utils/uuid_utils.py`
- **Functions**
  - `safe_uuid(value)` (line 8): no docstring

### `backend/shared/utils/worker_runner.py`
- **Functions**
  - `async run_worker_until_stopped(worker, task_name, running_attr, shutdown_on_exit)` (line 22): no docstring
  - `async run_component_lifecycle(component, init_method, run_method, close_method)` (line 83): no docstring

### `backend/shared/utils/writeback_conflicts.py`
- **Functions**
  - `parse_conflict_policy(value)` (line 11): no docstring
  - `normalize_conflict_policy(value)` (line 18): no docstring
  - `extract_action_targets(input_payload)` (line 22): Minimal, code-aligned target format (P0):
  - `normalize_changes(target)` (line 44): no docstring
  - `_normalize_link_ops(raw)` (line 70): no docstring
  - `compute_observed_base(base, changes)` (line 96): no docstring
  - `compute_base_token(db_name, class_id, instance_id, lifecycle_id, base_doc, object_type_version_id)` (line 121): no docstring
  - `detect_overlap_fields(observed_base, current_base)` (line 149): Return field names whose current value differs from the observed_base snapshot.
  - `detect_overlap_links(observed_base, current_base, changes)` (line 165): Element-level link conflict detection (P0, best-effort).
  - `resolve_applied_changes(conflict_policy, changes, conflict_fields, conflict_links)` (line 209): Resolve an Action target's applied changes based on conflict_policy.

### `backend/shared/utils/writeback_governance.py`
- **Functions**
  - `extract_backing_dataset_id(object_type_spec)` (line 6): no docstring
  - `policies_aligned(backing_policy, writeback_policy)` (line 19): no docstring
  - `format_acl_alignment_result(scope, writeback_dataset_id, backing_dataset_id, backing_policy, writeback_policy)` (line 25): no docstring

### `backend/shared/utils/writeback_lifecycle.py`
- **Functions**
  - `derive_lifecycle_id(instance_state)` (line 8): Derive a stable lifecycle/epoch identifier for an instance.
  - `overlay_doc_id(instance_id, lifecycle_id)` (line 58): Build the ES `_id` for overlay documents as (instance_id, lifecycle_id).

### `backend/shared/utils/writeback_patch_apply.py`
- **Functions**
  - `apply_changes_to_payload(payload, changes)` (line 6): Apply a writeback patchset `changes` object to a payload dict.

### `backend/shared/utils/writeback_paths.py`
- **Functions**
  - `writeback_patchset_key(action_log_id)` (line 4): no docstring
  - `writeback_patchset_metadata_key(action_log_id)` (line 8): no docstring
  - `snapshot_manifest_key(snapshot_id)` (line 12): no docstring
  - `snapshot_object_key(snapshot_id, object_type, instance_id, lifecycle_id)` (line 17): no docstring
  - `snapshot_latest_pointer_key()` (line 31): no docstring
  - `queue_compaction_marker_key()` (line 35): no docstring
  - `queue_entry_key(object_type, instance_id, lifecycle_id, action_applied_seq, action_log_id)` (line 39): no docstring
  - `queue_entry_prefix(object_type, instance_id, lifecycle_id)` (line 53): no docstring
  - `ref_key(ref, key)` (line 68): no docstring

### `backend/shared/validators/__init__.py`
- **Functions**
  - `get_validator(data_type)` (line 70): Get validator instance for a specific data type
  - `register_validator(data_type, validator_class)` (line 86): Register a new validator
  - `get_composite_validator()` (line 97): Get a composite validator with all registered validators

### `backend/shared/validators/address_validator.py`
- **Classes**
  - `AddressValidator` (line 12): Validator for addresses
    - `_validate_string_address(self, value)` (line 52): Validate simple string address.
    - `_validate_required_fields(self, value, required_fields)` (line 65): Validate required fields in structured address.
    - `_validate_us_address(self, value)` (line 75): Validate US address components.
    - `_validate_canadian_address(self, value)` (line 102): Validate Canadian address components.
    - `_validate_uk_address(self, value)` (line 112): Validate UK address components.
    - `_validate_country_specific(self, value, country)` (line 124): Validate country-specific address components.
    - `_format_address(self, value)` (line 137): Format structured address for display.
    - `_validate_structured_address(self, value, constraints)` (line 149): Validate structured address.
    - `validate(self, value, constraints)` (line 184): Validate address using type-specific validators.
    - `normalize(self, value)` (line 198): Normalize address value
    - `get_supported_types(self)` (line 216): Get supported types

### `backend/shared/validators/array_validator.py`
- **Classes**
  - `ArrayValidator` (line 16): Validator for array/list data types
    - `validate(self, value, constraints)` (line 19): Validate array with constraints
    - `normalize(self, value)` (line 73): Normalize array value
    - `get_supported_types(self)` (line 87): Get supported types
    - `_validate_item_types(self, array, item_type)` (line 91): Validate types of array items
    - `_validate_basic_type(self, value, type_name)` (line 124): Validate basic types

### `backend/shared/validators/base_validator.py`
- **Classes**
  - `ValidationResult` (line 11): Result of validation operation
    - `error(self)` (line 20): Get error message if validation failed
    - `to_tuple(self)` (line 24): Convert to legacy tuple format for backward compatibility
  - `BaseValidator` (line 29): Abstract base class for validators
    - `validate(self, value, constraints)` (line 33): Validate a value against constraints
    - `normalize(self, value)` (line 49): Normalize a value to standard format
    - `is_supported_type(self, data_type)` (line 61): Check if this validator supports the given data type
    - `get_supported_types(self)` (line 74): Get list of supported data types
    - `get_type_info(self)` (line 83): Get information about this validator
  - `CompositeValidator` (line 97): Validator that combines multiple validators
    - `__init__(self, validators)` (line 100): no docstring
    - `validate(self, value, constraints)` (line 107): Validate using appropriate sub-validator
    - `normalize(self, value)` (line 122): Normalize is not applicable for composite validator
    - `get_supported_types(self)` (line 126): Get all supported types from sub-validators
    - `add_validator(self, validator)` (line 130): Add a new validator

### `backend/shared/validators/cipher_validator.py`
- **Classes**
  - `CipherValidator` (line 13): Validator for cipher text values.
    - `validate(self, value, constraints)` (line 16): no docstring
    - `normalize(self, value)` (line 31): no docstring
    - `get_supported_types(self)` (line 34): no docstring

### `backend/shared/validators/complex_type_validator.py`
- **Classes**
  - `ComplexTypeConstraints` (line 14): Constraints for complex type validation.
    - `array_constraints(cls, min_items, max_items, unique_items, item_type)` (line 29): Create constraints for array validation.
    - `object_constraints(cls, schema, required, additional_properties)` (line 49): Create constraints for object validation.
    - `enum_constraints(cls, allowed_values)` (line 64): Create constraints for enum validation.
    - `money_constraints(cls, min_amount, max_amount, decimal_places, allowed_currencies)` (line 69): Create constraints for money validation.
    - `phone_constraints(cls, default_region, allowed_regions)` (line 87): Create constraints for phone validation.
    - `email_constraints(cls, allowed_domains)` (line 97): Create constraints for email validation.
    - `coordinate_constraints(cls, precision, bounding_box)` (line 105): Create constraints for coordinate validation.
    - `address_constraints(cls, required_fields, default_country)` (line 115): Create constraints for address validation.
    - `image_constraints(cls, allowed_extensions, allowed_domains, require_extension)` (line 125): Create constraints for image validation.
    - `file_constraints(cls, max_size, allowed_extensions)` (line 140): Create constraints for file validation.
    - `string_constraints(cls, min_length, max_length, pattern, format)` (line 152): Create constraints for string validation.
  - `ComplexTypeValidator` (line 172): Refactored validator that delegates to specialized validators.
    - `validate(cls, value, data_type, constraints)` (line 200): Validate a value against a complex data type with constraints.
    - `_validate_string_type(cls, value)` (line 249): Validate string type.
    - `_validate_integer_type(cls, value, data_type)` (line 256): Validate integer types (int, long, short, byte).
    - `_validate_unsigned_integer_type(cls, value, data_type)` (line 263): Validate unsigned integer types.
    - `_validate_float_type(cls, value)` (line 272): Validate float/double types.
    - `_validate_boolean_type(cls, value)` (line 279): Validate boolean type.
    - `_validate_decimal_type(cls, value)` (line 286): Validate decimal type.
    - `_validate_date_type(cls, value)` (line 294): Validate date type.
    - `_validate_datetime_type(cls, value)` (line 307): Validate datetime type.
    - `_validate_uri_type(cls, value)` (line 320): Validate URI type.
    - `_get_type_validator_map(cls)` (line 332): Get mapping of data types to their validator functions.
    - `_validate_xsd_type(cls, value, data_type, constraints)` (line 354): Validate XSD data types using type-specific validators.
    - `get_supported_types(cls)` (line 367): Get list of supported complex types.
    - `is_supported_type(cls, data_type)` (line 400): Check if data type is supported.

### `backend/shared/validators/constraint_validator.py`
- **Classes**
  - `ConstraintValidator` (line 14): Enhanced constraint validation for complex types
    - `validate_constraints(cls, value, data_type, constraints)` (line 18): Validate a value against a set of constraints
    - `_validate_string_constraints(cls, value, constraints)` (line 73): Validate string-specific constraints
    - `_validate_numeric_constraints(cls, value, constraints)` (line 109): Validate numeric constraints
    - `_validate_collection_constraints(cls, value, constraints)` (line 160): Validate collection constraints
    - `_validate_format(cls, value, format_name)` (line 210): Validate common format constraints
    - `_validate_pattern(cls, value, pattern)` (line 248): Validate against regex pattern
    - `_validate_custom(cls, value, validator_func)` (line 261): Validate using custom validation function
    - `merge_constraints(cls, *constraint_sets)` (line 284): Merge multiple constraint sets with proper precedence
    - `validate_constraint_compatibility(cls, constraints)` (line 297): Check if constraints are compatible with each other

### `backend/shared/validators/coordinate_validator.py`
- **Classes**
  - `CoordinateValidator` (line 11): Validator for geographic coordinates
    - `validate(self, value, constraints)` (line 14): Validate coordinate
    - `normalize(self, value)` (line 99): Normalize coordinate value
    - `get_supported_types(self)` (line 106): Get supported types
    - `calculate_distance(cls, coord1, coord2)` (line 111): Calculate distance between two coordinates using Haversine formula

### `backend/shared/validators/email_validator.py`
- **Classes**
  - `EmailValidator` (line 16): Validator for email addresses
    - `validate(self, value, constraints)` (line 22): Validate email address
    - `normalize(self, value)` (line 68): Normalize email address
    - `get_supported_types(self)` (line 80): Get supported types
    - `is_valid_email(cls, email)` (line 85): Quick check if email is valid

### `backend/shared/validators/enum_validator.py`
- **Classes**
  - `EnumValidator` (line 11): Validator for enum data types
    - `validate(self, value, constraints)` (line 14): Validate enum value
    - `normalize(self, value)` (line 43): Normalize enum value
    - `get_supported_types(self)` (line 48): Get supported types
    - `create_constraints(cls, allowed_values)` (line 53): Create enum constraints

### `backend/shared/validators/file_validator.py`
- **Classes**
  - `FileValidator` (line 15): Validator for file URLs and metadata
    - `validate(self, value, constraints)` (line 57): Validate file
    - `normalize(self, value)` (line 159): Normalize file value
    - `get_supported_types(self)` (line 165): Get supported types
    - `_get_file_type(self, extension)` (line 169): Determine file type from extension
    - `_format_file_size(self, size_bytes)` (line 192): Format file size in human-readable format

### `backend/shared/validators/geopoint_validator.py`
- **Classes**
  - `GeoPointValidator` (line 17): Validator for geopoint values (lat,lon or geohash).
    - `validate(self, value, constraints)` (line 20): no docstring
    - `normalize(self, value)` (line 73): no docstring
    - `get_supported_types(self)` (line 78): no docstring

### `backend/shared/validators/geoshape_validator.py`
- **Functions**
  - `_coordinates_valid(value)` (line 79): no docstring
- **Classes**
  - `GeoShapeValidator` (line 26): Validator for GeoJSON geometry payloads.
    - `validate(self, value, constraints)` (line 29): no docstring
    - `normalize(self, value)` (line 72): no docstring
    - `get_supported_types(self)` (line 75): no docstring

### `backend/shared/validators/google_sheets_validator.py`
- **Classes**
  - `GoogleSheetsValidator` (line 11): Validator for Google Sheets URLs
    - `validate(self, value, constraints)` (line 17): Validate Google Sheets URL
    - `normalize(self, value)` (line 86): Normalize Google Sheets URL
    - `get_supported_types(self)` (line 106): Get supported types

### `backend/shared/validators/image_validator.py`
- **Classes**
  - `ImageValidator` (line 12): Validator for image URLs
    - `validate(self, value, constraints)` (line 29): Validate image URL
    - `normalize(self, value)` (line 135): Normalize image URL
    - `get_supported_types(self)` (line 143): Get supported types
    - `is_image_extension(cls, filename)` (line 148): Check if filename has image extension

### `backend/shared/validators/ip_validator.py`
- **Classes**
  - `IpValidator` (line 14): Validator for IP addresses
    - `validate(self, value, constraints)` (line 23): Validate IP address
    - `normalize(self, value)` (line 118): Normalize IP address
    - `get_supported_types(self)` (line 134): Get supported types

### `backend/shared/validators/marking_validator.py`
- **Classes**
  - `MarkingValidator` (line 13): Validator for marking values.
    - `validate(self, value, constraints)` (line 16): no docstring
    - `normalize(self, value)` (line 31): no docstring
    - `get_supported_types(self)` (line 34): no docstring

### `backend/shared/validators/money_validator.py`
- **Classes**
  - `MoneyValidator` (line 17): Validator for money/currency data types
    - `_normalize_currency_token(cls, token, constraints)` (line 92): Normalize a currency token (symbol/code/unit) into an ISO 4217 code.
    - `validate(self, value, constraints)` (line 128): Validate money value
    - `_normalize_number_string(cls, raw)` (line 300): Normalize a locale-variant numeric string into a plain decimal representation.
    - `normalize(self, value)` (line 339): Normalize money value
    - `get_supported_types(self)` (line 346): Get supported types
    - `is_valid_currency(cls, currency)` (line 351): Check if currency code is valid

### `backend/shared/validators/name_validator.py`
- **Classes**
  - `NamingConvention` (line 13): Supported naming conventions
  - `NameValidator` (line 28): Validator for various naming conventions and patterns
    - `validate(self, value, constraints)` (line 56): Validate name according to constraints
    - `_detect_convention(self, value)` (line 152): Detect the naming convention used
    - `normalize(self, value)` (line 159): Normalize name
    - `convert_convention(self, value, from_convention, to_convention)` (line 167): Convert between naming conventions
    - `get_supported_types(self)` (line 208): Get supported types

### `backend/shared/validators/object_validator.py`
- **Classes**
  - `ObjectValidator` (line 15): Validator for object/JSON data types
    - `validate(self, value, constraints)` (line 18): Validate object/JSON with constraints
    - `normalize(self, value)` (line 86): Normalize object value
    - `get_supported_types(self)` (line 100): Get supported types
    - `_validate_field_type(self, value, field_type, constraints)` (line 104): Validate individual field type

### `backend/shared/validators/phone_validator.py`
- **Classes**
  - `PhoneValidator` (line 19): Validator for phone numbers
    - `validate(self, value, constraints)` (line 25): Validate phone number
    - `normalize(self, value)` (line 81): Normalize phone number
    - `get_supported_types(self)` (line 89): Get supported types
    - `format_phone(cls, phone, format_type)` (line 94): Format phone number

### `backend/shared/validators/string_validator.py`
- **Classes**
  - `StringValidator` (line 10): Validator for strings with constraints
    - `validate(self, value, constraints)` (line 13): Validate string with constraints
    - `normalize(self, value)` (line 99): Normalize string
    - `get_supported_types(self)` (line 107): Get supported types

### `backend/shared/validators/struct_validator.py`
- **Classes**
  - `StructValidator` (line 13): Validator for struct values (flat object without nested arrays/objects).
    - `validate(self, value, constraints)` (line 16): no docstring
    - `normalize(self, value)` (line 47): no docstring
    - `get_supported_types(self)` (line 50): no docstring

### `backend/shared/validators/url_validator.py`
- **Classes**
  - `UrlValidator` (line 15): Validator for URL strings
    - `validate(self, value, constraints)` (line 21): Validate URL
    - `normalize(self, value)` (line 105): Normalize URL
    - `get_supported_types(self)` (line 113): Get supported types

### `backend/shared/validators/uuid_validator.py`
- **Classes**
  - `UuidValidator` (line 15): Validator for UUIDs
    - `validate(self, value, constraints)` (line 21): Validate UUID
    - `normalize(self, value)` (line 109): Normalize UUID
    - `get_supported_types(self)` (line 125): Get supported types

### `backend/shared/validators/vector_validator.py`
- **Classes**
  - `VectorValidator` (line 16): Validator for numeric vector payloads.
    - `validate(self, value, constraints)` (line 19): no docstring
    - `normalize(self, value)` (line 70): no docstring
    - `get_supported_types(self)` (line 73): no docstring

## tests

### `backend/tests/__init__.py`

### `backend/tests/chaos_lite.py`
- **Functions**
  - `_read_env_file(path)` (line 35): no docstring
  - `_get_env_value(key)` (line 51): no docstring
  - `_require_token(keys)` (line 60): no docstring
  - `_run(cmd, cwd, env)` (line 98): no docstring
  - `_run_input(cmd, cwd, input_text, env)` (line 114): no docstring
  - `_docker_compose(args, extra_env)` (line 130): no docstring
  - `_http_json(method, url, payload, timeout_s)` (line 137): no docstring
  - `_wait_until(name, fn, timeout_s, interval_s)` (line 155): no docstring
  - `_wait_http_ok(url, timeout_s)` (line 167): no docstring
  - `_wait_db_exists(db_name, expected, timeout_s)` (line 175): no docstring
  - `_wait_ontology(db_name, class_id, timeout_s)` (line 188): no docstring
  - `_wait_command_completed(command_id, timeout_s)` (line 201): no docstring
  - `_instances_index(db_name)` (line 216): no docstring
  - `_wait_es_doc(index, doc_id, timeout_s)` (line 221): no docstring
  - `_graph_query(db_name, product_id, include_provenance)` (line 235): no docstring
  - `_assert_graph_full(result)` (line 255): no docstring
  - `_domain_event_id(command_id, event_type, aggregate_id)` (line 265): no docstring
  - `_check_s3_has_event_id(event_id)` (line 269): no docstring
  - `_wait_s3_has_event_id(event_id, timeout_s)` (line 289): no docstring
  - `_read_s3_event_envelope(event_id)` (line 297): no docstring
  - `_psql_scalar(sql)` (line 319): no docstring
  - `_write_side_last_sequence(aggregate_type, aggregate_id)` (line 340): no docstring
  - `_assert_registry_done(handler, event_id)` (line 351): no docstring
  - `_assert_registry_status(handler, event_id, expected_status)` (line 366): no docstring
  - `_wait_registry_done(handler, event_id, timeout_s)` (line 378): no docstring
  - `_wait_registry_status(handler, event_id, expected_status, timeout_s)` (line 386): no docstring
  - `_kafka_produce_json(topic, key, payload)` (line 396): no docstring
  - `_setup_db_and_ontologies()` (line 420): no docstring
  - `_create_customer_and_product(db_name, customer_id, product_id, wait_command)` (line 462): no docstring
  - `_assert_converged(db_name, customer_id, product_id, customer_command_id, product_command_id, retry_expected)` (line 498): no docstring
  - `scenario_kafka_down_then_recover()` (line 556): no docstring
  - `scenario_redis_down_then_recover()` (line 588): no docstring
  - `scenario_es_down_then_recover()` (line 610): no docstring
  - `scenario_terminus_down_then_recover()` (line 642): no docstring
  - `scenario_instance_worker_crash_after_claim()` (line 674): no docstring
  - `scenario_out_of_order_delivery()` (line 712): no docstring
  - `scenario_soak_random_failures(duration_s, seed)` (line 796): Soak test with real infra + random partial failures (no mocks).
  - `main()` (line 887): no docstring
- **Classes**
  - `Endpoints` (line 72): no docstring

### `backend/tests/conftest.py`
- **Functions**
  - `_load_repo_dotenv()` (line 10): no docstring
  - `_ensure_repo_root_on_sys_path()` (line 14): Ensure tests can import using either:
  - `_env_or_dotenv(dotenv, key, default)` (line 27): no docstring
  - `pytest_configure()` (line 31): Host-run integration defaults for the `backend/tests` suite.

### `backend/tests/connectors/__init__.py`

### `backend/tests/integration/test_pipeline_branch_lifecycle.py`
- **Functions**
  - `async test_pipeline_branch_lifecycle(monkeypatch)` (line 12): no docstring

### `backend/tests/test_access_policy_link_indexing_e2e.py`
- **Functions**
  - `_load_repo_dotenv()` (line 39): no docstring
  - `_ensure_lakefs_credentials()` (line 60): no docstring
  - `_lakefs_admin_credentials()` (line 84): no docstring
  - `async _ensure_lakefs_repository(repository, branch)` (line 98): no docstring
  - `async _release_stale_processing_events(max_age_seconds)` (line 138): no docstring
  - `async _cleanup_stale_processing_events()` (line 160): no docstring
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 164): no docstring
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 184): no docstring
  - `async _wait_for_ontology_present(session, db_name, ontology_id, branch, timeout_seconds, poll_interval_seconds)` (line 209): no docstring
  - `async _get_head_commit(session, db_name, branch)` (line 236): no docstring
  - `async _create_db(session, db_name)` (line 248): no docstring
  - `async _create_ontology(session, db_name, ontology, branch)` (line 261): no docstring
  - `async _create_branch(session, db_name, branch, from_branch)` (line 286): no docstring
  - `async _checkout_branch(session, db_name, branch)` (line 300): no docstring
  - `async _wait_for_instance_count(session, db_name, class_id, expected_count, timeout_seconds, poll_interval_seconds)` (line 313): no docstring
  - `async _create_instance(session, db_name, class_id, payload, branch)` (line 338): no docstring
  - `_render_csv(headers, rows)` (line 358): no docstring
  - `async _create_dataset_with_artifact(dataset_registry, db_name, name, columns, rows, branch)` (line 368): no docstring
  - `async _wait_for_link_index_status(session, db_name, link_type_id, branch, expected_status, timeout_seconds, poll_interval_seconds)` (line 412): no docstring
  - `async _wait_for_relationship(session, db_name, class_id, pk_field, pk_value, predicate, branch, timeout_seconds, poll_interval_seconds)` (line 442): no docstring
  - `async _wait_for_instance_edits(dataset_registry, db_name, class_id, min_count, timeout_seconds, poll_interval_seconds)` (line 479): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 497): no docstring
  - `async _delete_db_best_effort(session, db_name)` (line 552): no docstring
  - `async test_access_policy_filters_and_masks_query_results()` (line 572): no docstring
  - `async test_link_indexing_updates_relationships_and_status()` (line 667): no docstring
  - `async test_object_type_migration_requires_edit_reset()` (line 870): no docstring

### `backend/tests/test_action_writeback_e2e_smoke.py`
- **Functions**
  - `_load_repo_dotenv()` (line 46): no docstring
  - `_truthy(value)` (line 50): no docstring
  - `_port_from_env(env, key, fallback)` (line 54): no docstring
  - `_require_env(env, key, allow_empty)` (line 69): no docstring
  - `_base_headers(db_name, actor_id)` (line 83): no docstring
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 97): no docstring
  - `_extract_command_id(payload)` (line 127): no docstring
  - `async _get_branch_head_commit(session, db_name, branch, headers)` (line 138): no docstring
  - `async _record_deployed_commit(db_name, target_branch, ontology_commit_id)` (line 162): no docstring
  - `async _wait_for_action_log(session, db_name, action_log_id, headers, timeout_seconds, poll_interval_seconds)` (line 188): no docstring
  - `_pick_first_lifecycle_id(action_log_payload)` (line 216): no docstring
  - `async _wait_for_es_overlay_doc(es_base_url, index_name, doc_id, timeout_seconds, poll_interval_seconds)` (line 230): no docstring
  - `async _start_action_worker(env, backend_dir)` (line 266): no docstring
  - `async _run_subprocess(*args, cwd, env, timeout_seconds)` (line 283): no docstring
  - `async _docker_container_running(container_name)` (line 307): no docstring
  - `_smoke_worker_mode()` (line 319): no docstring
  - `async _ensure_action_worker_docker_running(repo_root, build)` (line 329): Ensure action-worker is running in Docker and return whether the test started it.
  - `async _docker_container_health(container_name)` (line 357): no docstring
  - `async _wait_for_container_healthy(container_name, timeout_seconds, poll_interval_seconds)` (line 372): no docstring
  - `async _ensure_kafka_docker_running(repo_root)` (line 388): Ensure Kafka is running before submitting commands.
  - `async _stop_action_worker_docker(repo_root)` (line 425): no docstring
  - `async _stop_process(proc, timeout_seconds)` (line 442): no docstring
  - `_es_base_url_from_dotenv(env)` (line 460): no docstring
  - `_action_worker_env_from_dotenv(dotenv)` (line 466): no docstring
  - `_apply_postgres_env_for_registry(dotenv)` (line 496): no docstring
  - `_apply_event_store_env(dotenv)` (line 505): no docstring
  - `_apply_lakefs_env(dotenv)` (line 512): no docstring
  - `_pick_first_lifecycle_id_any(action_log_payload)` (line 522): no docstring
  - `async _update_instance_ingest(session, db_name, base_branch, class_id, instance_id, expected_seq, patch, headers)` (line 548): no docstring
  - `async _assert_action_applied_event_in_event_store(action_applied_event_id)` (line 580): no docstring
  - `async test_action_writeback_e2e_smoke()` (line 594): no docstring
  - `async test_action_writeback_e2e_verification_suite()` (line 848): Verification suite for Action writeback behavior (ACTION_WRITEBACK_DESIGN.md):

### `backend/tests/test_auth_hardening_e2e.py`
- **Functions**
  - `_strip_auth_env(env)` (line 25): no docstring
  - `_run_auth_check(module, func, env)` (line 42): no docstring
  - `test_auth_disabled_requires_explicit_allow()` (line 51): no docstring
  - `async test_oms_write_requires_auth()` (line 65): no docstring

### `backend/tests/test_branch_virtualization_e2e.py`
- **Functions**
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 54): Best-effort: fetch current write-side sequence for OCC cleanup (returns None if Postgres unavailable).
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 109): no docstring
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 129): no docstring
  - `async _wait_for_ontology_present(session, db_name, ontology_id, timeout_seconds, poll_interval_seconds)` (line 154): no docstring
  - `async _wait_for_graph_node(session, db_name, branch, class_id, primary_key_value, expected_name, require_overlay_index, timeout_seconds, poll_interval_seconds)` (line 175): Poll BFF graph query until a single node is returned and ES enrichment is ready.
  - `async test_branch_virtualization_overlay_copy_on_write()` (line 253): no docstring

### `backend/tests/test_command_status_ttl_e2e.py`
- **Functions**
  - `_set_env(**updates)` (line 22): no docstring
  - `_redis_params()` (line 39): no docstring
  - `async test_command_status_ttl_configurable()` (line 58): no docstring

### `backend/tests/test_consistency_e2e_smoke.py`
- **Functions**
  - `_kafka_admin()` (line 65): no docstring
  - `_auth_headers()` (line 69): Get auth headers for BFF requests.
  - `_extract_constant_dict(node)` (line 74): Extract a best-effort {str: constant} mapping from an ast.Dict node.
  - `_extract_constant_kwargs(node)` (line 87): Extract a best-effort {str: constant} mapping from an ast.Call keyword list.
- **Classes**
  - `TestSafeKafkaConsumerConfiguration` (line 105): Verify SafeKafkaConsumer enforces critical settings.
    - `test_enforced_isolation_level(self)` (line 108): SafeKafkaConsumer should always use read_committed.
    - `test_enforced_auto_commit_disabled(self)` (line 115): SafeKafkaConsumer should disable auto-commit.
    - `test_workers_use_safe_consumer(self)` (line 122): Verify all Kafka-consuming workers use SafeKafkaConsumer (no raw Consumer).
    - `test_no_raw_consumer_in_worker_tree(self)` (line 173): Hard-fail if any *_worker module imports raw Consumer (regression guard).
  - `TestKafkaProducerConfiguration` (line 238): Verify critical producer-side idempotence settings are present where required.
    - `test_pipeline_job_queue_producer_is_idempotent(self)` (line 241): no docstring
    - `test_connector_trigger_producer_is_idempotent(self)` (line 287): no docstring
  - `TestKafkaConnectivity` (line 339): Verify Kafka connectivity and topic access.
    - `test_kafka_topics_exist(self)` (line 343): Verify all declared Kafka topics exist (SSoT: AppConfig.get_all_topics).
    - `test_kafka_consumer_groups_listable(self)` (line 356): Verify consumer groups can be listed (connectivity + broker feature check).
    - `test_expected_consumer_groups_present(self)` (line 366): Verify critical worker consumer groups exist (proxy for "workers are actually running").
    - `test_read_committed_filters_aborted_transactions(self)` (line 418): End-to-end verification: read_committed consumers must NOT see aborted transactional messages.
  - `TestBFFHealth` (line 511): Verify BFF is running and healthy.
    - `async test_bff_health_endpoint(self)` (line 516): BFF health endpoint should return success.
    - `async test_bff_databases_connected(self)` (line 527): BFF should have database connections.
  - `TestMSAServiceHealth` (line 541): Verify all first-class services are reachable (full stack).
    - `async test_oms_health_endpoint(self)` (line 546): no docstring
    - `async test_funnel_health_endpoint(self)` (line 553): no docstring
    - `async test_agent_health_endpoint(self)` (line 560): no docstring
    - `async test_ingest_reconciler_health_endpoint(self)` (line 567): no docstring
  - `TestDatabaseConnectivity` (line 578): Verify database connectivity.
    - `async test_postgres_connection(self)` (line 583): PostgreSQL should be connectable.
    - `async test_redis_connection(self)` (line 600): Redis should be connectable and require authentication (prod safety).
  - `TestInfraConnectivity` (line 655): Verify non-DB infra dependencies (S3/MinIO, lakeFS, Elasticsearch, TerminusDB).
    - `async test_minio_health(self)` (line 660): no docstring
    - `async test_lakefs_health(self)` (line 667): no docstring
    - `async test_elasticsearch_health(self)` (line 676): no docstring
    - `test_terminusdb_port_open(self)` (line 686): no docstring
  - `TestSystemWiring` (line 697): Verify producer/consumer wiring between key MSAs (no mocks).
    - `async test_message_relay_publishes_event_store_appends(self)` (line 707): Message relay must publish newly appended Event Store events to Kafka.
    - `async test_connector_trigger_publishes_outbox(self)` (line 781): Connector trigger service must publish pending outbox items to Kafka.
    - `async test_pipeline_scheduler_enqueues_scheduled_pipeline(self)` (line 835): Pipeline scheduler must enqueue due scheduled pipelines to Kafka.
    - `async test_action_outbox_emits_action_applied_event(self)` (line 916): Action outbox worker must emit ActionApplied into the event store, and the relay must publish it to Kafka.
  - `TestOutboxPatternVerification` (line 1039): Verify outbox pattern implementation.
    - `test_objectify_outbox_uses_aggregate_key(self)` (line 1042): Objectify outbox should scope ordering by run_id when present.
    - `test_objectify_outbox_tracks_delivery(self)` (line 1057): Objectify outbox should track individual delivery status.
    - `test_message_relay_prefers_ordering_key(self)` (line 1070): Message relay should prefer ordering_key for Kafka partitioning when present.
  - `TestProcessedEventRegistryIdempotency` (line 1085): Verify ProcessedEventRegistry provides idempotency.
    - `async test_claim_idempotency(self)` (line 1090): ProcessedEventRegistry.claim should be idempotent.
    - `async test_sequence_ordering(self)` (line 1124): ProcessedEventRegistry should enforce sequence ordering.
  - `TestPipelineJobQueue` (line 1178): Verify PipelineJobQueue implementation.
    - `test_pipeline_job_has_dedupe_key(self)` (line 1181): PipelineJob should auto-generate dedupe_key.
    - `test_pipeline_job_queue_uses_pipeline_id_key(self)` (line 1195): PipelineJobQueue should use pipeline_id as partition key.
  - `TestEndToEndFlowSimulation` (line 1209): Simulate E2E flows without actually processing jobs.
    - `async test_objectify_job_creation_flow(self)` (line 1213): Test creating an objectify job payload.
    - `async test_pipeline_job_creation_flow(self)` (line 1241): Test creating a pipeline job payload.
  - `TestWorkerModuleImports` (line 1276): Verify workers can be imported without errors.
    - `test_objectify_worker_import(self)` (line 1279): Objectify worker should be importable.
    - `test_pipeline_worker_import(self)` (line 1284): Pipeline worker should be importable.
    - `test_action_worker_import(self)` (line 1289): Action worker should be importable.
    - `test_connector_sync_worker_import(self)` (line 1294): Connector sync worker should be importable.
    - `test_ontology_worker_import(self)` (line 1299): Ontology worker should be importable.
    - `test_instance_worker_import(self)` (line 1304): Instance worker should be importable.
    - `test_projection_worker_import(self)` (line 1309): Projection worker should be importable.
    - `test_action_outbox_worker_import(self)` (line 1314): Action outbox worker should be importable.
    - `test_ingest_reconciler_worker_import(self)` (line 1319): Ingest reconciler worker should be importable.
    - `test_writeback_materializer_worker_import(self)` (line 1324): Writeback materializer worker should be importable.
    - `test_connector_trigger_service_import(self)` (line 1329): Connector trigger service should be importable.
    - `test_pipeline_scheduler_import(self)` (line 1334): Pipeline scheduler should be importable.
    - `test_message_relay_import(self)` (line 1339): Message relay should be importable.
  - `TestConsistencySummary` (line 1350): Summary tests for all consistency features.
    - `test_all_critical_settings_enforced(self)` (line 1353): All critical Kafka settings should be enforced.
    - `test_idempotency_patterns_present(self)` (line 1365): Idempotency patterns should be present.
    - `test_occ_support_present(self)` (line 1383): OCC support should be present in PipelineRegistry.

### `backend/tests/test_core_functionality.py`
- **Functions**
  - `_get_postgres_url_candidates()` (line 30): Return Postgres DSN candidates (env override first, then common local ports).
  - `async _resolve_bff_path(session, candidates)` (line 55): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 74): Fetch the current write-side sequence for an aggregate from Postgres.
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 138): no docstring
  - `async _wait_for_command_terminal_state(session, command_id, timeout_seconds, poll_interval_seconds)` (line 159): Wait until an async (202 Accepted) command reaches a terminal state.
  - `async _wait_for_ontology_present(session, db_name, ontology_id, branch, timeout_seconds, poll_interval_seconds)` (line 194): no docstring
  - `async _wait_for_es_doc(session, index_name, doc_id, timeout_seconds, poll_interval_seconds)` (line 220): no docstring
- **Classes**
  - `TestCoreOntologyManagement` (line 247): Test suite for Ontology Management Service
    - `async test_database_lifecycle(self)` (line 252): Test complete database lifecycle with Event Sourcing
    - `async test_ontology_creation(self)` (line 293): Test ontology creation with complex types
    - `async test_ontology_i18n_label_projection(self)` (line 378): Ensure i18n labels are normalized for ES and preserved in label_i18n.
    - `async test_ontology_creation_advanced_relationships(self)` (line 510): Test advanced ontology creation path is truly event-sourced and functional.
  - `TestBFFGraphFederation` (line 620): Test suite for BFF Graph Federation capabilities
    - `async test_schema_suggestion(self)` (line 625): Test ML-driven schema suggestion
    - `async test_graph_query_federation(self)` (line 668): Test federated graph queries with Elasticsearch
  - `TestEventSourcingInfrastructure` (line 681): Test Event Sourcing and CQRS infrastructure
    - `async test_s3_event_storage(self)` (line 689): Verify S3/MinIO event storage is working
    - `async test_postgresql_processed_event_registry(self)` (line 736): Verify Postgres processed_events registry is available (idempotency contract)
    - `async test_kafka_message_flow(self)` (line 800): Verify Kafka message flow is operational
  - `TestComplexTypes` (line 823): Test complex type validation and handling
    - `test_email_validation(self)` (line 827): Test email type validation
    - `test_phone_validation(self)` (line 843): Test phone number validation
    - `test_json_validation(self)` (line 854): Test JSON type validation
  - `TestHealthEndpoints` (line 871): Test all service health endpoints
    - `async test_oms_health(self)` (line 876): Test OMS health endpoint
    - `async test_bff_health(self)` (line 886): Test BFF health endpoint
    - `async test_funnel_health(self)` (line 896): Test Funnel health endpoint

### `backend/tests/test_critical_fixes_e2e.py`
- **Functions**
  - `_docker(*args)` (line 28): no docstring
  - `_match_container(names, candidates)` (line 41): no docstring
  - `_resolve_redis_container()` (line 56): no docstring
  - `_auth_headers()` (line 72): no docstring
  - `_resolve_postgres_container()` (line 76): no docstring
  - `async _wait_for_ok(session, url, timeout_seconds)` (line 92): no docstring
  - `async _wait_for_command_status_ok(session, command_id, timeout_seconds)` (line 107): no docstring
  - `async _redis_down()` (line 128): no docstring
  - `async _postgres_down()` (line 139): no docstring
  - `async test_config_monitor_current_returns_payload()` (line 151): no docstring
  - `async test_openapi_excludes_wip_projections()` (line 167): no docstring
  - `async test_i18n_translates_health_description()` (line 183): no docstring
  - `async test_rate_limit_headers_present_on_success()` (line 201): no docstring
  - `async test_redis_down_rate_limit_and_command_status_fallback()` (line 215): no docstring
  - `async test_bff_sensitive_get_requires_auth()` (line 256): no docstring
  - `async test_command_status_dual_outage_returns_503()` (line 273): no docstring

### `backend/tests/test_event_store_tls_guard.py`
- **Functions**
  - `_set_env(**updates)` (line 16): no docstring
  - `async test_event_store_tls_requirement()` (line 35): no docstring

### `backend/tests/test_financial_investigation_workflow_e2e.py`
- **Functions**
  - `_postgres_url_candidates()` (line 30): no docstring
  - `async _grant_db_role(db_name, principal_id, role, principal_type)` (line 40): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds)` (line 84): no docstring
  - `async _create_db_with_retry(client, db_name)` (line 107): no docstring
  - `async _wait_for_ontology(client, db_name, class_id, branch, timeout_seconds)` (line 120): no docstring
  - `async _wait_for_es_doc(client, index_name, doc_id, timeout_seconds)` (line 142): no docstring
  - `_extract_funnel_types(payload)` (line 166): no docstring
  - `_build_transactions_xlsx_bytes()` (line 185): no docstring
  - `async test_financial_investigation_workflow_e2e()` (line 210): no docstring

### `backend/tests/test_idempotency_chaos.py`
- **Functions**
  - `_set_env(**updates)` (line 27): no docstring
  - `_get_postgres_url_candidates()` (line 44): no docstring
  - `async _make_registry(dsn, schema, lease_timeout_seconds)` (line 58): no docstring
  - `async _truncate(reg, schema)` (line 81): no docstring
  - `async test_registry_duplicate_delivery_causes_one_side_effect()` (line 90): no docstring
  - `async test_registry_reclaims_stuck_processing_after_lease_timeout()` (line 150): no docstring
  - `async test_registry_concurrent_claim_has_single_winner()` (line 223): no docstring
  - `async test_registry_mark_failed_owner_mismatch_raises()` (line 273): no docstring
  - `async test_registry_sequence_guard_is_monotonic()` (line 302): no docstring
  - `async test_command_status_endpoint_exposes_failure_reason()` (line 383): no docstring
  - `test_event_store_rejects_event_id_reuse_with_different_command_payload()` (line 413): no docstring
- **Classes**
  - `_FakeRedisService` (line 353): no docstring
    - `__init__(self)` (line 354): no docstring
    - `async set_command_status(self, command_id, status, data, ttl)` (line 358): no docstring
    - `async get_command_status(self, command_id)` (line 365): no docstring
    - `async set_command_result(self, command_id, result, ttl)` (line 368): no docstring
    - `async get_command_result(self, command_id)` (line 371): no docstring
    - `async publish_command_update(self, _command_id, _data)` (line 374): no docstring
    - `async keys(self, _pattern)` (line 377): no docstring

### `backend/tests/test_oms_smoke.py`
- **Functions**
  - `async _assert_command_event_has_ontology_stamp(event_id)` (line 42): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 58): Best-effort: fetch current write-side sequence for OCC cleanup (returns None if Postgres unavailable).
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 113): no docstring
  - `async _wait_for_command_completed(session, command_id, db_name, timeout_seconds, poll_interval_seconds)` (line 133): no docstring
  - `async _wait_for_ontology_present(session, db_name, ontology_id, branch, timeout_seconds, poll_interval_seconds)` (line 165): no docstring
  - `async _wait_for_instance_count(session, db_name, class_id, expected_count, branch, timeout_seconds, poll_interval_seconds)` (line 190): no docstring
  - `async test_oms_end_to_end_smoke()` (line 219): no docstring

### `backend/tests/test_openapi_contract_smoke.py`
- **Functions**
  - `_build_smoke_user_jwt()` (line 55): Build a deterministic HS256 user JWT for integration smoke tests.
  - `_load_repo_dotenv()` (line 87): Best-effort loader for the repo root `.env` used by docker-compose port overrides.
  - `_get_postgres_url_candidates()` (line 116): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 143): Fetch current write-side aggregate sequence (OCC expected_seq) from Postgres.
  - `async _get_ontology_head_commit(session, db_name, branch)` (line 194): no docstring
  - `async _record_deployed_commit(db_name, target_branch, ontology_commit_id)` (line 216): no docstring
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 242): no docstring
  - `_xlsx_bytes(header, rows)` (line 272): no docstring
  - `_csv_bytes(header, rows)` (line 290): no docstring
  - `_is_wip(op)` (line 310): no docstring
  - `_is_ops_only(op)` (line 320): no docstring
  - `_safe_pipeline_ref(value)` (line 376): no docstring
  - `async _request(session, plan)` (line 407): no docstring
  - `_format_path(template, ctx, overrides)` (line 441): no docstring
  - `_normalize_db_path(path)` (line 486): no docstring
  - `_pick_spec_path(paths, *candidates)` (line 494): no docstring
  - `_ontology_payload(class_id, label_en, label_ko)` (line 502): no docstring
  - `_mapping_file_bytes(ctx)` (line 528): no docstring
  - `_target_schema_json(ctx)` (line 550): no docstring
  - `_mappings_json(ctx)` (line 559): no docstring
  - `async _build_plan(op, ctx)` (line 567): Return a runnable RequestPlan for every non-WIP/non-ops operation.
  - `async test_openapi_stable_contract_smoke()` (line 1705): no docstring
- **Classes**
  - `Operation` (line 303): no docstring
  - `SmokeContext` (line 329): no docstring
    - `ontology_aggregate_id(self)` (line 357): no docstring
    - `advanced_ontology_aggregate_id(self)` (line 361): no docstring
    - `instance_aggregate_id(self)` (line 365): no docstring
    - `pipeline_name(self)` (line 369): no docstring
    - `dataset_name(self)` (line 373): no docstring
  - `RequestPlan` (line 393): no docstring

### `backend/tests/test_pipeline_execution_semantics_e2e.py`
- **Functions**
  - `_parse_s3_uri(uri)` (line 31): no docstring
  - `_idem_key(prefix)` (line 39): no docstring
  - `_lakefs_s3_client()` (line 44): no docstring
  - `_list_relative_object_keys(bucket, commit_id, artifact_prefix)` (line 71): List the object keys for a dataset artifact under a specific lakeFS ref (commit id),
  - `_load_rows_from_artifact(bucket, commit_id, artifact_prefix)` (line 107): no docstring
  - `_load_partitioned_rows_from_artifact(bucket, commit_id, artifact_prefix)` (line 157): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 194): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 238): no docstring
  - `async _wait_for_output_artifact(client, pipeline_id, job_id, node_id, timeout_seconds)` (line 272): no docstring
  - `async _wait_for_run_errors(client, pipeline_id, job_id, timeout_seconds)` (line 314): no docstring
  - `async _post_with_retry(client, url, json_payload, retries, retry_sleep)` (line 350): no docstring
  - `async _create_db_with_retry(client, db_name, description)` (line 376): no docstring
  - `_artifact_for_output(run, node_id)` (line 396): no docstring
  - `_commit_and_prefix_from_artifact(artifact_key)` (line 411): no docstring
  - `async test_snapshot_overwrites_outputs_across_runs()` (line 421): Checklist CL-015:
  - `async test_incremental_appends_outputs_and_preserves_previous_parts()` (line 545): Checklist CL-016:
  - `async test_incremental_watermark_boundary_includes_equal_timestamp_rows()` (line 690): Boundary check: rows with watermark == previous max should still appear in output (no gaps).
  - `async test_incremental_empty_diff_noop()` (line 816): Checklist CL-020:
  - `async test_incremental_removed_files_noop()` (line 957): Checklist CL-021:
  - `async test_run_branch_conflict_fallback_and_cleanup()` (line 1150): Retry safety: pre-existing run branch should not block deploy; fallback branch must be used and cleaned up.
  - `async test_partition_column_special_chars_roundtrip()` (line 1277): Checklist CL-021:
  - `async test_pk_semantics_append_log_allows_duplicate_ids()` (line 1387): P0-3: append_log should not enforce unique PK.
  - `async test_pk_semantics_append_state_blocks_duplicate_ids()` (line 1515): P0-3: append_state must enforce unique PK and fail on duplicates.
  - `async test_pk_semantics_remove_requires_delete_column()` (line 1644): P0-3: remove semantics must enforce deleteColumn.
  - `async test_schema_contract_breach_blocks_deploy()` (line 1741): Schema contract should fail when required columns or types mismatch.
  - `async test_executor_vs_worker_validation_consistency()` (line 1832): Compare in-memory executor vs spark worker validation errors for PK expectations.
  - `async test_incremental_small_files_compaction_metrics()` (line 1977): Perf check: measure small file growth across repeated incremental runs.
  - `async test_composite_pk_unique_perf()` (line 2154): Perf check: composite PK unique validation should complete without OOM/shuffle failures.

### `backend/tests/test_pipeline_objectify_es_e2e.py`
- **Functions**
  - `_postgres_url_candidates()` (line 40): no docstring
  - `async _grant_db_role(db_name, principal_id, role, principal_type)` (line 50): no docstring
  - `async _post_with_retry(client, url, json_payload, headers, retries, retry_sleep)` (line 96): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 123): no docstring
  - `async _create_db_with_retry(client, db_name, description)` (line 167): no docstring
  - `async _wait_for_ontology(client, db_name, class_id, branch, timeout_seconds)` (line 187): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 215): no docstring
  - `async _wait_for_artifact(client, pipeline_id, job_id, timeout_seconds)` (line 249): no docstring
  - `async _wait_for_es_doc(client, index_name, doc_id, timeout_seconds)` (line 288): no docstring
  - `_commit_id_from_artifact(artifact_key)` (line 315): no docstring
  - `async _get_head_commit(client, db_name, branch)` (line 326): no docstring
  - `async test_pipeline_objectify_es_projection()` (line 342): Full flow: raw ingest -> pipeline build -> objectify -> ES projection.

### `backend/tests/test_pipeline_streaming_semantics_e2e.py`
- **Functions**
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 21): no docstring
  - `async _post_with_retry(client, url, json_payload, headers, retries, retry_sleep)` (line 62): no docstring
  - `_idem_key(prefix)` (line 89): no docstring
  - `async _create_db_with_retry(client, db_name, description)` (line 94): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 114): no docstring
  - `_extract_error_detail(payload)` (line 138): no docstring
  - `async test_streaming_build_deploy_promotes_all_outputs()` (line 157): Checklist CL-017:
  - `async test_streaming_build_fails_as_job_group_on_contract_mismatch()` (line 290): Checklist CL-017:

### `backend/tests/test_pipeline_transform_cleansing_e2e.py`
- **Functions**
  - `_lakefs_s3_client()` (line 28): no docstring
  - `_artifact_commit_and_prefix(artifact_key)` (line 55): no docstring
  - `_load_rows_from_artifact(bucket, commit_id, artifact_prefix)` (line 66): no docstring
  - `async _post_with_retry(client, url, json_payload, headers, retries, retry_sleep)` (line 102): no docstring
  - `async _create_db_with_retry(client, db_name, description)` (line 129): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 149): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 193): no docstring
  - `async _wait_for_artifact(client, pipeline_id, job_id, timeout_seconds)` (line 227): no docstring
  - `async _wait_for_run_errors(client, pipeline_id, job_id, timeout_seconds)` (line 266): no docstring
  - `_select_output(artifact, dataset_name)` (line 302): no docstring
  - `async test_pipeline_transform_cleansing_and_validation_e2e()` (line 312): Validate transform/cleansing ops + schema/type/bad-record handling via real pipeline builds.

### `backend/tests/test_pipeline_type_mismatch_guard_e2e.py`
- **Functions**
  - `_idem_key(prefix)` (line 20): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 25): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 66): no docstring
  - `async _post_with_retry(client, url, json_payload, headers, retries, retry_sleep)` (line 90): no docstring
  - `async _create_db_with_retry(client, db_name, description)` (line 117): no docstring
  - `async test_preview_rejects_type_mismatch_in_compute_expression()` (line 139): Checklist CL-006:

### `backend/tests/test_sequence_allocator.py`
- **Functions**
  - `_get_postgres_url_candidates()` (line 19): no docstring
  - `async _make_allocator(dsn, schema)` (line 33): no docstring
  - `async _truncate(alloc, schema)` (line 52): no docstring
  - `async _reserve_like_event_store(alloc, aggregate_type, aggregate_id)` (line 59): no docstring
  - `async test_allocator_concurrent_reservation_is_unique_and_monotonic()` (line 70): no docstring
  - `async test_allocator_seeding_starts_after_existing_stream_max()` (line 99): no docstring
  - `async test_allocator_catches_up_when_seed_is_ahead_of_db_state()` (line 119): no docstring
  - `async test_allocator_occ_reserves_only_when_expected_matches()` (line 143): no docstring

### `backend/tests/test_terminus_version_control.py`
- **Functions**
  - `async test_terminus_branch_lifecycle_v12()` (line 25): no docstring

### `backend/tests/test_websocket_auth_e2e.py`
- **Functions**
  - `_ws_url(path)` (line 20): no docstring
  - `async test_ws_requires_token()` (line 32): no docstring
  - `async test_ws_allows_token()` (line 53): no docstring

### `backend/tests/test_worker_lease_safety_e2e.py`
- **Functions**
  - `_set_env(**updates)` (line 23): no docstring
  - `test_invalid_lease_settings_fail_fast()` (line 43): no docstring
  - `test_registry_disable_rejected()` (line 53): no docstring
  - `async test_heartbeat_not_blocked_by_poll()` (line 61): no docstring

### `backend/tests/unit/config/test_app_config_topics.py`
- **Functions**
  - `test_get_all_topics_includes_command_dlqs()` (line 6): no docstring

### `backend/tests/unit/config/test_config_drift_guards.py`
- **Functions**
  - `_disable_env_file(monkeypatch)` (line 11): no docstring
  - `_iter_runtime_python_files(repo_backend)` (line 17): no docstring
  - `test_app_config_reflects_current_settings(monkeypatch)` (line 51): no docstring
  - `test_no_os_getenv_calls_outside_settings_module()` (line 61): no docstring
  - `test_no_application_settings_instantiation_outside_settings_module()` (line 84): no docstring
  - `test_no_import_global_settings_symbol_outside_settings_module()` (line 107): no docstring

### `backend/tests/unit/config/test_kafka_config.py`
- **Functions**
  - `test_kafka_eos_producer_config(monkeypatch)` (line 33): no docstring
  - `test_kafka_eos_consumer_config(monkeypatch)` (line 44): no docstring
  - `test_transactional_producer_batch_success()` (line 56): no docstring
  - `test_transactional_producer_batch_no_transactions()` (line 74): no docstring
- **Classes**
  - `_FakeProducer` (line 8): no docstring
    - `__init__(self)` (line 9): no docstring
    - `init_transactions(self, timeout)` (line 14): no docstring
    - `begin_transaction(self)` (line 17): no docstring
    - `commit_transaction(self, timeout)` (line 20): no docstring
    - `abort_transaction(self, timeout)` (line 23): no docstring
    - `produce(self, topic, value, key)` (line 26): no docstring
    - `flush(self)` (line 29): no docstring

### `backend/tests/unit/config/test_settings_ssot.py`
- **Functions**
  - `_disable_env_file(monkeypatch)` (line 15): no docstring
  - `test_pipeline_publish_lock_timeout_fallback(monkeypatch)` (line 21): no docstring
  - `test_agent_bff_token_and_command_timeout_fallback(monkeypatch)` (line 38): no docstring
  - `test_client_token_fallbacks(monkeypatch)` (line 56): no docstring
  - `test_lakefs_repository_defaults(monkeypatch)` (line 76): no docstring
  - `test_local_port_host_aliases(monkeypatch)` (line 83): no docstring
  - `test_objectify_dataset_primary_chunk_size_defaults(monkeypatch)` (line 104): no docstring
  - `test_objectify_dataset_primary_chunk_size_clamps_lower_bound(monkeypatch)` (line 111): no docstring

### `backend/tests/unit/errors/__init__.py`

### `backend/tests/unit/errors/test_error_envelope_lint.py`
- **Functions**
  - `_collect_status_error_literals(backend_dir)` (line 21): no docstring
  - `test_no_direct_status_error_dicts()` (line 56): no docstring
  - `test_error_response_contains_enterprise_metadata()` (line 73): no docstring
  - `test_classified_http_exception_preserves_enterprise_and_external_codes()` (line 104): no docstring

### `backend/tests/unit/errors/test_error_taxonomy_audit_guard.py`
- **Functions**
  - `test_error_taxonomy_audit_guard()` (line 54): no docstring

### `backend/tests/unit/errors/test_error_taxonomy_coverage.py`
- **Functions**
  - `_extract_enum_values(path, class_name)` (line 13): no docstring
  - `_extract_dict_literal_keys(path, var_name)` (line 33): no docstring
  - `_collect_code_like_literals(backend_dir)` (line 57): no docstring
  - `test_error_taxonomy_covers_all_code_like_literals()` (line 91): no docstring

### `backend/tests/unit/errors/test_legacy_error_code_sync.py`
- **Functions**
  - `_extract_enum_values(path, class_name)` (line 9): no docstring
  - `_extract_dict_literal_keys(path, var_name)` (line 29): no docstring
  - `test_legacy_error_code_enum_is_registered_in_catalog()` (line 54): no docstring

### `backend/tests/unit/errors/test_policy_drift_guards.py`
- **Functions**
  - `test_enterprise_catalog_fingerprint_is_pinned()` (line 18): no docstring
  - `test_agent_tool_allowlist_bundle_hash_is_pinned()` (line 22): no docstring

### `backend/tests/unit/errors/test_runtime_silent_failure_guards.py`
- **Functions**
  - `_iter_runtime_python_files(backend_dir)` (line 55): no docstring
  - `test_runtime_has_no_return_in_finally()` (line 76): no docstring
  - `test_runtime_scope_disallows_silent_failures()` (line 99): no docstring
- **Classes**
  - `_ReturnInFinallyVisitor` (line 63): no docstring
    - `__init__(self)` (line 64): no docstring
    - `visit_Try(self, node)` (line 67): no docstring

### `backend/tests/unit/errors/test_service_factory_error_handlers.py`
- **Functions**
  - `test_service_factory_installs_error_handlers_by_default()` (line 9): no docstring

### `backend/tests/unit/idempotency/__init__.py`

### `backend/tests/unit/idempotency/test_enterprise_idempotency.py`
- **Classes**
  - `TestPipelineJobDedupeKey` (line 28): Test PipelineJob dedupe_key auto-generation (Gap 2)
    - `test_dedupe_key_auto_generated(self)` (line 31): PipelineJob should auto-generate dedupe_key when not provided
    - `test_dedupe_key_preserved_when_provided(self)` (line 45): PipelineJob should preserve dedupe_key if explicitly provided
    - `test_dedupe_key_deterministic(self)` (line 60): Same inputs should produce same dedupe_key
    - `test_dedupe_key_includes_idempotency_key(self)` (line 85): dedupe_key should incorporate client idempotency_key
    - `test_dedupe_key_includes_definition_hash(self)` (line 106): dedupe_key should incorporate definition_hash
    - `test_dedupe_key_includes_node_id(self)` (line 127): dedupe_key should incorporate node_id
    - `test_dedupe_key_different_for_different_modes(self)` (line 148): dedupe_key should differ for different modes
    - `test_build_dedupe_key_static_method(self)` (line 170): build_dedupe_key static method should work correctly
    - `test_build_dedupe_key_matches_instance(self)` (line 183): Static build_dedupe_key should match instance dedupe_key
  - `TestKafkaHeadersWithDedup` (line 212): Test kafka_headers_with_dedup function (Gap 5)
    - `test_dedup_header_with_explicit_dedup_id(self)` (line 215): Should include explicit dedup_id in headers
    - `test_dedup_header_computed_from_event_id(self)` (line 228): Should compute dedup_id from event_id and aggregate_id
    - `test_dedup_header_with_global_aggregate(self)` (line 242): Should use 'global' when aggregate_id is None
    - `test_dedup_header_extracted_from_payload(self)` (line 256): Should extract event_id from payload if not provided
    - `test_dedup_header_with_pipeline_id_fallback(self)` (line 271): Should use pipeline_id as aggregate_id fallback
    - `test_no_dedup_header_when_no_id(self)` (line 286): Should not add dedup-id when no identifiers available
    - `test_preserves_trace_headers(self)` (line 295): Should preserve trace context headers alongside dedup-id
  - `TestPipelineRegistryOCC` (line 316): Test Pipeline Registry Optimistic Concurrency Control (Gap 3)
    - `test_occ_conflict_error_attributes(self)` (line 319): PipelineOCCConflictError should have correct attributes
    - `test_occ_conflict_error_without_actual_version(self)` (line 336): PipelineOCCConflictError should work without actual_version
    - `test_pipeline_record_has_occ_version(self)` (line 348): PipelineRecord should have occ_version field
    - `test_pipeline_record_occ_version_default(self)` (line 393): PipelineRecord occ_version should default to 1
  - `TestPipelineAPIIdempotencyKeys` (line 443): Test Pipeline API idempotency key validation (Gap 1)
    - `test_get_idempotency_key_from_primary_header(self)` (line 446): _get_idempotency_key should extract from Idempotency-Key header
    - `test_get_idempotency_key_from_fallback_header(self)` (line 456): _get_idempotency_key should fallback to X-Idempotency-Key
    - `test_get_idempotency_key_returns_none_when_missing(self)` (line 472): _get_idempotency_key should return None when no header present
    - `test_get_idempotency_key_strips_whitespace(self)` (line 482): _get_idempotency_key should strip whitespace from header value
    - `test_get_idempotency_key_returns_none_for_empty_string(self)` (line 492): _get_idempotency_key should return None for empty/whitespace-only
    - `test_require_pipeline_idempotency_key_success(self)` (line 502): _require_pipeline_idempotency_key should return key when present
    - `test_require_pipeline_idempotency_key_raises_when_missing(self)` (line 512): _require_pipeline_idempotency_key should raise HTTPException when missing
    - `test_require_pipeline_idempotency_key_includes_operation_in_error(self)` (line 526): Error message should include the operation name
  - `TestWorkerAggregateOrdering` (line 545): Test worker aggregate ordering with sequence_number (Gap 4)
    - `test_pipeline_job_has_sequence_number(self)` (line 548): PipelineJob should have sequence_number field
    - `test_pipeline_job_sequence_number_optional(self)` (line 562): PipelineJob sequence_number should be optional
  - `TestIdempotencyIntegration` (line 581): Integration tests for idempotency features working together
    - `test_pipeline_job_complete_idempotency_flow(self)` (line 584): Complete idempotency flow from API to job creation
    - `test_kafka_headers_include_job_dedupe(self)` (line 620): Kafka headers should include job dedupe_key

### `backend/tests/unit/kafka/__init__.py`

### `backend/tests/unit/kafka/test_event_envelope_worker_dlq.py`
- **Functions**
  - `async test_publish_envelope_failure_to_dlq_uses_default_key_and_shape()` (line 77): no docstring
  - `async test_publish_envelope_failure_to_dlq_noops_without_producer()` (line 118): no docstring
  - `async test_send_envelope_failure_to_dlq_builds_key_with_fallback()` (line 146): no docstring
- **Classes**
  - `_FakeProducerOps` (line 14): no docstring
    - `__init__(self)` (line 15): no docstring
    - `async produce(self, **kwargs)` (line 19): no docstring
    - `async flush(self, timeout_s)` (line 22): no docstring
  - `_FakeTracing` (line 27): no docstring
    - `span(self, *_args, **_kwargs)` (line 28): no docstring
  - `_FakeMetrics` (line 32): no docstring
    - `record_event(self, *_args, **_kwargs)` (line 33): no docstring
  - `_Msg` (line 37): no docstring
    - `topic(self)` (line 38): no docstring
    - `partition(self)` (line 41): no docstring
    - `offset(self)` (line 44): no docstring
  - `_StubEnvelopeWorker` (line 48): no docstring
    - `__init__(self)` (line 49): no docstring
    - `async _process_payload(self, payload)` (line 61): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 64): no docstring

### `backend/tests/unit/kafka/test_processed_event_worker_bootstrap.py`
- **Functions**
  - `test_initialize_safe_consumer_runtime_sets_consumer_ops(monkeypatch)` (line 45): no docstring
  - `test_initialize_safe_consumer_runtime_without_partition_reset(monkeypatch)` (line 86): no docstring
- **Classes**
  - `_StubWorker` (line 8): no docstring
    - `__init__(self)` (line 9): no docstring
    - `_parse_payload(self, payload)` (line 18): no docstring
    - `_registry_key(self, payload)` (line 21): no docstring
    - `async _process_payload(self, payload)` (line 24): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 27): no docstring
  - `_FakeConsumerOps` (line 39): no docstring
    - `__init__(self, consumer, thread_name_prefix)` (line 40): no docstring

### `backend/tests/unit/kafka/test_processed_event_worker_dlq_normalization.py`
- **Functions**
  - `test_normalize_dlq_publish_inputs_uses_inferred_defaults()` (line 94): no docstring
  - `test_normalize_dlq_publish_inputs_preserves_explicit_values()` (line 118): no docstring
  - `test_parse_error_context_extracts_custom_error_fields()` (line 142): no docstring
  - `test_fallback_metadata_from_raw_payload_extracts_metadata_dict()` (line 153): no docstring
  - `test_fallback_metadata_from_raw_payload_returns_none_for_invalid_payload()` (line 161): no docstring
  - `async test_publish_parse_error_to_dlq_calls_sender()` (line 169): no docstring
  - `async test_publish_parse_error_to_dlq_raise_toggle()` (line 209): no docstring
  - `async test_publish_standard_dlq_record_missing_producer_modes()` (line 245): no docstring
  - `async test_publish_standard_dlq_record_success()` (line 282): no docstring
  - `async test_send_standard_dlq_record_normalizes_and_invokes_publisher()` (line 306): no docstring
- **Classes**
  - `_StubWorker` (line 11): no docstring
    - `__init__(self)` (line 12): no docstring
    - `_parse_payload(self, payload)` (line 21): no docstring
    - `_registry_key(self, payload)` (line 24): no docstring
    - `async _process_payload(self, payload)` (line 27): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 30): no docstring
  - `_MsgWithHeaders` (line 42): no docstring
    - `headers(self)` (line 43): no docstring
  - `_KafkaMsg` (line 47): no docstring
    - `__init__(self)` (line 48): no docstring
    - `topic(self)` (line 51): no docstring
    - `partition(self)` (line 54): no docstring
    - `offset(self)` (line 57): no docstring
    - `key(self)` (line 60): no docstring
    - `value(self)` (line 63): no docstring
    - `headers(self)` (line 66): no docstring
    - `timestamp(self)` (line 69): no docstring
  - `_Producer` (line 73): no docstring
    - `__init__(self)` (line 74): no docstring
    - `produce(self, topic, key, value, headers, **_kwargs)` (line 77): no docstring
    - `flush(self, *_args, **_kwargs)` (line 80): no docstring
  - `_ParseErr` (line 84): no docstring
    - `__init__(self)` (line 85): no docstring

### `backend/tests/unit/kafka/test_processed_event_worker_silent_failure_guards.py`
- **Functions**
  - `async test_handle_partition_message_propagates_error_when_partition_revoked()` (line 82): no docstring
  - `async test_handle_partition_message_keeps_primary_error_when_cleanup_fails(caplog)` (line 93): no docstring
  - `async test_handle_claimed_logs_warning_on_heartbeat_join_failure(caplog, monkeypatch)` (line 164): no docstring
- **Classes**
  - `_Msg` (line 19): no docstring
    - `__init__(self, topic, partition, offset)` (line 20): no docstring
    - `topic(self)` (line 25): no docstring
    - `partition(self)` (line 28): no docstring
    - `offset(self)` (line 31): no docstring
  - `_PartitionWorker` (line 35): no docstring
    - `__init__(self)` (line 36): no docstring
    - `_parse_payload(self, payload)` (line 47): no docstring
    - `_registry_key(self, payload)` (line 50): no docstring
    - `async _process_payload(self, payload)` (line 53): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 57): no docstring
    - `_buffer_messages(self)` (line 68): no docstring
    - `_uses_commit_state(self)` (line 71): no docstring
    - `async _resume_partition(self, topic, partition)` (line 74): no docstring
    - `async handle_message(self, msg)` (line 77): no docstring
  - `_ProcessedStub` (line 114): no docstring
    - `async claim(self, **_kwargs)` (line 115): no docstring
    - `async mark_done(self, **_kwargs)` (line 118): no docstring
    - `async mark_failed(self, **_kwargs)` (line 121): no docstring
  - `_HeartbeatJoinWorker` (line 125): no docstring
    - `__init__(self)` (line 126): no docstring
    - `_parse_payload(self, payload)` (line 135): no docstring
    - `_registry_key(self, payload)` (line 138): no docstring
    - `async _process_payload(self, payload)` (line 141): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 144): no docstring
    - `async _commit(self, msg)` (line 155): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 158): no docstring

### `backend/tests/unit/kafka/test_producer_ops_shutdown.py`
- **Functions**
  - `async test_close_kafka_producer_prefers_ops_when_present()` (line 37): no docstring
  - `async test_close_kafka_producer_flushes_raw_producer()` (line 52): no docstring
  - `async test_close_kafka_producer_handles_noarg_flush_signature()` (line 64): no docstring
  - `async test_close_kafka_producer_swallows_close_errors(caplog)` (line 76): no docstring
- **Classes**
  - `_StubProducerOps` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `async close(self, timeout_s)` (line 14): no docstring
  - `_StubProducer` (line 18): no docstring
    - `__init__(self)` (line 19): no docstring
    - `flush(self, timeout_s)` (line 22): no docstring
  - `_NoArgFlushProducer` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `flush(self)` (line 31): no docstring

### `backend/tests/unit/kafka/test_retry_classifier.py`
- **Functions**
  - `test_normalize_error_message_lowercases()` (line 14): no docstring
  - `test_contains_marker_trims_and_matches_case_insensitive()` (line 19): no docstring
  - `test_classify_retryable_by_markers_priority_and_default()` (line 24): no docstring
  - `test_create_retry_policy_profile_normalizes_markers()` (line 58): no docstring
  - `test_classify_retryable_with_profile_uses_predefined_profile()` (line 71): no docstring

### `backend/tests/unit/kafka/test_safe_consumer.py`
- **Classes**
  - `TestSafeKafkaConsumerConfig` (line 16): Test SafeKafkaConsumer configuration enforcement.
    - `test_enforced_settings_cannot_be_overridden(self)` (line 19): ENFORCED_SETTINGS should not allow override via extra_config.
    - `test_isolation_level_read_committed(self)` (line 27): SafeKafkaConsumer should always use read_committed isolation.
    - `test_auto_commit_disabled(self)` (line 33): SafeKafkaConsumer should always disable auto-commit.
    - `test_validate_consumer_config_rejects_uncommitted(self)` (line 39): validate_consumer_config should reject read_uncommitted.
    - `test_validate_consumer_config_rejects_auto_commit(self)` (line 46): validate_consumer_config should reject auto-commit.
    - `test_validate_consumer_config_accepts_valid(self)` (line 56): validate_consumer_config should accept valid config.
  - `TestConsumerState` (line 67): Test ConsumerState enum.
    - `test_consumer_state_values(self)` (line 70): ConsumerState should have all lifecycle states.
  - `TestPartitionState` (line 81): Test PartitionState dataclass.
    - `test_partition_state_creation(self)` (line 84): PartitionState should track partition info.
  - `TestRebalanceHandler` (line 101): Test RebalanceHandler callbacks.
    - `test_rebalance_handler_on_revoke_marks_rebalancing(self)` (line 104): on_revoke should set consumer state to REBALANCING.
    - `test_rebalance_handler_on_assign_marks_running(self)` (line 133): on_assign should set consumer state to RUNNING.
    - `test_rebalance_handler_calls_user_callbacks(self)` (line 160): RebalanceHandler should call user-provided callbacks.
  - `TestSafeKafkaConsumerIntegration` (line 199): Integration tests (with mocked Kafka client).
    - `test_consumer_creation_with_enforced_settings(self, mock_settings, mock_consumer_class)` (line 204): SafeKafkaConsumer should create consumer with enforced settings.
    - `test_extra_config_cannot_override_enforced(self, mock_settings, mock_consumer_class)` (line 228): extra_config should not override ENFORCED_SETTINGS.
    - `test_is_rebalancing_property(self, mock_settings, mock_consumer_class)` (line 257): is_rebalancing should reflect REBALANCING state.
    - `test_commit_message_marks_partition_processed(self, mock_settings, mock_consumer_class)` (line 275): commit(message=...) should clear processing state (rebalance-safe bookkeeping).
    - `test_seek_delegates_and_clears_inflight_state(self, mock_settings, mock_consumer_class)` (line 307): seek() should delegate and clear in-flight state to avoid stale bookkeeping.

### `backend/tests/unit/kafka/test_worker_consumer_runtime.py`
- **Functions**
  - `async test_commit_checks_revoked_at_execution_time()` (line 94): no docstring
  - `async test_seek_checks_revoked_at_execution_time()` (line 123): no docstring
  - `async test_commit_updates_commit_state_after_commit()` (line 151): no docstring
  - `async test_seek_updates_commit_state_after_seek()` (line 177): no docstring
- **Classes**
  - `_StubMsg` (line 12): no docstring
    - `__init__(self, topic, partition, offset)` (line 13): no docstring
    - `topic(self)` (line 18): no docstring
    - `partition(self)` (line 21): no docstring
    - `offset(self)` (line 24): no docstring
  - `_StubConsumer` (line 28): no docstring
    - `__init__(self)` (line 29): no docstring
    - `poll(self, timeout)` (line 33): no docstring
    - `commit_sync(self, msg)` (line 36): no docstring
    - `seek(self, tp)` (line 39): no docstring
    - `pause(self, partitions)` (line 42): no docstring
    - `resume(self, partitions)` (line 45): no docstring
    - `close(self)` (line 48): no docstring
  - `_DeferredOps` (line 52): A KafkaConsumerOps stub that defers `call(...)` execution.
    - `__init__(self, consumer)` (line 60): no docstring
    - `async poll(self, timeout)` (line 64): no docstring
    - `async commit_sync(self, msg)` (line 67): no docstring
    - `async seek(self, tp)` (line 70): no docstring
    - `async pause(self, partitions)` (line 73): no docstring
    - `async resume(self, partitions)` (line 76): no docstring
    - `async close(self)` (line 79): no docstring
    - `async call(self, fn, *args, **kwargs)` (line 82): no docstring
    - `flush_next(self)` (line 88): no docstring

### `backend/tests/unit/mcp/test_critical_gap_fixes.py`
- **Classes**
  - `TestExtractSparkErrorDetailsLogic` (line 32): Tests for the _extract_spark_error_details logic without MCP import.
    - `_extract_spark_error_details(self, run)` (line 35): Copy of the actual function logic for isolated testing.
    - `test_extract_errors_from_output_json(self)` (line 69): Should extract errors list from output_json.
    - `test_extract_error_summary(self)` (line 82): Should extract error summary from output_json.
    - `test_extract_error_summary_from_dict(self)` (line 94): Should extract error summary when error is a dict.
    - `test_extract_stack_trace(self)` (line 106): Should extract stack trace if available.
    - `test_extract_traceback_alternative(self)` (line 119): Should extract traceback as alternative to stack_trace.
    - `test_extract_exception_type(self)` (line 132): Should extract exception type if available.
    - `test_empty_output_json(self)` (line 145): Should return generic message when no error details available.
    - `test_missing_output_json(self)` (line 153): Should handle missing output_json gracefully.
    - `test_output_json_not_dict(self)` (line 160): Should handle non-dict output_json.
    - `test_truncates_long_error_summary(self)` (line 167): Should truncate very long error summaries.
    - `test_truncates_long_stack_trace(self)` (line 178): Should truncate very long stack traces.
    - `test_filters_empty_errors(self)` (line 190): Should filter out empty error strings.
  - `TestColumnValidationLogic` (line 205): Tests for the column validation logic used in dataset_validate_columns.
    - `test_exact_match_columns(self)` (line 208): Exact column name matches should be valid.
    - `test_case_insensitive_match(self)` (line 224): Case-insensitive matches should be found.
    - `test_suggest_similar_columns(self)` (line 235): Should suggest similar column names.
    - `test_no_suggestions_for_unrelated(self)` (line 248): Should not suggest unrelated columns.
  - `TestObjectifyWaitLogic` (line 263): Tests for the objectify_wait polling logic.
    - `test_completed_status_detection(self)` (line 266): Should detect completed status.
    - `test_failed_status_detection(self)` (line 271): Should detect failed status.
    - `test_running_status_not_final(self)` (line 276): Running status should not be final.
    - `test_queued_status_not_final(self)` (line 281): Queued status should not be final.
  - `TestOntologyQueryLogic` (line 289): Tests for the ontology_query_instances request building logic.
    - `test_build_query_body_basic(self)` (line 292): Should build basic query body.
    - `test_build_query_body_with_filters(self)` (line 305): Should include filters in query body.
    - `test_limit_capping(self)` (line 320): Should cap limit to max 100.
    - `test_limit_minimum(self)` (line 326): Should enforce minimum limit of 1.
  - `TestDatasetLookupLogic` (line 335): Tests for the dataset lookup logic.
    - `test_default_branch(self)` (line 338): Should use 'main' as default branch.
    - `test_custom_branch(self)` (line 344): Should use custom branch when provided.
    - `test_whitespace_branch_handling(self)` (line 350): Should strip whitespace from branch.
  - `TestMCPServerIntegration` (line 360): Tests requiring MCP module - skipped in local env.
    - `test_pipeline_mcp_server_imports(self)` (line 363): Should be able to import PipelineMCPServer.
    - `test_extract_spark_error_details_import(self)` (line 368): Should be able to import _extract_spark_error_details.
    - `test_server_instantiation(self)` (line 373): Should be able to instantiate PipelineMCPServer.

### `backend/tests/unit/mcp/test_debug_mcp_tools.py`
- **Functions**
  - `test_debug_get_errors_returns_accumulated_errors()` (line 30): Test debug_get_errors returns errors and warnings from run.
  - `test_debug_get_errors_respects_limit()` (line 57): Test debug_get_errors respects limit parameter.
  - `test_debug_get_errors_excludes_warnings()` (line 83): Test debug_get_errors excludes warnings when requested.
  - `test_debug_get_errors_empty()` (line 104): Test debug_get_errors with no errors.
  - `test_debug_get_execution_log_returns_entries()` (line 124): Test debug_get_execution_log returns tool call log entries.
  - `test_debug_get_execution_log_filters_by_step()` (line 183): Test debug_get_execution_log filters by step number.
  - `test_debug_get_execution_log_handles_invalid_json()` (line 207): Test debug_get_execution_log handles invalid JSON gracefully.
  - `test_debug_inspect_node_returns_node_info()` (line 233): Test debug_inspect_node returns node details and connections.
  - `test_debug_inspect_node_not_found()` (line 283): Test debug_inspect_node returns error for non-existent node.
  - `test_debug_inspect_node_requires_node_id()` (line 314): Test debug_inspect_node requires node_id parameter.
  - `test_debug_explain_failure_diagnoses_errors()` (line 329): Test debug_explain_failure provides diagnostic suggestions.
  - `test_debug_explain_failure_handles_permission_error()` (line 387): Test debug_explain_failure handles permission errors.
  - `test_debug_explain_failure_handles_timeout()` (line 405): Test debug_explain_failure handles timeout errors.
  - `test_debug_explain_failure_empty_errors()` (line 423): Test debug_explain_failure with no errors.
  - `test_debug_dry_run_valid_plan()` (line 447): Test debug_dry_run validates a correct plan.
  - `test_debug_dry_run_empty_plan()` (line 495): Test debug_dry_run rejects empty plan.
  - `test_debug_dry_run_no_nodes()` (line 518): Test debug_dry_run rejects plan with no nodes.
  - `test_debug_dry_run_missing_input()` (line 544): Test debug_dry_run warns about missing input node.
  - `test_debug_dry_run_missing_output()` (line 575): Test debug_dry_run warns about missing output node.
  - `test_debug_dry_run_disconnected_nodes()` (line 608): Test debug_dry_run warns about disconnected nodes.
- **Classes**
  - `MockAgentState` (line 19): Mock agent state for testing debug tools.
    - `__init__(self)` (line 22): no docstring

### `backend/tests/unit/mcp/test_objectify_mcp_tools.py`
- **Functions**
  - `async test_objectify_suggest_mapping_basic()` (line 162): Test objectify_suggest_mapping returns suggestions based on schema matching.
  - `async test_objectify_create_mapping_spec()` (line 248): Test objectify_create_mapping_spec creates a mapping spec.
  - `async test_objectify_list_mapping_specs()` (line 292): Test objectify_list_mapping_specs returns specs for a dataset.
  - `async test_objectify_run()` (line 339): Test objectify_run enqueues an objectify job.
  - `async test_objectify_get_status()` (line 399): Test objectify_get_status returns job status.
  - `async test_objectify_get_status_not_found()` (line 427): Test objectify_get_status returns None for non-existent job.
  - `async test_objectify_suggest_mapping_no_matches()` (line 438): Test objectify_suggest_mapping handles case with no matches.
  - `async test_objectify_create_mapping_spec_validates_mappings()` (line 468): Test that empty or invalid mappings are rejected.
  - `async test_objectify_run_finds_active_mapping_spec()` (line 507): Test objectify_run can find active mapping spec when not explicitly provided.
- **Classes**
  - `MockDataset` (line 22): no docstring
  - `MockDatasetVersion` (line 33): no docstring
  - `MockMappingSpec` (line 41): no docstring
    - `__post_init__(self)` (line 51): no docstring
  - `MockObjectifyJob` (line 57): no docstring
  - `MockDatasetRegistry` (line 70): no docstring
    - `__init__(self)` (line 71): no docstring
    - `async initialize(self)` (line 75): no docstring
    - `async get_dataset(self, dataset_id)` (line 78): no docstring
    - `async get_version(self, version_id)` (line 81): no docstring
    - `async get_latest_version(self, dataset_id)` (line 84): no docstring
  - `MockObjectifyRegistry` (line 91): no docstring
    - `__init__(self)` (line 92): no docstring
    - `async initialize(self)` (line 98): no docstring
    - `async create_mapping_spec(self, dataset_id, dataset_branch, artifact_output_name, schema_hash, target_class_id, mappings, auto_sync, status, options)` (line 101): no docstring
    - `async list_mapping_specs(self, dataset_id, limit)` (line 131): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 134): no docstring
    - `async get_objectify_job(self, job_id)` (line 137): no docstring
    - `async get_objectify_job_by_dedupe_key(self, dedupe_key)` (line 140): no docstring
    - `async enqueue_objectify_job(self, job)` (line 143): no docstring
    - `build_dedupe_key(self, **kwargs)` (line 155): no docstring

### `backend/tests/unit/mcp/test_pipeline_plan_add_output_contract.py`
- **Functions**
  - `async test_plan_add_output_accepts_dataset_canonical_fields()` (line 11): no docstring
  - `async test_plan_add_output_normalizes_dataset_camel_case_aliases()` (line 39): no docstring

### `backend/tests/unit/mcp/test_pipeline_plan_add_udf_contract.py`
- **Functions**
  - `async test_plan_add_udf_accepts_reference_fields()` (line 11): no docstring
  - `async test_plan_add_udf_accepts_camel_case_aliases()` (line 35): no docstring
  - `async test_plan_add_udf_requires_pinned_version()` (line 59): no docstring

### `backend/tests/unit/middleware/test_middleware_fixes.py`
- **Functions**
  - `_set_env(**updates)` (line 20): no docstring
  - `test_i18n_large_json_not_truncated()` (line 38): no docstring
  - `test_i18n_translates_description_field()` (line 55): no docstring
  - `test_rate_limit_headers_attach_for_dict_response()` (line 74): no docstring
  - `test_bff_auth_middleware_blocks_unsafe_methods()` (line 93): no docstring
  - `test_bff_auth_accepts_rotated_admin_tokens()` (line 115): no docstring
  - `test_bff_dev_master_auth_allows_missing_token_in_development()` (line 130): no docstring
  - `test_bff_dev_master_auth_is_disabled_in_production()` (line 151): no docstring
  - `test_bff_admin_guard_allows_dev_master_without_token_in_development()` (line 171): no docstring
  - `test_bff_admin_guard_requires_token_in_production_even_with_dev_master_flag()` (line 190): no docstring
  - `test_rate_limit_admin_bypass_requires_valid_token()` (line 204): no docstring
  - `test_bff_auth_allows_user_jwt_when_enabled()` (line 240): no docstring
  - `test_bff_user_jwt_accepts_rotated_hs256_secrets()` (line 256): no docstring
  - `test_bff_agent_auth_requires_delegated_user_jwt_when_enabled()` (line 276): no docstring
  - `test_bff_agent_auth_accepts_rotated_agent_tokens()` (line 308): no docstring
  - `test_bff_agent_auth_requires_user_jwt_enabled_for_agent_calls()` (line 336): no docstring
  - `test_oms_auth_accepts_rotated_tokens()` (line 361): no docstring
  - `test_bff_admin_token_requires_user_jwt_for_agent_endpoints_when_enabled()` (line 377): no docstring
  - `test_bff_agent_tool_policy_enforced_via_tool_registry()` (line 406): no docstring
  - `test_bff_agent_tool_policy_enforces_session_enabled_tools_and_abac()` (line 554): no docstring
  - `test_bff_agent_tool_policy_enforces_action_type_and_ontology_abac()` (line 700): no docstring
  - `test_bff_agent_tool_idempotency_replays_without_reexecution()` (line 830): no docstring

### `backend/tests/unit/monitoring/test_monitoring_configs.py`
- **Functions**
  - `_backend_dir()` (line 10): Resolve the `backend/` directory regardless of where the repo is checked out.
  - `_repo_root()` (line 25): no docstring
  - `test_prometheus_config_yaml_is_valid_and_wired()` (line 30): no docstring
  - `test_alert_rules_yaml_is_valid_and_has_minimum_alerts()` (line 47): no docstring
  - `test_alertmanager_yaml_is_valid_and_references_default_receiver()` (line 77): no docstring
  - `test_grafana_dashboard_json_is_valid()` (line 92): no docstring
  - `test_operations_doc_mentions_backup_scripts()` (line 103): Guard against ops runbook drift: docs should reference code-backed scripts.

### `backend/tests/unit/observability/test_config_monitor.py`
- **Functions**
  - `test_config_monitor_current_endpoint_ok()` (line 9): no docstring

### `backend/tests/unit/observability/test_context_propagation.py`
- **Functions**
  - `test_kafka_headers_roundtrip_via_attached_context()` (line 11): no docstring
  - `test_kafka_headers_from_envelope_metadata_only_emits_known_keys()` (line 33): no docstring

### `backend/tests/unit/observability/test_request_context.py`
- **Functions**
  - `test_parse_baggage_header_best_effort()` (line 7): no docstring
  - `test_trace_context_filter_includes_request_context_fields()` (line 20): no docstring
  - `test_carrier_from_envelope_metadata_appends_baggage_from_fields()` (line 44): no docstring
  - `test_attach_context_from_kafka_uses_fallback_metadata_for_contextvars()` (line 65): no docstring
  - `test_event_envelope_from_command_includes_context_correlation_id()` (line 77): no docstring
  - `test_event_envelope_from_command_prefers_command_metadata_over_context()` (line 92): no docstring

### `backend/tests/unit/observability/test_tracing_config.py`
- **Functions**
  - `_set_env(**updates)` (line 11): no docstring
  - `test_otlp_export_disabled_when_no_endpoint()` (line 29): no docstring
  - `test_span_omits_kind_when_none()` (line 36): no docstring
  - `test_span_passes_kind_when_set()` (line 74): no docstring

### `backend/tests/unit/oms/test_action_async_permission_profile_api.py`
- **Functions**
  - `_build_action_spec()` (line 15): no docstring
  - `_install_deployment_and_resource_mocks(monkeypatch, action_spec)` (line 44): no docstring
  - `action_async_app()` (line 77): no docstring
  - `async test_submit_returns_403_for_datasource_derived_without_data_engineer_role(action_async_app, monkeypatch)` (line 109): no docstring
  - `async test_simulate_returns_503_when_datasource_derived_data_access_is_unverifiable(action_async_app, monkeypatch)` (line 137): no docstring
  - `async test_submit_returns_403_when_target_class_misses_required_interface(action_async_app, monkeypatch)` (line 213): no docstring
  - `async test_submit_returns_503_when_target_edit_access_is_unverifiable(action_async_app, monkeypatch)` (line 255): no docstring
- **Classes**
  - `_FakeEventStore` (line 71): no docstring
    - `async append_event(self, _event)` (line 72): no docstring

### `backend/tests/unit/oms/test_instance_router.py`
- **Functions**
  - `_mock_es()` (line 19): no docstring
  - `mock_es()` (line 39): no docstring
  - `override_deps(mock_es)` (line 44): Override FastAPI dependencies with mocks.
  - `async test_get_class_instances(mock_es)` (line 60): no docstring
  - `async test_get_class_instances_with_search(mock_es)` (line 75): no docstring
  - `async test_get_instance(mock_es)` (line 89): no docstring
  - `async test_get_instance_not_found(mock_es)` (line 102): no docstring
  - `async test_get_instance_class_mismatch(mock_es)` (line 112): no docstring
  - `async test_get_class_instance_count(mock_es)` (line 121): no docstring
  - `async test_sparql_returns_410()` (line 132): no docstring

### `backend/tests/unit/openapi/test_wip_hidden.py`
- **Functions**
  - `test_wip_projection_endpoints_hidden_from_openapi()` (line 8): no docstring

### `backend/tests/unit/pipeline_functions/test_functions_matrix_contract.py`
- **Functions**
  - `test_functions_snapshot_entries_have_valid_classification()` (line 14): no docstring
  - `test_functions_snapshot_has_no_unclassified_rows()` (line 25): no docstring
  - `test_functions_snapshot_loader_fallback_parser_without_pyyaml(monkeypatch)` (line 36): no docstring

### `backend/tests/unit/pipeline_functions/test_functions_preview_compat.py`
- **Functions**
  - `async _build_executor()` (line 50): no docstring
  - `async _run_group_aggregate(op)` (line 82): no docstring
  - `async test_functions_preview_supported_matrix_contract()` (line 109): no docstring
- **Classes**
  - `_Dataset` (line 17): no docstring
  - `_Version` (line 26): no docstring
  - `_DatasetRegistry` (line 32): no docstring
    - `__init__(self)` (line 33): no docstring
    - `async get_dataset(self, dataset_id)` (line 37): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 43): no docstring
    - `async get_latest_version(self, dataset_id)` (line 46): no docstring

### `backend/tests/unit/pipeline_functions/test_functions_spark_compat.py`
- **Functions**
  - `_resolve_java_home()` (line 28): no docstring
  - `spark()` (line 43): no docstring
  - `worker(spark)` (line 64): no docstring
  - `_run_group_aggregate(worker, op)` (line 70): no docstring
  - `test_functions_spark_supported_matrix_contract(worker)` (line 90): no docstring

### `backend/tests/unit/routers/test_monitoring_router_deprecation_headers.py`
- **Functions**
  - `_build_client()` (line 9): no docstring
  - `test_metrics_redirect_without_deprecated_query_has_no_deprecation_headers()` (line 15): no docstring
  - `test_metrics_redirect_with_deprecated_query_sets_deprecation_headers()` (line 25): no docstring

### `backend/tests/unit/routers/test_schema_changes.py`
- **Classes**
  - `TestSchemaChangesModels` (line 18): Test request/response models
    - `test_subscription_create_request_valid(self)` (line 21): SubscriptionCreateRequest should accept valid data
    - `test_subscription_create_request_defaults(self)` (line 35): SubscriptionCreateRequest should have sensible defaults
    - `test_subscription_create_request_invalid_subject_type(self)` (line 45): SubscriptionCreateRequest should reject invalid subject types
    - `test_acknowledge_request(self)` (line 54): AcknowledgeRequest should require acknowledged_by
    - `test_compatibility_check_request_optional_version(self)` (line 59): CompatibilityCheckRequest should allow optional version
  - `TestSchemaChangesEndpoints` (line 68): Test endpoint logic (mocked database)
    - `mock_pool(self)` (line 72): Create a mock database pool
    - `mock_registry(self, mock_pool)` (line 81): Create a mock dataset registry with pool
    - `async test_list_schema_changes_empty(self, mock_registry)` (line 89): list_schema_changes should return empty list when no history
    - `async test_list_schema_changes_with_results(self, mock_registry)` (line 104): list_schema_changes should return formatted results
    - `async test_list_schema_changes_with_filters(self, mock_registry)` (line 137): list_schema_changes should apply filters correctly
    - `async test_acknowledge_drift_success(self, mock_registry)` (line 158): acknowledge_drift should update drift record
    - `async test_acknowledge_drift_not_found(self, mock_registry)` (line 175): acknowledge_drift should return 404 for unknown drift
    - `async test_create_subscription_success(self, mock_registry)` (line 193): create_subscription should create new subscription
    - `async test_delete_subscription_success(self, mock_registry)` (line 222): delete_subscription should soft-delete subscription
    - `async test_get_stats_empty(self, mock_registry)` (line 241): get_schema_change_stats should return zero stats when empty
    - `async test_get_stats_with_data(self, mock_registry)` (line 255): get_schema_change_stats should aggregate by severity and type
  - `TestMappingCompatibility` (line 276): Test mapping compatibility check endpoint
    - `mock_registries(self)` (line 280): Create mock registries
    - `async test_check_compatibility_no_drift(self, mock_registries)` (line 288): check_mapping_compatibility should return compatible when no drift
    - `async test_check_compatibility_with_breaking_drift(self, mock_registries)` (line 325): check_mapping_compatibility should return incompatible on breaking drift
    - `async test_check_compatibility_mapping_not_found(self, mock_registries)` (line 381): check_mapping_compatibility should return 404 for unknown mapping

### `backend/tests/unit/security/__init__.py`

### `backend/tests/unit/security/test_data_encryption.py`
- **Functions**
  - `test_data_encryptor_round_trip_text(monkeypatch)` (line 18): no docstring
  - `test_data_encryptor_round_trip_json(monkeypatch)` (line 31): no docstring
  - `test_data_encryptor_round_trip_bytes(monkeypatch)` (line 44): no docstring
  - `test_data_encryptor_supports_key_rotation(monkeypatch)` (line 57): no docstring
  - `test_agent_session_registry_decrypts_message_content(monkeypatch)` (line 75): no docstring
  - `test_agent_session_registry_decrypts_tool_call_payloads(monkeypatch)` (line 105): no docstring

### `backend/tests/unit/security/test_database_access.py`
- **Functions**
  - `async test_get_database_access_role_returns_none_when_table_missing(monkeypatch)` (line 24): no docstring
  - `async test_has_database_access_config_returns_false_when_table_missing(monkeypatch)` (line 41): no docstring
  - `async test_enforce_database_role_allows_when_unconfigured_and_flag_unset(monkeypatch)` (line 58): no docstring
  - `async test_enforce_database_role_denies_when_flag_true_and_no_role(monkeypatch)` (line 82): no docstring
- **Classes**
  - `_FakeConn` (line 7): no docstring
    - `__init__(self, fetchrow_exception, fetchrow_result)` (line 8): no docstring
    - `async fetchrow(self, *args, **kwargs)` (line 13): no docstring
    - `async close(self)` (line 18): no docstring

### `backend/tests/unit/security/test_principal_utils.py`
- **Functions**
  - `test_resolve_principal_defaults_when_missing_headers()` (line 4): no docstring
  - `test_resolve_principal_uses_header_values()` (line 8): no docstring
  - `test_resolve_principal_enforces_allowed_types()` (line 13): no docstring
  - `test_resolve_principal_supports_custom_defaults()` (line 18): no docstring
  - `test_resolve_principal_uses_custom_header_keys()` (line 30): no docstring
  - `test_actor_label_defaults()` (line 42): no docstring

### `backend/tests/unit/serializers/test_complex_type_serializer.py`
- **Functions**
  - `test_array_roundtrip()` (line 7): no docstring
  - `test_object_roundtrip()` (line 16): no docstring
  - `test_enum_serialization()` (line 25): no docstring
  - `test_money_serialization_object()` (line 33): no docstring
  - `test_coordinate_string_deserialization()` (line 42): no docstring
  - `test_image_and_file_serialization()` (line 48): no docstring

### `backend/tests/unit/services/__init__.py`

### `backend/tests/unit/services/fake_async_redis.py`
- **Functions**
  - `_to_bytes(value)` (line 7): no docstring
- **Classes**
  - `_ValueEntry` (line 18): no docstring
  - `FakeAsyncRedis` (line 23): Minimal async Redis stub for unit tests.
    - `__init__(self)` (line 34): no docstring
    - `async aclose(self)` (line 39): no docstring
    - `async delete(self, key)` (line 42): no docstring
    - `async get(self, key)` (line 55): no docstring
    - `async set(self, key, value, nx, ex)` (line 61): no docstring
    - `async setex(self, key, time, value)` (line 67): no docstring
    - `async hset(self, key, mapping)` (line 71): no docstring
    - `async hgetall(self, key)` (line 78): no docstring
    - `async expire(self, _key, _ttl_seconds)` (line 82): no docstring
    - `async lpush(self, key, value)` (line 86): no docstring
    - `async ltrim(self, key, start, stop)` (line 91): no docstring
    - `async lrange(self, key, start, stop)` (line 97): no docstring

### `backend/tests/unit/services/test_action_simulation_assumptions.py`
- **Functions**
  - `test_apply_assumption_patch_applies_set_unset_and_links()` (line 14): no docstring
  - `test_apply_assumption_patch_rejects_forbidden_field()` (line 31): no docstring
  - `test_apply_observed_base_overrides_rejects_unknown_field()` (line 40): no docstring
  - `test_observed_base_override_can_create_conflict()` (line 49): no docstring

### `backend/tests/unit/services/test_action_simulation_permission_profile.py`
- **Functions**
  - `async test_enforce_action_permission_allows_ontology_roles_model(monkeypatch)` (line 11): no docstring
  - `async test_enforce_action_permission_rejects_policy_mismatch(monkeypatch)` (line 31): no docstring
  - `async test_enforce_action_permission_allows_datasource_derived_without_policy(monkeypatch)` (line 53): no docstring
  - `async test_enforce_action_permission_rejects_datasource_derived_for_non_engineer_role(monkeypatch)` (line 72): no docstring
  - `async test_enforce_action_permission_rejects_edits_beyond_actions_without_engineer_role(monkeypatch)` (line 93): no docstring
  - `async test_enforce_action_permission_rejects_invalid_permission_profile(monkeypatch)` (line 118): no docstring

### `backend/tests/unit/services/test_action_simulation_scenarios.py`
- **Functions**
  - `_preflight(conflict_fields, conflict_policy)` (line 13): no docstring
  - `test_conflict_policy_fail_rejects()` (line 53): no docstring
  - `test_conflict_policy_base_wins_skips()` (line 62): no docstring
  - `test_conflict_policy_writeback_wins_applies()` (line 79): no docstring
  - `test_no_conflict_does_not_reject_under_fail()` (line 94): no docstring

### `backend/tests/unit/services/test_admin_recompute_projection_service.py`
- **Functions**
  - `_request_payload(projection)` (line 27): no docstring
  - `_http_request_stub()` (line 38): no docstring
  - `async test_start_recompute_projection_blocks_instances_in_dataset_primary_mode()` (line 46): no docstring
  - `async test_start_recompute_projection_allows_ontologies_in_dataset_primary_mode()` (line 68): no docstring
  - `async test_recompute_projection_task_blocks_instances_in_dataset_primary_mode()` (line 87): no docstring
- **Classes**
  - `_TaskManagerStub` (line 16): no docstring
    - `__init__(self)` (line 17): no docstring
    - `async create_task(self, *args, **kwargs)` (line 20): no docstring

### `backend/tests/unit/services/test_admin_reindex_instances.py`
- **Functions**
  - `async test_reindex_no_mapping_specs()` (line 19): no docstring
  - `async test_reindex_submits_jobs()` (line 36): no docstring
  - `async test_reindex_handles_missing_dataset_version()` (line 72): no docstring
  - `async test_reindex_with_multiple_specs()` (line 103): no docstring
- **Classes**
  - `_FakeSpec` (line 12): no docstring
    - `__init__(self, **kwargs)` (line 13): no docstring

### `backend/tests/unit/services/test_agent_graph_retry.py`
- **Functions**
  - `async test_agent_graph_retries_transient_read_failure()` (line 51): no docstring
  - `async test_agent_graph_does_not_retry_writes_by_default()` (line 113): no docstring
  - `async test_agent_graph_respects_enterprise_max_attempts()` (line 157): no docstring
  - `async test_agent_graph_uses_retry_after_when_allowed()` (line 219): no docstring
- **Classes**
  - `_StubRuntime` (line 9): no docstring
    - `__init__(self, results)` (line 10): no docstring
    - `async record_event(self, **kwargs)` (line 24): no docstring
    - `async execute_tool_call(self, run_id, actor, step_index, attempt, tool_call, context, dry_run, request_headers, request_id)` (line 28): no docstring

### `backend/tests/unit/services/test_agent_graph_simulation_gate.py`
- **Functions**
  - `async test_simulation_rejection_stops_before_submit()` (line 49): no docstring
- **Classes**
  - `_StubRuntime` (line 9): no docstring
    - `__init__(self, results)` (line 10): no docstring
    - `async record_event(self, **kwargs)` (line 24): no docstring
    - `async execute_tool_call(self, run_id, actor, step_index, attempt, tool_call, context, dry_run, request_headers, request_id)` (line 28): no docstring

### `backend/tests/unit/services/test_agent_overlay_policy.py`
- **Functions**
  - `async test_agent_blocks_write_when_overlay_degraded()` (line 19): no docstring
- **Classes**
  - `DummyEventStore` (line 9): no docstring
    - `__init__(self)` (line 10): no docstring
    - `async append_event(self, envelope)` (line 13): no docstring

### `backend/tests/unit/services/test_agent_policy.py`
- **Functions**
  - `test_policy_overlay_degraded_safe_mode()` (line 10): no docstring
  - `test_policy_idempotency_in_progress_is_safe_retry()` (line 23): no docstring
  - `test_policy_timeout_retry_for_reads()` (line 41): no docstring
  - `test_policy_validation_no_retry()` (line 72): no docstring
  - `test_policy_submission_criteria_failed_includes_reason()` (line 104): no docstring
  - `test_policy_submission_criteria_failed_state_mismatch_proposes_check_state()` (line 136): no docstring

### `backend/tests/unit/services/test_agent_retention_worker.py`
- **Functions**
  - `async test_agent_retention_worker_calls_apply_retention_once()` (line 14): no docstring
  - `async test_agent_retention_worker_supports_policy_per_object_type()` (line 46): no docstring

### `backend/tests/unit/services/test_agent_runtime_artifacts.py`
- **Functions**
  - `async test_agent_runtime_blocks_missing_required_artifacts(monkeypatch)` (line 20): no docstring
  - `async test_agent_runtime_stores_produced_artifacts_and_resolves_templates(monkeypatch)` (line 89): no docstring
  - `async test_agent_runtime_compacts_large_tool_payload_instead_of_omitting(monkeypatch)` (line 239): When a tool response is too large, AgentRuntime should attempt to compact it
- **Classes**
  - `DummyEventStore` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `async append_event(self, envelope)` (line 14): no docstring

### `backend/tests/unit/services/test_agent_runtime_delegated_auth.py`
- **Functions**
  - `async test_agent_runtime_sends_service_token_and_delegated_user_token(monkeypatch)` (line 20): no docstring
- **Classes**
  - `DummyEventStore` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `async append_event(self, envelope)` (line 14): no docstring

### `backend/tests/unit/services/test_agent_runtime_pipeline_wait.py`
- **Functions**
  - `async test_agent_runtime_waits_for_pipeline_job_completion(monkeypatch)` (line 20): no docstring
- **Classes**
  - `DummyEventStore` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `async append_event(self, envelope)` (line 14): no docstring

### `backend/tests/unit/services/test_agent_runtime_simulation_signals.py`
- **Functions**
  - `test_extract_action_simulation_signals_rejected_includes_reason()` (line 9): no docstring
  - `test_extract_action_simulation_rejection_returns_enterprise()` (line 39): no docstring
  - `test_extract_action_simulation_rejection_returns_none_when_accepted()` (line 66): no docstring

### `backend/tests/unit/services/test_agent_runtime_templating.py`
- **Functions**
  - `async test_agent_runtime_resolves_step_output_templates(monkeypatch)` (line 19): no docstring
- **Classes**
  - `DummyEventStore` (line 9): no docstring
    - `__init__(self)` (line 10): no docstring
    - `async append_event(self, envelope)` (line 13): no docstring

### `backend/tests/unit/services/test_agent_session_state_machine.py`
- **Functions**
  - `test_validate_session_status_transition_allows_valid_transitions(current_status, next_status)` (line 32): no docstring
  - `test_validate_session_status_transition_rejects_invalid_transitions(current_status, next_status)` (line 43): no docstring
  - `test_validate_session_status_transition_rejects_unknown_states()` (line 48): no docstring

### `backend/tests/unit/services/test_async_terminus_branch_info.py`
- **Functions**
  - `async test_get_branch_info_returns_current_and_protection_flags()` (line 11): no docstring
  - `async test_get_branch_info_raises_for_unknown_branch()` (line 22): no docstring

### `backend/tests/unit/services/test_changelog_store.py`
- **Functions**
  - `async test_record_changelog()` (line 44): no docstring
  - `async test_list_changelogs()` (line 68): no docstring
  - `async test_list_changelogs_with_class_filter()` (line 82): no docstring
  - `async test_get_changelog()` (line 95): no docstring
  - `async test_get_changelog_not_found()` (line 106): no docstring
  - `async test_record_changelog_failure_handled()` (line 116): ChangelogStore should not raise on DB failure.
- **Classes**
  - `_FakePool` (line 12): Minimal asyncpg pool fake.
    - `__init__(self)` (line 15): no docstring
    - `acquire(self)` (line 19): no docstring
  - `_FakeConnection` (line 23): no docstring
    - `__init__(self, pool)` (line 24): no docstring
    - `async __aenter__(self)` (line 27): no docstring
    - `async __aexit__(self, *args)` (line 30): no docstring
    - `async execute(self, query, *args)` (line 33): no docstring
    - `async fetch(self, query, *args)` (line 36): no docstring
    - `async fetchrow(self, query, *args)` (line 39): no docstring

### `backend/tests/unit/services/test_command_status_fallback.py`
- **Functions**
  - `async test_command_status_falls_back_to_registry()` (line 27): no docstring
  - `async test_command_status_falls_back_to_event_store_when_registry_has_no_record()` (line 56): no docstring
- **Classes**
  - `DummyRegistry` (line 9): no docstring
    - `__init__(self, record)` (line 10): no docstring
    - `async get_event_record(self, event_id)` (line 13): no docstring
  - `DummyEventStore` (line 17): no docstring
    - `__init__(self, key)` (line 18): no docstring
    - `async get_event_object_key(self, event_id)` (line 21): no docstring

### `backend/tests/unit/services/test_consistency_token.py`
- **Functions**
  - `async test_token_roundtrip()` (line 12): no docstring
  - `async test_consistency_token_service_creates_metadata()` (line 33): no docstring

### `backend/tests/unit/services/test_database_error_policy.py`
- **Functions**
  - `test_apply_message_error_policies_raises_policy_http_exception()` (line 11): no docstring
  - `test_apply_message_error_policies_returns_fallback_when_configured()` (line 31): no docstring

### `backend/tests/unit/services/test_database_service_error_mapping.py`
- **Functions**
  - `async test_list_branches_maps_not_found_to_404()` (line 27): no docstring
  - `async test_create_branch_maps_duplicate_to_409()` (line 35): no docstring
  - `async test_get_versions_returns_empty_on_empty_history()` (line 43): no docstring
- **Classes**
  - `_FailingOms` (line 9): no docstring
    - `__init__(self, message)` (line 10): no docstring
    - `async list_branches(self, db_name)` (line 13): no docstring
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 17): no docstring
    - `async get_version_history(self, db_name)` (line 21): no docstring

### `backend/tests/unit/services/test_dataset_ingest_commit_service.py`
- **Functions**
  - `async test_ensure_lakefs_commit_artifact_uses_existing_values(monkeypatch)` (line 62): no docstring
  - `async test_ensure_lakefs_commit_artifact_commits_when_missing(monkeypatch)` (line 100): no docstring
  - `async test_persist_ingest_commit_state_skips_when_unchanged()` (line 138): no docstring
  - `async test_persist_ingest_commit_state_marks_and_updates_transaction()` (line 161): no docstring
- **Classes**
  - `_IngestRequest` (line 15): no docstring
  - `_IngestTransaction` (line 22): no docstring
  - `_Registry` (line 26): no docstring
    - `__init__(self)` (line 27): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 31): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 45): no docstring

### `backend/tests/unit/services/test_dataset_output_semantics.py`
- **Functions**
  - `test_normalize_dataset_output_metadata_legacy_aliases()` (line 14): no docstring
  - `test_resolve_dataset_write_policy_default_snapshot()` (line 33): no docstring
  - `test_resolve_dataset_write_policy_default_incremental_with_pk_without_additive_signal()` (line 44): no docstring
  - `test_resolve_dataset_write_policy_default_incremental_without_pk()` (line 56): no docstring
  - `test_resolve_dataset_write_policy_default_streaming_with_pk_uses_append_only_new_rows()` (line 67): no docstring
  - `test_resolve_dataset_write_policy_default_streaming_without_pk_uses_always_append()` (line 79): no docstring
  - `test_resolve_dataset_write_policy_default_incremental_with_additive_updates_false()` (line 92): no docstring
  - `test_resolve_dataset_write_policy_default_incremental_with_additive_updates_true()` (line 105): no docstring
  - `test_resolve_dataset_write_policy_default_incremental_additive_without_pk()` (line 118): no docstring
  - `test_resolve_dataset_write_policy_default_without_incremental_inputs()` (line 132): no docstring
  - `test_resolve_dataset_write_policy_snapshot_difference_uses_snapshot_runtime()` (line 144): no docstring
  - `test_resolve_dataset_write_policy_changelog_uses_append_runtime()` (line 155): no docstring
  - `test_validate_dataset_output_metadata_requires_pk_and_post_filtering()` (line 166): no docstring
  - `test_validate_dataset_output_metadata_checks_available_columns()` (line 177): no docstring
  - `test_validate_dataset_output_metadata_rejects_csv_with_partition_by()` (line 193): no docstring
  - `test_validate_dataset_output_metadata_rejects_json_with_partition_by()` (line 207): no docstring
  - `test_dataset_write_policy_hash_stable()` (line 221): no docstring

### `backend/tests/unit/services/test_dlq_handler_fixed.py`
- **Functions**
  - `_bootstrap_servers()` (line 13): no docstring
  - `async test_dlq_handler_retry_and_poison_flow()` (line 85): no docstring
  - `async test_dlq_handler_retry_success_records_recovery()` (line 130): no docstring
- **Classes**
  - `_FakeProducer` (line 17): no docstring
    - `__init__(self)` (line 18): no docstring
    - `produce(self, topic, key, value)` (line 21): no docstring
    - `flush(self, *_args, **_kwargs)` (line 24): no docstring
  - `_FakeRedis` (line 28): no docstring
    - `__init__(self)` (line 29): no docstring
    - `async hset(self, key, mapping)` (line 33): no docstring
    - `async expire(self, _key, _ttl)` (line 40): no docstring
    - `async hgetall(self, key)` (line 43): no docstring
    - `async lpush(self, key, value)` (line 47): no docstring
    - `async ltrim(self, key, start, end)` (line 52): no docstring
    - `async lrange(self, key, start, end)` (line 61): no docstring
    - `async delete(self, *keys)` (line 69): no docstring
    - `async aclose(self)` (line 80): no docstring

### `backend/tests/unit/services/test_dlq_payload_shapes.py`
- **Functions**
  - `async test_action_worker_send_to_dlq_payload_shape()` (line 77): no docstring
  - `async test_ontology_worker_send_to_dlq_payload_shape()` (line 123): no docstring
  - `async test_instance_worker_send_to_dlq_payload_shape()` (line 166): no docstring
- **Classes**
  - `_DummyMsg` (line 10): no docstring
    - `__init__(self, topic, partition, offset, value, key, timestamp_ms, headers)` (line 11): no docstring
    - `topic(self)` (line 30): no docstring
    - `partition(self)` (line 33): no docstring
    - `offset(self)` (line 36): no docstring
    - `value(self)` (line 39): no docstring
    - `key(self)` (line 42): no docstring
    - `timestamp(self)` (line 45): no docstring
    - `headers(self)` (line 50): no docstring
  - `_CaptureProducer` (line 54): no docstring
    - `__init__(self)` (line 55): no docstring
    - `produce(self, topic, key, value, headers, **kwargs)` (line 58): no docstring
    - `flush(self, *_args, **_kwargs)` (line 61): no docstring
  - `_NoopTracing` (line 65): no docstring
    - `span(self, *_args, **_kwargs)` (line 67): no docstring
  - `_NoopMetrics` (line 71): no docstring
    - `record_event(self, *_args, **_kwargs)` (line 72): no docstring

### `backend/tests/unit/services/test_envelope_dlq_publisher.py`
- **Functions**
  - `async test_build_envelope_dlq_event_applies_standard_metadata()` (line 45): no docstring
  - `async test_publish_envelope_dlq_uses_producer_ops_and_flushes()` (line 80): no docstring
- **Classes**
  - `_CaptureProducerOps` (line 17): no docstring
    - `__init__(self)` (line 18): no docstring
    - `async produce(self, **kwargs)` (line 22): no docstring
    - `async flush(self, timeout_s)` (line 25): no docstring
  - `_CaptureMetrics` (line 30): no docstring
    - `__init__(self)` (line 31): no docstring
    - `record_event(self, name, action)` (line 34): no docstring
  - `_NoopTracing` (line 38): no docstring
    - `span(self, *_args, **_kwargs)` (line 40): no docstring

### `backend/tests/unit/services/test_event_replay.py`
- **Functions**
  - `_s3_client()` (line 57): no docstring
  - `_ensure_bucket(client, bucket)` (line 61): no docstring
  - `_put_event(client, bucket, key, payload)` (line 68): no docstring
  - `_cleanup_prefix(client, bucket, prefix)` (line 72): no docstring
  - `async test_event_replay_aggregate_and_history()` (line 82): no docstring
  - `async test_event_replay_all_and_determinism()` (line 126): no docstring
- **Classes**
  - `_InMemoryS3Client` (line 13): no docstring
    - `__init__(self)` (line 14): no docstring
    - `head_bucket(self, Bucket)` (line 17): no docstring
    - `create_bucket(self, Bucket)` (line 21): no docstring
    - `put_object(self, Bucket, Key, Body)` (line 24): no docstring
    - `get_object(self, Bucket, Key)` (line 31): no docstring
    - `list_objects_v2(self, Bucket, Prefix, MaxKeys)` (line 35): no docstring
    - `delete_object(self, Bucket, Key)` (line 52): no docstring

### `backend/tests/unit/services/test_event_store_connect_idempotent.py`
- **Functions**
  - `async test_event_store_connect_is_idempotent_under_concurrency(monkeypatch)` (line 49): no docstring
  - `async test_event_store_connect_fails_when_lineage_required_and_store_unavailable(monkeypatch)` (line 79): no docstring
  - `async test_event_store_lineage_record_failure_propagates_when_required(monkeypatch)` (line 102): no docstring
- **Classes**
  - `_DummyS3` (line 14): no docstring
    - `__init__(self, counters)` (line 15): no docstring
    - `async head_bucket(self, **_)` (line 18): no docstring
    - `async create_bucket(self, **_)` (line 21): no docstring
    - `async put_bucket_versioning(self, **_)` (line 24): no docstring
  - `_DummyS3ClientContext` (line 28): no docstring
    - `__init__(self, s3)` (line 29): no docstring
    - `async __aenter__(self)` (line 32): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 35): no docstring
  - `_DummySession` (line 39): no docstring
    - `__init__(self, s3)` (line 40): no docstring
    - `client(self, **_)` (line 43): no docstring

### `backend/tests/unit/services/test_fk_pattern_detector.py`
- **Classes**
  - `TestFKDetectionConfig` (line 15): no docstring
    - `test_default_config(self)` (line 16): no docstring
    - `test_from_dict(self)` (line 23): no docstring
  - `TestForeignKeyPatternDetector` (line 34): no docstring
    - `test_detect_by_naming_id_suffix(self)` (line 35): _id suffix columns should be detected as FK candidates
    - `test_detect_by_naming_fk_suffix(self)` (line 55): _fk suffix columns should be detected with high confidence
    - `test_exclude_timestamp_columns(self)` (line 71): Timestamp columns should be excluded from FK detection
    - `test_exclude_boolean_prefixes(self)` (line 91): Boolean-like columns should be excluded
    - `test_match_with_target_candidates(self)` (line 109): FK should match with target candidates when name matches
    - `test_match_plural_target_name(self)` (line 135): FK should match with pluralized target names
    - `test_min_confidence_filter(self)` (line 159): Patterns below min_confidence should be filtered out
    - `test_suggest_link_type(self)` (line 175): suggest_link_type should generate valid link_type structure
    - `test_generate_predicate_from_column(self)` (line 197): Predicate generation should follow naming convention
    - `test_incompatible_types_excluded(self)` (line 206): Non-FK-compatible types should be excluded
    - `test_empty_schema(self)` (line 223): Empty schema should return no patterns
    - `test_patterns_sorted_by_confidence(self)` (line 232): Patterns should be sorted by confidence descending

### `backend/tests/unit/services/test_funnel_data_processor.py`
- **Functions**
  - `async test_process_google_sheets_preview_sets_explicit_timeout(monkeypatch)` (line 7): no docstring

### `backend/tests/unit/services/test_graph_federation_service_es.py`
- **Functions**
  - `_make_es_service()` (line 13): no docstring
  - `_make_doc(class_id, instance_id, relationships)` (line 19): no docstring
  - `_mget_response(docs)` (line 28): Build an ES _mget response from a list of source dicts.
  - `test_normalize_hops_dict()` (line 42): no docstring
  - `test_normalize_hops_tuple()` (line 50): no docstring
  - `test_normalize_hops_empty()` (line 58): no docstring
  - `test_node_id()` (line 68): no docstring
  - `test_parse_ref()` (line 73): no docstring
  - `test_get_relationship_refs()` (line 78): no docstring
  - `async test_forward_single_hop()` (line 91): Customer -> orders -> Order
  - `async test_reverse_single_hop()` (line 132): Order -> (reverse) orders -> Customer
  - `async test_multi_hop_3_layers()` (line 174): Customer -> orders -> Order -> payments -> Payment
  - `async test_fan_out_cap()` (line 218): Verify max_fan_out caps the number of targets fetched.
  - `async test_no_cycles()` (line 261): A -> B -> A should be filtered when no_cycles=True.
  - `async test_empty_relationships()` (line 306): Docs with no relationships yield no targets.
  - `async test_simple_graph_query()` (line 340): no docstring
  - `async test_find_paths_es_sampling()` (line 363): Discover Customer -> Order -> Payment path via ES doc sampling.
  - `async test_empty_start_returns_empty()` (line 410): no docstring

### `backend/tests/unit/services/test_graph_service_health.py`
- **Functions**
  - `_make_graph_service(es_status)` (line 12): Create a mock GraphFederationServiceES with ES health.
  - `async test_health_green()` (line 22): no docstring
  - `async test_health_yellow()` (line 32): no docstring
  - `async test_health_red()` (line 41): no docstring
  - `async test_health_connection_failure()` (line 51): no docstring

### `backend/tests/unit/services/test_health_check_redis.py`
- **Functions**
  - `async test_redis_health_check_includes_info_details()` (line 24): no docstring
  - `async test_redis_health_check_ignores_info_errors()` (line 37): no docstring
- **Classes**
  - `RedisServiceWithInfo` (line 6): no docstring
    - `async ping(self)` (line 7): no docstring
    - `async info(self)` (line 10): no docstring
  - `RedisServiceInfoError` (line 14): no docstring
    - `async ping(self)` (line 15): no docstring
    - `async info(self)` (line 18): no docstring

### `backend/tests/unit/services/test_idempotency_service.py`
- **Functions**
  - `async test_idempotency_service_detects_duplicates()` (line 12): no docstring
  - `async test_idempotency_service_marks_processed_and_failed()` (line 45): no docstring

### `backend/tests/unit/services/test_input_validation_service.py`
- **Functions**
  - `test_validated_db_name_returns_valid_value(monkeypatch)` (line 10): no docstring
  - `test_validated_db_name_converts_value_error_to_http_400(monkeypatch)` (line 15): no docstring
  - `test_validated_branch_name_converts_security_violation(monkeypatch)` (line 26): no docstring
  - `test_sanitized_payload_returns_sanitized_data(monkeypatch)` (line 37): no docstring
  - `test_sanitized_payload_converts_security_violation(monkeypatch)` (line 42): no docstring

### `backend/tests/unit/services/test_instance_index_rebuild_service.py`
- **Functions**
  - `_mock_es()` (line 18): Create a mock ElasticsearchService with common methods.
  - `async test_rebuild_success_with_existing_alias()` (line 49): Rebuild with existing alias: reindex + swap.
  - `async test_rebuild_success_no_existing_data()` (line 77): Rebuild when no existing index exists — creates empty index with alias.
  - `async test_rebuild_success_concrete_index()` (line 98): Rebuild when base_index is a concrete index (not alias).
  - `async test_rebuild_alias_swap_failure()` (line 116): Rebuild fails when alias swap fails — cleanup partial index.
  - `async test_resolve_alias_targets()` (line 137): _resolve_alias_targets returns concrete indices behind alias.
  - `async test_resolve_alias_targets_no_alias()` (line 151): _resolve_alias_targets returns empty list when alias doesn't exist.
  - `async test_reindex_from_source()` (line 161): _reindex_from_source calls ES reindex API.
  - `async test_get_class_counts()` (line 174): _get_class_counts returns per-class document counts.
  - `async test_get_class_counts_empty_index()` (line 183): _get_class_counts returns empty dict on empty index.

### `backend/tests/unit/services/test_instances_service_projection_mode.py`
- **Functions**
  - `async test_list_instances_es_error_fails_closed_without_fallback()` (line 54): no docstring
  - `async test_get_instance_es_error_fails_closed_without_fallback()` (line 82): no docstring
  - `async test_get_instance_missing_doc_returns_404_without_fallback()` (line 105): no docstring
  - `async test_sample_values_reads_from_es()` (line 126): no docstring
  - `async test_sample_values_es_error_fails_closed()` (line 156): no docstring
- **Classes**
  - `_FakeDatasetRegistry` (line 16): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 17): no docstring
  - `_FakeElasticsearchService` (line 28): no docstring
    - `__init__(self, search_result, search_exception, document_result)` (line 29): no docstring
    - `async search(self, **kwargs)` (line 42): no docstring
    - `async get_document(self, index, doc_id)` (line 48): no docstring

### `backend/tests/unit/services/test_lakefs_branch_utils.py`
- **Functions**
  - `async test_ensure_lakefs_branch_creates_branch()` (line 19): no docstring
  - `async test_ensure_lakefs_branch_ignores_conflict()` (line 26): no docstring
  - `async test_ensure_lakefs_branch_requires_client()` (line 33): no docstring
- **Classes**
  - `_FakeLakeFSClient` (line 7): no docstring
    - `__init__(self, should_conflict)` (line 8): no docstring
    - `async create_branch(self, repository, name, source)` (line 12): no docstring

### `backend/tests/unit/services/test_llm_gateway_resilience.py`
- **Functions**
  - `async test_llm_gateway_retries_on_http_5xx(monkeypatch)` (line 18): no docstring
  - `async test_llm_gateway_circuit_breaker_opens(monkeypatch)` (line 60): no docstring
  - `async test_llm_gateway_cache_key_is_partition_scoped(monkeypatch)` (line 108): no docstring
  - `async test_llm_gateway_native_tool_calling_falls_back_on_unsupported(monkeypatch)` (line 156): no docstring
  - `async test_llm_gateway_provider_policy_can_disable_cache(monkeypatch)` (line 191): no docstring
  - `async test_llm_gateway_provider_policy_can_set_no_store_and_partition_isolation(monkeypatch)` (line 229): no docstring
  - `async test_llm_gateway_provider_policy_can_set_actor_isolation_for_anthropic(monkeypatch)` (line 264): no docstring
- **Classes**
  - `_Out` (line 12): no docstring

### `backend/tests/unit/services/test_llm_quota.py`
- **Functions**
  - `async test_llm_quota_is_noop_without_policy()` (line 29): no docstring
  - `async test_llm_quota_raises_when_denied()` (line 44): no docstring
  - `async test_llm_quota_consumes_for_tenant_and_user()` (line 61): no docstring
- **Classes**
  - `_Client` (line 10): no docstring
    - `__init__(self, responses)` (line 11): no docstring
    - `async eval(self, *args)` (line 15): no docstring
  - `_Redis` (line 22): no docstring
    - `__init__(self, responses)` (line 23): no docstring

### `backend/tests/unit/services/test_object_type_meta_resolver.py`
- **Functions**
  - `async test_object_type_meta_resolver_parses_and_caches()` (line 27): no docstring
  - `async test_object_type_meta_resolver_blank_id_uses_default()` (line 40): no docstring
  - `async test_object_type_meta_resolver_on_error_returns_default_and_caches()` (line 51): no docstring
- **Classes**
  - `_FakeResources` (line 6): no docstring
    - `__init__(self, response, should_raise)` (line 7): no docstring
    - `async get_resource(self, db_name, branch, resource_type, resource_id)` (line 12): no docstring

### `backend/tests/unit/services/test_objectify_dag_service.py`
- **Functions**
  - `_make_orchestrator(record)` (line 21): no docstring
  - `async test_wait_for_objectify_submitted_allows_dataset_primary_completed_without_commands()` (line 33): no docstring
  - `async test_wait_for_objectify_submitted_defaults_to_dataset_primary_when_report_missing()` (line 47): no docstring
  - `async test_wait_for_objectify_submitted_requires_commands_for_submitted_status()` (line 61): no docstring
- **Classes**
  - `_FakeObjectifyRegistry` (line 14): no docstring
    - `async get_objectify_job(self, job_id)` (line 17): no docstring

### `backend/tests/unit/services/test_objectify_delta_utils.py`
- **Classes**
  - `TestDeltaResult` (line 16): no docstring
    - `test_default_stats(self)` (line 17): DeltaResult should compute stats on creation
    - `test_has_changes_true(self)` (line 30): has_changes should be True when there are changes
    - `test_has_changes_false(self)` (line 35): has_changes should be False when empty
  - `TestWatermarkState` (line 41): no docstring
    - `test_to_dict(self)` (line 42): to_dict should serialize watermark state
    - `test_from_dict(self)` (line 58): from_dict should deserialize watermark state
  - `TestObjectifyDeltaComputer` (line 76): no docstring
    - `test_compute_row_key_single_pk(self)` (line 77): compute_row_key should create key from single PK
    - `test_compute_row_key_composite_pk(self)` (line 85): compute_row_key should create key from composite PK
    - `test_compute_row_hash(self)` (line 93): compute_row_hash should be stable
    - `test_compare_watermarks_datetime(self)` (line 104): _compare_watermarks should handle datetime
    - `test_compare_watermarks_numeric(self)` (line 115): _compare_watermarks should handle numeric values
    - `test_compare_watermarks_string(self)` (line 123): _compare_watermarks should handle string values
    - `test_compare_watermarks_none_handling(self)` (line 131): _compare_watermarks should handle None values
    - `test_filter_rows_by_watermark(self)` (line 139): filter_rows_by_watermark should filter correctly
    - `test_filter_rows_by_watermark_empty(self)` (line 161): filter_rows_by_watermark should handle no matches
    - `test_filter_rows_by_watermark_null_values(self)` (line 179): filter_rows_by_watermark should skip null watermark values
  - `TestCreateDeltaComputerForMappingSpec` (line 201): no docstring
    - `test_creates_computer_with_pk_from_spec(self)` (line 202): Should extract pk_columns from mapping_spec
    - `test_fallback_to_id_field(self)` (line 216): Should fall back to 'id' field if no pk_spec
    - `test_fallback_to_first_field(self)` (line 229): Should fall back to first field if no id column

### `backend/tests/unit/services/test_occ_patterns.py`
- **Classes**
  - `TestOCCConflictError` (line 14): no docstring
    - `test_error_message(self)` (line 15): OCCConflictError should include helpful details
    - `test_error_without_actual_version(self)` (line 33): OCCConflictError should handle missing actual version
  - `TestObjectifyJobRecordOCC` (line 46): no docstring
    - `test_record_has_occ_version_default(self)` (line 47): ObjectifyJobRecord should have occ_version field with default
    - `test_record_with_explicit_occ_version(self)` (line 74): ObjectifyJobRecord should accept explicit occ_version
  - `TestOntologyMappingSpecRecordOCC` (line 102): no docstring
    - `test_record_has_occ_version_default(self)` (line 103): OntologyMappingSpecRecord should have occ_version field
    - `test_record_with_explicit_occ_version(self)` (line 131): OntologyMappingSpecRecord should accept explicit occ_version
  - `TestOCCPatternIntegration` (line 159): Tests for OCC pattern usage scenarios
    - `test_occ_conflict_is_runtime_error(self)` (line 162): OCCConflictError should be a RuntimeError for easy catching
    - `test_occ_error_can_be_raised_and_caught(self)` (line 173): OCCConflictError should be properly catchable

### `backend/tests/unit/services/test_oms_client_branch_api.py`
- **Functions**
  - `_build_client()` (line 39): no docstring
  - `async test_create_branch_accepts_string_input()` (line 47): no docstring
  - `async test_create_branch_accepts_dict_input_for_backward_compatibility()` (line 57): no docstring
  - `async test_get_and_delete_branch_use_typed_paths()` (line 67): no docstring
- **Classes**
  - `_FakeResponse` (line 10): no docstring
    - `__init__(self, payload)` (line 11): no docstring
    - `raise_for_status(self)` (line 15): no docstring
    - `json(self)` (line 18): no docstring
  - `_FakeAsyncClient` (line 22): no docstring
    - `__init__(self)` (line 23): no docstring
    - `async get(self, path, **kwargs)` (line 26): no docstring
    - `async post(self, path, **kwargs)` (line 30): no docstring
    - `async delete(self, path, **kwargs)` (line 34): no docstring

### `backend/tests/unit/services/test_oms_error_policy.py`
- **Functions**
  - `_http_status_error(status_code, body)` (line 12): no docstring
  - `test_raise_oms_boundary_exception_with_custom_http_status_detail()` (line 18): no docstring
  - `test_raise_oms_boundary_exception_maps_value_error_to_400()` (line 33): no docstring
  - `test_raise_oms_boundary_exception_maps_generic_to_500()` (line 44): no docstring

### `backend/tests/unit/services/test_ontology_class_id_service.py`
- **Functions**
  - `test_resolve_or_generate_class_id_returns_validated_id(monkeypatch)` (line 10): no docstring
  - `test_resolve_or_generate_class_id_converts_validation_error(monkeypatch)` (line 16): no docstring
  - `test_resolve_or_generate_class_id_converts_security_violation(monkeypatch)` (line 27): no docstring
  - `test_resolve_or_generate_class_id_generates_when_missing(monkeypatch)` (line 38): no docstring

### `backend/tests/unit/services/test_ontology_deployment_registry_template_method.py`
- **Functions**
  - `async test_registry_ensure_schema_builds_indexes_for_v1(monkeypatch)` (line 44): no docstring
  - `async test_registry_ensure_schema_builds_indexes_for_v2(monkeypatch)` (line 62): no docstring
  - `async test_registry_v2_claim_batch_normalizes_json_payload(monkeypatch)` (line 80): no docstring
  - `async test_registry_purge_outbox_uses_registry_table(monkeypatch)` (line 110): no docstring
- **Classes**
  - `_FakeOutboxStore` (line 13): no docstring
    - `__init__(self, claim_items)` (line 14): no docstring
    - `async claim_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 20): no docstring
    - `async mark_published(self, outbox_id)` (line 36): no docstring
    - `async mark_failed(self, outbox_id, error, next_attempt_at)` (line 39): no docstring

### `backend/tests/unit/services/test_ontology_interface_contract.py`
- **Functions**
  - `test_interface_contract_missing_property_is_reported()` (line 5): no docstring
  - `test_interface_contract_missing_interface_is_reported()` (line 19): no docstring

### `backend/tests/unit/services/test_ontology_linter_pk_branching.py`
- **Functions**
  - `_reset_env(monkeypatch)` (line 9): no docstring
  - `_make_properties()` (line 24): no docstring
  - `test_linter_allows_implicit_pk_on_dev_branch(monkeypatch)` (line 40): no docstring
  - `test_linter_blocks_implicit_pk_on_protected_branch(monkeypatch)` (line 60): no docstring
  - `test_linter_requires_explicit_title_key_when_disabled(monkeypatch)` (line 79): no docstring

### `backend/tests/unit/services/test_ontology_occ_guard_service.py`
- **Functions**
  - `_http_404()` (line 58): no docstring
  - `async test_resolve_expected_head_commit_uses_given_value_without_calling_oms()` (line 64): no docstring
  - `async test_fetch_branch_head_commit_id_extracts_from_head_payload()` (line 79): no docstring
  - `async test_resolve_expected_head_commit_falls_back_to_commit_keys()` (line 95): no docstring
  - `async test_resolve_expected_head_commit_raises_when_unresolved()` (line 116): no docstring
  - `async test_resolve_expected_head_commit_allows_none_when_configured()` (line 132): no docstring
  - `async test_resolve_branch_head_commit_with_bootstrap_retries_after_branch_create()` (line 147): no docstring
  - `async test_resolve_branch_head_commit_with_bootstrap_accepts_branch_conflict()` (line 169): no docstring
  - `async test_resolve_branch_head_commit_with_bootstrap_returns_none_when_unresolved()` (line 191): no docstring
- **Classes**
  - `_FakeOMSClient` (line 14): no docstring
    - `__init__(self, payload)` (line 15): no docstring
    - `async get_version_head(self, db_name, branch)` (line 21): no docstring
  - `_BootstrapOMSClient` (line 28): no docstring
    - `__init__(self, heads, create_branch_status)` (line 29): no docstring
    - `async get_version_head(self, db_name, branch)` (line 35): no docstring
    - `async create_branch(self, db_name, branch_data)` (line 46): no docstring

### `backend/tests/unit/services/test_ontology_resource_validator.py`
- **Functions**
  - `test_action_type_requires_input_schema_and_policy()` (line 20): no docstring
  - `test_object_type_requires_pk_spec_and_backing_source()` (line 29): no docstring
  - `test_shared_property_requires_properties_list()` (line 38): no docstring
  - `test_function_requires_expression_and_return_type_ref()` (line 44): no docstring
  - `test_action_type_rejects_unsafe_submission_criteria_expression()` (line 51): no docstring
  - `test_action_type_rejects_invalid_validation_rules()` (line 67): no docstring
  - `test_link_type_invalid_predicate_is_reported()` (line 83): no docstring
  - `test_relationship_spec_missing_is_reported()` (line 90): no docstring
  - `test_relationship_spec_invalid_type_is_reported()` (line 96): no docstring
  - `test_relationship_spec_object_backed_requires_object_type()` (line 102): no docstring
  - `test_relationship_spec_join_table_requires_dataset_or_auto_create()` (line 117): no docstring
  - `async test_link_type_missing_refs_are_reported()` (line 134): no docstring
  - `test_permission_policy_allows_legacy_roles_alias_for_runtime_compat()` (line 146): no docstring
  - `test_permission_policy_allows_legacy_users_alias_for_runtime_compat()` (line 157): no docstring
  - `test_permission_policy_rejects_unsupported_legacy_fields()` (line 168): no docstring
  - `test_action_type_datasource_derived_allows_missing_permission_policy()` (line 181): no docstring
  - `test_action_type_rejects_invalid_permission_model()` (line 197): no docstring
  - `test_action_type_rejects_non_boolean_edits_beyond_actions()` (line 213): no docstring
- **Classes**
  - `_FakeTerminus` (line 12): no docstring
    - `__init__(self, existing)` (line 13): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 16): no docstring

### `backend/tests/unit/services/test_ontology_router_helpers.py`
- **Functions**
  - `test_extract_group_refs_dedupes()` (line 32): no docstring
  - `async test_validate_group_refs_reports_missing()` (line 44): no docstring
  - `async test_apply_shared_properties_merges_and_tracks_duplicates()` (line 58): no docstring
  - `async test_validate_value_type_refs_detects_base_type_mismatch()` (line 87): no docstring
  - `async test_collect_interface_issues_reports_missing_property()` (line 114): no docstring
  - `async test_validate_relationships_gate_returns_422()` (line 137): no docstring
- **Classes**
  - `_FakeResourceService` (line 16): no docstring
    - `__init__(self, resources)` (line 17): no docstring
    - `async get_resource(self, db_name, branch, resource_type, resource_id)` (line 20): no docstring
  - `_FakeTerminus` (line 24): no docstring
    - `__init__(self, response)` (line 25): no docstring
    - `async validate_relationships(self, db_name, ontology_payload, branch)` (line 28): no docstring

### `backend/tests/unit/services/test_ontology_value_type_immutability.py`
- **Functions**
  - `test_value_type_immutability_blocks_base_type_change()` (line 8): no docstring
  - `test_value_type_immutability_blocks_constraint_change()` (line 18): no docstring
  - `test_value_type_immutability_allows_same_spec()` (line 28): no docstring

### `backend/tests/unit/services/test_outbox_runtime.py`
- **Functions**
  - `test_build_outbox_worker_id_prefers_configured_value()` (line 17): no docstring
  - `test_build_outbox_worker_id_uses_service_and_hostname()` (line 29): no docstring
  - `async test_maybe_purge_with_interval_executes_and_logs()` (line 40): no docstring
  - `async test_run_outbox_poll_loop_runs_flush_purge_and_close()` (line 68): no docstring
  - `async test_flush_outbox_until_empty_stops_and_closes()` (line 104): no docstring
  - `async test_flush_outbox_until_empty_propagates_primary_error_without_close()` (line 127): no docstring
  - `async test_flush_outbox_until_empty_keeps_primary_error_when_close_also_fails(caplog)` (line 140): no docstring
  - `async test_flush_outbox_until_empty_raises_close_error_when_no_primary_error()` (line 159): no docstring
  - `async test_run_outbox_poll_loop_raises_close_error_when_no_primary_error()` (line 175): no docstring

### `backend/tests/unit/services/test_output_plugins.py`
- **Functions**
  - `test_normalize_output_kind_supports_legacy_aliases()` (line 19): no docstring
  - `test_resolve_output_kind_reports_alias_usage()` (line 26): no docstring
  - `test_validate_output_payload_ontology_object_requires_target_class()` (line 34): no docstring
  - `test_validate_output_payload_ontology_link_requires_full_metadata()` (line 40): no docstring
  - `test_validate_output_payload_ontology_rejects_unknown_relationship_spec_type()` (line 55): no docstring
  - `test_resolve_ontology_output_semantics_exposes_required_columns_for_link()` (line 67): no docstring
  - `test_validate_output_payload_dataset_has_no_required_fields()` (line 85): no docstring
  - `test_validate_output_payload_dataset_requires_pk_for_append_only_new_rows()` (line 90): no docstring
  - `test_validate_output_payload_dataset_requires_post_filtering_for_snapshot_remove()` (line 99): no docstring
  - `test_validate_output_payload_dataset_rejects_unsupported_output_format()` (line 111): no docstring
  - `test_validate_output_payload_dataset_rejects_partitioned_csv()` (line 120): no docstring
  - `test_validate_output_payload_dataset_rejects_partitioned_json()` (line 131): no docstring
  - `test_validate_output_payload_dataset_accepts_full_metadata()` (line 142): no docstring
  - `test_validate_output_payload_geotemporal_requires_metadata()` (line 159): no docstring
  - `test_validate_output_payload_geotemporal_accepts_camel_case()` (line 166): no docstring
  - `test_validate_output_payload_media_requires_type_enum()` (line 181): no docstring
  - `test_validate_output_payload_virtual_requires_refresh_mode_enum()` (line 193): no docstring
  - `test_validate_output_payload_virtual_accepts_required_values()` (line 205): no docstring
  - `test_validate_output_payload_virtual_rejects_dataset_write_settings()` (line 219): no docstring
  - `test_normalize_output_kind_rejects_unknown_kind()` (line 235): no docstring

### `backend/tests/unit/services/test_pipeline_advanced_transforms.py`
- **Functions**
  - `test_add_split_expands_to_true_false_filter_nodes()` (line 20): no docstring
  - `test_validate_pipeline_definition_accepts_advanced_transforms()` (line 51): no docstring
  - `test_validate_pipeline_definition_rejects_invalid_stream_join_strategy()` (line 128): no docstring
  - `test_validate_pipeline_definition_rejects_invalid_stream_join_time_direction()` (line 161): no docstring
  - `test_validate_pipeline_definition_rejects_dynamic_stream_join_without_event_time_fields()` (line 202): no docstring
  - `test_validate_pipeline_definition_rejects_left_lookup_with_transformed_right_input()` (line 239): no docstring
  - `test_validate_pipeline_definition_rejects_left_lookup_with_streaming_right_input()` (line 278): no docstring
  - `test_validate_pipeline_definition_rejects_static_with_transformed_right_input()` (line 315): no docstring
  - `test_validate_pipeline_definition_rejects_static_with_streaming_right_input()` (line 354): no docstring
  - `test_validate_pipeline_definition_rejects_static_non_left_join_type()` (line 391): no docstring
  - `test_builder_helpers_add_geospatial_pattern_mining_stream_join()` (line 425): no docstring

### `backend/tests/unit/services/test_pipeline_agent_allowed_tools.py`
- **Functions**
  - `_extract_agent_allowed_tools()` (line 10): no docstring
  - `_extract_pipeline_mcp_plan_tools()` (line 28): no docstring
  - `test_agent_allowed_tools_include_advanced_plan_tools()` (line 35): no docstring
  - `test_agent_allowed_tools_cover_pipeline_plan_tools()` (line 48): no docstring

### `backend/tests/unit/services/test_pipeline_claim_refuter.py`
- **Functions**
  - `async test_refuter_pk_duplicate_is_hard_failure()` (line 10): no docstring
  - `async test_refuter_join_functional_right_detects_duplicate_right_matches_left()` (line 54): no docstring
  - `async test_refuter_cast_success_finds_parse_failure()` (line 100): no docstring
  - `async test_refuter_cast_lossless_detects_leading_zero_loss()` (line 136): no docstring
  - `async test_refuter_cast_lossless_allows_strip_leading_zeros()` (line 180): no docstring
  - `async test_refuter_fk_hard_is_downgraded_to_soft()` (line 222): no docstring
  - `async test_refuter_filter_only_nulls_finds_removed_non_null_row()` (line 252): no docstring
  - `async test_refuter_filter_min_retain_rate_finds_low_retention()` (line 297): no docstring
  - `async test_refuter_union_row_lossless_finds_missing_input_row()` (line 341): no docstring
  - `async test_refuter_fails_open_when_unable_to_execute_preview()` (line 381): Preview execution failures are not counterexamples; the refuter must never hard-block without a witness.

### `backend/tests/unit/services/test_pipeline_cleansing_upstream_push.py`
- **Functions**
  - `test_cleansing_upstream_push_common_ancestor()` (line 10): no docstring

### `backend/tests/unit/services/test_pipeline_control_plane_events.py`
- **Functions**
  - `async test_control_plane_events_always_on(monkeypatch)` (line 9): no docstring
  - `async test_control_plane_event_emits_with_topic(monkeypatch)` (line 34): no docstring

### `backend/tests/unit/services/test_pipeline_definition_utils_columns.py`
- **Functions**
  - `test_normalize_expectation_columns_with_csv()` (line 4): no docstring
  - `test_normalize_expectation_columns_with_list()` (line 8): no docstring
  - `test_normalize_expectation_columns_with_none()` (line 12): no docstring

### `backend/tests/unit/services/test_pipeline_definition_validator.py`
- **Functions**
  - `_spark_policy(require_output)` (line 10): no docstring
  - `test_validate_pipeline_definition_requires_nodes()` (line 19): no docstring
  - `test_validate_pipeline_definition_requires_output_node_when_configured()` (line 24): no docstring
  - `test_validate_pipeline_definition_detects_missing_edge_nodes()` (line 36): no docstring
  - `test_validate_pipeline_definition_normalizes_metadata_fields_to_columns()` (line 48): no docstring
  - `test_validate_pipeline_definition_reports_missing_columns_for_normalize()` (line 61): no docstring
  - `test_validate_pipeline_definition_uses_custom_udf_message()` (line 74): no docstring
  - `test_validate_pipeline_definition_udf_requires_udf_id_when_reference_policy_enabled()` (line 93): no docstring
  - `test_validate_pipeline_definition_udf_rejects_inline_code_when_reference_policy_enabled()` (line 106): no docstring
  - `test_validate_pipeline_definition_udf_requires_version_when_pinning_enabled()` (line 123): no docstring
  - `test_validate_pipeline_definition_rejects_invalid_dataset_output_metadata()` (line 147): no docstring
  - `test_validate_pipeline_definition_rejects_streaming_non_kafka_external_input()` (line 163): no docstring
  - `test_validate_pipeline_definition_rejects_streaming_without_checkpoint_location()` (line 187): no docstring
  - `test_validate_pipeline_definition_rejects_kafka_json_without_schema()` (line 211): no docstring
  - `test_validate_pipeline_definition_allows_kafka_avro_with_schema_registry_reference()` (line 235): no docstring
  - `test_validate_pipeline_definition_rejects_kafka_avro_without_schema_or_registry()` (line 261): no docstring
  - `test_validate_pipeline_definition_rejects_kafka_avro_with_missing_registry_version()` (line 280): no docstring
  - `test_validate_pipeline_definition_rejects_kafka_avro_with_latest_registry_version()` (line 305): no docstring
  - `test_validate_pipeline_definition_allows_batch_kafka_without_checkpoint()` (line 334): no docstring
  - `test_validate_pipeline_definition_requires_watermark_for_streaming_semantics()` (line 355): no docstring

### `backend/tests/unit/services/test_pipeline_execution_service_dataset_policy.py`
- **Functions**
  - `test_resolve_output_contract_from_definition_merges_declared_metadata()` (line 10): no docstring
  - `test_dataset_write_policy_hash_consistent_for_definition_contract()` (line 48): no docstring

### `backend/tests/unit/services/test_pipeline_executor_csv_parsing.py`
- **Functions**
  - `async test_executor_parses_quoted_csv_headers_and_joins_correctly()` (line 65): no docstring
- **Classes**
  - `_Dataset` (line 12): no docstring
  - `_Version` (line 21): no docstring
  - `_DatasetRegistry` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 38): no docstring
    - `async get_latest_version(self, dataset_id)` (line 41): no docstring
  - `_StorageService` (line 45): no docstring
    - `__init__(self, objects)` (line 46): no docstring
    - `async load_bytes_lines(self, bucket, key, max_lines, max_bytes)` (line 49): no docstring
    - `async load_bytes(self, bucket, key)` (line 56): no docstring
    - `async list_objects(self, bucket, prefix)` (line 59): no docstring

### `backend/tests/unit/services/test_pipeline_executor_function_categories.py`
- **Functions**
  - `async test_function_categories_row_aggregation_generator_are_distinct_and_work()` (line 47): Checklist CL-007:
- **Classes**
  - `_Dataset` (line 12): no docstring
  - `_Version` (line 21): no docstring
  - `_DatasetRegistry` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 38): no docstring
    - `async get_latest_version(self, dataset_id)` (line 41): no docstring

### `backend/tests/unit/services/test_pipeline_executor_normalize.py`
- **Functions**
  - `async test_normalize_transform_trims_and_nulls()` (line 47): no docstring
- **Classes**
  - `_Dataset` (line 12): no docstring
  - `_Version` (line 21): no docstring
  - `_DatasetRegistry` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 38): no docstring
    - `async get_latest_version(self, dataset_id)` (line 41): no docstring

### `backend/tests/unit/services/test_pipeline_executor_preview.py`
- **Functions**
  - `async test_executor_preview_supports_node_level_preview_and_row_count()` (line 47): no docstring
  - `async test_executor_compute_structured_target_column_overwrites_existing()` (line 95): no docstring
  - `async test_executor_compute_equals_is_treated_as_comparison_when_lhs_exists()` (line 137): no docstring
  - `async test_executor_stream_join_dynamic_uses_backward_time_direction_by_default()` (line 175): no docstring
  - `async test_executor_stream_join_dynamic_supports_forward_direction()` (line 248): no docstring
  - `async test_executor_stream_join_dynamic_applies_cache_expiration_window()` (line 319): no docstring
  - `async test_executor_stream_join_dynamic_selects_single_best_match_per_left_row()` (line 392): no docstring
  - `async test_executor_stream_join_dynamic_emits_unmatched_rows_as_outer_join()` (line 478): no docstring
  - `async test_executor_stream_join_left_lookup_forces_left_join_semantics()` (line 550): no docstring
  - `async test_executor_stream_join_static_forces_left_join_semantics_even_when_full_requested()` (line 619): no docstring
  - `async test_executor_stream_join_left_lookup_picks_latest_right_row_per_key_without_event_time()` (line 686): no docstring
- **Classes**
  - `_Dataset` (line 12): no docstring
  - `_Version` (line 21): no docstring
  - `_DatasetRegistry` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 38): no docstring
    - `async get_latest_version(self, dataset_id)` (line 41): no docstring

### `backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- **Functions**
  - `test_join_requires_keys_by_default()` (line 7): no docstring
  - `test_join_allow_cross_join_requires_cross_type()` (line 16): no docstring
  - `test_cross_join_explicit_opt_in_produces_cartesian_product_and_preserves_column_mapping()` (line 25): no docstring
  - `test_union_strict_raises_on_schema_mismatch()` (line 51): no docstring
  - `test_union_common_only_keeps_only_shared_columns()` (line 60): no docstring
  - `test_union_pad_missing_nulls_includes_superset_columns()` (line 71): no docstring

### `backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- **Functions**
  - `test_expectations_unique_detects_duplicate_primary_key()` (line 8): no docstring
  - `test_expectations_unique_detects_duplicate_composite_key()` (line 16): no docstring
  - `test_expectations_row_count_bounds()` (line 27): no docstring
  - `test_schema_contract_missing_required_column_is_reported()` (line 38): no docstring
  - `test_schema_contract_type_mismatch_is_reported()` (line 49): no docstring

### `backend/tests/unit/services/test_pipeline_join_evaluator.py`
- **Functions**
  - `async test_join_evaluator_reports_coverage_and_explosion()` (line 47): no docstring
  - `async test_join_evaluator_aligns_inputs_to_join_keys()` (line 117): no docstring
- **Classes**
  - `_Dataset` (line 12): no docstring
  - `_Version` (line 21): no docstring
  - `_DatasetRegistry` (line 27): no docstring
    - `__init__(self)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 38): no docstring
    - `async get_latest_version(self, dataset_id)` (line 41): no docstring

### `backend/tests/unit/services/test_pipeline_kafka_avro_schema.py`
- **Functions**
  - `test_resolve_inline_avro_schema_prefers_inline_metadata()` (line 14): no docstring
  - `test_validate_kafka_avro_schema_requires_inline_or_registry()` (line 21): no docstring
  - `test_validate_kafka_avro_schema_rejects_missing_registry_subject()` (line 32): no docstring
  - `test_validate_kafka_avro_schema_rejects_invalid_registry_version()` (line 43): no docstring
  - `test_validate_kafka_avro_schema_rejects_latest_registry_version_explicitly()` (line 54): no docstring
  - `test_resolve_kafka_avro_schema_registry_reference_supports_options_aliases()` (line 68): no docstring
  - `test_fetch_kafka_avro_schema_from_registry_parses_schema_payload(monkeypatch)` (line 87): no docstring

### `backend/tests/unit/services/test_pipeline_ops_preflight.py`
- **Functions**
  - `async test_run_pipeline_preflight_fail_closed_raises_http_exception(monkeypatch)` (line 13): no docstring
  - `async test_run_pipeline_preflight_fail_open_returns_warning_payload(monkeypatch)` (line 43): no docstring
  - `async test_run_pipeline_preflight_blocks_unpinned_udf_version(monkeypatch)` (line 72): no docstring
  - `async test_run_pipeline_preflight_blocks_missing_udf_reference(monkeypatch)` (line 111): no docstring

### `backend/tests/unit/services/test_pipeline_plan_builder.py`
- **Functions**
  - `test_new_plan_has_minimum_shape()` (line 35): no docstring
  - `test_add_input_and_output_wires_edges()` (line 43): no docstring
  - `test_add_output_persists_output_metadata_into_outputs_entry()` (line 62): no docstring
  - `test_add_output_warns_when_legacy_alias_kind_used()` (line 78): no docstring
  - `test_add_output_normalizes_dataset_write_metadata_aliases()` (line 86): no docstring
  - `test_add_input_supports_read_config()` (line 110): no docstring
  - `test_add_external_input_creates_input_node_without_dataset_selection()` (line 123): no docstring
  - `test_configure_input_read_patches_input_nodes_only()` (line 139): no docstring
  - `test_add_join_requires_two_inputs_and_keys()` (line 152): no docstring
  - `test_add_join_accepts_hints_and_broadcast_flags()` (line 171): no docstring
  - `test_add_join_rejects_cross_join()` (line 189): no docstring
  - `test_add_cast_requires_column_and_type()` (line 205): no docstring
  - `test_compute_column_and_assignments_build_metadata()` (line 217): no docstring
  - `test_add_udf_builds_reference_metadata()` (line 236): no docstring
  - `test_add_udf_rejects_invalid_version()` (line 251): no docstring
  - `test_select_expr_builds_metadata()` (line 258): no docstring
  - `test_add_sort_supports_desc_prefix_and_dict_form()` (line 267): no docstring
  - `test_add_explode_builds_metadata()` (line 281): no docstring
  - `test_add_union_builds_metadata()` (line 290): no docstring
  - `test_add_stream_join_rejects_left_lookup_with_transformed_right_input()` (line 300): no docstring
  - `test_add_stream_join_rejects_left_lookup_with_streaming_right_input()` (line 317): no docstring
  - `test_add_stream_join_rejects_static_with_transformed_right_input()` (line 338): no docstring
  - `test_add_stream_join_rejects_static_with_streaming_right_input()` (line 355): no docstring
  - `test_add_stream_join_static_defaults_join_type_to_left()` (line 376): no docstring
  - `test_add_stream_join_static_rejects_non_left_join_type()` (line 394): no docstring
  - `test_add_pivot_builds_metadata()` (line 412): no docstring
  - `test_add_edge_is_idempotent()` (line 431): no docstring
  - `test_delete_edge_is_noop_if_missing_but_warns()` (line 443): no docstring
  - `test_set_node_inputs_replaces_incoming_edges_ordered()` (line 456): no docstring
  - `test_update_node_metadata_merges_and_unsets()` (line 471): no docstring
  - `test_update_settings_patches_definition_settings()` (line 489): no docstring
  - `test_delete_node_removes_outputs_entry()` (line 501): no docstring
  - `test_update_output_renames_and_syncs_output_node_metadata()` (line 513): no docstring

### `backend/tests/unit/services/test_pipeline_preflight_dataset_output.py`
- **Functions**
  - `async test_compute_pipeline_preflight_blocks_when_dataset_pk_columns_missing(monkeypatch)` (line 12): no docstring
  - `async test_compute_pipeline_preflight_accepts_valid_dataset_write_metadata(monkeypatch)` (line 59): no docstring
  - `async test_compute_pipeline_preflight_blocks_ontology_link_missing_required_columns(monkeypatch)` (line 105): no docstring
  - `async test_compute_pipeline_preflight_blocks_invalid_ontology_relationship_spec_type(monkeypatch)` (line 156): no docstring
  - `async test_compute_pipeline_preflight_blocks_partitioned_csv_dataset_output(monkeypatch)` (line 201): no docstring
  - `async test_compute_pipeline_preflight_blocks_partitioned_json_dataset_output(monkeypatch)` (line 247): no docstring
  - `async test_compute_pipeline_preflight_blocks_streaming_external_non_kafka_input()` (line 293): no docstring
  - `async test_compute_pipeline_preflight_blocks_streaming_external_missing_checkpoint()` (line 333): no docstring
  - `async test_compute_pipeline_preflight_blocks_kafka_json_without_schema()` (line 372): no docstring
  - `async test_compute_pipeline_preflight_allows_kafka_avro_with_schema_registry_reference()` (line 411): no docstring
  - `async test_compute_pipeline_preflight_blocks_kafka_avro_without_schema_or_registry()` (line 454): no docstring
  - `async test_compute_pipeline_preflight_blocks_kafka_avro_with_missing_registry_version()` (line 492): no docstring
  - `async test_compute_pipeline_preflight_blocks_kafka_avro_with_latest_registry_version()` (line 535): no docstring
  - `async test_compute_pipeline_preflight_allows_batch_kafka_without_checkpoint()` (line 579): no docstring
  - `async test_compute_pipeline_preflight_blocks_streaming_without_watermark()` (line 620): no docstring
  - `async test_compute_pipeline_preflight_blocks_geotemporal_missing_required_columns(monkeypatch)` (line 661): no docstring
  - `async test_compute_pipeline_preflight_blocks_virtual_dataset_write_settings(monkeypatch)` (line 707): no docstring
  - `async test_compute_pipeline_preflight_blocks_left_lookup_with_transformed_right_input(monkeypatch)` (line 753): no docstring
  - `async test_compute_pipeline_preflight_blocks_left_lookup_with_streaming_right_input(monkeypatch)` (line 814): no docstring
  - `async test_compute_pipeline_preflight_blocks_static_with_transformed_right_input(monkeypatch)` (line 875): no docstring
  - `async test_compute_pipeline_preflight_blocks_static_with_streaming_right_input(monkeypatch)` (line 930): no docstring
  - `async test_compute_pipeline_preflight_blocks_static_non_left_join_type(monkeypatch)` (line 991): no docstring

### `backend/tests/unit/services/test_pipeline_preview_inspector.py`
- **Functions**
  - `test_preview_inspector_suggests_normalize_and_casts()` (line 10): no docstring
  - `test_preview_inspector_no_cleansing_needed_for_clean_columns()` (line 43): no docstring
  - `test_preview_inspector_suggests_dedupe_for_duplicate_rows()` (line 58): no docstring
  - `test_preview_inspector_suggests_regex_replace_for_phone()` (line 77): no docstring

### `backend/tests/unit/services/test_pipeline_profiler.py`
- **Functions**
  - `test_compute_column_stats_string_column_counts_null_empty_whitespace_and_top_values()` (line 7): no docstring
  - `test_compute_column_stats_numeric_min_max_mean_from_mixed_values()` (line 29): no docstring

### `backend/tests/unit/services/test_pipeline_registry_branch_idempotency.py`
- **Functions**
  - `_pipeline_record(pipeline_id, db_name, name, branch)` (line 11): no docstring
  - `async test_create_branch_is_idempotent_when_db_unique_violation_races(monkeypatch)` (line 52): If branch creation is raced (or retried), the DB may return a unique constraint violation.

### `backend/tests/unit/services/test_pipeline_registry_commit_predicate_fallback.py`
- **Functions**
  - `_pipeline_record(pipeline_id, db_name, name, branch)` (line 31): no docstring
  - `async test_add_version_handles_lakefs_predicate_failed_by_resolving_head_commit(monkeypatch)` (line 72): lakeFS commits apply to the entire branch working copy. If another actor commits concurrently,
- **Classes**
  - `_Acquire` (line 12): no docstring
    - `__init__(self, conn)` (line 13): no docstring
    - `async __aenter__(self)` (line 16): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 19): no docstring
  - `_FakePool` (line 23): no docstring
    - `__init__(self, conn)` (line 24): no docstring
    - `acquire(self)` (line 27): no docstring

### `backend/tests/unit/services/test_pipeline_scheduler_control_plane_events.py`
- **Functions**
  - `async test_scheduler_emits_ignored_event(monkeypatch)` (line 24): no docstring
- **Classes**
  - `_Registry` (line 10): no docstring
    - `async record_run(self, **kwargs)` (line 11): no docstring
  - `_Queue` (line 15): no docstring
    - `__init__(self)` (line 16): no docstring
    - `async publish(self, job)` (line 19): no docstring

### `backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- **Functions**
  - `async test_scheduler_records_ignored_when_schedule_due_but_dependencies_up_to_date(monkeypatch)` (line 59): no docstring
  - `async test_scheduler_records_ignored_when_schedule_due_but_dependency_not_satisfied(monkeypatch)` (line 106): no docstring
  - `async test_scheduler_does_not_trigger_dependency_only_when_pipeline_is_newer_than_deps(monkeypatch)` (line 153): no docstring
  - `async test_scheduler_triggers_interval_schedule_when_due(monkeypatch)` (line 198): no docstring
  - `async test_scheduler_triggers_cron_schedule_when_matches(monkeypatch)` (line 243): no docstring
  - `async test_scheduler_triggers_when_dependency_is_newer_than_pipeline_build(monkeypatch)` (line 288): no docstring
- **Classes**
  - `_PipelineRecord` (line 13): no docstring
  - `_Queue` (line 20): no docstring
    - `__init__(self)` (line 21): no docstring
    - `async publish(self, job)` (line 24): no docstring
  - `_Registry` (line 28): no docstring
    - `__init__(self, pipelines, records)` (line 29): no docstring
    - `async list_scheduled_pipelines(self)` (line 35): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 38): no docstring
    - `async record_run(self, pipeline_id, job_id, mode, status, output_json, finished_at, **kwargs)` (line 41): no docstring
    - `async record_schedule_tick(self, pipeline_id, scheduled_at)` (line 53): no docstring

### `backend/tests/unit/services/test_pipeline_scheduler_validation.py`
- **Functions**
  - `test_normalize_dependencies_accepts_pipeline_id_variants()` (line 11): no docstring
  - `test_normalize_dependencies_reports_invalid_entries()` (line 29): no docstring
  - `test_cron_expression_validation_matches_supported_subset()` (line 44): no docstring
  - `async test_dependencies_satisfied_raises_when_dependency_pipeline_missing()` (line 66): no docstring
- **Classes**
  - `_MissingPipelineRegistry` (line 59): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 60): no docstring

### `backend/tests/unit/services/test_pipeline_schema_casts.py`
- **Functions**
  - `test_extract_schema_casts_returns_empty_for_non_dict_inputs()` (line 6): no docstring
  - `test_extract_schema_casts_parses_columns_dicts_and_strings()` (line 12): no docstring
  - `test_extract_schema_casts_falls_back_to_fields()` (line 30): no docstring
  - `test_extract_schema_casts_extracts_from_properties()` (line 37): no docstring

### `backend/tests/unit/services/test_pipeline_task_spec_policy.py`
- **Functions**
  - `_plan_with_op(op)` (line 8): no docstring
  - `test_clamp_task_spec_disables_join_for_single_dataset()` (line 31): no docstring
  - `test_policy_rejects_join_when_disallowed()` (line 38): no docstring
  - `test_policy_rejects_advanced_transform_when_disallowed()` (line 46): no docstring
  - `test_policy_rejects_report_only_scope()` (line 53): no docstring

### `backend/tests/unit/services/test_pipeline_type_inference.py`
- **Functions**
  - `test_infer_xsd_type_with_confidence_integer()` (line 6): no docstring
  - `test_infer_xsd_type_with_confidence_decimal()` (line 12): no docstring
  - `test_infer_xsd_type_with_confidence_boolean()` (line 18): no docstring
  - `test_infer_xsd_type_with_confidence_datetime()` (line 24): no docstring
  - `test_infer_xsd_type_with_confidence_falls_back_to_string_for_mixed_values()` (line 36): no docstring
  - `test_common_join_key_type_biases_to_string_on_mismatch()` (line 42): no docstring

### `backend/tests/unit/services/test_pipeline_udf_runtime.py`
- **Functions**
  - `test_compile_row_udf_accepts_simple_transform()` (line 29): no docstring
  - `test_compile_row_udf_accepts_escaped_newline_source()` (line 43): no docstring
  - `test_compile_row_udf_accepts_flat_map_output()` (line 53): no docstring
  - `test_compile_row_udf_rejects_imports()` (line 66): no docstring
  - `test_compile_row_udf_rejects_private_attribute_access()` (line 79): no docstring
  - `test_compile_row_udf_rejects_top_level_statements()` (line 90): no docstring
  - `test_compile_row_udf_rejects_loops()` (line 103): no docstring
  - `async test_resolve_udf_reference_requires_udf_id_when_policy_enabled()` (line 117): no docstring
  - `async test_resolve_udf_reference_requires_version_when_pinning_enabled()` (line 129): no docstring
  - `async test_resolve_udf_reference_uses_registry_and_cache_key()` (line 145): no docstring
  - `async test_resolve_udf_reference_allows_latest_when_version_pinning_disabled()` (line 166): no docstring
  - `async test_resolve_udf_reference_rejects_inline_code_even_when_reference_flag_disabled()` (line 182): no docstring
- **Classes**
  - `_FakeUdfVersion` (line 10): no docstring
    - `__init__(self, version, code)` (line 11): no docstring
  - `_FakeUdfRegistry` (line 16): no docstring
    - `__init__(self)` (line 17): no docstring
    - `async get_udf_latest_version(self, udf_id)` (line 21): no docstring
    - `async get_udf_version(self, udf_id, version)` (line 24): no docstring

### `backend/tests/unit/services/test_pipeline_udf_versioning.py`
- **Functions**
  - `_get_postgres_url_candidates()` (line 13): no docstring
  - `async test_udf_can_be_created_reused_and_version_upgraded()` (line 64): Checklist CL-011:
- **Classes**
  - `_Dataset` (line 28): no docstring
  - `_Version` (line 37): no docstring
  - `_DatasetRegistry` (line 43): no docstring
    - `__init__(self)` (line 44): no docstring
    - `async get_dataset(self, dataset_id)` (line 48): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 54): no docstring
    - `async get_latest_version(self, dataset_id)` (line 57): no docstring

### `backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- **Functions**
  - `async test_pipeline_unit_tests_define_inputs_and_expected_outputs()` (line 19): CL-025: Unit tests are defined by (test inputs, transform graph, expected outputs).
  - `async test_pipeline_unit_tests_report_diffs_for_breaking_changes()` (line 72): CL-026: Unit tests are effective for breaking-change detection/debugging via clear diffs.
- **Classes**
  - `_DummyDatasetRegistry` (line 6): no docstring
    - `async get_dataset(self, *args, **kwargs)` (line 7): no docstring
    - `async get_latest_version(self, *args, **kwargs)` (line 10): no docstring
    - `async get_dataset_by_name(self, *args, **kwargs)` (line 13): no docstring

### `backend/tests/unit/services/test_pipeline_value_predicates.py`
- **Functions**
  - `test_is_bool_like()` (line 11): no docstring
  - `test_is_int_like()` (line 17): no docstring
  - `test_is_decimal_like_include_int_policy()` (line 24): no docstring
  - `test_is_datetime_like_iso_only_and_general_modes()` (line 31): no docstring

### `backend/tests/unit/services/test_pipeline_worker_diff_handling.py`
- **Functions**
  - `async test_list_lakefs_diff_paths_ignores_removed()` (line 14): no docstring
  - `async test_load_input_dataframe_fallback_on_diff_failure(monkeypatch)` (line 42): no docstring
  - `async test_load_input_dataframe_removed_only_diff_returns_empty(monkeypatch)` (line 88): no docstring

### `backend/tests/unit/services/test_postgres_schema_registry.py`
- **Functions**
  - `async test_health_check_connects_when_pool_missing()` (line 44): no docstring
  - `async test_health_check_returns_false_on_query_failure()` (line 61): no docstring
- **Classes**
  - `_AcquireCtx` (line 8): no docstring
    - `__init__(self, conn)` (line 9): no docstring
    - `async __aenter__(self)` (line 12): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 15): no docstring
  - `_Conn` (line 19): no docstring
    - `__init__(self, fail)` (line 20): no docstring
    - `async execute(self, sql)` (line 24): no docstring
  - `_Pool` (line 30): no docstring
    - `__init__(self, conn)` (line 31): no docstring
    - `acquire(self)` (line 34): no docstring
  - `_Registry` (line 38): no docstring
    - `async _ensure_tables(self, conn)` (line 39): no docstring

### `backend/tests/unit/services/test_projection_position_tracker.py`
- **Functions**
  - `async test_get_position_empty()` (line 70): no docstring
  - `async test_update_and_get_position()` (line 82): no docstring
  - `async test_update_position_monotonic()` (line 104): Sequence should only increase, never decrease.
  - `async test_reset_position()` (line 128): no docstring
  - `async test_list_positions()` (line 151): no docstring
  - `async test_compute_lag()` (line 171): no docstring
  - `async test_compute_lag_unhealthy()` (line 191): no docstring
- **Classes**
  - `_FakePool` (line 11): Minimal asyncpg pool fake for position tracker tests.
    - `__init__(self)` (line 14): no docstring
    - `_key(self, name, db, branch)` (line 18): no docstring
    - `acquire(self)` (line 21): no docstring
  - `_FakeConn` (line 25): no docstring
    - `__init__(self, pool)` (line 26): no docstring
    - `async __aenter__(self)` (line 29): no docstring
    - `async __aexit__(self, *args)` (line 32): no docstring
    - `async execute(self, query, *args)` (line 35): no docstring
    - `async fetchrow(self, query, *args)` (line 56): no docstring
    - `async fetch(self, query, *args)` (line 62): no docstring

### `backend/tests/unit/services/test_relationship_extractor.py`
- **Classes**
  - `TestExtractRelationshipsWithRelMap` (line 10): Test extraction using pre-parsed rel_map (objectify_worker fast path).
    - `test_single_relationship(self)` (line 13): no docstring
    - `test_many_relationship(self)` (line 25): no docstring
    - `test_deduplicates_many(self)` (line 36): no docstring
    - `test_missing_field_skipped(self)` (line 46): no docstring
    - `test_empty_rel_map(self)` (line 54): no docstring
  - `TestExtractRelationshipsWithOntologyData` (line 60): Test extraction using raw ontology data (instance_worker path).
    - `test_oms_dict_format(self)` (line 63): no docstring
    - `test_terminus_schema_format(self)` (line 73): no docstring
  - `TestPatternFallback` (line 87): Test pattern-based relationship detection.
    - `test_detects_by_pattern(self)` (line 90): no docstring
    - `test_pattern_fallback_disabled(self)` (line 103): no docstring
    - `test_pattern_list_refs(self)` (line 108): no docstring
  - `TestEdgeCases` (line 116): Test error handling and edge cases.
    - `test_invalid_ref_format(self)` (line 119): no docstring
    - `test_non_string_ref(self)` (line 125): no docstring
    - `test_single_cardinality_with_multiple_values(self)` (line 131): no docstring
    - `test_single_cardinality_with_empty_list(self)` (line 137): no docstring
    - `test_none_inputs(self)` (line 143): no docstring

### `backend/tests/unit/services/test_relationship_reconciler.py`
- **Functions**
  - `test_collect_relationships_extracts_from_class_defs()` (line 65): no docstring
  - `test_collect_relationships_deduplicates()` (line 88): no docstring
  - `async test_scan_and_group_fk()` (line 102): no docstring
  - `async test_bulk_update_relationships()` (line 128): no docstring
  - `async test_detect_fk_field_via_pattern()` (line 158): no docstring
  - `async test_reconcile_returns_no_classes_when_oms_empty()` (line 181): no docstring
- **Classes**
  - `_FakeElasticsearchService` (line 18): Minimal fake for reconciler tests.
    - `__init__(self)` (line 21): no docstring
    - `async connect(self)` (line 27): no docstring
    - `add_search_response(self, resp)` (line 30): no docstring
    - `async search(self, index, query, size, from_, sort, source_includes, source_excludes, aggregations)` (line 33): no docstring
    - `async update_document(self, index, doc_id, doc, script, upsert, refresh)` (line 48): no docstring
    - `async refresh_index(self, index)` (line 60): no docstring

### `backend/tests/unit/services/test_schema_drift_detector.py`
- **Classes**
  - `TestSchemaDriftConfig` (line 15): no docstring
    - `test_default_config(self)` (line 16): no docstring
    - `test_type_compatibility_widening(self)` (line 21): Widening conversions should be allowed
  - `TestSchemaDriftDetector` (line 29): no docstring
    - `test_no_drift_same_schema(self)` (line 30): No drift when schemas are identical
    - `test_detect_column_added(self)` (line 48): Detect when a new column is added
    - `test_detect_column_removed(self)` (line 74): Detect when a column is removed - breaking change
    - `test_detect_type_changed(self)` (line 98): Detect when a column type changes
    - `test_detect_column_renamed(self)` (line 126): Detect when a column is renamed (similar name, same type)
    - `test_mixed_changes(self)` (line 151): Detect multiple changes at once
    - `test_change_summary(self)` (line 176): change_summary should count changes by type
    - `test_severity_classification_info(self)` (line 201): Adding columns should be classified as info
    - `test_severity_classification_breaking(self)` (line 221): Removing columns should be classified as breaking
    - `test_pk_column_removal_is_critical(self)` (line 241): Removing a PK-like column should be data_loss impact
    - `test_hash_based_quick_check(self)` (line 263): Hash comparison should enable quick no-drift check
    - `test_to_notification_payload(self)` (line 281): to_notification_payload should produce valid structure
    - `test_empty_schema_handling(self)` (line 308): Empty schemas should be handled gracefully

### `backend/tests/unit/services/test_schema_versioning.py`
- **Functions**
  - `test_schema_version_parsing_and_comparison()` (line 13): no docstring
  - `test_schema_registry_register_and_migrate()` (line 28): no docstring
  - `test_schema_versioning_service_event_helpers()` (line 54): no docstring

### `backend/tests/unit/services/test_sequence_service.py`
- **Functions**
  - `async test_sequence_service_increments_and_caches()` (line 58): no docstring
  - `async test_sequence_service_set_reset_and_batch()` (line 73): no docstring
  - `async test_sequence_service_lists_sequences()` (line 89): no docstring
- **Classes**
  - `_FakeRedis` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `_coerce_key(self, key)` (line 14): no docstring
    - `async incr(self, key)` (line 19): no docstring
    - `async get(self, key)` (line 24): no docstring
    - `async eval(self, _script, _numkeys, key, sequence)` (line 31): no docstring
    - `async delete(self, key)` (line 39): no docstring
    - `async incrby(self, key, count)` (line 46): no docstring
    - `async scan_iter(self, match)` (line 51): no docstring

### `backend/tests/unit/services/test_standard_dlq_publisher.py`
- **Functions**
  - `async test_publish_standard_dlq_builds_payload_and_flushes()` (line 58): no docstring
  - `async test_publish_contextual_dlq_json_uses_synthetic_source_key()` (line 92): no docstring
- **Classes**
  - `_DummyMsg` (line 12): no docstring
    - `__init__(self)` (line 13): no docstring
    - `topic(self)` (line 16): no docstring
    - `partition(self)` (line 19): no docstring
    - `offset(self)` (line 22): no docstring
    - `value(self)` (line 25): no docstring
    - `key(self)` (line 28): no docstring
    - `timestamp(self)` (line 31): no docstring
    - `headers(self)` (line 34): no docstring
  - `_CaptureProducer` (line 38): no docstring
    - `__init__(self)` (line 39): no docstring
    - `produce(self, topic, key, value, headers, **kwargs)` (line 43): no docstring
    - `flush(self, timeout_s)` (line 46): no docstring
  - `_NoopTracing` (line 51): no docstring
    - `span(self, *_args, **_kwargs)` (line 53): no docstring

### `backend/tests/unit/services/test_storage_service.py`
- **Functions**
  - `async test_list_command_files_paginates_filters_and_sorts()` (line 19): no docstring
  - `test_storage_service_does_not_disable_tls_verify_by_default(monkeypatch)` (line 50): no docstring
  - `test_storage_service_respects_explicit_tls_verify_flag(monkeypatch)` (line 75): no docstring
  - `test_storage_service_supports_tls_ca_bundle_path(monkeypatch)` (line 101): no docstring
- **Classes**
  - `_FakeS3Client` (line 8): no docstring
    - `__init__(self, pages)` (line 9): no docstring
    - `list_objects_v2(self, **kwargs)` (line 12): no docstring

### `backend/tests/unit/services/test_sync_wrapper_service.py`
- **Functions**
  - `async test_wait_for_command_success()` (line 33): no docstring
  - `async test_wait_for_command_timeout()` (line 48): no docstring
  - `async test_wait_for_command_failure()` (line 59): no docstring
  - `async test_execute_sync_calls_wait()` (line 70): no docstring
- **Classes**
  - `_FakeCommandStatusService` (line 14): no docstring
    - `__init__(self, sequence)` (line 15): no docstring
    - `async get_command_details(self, command_id)` (line 19): no docstring
  - `_FakeCommandResult` (line 28): no docstring

### `backend/tests/unit/services/test_watermark_monitor.py`
- **Functions**
  - `async test_watermark_monitor_metrics_and_alerts()` (line 10): no docstring
  - `test_partition_and_global_watermark_helpers()` (line 64): no docstring

### `backend/tests/unit/services/test_worker_stores_lineage_policy.py`
- **Functions**
  - `async test_initialize_worker_stores_fails_when_lineage_required(monkeypatch)` (line 19): no docstring
  - `async test_initialize_worker_stores_allows_fail_open_override(monkeypatch)` (line 44): no docstring
- **Classes**
  - `_DummyLineageStore` (line 8): no docstring
    - `__init__(self, should_fail)` (line 9): no docstring
    - `async initialize(self)` (line 12): no docstring

### `backend/tests/unit/utils/__init__.py`

### `backend/tests/unit/utils/test_access_policy.py`
- **Functions**
  - `test_access_policy_allows_matching_rows()` (line 4): no docstring
  - `test_access_policy_denies_matching_rows()` (line 15): no docstring
  - `test_access_policy_masks_columns()` (line 29): no docstring

### `backend/tests/unit/utils/test_action_audit_policy.py`
- **Functions**
  - `test_audit_action_log_input_redacts_keys_recursively()` (line 4): no docstring
  - `test_audit_action_log_input_truncates_when_exceeds_max_bytes()` (line 13): no docstring
  - `test_audit_action_log_result_summarizes_large_change_arrays()` (line 22): no docstring

### `backend/tests/unit/utils/test_action_data_access.py`
- **Functions**
  - `async test_evaluate_action_target_data_access_denied()` (line 40): no docstring
  - `async test_evaluate_action_target_data_access_allows_when_policy_missing()` (line 68): no docstring
  - `async test_evaluate_action_target_data_access_marks_unverifiable_on_registry_error()` (line 87): no docstring
  - `async test_evaluate_action_target_data_access_edit_denied_by_object_edit_policy()` (line 106): no docstring
  - `async test_evaluate_action_target_data_access_attachment_policy_missing_is_unverifiable()` (line 137): no docstring
  - `async test_evaluate_action_target_data_access_object_set_policy_enforced_for_link_changes()` (line 164): no docstring
- **Classes**
  - `_StubDatasetRegistry` (line 10): no docstring
    - `__init__(self, policy_by_class, policy_by_scope_class, raise_for)` (line 11): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 22): no docstring

### `backend/tests/unit/utils/test_action_input_schema.py`
- **Functions**
  - `test_validate_action_input_validates_and_normalizes_object_ref()` (line 10): no docstring
  - `test_validate_action_input_rejects_unknown_fields_by_default()` (line 23): no docstring
  - `test_validate_action_input_rejects_reserved_internal_keys_anywhere()` (line 29): no docstring
  - `test_validate_action_input_reports_invalid_schema()` (line 35): no docstring

### `backend/tests/unit/utils/test_action_permission_profile.py`
- **Functions**
  - `test_resolve_action_permission_profile_defaults()` (line 15): no docstring
  - `test_resolve_action_permission_profile_supports_aliases()` (line 24): no docstring
  - `test_resolve_action_permission_profile_rejects_invalid_model()` (line 38): no docstring
  - `test_resolve_action_permission_profile_rejects_non_boolean_edits_flag()` (line 45): no docstring
  - `test_requires_action_data_access_enforcement_for_profile_or_global_flag()` (line 52): no docstring

### `backend/tests/unit/utils/test_action_runtime_contracts.py`
- **Functions**
  - `test_extract_required_action_interfaces_normalizes_prefix_and_dedupes()` (line 16): no docstring
  - `test_extract_interfaces_from_metadata_supports_aliases()` (line 26): no docstring
  - `test_build_property_type_map_from_properties_handles_models_and_dicts()` (line 36): no docstring
  - `async test_load_action_target_runtime_contract_returns_none_when_class_missing()` (line 47): no docstring
  - `async test_load_action_target_runtime_contract_extracts_metadata_and_properties()` (line 63): no docstring

### `backend/tests/unit/utils/test_action_template_engine.py`
- **Functions**
  - `test_compile_template_v1_change_shape_merges_and_tracks_touched_fields()` (line 12): no docstring
  - `test_compile_template_v1_resolves_refs_and_now()` (line 47): no docstring
  - `test_compile_template_v1_rejects_delete_plus_edits_for_same_target()` (line 81): no docstring
  - `test_compile_template_v1_supports_bulk_targets_from_list()` (line 95): no docstring

### `backend/tests/unit/utils/test_blank_utils.py`
- **Functions**
  - `test_is_blank_value()` (line 4): no docstring
  - `test_strip_to_none()` (line 12): no docstring

### `backend/tests/unit/utils/test_canonical_json.py`
- **Functions**
  - `test_canonical_json_dumps_sorts_keys_and_is_compact()` (line 6): no docstring
  - `test_canonical_json_dumps_normalizes_datetime_to_utc()` (line 10): no docstring
  - `test_sha256_prefixed_has_expected_prefix()` (line 16): no docstring

### `backend/tests/unit/utils/test_common_base_response_deprecation.py`
- **Functions**
  - `test_base_response_emits_deprecation_once(monkeypatch, caplog)` (line 7): no docstring

### `backend/tests/unit/utils/test_dependency_parsing.py`
- **Functions**
  - `test_parse_requirements_txt_extracts_versions(tmp_path)` (line 6): no docstring
  - `test_parse_pyproject_toml_extracts_dependencies(tmp_path)` (line 28): no docstring

### `backend/tests/unit/utils/test_deprecation_utils.py`
- **Functions**
  - `test_deprecated_decorator_sync()` (line 8): no docstring
  - `test_legacy_and_experimental_decorators()` (line 24): no docstring

### `backend/tests/unit/utils/test_json_utils_coercion.py`
- **Functions**
  - `test_coerce_json_list_from_list()` (line 4): no docstring
  - `test_coerce_json_list_from_string_list()` (line 8): no docstring
  - `test_coerce_json_list_from_wrapped_dict()` (line 12): no docstring
  - `test_coerce_json_list_wrap_dict_when_requested()` (line 17): no docstring
  - `test_coerce_json_dict_from_dict_and_string()` (line 22): no docstring
  - `test_coerce_json_dict_wraps_non_dict_json_by_default()` (line 27): no docstring
  - `test_coerce_json_dict_can_disable_parsed_fallback()` (line 32): no docstring

### `backend/tests/unit/utils/test_label_mapper_i18n.py`
- **Functions**
  - `async test_label_mapper_detects_language_for_string_and_falls_back(tmp_path)` (line 7): no docstring
  - `async test_label_mapper_supports_language_map_and_reverse_lookup(tmp_path)` (line 22): no docstring
  - `async test_label_mapper_batch_fallback_returns_best_available(tmp_path)` (line 42): no docstring

### `backend/tests/unit/utils/test_llm_safety.py`
- **Functions**
  - `test_mask_pii_text_masks_email()` (line 28): no docstring
  - `test_mask_pii_text_preserves_uuid()` (line 37): UUIDs should NOT be masked (they're identifiers, not PII).
  - `test_mask_pii_dict()` (line 45): no docstring
- **Classes**
  - `TestDetectValuePattern` (line 61): no docstring
    - `test_detects_uuid(self)` (line 62): no docstring
    - `test_detects_sequential_id(self)` (line 66): no docstring
    - `test_detects_iso_date(self)` (line 71): no docstring
    - `test_detects_ambiguous_dates(self)` (line 75): no docstring
    - `test_detects_email(self)` (line 79): no docstring
    - `test_detects_boolean_string(self)` (line 82): no docstring
    - `test_detects_numeric_strings(self)` (line 87): no docstring
    - `test_detects_native_types(self)` (line 94): no docstring
    - `test_detects_url(self)` (line 100): no docstring
    - `test_detects_short_codes(self)` (line 104): no docstring
    - `test_detects_identifier_like(self)` (line 108): no docstring
    - `test_free_text_fallback(self)` (line 112): no docstring
  - `TestExtractColumnValuePatterns` (line 123): no docstring
    - `test_uuid_column(self)` (line 124): no docstring
    - `test_sequential_id_column(self)` (line 137): no docstring
    - `test_mixed_patterns_column(self)` (line 144): no docstring
    - `test_empty_values(self)` (line 151): no docstring
    - `test_provides_examples(self)` (line 157): no docstring
  - `TestBuildColumnSemanticObservations` (line 172): no docstring
    - `test_id_column_with_uuid_values(self)` (line 173): no docstring
    - `test_created_at_column(self)` (line 185): no docstring
    - `test_foreign_key_pattern(self)` (line 191): no docstring
    - `test_sensitive_column_warning(self)` (line 198): no docstring
    - `test_status_column_categorical(self)` (line 204): no docstring
    - `test_boolean_flag_column(self)` (line 210): no docstring
  - `TestBuildRelationshipObservations` (line 223): no docstring
    - `test_high_overlap_relationship(self)` (line 224): no docstring
    - `test_no_overlap_relationship(self)` (line 235): no docstring
    - `test_one_to_many_hint(self)` (line 246): no docstring
    - `test_null_handling(self)` (line 258): no docstring
    - `test_provides_agent_instruction(self)` (line 269): no docstring
    - `test_non_matching_examples(self)` (line 277): no docstring

### `backend/tests/unit/utils/test_log_rotation.py`
- **Functions**
  - `_touch_file(path, size)` (line 9): no docstring
  - `test_rotate_and_limit_logs(tmp_path)` (line 17): no docstring
  - `test_compress_and_cleanup(tmp_path)` (line 42): no docstring

### `backend/tests/unit/utils/test_number_utils.py`
- **Functions**
  - `test_to_int_or_none_with_none()` (line 4): no docstring
  - `test_to_int_or_none_with_valid_values()` (line 8): no docstring
  - `test_to_int_or_none_with_invalid_values()` (line 13): no docstring

### `backend/tests/unit/utils/test_ontology_stamp.py`
- **Functions**
  - `test_merge_ontology_stamp_prefers_existing()` (line 6): no docstring
  - `test_merge_ontology_stamp_fills_missing()` (line 15): no docstring

### `backend/tests/unit/utils/test_principal_policy.py`
- **Functions**
  - `test_build_principal_tags_user_id_back_compat()` (line 4): no docstring
  - `test_build_principal_tags_emits_typed_principal()` (line 9): no docstring
  - `test_policy_allows_matches_typed_principal()` (line 14): no docstring
  - `test_policy_allows_legacy_roles_alias()` (line 20): no docstring
  - `test_policy_denies_legacy_roles_alias_for_deny_effect()` (line 26): no docstring
  - `test_policy_fails_closed_for_unsupported_legacy_policy_shapes()` (line 32): no docstring
  - `test_policy_fails_closed_for_unsupported_legacy_fields_even_with_principals()` (line 38): no docstring

### `backend/tests/unit/utils/test_pythonpath_setup.py`
- **Functions**
  - `test_detect_backend_directory_with_markers(monkeypatch, tmp_path)` (line 12): no docstring
  - `test_setup_pythonpath_updates_env(monkeypatch)` (line 30): no docstring
  - `test_setup_pythonpath_invalid_directory(tmp_path, capsys)` (line 43): no docstring
  - `test_configure_python_environment_success(monkeypatch)` (line 50): no docstring

### `backend/tests/unit/utils/test_resource_rid.py`
- **Functions**
  - `test_parse_metadata_rev_defaults_to_one()` (line 4): no docstring
  - `test_parse_metadata_rev_parses_int()` (line 10): no docstring
  - `test_format_resource_rid_always_includes_revision()` (line 16): no docstring
  - `test_strip_rid_revision_handles_prefixed_and_unprefixed()` (line 21): no docstring

### `backend/tests/unit/utils/test_safe_bool_expression.py`
- **Functions**
  - `test_safe_eval_bool_expression_supports_attribute_compare()` (line 11): no docstring
  - `test_safe_eval_bool_expression_supports_boolean_ops()` (line 16): no docstring
  - `test_safe_eval_bool_expression_supports_subscript()` (line 23): no docstring
  - `test_safe_eval_bool_expression_rejects_private_attribute_access()` (line 28): no docstring
  - `test_safe_eval_bool_expression_rejects_calls()` (line 33): no docstring
  - `test_safe_eval_bool_expression_rejects_non_constant_subscript()` (line 38): no docstring
  - `test_safe_eval_bool_expression_errors_on_unknown_identifier()` (line 43): no docstring
  - `test_safe_eval_bool_expression_errors_on_non_boolean_result()` (line 48): no docstring
  - `test_safe_eval_bool_expression_errors_on_not_non_boolean_operand()` (line 53): no docstring
  - `test_validate_bool_expression_syntax_accepts_safe_expressions()` (line 58): no docstring
  - `test_validate_bool_expression_syntax_rejects_calls()` (line 62): no docstring

### `backend/tests/unit/utils/test_schema_columns_hash_utils.py`
- **Functions**
  - `test_extract_schema_columns_supports_list_payload()` (line 10): no docstring
  - `test_extract_schema_columns_supports_dict_shapes()` (line 16): no docstring
  - `test_extract_schema_names_and_types()` (line 21): no docstring
  - `test_compute_schema_hash_from_payload_matches_columns_hash()` (line 30): no docstring

### `backend/tests/unit/utils/test_submission_criteria_diagnostics.py`
- **Functions**
  - `test_submission_criteria_reason_missing_role()` (line 9): no docstring
  - `test_submission_criteria_reason_state_mismatch()` (line 16): no docstring
  - `test_submission_criteria_reason_mixed()` (line 23): no docstring

### `backend/tests/unit/utils/test_token_count.py`
- **Functions**
  - `test_approx_token_count_handles_none_and_empty_string()` (line 4): no docstring
  - `test_approx_token_count_counts_payload()` (line 10): no docstring
  - `test_approx_token_count_collection_policy_toggle()` (line 15): no docstring
  - `test_approx_token_count_json_matches_json_serialized_size()` (line 21): no docstring

### `backend/tests/unit/utils/test_utils_core.py`
- **Functions**
  - `test_parse_bool_env_and_int_env(monkeypatch)` (line 6): no docstring
  - `test_safe_path_helpers()` (line 20): no docstring
  - `test_s3_uri_helpers()` (line 27): no docstring
  - `test_branch_utils_defaults(monkeypatch)` (line 35): no docstring

### `backend/tests/unit/utils/test_uuid_utils.py`
- **Functions**
  - `test_safe_uuid_accepts_valid_uuid()` (line 4): no docstring
  - `test_safe_uuid_rejects_invalid_values()` (line 8): no docstring

### `backend/tests/unit/utils/test_worker_runner.py`
- **Functions**
  - `async test_run_component_lifecycle_propagates_run_error_without_close()` (line 11): no docstring
  - `async test_run_component_lifecycle_keeps_primary_error_when_close_also_fails(caplog)` (line 24): no docstring
  - `async test_run_component_lifecycle_raises_close_error_when_no_primary_error()` (line 45): no docstring

### `backend/tests/unit/utils/test_writeback_conflicts.py`
- **Functions**
  - `test_normalize_conflict_policy_defaults_to_fail()` (line 10): no docstring
  - `test_parse_conflict_policy_returns_none_for_missing_or_unknown()` (line 16): no docstring
  - `test_parse_conflict_policy_accepts_known_values_case_insensitive()` (line 22): no docstring
  - `test_normalize_conflict_policy_accepts_known_values_case_insensitive()` (line 29): no docstring
  - `test_detect_overlap_fields_compares_current_to_observed()` (line 35): no docstring
  - `test_detect_overlap_links_flags_base_removed_patch_adds()` (line 41): no docstring
  - `test_detect_overlap_links_flags_base_added_patch_removes()` (line 49): no docstring
  - `test_resolve_applied_changes_no_conflict_always_applies()` (line 57): no docstring
  - `test_resolve_applied_changes_writeback_wins_applies_on_conflict()` (line 64): no docstring
  - `test_resolve_applied_changes_base_wins_skips_on_conflict()` (line 73): no docstring
  - `test_resolve_applied_changes_base_wins_skips_on_link_conflict()` (line 80): no docstring
  - `test_resolve_applied_changes_fail_rejects_on_conflict()` (line 92): no docstring
  - `test_resolve_applied_changes_fail_rejects_on_link_conflict()` (line 99): no docstring

### `backend/tests/unit/utils/test_writeback_governance.py`
- **Functions**
  - `test_extract_backing_dataset_id_reads_object_type_spec()` (line 4): no docstring
  - `test_extract_backing_dataset_id_returns_none_for_missing_shape()` (line 9): no docstring
  - `test_policies_aligned_requires_dicts_and_exact_match()` (line 15): no docstring

### `backend/tests/unit/utils/test_writeback_lifecycle.py`
- **Functions**
  - `test_derive_lifecycle_id_prefers_top_level_value()` (line 6): no docstring
  - `test_derive_lifecycle_id_reads_metadata_value()` (line 10): no docstring
  - `test_derive_lifecycle_id_uses_last_create_command_id()` (line 14): no docstring
  - `test_derive_lifecycle_id_defaults_when_missing()` (line 27): no docstring
  - `test_overlay_doc_id_composes_instance_and_lifecycle()` (line 31): no docstring
  - `test_overlay_doc_id_rejects_delimiter_collision()` (line 35): no docstring

### `backend/tests/unit/utils/test_writeback_patch_apply.py`
- **Functions**
  - `test_apply_changes_set_unset()` (line 6): no docstring
  - `test_apply_changes_link_add_and_remove()` (line 12): no docstring
  - `test_apply_changes_link_scalar_coerces_to_list()` (line 24): no docstring
  - `test_apply_changes_delete_short_circuits()` (line 30): no docstring
  - `test_apply_changes_type_validation()` (line 36): no docstring

### `backend/tests/unit/utils/test_writeback_paths.py`
- **Functions**
  - `test_queue_entry_prefix_builds_expected_path()` (line 10): no docstring
  - `test_snapshot_keys_match_design_layout()` (line 17): no docstring

### `backend/tests/unit/validators/test_base_type_validators.py`
- **Functions**
  - `test_array_validator_rejects_null_items()` (line 14): no docstring
  - `test_array_validator_rejects_nested_arrays()` (line 20): no docstring
  - `test_struct_validator_rejects_nested_struct()` (line 26): no docstring
  - `test_struct_validator_rejects_array_field()` (line 32): no docstring
  - `test_geopoint_validator_accepts_latlon()` (line 38): no docstring
  - `test_geopoint_validator_rejects_out_of_range()` (line 44): no docstring
  - `test_geoshape_validator_accepts_point()` (line 50): no docstring
  - `test_geoshape_validator_rejects_invalid_type()` (line 56): no docstring
  - `test_vector_validator_accepts_numeric_list()` (line 62): no docstring
  - `test_vector_validator_rejects_wrong_dimensions()` (line 68): no docstring
  - `test_marking_validator_rejects_non_string()` (line 74): no docstring
  - `test_cipher_validator_rejects_non_string()` (line 80): no docstring
  - `test_media_uses_string_validator()` (line 86): no docstring
  - `test_attachment_uses_string_validator()` (line 93): no docstring
  - `test_time_series_uses_string_validator()` (line 100): no docstring

### `backend/tests/unit/workers/__init__.py`

### `backend/tests/unit/workers/test_action_worker_permission_profile.py`
- **Functions**
  - `_worker_without_init()` (line 10): no docstring
  - `async test_action_worker_enforce_permission_allows_datasource_derived_without_policy(monkeypatch)` (line 16): no docstring
  - `async test_action_worker_enforce_permission_rejects_edits_beyond_actions_without_engineer_role(monkeypatch)` (line 40): no docstring
  - `async test_action_worker_enforce_permission_rejects_invalid_permission_profile(monkeypatch)` (line 64): no docstring

### `backend/tests/unit/workers/test_connector_sync_worker.py`
- **Functions**
  - `async test_sync_worker_bff_scope_headers()` (line 159): no docstring
  - `async test_sync_worker_fetch_schema_and_target_types(monkeypatch)` (line 166): no docstring
  - `async test_sync_worker_process_google_sheets_update(monkeypatch)` (line 182): no docstring
  - `async test_sync_worker_handle_envelope_rejects_unknown()` (line 242): no docstring
  - `async test_sync_worker_run_processes_message(monkeypatch)` (line 256): no docstring
  - `async test_sync_worker_heartbeat_loop_stops(monkeypatch)` (line 285): no docstring
- **Classes**
  - `_FakeTracing` (line 17): no docstring
    - `span(self, *args, **kwargs)` (line 18): no docstring
  - `_FakeRegistry` (line 22): no docstring
    - `__init__(self)` (line 23): no docstring
    - `async get_source(self, source_type, source_id)` (line 29): no docstring
    - `async get_mapping(self, source_type, source_id)` (line 32): no docstring
    - `async upsert_source(self, source_type, source_id, enabled, config_json)` (line 35): no docstring
    - `async record_sync_outcome(self, **kwargs)` (line 38): no docstring
  - `_FakeProcessed` (line 42): no docstring
    - `__init__(self)` (line 43): no docstring
    - `async claim(self, **kwargs)` (line 49): no docstring
    - `async mark_done(self, **kwargs)` (line 53): no docstring
    - `async mark_failed(self, **kwargs)` (line 56): no docstring
    - `async heartbeat(self, **kwargs)` (line 59): no docstring
  - `_FakeSheets` (line 64): no docstring
    - `__init__(self)` (line 65): no docstring
    - `async fetch_sheet_values(self, *args, **kwargs)` (line 68): no docstring
  - `_FakeResponse` (line 72): no docstring
    - `__init__(self, payload)` (line 73): no docstring
    - `raise_for_status(self)` (line 76): no docstring
    - `json(self)` (line 79): no docstring
  - `_FakeHttp` (line 83): no docstring
    - `__init__(self)` (line 84): no docstring
    - `async get(self, url, **kwargs)` (line 88): no docstring
    - `async post(self, url, **kwargs)` (line 99): no docstring
  - `_FakeLineage` (line 104): no docstring
    - `__init__(self)` (line 105): no docstring
    - `node_event(self, value)` (line 108): no docstring
    - `async record_link(self, **kwargs)` (line 111): no docstring
  - `_FakeMessage` (line 115): no docstring
    - `__init__(self, payload)` (line 116): no docstring
    - `error(self)` (line 119): no docstring
    - `value(self)` (line 122): no docstring
    - `headers(self)` (line 125): no docstring
    - `topic(self)` (line 128): no docstring
    - `partition(self)` (line 131): no docstring
    - `offset(self)` (line 134): no docstring
  - `_FakeConsumer` (line 138): no docstring
    - `__init__(self, worker)` (line 139): no docstring
    - `poll(self, timeout)` (line 144): no docstring
    - `commit(self, msg, asynchronous)` (line 149): no docstring
    - `commit_sync(self, msg)` (line 153): no docstring

### `backend/tests/unit/workers/test_connector_trigger_service.py`
- **Functions**
  - `async test_trigger_service_initialize_and_close(monkeypatch)` (line 93): no docstring
  - `async test_trigger_service_is_due(monkeypatch)` (line 118): no docstring
  - `async test_trigger_service_poll_google_sheets_refreshes_token(monkeypatch)` (line 155): no docstring
  - `async test_trigger_service_publish_outbox(monkeypatch)` (line 198): no docstring
- **Classes**
  - `_FakeTracing` (line 15): no docstring
    - `span(self, *args, **kwargs)` (line 16): no docstring
  - `_FakeRegistry` (line 20): no docstring
    - `__init__(self)` (line 21): no docstring
    - `async initialize(self)` (line 33): no docstring
    - `async close(self)` (line 36): no docstring
    - `async get_sync_state(self, source_type, source_id)` (line 39): no docstring
    - `async list_sources(self, source_type, enabled, limit)` (line 42): no docstring
    - `async record_poll_result(self, **kwargs)` (line 45): no docstring
    - `async claim_outbox_batch(self, limit)` (line 48): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 55): no docstring
    - `async mark_outbox_failed(self, outbox_id, error)` (line 58): no docstring
    - `async upsert_source(self, source_type, source_id, enabled, config_json)` (line 61): no docstring
  - `_FakeSheets` (line 65): no docstring
    - `__init__(self, api_key)` (line 66): no docstring
    - `async close(self)` (line 71): no docstring
    - `async fetch_sheet_values(self, *args, **kwargs)` (line 74): no docstring
  - `_FakeProducer` (line 78): no docstring
    - `__init__(self, config)` (line 79): no docstring
    - `produce(self, **kwargs)` (line 84): no docstring
    - `flush(self, timeout)` (line 87): no docstring

### `backend/tests/unit/workers/test_instance_worker_helpers.py`
- **Functions**
  - `async test_extract_payload_from_message_success()` (line 10): no docstring
  - `async test_extract_payload_from_message_rejects_non_command()` (line 26): no docstring
  - `test_primary_key_and_objectify_helpers()` (line 40): no docstring
  - `test_retryable_error_detection()` (line 56): no docstring

### `backend/tests/unit/workers/test_instance_worker_objectify_gates.py`
- **Functions**
  - `test_primary_key_required_when_generation_disabled()` (line 6): no docstring
  - `async test_relationship_fallback_can_be_disabled()` (line 14): no docstring

### `backend/tests/unit/workers/test_instance_worker_s3.py`
- **Functions**
  - `async test_s3_call_does_not_block_event_loop()` (line 11): no docstring

### `backend/tests/unit/workers/test_message_relay_process.py`
- **Functions**
  - `_s3_client()` (line 19): no docstring
  - `_cleanup_bucket(client, bucket)` (line 30): no docstring
  - `async test_event_publisher_processes_index(monkeypatch)` (line 44): no docstring

### `backend/tests/unit/workers/test_objectify_delta_lakefs.py`
- **Functions**
  - `test_delta_result_has_changes_true()` (line 16): no docstring
  - `test_delta_result_has_changes_false()` (line 21): no docstring
  - `test_delta_result_stats_auto_computed()` (line 26): no docstring
  - `test_compute_row_key()` (line 38): no docstring
  - `test_compute_row_hash_deterministic()` (line 44): no docstring
  - `test_compute_row_hash_differs_on_change()` (line 52): no docstring
  - `async test_compute_delta_from_lakefs_diff_added_file()` (line 60): no docstring
  - `async test_compute_delta_from_lakefs_diff_removed_file()` (line 91): no docstring
  - `async test_compute_delta_from_lakefs_diff_changed_file()` (line 121): no docstring
  - `async test_compute_delta_from_snapshots()` (line 165): no docstring
  - `test_create_delta_computer_from_mapping_spec()` (line 194): no docstring
  - `test_create_delta_computer_fallback_pk()` (line 204): no docstring

### `backend/tests/unit/workers/test_objectify_incremental_default.py`
- **Functions**
  - `test_default_execution_mode_is_incremental()` (line 9): Default execution mode should be 'incremental' (not 'full').
  - `test_explicit_full_mode()` (line 23): no docstring
  - `test_explicit_delta_mode()` (line 37): no docstring
  - `test_watermark_fields()` (line 53): no docstring
  - `test_auto_detect_watermark_column()` (line 70): Test the auto-detect helper function from objectify_worker.

### `backend/tests/unit/workers/test_objectify_worker_helpers.py`
- **Functions**
  - `test_objectify_worker_field_helpers()` (line 7): no docstring
  - `test_objectify_worker_row_key_derivation()` (line 39): no docstring

### `backend/tests/unit/workers/test_objectify_worker_lineage_dataset_version.py`
- **Functions**
  - `async test_instance_lineage_records_dataset_version()` (line 21): no docstring
- **Classes**
  - `_FakeLineageStore` (line 9): no docstring
    - `__init__(self)` (line 10): no docstring
    - `node_aggregate(self, kind, aggregate_id)` (line 13): no docstring
    - `async record_link(self, **kwargs)` (line 16): no docstring

### `backend/tests/unit/workers/test_objectify_worker_link_index_dangling.py`
- **Functions**
  - `_link_job(dangling_policy)` (line 79): no docstring
  - `_object_backed_job(dangling_policy)` (line 96): no docstring
  - `_fk_job(dangling_policy)` (line 113): no docstring
  - `async test_link_index_fails_on_missing_target_when_policy_fail()` (line 131): no docstring
  - `async test_link_index_warns_on_missing_target_when_policy_warn()` (line 162): no docstring
  - `async test_link_index_creates_link_when_fk_matches_target()` (line 194): no docstring
  - `async test_link_index_dedupes_duplicate_pairs_for_join_table()` (line 226): no docstring
  - `async test_object_backed_link_index_creates_link()` (line 262): no docstring
  - `async test_object_backed_full_sync_clears_links_when_no_rows()` (line 298): no docstring
  - `async test_link_index_records_pass_result_with_lineage()` (line 334): no docstring
  - `async test_link_index_records_warn_when_dangling_policy_warn()` (line 376): no docstring
  - `async test_link_index_records_fail_when_dangling_policy_fail()` (line 416): no docstring
  - `async test_link_edits_are_applied_to_updates()` (line 456): no docstring
- **Classes**
  - `_StubObjectifyRegistry` (line 10): no docstring
    - `__init__(self)` (line 11): no docstring
    - `async update_objectify_job_status(self, **kwargs)` (line 14): no docstring
  - `_StubDatasetRegistry` (line 18): no docstring
    - `__init__(self, edits)` (line 19): no docstring
    - `async record_relationship_index_result(self, **kwargs)` (line 23): no docstring
    - `async list_link_edits(self, **kwargs)` (line 26): no docstring
  - `_DummyWorker` (line 30): no docstring
    - `__init__(self, rows, target_ids, ids_by_class)` (line 31): no docstring
    - `async _iter_dataset_batches(self, **kwargs)` (line 39): no docstring
    - `async _fetch_object_type_contract(self, job)` (line 42): no docstring
    - `async _fetch_ontology_version(self, job)` (line 52): no docstring
    - `async _bulk_update_instances(self, job, updates, ontology_version)` (line 55): no docstring
    - `async _iter_class_instance_ids(self, db_name, class_id, branch, limit)` (line 58): no docstring
    - `async _record_gate_result(self, **kwargs)` (line 64): no docstring
  - `_CaptureWorker` (line 68): no docstring
    - `__init__(self, rows, target_ids, ids_by_class)` (line 69): no docstring
    - `async _bulk_update_instances(self, job, updates, ontology_version)` (line 73): no docstring

### `backend/tests/unit/workers/test_objectify_worker_p0_gates.py`
- **Functions**
  - `_build_instances(worker, columns, rows, mappings, target_field_types, mapping_sources, sources_by_target, required_targets, pk_targets, pk_fields, field_constraints, field_raw_types, seen_row_keys, relationship_mappings, relationship_meta)` (line 7): no docstring
  - `test_missing_source_column_is_fatal()` (line 64): no docstring
  - `test_primary_key_missing_when_source_blank()` (line 79): no docstring
  - `test_required_field_missing_is_reported()` (line 98): no docstring
  - `test_value_constraints_fail_fast()` (line 116): no docstring
  - `test_value_constraints_format_failures(raw_type, format_hint, value)` (line 140): no docstring
  - `test_value_constraints_min_length_enforced()` (line 156): no docstring
  - `test_value_constraints_pattern_enforced()` (line 172): no docstring
  - `test_duplicate_primary_key_is_blocked()` (line 188): no docstring
  - `test_instance_id_requires_row_key()` (line 205): no docstring

### `backend/tests/unit/workers/test_objectify_worker_pk_uniqueness.py`
- **Functions**
  - `async test_pk_duplicates_fail_before_writes()` (line 100): no docstring
- **Classes**
  - `_StubObjectifyRegistry` (line 9): no docstring
    - `__init__(self, mapping_spec)` (line 10): no docstring
    - `async get_objectify_job(self, job_id)` (line 14): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 18): no docstring
    - `async update_objectify_job_status(self, **kwargs)` (line 23): no docstring
  - `_StubDatasetRegistry` (line 27): no docstring
    - `__init__(self, dataset, version)` (line 28): no docstring
    - `async get_dataset(self, dataset_id)` (line 32): no docstring
    - `async get_version(self, version_id)` (line 37): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 42): no docstring
  - `_FailIfWritePathCalled` (line 48): no docstring
    - `async write_instances(self, **kwargs)` (line 49): no docstring
    - `async finalize_job(self, **kwargs)` (line 52): no docstring
  - `_PKDuplicateWorker` (line 56): no docstring
    - `__init__(self, rows, mapping_spec, dataset, version)` (line 57): no docstring
    - `async _iter_dataset_batches(self, **kwargs)` (line 64): no docstring
    - `async _fetch_class_schema(self, job)` (line 67): no docstring
    - `async _fetch_object_type_contract(self, job)` (line 70): no docstring
    - `async _fetch_ontology_version(self, job)` (line 80): no docstring
    - `async _fetch_value_type_defs(self, job, value_type_refs)` (line 83): no docstring
    - `async _record_lineage_header(self, **kwargs)` (line 86): no docstring
    - `async _validate_batches(self, **kwargs)` (line 89): no docstring
    - `async _bulk_update_instances(self, **kwargs)` (line 92): no docstring
    - `async _record_gate_result(self, **kwargs)` (line 95): no docstring

### `backend/tests/unit/workers/test_objectify_write_paths.py`
- **Functions**
  - `_build_job()` (line 11): no docstring
  - `async test_dataset_primary_write_path_indexes_instances_directly()` (line 82): no docstring
  - `async test_build_document_populates_properties_from_flat_instance()` (line 114): Properties nested array should be auto-built from flat instance fields.
  - `async test_build_document_preserves_existing_properties()` (line 152): If properties list is already populated, it should not be overwritten.
  - `async test_build_document_skips_none_values()` (line 174): Properties with None values should not be included.
  - `async test_write_instances_passes_target_field_types()` (line 193): write_instances() should forward target_field_types to _build_document().
  - `async test_dataset_primary_finalize_prunes_stale_docs_on_full()` (line 220): no docstring
- **Classes**
  - `_FakeElasticsearchService` (line 26): no docstring
    - `__init__(self)` (line 27): no docstring
    - `async index_exists(self, index)` (line 37): no docstring
    - `async create_index(self, index, mappings, settings)` (line 40): no docstring
    - `async update_mapping(self, index, properties)` (line 45): no docstring
    - `async bulk_index(self, index, documents, chunk_size, refresh)` (line 49): no docstring
    - `async delete_document(self, index, doc_id, refresh)` (line 66): no docstring
    - `async refresh_index(self, index)` (line 70): no docstring
    - `async search(self, index, body)` (line 74): no docstring

### `backend/tests/unit/workers/test_ontology_worker_helpers.py`
- **Functions**
  - `async test_wait_for_database_exists_success()` (line 20): no docstring
  - `async test_wait_for_database_exists_timeout()` (line 28): no docstring
  - `async test_consumer_ops_defaults_to_inline()` (line 37): no docstring
  - `async test_heartbeat_loop_no_registry()` (line 51): no docstring
- **Classes**
  - `_StubTerminus` (line 11): no docstring
    - `__init__(self, exists)` (line 12): no docstring
    - `async database_exists(self, db_name)` (line 15): no docstring

### `backend/tests/unit/workers/test_pipeline_worker_helpers.py`
- **Functions**
  - `test_resolve_code_version_and_sensitive_keys(monkeypatch)` (line 25): no docstring
  - `test_resolve_lakefs_repository(monkeypatch)` (line 32): no docstring
  - `test_watermark_snapshot_helpers()` (line 37): no docstring
  - `test_resolve_output_format_and_partitions()` (line 63): no docstring
  - `test_resolve_external_read_mode_streaming_aliases()` (line 77): no docstring
  - `test_resolve_streaming_trigger_mode_and_timeout()` (line 84): no docstring
  - `test_streaming_external_source_requires_kafka_format()` (line 98): no docstring
  - `test_streaming_external_source_respects_global_toggle()` (line 113): no docstring
  - `test_resolve_kafka_value_format_and_checkpoint_policy()` (line 129): no docstring
  - `test_resolve_kafka_avro_schema_accepts_schema_registry_reference(monkeypatch)` (line 139): no docstring
  - `test_resolve_kafka_avro_schema_rejects_incomplete_schema_registry_reference()` (line 164): no docstring
  - `test_resolve_kafka_avro_schema_rejects_latest_registry_version()` (line 177): no docstring

### `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue.py`
- **Functions**
  - `async test_pipeline_worker_enqueues_objectify_job()` (line 72): no docstring
  - `async test_pipeline_worker_schema_mismatch_records_gate()` (line 108): no docstring
- **Classes**
  - `_FakeObjectifyRegistry` (line 13): no docstring
    - `__init__(self, mapping_spec, existing_job, mismatched_specs)` (line 14): no docstring
    - `async get_active_mapping_spec(self, **kwargs)` (line 19): no docstring
    - `async list_mapping_specs(self, **kwargs)` (line 23): no docstring
    - `build_dedupe_key(self, **kwargs)` (line 27): no docstring
    - `async get_objectify_job_by_dedupe_key(self, **kwargs)` (line 30): no docstring
  - `_FakeObjectifyJobQueue` (line 35): no docstring
    - `__init__(self)` (line 36): no docstring
    - `async publish(self, job, require_delivery)` (line 39): no docstring
  - `_FakeDatasetRegistry` (line 43): no docstring
    - `__init__(self)` (line 44): no docstring
    - `async record_gate_result(self, **kwargs)` (line 57): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 60): no docstring
    - `async get_version(self, version_id)` (line 65): no docstring

### `backend/tests/unit/workers/test_pipeline_worker_objectify_auto_enqueue_nospark.py`
- **Functions**
  - `_ensure_pyspark_stub()` (line 12): no docstring
  - `async test_pipeline_worker_enqueues_objectify_job_without_pyspark()` (line 115): no docstring
  - `async test_pipeline_worker_schema_mismatch_records_gate_without_pyspark()` (line 151): no docstring
- **Classes**
  - `_FakeObjectifyRegistry` (line 56): no docstring
    - `__init__(self, mapping_spec, existing_job, mismatched_specs)` (line 57): no docstring
    - `async get_active_mapping_spec(self, **kwargs)` (line 62): no docstring
    - `async list_mapping_specs(self, **kwargs)` (line 66): no docstring
    - `build_dedupe_key(self, **kwargs)` (line 70): no docstring
    - `async get_objectify_job_by_dedupe_key(self, **kwargs)` (line 73): no docstring
  - `_FakeObjectifyJobQueue` (line 78): no docstring
    - `__init__(self)` (line 79): no docstring
    - `async publish(self, job, require_delivery)` (line 82): no docstring
  - `_FakeDatasetRegistry` (line 86): no docstring
    - `__init__(self)` (line 87): no docstring
    - `async record_gate_result(self, **kwargs)` (line 100): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 103): no docstring
    - `async get_version(self, version_id)` (line 108): no docstring

### `backend/tests/unit/workers/test_pipeline_worker_transforms.py`
- **Functions**
  - `_resolve_java_home()` (line 29): no docstring
  - `spark()` (line 44): no docstring
  - `worker(spark)` (line 75): no docstring
  - `test_apply_transform_basic_ops(worker)` (line 81): no docstring
  - `test_apply_transform_join_union_groupby_pivot_window(worker)` (line 150): no docstring
  - `test_watermark_helpers(worker)` (line 192): no docstring
  - `test_pipeline_worker_file_helpers(worker, tmp_path)` (line 205): no docstring

### `backend/tests/unit/workers/test_spark_advanced_transforms.py`
- **Functions**
  - `_resolve_java_home()` (line 23): no docstring
  - `spark()` (line 38): no docstring
  - `worker(spark)` (line 69): no docstring
  - `test_split_transform(worker)` (line 76): no docstring
  - `test_geospatial_point_and_distance(worker)` (line 90): no docstring
  - `test_geospatial_geohash(worker)` (line 138): no docstring
  - `test_pattern_mining_contains_and_extract(worker)` (line 160): no docstring
  - `test_stream_join_transform(worker)` (line 219): no docstring
  - `test_stream_join_transform_respects_cache_expiration(worker)` (line 255): no docstring
  - `test_stream_join_transform_dynamic_selects_single_best_match_per_left_row(worker)` (line 290): no docstring
  - `test_stream_join_left_lookup_selects_single_latest_right_row_without_event_time(worker)` (line 329): no docstring
  - `test_stream_join_static_defaults_to_left_join_semantics(worker)` (line 360): no docstring
  - `async test_materialize_output_dataframe_rejects_partitioned_json(worker)` (line 416): no docstring
  - `async test_materialize_dataset_output_deduplicates_duplicate_primary_keys_for_append_only_new_rows(worker)` (line 435): no docstring
  - `async test_materialize_dataset_output_changelog_appends_only_new_or_changed_rows(worker)` (line 475): no docstring
  - `async test_materialize_dataset_output_snapshot_difference_keeps_current_transaction_duplicates(worker)` (line 527): no docstring
  - `async test_materialize_dataset_output_default_incremental_without_additive_signal_uses_snapshot_runtime(worker)` (line 581): no docstring
  - `async test_materialize_dataset_output_default_incremental_additive_updates_uses_append_runtime(worker)` (line 616): no docstring
  - `async test_materialize_virtual_output_writes_manifest_artifact(worker)` (line 651): no docstring
  - `test_select_new_or_changed_rows_returns_new_and_changed(worker)` (line 691): no docstring
  - `test_select_new_or_changed_rows_without_pk_returns_input(worker)` (line 718): no docstring
  - `test_select_new_or_changed_rows_can_preserve_input_duplicates_for_changelog(worker)` (line 742): no docstring
  - `test_udf_transform_applies_resolved_code(worker)` (line 770): no docstring
  - `test_udf_transform_supports_flat_map_rows(worker)` (line 794): no docstring
  - `test_udf_transform_rejects_schema_drift(worker)` (line 819): no docstring
- **Classes**
  - `_StorageStub` (line 387): no docstring
    - `__init__(self)` (line 388): no docstring
    - `async create_bucket(self, bucket_name)` (line 393): no docstring
    - `async delete_prefix(self, bucket, prefix)` (line 397): no docstring
    - `async save_bytes(self, bucket, key, data, content_type, metadata)` (line 401): no docstring

### `backend/tests/utils/__init__.py`

### `backend/tests/utils/auth.py`
- **Functions**
  - `_load_repo_dotenv()` (line 11): no docstring
  - `require_token(env_keys)` (line 15): no docstring
  - `bff_auth_headers()` (line 28): no docstring
  - `oms_auth_headers()` (line 45): no docstring
  - `build_smoke_user_jwt(subject, roles, tenant_id, org_id, email)` (line 58): Build a deterministic HS256 user JWT for integration tests.
  - `with_delegated_user(headers)` (line 93): no docstring

## writeback_materializer_worker

### `backend/writeback_materializer_worker/__init__.py`

### `backend/writeback_materializer_worker/main.py`
- **Functions**
  - `_parse_queue_seq(filename)` (line 42): no docstring
  - `_hash_inputs(keys)` (line 54): no docstring
  - `_extract_base_dataset_version_id(payload)` (line 63): no docstring
  - `async main()` (line 336): no docstring
- **Classes**
  - `WritebackMaterializerWorker` (line 72): no docstring
    - `__init__(self)` (line 73): no docstring
    - `async initialize(self)` (line 81): no docstring
    - `async shutdown(self)` (line 91): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 94): no docstring
    - `async _scan_queue(self, repository, branch)` (line 97): no docstring
    - `async materialize_db(self, db_name)` (line 130): no docstring
    - `async _materialize_db_inner(self, db_name)` (line 146): no docstring
    - `async run(self)` (line 315): no docstring
