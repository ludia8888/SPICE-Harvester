# Backend Method Index

> Generated: 2026-02-04T22:17:49+09:00
> Scope: backend/**/*.py (including scripts and tests, excluding __pycache__)

## action_outbox_worker

### `backend/action_outbox_worker/__init__.py`

### `backend/action_outbox_worker/main.py`
- **Functions**
  - `_resolve_overlay_branch(log)` (line 49): no docstring
  - `async main()` (line 402): no docstring
- **Classes**
  - `ActionOutboxWorker` (line 63): no docstring
    - `__init__(self)` (line 64): no docstring
    - `async initialize(self)` (line 73): no docstring
    - `async shutdown(self)` (line 90): no docstring
    - `async _get_event_seq(self, event_id)` (line 96): no docstring
    - `async _emit_action_applied(self, log)` (line 103): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 157): no docstring
    - `async _append_queue_entries(self, log)` (line 165): no docstring
    - `async _reconcile_log(self, log)` (line 288): no docstring
    - `async run(self)` (line 328): no docstring

## action_worker

### `backend/action_worker/__init__.py`

### `backend/action_worker/main.py`
- **Functions**
  - `async main()` (line 1839): no docstring
- **Classes**
  - `_ActionCommandPayload` (line 94): no docstring
  - `_ActionCommandParseError` (line 101): no docstring
    - `__init__(self, stage, payload_text, payload_obj, fallback_metadata, cause)` (line 102): no docstring
  - `_ActionRejected` (line 119): Used to short-circuit retries when the ActionLog is already finalized with a rejection result.
  - `ActionWorker` (line 123): no docstring
    - `__init__(self)` (line 124): no docstring
    - `async initialize(self)` (line 154): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 221): Handle partition revocation during rebalance.
    - `_on_partitions_assigned(self, partitions)` (line 229): Handle partition assignment during rebalance.
    - `async shutdown(self)` (line 237): no docstring
    - `async _poll(self, timeout)` (line 254): no docstring
    - `async _commit(self, msg)` (line 259): no docstring
    - `_heartbeat_options(self)` (line 264): no docstring
    - `_parse_payload(self, payload)` (line 270): no docstring
    - `_fallback_metadata(self, payload)` (line 323): no docstring
    - `_registry_key(self, payload)` (line 326): no docstring
    - `async _process_payload(self, payload)` (line 337): no docstring
    - `_span_name(self, payload)` (line 372): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 375): no docstring
    - `_metric_event_name(self, payload)` (line 396): no docstring
    - `async _seek(self, topic, partition, offset)` (line 399): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 404): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 439): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 455): no docstring
    - `_is_retryable_error(exc, payload)` (line 471): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 509): no docstring
    - `async _send_to_dlq(self, msg, error, attempt_count, payload, raw_payload, stage, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 571): no docstring
    - `async run(self)` (line 613): no docstring
    - `async _enforce_permission(self, db_name, submitted_by, submitted_by_type, action_spec)` (line 617): no docstring
    - `async _check_writeback_dataset_acl_alignment(self, db_name, submitted_by, submitted_by_type, actor_role, ontology_commit_id, resources, class_ids)` (line 641): no docstring
    - `async _execute_action(self, db_name, action_log_id, command, envelope)` (line 846): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 1684): no docstring
    - `async _write_patchset_commit(self, repository, branch, action_log_id, patchset, metadata_doc)` (line 1692): no docstring
    - `async _append_queue_entries(self, repository, branch, patchset_commit_id, action_log_id, action_applied_seq)` (line 1738): no docstring

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
- **Classes**
  - `PipelineOutputBinding` (line 10): no docstring
  - `PipelineAgentRunRequest` (line 26): no docstring

### `backend/agent/routers/__init__.py`

### `backend/agent/routers/agent.py`
- **Functions**
  - `_resolve_principal(request)` (line 21): no docstring
  - `_resolve_tenant_id(request)` (line 42): no docstring
  - `_actor_label(principal_type, principal_id)` (line 49): no docstring
  - `_request_meta(request, body)` (line 55): no docstring
  - `_step_id(index)` (line 63): no docstring
  - `_resolve_tool_id(tool_call)` (line 67): no docstring
  - `_extract_plan_id(context)` (line 76): no docstring
  - `_extract_plan_snapshot(context)` (line 89): no docstring
  - `_extract_risk_level(context)` (line 98): no docstring
  - `async _record_run_start(agent_registry, run_id, tenant_id, actor, requester, delegated_actor, body, request_meta)` (line 105): no docstring
  - `async _execute_agent_run(runtime, state, request_id, agent_registry, tenant_id)` (line 153): no docstring
  - `async create_agent_run(request, body)` (line 252): no docstring
  - `async get_agent_run(request, run_id, include_events, limit)` (line 343): no docstring
  - `async list_agent_run_events(request, run_id, limit, offset)` (line 436): no docstring

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
  - `async _run_single_step(runtime, semaphore, run_id, actor, step_index, tool_call, attempts, context, dry_run, request_headers, request_id)` (line 39): Execute exactly one tool-call step, applying deterministic enterprise retry policy.
  - `async run_agent_steps(runtime, initial_state)` (line 204): Execute the provided steps in order, stopping at the first failure.
- **Classes**
  - `AgentState` (line 21): no docstring

### `backend/agent/services/agent_runtime.py`
- **Functions**
  - `_clean_url(value)` (line 61): no docstring
  - `_safe_json(obj)` (line 65): no docstring
  - `_normalize_error_code(value)` (line 72): no docstring
  - `_extract_error_message(payload)` (line 84): no docstring
  - `_compute_tool_run_id(run_id, step_id, step_index, attempt)` (line 116): Deterministic-ish tool run id for observability.
  - `_extract_retry_after_ms(headers)` (line 129): no docstring
  - `_is_agent_proxy_path(path)` (line 154): no docstring
  - `_resolve_template_token(token, context)` (line 159): no docstring
  - `_resolve_template_string(value, context)` (line 203): no docstring
  - `_resolve_templates(obj, context)` (line 228): no docstring
  - `_artifact_value_from_response(payload)` (line 240): Choose the most useful artifact value from a tool response payload.
  - `_compact_tool_payload(payload)` (line 254): Best-effort compaction for large tool payloads.
  - `_compact_artifact_value(obj, depth)` (line 282): no docstring
  - `_walk_json_for_key(obj, wanted_keys, max_depth, max_nodes)` (line 307): no docstring
  - `_coerce_scalar(value)` (line 335): no docstring
  - `_extract_step_outputs(payload)` (line 345): no docstring
  - `_extract_scope(context)` (line 363): no docstring
  - `_normalize_status(value)` (line 383): no docstring
  - `_extract_command_id_from_url(url)` (line 390): no docstring
  - `_extract_command_id(payload)` (line 397): no docstring
  - `_extract_command_status(payload)` (line 423): no docstring
  - `_extract_pipeline_job_id(payload)` (line 442): no docstring
  - `_extract_pipeline_id(payload)` (line 458): no docstring
  - `_extract_pipeline_id_from_path(path)` (line 474): no docstring
  - `_extract_status_url(payload)` (line 481): no docstring
  - `_extract_progress(payload)` (line 495): no docstring
  - `_method_is_write(method)` (line 539): no docstring
  - `_tool_call_expects_pipeline_job(tool_id, path)` (line 543): no docstring
  - `_extract_overlay_status(payload)` (line 553): no docstring
  - `_extract_enterprise_legacy_code(payload)` (line 578): no docstring
  - `_iter_error_candidates(payload)` (line 590): no docstring
  - `_extract_error_key(payload)` (line 601): no docstring
  - `_extract_enterprise(payload)` (line 614): no docstring
  - `_extract_api_code(payload)` (line 622): no docstring
  - `_extract_api_category(payload)` (line 630): no docstring
  - `_extract_retryable(payload)` (line 638): no docstring
  - `_extract_action_log_signals(payload)` (line 651): no docstring
  - `_extract_action_simulation_signals(payload)` (line 672): Extract minimal, policy-relevant signals from ActionSimulation responses.
  - `_extract_action_simulation_rejection(payload)` (line 733): If the payload is an ActionSimulation response and the effective scenario is REJECTED,
- **Classes**
  - `AgentRuntimeConfig` (line 780): no docstring
  - `AgentRuntime` (line 802): no docstring
    - `__init__(self, event_store, audit_store, config)` (line 803): no docstring
    - `from_env(cls, event_store, audit_store)` (line 815): no docstring
    - `_resolve_base_url(self, tool_call)` (line 853): no docstring
    - `_preview_payload(self, obj)` (line 861): no docstring
    - `_payload_size(self, obj)` (line 866): no docstring
    - `_forward_headers(self, headers, actor)` (line 872): no docstring
    - `_resolve_status_url(self, command_id, payload)` (line 917): no docstring
    - `_resolve_ws_token(self, request_headers)` (line 927): no docstring
    - `_resolve_ws_url(self, command_id, token)` (line 943): no docstring
    - `_update_progress_context(self, context, command_id, status, progress_payload)` (line 956): no docstring
    - `async _fetch_command_status(self, status_url, actor, request_headers)` (line 975): no docstring
    - `async _wait_for_command_completion(self, run_id, actor, step_index, attempt, command_id, initial_payload, request_id, request_headers, context)` (line 997): no docstring
    - `_update_pipeline_progress_context(self, context, pipeline_id, job_id, status, run_payload)` (line 1136): no docstring
    - `async _fetch_pipeline_runs(self, pipeline_id, actor, request_headers, limit)` (line 1160): no docstring
    - `async _wait_for_pipeline_run_completion(self, run_id, actor, step_index, attempt, pipeline_id, job_id, initial_payload, request_id, request_headers, context)` (line 1187): no docstring
    - `async record_event(self, event_type, run_id, actor, status, data, request_id, step_index, resource_type, resource_id, error)` (line 1297): no docstring
    - `async execute_tool_call(self, run_id, actor, step_index, attempt, tool_call, context, dry_run, request_headers, request_id)` (line 1347): no docstring

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
  - `async get_terminus_service(oms_client)` (line 608): Get TerminusService with modern dependency injection
  - `async check_bff_dependencies_health(container)` (line 631): Check health of all BFF dependencies
- **Classes**
  - `BFFDependencyProvider` (line 51): Modern dependency provider for BFF services
    - `async get_oms_client(container)` (line 60): Get OMS client from container
    - `async get_label_mapper(container)` (line 84): Get label mapper from container
    - `async get_jsonld_converter(container)` (line 108): Get JSON-LD converter from container
    - `async get_action_log_registry(container)` (line 132): Get ActionLogRegistry (Postgres-backed) from container.
  - `TerminusService` (line 162): OMS client wrapper for TerminusService compatibility - Modernized version
    - `__init__(self, oms_client)` (line 170): Initialize with OMS client dependency
    - `async _await_if_needed(value)` (line 181): no docstring
    - `async _raise_for_status(self, response)` (line 186): no docstring
    - `async _response_json(self, response)` (line 189): no docstring
    - `async list_databases(self)` (line 192): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 203): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 208): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 213): 데이터베이스 정보 조회
    - `async list_classes(self, db_name, branch)` (line 218): 클래스 목록 조회
    - `async create_class(self, db_name, class_data, branch, headers)` (line 226): 클래스 생성
    - `async get_class(self, db_name, class_id, branch)` (line 241): 클래스 조회
    - `async update_class(self, db_name, class_id, class_data, expected_seq, branch, headers)` (line 265): 클래스 업데이트
    - `async delete_class(self, db_name, class_id, expected_seq, branch, headers)` (line 281): 클래스 삭제
    - `async query_database(self, db_name, query)` (line 296): 데이터베이스 쿼리
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 302): 브랜치 생성 - 실제 OMS API 호출
    - `async delete_branch(self, db_name, branch_name)` (line 313): 브랜치 삭제 - 실제 OMS API 호출
    - `async checkout(self, db_name, target, target_type)` (line 322): 체크아웃 - 실제 OMS API 호출
    - `async commit_changes(self, db_name, message, author, branch)` (line 334): 변경사항 커밋 - 실제 OMS API 호출
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 351): 커밋 히스토리 조회 - 실제 OMS API 호출
    - `async get_diff(self, db_name, base, compare)` (line 358): 차이 비교 - 실제 OMS API 호출
    - `async merge_branches(self, db_name, source, target, strategy, message, author)` (line 368): 브랜치 병합 - 실제 OMS API 호출
    - `async rollback(self, db_name, target_commit, create_branch, branch_name)` (line 391): 롤백 - 실제 OMS API 호출
    - `async get_branch_info(self, db_name, branch_name)` (line 412): 브랜치 정보 조회 - 실제 OMS API 호출
    - `async simulate_merge(self, db_name, source_branch, target_branch, strategy)` (line 424): 병합 시뮬레이션 - 충돌 감지 without 실제 병합
    - `async resolve_merge_conflicts(self, db_name, source_branch, target_branch, resolutions, strategy, message, author)` (line 442): 수동 충돌 해결 및 병합 실행
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references, headers)` (line 474): 고급 관계 관리 기능을 포함한 온톨로지 생성 - OMS API 호출
    - `async validate_relationships(self, db_name, ontology_data, branch, headers)` (line 508): 온톨로지 관계 검증 - OMS API 호출 (no write).
    - `async detect_circular_references(self, db_name, branch, new_ontology, headers)` (line 532): 순환 참조 탐지 - OMS API 호출 (no write).
    - `async analyze_relationship_network(self, db_name, branch, headers)` (line 556): 관계 네트워크 분석 - OMS API 호출 (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch, headers)` (line 578): 관계 경로 탐색 - OMS API 호출 (no write).

### `backend/bff/main.py`
- **Functions**
  - `async lifespan(app)` (line 680): Modern application lifecycle management
  - `async get_oms_client()` (line 925): Get OMS client from BFF container
  - `async get_label_mapper()` (line 935): Get label mapper from BFF container
  - `async get_google_sheets_service()` (line 945): Get Google Sheets service from BFF container
  - `async get_connector_registry()` (line 955): Get ConnectorRegistry from BFF container
  - `async get_dataset_registry()` (line 965): Get DatasetRegistry from BFF container
  - `async get_dataset_profile_registry()` (line 975): Get DatasetProfileRegistry from BFF container
  - `async get_pipeline_registry()` (line 985): Get PipelineRegistry from BFF container
  - `async get_pipeline_plan_registry()` (line 1002): Get PipelinePlanRegistry from BFF container
  - `async get_objectify_registry()` (line 1012): no docstring
  - `async get_agent_registry()` (line 1017): no docstring
  - `async get_agent_session_registry()` (line 1023): no docstring
  - `async get_agent_policy_registry()` (line 1029): no docstring
  - `async get_pipeline_executor()` (line 1035): Get PipelineExecutor from BFF container
- **Classes**
  - `BFFServiceContainer` (line 160): BFF-specific service container to manage BFF services
    - `__init__(self, container, settings)` (line 168): no docstring
    - `async initialize_bff_services(self)` (line 173): Initialize BFF-specific services
    - `async _initialize_oms_client(self)` (line 230): Initialize OMS client with health check
    - `async _initialize_label_mapper(self)` (line 251): Initialize label mapper
    - `async _initialize_type_inference(self)` (line 257): Initialize type inference service
    - `async _initialize_websocket_service(self)` (line 271): Initialize WebSocket notification service
    - `async _initialize_rate_limiter(self)` (line 293): Initialize rate limiting service
    - `async _initialize_connector_registry(self)` (line 309): Initialize Postgres-backed connector registry.
    - `async _initialize_dataset_registry(self)` (line 321): Initialize Postgres-backed dataset registry.
    - `async _initialize_dataset_profile_registry(self)` (line 332): Initialize Postgres-backed dataset profile registry.
    - `async _initialize_pipeline_registry(self)` (line 343): Initialize Postgres-backed pipeline registry.
    - `async _initialize_pipeline_plan_registry(self)` (line 354): Initialize Postgres-backed pipeline plan registry.
    - `async _initialize_objectify_registry(self)` (line 365): Initialize Postgres-backed objectify registry.
    - `async _initialize_agent_registry(self)` (line 376): Initialize Postgres-backed agent registry.
    - `async _initialize_agent_session_registry(self)` (line 387): Initialize Postgres-backed agent session registry.
    - `async _initialize_agent_policy_registry(self)` (line 398): Initialize Postgres-backed agent policy registry.
    - `async _initialize_agent_tool_registry(self)` (line 409): Initialize Postgres-backed agent tool registry (internal allowlist/policy).
    - `async _initialize_pipeline_executor(self)` (line 428): Initialize pipeline executor (preview/build engine).
    - `async _initialize_google_sheets_service(self)` (line 446): Initialize Google Sheets service (connector library)
    - `async shutdown_bff_services(self)` (line 465): Shutdown BFF-specific services
    - `get_oms_client(self)` (line 590): Get OMS client instance
    - `get_label_mapper(self)` (line 596): Get label mapper instance
    - `get_google_sheets_service(self)` (line 602): Get Google Sheets service instance
    - `get_connector_registry(self)` (line 608): Get connector registry instance
    - `get_dataset_registry(self)` (line 614): Get dataset registry instance
    - `get_dataset_profile_registry(self)` (line 620): Get dataset profile registry instance
    - `get_pipeline_registry(self)` (line 626): Get pipeline registry instance
    - `get_pipeline_plan_registry(self)` (line 632): Get pipeline plan registry instance
    - `get_objectify_registry(self)` (line 638): Get objectify registry instance
    - `get_agent_registry(self)` (line 644): Get agent registry instance
    - `get_agent_session_registry(self)` (line 650): Get agent session registry instance
    - `get_agent_policy_registry(self)` (line 656): Get agent policy registry instance
    - `get_agent_tool_registry(self)` (line 662): Get agent tool registry instance
    - `get_pipeline_executor(self)` (line 668): Get pipeline executor instance

### `backend/bff/middleware/__init__.py`

### `backend/bff/middleware/auth.py`
- **Functions**
  - `_dev_master_auth_enabled()` (line 55): no docstring
  - `_attach_dev_master_principal(request)` (line 63): no docstring
  - `_approx_token_count(payload)` (line 80): no docstring
  - `_safe_uuid(value)` (line 93): no docstring
  - `_resolve_agent_tool_run_id(request)` (line 103): no docstring
  - `_error_response(request, status_code, message, code, category, detail, context, headers)` (line 112): no docstring
  - `_internal_error_payload(request, message, detail)` (line 144): no docstring
  - `_set_scope_header(request, name, value)` (line 166): Attach a trusted header into the ASGI scope so downstream code that still
  - `_attach_verified_principal(request, principal)` (line 181): no docstring
  - `_compile_agent_tool_path(pattern)` (line 199): no docstring
  - `_path_matches_tool_policy(policy, request_path)` (line 213): no docstring
  - `_compile_agent_tool_path_params(pattern)` (line 220): no docstring
  - `_extract_agent_tool_path_params(policy, request_path)` (line 249): no docstring
  - `_resolve_agent_tool_id(request)` (line 259): no docstring
  - `async _capture_agent_tool_request(request)` (line 264): no docstring
  - `async _maybe_start_session_tool_call(request)` (line 293): no docstring
  - `async _finalize_session_tool_call(request, response, terminal_status)` (line 379): no docstring
  - `_resolve_agent_tool_registry(request)` (line 458): no docstring
  - `_resolve_agent_session_registry(request)` (line 468): no docstring
  - `_resolve_agent_policy_registry(request)` (line 478): no docstring
  - `_resolve_agent_registry(request)` (line 488): no docstring
  - `async _get_cached_tenant_policy(request, tenant_id)` (line 498): no docstring
  - `async _enforce_internal_agent_tool_policy(request)` (line 512): no docstring
  - `async _compute_agent_tool_idempotency_digest(request, tool_id)` (line 764): no docstring
  - `async _wait_for_tool_idempotency(registry, tenant_id, idempotency_key, timeout_s, interval_s)` (line 793): no docstring
  - `async _maybe_replay_or_start_tool_idempotency(request)` (line 814): no docstring
  - `async _finalize_tool_idempotency(request, response)` (line 925): no docstring
  - `async _finalize_tool_idempotency_error(request, exc)` (line 994): no docstring
  - `ensure_bff_auth_configured()` (line 1018): no docstring
  - `async _bff_auth_handle_missing_token(request, call_next, ctx)` (line 1054): no docstring
  - `async _bff_auth_handle_agent_token(request, call_next, ctx)` (line 1067): no docstring
  - `async _bff_auth_handle_expected_token(request, call_next, ctx)` (line 1183): no docstring
  - `async _bff_auth_handle_user_jwt(request, call_next, ctx)` (line 1233): no docstring
  - `async _bff_auth_handle_no_token_configured(request, call_next, ctx)` (line 1256): no docstring
  - `async _bff_auth_handle_invalid_credentials(request, call_next, ctx)` (line 1270): no docstring
  - `install_bff_auth_middleware(app)` (line 1282): no docstring
  - `async enforce_bff_websocket_auth(websocket, token)` (line 1319): no docstring
- **Classes**
  - `_BffAuthContext` (line 1045): no docstring

### `backend/bff/middleware/rbac.py`
- **Classes**
  - `Role` (line 18): 시스템 역할 정의
  - `Permission` (line 36): 세분화된 권한 정의
  - `BranchProtectionRule` (line 128): 브랜치 보호 규칙
  - `UserContext` (line 203): 인증된 사용자 정보
    - `from_token(cls, token)` (line 224): JWT 토큰에서 사용자 컨텍스트 생성
  - `RBACMiddleware` (line 243): Role-Based Access Control 미들웨어
    - `__init__(self, branch_protection_rules, enable_audit_log)` (line 255): 초기화
    - `async check_permission(self, user, permission, resource)` (line 270): 권한 확인
    - `async check_branch_permission(self, user, branch, action, db_name)` (line 303): 브랜치별 권한 확인
    - `_match_branch_pattern(self, branch, pattern)` (line 346): 브랜치 패턴 매칭
    - `async _log_access(self, user, permission, resource, granted)` (line 356): 감사 로그 기록

### `backend/bff/routers/__init__.py`

### `backend/bff/routers/actions.py`
- **Functions**
  - `async submit_action(db_name, action_type_id, request, http_request, base_branch, overlay_branch, oms_client)` (line 28): no docstring
  - `async simulate_action(db_name, action_type_id, request, http_request, oms_client)` (line 53): Action writeback simulation (dry-run) surface.
  - `async get_action_log(db_name, action_log_id, http_request, action_logs)` (line 76): no docstring
  - `async list_action_logs(db_name, http_request, status_filter, action_type_id, submitted_by, limit, offset, action_logs)` (line 92): no docstring
  - `async list_action_simulations(db_name, http_request, action_type_id, limit, offset)` (line 116): no docstring
  - `async get_action_simulation(db_name, simulation_id, http_request, include_versions, version_limit)` (line 134): no docstring
  - `async list_action_simulation_versions(db_name, simulation_id, http_request, limit, offset)` (line 152): no docstring
  - `async get_action_simulation_version(db_name, simulation_id, version, http_request)` (line 170): no docstring

### `backend/bff/routers/admin.py`

### `backend/bff/routers/admin_deps.py`
- **Functions**
  - `async require_admin(request)` (line 24): Minimal admin guard for operational endpoints.

### `backend/bff/routers/admin_lakefs.py`
- **Functions**
  - `async list_lakefs_credentials(registry)` (line 31): no docstring
  - `async upsert_lakefs_credentials(payload, request, registry)` (line 39): no docstring
- **Classes**
  - `LakeFSCredentialsUpsertRequest` (line 21): Upsert request for lakeFS credentials stored in Postgres (encrypted).

### `backend/bff/routers/admin_recompute_projection.py`
- **Functions**
  - `async recompute_projection(http_request, request, background_tasks, task_manager, redis_service, audit_store, lineage_store, elasticsearch_service)` (line 31): no docstring
  - `async get_recompute_projection_result(task_id, task_manager, redis_service)` (line 55): no docstring

### `backend/bff/routers/admin_replay.py`
- **Functions**
  - `async replay_instance_state(request, background_tasks, storage_service, task_manager, redis_service)` (line 29): no docstring
  - `async get_replay_result(task_id, task_manager, redis_service)` (line 47): no docstring
  - `async get_replay_trace(task_id, command_id, include_audit, audit_limit, include_lineage, lineage_direction, lineage_max_depth, lineage_max_nodes, lineage_max_edges, timeline_limit, task_manager, redis_service, audit_store, lineage_store)` (line 60): no docstring
  - `async cleanup_old_replay_results(older_than_hours, redis_service)` (line 99): no docstring

### `backend/bff/routers/admin_system.py`
- **Functions**
  - `async get_system_health(task_manager, redis_service)` (line 21): no docstring

### `backend/bff/routers/admin_task_monitor.py`
- **Functions**
  - `async monitor_admin_task(task_id, task_manager)` (line 20): no docstring

### `backend/bff/routers/agent_proxy.py`
- **Functions**
  - `async create_pipeline_run(request, body, llm, redis_service, audit_store, dataset_registry, pipeline_registry, plan_registry)` (line 48): Pipeline agent runs are handled in the BFF as a single autonomous loop + MCP tools.
  - `async stream_pipeline_run(request, body, llm, redis_service, audit_store, dataset_registry, plan_registry)` (line 135): SSE 스트리밍 버전의 Pipeline Agent API.

### `backend/bff/routers/ai.py`
- **Functions**
  - `async ai_intent(body, request, llm, redis_service, audit_store, sessions, dataset_registry)` (line 24): no docstring
  - `async translate_query_plan(db_name, body, request, llm, redis_service, audit_store, oms, sessions, dataset_registry)` (line 47): no docstring
  - `async ai_query(db_name, body, request, llm, redis_service, audit_store, lineage_store, oms, mapper, terminus, sessions, dataset_registry)` (line 73): no docstring

### `backend/bff/routers/audit.py`
- **Functions**
  - `async list_audit_logs(partition_key, action, status_filter, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset, audit_store)` (line 21): no docstring
  - `async get_chain_head(partition_key, audit_store)` (line 63): no docstring

### `backend/bff/routers/ci_webhooks.py`
- **Functions**
  - `_safe_uuid(value)` (line 49): no docstring
  - `async ingest_ci_result(body, request, sessions)` (line 59): no docstring
- **Classes**
  - `AgentSessionCIResultIngestRequest` (line 31): no docstring

### `backend/bff/routers/command_status.py`
- **Functions**
  - `async get_command_status(command_id, oms)` (line 26): Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

### `backend/bff/routers/context7.py`
- **Functions**
  - `_context7_unavailable_exc()` (line 37): no docstring
  - `async get_context7_client()` (line 48): no docstring
  - `async search_context7(request, client)` (line 62): no docstring
  - `async get_entity_context(entity_id, client)` (line 67): no docstring
  - `async add_knowledge(request, client)` (line 72): no docstring
  - `async create_entity_link(request, client)` (line 77): no docstring
  - `async analyze_ontology(request, client, oms_client)` (line 82): no docstring
  - `async get_ontology_suggestions(db_name, class_id, client)` (line 91): no docstring
  - `async check_context7_health(client)` (line 100): no docstring

### `backend/bff/routers/context_tools.py`
- **Functions**
  - `_resolve_verified_principal(request)` (line 22): no docstring
  - `_policy_set(value)` (line 30): no docstring
  - `_enforce_policy_value(allowed, value)` (line 40): no docstring
  - `_filter_dataset_ids(dataset_ids, allowed_dataset_ids)` (line 57): no docstring
  - `async describe_datasets(body, request, dataset_registry, policy_registry)` (line 73): no docstring
  - `async snapshot_ontology(body, request, oms_client, policy_registry)` (line 132): no docstring
- **Classes**
  - `OntologySnapshotRequest` (line 45): no docstring
  - `DatasetDescribeRequest` (line 51): no docstring

### `backend/bff/routers/data_connector.py`

### `backend/bff/routers/data_connector_browse.py`
- **Functions**
  - `async list_google_sheets_spreadsheets(http_request, connection_id, query, limit, connector_registry, google_sheets_service)` (line 32): no docstring
  - `async list_google_sheets_worksheets(sheet_id, http_request, connection_id, connector_registry, google_sheets_service)` (line 60): no docstring

### `backend/bff/routers/data_connector_connections.py`
- **Functions**
  - `async list_google_sheets_connections(http_request, connector_registry)` (line 31): no docstring
  - `async delete_google_sheets_connection(connection_id, http_request, connector_registry)` (line 56): no docstring

### `backend/bff/routers/data_connector_deps.py`
- **Functions**
  - `async get_google_sheets_service()` (line 17): Import here to avoid circular dependency.
  - `async get_connector_registry()` (line 24): Import here to avoid circular dependency.
  - `async get_objectify_job_queue(objectify_registry)` (line 31): no docstring

### `backend/bff/routers/data_connector_oauth.py`
- **Functions**
  - `async start_google_sheets_oauth(payload, http_request)` (line 39): no docstring
  - `async google_sheets_oauth_callback(request, code, state, connector_registry)` (line 76): no docstring

### `backend/bff/routers/data_connector_ops.py`
- **Functions**
  - `_build_google_oauth_client()` (line 22): no docstring
  - `_connector_oauth_enabled(oauth_client)` (line 26): no docstring
  - `_append_query_param(url, key, value)` (line 30): no docstring
  - `async _resolve_google_connection(connector_registry, oauth_client, connection_id)` (line 37): no docstring
  - `async _resolve_optional_access_token(connector_registry, connection_id)` (line 87): no docstring

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
  - `async extract_google_sheet_grid(request, http_request, google_sheets_service, connector_registry)` (line 34): no docstring
  - `async preview_google_sheet_for_funnel(request, http_request, limit, google_sheets_service, connector_registry)` (line 91): no docstring

### `backend/bff/routers/database.py`
- **Functions**
  - `async list_databases(request, oms, dataset_registry)` (line 21): 데이터베이스 목록 조회
  - `async create_database(request, http_request, oms)` (line 40): 데이터베이스 생성
  - `async delete_database(db_name, http_request, expected_seq, oms)` (line 55): 데이터베이스 삭제
  - `async get_branch_info(db_name, branch_name, oms)` (line 75): 브랜치 정보 조회 (프론트엔드용 BFF 래핑)
  - `async delete_branch(db_name, branch_name, force, oms)` (line 81): 브랜치 삭제 (프론트엔드용 BFF 래핑)
  - `async get_database(db_name, oms)` (line 92): 데이터베이스 정보 조회
  - `async get_database_expected_seq(db_name)` (line 98): Resolve the current `expected_seq` for database (aggregate) operations.
  - `async list_classes(db_name, type, limit, oms)` (line 110): 데이터베이스의 클래스 목록 조회
  - `async create_class(db_name, class_data, oms)` (line 121): 데이터베이스에 새 클래스 생성
  - `async get_class(db_name, class_id, request, oms)` (line 129): 특정 클래스 조회
  - `async list_branches(db_name, oms)` (line 138): 브랜치 목록 조회
  - `async create_branch(db_name, branch_data, oms)` (line 144): 새 브랜치 생성
  - `async get_versions(db_name, oms)` (line 152): 버전 히스토리 조회

### `backend/bff/routers/document_bundles.py`
- **Functions**
  - `_resolve_verified_principal(request)` (line 20): no docstring
  - `_enforce_bundle_access(tenant_policy, bundle_id)` (line 29): no docstring
  - `async search_document_bundle(bundle_id, body, request, policy_registry, client)` (line 46): no docstring
- **Classes**
  - `DocumentBundleSearchRequest` (line 39): no docstring

### `backend/bff/routers/governance.py`
- **Functions**
  - `async create_backing_datasource(body, request, dataset_registry)` (line 30): no docstring
  - `async list_backing_datasources(request, dataset_id, db_name, branch, dataset_registry)` (line 44): no docstring
  - `async get_backing_datasource(backing_id, request, dataset_registry)` (line 62): no docstring
  - `async create_backing_datasource_version(backing_id, body, request, dataset_registry)` (line 76): no docstring
  - `async list_backing_datasource_versions(backing_id, request, dataset_registry)` (line 92): no docstring
  - `async get_backing_datasource_version(version_id, request, dataset_registry)` (line 106): no docstring
  - `async create_key_spec(body, request, dataset_registry)` (line 120): no docstring
  - `async list_key_specs(request, dataset_id, dataset_registry)` (line 134): no docstring
  - `async get_key_spec(key_spec_id, request, dataset_registry)` (line 148): no docstring
  - `async list_schema_migration_plans(request, db_name, subject_type, subject_id, status_value, dataset_registry)` (line 162): no docstring
  - `async upsert_gate_policy(body, dataset_registry)` (line 182): no docstring
  - `async list_gate_policies(scope, dataset_registry)` (line 194): no docstring
  - `async list_gate_results(scope, subject_type, subject_id, dataset_registry)` (line 206): no docstring
  - `async upsert_access_policy(body, request, dataset_registry)` (line 222): no docstring
  - `async list_access_policies(request, db_name, scope, subject_type, subject_id, policy_status, dataset_registry)` (line 236): no docstring

### `backend/bff/routers/graph.py`
- **Functions**
  - `async execute_graph_query(db_name, query, request, lineage_store, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 34): no docstring
  - `async execute_simple_graph_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 59): no docstring
  - `async execute_multi_hop_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 82): no docstring
  - `async find_relationship_paths(db_name, source_class, target_class, max_depth, graph_service, branch)` (line 105): no docstring
  - `async graph_service_health(graph_service)` (line 124): no docstring
  - `async register_projection(db_name, request, graph_service)` (line 151): no docstring
  - `async query_projection(db_name, request, graph_service)` (line 179): no docstring
  - `async list_projections(db_name, graph_service)` (line 207): no docstring
- **Classes**
  - `ProjectionRegistrationRequest` (line 131): no docstring
  - `ProjectionQueryRequest` (line 139): no docstring

### `backend/bff/routers/health.py`
- **Functions**
  - `async root()` (line 22): 루트 엔드포인트
  - `async health_check(oms_client)` (line 36): 헬스체크 엔드포인트

### `backend/bff/routers/instance_async.py`
- **Functions**
  - `async create_instance_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 29): 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
  - `async update_instance_async(db_name, class_label, instance_id, request, http_request, expected_seq, branch, oms_client, label_mapper, user_id)` (line 63): 인스턴스 수정 명령을 비동기로 처리 (Label 기반)
  - `async delete_instance_async(db_name, class_label, instance_id, http_request, branch, expected_seq, oms_client, label_mapper, user_id)` (line 98): 인스턴스 삭제 명령을 비동기로 처리 (Label 기반)
  - `async bulk_create_instances_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 130): 대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)

### `backend/bff/routers/instances.py`
- **Functions**
  - `async _maybe_get_action_log_registry(class_id)` (line 31): no docstring
  - `async get_class_instances(db_name, class_id, http_request, base_branch, overlay_branch, branch, limit, offset, search, status_filter, action_type_id, submitted_by, elasticsearch_service, oms_client, dataset_registry, action_logs)` (line 44): no docstring
  - `async get_class_sample_values(db_name, class_id, property_name, limit, oms_client, dataset_registry)` (line 92): no docstring
  - `async get_instance(db_name, class_id, instance_id, http_request, base_branch, overlay_branch, branch, elasticsearch_service, oms_client, dataset_registry, action_logs)` (line 120): no docstring

### `backend/bff/routers/lineage.py`
- **Functions**
  - `_parse_artifact_node_id(node_id)` (line 32): Parse artifact node id: artifact:<kind>:<...>
  - `_suggest_remediation_actions(artifacts)` (line 50): Recommend safe operational actions.
  - `async get_lineage_graph(root, db_name, direction, max_depth, max_nodes, max_edges, lineage_store)` (line 103): no docstring
  - `async get_lineage_impact(root, db_name, direction, max_depth, artifact_kind, max_nodes, max_edges, lineage_store)` (line 135): no docstring
  - `async get_lineage_metrics(db_name, window_minutes, lineage_store, audit_store)` (line 196): Operational lineage metrics.

### `backend/bff/routers/link_types.py`

### `backend/bff/routers/link_types_deps.py`

### `backend/bff/routers/link_types_edits.py`
- **Functions**
  - `async list_link_edits(db_name, link_type_id, branch, dataset_registry)` (line 22): no docstring
  - `async create_link_edit(db_name, link_type_id, body, request, dataset_registry)` (line 62): no docstring

### `backend/bff/routers/link_types_ops.py`

### `backend/bff/routers/link_types_read.py`
- **Functions**
  - `async list_link_types(db_name, branch, oms_client, dataset_registry)` (line 30): no docstring
  - `async get_link_type(db_name, link_type_id, request, branch, oms_client, dataset_registry, _)` (line 67): no docstring

### `backend/bff/routers/link_types_write.py`
- **Functions**
  - `async create_link_type(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 29): no docstring
  - `async update_link_type(db_name, link_type_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 55): no docstring
  - `async reindex_link_type(db_name, link_type_id, request, dataset_version_id, dataset_registry, objectify_registry)` (line 83): no docstring

### `backend/bff/routers/mapping.py`
- **Functions**
  - `async export_mappings(db_name, mapper)` (line 20): no docstring
  - `async import_mappings(db_name, file, mapper, oms_client)` (line 25): no docstring
  - `async validate_mappings(db_name, file, mapper, oms_client)` (line 40): no docstring
  - `async get_mappings_summary(db_name, mapper)` (line 55): no docstring
  - `async clear_mappings(db_name, mapper)` (line 60): no docstring

### `backend/bff/routers/merge_conflict.py`
- **Functions**
  - `async simulate_merge(db_name, request, oms_client)` (line 21): no docstring
  - `async resolve_merge_conflicts(db_name, request, oms_client)` (line 30): no docstring

### `backend/bff/routers/object_types.py`
- **Functions**
  - `async _require_domain_role(request, db_name)` (line 30): no docstring
  - `async create_object_type_contract(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 38): no docstring
  - `async get_object_type_contract(db_name, class_id, request, branch, oms_client, dataset_registry, objectify_registry)` (line 66): no docstring
  - `async update_object_type_contract(db_name, class_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 89): no docstring

### `backend/bff/routers/object_types_deps.py`

### `backend/bff/routers/objectify.py`

### `backend/bff/routers/objectify_dag.py`
- **Functions**
  - `async run_objectify_dag(db_name, body, request, dataset_registry, objectify_registry, job_queue, oms_client)` (line 27): no docstring

### `backend/bff/routers/objectify_deps.py`
- **Functions**
  - `async get_objectify_job_queue(objectify_registry)` (line 18): no docstring
  - `async _require_db_role(request, db_name, roles)` (line 24): no docstring

### `backend/bff/routers/objectify_enterprise.py`
- **Functions**
  - `async detect_relationships(db_name, dataset_id, request, body, branch, dataset_registry)` (line 29): Detect potential FK relationships in a dataset.

### `backend/bff/routers/objectify_incremental.py`
- **Functions**
  - `async trigger_incremental_objectify(mapping_spec_id, request, body, branch, dataset_registry, objectify_registry, job_queue)` (line 33): Trigger objectify with incremental execution mode.
  - `async get_mapping_spec_watermark(mapping_spec_id, request, branch, objectify_registry)` (line 125): Get watermark state for incremental objectify.

### `backend/bff/routers/objectify_job_ops.py`
- **Functions**
  - `async enqueue_objectify_job_for_mapping_spec(objectify_registry, mapping_spec_id, mapping_spec_version, dataset_registry, dataset_id, dataset_version_id, dataset, version, mapping_spec_record, options_override, options_defaults, strict_dataset_match)` (line 18): no docstring

### `backend/bff/routers/objectify_mapping_specs.py`
- **Functions**
  - `async create_mapping_spec(body, request, dataset_registry, objectify_registry, oms_client)` (line 26): no docstring
  - `async list_mapping_specs(dataset_id, include_inactive, objectify_registry)` (line 43): no docstring

### `backend/bff/routers/objectify_ops.py`

### `backend/bff/routers/objectify_runs.py`
- **Functions**
  - `async run_objectify(dataset_id, body, request, dataset_registry, objectify_registry, job_queue, pipeline_registry)` (line 31): no docstring

### `backend/bff/routers/ontology.py`

### `backend/bff/routers/ontology_agent.py`
- **Functions**
  - `_get_tenant_id(request)` (line 47): Extract tenant ID from request headers.
  - `_get_user_id(request)` (line 57): Extract user ID from request headers.
  - `_get_actor(request)` (line 63): Extract actor from request headers.
  - `async run_ontology_agent(request, body)` (line 89): Run the autonomous ontology agent by delegating to Pipeline Agent.
- **Classes**
  - `OntologyAgentRunRequest` (line 32): Request body for ontology agent runs.

### `backend/bff/routers/ontology_crud.py`
- **Functions**
  - `async create_ontology(db_name, ontology, branch, mapper, oms_client)` (line 41): no docstring
  - `async list_ontologies(db_name, request, branch, class_type, limit, offset, mapper, terminus)` (line 58): no docstring
  - `async get_ontology(db_name, class_label, request, branch, mapper, terminus)` (line 81): no docstring
  - `async validate_ontology_create_bff(db_name, ontology, request, branch, oms_client)` (line 100): no docstring
  - `async validate_ontology_update_bff(db_name, class_label, ontology, request, branch, mapper, oms_client)` (line 117): no docstring
  - `async update_ontology(db_name, class_label, ontology, request, expected_seq, branch, mapper, terminus)` (line 147): no docstring
  - `async delete_ontology(db_name, class_label, request, expected_seq, branch, mapper, terminus)` (line 179): no docstring
  - `async get_ontology_schema(db_name, class_id, request, format, branch, mapper, terminus, jsonld_conv)` (line 200): no docstring

### `backend/bff/routers/ontology_extensions.py`
- **Functions**
  - `_resource_routes(resource_type)` (line 29): no docstring
  - `async list_ontology_branches(db_name, oms_client)` (line 130): no docstring
  - `async create_ontology_branch(db_name, request, oms_client)` (line 138): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, oms_client)` (line 147): no docstring
  - `async create_ontology_proposal(db_name, request, oms_client)` (line 162): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, oms_client)` (line 175): no docstring
  - `async deploy_ontology(db_name, request, oms_client)` (line 190): no docstring
  - `async ontology_health(db_name, branch, oms_client)` (line 203): no docstring

### `backend/bff/routers/ontology_imports.py`
- **Functions**
  - `async dry_run_import_from_google_sheets(db_name, request)` (line 25): no docstring
  - `async commit_import_from_google_sheets(db_name, request, oms_client)` (line 33): no docstring
  - `async dry_run_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, dry_run_rows, max_import_rows, options_json)` (line 46): no docstring
  - `async commit_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, allow_partial, max_import_rows, batch_size, return_instances, max_return_instances, options_json, oms_client)` (line 87): no docstring

### `backend/bff/routers/ontology_metadata.py`
- **Functions**
  - `async save_mapping_metadata(db_name, class_id, metadata, oms, mapper)` (line 24): 매핑 메타데이터를 온톨로지 클래스에 저장

### `backend/bff/routers/ontology_ops.py`

### `backend/bff/routers/ontology_relationships.py`
- **Functions**
  - `async create_ontology_with_relationship_validation(db_name, ontology, request, branch, auto_generate_inverse, validate_relationships, check_circular_references, mapper, terminus)` (line 33): no docstring
  - `async validate_ontology_relationships_bff(db_name, ontology, request, branch, mapper, terminus)` (line 58): no docstring
  - `async check_circular_references_bff(db_name, request, ontology, branch, mapper, terminus)` (line 77): no docstring
  - `async analyze_relationship_network_bff(db_name, request, terminus, mapper)` (line 96): no docstring
  - `async find_relationship_paths_bff(request, db_name, start_entity, end_entity, max_depth, path_type, terminus, mapper)` (line 111): no docstring

### `backend/bff/routers/ontology_suggestions.py`
- **Functions**
  - `async suggest_schema_from_data(db_name, request, terminus)` (line 29): no docstring
  - `async suggest_mappings(db_name, request)` (line 39): no docstring
  - `async suggest_mappings_from_google_sheets(db_name, request)` (line 47): no docstring
  - `async suggest_mappings_from_excel(db_name, target_class_id, file, target_schema_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, include_relationships, enable_semantic_hints, max_tables, max_rows, max_cols)` (line 55): no docstring
  - `async suggest_schema_from_google_sheets(db_name, request, terminus)` (line 92): no docstring
  - `async suggest_schema_from_excel(db_name, file, sheet_name, class_name, table_id, table_top, table_left, table_bottom, table_right, include_complex_types, max_tables, max_rows, max_cols)` (line 102): no docstring

### `backend/bff/routers/ops.py`
- **Functions**
  - `async ops_status(dataset_registry, objectify_registry)` (line 19): no docstring

### `backend/bff/routers/pipeline.py`

### `backend/bff/routers/pipeline_branches.py`
- **Functions**
  - `async list_pipeline_branches(db_name, pipeline_registry)` (line 29): no docstring
  - `async archive_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 49): no docstring
  - `async restore_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 91): no docstring
  - `async create_pipeline_branch(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 131): no docstring

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
- **Functions**
  - `async get_objectify_job_queue(objectify_registry)` (line 16): no docstring

### `backend/bff/routers/pipeline_datasets_ingest.py`
- **Functions**
  - `async get_dataset_ingest_request(ingest_request_id, request, dataset_registry)` (line 30): no docstring
  - `async approve_dataset_schema(ingest_request_id, payload, request, dataset_registry)` (line 62): no docstring

### `backend/bff/routers/pipeline_datasets_ops.py`

### `backend/bff/routers/pipeline_datasets_ops_funnel.py`
- **Functions**
  - `_normalize_inferred_type(type_value)` (line 18): no docstring
  - `_build_schema_columns(columns, inferred_schema)` (line 42): no docstring
  - `_columns_from_schema(schema_columns)` (line 66): no docstring
  - `_rows_from_preview(columns, sample_rows)` (line 74): no docstring
  - `_build_funnel_analysis_payload(analysis, inferred_schema)` (line 89): no docstring
  - `_extract_sample_columns(sample_json)` (line 102): no docstring
  - `_extract_sample_rows(sample_json, columns)` (line 133): no docstring
  - `async _compute_funnel_analysis_from_sample(sample_json)` (line 163): no docstring
  - `_select_sample_row(sample_json, filename, file_index)` (line 199): no docstring

### `backend/bff/routers/pipeline_datasets_ops_ingest.py`
- **Functions**
  - `async _ensure_ingest_transaction(dataset_registry, ingest_request_id)` (line 17): no docstring
  - `_build_ingest_request_fingerprint(payload)` (line 32): no docstring
  - `_ingest_staging_prefix(prefix, ingest_request_id)` (line 40): no docstring
  - `_sanitize_s3_metadata(metadata)` (line 45): no docstring
  - `_dataset_artifact_prefix(db_name, dataset_id, dataset_name)` (line 61): no docstring

### `backend/bff/routers/pipeline_datasets_ops_lakefs.py`
- **Functions**
  - `async _acquire_lakefs_commit_lock(repository, branch, job_id)` (line 25): no docstring
  - `async _release_lakefs_commit_lock(redis_service, lock_key, token)` (line 61): no docstring
  - `_lakefs_commit_retry_delay(attempt)` (line 74): no docstring
  - `async _resolve_lakefs_commit_from_head(lakefs_client, lakefs_storage_service, repository, branch, object_key, expected_checksum, attempts)` (line 78): no docstring
  - `_resolve_lakefs_raw_repository()` (line 123): no docstring
  - `async _ensure_lakefs_branch_exists(lakefs_client, repository, branch, source_branch)` (line 128): no docstring
  - `_extract_lakefs_ref_from_artifact_key(artifact_key)` (line 149): no docstring
  - `async _commit_lakefs_with_predicate_fallback(lakefs_client, lakefs_storage_service, repository, branch, message, metadata, object_key, expected_checksum)` (line 160): no docstring

### `backend/bff/routers/pipeline_datasets_ops_objectify.py`
- **Functions**
  - `async _maybe_enqueue_objectify_job(dataset, version, objectify_registry, job_queue, dataset_registry, actor_user_id)` (line 21): no docstring

### `backend/bff/routers/pipeline_datasets_ops_parsing.py`
- **Functions**
  - `_default_dataset_name(filename)` (line 16): no docstring
  - `_convert_xls_to_xlsx_bytes(xls_bytes)` (line 25): no docstring
  - `_normalize_table_bbox(table_top, table_left, table_bottom, table_right)` (line 70): no docstring
  - `_detect_csv_delimiter(sample)` (line 93): no docstring
  - `_parse_csv_rows(reader, has_header, preview_limit)` (line 103): no docstring
  - `_parse_csv_file(file_obj, delimiter, has_header, preview_limit)` (line 133): no docstring
  - `_parse_csv_content(content, delimiter, has_header, preview_limit)` (line 201): no docstring

### `backend/bff/routers/pipeline_datasets_uploads.py`

### `backend/bff/routers/pipeline_datasets_uploads_csv.py`
- **Functions**
  - `async upload_csv_dataset(db_name, branch, file, dataset_name, description, delimiter, has_header, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 45): no docstring

### `backend/bff/routers/pipeline_datasets_uploads_excel.py`
- **Functions**
  - `async upload_excel_dataset(db_name, branch, file, dataset_name, description, sheet_name, table_id, table_top, table_left, table_bottom, table_right, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 46): no docstring

### `backend/bff/routers/pipeline_datasets_uploads_media.py`
- **Functions**
  - `async upload_media_dataset(db_name, branch, files, dataset_name, description, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 27): no docstring

### `backend/bff/routers/pipeline_datasets_versions.py`
- **Functions**
  - `async create_dataset(payload, dataset_registry)` (line 37): no docstring
  - `async create_dataset_version(dataset_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue)` (line 100): no docstring
  - `async reanalyze_dataset_version(dataset_id, version_id, request, dataset_registry)` (line 127): no docstring

### `backend/bff/routers/pipeline_deps.py`
- **Functions**
  - `async get_pipeline_executor()` (line 16): no docstring
  - `async get_pipeline_job_queue()` (line 22): no docstring

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
  - `async list_pipeline_runs(pipeline_id, limit, pipeline_registry, request)` (line 25): no docstring
  - `async list_pipeline_artifacts(pipeline_id, mode, limit, pipeline_registry, request)` (line 52): no docstring
  - `async get_pipeline_artifact(pipeline_id, artifact_id, pipeline_registry, request)` (line 82): no docstring

### `backend/bff/routers/pipeline_ops.py`

### `backend/bff/routers/pipeline_ops_augmentation.py`

### `backend/bff/routers/pipeline_ops_augmentation_casts.py`

### `backend/bff/routers/pipeline_ops_augmentation_contract.py`

### `backend/bff/routers/pipeline_ops_definition.py`
- **Functions**
  - `_stable_definition_hash(definition_json)` (line 16): no docstring
  - `_resolve_definition_commit_id(definition_json, latest_version, definition_hash)` (line 24): no docstring
  - `_normalize_location(location)` (line 39): no docstring
  - `_extract_node_ids(definition_json)` (line 51): no docstring
  - `_extract_edge_ids(definition_json)` (line 67): no docstring
  - `_definition_diff(previous, current)` (line 89): no docstring

### `backend/bff/routers/pipeline_ops_dependencies.py`
- **Functions**
  - `_normalize_dependencies_payload(raw)` (line 20): no docstring
  - `async _validate_dependency_targets(pipeline_registry, db_name, pipeline_id, dependencies)` (line 46): no docstring
  - `_format_dependencies_for_api(dependencies)` (line 69): no docstring

### `backend/bff/routers/pipeline_ops_locks.py`
- **Functions**
  - `async _acquire_pipeline_publish_lock(pipeline_id, branch, job_id)` (line 23): no docstring
  - `async _release_pipeline_publish_lock(redis_service, lock_key, token)` (line 59): no docstring

### `backend/bff/routers/pipeline_ops_policy.py`
- **Functions**
  - `_resolve_pipeline_protected_branches()` (line 11): no docstring
  - `_pipeline_requires_proposal(branch)` (line 15): no docstring

### `backend/bff/routers/pipeline_ops_preflight.py`
- **Functions**
  - `async _run_pipeline_preflight(definition_json, db_name, branch, dataset_registry)` (line 24): no docstring
  - `_validate_pipeline_definition(definition_json, require_output)` (line 53): no docstring

### `backend/bff/routers/pipeline_ops_schema.py`
- **Functions**
  - `_normalize_schema_column_type(value)` (line 16): no docstring
  - `_coerce_schema_columns(raw)` (line 20): no docstring
  - `_detect_breaking_schema_changes(previous_schema, next_columns)` (line 38): no docstring
  - `_resolve_output_pk_columns(definition_json, node_id, output_name)` (line 59): no docstring

### `backend/bff/routers/pipeline_plans.py`

### `backend/bff/routers/pipeline_plans_compile.py`
- **Functions**
  - `async compile_plan(body, request, llm, redis_service, audit_store, dataset_registry, plan_registry)` (line 28): no docstring

### `backend/bff/routers/pipeline_plans_deps.py`
- **Functions**
  - `async get_dataset_profile_registry()` (line 8): no docstring
  - `async get_pipeline_plan_registry()` (line 14): no docstring

### `backend/bff/routers/pipeline_plans_ops.py`

### `backend/bff/routers/pipeline_plans_preview.py`
- **Functions**
  - `async preview_plan(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 28): no docstring
  - `async inspect_plan_preview(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 47): no docstring
  - `async evaluate_joins(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 66): no docstring

### `backend/bff/routers/pipeline_plans_read.py`
- **Functions**
  - `async get_plan(plan_id, request, plan_registry)` (line 14): no docstring

### `backend/bff/routers/pipeline_proposals.py`
- **Functions**
  - `async list_pipeline_proposals(db_name, branch, status_filter, pipeline_registry, request)` (line 27): no docstring
  - `async submit_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, objectify_registry, request)` (line 45): no docstring
  - `async approve_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 67): no docstring
  - `async reject_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 85): no docstring

### `backend/bff/routers/pipeline_shared.py`
- **Functions**
  - `_resolve_principal(request)` (line 22): no docstring
  - `_actor_label(principal_type, principal_id)` (line 48): no docstring
  - `async _filter_pipeline_records_for_read_access(pipeline_registry, records, request, required_role, pipeline_id_keys)` (line 54): no docstring
  - `async _ensure_pipeline_permission(pipeline_registry, pipeline_id, request, required_role)` (line 95): no docstring
  - `async _log_pipeline_audit(audit_store, request, action, status, pipeline_id, db_name, metadata, error)` (line 130): no docstring
  - `_require_idempotency_key(request)` (line 156): no docstring
  - `_require_pipeline_idempotency_key(request, operation)` (line 160): Require idempotency key for pipeline mutation operations.

### `backend/bff/routers/pipeline_simulation.py`
- **Functions**
  - `async simulate_pipeline_definition(payload, dataset_registry, request)` (line 33): no docstring

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
  - `async execute_query(db_name, query, request, mapper, terminus, dataset_registry)` (line 29): 온톨로지 쿼리 실행
  - `async execute_raw_query(db_name, query, terminus, dataset_registry)` (line 106): 원시 쿼리 실행 (제한적 접근)
  - `async query_builder_info()` (line 164): 쿼리 빌더 정보

### `backend/bff/routers/registry_deps.py`
- **Functions**
  - `async get_dataset_registry()` (line 21): no docstring
  - `async get_objectify_registry()` (line 27): no docstring
  - `async get_pipeline_registry()` (line 33): no docstring
  - `async get_agent_policy_registry()` (line 39): no docstring
  - `async get_agent_session_registry()` (line 45): no docstring

### `backend/bff/routers/role_deps.py`
- **Functions**
  - `async enforce_required_database_role(request, db_name, roles)` (line 18): no docstring
  - `require_database_role(roles)` (line 25): no docstring

### `backend/bff/routers/schema_changes.py`
- **Functions**
  - `async list_schema_changes(db_name, subject_type, subject_id, severity, since, limit, offset)` (line 39): List schema drift history for a database.
  - `async acknowledge_drift(drift_id, request)` (line 68): Acknowledge a schema drift.
  - `async list_subscriptions(request, db_name, status_filter, limit)` (line 87): List schema change subscriptions for the current user.
  - `async create_subscription(request, body)` (line 110): Create a new schema change subscription.
  - `async delete_subscription(request, subscription_id)` (line 135): Delete a schema change subscription.
  - `async check_mapping_compatibility(mapping_spec_id, db_name, dataset_version_id)` (line 154): Check if a mapping spec is compatible with the current dataset schema.
  - `async get_schema_change_stats(db_name, days)` (line 178): Get schema change statistics for a database.

### `backend/bff/routers/summary.py`
- **Functions**
  - `async get_summary(db, branch, oms, redis_service, es_service)` (line 26): Summarize context + cross-service health for UI.

### `backend/bff/routers/tasks.py`
- **Functions**
  - `async get_task_status(task_id, task_manager)` (line 31): Get current status of a background task.
  - `async list_tasks(status, task_type, limit, task_manager)` (line 45): List background tasks with optional filtering.
  - `async cancel_task(task_id, task_manager)` (line 67): Cancel a running background task.
  - `async get_task_metrics(task_manager)` (line 81): Get aggregated metrics for all background tasks.
  - `async retry_task(task_id, task_manager)` (line 94): Retry a failed task.
  - `async get_task_result(task_id, task_manager)` (line 131): Get the result of a completed task.

### `backend/bff/routers/websocket.py`
- **Functions**
  - `get_ws_manager()` (line 20): WebSocket 연결 관리자 의존성
  - `async websocket_command_updates(websocket, command_id, client_id, user_id, token, manager)` (line 26): 특정 Command의 실시간 상태 업데이트 구독
  - `async websocket_user_commands(websocket, user_id, client_id, token, manager)` (line 53): 사용자의 모든 Command 실시간 업데이트 구독

### `backend/bff/schemas/__init__.py`

### `backend/bff/schemas/actions_requests.py`
- **Classes**
  - `ActionSubmitRequest` (line 15): no docstring
  - `ActionSimulateScenarioRequest` (line 21): no docstring
  - `ActionSimulateStatePatch` (line 29): Patch-like state override for decision simulation (what-if).
    - `_reject_delete(cls, value)` (line 40): no docstring
  - `ActionSimulateObservedBaseOverrides` (line 46): Override observed_base snapshot fields/links to simulate stale reads.
  - `ActionSimulateTargetAssumption` (line 53): no docstring
  - `ActionSimulateAssumptions` (line 60): no docstring
  - `ActionSimulateRequest` (line 67): no docstring

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
  - `RunObjectifyDAGRequest` (line 48): Topologically enqueue objectify jobs based on mapping-spec relationship dependencies.
  - `DetectRelationshipsRequest` (line 62): no docstring
  - `DetectRelationshipsResponse` (line 67): no docstring
  - `TriggerIncrementalRequest` (line 72): no docstring

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
  - `_validated_db_name(db_name)` (line 29): no docstring
  - `_require_action_type_id(action_type_id)` (line 36): no docstring
  - `async _enforce_domain_model_role(http_request, db_name, enforce_role)` (line 43): no docstring
  - `_parse_uuid(value)` (line 55): no docstring
  - `_oms_metadata(request_metadata, principal_type, principal_id)` (line 62): no docstring
  - `_sanitize_action_input(value)` (line 75): no docstring
  - `async _oms_post(oms_client, path, payload)` (line 82): no docstring
  - `async _action_simulation_registry()` (line 93): no docstring
  - `_serialize_action_simulation(record)` (line 102): no docstring
  - `_serialize_action_simulation_version(record)` (line 116): no docstring
  - `async submit_action(db_name, action_type_id, body, http_request, base_branch, overlay_branch, oms_client, enforce_role)` (line 137): no docstring
  - `async simulate_action(db_name, action_type_id, body, http_request, oms_client, enforce_role)` (line 173): no docstring
  - `async get_action_log(db_name, action_log_id, http_request, action_logs, enforce_role)` (line 214): no docstring
  - `async list_action_logs(db_name, http_request, status_filter, action_type_id, submitted_by, limit, offset, action_logs, enforce_role)` (line 234): no docstring
  - `async list_action_simulations(db_name, http_request, action_type_id, limit, offset, enforce_role)` (line 261): no docstring
  - `async get_action_simulation(db_name, simulation_id, http_request, include_versions, version_limit, enforce_role)` (line 283): no docstring
  - `async list_action_simulation_versions(db_name, simulation_id, http_request, limit, offset, enforce_role)` (line 308): no docstring
  - `async get_action_simulation_version(db_name, simulation_id, version, http_request, enforce_role)` (line 334): no docstring

### `backend/bff/services/adapter_service.py`
- **Classes**
  - `BFFAdapterService` (line 30): Adapter service that bridges the user-friendly BFF layer with the core OMS layer.
    - `__init__(self, terminus_service, label_mapper)` (line 41): no docstring
    - `async create_ontology(self, db_name, ontology_data, language)` (line 45): Create ontology through OMS with label-to-ID conversion.
    - `async create_advanced_ontology(self, db_name, ontology_data, language)` (line 93): Create ontology with advanced relationship validation through OMS.
    - `async validate_relationships(self, db_name, validation_data, language)` (line 136): Validate relationships through OMS.
    - `async detect_circular_references(self, db_name, detection_data, language)` (line 165): Detect circular references through OMS.
    - `async find_relationship_paths(self, db_name, start_entity_label, target_entity_label, max_depth, path_type, language)` (line 192): Find relationship paths through OMS.
    - `async _register_label_mappings(self, db_name, ontology_dict)` (line 228): Register class and property label mappings
    - `async _convert_labels_to_ids(self, db_name, data, language)` (line 260): Convert labels in request data to internal IDs
    - `async _convert_ids_to_labels(self, db_name, data, language)` (line 281): Convert internal IDs in response data to user-friendly labels
    - `_transform_ontology_response(self, oms_result, success_message, ontology_dict)` (line 301): Transform OMS response into user-friendly OntologyResponse

### `backend/bff/services/admin_recompute_projection_service.py`
- **Functions**
  - `_normalize_dt(dt)` (line 38): no docstring
  - `_load_projection_mapping(projection)` (line 44): no docstring
  - `async _ensure_es_connected(es)` (line 52): no docstring
  - `_versioning_kwargs(seq)` (line 57): no docstring
  - `_strategy_for_projection(projection)` (line 272): no docstring
  - `async start_recompute_projection(http_request, request, background_tasks, task_manager, redis_service, audit_store, lineage_store, elasticsearch_service)` (line 280): no docstring
  - `async get_recompute_projection_result(task_id, task_manager, redis_service)` (line 327): no docstring
  - `async _log_audit_safe(audit_store, action, db_name, resource_id, metadata, occurred_at)` (line 359): no docstring
  - `async _maybe_record_lineage(lineage_store, envelope, db_name, index_name, doc_id, seq, event_ts, ontology_ref, ontology_commit)` (line 382): no docstring
  - `async _promote_alias_to_index(elasticsearch_service, base_index, new_index, allow_delete_base_index)` (line 424): no docstring
  - `async recompute_projection_task(task_id, request, elasticsearch_service, redis_service, audit_store, lineage_store, requested_by, request_ip)` (line 459): no docstring
- **Classes**
  - `IndexDecision` (line 64): no docstring
  - `ProjectionStrategy` (line 71): no docstring
    - `event_types(self)` (line 75): no docstring
    - `base_index(self, db_name, branch)` (line 78): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 81): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 84): no docstring
  - `InstancesProjectionStrategy` (line 102): no docstring
    - `base_index(self, db_name, branch)` (line 106): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 109): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 112): no docstring
  - `OntologiesProjectionStrategy` (line 187): no docstring
    - `base_index(self, db_name, branch)` (line 191): no docstring
    - `new_index(self, db_name, branch, version_suffix)` (line 194): no docstring
    - `async decide(self, envelope, data, db_name, branch, new_index, ontology_ref, ontology_commit, seq, event_ts, created_at_cache, elasticsearch_service)` (line 197): no docstring

### `backend/bff/services/admin_replay_service.py`
- **Functions**
  - `async start_replay_instance_state(request, background_tasks, storage_service, task_manager, redis_service)` (line 30): no docstring
  - `async get_replay_result(task_id, task_manager, redis_service)` (line 66): no docstring
  - `async get_replay_trace(task_id, command_id, include_audit, audit_limit, include_lineage, lineage_direction, lineage_max_depth, lineage_max_nodes, lineage_max_edges, timeline_limit, task_manager, redis_service, audit_store, lineage_store)` (line 90): no docstring
  - `async cleanup_old_replay_results(older_than_hours, redis_service)` (line 185): no docstring
  - `async replay_instance_state_task(task_id, request, storage_service, redis_service)` (line 232): no docstring
  - `async _require_completed_task(task_id, task_manager)` (line 284): no docstring
  - `async _get_replay_payload(task_id, redis_service)` (line 299): no docstring
  - `_extract_command_history(instance_state)` (line 318): no docstring
  - `_select_command(command_history, command_id)` (line 333): no docstring
  - `_resolve_target(task, instance_state)` (line 349): no docstring
  - `_build_timeline(command_history, timeline_limit, db_name)` (line 362): no docstring
  - `async _load_audit_logs(audit_store, db_name, command_id, limit)` (line 390): no docstring
  - `_parse_iso_datetime(raw)` (line 414): no docstring

### `backend/bff/services/ai_service.py`
- **Functions**
  - `_approx_token_count(payload)` (line 53): no docstring
  - `_trim_text(value, max_chars)` (line 61): no docstring
  - `_resolve_optional_principal(request)` (line 70): no docstring
  - `_ensure_session_owner(record, user_id)` (line 80): no docstring
  - `async _load_session_context(session_id, request, sessions, max_messages)` (line 88): no docstring
  - `async _record_session_message(session_id, request, sessions, role, content, metadata)` (line 146): no docstring
  - `_log_ai_event(event, payload, max_chars)` (line 198): no docstring
  - `_build_intent_prompts(question, lang, context)` (line 206): no docstring
  - `_build_intent_repair_prompts(question, lang, context, draft, issue)` (line 235): no docstring
  - `async ai_intent(body, request, llm, redis_service, audit_store, sessions, dataset_registry)` (line 264): no docstring
  - `_now_iso()` (line 515): no docstring
  - `_cap_int(value, lo, hi)` (line 519): no docstring
  - `async _load_schema_context(db_name, oms, redis_service, cache_ttl_s, max_classes, max_properties_per_class, max_relationships_per_class)` (line 523): Build a minimal, LLM-friendly schema context.
  - `_build_plan_prompts(question, schema_context, mode, branch, limit_cap, conversation_context, dataset_inventory)` (line 620): no docstring
  - `_build_plan_repair_prompts(question, schema_context, mode, branch, limit_cap, conversation_context, dataset_inventory, prior_plan, issue)` (line 718): no docstring
  - `_build_answer_prompts(question, grounding)` (line 819): no docstring
  - `_validate_and_cap_plan(plan, limit_cap)` (line 855): Enforce server-side caps regardless of what the LLM produced.
  - `async _execute_label_query(db_name, query_dict, lang, mapper, terminus)` (line 892): Execute label query by reusing the same deterministic pipeline as /database/{db_name}/query.
  - `_ground_label_query_result(execution, max_rows)` (line 917): no docstring
  - `_ground_graph_query_result(execution, max_nodes, max_edges)` (line 929): no docstring
  - `_dataset_item_min(item)` (line 1000): no docstring
  - `_ground_dataset_list_result(datasets, total_count, max_items)` (line 1020): no docstring
  - `_build_dataset_inventory(datasets, max_items)` (line 1029): no docstring
  - `async translate_query_plan(db_name, body, request, llm, redis_service, audit_store, oms, sessions, dataset_registry)` (line 1041): Natural language → constrained query plan JSON.
  - `async ai_query(db_name, body, request, llm, redis_service, audit_store, lineage_store, oms, mapper, terminus, sessions, dataset_registry)` (line 1178): End-to-end natural language query:

### `backend/bff/services/context7_service.py`
- **Functions**
  - `async _call_context7(action, func)` (line 23): no docstring
  - `async search_context7(request, client)` (line 33): no docstring
  - `async get_entity_context(entity_id, client)` (line 41): no docstring
  - `async add_knowledge(request, client)` (line 46): no docstring
  - `async create_entity_link(request, client)` (line 59): no docstring
  - `async analyze_ontology(request, client, oms_client)` (line 76): no docstring
  - `async get_ontology_suggestions(db_name, class_id, client)` (line 104): no docstring
  - `async check_context7_health(client)` (line 115): no docstring

### `backend/bff/services/data_connector_pipelining_service.py`
- **Functions**
  - `async start_pipelining_google_sheet(sheet_id, payload, http_request, google_sheets_service, connector_registry, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 34): no docstring

### `backend/bff/services/data_connector_registration_service.py`
- **Functions**
  - `_as_int_or_none(value)` (line 27): no docstring
  - `async _resolve_tokens(connector_registry, connection_id)` (line 36): no docstring
  - `async register_google_sheet(sheet_data, google_sheets_service, connector_registry, dataset_registry, lineage_store)` (line 56): no docstring
  - `async preview_google_sheet(sheet_id, worksheet_name, limit, google_sheets_service, connector_registry)` (line 212): no docstring
  - `async list_registered_sheets(database_name, connector_registry)` (line 266): no docstring
  - `async unregister_google_sheet(sheet_id, connector_registry)` (line 323): no docstring

### `backend/bff/services/database_service.py`
- **Functions**
  - `_is_dev_mode()` (line 40): no docstring
  - `async _get_expected_seq_for_database(db_name)` (line 44): no docstring
  - `_coerce_db_entry(entry)` (line 69): no docstring
  - `_enrich_db_entry(entry, actor_type, actor_id, actor_name, access_rows)` (line 83): no docstring
  - `async list_databases(request, oms, dataset_registry)` (line 129): 데이터베이스 목록 조회
  - `async create_database(body, http_request, oms)` (line 183): 데이터베이스 생성
  - `async delete_database(db_name, http_request, expected_seq, oms)` (line 273): 데이터베이스 삭제
  - `async get_branch_info(db_name, branch_name, oms)` (line 357): 브랜치 정보 조회 (프론트엔드용 BFF 래핑)
  - `async delete_branch(db_name, branch_name, force, oms)` (line 379): 브랜치 삭제 (프론트엔드용 BFF 래핑)
  - `async get_database(db_name, oms)` (line 412): 데이터베이스 정보 조회
  - `async get_database_expected_seq(db_name)` (line 433): Resolve the current `expected_seq` for database (aggregate) operations.
  - `async list_classes(db_name, type, limit, oms)` (line 456): 데이터베이스의 클래스 목록 조회
  - `async create_class(db_name, class_data, oms)` (line 505): 데이터베이스에 새 클래스 생성
  - `async get_class(db_name, class_id, oms)` (line 551): 특정 클래스 조회
  - `async list_branches(db_name, oms)` (line 606): 브랜치 목록 조회
  - `async create_branch(db_name, branch_data, oms)` (line 634): 새 브랜치 생성
  - `async get_versions(db_name, oms)` (line 675): 버전 히스토리 조회

### `backend/bff/services/dataset_ingest_failures.py`
- **Functions**
  - `async mark_ingest_failed(dataset_registry, ingest_request, error, stage)` (line 11): no docstring

### `backend/bff/services/dataset_ingest_outbox_builder.py`
- **Classes**
  - `DatasetIngestOutboxBuilder` (line 18): no docstring
    - `dataset_created(self, dataset_id, db_name, name, actor, transaction_id, extra_data)` (line 22): no docstring
    - `version_created(self, event_id, dataset_id, db_name, name, actor, command_type, lakefs_commit_id, artifact_key, transaction_id, extra_data)` (line 48): no docstring
    - `artifact_stored_lineage(self, version_event_id, artifact_key, db_name, from_label, edge_metadata, to_label)` (line 85): no docstring

### `backend/bff/services/dataset_ingest_outbox_flusher.py`
- **Functions**
  - `_dataset_ingest_outbox_worker_enabled()` (line 11): no docstring
  - `async maybe_flush_dataset_ingest_outbox_inline(dataset_registry, lineage_store, flush_dataset_ingest_outbox, batch_size)` (line 15): Best-effort inline flush of the dataset ingest outbox.

### `backend/bff/services/funnel_client.py`
- **Classes**
  - `FunnelClient` (line 18): Funnel HTTP 클라이언트
    - `__init__(self, base_url)` (line 21): no docstring
    - `_resolve_excel_timeout_seconds()` (line 38): no docstring
    - `async close(self)` (line 41): 클라이언트 연결 종료
    - `async check_health(self)` (line 45): Funnel 서비스 상태 확인
    - `async analyze_dataset(self, request_data, timeout_seconds)` (line 62): 데이터셋 타입 분석
    - `async suggest_schema(self, analysis_results, class_name)` (line 89): 분석 결과를 기반으로 OMS 스키마 제안
    - `async preview_google_sheets(self, sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types)` (line 116): Google Sheets 데이터 미리보기와 타입 추론
    - `async analyze_and_suggest_schema(self, data, columns, class_name, sample_size, include_complex_types)` (line 159): 데이터 분석과 스키마 제안을 한 번에 실행
    - `async google_sheets_to_schema(self, sheet_url, worksheet_name, class_name, api_key, connection_id, table_id, table_bbox)` (line 199): Google Sheets에서 직접 스키마 생성
    - `async google_sheets_to_structure_preview(self, sheet_url, worksheet_name, api_key, connection_id, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 291): Google Sheets URL → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_google_sheets_structure(self, sheet_url, worksheet_name, api_key, connection_id, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 335): Analyze sheet structure via Funnel (Google Sheets URL → grid/merged_cells → structure analysis).
    - `async excel_to_structure_preview(self, xlsx_bytes, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 371): Excel bytes → (grid/merged_cells) → structure analysis → selected table preview.
    - `async excel_to_structure_preview_stream(self, fileobj, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 412): Excel stream → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_excel_structure(self, xlsx_bytes, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 449): Analyze sheet structure via Funnel (Excel bytes → grid/merged_cells → structure analysis).
    - `async analyze_excel_structure_stream(self, fileobj, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 494): Analyze sheet structure via Funnel (streaming Excel upload).
    - `_select_primary_table(structure)` (line 576): Choose a single "primary" table for schema suggestion.
    - `_select_requested_table(cls, structure, table_id, table_bbox)` (line 609): Select a table from structure analysis output.
    - `_normalize_bbox_dict(bbox)` (line 657): no docstring
    - `_structure_table_to_preview(structure, table, sheet_url, worksheet_name)` (line 667): no docstring
    - `_structure_table_to_excel_preview(structure, table, file_name, sheet_name)` (line 719): no docstring
    - `async excel_to_schema(self, xlsx_bytes, filename, sheet_name, class_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 764): Excel 업로드에서 직접 스키마 생성 (구조 분석 기반).
    - `async __aenter__(self)` (line 823): no docstring
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 826): no docstring

### `backend/bff/services/funnel_type_inference_adapter.py`
- **Classes**
  - `FunnelHTTPTypeInferenceAdapter` (line 18): HTTP 기반 Funnel 마이크로서비스를 TypeInferenceInterface로 adapting하는 클래스.
    - `__init__(self, funnel_client)` (line 26): no docstring
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 29): 단일 컬럼 데이터의 타입을 추론합니다.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 75): 전체 데이터셋을 분석하여 모든 컬럼의 타입을 추론합니다.
    - `async infer_type_with_confidence(self, values, check_complex)` (line 92): 값 리스트에서 타입을 추론하고 신뢰도를 반환합니다.
    - `async infer_single_value_type(self, value, context)` (line 104): 단일 값의 타입을 추론합니다.
    - `async _analyze_single_column(self, data, headers, include_complex_types)` (line 125): 단일 컬럼 분석을 위한 비동기 헬퍼 메서드
    - `async _analyze_dataset_async(self, data, headers, include_complex_types, sample_size)` (line 163): 데이터셋 분석을 위한 비동기 헬퍼 메서드
    - `_convert_funnel_column_result(self, funnel_result)` (line 205): Funnel 서비스 응답을 Interface 형식으로 변환
    - `async close(self)` (line 221): 클라이언트 연결 종료

### `backend/bff/services/governance_service.py`
- **Functions**
  - `_enforce_db_scope_or_403(request, db_name)` (line 37): no docstring
  - `async _require_db_role(request, db_name, roles)` (line 44): no docstring
  - `async create_backing_datasource(body, request, dataset_registry)` (line 48): no docstring
  - `async list_backing_datasources(request, dataset_id, db_name, branch, dataset_registry)` (line 90): no docstring
  - `async get_backing_datasource(backing_id, request, dataset_registry)` (line 121): no docstring
  - `async create_backing_datasource_version(backing_id, body, request, dataset_registry)` (line 135): no docstring
  - `async list_backing_datasource_versions(backing_id, request, dataset_registry)` (line 168): no docstring
  - `async get_backing_datasource_version(version_id, request, dataset_registry)` (line 186): no docstring
  - `async create_key_spec(body, request, dataset_registry)` (line 205): no docstring
  - `async list_key_specs(request, dataset_id, dataset_registry)` (line 258): no docstring
  - `async get_key_spec(key_spec_id, request, dataset_registry)` (line 274): no docstring
  - `async list_schema_migration_plans(request, db_name, subject_type, subject_id, status_value, dataset_registry)` (line 290): no docstring
  - `async upsert_gate_policy(body, dataset_registry)` (line 314): no docstring
  - `async list_gate_policies(scope, dataset_registry)` (line 334): no docstring
  - `async list_gate_results(scope, subject_type, subject_id, dataset_registry)` (line 343): no docstring
  - `async upsert_access_policy(body, request, dataset_registry)` (line 354): no docstring
  - `async list_access_policies(request, db_name, scope, subject_type, subject_id, policy_status, dataset_registry)` (line 394): no docstring
  - `async handle_request_errors(fn, *args, **kwargs)` (line 418): Helper to wrap governance service calls with consistent error mapping.

### `backend/bff/services/graph_federation_provider.py`
- **Functions**
  - `_build_graph_federation_service(settings)` (line 20): no docstring
  - `async _get_from_container(container)` (line 42): no docstring
  - `async get_graph_federation_service()` (line 49): FastAPI dependency to get a GraphFederationServiceWOQL instance.

### `backend/bff/services/graph_query_service.py`
- **Functions**
  - `_resolve_graph_branches(db_name, base_branch, overlay_branch, branch, include_documents, classes_in_query)` (line 45): no docstring
  - `_raise_overlay_degraded(ctx)` (line 96): no docstring
  - `async _load_access_policies(dataset_registry, db_name, class_ids)` (line 113): no docstring
  - `_apply_access_policies_to_nodes(nodes, policies)` (line 132): no docstring
  - `_apply_access_policies_to_documents(documents, policy)` (line 163): no docstring
  - `_collect_class_ids_from_hops(hops)` (line 187): no docstring
  - `async execute_graph_query(db_name, query, request, lineage_store, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 200): Execute multi-hop graph query with ES federation (TerminusDB + Elasticsearch).
  - `async execute_simple_graph_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 462): Execute simple single-class graph query.
  - `async execute_multi_hop_query(db_name, query, request, graph_service, dataset_registry, base_branch, overlay_branch, branch)` (line 547): Execute multi-hop graph query (legacy dict payload).
  - `async find_relationship_paths(db_name, source_class, target_class, max_depth, graph_service, branch)` (line 655): no docstring
  - `async graph_service_health(graph_service)` (line 695): Check health of graph federation service.
- **Classes**
  - `GraphBranchContext` (line 34): no docstring

### `backend/bff/services/http_idempotency.py`
- **Functions**
  - `require_idempotency_key(request)` (line 13): no docstring
  - `get_idempotency_key(request)` (line 22): no docstring

### `backend/bff/services/instance_async_service.py`
- **Functions**
  - `_metadata_dict(value)` (line 33): no docstring
  - `_fallback_langs(primary)` (line 39): no docstring
  - `async resolve_class_id(db_name, class_label, label_mapper, lang)` (line 49): no docstring
  - `async convert_labels_to_ids(data, db_name, class_id, label_mapper, lang)` (line 63): Label 기반 데이터를 ID 기반으로 변환.
  - `async _handle_command_errors(fn, *args, op_message, **kwargs)` (line 113): no docstring
  - `async create_instance_async(db_name, class_label, data, metadata, http_request, branch, oms_client, label_mapper, user_id)` (line 130): no docstring
  - `async update_instance_async(db_name, class_label, instance_id, data, metadata, http_request, expected_seq, branch, oms_client, label_mapper, user_id)` (line 182): no docstring
  - `async delete_instance_async(db_name, class_label, instance_id, http_request, branch, expected_seq, oms_client, label_mapper, user_id)` (line 237): no docstring
  - `async bulk_create_instances_async(db_name, class_label, instances, metadata, http_request, branch, oms_client, label_mapper, user_id)` (line 272): no docstring

### `backend/bff/services/instances_service.py`
- **Functions**
  - `_is_action_log_class_id(class_id)` (line 48): no docstring
  - `_action_log_as_instance(record)` (line 52): no docstring
  - `async _apply_access_policy_to_instances(dataset_registry, db_name, class_id, instances)` (line 56): no docstring
  - `_normalize_es_search_result(result)` (line 77): Normalize Elasticsearch search results across return shapes.
  - `_resolve_overlay_context(db_name, class_id, overlay_branch)` (line 119): no docstring
  - `_sanitize_search_query(search)` (line 143): no docstring
  - `_overlay_key_for_doc(doc)` (line 164): no docstring
  - `_merge_overlay_instances(base_instances, overlay_instances)` (line 182): no docstring
  - `async list_class_instances(db_name, class_id, request_headers, base_branch, overlay_branch, branch, limit, offset, search, status_filter, action_type_id, submitted_by, elasticsearch_service, oms_client, dataset_registry, action_logs)` (line 210): no docstring
  - `async get_class_sample_values(db_name, class_id, property_name, limit, oms_client, dataset_registry)` (line 511): no docstring
  - `async _server_merge_fallback(db_name, class_id, instance_id, resolved_base_branch, resolved_overlay_branch, writeback_enabled, dataset_registry)` (line 587): no docstring
  - `async get_instance_detail(db_name, class_id, instance_id, request_headers, base_branch, overlay_branch, branch, elasticsearch_service, oms_client, dataset_registry, action_logs)` (line 653): no docstring
- **Classes**
  - `OverlayContext` (line 42): no docstring

### `backend/bff/services/label_mapping_service.py`
- **Functions**
  - `_validation_details_template()` (line 59): no docstring
  - `async export_mappings(db_name, mapper)` (line 63): 레이블 매핑 내보내기
  - `async _validate_file_upload(file)` (line 83): Validate uploaded file size, type, and extension.
  - `async _read_and_parse_file(file)` (line 104): Read file content and parse JSON.
  - `_sanitize_and_validate_schema(raw_mappings, db_name)` (line 151): Sanitize input and validate schema.
  - `_validate_business_logic(mapping_request, sanitized_mappings, db_name)` (line 181): Validate business logic and data consistency.
  - `async _load_ontology_ids(oms_client, db_name)` (line 218): no docstring
  - `async _load_existing_label_map(mapper, db_name)` (line 225): no docstring
  - `_compute_validation_details(mapping_request, existing_class_ids, existing_property_ids, existing_class_labels, existing_prop_labels)` (line 232): no docstring
  - `_validation_passed(details)` (line 295): no docstring
  - `async _perform_mapping_import(mapper, validated_mappings, db_name)` (line 299): no docstring
  - `async import_mappings(db_name, file, mapper, oms_client)` (line 532): Import label mappings from JSON file with enhanced security validation.
  - `async validate_mappings(db_name, file, mapper, oms_client)` (line 545): Validate mappings without importing.
  - `async get_mappings_summary(db_name, mapper)` (line 558): 레이블 매핑 요약 조회
  - `async clear_mappings(db_name, mapper)` (line 602): 레이블 매핑 초기화
- **Classes**
  - `MappingImportPayload` (line 35): Label mapping bundle file schema.
  - `MappingBundleContext` (line 51): no docstring
  - `_MappingBundleProcessor` (line 323): Template Method for mapping bundle file processing.
    - `async process(self, db_name, file, mapper, oms_client)` (line 326): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 348): no docstring
  - `_ImportProcessor` (line 351): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 352): no docstring
  - `_ValidateProcessor` (line 425): no docstring
    - `async _handle(self, ctx, mapper, oms_client)` (line 426): no docstring

### `backend/bff/services/link_types_mapping_service.py`
- **Functions**
  - `extract_schema_columns(schema)` (line 36): no docstring
  - `extract_schema_types(schema)` (line 40): no docstring
  - `compute_schema_hash(schema)` (line 51): no docstring
  - `build_join_schema(source_key_column, target_key_column, source_key_type, target_key_type)` (line 58): no docstring
  - `extract_ontology_properties(payload)` (line 73): no docstring
  - `extract_ontology_relationships(payload)` (line 87): no docstring
  - `normalize_spec_type(value)` (line 101): no docstring
  - `normalize_policy(value, default)` (line 105): no docstring
  - `normalize_pk_fields(value)` (line 112): no docstring
  - `async resolve_object_type_contract(oms_client, db_name, class_id, branch)` (line 122): no docstring
  - `async resolve_dataset_and_version(dataset_registry, dataset_id, dataset_version_id)` (line 142): no docstring
  - `async ensure_join_dataset(dataset_registry, request, db_name, join_dataset_id, join_dataset_version_id, join_dataset_name, join_dataset_branch, auto_create, default_name, source_key_column, target_key_column, source_key_type, target_key_type)` (line 166): no docstring
  - `resolve_property_type(prop_map, field)` (line 250): no docstring
  - `_extract_pk_fields(contract, props)` (line 258): no docstring
  - `async build_mapping_request(db_name, request, oms_client, dataset_registry, relationship_spec_id, link_type_id, source_class, target_class, predicate, cardinality, branch, source_props, target_props, source_contract, target_contract, spec_payload)` (line 595): no docstring
- **Classes**
  - `_MappingContext` (line 264): no docstring
  - `_MappingResult` (line 283): no docstring
  - `_ForeignKeyMappingStrategy` (line 290): no docstring
    - `async build(self, ctx, fk_spec, source_pk_fields, target_pk_fields)` (line 291): no docstring
  - `_JoinTableMappingStrategy` (line 416): no docstring
    - `async build(self, ctx, join_spec, source_pk_fields, target_pk_fields, spec_type, relationship_kind, relationship_object_type)` (line 417): no docstring
  - `_ObjectBackedMappingStrategy` (line 534): no docstring
    - `__init__(self, join_strategy)` (line 535): no docstring
    - `async build(self, ctx, object_backed_spec, source_pk_fields, target_pk_fields)` (line 538): no docstring

### `backend/bff/services/link_types_write_service.py`
- **Functions**
  - `_validated_db_name(db_name)` (line 40): no docstring
  - `_validated_branch(branch)` (line 47): no docstring
  - `_sanitize_payload(payload)` (line 54): no docstring
  - `async _require_role(request, db_name, roles, enforce_role)` (line 61): no docstring
  - `_require_non_empty(value, detail)` (line 71): no docstring
  - `_extract_mapping_payload(mapping_response)` (line 78): no docstring
  - `async _enqueue_link_index_job(enqueue_job, dataset_registry, objectify_registry, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id)` (line 88): no docstring
  - `async create_link_type(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry, enforce_role, create_mapping_spec, enqueue_job)` (line 108): no docstring
  - `async update_link_type(db_name, link_type_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry, enforce_role, create_mapping_spec, enqueue_job)` (line 302): no docstring
  - `async reindex_link_type(db_name, link_type_id, request, dataset_version_id, dataset_registry, objectify_registry, enforce_role, enqueue_job)` (line 513): no docstring

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
  - `async _await_if_needed(value)` (line 27): no docstring
  - `async _raise_for_status(response)` (line 33): no docstring
  - `async _response_json(response)` (line 37): no docstring
  - `_validate_inputs(db_name, source_branch, target_branch)` (line 41): no docstring
  - `async simulate_merge(db_name, request, oms_client)` (line 48): Simulate a merge (no write) and return UI-friendly conflict format.
  - `async resolve_merge_conflicts(db_name, request, oms_client)` (line 167): Resolve merge conflicts using user-provided resolutions, then perform merge.
  - `async _detect_merge_conflicts(source_changes, target_changes, common_ancestor, db_name, oms_client)` (line 231): no docstring
  - `async _convert_resolution_to_terminus_format(resolution)` (line 305): no docstring

### `backend/bff/services/object_type_contract_service.py`
- **Functions**
  - `_extract_resource_payload(response)` (line 33): no docstring
  - `_schema_hash_from_version(sample_json, schema_json)` (line 42): no docstring
  - `async _maybe_get_head_commit_id(oms_client, db_name, branch)` (line 54): no docstring
  - `async _resolve_expected_head_commit(oms_client, db_name, branch, expected_head_commit)` (line 70): no docstring
  - `async _resolve_backing(db_name, request, dataset_registry, backing_dataset_id, backing_datasource_id, backing_datasource_version_id, dataset_version_id, schema_hash)` (line 90): no docstring
  - `_extract_ontology_property_names(payload)` (line 181): no docstring
  - `_normalize_and_validate_pk_spec(raw_pk_spec, ontology_property_names)` (line 197): no docstring
  - `async create_object_type_contract(db_name, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 212): no docstring
  - `async get_object_type_contract(db_name, class_id, request, branch, oms_client, dataset_registry, objectify_registry)` (line 393): no docstring
  - `_normalize_field_list(raw_value)` (line 470): no docstring
  - `_normalize_field_moves(raw_value)` (line 480): no docstring
  - `async update_object_type_contract(db_name, class_id, body, request, branch, expected_head_commit, oms_client, dataset_registry, objectify_registry)` (line 497): no docstring

### `backend/bff/services/objectify_dag_service.py`
- **Functions**
  - `async run_objectify_dag(db_name, body, request, dataset_registry, objectify_registry, job_queue, oms_client)` (line 584): Enterprise helper: enqueue multiple objectify jobs in dependency order.
- **Classes**
  - `_DagPlanItem` (line 38): no docstring
    - `to_dict(self)` (line 46): no docstring
  - `_ObjectifyDagOrchestrator` (line 57): no docstring
    - `__init__(self, db_name, body, dataset_registry, objectify_registry, job_queue, oms_client)` (line 58): no docstring
    - `async _fetch_object_type_contract(self, class_id)` (line 88): no docstring
    - `async _resolve_mapping_spec_for_object_type(self, class_id, backing_source)` (line 107): no docstring
    - `async _fetch_relationship_targets(self, class_id)` (line 147): no docstring
    - `async _load_class_info(self, class_id)` (line 165): no docstring
    - `async _build_dependency_closure(self, start)` (line 209): no docstring
    - `_toposort(self, start)` (line 260): no docstring
    - `_build_plan(self, ordered)` (line 314): no docstring
    - `async compute_plan(self, class_ids)` (line 331): no docstring
    - `async _enqueue_class_job(self, class_id, run_id, start)` (line 339): no docstring
    - `_extract_command_status(payload)` (line 401): no docstring
    - `async _wait_for_objectify_submitted(self, job_id, timeout_seconds)` (line 410): no docstring
    - `async _wait_for_command_terminal(self, command_id, timeout_seconds)` (line 443): no docstring
    - `async _wait_for_class_ready(self, class_id)` (line 469): no docstring
    - `async _run_ready_orchestrator(self, ordered, initial_classes, start)` (line 477): no docstring
    - `async enqueue_plan(self, ordered, start)` (line 524): no docstring

### `backend/bff/services/objectify_mapping_spec_service.py`
- **Functions**
  - `async create_mapping_spec(body, request, dataset_registry, objectify_registry, oms_client)` (line 41): Create an objectify mapping spec with full validation.

### `backend/bff/services/objectify_ops_service.py`
- **Functions**
  - `_match_output_name(output, name)` (line 31): no docstring
  - `_compute_schema_hash_from_sample(sample_json)` (line 35): no docstring
  - `_extract_schema_columns(schema)` (line 39): no docstring
  - `_extract_schema_types(schema)` (line 49): no docstring
  - `_normalize_mapping_pair(item)` (line 59): no docstring
  - `_build_mapping_change_summary(previous_mappings, new_mappings)` (line 69): no docstring
  - `_extract_ontology_fields(payload)` (line 122): no docstring
  - `_resolve_import_type(raw_type)` (line 126): no docstring
  - `_unwrap_data_payload(payload)` (line 130): no docstring

### `backend/bff/services/objectify_run_service.py`
- **Functions**
  - `async run_objectify(dataset_id, body, request, dataset_registry, objectify_registry, job_queue, pipeline_registry)` (line 33): no docstring

### `backend/bff/services/oms_client.py`
- **Classes**
  - `OntologyRef` (line 30): Minimal ontology reference used by BFF validation flows.
  - `OMSClient` (line 37): OMS HTTP 클라이언트
    - `__init__(self, base_url)` (line 40): no docstring
    - `_get_auth_token()` (line 65): no docstring
    - `async close(self)` (line 69): 클라이언트 연결 종료
    - `async get(self, path, **kwargs)` (line 77): Low-level GET helper (returns JSON dict).
    - `async post(self, path, **kwargs)` (line 85): Low-level POST helper (returns JSON dict).
    - `async put(self, path, **kwargs)` (line 93): Low-level PUT helper (returns JSON dict).
    - `async delete(self, path, **kwargs)` (line 101): Low-level DELETE helper (returns JSON dict when available).
    - `async check_health(self)` (line 109): OMS 서비스 상태 확인
    - `async list_databases(self)` (line 121): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 131): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 154): 데이터베이스 삭제
    - `async get_database(self, db_name)` (line 167): 데이터베이스 정보 조회
    - `async create_ontology(self, db_name, ontology_data, branch, headers)` (line 177): 온톨로지 생성
    - `async validate_ontology_create(self, db_name, ontology_data, branch)` (line 205): 온톨로지 생성 검증 (no write).
    - `async get_ontology(self, db_name, class_id, branch)` (line 225): 온톨로지 조회
    - `async list_ontologies(self, db_name, branch)` (line 238): 온톨로지 목록 조회
    - `async get_ontologies(self, db_name, branch)` (line 251): Compatibility helper for BFF callers that need a flattened view of:
    - `async list_branches(self, db_name)` (line 296): 브랜치 목록 조회
    - `async list_ontology_resources(self, db_name, resource_type, branch, limit, offset)` (line 306): 온톨로지 리소스 목록 조회
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 334): 단일 온톨로지 리소스 조회
    - `async create_ontology_resource(self, db_name, resource_type, payload, branch, expected_head_commit)` (line 354): 온톨로지 리소스 생성
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 379): 온톨로지 리소스 업데이트
    - `async delete_ontology_resource(self, db_name, resource_type, resource_id, branch, expected_head_commit)` (line 405): 온톨로지 리소스 삭제
    - `async list_ontology_branches(self, db_name)` (line 429): 온톨로지 브랜치 목록 조회
    - `async create_ontology_branch(self, db_name, payload)` (line 439): 온톨로지 브랜치 생성
    - `async list_ontology_proposals(self, db_name, status_filter, limit)` (line 452): 온톨로지 제안 목록 조회
    - `async create_ontology_proposal(self, db_name, payload)` (line 470): 온톨로지 제안 생성
    - `async approve_ontology_proposal(self, db_name, proposal_id, payload)` (line 483): 온톨로지 제안 승인
    - `async deploy_ontology(self, db_name, payload)` (line 498): 온톨로지 배포(승격)
    - `async get_ontology_health(self, db_name, branch)` (line 511): 온톨로지 헬스 체크
    - `async create_branch(self, db_name, branch_data)` (line 524): 브랜치 생성
    - `async get_version_history(self, db_name)` (line 534): 버전 히스토리 조회
    - `async get_version_head(self, db_name, branch)` (line 544): 브랜치 head 커밋 ID 조회 (deploy gate).
    - `async update_ontology(self, db_name, class_id, update_data, expected_seq, branch, headers)` (line 557): 온톨로지 업데이트
    - `async validate_ontology_update(self, db_name, class_id, update_data, branch)` (line 581): 온톨로지 업데이트 검증 (no write).
    - `async delete_ontology(self, db_name, class_id, expected_seq, branch, headers)` (line 602): 온톨로지 삭제
    - `async query_ontologies(self, db_name, query)` (line 629): 온톨로지 쿼리
    - `async database_exists(self, db_name)` (line 639): 데이터베이스 존재 여부 확인
    - `async commit_database_change(self, db_name, message, author)` (line 650): 데이터베이스 변경사항 자동 커밋
    - `async commit_system_change(self, message, author, operation, target)` (line 677): 시스템 레벨 변경사항 커밋 (데이터베이스 생성/삭제 등)
    - `async get_class_metadata(self, db_name, class_id)` (line 710): 클래스의 메타데이터 가져오기
    - `async update_class_metadata(self, db_name, class_id, metadata)` (line 733): 클래스의 메타데이터 업데이트
    - `async get_class_instances(self, db_name, class_id, limit, offset, search)` (line 776): 특정 클래스의 인스턴스 목록을 효율적으로 조회 (N+1 Query 최적화)
    - `async get_instance(self, db_name, instance_id, class_id)` (line 815): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id)` (line 847): 특정 클래스의 인스턴스 개수 조회
    - `async execute_sparql(self, db_name, query, limit, offset)` (line 872): SPARQL 쿼리 실행
    - `async __aenter__(self)` (line 910): no docstring
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 913): no docstring

### `backend/bff/services/ontology_crud_service.py`
- **Functions**
  - `_as_string(value, lang, fallback)` (line 37): no docstring
  - `async _resolve_class_id(db_name, class_label, lang, mapper)` (line 50): no docstring
  - `async create_ontology(db_name, body, branch, mapper, oms_client)` (line 55): 온톨로지 생성
  - `async list_ontologies(db_name, request, branch, class_type, limit, offset, mapper, terminus)` (line 126): 온톨로지 목록 조회
  - `async get_ontology(db_name, class_label, request, branch, mapper, terminus)` (line 196): 온톨로지 조회
  - `async validate_ontology_create(db_name, body, branch, oms_client)` (line 262): 온톨로지 생성 검증 (no write) - OMS proxy.
  - `async validate_ontology_update(db_name, class_label, body, request, branch, mapper, oms_client)` (line 289): 온톨로지 업데이트 검증 (no write) - OMS proxy.
  - `async update_ontology(db_name, class_label, body, request, expected_seq, branch, mapper, terminus)` (line 323): 온톨로지 수정
  - `async delete_ontology(db_name, class_label, request, expected_seq, branch, mapper, terminus)` (line 387): 온톨로지 삭제
  - `async get_ontology_schema(db_name, class_id, request, format, branch, mapper, terminus, jsonld_conv)` (line 446): 온톨로지 스키마 조회

### `backend/bff/services/ontology_extensions_service.py`
- **Functions**
  - `async _call_oms(action, func)` (line 33): no docstring
  - `async list_resources(oms_client, db_name, resource_type, branch, limit, offset)` (line 47): no docstring
  - `async create_resource(oms_client, db_name, resource_type, payload, branch, expected_head_commit)` (line 68): no docstring
  - `async update_resource(oms_client, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 89): no docstring
  - `async get_resource(oms_client, db_name, resource_type, resource_id, branch)` (line 112): no docstring
  - `async delete_resource(oms_client, db_name, resource_type, resource_id, branch, expected_head_commit)` (line 131): no docstring
  - `async list_ontology_branches(oms_client, db_name)` (line 152): no docstring
  - `async create_ontology_branch(oms_client, db_name, request)` (line 159): no docstring
  - `async list_ontology_proposals(oms_client, db_name, status_filter, limit)` (line 169): no docstring
  - `async create_ontology_proposal(oms_client, db_name, request)` (line 186): no docstring
  - `async approve_ontology_proposal(oms_client, db_name, proposal_id, request)` (line 201): no docstring
  - `async deploy_ontology(oms_client, db_name, request)` (line 218): no docstring
  - `async ontology_health(oms_client, db_name, branch)` (line 233): no docstring

### `backend/bff/services/ontology_imports_service.py`
- **Functions**
  - `async dry_run_import_from_google_sheets(db_name, body)` (line 374): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_google_sheets(db_name, body, oms_client)` (line 425): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
  - `async dry_run_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, dry_run_rows, max_import_rows, options_json)` (line 485): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, allow_partial, max_import_rows, batch_size, return_instances, max_return_instances, options_json, oms_client)` (line 580): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
- **Classes**
  - `_ImportSourceStrategy` (line 27): no docstring
    - `async fetch_structure_preview(self, options)` (line 30): no docstring
    - `dry_run_source_info(self, table)` (line 32): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 34): no docstring
  - `_GoogleSheetsSource` (line 38): no docstring
    - `async fetch_structure_preview(self, options)` (line 52): no docstring
    - `dry_run_source_info(self, table)` (line 71): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 80): no docstring
  - `_ExcelSource` (line 93): no docstring
    - `async fetch_structure_preview(self, options)` (line 105): no docstring
    - `dry_run_source_info(self, table)` (line 122): no docstring
    - `commit_base_metadata(self, table, mappings)` (line 131): no docstring
  - `_PreparedImport` (line 144): no docstring
  - `_ImportProcessorTemplate` (line 151): no docstring
    - `__init__(self, source)` (line 152): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 155): no docstring
    - `async _prepare(self, target_schema, mappings, max_import_rows, options)` (line 158): no docstring
  - `_DryRunImportProcessor` (line 196): no docstring
    - `__init__(self, source, dry_run_rows)` (line 197): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 201): no docstring
    - `async run(self, target_class_id, target_schema, mappings, max_import_rows, options)` (line 208): no docstring
  - `_CommitImportProcessor` (line 245): no docstring
    - `_compute_sample_row_limit(self, max_import_rows)` (line 246): no docstring
    - `async run(self, db_name, target_class_id, target_schema, mappings, max_import_rows, options, allow_partial, batch_size, return_instances, max_return_instances, oms_client)` (line 251): no docstring

### `backend/bff/services/ontology_label_mapper_service.py`
- **Functions**
  - `async map_relationship_targets(mapper, db_name, ontology_dict, lang)` (line 14): Convert relationship targets from labels → class ids when mappings exist.
  - `async register_ontology_label_mappings(mapper, db_name, class_id, ontology_dict)` (line 36): Register class/property/relationship label mappings for a created ontology.

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
  - `_validated_db_name(db_name)` (line 37): no docstring
  - `_validated_branch(branch)` (line 44): no docstring
  - `_sanitize_payload(payload)` (line 51): no docstring
  - `_resolve_or_generate_class_id(payload)` (line 58): no docstring
  - `async create_ontology_with_relationship_validation(db_name, ontology, request, branch, auto_generate_inverse, validate_relationships, check_circular_references, mapper, terminus)` (line 71): Create ontology with advanced relationship validation (BFF).
  - `async validate_ontology_relationships(db_name, ontology, request, branch, mapper, terminus)` (line 152): Validate ontology relationships (no write).
  - `async check_circular_references(db_name, request, ontology, branch, mapper, terminus)` (line 189): Detect circular references in ontology graph (no write).
  - `async analyze_relationship_network(db_name, request, terminus)` (line 224): Analyze ontology relationship network and return user-friendly metrics.
  - `async find_relationship_paths(request, db_name, start_entity, end_entity, max_depth, path_type, terminus, mapper)` (line 315): Find relationship paths between ontology entities.

### `backend/bff/services/ontology_suggestions_service.py`
- **Functions**
  - `_load_mapping_config()` (line 37): no docstring
  - `_apply_semantic_hints(config, enabled)` (line 50): no docstring
  - `_format_mapping_suggestion(suggestion, source_schema, target_schema)` (line 58): no docstring
  - `async suggest_schema_from_data(db_name, body)` (line 87): 🔥 데이터에서 스키마 자동 제안
  - `async suggest_mappings(db_name, body)` (line 153): no docstring
  - `async suggest_mappings_from_google_sheets(db_name, body)` (line 192): no docstring
  - `async suggest_mappings_from_excel(db_name, target_class_id, file, target_schema_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, include_relationships, enable_semantic_hints, max_tables, max_rows, max_cols)` (line 273): no docstring
  - `async suggest_schema_from_google_sheets(db_name, body)` (line 382): no docstring
  - `async suggest_schema_from_excel(db_name, file, sheet_name, class_name, table_id, table_top, table_left, table_bottom, table_right, include_complex_types, max_tables, max_rows, max_cols)` (line 437): no docstring

### `backend/bff/services/pipeline_agent_autonomous_loop.py`
- **Functions**
  - `_tool_alias(tool_name)` (line 133): Convert a tool name into a short alias for intra-batch reference resolution.
  - `_resolve_ref_path(value, path)` (line 149): no docstring
  - `_drop_none_values(value)` (line 159): MCP tools validate inputs via JSON schema where `null` is frequently invalid for optional
  - `_resolve_batch_placeholders(value, last, last_by_alias)` (line 173): Resolve lightweight placeholders inside a *single batch* of tool calls.
  - `_calculate_trim_limit(total_count, default_limit, min_limit)` (line 369): Dynamically calculate trim limit based on data size.
  - `_trim_null_report(report)` (line 383): no docstring
  - `_trim_key_inference(value)` (line 414): no docstring
  - `_trim_type_inference(value)` (line 451): no docstring
  - `_trim_join_plan(value)` (line 489): no docstring
  - `_summarize_plan(plan_obj)` (line 501): no docstring
  - `_summarize_pipeline_progress(state)` (line 595): no docstring
  - `_record_pipeline_event(state, tool_name, args, observation)` (line 629): no docstring
  - `_pipeline_has_unresolved_status(state)` (line 703): If pipeline execution was attempted, ensure we don't incorrectly report success while
  - `_plan_status(plan_obj)` (line 726): no docstring
  - `_build_system_prompt(allowed_tools)` (line 749): no docstring
  - `_prompt_text(items)` (line 868): no docstring
  - `_build_prompt_header(state, answers, planner_hints, task_spec)` (line 875): no docstring
  - `_build_user_prompt(state, answers, planner_hints, task_spec, target_chars)` (line 896): Backward-compatible prompt builder for the SSE streaming agent path.
  - `_summarize_ontology(ontology)` (line 945): Create a summary of the working ontology for compaction.
  - `_build_compaction_snapshot(state, answers, planner_hints, task_spec)` (line 967): no docstring
  - `_progressive_compress_prompt_items(state, answers, planner_hints, task_spec, target_chars, preserve_recent_n)` (line 1001): Progressive compression: remove oldest items until within target.
  - `_compute_prompt_hash(text)` (line 1098): Compute a short hash of prompt text for identification.
  - `async _log_pre_compression_state(run_id, prompt_items, compression_reason, audit_store, event_store)` (line 1104): Save pre-compression state for debugging.
  - `async _maybe_compact_prompt_items(state, answers, planner_hints, task_spec, max_chars, run_id, audit_store, event_store)` (line 1201): Improved compaction with progressive compression and pre-compression logging.
  - `_mask_tool_observation(payload)` (line 1258): no docstring
  - `_is_internal_budget_clarification(questions)` (line 1263): no docstring
  - `async run_pipeline_agent_mcp_autonomous(goal, data_scope, answers, planner_hints, task_spec, persist_plan, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, event_store, dataset_registry, plan_registry)` (line 1271): Returns a payload compatible with the existing UI expectations for `/agent/pipeline-runs`.
  - `async run_pipeline_agent_streaming(goal, data_scope, answers, planner_hints, task_spec, persist_plan, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, event_store, dataset_registry, plan_registry)` (line 2514): SSE 스트리밍 버전의 Pipeline Agent.
- **Classes**
  - `AutonomousPipelineAgentToolCall` (line 47): no docstring
    - `_coerce_args(cls, v)` (line 53): no docstring
  - `AutonomousPipelineAgentDecision` (line 58): no docstring
    - `_coerce_args(cls, v)` (line 76): no docstring
    - `_coerce_tool_calls(cls, v)` (line 82): no docstring
    - `_coerce_questions(cls, v)` (line 88): no docstring
    - `_coerce_lists(cls, v)` (line 113): no docstring
    - `_validate_action(self)` (line 118): no docstring
  - `_AgentState` (line 218): no docstring
  - `StreamEvent` (line 2508): SSE 이벤트 데이터 구조

### `backend/bff/services/pipeline_catalog_service.py`
- **Functions**
  - `async list_pipelines(db_name, branch, pipeline_registry, request)` (line 36): no docstring
  - `async create_pipeline(payload, audit_store, pipeline_registry, dataset_registry, event_store, request)` (line 62): no docstring

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
  - `async upload_media_dataset(db_name, branch, files, dataset_name, description, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, flush_dataset_ingest_outbox, build_dataset_event_payload)` (line 25): no docstring

### `backend/bff/services/pipeline_dataset_upload_context.py`
- **Functions**
  - `async _prepare_dataset_upload_context(request, db_name, branch, pipeline_registry)` (line 34): no docstring
  - `_build_dataset_upload_response(result, preview, source, funnel_analysis, schema_json)` (line 67): no docstring
- **Classes**
  - `_DatasetUploadContext` (line 25): no docstring

### `backend/bff/services/pipeline_dataset_upload_service.py`
- **Functions**
  - `async _save_artifact(lakefs_storage_service, repo, key, fileobj, content_type, metadata, checksum)` (line 59): no docstring
  - `async upload_tabular_dataset(inputs, lakefs_client, lakefs_storage_service, dataset_registry, objectify_registry, objectify_job_queue, lineage_store, build_dataset_event_payload, flush_dataset_ingest_outbox)` (line 100): no docstring
- **Classes**
  - `TabularDatasetUploadInput` (line 27): no docstring
  - `TabularDatasetUploadResult` (line 51): no docstring

### `backend/bff/services/pipeline_dataset_version_service.py`
- **Functions**
  - `async create_dataset_version(dataset_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, flush_dataset_ingest_outbox, build_dataset_event_payload)` (line 25): no docstring

### `backend/bff/services/pipeline_detail_service.py`
- **Functions**
  - `async get_pipeline(pipeline_id, pipeline_registry, branch, preview_node_id, request)` (line 38): no docstring
  - `async get_pipeline_readiness(pipeline_id, branch, pipeline_registry, dataset_registry, request)` (line 91): no docstring
  - `async update_pipeline(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, request, event_store)` (line 271): no docstring

### `backend/bff/services/pipeline_execution_service.py`
- **Functions**
  - `async preview_pipeline(pipeline_id, payload, request, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, event_store)` (line 55): no docstring
  - `async build_pipeline(pipeline_id, payload, request, audit_store, pipeline_registry, pipeline_job_queue, dataset_registry, oms_client, emit_pipeline_control_plane_event)` (line 313): no docstring
  - `async deploy_pipeline(pipeline_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, oms_client, lineage_store, audit_store, emit_pipeline_control_plane_event, _acquire_pipeline_publish_lock, _release_pipeline_publish_lock)` (line 573): no docstring

### `backend/bff/services/pipeline_join_evaluator.py`
- **Functions**
  - `_ratio(numerator, denominator)` (line 43): no docstring
  - `_count_matches(left, right, left_keys, right_keys)` (line 49): no docstring
  - `_coerce_table(payload)` (line 90): no docstring
  - `_coerce_tables(payload)` (line 109): no docstring
  - `_choose_join_inputs(inputs, tables, left_key, right_key, left_keys, right_keys)` (line 125): no docstring
  - `async evaluate_pipeline_joins(definition_json, db_name, dataset_registry, node_filter, run_tables, storage_service)` (line 178): no docstring
- **Classes**
  - `JoinEvaluation` (line 19): no docstring

### `backend/bff/services/pipeline_plan_autonomous_compiler.py`
- **Functions**
  - `_coerce_questions(raw)` (line 27): no docstring
  - `_coerce_llm_meta(raw)` (line 40): no docstring
  - `_coerce_planner_fields(payload)` (line 55): no docstring
  - `async compile_pipeline_plan_mcp_autonomous(goal, data_scope, answers, planner_hints, task_spec, actor, tenant_id, user_id, data_policies, selected_model, allowed_models, llm_gateway, redis_service, audit_store, dataset_registry, plan_registry)` (line 69): Compile a pipeline plan using the single autonomous loop runtime.

### `backend/bff/services/pipeline_plan_models.py`
- **Classes**
  - `PipelineClarificationQuestion` (line 13): no docstring
  - `PipelinePlanCompileResult` (line 23): no docstring

### `backend/bff/services/pipeline_plan_preview_service.py`
- **Functions**
  - `async _try_get_lakefs_storage(pipeline_registry, request, purpose)` (line 46): no docstring
  - `async _validate_or_warn(plan, dataset_registry, db_name, branch, require_output)` (line 57): no docstring
  - `async preview_plan(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 84): no docstring
  - `async inspect_plan_preview(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 202): no docstring
  - `async evaluate_joins(plan_id, body, request, dataset_registry, pipeline_registry, plan_registry)` (line 267): no docstring

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
  - `_parse_plan_id(plan_id)` (line 21): no docstring
  - `async _get_plan_record_or_404(plan_id, request, plan_registry)` (line 28): no docstring
  - `_load_plan_or_400(record)` (line 39): no docstring
  - `_extract_db_name_or_400(plan)` (line 46): no docstring
  - `_enforce_db_scope_or_403(request, db_name)` (line 55): no docstring
  - `async _load_scoped_plan(plan_id, request, plan_registry)` (line 71): no docstring
- **Classes**
  - `PipelinePlanRequestContext` (line 63): no docstring

### `backend/bff/services/pipeline_plan_tenant_service.py`
- **Functions**
  - `resolve_tenant_id(request)` (line 17): no docstring
  - `resolve_actor(request)` (line 29): no docstring
  - `async resolve_tenant_policy(request)` (line 33): no docstring

### `backend/bff/services/pipeline_plan_validation.py`
- **Functions**
  - `_is_acyclic(nodes, edges)` (line 37): no docstring
  - `async validate_pipeline_plan(plan, dataset_registry, db_name, branch, require_output, task_spec)` (line 42): no docstring
- **Classes**
  - `PipelinePlanValidationResult` (line 29): no docstring

### `backend/bff/services/pipeline_proposal_service.py`
- **Functions**
  - `normalize_mapping_spec_ids(raw)` (line 28): no docstring
  - `async _build_proposal_bundle(pipeline, build_job_id, mapping_spec_ids, pipeline_registry, dataset_registry, objectify_registry)` (line 55): no docstring
  - `async list_pipeline_proposals(db_name, branch, status_filter, pipeline_registry, request)` (line 218): no docstring
  - `async submit_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, objectify_registry, request)` (line 245): no docstring
  - `async approve_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 321): no docstring
  - `async reject_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 406): no docstring

### `backend/bff/services/pipeline_udf_service.py`
- **Functions**
  - `_parse_uuid_or_404(value, detail)` (line 18): no docstring
  - `async create_udf(db_name, name, code, description, pipeline_registry)` (line 25): no docstring
  - `async list_udfs(db_name, pipeline_registry)` (line 60): no docstring
  - `async get_udf(udf_id, pipeline_registry)` (line 92): no docstring
  - `async create_udf_version(udf_id, code, pipeline_registry)` (line 124): no docstring
  - `async get_udf_version(udf_id, version, pipeline_registry)` (line 157): no docstring

### `backend/bff/services/schema_changes_service.py`
- **Functions**
  - `async list_schema_changes(pool, db_name, subject_type, subject_id, severity, since, limit, offset)` (line 20): no docstring
  - `async acknowledge_drift(pool, drift_id, acknowledged_by)` (line 101): no docstring
  - `async list_subscriptions(pool, user_id, db_name, status_filter, limit)` (line 133): no docstring
  - `async create_subscription(pool, user_id, subject_type, subject_id, db_name, severity_filter, notification_channels)` (line 197): no docstring
  - `async delete_subscription(pool, user_id, subscription_id)` (line 256): no docstring
  - `async check_mapping_compatibility(mapping_spec_id, db_name, dataset_version_id, dataset_registry, objectify_registry, detector)` (line 291): no docstring
  - `async get_schema_change_stats(pool, db_name, days)` (line 407): no docstring

### `backend/bff/services/sheet_import_parsing.py`
- **Functions**
  - `async read_excel_upload(file)` (line 14): no docstring
  - `parse_table_bbox(table_top, table_left, table_bottom, table_right)` (line 28): no docstring
  - `parse_json_array(value, field_name, required_message, treat_blank_as_missing, type_error_message)` (line 51): no docstring
  - `parse_json_object(value, field_name, default, treat_blank_as_missing, type_error_message)` (line 79): no docstring
  - `parse_target_schema_json(value)` (line 104): no docstring

### `backend/bff/services/sheet_import_service.py`

### `backend/bff/services/tasks_service.py`
- **Functions**
  - `_to_status_response(task)` (line 19): no docstring
  - `async get_task_status(task_id, task_manager)` (line 34): no docstring
  - `async list_tasks(status_filter, task_type, limit, task_manager)` (line 41): no docstring
  - `async cancel_task(task_id, task_manager)` (line 53): no docstring
  - `async get_task_metrics(task_manager)` (line 63): no docstring
  - `async get_task_result(task_id, task_manager)` (line 68): no docstring

### `backend/bff/services/websocket_service.py`
- **Functions**
  - `_is_valid_identifier(value)` (line 27): no docstring
  - `async _send_json(websocket, payload)` (line 35): no docstring
  - `async _send_error(websocket, message)` (line 39): no docstring
  - `async _send_connection_established(websocket, client_id, command_id, user_id)` (line 43): no docstring
  - `async handle_client_message(websocket, client_id, message, manager)` (line 71): Handle a message received from the client.
  - `async _run_session(websocket, client_id, user_id, token, manager, command_id)` (line 142): no docstring
  - `async run_command_updates(websocket, command_id, client_id, user_id, token, manager)` (line 198): no docstring
  - `async run_user_updates(websocket, user_id, client_id, token, manager)` (line 229): no docstring

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
  - `test_mapping_spec_target_unknown_is_rejected()` (line 253): no docstring
  - `test_mapping_spec_relationship_target_is_rejected()` (line 267): no docstring
  - `test_mapping_spec_dataset_pk_target_mismatch_is_rejected()` (line 284): no docstring
  - `test_mapping_spec_required_missing_is_rejected()` (line 312): no docstring
  - `test_mapping_spec_primary_key_missing_is_rejected()` (line 332): no docstring
  - `test_mapping_spec_unsupported_type_is_rejected()` (line 352): no docstring
  - `test_mapping_spec_target_type_mismatch_is_rejected()` (line 377): no docstring
  - `test_mapping_spec_source_type_incompatible_is_rejected()` (line 395): no docstring
  - `test_mapping_spec_source_type_unsupported_is_rejected()` (line 424): no docstring
  - `test_mapping_spec_change_summary_is_recorded()` (line 446): no docstring
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
  - `async test_create_dataset_version_materializes_manual_sample_to_artifact(monkeypatch)` (line 92): no docstring
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
  - `_StubDatasetRegistry` (line 24): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id)` (line 25): no docstring
  - `TestInformationLeakagePrevention` (line 29): Test suite to verify BFF APIs don't leak internal architecture information
    - `client(self)` (line 33): Create test client
    - `mock_elasticsearch_service(self)` (line 38): Mock Elasticsearch service
    - `mock_oms_client(self)` (line 43): Mock OMS client
    - `async test_get_instance_elasticsearch_success_no_source_leak(self, client)` (line 48): Test: get_instance with ES success should not leak source information
    - `async test_get_instance_terminus_fallback_no_source_leak(self, client)` (line 91): Test: get_instance with TerminusDB fallback should not leak source information
    - `async test_get_class_instances_elasticsearch_success_no_source_leak(self, client)` (line 135): Test: get_class_instances with ES success should not leak source information
    - `async test_get_class_instances_terminus_fallback_no_source_leak(self, client)` (line 175): Test: get_class_instances with TerminusDB fallback should not leak source information
    - `async test_error_responses_no_internal_info_leak(self, client)` (line 215): Test: Error responses should not leak internal architecture details
    - `test_response_structure_consistency(self)` (line 248): Test: All successful responses should have consistent structure regardless of data source
    - `test_forbidden_fields_in_responses(self)` (line 269): Test: Ensure specific internal fields are never exposed in API responses

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
  - `dt_iso(value)` (line 15): no docstring
  - `serialize_action_log_record(record)` (line 24): no docstring

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
  - `extract_httpx_detail(exc)` (line 14): no docstring
  - `raise_httpx_as_http_exception(exc)` (line 29): no docstring

### `backend/bff/utils/request_headers.py`
- **Functions**
  - `extract_forward_headers(request, keys)` (line 21): no docstring

### `backend/bff/verify_implementation.py`
- **Functions**
  - `test_core_conflict_system()` (line 12): 핵심 충돌 시스템 검증 (UI/클라이언트 친화 포맷)
  - `test_real_world_scenario()` (line 269): 실제 시나리오 시뮬레이션

## conftest.py

### `backend/conftest.py`
- **Functions**
  - `_ensure_test_env()` (line 9): no docstring

## connector_sync_worker

### `backend/connector_sync_worker/__init__.py`

### `backend/connector_sync_worker/main.py`
- **Functions**
  - `async _main()` (line 582): no docstring
- **Classes**
  - `ConnectorSyncWorker` (line 54): no docstring
    - `__init__(self)` (line 55): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 84): no docstring
    - `async _producer_call(self, func, *args, **kwargs)` (line 87): no docstring
    - `async _poll_message(self, timeout)` (line 90): no docstring
    - `async initialize(self)` (line 95): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 151): Handle partition revocation during rebalance.
    - `_on_partitions_assigned(self, partitions)` (line 159): Handle partition assignment during rebalance.
    - `async close(self)` (line 167): no docstring
    - `_heartbeat_options(self)` (line 199): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 205): no docstring
    - `async _process_payload(self, payload)` (line 257): no docstring
    - `_span_name(self, payload)` (line 260): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 263): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 266): no docstring
    - `_should_seek_on_in_progress(self, claim, payload)` (line 269): no docstring
    - `_should_seek_on_retry(self, attempt_count, payload)` (line 272): no docstring
    - `_should_mark_done_after_dlq(self, payload, error)` (line 275): no docstring
    - `async _commit(self, msg)` (line 278): no docstring
    - `async _seek(self, topic, partition, offset)` (line 283): no docstring
    - `async _on_success(self, payload, result, duration_s)` (line 290): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 303): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 325): no docstring
    - `_bff_scope_headers(self, db_name)` (line 347): no docstring
    - `async _fetch_ontology_schema(self, db_name, class_label, branch)` (line 353): no docstring
    - `async _target_field_types(self, db_name, class_label, branch)` (line 366): no docstring
    - `async _process_google_sheets_update(self, envelope)` (line 383): no docstring
    - `async _handle_envelope(self, envelope)` (line 562): no docstring
    - `async run(self)` (line 572): no docstring

## connector_trigger_service

### `backend/connector_trigger_service/__init__.py`

### `backend/connector_trigger_service/main.py`
- **Functions**
  - `async _main()` (line 335): no docstring
- **Classes**
  - `ConnectorTriggerService` (line 44): no docstring
    - `__init__(self)` (line 45): no docstring
    - `async initialize(self)` (line 63): no docstring
    - `async close(self)` (line 92): no docstring
    - `async _producer_call(self, func, *args, **kwargs)` (line 113): no docstring
    - `async _is_due(self, source)` (line 116): no docstring
    - `async _poll_google_sheets(self, source)` (line 127): no docstring
    - `async _poll_source(self, source, sem)` (line 193): no docstring
    - `async _poll_loop(self)` (line 208): no docstring
    - `async _publish_outbox_loop(self)` (line 227): no docstring
    - `async run(self)` (line 319): no docstring

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
  - `main()` (line 148): 메인 진입점
- **Classes**
  - `OntologyEventConsumer` (line 25): 온톨로지 이벤트 컨슈머 예제
    - `__init__(self, consumer_group)` (line 28): no docstring
    - `initialize(self)` (line 34): 컨슈머 초기화
    - `process_event(self, event)` (line 50): 이벤트 처리 로직
    - `handle_class_created(self, class_id, data)` (line 68): 온톨로지 클래스 생성 이벤트 처리
    - `handle_class_updated(self, class_id, data)` (line 78): 온톨로지 클래스 업데이트 이벤트 처리
    - `handle_class_deleted(self, class_id, data)` (line 85): 온톨로지 클래스 삭제 이벤트 처리
    - `run(self)` (line 91): 메인 실행 루프
    - `shutdown(self)` (line 137): 컨슈머 종료

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
  - `get_data_processor()` (line 33): 데이터 프로세서 의존성
  - `async analyze_dataset(request, processor)` (line 39): 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.
  - `async analyze_sheet_structure(request)` (line 69): Raw sheet grid(엑셀/스프레드시트)의 구조를 분석합니다.
  - `async analyze_excel_structure(file, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options_json)` (line 117): Excel(.xlsx/.xlsm) 파일을 업로드 받아 grid + merged_cells로 파싱한 뒤,
  - `async analyze_google_sheets_structure(request)` (line 213): Google Sheets URL → (BFF에서 values+metadata(merges) 가져오기) → grid/merged_cells → 구조 분석
  - `async upsert_structure_patch(patch)` (line 297): Store/update a structure-analysis patch for a given sheet_signature.
  - `async get_structure_patch(sheet_signature)` (line 305): no docstring
  - `async delete_structure_patch(sheet_signature)` (line 313): no docstring
  - `async preview_google_sheets_with_inference(sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types, processor)` (line 319): Google Sheets 데이터를 미리보기하고 타입을 추론합니다.
  - `async suggest_schema(analysis_results, class_name, processor)` (line 361): 분석 결과를 기반으로 OMS 스키마를 제안합니다.
  - `async health_check()` (line 391): Funnel 서비스 상태 확인

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
  - `_CellInfo` (line 47): no docstring
  - `FunnelStructureAnalyzer` (line 61): Structure analyzer for sheet-like 2D grids.
    - `_is_blank(value)` (line 80): no docstring
    - `_cache_get(cls, key)` (line 90): no docstring
    - `_cache_set(cls, key, payload, ttl_seconds, max_entries)` (line 102): no docstring
    - `_safe_json_dumps(value)` (line 132): no docstring
    - `_hash_grid(cls, grid)` (line 139): no docstring
    - `_hash_style_hints(cls, style_hints)` (line 154): no docstring
    - `_hash_merges(cls, merged_cells)` (line 170): no docstring
    - `_make_cache_key(cls, grid, merged_cells, style_hints, include_complex_types, max_tables, options)` (line 183): no docstring
    - `_compute_sheet_signature(cls, grid, merged_cells, style_hints, opts)` (line 215): Compute a "sheet_signature" designed to be stable across repeated uploads of the same template.
    - `_compute_coarse_strides(rows, cols, target_cells)` (line 304): Choose downsampling strides so that coarse_rows * coarse_cols ~= target_cells.
    - `_downsample_grid(cls, grid, row_stride, col_stride)` (line 324): no docstring
    - `_map_coarse_bbox_to_full(cls, coarse, row_stride, col_stride, rows, cols, margin_rows, margin_cols)` (line 345): no docstring
    - `_score_cells_in_bbox(cls, grid, bbox, include_complex_types, style_hints)` (line 374): Score only cells inside a bbox (used by coarse-to-fine mode).
    - `_analyze_coarse_to_fine(cls, grid, style_hints, include_complex_types, merged_cells, max_tables, opts)` (line 448): no docstring
    - `analyze(cls, grid, include_complex_types, merged_cells, cell_style_hints, max_tables, options)` (line 565): no docstring
    - `analyze_bbox(cls, grid, bbox, include_complex_types, merged_cells, cell_style_hints, options, table_id, override_mode, override_header_rows, override_header_cols)` (line 745): Analyze a single bbox (used for patch re-evaluation / UI corrections).
    - `_detect_data_islands(cls, grid, cell_map, row_stats, max_tables, opts, style_hints)` (line 918): no docstring
    - `_bbox_quality_score(cls, grid, bbox, cell_map)` (line 1034): no docstring
    - `_split_bbox_by_row_separators(cls, grid, bbox, cell_map, opts, style_hints)` (line 1056): Split a bbox into multiple bboxes when internal separator rows exist.
    - `_split_bbox_by_row_profile(cls, grid, bbox, opts)` (line 1184): Split hybrid blocks where top rows are narrow (key-value) and bottom rows are wide (table).
    - `_expand_bbox_to_dense_region(cls, grid, bbox, cell_map, expand_threshold, max_header_scan)` (line 1234): Expand a bbox derived from "core" cells to cover adjacent string/header cells that
    - `_analyze_island(cls, grid, cell_map, bbox, include_complex_types, table_id, opts, merged_cells)` (line 1320): no docstring
    - `_detect_preamble_skip(cls, grid, bbox, cell_map, opts)` (line 1494): Detect leading "title/description" rows inside a detected bbox.
    - `_rank_header_row_candidates(cls, sub, bbox, cell_map, max_k, include_complex_types, merged_cells, opts)` (line 1596): no docstring
    - `_rank_header_col_candidates(cls, sub, bbox, cell_map, max_k, include_complex_types, merged_cells, opts)` (line 1786): no docstring
    - `_score_property_mode(cls, sub, bbox, cell_map)` (line 1949): no docstring
    - `_best_header_row_candidate(cls, sub, bbox, cell_map, max_k, include_complex_types)` (line 2022): no docstring
    - `_best_header_col_candidate(cls, sub, bbox, cell_map, max_k, include_complex_types)` (line 2048): no docstring
    - `_header_row_score(cls, sub, bbox, cell_map, header_rows)` (line 2074): no docstring
    - `_header_col_score(cls, sub, bbox, cell_map, header_cols)` (line 2107): no docstring
    - `_axis_type_consistency(cls, sequences, include_complex_types)` (line 2142): no docstring
    - `_string_sequence_consistency(cls, values)` (line 2167): Estimate how "table-like" a string-only sequence is.
    - `_flatten_merged_cells(cls, grid, merged_cells, include_complex_types, fill_boxes)` (line 2218): no docstring
    - `_should_fill_merge(cls, value, mr, include_complex_types)` (line 2244): no docstring
    - `_bboxes_intersect(a, b)` (line 2265): no docstring
    - `_extract_key_values(cls, grid, cell_map, exclude_boxes, include_complex_types, opts)` (line 2278): no docstring
    - `_looks_like_kv_label(cls, text)` (line 2400): no docstring
    - `_looks_like_explicit_kv_label(cls, text)` (line 2414): Strict label detector used to avoid pairing label-to-label in KV extraction.
    - `_looks_like_data_value_text(cls, text)` (line 2425): Heuristic: some strings are much more likely to be data values than headers/labels.
    - `_find_nearest_label(cls, cell_map, row, col, radius)` (line 2442): no docstring
    - `_extract_property_table_kv(cls, sub, bbox, cell_map)` (line 2479): no docstring
    - `_pivot_transposed(cls, sub, header_cols)` (line 2518): no docstring
    - `_build_table_column_provenance(cls, headers, bbox, header_rows)` (line 2555): no docstring
    - `_build_transposed_column_provenance(cls, headers, bbox, header_cols, field_row_offsets)` (line 2580): no docstring
    - `_extract_table(cls, sub, header_rows)` (line 2609): no docstring
    - `_build_header_tree(cls, header_grid)` (line 2645): Build a hierarchical header tree from a multi-row header grid.
    - `_collect_cell_evidence(cls, cell_map, bbox, limit)` (line 2700): Collect a small sample of "evidence" cells for explainability.
    - `_infer_schema(cls, headers, rows, include_complex_types)` (line 2747): no docstring
    - `_dedupe_headers(cls, headers)` (line 2766): no docstring
    - `_normalize_grid(cls, grid)` (line 2782): no docstring
    - `_normalize_style_hints(cls, style_hints, rows, cols)` (line 2787): no docstring
    - `_score_cells(cls, grid, include_complex_types, style_hints)` (line 2806): no docstring
    - `_infer_single_value_type(cls, text, include_complex_types)` (line 2876): no docstring
    - `_cell_score(cls, text, inferred_type, row, non_empty_in_row)` (line 2883): no docstring
    - `_is_label_like_text(cls, text)` (line 2912): no docstring
    - `_is_header_like_text(cls, text)` (line 2926): no docstring
    - `_connected_components(points)` (line 2937): no docstring
    - `_bbox_for_points(points)` (line 2959): no docstring
    - `_tighten_bbox(grid, bbox)` (line 2965): no docstring
    - `_bbox_area(bbox)` (line 2987): no docstring
    - `_count_non_empty_in_bbox(cls, grid, bbox)` (line 2991): no docstring
    - `_slice_bbox(grid, bbox)` (line 3001): no docstring
    - `_extract_columns_from_sub(rows)` (line 3005): no docstring
    - `_extract_rows_from_sub(rows, start_col)` (line 3016): no docstring
    - `_ensure_row_len(grid, row, length)` (line 3023): no docstring
    - `_get_cell(grid, row, col)` (line 3030): no docstring

### `backend/funnel/services/structure_patch.py`
- **Functions**
  - `_resolve_table_index(tables, op)` (line 15): no docstring
  - `apply_structure_patch(analysis, patch, grid, merged_cells, cell_style_hints, include_complex_types, options)` (line 33): Apply a stored patch to an analysis result.

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
  - `FunnelTypeInferenceAdapter` (line 13): Adapter that wraps FunnelTypeInferenceService to implement TypeInferenceInterface.
    - `__init__(self)` (line 21): 🔥 REAL IMPLEMENTATION! Initialize adapter with logging and validation.
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 29): Analyze a column of data and infer its type.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 50): Analyze an entire dataset and infer types for all columns.
    - `async infer_single_value_type(self, value, context)` (line 72): Infer the type of a single value.

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
  - `async main()` (line 3141): Main entry point
- **Classes**
  - `_InstanceCommandPayload` (line 78): no docstring
  - `_InstanceCommandParseError` (line 83): no docstring
    - `__init__(self, stage, payload_text, payload_obj, fallback_metadata, cause)` (line 84): no docstring
  - `StrictInstanceWorker` (line 101): STRICT Lightweight Instance Worker
    - `__init__(self)` (line 109): no docstring
    - `_is_ingest_metadata(metadata)` (line 160): no docstring
    - `_writeback_guard_blocks(cls, command)` (line 166): no docstring
    - `async initialize(self)` (line 187): Initialize all connections
    - `async _s3_call(self, func, *args, **kwargs)` (line 327): no docstring
    - `async _s3_read_body(self, body)` (line 330): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 333): no docstring
    - `async _poll_message(self, timeout)` (line 336): no docstring
    - `_extract_payload_from_message(self, message)` (line 341): Unwrap a command from the canonical EventEnvelope message.
    - `async extract_payload_from_message(self, message)` (line 370): no docstring
    - `get_primary_key_value(self, class_id, payload, allow_generate)` (line 373): Extract primary key value dynamically based on class naming convention
    - `_is_objectify_command(command)` (line 404): no docstring
    - `async extract_relationships(self, db_name, class_id, payload, branch, allow_pattern_fallback, strict_schema)` (line 413): Extract ONLY relationship fields from payload
    - `async extract_required_properties(self, db_name, class_id, branch)` (line 627): Extract required property names from the class schema.
    - `async _apply_create_instance_side_effects(self, command_id, db_name, class_id, branch, payload, instance_id, command_log, ontology_version, created_by, allow_pattern_fallback)` (line 679): Apply the create-instance side-effects without touching command status.
    - `async process_create_instance(self, command)` (line 979): Process CREATE_INSTANCE command - strict lightweight mode.
    - `async process_bulk_create_instances(self, command)` (line 1349): Process BULK_CREATE_INSTANCES command (idempotent per event_id; no sequence-guard).
    - `async process_bulk_update_instances(self, command)` (line 1545): Process BULK_UPDATE_INSTANCES command (updates multiple instances).
    - `async process_update_instance(self, command, skip_status)` (line 1650): Process UPDATE_INSTANCE command (idempotent + ordered via registry claim).
    - `async process_delete_instance(self, command)` (line 2151): Process DELETE_INSTANCE command (idempotent delete).
    - `async _record_instance_edit(self, db_name, class_id, instance_id, edit_type, fields, metadata)` (line 2441): no docstring
    - `async _resolve_instance_payload(self, db_name, branch, class_id, instance_id)` (line 2465): no docstring
    - `async _enqueue_link_reindex(self, db_name, link_type_id)` (line 2535): no docstring
    - `async _apply_relationship_object_link_edits(self, db_name, branch, class_id, instance_id, current_payload, previous_payload)` (line 2546): no docstring
    - `async set_command_status(self, command_id, status, result)` (line 2626): Set command status using CommandStatusService (preserves history + pubsub).
    - `_heartbeat_options(self)` (line 2694): no docstring
    - `_is_retryable_error_impl(exc)` (line 2701): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 2725): no docstring
    - `_parse_payload(self, payload)` (line 2789): no docstring
    - `_fallback_metadata(self, payload)` (line 2838): no docstring
    - `_registry_key(self, payload)` (line 2841): no docstring
    - `async _process_payload(self, payload)` (line 2869): no docstring
    - `_span_name(self, payload)` (line 2920): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 2923): no docstring
    - `_metric_event_name(self, payload)` (line 2946): no docstring
    - `_is_retryable_error(exc, payload)` (line 2953): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 2958): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 2966): no docstring
    - `async _commit(self, msg)` (line 2981): no docstring
    - `async _seek(self, topic, partition, offset)` (line 2986): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 2991): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 3023): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 3049): no docstring
    - `async _send_to_dlq(self, msg, error, attempt_count, payload, raw_payload, stage, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 3069): no docstring
    - `async run(self)` (line 3107): Main processing loop
    - `async shutdown(self)` (line 3119): Graceful shutdown

## mcp_servers

### `backend/mcp_servers/__init__.py`

### `backend/mcp_servers/context7_development.py`
- **Functions**
  - `get_context7_developer()` (line 362): Get or create Context7 developer instance
  - `async analyze_feature(name, description)` (line 371): Quick analysis before implementing a feature
  - `async validate_code(feature, details, files)` (line 377): Quick validation of implementation
  - `async document_feature(name, description, details, lessons)` (line 383): Quick documentation of implemented feature
- **Classes**
  - `Context7Developer` (line 16): Development helper that integrates Context7 for code analysis and suggestions
    - `__init__(self)` (line 21): no docstring
    - `async analyze_before_implementation(self, feature_name, description, related_files)` (line 25): Analyze codebase before implementing a new feature
    - `async validate_implementation(self, feature_name, implementation_details, files_modified)` (line 97): Validate implementation against best practices
    - `async document_implementation(self, feature_name, description, technical_details, lessons_learned)` (line 155): Document implementation in Context7 knowledge base
    - `async get_coding_suggestions(self, code_snippet, language, context)` (line 227): Get coding suggestions from Context7
    - `_generate_recommendations(self, patterns, insights)` (line 259): Generate recommendations based on patterns and insights
    - `async _get_improvement_suggestions(self, feature_name, files_modified)` (line 289): Get improvement suggestions for the implementation
    - `async _check_code_smells(self, implementation_details)` (line 307): Check for potential code smells
    - `_calculate_quality_score(self, validation_results, code_smells)` (line 327): Calculate overall quality score
    - `_format_technical_details(self, details)` (line 344): Format technical details for documentation
    - `_format_lessons(self, lessons)` (line 351): Format lessons learned

### `backend/mcp_servers/mcp_client.py`
- **Functions**
  - `get_mcp_manager()` (line 341): Get or create MCP manager singleton
  - `get_context7_client()` (line 349): Get Context7 client
- **Classes**
  - `MCPServerConfig` (line 25): Configuration for an MCP server
  - `MCPClientManager` (line 34): Manager for multiple MCP client connections
    - `__init__(self, config_path)` (line 40): no docstring
    - `_resolve_config_path(config_path)` (line 48): no docstring
    - `_load_config(self)` (line 66): Load MCP configuration from file
    - `async connect_server(self, server_name)` (line 94): Connect to a specific MCP server
    - `async disconnect_server(self, server_name)` (line 144): Disconnect from a specific MCP server
    - `async reconnect_server(self, server_name)` (line 153): Reconnect to a server to refresh the tool list cache.
    - `async call_tool(self, server_name, tool_name, arguments)` (line 164): Call a tool on a specific MCP server
    - `async list_tools(self, server_name)` (line 192): List available tools from a server
  - `Context7Client` (line 206): Specialized client for Context7 MCP server
    - `__init__(self, mcp_manager)` (line 212): no docstring
    - `async search(self, query, limit, filters)` (line 216): Search Context7 knowledge base
    - `async get_context(self, entity_id)` (line 244): Get context for a specific entity
    - `async add_knowledge(self, title, content, metadata)` (line 260): Add knowledge to Context7
    - `async link_entities(self, source_id, target_id, relationship, properties)` (line 287): Create relationship between entities
    - `async analyze_ontology(self, ontology_data)` (line 320): Analyze ontology with Context7

### `backend/mcp_servers/ontology_mcp_server.py`
- **Functions**
  - `_bff_api_base_url()` (line 35): Internal helper for MCP tools that need to call the BFF's REST API.
  - `_bff_admin_token()` (line 41): no docstring
  - `_normalize_string(value)` (line 45): Normalize a value to a trimmed string.
  - `_normalize_string_list(value)` (line 50): Normalize to a list of strings.
  - `_mask_observation(payload)` (line 63): Mask PII in tool observations.
  - `_build_error_response(tool_name, error, hint)` (line 68): Build a structured error response.
  - `async main()` (line 1364): no docstring
- **Classes**
  - `OntologyMCPServer` (line 76): MCP server for ontology building tools.
    - `__init__(self)` (line 79): no docstring
    - `_get_or_create_ontology(self, session_id)` (line 84): Get or create a working ontology for a session.
    - `_setup_handlers(self)` (line 99): no docstring
    - `async _handle_tool_call(self, name, arguments)` (line 459): Route tool calls to implementations (Command pattern via naming convention).
    - `async _tool_ontology_new(self, args)` (line 468): Create a new empty ontology in memory.
    - `async _tool_ontology_load(self, args)` (line 505): Load an existing ontology class into memory.
    - `async _tool_ontology_reset(self, args)` (line 550): Reset working ontology to empty state.
    - `async _tool_ontology_set_class_meta(self, args)` (line 561): Set class metadata.
    - `async _tool_ontology_set_abstract(self, args)` (line 588): Set whether class is abstract.
    - `async _tool_ontology_add_property(self, args)` (line 599): Add a property to the ontology.
    - `async _tool_ontology_update_property(self, args)` (line 663): Update an existing property.
    - `async _tool_ontology_remove_property(self, args)` (line 722): Remove a property.
    - `async _tool_ontology_set_primary_key(self, args)` (line 740): Set a property as primary key.
    - `async _tool_ontology_add_relationship(self, args)` (line 780): Add a relationship to the ontology.
    - `async _tool_ontology_update_relationship(self, args)` (line 833): Update an existing relationship.
    - `async _tool_ontology_remove_relationship(self, args)` (line 864): Remove a relationship.
    - `async _tool_ontology_infer_schema_from_data(self, args)` (line 882): Infer schema from sample data using FunnelClient.
    - `async _tool_ontology_suggest_mappings(self, args)` (line 907): Suggest mappings between source schema and target ontology.
    - `async _tool_ontology_validate(self, args)` (line 969): Validate the working ontology structure.
    - `async _tool_ontology_check_relationships(self, args)` (line 1024): Check if relationship targets exist.
    - `async _tool_ontology_check_circular_refs(self, args)` (line 1064): Check for circular references in parent class chain.
    - `async _tool_ontology_list_classes(self, args)` (line 1110): List all ontology classes.
    - `async _tool_ontology_get_class(self, args)` (line 1151): Get details of a specific ontology class.
    - `async _tool_ontology_search_classes(self, args)` (line 1184): Search ontology classes by label or property names.
    - `async _tool_ontology_create(self, args)` (line 1243): Create the ontology class in the database.
    - `async _tool_ontology_update(self, args)` (line 1291): Update an existing ontology class.
    - `async _tool_ontology_preview(self, args)` (line 1345): Preview the working ontology without saving.

### `backend/mcp_servers/pipeline_mcp_errors.py`
- **Functions**
  - `tool_error(message, detail, status_code, code, category, external_code, context)` (line 9): no docstring
  - `missing_required_params(tool_name, required, arguments)` (line 35): no docstring

### `backend/mcp_servers/pipeline_mcp_helpers.py`
- **Functions**
  - `normalize_string_list(value)` (line 6): no docstring
  - `normalize_aggregates(value)` (line 18): Normalize aggregates list and return (aggregates, warnings).
  - `extract_spark_error_details(run)` (line 43): Extract error details from a pipeline run's output_json.
  - `trim_preview_payload(preview, max_rows)` (line 92): no docstring
  - `trim_build_output(output_json, max_rows)` (line 102): no docstring

### `backend/mcp_servers/pipeline_mcp_http.py`
- **Functions**
  - `async http_json(method, url, headers, json_body, params, timeout_seconds, error_prefix, error_path)` (line 11): no docstring
  - `bff_api_base_url()` (line 40): Internal helper for MCP tools that need to call the BFF's REST API.
  - `_bff_admin_token()` (line 51): no docstring
  - `bff_headers(db_name, principal_id, principal_type)` (line 56): no docstring
  - `async bff_json(method, path, db_name, principal_id, principal_type, json_body, params, timeout_seconds)` (line 87): no docstring
  - `oms_api_base_url()` (line 113): Get OMS API base URL from environment.
  - `async oms_json(method, path, params, json_body, timeout_seconds)` (line 118): Make an HTTP request to OMS API and return JSON response.

### `backend/mcp_servers/pipeline_mcp_server.py`
- **Functions**
  - `_build_tool_error_response(tool_name, error, arguments, hint)` (line 117): Enterprise Enhancement: Build a helpful error response for MCP tool failures.
  - `async main()` (line 1327): no docstring
- **Classes**
  - `_ToolCallRateLimiter` (line 57): Simple rate limiter for MCP tool calls to prevent runaway Agent loops.
    - `__init__(self, max_calls_per_minute, max_calls_per_tool_per_minute)` (line 63): no docstring
    - `check_and_record(self, tool_name)` (line 76): Check if the call is allowed and record it.
  - `PipelineMCPServer` (line 160): no docstring
    - `__init__(self)` (line 161): no docstring
    - `async _ensure_registries(self)` (line 170): no docstring
    - `async _ensure_pipeline_registry(self)` (line 179): no docstring
    - `async _ensure_objectify_registry(self)` (line 185): no docstring
    - `async _ensure_websocket_service(self)` (line 191): Lazy-init WebSocket service for schema drift broadcasts.
    - `_setup_handlers(self)` (line 203): no docstring
    - `async run(self)` (line 1316): no docstring

### `backend/mcp_servers/pipeline_tools/__init__.py`

### `backend/mcp_servers/pipeline_tools/dataset_tools.py`
- **Functions**
  - `async _dataset_get_by_name(server, arguments)` (line 12): no docstring
  - `async _dataset_get_latest_version(server, arguments)` (line 44): no docstring
  - `async _dataset_validate_columns(server, arguments)` (line 67): no docstring
  - `build_dataset_tool_handlers()` (line 139): no docstring

### `backend/mcp_servers/pipeline_tools/debug_tools.py`
- **Functions**
  - `async _debug_inspect_node(_server, arguments)` (line 12): no docstring
  - `async _debug_dry_run(_server, arguments)` (line 56): no docstring
  - `build_debug_tool_handlers()` (line 135): no docstring

### `backend/mcp_servers/pipeline_tools/objectify_tools.py`
- **Functions**
  - `async _objectify_suggest_mapping(server, arguments)` (line 18): no docstring
  - `async _objectify_create_mapping_spec(server, arguments)` (line 122): no docstring
  - `async _objectify_list_mapping_specs(server, arguments)` (line 209): no docstring
  - `async _objectify_run(server, arguments)` (line 237): no docstring
  - `async _objectify_get_status(server, arguments)` (line 356): no docstring
  - `async _objectify_wait(server, arguments)` (line 391): no docstring
  - `async _trigger_incremental_objectify(server, arguments)` (line 451): no docstring
  - `async _get_objectify_watermark(server, arguments)` (line 518): no docstring
  - `build_objectify_tool_handlers()` (line 561): no docstring

### `backend/mcp_servers/pipeline_tools/ontology_tools.py`
- **Functions**
  - `async _ontology_register_object_type(_server, arguments)` (line 17): no docstring
  - `async _ontology_query_instances(_server, arguments)` (line 135): no docstring
  - `async _detect_foreign_keys(server, arguments)` (line 203): no docstring
  - `async _create_link_type_from_fk(_server, arguments)` (line 293): no docstring
  - `build_ontology_tool_handlers()` (line 377): no docstring

### `backend/mcp_servers/pipeline_tools/pipeline_tools.py`
- **Functions**
  - `_run_status(run)` (line 23): no docstring
  - `async _pipeline_wait_for_mode(server, pipeline_id, mode, enqueue_path, output_builder, enqueue_job_id_extractor, timeout_message, arguments)` (line 27): no docstring
  - `_preview_job_id(resp)` (line 163): no docstring
  - `_build_job_id(resp)` (line 171): no docstring
  - `_preview_output(selected_run, job_id, reused_existing)` (line 176): no docstring
  - `_build_output(selected_run, job_id, reused_existing)` (line 191): no docstring
  - `async _preview_inspect(_server, arguments)` (line 208): no docstring
  - `async _pipeline_create_from_plan(_server, arguments)` (line 216): no docstring
  - `async _pipeline_update_from_plan(_server, arguments)` (line 340): no docstring
  - `async _pipeline_preview_wait(server, arguments)` (line 384): no docstring
  - `async _pipeline_build_wait(server, arguments)` (line 400): no docstring
  - `async _pipeline_deploy_promote_build(server, arguments)` (line 416): no docstring
  - `build_pipeline_tool_handlers()` (line 580): no docstring

### `backend/mcp_servers/pipeline_tools/plan_tools.py`
- **Functions**
  - `async _plan_new(_server, arguments)` (line 59): no docstring
  - `async _plan_reset(_server, arguments)` (line 69): no docstring
  - `async _plan_add_input(_server, arguments)` (line 75): no docstring
  - `async _plan_add_external_input(_server, arguments)` (line 88): no docstring
  - `async _plan_configure_input_read(_server, arguments)` (line 102): no docstring
  - `async _plan_add_join(_server, arguments)` (line 136): no docstring
  - `async _plan_add_group_by(_server, arguments)` (line 202): no docstring
  - `async _plan_add_group_by_expr(_server, arguments)` (line 225): no docstring
  - `async _plan_add_window(_server, arguments)` (line 244): no docstring
  - `async _plan_add_window_expr(_server, arguments)` (line 263): no docstring
  - `async _plan_add_transform(_server, arguments)` (line 276): no docstring
  - `async _plan_add_sort(_server, arguments)` (line 310): no docstring
  - `async _plan_add_explode(_server, arguments)` (line 324): no docstring
  - `async _plan_add_union(_server, arguments)` (line 337): no docstring
  - `async _plan_add_pivot(_server, arguments)` (line 349): no docstring
  - `async _plan_add_filter(_server, arguments)` (line 379): no docstring
  - `async _plan_add_compute(_server, arguments)` (line 390): no docstring
  - `async _plan_add_compute_column(_server, arguments)` (line 465): no docstring
  - `async _plan_add_compute_assignments(_server, arguments)` (line 477): no docstring
  - `async _plan_add_cast(_server, arguments)` (line 488): no docstring
  - `async _plan_add_rename(_server, arguments)` (line 499): no docstring
  - `async _plan_add_select(_server, arguments)` (line 552): no docstring
  - `async _plan_add_select_expr(_server, arguments)` (line 563): no docstring
  - `async _plan_add_drop(_server, arguments)` (line 574): no docstring
  - `async _plan_add_dedupe(_server, arguments)` (line 585): no docstring
  - `async _plan_add_normalize(_server, arguments)` (line 596): no docstring
  - `async _plan_add_regex_replace(_server, arguments)` (line 612): no docstring
  - `async _plan_add_output(_server, arguments)` (line 623): no docstring
  - `async _plan_add_edge(_server, arguments)` (line 636): no docstring
  - `async _plan_delete_edge(_server, arguments)` (line 646): no docstring
  - `async _plan_set_node_inputs(_server, arguments)` (line 656): no docstring
  - `async _plan_update_node_metadata(_server, arguments)` (line 666): no docstring
  - `async _plan_update_settings(_server, arguments)` (line 688): no docstring
  - `async _plan_delete_node(_server, arguments)` (line 704): no docstring
  - `async _plan_update_output(_server, arguments)` (line 713): no docstring
  - `async _plan_validate_structure(_server, arguments)` (line 746): no docstring
  - `async _plan_validate(server, arguments)` (line 752): no docstring
  - `async _plan_preview(server, arguments)` (line 782): no docstring
  - `async _plan_refute_claims(server, arguments)` (line 867): no docstring
  - `async _plan_evaluate_joins(server, arguments)` (line 901): no docstring
  - `build_plan_tool_handlers()` (line 971): no docstring

### `backend/mcp_servers/pipeline_tools/registry.py`
- **Functions**
  - `merge_tool_handlers(*maps)` (line 8): no docstring

### `backend/mcp_servers/pipeline_tools/schema_tools.py`
- **Functions**
  - `async _check_schema_drift(server, arguments)` (line 15): no docstring
  - `async _list_schema_changes(server, arguments)` (line 118): no docstring
  - `build_schema_tool_handlers()` (line 199): no docstring

### `backend/mcp_servers/terminus_mcp_server.py`
- **Functions**
  - `async main()` (line 309): Main entry point
- **Classes**
  - `TerminusDBMCPServer` (line 37): MCP Server for TerminusDB operations
    - `__init__(self)` (line 43): no docstring
    - `_setup_handlers(self)` (line 48): Setup MCP request handlers
    - `async _connect_terminus(self)` (line 287): Connect to TerminusDB
    - `async run(self)` (line 295): Run the MCP server

## message_relay

### `backend/message_relay/__init__.py`

### `backend/message_relay/main.py`
- **Functions**
  - `async main()` (line 756): 메인 진입점
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
    - `async ensure_kafka_topics(self)` (line 190): 필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성
    - `async _load_checkpoint(self)` (line 247): no docstring
    - `async _save_checkpoint(self, checkpoint)` (line 259): no docstring
    - `_log_metrics_if_due(self)` (line 273): no docstring
    - `_flush_producer(self, timeout_s)` (line 296): no docstring
    - `_initial_checkpoint(self)` (line 306): no docstring
    - `_advance_checkpoint(checkpoint, ts_ms, idx_key)` (line 320): Advance the durable checkpoint monotonically (never move backwards).
    - `async _list_next_index_keys(self, checkpoint)` (line 344): no docstring
    - `async process_events(self)` (line 419): Tail S3 by-date index and publish to Kafka.
    - `async run(self)` (line 722): 메인 실행 루프
    - `async shutdown(self)` (line 742): 서비스 종료

## monitoring

### `backend/monitoring/s3_event_store_dashboard.py`
- **Functions**
  - `async main()` (line 355): Run dashboard in CLI mode
- **Classes**
  - `S3EventStoreDashboard` (line 91): S3/MinIO Event Store Monitoring Dashboard
    - `__init__(self)` (line 94): no docstring
    - `async connect(self)` (line 102): Initialize connection to S3/MinIO
    - `async collect_storage_metrics(self)` (line 107): Collect storage usage metrics
    - `async collect_performance_metrics(self)` (line 156): Collect performance metrics from recent operations
    - `async collect_publisher_checkpoint_metrics(self)` (line 178): Collect EventPublisher checkpoint metrics from S3/MinIO.
    - `async collect_health_metrics(self)` (line 241): Collect health and availability metrics
    - `async generate_dashboard(self)` (line 289): Generate complete dashboard data
    - `async start_monitoring(self, interval_seconds)` (line 317): Start continuous monitoring
    - `get_prometheus_metrics(self)` (line 344): Export metrics in Prometheus format
    - `async get_json_dashboard(self)` (line 348): Get dashboard data as JSON

## objectify_worker

### `backend/objectify_worker/__init__.py`

### `backend/objectify_worker/main.py`
- **Functions**
  - `async main()` (line 3631): no docstring
- **Classes**
  - `ObjectifyNonRetryableError` (line 62): Raised for objectify failures that should not be retried.
  - `ObjectifyWorker` (line 66): no docstring
    - `__init__(self)` (line 95): no docstring
    - `_build_error_report(self, error, report, job, message, context)` (line 129): no docstring
    - `async _record_gate_result(self, job, status, details)` (line 168): no docstring
    - `async _update_object_type_active_version(self, job, mapping_spec)` (line 196): no docstring
    - `_normalize_ontology_payload(payload)` (line 255): no docstring
    - `_extract_ontology_fields(cls, payload)` (line 263): no docstring
    - `_is_blank(value)` (line 290): no docstring
    - `_normalize_relationship_ref(value, target_class)` (line 296): no docstring
    - `_normalize_constraints(constraints, raw_type)` (line 320): no docstring
    - `_resolve_import_type(raw_type)` (line 364): no docstring
    - `_validate_value_constraints(self, value, constraints, raw_type)` (line 367): no docstring
    - `_validate_value_constraints_single(self, value, constraints, raw_type)` (line 386): no docstring
    - `_map_mappings_by_target(mappings)` (line 451): no docstring
    - `_has_p0_errors(self, errors)` (line 461): no docstring
    - `async initialize(self)` (line 468): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 525): Handle partition revocation during rebalance.
    - `_on_partitions_assigned(self, partitions)` (line 534): Handle partition assignment during rebalance.
    - `async close(self)` (line 543): no docstring
    - `async run(self)` (line 572): no docstring
    - `async _poll_message(self, timeout)` (line 582): no docstring
    - `_parse_payload(self, payload)` (line 588): no docstring
    - `_registry_key(self, payload)` (line 591): no docstring
    - `async _process_payload(self, payload)` (line 598): no docstring
    - `_fallback_metadata(self, payload)` (line 613): no docstring
    - `_span_name(self, payload)` (line 627): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 630): no docstring
    - `_metric_event_name(self, payload)` (line 651): no docstring
    - `_heartbeat_options(self)` (line 654): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 659): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 668): no docstring
    - `async _persist_objectify_failure_status(self, job, status, error, attempt_count, retryable, completed_at)` (line 677): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 710): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 735): no docstring
    - `async _process_job(self, job)` (line 757): no docstring
    - `async _bulk_create_instances(self, job, instances, ontology_version, objectify_pk_fields, objectify_instance_id_field)` (line 1716): no docstring
    - `async _bulk_update_instances(self, job, updates, ontology_version)` (line 1757): no docstring
    - `async _iter_class_instance_ids(self, db_name, class_id, branch, limit)` (line 1793): no docstring
    - `async _resolve_artifact_output(self, job)` (line 1832): no docstring
    - `async _fetch_target_field_types(self, job)` (line 1877): no docstring
    - `async _fetch_class_schema(self, job)` (line 1901): no docstring
    - `async _fetch_object_type_contract(self, job)` (line 1912): no docstring
    - `async _fetch_value_type_defs(self, job, value_type_refs)` (line 1925): no docstring
    - `async _fetch_ontology_version(self, job)` (line 1960): no docstring
    - `async _fetch_ontology_head_commit(self, job)` (line 1980): no docstring
    - `_normalize_pk_fields(value)` (line 1998): no docstring
    - `_hash_payload(payload)` (line 2008): no docstring
    - `_derive_row_key(self, columns, col_index, row, instance, pk_fields, pk_targets)` (line 2012): no docstring
    - `_derive_unique_key(self, instance, key_fields)` (line 2042): no docstring
    - `async _iter_dataset_batches(self, job, options, row_batch_size, max_rows)` (line 2050): no docstring
    - `async _iter_csv_batches(self, bucket, key, delimiter, has_header, row_batch_size, max_rows)` (line 2090): no docstring
    - `async _iter_json_part_batches(self, bucket, prefix, row_batch_size, max_rows)` (line 2182): no docstring
    - `async _iter_dataset_batches_incremental(self, job, options, row_batch_size, max_rows, mapping_spec)` (line 2238): Iterate dataset batches with incremental filtering.
    - `async _update_watermark_after_job(self, job, new_watermark)` (line 2343): Update watermark in registry after successful incremental job.
    - `_build_instances_with_validation(self, columns, rows, row_offset, mappings, relationship_mappings, relationship_meta, target_field_types, mapping_sources, sources_by_target, required_targets, pk_targets, pk_fields, field_constraints, field_raw_types, seen_row_keys)` (line 2372): no docstring
    - `async _run_link_index_job(self, job, mapping_spec, options, mappings, mapping_sources, mapping_targets, sources_by_target, prop_map, rel_map, relationship_mappings, stable_seed, row_batch_size, max_rows)` (line 2599): no docstring
    - `async _validate_batches(self, job, options, mappings, relationship_mappings, relationship_meta, target_field_types, mapping_sources, sources_by_target, required_targets, pk_targets, pk_fields, field_constraints, field_raw_types, row_batch_size, max_rows)` (line 3181): no docstring
    - `async _scan_key_constraints(self, job, options, mappings, relationship_meta, target_field_types, sources_by_target, required_targets, pk_targets, pk_fields, unique_keys, row_batch_size, max_rows)` (line 3249): no docstring
    - `_ensure_instance_ids(self, instances, class_id, stable_seed, mapping_spec_version, row_keys, instance_id_field)` (line 3371): no docstring
    - `async _record_lineage_header(self, job, mapping_spec, ontology_version, input_type, artifact_output_name)` (line 3403): no docstring
    - `async _record_instance_lineage(self, job, job_node_id, instance_ids, mapping_spec_id, mapping_spec_version, ontology_version, limit_remaining, input_type, artifact_output_name)` (line 3485): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 3565): no docstring
    - `_is_retryable_error_impl(exc)` (line 3595): no docstring

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
  - `ValidatedDatabaseName(db_name)` (line 243): 데이터베이스 이름 검증 의존성 - Modernized version
  - `ValidatedClassId(class_id)` (line 254): 클래스 ID 검증 의존성 - Modernized version
  - `async ensure_database_exists(db_name, terminus)` (line 266): 데이터베이스 존재 확인 및 검증된 이름 반환 - Modernized version
  - `async check_oms_dependencies_health(container)` (line 304): Check health of all OMS dependencies
- **Classes**
  - `OMSDependencyProvider` (line 51): Modern dependency provider for OMS services
    - `async get_terminus_service(container)` (line 60): Get AsyncTerminusService from container
    - `async get_jsonld_converter(container)` (line 90): Get JSON-LD converter from container
    - `async get_label_mapper(container)` (line 114): Get label mapper from container
    - `async get_event_store(container)` (line 138): Get S3/MinIO Event Store - The Single Source of Truth.
    - `async get_command_status_service(container)` (line 148): Get command status service from container
    - `async get_processed_event_registry(container)` (line 209): no docstring

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
  - `Property` (line 14): 속성 엔티티
    - `validate_value(self, value)` (line 25): 값 유효성 검증
  - `Relationship` (line 57): 관계 엔티티
    - `is_valid_cardinality(self)` (line 68): 카디널리티 유효성 확인
  - `Ontology` (line 75): 온톨로지 엔티티
    - `__post_init__(self)` (line 89): 초기화 후 처리
    - `add_property(self, property)` (line 96): 속성 추가
    - `remove_property(self, property_name)` (line 103): 속성 제거
    - `add_relationship(self, relationship)` (line 112): 관계 추가
    - `validate(self)` (line 121): 엔티티 유효성 검증
    - `to_dict(self)` (line 143): 딕셔너리로 변환

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
  - `async lifespan(app)` (line 345): Modern application lifecycle management
  - `async get_terminus_service()` (line 439): Get TerminusDB service from OMS container
  - `async root()` (line 448): 루트 엔드포인트 - Modernized version
  - `async health_check()` (line 468): 헬스 체크 - Modernized version
  - `async container_health_check()` (line 554): Health check for the modernized container system
- **Classes**
  - `OMSServiceContainer` (line 88): OMS-specific service container to manage OMS services
    - `__init__(self, container, settings)` (line 96): no docstring
    - `async initialize_oms_services(self)` (line 101): Initialize OMS-specific services
    - `async _initialize_event_store(self)` (line 131): Initialize S3/MinIO Event Store - The Single Source of Truth.
    - `async _initialize_terminus_service(self)` (line 151): Initialize TerminusDB service with health check
    - `async _initialize_postgres(self)` (line 177): Initialize Postgres MVCC pool (required for pull requests/proposals).
    - `async _initialize_jsonld_converter(self)` (line 187): Initialize JSON-LD converter
    - `async _initialize_label_mapper(self)` (line 193): Initialize label mapper
    - `async _initialize_redis_and_command_status(self)` (line 199): Initialize Redis service and Command Status service
    - `async _initialize_elasticsearch(self)` (line 224): Initialize Elasticsearch service
    - `async _initialize_rate_limiter(self)` (line 243): Initialize rate limiting service
    - `async shutdown_oms_services(self)` (line 259): Shutdown OMS-specific services
    - `get_terminus_service(self)` (line 309): Get TerminusDB service instance
    - `get_jsonld_converter(self)` (line 315): Get JSON-LD converter instance
    - `get_label_mapper(self)` (line 321): Get label mapper instance
    - `get_redis_service(self)` (line 327): Get Redis service instance (can be None)
    - `get_command_status_service(self)` (line 331): Get command status service instance (can be None)
    - `get_elasticsearch_service(self)` (line 335): Get Elasticsearch service instance (can be None)

### `backend/oms/middleware/__init__.py`

### `backend/oms/middleware/auth.py`
- **Functions**
  - `ensure_oms_auth_configured()` (line 15): no docstring
  - `install_oms_auth_middleware(app)` (line 31): no docstring

### `backend/oms/routers/__init__.py`

### `backend/oms/routers/action_async.py`
- **Functions**
  - `_resolve_writeback_target(db_name, raw_target)` (line 160): no docstring
  - `async submit_action_async(db_name, action_type_id, request, base_branch, terminus, event_store)` (line 181): Submit an Action for async execution.
  - `async simulate_action_async(db_name, action_type_id, request, terminus)` (line 438): Simulate an Action writeback (dry-run).
- **Classes**
  - `ActionSubmitRequest` (line 69): no docstring
  - `ActionSubmitResponse` (line 80): no docstring
  - `ActionSimulateScenarioRequest` (line 91): no docstring
  - `ActionSimulateStatePatch` (line 99): Patch-like state override for decision simulation (what-if).
    - `_reject_delete(cls, value)` (line 110): no docstring
  - `ActionSimulateObservedBaseOverrides` (line 116): Override observed_base snapshot fields/links to simulate stale reads.
  - `ActionSimulateTargetAssumption` (line 123): no docstring
  - `ActionSimulateAssumptions` (line 130): no docstring
  - `ActionSimulateRequest` (line 137): no docstring

### `backend/oms/routers/branch.py`
- **Functions**
  - `async list_branches(db_name, terminus)` (line 31): 브랜치 목록 조회
  - `async create_branch(db_name, request, terminus, elasticsearch_service)` (line 74): 새 브랜치 생성
  - `async delete_branch(db_name, branch_name, force, terminus, elasticsearch_service)` (line 157): 브랜치 삭제
  - `async checkout(db_name, request, terminus)` (line 237): 브랜치 또는 커밋 체크아웃
  - `async get_branch_info(db_name, branch_name, terminus)` (line 313): 브랜치 정보 조회
  - `async commit_changes(db_name, request, terminus)` (line 377): 브랜치에 변경사항 커밋
- **Classes**
  - `CommitRequest` (line 367): 커밋 요청 모델

### `backend/oms/routers/command_status.py`
- **Functions**
  - `async _fallback_from_registry(command_uuid, registry)` (line 28): no docstring
  - `async get_command_status(command_id, command_status_service, processed_event_registry, event_store)` (line 72): Get command execution status/result.

### `backend/oms/routers/database.py`
- **Functions**
  - `async list_databases(terminus_service)` (line 29): 데이터베이스 목록 조회
  - `async create_database(request, event_store, command_status_service, terminus_service)` (line 50): 새 데이터베이스 생성
  - `async delete_database(db_name, expected_seq, event_store, command_status_service)` (line 200): 데이터베이스 삭제
  - `async database_exists(db_name, terminus_service)` (line 298): 데이터베이스 존재 여부 확인

### `backend/oms/routers/instance.py`
- **Functions**
  - `async get_class_instances(db_name, class_id, limit, offset, branch, search, terminus)` (line 27): 특정 클래스의 인스턴스 목록을 효율적으로 조회
  - `async get_instance(db_name, instance_id, class_id, terminus)` (line 123): 개별 인스턴스를 효율적으로 조회
  - `async get_class_instance_count(db_name, class_id, terminus)` (line 186): 특정 클래스의 인스턴스 개수를 효율적으로 조회
  - `async execute_sparql_query(db_name, query, limit, offset, terminus)` (line 236): SPARQL 쿼리 직접 실행

### `backend/oms/routers/instance_async.py`
- **Functions**
  - `_enforce_ingest_only_if_writeback_enabled(class_id, metadata)` (line 51): no docstring
  - `async _fallback_from_registry(command_uuid, registry)` (line 76): no docstring
  - `async _append_command_event(command, event_store, topic, actor)` (line 116): Store command as an immutable event in S3/MinIO for the publisher to relay to Kafka.
  - `_derive_instance_id(class_id, payload)` (line 137): Derive a stable instance_id for CREATE_INSTANCE so command/event aggregate_id matches.
  - `async create_instance_async(db_name, class_id, branch, request, terminus, command_status_service, event_store, user_id)` (line 190): 인스턴스 생성 명령을 비동기로 처리
  - `async update_instance_async(db_name, class_id, instance_id, branch, expected_seq, request, terminus, command_status_service, event_store, user_id)` (line 310): 인스턴스 수정 명령을 비동기로 처리
  - `async delete_instance_async(db_name, class_id, instance_id, branch, expected_seq, request, terminus, command_status_service, event_store, user_id)` (line 426): 인스턴스 삭제 명령을 비동기로 처리
  - `async bulk_create_instances_async(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 541): 대량 인스턴스 생성 명령을 비동기로 처리
  - `async bulk_update_instances_async(db_name, class_id, branch, request, terminus, command_status_service, event_store, user_id)` (line 669): 대량 인스턴스 수정 명령을 비동기로 처리
  - `async get_instance_command_status(db_name, command_id, command_status_service, processed_event_registry, event_store)` (line 784): 인스턴스 명령의 상태 조회
  - `async _track_bulk_create_progress(command_id, total_instances, command_status_service)` (line 874): Track progress of bulk create operation in background.
  - `async bulk_create_instances_with_tracking(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 940): Enhanced bulk instance creation with proper background task tracking.
  - `async _process_bulk_create_in_background(task_id, db_name, class_id, branch, instances, metadata, user_id, ontology_version, event_store, command_status_service)` (line 995): Process bulk create operation in background with proper error handling.
- **Classes**
  - `InstanceCreateRequest` (line 159): 인스턴스 생성 요청
  - `InstanceUpdateRequest` (line 165): 인스턴스 수정 요청
  - `InstanceDeleteRequest` (line 171): 인스턴스 삭제 요청 (optional body for metadata/ingest marker).
  - `BulkInstanceCreateRequest` (line 177): 대량 인스턴스 생성 요청
  - `BulkInstanceUpdateRequest` (line 183): 대량 인스턴스 수정 요청

### `backend/oms/routers/ontology.py`
- **Functions**
  - `_is_protected_branch(branch)` (line 92): no docstring
  - `_require_proposal_for_branch(branch)` (line 96): no docstring
  - `_reject_direct_write_if_required(branch)` (line 102): no docstring
  - `_admin_authorized(request)` (line 113): no docstring
  - `_extract_change_reason(request)` (line 123): no docstring
  - `_extract_actor(request)` (line 128): no docstring
  - `async _collect_interface_issues(terminus, db_name, branch, ontology_id, metadata, properties, relationships, resource_service)` (line 133): no docstring
  - `_extract_shared_property_refs(metadata)` (line 172): no docstring
  - `_extract_group_refs(metadata)` (line 198): no docstring
  - `async _validate_group_refs(terminus, db_name, branch, metadata, resource_service)` (line 225): no docstring
  - `async _apply_shared_properties(terminus, db_name, branch, properties, metadata, resource_service)` (line 250): no docstring
  - `async _validate_value_type_refs(terminus, db_name, branch, properties, resource_service)` (line 317): no docstring
  - `_is_internal_ontology(ontology)` (line 374): no docstring
  - `_localized_to_string(value, lang)` (line 383): no docstring
  - `_merge_lint_reports(*reports)` (line 405): no docstring
  - `_relationship_validation_enabled(flag)` (line 427): no docstring
  - `async _validate_relationships_gate(terminus, db_name, branch, ontology_payload, enabled)` (line 434): no docstring
  - `async _ensure_database_exists(db_name, terminus)` (line 460): 데이터베이스 존재 여부 확인 후 404 예외 발생
  - `async create_ontology(ontology_request, request, db_name, branch, terminus, event_store, command_status_service)` (line 488): 내부 ID 기반 온톨로지 생성
  - `async validate_ontology_create(ontology_request, request, db_name, branch, terminus)` (line 838): 온톨로지 생성 검증 (no write).
  - `async validate_ontology_update(ontology_data, request, db_name, class_id, branch, terminus)` (line 941): 온톨로지 업데이트 검증 (no write).
  - `async list_ontologies(db_name, branch, class_type, limit, offset, terminus, label_mapper)` (line 1058): 내부 ID 기반 온톨로지 목록 조회
  - `async analyze_relationship_network(db_name, terminus)` (line 1135): 🔥 관계 네트워크 종합 분석 엔드포인트
  - `async get_ontology(db_name, class_id, branch, terminus, converter, label_mapper)` (line 1166): 내부 ID 기반 온톨로지 조회
  - `async update_ontology(ontology_data, request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 1251): 내부 ID 기반 온톨로지 업데이트
  - `async delete_ontology(request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 1603): 내부 ID 기반 온톨로지 삭제
  - `async query_ontologies(query, db_name, branch, terminus)` (line 1737): 내부 ID 기반 온톨로지 쿼리
  - `async create_ontology_with_advanced_relationships(ontology_request, request, db_name, branch, auto_generate_inverse, validate_relationships, check_circular_references, terminus, event_store, command_status_service)` (line 1836): 🔥 고급 관계 관리 기능을 포함한 온톨로지 생성
  - `async validate_ontology_relationships(request, db_name, terminus)` (line 2135): 🔥 온톨로지 관계 검증 전용 엔드포인트
  - `async detect_circular_references(db_name, new_ontology, terminus)` (line 2181): 🔥 순환 참조 탐지 전용 엔드포인트
  - `async find_relationship_paths(start_entity, db_name, end_entity, max_depth, path_type, terminus)` (line 2229): 🔥 관계 경로 탐색 엔드포인트
  - `async get_reachable_entities(start_entity, db_name, max_depth, terminus)` (line 2288): 🔥 도달 가능한 엔티티 조회 엔드포인트

### `backend/oms/routers/ontology_extensions.py`
- **Functions**
  - `async _get_pr_service()` (line 98): no docstring
  - `_normalize_resource_payload(payload)` (line 107): no docstring
  - `_extract_value_type_spec(payload)` (line 127): no docstring
  - `_validate_value_type_immutability(existing, incoming)` (line 135): no docstring
  - `_resource_validation_strict()` (line 152): no docstring
  - `_require_health_gate(branch)` (line 156): no docstring
  - `_ensure_branch_writable(branch)` (line 162): no docstring
  - `async _assert_expected_head_commit(terminus, db_name, branch, expected_head_commit)` (line 173): no docstring
  - `async _compute_ontology_health(db_name, branch, terminus)` (line 211): no docstring
  - `async list_resources(db_name, resource_type, branch, limit, offset, terminus)` (line 463): no docstring
  - `async list_resources_by_type(db_name, resource_type, branch, limit, offset, terminus)` (line 497): no docstring
  - `async create_resource(db_name, resource_type, payload, branch, expected_head_commit, terminus)` (line 516): no docstring
  - `async get_resource(db_name, resource_type, resource_id, branch, terminus)` (line 579): no docstring
  - `async update_resource(db_name, resource_type, resource_id, payload, branch, expected_head_commit, terminus)` (line 615): no docstring
  - `async delete_resource(db_name, resource_type, resource_id, branch, expected_head_commit, terminus)` (line 682): no docstring
  - `async list_ontology_branches(db_name, terminus)` (line 722): no docstring
  - `async create_ontology_branch(db_name, request, terminus)` (line 739): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, pr_service)` (line 762): no docstring
  - `async create_ontology_proposal(db_name, request, pr_service)` (line 783): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, pr_service, terminus)` (line 811): no docstring
  - `async deploy_ontology(db_name, request, pr_service, terminus)` (line 886): no docstring
  - `async ontology_health(db_name, branch, terminus)` (line 988): no docstring
  - `_validation_result_to_issue(result)` (line 1015): no docstring
  - `_resource_is_referenced(resource_type, resource_id, references)` (line 1025): no docstring
  - `_resource_ref(resource_type, resource_id)` (line 1042): no docstring
  - `_build_issue(code, severity, resource_ref, details, suggested_fix, message, source)` (line 1046): no docstring
  - `_normalize_issue(issue, source)` (line 1067): no docstring
  - `_resolve_relationship_resource_ref(result, class_ids)` (line 1088): no docstring
- **Classes**
  - `OntologyResourceRequest` (line 67): no docstring
  - `OntologyProposalRequest` (line 77): no docstring
  - `OntologyDeployRequest` (line 85): no docstring
  - `OntologyApproveRequest` (line 93): no docstring

### `backend/oms/routers/pull_request.py`
- **Functions**
  - `async get_pr_service()` (line 72): Get PullRequestService instance with MVCC support
  - `async create_pull_request(db_name, request, pr_service)` (line 85): Create a new pull request
  - `async get_pull_request(db_name, pr_id, pr_service)` (line 138): Get pull request details
  - `async list_pull_requests(db_name, status, limit, pr_service)` (line 184): List pull requests for a database
  - `async merge_pull_request(db_name, pr_id, request, pr_service)` (line 236): Merge a pull request
  - `async close_pull_request(db_name, pr_id, request, pr_service)` (line 286): Close a pull request without merging
  - `async get_pull_request_diff(db_name, pr_id, refresh, pr_service)` (line 328): Get diff for a pull request
- **Classes**
  - `PRCreateRequest` (line 30): Pull Request creation request model
  - `PRMergeRequest` (line 51): Pull Request merge request model
  - `PRCloseRequest` (line 66): Pull Request close request model

### `backend/oms/routers/query.py`
- **Functions**
  - `async get_elasticsearch()` (line 40): Get Elasticsearch client
  - `async execute_simple_query(db_name, query, es_client)` (line 63): Execute simple SQL-like query against Elasticsearch
  - `async execute_woql_query(db_name, query, terminus, es_client)` (line 172): Execute WOQL query for graph analysis
  - `async list_instances(db_name, class_id, limit, offset, es_client)` (line 211): List instances of a class from Elasticsearch
- **Classes**
  - `SimpleQuery` (line 29): Simple SQL-like query
  - `WOQLQuery` (line 34): WOQL query for graph analysis

### `backend/oms/routers/tasks.py`
- **Functions**
  - `async get_internal_task_status(task_id, task_manager)` (line 21): Get internal task status for monitoring.
  - `async get_active_tasks(task_manager)` (line 50): Get all active (running) tasks.
  - `async cleanup_old_tasks(older_than_days, task_manager, redis_service)` (line 90): Clean up old completed tasks.
  - `async task_service_health(task_manager)` (line 124): Get task service health status.

### `backend/oms/routers/version.py`
- **Functions**
  - `_rollback_enabled()` (line 77): Rollback is effectively a "force-push/reset" of the ontology graph.
  - `async get_branch_head_commit(db_name, branch, terminus)` (line 92): 브랜치 HEAD 커밋 ID 조회
  - `async create_commit(db_name, request, terminus)` (line 142): 변경사항 커밋
  - `async get_commit_history(db_name, branch, limit, offset, terminus)` (line 199): 커밋 히스토리 조회
  - `async get_diff(db_name, from_ref, to_ref, terminus)` (line 266): 차이점 조회
  - `async merge_branches(db_name, request, terminus)` (line 321): 브랜치 머지
  - `async rollback(db_name, request, audit_store, branch, terminus)` (line 407): 변경사항 롤백
  - `async rebase_branch(db_name, onto, branch, terminus)` (line 582): 브랜치 리베이스
  - `async get_common_ancestor(db_name, branch1, branch2, terminus)` (line 647): 두 브랜치의 공통 조상 찾기
- **Classes**
  - `CommitRequest` (line 32): 커밋 요청
  - `MergeRequest` (line 43): 머지 요청
  - `RollbackRequest` (line 61): 롤백 요청

### `backend/oms/services/__init__.py`

### `backend/oms/services/action_simulation_service.py`
- **Functions**
  - `_enterprise_payload_for_error(error_key)` (line 57): no docstring
  - `_attach_enterprise(payload)` (line 81): no docstring
  - `_apply_changes_to_payload(payload, changes)` (line 91): no docstring
  - `_assumption_is_forbidden_field(field)` (line 159): no docstring
  - `_extract_link_fields(ops)` (line 168): no docstring
  - `_extract_patch_field_lists(patch)` (line 188): no docstring
  - `_apply_assumption_patch(scope, base_state, patch)` (line 211): no docstring
  - `_apply_observed_base_overrides(observed_base, overrides)` (line 264): no docstring
  - `_coerce_overlay_branch(db_name, writeback_target, overlay_branch)` (line 386): no docstring
  - `async enforce_action_permission(db_name, submitted_by, submitted_by_type, action_spec)` (line 395): no docstring
  - `async _check_writeback_dataset_acl_alignment(db_name, submitted_by, submitted_by_type, actor_role, ontology_commit_id, resources, dataset_registry, class_ids)` (line 433): no docstring
  - `async preflight_action_writeback(terminus, base_storage, dataset_registry, db_name, action_type_id, ontology_commit_id, action_spec, action_type_rid, input_payload, assumptions, submitted_by, submitted_by_type, actor_role, base_branch, overlay_branch)` (line 683): no docstring
  - `build_patchset_for_scenario(preflight, action_log_id, conflict_policy_override)` (line 1159): no docstring
  - `async simulate_effects_for_patchset(base_storage, lakefs_storage, db_name, base_branch, overlay_branch, writeback_repo, writeback_branch, action_log_id, patchset_id, targets, base_overrides_by_target)` (line 1247): no docstring
- **Classes**
  - `ActionSimulationRejected` (line 50): no docstring
    - `__init__(self, payload, status_code)` (line 51): no docstring
  - `ActionSimulationScenario` (line 347): no docstring
  - `TargetPreflight` (line 353): no docstring
  - `ActionPreflight` (line 370): no docstring

### `backend/oms/services/async_terminus.py`
- **Classes**
  - `AtomicUpdateError` (line 63): Base exception for atomic update operations
  - `PatchUpdateError` (line 67): Exception for PATCH-based update failures
  - `TransactionUpdateError` (line 71): Exception for transaction-based update failures
  - `WOQLUpdateError` (line 75): Exception for WOQL-based update failures
  - `BackupCreationError` (line 79): Exception for backup creation failures
  - `RestoreError` (line 83): Exception for restore operation failures
  - `BackupRestoreError` (line 87): Exception for backup and restore operation failures
  - `AsyncTerminusService` (line 106): 비동기 TerminusDB 서비스 클래스 - Clean Facade Pattern
    - `__init__(self, connection_info)` (line 121): 초기화
    - `async check_connection(self)` (line 163): 연결 상태 확인
    - `async connect(self)` (line 176): 연결 설정
    - `async disconnect(self)` (line 182): 연결 해제
    - `async close(self)` (line 186): 모든 서비스 종료
    - `async create_database(self, db_name, description)` (line 212): 데이터베이스 생성
    - `async database_exists(self, db_name)` (line 224): 데이터베이스 존재 여부 확인
    - `async list_databases(self)` (line 228): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 233): 데이터베이스 삭제
    - `async execute_query(self, db_name, query_dict, branch)` (line 241): Query execution (query-spec or raw WOQL passthrough).
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 247): SPARQL 쿼리 직접 실행
    - `async get_class_instances_optimized(self, db_name, class_id, branch, limit, offset, filter_conditions)` (line 261): 특정 클래스의 모든 인스턴스를 효율적으로 조회
    - `async get_instance_optimized(self, db_name, instance_id, branch, class_id)` (line 285): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id, branch, filter_conditions)` (line 300): 특정 클래스의 인스턴스 개수를 효율적으로 조회
    - `async get_ontology(self, db_name, class_id, raise_if_missing, branch)` (line 319): 온톨로지 조회 (branch-aware).
    - `async create_ontology(self, db_name, ontology_data, branch)` (line 337): 온톨로지 생성
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references)` (line 350): no docstring
    - `async update_ontology(self, db_name, class_id, ontology_data, branch)` (line 421): 온톨로지 업데이트 - Atomic 버전
    - `async delete_ontology(self, db_name, class_id, branch)` (line 432): 온톨로지 삭제
    - `async list_ontology_classes(self, db_name)` (line 436): 데이터베이스의 모든 온톨로지 목록 조회
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 445): 브랜치 생성
    - `async list_branches(self, db_name)` (line 450): 브랜치 목록 조회
    - `async get_current_branch(self, db_name)` (line 457): 현재 브랜치 (best-effort, 기본값: main)
    - `async delete_branch(self, db_name, branch_name)` (line 461): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 465): 브랜치 체크아웃
    - `async checkout(self, db_name, target, target_type)` (line 472): Router 호환 checkout (branch/commit).
    - `async merge_branches(self, db_name, source_branch, target_branch, message, author)` (line 479): 브랜치 병합
    - `async commit(self, db_name, message, author, branch)` (line 496): 커밋 생성
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 506): 커밋 히스토리 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 516): 차이점 조회
    - `async merge(self, db_name, source_branch, target_branch, strategy)` (line 520): Router 호환 merge API.
    - `async rebase(self, db_name, onto, branch, message)` (line 536): Router 호환 rebase API (branch -> onto).
    - `async rollback(self, db_name, target)` (line 547): Router 호환 rollback API (reset current branch to target).
    - `async find_common_ancestor(self, db_name, branch1, branch2)` (line 552): 공통 조상 찾기 (현재는 best-effort 미구현).
    - `async create_instance(self, db_name, class_id, instance_data, branch)` (line 562): 인스턴스 생성
    - `async update_instance(self, db_name, class_id, instance_id, update_data, branch)` (line 573): 인스턴스 업데이트
    - `async delete_instance(self, db_name, class_id, instance_id, branch)` (line 587): 인스턴스 삭제
    - `async validate_relationships(self, db_name, ontology_data, branch, fix_issues)` (line 602): Validate ontology relationships against current schema (no write).
    - `async detect_circular_references(self, db_name, branch, include_new_ontology, max_cycle_depth)` (line 679): Detect circular references across ontology relationship graph (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch)` (line 751): Find relationship paths between entities in the ontology graph (no write).
    - `async analyze_relationship_network(self, db_name, branch)` (line 828): Analyze relationship network health and statistics (no write).
    - `convert_properties_to_relationships(self, ontology)` (line 923): 속성을 관계로 변환
    - `clear_cache(self, db_name)` (line 934): 캐시 초기화
    - `async ping(self)` (line 941): 서버 연결 상태 확인
    - `get_connection_info(self)` (line 950): 현재 연결 정보 반환
    - `async __aenter__(self)` (line 954): Async context manager entry
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 958): Async context manager exit

### `backend/oms/services/event_store.py`

### `backend/oms/services/ontology_deploy_outbox.py`
- **Functions**
  - `async run_ontology_deploy_outbox_worker(registry, poll_interval_seconds, batch_size, stop_event)` (line 100): no docstring
- **Classes**
  - `OntologyDeployOutboxPublisher` (line 18): no docstring
    - `__init__(self, registry, batch_size)` (line 19): no docstring
    - `_next_attempt_at(self, attempts)` (line 45): no docstring
    - `async flush_once(self)` (line 49): no docstring
    - `async maybe_purge(self)` (line 82): no docstring

### `backend/oms/services/ontology_deployment_registry.py`
- **Classes**
  - `OntologyDeployOutboxItem` (line 21): no docstring
  - `OntologyDeploymentRegistry` (line 35): Record ontology deployments in Postgres.
    - `async ensure_schema(self)` (line 38): no docstring
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, occurred_at)` (line 98): no docstring
    - `async record_deployment(self, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, status, metadata)` (line 142): no docstring
    - `async claim_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 215): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 285): no docstring
    - `async mark_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 300): no docstring
    - `async purge_outbox(self, retention_days, limit)` (line 321): no docstring

### `backend/oms/services/ontology_deployment_registry_v2.py`
- **Classes**
  - `OntologyDeployOutboxItem` (line 22): no docstring
  - `OntologyDeploymentRegistryV2` (line 36): Record ontology deployments in Postgres (v2 schema).
    - `_json_default(value)` (line 40): no docstring
    - `_maybe_decode_json(value)` (line 46): no docstring
    - `async ensure_schema(self)` (line 54): no docstring
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, target_branch, ontology_commit_id, snapshot_rid, deployed_by, gate_policy, health_summary, occurred_at)` (line 115): no docstring
    - `async record_deployment(self, db_name, target_branch, ontology_commit_id, snapshot_rid, proposal_id, status, gate_policy, health_summary, deployed_by, error, metadata)` (line 159): no docstring
    - `async get_latest_deployed_commit(self, db_name, target_branch)` (line 246): Return the latest succeeded deployment record for a db/branch.
    - `async claim_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 289): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 361): no docstring
    - `async mark_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 376): no docstring
    - `async purge_outbox(self, retention_days, limit)` (line 397): no docstring

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
  - `extract_interface_refs(metadata)` (line 12): no docstring
  - `strip_interface_prefix(value)` (line 33): no docstring
  - `collect_interface_contract_issues(ontology_id, metadata, properties, relationships, interface_index)` (line 42): no docstring
  - `extract_required_entries(items, name_keys)` (line 181): no docstring
  - `extract_entry_value(entry, keys)` (line 203): no docstring
  - `build_property_map(items)` (line 213): no docstring
  - `build_relationship_map(items)` (line 226): no docstring
  - `extract_property_type(item)` (line 239): no docstring
  - `extract_relationship_target(item)` (line 249): no docstring
  - `normalize_reference_value(value)` (line 259): no docstring

### `backend/oms/services/ontology_resource_validator.py`
- **Functions**
  - `_normalize_spec(spec)` (line 111): no docstring
  - `_merge_payload_spec(payload)` (line 124): no docstring
  - `_extract_reference_values(value, keys, parent_is_ref)` (line 135): no docstring
  - `collect_reference_values(spec)` (line 148): no docstring
  - `check_required_fields(resource_type, spec)` (line 152): no docstring
  - `async find_missing_references(db_name, resource_type, payload, terminus, branch)` (line 157): no docstring
  - `_canonicalize_ref(raw)` (line 193): no docstring
  - `_is_primitive_reference(value)` (line 203): no docstring
  - `_strip_object_ref(raw)` (line 217): no docstring
  - `_collect_link_type_issues(spec)` (line 232): no docstring
  - `_collect_relationship_spec_issues(spec)` (line 255): no docstring
  - `async _find_missing_link_type_refs(terminus, db_name, branch, spec)` (line 327): no docstring
  - `_validate_required_fields(resource_type, spec)` (line 346): no docstring
  - `_collect_required_field_issues(resource_type, spec)` (line 352): no docstring
  - `_collect_required_items_issues(items, item_name, name_keys)` (line 693): no docstring
  - `_collect_permission_policy_issues(policy)` (line 731): no docstring
  - `_validate_string_list(value, field_name)` (line 848): no docstring
  - `_append_spec_issue(issues, message, missing_fields, invalid_fields)` (line 858): no docstring
  - `async _reference_exists(terminus, resources, db_name, branch, ref_type, ref)` (line 877): no docstring
  - `async validate_resource(db_name, resource_type, payload, terminus, branch, expected_head_commit, strict)` (line 919): no docstring
- **Classes**
  - `ResourceSpecError` (line 21): Raised when resource spec is invalid or missing required fields.
  - `ResourceReferenceError` (line 25): Raised when resource spec references missing entities.

### `backend/oms/services/ontology_resources.py`
- **Functions**
  - `normalize_resource_type(value)` (line 50): no docstring
  - `_resource_doc_id(resource_type, resource_id)` (line 60): no docstring
  - `_localized_to_string(value)` (line 65): no docstring
- **Classes**
  - `OntologyResourceService` (line 90): CRUD for ontology resource instances stored in TerminusDB.
    - `__init__(self, terminus)` (line 93): no docstring
    - `async ensure_resource_schema(self, db_name, branch)` (line 97): no docstring
    - `async create_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 134): no docstring
    - `async update_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 162): no docstring
    - `async delete_resource(self, db_name, branch, resource_type, resource_id)` (line 199): no docstring
    - `async get_resource(self, db_name, branch, resource_type, resource_id)` (line 217): no docstring
    - `async list_resources(self, db_name, branch, resource_type, limit, offset)` (line 231): no docstring
    - `_payload_to_document(self, resource_type, resource_id, payload, doc_id, is_create, existing)` (line 259): no docstring
    - `_document_to_payload(self, doc)` (line 324): no docstring

### `backend/oms/services/property_to_relationship_converter.py`
- **Classes**
  - `PropertyToRelationshipConverter` (line 15): Property를 Relationship으로 자동 변환하는 컨버터
    - `__init__(self)` (line 23): no docstring
    - `process_class_data(self, class_data)` (line 26): 클래스 데이터를 처리하여 property를 relationship으로 자동 변환
    - `detect_class_references(self, properties)` (line 110): 속성 목록에서 클래스 참조를 감지
    - `validate_class_references(self, class_data, existing_classes)` (line 126): 클래스 참조의 유효성 검증
    - `generate_inverse_relationships(self, class_data)` (line 156): 자동 변환된 관계에 대한 역관계 생성 정보
    - `_inverse_cardinality(self, cardinality)` (line 182): 카디널리티 역변환

### `backend/oms/services/pull_request_service.py`
- **Functions**
  - `_maybe_decode_json(value)` (line 40): no docstring
- **Classes**
  - `PullRequestStatus` (line 32): PR status constants
  - `PullRequestService` (line 49): Pull Request management service following SRP
    - `__init__(self, mvcc_manager, *args, **kwargs)` (line 65): Initialize PullRequestService
    - `async create_pull_request(self, db_name, source_branch, target_branch, title, description, author)` (line 79): Create a new pull request
    - `async get_branch_diff(self, db_name, source_branch, target_branch)` (line 182): Get diff between two branches using TerminusDB diff API
    - `async check_merge_conflicts(self, db_name, source_branch, target_branch)` (line 210): Check for potential merge conflicts
    - `async merge_pull_request(self, pr_id, merge_message, author)` (line 258): Merge a pull request using rebase strategy
    - `async get_pull_request(self, pr_id)` (line 361): Get pull request details
    - `async list_pull_requests(self, db_name, status, limit)` (line 410): List pull requests with optional filters
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
    - `async __aenter__(self)` (line 303): 비동기 컨텍스트 매니저 진입
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 308): 비동기 컨텍스트 매니저 종료
    - `is_connected(self)` (line 312): 연결 상태 반환

### `backend/oms/services/terminus/database.py`
- **Classes**
  - `DatabaseService` (line 20): TerminusDB 데이터베이스 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 27): no docstring
    - `async database_exists(self, db_name)` (line 32): 데이터베이스 존재 여부 확인
    - `async ensure_db_exists(self, db_name, description)` (line 44): 데이터베이스 존재 확인 및 생성
    - `async create_database(self, db_name, description)` (line 68): 새 데이터베이스 생성
    - `async list_databases(self)` (line 109): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 201): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 233): 데이터베이스 상세 정보 조회
    - `clear_cache(self)` (line 262): 데이터베이스 캐시 초기화

### `backend/oms/services/terminus/document.py`
- **Classes**
  - `DocumentService` (line 21): TerminusDB 문서(인스턴스) 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 28): no docstring
    - `async disconnect(self)` (line 33): no docstring
    - `async create_document(self, db_name, document, graph_type, branch, author, message)` (line 40): 새 문서 생성
    - `async update_document(self, db_name, doc_id, document, graph_type, branch, author, message)` (line 111): 문서 업데이트
    - `async delete_document(self, db_name, doc_id, graph_type, branch, author, message)` (line 176): 문서 삭제
    - `async get_document(self, db_name, doc_id, graph_type, branch)` (line 231): 특정 문서 조회
    - `async list_documents(self, db_name, graph_type, branch, doc_type, limit, offset)` (line 275): 문서 목록 조회
    - `async document_exists(self, db_name, doc_id, graph_type, branch)` (line 332): 문서 존재 여부 확인
    - `async bulk_create_documents(self, db_name, documents, graph_type, branch, author, message)` (line 356): 여러 문서를 한 번에 생성
    - `async bulk_update_documents(self, db_name, documents, graph_type, branch, author, message)` (line 414): 여러 문서를 한 번에 업데이트
    - `async bulk_delete_documents(self, db_name, doc_ids, graph_type, branch, author, message)` (line 465): 여러 문서를 한 번에 삭제
    - `async search_documents(self, db_name, search_query, graph_type, branch, doc_type, limit)` (line 513): 문서 검색
    - `async create_instance(self, db_name, class_id, instance_data, branch)` (line 584): Create an instance document (alias for create_document)
    - `async update_instance(self, db_name, class_id, instance_id, update_data, branch)` (line 619): Update an instance document (alias for update_document)
    - `async delete_instance(self, db_name, class_id, instance_id, branch)` (line 653): Delete an instance document (alias for delete_document)

### `backend/oms/services/terminus/instance.py`
- **Classes**
  - `InstanceService` (line 17): TerminusDB 인스턴스 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 24): no docstring
    - `async disconnect(self)` (line 29): no docstring
    - `async get_class_instances_optimized(self, db_name, class_id, branch, limit, offset, search)` (line 36): 특정 클래스의 인스턴스 목록을 효율적으로 조회
    - `async get_instance_optimized(self, db_name, instance_id, branch, class_id)` (line 150): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id, branch, filter_conditions)` (line 252): 특정 클래스의 인스턴스 개수를 효율적으로 조회

### `backend/oms/services/terminus/ontology.py`
- **Classes**
  - `OntologyService` (line 27): TerminusDB 온톨로지 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 34): no docstring
    - `async disconnect(self)` (line 40): no docstring
    - `async _overlay_key_spec(self, db_name, branch, ontology)` (line 49): Overlay ordered key spec from Postgres registry into an ontology response.
    - `async create_ontology(self, db_name, ontology, branch)` (line 95): 새 온톨로지(클래스) 생성
    - `async update_ontology(self, db_name, ontology_id, ontology, branch)` (line 262): 온톨로지 업데이트
    - `async delete_ontology(self, db_name, ontology_id, branch)` (line 297): 온톨로지 삭제
    - `async get_ontology(self, db_name, ontology_id, branch)` (line 340): 특정 온톨로지 조회
    - `async list_ontologies(self, db_name, branch, limit, offset)` (line 383): 데이터베이스의 모든 온톨로지 목록 조회
    - `async ontology_exists(self, db_name, ontology_id, branch)` (line 489): 온톨로지 존재 여부 확인
    - `_create_property_schema(self, prop)` (line 511): 속성 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_create_relationship_schema(self, rel)` (line 566): 관계 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_map_datatype_to_terminus(self, datatype)` (line 631): DataType을 TerminusDB 타입으로 매핑
    - `_parse_ontology_document(self, doc)` (line 700): TerminusDB 문서를 OntologyResponse로 파싱
    - `_map_terminus_to_datatype(self, terminus_type)` (line 871): TerminusDB 타입을 DataType으로 매핑

### `backend/oms/services/terminus/query.py`
- **Classes**
  - `QueryService` (line 17): TerminusDB 쿼리 실행 서비스
    - `_normalize_field_name(field)` (line 25): no docstring
    - `_coerce_scalar(value)` (line 35): no docstring
    - `_as_list(value)` (line 41): no docstring
    - `_try_float(value)` (line 49): no docstring
    - `_matches_filters(self, doc, filters)` (line 61): no docstring
    - `async execute_query(self, db_name, query_dict, branch)` (line 153): Execute a query against instance documents.
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 313): SPARQL 쿼리 실행
    - `convert_to_woql(self, query_dict)` (line 384): 간단한 쿼리 딕셔너리를 WOQL 형식으로 변환

### `backend/oms/services/terminus/version_control.py`
- **Classes**
  - `VersionControlService` (line 21): TerminusDB 버전 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 28): no docstring
    - `async disconnect(self)` (line 33): Close nested services before closing this client's HTTP resources.
    - `_is_origin_branch_missing_error(exc)` (line 52): TerminusDB can transiently report `OriginBranchDoesNotExist` right after DB creation.
    - `_is_branch_already_exists_error(exc)` (line 61): no docstring
    - `_is_transient_request_handler_error(exc)` (line 70): TerminusDB occasionally returns a 500 "Unexpected failure in request handler" right after DB creation.
    - `_is_merge_endpoint_missing_error(exc)` (line 85): no docstring
    - `async _get_branch_head(self, db_name, branch_name)` (line 96): no docstring
    - `async _get_branch_head_with_retries(self, db_name, branch_name, max_attempts)` (line 112): no docstring
    - `async list_branches(self, db_name)` (line 122): 데이터베이스의 모든 브랜치 목록 조회
    - `async create_branch(self, db_name, branch_name, source_branch, empty)` (line 170): 새 브랜치 생성
    - `async delete_branch(self, db_name, branch_name)` (line 245): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 281): TerminusDB는 stateless HTTP API이므로 'checkout'은 서버 상태를 바꾸지 않습니다.
    - `async reset_branch(self, db_name, branch_name, commit_id)` (line 291): 브랜치를 특정 커밋으로 리셋
    - `async get_commits(self, db_name, branch_name, limit, offset)` (line 340): 브랜치의 커밋 이력 조회
    - `async create_commit(self, db_name, branch_name, message, author)` (line 411): 새 커밋 생성
    - `async rebase_branch(self, db_name, branch_name, target_branch, message)` (line 489): 브랜치 리베이스
    - `async rebase(self, db_name, branch, onto, message)` (line 539): Router 호환 rebase API (branch -> onto).
    - `async squash_commits(self, db_name, branch_name, commit_id, message)` (line 550): 커밋 스쿼시 (여러 커밋을 하나로 합치기)
    - `async get_diff(self, db_name, from_ref, to_ref)` (line 594): 두 참조(브랜치/커밋) 간의 차이점 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 643): Router 호환 diff API: changes(리스트/딕트)만 반환.
    - `async commit(self, db_name, message, author, branch)` (line 650): Router 호환 commit API: commit_id 문자열 반환.
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 662): Router 호환 commit history API.
    - `async merge(self, db_name, source_branch, target_branch, strategy, author, message)` (line 672): Router 호환 merge API.
    - `_parse_commit_info(self, commit_data)` (line 749): 커밋 정보 파싱

### `backend/oms/utils/__init__.py`

### `backend/oms/utils/circular_reference_detector.py`
- **Classes**
  - `CycleType` (line 17): 순환 참조 유형
  - `CycleInfo` (line 27): 순환 참조 정보
  - `RelationshipEdge` (line 40): 관계 그래프의 엣지
  - `CircularReferenceDetector` (line 50): 🔥 THINK ULTRA! 순환 참조 탐지기
    - `__init__(self, max_cycle_depth)` (line 63): 초기화
    - `build_relationship_graph(self, ontologies)` (line 93): 온톨로지들로부터 관계 그래프 구축
    - `detect_all_cycles(self)` (line 141): 모든 순환 참조 탐지
    - `detect_cycle_for_new_relationship(self, source, target, predicate)` (line 168): 새로운 관계 추가 시 발생할 수 있는 순환 참조 탐지
    - `_detect_self_references(self)` (line 231): 자기 참조 탐지
    - `_detect_direct_cycles(self)` (line 254): 직접 순환 탐지 (A -> B -> A)
    - `_detect_indirect_cycles(self)` (line 284): 간접 순환 탐지 (A -> B -> C -> A)
    - `_detect_complex_cycles(self)` (line 308): 복잡한 다중 경로 순환 탐지
    - `_dfs_cycle_detection(self, current, start, visited, path_stack, predicate_stack, depth)` (line 335): DFS를 사용한 순환 탐지
    - `_find_strongly_connected_components(self)` (line 388): Tarjan 알고리즘을 사용한 강하게 연결된 컴포넌트 탐지
    - `_find_representative_cycle(self, scc)` (line 429): SCC 내의 대표 순환 경로 찾기
    - `_find_paths_between(self, start, end, max_depth)` (line 464): 두 노드 간의 모든 경로 찾기 (최대 깊이 제한)
    - `_get_path_edges(self, path)` (line 490): 경로의 엣지 목록 반환
    - `_assess_self_reference_severity(self, predicate)` (line 502): 자기 참조의 심각도 평가
    - `_assess_direct_cycle_severity(self, edge1, edge2)` (line 512): 직접 순환의 심각도 평가
    - `_assess_cycle_severity(self, path, predicates)` (line 525): 순환의 심각도 평가
    - `_can_break_cycle(self, predicates)` (line 536): 순환을 끊을 수 있는지 판별
    - `_get_inverse_cardinality(self, cardinality)` (line 542): 카디널리티의 역관계 반환
    - `_deduplicate_cycles(self, cycles)` (line 555): 중복 순환 제거
    - `_sort_cycles_by_severity(self, cycles)` (line 570): 심각도별로 순환 정렬
    - `suggest_cycle_resolution(self, cycle)` (line 579): 순환 해결 방안 제안
    - `get_cycle_analysis_report(self, cycles)` (line 605): 순환 분석 보고서 생성
    - `_generate_recommendations(self, cycles)` (line 625): 전체 권장사항 생성

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
  - `PathType` (line 18): 경로 유형
  - `TraversalDirection` (line 27): 탐색 방향
  - `RelationshipHop` (line 36): 관계 홉 (한 단계 관계)
  - `RelationshipPath` (line 49): 관계 경로
    - `entities(self)` (line 62): 경로상의 모든 엔티티 반환
    - `predicates(self)` (line 73): 경로상의 모든 predicate 반환
    - `to_readable_string(self)` (line 77): 읽기 쉬운 경로 문자열 반환
  - `PathQuery` (line 91): 경로 탐색 쿼리
  - `RelationshipPathTracker` (line 106): 🔥 THINK ULTRA! 관계 경로 추적기
    - `__init__(self)` (line 119): no docstring
    - `build_graph(self, ontologies)` (line 159): 온톨로지들로부터 관계 그래프 구축
    - `find_paths(self, query)` (line 231): 경로 탐색 (쿼리 기반)
    - `find_shortest_path(self, start, end, max_depth)` (line 262): 최단 경로 탐색 (단순 버전)
    - `find_all_reachable_entities(self, start, max_depth)` (line 274): 시작 엔티티에서 도달 가능한 모든 엔티티와 경로
    - `find_connecting_entities(self, entity1, entity2, max_depth)` (line 311): 두 엔티티를 연결하는 중간 엔티티들 탐색
    - `_find_shortest_paths(self, query)` (line 325): Dijkstra 알고리즘으로 최단 경로 탐색
    - `_find_all_paths(self, query)` (line 371): DFS로 모든 경로 탐색
    - `_find_weighted_paths(self, query)` (line 412): 가중치 기반 최적 경로 탐색
    - `_find_semantic_paths(self, query)` (line 427): 의미적 관련성 기반 경로 탐색
    - `_is_hop_allowed(self, hop, query)` (line 439): 홉이 쿼리 조건에 맞는지 확인
    - `_calculate_hop_weight(self, hop, query)` (line 460): 홉의 가중치 계산
    - `_calculate_semantic_score(self, path, query)` (line 475): 경로의 의미적 점수 계산
    - `_get_inverse_cardinality(self, cardinality)` (line 504): 카디널리티의 역관계 반환
    - `get_path_statistics(self, paths)` (line 517): 경로 통계 정보
    - `_find_common_predicates(self, paths)` (line 535): 경로들에서 공통으로 사용되는 predicate 찾기
    - `visualize_path(self, path, format)` (line 551): 경로 시각화
    - `export_graph_summary(self)` (line 582): 그래프 요약 정보 내보내기
    - `_get_cardinality_distribution(self)` (line 600): 카디널리티 분포 통계

### `backend/oms/utils/terminus_retry.py`
- **Functions**
  - `build_async_retry(retry_exceptions, backoff, logger, on_failure)` (line 14): no docstring

### `backend/oms/utils/terminus_schema_types.py`
- **Functions**
  - `create_basic_class_schema(class_id, key_type)` (line 441): 기본 클래스 스키마 빌더 생성
  - `create_subdocument_schema(class_id)` (line 446): 서브문서 스키마 빌더 생성
  - `convert_simple_schema(class_data)` (line 451): 간단한 스키마 데이터를 TerminusDB 형식으로 변환
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
    - `add_geopoint_property(self, name, optional)` (line 181): 지리적 좌표 속성 추가
    - `add_documentation(self, comment, description)` (line 191): 문서화 정보 추가
    - `build(self)` (line 202): 완성된 스키마 반환
  - `TerminusSchemaConverter` (line 207): 기존 스키마 데이터를 TerminusDB 형식으로 변환하는 클래스
    - `convert_property_type(prop_type, constraints)` (line 211): 속성 타입을 TerminusDB 형식으로 변환
    - `convert_relationship_cardinality(cardinality)` (line 326): 관계 카디널리티를 TerminusDB 형식으로 변환
    - `convert_complex_type(type_config)` (line 344): 복잡한 타입 구성을 변환
  - `TerminusConstraintProcessor` (line 391): TerminusDB 제약조건 처리 클래스
    - `extract_constraints_for_validation(constraints)` (line 395): 스키마 제약조건에서 런타임 검증용 제약조건 추출
    - `apply_schema_level_constraints(schema, constraints)` (line 422): 스키마 레벨에서 적용 가능한 제약조건 적용

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
- **Functions**
  - `async main()` (line 1478): 메인 진입점
- **Classes**
  - `_OntologyCommandPayload` (line 66): no docstring
  - `_OntologyCommandParseError` (line 71): no docstring
    - `__init__(self, stage, payload_text, payload_obj, fallback_metadata, cause)` (line 72): no docstring
  - `OntologyWorker` (line 89): 온톨로지 Command를 처리하는 워커
    - `__init__(self)` (line 92): no docstring
    - `_extract_key_spec_from_payload(payload)` (line 130): Extract ordered primary/title keys from an ontology payload.
    - `async _wait_for_database_exists(self, db_name, expected, timeout_seconds)` (line 167): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 183): no docstring
    - `async _poll_message(self, timeout)` (line 186): no docstring
    - `async initialize(self)` (line 191): 워커 초기화
    - `async _publish_to_dlq(self, msg, stage, error, attempt_count, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 285): no docstring
    - `async _send_to_dlq(self, msg, error, attempt_count, payload, raw_payload, stage, payload_text, payload_obj, kafka_headers, fallback_metadata)` (line 348): no docstring
    - `async process_command(self, command_data)` (line 386): Command 처리
    - `async handle_create_ontology(self, command_data)` (line 431): 온톨로지 생성 처리
    - `async handle_update_ontology(self, command_data)` (line 635): 온톨로지 업데이트 처리
    - `async handle_delete_ontology(self, command_data)` (line 831): 온톨로지 삭제 처리
    - `async handle_create_database(self, command_data)` (line 995): 데이터베이스 생성 처리
    - `async handle_delete_database(self, command_data)` (line 1063): 데이터베이스 삭제 처리
    - `_to_domain_envelope(self, event, kafka_topic)` (line 1114): no docstring
    - `async publish_event(self, event)` (line 1162): 이벤트 발행 (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka).
    - `async publish_failure_event(self, command_data, error)` (line 1174): 실패 이벤트 발행
    - `_heartbeat_options(self)` (line 1196): no docstring
    - `_parse_payload(self, payload)` (line 1202): no docstring
    - `_fallback_metadata(self, payload)` (line 1278): no docstring
    - `_registry_key(self, payload)` (line 1281): no docstring
    - `async _process_payload(self, payload)` (line 1302): no docstring
    - `_span_name(self, payload)` (line 1306): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 1309): no docstring
    - `_is_retryable_error(exc, payload)` (line 1332): no docstring
    - `async _commit(self, msg)` (line 1344): no docstring
    - `async _seek(self, topic, partition, offset)` (line 1349): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 1354): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 1387): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 1417): no docstring
    - `async run(self)` (line 1441): 메인 실행 루프
    - `async shutdown(self)` (line 1454): 워커 종료

## perf

### `backend/perf/cleanup_perf_databases.py`
- **Functions**
  - `_postgres_dsn_candidates()` (line 28): no docstring
  - `_bff_base_url()` (line 33): no docstring
  - `_admin_token()` (line 40): no docstring
  - `_extract_command_id(payload)` (line 46): no docstring
  - `async _connect_postgres()` (line 60): no docstring
  - `async _fetch_db_expected_seq(conn, db_name)` (line 72): no docstring
  - `async _wait_for_command(client, base_url, command_id, timeout_seconds)` (line 90): no docstring
  - `async _list_databases(client, base_url)` (line 113): no docstring
  - `async _delete_database(client, base_url, db_name, expected_seq)` (line 134): no docstring
  - `_matches_any_prefix(name, prefixes)` (line 153): no docstring
  - `async main()` (line 160): no docstring

## pipeline_scheduler

### `backend/pipeline_scheduler/main.py`
- **Functions**
  - `async main()` (line 21): no docstring

## pipeline_worker

### `backend/pipeline_worker/__init__.py`

### `backend/pipeline_worker/main.py`
- **Functions**
  - `async main()` (line 4417): no docstring
- **Classes**
  - `_PipelinePayloadParseError` (line 135): no docstring
    - `__init__(self, stage, payload_text, payload_obj, cause)` (line 136): no docstring
  - `PipelineWorker` (line 151): no docstring
    - `__init__(self)` (line 152): no docstring
    - `_build_error_payload(self, message, errors, code, category, status_code, external_code, stage, job, pipeline_id, node_id, mode, context)` (line 214): no docstring
    - `async initialize(self)` (line 260): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 345): Handle partition revocation during rebalance.
    - `_on_partitions_assigned(self, partitions)` (line 354): Handle partition assignment during rebalance.
    - `async close(self)` (line 363): no docstring
    - `_create_spark_session(self)` (line 408): no docstring
    - `_extract_job_settings(self, definition)` (line 436): no docstring
    - `_extract_job_spark_conf(self, definition)` (line 440): no docstring
    - `_apply_job_overrides(self, definition)` (line 456): Apply per-job Spark/cast overrides from definition.settings.
    - `_restart_spark_session(self)` (line 495): no docstring
    - `_is_spark_gateway_error(exc)` (line 525): no docstring
    - `async _run_spark(self, fn, label)` (line 542): Run a blocking Spark action off the main event loop.
    - `async run(self)` (line 561): no docstring
    - `_service_name(self)` (line 571): no docstring
    - `_cancel_inflight_on_revoke(self)` (line 574): no docstring
    - `_buffer_messages(self)` (line 580): no docstring
    - `_pending_log_thresholds(self)` (line 583): no docstring
    - `_uses_commit_state(self)` (line 586): no docstring
    - `async _poll_message(self, timeout)` (line 589): no docstring
    - `_parse_payload(self, payload)` (line 594): no docstring
    - `_registry_key(self, payload)` (line 633): no docstring
    - `async _process_payload(self, payload)` (line 641): no docstring
    - `_fallback_metadata(self, payload)` (line 691): no docstring
    - `_span_name(self, payload)` (line 700): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 703): no docstring
    - `_metric_event_name(self, payload)` (line 722): no docstring
    - `_heartbeat_options(self)` (line 725): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 730): no docstring
    - `async _mark_retryable_failure(self, payload, registry_key, handler, error)` (line 733): no docstring
    - `async _commit(self, msg)` (line 747): no docstring
    - `async _seek(self, topic, partition, offset)` (line 752): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 757): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 793): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 810): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 824): no docstring
    - `async _publish_to_dlq(self, msg, stage, error, payload_text, payload_obj, job, attempt_count)` (line 843): no docstring
    - `async _best_effort_record_invalid_job(self, payload, error)` (line 893): no docstring
    - `async _resolve_pipeline_id_from_fields(self, db_name, pipeline_id, branch)` (line 940): no docstring
    - `async _execute_job(self, job)` (line 958): no docstring
    - `async _maybe_enqueue_objectify_job(self, dataset, version)` (line 2516): no docstring
    - `async _maybe_enqueue_relationship_jobs(self, dataset, version)` (line 2625): no docstring
    - `async _materialize_output_dataframe(self, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 2739): no docstring
    - `_row_hash_expr(self, df)` (line 2819): no docstring
    - `_apply_watermark_filter(self, df, watermark_column, watermark_after, watermark_keys)` (line 2832): no docstring
    - `_collect_watermark_keys(self, df, watermark_column, watermark_value)` (line 2855): no docstring
    - `async _load_input_dataframe(self, db_name, metadata, temp_dirs, branch, node_id, input_snapshots, previous_commit_id, use_lakefs_diff, watermark_column, watermark_after, watermark_keys)` (line 2881): no docstring
    - `_preview_sampling_seed(self, job_id)` (line 3140): no docstring
    - `_resolve_sampling_strategy(self, metadata, preview_meta)` (line 3144): no docstring
    - `_attach_sampling_snapshot(self, input_snapshots, node_id, sampling_strategy)` (line 3160): no docstring
    - `_normalize_sampling_fraction(self, value, field)` (line 3172): no docstring
    - `_apply_sampling_strategy(self, df, sampling_strategy, node_id, seed)` (line 3181): no docstring
    - `_strip_commit_prefix(self, key, commit_id)` (line 3226): no docstring
    - `async _list_lakefs_diff_paths(self, repository, ref, since, prefix, node_id)` (line 3232): no docstring
    - `async _load_parquet_keys_dataframe(self, bucket, keys, temp_dirs, prefix)` (line 3278): no docstring
    - `async _load_media_prefix_dataframe(self, bucket, key, node_id)` (line 3312): Treat the artifact_key as an unstructured/media prefix.
    - `async _resolve_pipeline_id(self, job)` (line 3363): no docstring
    - `_collect_spark_conf(self)` (line 3382): no docstring
    - `_build_input_commit_payload(self, input_snapshots)` (line 3398): no docstring
    - `async _acquire_pipeline_lock(self, job)` (line 3422): no docstring
    - `_validate_required_subgraph(self, nodes, incoming, required_node_ids)` (line 3453): no docstring
    - `_normalize_transform_metadata(self, metadata)` (line 3469): no docstring
    - `_normalize_nodes_metadata(self, nodes)` (line 3507): no docstring
    - `_validate_definition(self, definition, require_output)` (line 3511): no docstring
    - `_build_table_ops(self, df)` (line 3694): no docstring
    - `_extract_schema_casts(self, schema_json)` (line 3765): no docstring
    - `_sql_ident(self, name)` (line 3788): no docstring
    - `_clean_string_column(self, column)` (line 3791): no docstring
    - `_try_cast_column(self, column, spark_type)` (line 3795): no docstring
    - `_safe_cast_column(self, column, target_type)` (line 3821): no docstring
    - `_apply_casts(self, df, casts)` (line 3832): no docstring
    - `_apply_schema_casts(self, df, dataset, version)` (line 3844): no docstring
    - `_normalize_fk_columns(self, value)` (line 3853): no docstring
    - `_parse_fk_expectation(self, expectation, default_branch)` (line 3858): no docstring
    - `async _load_fk_reference_dataframe(self, db_name, dataset_id, dataset_name, branch, temp_dirs)` (line 3906): no docstring
    - `async _evaluate_fk_expectations(self, expectations, output_df, db_name, branch, temp_dirs)` (line 3937): no docstring
    - `_normalize_read_options(self, read_config)` (line 4006): no docstring
    - `_mask_sensitive_options(self, options)` (line 4042): no docstring
    - `_schema_ddl_from_read_config(self, read_config)` (line 4063): no docstring
    - `_resolve_read_format(self, path, read_config)` (line 4086): no docstring
    - `_load_external_input_dataframe(self, read_config, node_id)` (line 4101): Load an input DataFrame directly from Spark using metadata.read (no DatasetRegistry artifact).
    - `async _load_artifact_dataframe(self, bucket, key, temp_dirs, read_config)` (line 4181): no docstring
    - `async _load_prefix_dataframe(self, bucket, prefix, temp_dirs, read_config)` (line 4203): no docstring
    - `async _download_object_to_path(self, bucket, key, local_path)` (line 4281): no docstring
    - `async _download_object(self, bucket, key, temp_dirs, temp_dir)` (line 4294): no docstring
    - `_read_local_file(self, path, read_config)` (line 4310): no docstring
    - `_strip_bom_headers(self, df)` (line 4337): Normalize UTF-8 BOM artifacts in CSV headers.
    - `_load_excel_path(self, path)` (line 4375): no docstring
    - `_load_json_path(self, path, reader)` (line 4381): no docstring
    - `_empty_dataframe(self)` (line 4398): no docstring
    - `_apply_transform(self, metadata, inputs, parameters)` (line 4401): no docstring

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
  - `apply_spark_transform(metadata, inputs, parameters, apply_casts)` (line 45): no docstring
  - `_clean_col_name(value)` (line 76): no docstring
  - `_clean_expr(value, parameters)` (line 80): no docstring
  - `_regex_inline_flags(value)` (line 84): no docstring
  - `_resolve_join_col(df, col_name)` (line 96): Resolve join column names defensively (handles UTF-8 BOM artifacts).
  - `_apply_join(ctx)` (line 109): no docstring
  - `_apply_filter(ctx)` (line 195): no docstring
  - `_apply_compute(ctx)` (line 202): no docstring
  - `_apply_normalize(ctx)` (line 247): no docstring
  - `_apply_explode(ctx)` (line 282): no docstring
  - `_apply_select(ctx)` (line 292): no docstring
  - `_apply_drop(ctx)` (line 305): no docstring
  - `_apply_rename(ctx)` (line 315): no docstring
  - `_apply_cast(ctx)` (line 354): no docstring
  - `_apply_regex_replace(ctx)` (line 361): no docstring
  - `_apply_dedupe(ctx)` (line 397): no docstring
  - `_parse_sort_specs(items)` (line 405): no docstring
  - `_apply_sort(ctx)` (line 435): no docstring
  - `_apply_union(ctx)` (line 443): no docstring
  - `_apply_group_by(ctx)` (line 478): no docstring
  - `_apply_pivot(ctx)` (line 548): no docstring
  - `_apply_window(ctx)` (line 573): no docstring
- **Classes**
  - `_SparkTransformContext` (line 35): no docstring

### `backend/pipeline_worker/worker_helpers.py`
- **Functions**
  - `_resolve_code_version()` (line 30): no docstring
  - `_is_sensitive_conf_key(key)` (line 34): no docstring
  - `_resolve_lakefs_repository()` (line 39): no docstring
  - `_resolve_watermark_column(incremental, metadata)` (line 44): no docstring
  - `_max_watermark_from_snapshots(input_snapshots, watermark_column)` (line 55): no docstring
  - `_watermark_values_match(left, right)` (line 86): no docstring
  - `_collect_watermark_keys_from_snapshots(input_snapshots, watermark_column, watermark_value)` (line 95): no docstring
  - `_collect_input_commit_map(input_snapshots)` (line 127): no docstring
  - `_inputs_diff_empty(input_snapshots)` (line 139): no docstring
  - `_resolve_execution_semantics(job, definition)` (line 150): no docstring
  - `_resolve_output_format(definition, output_metadata)` (line 154): no docstring
  - `_resolve_partition_columns(definition, output_metadata)` (line 169): no docstring

## projection_worker

### `backend/projection_worker/__init__.py`

### `backend/projection_worker/main.py`
- **Functions**
  - `async main()` (line 2773): 메인 함수
- **Classes**
  - `ProjectionWorker` (line 63): Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커
    - `__init__(self)` (line 70): no docstring
    - `_is_es_version_conflict(error)` (line 127): no docstring
    - `_parse_sequence(value)` (line 135): no docstring
    - `_normalize_localized_field(value, default_lang)` (line 144): no docstring
    - `_normalize_ontology_properties(self, properties, default_lang)` (line 154): no docstring
    - `_normalize_ontology_relationships(self, relationships, default_lang)` (line 191): no docstring
    - `_extract_envelope_metadata(event_data)` (line 240): no docstring
    - `async _record_es_side_effect(self, event_id, event_data, db_name, index_name, doc_id, operation, status, record_lineage, skip_reason, error, extra_metadata)` (line 258): Record projection side-effects for provenance (lineage) + audit.
    - `async _consumer_call(self, func, *args, **kwargs)` (line 352): no docstring
    - `async _poll_message(self, timeout)` (line 355): no docstring
    - `async initialize(self)` (line 360): 워커 초기화
    - `async _setup_indices(self)` (line 460): 매핑 파일 로드 (인덱스는 DB별로 동적 생성)
    - `async _ensure_index_exists(self, db_name, index_type, branch)` (line 472): 특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성
    - `async _load_mapping(self, filename)` (line 564): 매핑 파일 로드
    - `async run(self)` (line 578): 메인 실행 루프
    - `_parse_payload(self, payload)` (line 590): no docstring
    - `_fallback_metadata(self, payload)` (line 602): no docstring
    - `_span_name(self, payload)` (line 605): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 608): no docstring
    - `_registry_handler(self, msg, payload)` (line 624): no docstring
    - `_registry_key(self, payload)` (line 627): no docstring
    - `_heartbeat_options(self)` (line 634): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 640): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 645): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 657): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 669): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 680): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 710): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 724): no docstring
    - `async _commit(self, msg)` (line 753): no docstring
    - `async _seek(self, topic, partition, offset)` (line 759): no docstring
    - `async _publish_to_dlq(self, msg, error, attempt_count, payload_text, kafka_headers, fallback_metadata)` (line 764): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 820): no docstring
    - `async _process_payload(self, payload)` (line 842): no docstring
    - `async _handle_instance_event(self, event_data)` (line 867): 인스턴스 이벤트 처리
    - `async _handle_ontology_event(self, event_data)` (line 887): 온톨로지 이벤트 처리
    - `async _handle_action_event(self, event_data)` (line 911): Action writeback events -> overlay projection.
    - `async _handle_action_applied(self, action_data, event_id, event_data)` (line 926): no docstring
    - `async _handle_instance_created(self, instance_data, event_id, event_data)` (line 1126): 인스턴스 생성 이벤트 처리
    - `async _handle_instance_updated(self, instance_data, event_id, event_data)` (line 1285): 인스턴스 업데이트 이벤트 처리
    - `async _handle_instance_deleted(self, instance_data, event_id, event_data)` (line 1468): 인스턴스 삭제 이벤트 처리
    - `async _handle_ontology_class_created(self, ontology_data, event_id, event_data)` (line 1716): 온톨로지 클래스 생성 이벤트 처리
    - `async _handle_ontology_class_updated(self, ontology_data, event_id, event_data)` (line 1899): 온톨로지 클래스 업데이트 이벤트 처리
    - `async _handle_ontology_class_deleted(self, ontology_data, event_id, event_data)` (line 2098): 온톨로지 클래스 삭제 이벤트 처리
    - `async _handle_database_created(self, db_data, event_id, event_data)` (line 2353): 데이터베이스 생성 이벤트 처리
    - `async _handle_database_deleted(self, db_data, event_id, event_data)` (line 2409): 데이터베이스 삭제 이벤트 처리
    - `async _get_class_label(self, class_id, db_name, branch)` (line 2483): Redis에서 클래스 라벨 조회 (Cache Stampede 방지)
    - `async _get_class_label_fallback(self, class_id, db_name, branch)` (line 2589): 락 획득 실패 시 fallback 조회 (성능보다 안정성 우선)
    - `get_cache_efficiency_metrics(self)` (line 2625): 캐시 효율성 및 락 경합 메트릭 반환
    - `log_cache_metrics(self)` (line 2678): 캐시 메트릭을 로그로 출력
    - `async _cache_class_label(self, class_id, label, db_name, branch)` (line 2697): 클래스 라벨을 Redis에 캐싱
    - `_normalize_properties(self, properties)` (line 2712): 속성을 검색 최적화된 형태로 정규화
    - `_is_transient_infra_error(error)` (line 2724): Return True for errors that are expected to recover via retry (e.g. ES outage).
    - `async _shutdown(self)` (line 2748): 워커 종료

## scripts

### `backend/scripts/backfill_lineage.py`
- **Functions**
  - `_parse_dt(value)` (line 22): no docstring
  - `async _run_queue(limit, db_name)` (line 32): no docstring
  - `async _run_replay(from_dt, to_dt, limit)` (line 68): no docstring
  - `async main()` (line 103): no docstring

### `backend/scripts/debug/check_graph_data.py`
- **Functions**
  - `async check_data()` (line 15): no docstring

### `backend/scripts/debug/check_kafka_topics.py`
- **Functions**
  - `check_kafka_topics()` (line 24): Kafka 토픽 및 메시지 확인
  - `check_topic_messages(topic_name)` (line 57): 특정 토픽의 메시지 확인
  - `main()` (line 137): no docstring

### `backend/scripts/debug/check_syntax.py`
- **Functions**
  - `check_python_syntax()` (line 13): 백엔드 Python 파일들의 문법을 검사합니다.

### `backend/scripts/debug/debug_404_error.py`
- **Functions**
  - `async debug_404()` (line 8): 404 에러 원인 파악

### `backend/scripts/debug/debug_404_root_cause.py`
- **Functions**
  - `async debug_ontology_404()` (line 11): no docstring

### `backend/scripts/debug/debug_async_terminus.py`
- **Functions**
  - `async debug_async_terminus_service()` (line 15): no docstring

### `backend/scripts/debug/debug_database_check.py`
- **Functions**
  - `async debug_database_list()` (line 9): no docstring

### `backend/scripts/debug/debug_link_dependency.py`
- **Functions**
  - `async debug_link_dependency()` (line 10): no docstring

### `backend/scripts/debug/debug_link_transformation.py`
- **Functions**
  - `async debug_transformation()` (line 10): no docstring

### `backend/scripts/debug/debug_oms_database_creation.py`
- **Functions**
  - `async debug_oms_database_creation()` (line 15): no docstring

### `backend/scripts/debug/debug_pydantic.py`
- **Functions**
  - `debug_pydantic()` (line 9): no docstring

### `backend/scripts/debug/debug_settings.py`
- **Functions**
  - `debug_settings()` (line 9): no docstring

### `backend/scripts/debug/debug_terminus_auth.py`
- **Functions**
  - `async debug_terminus_auth()` (line 12): no docstring

### `backend/scripts/debug/debug_terminus_direct.py`
- **Functions**
  - `async debug_terminusdb_directly()` (line 13): no docstring

### `backend/scripts/debug/debug_terminusdb_direct.py`
- **Functions**
  - `get_auth_header()` (line 16): Basic 인증 헤더 생성
  - `test_direct_terminusdb_query()` (line 22): TerminusDB에 직접 쿼리하여 Team 데이터 확인

### `backend/scripts/debug/debug_woql_bindings.py`
- **Functions**
  - `async debug_bindings()` (line 16): Debug why bindings show Unknown
  - `async main()` (line 161): Main execution

### `backend/scripts/debug/debug_woql_syntax.py`
- **Functions**
  - `async test_woql_queries()` (line 16): Test different WOQL query formats
  - `async test_working_format()` (line 138): Test the format that should work based on TerminusDB docs
  - `async main()` (line 172): Run all tests

### `backend/scripts/debug/demo_validation_improvement.py`
- **Functions**
  - `show_validation_improvement()` (line 13): 매핑 검증 개선 내용을 시각적으로 보여주기

### `backend/scripts/debug/final_system_verification.py`
- **Functions**
  - `async main()` (line 23): no docstring

### `backend/scripts/debug/final_verification.py`
- **Functions**
  - `async verify_all_systems()` (line 15): no docstring

### `backend/scripts/debug/quick_performance_test.py`
- **Functions**
  - `async test_event_sourcing_complete()` (line 15): Test the complete Event Sourcing flow with the fixed field name

### `backend/scripts/debug/quick_production_test.py`
- **Functions**
  - `async quick_production_test()` (line 13): Quick test to verify production system works

### `backend/scripts/debug/simple_schema_test.py`
- **Functions**
  - `_oms_base_url()` (line 15): no docstring
  - `_admin_headers()` (line 19): no docstring
  - `async _post_with_retry(client, url, json_payload, headers, retries, retry_sleep)` (line 25): no docstring
  - `async create_simple_schema(db_name)` (line 47): Create schema using OMS ontology endpoints.
  - `async test_create_instance(db_name)` (line 126): Test creating a lightweight instance
  - `db_name()` (line 167): no docstring
  - `async main()` (line 170): Main function

### `backend/scripts/debug/test_architecture_verification.py`
- **Functions**
  - `async test_complete_user_flow()` (line 26): no docstring

### `backend/scripts/debug/test_full_api_integration_ultra.py`
- **Functions**
  - `async _wait_for_command_completed(session, command_id, db_name, timeout_seconds, poll_interval_seconds)` (line 30): no docstring
  - `async _request_json(session, method, url, retries, retry_sleep, **kwargs)` (line 70): no docstring
  - `_extract_command_id(result)` (line 101): no docstring
  - `async _wait_for_bff_query(session, url, payload, timeout_seconds)` (line 106): no docstring
  - `async test_full_api_integration()` (line 135): no docstring

### `backend/scripts/debug/test_graph_federation.py`
- **Functions**
  - `async _request_json(session, method, url, retries, retry_sleep, **kwargs)` (line 26): no docstring
  - `async test_graph_federation()` (line 58): no docstring

### `backend/scripts/debug/test_lightweight_nodes_ultra.py`
- **Functions**
  - `async _post_json_with_retry(session, url, payload, timeout_seconds, retry_sleep)` (line 22): no docstring
  - `async test_lightweight_architecture()` (line 51): no docstring

### `backend/scripts/debug/test_ontology_issue.py`
- **Functions**
  - `async test_ontology_creation()` (line 14): no docstring

### `backend/scripts/debug/test_real_production_flow.py`
- **Functions**
  - `async test_real_production_flow()` (line 540): no docstring
  - `async main()` (line 547): Run the production test
- **Classes**
  - `ProductionFlowTest` (line 58): REAL production flow test - no mocks
    - `__init__(self)` (line 61): no docstring
    - `async verify_infrastructure(self)` (line 66): Verify ALL services are running and accessible
    - `async create_test_database(self)` (line 187): Create a test database via OMS API
    - `async create_test_ontology(self, db_name)` (line 220): Create test ontology
    - `async create_test_instance(self, db_name, class_id)` (line 270): Create test instance
    - `async verify_s3_events(self)` (line 303): Verify events in S3
    - `async verify_postgresql_registry(self)` (line 344): Verify processed-event registry entries
    - `async verify_kafka_messages(self)` (line 382): Verify Kafka message flow
    - `async run_complete_test(self)` (line 460): Run the complete production test

### `backend/scripts/ghost_dependency_audit.py`
- **Functions**
  - `parse_requirements_txt(file_path)` (line 30): Parse requirements.txt file and return dependencies with versions
  - `parse_pyproject_toml(file_path)` (line 50): Parse pyproject.toml dependencies
  - `check_service_imports(service_dir)` (line 85): Check what external libraries a service actually imports
  - `audit_service(service_name, service_dir)` (line 113): Comprehensive audit of a single service
  - `main()` (line 168): Main audit execution

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
  - `async _main()` (line 18): no docstring

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
  - `parse_requirements_txt(file_path)` (line 30): Parse requirements.txt file and extract dependencies with versions
  - `parse_pyproject_toml(file_path)` (line 55): Parse pyproject.toml dependencies
  - `check_duplicate_dependencies()` (line 94): Check for duplicate dependency declarations across files
  - `check_version_consistency()` (line 140): Check for version inconsistencies across files
  - `check_single_source_compliance()` (line 176): Verify that all services use only -e ../shared in requirements.txt
  - `main()` (line 212): Main audit execution

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

## search_projection_worker

### `backend/search_projection_worker/__init__.py`

### `backend/search_projection_worker/main.py`
- **Functions**
  - `async main()` (line 250): no docstring
- **Classes**
  - `SearchProjectionWorker` (line 34): no docstring
    - `__init__(self)` (line 35): no docstring
    - `async initialize(self)` (line 59): no docstring
    - `_on_partitions_revoked(self, partitions)` (line 100): Handle partition revocation during rebalance.
    - `_on_partitions_assigned(self, partitions)` (line 108): Handle partition assignment during rebalance.
    - `async close(self)` (line 116): no docstring
    - `async _poll_message(self, timeout)` (line 133): no docstring
    - `async run(self)` (line 138): no docstring
    - `async _index_event(self, envelope)` (line 149): no docstring
    - `_heartbeat_options(self)` (line 167): no docstring
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 172): no docstring
    - `async _process_payload(self, payload)` (line 219): no docstring
    - `_span_name(self, payload)` (line 222): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 225): no docstring
    - `_is_retryable_error_impl(exc)` (line 229): no docstring

## shared

### `backend/shared/__init__.py`

### `backend/shared/config/__init__.py`
- **Functions**
  - `__getattr__(name)` (line 175): Lazy, drift-free compatibility exports.
- **Classes**
  - `Config` (line 33): 통합 설정 클래스
    - `get_postgres_url()` (line 45): PostgreSQL 연결 URL
    - `get_redis_url()` (line 50): Redis 연결 URL
    - `get_elasticsearch_url()` (line 55): Elasticsearch 연결 URL
    - `get_kafka_bootstrap_servers()` (line 60): Kafka Bootstrap Servers
    - `get_terminus_url()` (line 65): TerminusDB 연결 URL
    - `get_minio_url()` (line 70): MinIO 연결 URL
    - `get_instances_index_name(db_name, version)` (line 79): 인스턴스 Elasticsearch 인덱스 이름
    - `get_ontologies_index_name(db_name, version)` (line 84): 온톨로지 Elasticsearch 인덱스 이름
    - `sanitize_index_name(name)` (line 89): Elasticsearch 인덱스 이름 정제
    - `get_default_index_settings()` (line 94): 기본 인덱스 설정
    - `validate_all_config(cls)` (line 103): 모든 설정의 유효성 검증
    - `get_full_config_summary(cls)` (line 132): 전체 시스템 설정 요약 반환

### `backend/shared/config/app_config.py`
- **Classes**
  - `_SettingsValue` (line 14): Descriptor that resolves values from the current settings instance (SSoT).
    - `__init__(self, path)` (line 17): no docstring
    - `__get__(self, instance, owner)` (line 21): no docstring
  - `AppConfig` (line 25): SPICE HARVESTER 애플리케이션 전체 설정 중앙 관리 클래스
    - `get_instance_command_key(db_name, command_id)` (line 79): 인스턴스 Command S3 키 생성
    - `get_instance_latest_key(db_name, instance_id)` (line 84): 인스턴스 최신 상태 S3 키 생성 (deprecated - 순수 append-only로 변경됨)
    - `get_command_status_key(command_id)` (line 92): Command 상태 Redis 키 생성
    - `get_command_result_key(command_id)` (line 97): Command 결과 Redis 키 생성
    - `get_command_status_pattern()` (line 102): 모든 Command 상태 키 패턴
    - `get_class_label_key(db_name, class_id, branch)` (line 107): 클래스 라벨 캐시 Redis 키 생성 (branch-aware).
    - `get_user_session_key(user_id)` (line 114): 사용자 세션 Redis 키 생성
    - `get_websocket_connection_key(client_id)` (line 119): WebSocket 연결 Redis 키 생성
    - `get_instances_index_name(db_name, version)` (line 128): 인스턴스 Elasticsearch 인덱스 이름 생성
    - `get_ontologies_index_name(db_name, version)` (line 134): 온톨로지 Elasticsearch 인덱스 이름 생성
    - `get_oms_url()` (line 143): OMS 서비스 URL
    - `get_bff_url()` (line 148): BFF 서비스 URL
    - `get_funnel_url()` (line 153): Funnel 서비스 URL
    - `_normalize_object_type_id(value)` (line 188): no docstring
    - `get_writeback_enabled_object_types(cls)` (line 202): no docstring
    - `is_writeback_enabled_object_type(cls, class_id)` (line 211): no docstring
    - `get_ontology_writeback_branch(cls, db_name)` (line 221): Return a lakeFS-compatible writeback branch id.
    - `sanitize_lakefs_branch_id(value)` (line 236): no docstring
    - `validate_config(cls)` (line 290): 설정값들의 유효성 검증
    - `get_all_topics(cls)` (line 319): 모든 Kafka 토픽 목록 반환
    - `get_config_summary(cls)` (line 353): 현재 설정 요약 반환 (디버깅용)

### `backend/shared/config/kafka_config.py`
- **Functions**
  - `create_eos_producer(service_name, instance_id)` (line 272): Create a Kafka producer with EOS v2 configuration
  - `create_eos_consumer(service_name, group_id)` (line 294): Create a Kafka consumer with EOS v2 configuration
- **Classes**
  - `KafkaEOSConfig` (line 15): Kafka Exactly-Once Semantics v2 Configuration
    - `get_producer_config(service_name, instance_id, enable_transactions)` (line 27): Get producer configuration with EOS v2 support
    - `get_consumer_config(service_name, group_id, read_committed, auto_commit)` (line 79): Get consumer configuration with EOS v2 support
    - `get_admin_config()` (line 128): Get admin client configuration for topic management
    - `get_topic_config(retention_ms, min_insync_replicas, replication_factor)` (line 142): Get topic configuration for durability and performance
  - `TransactionalProducer` (line 168): Helper class for transactional message production
    - `__init__(self, producer, enable_transactions)` (line 176): Initialize transactional producer wrapper
    - `init_transactions(self, timeout)` (line 188): Initialize transactions (must be called once before any transaction)
    - `begin_transaction(self)` (line 199): Begin a new transaction
    - `commit_transaction(self, timeout)` (line 204): Commit the current transaction
    - `abort_transaction(self, timeout)` (line 214): Abort the current transaction
    - `send_transactional_batch(self, messages, topic, key_extractor)` (line 224): Send a batch of messages in a single transaction

### `backend/shared/config/model_context_limits.py`
- **Functions**
  - `get_model_context_config(model_id, registry_record)` (line 68): Resolve context config with priority:
- **Classes**
  - `ModelContextConfig` (line 13): Configuration for a specific LLM model's context capabilities.
    - `safe_prompt_chars(self)` (line 21): Calculate safe prompt character limit.

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
  - `get_oms_url()` (line 487): Get OMS URL - convenience function.
  - `get_bff_url()` (line 492): Get BFF URL - convenience function.
  - `get_funnel_url()` (line 497): Get Funnel URL - convenience function.
  - `get_agent_url()` (line 502): Get Agent URL - convenience function.
- **Classes**
  - `_SettingsProxy` (line 30): no docstring
    - `__getattr__(self, name)` (line 31): no docstring
  - `ServiceConfig` (line 38): Centralized service configuration management.
    - `get_oms_port()` (line 55): Get OMS (Ontology Management Service) port from environment or default.
    - `get_bff_port()` (line 60): Get BFF (Backend for Frontend) port from environment or default.
    - `get_funnel_port()` (line 65): Get Funnel service port from environment or default.
    - `get_agent_port()` (line 70): Get Agent service port from environment or default.
    - `get_oms_host()` (line 75): Get OMS host from environment or default.
    - `get_bff_host()` (line 80): Get BFF host from environment or default.
    - `get_funnel_host()` (line 85): Get Funnel host from environment or default.
    - `get_agent_host()` (line 90): Get Agent host from environment or default.
    - `get_oms_url()` (line 95): Get complete OMS URL from environment or construct from host/port.
    - `get_bff_url()` (line 107): Get complete BFF URL from environment or construct from host/port.
    - `get_funnel_url()` (line 119): Get complete Funnel URL from environment or construct from host/port.
    - `get_agent_url()` (line 131): Get complete Agent URL from environment or construct from host/port.
    - `get_terminus_url()` (line 143): Get TerminusDB URL from environment or default.
    - `get_postgres_url()` (line 148): Get PostgreSQL connection URL from environment or default.
    - `get_kafka_bootstrap_servers()` (line 153): Get Kafka bootstrap servers from environment or default.
    - `get_redis_host()` (line 158): Get Redis host from environment or default.
    - `get_redis_port()` (line 163): Get Redis port from environment or default.
    - `get_redis_url()` (line 168): Get Redis connection URL from environment or construct from host/port.
    - `get_elasticsearch_host()` (line 173): Get Elasticsearch host from environment or default.
    - `get_elasticsearch_port()` (line 178): Get Elasticsearch port from environment or default.
    - `get_elasticsearch_url()` (line 183): Get Elasticsearch base URL from environment or construct from host/port.
    - `is_docker_environment()` (line 188): Check if running in Docker environment.
    - `get_minio_endpoint()` (line 197): Get MinIO/S3 endpoint URL.
    - `get_minio_access_key()` (line 202): Get MinIO/S3 access key.
    - `get_minio_secret_key()` (line 207): Get MinIO/S3 secret key.
    - `get_event_store_bucket()` (line 212): Get S3/MinIO bucket name for immutable event store.
    - `get_lakefs_api_url()` (line 217): Get lakeFS API base URL.
    - `get_lakefs_s3_endpoint()` (line 228): Get lakeFS S3 Gateway endpoint URL.
    - `get_service_url(service_name)` (line 237): Get URL for a specific service by name.
    - `get_all_service_urls()` (line 264): Get all service URLs as a dictionary.
    - `validate_configuration()` (line 275): Validate that all required configuration is present.
    - `use_https()` (line 292): Check if HTTPS should be used for service communication.
    - `is_production()` (line 302): Check if running in production environment.
    - `is_debug_endpoints_enabled()` (line 307): Enable opt-in debug endpoints (never on by default).
    - `get_ssl_cert_path()` (line 312): Get SSL certificate path from environment.
    - `get_ssl_key_path()` (line 324): Get SSL key path from environment.
    - `get_ssl_ca_path()` (line 336): Get SSL CA certificate path from environment.
    - `verify_ssl()` (line 348): Check if SSL certificate verification should be enabled.
    - `get_protocol()` (line 363): Get the protocol to use for service communication.
    - `get_ssl_config()` (line 373): Get complete SSL configuration as a dictionary.
    - `get_client_ssl_config()` (line 383): Get SSL configuration for HTTP clients (requests, httpx).
    - `get_cors_origins()` (line 395): Get CORS allowed origins from environment variables.
    - `_get_environment_default_origins()` (line 410): Get environment-based default CORS origins.
    - `_get_dev_cors_origins()` (line 429): Get development CORS origins for common frontend ports.
    - `get_cors_config()` (line 456): Get complete CORS configuration for FastAPI middleware.
    - `is_cors_enabled()` (line 466): Check if CORS should be enabled.
    - `get_cors_debug_info()` (line 476): Get CORS configuration debug information.

### `backend/shared/config/settings.py`
- **Functions**
  - `_is_docker_environment()` (line 28): no docstring
  - `_clamp_int(raw, default, min_value, max_value)` (line 37): no docstring
  - `_parse_boolish(raw)` (line 47): no docstring
  - `_env_truthy(name)` (line 58): no docstring
  - `_should_load_dotenv()` (line 63): Whether settings should read from a local `.env` file.
  - `get_settings()` (line 4332): Get the global settings instance
  - `reload_settings()` (line 4346): Reload settings from environment (useful for testing)
  - `build_client_ssl_config(settings)` (line 4358): SSL config for HTTP clients (httpx/requests/etc).
  - `build_server_ssl_config(settings)` (line 4379): SSL config for uvicorn (server-side TLS).
  - `_get_dev_cors_origins()` (line 4409): no docstring
  - `_get_environment_default_origins(settings)` (line 4425): no docstring
  - `resolve_cors_origins(settings)` (line 4435): Resolve CORS origins with production safety.
  - `build_cors_middleware_config(settings)` (line 4477): no docstring
  - `get_cors_debug_info(settings)` (line 4507): no docstring
- **Classes**
  - `Environment` (line 83): Application environment types
  - `DatabaseSettings` (line 90): Database configuration settings
    - `get_terminus_url(cls, v)` (line 122): no docstring
    - `get_terminus_user(cls, v)` (line 127): no docstring
    - `get_terminus_password(cls, v)` (line 132): no docstring
    - `get_terminus_account(cls, v)` (line 137): no docstring
    - `clamp_elasticsearch_default_shards(cls, v)` (line 280): no docstring
    - `clamp_elasticsearch_default_replicas(cls, v)` (line 285): no docstring
    - `postgres_url(self)` (line 294): Construct PostgreSQL connection URL
    - `kafka_servers(self)` (line 301): Get Kafka bootstrap servers
    - `elasticsearch_url(self)` (line 308): Construct Elasticsearch URL with authentication
    - `redis_url(self)` (line 317): Construct Redis URL
  - `ServiceSettings` (line 326): Service configuration settings
    - `get_oms_base_url_override(cls, v)` (line 432): no docstring
    - `get_bff_base_url_override(cls, v)` (line 440): no docstring
    - `get_funnel_base_url_override(cls, v)` (line 448): no docstring
    - `get_agent_base_url_override(cls, v)` (line 456): no docstring
    - `resolve_funnel_excel_timeout(cls, v)` (line 464): no docstring
    - `clamp_funnel_excel_timeout(cls, v)` (line 472): no docstring
    - `resolve_funnel_infer_timeout(cls, v)` (line 481): no docstring
    - `clamp_funnel_infer_timeout(cls, v)` (line 489): no docstring
    - `oms_base_url(self)` (line 497): Construct OMS base URL
    - `bff_base_url(self)` (line 505): Construct BFF base URL
    - `funnel_base_url(self)` (line 513): Construct Funnel base URL
    - `agent_base_url(self)` (line 521): Construct Agent base URL
    - `cors_origins_list(self)` (line 529): Parse CORS origins from JSON string
  - `LLMSettings` (line 541): LLM gateway settings (shared across services).
    - `strip_strings(cls, v)` (line 626): no docstring
    - `fallback_anthropic_api_key(cls, v)` (line 634): no docstring
    - `fallback_google_api_key(cls, v)` (line 642): no docstring
    - `fallback_openai_api_key(cls, v)` (line 650): no docstring
    - `fallback_openai_base_url(cls, v)` (line 664): no docstring
    - `fallback_openai_model(cls, v)` (line 678): no docstring
    - `anthropic_api_key_effective(self)` (line 691): no docstring
    - `google_api_key_effective(self)` (line 695): no docstring
    - `mock_json_for_task(self, task)` (line 698): Resolve the mock provider JSON payload for a task.
  - `ObservabilitySettings` (line 737): Logging/observability settings (shared across services/workers).
    - `normalize_log_level(cls, v)` (line 843): no docstring
    - `normalize_service_name(cls, v)` (line 849): no docstring
    - `normalize_hostname(cls, v)` (line 855): no docstring
    - `fallback_run_id(cls, v)` (line 861): no docstring
    - `fallback_code_sha(cls, v)` (line 872): no docstring
    - `normalize_source_version(cls, v)` (line 883): no docstring
    - `normalize_enterprise_catalog_ref(cls, v)` (line 889): no docstring
    - `normalize_otel_service_name(cls, v)` (line 895): no docstring
    - `normalize_otel_service_version(cls, v)` (line 901): no docstring
    - `normalize_otel_environment(cls, v)` (line 907): no docstring
    - `normalize_otel_exporter_otlp_endpoint(cls, v)` (line 913): no docstring
    - `normalize_jaeger_endpoint(cls, v)` (line 919): no docstring
    - `clamp_trace_sample_rate(cls, v)` (line 925): no docstring
    - `parse_otel_export_otlp(cls, v)` (line 938): no docstring
    - `clamp_metric_export_interval_seconds(cls, v)` (line 943): no docstring
    - `service_name_effective(self)` (line 951): no docstring
    - `enterprise_catalog_ref_effective(self)` (line 959): no docstring
    - `otel_export_otlp_effective(self)` (line 969): no docstring
  - `GraphQuerySettings` (line 975): Graph query guardrails (graph federation).
    - `clamp_max_hops(cls, v)` (line 993): no docstring
    - `clamp_max_limit(cls, v)` (line 998): no docstring
    - `clamp_max_paths(cls, v)` (line 1003): no docstring
  - `FeatureFlagsSettings` (line 1007): Feature flags / opt-in endpoints.
  - `PipelineSettings` (line 1035): Pipeline Builder + pipeline worker settings.
    - `strip_strings(cls, v)` (line 1184): no docstring
    - `fallback_publish_lock_timeout(cls, v)` (line 1191): no docstring
    - `clamp_jobs_max_retries(cls, v)` (line 1200): no docstring
    - `clamp_job_queue_flush_timeout_seconds(cls, v)` (line 1205): no docstring
    - `clamp_jobs_backoff_base_seconds(cls, v)` (line 1214): no docstring
    - `clamp_jobs_backoff_max_seconds(cls, v)` (line 1219): no docstring
    - `clamp_spark_executor_threads(cls, v)` (line 1224): no docstring
    - `clamp_spark_shuffle_partitions(cls, v)` (line 1229): no docstring
    - `clamp_lock_ttl_seconds(cls, v)` (line 1234): no docstring
    - `clamp_lock_renew_seconds(cls, v)` (line 1239): no docstring
    - `clamp_lock_retry_seconds(cls, v)` (line 1244): no docstring
    - `clamp_lock_acquire_timeout_seconds(cls, v)` (line 1249): no docstring
    - `clamp_publish_lock_acquire_timeout_seconds(cls, v)` (line 1254): no docstring
    - `clamp_scheduler_poll_seconds(cls, v)` (line 1259): no docstring
    - `protected_branches_set(self)` (line 1263): no docstring
    - `fallback_branches_list(self)` (line 1270): no docstring
  - `OntologySettings` (line 1278): Ontology API + linter governance settings.
    - `strip_protected_branches(cls, v)` (line 1344): no docstring
    - `parse_optional_bool(cls, v)` (line 1349): no docstring
    - `protected_branches_set(self)` (line 1353): no docstring
    - `allow_implicit_primary_key_effective(self, is_production, branch)` (line 1358): no docstring
    - `allow_implicit_title_key_effective(self, is_production, branch)` (line 1367): no docstring
  - `AgentRuntimeSettings` (line 1377): Agent runtime settings (agent service tool runner).
    - `clamp_context_upload_max_bytes(cls, v)` (line 1503): no docstring
    - `clamp_context_upload_max_text_chars(cls, v)` (line 1508): no docstring
    - `strip_context_upload_clamav_host(cls, v)` (line 1513): no docstring
    - `clamp_context_upload_clamav_port(cls, v)` (line 1521): no docstring
    - `fallback_bff_token(cls, v)` (line 1526): no docstring
    - `fallback_command_timeout(cls, v)` (line 1534): no docstring
  - `AgentPlanSettings` (line 1544): LLM-native control plane settings (planner + allowlist bootstrap).
    - `strip_allowlist_bundle_path(cls, v)` (line 1570): no docstring
  - `PipelinePlanSettings` (line 1577): Pipeline plan planner settings (LLM-backed pipeline definition proposals).
  - `ClientSettings` (line 1594): Internal service-to-service client settings (BFF/OMS/etc).
    - `clamp_agent_proxy_timeout(cls, v)` (line 1627): no docstring
    - `fallback_oms_client_token(cls, v)` (line 1636): no docstring
    - `fallback_bff_admin_token(cls, v)` (line 1647): no docstring
  - `MCPSettings` (line 1657): MCP integration settings (BFF/agent).
    - `strip_config_path(cls, v)` (line 1675): no docstring
  - `AuthSettings` (line 1682): Service auth configuration (BFF/OMS).
    - `strip_strings(cls, v)` (line 1836): no docstring
    - `bff_auth_disable_allowed(self)` (line 1843): no docstring
    - `oms_auth_disable_allowed(self)` (line 1847): no docstring
    - `_split_tokens(raw)` (line 1851): no docstring
    - `_tokens_from_values(cls, *values)` (line 1858): no docstring
    - `bff_expected_tokens(self)` (line 1865): no docstring
    - `bff_agent_tokens(self)` (line 1869): no docstring
    - `oms_expected_tokens(self)` (line 1873): no docstring
    - `bff_expected_token(self)` (line 1877): no docstring
    - `bff_admin_only_token(self)` (line 1887): no docstring
    - `oms_expected_token(self)` (line 1892): no docstring
    - `admin_bypass_tokens(self)` (line 1897): no docstring
    - `is_bff_auth_required(self, allow_pytest, default_required)` (line 1908): no docstring
    - `is_oms_auth_required(self, default_required)` (line 1917): no docstring
    - `_parse_exempt_paths(raw, defaults)` (line 1925): no docstring
    - `resolve_bff_exempt_paths(self, defaults)` (line 1932): no docstring
    - `resolve_oms_exempt_paths(self, defaults)` (line 1935): no docstring
    - `dev_master_role_set(self)` (line 1939): no docstring
  - `RateLimitSettings` (line 1943): Rate limiter runtime configuration.
    - `clamp_local_max_entries(cls, v)` (line 1965): no docstring
  - `MessagingSettings` (line 1969): Kafka topic/group configuration settings
  - `StorageSettings` (line 2093): Storage configuration settings
    - `normalize_minio_endpoint_url(cls, v)` (line 2148): no docstring
    - `clamp_lakefs_client_timeout(cls, v)` (line 2218): no docstring
    - `normalize_lakefs_credentials_source(cls, v)` (line 2226): no docstring
    - `use_ssl(self)` (line 2237): Determine if SSL should be used based on endpoint URL
    - `lakefs_api_url_effective(self)` (line 2242): Return lakeFS API base URL (without /api/v1).
    - `lakefs_s3_endpoint_effective(self)` (line 2250): Return lakeFS S3 Gateway endpoint URL.
  - `CacheSettings` (line 2257): Cache and TTL configuration settings
    - `clamp_command_status_ttl_seconds(cls, v)` (line 2295): no docstring
  - `SecuritySettings` (line 2299): Security configuration settings
  - `PerformanceSettings` (line 2366): Performance and optimization settings
    - `clamp_pg_pool_min(cls, v)` (line 2494): no docstring
    - `clamp_pg_pool_max(cls, v)` (line 2509): no docstring
    - `clamp_pg_command_timeout_seconds(cls, v)` (line 2524): no docstring
    - `clamp_lineage_latest_edges_max_ids(cls, v)` (line 2529): no docstring
  - `EventSourcingSettings` (line 2533): Event sourcing / CQRS tuning settings
    - `normalize_event_store_strings(cls, v)` (line 2635): no docstring
  - `BranchVirtualizationSettings` (line 2641): Branch virtualization defaults (OCC seeding).
    - `normalize_base_branch(cls, v)` (line 2659): no docstring
  - `InstanceWorkerSettings` (line 2663): Instance worker runtime settings.
    - `fallback_allow_pk_generation(cls, v)` (line 2701): no docstring
    - `fallback_relationship_strict(cls, v)` (line 2707): no docstring
    - `clamp_max_retry_attempts(cls, v)` (line 2713): no docstring
    - `clamp_untyped_ref_max_retry_attempts(cls, v)` (line 2718): no docstring
    - `clamp_untyped_ref_backoff_max_seconds(cls, v)` (line 2723): no docstring
  - `OntologyWorkerSettings` (line 2731): Ontology worker runtime settings.
    - `clamp_max_retry_attempts(cls, v)` (line 2757): no docstring
  - `ProjectionWorkerSettings` (line 2761): Projection worker runtime settings.
    - `clamp_max_retries(cls, v)` (line 2779): no docstring
  - `ActionWorkerSettings` (line 2783): Action worker runtime settings.
    - `clamp_dlq_retries(cls, v)` (line 2809): no docstring
    - `clamp_max_retry_attempts(cls, v)` (line 2814): no docstring
  - `ActionOutboxSettings` (line 2818): Action outbox worker settings.
    - `clamp_batch_size(cls, v)` (line 2840): no docstring
  - `OntologyDeployOutboxSettings` (line 2844): Ontology deployment outbox worker settings (OMS embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 2912): no docstring
    - `clamp_batch_size(cls, v)` (line 2917): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 2922): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 2927): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 2932): no docstring
    - `clamp_retention_days(cls, v)` (line 2937): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 2942): no docstring
    - `clamp_purge_limit(cls, v)` (line 2947): no docstring
  - `ConnectorSyncSettings` (line 2951): Connector sync worker settings.
    - `strip_strings(cls, v)` (line 2985): no docstring
    - `clamp_max_retries(cls, v)` (line 2992): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 2997): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3002): no docstring
  - `ConnectorTriggerSettings` (line 3006): Connector trigger service settings.
    - `strip_source_type(cls, v)` (line 3036): no docstring
    - `clamp_tick_seconds(cls, v)` (line 3041): no docstring
    - `clamp_poll_concurrency(cls, v)` (line 3046): no docstring
    - `clamp_outbox_batch(cls, v)` (line 3051): no docstring
  - `SearchProjectionSettings` (line 3055): Search projection worker settings.
    - `fallback_enabled(cls, v)` (line 3093): no docstring
    - `fallback_index_name(cls, v)` (line 3099): no docstring
    - `strip_handler(cls, v)` (line 3105): no docstring
    - `clamp_max_retries(cls, v)` (line 3110): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3115): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3120): no docstring
  - `ObjectifySettings` (line 3124): Objectify worker settings.
    - `strip_worker_handler(cls, v)` (line 3185): no docstring
    - `clamp_batch_size(cls, v)` (line 3190): no docstring
    - `clamp_row_batch_size(cls, v)` (line 3195): no docstring
    - `clamp_bulk_update_batch_size(cls, v)` (line 3200): no docstring
    - `clamp_list_page_size(cls, v)` (line 3207): no docstring
    - `clamp_max_rows(cls, v)` (line 3212): no docstring
    - `clamp_lineage_max_links(cls, v)` (line 3217): no docstring
    - `clamp_max_retries(cls, v)` (line 3222): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3227): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3232): no docstring
    - `normalize_ontology_pk_validation_mode(cls, v)` (line 3237): no docstring
    - `bulk_update_batch_size_effective(self)` (line 3250): no docstring
  - `IngestReconcilerSettings` (line 3254): Dataset ingest reconciler worker settings.
    - `fallback_alert_webhook_url(cls, v)` (line 3317): no docstring
    - `clamp_poll_seconds(cls, v)` (line 3326): no docstring
    - `clamp_stale_seconds(cls, v)` (line 3331): no docstring
    - `clamp_limit(cls, v)` (line 3336): no docstring
    - `clamp_lock_key(cls, v)` (line 3341): no docstring
    - `clamp_alert_published_threshold(cls, v)` (line 3346): no docstring
    - `clamp_alert_aborted_threshold(cls, v)` (line 3351): no docstring
    - `clamp_alert_cooldown_seconds(cls, v)` (line 3356): no docstring
  - `DatasetIngestOutboxSettings` (line 3360): Dataset ingest outbox worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3453): no docstring
    - `clamp_flush_timeout_seconds(cls, v)` (line 3458): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3467): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3472): no docstring
    - `clamp_max_retries(cls, v)` (line 3477): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 3482): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 3487): no docstring
    - `clamp_retention_days(cls, v)` (line 3492): no docstring
    - `clamp_purge_limit(cls, v)` (line 3497): no docstring
    - `clamp_dlq_max_in_flight(cls, v)` (line 3502): no docstring
    - `clamp_dlq_delivery_timeout_ms(cls, v)` (line 3507): no docstring
    - `clamp_dlq_request_timeout_ms(cls, v)` (line 3512): no docstring
    - `clamp_dlq_retries(cls, v)` (line 3517): no docstring
  - `ObjectifyOutboxWorkerSettings` (line 3521): Objectify outbox worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3609): no docstring
    - `clamp_batch_size(cls, v)` (line 3614): no docstring
    - `clamp_flush_timeout_seconds(cls, v)` (line 3619): no docstring
    - `clamp_backoff_base_seconds(cls, v)` (line 3628): no docstring
    - `clamp_backoff_max_seconds(cls, v)` (line 3633): no docstring
    - `clamp_claim_timeout_seconds(cls, v)` (line 3638): no docstring
    - `clamp_purge_interval_seconds(cls, v)` (line 3643): no docstring
    - `clamp_retention_days(cls, v)` (line 3648): no docstring
    - `clamp_purge_limit(cls, v)` (line 3653): no docstring
    - `clamp_producer_max_in_flight(cls, v)` (line 3658): no docstring
    - `clamp_producer_delivery_timeout_ms(cls, v)` (line 3663): no docstring
    - `clamp_producer_request_timeout_ms(cls, v)` (line 3668): no docstring
    - `clamp_producer_retries(cls, v)` (line 3673): no docstring
  - `ObjectifyReconcilerSettings` (line 3677): Objectify reconciler worker settings (BFF embedded worker).
    - `clamp_poll_seconds(cls, v)` (line 3715): no docstring
    - `clamp_stale_after_seconds(cls, v)` (line 3720): no docstring
    - `clamp_enqueued_stale_seconds(cls, v)` (line 3725): no docstring
    - `clamp_lock_key(cls, v)` (line 3730): no docstring
    - `enqueued_stale_seconds_effective(self)` (line 3734): no docstring
  - `WritebackMaterializerSettings` (line 3739): Writeback materializer worker settings.
    - `normalize_base_branch(cls, v)` (line 3769): no docstring
    - `normalize_db_names(cls, v)` (line 3774): no docstring
    - `db_names_list(self)` (line 3779): no docstring
  - `EventPublisherSettings` (line 3786): Event publisher (message relay) settings.
    - `fallback_poll_interval(cls, v)` (line 3848): no docstring
    - `fallback_batch_size(cls, v)` (line 3856): no docstring
    - `fallback_topic_bootstrap_timeout(cls, v)` (line 3864): no docstring
    - `clamp_poll_interval_seconds(cls, v)` (line 3870): no docstring
    - `clamp_batch_size(cls, v)` (line 3875): no docstring
    - `clamp_kafka_flush_batch_size(cls, v)` (line 3880): no docstring
    - `clamp_metrics_log_interval_seconds(cls, v)` (line 3887): no docstring
    - `clamp_lookback_seconds(cls, v)` (line 3892): no docstring
    - `clamp_lookback_max_keys(cls, v)` (line 3897): no docstring
    - `clamp_dedup_max_events(cls, v)` (line 3902): no docstring
    - `clamp_dedup_checkpoint_max_events(cls, v)` (line 3907): no docstring
    - `clamp_topic_bootstrap_timeout_seconds(cls, v)` (line 3912): no docstring
    - `kafka_flush_batch_size_effective(self)` (line 3916): no docstring
  - `AgentRetentionWorkerSettings` (line 3920): Agent session retention worker settings (SEC-005).
    - `clamp_poll_seconds(cls, v)` (line 3942): no docstring
    - `clamp_retention_days(cls, v)` (line 3947): no docstring
    - `normalize_action(cls, v)` (line 3952): no docstring
    - `strip_policy_json(cls, v)` (line 3962): no docstring
  - `SchemaChangeMonitorSettings` (line 3969): Schema change monitor settings for proactive drift detection.
    - `clamp_check_interval(cls, v)` (line 3995): no docstring
    - `clamp_cooldown(cls, v)` (line 4000): no docstring
  - `ChaosSettings` (line 4004): Chaos/fault injection settings (test-only).
    - `coerce_enabled(cls, v)` (line 4035): no docstring
    - `blank_to_none(cls, v)` (line 4044): no docstring
    - `coerce_crash_once(cls, v)` (line 4052): no docstring
    - `clamp_crash_exit_code(cls, v)` (line 4063): no docstring
  - `WorkersSettings` (line 4067): Workers/services runtime settings.
  - `WritebackSettings` (line 4098): Ontology writeback + read overlay settings
    - `strip_strings(cls, v)` (line 4155): no docstring
    - `blank_to_none(cls, v)` (line 4162): no docstring
  - `TestSettings` (line 4169): Test environment configuration
  - `GoogleSheetsSettings` (line 4193): Google Sheets integration settings
    - `fallback_google_api_key(cls, v)` (line 4229): no docstring
  - `ApplicationSettings` (line 4236): Main application settings - aggregates all other settings
    - `normalize_environment(cls, v)` (line 4288): no docstring
    - `is_development(self)` (line 4309): Check if running in development mode
    - `is_production(self)` (line 4314): Check if running in production mode
    - `is_test(self)` (line 4319): Check if running in test mode
    - `is_pytest(self)` (line 4324): Check if running under pytest (PYTEST_CURRENT_TEST set).

### `backend/shared/dependencies/__init__.py`

### `backend/shared/dependencies/container.py`
- **Functions**
  - `async get_container()` (line 336): Get the global service container
  - `async initialize_container(settings)` (line 354): Initialize the global service container (thread-safe)
  - `async shutdown_container()` (line 380): Shutdown the global service container
  - `async container_lifespan(settings)` (line 395): Async context manager for container lifecycle
  - `get_settings_from_container()` (line 417): Get settings from the global container (synchronous)
- **Classes**
  - `ServiceLifecycle` (line 30): Protocol for services that have lifecycle management
    - `async initialize(self)` (line 33): Initialize the service
    - `async health_check(self)` (line 37): Check if the service is healthy
    - `async shutdown(self)` (line 41): Shutdown the service gracefully
  - `ServiceFactory` (line 46): Protocol for service factory functions
    - `__call__(self, settings)` (line 49): Create a service instance from settings
  - `ServiceRegistration` (line 55): Service registration information
  - `ServiceContainer` (line 64): Modern dependency injection container
    - `__init__(self, settings)` (line 72): Initialize the service container
    - `is_initialized(self)` (line 85): Check if container is initialized
    - `register_singleton(self, service_type, factory)` (line 89): Register a singleton service with a factory function
    - `register_instance(self, service_type, instance)` (line 112): Register a service instance directly
    - `async get(self, service_type)` (line 132): Get a service instance (thread-safe)
    - `get_sync(self, service_type)` (line 184): Get a service instance synchronously (for use in factory functions)
    - `has(self, service_type)` (line 228): Check if a service is registered
    - `is_created(self, service_type)` (line 240): Check if a service instance has been created
    - `async health_check_all(self)` (line 255): Perform health check on all created services
    - `async shutdown_all(self)` (line 281): Shutdown all created services gracefully
    - `get_service_info(self)` (line 306): Get information about registered services
    - `async initialize_container(self)` (line 323): Initialize the container and mark as ready

### `backend/shared/dependencies/providers.py`
- **Functions**
  - `async get_settings_dependency()` (line 37): FastAPI dependency to get application settings
  - `async get_storage_service(container)` (line 47): FastAPI dependency to get StorageService instance
  - `async get_lakefs_storage_service(container)` (line 70): FastAPI dependency to get LakeFSStorageService instance (S3 gateway via lakeFS).
  - `async get_redis_service(container)` (line 84): FastAPI dependency to get RedisService instance
  - `async get_elasticsearch_service(container)` (line 103): FastAPI dependency to get ElasticsearchService instance
  - `async get_lineage_store(container)` (line 121): FastAPI dependency to get LineageStore instance.
  - `async get_audit_log_store(container)` (line 130): FastAPI dependency to get AuditLogStore instance.
  - `async get_llm_gateway(container)` (line 138): FastAPI dependency to get LLMGateway instance.
  - `async get_background_task_manager(container)` (line 146): FastAPI dependency to get BackgroundTaskManager instance.
  - `async get_initialized_background_task_manager(container)` (line 168): FastAPI dependency to get an already-initialized BackgroundTaskManager, if present.
  - `register_core_services(container)` (line 194): Register all core services with the container
  - `async health_check_core_services(container)` (line 218): Perform health check on all core services

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
  - `is_external_code(value)` (line 2156): no docstring
  - `_resolve_http_status(spec, status_code, prefer_status_code)` (line 2162): no docstring
  - `_resolve_retryable(spec, retryable_hint)` (line 2175): no docstring
  - `_resolve_http_status_hint(spec, status_code)` (line 2187): no docstring
  - `_resolve_default_retry_policy(spec)` (line 2193): no docstring
  - `_resolve_human_required(spec)` (line 2199): no docstring
  - `_resolve_max_attempts(spec, retry_policy)` (line 2205): no docstring
  - `_resolve_base_delay_ms(spec, retry_policy)` (line 2214): no docstring
  - `_resolve_max_delay_ms(spec, retry_policy)` (line 2223): no docstring
  - `_resolve_jitter_strategy(spec, retry_policy)` (line 2232): no docstring
  - `_resolve_retry_after_header_respect(spec)` (line 2238): no docstring
  - `enterprise_catalog_fingerprint()` (line 2247): no docstring
  - `_resolve_runbook_ref(spec, legacy_code)` (line 2287): no docstring
  - `_resolve_safe_next_actions(spec, legacy_code, retry_policy, human_required)` (line 2293): no docstring
  - `_resolve_action(spec)` (line 2322): no docstring
  - `_resolve_owner(spec)` (line 2326): no docstring
  - `resolve_enterprise_error(service_name, code, category, status_code, external_code, retryable_hint, prefer_status_code)` (line 2330): no docstring
  - `resolve_objectify_error(error)` (line 2397): no docstring
  - `_normalize_objectify_error_key(error)` (line 2450): no docstring
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
  - `_normalize_origin(service_name, origin)` (line 31): no docstring
  - `_derive_category_code(enterprise)` (line 44): no docstring
  - `build_error_envelope(service_name, message, detail, code, category, status_code, errors, context, external_code, objectify_error, enterprise, origin, request_id, correlation_id, trace_id, prefer_status_code)` (line 50): no docstring

### `backend/shared/errors/error_response.py`
- **Functions**
  - `_get_request_id(request)` (line 45): no docstring
  - `_get_correlation_id(request)` (line 54): no docstring
  - `_get_origin(request, service_name)` (line 63): no docstring
  - `_normalize_message(detail)` (line 76): no docstring
  - `_extract_upstream_metadata(body)` (line 88): no docstring
  - `_extract_external_code(detail)` (line 103): no docstring
  - `_classify_upstream_url(url, status_code)` (line 118): no docstring
  - `_classify_db_error(exc)` (line 140): no docstring
  - `_build_payload(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 161): no docstring
  - `_build_response(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 191): no docstring
  - `_resolve_validation_error(exc)` (line 219): no docstring
  - `install_error_handlers(app, service_name, validation_status)` (line 226): no docstring

### `backend/shared/errors/error_types.py`
- **Classes**
  - `ErrorCategory` (line 4): no docstring
  - `ErrorCode` (line 15): no docstring

### `backend/shared/i18n/__init__.py`

### `backend/shared/i18n/context.py`
- **Functions**
  - `set_language(lang)` (line 11): no docstring
  - `reset_language(token)` (line 15): no docstring
  - `get_language()` (line 19): no docstring

### `backend/shared/i18n/middleware.py`
- **Functions**
  - `install_i18n_middleware(app, max_body_bytes)` (line 14): Install request-scoped language + best-effort response localization.
  - `_rewrite_payload(payload, target_lang, status_code, api_status, is_root)` (line 97): no docstring

### `backend/shared/i18n/translator.py`
- **Functions**
  - `m(en, ko, lang, **params)` (line 9): Inline bilingual message helper.
  - `_generic_http_detail(status_code, lang)` (line 27): no docstring
  - `_generic_api_message(api_status, lang)` (line 48): no docstring
  - `_translate_known(text, target_lang)` (line 92): Small curated dictionary for common phrases.
  - `_translate_ko_to_en(text)` (line 139): no docstring
  - `localize_free_text(text, target_lang, status_code, api_status)` (line 160): Best-effort localization for existing free-text messages.

### `backend/shared/interfaces/__init__.py`

### `backend/shared/interfaces/type_inference.py`
- **Functions**
  - `get_production_type_inference_service()` (line 163): 🔥 Get REAL production type inference service!
  - `get_mock_type_inference_service()` (line 175): Legacy helper kept for backward compatibility.
- **Classes**
  - `TypeInferenceInterface` (line 13): Abstract interface for type inference services.
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 22): Analyze a single column and infer its type.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 46): Analyze an entire dataset and infer types for all columns.
    - `async infer_single_value_type(self, value, context)` (line 70): Infer the type of a single value.
  - `RealTypeInferenceService` (line 86): 🔥 REAL IMPLEMENTATION! Production-ready type inference service.
    - `__init__(self)` (line 94): Initialize with real pattern-based type detection service.
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 101): 🔥 REAL implementation using Funnel service algorithms.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 124): 🔥 REAL implementation using Funnel service algorithms.
    - `async infer_single_value_type(self, value, context)` (line 147): 🔥 REAL implementation using Funnel service algorithms.

### `backend/shared/middleware/__init__.py`

### `backend/shared/middleware/rate_limiter.py`
- **Functions**
  - `_is_valid_admin_bypass_token(headers)` (line 22): no docstring
  - `rate_limit(requests, window, strategy, cost)` (line 392): Rate limiting decorator for FastAPI endpoints
  - `install_rate_limit_headers_middleware(app)` (line 547): no docstring
  - `async get_rate_limiter()` (line 586): Get or create global rate limiter instance
- **Classes**
  - `TokenBucket` (line 33): Token Bucket algorithm implementation for rate limiting
    - `__init__(self, redis_client, capacity, refill_rate, key_prefix, fail_open)` (line 43): Initialize Token Bucket
    - `async consume(self, key, tokens)` (line 66): Try to consume tokens from the bucket
  - `LocalTokenBucket` (line 161): In-memory token bucket for degraded mode when Redis is unavailable.
    - `__init__(self, capacity, refill_rate, max_entries)` (line 164): no docstring
    - `_evict_if_needed(self)` (line 171): no docstring
    - `async consume(self, key, tokens)` (line 179): no docstring
  - `RateLimiter` (line 213): Rate limiting middleware for FastAPI
    - `__init__(self, redis_url)` (line 219): Initialize rate limiter
    - `async initialize(self)` (line 237): Initialize Redis connection
    - `async close(self)` (line 265): Close Redis connection
    - `get_bucket(self, bucket_type, capacity, refill_rate)` (line 270): Get or create a token bucket
    - `get_local_bucket(self, bucket_type, capacity, refill_rate)` (line 295): no docstring
    - `get_client_id(self, request, strategy)` (line 305): Get client identifier based on strategy
    - `async check_rate_limit(self, request, capacity, refill_rate, strategy, tokens)` (line 340): Check if request should be rate limited
  - `RateLimitPresets` (line 560): Common rate limit configurations

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
- **Classes**
  - `DataType` (line 13): Data type enumeration
    - `from_python_type(cls, py_type)` (line 62): Convert Python type to DataType
    - `is_numeric(cls, data_type)` (line 73): Check if data type is numeric
    - `is_temporal(cls, data_type)` (line 91): Check if data type is temporal
    - `validate_value(self, value)` (line 106): Validate if value matches this data type
    - `is_complex_type(cls, data_type)` (line 130): Check if data type is complex
    - `get_base_type(cls, data_type)` (line 154): Get base type for complex types
  - `Cardinality` (line 211): Cardinality enumeration
    - `is_valid(cls, value)` (line 222): Check if value is a valid cardinality
  - `QueryOperator` (line 228): Query operator definition
    - `can_apply_to(self, data_type)` (line 236): Check if operator can apply to data type

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
  - `EventEnvelope` (line 22): Canonical event envelope.
    - `_normalize_datetime(value)` (line 50): no docstring
    - `from_command(cls, command, actor, event_type, kafka_topic, metadata)` (line 56): no docstring
    - `from_base_event(cls, event, kafka_topic, metadata)` (line 111): no docstring
    - `from_connector_update(cls, source_type, source_id, cursor, previous_cursor, sequence_number, occurred_at, event_type, actor, kafka_topic, data, metadata)` (line 147): Build a canonical connector update envelope.
    - `as_kafka_key(self)` (line 220): no docstring
    - `as_json(self)` (line 224): no docstring

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
- **Classes**
  - `GoogleSheetPreviewRequest` (line 10): Google Sheet preview request model
    - `validate_google_sheet_url(cls, v)` (line 28): Validate Google Sheets URL format
  - `GoogleSheetPreviewResponse` (line 40): Google Sheet preview response model
    - `validate_sheet_id(cls, v)` (line 53): Validate sheet ID format
    - `validate_columns(cls, v)` (line 61): Validate columns
    - `validate_sample_rows(cls, v)` (line 69): Validate sample rows
    - `validate_total_rows(cls, v)` (line 77): Validate total rows
    - `validate_total_columns(cls, v)` (line 85): Validate total columns
  - `GoogleSheetError` (line 92): Google Sheet error response model
  - `GoogleSheetRegisterRequest` (line 110): Google Sheet registration request model
    - `validate_google_sheet_url(cls, v)` (line 127): Validate Google Sheets URL format
  - `GoogleSheetRegisterResponse` (line 139): Google Sheet registration response model

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
  - `_validate_localized_required(value, field_name)` (line 28): no docstring
  - `_validate_localized_optional(value, field_name)` (line 59): no docstring
- **Classes**
  - `Cardinality` (line 17): Cardinality enumeration
  - `QueryOperator` (line 65): Query operator definition
    - `can_apply_to(self, data_type)` (line 73): Check if operator can apply to data type
  - `OntologyBase` (line 78): Base ontology model with MVCC support through version field.
    - `validate_id(cls, v)` (line 99): Validate ID format
    - `validate_label(cls, v)` (line 107): no docstring
    - `validate_description(cls, v)` (line 112): no docstring
    - `set_timestamps(cls, values)` (line 117): Set timestamps if not provided
  - `Relationship` (line 137): Relationship model
    - `validate_label(cls, v)` (line 150): no docstring
    - `validate_description(cls, v)` (line 155): no docstring
    - `validate_inverse_label(cls, v)` (line 160): no docstring
    - `validate_cardinality(cls, v)` (line 165): Validate cardinality format
    - `is_valid_cardinality(self)` (line 175): Check if cardinality is valid
  - `Property` (line 181): Property model with class reference support
    - `validate_name(cls, v)` (line 225): Validate property name
    - `validate_type(cls, v)` (line 233): Validate property type
    - `validate_label(cls, v)` (line 241): no docstring
    - `validate_description(cls, v)` (line 246): no docstring
    - `validate_value(self, value)` (line 249): Validate property value
    - `is_class_reference(self)` (line 277): Check if this property is a class reference (ObjectProperty)
    - `to_relationship(self)` (line 318): Convert property to relationship format
  - `OntologyCreateRequest` (line 347): Request model for creating ontology
    - `validate_id(cls, v)` (line 363): Validate ID format (optional).
    - `validate_label(cls, v)` (line 373): Validate label is not empty (string or language map).
    - `validate_properties(cls, v)` (line 387): Validate properties don't have duplicate names
    - `validate_relationships(cls, v)` (line 397): Validate relationships don't have duplicate predicates
  - `OntologyUpdateRequest` (line 406): Request model for updating ontology
    - `validate_label(cls, v)` (line 419): no docstring
    - `validate_description(cls, v)` (line 424): no docstring
    - `validate_properties(cls, v)` (line 429): Validate properties don't have duplicate names
    - `validate_relationships(cls, v)` (line 439): Validate relationships don't have duplicate predicates
    - `has_changes(self)` (line 447): Check if request has any changes
  - `OntologyResponse` (line 462): Response model for ontology operations
    - `validate_structure(self)` (line 473): Validate ontology structure
  - `QueryFilter` (line 497): Query filter model
    - `validate_field(cls, v)` (line 506): Validate field name
    - `validate_operator(cls, v)` (line 514): Validate operator
  - `QueryInput` (line 534): Query input model
    - `validate_limit(cls, v)` (line 548): Validate limit
    - `validate_offset(cls, v)` (line 556): Validate offset
    - `validate_order_direction(cls, v)` (line 564): Validate order direction
    - `validate_class_identifier(self)` (line 571): Validate that either class_label or class_id is provided

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
  - `PipelinePlanOutputKind` (line 20): no docstring
  - `PipelinePlanDataScope` (line 26): no docstring
  - `PipelinePlanOutput` (line 34): no docstring
  - `PipelinePlanAssociation` (line 48): no docstring
    - `_normalize_fields(cls, data)` (line 63): no docstring
  - `PipelinePlan` (line 137): no docstring
    - `_validate_definition(cls, value)` (line 152): no docstring

### `backend/shared/models/pipeline_task_spec.py`
- **Classes**
  - `PipelineTaskScope` (line 17): no docstring
  - `PipelineTaskIntent` (line 22): no docstring
  - `PipelineTaskSpec` (line 31): Typed task spec returned by the task classifier.

### `backend/shared/models/requests.py`
- **Classes**
  - `BranchCreateRequest` (line 11): Request model for creating a branch
  - `CheckoutRequest` (line 20): Request model for checking out a branch or commit
  - `CommitRequest` (line 27): Request model for creating a commit
  - `MergeRequest` (line 35): Request model for merging branches
  - `RollbackRequest` (line 44): Request model for rolling back changes
  - `DatabaseCreateRequest` (line 51): Request model for creating a database
  - `MappingImportRequest` (line 59): Request model for importing mappings
    - `accepted(cls, message, data)` (line 70): Create accepted response (202 Accepted)
    - `no_content(cls, message)` (line 75): Create no content response (204 No Content)
    - `error(cls, message, errors)` (line 80): Create error response (4xx/5xx status codes)
    - `warning(cls, message, data)` (line 85): Create warning response (successful but with warnings)
    - `partial(cls, message, data, errors)` (line 90): Create partial success response (some operations succeeded, some failed)
    - `health_check(cls, service_name, version, description)` (line 97): Create standardized health check response
    - `is_success(self)` (line 107): Check if response indicates success
    - `is_error(self)` (line 111): Check if response indicates error
    - `is_warning(self)` (line 115): Check if response has warnings

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
  - `_default_risk_policy()` (line 13): no docstring
- **Classes**
  - `TypeInferenceResult` (line 17): Type inference result with confidence and reasoning
  - `FunnelRiskItem` (line 26): Risk signal emitted by Funnel (suggestion-only).
  - `ColumnProfile` (line 41): Lightweight column profiling summary (sample-based).
  - `ColumnAnalysisResult` (line 49): Analysis result for a single column
  - `FunnelAnalysisPayload` (line 93): Funnel analysis payload (suggestion-only).
  - `DatasetAnalysisRequest` (line 101): Request for dataset type analysis
  - `DatasetAnalysisResponse` (line 110): Response for dataset type analysis
  - `SchemaGenerationRequest` (line 124): Request for schema generation based on analysis
  - `SchemaGenerationResponse` (line 134): Generated schema based on type analysis
  - `FunnelPreviewRequest` (line 143): Request for data preview with type inference
  - `FunnelPreviewResponse` (line 152): Preview response with inferred types
  - `TypeMappingRequest` (line 169): Request for mapping inferred types to target schema
  - `TypeMappingResponse` (line 177): Response with mapped types for target system

### `backend/shared/observability/__init__.py`

### `backend/shared/observability/config_monitor.py`
- **Classes**
  - `ConfigChangeType` (line 30): Types of configuration changes
  - `ConfigSeverity` (line 39): Severity levels for configuration issues
  - `ConfigChange` (line 48): Represents a configuration change
    - `to_dict(self)` (line 60): Convert to dictionary for serialization
    - `_sanitize_value(self, value)` (line 74): Sanitize sensitive values for logging
    - `_is_sensitive_key(self, key_path)` (line 80): Check if a key path contains sensitive information
  - `ConfigValidationRule` (line 90): Configuration validation rule
  - `ConfigSecurityAudit` (line 101): Security audit result for configuration
    - `to_dict(self)` (line 109): Convert to dictionary for serialization
  - `ConfigurationMonitor` (line 120): Monitors configuration changes and provides observability
    - `__init__(self, settings)` (line 128): no docstring
    - `_initialize_default_validation_rules(self)` (line 141): Initialize default configuration validation rules
    - `add_validation_rule(self, rule)` (line 212): Add custom validation rule
    - `add_change_callback(self, callback)` (line 217): Add callback for configuration changes
    - `get_config_snapshot(self)` (line 221): Get current configuration snapshot
    - `calculate_config_hash(self, config_dict)` (line 257): Calculate hash of configuration for change detection
    - `detect_changes(self)` (line 262): Detect configuration changes since last check
    - `_compare_snapshots(self, old_snapshot, new_snapshot)` (line 293): Compare two configuration snapshots and return changes
    - `_analyze_change_impact(self, change)` (line 353): Analyze the impact of a configuration change
    - `validate_configuration(self)` (line 386): Validate current configuration against rules
    - `perform_security_audit(self)` (line 412): Perform comprehensive security audit of configuration
    - `_check_debug_mode_in_production(self, config)` (line 450): Check if debug mode is enabled in production
    - `_check_default_passwords(self, config)` (line 469): Check for default or weak passwords
    - `_check_insecure_connections(self, config)` (line 493): Check for insecure connection configurations
    - `_check_exposed_sensitive_data(self, config)` (line 515): Check for potentially exposed sensitive data
    - `_check_weak_authentication(self, config)` (line 526): Check for weak authentication configurations
    - `_get_nested_value(self, data, key_path)` (line 548): Get nested value from dictionary using dot notation
    - `_sanitize_sensitive_value(self, key_path, value)` (line 561): Sanitize sensitive values for display
    - `_count_total_settings(self, config)` (line 568): Count total number of configuration settings
    - `get_change_history(self, limit)` (line 578): Get recent configuration change history
    - `get_configuration_report(self)` (line 583): Get comprehensive configuration report

### `backend/shared/observability/context_propagation.py`
- **Functions**
  - `_to_text(value)` (line 60): no docstring
  - `_encode_baggage_value(value)` (line 76): no docstring
  - `_merge_baggage(existing, request_id, correlation_id, db_name)` (line 80): no docstring
  - `carrier_from_kafka_headers(kafka_headers)` (line 109): Extract a W3C carrier dict from confluent_kafka headers.
  - `carrier_from_envelope_metadata(payload_or_metadata)` (line 128): Extract a W3C carrier dict from an EventEnvelope.metadata-like dict.
  - `kafka_headers_from_carrier(carrier)` (line 177): no docstring
  - `kafka_headers_from_envelope_metadata(payload_or_metadata)` (line 190): no docstring
  - `kafka_headers_with_dedup(payload_or_metadata, dedup_id, event_id, aggregate_id)` (line 195): Build Kafka headers with deduplication ID for idempotent message processing.
  - `kafka_headers_from_current_context()` (line 234): Build Kafka headers (W3C Trace Context + baggage) from the current OTel context.
  - `enrich_metadata_with_current_trace(metadata)` (line 257): Mutate a metadata/payload dict by adding W3C trace context keys.
  - `attach_context_from_carrier(carrier, service_name)` (line 328): Attach an extracted context for the duration of the `with` block.
  - `attach_context_from_kafka(kafka_headers, fallback_metadata, service_name)` (line 372): Attach trace context from Kafka headers (preferred) or fallback metadata.

### `backend/shared/observability/logging.py`
- **Functions**
  - `install_trace_context_record_factory()` (line 39): Install a global LogRecord factory that always provides `trace_id`/`span_id`.
  - `install_trace_context_filter(logger)` (line 173): Install TraceContextFilter on the given logger (default: root logger).
- **Classes**
  - `TraceContextFilter` (line 112): Attach `trace_id` and `span_id` fields to every LogRecord.
    - `filter(self, record)` (line 119): no docstring

### `backend/shared/observability/metrics.py`
- **Functions**
  - `_log_no_op_once(reason)` (line 65): no docstring
  - `initialize_metrics_provider(service_name)` (line 73): Configure a global MeterProvider so OTel metrics are actually exported.
  - `_prom_counter(name, description, labelnames)` (line 141): no docstring
  - `_prom_histogram(name, description, labelnames)` (line 149): no docstring
  - `_prom_gauge(name, description, labelnames)` (line 157): no docstring
  - `measure_time(metric_name, collector)` (line 662): Decorator for measuring function execution time
  - `prometheus_latest()` (line 775): Render Prometheus metrics for `/metrics`.
  - `get_metrics_collector(service_name)` (line 789): Get or create global metrics collector
- **Classes**
  - `_SettingsValue` (line 40): no docstring
    - `__init__(self, getter)` (line 41): no docstring
    - `__get__(self, instance, owner)` (line 44): no docstring
  - `OpenTelemetryMetricsConfig` (line 48): no docstring
  - `MetricsCollector` (line 165): Centralized metrics collection based on Context7 patterns
    - `__init__(self, service_name)` (line 170): Initialize metrics collector
    - `_initialize_metrics(self)` (line 183): Initialize all metrics
    - `record_request(self, method, endpoint, status_code, duration, request_size, response_size)` (line 411): Record HTTP request metrics
    - `record_db_query(self, operation, table, duration, success)` (line 460): Record database query metrics
    - `record_cache_access(self, hit, cache_name)` (line 499): Record cache access
    - `record_event(self, event_type, action, duration)` (line 524): Record event sourcing metrics
    - `record_rate_limit(self, endpoint, rejected, strategy)` (line 562): Record rate limiting metrics
    - `record_business_metric(self, metric_name, value, attributes)` (line 599): Record custom business metrics
    - `timer(self, metric_name, attributes)` (line 635): Context manager for timing operations
  - `RequestMetricsMiddleware` (line 716): FastAPI middleware for automatic request metrics collection
    - `__init__(self, metrics_collector)` (line 721): Initialize middleware
    - `async __call__(self, request, call_next)` (line 731): Process request and collect metrics

### `backend/shared/observability/request_context.py`
- **Functions**
  - `_norm(value)` (line 42): no docstring
  - `generate_request_id()` (line 49): no docstring
  - `get_request_id()` (line 53): no docstring
  - `get_correlation_id()` (line 57): no docstring
  - `get_db_name()` (line 61): no docstring
  - `get_principal()` (line 65): no docstring
  - `parse_baggage_header(header_value)` (line 69): Best-effort parser for W3C `baggage` header.
  - `context_from_headers(headers)` (line 102): no docstring
  - `context_from_metadata(metadata)` (line 119): no docstring
  - `request_context(request_id, correlation_id, db_name, principal, inject_baggage)` (line 142): Attach request/debug context for the duration of a `with` block.

### `backend/shared/observability/tracing.py`
- **Functions**
  - `get_tracing_service(service_name)` (line 469): no docstring
  - `trace_endpoint(name)` (line 478): Lazily create a tracing decorator for request handlers.
  - `trace_db_operation(name)` (line 491): no docstring
  - `trace_external_call(name)` (line 497): no docstring
  - `_lazy_trace(name, kind, attributes)` (line 502): no docstring
- **Classes**
  - `_SettingsValue` (line 128): no docstring
    - `__init__(self, getter)` (line 129): no docstring
    - `__get__(self, instance, owner)` (line 132): no docstring
  - `OpenTelemetryConfig` (line 136): no docstring
  - `TracingService` (line 161): Distributed tracing facade.
    - `__init__(self, service_name)` (line 169): no docstring
    - `_log_no_op_once(self, reason)` (line 178): no docstring
    - `initialize(self)` (line 184): no docstring
    - `instrument_fastapi(self, app)` (line 276): no docstring
    - `instrument_clients(self)` (line 292): no docstring
    - `span(self, name, kind, attributes)` (line 376): no docstring
    - `trace(self, name, kind, attributes)` (line 391): no docstring
    - `get_current_span(self)` (line 411): no docstring
    - `record_exception(self, exception)` (line 416): no docstring
    - `set_span_attribute(self, key, value)` (line 423): no docstring
    - `get_trace_id(self)` (line 429): no docstring
    - `get_span_id(self)` (line 438): no docstring
    - `inject_trace_context(self, headers)` (line 447): no docstring
    - `extract_trace_context(self, headers)` (line 456): no docstring

### `backend/shared/routers/__init__.py`

### `backend/shared/routers/config_monitoring.py`
- **Functions**
  - `async get_settings()` (line 33): Get application settings
  - `async get_config_monitor(settings)` (line 38): Get or create configuration monitor
  - `async get_current_configuration(include_validation, monitor)` (line 60): Get current application configuration
  - `async get_configuration_changes(limit, severity, change_type, since, monitor)` (line 93): Get configuration change history
  - `async validate_configuration(monitor)` (line 178): Validate current configuration
  - `async perform_security_audit(monitor)` (line 221): Perform security audit of configuration
  - `async get_configuration_report(monitor)` (line 265): Get comprehensive configuration report
  - `async check_configuration_changes(background_tasks, monitor)` (line 304): Manually trigger configuration change detection
  - `async analyze_environment_drift(compare_environment, monitor)` (line 333): Analyze configuration drift between environments
  - `async analyze_configuration_health_impact(monitor)` (line 438): Analyze configuration health impact
  - `async get_monitoring_status(monitor)` (line 530): Get configuration monitoring system status

### `backend/shared/routers/monitoring.py`
- **Functions**
  - `async get_settings()` (line 36): Get application settings for monitoring
  - `_resolve_service_name(request)` (line 41): no docstring
  - `async _check_service_instance(instance)` (line 49): Best-effort, runtime-validated service health check.
  - `async basic_health_check()` (line 94): Basic health check endpoint
  - `async detailed_health_check(include_metrics, settings, container)` (line 110): Detailed health check with comprehensive service information
  - `async readiness_probe(container)` (line 174): Kubernetes readiness probe
  - `async liveness_probe(container)` (line 196): Kubernetes liveness probe
  - `async get_service_metrics(service_name)` (line 230): Get comprehensive service metrics
  - `async get_service_status(container)` (line 248): Get current status of all services
  - `async get_configuration_overview(include_sensitive, settings)` (line 275): Get current application configuration
  - `async restart_service(service_name, _)` (line 345): Restart a specific service
  - `async get_service_dependencies(_)` (line 365): Get service dependency information
  - `async get_background_task_metrics(request, task_manager)` (line 383): Get background task execution metrics
  - `async get_active_background_tasks(request, limit, task_manager)` (line 447): Get list of all active background tasks
  - `async get_background_task_health(request, task_manager)` (line 524): Get health status of background task processing system

### `backend/shared/security/__init__.py`

### `backend/shared/security/auth_utils.py`
- **Functions**
  - `get_expected_token(env_keys)` (line 16): no docstring
  - `extract_presented_token(headers)` (line 40): no docstring
  - `auth_disable_allowed(allow_disable_env_keys)` (line 50): no docstring
  - `auth_required(require_env_key, token_env_keys, default_required, allow_pytest, pytest_env_key)` (line 63): no docstring
  - `get_exempt_paths(env_key, defaults)` (line 86): no docstring
  - `is_exempt_path(path, exempt_paths)` (line 101): no docstring
  - `get_db_scope(headers)` (line 105): no docstring
  - `enforce_db_scope(headers, db_name, require_env_key)` (line 113): no docstring

### `backend/shared/security/data_encryption.py`
- **Functions**
  - `_strip_prefix(value, prefix)` (line 16): no docstring
  - `_b64decode(value)` (line 23): no docstring
  - `_b64encode(value)` (line 31): no docstring
  - `parse_encryption_keys(raw)` (line 35): Parse a comma-separated list of keys from env/settings.
  - `is_encrypted_text(value)` (line 153): no docstring
  - `is_encrypted_json(value)` (line 157): no docstring
  - `is_encrypted_bytes(value)` (line 161): no docstring
  - `encryptor_from_keys(raw_keys)` (line 165): no docstring
- **Classes**
  - `DataEncryptor` (line 68): no docstring
    - `_aesgcm(self, key)` (line 71): no docstring
    - `encrypt_text(self, plaintext, aad)` (line 74): no docstring
    - `decrypt_text(self, ciphertext, aad)` (line 84): no docstring
    - `encrypt_bytes(self, plaintext, aad)` (line 105): no docstring
    - `decrypt_bytes(self, ciphertext, aad)` (line 114): no docstring
    - `encrypt_json(self, value, aad)` (line 133): no docstring
    - `decrypt_json(self, value, aad)` (line 143): no docstring

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
  - `sanitize_input(data)` (line 693): 전역 입력 정화 함수
  - `sanitize_label_input(data)` (line 705): Sanitize a label-keyed payload (BFF).
  - `validate_db_name(db_name)` (line 724): 데이터베이스 이름 검증 함수
  - `validate_class_id(class_id)` (line 729): 클래스 ID 검증 함수
  - `validate_branch_name(branch_name)` (line 734): 브랜치 이름 검증 함수
  - `validate_instance_id(instance_id)` (line 739): 인스턴스 ID 검증 함수
  - `sanitize_es_query(query)` (line 744): Elasticsearch 쿼리 문자열 정제
- **Classes**
  - `SecurityViolationError` (line 18): 보안 위반 시 발생하는 예외
  - `InputSanitizer` (line 24): 포괄적인 입력 데이터 보안 검증 및 정화 클래스
    - `__init__(self)` (line 131): no docstring
    - `detect_sql_injection(self, value)` (line 158): SQL Injection 패턴 탐지
    - `detect_xss(self, value)` (line 165): XSS 패턴 탐지
    - `detect_path_traversal(self, value)` (line 172): Path Traversal 패턴 탐지
    - `detect_command_injection(self, value, is_shell_context)` (line 179): Command Injection 패턴 탐지
    - `detect_nosql_injection(self, value)` (line 199): NoSQL Injection 패턴 탐지
    - `detect_ldap_injection(self, value)` (line 206): LDAP Injection 패턴 탐지
    - `sanitize_string(self, value, max_length)` (line 213): 문자열 정화 처리
    - `sanitize_field_name(self, value)` (line 261): 필드명 정화 (id, name 등 일반적인 필드명 허용)
    - `sanitize_label_key(self, value)` (line 291): Label-key sanitizer for "label-based" payloads (BFF).
    - `sanitize_label_dict(self, data, max_depth, current_depth)` (line 313): Sanitize a dict whose keys are *labels* (human-facing), not internal field names.
    - `sanitize_map_key(self, value, max_length)` (line 346): Sanitize keys for *free-form* key/value maps embedded in payloads.
    - `sanitize_identifier_mapping(self, data, max_depth, current_depth)` (line 375): Sanitize a mapping whose keys AND values are identifiers (e.g., column rename maps).
    - `sanitize_description(self, value)` (line 410): 설명 텍스트 정화 (command injection 체크 안함)
    - `sanitize_sql_expression(self, value, max_length)` (line 434): Sanitize a SQL *expression* (Spark SQL / ETL compute predicate / select expr).
    - `sanitize_shell_command(self, value)` (line 454): Shell 명령어 컨텍스트의 문자열 정화 (모든 보안 체크 적용)
    - `sanitize_dict(self, data, max_depth, current_depth)` (line 468): 딕셔너리 재귀적 정화 처리
    - `sanitize_list(self, data, max_depth, current_depth)` (line 550): 리스트 정화 처리
    - `sanitize_any(self, value, max_depth, current_depth)` (line 572): 모든 타입의 데이터 정화 처리
    - `validate_database_name(self, db_name)` (line 600): 데이터베이스 이름 검증 - 엄격한 규칙 적용
    - `validate_class_id(self, class_id)` (line 637): 클래스 ID 검증
    - `validate_branch_name(self, branch_name)` (line 651): 브랜치 이름 검증
    - `validate_instance_id(self, instance_id)` (line 670): 인스턴스 ID 검증

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
  - `ComplexTypeSerializer` (line 15): Complex type serializer for converting between internal and external representations
    - `serialize(value, data_type, constraints)` (line 19): Serialize a complex type value to string representation
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
  - `async run_agent_session_retention_worker(session_registry, poll_interval_seconds, retention_days, stop_event, tenant_id, action, storage_service, delete_file_upload_objects, retention_policy_json)` (line 18): Background retention worker for agent session data (SEC-005).
  - `_parse_retention_policy(raw)` (line 87): no docstring
  - `_policy_days_action(policy, key, default_days, default_action)` (line 132): no docstring
  - `async _apply_policy(session_registry, storage_service, tenant_id, delete_uploads, default_days, default_action, now, policy)` (line 154): no docstring

### `backend/shared/services/agent/agent_tool_allowlist.py`
- **Functions**
  - `_derive_resource_scopes(path)` (line 10): no docstring
  - `_derive_tool_type(tool_id, method, risk_level)` (line 28): no docstring
  - `_default_allowlist_bundle_path()` (line 52): no docstring
  - `load_agent_tool_allowlist_bundle(bundle_path)` (line 56): no docstring
  - `async bootstrap_agent_tool_allowlist(tool_registry, bundle_path, only_if_empty)` (line 76): no docstring

### `backend/shared/services/agent/llm_gateway.py`
- **Functions**
  - `_load_pricing_table(raw)` (line 85): no docstring
  - `_estimate_cost(model, prompt_tokens, completion_tokens, pricing_json)` (line 118): no docstring
  - `_parse_provider_policies(raw)` (line 130): no docstring
  - `_coerce_provider_extra_headers(value)` (line 151): no docstring
  - `_coerce_provider_extra_body(value)` (line 166): no docstring
  - `_filter_provider_extra_body(provider, extra_body)` (line 178): no docstring
  - `_build_provider_send_overrides(provider, provider_policy, audit_partition_key, audit_actor)` (line 189): Build provider send overrides (SEC-002).
  - `_extract_json_object(text)` (line 241): Best-effort JSON object extraction.
  - `_tool_parameters_from_model(model)` (line 283): Build an OpenAI tool/function `parameters` schema from a Pydantic model.
  - `_openai_max_tokens_params(model, max_tokens)` (line 311): OpenAI compatibility: newer models (ex: gpt-5) require max_completion_tokens.
  - `_openai_temperature_params(model, temperature)` (line 321): OpenAI compatibility: gpt-5 only supports the default temperature (1).
  - `_openai_reasoning_params(model)` (line 331): OpenAI responses API: gpt-5 needs low reasoning effort to emit output tokens.
  - `_use_openai_responses_api(model)` (line 341): no docstring
  - `_extract_openai_responses_text(data)` (line 346): no docstring
  - `_mask_payload(value, max_chars)` (line 375): no docstring
  - `_log_llm_event(event, payload, max_chars)` (line 385): no docstring
  - `create_llm_gateway(settings)` (line 1447): no docstring
- **Classes**
  - `LLMUnavailableError` (line 47): no docstring
  - `LLMRequestError` (line 51): no docstring
  - `LLMOutputValidationError` (line 55): no docstring
  - `LLMPolicyError` (line 59): Raised when a model/tool/policy guard blocks an LLM call.
  - `LLMHTTPStatusError` (line 63): no docstring
    - `__init__(self, status_code, body_preview)` (line 64): no docstring
  - `LLMCallMeta` (line 71): no docstring
  - `LLMGateway` (line 393): A thin, safe wrapper around an LLM provider.
    - `__init__(self, settings)` (line 402): no docstring
    - `is_enabled(self)` (line 447): no docstring
    - `_circuit_key(self, provider, model)` (line 460): no docstring
    - `_is_circuit_open(self, circuit_key)` (line 463): no docstring
    - `_record_circuit_success(self, circuit_key)` (line 468): no docstring
    - `_record_circuit_failure(self, circuit_key)` (line 472): no docstring
    - `_retry_delay_s(self, prompt_hash, attempt)` (line 480): no docstring
    - `_should_retry(self, exc)` (line 490): no docstring
    - `async _call_openai_compat_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 497): no docstring
    - `async _call_openai_compat_tool_call(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, tool_name, tool_parameters, extra_headers, extra_body)` (line 601): no docstring
    - `async _call_openai_compat_responses_json(self, task, system_prompt, user_prompt, model, max_tokens, prompt_hash, tool_parameters, schema_name, extra_headers, extra_body)` (line 727): no docstring
    - `async _call_anthropic_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 904): no docstring
    - `async _call_google_json(self, task, system_prompt, user_prompt, model, temperature, max_tokens, prompt_hash, extra_headers, extra_body)` (line 959): no docstring
    - `async complete_json(self, task, system_prompt, user_prompt, response_model, model, allowed_models, use_native_tool_calling, redis_service, audit_store, audit_partition_key, audit_actor, audit_resource_id, audit_metadata, temperature, max_tokens)` (line 1018): no docstring

### `backend/shared/services/agent/llm_quota.py`
- **Functions**
  - `approx_token_count(payload)` (line 26): no docstring
  - `_sanitize_key_part(value)` (line 37): no docstring
  - `_extract_quota_spec(data_policies, model_id)` (line 44): no docstring
  - `async enforce_llm_quota(redis_service, tenant_id, user_id, model_id, system_prompt, user_prompt, data_policies, now)` (line 85): Best-effort quota enforcement (NFR-004) using Redis counters.
- **Classes**
  - `LLMQuotaSpec` (line 12): no docstring
  - `LLMQuotaExceededError` (line 18): no docstring
    - `__init__(self, message, spec, used_calls, used_tokens)` (line 19): no docstring

### `backend/shared/services/core/__init__.py`

### `backend/shared/services/core/async_terminus.py`
- **Classes**
  - `AsyncTerminusService` (line 14): Lightweight TerminusDB service for BFF
    - `__init__(self, connection_info)` (line 17): no docstring
    - `async connect(self)` (line 21): Establish connection to TerminusDB
    - `async ping(self)` (line 30): Check if TerminusDB is accessible
    - `async close(self)` (line 41): Close the connection

### `backend/shared/services/core/audit_log_store.py`
- **Functions**
  - `create_audit_log_store(settings)` (line 444): no docstring
- **Classes**
  - `AuditLogStore` (line 22): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 23): no docstring
    - `async initialize(self)` (line 39): no docstring
    - `async connect(self)` (line 42): no docstring
    - `async shutdown(self)` (line 53): no docstring
    - `async close(self)` (line 56): no docstring
    - `async health_check(self)` (line 61): no docstring
    - `async ensure_schema(self)` (line 71): no docstring
    - `_canonical_json(value)` (line 132): no docstring
    - `_compute_hash(cls, prev_hash, payload)` (line 136): no docstring
    - `async append(self, entry, partition_key)` (line 141): no docstring
    - `async log(self, partition_key, actor, action, status, resource_type, resource_id, event_id, command_id, trace_id, correlation_id, metadata, error, occurred_at)` (line 244): no docstring
    - `async list_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset)` (line 277): no docstring
    - `async count_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until)` (line 365): no docstring
    - `async get_chain_head(self, partition_key)` (line 419): no docstring

### `backend/shared/services/core/background_task_manager.py`
- **Functions**
  - `create_background_task_manager(redis_service, websocket_service)` (line 664): Create a BackgroundTaskManager instance.
- **Classes**
  - `TaskPriority` (line 42): Task execution priority levels.
  - `BackgroundTaskManager` (line 49): Centralized background task management service.
    - `__init__(self, redis_service, websocket_service)` (line 57): Initialize the background task manager.
    - `async start(self)` (line 84): Start the background task manager.
    - `async stop(self)` (line 91): Stop the background task manager and cancel all tasks.
    - `async create_task(self, func, *args, task_id, task_name, task_type, priority, metadata, **kwargs)` (line 109): Create and track a new background task.
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
  - `CommandStatus` (line 20): Command execution status enumeration.
  - `CommandStatusService` (line 30): Service for tracking command execution status.
    - `__init__(self, redis_service)` (line 42): no docstring
    - `async create_command_status(self, command_id, command_type, aggregate_id, payload, user_id)` (line 46): Create initial command status entry.
    - `async update_status(self, command_id, status, message, error, progress)` (line 88): Update command status.
    - `async start_processing(self, command_id, worker_id)` (line 167): Mark command as being processed.
    - `async complete_command(self, command_id, result, message)` (line 192): Mark command as completed with result.
    - `async fail_command(self, command_id, error, retry_count)` (line 220): Mark command as failed.
    - `async cancel_command(self, command_id, reason)` (line 248): Cancel a pending or processing command.
    - `async get_command_details(self, command_id)` (line 283): Get complete command details including status and result.
    - `async list_user_commands(self, user_id, status_filter, limit)` (line 316): List commands for a specific user.
    - `async cleanup_old_commands(self, days)` (line 363): Clean up commands older than specified days.
    - `async set_command_status(self, command_id, status, metadata)` (line 379): Legacy compatibility method for setting command status.
    - `async get_command_status(self, command_id)` (line 422): Legacy compatibility method for getting command status.
    - `async get_command_result(self, command_id)` (line 449): Legacy compatibility method for getting command result.

### `backend/shared/services/core/consistency_checker.py`
- **Functions**
  - `async run_consistency_check(db_name, class_id)` (line 405): Run complete consistency check
- **Classes**
  - `ConsistencyChecker` (line 27): Real-time consistency verification for Event Sourcing + CQRS
    - `__init__(self, es_url, tdb_url, redis_url, s3_endpoint, s3_access_key, s3_secret_key)` (line 33): no docstring
    - `async check_all_invariants(self, db_name, class_id)` (line 61): Check all 6 production invariants
    - `async check_es_tdb_function(self, db_name, class_id)` (line 92): Invariant 1: ES projection = TDB partial function
    - `async check_idempotency(self, db_name, class_id)` (line 142): Invariant 2: Idempotency (exactly-once processing)
    - `async check_ordering(self, db_name, class_id)` (line 180): Invariant 3: Ordering guarantee (per-aggregate)
    - `async check_schema_composition(self, db_name, class_id)` (line 241): Invariant 4: Schema-projection composition preservation
    - `async check_replay_determinism(self, db_name, class_id)` (line 296): Invariant 5: Replay determinism
    - `async check_read_your_writes(self, db_name, class_id)` (line 362): Invariant 6: Read-your-writes guarantee

### `backend/shared/services/core/consistency_token.py`
- **Functions**
  - `async demo_consistency_token()` (line 354): Demo the consistency token functionality
- **Classes**
  - `ConsistencyToken` (line 20): Token containing sufficient information to ensure read-your-writes consistency
    - `to_string(self)` (line 31): Encode token as a compact string
    - `from_string(cls, token_str)` (line 50): Decode token from string
  - `ConsistencyTokenService` (line 82): Service for managing consistency tokens
    - `__init__(self, redis_url)` (line 88): no docstring
    - `async connect(self)` (line 92): Initialize Redis connection
    - `async disconnect(self)` (line 96): Close Redis connection
    - `async create_token(self, command_id, aggregate_id, sequence_number, version)` (line 101): Create a new consistency token after a write operation
    - `async _estimate_projection_lag(self)` (line 137): Estimate current projection lag in milliseconds
    - `async _store_token_metadata(self, token)` (line 154): Store token metadata for validation
    - `async wait_for_consistency(self, token, es_client, max_wait_ms)` (line 179): Wait until the write represented by the token is visible
    - `async _check_write_visible(self, token, es_client)` (line 222): Check if a write is visible in Elasticsearch
    - `async validate_token(self, token_str)` (line 251): Validate a consistency token
    - `async get_read_timestamp(self, token)` (line 279): Get the minimum timestamp that guarantees consistency
    - `async update_projection_lag(self, actual_lag_ms)` (line 292): Update the estimated projection lag based on actual measurements
  - `CommandResponseWithToken` (line 317): Enhanced command response that includes consistency token
    - `__init__(self, command_id, status, result, consistency_token)` (line 322): no docstring
    - `to_dict(self)` (line 334): Convert to dictionary for API response

### `backend/shared/services/core/graph_federation_service_woql.py`
- **Classes**
  - `GraphFederationServiceWOQL` (line 27): Production-ready Graph Federation using REAL WOQL
    - `__init__(self, terminus_service, es_host, es_port, es_username, es_password)` (line 35): no docstring
    - `_terminus_branch_path(branch)` (line 52): Build `/local/branch/{...}` descriptor segment for TerminusDB APIs (WOQL/Document).
    - `_extract_binding_literal(value)` (line 60): no docstring
    - `_parse_timestamp(value)` (line 76): no docstring
    - `async _fetch_schema_class_doc(self, db_name, class_id, branch)` (line 99): no docstring
    - `_schema_range_for_predicate(schema_doc, predicate)` (line 117): no docstring
    - `_normalize_hops(hops)` (line 131): Normalize hop specs into a stable (predicate, target_class, reverse) tuple list.
    - `async _validate_hop_semantics(self, db_name, branch, start_class, hops)` (line 161): no docstring
    - `async multi_hop_query(self, db_name, start_class, hops, base_branch, overlay_branch, terminus_branch, strict_overlay, filters, limit, offset, max_nodes, max_edges, include_paths, max_paths, no_cycles, include_documents, include_audit)` (line 193): Execute multi-hop graph query with ES federation using REAL WOQL
    - `async simple_graph_query(self, db_name, class_name, base_branch, overlay_branch, terminus_branch, strict_overlay, filters, include_documents, include_audit)` (line 491): Simple single-class query - lightweight graph federation
    - `_build_simple_woql(self, class_name, filters)` (line 585): Build WOQL query for simple class query
    - `_build_multi_hop_woql(self, start_class, hops, filters)` (line 625): Build WOQL query for multi-hop traversal
    - `_extract_es_doc_id(self, instance_id)` (line 738): Extract ES document ID from instance ID
    - `async find_relationship_paths(self, db_name, source_class, target_class, branch, max_depth)` (line 745): Find all relationship paths between two classes using REAL WOQL schema queries
    - `_get_known_paths(self, source_class, target_class)` (line 833): Fallback to known relationship paths
    - `_get_known_multi_hop_paths(self, source_class, target_class, max_depth)` (line 850): Get known multi-hop paths (would be replaced by recursive WOQL search)
    - `async _fetch_es_documents(self, db_name, doc_ids, base_branch, overlay_branch, strict_overlay)` (line 860): Fetch documents from Elasticsearch for the given graph node IDs.
    - `async _fetch_audit_records(self, db_name, doc_ids)` (line 997): Fetch audit records from Elasticsearch audit index

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
  - `DatabaseHealthCheck` (line 116): Health check for database connections
    - `__init__(self, db_connection, service_name)` (line 119): no docstring
    - `service_name(self)` (line 124): no docstring
    - `service_type(self)` (line 128): no docstring
    - `async health_check(self)` (line 131): Check database connectivity and performance
  - `RedisHealthCheck` (line 172): Health check for Redis connections
    - `__init__(self, redis_service, service_name)` (line 175): no docstring
    - `service_name(self)` (line 180): no docstring
    - `service_type(self)` (line 184): no docstring
    - `async health_check(self)` (line 187): Check Redis connectivity and performance
  - `ElasticsearchHealthCheck` (line 236): Health check for Elasticsearch connections
    - `__init__(self, elasticsearch_service, service_name)` (line 239): no docstring
    - `service_name(self)` (line 244): no docstring
    - `service_type(self)` (line 248): no docstring
    - `async health_check(self)` (line 251): Check Elasticsearch connectivity and cluster health
  - `StorageHealthCheck` (line 304): Health check for storage services (S3, etc.)
    - `__init__(self, storage_service, service_name)` (line 307): no docstring
    - `service_name(self)` (line 312): no docstring
    - `service_type(self)` (line 316): no docstring
    - `async health_check(self)` (line 319): Check storage service connectivity
  - `TerminusDBHealthCheck` (line 369): Health check for TerminusDB connections
    - `__init__(self, terminus_service, service_name)` (line 372): no docstring
    - `service_name(self)` (line 377): no docstring
    - `service_type(self)` (line 381): no docstring
    - `async health_check(self)` (line 384): Check TerminusDB connectivity and basic operations
  - `AggregatedHealthStatus` (line 445): Aggregated health status for the entire system
    - `to_dict(self)` (line 452): Convert to dictionary for serialization
  - `HealthCheckAggregator` (line 462): Aggregates health checks from multiple services
    - `__init__(self)` (line 465): no docstring
    - `register_health_checker(self, health_checker)` (line 468): Register a health checker
    - `async check_all_services(self, timeout_seconds)` (line 472): Check all registered services and aggregate results
    - `_determine_overall_status(self, results)` (line 515): Determine overall system health based on individual service results
    - `_generate_summary(self, results)` (line 537): Generate summary statistics

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

### `backend/shared/services/core/projection_manager.py`
- **Classes**
  - `ProjectionManager` (line 23): 프로젝션 기반 조회 최적화
    - `__init__(self, graph_service, es_service, redis_service)` (line 33): no docstring
    - `async initialize(self)` (line 52): 프로젝션 매니저 초기화
    - `async register_projection(self, db_name, view_name, start_class, hops, filters, refresh_interval)` (line 80): 새로운 프로젝션 등록
    - `async materialize_view(self, projection_id, force_refresh)` (line 154): 프로젝션 뷰 실체화 (WOQL 실행 → ES 저장)
    - `async query_projection(self, view_name, filters, limit)` (line 274): 프로젝션 뷰 조회 (캐시된 데이터)
    - `async _projection_refresh_loop(self, projection_id)` (line 326): 프로젝션 자동 갱신 루프
    - `async _load_existing_projections(self)` (line 348): 기존 프로젝션 메타데이터 로드
    - `_generate_query_hash(self, db_name, woql_config)` (line 374): 쿼리 설정의 해시 생성
    - `async stop_projection(self, projection_id)` (line 379): 프로젝션 중지
    - `async shutdown(self)` (line 401): 프로젝션 매니저 종료

### `backend/shared/services/core/schema_change_monitor.py`
- **Classes**
  - `MonitorConfig` (line 34): Configuration for schema change monitoring
    - `from_dict(cls, config_dict)` (line 52): no docstring
  - `SchemaChangeMonitor` (line 62): Monitors schema changes for active mapping specs and broadcasts notifications.
    - `__init__(self, drift_detector, dataset_registry, objectify_registry, websocket_service, config)` (line 73): Args:
    - `add_drift_callback(self, callback)` (line 105): Add a callback to be invoked when drift is detected
    - `async start(self)` (line 112): Start the schema change monitor.
    - `async stop(self)` (line 132): Stop the schema change monitor
    - `_on_task_done(self, task)` (line 145): Handle task completion
    - `async _monitoring_loop(self)` (line 154): Main monitoring loop
    - `async _check_all_active_mappings(self)` (line 168): Check all active mapping specs for schema drift
    - `async _check_mapping_spec(self, spec)` (line 193): Check a single mapping spec for schema drift
    - `_should_notify(self, drift, subject_id)` (line 270): Determine if notification should be sent
    - `async _notify_drift(self, drift, impacted_mappings)` (line 289): Send drift notification
    - `async check_mapping_spec_compatibility(self, mapping_spec_id)` (line 316): On-demand compatibility check for a specific mapping spec.
    - `get_status(self)` (line 416): Get monitor status

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
  - `SchemaVersion` (line 18): Represents a schema version with comparison capabilities.
    - `__init__(self, version_string)` (line 24): Initialize schema version from string.
    - `__str__(self)` (line 40): no docstring
    - `__eq__(self, other)` (line 43): no docstring
    - `__lt__(self, other)` (line 50): no docstring
    - `__le__(self, other)` (line 59): no docstring
    - `is_backward_compatible(self, other)` (line 62): Check if this version is backward compatible with another.
  - `MigrationStrategy` (line 81): Migration strategies for schema changes
  - `SchemaMigration` (line 91): Represents a migration from one schema version to another.
    - `__init__(self, from_version, to_version, entity_type, migration_func, description)` (line 96): Initialize schema migration.
    - `apply(self, data)` (line 120): Apply migration to data.
  - `SchemaRegistry` (line 139): Central registry for schema versions and migrations.
    - `__init__(self)` (line 144): Initialize schema registry
    - `register_schema(self, entity_type, version, schema)` (line 150): Register a schema version.
    - `register_migration(self, migration)` (line 177): Register a migration between versions.
    - `get_migration_path(self, entity_type, from_version, to_version)` (line 195): Find migration path between versions.
    - `migrate_data(self, data, entity_type, target_version)` (line 252): Migrate data to target version.
  - `SchemaVersioningService` (line 293): High-level service for schema versioning operations.
    - `__init__(self, registry)` (line 298): Initialize schema versioning service.
    - `_initialize_default_schemas(self)` (line 308): Initialize default schemas and migrations
    - `version_event(self, event)` (line 378): Add or update schema version for an event.
    - `migrate_event(self, event, target_version)` (line 399): Migrate event to target version.
    - `is_compatible(self, data, entity_type, required_version)` (line 416): Check if data version is compatible with required version.

### `backend/shared/services/core/sequence_service.py`
- **Classes**
  - `SequenceService` (line 17): Provides sequence number generation for aggregates.
    - `__init__(self, redis_client, namespace)` (line 25): Initialize sequence service.
    - `_get_key(self, aggregate_id)` (line 41): Get Redis key for aggregate sequence.
    - `async get_next_sequence(self, aggregate_id)` (line 53): Get next sequence number for aggregate.
    - `async get_current_sequence(self, aggregate_id)` (line 74): Get current sequence number without incrementing.
    - `async set_sequence(self, aggregate_id, sequence)` (line 99): Set sequence number for aggregate (used for recovery/replay).
    - `async reset_aggregate(self, aggregate_id)` (line 133): Reset sequence for an aggregate (dangerous - use carefully).
    - `async get_batch_sequences(self, aggregate_id, count)` (line 153): Reserve a batch of sequence numbers for bulk operations.
    - `async get_all_sequences(self, pattern)` (line 184): Get all current sequences (for monitoring/debugging).
    - `async cleanup_old_sequences(self, ttl_seconds)` (line 206): Set TTL on sequence keys for inactive aggregates.
  - `SequenceValidator` (line 231): Validates event sequences for consistency.
    - `__init__(self, sequence_service)` (line 236): Initialize sequence validator.
    - `async validate_sequence(self, aggregate_id, sequence, allow_gaps)` (line 246): Validate sequence number for aggregate.
    - `reset_expectations(self)` (line 282): Reset local sequence expectations

### `backend/shared/services/core/service_factory.py`
- **Functions**
  - `create_fastapi_service(service_info, custom_lifespan, include_health_check, include_logging_middleware, custom_tags, include_error_handlers, validation_error_status)` (line 60): Create a standardized FastAPI application with common configurations.
  - `_install_openapi_language_contract(app)` (line 154): Add `?lang=en|ko` and `Accept-Language` to OpenAPI so clients discover i18n support.
  - `_configure_cors(app)` (line 204): Configure CORS middleware based on environment variables
  - `_add_logging_middleware(app)` (line 219): Add request logging middleware
  - `_add_health_check(app, service_info)` (line 262): Add standardized health check endpoints
  - `_add_debug_endpoints(app)` (line 286): Add debug endpoints for development environment
  - `_install_observability(app, service_info)` (line 295): Install tracing + metrics in a way that is:
  - `create_uvicorn_config(service_info, reload)` (line 338): Create standardized uvicorn configuration.
  - `_get_logging_config(service_name)` (line 371): Get standardized logging configuration for uvicorn
  - `run_service(app, service_info, app_module_path, reload)` (line 416): Run the service with standardized uvicorn configuration.
  - `get_bff_service_info()` (line 435): no docstring
  - `get_oms_service_info()` (line 467): no docstring
  - `get_funnel_service_info()` (line 485): no docstring
  - `get_agent_service_info()` (line 501): no docstring
- **Classes**
  - `ServiceInfo` (line 38): Service configuration container
    - `__init__(self, name, title, description, version, port, host, tags)` (line 41): no docstring

### `backend/shared/services/core/sheet_grid_parser.py`
- **Classes**
  - `SheetGridParseOptions` (line 28): Options shared across parsers.
  - `SheetGridParser` (line 44): Parsers for Excel and Google Sheets into SheetGrid.
    - `from_google_sheets_values(cls, values, merged_cells, sheet_name, options, metadata)` (line 52): Build SheetGrid from Google Sheets "values" matrix (A1-anchored).
    - `merged_cells_from_google_metadata(cls, sheets_metadata, worksheet_name, sheet_id)` (line 91): Extract merged ranges from Google Sheets "spreadsheets.get" metadata JSON.
    - `from_excel_bytes(cls, xlsx_bytes, sheet_name, options, metadata)` (line 133): Parse an .xlsx file into SheetGrid.
    - `_normalize_grid(cls, grid, max_rows, max_cols)` (line 332): no docstring
    - `_json_safe_cell(value)` (line 352): no docstring
    - `_trim_trailing_empty(cls, grid, min_rows, min_cols)` (line 367): no docstring
    - `_clip_merge_ranges(cls, merges, rows, cols)` (line 401): no docstring
    - `_excel_cell_to_display_value(cls, cell, fallback_to_formula)` (line 423): Best-effort conversion of an openpyxl cell into a display-like value.
    - `_format_excel_number(cls, value, fmt)` (line 460): no docstring
    - `_detect_currency_affixes(cls, fmt)` (line 496): no docstring
    - `_infer_decimal_places_from_format(fmt)` (line 522): no docstring

### `backend/shared/services/core/sheet_import_service.py`
- **Classes**
  - `FieldMapping` (line 27): no docstring
  - `SheetImportService` (line 32): Pure helpers; no network/IO.
    - `build_column_index(columns)` (line 36): no docstring
    - `_is_blank(value)` (line 49): no docstring
    - `_strip_numeric_affixes(raw)` (line 55): Remove common affixes around numeric strings (currency symbols/units/codes, percent).
    - `coerce_value(cls, value, target_type)` (line 113): Coerce a cell value into a JSON-serializable value compatible with target_type.
    - `build_instances(cls, columns, rows, mappings, target_field_types, max_rows)` (line 233): Apply mappings and type coercion to build target instances.

### `backend/shared/services/core/sync_wrapper_service.py`
- **Classes**
  - `SyncWrapperService` (line 20): 비동기 Command API를 동기적으로 래핑하는 서비스.
    - `__init__(self, command_status_service)` (line 27): no docstring
    - `async wait_for_command(self, command_id, options)` (line 30): Command가 완료될 때까지 기다리고 결과를 반환합니다.
    - `async _poll_until_complete(self, command_id, options, progress_history, start_time)` (line 113): Command가 완료될 때까지 주기적으로 상태를 확인합니다.
    - `async execute_sync(self, async_func, request_data, options)` (line 177): 비동기 함수를 실행하고 결과를 기다립니다.
    - `async get_command_progress(self, command_id)` (line 217): Command의 현재 진행 상태를 조회합니다.

### `backend/shared/services/core/watermark_monitor.py`
- **Functions**
  - `async create_watermark_monitor(kafka_config, redis_url, consumer_groups, topics)` (line 423): Create and start a watermark monitor
- **Classes**
  - `PartitionWatermark` (line 24): Watermark information for a single partition
    - `progress_percentage(self)` (line 35): Calculate progress as percentage
  - `GlobalWatermark` (line 45): Aggregated watermark across all partitions
    - `is_healthy(self)` (line 58): Check if lag is within acceptable limits
    - `estimated_catch_up_time_ms(self)` (line 64): Estimate time to catch up based on processing rate
  - `WatermarkMonitor` (line 74): Monitor Kafka consumer lag and watermarks across all partitions
    - `__init__(self, kafka_config, redis_client, consumer_groups, topics, alert_threshold_ms)` (line 86): Initialize watermark monitor
    - `async start_monitoring(self, interval_seconds)` (line 124): Start monitoring watermarks
    - `async stop_monitoring(self)` (line 141): Stop monitoring watermarks
    - `async _monitor_loop(self, interval_seconds)` (line 158): Main monitoring loop
    - `async update_watermarks(self, consumer_group)` (line 185): Update watermarks for a consumer group
    - `calculate_global_watermark(self)` (line 238): Calculate global watermark across all partitions
    - `async store_metrics(self)` (line 276): Store metrics in Redis for historical tracking
    - `async check_alerts(self)` (line 320): Check for lag alerts and trigger notifications
    - `async export_prometheus_metrics(self)` (line 346): Export metrics in Prometheus format
    - `async get_current_lag(self)` (line 372): Get current lag information
    - `async get_partition_details(self, topic)` (line 396): Get detailed lag information for a specific topic

### `backend/shared/services/core/websocket_service.py`
- **Functions**
  - `utc_now()` (line 20): no docstring
  - `get_connection_manager()` (line 622): WebSocket 연결 관리자 싱글톤 인스턴스 반환
  - `get_notification_service(redis_service)` (line 630): WebSocket 알림 서비스 싱글톤 인스턴스 반환
- **Classes**
  - `WebSocketConnection` (line 25): WebSocket 연결 정보
  - `WebSocketConnectionManager` (line 36): WebSocket 연결 관리자
    - `__init__(self)` (line 45): no docstring
    - `async connect(self, websocket, client_id, user_id)` (line 56): 새로운 WebSocket 연결 수락
    - `async disconnect(self, client_id)` (line 81): WebSocket 연결 해제
    - `async subscribe_command(self, client_id, command_id)` (line 107): 특정 Command에 대한 업데이트 구독
    - `async unsubscribe_command(self, client_id, command_id)` (line 122): Command 구독 해제
    - `async send_to_client(self, client_id, message)` (line 138): 특정 클라이언트에게 메시지 전송
    - `async broadcast_command_update(self, command_id, update_data)` (line 156): Command 업데이트를 구독 중인 클라이언트들에게 브로드캐스트
    - `async send_to_user(self, user_id, message)` (line 189): 특정 사용자의 모든 연결에 메시지 전송
    - `async broadcast_to_all(self, message)` (line 206): Broadcast a message to all connected clients.
    - `async ping_all_clients(self)` (line 226): 모든 클라이언트에 ping 전송 (연결 상태 확인)
    - `get_connection_stats(self)` (line 249): 연결 통계 반환
    - `_build_schema_subject_key(self, db_name, subject_type, subject_id)` (line 266): Build schema subscription key.
    - `async subscribe_schema_changes(self, client_id, db_name, subject_type, subject_id, severity_filter)` (line 277): Subscribe to schema change notifications.
    - `async unsubscribe_schema_changes(self, client_id, subject_key)` (line 313): Unsubscribe from schema change notifications.
    - `async broadcast_schema_drift(self, db_name, drift_payload)` (line 333): Broadcast schema drift notification to subscribed clients.
    - `async get_schema_subscription_info(self, client_id)` (line 408): Get schema subscription info for a client.
  - `WebSocketNotificationService` (line 420): WebSocket 알림 서비스 with proper task tracking
    - `__init__(self, redis_service, connection_manager, task_manager)` (line 428): no docstring
    - `async start(self)` (line 441): 알림 서비스 시작 with proper task tracking
    - `async stop(self)` (line 463): 알림 서비스 중지 with proper cleanup
    - `_handle_pubsub_task_done(self, task)` (line 482): Handle completion of pubsub task.
    - `async _restart_pubsub_listener(self)` (line 495): Restart the pubsub listener after a failure.
    - `async _listen_redis_updates(self)` (line 502): Redis Pub/Sub 채널을 수신하여 WebSocket으로 전달 with improved error handling
    - `async notify_task_update(self, update_data)` (line 563): Send task update notification to all connected clients.
    - `async publish_schema_drift(self, db_name, drift_payload)` (line 575): Publish schema drift notification via Redis Pub/Sub.
    - `async notify_schema_drift_direct(self, db_name, drift_payload)` (line 597): Send schema drift notification directly to WebSocket clients.

### `backend/shared/services/core/writeback_merge_service.py`
- **Functions**
  - `_parse_queue_entry_seq(key)` (line 20): no docstring
  - `_coerce_object_type(resource_rid, fallback)` (line 32): no docstring
  - `_apply_changes_to_payload(payload, changes)` (line 41): no docstring
- **Classes**
  - `WritebackMergedInstance` (line 97): no docstring
  - `WritebackMergeService` (line 107): Authoritative server-side merge path for Action writeback.
    - `__init__(self, base_storage, lakefs_storage)` (line 114): no docstring
    - `async merge_instance(self, db_name, base_branch, overlay_branch, class_id, instance_id, writeback_repo, writeback_branch)` (line 123): no docstring

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
  - `async flush_dataset_ingest_outbox(dataset_registry, lineage_store, batch_size)` (line 241): no docstring
  - `async run_dataset_ingest_outbox_worker(dataset_registry, lineage_store, poll_interval_seconds, batch_size, stop_event)` (line 261): no docstring
  - `build_dataset_event_payload(event_id, event_type, aggregate_type, aggregate_id, command_type, actor, data)` (line 290): no docstring
- **Classes**
  - `DatasetIngestOutboxPublisher` (line 28): no docstring
    - `__init__(self, dataset_registry, lineage_store, batch_size)` (line 29): no docstring
    - `async close(self)` (line 86): no docstring
    - `_next_attempt_at(self, attempts)` (line 93): no docstring
    - `async _send_to_dlq(self, item, error, attempts)` (line 97): no docstring
    - `async _handle_failure(self, item, error)` (line 138): no docstring
    - `async _publish_item(self, item)` (line 157): no docstring
    - `async flush_once(self)` (line 207): no docstring
    - `async maybe_purge(self)` (line 223): no docstring

### `backend/shared/services/events/dataset_ingest_reconciler.py`
- **Functions**
  - `async run_dataset_ingest_reconciler(dataset_registry, poll_interval_seconds, stale_after_seconds, stop_event)` (line 12): no docstring

### `backend/shared/services/events/dlq_handler_fixed.py`
- **Classes**
  - `RetryStrategy` (line 27): Retry strategies for failed messages
  - `RetryPolicy` (line 36): Configuration for retry behavior
  - `FailedMessage` (line 47): Representation of a failed message
    - `is_poison(self)` (line 63): Check if message should be considered poison
    - `age_hours(self)` (line 75): Get age of the message in hours
    - `calculate_next_retry_time(self, policy)` (line 80): Calculate when this message should be retried
  - `DLQHandlerFixed` (line 100): FIXED: Handles Dead Letter Queue processing with intelligent retry
    - `__init__(self, dlq_topic, kafka_config, redis_client, retry_policy, poison_topic, consumer_group)` (line 112): Initialize DLQ handler
    - `register_processor(self, topic, processor)` (line 155): Register a message processor for a specific topic
    - `async start_processing(self)` (line 160): Start processing DLQ messages
    - `async stop_processing(self)` (line 189): Stop processing DLQ messages
    - `_poll_message(self, timeout)` (line 208): Poll for message in thread (blocking operation)
    - `async _process_loop(self)` (line 212): Main DLQ processing loop - FIXED to not block event loop
    - `async _process_dlq_message(self, msg)` (line 241): Process a message from the DLQ
    - `async _retry_scheduler(self)` (line 289): Background task to retry messages when their time comes
    - `async _retry_message(self, failed_msg)` (line 316): Retry a failed message
    - `async _add_to_retry_queue(self, failed_msg)` (line 360): Add message to retry queue
    - `async _move_to_poison_queue(self, failed_msg)` (line 386): Move message to poison queue
    - `async _record_recovery(self, failed_msg)` (line 422): Record successful recovery metrics
    - `_generate_message_id(self, value)` (line 438): Generate unique ID for a message
    - `async get_metrics(self)` (line 442): Get current metrics

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
  - `IdempotencyService` (line 20): Provides idempotency guarantees for event processing.
    - `__init__(self, redis_client, ttl_seconds, namespace)` (line 28): Initialize idempotency service.
    - `_generate_key(self, event_id, aggregate_id)` (line 46): Generate Redis key for idempotency check.
    - `_generate_event_hash(self, event_data)` (line 61): Generate deterministic hash of event data.
    - `async is_duplicate(self, event_id, event_data, aggregate_id)` (line 75): Check if event is duplicate and acquire processing lock.
    - `async mark_processed(self, event_id, result, aggregate_id)` (line 134): Mark event as successfully processed with optional result.
    - `async mark_failed(self, event_id, error, aggregate_id, retry_after)` (line 176): Mark event as failed with error details.
    - `async get_processing_status(self, event_id, aggregate_id)` (line 225): Get current processing status of an event.
    - `async cleanup_expired(self, pattern)` (line 247): Clean up expired idempotency keys (Redis handles this automatically).
  - `IdempotentEventProcessor` (line 273): Wrapper for idempotent event processing.
    - `__init__(self, idempotency_service)` (line 281): Initialize idempotent processor.
    - `async process_event(self, event_id, event_data, processor_func, aggregate_id)` (line 290): Process event with idempotency guarantee.

### `backend/shared/services/events/objectify_job_queue.py`
- **Classes**
  - `ObjectifyJobQueue` (line 16): no docstring
    - `__init__(self, objectify_registry)` (line 17): no docstring
    - `async _get_registry(self)` (line 21): no docstring
    - `async close(self)` (line 28): no docstring
    - `async publish(self, job, require_delivery)` (line 34): no docstring

### `backend/shared/services/events/objectify_outbox.py`
- **Functions**
  - `async run_objectify_outbox_worker(objectify_registry, poll_interval_seconds, batch_size, stop_event)` (line 249): no docstring
- **Classes**
  - `ObjectifyOutboxPublisher` (line 26): no docstring
    - `__init__(self, objectify_registry, topic, batch_size)` (line 27): no docstring
    - `async close(self)` (line 80): no docstring
    - `_next_attempt_at(self, attempts)` (line 86): no docstring
    - `async _publish_batch(self, batch)` (line 90): Publish a batch of outbox items with atomic delivery tracking.
    - `async flush_once(self)` (line 221): no docstring
    - `async maybe_purge(self)` (line 231): no docstring

### `backend/shared/services/events/objectify_reconciler.py`
- **Functions**
  - `async _build_objectify_payload(job, dataset_registry, objectify_registry, pipeline_registry)` (line 18): no docstring
  - `async reconcile_objectify_jobs(objectify_registry, dataset_registry, pipeline_registry, stale_after_seconds, enqueued_stale_seconds, limit, use_lock, lock_key)` (line 95): no docstring
  - `async run_objectify_reconciler(objectify_registry, dataset_registry, pipeline_registry, poll_interval_seconds, stale_after_seconds, enqueued_stale_seconds, stop_event)` (line 197): no docstring

### `backend/shared/services/kafka/__init__.py`

### `backend/shared/services/kafka/processed_event_worker.py`
- **Classes**
  - `RegistryKey` (line 45): no docstring
  - `HeartbeatOptions` (line 52): no docstring
  - `ProcessedEventKafkaWorker` (line 59): Template Method for Kafka workers guarded by ProcessedEventRegistry.
    - `_parse_payload(self, payload)` (line 82): Parse msg.value() into a domain payload (may raise).
    - `_registry_key(self, payload)` (line 86): Extract the ProcessedEventRegistry key for this payload.
    - `async _process_payload(self, payload)` (line 90): Perform the worker side-effect for a claimed payload.
    - `async _send_to_dlq(self, msg, payload, raw_payload, error, attempt_count)` (line 94): Publish a DLQ record for a terminal failure.
    - `_service_name(self)` (line 106): no docstring
    - `_fallback_metadata(self, payload)` (line 109): no docstring
    - `_span_name(self, payload)` (line 112): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 116): no docstring
    - `_metric_event_name(self, payload)` (line 127): no docstring
    - `_registry_handler(self, msg, payload)` (line 130): no docstring
    - `_is_retryable_error(self, exc, payload)` (line 133): no docstring
    - `_in_progress_sleep_seconds(self, claim, payload)` (line 137): no docstring
    - `_should_seek_on_in_progress(self, claim, payload)` (line 140): no docstring
    - `_should_seek_on_retry(self, attempt_count, payload)` (line 143): no docstring
    - `_should_mark_done_after_dlq(self, payload, error)` (line 146): no docstring
    - `_cancel_inflight_on_revoke(self)` (line 149): Whether to cancel in-flight partition tasks on Kafka rebalance revoke.
    - `_backoff_seconds(self, attempt_count, payload)` (line 161): no docstring
    - `_max_retries_for_error(self, exc, payload, error, retryable)` (line 164): no docstring
    - `_backoff_seconds_for_error(self, exc, payload, error, attempt_count, retryable)` (line 167): no docstring
    - `async _on_parse_error(self, msg, raw_payload, error)` (line 178): no docstring
    - `async _on_success(self, payload, result, duration_s)` (line 181): no docstring
    - `async _on_retry_scheduled(self, payload, error, attempt_count, backoff_s, retryable)` (line 191): no docstring
    - `async _on_terminal_failure(self, payload, error, attempt_count, retryable)` (line 202): no docstring
    - `async _mark_retryable_failure(self, payload, registry_key, handler, error)` (line 212): no docstring
    - `async _commit(self, msg)` (line 226): no docstring
    - `async _seek(self, topic, partition, offset)` (line 241): no docstring
    - `_heartbeat_options(self)` (line 256): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 259): no docstring
    - `async _poll_message(self, timeout)` (line 272): no docstring
    - `_is_partition_eof(self, msg)` (line 280): no docstring
    - `_loop_label(self)` (line 292): no docstring
    - `async _on_poll_exception(self, exc)` (line 295): no docstring
    - `async _on_kafka_message_error(self, msg)` (line 298): no docstring
    - `async _on_unexpected_message_error(self, exc, msg)` (line 304): no docstring
    - `async _seek_on_unexpected_error(self, msg)` (line 307): no docstring
    - `_init_partition_state(self, reset)` (line 315): no docstring
    - `_buffer_messages(self)` (line 327): no docstring
    - `_pending_log_thresholds(self)` (line 330): no docstring
    - `_uses_commit_state(self)` (line 333): no docstring
    - `_busy_partition_sleep_seconds(self)` (line 336): no docstring
    - `_partition_task_name(self, msg)` (line 339): no docstring
    - `_partition_key(self, msg)` (line 343): no docstring
    - `_log_buffered_message(self, msg, pending_count)` (line 346): no docstring
    - `_pause_partition(self, topic, partition)` (line 356): no docstring
    - `_resume_partition(self, topic, partition)` (line 370): no docstring
    - `_log_background_task_exception(self, task)` (line 384): no docstring
    - `_handle_partitions_revoked(self, partitions, clear_pending)` (line 395): no docstring
    - `_handle_partitions_assigned(self, partitions, resume)` (line 405): no docstring
    - `async _handle_busy_partition_message(self, msg)` (line 414): no docstring
    - `_start_partition_task(self, msg)` (line 431): no docstring
    - `async _handle_partition_message(self, msg)` (line 446): no docstring
    - `async _cancel_inflight_tasks(self)` (line 482): no docstring
    - `async run_loop(self, poll_timeout, idle_sleep, missing_consumer_sleep, poll_exception_sleep, unexpected_error_sleep, post_seek_sleep, seek_on_error, catch_exceptions)` (line 489): no docstring
    - `async run_partitioned_loop(self, poll_timeout, idle_sleep, missing_consumer_sleep, poll_exception_sleep)` (line 543): no docstring
    - `async handle_message(self, msg)` (line 601): Handle a single Kafka message (msg.value()).
    - `async _handle_claimed(self, msg, payload, registry_key, topic, partition, offset, raw_text, start)` (line 691): no docstring
  - `EventEnvelopeKafkaWorker` (line 838): Specialization of ProcessedEventKafkaWorker for EventEnvelope payloads.
    - `_parse_payload(self, payload)` (line 848): no docstring
    - `_registry_key(self, payload)` (line 862): no docstring
    - `_fallback_metadata(self, payload)` (line 869): no docstring
    - `_span_attributes(self, msg, payload, registry_key)` (line 872): no docstring
    - `_metric_event_name(self, payload)` (line 884): no docstring

### `backend/shared/services/kafka/safe_consumer.py`
- **Functions**
  - `create_safe_consumer(group_id, topics, service_name, **kwargs)` (line 500): Factory function to create a SafeKafkaConsumer.
  - `validate_consumer_config(config)` (line 529): Validate that a consumer config meets safety requirements.
- **Classes**
  - `ConsumerState` (line 29): Consumer lifecycle states.
  - `PartitionState` (line 39): Track state for each assigned partition.
  - `RebalanceHandler` (line 50): Handle consumer group rebalancing events.
    - `__init__(self, consumer, on_revoke_callback, on_assign_callback)` (line 57): no docstring
    - `on_revoke(self, consumer, partitions)` (line 68): Called before partitions are revoked.
    - `on_assign(self, consumer, partitions)` (line 120): Called after partitions are assigned.
  - `SafeKafkaConsumer` (line 152): Production-hardened Kafka consumer with strong consistency guarantees.
    - `__init__(self, group_id, topics, service_name, extra_config, on_revoke, on_assign, session_timeout_ms, max_poll_interval_ms, heartbeat_interval_ms)` (line 185): Create a safe Kafka consumer.
    - `state(self)` (line 273): Current consumer state.
    - `is_rebalancing(self)` (line 278): Check if consumer is currently rebalancing.
    - `poll(self, timeout)` (line 282): Poll for a message with rebalance awareness.
    - `wait_for_assignment(self, timeout_seconds)` (line 314): Block until partitions are assigned (or timeout).
    - `mark_processed(self, msg)` (line 338): Mark a message as successfully processed.
    - `commit(self, message, offsets, asynchronous, msg)` (line 355): Commit offsets.
    - `commit_sync(self, msg)` (line 424): Synchronously commit a specific message offset.
    - `seek(self, partition)` (line 428): Seek to a specific offset for a partition.
    - `close(self, timeout)` (line 446): Gracefully close the consumer.
    - `__enter__(self)` (line 468): no docstring
    - `__exit__(self, exc_type, exc_val, exc_tb)` (line 471): no docstring
    - `list_topics(self, topic, timeout)` (line 475): List available topics.
    - `assignment(self)` (line 479): Get current partition assignment.
    - `position(self, partitions)` (line 483): Get current position for partitions.
    - `pause(self, partitions)` (line 487): Pause fetching from the provided partitions (backpressure).
    - `resume(self, partitions)` (line 493): Resume fetching from the provided partitions (backpressure).

### `backend/shared/services/pipeline/__init__.py`

### `backend/shared/services/pipeline/fk_pattern_detector.py`
- **Classes**
  - `ForeignKeyPattern` (line 21): Detected foreign key pattern
  - `TargetCandidate` (line 34): Potential FK target (dataset or object_type)
  - `FKDetectionConfig` (line 45): Configuration for FK pattern detection - no hardcoding
    - `from_dict(cls, config_dict)` (line 85): Create config from dictionary (for settings injection)
  - `ForeignKeyPatternDetector` (line 97): Detects foreign key patterns in dataset schemas using configurable rules.
    - `__init__(self, config)` (line 108): no docstring
    - `_compile_patterns(self)` (line 114): Pre-compile regex patterns for performance
    - `detect_patterns(self, source_dataset_id, source_schema, source_sample, target_candidates)` (line 125): Detect potential FK relationships in a dataset.
    - `_is_excluded(self, column_name)` (line 193): Check if column should be excluded from FK detection
    - `_is_fk_compatible_type(self, column_type)` (line 197): Check if column type is compatible with FK references
    - `_detect_by_naming(self, source_dataset_id, column_name, target_candidates)` (line 205): Detect FK by naming convention patterns
    - `_extract_target_name(self, column_name, pattern)` (line 257): Extract potential target name from FK column name
    - `_find_matching_target(self, potential_name, candidates)` (line 265): Find a target candidate that matches the potential name
    - `_detect_by_value_overlap(self, source_dataset_id, column_name, source_sample, target_candidates)` (line 292): Detect FK by value overlap with target primary keys
    - `_extract_column_values(self, sample, column_name)` (line 345): Extract column values from sample data
    - `_calculate_overlap_confidence(self, source_set, target_set)` (line 358): Calculate FK confidence based on value overlap.
    - `suggest_link_type(self, fk_pattern, source_object_type)` (line 391): Generate a link_type suggestion from a detected FK pattern.
    - `_generate_predicate(self, column_name)` (line 427): Generate a predicate name from FK column name

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

### `backend/shared/services/pipeline/pipeline_artifact_store.py`
- **Classes**
  - `PipelineArtifactStore` (line 17): no docstring
    - `__init__(self, storage_service, bucket)` (line 18): no docstring
    - `async save_table(self, dataset_name, columns, rows, db_name, pipeline_id)` (line 22): no docstring

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
  - `_clone_definition_graph(definition_json)` (line 41): no docstring
  - `_output_name_for_node(node_id, node)` (line 55): no docstring
  - `_is_canonical_output(node_id, node, canonical_output_names)` (line 60): no docstring
  - `_sql_ident(name)` (line 73): no docstring
  - `_concat_expr(columns, null_if_any)` (line 77): no docstring
  - `_normalize_fk_columns(value)` (line 86): no docstring
  - `_merge_schema_checks(existing, additions)` (line 92): no docstring
  - `_sys_compute_expressions(output_name)` (line 117): no docstring
  - `_refresh_sys_compute_nodes(node_by_id, output_id, output_name)` (line 131): no docstring
  - `_inject_sys_columns_chain(edges, nodes, node_by_id, existing_ids, source_id, output_id, output_name)` (line 155): no docstring
  - `_inject_fk_join_check(edges, nodes, node_by_id, existing_ids, left_node_id, output_id, fk_index, fk_spec, default_branch)` (line 188): no docstring
  - `augment_definition_with_canonical_contract(definition_json, branch, canonical_output_names)` (line 406): no docstring
  - `_extract_schema_casts(schema_json)` (line 502): no docstring
  - `_is_cast_transform(node)` (line 526): no docstring
  - `async augment_definition_with_casts(definition_json, db_name, branch, dataset_registry)` (line 538): no docstring

### `backend/shared/services/pipeline/pipeline_definition_utils.py`
- **Functions**
  - `split_expectation_columns(column)` (line 8): no docstring
  - `resolve_execution_semantics(definition, pipeline_type)` (line 12): no docstring
  - `resolve_incremental_config(definition)` (line 49): no docstring
  - `is_truthy(value)` (line 54): no docstring
  - `normalize_pk_semantics(value)` (line 64): no docstring
  - `resolve_pk_semantics(execution_semantics, definition, output_metadata)` (line 81): no docstring
  - `resolve_delete_column(definition, output_metadata)` (line 115): no docstring
  - `coerce_pk_columns(value)` (line 129): no docstring
  - `collect_pk_columns(*candidates)` (line 170): no docstring
  - `match_output_declaration(output, node_id, output_name)` (line 181): no docstring
  - `resolve_pk_columns(definition, output_metadata, output_name, output_node_id, declared_outputs)` (line 204): no docstring
  - `build_expectations_with_pk(definition, output_metadata, output_name, output_node_id, declared_outputs, pk_semantics, delete_column, pk_columns, available_columns)` (line 248): no docstring
  - `validate_pk_semantics(available_columns, pk_semantics, pk_columns, delete_column)` (line 302): no docstring

### `backend/shared/services/pipeline/pipeline_dependency_utils.py`
- **Functions**
  - `normalize_dependency_entries(raw, strict)` (line 6): no docstring

### `backend/shared/services/pipeline/pipeline_executor.py`
- **Functions**
  - `_extract_schema_columns(schema)` (line 759): no docstring
  - `_extract_schema_types(schema)` (line 777): no docstring
  - `_extract_sample_rows(sample)` (line 800): no docstring
  - `_fallback_columns(node)` (line 819): no docstring
  - `_build_sample_rows(columns, count)` (line 829): no docstring
  - `_group_by_table(table, group_by, aggregates)` (line 839): no docstring
  - `_pivot_table(table, pivot_meta)` (line 912): no docstring
  - `_window_table(table, window_meta)` (line 955): no docstring
  - `_select_columns(table, columns)` (line 1013): no docstring
  - `_drop_columns(table, columns)` (line 1019): no docstring
  - `_rename_columns(table, rename_map)` (line 1026): no docstring
  - `_record_cast_stat(cast_stats, column, attempted, failed)` (line 1038): no docstring
  - `_cast_value_with_status(value, target, cast_mode)` (line 1053): no docstring
  - `_cast_value(value, target, cast_mode)` (line 1105): no docstring
  - `_apply_schema_casts(rows, schema_types, cast_mode, cast_stats)` (line 1110): no docstring
  - `_cast_columns(table, casts, cast_mode, cast_stats)` (line 1132): no docstring
  - `_dedupe_table(table, columns)` (line 1158): no docstring
  - `_sort_table(table, columns)` (line 1171): no docstring
  - `_union_tables(left, right, union_mode)` (line 1214): no docstring
  - `_join_tables(left, right, join_type, left_key, right_key, join_key, left_keys, right_keys, allow_cross_join, max_output_rows)` (line 1252): no docstring
  - `_merge_rows(left, right, right_column_map)` (line 1394): no docstring
  - `_find_similar_columns(target, available, cutoff)` (line 1411): Enterprise Enhancement (2026-01):
  - `_filter_table(table, expression, parameters)` (line 1439): Filter table rows based on expression.
  - `_parse_filter(expression, parameters)` (line 1487): no docstring
  - `_compare(left, op, right)` (line 1503): no docstring
  - `_compute_assignment_table(table, target, expression, parameters)` (line 1527): no docstring
  - `_compute_table(table, expression, parameters)` (line 1549): no docstring
  - `_explode_table(table, column)` (line 1579): no docstring
  - `_parse_assignment(expression)` (line 1602): no docstring
  - `_safe_eval(expression, row, parameters)` (line 1609): no docstring
  - `_is_safe_ast(node)` (line 1645): no docstring
  - `_eval_ast(node, variables)` (line 1661): no docstring
  - `_parse_literal(raw)` (line 1744): no docstring
  - `_parse_timestamp_literal(raw)` (line 1762): no docstring
  - `_normalize_table(table, columns, trim, empty_to_null, whitespace_to_null, lowercase, uppercase)` (line 1781): no docstring
  - `_regex_flags(raw)` (line 1819): no docstring
  - `_normalize_regex_rules(metadata)` (line 1833): no docstring
  - `_regex_replace_table(table, rules)` (line 1872): no docstring
  - `_parse_csv_bytes(raw_bytes, max_rows)` (line 1900): Parse a CSV payload into row dicts.
  - `_parse_excel_bytes(raw_bytes, max_rows)` (line 1951): no docstring
  - `_parse_json_bytes(raw_bytes, max_rows)` (line 1963): no docstring
  - `_infer_column_types(table)` (line 1998): no docstring
  - `_build_table_ops(table)` (line 2007): no docstring
- **Classes**
  - `PipelineExpectationError` (line 62): no docstring
  - `PipelineTable` (line 67): no docstring
    - `limited_rows(self, limit)` (line 71): no docstring
  - `PipelineRunResult` (line 78): no docstring
  - `PipelineArtifactStore` (line 83): no docstring
    - `__init__(self, base_path)` (line 84): no docstring
    - `save_table(self, table, dataset_name)` (line 89): no docstring
  - `PipelineExecutor` (line 101): no docstring
    - `__init__(self, dataset_registry, pipeline_registry, artifact_store, storage_service)` (line 102): no docstring
    - `async preview(self, definition, db_name, node_id, limit, input_overrides)` (line 119): no docstring
    - `async deploy(self, definition, db_name, node_id, dataset_name, store_local, input_overrides)` (line 132): no docstring
    - `async run(self, definition, db_name, input_overrides)` (line 152): no docstring
    - `async _load_input(self, node, db_name, branch, sample_limit)` (line 288): no docstring
    - `async _load_rows_from_artifact(self, artifact_key, max_rows)` (line 336): no docstring
    - `async _load_fk_reference_rows(self, db_name, dataset_id, dataset_name, branch)` (line 400): no docstring
    - `async _evaluate_fk_expectations(self, expectations, output_table, db_name, branch)` (line 430): no docstring
    - `async _apply_transform(self, metadata, inputs, parameters)` (line 535): no docstring
    - `async _apply_udf_transform(self, table, metadata)` (line 679): no docstring
    - `_table_to_sample(self, table, limit)` (line 721): no docstring
    - `_summarize_cast_stats(self, columns)` (line 733): no docstring
    - `_select_table(self, result, node_id)` (line 749): no docstring

### `backend/shared/services/pipeline/pipeline_funnel_fallback.py`
- **Functions**
  - `_is_non_empty(value)` (line 44): no docstring
  - `_sample_non_empty(values, max_samples)` (line 52): no docstring
  - `_phone_ratio(values)` (line 64): no docstring
  - `_email_ratio(values)` (line 77): no docstring
  - `_bool_ratio(values)` (line 90): no docstring
  - `_int_ratio(values)` (line 103): no docstring
  - `_decimal_ratio(values)` (line 118): no docstring
  - `_date_datetime_ratios(values)` (line 133): no docstring
  - `infer_type_fallback(values, column_name, include_complex_types)` (line 171): Deterministic, dependency-free(ish) inference used when Funnel is unavailable.
  - `build_funnel_analysis_fallback(columns, rows, include_complex_types, error, stage)` (line 283): Build a Funnel-compatible analysis payload from sample rows without calling Funnel.

### `backend/shared/services/pipeline/pipeline_graph_utils.py`
- **Functions**
  - `unique_node_id(base, existing, start_index)` (line 6): Return a node id that is unique within `existing`.
  - `normalize_nodes(nodes_raw)` (line 28): no docstring
  - `normalize_edges(edges_raw)` (line 42): no docstring
  - `build_incoming(edges)` (line 57): no docstring
  - `topological_sort(nodes, edges, include_unordered)` (line 64): no docstring

### `backend/shared/services/pipeline/pipeline_job_queue.py`
- **Classes**
  - `PipelineJobQueue` (line 24): no docstring
    - `__init__(self)` (line 25): no docstring
    - `_producer_instance(self)` (line 30): no docstring
    - `async publish(self, job, require_delivery)` (line 48): no docstring

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

### `backend/shared/services/pipeline/pipeline_parameter_utils.py`
- **Functions**
  - `normalize_parameters(parameters_raw)` (line 6): no docstring
  - `apply_parameters(expression, parameters)` (line 22): no docstring

### `backend/shared/services/pipeline/pipeline_plan_builder.py`
- **Functions**
  - `_ensure_dict(value, name)` (line 29): no docstring
  - `_ensure_list(value, name)` (line 35): no docstring
  - `_ensure_str(value, name)` (line 43): no docstring
  - `_ensure_string_list(value, name)` (line 50): no docstring
  - `_looks_like_spark_expr(text)` (line 67): Detect obvious Spark SQL expressions passed where a *column name* is required.
  - `_is_sql_expression(key)` (line 108): Detect if a join key is a SQL expression rather than a simple column name.
  - `_definition(plan)` (line 142): no docstring
  - `update_settings(plan, set_fields, unset_fields, replace)` (line 155): Patch plan.definition_json.settings (merge by default, replace if requested).
  - `_node_ids(definition)` (line 180): no docstring
  - `_find_node(definition, node_id)` (line 190): no docstring
  - `_incoming_sources_in_order(definition, node_id)` (line 202): Return incoming edge sources for `node_id` in the order they appear in definition_json.edges.
  - `new_plan(goal, db_name, branch, dataset_ids)` (line 225): no docstring
  - `reset_plan(plan)` (line 250): Reset a plan's definition to empty while preserving goal + data_scope.
  - `add_input(plan, dataset_id, dataset_name, dataset_branch, read, node_id)` (line 278): no docstring
  - `add_external_input(plan, read, source_name, node_id)` (line 328): Add an input node that is NOT backed by a DatasetRegistry artifact.
  - `configure_input_read(plan, node_id, read, replace)` (line 369): Patch input node read configuration.
  - `add_transform(plan, operation, input_node_ids, metadata, node_id)` (line 403): no docstring
  - `add_join(plan, left_node_id, right_node_id, left_keys, right_keys, join_type, join_hints, broadcast_left, broadcast_right, node_id)` (line 450): no docstring
  - `add_filter(plan, input_node_id, expression, node_id)` (line 513): no docstring
  - `add_compute(plan, input_node_id, expression, node_id)` (line 525): no docstring
  - `add_compute_column(plan, input_node_id, target_column, formula, node_id)` (line 537): Add a compute transform that writes a single column.
  - `add_compute_assignments(plan, input_node_id, assignments, node_id)` (line 562): Add a compute transform that writes multiple columns (assignments).
  - `add_sort(plan, input_node_id, columns, node_id)` (line 591): Add a sort transform node.
  - `add_explode(plan, input_node_id, column, node_id)` (line 634): Add an explode transform node for an array/map-like column.
  - `add_union(plan, left_node_id, right_node_id, union_mode, node_id)` (line 653): Add a union transform node for two inputs.
  - `add_pivot(plan, input_node_id, index, columns, values, agg, node_id)` (line 685): Add a pivot transform node.
  - `add_cast(plan, input_node_id, casts, node_id)` (line 716): no docstring
  - `add_rename(plan, input_node_id, rename, node_id)` (line 745): no docstring
  - `add_select(plan, input_node_id, columns, node_id)` (line 771): no docstring
  - `add_select_expr(plan, input_node_id, expressions, node_id)` (line 795): Add a select transform using Spark SQL selectExpr-style expressions.
  - `add_drop(plan, input_node_id, columns, node_id)` (line 813): no docstring
  - `add_dedupe(plan, input_node_id, columns, node_id)` (line 831): no docstring
  - `add_group_by_expr(plan, input_node_id, group_by, aggregate_expressions, operation, node_id)` (line 849): Add a groupBy/aggregate node using Spark SQL aggregate expressions.
  - `add_window_expr(plan, input_node_id, expressions, node_id)` (line 898): Add a window transform that computes one or more Spark SQL window expressions.
  - `add_normalize(plan, input_node_id, columns, trim, empty_to_null, whitespace_to_null, lowercase, uppercase, node_id)` (line 927): no docstring
  - `add_regex_replace(plan, input_node_id, rules, node_id)` (line 957): no docstring
  - `add_output(plan, input_node_id, output_name, output_kind, node_id, output_metadata)` (line 993): no docstring
  - `validate_structure(plan)` (line 1045): Lightweight structural validation for plan.definition_json.
  - `add_edge(plan, from_node_id, to_node_id)` (line 1209): Add a graph edge (idempotent).
  - `delete_edge(plan, from_node_id, to_node_id)` (line 1239): Delete all matching edges from->to (no-op if not found).
  - `set_node_inputs(plan, node_id, input_node_ids)` (line 1267): Replace all incoming edges to node_id with input_node_ids (in order).
  - `update_node_metadata(plan, node_id, set_fields, unset_fields, replace)` (line 1305): Patch node.metadata (merge by default, replace if requested).
  - `delete_node(plan, node_id)` (line 1352): Delete a node and any incident edges; also removes outputs[] entry for output nodes.
  - `update_output(plan, output_name, set_fields, unset_fields, replace)` (line 1386): Patch an outputs[] entry by output_name; keeps output node metadata.outputName in sync if renamed.
- **Classes**
  - `PipelinePlanBuilderError` (line 18): no docstring
  - `PlanMutation` (line 23): no docstring

### `backend/shared/services/pipeline/pipeline_preflight_utils.py`
- **Functions**
  - `_normalize_column_list(raw)` (line 21): no docstring
  - `_extract_schema_columns(schema)` (line 38): no docstring
  - `_extract_sample_rows(sample)` (line 52): no docstring
  - `_infer_types_from_rows(rows, columns)` (line 107): no docstring
  - `_merge_types(base, extra)` (line 117): no docstring
  - `_schema_for_input(dataset, version)` (line 128): no docstring
  - `_apply_select(schema, columns)` (line 153): no docstring
  - `_apply_drop(schema, columns)` (line 159): no docstring
  - `_apply_rename(schema, rename_map)` (line 166): no docstring
  - `_apply_cast(schema, casts)` (line 178): no docstring
  - `_apply_compute(schema, metadata)` (line 190): Compute transforms can be represented in a few deterministic forms:
  - `_apply_group_by(schema, group_by, aggregates)` (line 252): no docstring
  - `_parse_sql_alias(expr)` (line 276): Best-effort alias extraction for Spark SQL expressions.
  - `_apply_group_by_expr(schema, group_by, expressions)` (line 296): Schema propagation for Spark `groupBy(...).agg(expr(...).alias(...))` via `aggregateExpressions`.
  - `_apply_window(schema, metadata)` (line 336): Window transforms can either be declarative metadata (partition/order + row_number)
  - `_apply_join(left, right, left_keys, right_keys)` (line 379): Apply the deterministic join output naming policy.
  - `_apply_select_expr(schema, expressions)` (line 436): Best-effort schema propagation for Spark `selectExpr`.
  - `_apply_union(left, right, mode)` (line 537): no docstring
  - `_schema_empty(schema)` (line 563): no docstring
  - `_column_type(schema, column)` (line 567): no docstring
  - `async compute_pipeline_preflight(definition, db_name, dataset_registry, branch)` (line 580): no docstring
  - `async compute_schema_by_node(definition, db_name, dataset_registry, branch)` (line 1073): no docstring
- **Classes**
  - `SchemaInfo` (line 15): no docstring

### `backend/shared/services/pipeline/pipeline_preview_inspector.py`
- **Functions**
  - `_iter_values(rows, column)` (line 42): no docstring
  - `_ratio(count, total)` (line 58): no docstring
  - `_is_bool(value)` (line 64): no docstring
  - `_is_int(value)` (line 72): no docstring
  - `_is_decimal(value)` (line 86): no docstring
  - `_is_datetime(value)` (line 100): no docstring
  - `_looks_like_id(name)` (line 112): no docstring
  - `_domain_hint(name)` (line 125): no docstring
  - `_should_preserve_whitespace(column_name)` (line 138): Enterprise Enhancement (2026-01):
  - `_is_event_or_log_table(table_name, column_names)` (line 150): Enterprise Enhancement (2026-01):
  - `_row_key(row, columns)` (line 185): no docstring
  - `_case_variation_ratio(values)` (line 189): no docstring
  - `_parseability(values)` (line 201): no docstring
  - `_column_type_map(columns)` (line 218): no docstring
  - `inspect_preview(preview, table_name)` (line 233): Analyze preview data and suggest cleansing operations.

### `backend/shared/services/pipeline/pipeline_preview_policy.py`
- **Functions**
  - `_level_from_issues(issues)` (line 44): no docstring
  - `_count_comparison_ops(expression)` (line 66): no docstring
  - `_is_simple_filter_expression(expression)` (line 70): no docstring
  - `_is_safe_python_ast(tree)` (line 111): no docstring
  - `_is_timestamp_literal_arg(text)` (line 122): no docstring
  - `_is_preview_safe_compute_expression(expression)` (line 133): no docstring
  - `_select_expr_item_requires_spark(expr)` (line 156): no docstring
  - `evaluate_preview_policy(definition_json)` (line 179): Enterprise hard-gating for plan_preview.
- **Classes**
  - `PreviewPolicyIssue` (line 16): no docstring
    - `to_dict(self)` (line 23): no docstring

### `backend/shared/services/pipeline/pipeline_profiler.py`
- **Functions**
  - `_safe_stringify(value)` (line 15): no docstring
  - `_coerce_float(value)` (line 28): no docstring
  - `_compute_histogram(values, bins)` (line 46): no docstring
  - `_normalize_columns(columns)` (line 79): no docstring
  - `compute_column_stats(rows, columns, max_top_values)` (line 93): Returns a dict payload that is safe to JSON serialize:

### `backend/shared/services/pipeline/pipeline_scheduler.py`
- **Functions**
  - `_should_run_schedule(now, last_run, interval, cron)` (line 387): no docstring
  - `_cron_matches(now, expression)` (line 404): no docstring
  - `_cron_field_matches(field, value)` (line 419): no docstring
  - `_normalize_dependencies(raw)` (line 453): no docstring
  - `async _dependencies_satisfied(registry, dependencies)` (line 457): no docstring
  - `async _evaluate_dependencies(registry, dependencies)` (line 465): no docstring
  - `_is_valid_cron_expression(expression)` (line 500): no docstring
  - `_is_valid_cron_field(field)` (line 507): no docstring
- **Classes**
  - `ScheduledPipeline` (line 28): no docstring
  - `DependencyEvaluation` (line 37): no docstring
  - `PipelineScheduler` (line 43): no docstring
    - `__init__(self, registry, queue, poll_seconds, tracing, metrics)` (line 44): no docstring
    - `async run(self)` (line 60): no docstring
    - `async stop(self)` (line 90): no docstring
    - `async _tick(self)` (line 93): no docstring
    - `async _record_scheduler_config_error(self, pipeline_id, now, error_key, detail, extra)` (line 296): no docstring
    - `async _record_scheduler_ignored(self, pipeline_id, now, reason, detail, extra)` (line 340): no docstring

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
  - `clamp_task_spec(spec, dataset_count)` (line 29): Clamp a TaskSpec to safe defaults.
  - `normalize_task_spec(raw, dataset_count)` (line 53): no docstring
  - `_iter_transform_ops(definition_json)` (line 63): no docstring
  - `validate_plan_against_task_spec(plan, task_spec)` (line 82): Validate a plan against TaskSpec policy (overreach guardrails).

### `backend/shared/services/pipeline/pipeline_transform_spec.py`
- **Functions**
  - `normalize_operation(value)` (line 40): no docstring
  - `resolve_join_spec(metadata)` (line 44): no docstring
  - `normalize_union_mode(metadata)` (line 94): no docstring
- **Classes**
  - `JoinSpec` (line 31): no docstring

### `backend/shared/services/pipeline/pipeline_type_inference.py`
- **Functions**
  - `_is_bool(value)` (line 24): no docstring
  - `_is_datetime(value)` (line 32): no docstring
  - `_is_int(value)` (line 40): no docstring
  - `_is_decimal(value)` (line 50): no docstring
  - `_sample_non_null(values, max_samples)` (line 62): no docstring
  - `infer_xsd_type_with_confidence(values, max_samples)` (line 84): Best-effort inference with a confidence score based on parse rates.
  - `common_join_key_type(left, right)` (line 129): Pick a safe common type for join keys.
  - `normalize_declared_type(value)` (line 144): no docstring
- **Classes**
  - `TypeInferenceResult` (line 77): no docstring

### `backend/shared/services/pipeline/pipeline_type_utils.py`
- **Functions**
  - `_normalize_numeric_text(text)` (line 64): Normalize numeric text for parsing. Returns (cleaned_string, is_negative).
  - `parse_decimal_text_with_ambiguity(text)` (line 113): Enterprise Enhancement (2026-01):
  - `parse_int_text(text)` (line 223): no docstring
  - `parse_decimal_text(text)` (line 234): no docstring
  - `parse_datetime_text(text, allow_ambiguous)` (line 245): Parse datetime text, returning the primary interpretation.
  - `parse_datetime_text_with_ambiguity(text, allow_ambiguous)` (line 251): Enterprise Enhancement (2026-01):
  - `infer_xsd_type_from_values(values)` (line 369): no docstring
  - `normalize_cast_target(target)` (line 417): no docstring
  - `normalize_cast_mode(value)` (line 421): no docstring
  - `xsd_to_spark_type(value)` (line 428): no docstring
  - `spark_type_to_xsd(data_type)` (line 433): no docstring
- **Classes**
  - `NumericParseResult` (line 25): Enterprise Enhancement (2026-01):
  - `DateParseResult` (line 45): Enterprise Enhancement (2026-01):

### `backend/shared/services/pipeline/pipeline_udf_runtime.py`
- **Functions**
  - `_validate_udf_ast(tree)` (line 85): no docstring
  - `compile_row_udf(code)` (line 108): Compile a Python UDF for row-level transforms.
- **Classes**
  - `PipelineUdfError` (line 7): no docstring
  - `_UdfAstValidator` (line 70): no docstring
    - `generic_visit(self, node)` (line 71): no docstring
    - `visit_Attribute(self, node)` (line 77): no docstring

### `backend/shared/services/pipeline/pipeline_unit_test_runner.py`
- **Functions**
  - `_stable_row_key(row)` (line 19): no docstring
  - `_normalize_columns(spec)` (line 23): no docstring
  - `_normalize_rows(spec, columns)` (line 44): no docstring
  - `_table_from_spec(spec)` (line 59): no docstring
  - `_diff_tables(actual, expected, max_rows)` (line 65): no docstring
  - `_select_table(result, node_id)` (line 109): no docstring
  - `async run_unit_tests(executor, definition, db_name, unit_tests)` (line 124): no docstring
- **Classes**
  - `PipelineUnitTestResult` (line 12): no docstring

### `backend/shared/services/pipeline/pipeline_validation_utils.py`
- **Functions**
  - `_format_error(prefix, message)` (line 31): no docstring
  - `validate_schema_checks(ops, checks, error_prefix)` (line 35): no docstring
  - `validate_expectations(ops, expectations)` (line 91): no docstring
  - `validate_schema_contract(ops, contract)` (line 158): no docstring
- **Classes**
  - `TableOps` (line 19): no docstring

### `backend/shared/services/registries/__init__.py`

### `backend/shared/services/registries/action_log_registry.py`
- **Functions**
  - `_coerce_dt(value)` (line 56): no docstring
  - `_jsonb_to_dict(value)` (line 64): no docstring
  - `_jsonb_to_optional_dict(value)` (line 81): no docstring
  - `_row_to_record(row)` (line 98): no docstring
- **Classes**
  - `ActionLogStatus` (line 25): no docstring
  - `ActionLogRecord` (line 34): no docstring
  - `ActionLogRegistry` (line 124): Postgres-backed Action log registry.
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 133): no docstring
    - `_jsonb_param(value)` (line 150): asyncpg expects JSON/JSONB bind params as strings by default.
    - `async connect(self)` (line 166): no docstring
    - `async initialize(self)` (line 177): no docstring
    - `async close(self)` (line 180): no docstring
    - `async shutdown(self)` (line 185): no docstring
    - `async ensure_schema(self)` (line 188): no docstring
    - `async create_log(self, action_log_id, db_name, action_type_id, action_type_rid, resource_rid, ontology_commit_id, input_payload, correlation_id, submitted_by, writeback_target, metadata)` (line 258): no docstring
    - `async get_log(self, action_log_id)` (line 314): no docstring
    - `async list_logs(self, db_name, statuses, action_type_id, submitted_by, limit, offset)` (line 324): no docstring
    - `async list_outbox_candidates(self, limit, statuses)` (line 372): no docstring
    - `async mark_commit_written(self, action_log_id, writeback_commit_id, result)` (line 396): no docstring
    - `async mark_event_emitted(self, action_log_id, action_applied_event_id, action_applied_seq)` (line 423): no docstring
    - `async mark_succeeded(self, action_log_id, result, finished_at)` (line 450): no docstring
    - `async mark_failed(self, action_log_id, result, finished_at)` (line 480): no docstring

### `backend/shared/services/registries/action_simulation_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 22): no docstring
- **Classes**
  - `ActionSimulationRecord` (line 54): no docstring
  - `ActionSimulationVersionRecord` (line 67): no docstring
  - `ActionSimulationRegistry` (line 86): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 87): no docstring
    - `async connect(self)` (line 101): no docstring
    - `async initialize(self)` (line 112): no docstring
    - `async close(self)` (line 115): no docstring
    - `async shutdown(self)` (line 120): no docstring
    - `async ensure_schema(self)` (line 123): no docstring
    - `_row_to_simulation(self, row)` (line 187): no docstring
    - `_row_to_version(self, row)` (line 200): no docstring
    - `async create_simulation(self, simulation_id, db_name, action_type_id, created_by, created_by_type, title, description)` (line 220): no docstring
    - `async get_simulation(self, simulation_id)` (line 254): no docstring
    - `async list_simulations(self, db_name, action_type_id, limit, offset)` (line 271): no docstring
    - `async next_version(self, simulation_id)` (line 315): no docstring
    - `async create_version(self, simulation_id, version, status, base_branch, overlay_branch, ontology_commit_id, action_type_rid, preview_action_log_id, input_payload, assumptions, scenarios, result, error, created_by, created_by_type)` (line 326): no docstring
    - `async list_versions(self, simulation_id, limit, offset)` (line 394): no docstring
    - `async get_version(self, simulation_id, version)` (line 417): no docstring

### `backend/shared/services/registries/agent_function_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 26): no docstring
  - `_coerce_json_dict(value)` (line 43): no docstring
- **Classes**
  - `AgentFunctionRecord` (line 61): no docstring
  - `AgentFunctionRegistry` (line 75): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 76): no docstring
    - `async initialize(self)` (line 90): no docstring
    - `async connect(self)` (line 93): no docstring
    - `async close(self)` (line 104): no docstring
    - `async shutdown(self)` (line 109): no docstring
    - `async ensure_schema(self)` (line 112): no docstring
    - `_row_to_record(self, row)` (line 142): no docstring
    - `async upsert_function(self, function_id, version, status, handler, tags, roles, input_schema, output_schema, metadata)` (line 157): no docstring
    - `async get_function(self, function_id, version, status)` (line 217): no docstring
    - `async list_functions(self, function_id, status, limit, offset)` (line 248): no docstring

### `backend/shared/services/registries/agent_model_registry.py`
- **Functions**
  - `_coerce_json(value)` (line 22): no docstring
- **Classes**
  - `AgentModelRecord` (line 40): no docstring
  - `AgentModelRegistry` (line 56): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 57): no docstring
    - `async initialize(self)` (line 71): no docstring
    - `async connect(self)` (line 74): no docstring
    - `async close(self)` (line 85): no docstring
    - `async shutdown(self)` (line 90): no docstring
    - `async ensure_schema(self)` (line 93): no docstring
    - `_row_to_model(self, row)` (line 130): no docstring
    - `async upsert_model(self, model_id, provider, display_name, status, supports_json_mode, supports_native_tool_calling, max_context_tokens, max_output_tokens, prompt_per_1k, completion_per_1k, metadata)` (line 147): no docstring
    - `async get_model(self, model_id)` (line 218): no docstring
    - `async list_models(self, status, provider, limit, offset)` (line 239): no docstring

### `backend/shared/services/registries/agent_policy_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 23): no docstring
- **Classes**
  - `AgentTenantPolicyRecord` (line 42): no docstring
  - `AgentPolicyRegistry` (line 53): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 54): no docstring
    - `async initialize(self)` (line 68): no docstring
    - `async connect(self)` (line 71): no docstring
    - `async close(self)` (line 82): no docstring
    - `async shutdown(self)` (line 87): no docstring
    - `async ensure_schema(self)` (line 90): no docstring
    - `_row_to_policy(self, row)` (line 114): no docstring
    - `async upsert_tenant_policy(self, tenant_id, allowed_models, allowed_tools, default_model, auto_approve_rules, data_policies)` (line 126): no docstring
    - `async get_tenant_policy(self, tenant_id)` (line 174): no docstring
    - `async list_tenant_policies(self, limit, offset)` (line 190): no docstring

### `backend/shared/services/registries/agent_registry.py`
- **Classes**
  - `AgentRunRecord` (line 21): no docstring
  - `AgentStepRecord` (line 38): no docstring
  - `AgentApprovalRecord` (line 57): no docstring
  - `AgentApprovalRequestRecord` (line 71): no docstring
  - `AgentToolIdempotencyRecord` (line 92): no docstring
  - `AgentRegistry` (line 107): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 108): no docstring
    - `async initialize(self)` (line 122): no docstring
    - `async connect(self)` (line 125): no docstring
    - `async close(self)` (line 136): no docstring
    - `async shutdown(self)` (line 141): no docstring
    - `async ensure_schema(self)` (line 144): no docstring
    - `_row_to_run(self, row)` (line 311): no docstring
    - `_row_to_step(self, row)` (line 328): no docstring
    - `_row_to_approval(self, row)` (line 347): no docstring
    - `_row_to_approval_request(self, row)` (line 361): no docstring
    - `_row_to_tool_idempotency(self, row)` (line 382): no docstring
    - `async create_run(self, run_id, tenant_id, plan_id, status, risk_level, requester, delegated_actor, context, plan_snapshot, started_at)` (line 398): no docstring
    - `async update_run_status(self, run_id, tenant_id, status, finished_at)` (line 441): no docstring
    - `async get_run(self, run_id, tenant_id)` (line 470): no docstring
    - `async list_runs(self, tenant_id, plan_id, status, limit)` (line 487): no docstring
    - `async create_step(self, run_id, step_id, tenant_id, tool_id, status, command_id, task_id, input_digest, output_digest, error, started_at, metadata)` (line 522): no docstring
    - `async update_step_status(self, run_id, step_id, tenant_id, status, output_digest, error, finished_at)` (line 568): no docstring
    - `async list_steps(self, run_id, tenant_id)` (line 605): no docstring
    - `async create_approval(self, approval_id, plan_id, tenant_id, step_id, decision, approved_by, approved_at, comment, metadata)` (line 623): no docstring
    - `async list_approvals(self, plan_id, tenant_id)` (line 662): no docstring
    - `async create_approval_request(self, approval_request_id, plan_id, tenant_id, session_id, job_id, status, risk_level, requested_by, requested_at, request_payload, metadata)` (line 680): no docstring
    - `async get_approval_request(self, approval_request_id, tenant_id)` (line 733): no docstring
    - `async list_approval_requests(self, tenant_id, session_id, job_id, plan_id, status, limit, offset)` (line 754): no docstring
    - `async decide_approval_request(self, approval_request_id, tenant_id, decision, decided_by, decided_at, comment, status, metadata)` (line 801): no docstring
    - `async get_tool_idempotency(self, tenant_id, idempotency_key)` (line 850): no docstring
    - `async begin_tool_idempotency(self, tenant_id, idempotency_key, tool_id, request_digest, started_at)` (line 876): Create or fetch a tool idempotency record.
    - `async finalize_tool_idempotency(self, tenant_id, idempotency_key, tool_id, request_digest, response_status, response_body, error, finished_at)` (line 932): no docstring

### `backend/shared/services/registries/agent_session_registry.py`
- **Functions**
  - `_wrap_json_object(value)` (line 24): no docstring
  - `_get_encryptor()` (line 102): no docstring
  - `_aad_for_session(session_id)` (line 113): no docstring
  - `validate_session_status_transition(current_status, next_status)` (line 117): no docstring
- **Classes**
  - `AgentSessionRecord` (line 130): no docstring
  - `AgentSessionMessageRecord` (line 146): no docstring
  - `AgentSessionJobRecord` (line 164): no docstring
  - `AgentSessionContextItemRecord` (line 178): no docstring
  - `AgentSessionEventRecord` (line 191): no docstring
  - `AgentSessionCIResultRecord` (line 204): no docstring
  - `AgentSessionToolCallRecord` (line 221): no docstring
  - `AgentSessionLLMCallRecord` (line 253): no docstring
  - `AgentSessionLLMUsageAggregateRecord` (line 274): no docstring
  - `AgentSessionRegistry` (line 287): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 288): no docstring
    - `async initialize(self)` (line 302): no docstring
    - `async connect(self)` (line 305): no docstring
    - `async close(self)` (line 316): no docstring
    - `async shutdown(self)` (line 321): no docstring
    - `async ensure_schema(self)` (line 324): no docstring
    - `_row_to_session(self, row)` (line 595): no docstring
    - `_row_to_message(self, row)` (line 614): no docstring
    - `_row_to_job(self, row)` (line 642): no docstring
    - `_row_to_context_item(self, row)` (line 656): no docstring
    - `_row_to_event(self, row)` (line 669): no docstring
    - `_row_to_ci_result(self, row)` (line 682): no docstring
    - `_row_to_tool_call(self, row)` (line 701): no docstring
    - `_row_to_llm_call(self, row)` (line 751): no docstring
    - `async create_session(self, session_id, tenant_id, created_by, status, selected_model, enabled_tools, metadata, started_at)` (line 772): no docstring
    - `async get_session(self, session_id, tenant_id)` (line 812): no docstring
    - `async list_sessions(self, tenant_id, created_by, status, limit, offset)` (line 828): no docstring
    - `async update_session(self, session_id, tenant_id, status, selected_model, enabled_tools, summary, metadata, terminated_at)` (line 864): no docstring
    - `async add_message(self, message_id, session_id, tenant_id, role, content, content_digest, token_count, cost_estimate, latency_ms, metadata, created_at)` (line 919): no docstring
    - `async list_messages(self, session_id, tenant_id, limit, offset, include_removed)` (line 973): no docstring
    - `async list_recent_messages(self, session_id, tenant_id, limit, include_removed)` (line 1010): no docstring
    - `async get_messages_by_ids(self, session_id, tenant_id, message_ids, include_removed)` (line 1045): no docstring
    - `async mark_messages_removed(self, session_id, tenant_id, message_ids, removed_by, removed_reason, removed_at, placeholder)` (line 1085): no docstring
    - `async create_job(self, job_id, session_id, tenant_id, plan_id, run_id, status, error, metadata, created_at)` (line 1134): no docstring
    - `async update_job(self, job_id, tenant_id, status, run_id, error, metadata, finished_at)` (line 1202): no docstring
    - `async get_job(self, job_id, tenant_id)` (line 1244): no docstring
    - `async list_jobs(self, session_id, tenant_id, limit, offset)` (line 1263): no docstring
    - `async add_context_item(self, item_id, session_id, tenant_id, item_type, include_mode, ref, token_count, metadata, created_at)` (line 1293): no docstring
    - `async list_context_items(self, session_id, tenant_id, limit, offset)` (line 1339): no docstring
    - `async remove_context_item(self, session_id, tenant_id, item_id)` (line 1368): no docstring
    - `async append_event(self, event_id, session_id, tenant_id, event_type, data, occurred_at, trace_id, correlation_id, created_at)` (line 1400): no docstring
    - `async list_session_events(self, session_id, tenant_id, limit, after)` (line 1443): no docstring
    - `async start_tool_call(self, tool_run_id, session_id, tenant_id, tool_id, method, path, query, request_body, request_digest, request_token_count, idempotency_key, job_id, plan_id, run_id, step_id, started_at)` (line 1476): no docstring
    - `async finish_tool_call(self, tool_run_id, tenant_id, status, response_status, response_body, response_digest, response_token_count, error_code, error_message, side_effect_summary, latency_ms, finished_at)` (line 1564): no docstring
    - `async list_tool_calls(self, session_id, tenant_id, limit, offset)` (line 1640): no docstring
    - `async record_llm_call(self, llm_call_id, session_id, tenant_id, call_type, provider, model_id, cache_hit, latency_ms, prompt_tokens, completion_tokens, total_tokens, cost_estimate, input_digest, output_digest, job_id, plan_id, created_at)` (line 1674): no docstring
    - `async list_llm_calls(self, session_id, tenant_id, limit, offset)` (line 1740): no docstring
    - `async record_ci_result(self, ci_result_id, session_id, tenant_id, job_id, plan_id, run_id, provider, status, details_url, summary, checks, raw, created_at)` (line 1773): no docstring
    - `async list_ci_results(self, session_id, tenant_id, limit, offset)` (line 1835): no docstring
    - `async list_expired_file_uploads(self, cutoff, tenant_id, limit)` (line 1870): Return `file_upload` context items older than `cutoff` that still contain bucket/key refs.
    - `async aggregate_llm_usage(self, tenant_id, group_by, created_by, start_time, end_time, limit, offset)` (line 1935): Aggregate LLM usage/cost across sessions for a tenant (OBS-005).
    - `async apply_retention(self, cutoff, tenant_id, action, message_placeholder, removed_by, removed_reason, include_messages, include_tool_calls, include_context_items, include_ci_results, include_events, include_llm_calls, context_item_type, exclude_context_item_types)` (line 2043): Apply retention to agent session data (SEC-005).

### `backend/shared/services/registries/agent_tool_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 20): no docstring
  - `_coerce_json_dict(value)` (line 38): no docstring
- **Classes**
  - `AgentToolPolicyRecord` (line 58): no docstring
  - `AgentToolRegistry` (line 80): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 81): no docstring
    - `async initialize(self)` (line 95): no docstring
    - `async connect(self)` (line 98): no docstring
    - `async close(self)` (line 109): no docstring
    - `async shutdown(self)` (line 114): no docstring
    - `async ensure_schema(self)` (line 117): no docstring
    - `_row_to_policy(self, row)` (line 182): no docstring
    - `async upsert_tool_policy(self, tool_id, method, path, risk_level, requires_approval, requires_idempotency_key, status, roles, max_payload_bytes, version, tool_type, input_schema, output_schema, timeout_seconds, retry_policy, resource_scopes, metadata)` (line 205): no docstring
    - `async get_tool_policy(self, tool_id)` (line 286): no docstring
    - `async list_tool_policies(self, status, limit)` (line 302): no docstring

### `backend/shared/services/registries/connector_registry.py`
- **Functions**
  - `_parse_json_dict(value)` (line 31): no docstring
  - `_parse_json_list_of_dicts(value)` (line 48): no docstring
- **Classes**
  - `ConnectorSource` (line 68): no docstring
  - `ConnectorMapping` (line 78): no docstring
  - `SyncState` (line 93): no docstring
  - `OutboxItem` (line 110): no docstring
  - `ConnectorRegistry` (line 124): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 127): no docstring
    - `async initialize(self)` (line 143): no docstring
    - `async connect(self)` (line 146): no docstring
    - `async close(self)` (line 157): no docstring
    - `async ensure_schema(self)` (line 162): no docstring
    - `async upsert_source(self, source_type, source_id, config_json, enabled)` (line 264): no docstring
    - `async set_source_enabled(self, source_type, source_id, enabled)` (line 311): no docstring
    - `async get_source(self, source_type, source_id)` (line 329): no docstring
    - `async list_sources(self, source_type, enabled, limit)` (line 355): no docstring
    - `_deterministic_mapping_id(self, source_type, source_id)` (line 394): no docstring
    - `async upsert_mapping(self, source_type, source_id, enabled, status, target_db_name, target_branch, target_class_label, field_mappings)` (line 399): no docstring
    - `async get_mapping(self, source_type, source_id)` (line 471): no docstring
    - `async record_poll_result(self, source_type, source_id, current_cursor, kafka_topic)` (line 507): Record a poll result, and enqueue a connector update event when the cursor changed.
    - `async claim_outbox_batch(self, limit)` (line 632): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 684): no docstring
    - `async mark_outbox_failed(self, outbox_id, error)` (line 699): no docstring
    - `async record_sync_outcome(self, source_type, source_id, success, command_id, error, next_retry_at, rate_limit_until)` (line 717): no docstring
    - `async get_sync_state(self, source_type, source_id)` (line 787): no docstring

### `backend/shared/services/registries/dataset_profile_registry.py`
- **Classes**
  - `DatasetProfileRecord` (line 24): no docstring
  - `DatasetProfileRegistry` (line 38): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 39): no docstring
    - `async initialize(self)` (line 55): no docstring
    - `async connect(self)` (line 58): no docstring
    - `async close(self)` (line 69): no docstring
    - `async shutdown(self)` (line 74): no docstring
    - `async ensure_schema(self)` (line 77): no docstring
    - `_row_to_profile(self, row)` (line 119): no docstring
    - `async upsert_profile(self, dataset_id, dataset_version_id, db_name, branch, schema_hash, profile)` (line 134): no docstring
    - `async get_latest_profile(self, dataset_id, dataset_version_id)` (line 183): no docstring

### `backend/shared/services/registries/dataset_registry.py`
- **Functions**
  - `_inject_dataset_version(outbox_entries, dataset_version_id)` (line 270): Ensure dataset_version_id is propagated into outbox payloads that depend on it.
  - `_extract_schema_columns(schema)` (line 304): no docstring
  - `_compute_schema_hash_from_payload(payload)` (line 330): no docstring
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
  - `DatasetRegistry` (line 337): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 338): no docstring
    - `_row_to_backing(row)` (line 355): no docstring
    - `_row_to_backing_version(row)` (line 371): no docstring
    - `_row_to_key_spec(row)` (line 384): no docstring
    - `_row_to_gate_policy(row)` (line 396): no docstring
    - `_row_to_gate_result(row)` (line 409): no docstring
    - `_row_to_access_policy(row)` (line 422): no docstring
    - `_row_to_instance_edit(row)` (line 436): no docstring
    - `_row_to_relationship_spec(row)` (line 453): no docstring
    - `_row_to_relationship_index_result(row)` (line 486): no docstring
    - `_row_to_link_edit(row)` (line 504): no docstring
    - `_row_to_schema_migration_plan(row)` (line 522): no docstring
    - `async initialize(self)` (line 534): no docstring
    - `async connect(self)` (line 537): no docstring
    - `async close(self)` (line 548): no docstring
    - `async ensure_schema(self)` (line 553): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch, dataset_id)` (line 1245): no docstring
    - `async list_datasets(self, db_name, branch)` (line 1297): no docstring
    - `async count_datasets_by_db_names(self, db_names, branch)` (line 1352): no docstring
    - `async get_dataset(self, dataset_id)` (line 1384): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 1412): no docstring
    - `async get_dataset_by_source_ref(self, db_name, source_type, source_ref, branch)` (line 1448): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, version_id, ingest_request_id, promoted_from_artifact_id)` (line 1486): no docstring
    - `async get_latest_version(self, dataset_id)` (line 1580): no docstring
    - `async get_version(self, version_id)` (line 1611): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 1640): no docstring
    - `async create_backing_datasource(self, dataset_id, db_name, name, branch, description, source_type, source_ref, backing_id)` (line 1673): no docstring
    - `async get_backing_datasource(self, backing_id)` (line 1711): no docstring
    - `async get_backing_datasource_by_dataset(self, dataset_id, branch)` (line 1728): no docstring
    - `async list_backing_datasources(self, db_name, branch, limit)` (line 1754): no docstring
    - `async get_or_create_backing_datasource(self, dataset, source_type, source_ref)` (line 1780): no docstring
    - `async create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 1803): no docstring
    - `async get_backing_datasource_version(self, version_id)` (line 1849): no docstring
    - `async get_backing_datasource_version_by_dataset_version(self, dataset_version_id)` (line 1870): no docstring
    - `async list_backing_datasource_versions(self, backing_id, limit)` (line 1893): no docstring
    - `async get_or_create_backing_datasource_version(self, backing_id, dataset_version_id, schema_hash, metadata)` (line 1916): no docstring
    - `async create_key_spec(self, dataset_id, spec, dataset_version_id, status, key_spec_id)` (line 1936): no docstring
    - `async get_key_spec(self, key_spec_id)` (line 1967): no docstring
    - `async get_key_spec_for_dataset(self, dataset_id, dataset_version_id)` (line 1983): no docstring
    - `async list_key_specs(self, dataset_id, limit)` (line 2024): no docstring
    - `async upsert_gate_policy(self, scope, name, description, rules, status)` (line 2046): no docstring
    - `async get_gate_policy(self, scope, name)` (line 2082): no docstring
    - `async list_gate_policies(self, scope, limit)` (line 2104): no docstring
    - `async record_gate_result(self, scope, subject_type, subject_id, status, details, policy_name)` (line 2126): no docstring
    - `async list_gate_results(self, scope, subject_type, subject_id, limit)` (line 2168): no docstring
    - `async upsert_access_policy(self, db_name, scope, subject_type, subject_id, policy, status)` (line 2196): no docstring
    - `async get_access_policy(self, db_name, scope, subject_type, subject_id, status)` (line 2233): no docstring
    - `async list_access_policies(self, db_name, scope, subject_type, subject_id, status, limit)` (line 2262): no docstring
    - `async record_instance_edit(self, db_name, class_id, instance_id, edit_type, metadata, status, fields)` (line 2296): no docstring
    - `async count_instance_edits(self, db_name, class_id, status)` (line 2336): no docstring
    - `async list_instance_edits(self, db_name, class_id, instance_id, status, limit)` (line 2359): no docstring
    - `async clear_instance_edits(self, db_name, class_id)` (line 2390): no docstring
    - `async remap_instance_edits(self, db_name, class_id, id_map, status)` (line 2412): no docstring
    - `async get_instance_edit_field_stats(self, db_name, class_id, fields, status)` (line 2449): no docstring
    - `async apply_instance_edit_field_moves(self, db_name, class_id, field_moves, status)` (line 2523): no docstring
    - `async update_instance_edit_status_by_fields(self, db_name, class_id, fields, new_status, status, metadata_note)` (line 2586): no docstring
    - `async create_relationship_spec(self, link_type_id, db_name, source_object_type, target_object_type, predicate, spec_type, dataset_id, mapping_spec_id, mapping_spec_version, spec, dataset_version_id, status, auto_sync, relationship_spec_id)` (line 2642): no docstring
    - `async update_relationship_spec(self, relationship_spec_id, status, spec, auto_sync, dataset_id, dataset_version_id, mapping_spec_id, mapping_spec_version)` (line 2697): no docstring
    - `async record_relationship_index_result(self, relationship_spec_id, status, stats, errors, dataset_version_id, mapping_spec_version, lineage, indexed_at)` (line 2755): no docstring
    - `async get_relationship_spec(self, relationship_spec_id, link_type_id)` (line 2839): no docstring
    - `async list_relationship_specs(self, db_name, dataset_id, status, limit)` (line 2870): no docstring
    - `async list_relationship_specs_by_relationship_object_type(self, db_name, relationship_object_type, status, limit)` (line 2902): no docstring
    - `async list_relationship_index_results(self, relationship_spec_id, link_type_id, db_name, status, limit)` (line 2935): no docstring
    - `async record_link_edit(self, db_name, link_type_id, branch, source_object_type, target_object_type, predicate, source_instance_id, target_instance_id, edit_type, status, metadata, edit_id)` (line 2967): no docstring
    - `async list_link_edits(self, db_name, link_type_id, branch, status, source_instance_id, target_instance_id, limit)` (line 3017): no docstring
    - `async clear_link_edits(self, db_name, link_type_id, branch)` (line 3055): no docstring
    - `async create_schema_migration_plan(self, db_name, subject_type, subject_id, plan, status, plan_id)` (line 3080): no docstring
    - `async list_schema_migration_plans(self, db_name, subject_type, subject_id, status, limit)` (line 3113): no docstring
    - `async get_ingest_request_by_key(self, idempotency_key)` (line 3144): no docstring
    - `async get_ingest_request(self, ingest_request_id)` (line 3188): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 3232): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 3307): no docstring
    - `async create_ingest_transaction(self, ingest_request_id, status)` (line 3339): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 3378): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 3419): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 3457): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 3522): no docstring
    - `async update_ingest_request_payload(self, ingest_request_id, schema_json, sample_json, row_count, source_metadata)` (line 3555): no docstring
    - `async approve_ingest_schema(self, ingest_request_id, schema_json, approved_by)` (line 3587): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, apply_schema, outbox_entries)` (line 3697): no docstring
    - `async claim_ingest_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 3966): no docstring
    - `async mark_ingest_outbox_published(self, outbox_id)` (line 4043): no docstring
    - `async mark_ingest_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 4062): no docstring
    - `async mark_ingest_outbox_dead(self, outbox_id, error)` (line 4089): no docstring
    - `async purge_ingest_outbox(self, retention_days, limit)` (line 4109): no docstring
    - `async get_ingest_outbox_metrics(self)` (line 4139): no docstring
    - `async reconcile_ingest_state(self, stale_after_seconds, limit, use_lock, lock_key)` (line 4180): Best-effort reconciliation for ingest atomicity.

### `backend/shared/services/registries/lineage_store.py`
- **Functions**
  - `create_lineage_store(settings)` (line 1165): no docstring
- **Classes**
  - `LineageStore` (line 24): Postgres-backed lineage store.
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 34): no docstring
    - `async initialize(self)` (line 50): no docstring
    - `async connect(self)` (line 53): no docstring
    - `async shutdown(self)` (line 64): no docstring
    - `async close(self)` (line 67): no docstring
    - `async health_check(self)` (line 72): no docstring
    - `async ensure_schema(self)` (line 82): no docstring
    - `node_event(event_id)` (line 291): no docstring
    - `node_aggregate(aggregate_type, aggregate_id)` (line 295): no docstring
    - `node_artifact(kind, *parts)` (line 299): no docstring
    - `_infer_node_type(node_id)` (line 304): no docstring
    - `_parse_node_id(node_id)` (line 316): Decompose node_id into queryable columns (best-effort).
    - `_run_context(cls)` (line 376): no docstring
    - `_deterministic_edge_id(*parts)` (line 381): no docstring
    - `_coerce_metadata(value)` (line 386): Coerce a Postgres JSONB value into a Python dict.
    - `async upsert_node(self, node_id, node_type, label, metadata, created_at, recorded_at, db_name, run_id, code_sha, schema_version)` (line 418): no docstring
    - `async insert_edge(self, from_node_id, to_node_id, edge_type, occurred_at, metadata, projection_name, recorded_at, db_name, run_id, code_sha, schema_version, edge_id)` (line 483): no docstring
    - `async enqueue_backfill(self, envelope, s3_bucket, s3_key, error)` (line 547): Best-effort enqueue for eventual lineage recovery.
    - `async mark_backfill_done(self, event_id)` (line 596): no docstring
    - `async mark_backfill_failed(self, event_id, error)` (line 610): no docstring
    - `async claim_backfill_batch(self, limit, db_name)` (line 626): Claim a batch of pending backfill rows (best-effort).
    - `async get_backfill_metrics(self, db_name)` (line 682): no docstring
    - `async count_edges(self, edge_type, db_name, since, until)` (line 719): no docstring
    - `async get_latest_edges_to(self, to_node_ids, edge_type, projection_name, db_name)` (line 755): Fetch the latest edge (by occurred_at) for each `to_node_id`.
    - `async record_link(self, from_node_id, to_node_id, edge_type, occurred_at, edge_metadata, from_label, to_label, from_type, to_type, from_metadata, to_metadata, db_name, projection_name, run_id, code_sha, schema_version, edge_id)` (line 833): no docstring
    - `async record_event_envelope(self, envelope, s3_bucket, s3_key)` (line 909): Record the core lineage relationships for an EventEnvelope:
    - `normalize_root(root)` (line 1006): no docstring
    - `async get_graph(self, root, direction, max_depth, max_nodes, max_edges, db_name)` (line 1019): no docstring

### `backend/shared/services/registries/objectify_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 46): no docstring
- **Classes**
  - `OCCConflictError` (line 25): Raised when optimistic concurrency control check fails.
    - `__init__(self, table, record_id, expected_version, actual_version)` (line 28): no docstring
  - `OntologyMappingSpecRecord` (line 73): no docstring
  - `ObjectifyJobRecord` (line 94): no docstring
  - `ObjectifyOutboxItem` (line 116): no docstring
  - `ObjectifyRegistry` (line 130): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 131): no docstring
    - `async initialize(self)` (line 147): no docstring
    - `async connect(self)` (line 150): no docstring
    - `async close(self)` (line 161): no docstring
    - `async ensure_schema(self)` (line 166): no docstring
    - `_normalize_optional(value)` (line 371): no docstring
    - `build_dedupe_key(dataset_id, dataset_branch, mapping_spec_id, mapping_spec_version, dataset_version_id, artifact_id, artifact_output_name)` (line 378): no docstring
    - `_validate_objectify_inputs(self, dataset_version_id, artifact_id, artifact_output_name)` (line 402): no docstring
    - `async create_mapping_spec(self, dataset_id, dataset_branch, artifact_output_name, schema_hash, backing_datasource_id, backing_datasource_version_id, target_class_id, mappings, target_field_types, status, auto_sync, options)` (line 416): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 541): no docstring
    - `async list_mapping_specs(self, dataset_id, include_inactive, limit)` (line 579): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 644): no docstring
    - `async create_objectify_job(self, job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id, artifact_id, artifact_output_name, dataset_branch, target_class_id, status, outbox_payload, dedupe_key)` (line 707): no docstring
    - `async get_objectify_metrics(self)` (line 817): no docstring
    - `async enqueue_objectify_job(self, job)` (line 862): no docstring
    - `async enqueue_outbox_for_job(self, job_id, payload)` (line 890): no docstring
    - `async has_outbox_for_job(self, job_id, statuses)` (line 912): no docstring
    - `async claim_objectify_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 937): no docstring
    - `async mark_objectify_outbox_published(self, outbox_id, job_id)` (line 1011): no docstring
    - `async mark_objectify_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 1040): no docstring
    - `async purge_objectify_outbox(self, retention_days, limit)` (line 1066): no docstring
    - `async list_objectify_jobs(self, statuses, older_than, limit)` (line 1096): no docstring
    - `async get_objectify_job(self, job_id)` (line 1149): no docstring
    - `async get_objectify_job_by_dedupe_key(self, dedupe_key)` (line 1187): no docstring
    - `async find_objectify_job(self, dataset_version_id, mapping_spec_id, mapping_spec_version, statuses)` (line 1230): no docstring
    - `async find_objectify_job_for_artifact(self, artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, statuses)` (line 1284): no docstring
    - `async update_objectify_job_status(self, job_id, status, command_id, error, report, completed_at, expected_version)` (line 1343): Update objectify job status with optional OCC.
    - `async get_watermark(self, mapping_spec_id, dataset_branch)` (line 1467): Get the watermark state for a mapping spec.
    - `async update_watermark(self, mapping_spec_id, dataset_branch, watermark_column, watermark_value, dataset_version_id, lakefs_commit_id, rows_processed)` (line 1508): Update or create watermark state for a mapping spec.
    - `async delete_watermark(self, mapping_spec_id, dataset_branch)` (line 1569): Delete watermark state (for full refresh reset).
    - `async get_all_watermarks(self, mapping_spec_id, limit)` (line 1594): Get all watermarks, optionally filtered by mapping spec.

### `backend/shared/services/registries/ontology_key_spec_registry.py`
- **Functions**
  - `_normalize_str_list(values)` (line 27): no docstring
  - `_json_default(value)` (line 50): no docstring
- **Classes**
  - `OntologyKeySpec` (line 57): no docstring
  - `OntologyKeySpecRegistry` (line 67): Persist ordered ontology key specs in Postgres.
    - `__init__(self, postgres_url, pool_min, pool_max)` (line 76): no docstring
    - `async _ensure_pool(self)` (line 88): no docstring
    - `async close(self)` (line 99): no docstring
    - `async ensure_schema(self)` (line 104): no docstring
    - `async upsert_key_spec(self, db_name, branch, class_id, primary_key, title_key)` (line 125): no docstring
    - `async get_key_spec(self, db_name, branch, class_id)` (line 158): no docstring
    - `async delete_key_spec(self, db_name, branch, class_id)` (line 180): no docstring
    - `async get_key_spec_index(self, db_name, branch, class_ids)` (line 197): Fetch many key specs in a single query.
    - `_row_to_model(row)` (line 233): no docstring

### `backend/shared/services/registries/pipeline_plan_registry.py`
- **Classes**
  - `PipelinePlanRecord` (line 21): no docstring
  - `PipelinePlanRegistry` (line 35): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 36): no docstring
    - `async initialize(self)` (line 52): no docstring
    - `async connect(self)` (line 55): no docstring
    - `async close(self)` (line 66): no docstring
    - `async shutdown(self)` (line 71): no docstring
    - `async ensure_schema(self)` (line 74): no docstring
    - `_row_to_plan(self, row)` (line 107): no docstring
    - `async upsert_plan(self, plan_id, tenant_id, status, goal, db_name, branch, plan, created_by)` (line 122): no docstring
    - `async get_plan(self, plan_id, tenant_id)` (line 175): no docstring
    - `async list_plans(self, tenant_id, status, limit, offset)` (line 192): no docstring

### `backend/shared/services/registries/pipeline_registry.py`
- **Functions**
  - `_ensure_json_string(value)` (line 65): no docstring
  - `_normalize_output_list(value, field_name)` (line 71): no docstring
  - `_is_production_env()` (line 84): no docstring
  - `_lakefs_credentials_source()` (line 88): no docstring
  - `_lakefs_service_principal()` (line 96): no docstring
  - `_lakefs_fernet()` (line 110): no docstring
  - `_encrypt_secret(secret_access_key)` (line 123): no docstring
  - `_decrypt_secret(encrypted)` (line 130): no docstring
  - `_normalize_pipeline_role(value)` (line 148): no docstring
  - `_normalize_principal_type(value)` (line 157): no docstring
  - `_role_allows(required, assigned)` (line 164): no docstring
  - `_definition_object_key(db_name, pipeline_name)` (line 170): no docstring
  - `_row_to_pipeline_record(row)` (line 188): no docstring
  - `_row_to_pipeline_artifact(row)` (line 341): no docstring
- **Classes**
  - `PipelineMergeNotSupportedError` (line 33): no docstring
  - `PipelineOCCConflictError` (line 37): Raised when optimistic concurrency control detects a version conflict.
    - `__init__(self, pipeline_id, expected_version, actual_version)` (line 40): no docstring
  - `PipelineAlreadyExistsError` (line 57): no docstring
    - `__init__(self, db_name, name, branch)` (line 58): no docstring
  - `LakeFSCredentials` (line 178): no docstring
  - `PipelineRecord` (line 228): no docstring
  - `PipelineVersionRecord` (line 266): no docstring
  - `PipelineUdfRecord` (line 276): no docstring
  - `PipelineUdfVersionRecord` (line 287): no docstring
  - `PipelineArtifactRecord` (line 296): no docstring
  - `PromotionManifestRecord` (line 320): no docstring
  - `PipelineRegistry` (line 366): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 367): no docstring
    - `async _get_lakefs_credentials(self, principal_type, principal_id)` (line 383): no docstring
    - `async upsert_lakefs_credentials(self, principal_type, principal_id, access_key_id, secret_access_key, created_by)` (line 421): no docstring
    - `async list_lakefs_credentials(self)` (line 466): no docstring
    - `async resolve_lakefs_credentials(self, user_id)` (line 489): no docstring
    - `async get_lakefs_client(self, user_id)` (line 526): no docstring
    - `async get_lakefs_storage(self, user_id)` (line 537): no docstring
    - `async ensure_lakefs_branch(self, repository, branch, source, user_id)` (line 547): Ensure the target lakeFS branch exists before attempting S3-gateway writes.
    - `_resolve_repository(self, pipeline)` (line 581): no docstring
    - `async initialize(self)` (line 588): no docstring
    - `async connect(self)` (line 591): no docstring
    - `async close(self)` (line 602): no docstring
    - `async ensure_schema(self)` (line 607): no docstring
    - `async list_dependencies(self, pipeline_id)` (line 1140): no docstring
    - `async replace_dependencies(self, pipeline_id, dependencies)` (line 1159): no docstring
    - `async grant_permission(self, pipeline_id, principal_type, principal_id, role)` (line 1197): no docstring
    - `async revoke_permission(self, pipeline_id, principal_type, principal_id)` (line 1228): no docstring
    - `async list_permissions(self, pipeline_id)` (line 1253): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 1278): no docstring
    - `async get_permission_role(self, pipeline_id, principal_type, principal_id)` (line 1294): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 1323): no docstring
    - `async create_pipeline(self, db_name, name, description, pipeline_type, location, status, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle, schedule_interval_seconds, schedule_cron, pipeline_id)` (line 1340): no docstring
    - `async list_pipelines(self, db_name, branch)` (line 1427): no docstring
    - `async list_proposals(self, db_name, branch, status)` (line 1530): no docstring
    - `async submit_proposal(self, pipeline_id, title, description, proposal_bundle)` (line 1583): no docstring
    - `async review_proposal(self, pipeline_id, status, review_comment)` (line 1627): no docstring
    - `async merge_branch(self, pipeline_id, from_branch, to_branch, user_id)` (line 1665): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 1761): no docstring
    - `async get_pipeline_by_name(self, db_name, name, branch)` (line 1786): no docstring
    - `async update_pipeline(self, pipeline_id, name, description, location, status, schedule_interval_seconds, schedule_cron, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle, expected_version)` (line 1819): Update pipeline with Optimistic Concurrency Control.
    - `async add_version(self, pipeline_id, branch, definition_json, version_id, user_id)` (line 1938): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 2059): no docstring
    - `async get_version(self, pipeline_id, lakefs_commit_id, branch)` (line 2097): no docstring
    - `async record_preview(self, pipeline_id, status, row_count, sample_json, job_id, node_id)` (line 2134): no docstring
    - `async record_run(self, pipeline_id, job_id, mode, status, node_id, row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash, input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version, started_at, finished_at)` (line 2178): no docstring
    - `async upsert_artifact(self, pipeline_id, job_id, mode, status, run_id, artifact_id, definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id, inputs, lakefs_repository, lakefs_branch, lakefs_commit_id, outputs, declared_outputs, sampling_strategy, error)` (line 2287): no docstring
    - `async get_artifact(self, artifact_id)` (line 2405): no docstring
    - `async get_artifact_by_job(self, pipeline_id, job_id, mode)` (line 2426): no docstring
    - `async list_artifacts(self, pipeline_id, limit, mode)` (line 2462): no docstring
    - `async list_runs(self, pipeline_id, limit)` (line 2496): no docstring
    - `async get_run(self, pipeline_id, job_id)` (line 2545): no docstring
    - `async get_watermarks(self, pipeline_id, branch)` (line 2591): no docstring
    - `async upsert_watermarks(self, pipeline_id, branch, watermarks)` (line 2611): no docstring
    - `async record_build(self, pipeline_id, status, output_json, deployed_commit_id)` (line 2646): no docstring
    - `async record_promotion_manifest(self, pipeline_id, db_name, build_job_id, artifact_id, definition_hash, lakefs_repository, lakefs_commit_id, ontology_commit_id, mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id, promoted_dataset_version_id, promoted_dataset_name, target_branch, promoted_by, metadata, manifest_id, promoted_at)` (line 2678): no docstring
    - `async list_scheduled_pipelines(self)` (line 2799): no docstring
    - `async record_schedule_tick(self, pipeline_id, scheduled_at)` (line 2853): no docstring
    - `async list_pipeline_branches(self, db_name)` (line 2868): no docstring
    - `async get_pipeline_branch(self, db_name, branch)` (line 2895): no docstring
    - `async archive_pipeline_branch(self, db_name, branch)` (line 2921): no docstring
    - `async restore_pipeline_branch(self, db_name, branch)` (line 2958): no docstring
    - `async create_branch(self, pipeline_id, new_branch, user_id)` (line 2995): no docstring
    - `async create_udf(self, db_name, name, code, description)` (line 3079): no docstring
    - `async create_udf_version(self, udf_id, code)` (line 3138): no docstring
    - `async list_udfs(self, db_name)` (line 3199): List all UDFs for a database.
    - `async get_udf(self, udf_id)` (line 3221): no docstring
    - `async get_udf_version(self, udf_id, version)` (line 3241): no docstring
    - `async get_udf_latest_version(self, udf_id)` (line 3268): no docstring

### `backend/shared/services/registries/processed_event_heartbeat.py`
- **Functions**
  - `async run_processed_event_heartbeat_loop(registry, handler, event_id, interval_seconds, stop_when_false, continue_on_exception, logger, warning_message)` (line 13): Keep a ProcessedEventRegistry lease alive while processing an event.

### `backend/shared/services/registries/processed_event_registry.py`
- **Functions**
  - `validate_lease_settings()` (line 584): no docstring
  - `validate_registry_enabled()` (line 602): no docstring
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

### `backend/shared/services/storage/__init__.py`

### `backend/shared/services/storage/elasticsearch_service.py`
- **Functions**
  - `create_elasticsearch_service(settings)` (line 598): Elasticsearch 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_elasticsearch_service_legacy(host, port, username, password)` (line 621): 레거시 Elasticsearch 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `ElasticsearchService` (line 25): Async Elasticsearch client service with connection pooling and error handling.
    - `__init__(self, host, port, username, password, use_ssl, verify_certs, request_timeout, max_retries, retry_on_timeout)` (line 39): no docstring
    - `async connect(self)` (line 74): Initialize Elasticsearch connection.
    - `async disconnect(self)` (line 88): Close Elasticsearch connection.
    - `client(self)` (line 98): Get Elasticsearch client instance.
    - `async get_cluster_health(self)` (line 104): Get Elasticsearch cluster health status.
    - `async create_index(self, index, mappings, settings, aliases)` (line 116): Create an index with optional mappings, settings, and aliases.
    - `async delete_index(self, index)` (line 154): Delete an index.
    - `async index_exists(self, index)` (line 175): Check if index exists.
    - `async update_mapping(self, index, properties)` (line 183): Update index mapping.
    - `async index_document(self, index, document, doc_id, refresh, version, version_type, op_type)` (line 211): Index a single document.
    - `async get_document(self, index, doc_id, source_includes, source_excludes)` (line 254): Get a document by ID.
    - `async update_document(self, index, doc_id, doc, script, upsert, refresh)` (line 287): Update a document.
    - `async delete_document(self, index, doc_id, refresh, version, version_type)` (line 335): Delete a document.
    - `async bulk_index(self, index, documents, chunk_size, refresh)` (line 372): Bulk index documents.
    - `async search(self, index, query, size, from_, sort, source_includes, source_excludes, aggregations)` (line 421): Search documents.
    - `async count(self, index, query)` (line 474): Count documents matching query.
    - `async create_alias(self, index, alias, filter)` (line 502): Create an alias for an index with optional filter.
    - `async delete_alias(self, index, alias)` (line 535): Delete an alias.
    - `async update_aliases(self, actions)` (line 557): Perform multiple alias operations atomically.
    - `async refresh_index(self, index)` (line 580): Force refresh an index to make changes searchable.
    - `async ping(self)` (line 589): Check Elasticsearch connection.

### `backend/shared/services/storage/event_store.py`
- **Functions**
  - `async get_event_store()` (line 1168): Dependency to get the Event Store instance
- **Classes**
  - `EventStore` (line 45): The REAL Event Store using S3/MinIO as Single Source of Truth.
    - `__init__(self)` (line 60): no docstring
    - `_sequence_mode(self)` (line 74): no docstring
    - `_lineage_enabled(self)` (line 79): no docstring
    - `_audit_enabled(self)` (line 83): no docstring
    - `_s3_client_kwargs(self)` (line 86): no docstring
    - `async connect(self)` (line 113): Initialize S3/MinIO connection
    - `async _initialize_lineage_and_audit(self)` (line 152): no docstring
    - `_partition_key_for_envelope(envelope)` (line 172): no docstring
    - `async _record_lineage_and_audit(self, envelope, s3_key, audit_action)` (line 181): no docstring
    - `async _record_audit_failure(self, envelope, error)` (line 252): no docstring
    - `async _get_sequence_allocator(self)` (line 289): no docstring
    - `async _ensure_sequence_number(self, event)` (line 302): Ensure `event.sequence_number` is set using an atomic write-side allocator.
    - `async append_event(self, event)` (line 438): Append an immutable event to S3/MinIO.
    - `_enforce_idempotency_contract(self, existing, incoming, source)` (line 619): Detect event_id reuse with conflicting contents.
    - `_is_command_envelope(env)` (line 654): no docstring
    - `_normalize_for_idempotency_compare(env)` (line 660): no docstring
    - `_stable_hash(payload)` (line 675): no docstring
    - `async _get_existing_key_by_event_id(self, s3, event_id)` (line 679): no docstring
    - `async _get_existing_key_by_aggregate_index(self, s3, aggregate_type, aggregate_id, event_id)` (line 692): no docstring
    - `async _read_event_object(self, s3, key)` (line 714): no docstring
    - `async get_event_object_key(self, event_id)` (line 719): Resolve an event_id to its S3 object key using the by-event-id index.
    - `async read_event_by_key(self, key)` (line 731): Read an event envelope from S3/MinIO by object key.
    - `async get_events(self, aggregate_type, aggregate_id, from_version, to_version)` (line 739): Retrieve all events for an aggregate from S3/MinIO.
    - `_dedup_events(events)` (line 829): Best-effort dedup to hide historical duplicates during migration.
    - `_dedup_key(event)` (line 842): no docstring
    - `async replay_events(self, from_timestamp, to_timestamp, event_types)` (line 853): Replay events from S3/MinIO for a time range.
    - `async get_aggregate_version(self, aggregate_type, aggregate_id, allow_full_scan)` (line 945): Get the current version of an aggregate (max sequence_number).
    - `async _update_indexes(self, event, key, s3)` (line 989): Update various indexes for efficient querying.
    - `async _write_index_entries(self, s3, aggregate_index_key, date_index_key, event_id_index_key, payload)` (line 1057): no docstring
    - `async get_snapshot(self, aggregate_type, aggregate_id, version)` (line 1086): Get a snapshot of an aggregate at a specific version.
    - `async save_snapshot(self, aggregate_type, aggregate_id, version, state)` (line 1114): Save a snapshot for performance optimization.

### `backend/shared/services/storage/lakefs_client.py`
- **Functions**
  - `_extract_commit_id(payload)` (line 58): no docstring
  - `_normalize_metadata(metadata)` (line 67): lakeFS commit/merge metadata is stored as string key-value pairs.
- **Classes**
  - `LakeFSError` (line 19): no docstring
  - `LakeFSAuthError` (line 23): no docstring
  - `LakeFSNotFoundError` (line 27): no docstring
  - `LakeFSConflictError` (line 31): no docstring
  - `LakeFSConfig` (line 36): no docstring
    - `from_env()` (line 42): no docstring
  - `LakeFSClient` (line 93): Minimal async lakeFS REST client.
    - `__init__(self, config, timeout_seconds)` (line 102): no docstring
    - `_client(self)` (line 108): no docstring
    - `async create_branch(self, repository, name, source)` (line 115): no docstring
    - `async delete_branch(self, repository, name)` (line 137): no docstring
    - `async commit(self, repository, branch, message, metadata)` (line 155): no docstring
    - `async get_branch_head_commit_id(self, repository, branch)` (line 188): no docstring
    - `async list_diff_objects(self, repository, ref, since, prefix, amount)` (line 210): no docstring
    - `async merge(self, repository, source_ref, destination_branch, message, metadata, allow_empty)` (line 285): no docstring

### `backend/shared/services/storage/lakefs_storage_service.py`
- **Functions**
  - `create_lakefs_storage_service(settings)` (line 27): no docstring
- **Classes**
  - `LakeFSStorageService` (line 17): no docstring
    - `async create_bucket(self, bucket_name)` (line 18): no docstring
    - `async bucket_exists(self, bucket_name)` (line 22): no docstring

### `backend/shared/services/storage/redis_service.py`
- **Functions**
  - `create_redis_service(settings)` (line 432): Redis 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_redis_service_legacy(host, port, password)` (line 453): 레거시 Redis 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `RedisService` (line 20): Async Redis client service with connection pooling and error handling.
    - `__init__(self, host, port, password, db, decode_responses, max_connections, socket_timeout, connection_timeout, retry_on_timeout)` (line 32): no docstring
    - `async connect(self)` (line 66): Initialize Redis connection.
    - `async initialize(self)` (line 76): ServiceContainer-compatible initialization method.
    - `async disconnect(self)` (line 80): Close Redis connection and pool.
    - `client(self)` (line 99): Get Redis client instance.
    - `async set_command_status(self, command_id, status, data, ttl)` (line 107): Set command status with optional data.
    - `async get_command_status(self, command_id)` (line 140): Get command status and data.
    - `async update_command_progress(self, command_id, progress, message)` (line 158): Update command execution progress.
    - `async set_command_result(self, command_id, result, ttl)` (line 184): Store command execution result.
    - `async get_command_result(self, command_id)` (line 206): Get command execution result.
    - `async publish_command_update(self, command_id, data)` (line 226): Publish command status update to subscribers.
    - `async subscribe_command_updates(self, command_id, callback, task_manager)` (line 241): Subscribe to command status updates with proper task tracking.
    - `async _listen_for_updates(self, pubsub, callback, command_id)` (line 290): Listen for pub/sub updates with improved error handling.
    - `_handle_listener_done(self, task)` (line 327): Handle completion of a listener task.
    - `async set_json(self, key, value, ttl)` (line 338): Set JSON value with optional TTL.
    - `async get_json(self, key)` (line 350): Get JSON value.
    - `async set(self, key, value, ttl)` (line 357): Set key-value pair with optional TTL.
    - `async get(self, key)` (line 363): Get value for key.
    - `async delete(self, key)` (line 367): Delete key.
    - `async exists(self, key)` (line 371): Check if key exists.
    - `async expire(self, key, seconds)` (line 375): Set expiration on key.
    - `async keys(self, pattern)` (line 379): Get keys matching pattern.
    - `async ping(self)` (line 383): Check Redis connection.
    - `async cleanup_listeners(self)` (line 390): Clean up all active pub/sub listeners.
    - `async scan_keys(self, pattern, count)` (line 408): Scan keys matching pattern without blocking.

### `backend/shared/services/storage/s3_client_config.py`
- **Functions**
  - `build_s3_client_config(endpoint_url, addressing_style, extra_path_style_hosts)` (line 28): Return a botocore Config for S3 addressing style, or None when not needed.

### `backend/shared/services/storage/storage_service.py`
- **Functions**
  - `create_storage_service(settings)` (line 875): 스토리지 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_storage_service_legacy(endpoint_url, access_key, secret_key)` (line 909): 레거시 스토리지 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `StorageService` (line 29): S3/MinIO 스토리지 서비스 - Event Sourcing 지원
    - `__init__(self, endpoint_url, access_key, secret_key, region, use_ssl, ssl_verify)` (line 53): 스토리지 서비스 초기화
    - `async create_bucket(self, bucket_name)` (line 90): 버킷 생성
    - `async bucket_exists(self, bucket_name)` (line 111): 버킷 존재 여부 확인
    - `async save_json(self, bucket, key, data, metadata)` (line 130): JSON 데이터를 S3에 저장하고 체크섬 반환
    - `async save_bytes(self, bucket, key, data, content_type, metadata)` (line 177): Raw bytes를 S3에 저장하고 체크섬 반환
    - `async save_fileobj(self, bucket, key, fileobj, content_type, metadata, checksum)` (line 224): Stream a file-like object into S3 and return a checksum.
    - `async load_json(self, bucket, key)` (line 280): S3에서 JSON 데이터 로드
    - `async load_bytes(self, bucket, key)` (line 301): S3에서 Raw bytes 로드
    - `async load_bytes_lines(self, bucket, key, max_lines, max_bytes)` (line 320): Load up to `max_lines` newline-delimited lines from the start of an object.
    - `async verify_checksum(self, bucket, key, expected_checksum)` (line 372): 저장된 파일의 체크섬 검증
    - `async delete_object(self, bucket, key)` (line 397): S3 객체 삭제
    - `async delete_prefix(self, bucket, prefix)` (line 414): Delete all objects under a prefix.
    - `async list_objects(self, bucket, prefix, max_keys)` (line 456): 버킷의 객체 목록 조회
    - `async list_objects_paginated(self, bucket, prefix, max_keys, continuation_token)` (line 484): Paginated object listing (returns next continuation token if more).
    - `async iter_objects(self, bucket, prefix, max_keys)` (line 505): Async iterator over all objects under prefix (pagination-aware).
    - `async get_object_metadata(self, bucket, key)` (line 528): 객체 메타데이터 조회
    - `generate_instance_path(self, db_name, class_id, instance_id, command_id)` (line 556): 인스턴스 이벤트 저장 경로 생성
    - `async get_all_commands_for_instance(self, bucket, db_name, class_id, instance_id)` (line 577): 특정 인스턴스의 모든 Command 파일 목록 조회
    - `async list_command_files(self, bucket, prefix)` (line 640): List command JSON objects under a prefix (pagination-aware, sorted by LastModified).
    - `async replay_instance_state(self, bucket, command_files)` (line 680): Command 파일들을 순차적으로 읽어 인스턴스의 최종 상태 재구성
    - `is_instance_deleted(self, instance_state)` (line 837): 인스턴스가 삭제된 상태인지 확인
    - `get_deletion_info(self, instance_state)` (line 851): 삭제된 인스턴스의 삭제 정보 반환

### `backend/shared/setup.py`

### `backend/shared/testing/__init__.py`

### `backend/shared/testing/config_fixtures.py`
- **Functions**
  - `create_mock_storage_service()` (line 174): Create mock storage service for testing
  - `create_mock_redis_service()` (line 184): Create mock Redis service for testing
  - `create_mock_elasticsearch_service()` (line 196): Create mock Elasticsearch service for testing
  - `create_mock_label_mapper()` (line 207): Create mock label mapper for testing
  - `create_mock_jsonld_converter()` (line 215): Create mock JSON-LD converter for testing
  - `test_settings()` (line 224): Pytest fixture for test application settings
  - `test_settings_with_overrides()` (line 230): Pytest fixture factory for test settings with custom overrides
  - `async mock_container(test_settings)` (line 238): Pytest fixture for mock service container
  - `mock_command_status_service()` (line 256): Pytest fixture for mock command status service
  - `async isolated_test_environment(**config_overrides)` (line 295): Async context manager for completely isolated test environment
  - `setup_test_database_config(**overrides)` (line 323): Create test settings with database configuration
  - `setup_test_service_config(**overrides)` (line 337): Create test settings with service configuration
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
    - `async shutdown(self)` (line 153): Shutdown all services
  - `ConfigOverride` (line 265): Context manager for temporary configuration overrides
    - `__init__(self, **overrides)` (line 273): no docstring
    - `__enter__(self)` (line 277): no docstring
    - `__exit__(self, exc_type, exc_val, exc_tb)` (line 285): no docstring

### `backend/shared/tools/__init__.py`

### `backend/shared/tools/registry_cleanup.py`
- **Functions**
  - `_env_list(key)` (line 60): no docstring
  - `_is_safe_endpoint(base_url)` (line 67): no docstring
  - `_ensure_dev_only(base_url)` (line 77): no docstring
  - `_normalize_base_url(base_url)` (line 84): no docstring
  - `_resolve_bff_base_url()` (line 91): no docstring
  - `_resolve_admin_token()` (line 96): no docstring
  - `_matches_any_prefix(name, prefixes)` (line 104): no docstring
  - `_matches_any_name(name, names)` (line 108): no docstring
  - `_extract_command_id(payload)` (line 112): no docstring
  - `async _wait_for_command(client, base_url, command_id, timeout_seconds)` (line 135): no docstring
  - `async _list_databases(client, base_url)` (line 158): no docstring
  - `async _connect_postgres()` (line 179): no docstring
  - `_sequence_schema()` (line 183): no docstring
  - `_database_handler()` (line 190): no docstring
  - `async _fetch_updated_at_map(conn, db_names)` (line 195): no docstring
  - `async _delete_database(client, base_url, db_name)` (line 219): no docstring
  - `_build_plan(args)` (line 248): no docstring
  - `async _run_once(plan, base_url, token)` (line 280): no docstring
  - `build_parser()` (line 355): no docstring
  - `async _async_main(args)` (line 369): no docstring
  - `main()` (line 394): no docstring
- **Classes**
  - `CleanupPlan` (line 237): no docstring

### `backend/shared/utils/__init__.py`

### `backend/shared/utils/access_policy.py`
- **Functions**
  - `_as_list(value)` (line 25): no docstring
  - `_coerce_bool(value)` (line 33): no docstring
  - `_match_rule(value, op, expected)` (line 39): no docstring
  - `_match_filters(row, filters, operator)` (line 72): no docstring
  - `_apply_mask(row, columns, mask_value)` (line 103): no docstring
  - `apply_access_policy(rows, policy)` (line 121): Apply a row/column policy to result rows.

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

### `backend/shared/utils/action_input_schema.py`
- **Functions**
  - `_json_size_bytes(value)` (line 48): no docstring
  - `_walk_keys(value)` (line 56): no docstring
  - `_require_public_key(name)` (line 69): no docstring
  - `_normalize_field_spec(raw)` (line 76): no docstring
  - `normalize_input_schema(input_schema)` (line 148): Normalize an ActionType input_schema definition.
  - `validate_action_input(input_schema, payload, max_total_bytes)` (line 181): Validate and normalize an Action submission payload against ActionType.input_schema.
  - `_validate_value(field, value)` (line 241): no docstring
- **Classes**
  - `ActionInputSchemaError` (line 15): no docstring
  - `ActionInputValidationError` (line 19): no docstring
  - `_FieldSpec` (line 34): no docstring

### `backend/shared/utils/action_template_engine.py`
- **Functions**
  - `_is_non_empty_str(value)` (line 24): no docstring
  - `_require_public_identifier(value, label)` (line 28): no docstring
  - `_split_dotted_path(path, label)` (line 39): no docstring
  - `_get_by_path(obj, path, label)` (line 54): no docstring
  - `_normalize_object_ref(value, label)` (line 65): no docstring
  - `_coerce_link_value(value, label)` (line 76): no docstring
  - `_is_ref_object(value)` (line 85): no docstring
  - `_resolve_ref_object(value, input_payload, user, target, now)` (line 89): no docstring
  - `_resolve_value(value, input_payload, user, target, now)` (line 114): no docstring
  - `_extract_link_field_names(raw_ops)` (line 143): no docstring
  - `_compile_link_ops(raw_ops, input_payload, user, target, now, label)` (line 163): no docstring
  - `_normalize_unset_list(value, label)` (line 199): no docstring
  - `_normalize_set_ops(value, label)` (line 211): no docstring
  - `_is_noop_change_spec(changes)` (line 223): no docstring
  - `_merge_change_specs(existing, incoming)` (line 234): no docstring
  - `_validate_template_v1(implementation)` (line 259): no docstring
  - `validate_template_v1_definition(implementation)` (line 282): Validate that an ActionType.implementation is executable (P0).
  - `compile_template_v1_change_shape(implementation, input_payload)` (line 326): Compile a template_v1 into a per-target "change shape" (keys only) for submission-time observed_base snapshots.
  - `compile_template_v1(implementation, input_payload, user, target_docs, now)` (line 401): Compile a template_v1 into concrete per-target changes.
- **Classes**
  - `ActionImplementationError` (line 16): no docstring
  - `CompiledTarget` (line 276): no docstring

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
  - `chaos_enabled()` (line 28): no docstring
  - `_sanitize_marker(point)` (line 32): no docstring
  - `maybe_crash(point, logger)` (line 38): Crash the current process if CHAOS_CRASH_POINT matches.

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
  - `_looks_like_change(value)` (line 7): no docstring
  - `normalize_diff_changes(raw)` (line 14): no docstring
  - `_classify_change(change)` (line 40): no docstring
  - `summarize_diff_changes(changes)` (line 56): no docstring
  - `normalize_diff_response(from_ref, to_ref, raw)` (line 67): no docstring

### `backend/shared/utils/env_utils.py`
- **Functions**
  - `parse_bool(raw)` (line 7): no docstring
  - `parse_bool_env(name, default)` (line 16): no docstring
  - `parse_int_env(name, default, min_value, max_value)` (line 21): no docstring

### `backend/shared/utils/event_utils.py`
- **Functions**
  - `build_command_event(event_type, aggregate_type, aggregate_id, data, command_type, actor, event_id)` (line 10): no docstring

### `backend/shared/utils/executor_utils.py`
- **Functions**
  - `async call_in_executor(executor, func, *args, **kwargs)` (line 11): no docstring

### `backend/shared/utils/id_generator.py`
- **Functions**
  - `_normalize_korean_to_roman(text)` (line 22): 한국어를 로마자로 변환 (간단한 매핑)
  - `_extract_text_from_label(label)` (line 48): 레이블에서 텍스트 추출
  - `_clean_and_format_id(text, preserve_camel_case)` (line 95): 텍스트를 ID 형식으로 정리
  - `_generate_timestamp()` (line 124): 고유성을 위한 타임스탬프 생성
  - `_generate_short_timestamp()` (line 129): 짧은 타임스탬프 생성
  - `generate_ontology_id(label, preserve_camel_case, handle_korean, default_fallback)` (line 134): 온톨로지 ID 생성 (고급 옵션)
  - `generate_simple_id(label, use_timestamp_for_korean, default_fallback)` (line 188): 간단한 ID 생성 (타임스탬프 포함)
  - `generate_unique_id(label, prefix, suffix, max_length, force_unique)` (line 250): 고유 ID 생성 (확장 가능한 버전)
  - `generate_class_id(label)` (line 304): 클래스 ID 생성
  - `generate_property_id(label)` (line 309): 속성 ID 생성
  - `generate_relationship_id(label)` (line 314): 관계 ID 생성
  - `generate_instance_id(class_id, label)` (line 319): 인스턴스 ID 생성
  - `validate_generated_id(id_string)` (line 349): 생성된 ID의 유효성 검증
- **Classes**
  - `IDGenerationError` (line 17): ID 생성 관련 예외

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
  - `json_default(value)` (line 8): no docstring
  - `normalize_json_payload(value, default_handler)` (line 14): no docstring
  - `coerce_json_dataset(value)` (line 25): no docstring
  - `coerce_json_pipeline(value)` (line 47): no docstring
  - `coerce_json_strict(value)` (line 70): no docstring

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
  - `LabelMapper` (line 26): 레이블과 ID 간의 매핑을 관리하는 클래스
    - `_resolve_database_path(db_path)` (line 33): 데이터베이스 파일 경로를 안전하게 해결합니다.
    - `__init__(self, db_path)` (line 63): 초기화
    - `_ensure_directory(self)` (line 76): 데이터베이스 디렉토리 생성
    - `async _init_database(self)` (line 80): 데이터베이스 초기화 및 테이블 생성 (thread-safe)
    - `async _get_connection(self)` (line 171): 데이터베이스 연결 컨텍스트 매니저 (with connection pooling)
    - `async register_class(self, db_name, class_id, label, description)` (line 185): 클래스 레이블 매핑 등록
    - `async get_class_labels_in_batch(self, db_name, class_ids, lang)` (line 246): 여러 클래스의 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `async get_property_labels_in_batch(self, db_name, class_id, property_ids, lang)` (line 294): 특정 클래스의 여러 속성 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `async get_all_property_labels_in_batch(self, db_name, class_property_pairs, lang)` (line 343): 여러 클래스의 여러 속성 레이블을 한 번의 쿼리로 조회 (N+1 쿼리 문제 완전 해결)
    - `async get_relationship_labels_in_batch(self, db_name, predicates, lang)` (line 394): 여러 관계의 레이블을 한 번에 조회 (N+1 쿼리 문제 해결)
    - `_extract_ids_from_data_list(self, data_list)` (line 442): Extract class IDs, property IDs, and predicates from data list.
    - `_extract_property_ids_from_data(self, data, property_ids)` (line 470): Extract property IDs from a single data item.
    - `_extract_class_property_pairs(self, data_list)` (line 489): Extract all (class_id, property_id) pairs from data list.
    - `_convert_properties_to_display(self, properties, class_id, property_labels)` (line 508): Convert properties to display format with labels.
    - `_convert_relationships_to_display(self, relationships, relationship_labels)` (line 540): Convert relationships to display format with labels.
    - `_convert_data_item_to_display(self, data, class_labels, property_labels, relationship_labels)` (line 548): Convert a single data item to display format.
    - `async convert_to_display_batch(self, db_name, data_list, lang)` (line 578): Convert multiple data items to label-based format in batch (solves N+1 query problem)
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
  - `get_supported_languages()` (line 15): Get list of supported languages.
  - `get_default_language()` (line 25): Get default language.
  - `is_supported_language(lang)` (line 35): Check if language is supported.
  - `normalize_language(lang)` (line 48): Normalize language code.
  - `_parse_accept_language_header(value)` (line 84): Parse Accept-Language into a list of language codes ordered by preference.
  - `_normalize_language_map_key(key)` (line 114): Strict normalization for language-map keys.
  - `get_accept_language(request)` (line 133): Get the preferred language from the request.
  - `get_language_name(lang)` (line 156): Get human-readable name for language code.
  - `detect_language_from_text(text)` (line 171): Simple language detection from text.
  - `fallback_languages(lang)` (line 194): Languages to try in order when a translation is missing.
  - `coerce_localized_text(value, default_lang)` (line 206): Coerce a LocalizedText-like value into a normalized language map.
  - `select_localized_text(value, lang)` (line 249): Choose the best string for the requested language from a LocalizedText-like input.
- **Classes**
  - `MultilingualText` (line 261): Utility class for handling multilingual text.
    - `__init__(self, **kwargs)` (line 266): Initialize with language-specific text.
    - `get(self, language, fallback)` (line 275): Get text for specific language.
    - `set(self, language, text)` (line 305): Set text for specific language.
    - `has_language(self, language)` (line 315): Check if text exists for language.
    - `get_languages(self)` (line 327): Get list of available languages.
    - `to_dict(self)` (line 336): Convert to dictionary.

### `backend/shared/utils/llm_safety.py`
- **Functions**
  - `sha256_hex(value)` (line 27): no docstring
  - `stable_json_dumps(obj)` (line 32): no docstring
  - `digest_for_audit(obj)` (line 36): no docstring
  - `truncate_text(text, max_chars)` (line 40): no docstring
  - `_mask_email(text)` (line 48): no docstring
  - `_mask_long_digits(text)` (line 57): no docstring
  - `mask_pii_text(text, max_chars)` (line 80): no docstring
  - `mask_pii(obj, max_string_chars)` (line 89): Recursively mask likely PII in a JSON-like structure.
  - `sample_items(items, max_items)` (line 112): no docstring
  - `build_agent_error_response(error_message, error_code, recoverable, hint, suggested_action, context)` (line 118): Build a standard error response format for the Agent.
  - `detect_value_pattern(value)` (line 173): Best-effort value pattern detection for raw-data reasoning.
  - `extract_column_value_patterns(values, max_samples)` (line 232): Summarize observed value patterns for a column.
  - `build_column_semantic_observations(column_name, values)` (line 303): Produce higher-level semantic hints for a column based on:
  - `_normalize_value_for_overlap(value)` (line 362): no docstring
  - `build_relationship_observations(left_column, right_column, left_values, right_values, max_examples)` (line 369): Compare two columns' values to produce FK/relationship hints.

### `backend/shared/utils/log_rotation.py`
- **Functions**
  - `create_default_rotation_manager(log_dir)` (line 308): Create log rotation manager with sensible defaults for test services
- **Classes**
  - `LogRotationManager` (line 16): 🔥 THINK ULTRA! Professional log rotation with compression and cleanup
    - `__init__(self, log_dir, max_size_mb, max_files, compress_after_days, delete_after_days)` (line 19): no docstring
    - `get_file_size(self, file_path)` (line 36): Get file size in bytes, handling errors gracefully
    - `get_file_age_days(self, file_path)` (line 43): Get file age in days
    - `should_rotate(self, log_file)` (line 52): Check if log file should be rotated based on size
    - `rotate_log_file(self, log_file, service_name)` (line 60): Rotate a log file by renaming it with timestamp and creating new one
    - `compress_old_logs(self)` (line 93): Compress log files older than compress_after_days
    - `cleanup_old_logs(self)` (line 131): Remove log files older than delete_after_days
    - `limit_rotated_files(self, service_name)` (line 169): Ensure we don't exceed max_files limit for rotated logs
    - `perform_maintenance(self, service_logs)` (line 207): Perform complete log maintenance: rotation, compression, cleanup
    - `get_log_directory_info(self)` (line 282): Get information about the log directory

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
  - `build_principal_tags(principal_type, principal_id, user_id, role)` (line 11): no docstring
  - `policy_allows(policy, principal_tags)` (line 35): Evaluate a minimal principal policy:
- **Classes**
  - `PrincipalPolicyError` (line 7): no docstring

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

### `backend/shared/utils/schema_hash.py`
- **Functions**
  - `compute_schema_hash(columns)` (line 12): Produce a stable hash for a list of column definitions.
  - `compute_schema_hash_from_sample(sample_json)` (line 23): no docstring

### `backend/shared/utils/schema_type_compatibility.py`
- **Functions**
  - `is_type_compatible(source_type, target_type)` (line 21): no docstring

### `backend/shared/utils/spice_event_ids.py`
- **Functions**
  - `spice_event_id(command_id, event_type, aggregate_id)` (line 6): Deterministic domain event id derived from a command id.

### `backend/shared/utils/submission_criteria_diagnostics.py`
- **Functions**
  - `infer_submission_criteria_failure_reason(expression)` (line 7): Best-effort heuristics for classifying `submission_criteria` failures.

### `backend/shared/utils/terminus_branch.py`
- **Functions**
  - `encode_branch_name(branch_name)` (line 21): no docstring
  - `decode_branch_name(branch_name)` (line 30): no docstring

### `backend/shared/utils/time_utils.py`
- **Functions**
  - `utcnow()` (line 6): no docstring

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
    - `_validate_string_type(cls, value)` (line 248): Validate string type.
    - `_validate_integer_type(cls, value, data_type)` (line 255): Validate integer types (int, long, short, byte).
    - `_validate_unsigned_integer_type(cls, value, data_type)` (line 262): Validate unsigned integer types.
    - `_validate_float_type(cls, value)` (line 271): Validate float/double types.
    - `_validate_boolean_type(cls, value)` (line 278): Validate boolean type.
    - `_validate_decimal_type(cls, value)` (line 285): Validate decimal type.
    - `_validate_date_type(cls, value)` (line 293): Validate date type.
    - `_validate_datetime_type(cls, value)` (line 306): Validate datetime type.
    - `_validate_uri_type(cls, value)` (line 319): Validate URI type.
    - `_get_type_validator_map(cls)` (line 331): Get mapping of data types to their validator functions.
    - `_validate_xsd_type(cls, value, data_type, constraints)` (line 353): Validate XSD data types using type-specific validators.
    - `get_supported_types(cls)` (line 366): Get list of supported complex types.
    - `is_supported_type(cls, data_type)` (line 399): Check if data type is supported.

### `backend/shared/validators/constraint_validator.py`
- **Classes**
  - `ConstraintValidator` (line 13): Enhanced constraint validation for complex types
    - `validate_constraints(cls, value, data_type, constraints)` (line 17): Validate a value against a set of constraints
    - `_validate_string_constraints(cls, value, constraints)` (line 72): Validate string-specific constraints
    - `_validate_numeric_constraints(cls, value, constraints)` (line 108): Validate numeric constraints
    - `_validate_collection_constraints(cls, value, constraints)` (line 159): Validate collection constraints
    - `_validate_format(cls, value, format_name)` (line 209): Validate common format constraints
    - `_validate_pattern(cls, value, pattern)` (line 247): Validate against regex pattern
    - `_validate_custom(cls, value, validator_func)` (line 260): Validate using custom validation function
    - `merge_constraints(cls, *constraint_sets)` (line 282): Merge multiple constraint sets with proper precedence
    - `validate_constraint_compatibility(cls, constraints)` (line 295): Check if constraints are compatible with each other

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
  - `GeoPointValidator` (line 16): Validator for geopoint values (lat,lon or geohash).
    - `validate(self, value, constraints)` (line 19): no docstring
    - `normalize(self, value)` (line 70): no docstring
    - `get_supported_types(self)` (line 75): no docstring

### `backend/shared/validators/geoshape_validator.py`
- **Functions**
  - `_coordinates_valid(value)` (line 81): no docstring
- **Classes**
  - `GeoShapeValidator` (line 24): Validator for GeoJSON geometry payloads.
    - `validate(self, value, constraints)` (line 27): no docstring
    - `normalize(self, value)` (line 69): no docstring
    - `get_supported_types(self)` (line 77): no docstring

### `backend/shared/validators/google_sheets_validator.py`
- **Classes**
  - `GoogleSheetsValidator` (line 12): Validator for Google Sheets URLs
    - `validate(self, value, constraints)` (line 18): Validate Google Sheets URL
    - `normalize(self, value)` (line 87): Normalize Google Sheets URL
    - `get_supported_types(self)` (line 107): Get supported types

### `backend/shared/validators/image_validator.py`
- **Classes**
  - `ImageValidator` (line 12): Validator for image URLs
    - `validate(self, value, constraints)` (line 29): Validate image URL
    - `normalize(self, value)` (line 135): Normalize image URL
    - `get_supported_types(self)` (line 143): Get supported types
    - `is_image_extension(cls, filename)` (line 148): Check if filename has image extension

### `backend/shared/validators/ip_validator.py`
- **Classes**
  - `IpValidator` (line 16): Validator for IP addresses
    - `validate(self, value, constraints)` (line 25): Validate IP address
    - `normalize(self, value)` (line 120): Normalize IP address
    - `get_supported_types(self)` (line 136): Get supported types

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
  - `NamingConvention` (line 14): Supported naming conventions
  - `NameValidator` (line 29): Validator for various naming conventions and patterns
    - `validate(self, value, constraints)` (line 57): Validate name according to constraints
    - `_detect_convention(self, value)` (line 153): Detect the naming convention used
    - `normalize(self, value)` (line 160): Normalize name
    - `convert_convention(self, value, from_convention, to_convention)` (line 168): Convert between naming conventions
    - `get_supported_types(self)` (line 209): Get supported types

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
  - `StringValidator` (line 12): Validator for strings with constraints
    - `validate(self, value, constraints)` (line 15): Validate string with constraints
    - `normalize(self, value)` (line 101): Normalize string
    - `get_supported_types(self)` (line 109): Get supported types

### `backend/shared/validators/struct_validator.py`
- **Classes**
  - `StructValidator` (line 13): Validator for struct values (flat object without nested arrays/objects).
    - `validate(self, value, constraints)` (line 16): no docstring
    - `normalize(self, value)` (line 47): no docstring
    - `get_supported_types(self)` (line 50): no docstring

### `backend/shared/validators/url_validator.py`
- **Classes**
  - `UrlValidator` (line 16): Validator for URL strings
    - `validate(self, value, constraints)` (line 22): Validate URL
    - `normalize(self, value)` (line 106): Normalize URL
    - `get_supported_types(self)` (line 114): Get supported types

### `backend/shared/validators/uuid_validator.py`
- **Classes**
  - `UuidValidator` (line 16): Validator for UUIDs
    - `validate(self, value, constraints)` (line 22): Validate UUID
    - `normalize(self, value)` (line 110): Normalize UUID
    - `get_supported_types(self)` (line 126): Get supported types

### `backend/shared/validators/vector_validator.py`
- **Classes**
  - `VectorValidator` (line 14): Validator for numeric vector payloads.
    - `validate(self, value, constraints)` (line 17): no docstring
    - `normalize(self, value)` (line 66): no docstring
    - `get_supported_types(self)` (line 74): no docstring

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
  - `_load_repo_dotenv()` (line 8): no docstring
  - `_env_or_dotenv(dotenv, key, default)` (line 12): no docstring
  - `pytest_configure()` (line 16): Host-run integration defaults for the `backend/tests` suite.

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
- **Classes**
  - `TestSafeKafkaConsumerConfiguration` (line 92): Verify SafeKafkaConsumer enforces critical settings.
    - `test_enforced_isolation_level(self)` (line 95): SafeKafkaConsumer should always use read_committed.
    - `test_enforced_auto_commit_disabled(self)` (line 102): SafeKafkaConsumer should disable auto-commit.
    - `test_workers_use_safe_consumer(self)` (line 109): Verify all Kafka-consuming workers use SafeKafkaConsumer (no raw Consumer).
    - `test_no_raw_consumer_in_worker_tree(self)` (line 161): Hard-fail if any *_worker module imports raw Consumer (regression guard).
  - `TestKafkaProducerConfiguration` (line 226): Verify critical producer-side idempotence settings are present where required.
    - `test_pipeline_job_queue_producer_is_idempotent(self)` (line 229): no docstring
    - `test_connector_trigger_producer_is_idempotent(self)` (line 253): no docstring
  - `TestKafkaConnectivity` (line 283): Verify Kafka connectivity and topic access.
    - `test_kafka_topics_exist(self)` (line 287): Verify all declared Kafka topics exist (SSoT: AppConfig.get_all_topics).
    - `test_kafka_consumer_groups_listable(self)` (line 300): Verify consumer groups can be listed (connectivity + broker feature check).
    - `test_expected_consumer_groups_present(self)` (line 310): Verify critical worker consumer groups exist (proxy for "workers are actually running").
    - `test_read_committed_filters_aborted_transactions(self)` (line 366): End-to-end verification: read_committed consumers must NOT see aborted transactional messages.
  - `TestBFFHealth` (line 459): Verify BFF is running and healthy.
    - `async test_bff_health_endpoint(self)` (line 464): BFF health endpoint should return success.
    - `async test_bff_databases_connected(self)` (line 475): BFF should have database connections.
  - `TestMSAServiceHealth` (line 489): Verify all first-class services are reachable (full stack).
    - `async test_oms_health_endpoint(self)` (line 494): no docstring
    - `async test_funnel_health_endpoint(self)` (line 501): no docstring
    - `async test_agent_health_endpoint(self)` (line 508): no docstring
    - `async test_ingest_reconciler_health_endpoint(self)` (line 515): no docstring
  - `TestDatabaseConnectivity` (line 526): Verify database connectivity.
    - `async test_postgres_connection(self)` (line 531): PostgreSQL should be connectable.
    - `async test_redis_connection(self)` (line 548): Redis should be connectable and require authentication (prod safety).
  - `TestInfraConnectivity` (line 603): Verify non-DB infra dependencies (S3/MinIO, lakeFS, Elasticsearch, TerminusDB).
    - `async test_minio_health(self)` (line 608): no docstring
    - `async test_lakefs_health(self)` (line 615): no docstring
    - `async test_elasticsearch_health(self)` (line 624): no docstring
    - `test_terminusdb_port_open(self)` (line 634): no docstring
  - `TestSystemWiring` (line 645): Verify producer/consumer wiring between key MSAs (no mocks).
    - `async test_message_relay_publishes_event_store_appends(self)` (line 655): Message relay must publish newly appended Event Store events to Kafka.
    - `async test_connector_trigger_publishes_outbox(self)` (line 729): Connector trigger service must publish pending outbox items to Kafka.
    - `async test_pipeline_scheduler_enqueues_scheduled_pipeline(self)` (line 783): Pipeline scheduler must enqueue due scheduled pipelines to Kafka.
    - `async test_action_outbox_emits_action_applied_event(self)` (line 864): Action outbox worker must emit ActionApplied into the event store, and the relay must publish it to Kafka.
  - `TestOutboxPatternVerification` (line 987): Verify outbox pattern implementation.
    - `test_objectify_outbox_uses_aggregate_key(self)` (line 990): Objectify outbox should scope ordering by run_id when present.
    - `test_objectify_outbox_tracks_delivery(self)` (line 1005): Objectify outbox should track individual delivery status.
    - `test_message_relay_prefers_ordering_key(self)` (line 1018): Message relay should prefer ordering_key for Kafka partitioning when present.
  - `TestProcessedEventRegistryIdempotency` (line 1033): Verify ProcessedEventRegistry provides idempotency.
    - `async test_claim_idempotency(self)` (line 1038): ProcessedEventRegistry.claim should be idempotent.
    - `async test_sequence_ordering(self)` (line 1072): ProcessedEventRegistry should enforce sequence ordering.
  - `TestPipelineJobQueue` (line 1126): Verify PipelineJobQueue implementation.
    - `test_pipeline_job_has_dedupe_key(self)` (line 1129): PipelineJob should auto-generate dedupe_key.
    - `test_pipeline_job_queue_uses_pipeline_id_key(self)` (line 1143): PipelineJobQueue should use pipeline_id as partition key.
  - `TestEndToEndFlowSimulation` (line 1157): Simulate E2E flows without actually processing jobs.
    - `async test_objectify_job_creation_flow(self)` (line 1161): Test creating an objectify job payload.
    - `async test_pipeline_job_creation_flow(self)` (line 1189): Test creating a pipeline job payload.
  - `TestWorkerModuleImports` (line 1224): Verify workers can be imported without errors.
    - `test_objectify_worker_import(self)` (line 1227): Objectify worker should be importable.
    - `test_pipeline_worker_import(self)` (line 1232): Pipeline worker should be importable.
    - `test_action_worker_import(self)` (line 1237): Action worker should be importable.
    - `test_search_projection_worker_import(self)` (line 1242): Search projection worker should be importable.
    - `test_connector_sync_worker_import(self)` (line 1247): Connector sync worker should be importable.
    - `test_ontology_worker_import(self)` (line 1252): Ontology worker should be importable.
    - `test_instance_worker_import(self)` (line 1257): Instance worker should be importable.
    - `test_projection_worker_import(self)` (line 1262): Projection worker should be importable.
    - `test_action_outbox_worker_import(self)` (line 1267): Action outbox worker should be importable.
    - `test_ingest_reconciler_worker_import(self)` (line 1272): Ingest reconciler worker should be importable.
    - `test_writeback_materializer_worker_import(self)` (line 1277): Writeback materializer worker should be importable.
    - `test_connector_trigger_service_import(self)` (line 1282): Connector trigger service should be importable.
    - `test_pipeline_scheduler_import(self)` (line 1287): Pipeline scheduler should be importable.
    - `test_message_relay_import(self)` (line 1292): Message relay should be importable.
  - `TestConsistencySummary` (line 1303): Summary tests for all consistency features.
    - `test_all_critical_settings_enforced(self)` (line 1306): All critical Kafka settings should be enforced.
    - `test_idempotency_patterns_present(self)` (line 1318): Idempotency patterns should be present.
    - `test_occ_support_present(self)` (line 1336): OCC support should be present in PipelineRegistry.

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
  - `_disable_env_file(monkeypatch)` (line 8): no docstring
  - `test_pipeline_publish_lock_timeout_fallback(monkeypatch)` (line 14): no docstring
  - `test_agent_bff_token_and_command_timeout_fallback(monkeypatch)` (line 31): no docstring
  - `test_client_token_fallbacks(monkeypatch)` (line 49): no docstring
  - `test_lakefs_repository_defaults(monkeypatch)` (line 69): no docstring
  - `test_local_port_host_aliases(monkeypatch)` (line 76): no docstring

### `backend/tests/unit/errors/__init__.py`

### `backend/tests/unit/errors/test_error_envelope_lint.py`
- **Functions**
  - `_collect_status_error_literals(backend_dir)` (line 20): no docstring
  - `test_no_direct_status_error_dicts()` (line 55): no docstring
  - `test_error_response_contains_enterprise_metadata()` (line 72): no docstring

### `backend/tests/unit/errors/test_error_taxonomy_coverage.py`
- **Functions**
  - `_extract_enum_values(path, class_name)` (line 13): no docstring
  - `_extract_dict_literal_keys(path, var_name)` (line 33): no docstring
  - `_collect_code_like_literals(backend_dir)` (line 57): no docstring
  - `test_error_taxonomy_covers_all_code_like_literals()` (line 91): no docstring

### `backend/tests/unit/errors/test_policy_drift_guards.py`
- **Functions**
  - `test_enterprise_catalog_fingerprint_is_pinned()` (line 18): no docstring
  - `test_agent_tool_allowlist_bundle_hash_is_pinned()` (line 22): no docstring

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
  - `test_bff_agent_tool_idempotency_replays_without_reexecution()` (line 700): no docstring

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

### `backend/tests/unit/openapi/test_wip_hidden.py`
- **Functions**
  - `test_wip_projection_endpoints_hidden_from_openapi()` (line 8): no docstring

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

### `backend/tests/unit/services/test_action_simulation_scenarios.py`
- **Functions**
  - `_preflight(conflict_fields, conflict_policy)` (line 13): no docstring
  - `test_conflict_policy_fail_rejects()` (line 52): no docstring
  - `test_conflict_policy_base_wins_skips()` (line 61): no docstring
  - `test_conflict_policy_writeback_wins_applies()` (line 78): no docstring
  - `test_no_conflict_does_not_reject_under_fail()` (line 93): no docstring

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
  - `async test_event_store_connect_is_idempotent_under_concurrency(monkeypatch)` (line 47): no docstring
- **Classes**
  - `_DummyS3` (line 12): no docstring
    - `__init__(self, counters)` (line 13): no docstring
    - `async head_bucket(self, **_)` (line 16): no docstring
    - `async create_bucket(self, **_)` (line 19): no docstring
    - `async put_bucket_versioning(self, **_)` (line 22): no docstring
  - `_DummyS3ClientContext` (line 26): no docstring
    - `__init__(self, s3)` (line 27): no docstring
    - `async __aenter__(self)` (line 30): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 33): no docstring
  - `_DummySession` (line 37): no docstring
    - `__init__(self, s3)` (line 38): no docstring
    - `client(self, **_)` (line 41): no docstring

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

### `backend/tests/unit/services/test_graph_reverse_hops.py`
- **Functions**
  - `_make_service()` (line 8): no docstring
  - `_find_relationship_triples(woql)` (line 20): no docstring
  - `test_build_multi_hop_woql_reverse_hop_flips_triple_direction()` (line 29): no docstring
  - `test_build_multi_hop_woql_forward_hop_keeps_triple_direction()` (line 49): no docstring

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

### `backend/tests/unit/services/test_ontology_resource_validator.py`
- **Functions**
  - `test_action_type_requires_input_schema_and_policy()` (line 19): no docstring
  - `test_object_type_requires_pk_spec_and_backing_source()` (line 28): no docstring
  - `test_shared_property_requires_properties_list()` (line 37): no docstring
  - `test_function_requires_expression_and_return_type_ref()` (line 43): no docstring
  - `test_action_type_rejects_unsafe_submission_criteria_expression()` (line 50): no docstring
  - `test_action_type_rejects_invalid_validation_rules()` (line 66): no docstring
  - `test_link_type_invalid_predicate_is_reported()` (line 82): no docstring
  - `test_relationship_spec_missing_is_reported()` (line 89): no docstring
  - `test_relationship_spec_invalid_type_is_reported()` (line 95): no docstring
  - `test_relationship_spec_object_backed_requires_object_type()` (line 101): no docstring
  - `test_relationship_spec_join_table_requires_dataset_or_auto_create()` (line 116): no docstring
  - `async test_link_type_missing_refs_are_reported()` (line 133): no docstring
- **Classes**
  - `_FakeTerminus` (line 11): no docstring
    - `__init__(self, existing)` (line 12): no docstring
    - `async get_ontology(self, db_name, class_id, branch)` (line 15): no docstring

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

### `backend/tests/unit/services/test_pipeline_plan_builder.py`
- **Functions**
  - `test_new_plan_has_minimum_shape()` (line 33): no docstring
  - `test_add_input_and_output_wires_edges()` (line 41): no docstring
  - `test_add_input_supports_read_config()` (line 60): no docstring
  - `test_add_external_input_creates_input_node_without_dataset_selection()` (line 73): no docstring
  - `test_configure_input_read_patches_input_nodes_only()` (line 89): no docstring
  - `test_add_join_requires_two_inputs_and_keys()` (line 102): no docstring
  - `test_add_join_accepts_hints_and_broadcast_flags()` (line 121): no docstring
  - `test_add_join_rejects_cross_join()` (line 139): no docstring
  - `test_add_cast_requires_column_and_type()` (line 155): no docstring
  - `test_compute_column_and_assignments_build_metadata()` (line 167): no docstring
  - `test_select_expr_builds_metadata()` (line 186): no docstring
  - `test_add_sort_supports_desc_prefix_and_dict_form()` (line 195): no docstring
  - `test_add_explode_builds_metadata()` (line 209): no docstring
  - `test_add_union_builds_metadata()` (line 218): no docstring
  - `test_add_pivot_builds_metadata()` (line 228): no docstring
  - `test_add_edge_is_idempotent()` (line 247): no docstring
  - `test_delete_edge_is_noop_if_missing_but_warns()` (line 259): no docstring
  - `test_set_node_inputs_replaces_incoming_edges_ordered()` (line 272): no docstring
  - `test_update_node_metadata_merges_and_unsets()` (line 287): no docstring
  - `test_update_settings_patches_definition_settings()` (line 305): no docstring
  - `test_delete_node_removes_outputs_entry()` (line 317): no docstring
  - `test_update_output_renames_and_syncs_output_node_metadata()` (line 329): no docstring

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
  - `test_compile_row_udf_accepts_simple_transform()` (line 7): no docstring
  - `test_compile_row_udf_rejects_imports()` (line 19): no docstring
  - `test_compile_row_udf_rejects_private_attribute_access()` (line 32): no docstring
  - `test_compile_row_udf_rejects_top_level_statements()` (line 43): no docstring
  - `test_compile_row_udf_rejects_loops()` (line 56): no docstring

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

### `backend/tests/unit/services/test_pipeline_worker_diff_handling.py`
- **Functions**
  - `async test_list_lakefs_diff_paths_ignores_removed()` (line 14): no docstring
  - `async test_load_input_dataframe_fallback_on_diff_failure(monkeypatch)` (line 42): no docstring
  - `async test_load_input_dataframe_removed_only_diff_returns_empty(monkeypatch)` (line 88): no docstring

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

### `backend/tests/unit/utils/test_action_input_schema.py`
- **Functions**
  - `test_validate_action_input_validates_and_normalizes_object_ref()` (line 10): no docstring
  - `test_validate_action_input_rejects_unknown_fields_by_default()` (line 23): no docstring
  - `test_validate_action_input_rejects_reserved_internal_keys_anywhere()` (line 29): no docstring
  - `test_validate_action_input_reports_invalid_schema()` (line 35): no docstring

### `backend/tests/unit/utils/test_action_template_engine.py`
- **Functions**
  - `test_compile_template_v1_change_shape_merges_and_tracks_touched_fields()` (line 12): no docstring
  - `test_compile_template_v1_resolves_refs_and_now()` (line 47): no docstring
  - `test_compile_template_v1_rejects_delete_plus_edits_for_same_target()` (line 81): no docstring
  - `test_compile_template_v1_supports_bulk_targets_from_list()` (line 95): no docstring

### `backend/tests/unit/utils/test_canonical_json.py`
- **Functions**
  - `test_canonical_json_dumps_sorts_keys_and_is_compact()` (line 6): no docstring
  - `test_canonical_json_dumps_normalizes_datetime_to_utc()` (line 10): no docstring
  - `test_sha256_prefixed_has_expected_prefix()` (line 16): no docstring

### `backend/tests/unit/utils/test_deprecation_utils.py`
- **Functions**
  - `test_deprecated_decorator_sync()` (line 8): no docstring
  - `test_legacy_and_experimental_decorators()` (line 24): no docstring

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

### `backend/tests/unit/utils/test_ontology_stamp.py`
- **Functions**
  - `test_merge_ontology_stamp_prefers_existing()` (line 6): no docstring
  - `test_merge_ontology_stamp_fills_missing()` (line 15): no docstring

### `backend/tests/unit/utils/test_principal_policy.py`
- **Functions**
  - `test_build_principal_tags_user_id_back_compat()` (line 4): no docstring
  - `test_build_principal_tags_emits_typed_principal()` (line 9): no docstring
  - `test_policy_allows_matches_typed_principal()` (line 14): no docstring

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

### `backend/tests/unit/utils/test_submission_criteria_diagnostics.py`
- **Functions**
  - `test_submission_criteria_reason_missing_role()` (line 9): no docstring
  - `test_submission_criteria_reason_state_mismatch()` (line 16): no docstring
  - `test_submission_criteria_reason_mixed()` (line 23): no docstring

### `backend/tests/unit/utils/test_utils_core.py`
- **Functions**
  - `test_parse_bool_env_and_int_env(monkeypatch)` (line 6): no docstring
  - `test_safe_path_helpers()` (line 20): no docstring
  - `test_s3_uri_helpers()` (line 27): no docstring
  - `test_branch_utils_defaults(monkeypatch)` (line 35): no docstring

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

### `backend/tests/unit/workers/test_connector_sync_worker.py`
- **Functions**
  - `async test_sync_worker_bff_scope_headers()` (line 155): no docstring
  - `async test_sync_worker_fetch_schema_and_target_types(monkeypatch)` (line 162): no docstring
  - `async test_sync_worker_process_google_sheets_update(monkeypatch)` (line 178): no docstring
  - `async test_sync_worker_handle_envelope_rejects_unknown()` (line 238): no docstring
  - `async test_sync_worker_run_processes_message(monkeypatch)` (line 252): no docstring
  - `async test_sync_worker_heartbeat_loop_stops(monkeypatch)` (line 281): no docstring
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
  - `async test_pk_duplicates_fail_before_writes()` (line 91): no docstring
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
  - `_PKDuplicateWorker` (line 48): no docstring
    - `__init__(self, rows, mapping_spec, dataset, version)` (line 49): no docstring
    - `async _iter_dataset_batches(self, **kwargs)` (line 55): no docstring
    - `async _fetch_class_schema(self, job)` (line 58): no docstring
    - `async _fetch_object_type_contract(self, job)` (line 61): no docstring
    - `async _fetch_ontology_version(self, job)` (line 71): no docstring
    - `async _fetch_value_type_defs(self, job, value_type_refs)` (line 74): no docstring
    - `async _record_lineage_header(self, **kwargs)` (line 77): no docstring
    - `async _validate_batches(self, **kwargs)` (line 80): no docstring
    - `async _bulk_update_instances(self, **kwargs)` (line 83): no docstring
    - `async _record_gate_result(self, **kwargs)` (line 86): no docstring

### `backend/tests/unit/workers/test_ontology_worker_helpers.py`
- **Functions**
  - `async test_wait_for_database_exists_success()` (line 19): no docstring
  - `async test_wait_for_database_exists_timeout()` (line 27): no docstring
  - `async test_consumer_call_executes()` (line 36): no docstring
  - `async test_heartbeat_loop_no_registry()` (line 47): no docstring
- **Classes**
  - `_StubTerminus` (line 10): no docstring
    - `__init__(self, exists)` (line 11): no docstring
    - `async database_exists(self, db_name)` (line 14): no docstring

### `backend/tests/unit/workers/test_pipeline_worker_helpers.py`
- **Functions**
  - `test_resolve_code_version_and_sensitive_keys(monkeypatch)` (line 20): no docstring
  - `test_resolve_lakefs_repository(monkeypatch)` (line 27): no docstring
  - `test_watermark_snapshot_helpers()` (line 32): no docstring
  - `test_resolve_output_format_and_partitions()` (line 58): no docstring

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
  - `spark()` (line 30): no docstring
  - `worker(spark)` (line 48): no docstring
  - `test_apply_transform_basic_ops(worker)` (line 54): no docstring
  - `test_apply_transform_join_union_groupby_pivot_window(worker)` (line 113): no docstring
  - `test_watermark_helpers(worker)` (line 155): no docstring
  - `test_pipeline_worker_file_helpers(worker, tmp_path)` (line 168): no docstring

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
  - `async main()` (line 333): no docstring
- **Classes**
  - `WritebackMaterializerWorker` (line 72): no docstring
    - `__init__(self)` (line 73): no docstring
    - `async initialize(self)` (line 81): no docstring
    - `async shutdown(self)` (line 91): no docstring
    - `async _ensure_branch(self, repository, branch)` (line 94): no docstring
    - `async _scan_queue(self, repository, branch)` (line 102): no docstring
    - `async materialize_db(self, db_name)` (line 135): no docstring
    - `async _materialize_db_inner(self, db_name)` (line 151): no docstring
    - `async run(self)` (line 312): no docstring
