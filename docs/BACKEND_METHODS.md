# Backend Method Index

> Status: Auto-generated snapshot. It may be stale; regenerate from source if you need an up-to-date index.

> Generated: 2026-01-05T10:15:01.826369Z
> Scope: backend/**/*.py (including scripts and tests, excluding __pycache__)

## add_palantir_system_fields.py

### `backend/add_palantir_system_fields.py`
- **Functions**
  - `async add_system_fields_to_ontology(db_name, class_id, terminus_service)` (line 69): Add Palantir system fields to a specific ontology class
  - `async add_system_fields_to_all_ontologies(db_name)` (line 146): Add Palantir system fields to all ontologies in a database
  - `async verify_palantir_architecture(db_name)` (line 230): Verify that the Palantir architecture is properly configured

## add_system_fields_direct.py

### `backend/add_system_fields_direct.py`
- **Functions**
  - `async update_ontology_with_system_fields()` (line 10): Update ontologies to include system fields

## add_system_fields_to_schema.py

### `backend/add_system_fields_to_schema.py`
- **Functions**
  - `async add_system_fields_to_class(terminus_url, db_name, class_name, auth)` (line 11): Add system fields to a single class
  - `async get_all_classes(terminus_url, db_name, auth)` (line 80): Get all classes in the database
  - `async main()` (line 120): Main function to add system fields to all classes

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

### `backend/bff/dependencies.py`
- **Functions**
  - `async get_terminus_service(oms_client)` (line 576): Get TerminusService with modern dependency injection
  - `async check_bff_dependencies_health(container)` (line 598): Check health of all BFF dependencies
- **Classes**
  - `BFFDependencyProvider` (line 45): Modern dependency provider for BFF services
    - `async get_oms_client(container)` (line 54): Get OMS client from container
    - `async get_label_mapper(container)` (line 80): Get label mapper from container
    - `async get_jsonld_converter(container)` (line 104): Get JSON-LD converter from container
  - `TerminusService` (line 135): OMS client wrapper for TerminusService compatibility - Modernized version
    - `__init__(self, oms_client)` (line 143): Initialize with OMS client dependency
    - `async _await_if_needed(value)` (line 154): no docstring
    - `async _raise_for_status(self, response)` (line 159): no docstring
    - `async _response_json(self, response)` (line 162): no docstring
    - `async list_databases(self)` (line 165): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 176): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 181): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 186): 데이터베이스 정보 조회
    - `async list_classes(self, db_name, branch)` (line 191): 클래스 목록 조회
    - `async create_class(self, db_name, class_data, branch, headers)` (line 199): 클래스 생성
    - `async get_class(self, db_name, class_id, branch)` (line 214): 클래스 조회
    - `async update_class(self, db_name, class_id, class_data, expected_seq, branch, headers)` (line 238): 클래스 업데이트
    - `async delete_class(self, db_name, class_id, expected_seq, branch, headers)` (line 254): 클래스 삭제
    - `async query_database(self, db_name, query)` (line 269): 데이터베이스 쿼리
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 275): 브랜치 생성 - 실제 OMS API 호출
    - `async delete_branch(self, db_name, branch_name)` (line 286): 브랜치 삭제 - 실제 OMS API 호출
    - `async checkout(self, db_name, target, target_type)` (line 295): 체크아웃 - 실제 OMS API 호출
    - `async commit_changes(self, db_name, message, author, branch)` (line 307): 변경사항 커밋 - 실제 OMS API 호출
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 324): 커밋 히스토리 조회 - 실제 OMS API 호출
    - `async get_diff(self, db_name, base, compare)` (line 331): 차이 비교 - 실제 OMS API 호출
    - `async merge_branches(self, db_name, source, target, strategy, message, author)` (line 341): 브랜치 병합 - 실제 OMS API 호출
    - `async rollback(self, db_name, target_commit, create_branch, branch_name)` (line 364): 롤백 - 실제 OMS API 호출
    - `async get_branch_info(self, db_name, branch_name)` (line 385): 브랜치 정보 조회 - 실제 OMS API 호출
    - `async simulate_merge(self, db_name, source_branch, target_branch, strategy)` (line 397): 병합 시뮬레이션 - 충돌 감지 without 실제 병합
    - `async resolve_merge_conflicts(self, db_name, source_branch, target_branch, resolutions, strategy, message, author)` (line 415): 수동 충돌 해결 및 병합 실행
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references, headers)` (line 447): 고급 관계 관리 기능을 포함한 온톨로지 생성 - OMS API 호출
    - `async validate_relationships(self, db_name, ontology_data, branch, headers)` (line 476): 온톨로지 관계 검증 - OMS API 호출 (no write).
    - `async detect_circular_references(self, db_name, branch, new_ontology, headers)` (line 500): 순환 참조 탐지 - OMS API 호출 (no write).
    - `async analyze_relationship_network(self, db_name, branch, headers)` (line 524): 관계 네트워크 분석 - OMS API 호출 (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch, headers)` (line 546): 관계 경로 탐색 - OMS API 호출 (no write).

### `backend/bff/main.py`
- **Functions**
  - `async lifespan(app)` (line 477): Modern application lifecycle management
  - `async get_oms_client()` (line 657): Get OMS client from BFF container
  - `async get_label_mapper()` (line 667): Get label mapper from BFF container
  - `async get_google_sheets_service()` (line 677): Get Google Sheets service from BFF container
  - `async get_connector_registry()` (line 687): Get ConnectorRegistry from BFF container
  - `async get_dataset_registry()` (line 697): Get DatasetRegistry from BFF container
  - `async get_pipeline_registry()` (line 707): Get PipelineRegistry from BFF container
  - `async get_objectify_registry()` (line 724): no docstring
  - `async get_pipeline_executor()` (line 730): Get PipelineExecutor from BFF container
- **Classes**
  - `BFFServiceContainer` (line 122): BFF-specific service container to manage BFF services
    - `__init__(self, container, settings)` (line 130): no docstring
    - `async initialize_bff_services(self)` (line 135): Initialize BFF-specific services
    - `async _initialize_oms_client(self)` (line 174): Initialize OMS client with health check
    - `async _initialize_label_mapper(self)` (line 198): Initialize label mapper
    - `async _initialize_type_inference(self)` (line 204): Initialize type inference service
    - `async _initialize_websocket_service(self)` (line 218): Initialize WebSocket notification service
    - `async _initialize_rate_limiter(self)` (line 240): Initialize rate limiting service
    - `async _initialize_connector_registry(self)` (line 256): Initialize Postgres-backed connector registry.
    - `async _initialize_dataset_registry(self)` (line 268): Initialize Postgres-backed dataset registry.
    - `async _initialize_pipeline_registry(self)` (line 279): Initialize Postgres-backed pipeline registry.
    - `async _initialize_objectify_registry(self)` (line 290): Initialize Postgres-backed objectify registry.
    - `async _initialize_pipeline_executor(self)` (line 301): Initialize pipeline executor (preview/build engine).
    - `async _initialize_google_sheets_service(self)` (line 321): Initialize Google Sheets service (connector library)
    - `async shutdown_bff_services(self)` (line 340): Shutdown BFF-specific services
    - `get_oms_client(self)` (line 423): Get OMS client instance
    - `get_label_mapper(self)` (line 429): Get label mapper instance
    - `get_google_sheets_service(self)` (line 435): Get Google Sheets service instance
    - `get_connector_registry(self)` (line 441): Get connector registry instance
    - `get_dataset_registry(self)` (line 447): Get dataset registry instance
    - `get_pipeline_registry(self)` (line 453): Get pipeline registry instance
    - `get_objectify_registry(self)` (line 459): Get objectify registry instance
    - `get_pipeline_executor(self)` (line 465): Get pipeline executor instance

### `backend/bff/middleware/__init__.py`

### `backend/bff/middleware/auth.py`
- **Functions**
  - `ensure_bff_auth_configured()` (line 31): no docstring
  - `install_bff_auth_middleware(app)` (line 52): no docstring
  - `async enforce_bff_websocket_auth(websocket, token)` (line 91): no docstring

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

### `backend/bff/routers/admin.py`
- **Functions**
  - `async require_admin(request)` (line 52): Minimal admin guard for operational endpoints.
  - `async get_task_manager(container)` (line 141): Get BackgroundTaskManager from container.
  - `async get_redis_service(container)` (line 157): Get RedisService from container.
  - `async get_pipeline_registry()` (line 166): no docstring
  - `async replay_instance_state(request, background_tasks, storage_service, task_manager, redis_service)` (line 174): Replay instance state from event store.
  - `async get_replay_result(task_id, task_manager, redis_service)` (line 232): Get the result of instance state replay.
  - `async get_replay_trace(task_id, command_id, include_audit, audit_limit, include_lineage, lineage_direction, lineage_max_depth, lineage_max_nodes, lineage_max_edges, timeline_limit, task_manager, redis_service, audit_store, lineage_store)` (line 287): Trace a replayed instance state into Audit + Lineage.
  - `async recompute_projection(http_request, request, background_tasks, task_manager, redis_service, elasticsearch_service, audit_store, lineage_store)` (line 463): Recompute (re-materialize) an Elasticsearch projection from the Event Store.
  - `async get_recompute_projection_result(task_id, task_manager, redis_service)` (line 518): Get the result of a projection recompute task.
  - `async _replay_instance_state_task(task_id, request, storage_service, redis_service)` (line 554): Background task to replay instance state.
  - `_normalize_dt(dt)` (line 627): no docstring
  - `_load_projection_mapping(projection)` (line 633): no docstring
  - `async _ensure_es_connected(es)` (line 641): no docstring
  - `async _monitor_replay_task(task_id, task_manager)` (line 646): Monitor the replay task and log completion.
  - `async _monitor_admin_task(task_id, task_manager)` (line 679): Generic monitor for admin background tasks.
  - `async _recompute_projection_task(task_id, request, elasticsearch_service, redis_service, audit_store, lineage_store, requested_by, request_ip)` (line 703): Background task: replay domain events from S3/MinIO Event Store to rebuild an ES projection.
  - `async cleanup_old_replay_results(older_than_hours, redis_service)` (line 1135): Clean up old replay results from Redis.
  - `async get_system_health(task_manager, redis_service)` (line 1198): Get overall system health including background task metrics.
  - `async list_lakefs_credentials(registry)` (line 1248): List configured lakeFS principals (metadata only; secrets are never returned).
  - `async upsert_lakefs_credentials(payload, request, registry)` (line 1257): Upsert lakeFS credentials for a user/service principal (admin-only).
- **Classes**
  - `ReplayInstanceStateRequest` (line 79): Request model for instance state replay.
  - `ReplayInstanceStateResponse` (line 88): Response model for instance state replay.
  - `RecomputeProjectionRequest` (line 96): Request model for projection recompute (Versioning + Recompute).
  - `RecomputeProjectionResponse` (line 122): Response model for projection recompute.
  - `LakeFSCredentialsUpsertRequest` (line 131): Upsert request for lakeFS credentials stored in Postgres (encrypted).

### `backend/bff/routers/ai.py`
- **Functions**
  - `_now_iso()` (line 46): no docstring
  - `_cap_int(value, lo, hi)` (line 50): no docstring
  - `_wants_paths(question)` (line 54): Best-effort intent detection for "why/path" style questions.
  - `async _load_schema_context(db_name, oms, redis_service, cache_ttl_s, max_classes, max_properties_per_class, max_relationships_per_class)` (line 79): Build a minimal, LLM-friendly schema context.
  - `_build_plan_prompts(question, schema_context, mode, branch, limit_cap)` (line 176): no docstring
  - `_build_answer_prompts(question, grounding)` (line 244): no docstring
  - `_validate_and_cap_plan(plan, limit_cap)` (line 280): Enforce server-side caps regardless of what the LLM produced.
  - `_apply_rule_based_overrides(plan, question)` (line 312): Deterministic post-processing to reduce dependence on perfect LLM planning.
  - `async _execute_label_query(db_name, query_dict, lang, mapper, terminus)` (line 325): Execute label query by reusing the same deterministic pipeline as /database/{db_name}/query.
  - `_ground_label_query_result(execution, max_rows)` (line 350): no docstring
  - `_ground_graph_query_result(execution, max_nodes, max_edges)` (line 362): no docstring
  - `async translate_query_plan(db_name, body, request, llm, redis_service, audit_store, oms)` (line 435): Natural language → constrained query plan JSON.
  - `async ai_query(db_name, body, request, llm, redis_service, audit_store, lineage_store, oms, mapper, terminus)` (line 510): End-to-end natural language query:

### `backend/bff/routers/audit.py`
- **Functions**
  - `async list_audit_logs(partition_key, action, status_filter, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset, audit_store)` (line 21): no docstring
  - `async get_chain_head(partition_key, audit_store)` (line 63): no docstring

### `backend/bff/routers/command_status.py`
- **Functions**
  - `async get_command_status(command_id, oms)` (line 26): Proxy OMS: `GET /api/v1/commands/{command_id}/status`.

### `backend/bff/routers/context7.py`
- **Functions**
  - `async search_context7(request, client)` (line 58): Search Context7 knowledge base
  - `async get_entity_context(entity_id, client)` (line 86): Get context information for a specific entity
  - `async add_knowledge(request, client)` (line 113): Add new knowledge to Context7
  - `async create_entity_link(request, client)` (line 147): Create relationship between entities in Context7
  - `async analyze_ontology(request, client)` (line 180): Analyze ontology structure with Context7 AI
  - `async get_ontology_suggestions(db_name, class_id, client)` (line 216): Get AI-powered suggestions for ontology improvements
  - `async check_context7_health(client)` (line 249): Check Context7 connection health
- **Classes**
  - `SearchRequest` (line 26): Context7 search request
  - `KnowledgeRequest` (line 33): Request to add knowledge to Context7
  - `EntityLinkRequest` (line 41): Request to create entity relationship
  - `OntologyAnalysisRequest` (line 49): Request to analyze ontology with Context7

### `backend/bff/routers/data_connector.py`
- **Functions**
  - `async get_google_sheets_service()` (line 51): Import here to avoid circular dependency
  - `async get_connector_registry()` (line 58): Import here to avoid circular dependency
  - `async get_dataset_registry()` (line 65): Import here to avoid circular dependency
  - `async get_pipeline_registry()` (line 72): Import here to avoid circular dependency
  - `_build_google_oauth_client()` (line 79): no docstring
  - `_connector_oauth_enabled(oauth_client)` (line 83): no docstring
  - `_append_query_param(url, key, value)` (line 86): no docstring
  - `async _resolve_google_connection(connector_registry, oauth_client, connection_id)` (line 93): no docstring
  - `async start_google_sheets_oauth(payload, http_request)` (line 150): no docstring
  - `async google_sheets_oauth_callback(request, code, state, connector_registry)` (line 187): no docstring
  - `async list_google_sheets_connections(http_request, connector_registry)` (line 233): no docstring
  - `async delete_google_sheets_connection(connection_id, http_request, connector_registry)` (line 258): no docstring
  - `async list_google_sheets_spreadsheets(http_request, connection_id, query, limit, connector_registry, google_sheets_service)` (line 280): no docstring
  - `async list_google_sheets_worksheets(sheet_id, http_request, connection_id, connector_registry, google_sheets_service)` (line 312): no docstring
  - `async extract_google_sheet_grid(request, http_request, google_sheets_service, connector_registry)` (line 352): no docstring
  - `async preview_google_sheet_for_funnel(request, http_request, limit, google_sheets_service, connector_registry)` (line 415): no docstring
  - `async register_google_sheet(sheet_data, http_request, google_sheets_service, connector_registry, dataset_registry, lineage_store)` (line 464): Register a Google Sheet for data monitoring and automatic import.
  - `async preview_google_sheet(sheet_id, http_request, worksheet_name, limit, google_sheets_service, connector_registry)` (line 678): Preview data from a registered Google Sheet.
  - `async list_registered_sheets(http_request, database_name, connector_registry)` (line 763): List all registered Google Sheets.
  - `async start_pipelining_google_sheet(sheet_id, payload, http_request, google_sheets_service, connector_registry, pipeline_registry, dataset_registry, lineage_store)` (line 835): no docstring
  - `async unregister_google_sheet(sheet_id, http_request, connector_registry)` (line 1089): Unregister a Google Sheet from monitoring.

### `backend/bff/routers/database.py`
- **Functions**
  - `_is_dev_mode()` (line 38): no docstring
  - `async _get_expected_seq_for_database(db_name)` (line 42): no docstring
  - `_coerce_db_entry(entry)` (line 66): no docstring
  - `_enrich_db_entry(entry, actor_id, actor_name)` (line 80): no docstring
  - `async _fetch_database_access(db_names)` (line 109): no docstring
  - `async _upsert_database_owner(db_name, principal_type, principal_id, principal_name)` (line 142): no docstring
  - `async _ensure_database_access_table(conn)` (line 192): no docstring
  - `_resolve_actor(request)` (line 227): no docstring
  - `_enrich_db_entry(entry, actor_type, actor_id, actor_name, access_rows)` (line 239): no docstring
  - `async list_databases(request, oms)` (line 286): 데이터베이스 목록 조회
  - `async create_database(request, http_request, oms)` (line 337): 데이터베이스 생성
  - `async delete_database(db_name, http_request, expected_seq, oms)` (line 444): 데이터베이스 삭제
  - `async get_branch_info(db_name, branch_name, oms)` (line 540): 브랜치 정보 조회 (프론트엔드용 BFF 래핑)
  - `async delete_branch(db_name, branch_name, force, oms)` (line 563): 브랜치 삭제 (프론트엔드용 BFF 래핑)
  - `async get_database(db_name, oms)` (line 602): 데이터베이스 정보 조회
  - `async get_database_expected_seq(db_name)` (line 634): Resolve the current `expected_seq` for database (aggregate) operations.
  - `async list_classes(db_name, type, limit, oms)` (line 660): 데이터베이스의 클래스 목록 조회
  - `async create_class(db_name, class_data, oms)` (line 699): 데이터베이스에 새 클래스 생성
  - `async get_class(db_name, class_id, request, oms)` (line 742): 특정 클래스 조회
  - `async list_branches(db_name, oms)` (line 770): 브랜치 목록 조회
  - `async create_branch(db_name, branch_data, oms)` (line 805): 새 브랜치 생성
  - `async get_versions(db_name, oms)` (line 861): 버전 히스토리 조회

### `backend/bff/routers/graph.py`
- **Functions**
  - `async get_graph_federation_service()` (line 46): Get or create GraphFederationServiceWOQL instance - REAL WOQL solution
  - `async execute_graph_query(db_name, query, request, lineage_store, graph_service, branch)` (line 84): Execute multi-hop graph query with ES federation
  - `async execute_simple_graph_query(db_name, query, request, graph_service, branch)` (line 321): Execute simple single-class graph query
  - `async execute_multi_hop_query(db_name, query, request, graph_service, branch)` (line 377): Execute multi-hop graph query with Federation
  - `async find_relationship_paths(db_name, source_class, target_class, max_depth, graph_service, branch)` (line 447): Find all possible relationship paths between two classes
  - `async graph_service_health(graph_service)` (line 529): Check health of graph federation service
  - `async register_projection(db_name, request, graph_service)` (line 595): 🚧 (WIP) 빈번한 멀티홉 쿼리를 “프로젝션(=materialized view)”으로 등록
  - `async query_projection(db_name, request, graph_service)` (line 649): 🚧 (WIP) 프로젝션 뷰 조회 (캐시된 데이터)
  - `async list_projections(db_name, graph_service)` (line 699): 🚧 (WIP) 등록된 프로젝션 목록 조회
- **Classes**
  - `ProjectionRegistrationRequest` (line 573): 프로젝션 등록 요청
  - `ProjectionQueryRequest` (line 582): 프로젝션 조회 요청

### `backend/bff/routers/health.py`
- **Functions**
  - `async root()` (line 22): 루트 엔드포인트
  - `async health_check(oms_client)` (line 36): 헬스체크 엔드포인트

### `backend/bff/routers/instance_async.py`
- **Functions**
  - `_raise_httpx_as_http_exception(exc)` (line 39): no docstring
  - `async convert_labels_to_ids(data, db_name, class_id, label_mapper, lang)` (line 79): Label 기반 데이터를 ID 기반으로 변환
  - `async create_instance_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 147): 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
  - `async update_instance_async(db_name, class_label, instance_id, request, http_request, expected_seq, branch, oms_client, label_mapper, user_id)` (line 234): 인스턴스 수정 명령을 비동기로 처리 (Label 기반)
  - `async delete_instance_async(db_name, class_label, instance_id, http_request, branch, expected_seq, oms_client, label_mapper, user_id)` (line 318): 인스턴스 삭제 명령을 비동기로 처리 (Label 기반)
  - `async bulk_create_instances_async(db_name, class_label, request, http_request, branch, oms_client, label_mapper, user_id)` (line 381): 대량 인스턴스 생성 명령을 비동기로 처리 (Label 기반)
- **Classes**
  - `InstanceCreateRequest` (line 61): 인스턴스 생성 요청 (Label 기반)
  - `InstanceUpdateRequest` (line 67): 인스턴스 수정 요청 (Label 기반)
  - `BulkInstanceCreateRequest` (line 73): 대량 인스턴스 생성 요청 (Label 기반)

### `backend/bff/routers/instances.py`
- **Functions**
  - `_normalize_es_search_result(result)` (line 25): Normalize Elasticsearch search results across return shapes.
  - `async get_class_instances(db_name, class_id, limit, offset, search, elasticsearch_service, oms_client)` (line 70): 특정 클래스의 인스턴스 목록 조회 (Elasticsearch 사용)
  - `async get_class_sample_values(db_name, class_id, property_name, limit, oms_client)` (line 256): 특정 클래스/속성의 샘플 값 조회
  - `async get_instance(db_name, class_id, instance_id, elasticsearch_service, oms_client)` (line 344): 개별 인스턴스 조회 (Elasticsearch 우선, TerminusDB fallback)

### `backend/bff/routers/lineage.py`
- **Functions**
  - `_parse_artifact_node_id(node_id)` (line 32): Parse artifact node id: artifact:<kind>:<...>
  - `_suggest_remediation_actions(artifacts)` (line 50): Recommend safe operational actions.
  - `async get_lineage_graph(root, db_name, direction, max_depth, max_nodes, max_edges, lineage_store)` (line 103): no docstring
  - `async get_lineage_impact(root, db_name, direction, max_depth, artifact_kind, max_nodes, max_edges, lineage_store)` (line 135): no docstring
  - `async get_lineage_metrics(db_name, window_minutes, lineage_store, audit_store)` (line 196): Operational lineage metrics.

### `backend/bff/routers/mapping.py`
- **Functions**
  - `async export_mappings(db_name, mapper)` (line 43): 레이블 매핑 내보내기
  - `async _validate_file_upload(file)` (line 66): Validate uploaded file size, type, and extension.
  - `async _read_and_parse_file(file)` (line 95): Read file content and parse JSON.
  - `_sanitize_and_validate_schema(raw_mappings, db_name)` (line 151): Sanitize input and validate schema.
  - `_validate_business_logic(mapping_request, sanitized_mappings, db_name)` (line 182): Validate business logic and data consistency.
  - `async _perform_validation(mapping_request, mapper, db_name, oms_client)` (line 228): 실제 매핑 검증 수행
  - `async _perform_mapping_import(mapper, validated_mappings, db_name)` (line 294): Perform the actual mapping import with backup and rollback.
  - `async import_mappings(db_name, file, mapper, oms_client)` (line 326): Import label mappings from JSON file with enhanced security validation.
  - `async validate_mappings(db_name, file, mapper, oms_client)` (line 410): 매핑 검증 전용 엔드포인트
  - `async get_mappings_summary(db_name, mapper)` (line 569): 레이블 매핑 요약 조회
  - `async clear_mappings(db_name, mapper)` (line 623): 레이블 매핑 초기화
- **Classes**
  - `MappingImportPayload` (line 27): Label mapping bundle file schema.

### `backend/bff/routers/merge_conflict.py`
- **Functions**
  - `async _await_if_needed(value)` (line 37): no docstring
  - `async _raise_for_status(response)` (line 43): no docstring
  - `async _response_json(response)` (line 47): no docstring
  - `async simulate_merge(db_name, request, oms_client)` (line 52): 병합 시뮬레이션 - 실제 병합 없이 충돌 감지
  - `async resolve_merge_conflicts(db_name, request, oms_client)` (line 216): 수동 병합 충돌 해결
  - `async _detect_merge_conflicts(source_changes, target_changes, common_ancestor, db_name, oms_client)` (line 303): 3-way 병합 충돌 감지 엔진 (공통 조상 기반)
  - `async _convert_resolution_to_terminus_format(resolution)` (line 405): Foundry 해결책을 TerminusDB 형식으로 변환

### `backend/bff/routers/objectify.py`
- **Functions**
  - `async get_dataset_registry()` (line 30): no docstring
  - `async get_objectify_registry()` (line 36): no docstring
  - `async get_objectify_job_queue(objectify_registry)` (line 42): no docstring
  - `async get_pipeline_registry()` (line 48): no docstring
  - `_match_output_name(output, name)` (line 85): no docstring
  - `_compute_schema_hash_from_sample(sample_json)` (line 98): no docstring
  - `async create_mapping_spec(body, request, dataset_registry, objectify_registry)` (line 108): no docstring
  - `async list_mapping_specs(dataset_id, include_inactive, objectify_registry)` (line 178): no docstring
  - `async run_objectify(dataset_id, body, request, dataset_registry, objectify_registry, job_queue, pipeline_registry)` (line 198): no docstring
- **Classes**
  - `MappingSpecField` (line 54): no docstring
  - `CreateMappingSpecRequest` (line 59): no docstring
  - `TriggerObjectifyRequest` (line 74): no docstring

### `backend/bff/routers/ontology.py`
- **Functions**
  - `async create_ontology(db_name, ontology, branch, mapper, terminus, jsonld_conv)` (line 162): 온톨로지 생성
  - `async list_ontologies(db_name, request, branch, class_type, limit, offset, mapper, terminus)` (line 339): 온톨로지 목록 조회
  - `async get_ontology(db_name, class_label, request, branch, mapper, terminus)` (line 406): 온톨로지 조회
  - `async validate_ontology_create_bff(db_name, ontology, request, branch, oms_client)` (line 507): 온톨로지 생성 검증 (no write) - OMS proxy.
  - `async validate_ontology_update_bff(db_name, class_label, ontology, request, branch, mapper, oms_client)` (line 531): 온톨로지 업데이트 검증 (no write) - OMS proxy.
  - `async update_ontology(db_name, class_label, ontology, request, expected_seq, branch, mapper, terminus)` (line 573): 온톨로지 수정
  - `async delete_ontology(db_name, class_label, request, expected_seq, branch, mapper, terminus)` (line 654): 온톨로지 삭제
  - `async get_ontology_schema(db_name, class_id, request, format, branch, mapper, terminus, jsonld_conv)` (line 725): 온톨로지 스키마 조회
  - `async create_ontology_with_relationship_validation(db_name, ontology, request, branch, auto_generate_inverse, validate_relationships, check_circular_references, mapper, terminus)` (line 809): 🔥 고급 관계 검증을 포함한 온톨로지 생성 (BFF 레이어 - 리팩토링됨)
  - `async validate_ontology_relationships_bff(db_name, ontology, request, branch, mapper, terminus)` (line 960): 🔥 온톨로지 관계 검증 (BFF 레이어 - 리팩토링됨)
  - `async check_circular_references_bff(db_name, request, ontology, branch, mapper, terminus)` (line 1010): 🔥 순환 참조 탐지 (BFF 레이어 - 리팩토링됨)
  - `async analyze_relationship_network_bff(db_name, request, terminus, mapper)` (line 1054): 🔥 관계 네트워크 분석 (BFF 레이어)
  - `async find_relationship_paths_bff(request, db_name, start_entity, end_entity, max_depth, path_type, terminus, mapper)` (line 1155): 🔥 관계 경로 탐색 (BFF 레이어)
  - `async suggest_schema_from_data(db_name, request, terminus)` (line 1267): 🔥 데이터에서 스키마 자동 제안
  - `async suggest_mappings(db_name, request)` (line 1341): 두 스키마 간의 매핑을 자동으로 제안
  - `_normalize_mapping_type(type_value)` (line 1430): no docstring
  - `_build_source_schema_from_preview(preview)` (line 1458): no docstring
  - `_build_sample_data_from_preview(preview)` (line 1495): no docstring
  - `_build_target_schema_from_ontology(ontology, include_relationships)` (line 1508): no docstring
  - `_normalize_target_schema_for_mapping(target_schema, include_relationships)` (line 1560): Normalize a client-provided target schema to the shape expected by MappingSuggestionService.
  - `async suggest_mappings_from_google_sheets(db_name, request)` (line 1597): 🔥 Google Sheets → 온톨로지 클래스 매핑 자동 제안
  - `async suggest_mappings_from_excel(db_name, target_class_id, file, target_schema_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, include_relationships, enable_semantic_hints, max_tables, max_rows, max_cols)` (line 1726): 🔥 Excel 업로드 → 온톨로지 클래스 매핑 자동 제안
  - `_extract_target_field_types(ontology)` (line 1907): no docstring
  - `_normalize_import_target_type(type_value)` (line 1929): Normalize user-provided target field type for import.
  - `_extract_target_field_types_from_import_schema(target_schema)` (line 1940): no docstring
  - `async dry_run_import_from_google_sheets(db_name, request)` (line 1953): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_google_sheets(db_name, request, oms_client)` (line 2064): Google Sheets → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
  - `async dry_run_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, dry_run_rows, max_import_rows, options_json)` (line 2263): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환/검증 (dry-run)
  - `async commit_import_from_excel(db_name, file, target_class_id, target_schema_json, mappings_json, sheet_name, table_id, table_top, table_left, table_bottom, table_right, max_tables, max_rows, max_cols, allow_partial, max_import_rows, batch_size, return_instances, max_return_instances, options_json, oms_client)` (line 2434): Excel 업로드 → (구조 분석 + 테이블 선택) → 매핑 적용 → 타입 변환 → OMS bulk-create로 WRITE 파이프라인 시작
  - `async suggest_schema_from_google_sheets(db_name, request, terminus)` (line 2693): 🔥 Google Sheets에서 스키마 자동 제안
  - `async suggest_schema_from_excel(db_name, file, sheet_name, class_name, table_id, table_top, table_left, table_bottom, table_right, include_complex_types, max_tables, max_rows, max_cols)` (line 2766): 🔥 Excel 업로드에서 스키마 자동 제안
  - `async save_mapping_metadata(db_name, class_id, metadata, oms, mapper)` (line 2877): 매핑 메타데이터를 온톨로지 클래스에 저장
- **Classes**
  - `SchemaFromDataRequest` (line 40): Request model for schema suggestion from data
  - `SchemaFromGoogleSheetsRequest` (line 49): Request model for schema suggestion from Google Sheets
  - `MappingSuggestionRequest` (line 61): Request model for mapping suggestions between schemas
  - `MappingFromGoogleSheetsRequest` (line 70): Request model for mapping suggestions from Google Sheets → existing ontology class
  - `ImportFieldMapping` (line 89): Field mapping for import (source column → target property)
  - `ImportTargetField` (line 96): Target field definition for import (name + type).
  - `ImportFromGoogleSheetsRequest` (line 103): Request model for dry-run/commit import from Google Sheets

### `backend/bff/routers/ontology_extensions.py`
- **Functions**
  - `_extract_httpx_detail(exc)` (line 22): no docstring
  - `async _list_resources(oms_client, db_name, resource_type, branch, limit, offset)` (line 64): no docstring
  - `async _create_resource(oms_client, db_name, resource_type, payload, branch, expected_head_commit)` (line 82): no docstring
  - `async _update_resource(oms_client, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 101): no docstring
  - `_resource_routes(resource_type)` (line 122): no docstring
  - `async list_ontology_branches(db_name, oms_client)` (line 290): no docstring
  - `async create_ontology_branch(db_name, request, oms_client)` (line 306): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, oms_client)` (line 324): no docstring
  - `async create_ontology_proposal(db_name, request, oms_client)` (line 344): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, oms_client)` (line 362): no docstring
  - `async deploy_ontology(db_name, request, oms_client)` (line 381): no docstring
  - `async ontology_health(db_name, branch, oms_client)` (line 399): no docstring
- **Classes**
  - `OntologyResourceRequest` (line 33): no docstring
  - `OntologyProposalRequest` (line 43): no docstring
  - `OntologyApproveRequest` (line 51): no docstring
  - `OntologyDeployRequest` (line 56): no docstring

### `backend/bff/routers/ops.py`
- **Functions**
  - `async get_dataset_registry()` (line 17): no docstring
  - `async get_objectify_registry()` (line 23): no docstring
  - `async ops_status(dataset_registry, objectify_registry)` (line 30): no docstring

### `backend/bff/routers/pipeline.py`
- **Functions**
  - `_resolve_pipeline_protected_branches()` (line 69): no docstring
  - `_pipeline_requires_proposal(branch)` (line 74): no docstring
  - `_normalize_mapping_spec_ids(raw)` (line 81): no docstring
  - `async _build_proposal_bundle(pipeline, build_job_id, mapping_spec_ids, pipeline_registry, dataset_registry, objectify_registry)` (line 108): no docstring
  - `async get_dataset_registry()` (line 271): no docstring
  - `async get_pipeline_registry()` (line 277): no docstring
  - `async get_pipeline_job_queue()` (line 285): no docstring
  - `async get_pipeline_executor()` (line 289): no docstring
  - `async get_objectify_registry()` (line 295): no docstring
  - `async get_objectify_job_queue(objectify_registry)` (line 301): no docstring
  - `async _ensure_ingest_transaction(dataset_registry, ingest_request_id)` (line 307): no docstring
  - `async _maybe_enqueue_objectify_job(dataset, version, objectify_registry, job_queue, actor_user_id)` (line 322): no docstring
  - `_stable_definition_hash(definition_json)` (line 385): no docstring
  - `_resolve_definition_commit_id(definition_json, latest_version, definition_hash)` (line 393): no docstring
  - `async _acquire_pipeline_publish_lock(pipeline_id, branch, job_id)` (line 408): no docstring
  - `async _release_pipeline_publish_lock(redis_service, lock_key, token)` (line 439): no docstring
  - `_normalize_schema_column_type(value)` (line 452): no docstring
  - `_coerce_schema_columns(raw)` (line 456): no docstring
  - `_detect_breaking_schema_changes(previous_schema, next_columns)` (line 474): no docstring
  - `_normalize_dependencies_payload(raw)` (line 494): no docstring
  - `async _validate_dependency_targets(pipeline_registry, db_name, pipeline_id, dependencies)` (line 520): no docstring
  - `_format_dependencies_for_api(dependencies)` (line 543): no docstring
  - `_resolve_principal(request)` (line 556): no docstring
  - `_actor_label(principal_type, principal_id)` (line 582): no docstring
  - `async _ensure_pipeline_permission(pipeline_registry, pipeline_id, request, required_role)` (line 588): no docstring
  - `async _log_pipeline_audit(audit_store, request, action, status, pipeline_id, db_name, metadata, error)` (line 623): no docstring
  - `_normalize_location(location)` (line 649): no docstring
  - `_default_dataset_name(filename)` (line 661): no docstring
  - `_extract_node_ids(definition_json)` (line 670): no docstring
  - `_extract_edge_ids(definition_json)` (line 686): no docstring
  - `_definition_diff(previous, current)` (line 707): no docstring
  - `_normalize_table_bbox(table_top, table_left, table_bottom, table_right)` (line 725): no docstring
  - `_normalize_inferred_type(type_value)` (line 748): no docstring
  - `_build_schema_columns(columns, inferred_schema)` (line 772): no docstring
  - `_columns_from_schema(schema_columns)` (line 796): no docstring
  - `_rows_from_preview(columns, sample_rows)` (line 804): no docstring
  - `_detect_csv_delimiter(sample)` (line 816): no docstring
  - `_parse_csv_file(file_obj, delimiter, has_header, preview_limit)` (line 826): no docstring
  - `_parse_csv_content(content, delimiter, has_header, preview_limit)` (line 908): no docstring
  - `_require_idempotency_key(request)` (line 940): no docstring
  - `_build_ingest_request_fingerprint(payload)` (line 953): no docstring
  - `_ingest_staging_prefix(prefix, ingest_request_id)` (line 961): no docstring
  - `_sanitize_s3_metadata(metadata)` (line 966): no docstring
  - `_resolve_lakefs_raw_repository()` (line 982): no docstring
  - `async _ensure_lakefs_branch_exists(lakefs_client, repository, branch, source_branch)` (line 987): no docstring
  - `_extract_lakefs_ref_from_artifact_key(artifact_key)` (line 1008): no docstring
  - `_dataset_artifact_prefix(db_name, dataset_id, dataset_name)` (line 1019): no docstring
  - `async list_pipelines(db_name, branch, pipeline_registry, request)` (line 1026): no docstring
  - `async list_pipeline_proposals(db_name, branch, status_filter, pipeline_registry, request)` (line 1070): no docstring
  - `async submit_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, dataset_registry, objectify_registry, request)` (line 1115): no docstring
  - `async approve_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 1191): no docstring
  - `async reject_pipeline_proposal(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 1276): no docstring
  - `async list_datasets(db_name, branch, dataset_registry)` (line 1326): no docstring
  - `async list_pipeline_branches(db_name, pipeline_registry)` (line 1347): no docstring
  - `async archive_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 1367): no docstring
  - `async restore_pipeline_branch(branch, db_name, audit_store, pipeline_registry, request)` (line 1409): no docstring
  - `async list_pipeline_runs(pipeline_id, limit, pipeline_registry, request)` (line 1449): no docstring
  - `async list_pipeline_artifacts(pipeline_id, mode, limit, pipeline_registry, request)` (line 1476): no docstring
  - `async get_pipeline_artifact(pipeline_id, artifact_id, pipeline_registry, request)` (line 1506): no docstring
  - `async create_pipeline_branch(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 1531): no docstring
  - `async get_pipeline(pipeline_id, pipeline_registry, branch, preview_node_id, request)` (line 1597): no docstring
  - `async get_pipeline_readiness(pipeline_id, branch, pipeline_registry, dataset_registry, request)` (line 1651): no docstring
  - `async create_pipeline(payload, audit_store, pipeline_registry, request)` (line 1820): no docstring
  - `async update_pipeline(pipeline_id, payload, audit_store, pipeline_registry, request)` (line 1995): no docstring
  - `async preview_pipeline(pipeline_id, payload, audit_store, pipeline_registry, pipeline_job_queue, request)` (line 2191): no docstring
  - `async build_pipeline(pipeline_id, payload, audit_store, pipeline_registry, pipeline_job_queue, oms_client, request)` (line 2435): no docstring
  - `async deploy_pipeline(pipeline_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, oms_client, lineage_store, audit_store)` (line 2617): no docstring
  - `async create_dataset(payload, dataset_registry)` (line 3532): no docstring
  - `async create_dataset_version(dataset_id, payload, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue)` (line 3585): no docstring
  - `async upload_excel_dataset(db_name, branch, file, dataset_name, description, sheet_name, table_id, table_top, table_left, table_bottom, table_right, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 3866): Excel 업로드 → preview/스키마 추론 → dataset registry 저장 + artifact 저장
  - `async upload_csv_dataset(db_name, branch, file, dataset_name, description, delimiter, has_header, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 4212): CSV 업로드 → preview/타입 추론 → dataset registry 저장 + artifact 저장
  - `async upload_media_dataset(db_name, branch, files, dataset_name, description, request, pipeline_registry, dataset_registry, objectify_registry, objectify_job_queue, lineage_store)` (line 4582): Media upload → store raw files to S3/MinIO → register a "media" dataset version.

### `backend/bff/routers/query.py`
- **Functions**
  - `async execute_query(db_name, query, request, mapper, terminus)` (line 26): 온톨로지 쿼리 실행
  - `async execute_raw_query(db_name, query, terminus)` (line 87): 원시 쿼리 실행 (제한적 접근)
  - `async query_builder_info()` (line 129): 쿼리 빌더 정보

### `backend/bff/routers/summary.py`
- **Functions**
  - `async get_summary(db, branch, oms, redis_service, es_service)` (line 26): Summarize context + cross-service health for UI.

### `backend/bff/routers/tasks.py`
- **Functions**
  - `async get_task_manager(container)` (line 69): Get BackgroundTaskManager from container.
  - `async get_task_status(task_id, task_manager)` (line 94): Get current status of a background task.
  - `async list_tasks(status, task_type, limit, task_manager)` (line 127): List background tasks with optional filtering.
  - `async cancel_task(task_id, task_manager)` (line 168): Cancel a running background task.
  - `async get_task_metrics(task_manager)` (line 201): Get aggregated metrics for all background tasks.
  - `async retry_task(task_id, task_manager)` (line 219): Retry a failed task.
  - `async get_task_result(task_id, task_manager)` (line 256): Get the result of a completed task.
- **Classes**
  - `TaskStatusResponse` (line 42): Task status response model.
  - `TaskListResponse` (line 56): Task list response model.
  - `TaskMetricsResponse` (line 62): Task metrics response model.

### `backend/bff/routers/websocket.py`
- **Functions**
  - `get_ws_manager()` (line 30): WebSocket 연결 관리자 의존성
  - `async websocket_command_updates(websocket, command_id, client_id, user_id, token, manager)` (line 36): 특정 Command의 실시간 상태 업데이트 구독
  - `async websocket_user_commands(websocket, user_id, client_id, token, manager)` (line 116): 사용자의 모든 Command 실시간 업데이트 구독
  - `async handle_client_message(websocket, client_id, message, manager)` (line 188): 클라이언트로부터 받은 메시지 처리

### `backend/bff/schemas/label_mapping_schema.py`
- **Functions**
  - `get_label_mapping_schema()` (line 9): LabelMapping 클래스 스키마 반환
  - `get_label_mapping_properties()` (line 28): LabelMapping 클래스의 속성들 반환
  - `get_label_mapping_ontology()` (line 120): LabelMapping 전체 온톨로지 반환

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

### `backend/bff/services/funnel_client.py`
- **Classes**
  - `FunnelClient` (line 19): Funnel HTTP 클라이언트
    - `__init__(self, base_url)` (line 22): no docstring
    - `_resolve_excel_timeout_seconds()` (line 39): no docstring
    - `async close(self)` (line 48): 클라이언트 연결 종료
    - `async check_health(self)` (line 52): Funnel 서비스 상태 확인
    - `async analyze_dataset(self, request_data)` (line 63): 데이터셋 타입 분석
    - `async suggest_schema(self, analysis_results, class_name)` (line 81): 분석 결과를 기반으로 OMS 스키마 제안
    - `async preview_google_sheets(self, sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types)` (line 108): Google Sheets 데이터 미리보기와 타입 추론
    - `async analyze_and_suggest_schema(self, data, columns, class_name, sample_size, include_complex_types)` (line 151): 데이터 분석과 스키마 제안을 한 번에 실행
    - `async google_sheets_to_schema(self, sheet_url, worksheet_name, class_name, api_key, connection_id, table_id, table_bbox)` (line 191): Google Sheets에서 직접 스키마 생성
    - `async google_sheets_to_structure_preview(self, sheet_url, worksheet_name, api_key, connection_id, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 283): Google Sheets URL → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_google_sheets_structure(self, sheet_url, worksheet_name, api_key, connection_id, include_complex_types, max_tables, max_rows, max_cols, trim_trailing_empty, options)` (line 327): Analyze sheet structure via Funnel (Google Sheets URL → grid/merged_cells → structure analysis).
    - `async excel_to_structure_preview(self, xlsx_bytes, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 363): Excel bytes → (grid/merged_cells) → structure analysis → selected table preview.
    - `async excel_to_structure_preview_stream(self, fileobj, filename, sheet_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 404): Excel stream → (grid/merged_cells) → structure analysis → selected table preview.
    - `async analyze_excel_structure(self, xlsx_bytes, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 441): Analyze sheet structure via Funnel (Excel bytes → grid/merged_cells → structure analysis).
    - `async analyze_excel_structure_stream(self, fileobj, filename, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options)` (line 486): Analyze sheet structure via Funnel (streaming Excel upload).
    - `_select_primary_table(structure)` (line 568): Choose a single "primary" table for schema suggestion.
    - `_select_requested_table(cls, structure, table_id, table_bbox)` (line 601): Select a table from structure analysis output.
    - `_normalize_bbox_dict(bbox)` (line 649): no docstring
    - `_structure_table_to_preview(structure, table, sheet_url, worksheet_name)` (line 659): no docstring
    - `_structure_table_to_excel_preview(structure, table, file_name, sheet_name)` (line 711): no docstring
    - `async excel_to_schema(self, xlsx_bytes, filename, sheet_name, class_name, table_id, table_bbox, include_complex_types, max_tables, max_rows, max_cols, options)` (line 756): Excel 업로드에서 직접 스키마 생성 (구조 분석 기반).
    - `async __aenter__(self)` (line 815): no docstring
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 818): no docstring

### `backend/bff/services/funnel_type_inference_adapter.py`
- **Classes**
  - `FunnelHTTPTypeInferenceAdapter` (line 18): HTTP 기반 Funnel 마이크로서비스를 TypeInferenceInterface로 adapting하는 클래스.
    - `__init__(self, funnel_client)` (line 26): no docstring
    - `async infer_column_type(self, column_data, column_name, include_complex_types, context_columns, metadata)` (line 29): 단일 컬럼 데이터의 타입을 추론합니다.
    - `async analyze_dataset(self, data, columns, sample_size, include_complex_types, metadata)` (line 75): 전체 데이터셋을 분석하여 모든 컬럼의 타입을 추론합니다.
    - `async infer_type_with_confidence(self, values, check_complex)` (line 92): 값 리스트에서 타입을 추론하고 신뢰도를 반환합니다.
    - `async infer_single_value_type(self, value, context)` (line 104): 단일 값의 타입을 추론합니다.
    - `async _analyze_single_column(self, data, headers, include_complex_types)` (line 125): 단일 컬럼 분석을 위한 비동기 헬퍼 메서드
    - `async _analyze_dataset_async(self, data, headers, include_complex_types, sample_size)` (line 149): 데이터셋 분석을 위한 비동기 헬퍼 메서드
    - `_convert_funnel_column_result(self, funnel_result)` (line 177): Funnel 서비스 응답을 Interface 형식으로 변환
    - `async close(self)` (line 193): 클라이언트 연결 종료

### `backend/bff/services/mapping_suggestion_service.py`
- **Classes**
  - `MappingCandidate` (line 21): 매핑 후보
  - `MappingSuggestion` (line 31): 매핑 제안 결과
  - `MappingSuggestionService` (line 39): 스키마 간 매핑을 자동으로 제안하는 서비스
    - `__init__(self, config)` (line 53): no docstring
    - `suggest_mappings(self, source_schema, target_schema, sample_data, target_sample_data)` (line 120): 소스 스키마를 타겟 스키마에 매핑하는 제안 생성
    - `_field_name_candidates(field)` (line 233): Return candidate strings for name matching.
    - `_check_exact_match(self, source_field, target_field)` (line 275): 정확한 이름 매칭 검사
    - `_check_token_match(self, source_field, target_field)` (line 302): 토큰 기반 이름 매칭 검사
    - `_check_fuzzy_match(self, source_field, target_field)` (line 349): 퍼지 이름 매칭 검사
    - `_check_semantic_match(self, source_field, target_field)` (line 395): 의미론적 매칭 검사 (옵션: 별칭/동의어 그룹 기반)
    - `_check_type_match(self, source_field, target_field)` (line 435): 타입 기반 매칭 검사
    - `_check_pattern_match(self, source_field, target_field, sample_data)` (line 486): 값 패턴 기반 매칭 검사
    - `_normalize_field_name(self, name)` (line 548): 필드 이름 정규화
    - `_is_abbreviation(self, short, long)` (line 560): 약어 관계 검사
    - `_analyze_value_patterns(self, values)` (line 580): 값 패턴 분석
    - `_resolve_conflicts(self, candidates)` (line 623): 매핑 충돌 해결 (하나의 타겟에 여러 소스가 매핑되는 경우)
    - `_levenshtein_similarity(self, s1, s2)` (line 649): Calculate Levenshtein similarity between two strings
    - `_tokenize_field_name(self, name)` (line 689): 필드 이름을 토큰으로 분리 (스톱워드 제거)
    - `_token_similarity(self, source_name, target_name)` (line 706): 토큰 기반 이름 유사도 계산
    - `_normalize_text_advanced(self, text)` (line 732): 고급 텍스트 정규화 (NFKC, 공백, 특수문자)
    - `_distribution_similarity(self, source_values, target_values, source_type, target_type)` (line 749): 값 분포 유사도 계산
    - `_numeric_distribution_similarity(self, source_values, target_values)` (line 789): 수치형 분포 유사도 (KS-test 기반)
    - `_categorical_distribution_similarity(self, source_values, target_values)` (line 840): 범주형 분포 유사도 (Jaccard/Overlap)
    - `_string_distribution_similarity(self, source_values, target_values)` (line 863): 문자열 분포 유사도 (n-gram 기반)
    - `_temporal_distribution_similarity(self, source_values, target_values)` (line 909): 시간형 분포 유사도
    - `_is_numeric(self, value)` (line 926): Check if string represents a number
    - `_is_date_pattern(self, value)` (line 934): Check if string matches common date patterns
    - `_detect_date_format(self, date_str)` (line 949): Detect date format pattern
    - `_check_distribution_match(self, source_field, target_field, sample_data, target_sample_data)` (line 961): 값 분포 기반 매칭 검사

### `backend/bff/services/oms_client.py`
- **Classes**
  - `OMSClient` (line 24): OMS HTTP 클라이언트
    - `__init__(self, base_url)` (line 27): no docstring
    - `_get_auth_token()` (line 49): no docstring
    - `async close(self)` (line 56): 클라이언트 연결 종료
    - `async get(self, path, **kwargs)` (line 64): Low-level GET helper (returns JSON dict).
    - `async post(self, path, **kwargs)` (line 72): Low-level POST helper (returns JSON dict).
    - `async put(self, path, **kwargs)` (line 80): Low-level PUT helper (returns JSON dict).
    - `async delete(self, path, **kwargs)` (line 88): Low-level DELETE helper (returns JSON dict when available).
    - `async check_health(self)` (line 96): OMS 서비스 상태 확인
    - `async list_databases(self)` (line 108): 데이터베이스 목록 조회
    - `async create_database(self, db_name, description)` (line 118): 데이터베이스 생성
    - `async delete_database(self, db_name, expected_seq)` (line 141): 데이터베이스 삭제
    - `async get_database(self, db_name)` (line 154): 데이터베이스 정보 조회
    - `async create_ontology(self, db_name, ontology_data, branch, headers)` (line 164): 온톨로지 생성
    - `async validate_ontology_create(self, db_name, ontology_data, branch)` (line 193): 온톨로지 생성 검증 (no write).
    - `async get_ontology(self, db_name, class_id, branch)` (line 213): 온톨로지 조회
    - `async list_ontologies(self, db_name, branch)` (line 226): 온톨로지 목록 조회
    - `async list_branches(self, db_name)` (line 239): 브랜치 목록 조회
    - `async list_ontology_resources(self, db_name, resource_type, branch, limit, offset)` (line 249): 온톨로지 리소스 목록 조회
    - `async get_ontology_resource(self, db_name, resource_type, resource_id, branch)` (line 277): 단일 온톨로지 리소스 조회
    - `async create_ontology_resource(self, db_name, resource_type, payload, branch, expected_head_commit)` (line 297): 온톨로지 리소스 생성
    - `async update_ontology_resource(self, db_name, resource_type, resource_id, payload, branch, expected_head_commit)` (line 322): 온톨로지 리소스 업데이트
    - `async delete_ontology_resource(self, db_name, resource_type, resource_id, branch, expected_head_commit)` (line 348): 온톨로지 리소스 삭제
    - `async list_ontology_branches(self, db_name)` (line 372): 온톨로지 브랜치 목록 조회
    - `async create_ontology_branch(self, db_name, payload)` (line 382): 온톨로지 브랜치 생성
    - `async list_ontology_proposals(self, db_name, status_filter, limit)` (line 395): 온톨로지 제안 목록 조회
    - `async create_ontology_proposal(self, db_name, payload)` (line 413): 온톨로지 제안 생성
    - `async approve_ontology_proposal(self, db_name, proposal_id, payload)` (line 426): 온톨로지 제안 승인
    - `async deploy_ontology(self, db_name, payload)` (line 441): 온톨로지 배포(승격)
    - `async get_ontology_health(self, db_name, branch)` (line 454): 온톨로지 헬스 체크
    - `async create_branch(self, db_name, branch_data)` (line 467): 브랜치 생성
    - `async get_version_history(self, db_name)` (line 477): 버전 히스토리 조회
    - `async get_version_head(self, db_name, branch)` (line 487): 브랜치 head 커밋 ID 조회 (Foundry-style deploy gate).
    - `async update_ontology(self, db_name, class_id, update_data, expected_seq, branch, headers)` (line 500): 온톨로지 업데이트
    - `async validate_ontology_update(self, db_name, class_id, update_data, branch)` (line 524): 온톨로지 업데이트 검증 (no write).
    - `async delete_ontology(self, db_name, class_id, expected_seq, branch, headers)` (line 545): 온톨로지 삭제
    - `async query_ontologies(self, db_name, query)` (line 572): 온톨로지 쿼리
    - `async database_exists(self, db_name)` (line 582): 데이터베이스 존재 여부 확인
    - `async commit_database_change(self, db_name, message, author)` (line 593): 데이터베이스 변경사항 자동 커밋
    - `async commit_system_change(self, message, author, operation, target)` (line 620): 시스템 레벨 변경사항 커밋 (데이터베이스 생성/삭제 등)
    - `async get_class_metadata(self, db_name, class_id)` (line 653): 클래스의 메타데이터 가져오기
    - `async update_class_metadata(self, db_name, class_id, metadata)` (line 676): 클래스의 메타데이터 업데이트
    - `async get_class_instances(self, db_name, class_id, limit, offset, search)` (line 709): 특정 클래스의 인스턴스 목록을 효율적으로 조회 (N+1 Query 최적화)
    - `async get_instance(self, db_name, instance_id, class_id)` (line 748): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id)` (line 780): 특정 클래스의 인스턴스 개수 조회
    - `async execute_sparql(self, db_name, query, limit, offset)` (line 805): SPARQL 쿼리 실행
    - `async __aenter__(self)` (line 843): no docstring
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 846): no docstring

### `backend/bff/services/sheet_import_service.py`

### `backend/bff/tests/test_ai_query_router.py`
- **Functions**
  - `client()` (line 64): no docstring
  - `_install_common_overrides(llm_gateway)` (line 68): no docstring
  - `test_translate_query_plan_returns_plan(client)` (line 88): no docstring
  - `test_translate_query_plan_enables_paths_when_question_asks_for_path(client)` (line 111): no docstring
  - `test_ai_query_unsupported_returns_guidance_templates(client)` (line 147): no docstring
  - `test_ai_query_label_query_executes_and_answers(client)` (line 171): no docstring
- **Classes**
  - `_FakeOMSClient` (line 20): no docstring
    - `async list_ontologies(self, db_name)` (line 21): no docstring
  - `_FakeGateway` (line 39): no docstring
    - `__init__(self, plan, answer)` (line 40): no docstring
    - `async complete_json(self, task, response_model, **kwargs)` (line 44): no docstring

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

### `backend/bff/tests/test_dataset_ingest_idempotency.py`
- **Functions**
  - `async _noop_flush_outbox(*args, **kwargs)` (line 257): no docstring
  - `_build_upload(content)` (line 261): no docstring
  - `async test_csv_upload_idempotency_key_reuses_version(monkeypatch)` (line 270): no docstring
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
  - `_IngestTransaction` (line 81): no docstring
  - `_DatasetVersion` (line 91): no docstring
  - `_FakeDatasetRegistry` (line 102): no docstring
    - `__init__(self)` (line 103): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 110): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 113): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 134): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 168): no docstring
    - `async create_ingest_transaction(self, ingest_request_id)` (line 171): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 179): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 191): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 205): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, outbox_entries)` (line 208): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 244): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 250): no docstring

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
  - `test_bff_health_message_localizes_by_lang_param()` (line 10): no docstring
  - `test_bff_http_exception_detail_localizes_by_lang_param()` (line 29): no docstring

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
    - `expected_foundry_conflict(self)` (line 66): 예상되는 Foundry 스타일 충돌
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

### `backend/bff/tests/test_oms_client_http_helpers.py`
- **Functions**
  - `async test_oms_client_http_helpers_roundtrip_json()` (line 8): no docstring

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
  - `async test_promote_build_rejects_missing_ontology_commit()` (line 107): no docstring
  - `async test_promote_build_rejects_ontology_commit_mismatch()` (line 140): no docstring
  - `async test_promote_build_returns_503_when_ontology_gate_unavailable()` (line 176): no docstring
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
  - `_ObjectifyRegistry` (line 72): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 73): no docstring
  - `_AuditStore` (line 85): no docstring
    - `__init__(self)` (line 86): no docstring
    - `async log(self, **kwargs)` (line 89): no docstring
  - `_OMSClient` (line 93): no docstring
    - `__init__(self, head_commit_id)` (line 94): no docstring
    - `async get_version_head(self, db_name, branch)` (line 97): no docstring
  - `_FailingOMSClient` (line 101): no docstring
    - `async get_version_head(self, db_name, branch)` (line 102): no docstring

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
  - `lakefs_merge_stub(monkeypatch)` (line 255): no docstring
  - `async test_build_enqueues_job_and_records_run()` (line 260): no docstring
  - `async test_preview_enqueues_job_with_node_id_and_records_preview_and_run(monkeypatch)` (line 301): no docstring
  - `async test_promote_build_merges_build_branch_to_main_and_registers_version(lakefs_merge_stub, monkeypatch)` (line 355): no docstring
  - `async test_promote_build_rejects_non_staged_artifact_key()` (line 427): no docstring
  - `async test_promote_build_surfaces_build_errors_when_build_failed()` (line 479): no docstring
  - `async test_promote_build_blocks_deploy_when_expectations_failed()` (line 517): no docstring
  - `async test_promote_build_requires_replay_for_breaking_schema_changes(lakefs_merge_stub)` (line 554): no docstring
  - `async test_promote_build_allows_breaking_schema_changes_with_replay_flag(lakefs_merge_stub, monkeypatch)` (line 617): no docstring
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
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 182): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 185): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, promoted_from_artifact_id)` (line 196): no docstring
  - `_ObjectifyRegistry` (line 225): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 226): no docstring
  - `_OMSClient` (line 238): no docstring
    - `__init__(self, head_commit_id)` (line 239): no docstring
    - `async get_version_head(self, db_name, branch)` (line 242): no docstring
  - `_AuditStore` (line 246): no docstring
    - `__init__(self)` (line 247): no docstring
    - `async log(self, **kwargs)` (line 250): no docstring

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

### `backend/bff/tests/test_pipeline_router_uploads.py`
- **Functions**
  - `_build_request(headers)` (line 248): no docstring
  - `test_pipeline_helpers_normalize_inputs(monkeypatch)` (line 258): no docstring
  - `async test_upload_csv_dataset_creates_version(monkeypatch)` (line 287): no docstring
  - `async test_upload_excel_dataset_commits_preview(monkeypatch)` (line 331): no docstring
  - `async test_upload_media_dataset_stores_files(monkeypatch)` (line 377): no docstring
  - `async test_maybe_enqueue_objectify_job()` (line 415): no docstring
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
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 90): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch)` (line 93): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 110): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 146): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 149): no docstring
    - `async create_ingest_transaction(self, ingest_request_id)` (line 152): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 169): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 175): no docstring
    - `async update_ingest_request_payload(self, ingest_request_id, sample_json, row_count)` (line 181): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, outbox_entries)` (line 187): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 216): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 221): no docstring
  - `_FakeFunnelClient` (line 227): no docstring
    - `async __aenter__(self)` (line 228): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 231): no docstring
    - `async analyze_dataset(self, payload)` (line 234): no docstring
    - `async excel_to_structure_preview_stream(self, *args, **kwargs)` (line 237): no docstring

### `backend/bff/tests/test_security_information_leakage.py`
- **Classes**
  - `TestInformationLeakagePrevention` (line 22): Test suite to verify BFF APIs don't leak internal architecture information
    - `client(self)` (line 26): Create test client
    - `mock_elasticsearch_service(self)` (line 31): Mock Elasticsearch service
    - `mock_oms_client(self)` (line 36): Mock OMS client
    - `async test_get_instance_elasticsearch_success_no_source_leak(self, client)` (line 41): Test: get_instance with ES success should not leak source information
    - `async test_get_instance_terminus_fallback_no_source_leak(self, client)` (line 83): Test: get_instance with TerminusDB fallback should not leak source information
    - `async test_get_class_instances_elasticsearch_success_no_source_leak(self, client)` (line 126): Test: get_class_instances with ES success should not leak source information
    - `async test_get_class_instances_terminus_fallback_no_source_leak(self, client)` (line 165): Test: get_class_instances with TerminusDB fallback should not leak source information
    - `async test_error_responses_no_internal_info_leak(self, client)` (line 204): Test: Error responses should not leak internal architecture details
    - `test_response_structure_consistency(self)` (line 236): Test: All successful responses should have consistent structure regardless of data source
    - `test_forbidden_fields_in_responses(self)` (line 257): Test: Ensure specific internal fields are never exposed in API responses

### `backend/bff/tests/test_sheet_import_service.py`
- **Functions**
  - `test_coerce_integer_with_currency_suffix()` (line 4): no docstring
  - `test_coerce_decimal_with_currency_symbol()` (line 15): no docstring
  - `test_coerce_date_accepts_common_separators()` (line 26): no docstring
  - `test_boolean_parsing()` (line 37): no docstring
  - `test_error_rows_are_reported_and_can_be_filtered()` (line 48): no docstring

### `backend/bff/utils/__init__.py`

### `backend/bff/utils/conflict_converter.py`
- **Classes**
  - `ConflictSeverity` (line 16): 충돌 심각도
  - `PathType` (line 25): JSON-LD 경로 타입
  - `JsonLdPath` (line 37): JSON-LD 경로 분석 결과
  - `ConflictAnalysis` (line 49): 충돌 분석 결과
  - `ConflictConverter` (line 59): TerminusDB 충돌을 Foundry 스타일로 변환하는 클래스
    - `__init__(self)` (line 62): no docstring
    - `async convert_conflicts_to_foundry_format(self, terminus_conflicts, db_name, source_branch, target_branch)` (line 88): TerminusDB 충돌을 Foundry 스타일로 변환
    - `async _convert_single_conflict(self, conflict, conflict_id, db_name, source_branch, target_branch)` (line 124): 단일 충돌을 Foundry 형식으로 변환
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

### `backend/bff/verify_implementation.py`
- **Functions**
  - `test_core_foundry_conflict_system()` (line 12): 핵심 Foundry 충돌 시스템 검증
  - `test_real_world_scenario()` (line 269): 실제 시나리오 시뮬레이션

## check_graph_data.py

### `backend/check_graph_data.py`
- **Functions**
  - `async check_data()` (line 15): no docstring

## check_kafka_topics.py

### `backend/check_kafka_topics.py`
- **Functions**
  - `check_kafka_topics()` (line 24): Kafka 토픽 및 메시지 확인
  - `check_topic_messages(topic_name)` (line 57): 특정 토픽의 메시지 확인
  - `main()` (line 137): no docstring

## check_syntax.py

### `backend/check_syntax.py`
- **Functions**
  - `check_python_syntax()` (line 13): 백엔드 Python 파일들의 문법을 검사합니다.

## clean_terminus_reference.py

### `backend/clean_terminus_reference.py`
- **Functions**
  - `async clean_stale_reference()` (line 16): no docstring

## conftest.py

### `backend/conftest.py`
- **Functions**
  - `_load_repo_dotenv()` (line 7): no docstring
  - `_env_or_dotenv(dotenv, key, default)` (line 26): no docstring
  - `_ensure_test_env()` (line 30): no docstring

## connector_sync_worker

### `backend/connector_sync_worker/__init__.py`

### `backend/connector_sync_worker/main.py`
- **Functions**
  - `async _main()` (line 597): no docstring
- **Classes**
  - `ConnectorSyncWorker` (line 53): no docstring
    - `__init__(self)` (line 54): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 80): no docstring
    - `async _producer_call(self, func, *args, **kwargs)` (line 84): no docstring
    - `async initialize(self)` (line 88): no docstring
    - `async close(self)` (line 144): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 176): no docstring
    - `async _send_to_dlq(self, envelope, error, attempt_count)` (line 186): no docstring
    - `_bff_scope_headers(self, db_name)` (line 229): no docstring
    - `async _fetch_ontology_schema(self, db_name, class_label, branch)` (line 235): no docstring
    - `async _target_field_types(self, db_name, class_label, branch)` (line 248): no docstring
    - `async _process_google_sheets_update(self, envelope)` (line 265): no docstring
    - `async _handle_envelope(self, envelope)` (line 444): no docstring
    - `async run(self)` (line 454): no docstring

## connector_trigger_service

### `backend/connector_trigger_service/__init__.py`

### `backend/connector_trigger_service/main.py`
- **Functions**
  - `async _main()` (line 312): no docstring
- **Classes**
  - `ConnectorTriggerService` (line 42): no docstring
    - `__init__(self)` (line 43): no docstring
    - `async initialize(self)` (line 57): no docstring
    - `async close(self)` (line 83): no docstring
    - `async _producer_call(self, func, *args, **kwargs)` (line 104): no docstring
    - `async _is_due(self, source)` (line 108): no docstring
    - `async _poll_google_sheets(self, source)` (line 119): no docstring
    - `async _poll_source(self, source, sem)` (line 185): no docstring
    - `async _poll_loop(self)` (line 200): no docstring
    - `async _publish_outbox_loop(self)` (line 219): no docstring
    - `async run(self)` (line 296): no docstring

## create_db_and_schema_direct.py

### `backend/create_db_and_schema_direct.py`
- **Functions**
  - `async create_database_direct(db_name)` (line 11): Create database directly in TerminusDB
  - `async create_schema_with_system_fields(db_name)` (line 45): Create schema classes with system fields directly
  - `async verify_schema(db_name)` (line 132): Verify the schema was created correctly
  - `async main()` (line 229): Main function

## create_integration_schema.py

### `backend/create_integration_schema.py`
- **Functions**
  - `async create_schema()` (line 15): no docstring

## create_minio_bucket.py

### `backend/create_minio_bucket.py`

## create_test_schema.py

### `backend/create_test_schema.py`
- **Functions**
  - `async create_test_environment()` (line 10): Create test database with proper schema including system fields

## data_connector

### `backend/data_connector/__init__.py`

### `backend/data_connector/google_sheets/__init__.py`

### `backend/data_connector/google_sheets/auth.py`
- **Classes**
  - `GoogleOAuth2Client` (line 16): Google OAuth2 인증 클라이언트 (향후 확장용)
    - `__init__(self, client_id, client_secret, redirect_uri)` (line 35): 초기화
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
  - `GoogleSheetsService` (line 32): Google Sheets API client (read-only).
    - `__init__(self, api_key)` (line 35): no docstring
    - `async _get_client(self)` (line 41): no docstring
    - `async fetch_sheet_values(self, sheet_url, worksheet_name, api_key, access_token)` (line 49): Fetch raw values + metadata for a Google Sheet URL.
    - `async preview_sheet(self, sheet_url, worksheet_name, limit, api_key, access_token)` (line 110): no docstring
    - `async get_sheet_metadata(self, sheet_id, api_key, access_token)` (line 143): no docstring
    - `async list_spreadsheets(self, access_token, query, page_size)` (line 152): no docstring
    - `async _get_sheet_metadata(self, sheet_id, api_key, access_token)` (line 197): no docstring
    - `async _get_sheet_data(self, sheet_id, range_name, api_key, access_token)` (line 236): no docstring
    - `async close(self)` (line 276): no docstring

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

## debug_404_error.py

### `backend/debug_404_error.py`
- **Functions**
  - `async debug_404()` (line 8): 404 에러 원인 파악

## debug_404_root_cause.py

### `backend/debug_404_root_cause.py`
- **Functions**
  - `async debug_ontology_404()` (line 11): no docstring

## debug_async_terminus.py

### `backend/debug_async_terminus.py`
- **Functions**
  - `async debug_async_terminus_service()` (line 17): no docstring

## debug_database_check.py

### `backend/debug_database_check.py`
- **Functions**
  - `async debug_database_list()` (line 9): no docstring

## debug_link_dependency.py

### `backend/debug_link_dependency.py`
- **Functions**
  - `async debug_link_dependency()` (line 10): no docstring

## debug_link_transformation.py

### `backend/debug_link_transformation.py`
- **Functions**
  - `async debug_transformation()` (line 10): no docstring

## debug_oms_database_creation.py

### `backend/debug_oms_database_creation.py`
- **Functions**
  - `async debug_oms_database_creation()` (line 17): no docstring

## debug_pydantic.py

### `backend/debug_pydantic.py`
- **Functions**
  - `debug_pydantic()` (line 13): no docstring

## debug_settings.py

### `backend/debug_settings.py`
- **Functions**
  - `debug_settings()` (line 12): no docstring

## debug_terminus_auth.py

### `backend/debug_terminus_auth.py`
- **Functions**
  - `async debug_terminus_auth()` (line 15): no docstring

## debug_terminus_direct.py

### `backend/debug_terminus_direct.py`
- **Functions**
  - `async debug_terminusdb_directly()` (line 15): no docstring

## debug_terminusdb_direct.py

### `backend/debug_terminusdb_direct.py`
- **Functions**
  - `get_auth_header()` (line 16): Basic 인증 헤더 생성
  - `test_direct_terminusdb_query()` (line 22): TerminusDB에 직접 쿼리하여 Team 데이터 확인

## debug_woql_bindings.py

### `backend/debug_woql_bindings.py`
- **Functions**
  - `async debug_bindings()` (line 16): Debug why bindings show Unknown
  - `async main()` (line 161): Main execution

## debug_woql_syntax.py

### `backend/debug_woql_syntax.py`
- **Functions**
  - `async test_woql_queries()` (line 16): Test different WOQL query formats
  - `async test_working_format()` (line 138): Test the format that should work based on TerminusDB docs
  - `async main()` (line 172): Run all tests

## demo_validation_improvement.py

### `backend/demo_validation_improvement.py`
- **Functions**
  - `show_validation_improvement()` (line 13): 매핑 검증 개선 내용을 시각적으로 보여주기

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

## final_system_verification.py

### `backend/final_system_verification.py`
- **Functions**
  - `async main()` (line 21): no docstring

## final_verification.py

### `backend/final_verification.py`
- **Functions**
  - `async verify_all_systems()` (line 15): no docstring

## fix_all_datetime_utc.py

### `backend/fix_all_datetime_utc.py`
- **Functions**
  - `fix_file(filepath)` (line 10): Fix datetime.UTC to timezone.utc in a file
  - `main()` (line 41): Fix all Python files in OMS directory

## fix_datetime_deprecation.py

### `backend/fix_datetime_deprecation.py`
- **Functions**
  - `fix_datetime_in_file(filepath)` (line 11): Fix datetime deprecation in a single file
  - `main()` (line 53): Fix all Python files with datetime deprecation

## fix_terminus_schema.py

### `backend/fix_terminus_schema.py`
- **Functions**
  - `async fix_schemas()` (line 11): no docstring

## funnel

### `backend/funnel/__init__.py`

### `backend/funnel/main.py`
- **Functions**
  - `async lifespan(app)` (line 30): 애플리케이션 시작/종료 이벤트
  - `async root()` (line 66): 루트 엔드포인트
  - `async health_check()` (line 84): 서비스 상태 확인

### `backend/funnel/routers/__init__.py`

### `backend/funnel/routers/type_inference_router.py`
- **Functions**
  - `get_data_processor()` (line 33): 데이터 프로세서 의존성
  - `async analyze_dataset(request, processor)` (line 39): 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.
  - `async analyze_sheet_structure(request)` (line 69): Raw sheet grid(엑셀/스프레드시트)의 구조를 분석합니다.
  - `async analyze_excel_structure(file, sheet_name, include_complex_types, max_tables, max_rows, max_cols, options_json)` (line 117): Excel(.xlsx/.xlsm) 파일을 업로드 받아 grid + merged_cells로 파싱한 뒤,
  - `async analyze_google_sheets_structure(request)` (line 213): Google Sheets URL → (BFF에서 values+metadata(merges) 가져오기) → grid/merged_cells → 구조 분석
  - `async upsert_structure_patch(patch)` (line 296): Store/update a structure-analysis patch for a given sheet_signature.
  - `async get_structure_patch(sheet_signature)` (line 304): no docstring
  - `async delete_structure_patch(sheet_signature)` (line 312): no docstring
  - `async preview_google_sheets_with_inference(sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types, processor)` (line 318): Google Sheets 데이터를 미리보기하고 타입을 추론합니다.
  - `async suggest_schema(analysis_results, class_name, processor)` (line 360): 분석 결과를 기반으로 OMS 스키마를 제안합니다.
  - `async health_check()` (line 390): Funnel 서비스 상태 확인

### `backend/funnel/services/__init__.py`

### `backend/funnel/services/data_processor.py`
- **Classes**
  - `FunnelDataProcessor` (line 23): 🔥 THINK ULTRA! 데이터 처리 파이프라인
    - `__init__(self)` (line 33): no docstring
    - `async process_google_sheets_preview(self, sheet_url, worksheet_name, api_key, connection_id, infer_types, include_complex_types)` (line 36): Google Sheets 데이터를 처리하고 타입을 추론합니다.
    - `async analyze_dataset(self, request)` (line 104): 데이터셋을 분석하고 타입을 추론합니다.
    - `generate_schema_suggestion(self, analysis_results, class_name)` (line 134): 분석 결과를 기반으로 스키마를 제안합니다.
    - `_normalize_property_name(self, column_name)` (line 233): 컬럼 이름을 속성 이름으로 정규화
    - `_generate_class_id(self, class_name)` (line 249): 클래스 ID 생성

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
    - `_get_column_name_hint_scores(cls, column_name)` (line 249): Return type -> hint strength (0.0~1.0) based on column name.
    - `_extract_unit_from_values(cls, values)` (line 332): Best-effort unit extraction from sample values.
    - `_infer_semantic_label_and_unit(cls, values, column_name, inferred)` (line 373): Derive a semantic label (meaning) + unit from type + hints.
    - `_min_confidence_for_type(cls, type_id, thresholds, name_hints)` (line 464): Minimum acceptance confidence for a type (name hints can lower it).
    - `_type_priority(cls, type_id)` (line 490): Tie-break priority (lower is preferred).
    - `_select_best_candidate(cls, candidates, thresholds, name_hints)` (line 512): no docstring
    - `_check_complex_types_enhanced(cls, values, thresholds, column_name, name_hints)` (line 562): Evaluate complex/specialized types via validators and heuristics.
    - `_check_validator_type(cls, values, type_id, sample_limit, hint_reason, constraints)` (line 609): Check values against ComplexTypeValidator for a given type.
    - `_derive_money_constraints_from_samples(cls, values)` (line 693): Derive money constraints (allowedCurrencies) from explicit tokens in samples.
    - `_check_enum_enhanced(cls, values, thresholds, name_hints)` (line 724): Detect enum-like categorical strings and propose constraints.
    - `_check_boolean(cls, values)` (line 764): Check if values are boolean
    - `_check_integer(cls, values)` (line 788): Check if values are integers
    - `_check_decimal(cls, values)` (line 825): Check if values are decimal numbers
    - `_check_date(cls, values)` (line 872): Check if values are dates
    - `_check_datetime(cls, values)` (line 912): Check if values are datetime
    - `_check_column_name_hints(cls, column_name)` (line 954): Check column name for type hints
    - `_calculate_adaptive_thresholds(cls, values, sample_size)` (line 1030): 🔥 Adaptive Thresholds: tune acceptance based on sample size.
    - `_analyze_context(cls, column_name, context_columns)` (line 1062): 🔥 Contextual Analysis: Analyze surrounding columns for type hints
    - `_check_column_name_hints_enhanced(cls, column_name)` (line 1092): 🔥 Enhanced Column Name Hints with Multilingual Support
    - `_check_boolean_enhanced(cls, values, thresholds)` (line 1139): 🔥 Enhanced Boolean Detection with Fuzzy Matching
    - `_check_integer_enhanced(cls, values, thresholds)` (line 1171): 🔥 Enhanced Integer Detection with Statistical Analysis
    - `_check_decimal_enhanced(cls, values, thresholds)` (line 1254): 🔥 Enhanced Decimal Detection with Distribution Analysis
    - `_check_date_enhanced(cls, values, thresholds)` (line 1376): 🔥 Enhanced Date Detection with strict parsing and ambiguity handling.
    - `_check_datetime_enhanced(cls, values, thresholds)` (line 1524): 🔥 Enhanced DateTime Detection with Advanced Parsing
    - `analyze_dataset(cls, data, columns, sample_size, include_complex_types)` (line 1594): 전체 데이터셋을 분석하여 각 컬럼의 타입을 추론합니다.
    - `_check_phone_enhanced(cls, values, thresholds, column_name)` (line 1661): 🔥 Enhanced Phone Number Detection with Global Patterns

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
  - `async test_data_processor_analyze_dataset_metadata()` (line 36): no docstring
  - `test_generate_schema_suggestion_handles_confidence()` (line 51): no docstring
  - `async test_process_google_sheets_preview_success(monkeypatch)` (line 85): no docstring
  - `async test_process_google_sheets_preview_failure(monkeypatch)` (line 114): no docstring
- **Classes**
  - `_FakeResponse` (line 9): no docstring
    - `__init__(self, payload, status_code)` (line 10): no docstring
    - `json(self)` (line 15): no docstring
  - `_FakeClient` (line 19): no docstring
    - `__init__(self, response)` (line 20): no docstring
    - `async __aenter__(self)` (line 24): no docstring
    - `async __aexit__(self, exc_type, exc, tb)` (line 27): no docstring
    - `async post(self, url, json)` (line 30): no docstring

### `backend/funnel/tests/test_funnel_main.py`
- **Functions**
  - `async test_funnel_root_and_health()` (line 21): no docstring
  - `async test_funnel_lifespan_initializes_rate_limiter(monkeypatch)` (line 32): no docstring
- **Classes**
  - `_FakeRateLimiter` (line 8): no docstring
    - `__init__(self)` (line 9): no docstring
    - `async initialize(self)` (line 13): no docstring
    - `async close(self)` (line 16): no docstring

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

## implement_lightweight_schema.py

### `backend/implement_lightweight_schema.py`
- **Functions**
  - `async implement_lightweight_architecture()` (line 12): no docstring

## ingest_reconciler_worker

### `backend/ingest_reconciler_worker/__init__.py`

### `backend/ingest_reconciler_worker/main.py`
- **Functions**
  - `_resolve_webhook_url()` (line 31): no docstring
  - `async lifespan(app)` (line 209): no docstring
  - `main()` (line 236): no docstring
- **Classes**
  - `IngestReconcilerWorker` (line 39): no docstring
    - `__init__(self)` (line 40): no docstring
    - `async initialize(self)` (line 68): no docstring
    - `async close(self)` (line 74): no docstring
    - `_record_metrics(self, result)` (line 82): no docstring
    - `_record_error_metric(self)` (line 93): no docstring
    - `_record_alert_metric(self)` (line 96): no docstring
    - `_record_alert_failure_metric(self)` (line 99): no docstring
    - `_should_alert(self, result)` (line 102): no docstring
    - `_alert_allowed(self)` (line 109): no docstring
    - `async _emit_alert(self, payload)` (line 118): no docstring
    - `async run(self, stop_event)` (line 131): no docstring

## instance_worker

### `backend/instance_worker/__init__.py`

### `backend/instance_worker/main.py`
- **Functions**
  - `async main()` (line 2293): Main entry point
- **Classes**
  - `StrictPalantirInstanceWorker` (line 64): STRICT Palantir-style Instance Worker
    - `__init__(self)` (line 72): no docstring
    - `async initialize(self)` (line 102): Initialize all connections
    - `async _s3_call(self, func, *args, **kwargs)` (line 225): no docstring
    - `async _s3_read_body(self, body)` (line 228): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 231): no docstring
    - `async extract_payload_from_message(self, message)` (line 235): Unwrap a command from the canonical EventEnvelope message.
    - `get_primary_key_value(self, class_id, payload)` (line 264): Extract primary key value dynamically based on class naming convention
    - `async extract_relationships(self, db_name, class_id, payload, branch)` (line 286): Extract ONLY relationship fields from payload
    - `async extract_required_properties(self, db_name, class_id, branch)` (line 470): Extract required property names from the class schema.
    - `async _apply_create_instance_side_effects(self, command_id, db_name, class_id, branch, payload, instance_id, command_log, ontology_version, created_by)` (line 522): Apply the create-instance side-effects without touching command status.
    - `async process_create_instance(self, command)` (line 781): Process CREATE_INSTANCE command - STRICT Palantir style
    - `async process_bulk_create_instances(self, command)` (line 1114): Process BULK_CREATE_INSTANCES command (idempotent per event_id; no sequence-guard).
    - `async process_update_instance(self, command)` (line 1239): Process UPDATE_INSTANCE command (idempotent + ordered via registry claim).
    - `async process_delete_instance(self, command)` (line 1703): Process DELETE_INSTANCE command (idempotent delete).
    - `async set_command_status(self, command_id, status, result)` (line 1958): Set command status using CommandStatusService (preserves history + pubsub).
    - `async _heartbeat_loop(self, handler, event_id)` (line 2026): no docstring
    - `_is_retryable_error(exc)` (line 2037): no docstring
    - `async run(self)` (line 2061): Main processing loop
    - `async shutdown(self)` (line 2275): Graceful shutdown

## mcp

### `backend/mcp/context7_development.py`
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

### `backend/mcp/mcp_client.py`
- **Functions**
  - `get_mcp_manager()` (line 274): Get or create MCP manager singleton
  - `get_context7_client()` (line 282): Get Context7 client
- **Classes**
  - `MCPServerConfig` (line 21): Configuration for an MCP server
  - `MCPClientManager` (line 30): Manager for multiple MCP client connections
    - `__init__(self, config_path)` (line 36): no docstring
    - `_load_config(self)` (line 42): Load MCP configuration from file
    - `async connect_server(self, server_name)` (line 62): Connect to a specific MCP server
    - `async disconnect_server(self, server_name)` (line 107): Disconnect from a specific MCP server
    - `async call_tool(self, server_name, tool_name, arguments)` (line 114): Call a tool on a specific MCP server
    - `async list_tools(self, server_name)` (line 142): List available tools from a server
  - `Context7Client` (line 156): Specialized client for Context7 MCP server
    - `__init__(self, mcp_manager)` (line 162): no docstring
    - `async search(self, query, limit)` (line 166): Search Context7 knowledge base
    - `async get_context(self, entity_id)` (line 183): Get context for a specific entity
    - `async add_knowledge(self, title, content, metadata)` (line 199): Add knowledge to Context7
    - `async link_entities(self, source_id, target_id, relationship)` (line 226): Create relationship between entities
    - `async analyze_ontology(self, ontology_data)` (line 253): Analyze ontology with Context7

### `backend/mcp/terminus_mcp_server.py`
- **Functions**
  - `async main()` (line 298): Main entry point
- **Classes**
  - `TerminusDBMCPServer` (line 30): MCP Server for TerminusDB operations
    - `__init__(self)` (line 36): no docstring
    - `_setup_handlers(self)` (line 41): Setup MCP request handlers
    - `async _connect_terminus(self)` (line 279): Connect to TerminusDB
    - `async run(self)` (line 292): Run the MCP server

## message_relay

### `backend/message_relay/__init__.py`

### `backend/message_relay/main.py`
- **Functions**
  - `async main()` (line 820): 메인 진입점
- **Classes**
  - `_RecentPublishedEventIds` (line 49): Best-effort in-memory dedup window (publisher is still at-least-once).
    - `__init__(self, max_events)` (line 52): no docstring
    - `mark_published(self, event_id)` (line 56): no docstring
    - `was_published(self, event_id)` (line 66): no docstring
    - `load(self, event_ids)` (line 74): no docstring
    - `snapshot(self, max_events)` (line 85): no docstring
  - `EventPublisher` (line 93): S3/MinIO Event Store -> Kafka publisher.
    - `__init__(self)` (line 96): no docstring
    - `async initialize(self)` (line 150): 서비스 초기화
    - `async ensure_kafka_topics(self)` (line 208): 필요한 Kafka 토픽이 존재하는지 확인하고 없으면 생성
    - `async _load_checkpoint(self)` (line 265): no docstring
    - `async _save_checkpoint(self, checkpoint)` (line 292): no docstring
    - `_log_metrics_if_due(self)` (line 321): no docstring
    - `_flush_producer(self, timeout_s)` (line 344): no docstring
    - `_initial_checkpoint(self)` (line 354): no docstring
    - `_advance_checkpoint(checkpoint, ts_ms, idx_key)` (line 368): Advance the durable checkpoint monotonically (never move backwards).
    - `async _list_next_index_keys(self, checkpoint)` (line 392): no docstring
    - `async process_events(self)` (line 482): Tail S3 by-date index and publish to Kafka.
    - `async run(self)` (line 786): 메인 실행 루프
    - `async shutdown(self)` (line 806): 서비스 종료

## migrate_es_to_terminus_lightweight.py

### `backend/migrate_es_to_terminus_lightweight.py`
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
  - `async main()` (line 1340): no docstring
- **Classes**
  - `ObjectifyNonRetryableError` (line 43): Raised for objectify failures that should not be retried.
  - `ObjectifyWorker` (line 47): no docstring
    - `__init__(self)` (line 48): no docstring
    - `_build_error_report(self, error, report, job, message, context)` (line 76): no docstring
    - `async initialize(self)` (line 115): no docstring
    - `async close(self)` (line 172): no docstring
    - `async run(self)` (line 201): no docstring
    - `async _process_job(self, job)` (line 352): no docstring
    - `async _bulk_create_instances(self, job, instances, ontology_version)` (line 660): no docstring
    - `async _resolve_artifact_output(self, job)` (line 695): no docstring
    - `async _fetch_target_field_types(self, job)` (line 740): no docstring
    - `async _fetch_ontology_version(self, job)` (line 765): no docstring
    - `_normalize_pk_fields(value)` (line 786): no docstring
    - `_hash_payload(payload)` (line 796): no docstring
    - `_derive_row_key(self, columns, col_index, row, instance, pk_fields, pk_targets)` (line 800): no docstring
    - `async _iter_dataset_batches(self, job, options, row_batch_size, max_rows)` (line 831): no docstring
    - `async _iter_csv_batches(self, bucket, key, delimiter, has_header, row_batch_size, max_rows)` (line 871): no docstring
    - `async _iter_json_part_batches(self, bucket, prefix, row_batch_size, max_rows)` (line 963): no docstring
    - `async _validate_batches(self, job, options, mappings, target_field_types, row_batch_size, max_rows)` (line 1019): no docstring
    - `_ensure_instance_ids(self, instances, class_id, stable_seed, mapping_spec_version, row_keys, instance_id_field)` (line 1073): no docstring
    - `async _record_lineage_header(self, job, mapping_spec, ontology_version, input_type, artifact_output_name)` (line 1103): no docstring
    - `async _record_instance_lineage(self, job, job_node_id, instance_ids, mapping_spec_id, mapping_spec_version, ontology_version, limit_remaining, input_type, artifact_output_name)` (line 1185): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 1265): no docstring
    - `async _send_to_dlq(self, job, raw_payload, error, attempt_count)` (line 1278): no docstring
    - `_is_retryable_error(exc)` (line 1304): no docstring

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
  - `ValidatedDatabaseName(db_name)` (line 239): 데이터베이스 이름 검증 의존성 - Modernized version
  - `ValidatedClassId(class_id)` (line 250): 클래스 ID 검증 의존성 - Modernized version
  - `async ensure_database_exists(db_name, terminus)` (line 262): 데이터베이스 존재 확인 및 검증된 이름 반환 - Modernized version
  - `async check_oms_dependencies_health(container)` (line 300): Check health of all OMS dependencies
- **Classes**
  - `OMSDependencyProvider` (line 47): Modern dependency provider for OMS services
    - `async get_terminus_service(container)` (line 56): Get AsyncTerminusService from container
    - `async get_jsonld_converter(container)` (line 86): Get JSON-LD converter from container
    - `async get_label_mapper(container)` (line 110): Get label mapper from container
    - `async get_event_store(container)` (line 134): Get S3/MinIO Event Store - The Single Source of Truth.
    - `async get_command_status_service(container)` (line 144): Get command status service from container
    - `async get_processed_event_registry(container)` (line 205): no docstring

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
  - `async lifespan(app)` (line 348): Modern application lifecycle management
  - `async get_terminus_service()` (line 440): Get TerminusDB service from OMS container
  - `async root()` (line 449): 루트 엔드포인트 - Modernized version
  - `async health_check()` (line 468): 헬스 체크 - Modernized version
  - `async container_health_check()` (line 553): Health check for the modernized container system
- **Classes**
  - `OMSServiceContainer` (line 91): OMS-specific service container to manage OMS services
    - `__init__(self, container, settings)` (line 99): no docstring
    - `async initialize_oms_services(self)` (line 104): Initialize OMS-specific services
    - `async _initialize_event_store(self)` (line 134): Initialize S3/MinIO Event Store - The Single Source of Truth.
    - `async _initialize_terminus_service(self)` (line 154): Initialize TerminusDB service with health check
    - `async _initialize_postgres(self)` (line 180): Initialize Postgres MVCC pool (required for pull requests/proposals).
    - `async _initialize_jsonld_converter(self)` (line 190): Initialize JSON-LD converter
    - `async _initialize_label_mapper(self)` (line 196): Initialize label mapper
    - `async _initialize_redis_and_command_status(self)` (line 202): Initialize Redis service and Command Status service
    - `async _initialize_elasticsearch(self)` (line 227): Initialize Elasticsearch service
    - `async _initialize_rate_limiter(self)` (line 246): Initialize rate limiting service
    - `async shutdown_oms_services(self)` (line 262): Shutdown OMS-specific services
    - `get_terminus_service(self)` (line 312): Get TerminusDB service instance
    - `get_jsonld_converter(self)` (line 318): Get JSON-LD converter instance
    - `get_label_mapper(self)` (line 324): Get label mapper instance
    - `get_redis_service(self)` (line 330): Get Redis service instance (can be None)
    - `get_command_status_service(self)` (line 334): Get command status service instance (can be None)
    - `get_elasticsearch_service(self)` (line 338): Get Elasticsearch service instance (can be None)

### `backend/oms/middleware/auth.py`
- **Functions**
  - `ensure_oms_auth_configured()` (line 26): no docstring
  - `install_oms_auth_middleware(app)` (line 42): no docstring

### `backend/oms/routers/__init__.py`

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
  - `async delete_database(db_name, expected_seq, event_store, command_status_service)` (line 205): 데이터베이스 삭제
  - `async database_exists(db_name, terminus_service)` (line 303): 데이터베이스 존재 여부 확인

### `backend/oms/routers/instance.py`
- **Functions**
  - `async get_class_instances(db_name, class_id, limit, offset, search, terminus)` (line 26): 특정 클래스의 인스턴스 목록을 효율적으로 조회
  - `async get_instance(db_name, instance_id, class_id, terminus)` (line 118): 개별 인스턴스를 효율적으로 조회
  - `async get_class_instance_count(db_name, class_id, terminus)` (line 181): 특정 클래스의 인스턴스 개수를 효율적으로 조회
  - `async execute_sparql_query(db_name, query, limit, offset, terminus)` (line 231): SPARQL 쿼리 직접 실행

### `backend/oms/routers/instance_async.py`
- **Functions**
  - `async _fallback_from_registry(command_uuid, registry)` (line 52): no docstring
  - `async _append_command_event(command, event_store, topic, actor)` (line 92): Store command as an immutable event in S3/MinIO for the publisher to relay to Kafka.
  - `_derive_instance_id(class_id, payload)` (line 104): Derive a stable instance_id for CREATE_INSTANCE so command/event aggregate_id matches.
  - `async create_instance_async(db_name, class_id, branch, request, terminus, command_status_service, event_store, user_id)` (line 145): 인스턴스 생성 명령을 비동기로 처리
  - `async update_instance_async(db_name, class_id, instance_id, branch, expected_seq, request, terminus, command_status_service, event_store, user_id)` (line 264): 인스턴스 수정 명령을 비동기로 처리
  - `async delete_instance_async(db_name, class_id, instance_id, branch, expected_seq, terminus, command_status_service, event_store, user_id)` (line 379): 인스턴스 삭제 명령을 비동기로 처리
  - `async bulk_create_instances_async(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 488): 대량 인스턴스 생성 명령을 비동기로 처리
  - `async get_instance_command_status(db_name, command_id, command_status_service, processed_event_registry, event_store)` (line 615): 인스턴스 명령의 상태 조회
  - `async _track_bulk_create_progress(command_id, total_instances, command_status_service)` (line 705): Track progress of bulk create operation in background.
  - `async bulk_create_instances_with_tracking(db_name, class_id, branch, request, background_tasks, terminus, command_status_service, event_store, user_id)` (line 754): Enhanced bulk instance creation with proper background task tracking.
  - `async _process_bulk_create_in_background(task_id, db_name, class_id, branch, instances, metadata, user_id, ontology_version, event_store, command_status_service)` (line 808): Process bulk create operation in background with proper error handling.
- **Classes**
  - `InstanceCreateRequest` (line 126): 인스턴스 생성 요청
  - `InstanceUpdateRequest` (line 132): 인스턴스 수정 요청
  - `BulkInstanceCreateRequest` (line 138): 대량 인스턴스 생성 요청

### `backend/oms/routers/ontology.py`
- **Functions**
  - `_is_protected_branch(branch)` (line 88): no docstring
  - `_require_proposal_for_branch(branch)` (line 92): no docstring
  - `_reject_direct_write_if_required(branch)` (line 98): no docstring
  - `_admin_authorized(request)` (line 109): no docstring
  - `_extract_change_reason(request)` (line 119): no docstring
  - `_extract_actor(request)` (line 124): no docstring
  - `async _collect_interface_issues(terminus, db_name, branch, ontology_id, metadata, properties, relationships)` (line 129): no docstring
  - `_is_internal_ontology(ontology)` (line 167): no docstring
  - `_localized_to_string(value, lang)` (line 176): no docstring
  - `_merge_lint_reports(*reports)` (line 198): no docstring
  - `async _ensure_database_exists(db_name, terminus)` (line 220): 데이터베이스 존재 여부 확인 후 404 예외 발생
  - `async create_ontology(ontology_request, request, db_name, branch, terminus, event_store, command_status_service)` (line 248): 내부 ID 기반 온톨로지 생성
  - `async validate_ontology_create(ontology_request, request, db_name, branch, terminus)` (line 481): 온톨로지 생성 검증 (no write).
  - `async validate_ontology_update(ontology_data, request, db_name, class_id, branch, terminus)` (line 561): 온톨로지 업데이트 검증 (no write).
  - `async list_ontologies(db_name, branch, class_type, limit, offset, terminus, label_mapper)` (line 655): 내부 ID 기반 온톨로지 목록 조회
  - `async analyze_relationship_network(db_name, terminus)` (line 718): 🔥 관계 네트워크 종합 분석 엔드포인트
  - `async get_ontology(db_name, class_id, branch, terminus, converter, label_mapper)` (line 749): 내부 ID 기반 온톨로지 조회
  - `async update_ontology(ontology_data, request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 834): 내부 ID 기반 온톨로지 업데이트
  - `async delete_ontology(request, db_name, class_id, branch, expected_seq, terminus, event_store, command_status_service)` (line 1072): 내부 ID 기반 온톨로지 삭제
  - `async query_ontologies(query, db_name, terminus)` (line 1206): 내부 ID 기반 온톨로지 쿼리
  - `async create_ontology_with_advanced_relationships(ontology_request, request, db_name, branch, auto_generate_inverse, validate_relationships, check_circular_references, terminus, event_store, command_status_service)` (line 1303): 🔥 고급 관계 관리 기능을 포함한 온톨로지 생성
  - `async validate_ontology_relationships(request, db_name, terminus)` (line 1524): 🔥 온톨로지 관계 검증 전용 엔드포인트
  - `async detect_circular_references(db_name, new_ontology, terminus)` (line 1570): 🔥 순환 참조 탐지 전용 엔드포인트
  - `async find_relationship_paths(start_entity, db_name, end_entity, max_depth, path_type, terminus)` (line 1618): 🔥 관계 경로 탐색 엔드포인트
  - `async get_reachable_entities(start_entity, db_name, max_depth, terminus)` (line 1677): 🔥 도달 가능한 엔티티 조회 엔드포인트

### `backend/oms/routers/ontology_extensions.py`
- **Functions**
  - `async _get_pr_service()` (line 97): no docstring
  - `_normalize_resource_payload(payload)` (line 106): no docstring
  - `_resource_validation_strict()` (line 126): no docstring
  - `_require_health_gate(branch)` (line 130): no docstring
  - `_ensure_branch_writable(branch)` (line 136): no docstring
  - `async _assert_expected_head_commit(terminus, db_name, branch, expected_head_commit)` (line 145): no docstring
  - `async _compute_ontology_health(db_name, branch, terminus)` (line 183): no docstring
  - `async list_resources(db_name, resource_type, branch, limit, offset, terminus)` (line 435): no docstring
  - `async list_resources_by_type(db_name, resource_type, branch, limit, offset, terminus)` (line 469): no docstring
  - `async create_resource(db_name, resource_type, payload, branch, expected_head_commit, terminus)` (line 488): no docstring
  - `async get_resource(db_name, resource_type, resource_id, branch, terminus)` (line 551): no docstring
  - `async update_resource(db_name, resource_type, resource_id, payload, branch, expected_head_commit, terminus)` (line 587): no docstring
  - `async delete_resource(db_name, resource_type, resource_id, branch, expected_head_commit, terminus)` (line 645): no docstring
  - `async list_ontology_branches(db_name, terminus)` (line 685): no docstring
  - `async create_ontology_branch(db_name, request, terminus)` (line 702): no docstring
  - `async list_ontology_proposals(db_name, status_filter, limit, pr_service)` (line 725): no docstring
  - `async create_ontology_proposal(db_name, request, pr_service)` (line 746): no docstring
  - `async approve_ontology_proposal(db_name, proposal_id, request, pr_service, terminus)` (line 774): no docstring
  - `async deploy_ontology(db_name, request, pr_service, terminus)` (line 849): no docstring
  - `async ontology_health(db_name, branch, terminus)` (line 951): no docstring
  - `_validation_result_to_issue(result)` (line 978): no docstring
  - `_resource_is_referenced(resource_type, resource_id, references)` (line 988): no docstring
  - `_resource_ref(resource_type, resource_id)` (line 1005): no docstring
  - `_build_issue(code, severity, resource_ref, details, suggested_fix, message, source)` (line 1009): no docstring
  - `_normalize_issue(issue, source)` (line 1030): no docstring
  - `_resolve_relationship_resource_ref(result, class_ids)` (line 1051): no docstring
- **Classes**
  - `OntologyResourceRequest` (line 66): no docstring
  - `OntologyProposalRequest` (line 76): no docstring
  - `OntologyDeployRequest` (line 84): no docstring
  - `OntologyApproveRequest` (line 92): no docstring

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
  - `async get_task_manager(container)` (line 31): Get BackgroundTaskManager from container.
  - `async get_internal_task_status(task_id, task_manager)` (line 55): Get internal task status for monitoring.
  - `async get_active_tasks(task_manager)` (line 84): Get all active (running) tasks.
  - `async cleanup_old_tasks(older_than_days, task_manager, redis_service)` (line 124): Clean up old completed tasks.
  - `async task_service_health(task_manager)` (line 157): Get task service health status.

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

### `backend/oms/services/async_terminus.py`
- **Classes**
  - `AtomicUpdateError` (line 64): Base exception for atomic update operations
  - `PatchUpdateError` (line 68): Exception for PATCH-based update failures
  - `TransactionUpdateError` (line 72): Exception for transaction-based update failures
  - `WOQLUpdateError` (line 76): Exception for WOQL-based update failures
  - `BackupCreationError` (line 80): Exception for backup creation failures
  - `RestoreError` (line 84): Exception for restore operation failures
  - `BackupRestoreError` (line 88): Exception for backup and restore operation failures
  - `AsyncTerminusService` (line 107): 비동기 TerminusDB 서비스 클래스 - Clean Facade Pattern
    - `__init__(self, connection_info)` (line 122): 초기화
    - `async check_connection(self)` (line 164): 연결 상태 확인
    - `async connect(self)` (line 177): 연결 설정
    - `async disconnect(self)` (line 183): 연결 해제
    - `async close(self)` (line 187): 모든 서비스 종료
    - `async create_database(self, db_name, description)` (line 213): 데이터베이스 생성
    - `async database_exists(self, db_name)` (line 225): 데이터베이스 존재 여부 확인
    - `async list_databases(self)` (line 229): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 234): 데이터베이스 삭제
    - `async execute_query(self, db_name, query_dict)` (line 242): Query execution (query-spec or raw WOQL passthrough).
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 246): SPARQL 쿼리 직접 실행
    - `async get_class_instances_optimized(self, db_name, class_id, branch, limit, offset, filter_conditions)` (line 260): 특정 클래스의 모든 인스턴스를 효율적으로 조회
    - `async get_instance_optimized(self, db_name, instance_id, branch, class_id)` (line 284): 개별 인스턴스를 효율적으로 조회
    - `async count_class_instances(self, db_name, class_id, branch, filter_conditions)` (line 299): 특정 클래스의 인스턴스 개수를 효율적으로 조회
    - `async get_ontology(self, db_name, class_id, raise_if_missing, branch)` (line 318): 온톨로지 조회 (branch-aware).
    - `async create_ontology(self, db_name, ontology_data, branch)` (line 336): 온톨로지 생성
    - `async create_ontology_with_advanced_relationships(self, db_name, ontology_data, branch, auto_generate_inverse, validate_relationships, check_circular_references)` (line 349): no docstring
    - `async update_ontology(self, db_name, class_id, ontology_data, branch)` (line 420): 온톨로지 업데이트 - Atomic 버전
    - `async delete_ontology(self, db_name, class_id, branch)` (line 431): 온톨로지 삭제
    - `async list_ontology_classes(self, db_name)` (line 435): 데이터베이스의 모든 온톨로지 목록 조회
    - `async create_branch(self, db_name, branch_name, from_branch)` (line 444): 브랜치 생성
    - `async list_branches(self, db_name)` (line 449): 브랜치 목록 조회
    - `async get_current_branch(self, db_name)` (line 456): 현재 브랜치 (best-effort, 기본값: main)
    - `async delete_branch(self, db_name, branch_name)` (line 460): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 464): 브랜치 체크아웃
    - `async checkout(self, db_name, target, target_type)` (line 471): Router 호환 checkout (branch/commit).
    - `async merge_branches(self, db_name, source_branch, target_branch, message, author)` (line 478): 브랜치 병합
    - `async commit(self, db_name, message, author, branch)` (line 495): 커밋 생성
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 505): 커밋 히스토리 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 515): 차이점 조회
    - `async merge(self, db_name, source_branch, target_branch, strategy)` (line 519): Router 호환 merge API.
    - `async rebase(self, db_name, onto, branch, message)` (line 535): Router 호환 rebase API (branch -> onto).
    - `async rollback(self, db_name, target)` (line 546): Router 호환 rollback API (reset current branch to target).
    - `async find_common_ancestor(self, db_name, branch1, branch2)` (line 551): 공통 조상 찾기 (현재는 best-effort 미구현).
    - `async create_instance(self, db_name, class_id, instance_data, branch)` (line 561): 인스턴스 생성
    - `async update_instance(self, db_name, class_id, instance_id, update_data, branch)` (line 572): 인스턴스 업데이트
    - `async delete_instance(self, db_name, class_id, instance_id, branch)` (line 586): 인스턴스 삭제
    - `async validate_relationships(self, db_name, ontology_data, branch, fix_issues)` (line 601): Validate ontology relationships against current schema (no write).
    - `async detect_circular_references(self, db_name, branch, include_new_ontology, max_cycle_depth)` (line 678): Detect circular references across ontology relationship graph (no write).
    - `async find_relationship_paths(self, db_name, start_entity, end_entity, max_depth, path_type, branch)` (line 750): Find relationship paths between entities in the ontology graph (no write).
    - `async analyze_relationship_network(self, db_name, branch)` (line 827): Analyze relationship network health and statistics (no write).
    - `convert_properties_to_relationships(self, ontology)` (line 922): 속성을 관계로 변환
    - `clear_cache(self, db_name)` (line 933): 캐시 초기화
    - `async ping(self)` (line 940): 서버 연결 상태 확인
    - `get_connection_info(self)` (line 949): 현재 연결 정보 반환
    - `async __aenter__(self)` (line 953): Async context manager entry
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 957): Async context manager exit

### `backend/oms/services/event_store.py`

### `backend/oms/services/ontology_deploy_outbox.py`
- **Functions**
  - `async run_ontology_deploy_outbox_worker(registry, poll_interval_seconds, batch_size, stop_event)` (line 124): no docstring
- **Classes**
  - `OntologyDeployOutboxPublisher` (line 18): no docstring
    - `__init__(self, registry, batch_size)` (line 19): no docstring
    - `_next_attempt_at(self, attempts)` (line 69): no docstring
    - `async flush_once(self)` (line 73): no docstring
    - `async maybe_purge(self)` (line 106): no docstring

### `backend/oms/services/ontology_deployment_registry.py`
- **Classes**
  - `OntologyDeployOutboxItem` (line 20): no docstring
  - `OntologyDeploymentRegistry` (line 34): Record ontology deployments in Postgres.
    - `async ensure_schema(self)` (line 37): no docstring
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, occurred_at)` (line 97): no docstring
    - `async record_deployment(self, db_name, proposal_id, source_branch, target_branch, approved_ontology_commit_id, merge_commit_id, deployed_by, definition_hash, status, metadata)` (line 141): no docstring
    - `async claim_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 214): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 284): no docstring
    - `async mark_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 299): no docstring
    - `async purge_outbox(self, retention_days, limit)` (line 320): no docstring

### `backend/oms/services/ontology_deployment_registry_v2.py`
- **Classes**
  - `OntologyDeployOutboxItem` (line 21): no docstring
  - `OntologyDeploymentRegistryV2` (line 35): Record ontology deployments in Postgres (v2 schema).
    - `_json_default(value)` (line 39): no docstring
    - `_maybe_decode_json(value)` (line 45): no docstring
    - `async ensure_schema(self)` (line 53): no docstring
    - `build_deploy_event_payload(deployment_id, db_name, proposal_id, target_branch, ontology_commit_id, snapshot_rid, deployed_by, gate_policy, health_summary, occurred_at)` (line 114): no docstring
    - `async record_deployment(self, db_name, target_branch, ontology_commit_id, snapshot_rid, proposal_id, status, gate_policy, health_summary, deployed_by, error, metadata)` (line 158): no docstring
    - `async claim_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 245): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 317): no docstring
    - `async mark_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 332): no docstring
    - `async purge_outbox(self, retention_days, limit)` (line 353): no docstring

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
  - `_normalize_spec(spec)` (line 84): no docstring
  - `_merge_payload_spec(payload)` (line 97): no docstring
  - `_extract_reference_values(value, keys, parent_is_ref)` (line 108): no docstring
  - `collect_reference_values(spec)` (line 121): no docstring
  - `check_required_fields(resource_type, spec)` (line 125): no docstring
  - `async find_missing_references(db_name, resource_type, payload, terminus, branch)` (line 130): no docstring
  - `_canonicalize_ref(raw)` (line 166): no docstring
  - `_is_primitive_reference(value)` (line 176): no docstring
  - `_validate_required_fields(resource_type, spec)` (line 186): no docstring
  - `_collect_required_field_issues(resource_type, spec)` (line 192): no docstring
  - `_collect_required_items_issues(items, item_name, name_keys)` (line 347): no docstring
  - `_collect_permission_policy_issues(policy)` (line 385): no docstring
  - `_validate_string_list(value, field_name)` (line 454): no docstring
  - `_append_spec_issue(issues, message, missing_fields, invalid_fields)` (line 464): no docstring
  - `async _reference_exists(terminus, resources, db_name, branch, ref_type, ref)` (line 483): no docstring
  - `async validate_resource(db_name, resource_type, payload, terminus, branch, expected_head_commit, strict)` (line 525): no docstring
- **Classes**
  - `ResourceSpecError` (line 16): Raised when resource spec is invalid or missing required fields.
  - `ResourceReferenceError` (line 20): Raised when resource spec references missing entities.

### `backend/oms/services/ontology_resources.py`
- **Functions**
  - `normalize_resource_type(value)` (line 46): no docstring
  - `_resource_doc_id(resource_type, resource_id)` (line 56): no docstring
  - `_localized_to_string(value)` (line 61): no docstring
- **Classes**
  - `OntologyResourceService` (line 86): CRUD for ontology resource instances stored in TerminusDB.
    - `__init__(self, terminus)` (line 89): no docstring
    - `async ensure_resource_schema(self, db_name, branch)` (line 93): no docstring
    - `async create_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 130): no docstring
    - `async update_resource(self, db_name, branch, resource_type, resource_id, payload)` (line 158): no docstring
    - `async delete_resource(self, db_name, branch, resource_type, resource_id)` (line 195): no docstring
    - `async get_resource(self, db_name, branch, resource_type, resource_id)` (line 213): no docstring
    - `async list_resources(self, db_name, branch, resource_type, limit, offset)` (line 227): no docstring
    - `_payload_to_document(self, resource_type, resource_id, payload, doc_id, is_create, existing)` (line 255): no docstring
    - `_document_to_payload(self, doc)` (line 304): no docstring

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
  - `_maybe_decode_json(value)` (line 39): no docstring
- **Classes**
  - `PullRequestStatus` (line 31): PR status constants
  - `PullRequestService` (line 48): Pull Request management service following SRP
    - `__init__(self, mvcc_manager, *args, **kwargs)` (line 64): Initialize PullRequestService
    - `async create_pull_request(self, db_name, source_branch, target_branch, title, description, author)` (line 78): Create a new pull request
    - `async get_branch_diff(self, db_name, source_branch, target_branch)` (line 181): Get diff between two branches using TerminusDB diff API
    - `async check_merge_conflicts(self, db_name, source_branch, target_branch)` (line 209): Check for potential merge conflicts
    - `async merge_pull_request(self, pr_id, merge_message, author)` (line 254): Merge a pull request using rebase strategy
    - `async get_pull_request(self, pr_id)` (line 352): Get pull request details
    - `async list_pull_requests(self, db_name, status, limit)` (line 401): List pull requests with optional filters
    - `async close_pull_request(self, pr_id, reason)` (line 454): Close a pull request without merging

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
  - `BaseTerminusService` (line 28): TerminusDB 기본 서비스
    - `__init__(self, connection_info)` (line 36): TerminusDB 서비스 초기화
    - `async _get_client(self)` (line 68): HTTP 클라이언트 가져오기 (lazy initialization)
    - `_branch_descriptor(self, branch)` (line 109): Build the TerminusDB descriptor path for a branch.
    - `async _authenticate(self)` (line 123): TerminusDB 인증 토큰 획득
    - `async _make_request(self, method, endpoint, data, params, headers, **kwargs)` (line 140): HTTP 요청 실행
    - `async connect(self, db_name)` (line 267): TerminusDB 연결
    - `async disconnect(self)` (line 284): 연결 종료
    - `async check_connection(self)` (line 293): 연결 상태 확인
    - `async __aenter__(self)` (line 301): 비동기 컨텍스트 매니저 진입
    - `async __aexit__(self, exc_type, exc_val, exc_tb)` (line 306): 비동기 컨텍스트 매니저 종료
    - `is_connected(self)` (line 310): 연결 상태 반환

### `backend/oms/services/terminus/database.py`
- **Classes**
  - `DatabaseService` (line 20): TerminusDB 데이터베이스 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 27): no docstring
    - `async database_exists(self, db_name)` (line 32): 데이터베이스 존재 여부 확인
    - `async ensure_db_exists(self, db_name, description)` (line 44): 데이터베이스 존재 확인 및 생성
    - `async create_database(self, db_name, description)` (line 62): 새 데이터베이스 생성
    - `async list_databases(self)` (line 103): 사용 가능한 데이터베이스 목록 조회
    - `async delete_database(self, db_name)` (line 195): 데이터베이스 삭제
    - `async get_database_info(self, db_name)` (line 227): 데이터베이스 상세 정보 조회
    - `clear_cache(self)` (line 256): 데이터베이스 캐시 초기화

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
  - `OntologyService` (line 25): TerminusDB 온톨로지 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 32): no docstring
    - `async disconnect(self)` (line 37): no docstring
    - `async create_ontology(self, db_name, ontology, branch)` (line 44): 새 온톨로지(클래스) 생성
    - `async update_ontology(self, db_name, ontology_id, ontology, branch)` (line 146): 온톨로지 업데이트
    - `async delete_ontology(self, db_name, ontology_id, branch)` (line 181): 온톨로지 삭제
    - `async get_ontology(self, db_name, ontology_id, branch)` (line 224): 특정 온톨로지 조회
    - `async list_ontologies(self, db_name, branch, limit, offset)` (line 265): 데이터베이스의 모든 온톨로지 목록 조회
    - `async ontology_exists(self, db_name, ontology_id, branch)` (line 339): 온톨로지 존재 여부 확인
    - `_create_property_schema(self, prop)` (line 361): 속성 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_create_relationship_schema(self, rel)` (line 379): 관계 스키마 생성 - TerminusDB 공식 패턴 준수
    - `_map_datatype_to_terminus(self, datatype)` (line 444): DataType을 TerminusDB 타입으로 매핑
    - `_parse_ontology_document(self, doc)` (line 487): TerminusDB 문서를 OntologyResponse로 파싱
    - `_map_terminus_to_datatype(self, terminus_type)` (line 616): TerminusDB 타입을 DataType으로 매핑

### `backend/oms/services/terminus/query.py`
- **Classes**
  - `QueryService` (line 16): TerminusDB 쿼리 실행 서비스
    - `_normalize_field_name(field)` (line 24): no docstring
    - `_coerce_scalar(value)` (line 34): no docstring
    - `_as_list(value)` (line 40): no docstring
    - `_try_float(value)` (line 48): no docstring
    - `_matches_filters(self, doc, filters)` (line 60): no docstring
    - `async execute_query(self, db_name, query_dict, branch)` (line 152): Execute a query against instance documents.
    - `async execute_sparql(self, db_name, sparql_query, limit, offset)` (line 312): SPARQL 쿼리 실행
    - `convert_to_woql(self, query_dict)` (line 383): 간단한 쿼리 딕셔너리를 WOQL 형식으로 변환

### `backend/oms/services/terminus/version_control.py`
- **Classes**
  - `VersionControlService` (line 20): TerminusDB 버전 관리 서비스
    - `__init__(self, *args, **kwargs)` (line 27): no docstring
    - `async disconnect(self)` (line 32): Close nested services before closing this client's HTTP resources.
    - `_is_origin_branch_missing_error(exc)` (line 51): TerminusDB can transiently report `OriginBranchDoesNotExist` right after DB creation.
    - `_is_branch_already_exists_error(exc)` (line 60): no docstring
    - `_is_transient_request_handler_error(exc)` (line 69): TerminusDB occasionally returns a 500 "Unexpected failure in request handler" right after DB creation.
    - `async list_branches(self, db_name)` (line 83): 데이터베이스의 모든 브랜치 목록 조회
    - `async create_branch(self, db_name, branch_name, source_branch, empty)` (line 131): 새 브랜치 생성
    - `async delete_branch(self, db_name, branch_name)` (line 206): 브랜치 삭제
    - `async checkout_branch(self, db_name, branch_name)` (line 242): TerminusDB는 stateless HTTP API이므로 'checkout'은 서버 상태를 바꾸지 않습니다.
    - `async reset_branch(self, db_name, branch_name, commit_id)` (line 252): 브랜치를 특정 커밋으로 리셋
    - `async get_commits(self, db_name, branch_name, limit, offset)` (line 293): 브랜치의 커밋 이력 조회
    - `async create_commit(self, db_name, branch_name, message, author)` (line 363): 새 커밋 생성
    - `async rebase_branch(self, db_name, branch_name, target_branch, message)` (line 425): 브랜치 리베이스
    - `async rebase(self, db_name, branch, onto, message)` (line 472): Router 호환 rebase API (branch -> onto).
    - `async squash_commits(self, db_name, branch_name, commit_id, message)` (line 483): 커밋 스쿼시 (여러 커밋을 하나로 합치기)
    - `async get_diff(self, db_name, from_ref, to_ref)` (line 527): 두 참조(브랜치/커밋) 간의 차이점 조회
    - `async diff(self, db_name, from_ref, to_ref)` (line 576): Router 호환 diff API: changes(리스트/딕트)만 반환.
    - `async commit(self, db_name, message, author, branch)` (line 583): Router 호환 commit API: commit_id 문자열 반환.
    - `async get_commit_history(self, db_name, branch, limit, offset)` (line 595): Router 호환 commit history API.
    - `async merge(self, db_name, source_branch, target_branch, strategy, author, message)` (line 605): Router 호환 merge API.
    - `_parse_commit_info(self, commit_data)` (line 658): 커밋 정보 파싱

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
  - `async main()` (line 1179): 메인 진입점
- **Classes**
  - `OntologyWorker` (line 63): 온톨로지 Command를 처리하는 워커
    - `__init__(self)` (line 66): no docstring
    - `async _wait_for_database_exists(self, db_name, expected, timeout_seconds)` (line 88): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 104): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 114): no docstring
    - `async initialize(self)` (line 118): 워커 초기화
    - `async process_command(self, command_data)` (line 215): Command 처리
    - `_is_retryable_error(exc)` (line 261): no docstring
    - `async handle_create_ontology(self, command_data)` (line 273): 온톨로지 생성 처리
    - `async handle_update_ontology(self, command_data)` (line 454): 온톨로지 업데이트 처리
    - `async handle_delete_ontology(self, command_data)` (line 629): 온톨로지 삭제 처리
    - `async handle_create_database(self, command_data)` (line 775): 데이터베이스 생성 처리
    - `async handle_delete_database(self, command_data)` (line 838): 데이터베이스 삭제 처리
    - `_to_domain_envelope(self, event, kafka_topic)` (line 889): no docstring
    - `async publish_event(self, event)` (line 932): 이벤트 발행 (Event Sourcing: S3/MinIO -> EventPublisher -> Kafka).
    - `async publish_failure_event(self, command_data, error)` (line 944): 실패 이벤트 발행
    - `async run(self)` (line 965): 메인 실행 루프
    - `async shutdown(self)` (line 1158): 워커 종료

## perf

### `backend/perf/cleanup_perf_databases.py`
- **Functions**
  - `_load_repo_dotenv()` (line 29): no docstring
  - `_postgres_dsn_candidates()` (line 52): no docstring
  - `_bff_base_url()` (line 74): no docstring
  - `_admin_token()` (line 81): no docstring
  - `_extract_command_id(payload)` (line 89): no docstring
  - `async _connect_postgres()` (line 103): no docstring
  - `async _fetch_db_expected_seq(conn, db_name)` (line 115): no docstring
  - `async _wait_for_command(client, base_url, command_id, timeout_seconds)` (line 133): no docstring
  - `async _list_databases(client, base_url)` (line 156): no docstring
  - `async _delete_database(client, base_url, db_name, expected_seq)` (line 177): no docstring
  - `_matches_any_prefix(name, prefixes)` (line 196): no docstring
  - `async main()` (line 203): no docstring

## pipeline_scheduler

### `backend/pipeline_scheduler/main.py`
- **Functions**
  - `async main()` (line 19): no docstring

## pipeline_worker

### `backend/pipeline_worker/__init__.py`

### `backend/pipeline_worker/main.py`
- **Functions**
  - `_resolve_code_version()` (line 100): no docstring
  - `_is_sensitive_conf_key(key)` (line 108): no docstring
  - `_resolve_lakefs_repository()` (line 113): no docstring
  - `_resolve_pipeline_execution_mode(pipeline_type, definition)` (line 195): Normalize execution semantics into one of:
  - `_resolve_incremental_config(definition)` (line 230): no docstring
  - `_resolve_watermark_column(incremental, metadata)` (line 235): no docstring
  - `_max_watermark_from_snapshots(input_snapshots, watermark_column)` (line 246): no docstring
  - `_watermark_values_match(left, right)` (line 277): no docstring
  - `_collect_watermark_keys_from_snapshots(input_snapshots, watermark_column, watermark_value)` (line 286): no docstring
  - `_collect_input_commit_map(input_snapshots)` (line 318): no docstring
  - `_inputs_diff_empty(input_snapshots)` (line 330): no docstring
  - `_resolve_execution_semantics(job, definition)` (line 341): no docstring
  - `_resolve_output_format(definition, output_metadata)` (line 345): no docstring
  - `_resolve_partition_columns(definition, output_metadata)` (line 360): no docstring
  - `_is_data_object(key)` (line 3949): no docstring
  - `_schema_from_dataframe(frame)` (line 3960): no docstring
  - `_hash_schema_columns(columns)` (line 3968): no docstring
  - `_spark_type_to_xsd(data_type)` (line 3972): no docstring
  - `_list_part_files(path, extensions)` (line 3984): no docstring
  - `async main()` (line 3999): no docstring
- **Classes**
  - `PipelineLockError` (line 120): no docstring
  - `PipelineLock` (line 124): no docstring
    - `__init__(self, redis_client, key, token, ttl_seconds, renew_seconds)` (line 125): no docstring
    - `async start(self)` (line 142): no docstring
    - `raise_if_lost(self)` (line 149): no docstring
    - `async release(self)` (line 153): no docstring
    - `async _renew_loop(self)` (line 170): no docstring
    - `async _extend(self)` (line 186): no docstring
  - `PipelineWorker` (line 392): no docstring
    - `__init__(self)` (line 393): no docstring
    - `_build_error_payload(self, message, errors, code, category, status_code, external_code, stage, job, pipeline_id, node_id, mode, context)` (line 435): no docstring
    - `async initialize(self)` (line 481): no docstring
    - `async close(self)` (line 568): no docstring
    - `async run(self)` (line 606): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 782): no docstring
    - `async _send_to_dlq(self, msg, stage, error, payload_text, payload_obj, job, attempt_count)` (line 795): no docstring
    - `async _best_effort_record_invalid_job(self, payload, error)` (line 843): no docstring
    - `async _resolve_pipeline_id_from_fields(self, db_name, pipeline_id, branch)` (line 890): no docstring
    - `async _execute_job(self, job)` (line 908): no docstring
    - `async _maybe_enqueue_objectify_job(self, dataset, version)` (line 2349): no docstring
    - `async _materialize_output_dataframe(self, df, artifact_bucket, prefix, write_mode, file_prefix, file_format, partition_cols)` (line 2405): no docstring
    - `_row_hash_expr(self, df)` (line 2479): no docstring
    - `_apply_watermark_filter(self, df, watermark_column, watermark_after, watermark_keys)` (line 2492): no docstring
    - `_collect_watermark_keys(self, df, watermark_column, watermark_value)` (line 2515): no docstring
    - `async _load_input_dataframe(self, db_name, metadata, temp_dirs, branch, node_id, input_snapshots, previous_commit_id, use_lakefs_diff, watermark_column, watermark_after, watermark_keys)` (line 2541): no docstring
    - `_preview_sampling_seed(self, job_id)` (line 2800): no docstring
    - `_resolve_sampling_strategy(self, metadata, preview_meta)` (line 2804): no docstring
    - `_attach_sampling_snapshot(self, input_snapshots, node_id, sampling_strategy)` (line 2820): no docstring
    - `_normalize_sampling_fraction(self, value, field)` (line 2832): no docstring
    - `_apply_sampling_strategy(self, df, sampling_strategy, node_id, seed)` (line 2841): no docstring
    - `_strip_commit_prefix(self, key, commit_id)` (line 2886): no docstring
    - `async _list_lakefs_diff_paths(self, repository, ref, since, prefix, node_id)` (line 2892): no docstring
    - `async _load_parquet_keys_dataframe(self, bucket, keys, temp_dirs, prefix)` (line 2938): no docstring
    - `async _load_media_prefix_dataframe(self, bucket, key, node_id)` (line 2972): Treat the artifact_key as an unstructured/media prefix.
    - `async _resolve_pipeline_id(self, job)` (line 3020): no docstring
    - `_collect_spark_conf(self)` (line 3039): no docstring
    - `_build_input_commit_payload(self, input_snapshots)` (line 3055): no docstring
    - `async _acquire_pipeline_lock(self, job)` (line 3079): no docstring
    - `_validate_required_subgraph(self, nodes, incoming, required_node_ids)` (line 3110): no docstring
    - `_validate_definition(self, definition, require_output)` (line 3126): no docstring
    - `_split_expectation_columns(column)` (line 3267): no docstring
    - `_coerce_pk_columns(self, value)` (line 3270): no docstring
    - `_collect_pk_columns(self, *candidates)` (line 3310): no docstring
    - `_match_output_declaration(output, node_id, output_name)` (line 3321): no docstring
    - `_normalize_pk_semantics(value)` (line 3344): no docstring
    - `_is_truthy(value)` (line 3361): no docstring
    - `_resolve_pk_semantics(self, execution_semantics, definition, output_metadata)` (line 3370): no docstring
    - `_resolve_delete_column(self, definition, output_metadata)` (line 3404): no docstring
    - `_validate_pk_semantics(df, pk_semantics, pk_columns, delete_column)` (line 3419): no docstring
    - `_resolve_pk_columns(self, definition, output_metadata, output_name, output_node_id, declared_outputs)` (line 3438): no docstring
    - `_build_expectations_with_pk(self, definition, output_metadata, output_name, output_node_id, declared_outputs, pk_semantics, delete_column, pk_columns, available_columns)` (line 3482): no docstring
    - `_validate_expectations(self, df, expectations)` (line 3536): no docstring
    - `_validate_schema_contract(self, df, contract)` (line 3598): no docstring
    - `async _load_artifact_dataframe(self, bucket, key, temp_dirs)` (line 3616): no docstring
    - `async _load_prefix_dataframe(self, bucket, prefix, temp_dirs)` (line 3627): no docstring
    - `async _download_object_to_path(self, bucket, key, local_path)` (line 3667): no docstring
    - `async _download_object(self, bucket, key, temp_dirs, temp_dir)` (line 3676): no docstring
    - `_read_local_file(self, path)` (line 3692): no docstring
    - `_load_excel_path(self, path)` (line 3703): no docstring
    - `_load_json_path(self, path)` (line 3709): no docstring
    - `_empty_dataframe(self)` (line 3724): no docstring
    - `_apply_transform(self, metadata, inputs, parameters)` (line 3727): no docstring
    - `_validate_schema_checks(self, df, checks, node_id)` (line 3897): no docstring

## projection_worker

### `backend/projection_worker/__init__.py`

### `backend/projection_worker/main.py`
- **Functions**
  - `async main()` (line 2502): 메인 함수
- **Classes**
  - `_InProgressLeaseError` (line 61): Raised when another worker holds the processed_events lease for this event.
  - `ProjectionWorker` (line 65): Instance와 Ontology 이벤트를 Elasticsearch에 프로젝션하는 워커
    - `__init__(self)` (line 72): no docstring
    - `_is_es_version_conflict(error)` (line 119): no docstring
    - `_parse_sequence(value)` (line 127): no docstring
    - `_normalize_localized_field(value, default_lang)` (line 136): no docstring
    - `_normalize_ontology_properties(self, properties, default_lang)` (line 146): no docstring
    - `_normalize_ontology_relationships(self, relationships, default_lang)` (line 183): no docstring
    - `_extract_envelope_metadata(event_data)` (line 232): no docstring
    - `async _record_es_side_effect(self, event_id, event_data, db_name, index_name, doc_id, operation, status, record_lineage, skip_reason, error, extra_metadata)` (line 250): Record projection side-effects for provenance (lineage) + audit.
    - `async _heartbeat_loop(self, handler, event_id)` (line 344): no docstring
    - `async _consumer_call(self, func, *args, **kwargs)` (line 354): no docstring
    - `async initialize(self)` (line 358): 워커 초기화
    - `async _setup_indices(self)` (line 447): 매핑 파일 로드 (인덱스는 DB별로 동적 생성)
    - `async _ensure_index_exists(self, db_name, index_type, branch)` (line 459): 특정 데이터베이스의 인덱스가 존재하는지 확인하고 없으면 생성
    - `async _load_mapping(self, filename)` (line 538): 매핑 파일 로드
    - `async run(self)` (line 552): 메인 실행 루프
    - `async _process_event(self, msg)` (line 592): 이벤트 처리
    - `async _handle_instance_event(self, event_data)` (line 711): 인스턴스 이벤트 처리
    - `async _handle_ontology_event(self, event_data)` (line 731): 온톨로지 이벤트 처리
    - `async _handle_instance_created(self, instance_data, event_id, event_data)` (line 755): 인스턴스 생성 이벤트 처리
    - `async _handle_instance_updated(self, instance_data, event_id, event_data)` (line 909): 인스턴스 업데이트 이벤트 처리
    - `async _handle_instance_deleted(self, instance_data, event_id, event_data)` (line 1084): 인스턴스 삭제 이벤트 처리
    - `async _handle_ontology_class_created(self, ontology_data, event_id, event_data)` (line 1332): 온톨로지 클래스 생성 이벤트 처리
    - `async _handle_ontology_class_updated(self, ontology_data, event_id, event_data)` (line 1515): 온톨로지 클래스 업데이트 이벤트 처리
    - `async _handle_ontology_class_deleted(self, ontology_data, event_id, event_data)` (line 1714): 온톨로지 클래스 삭제 이벤트 처리
    - `async _handle_database_created(self, db_data, event_id, event_data)` (line 1969): 데이터베이스 생성 이벤트 처리
    - `async _handle_database_deleted(self, db_data, event_id, event_data)` (line 2025): 데이터베이스 삭제 이벤트 처리
    - `async _get_class_label(self, class_id, db_name, branch)` (line 2099): Redis에서 클래스 라벨 조회 (Cache Stampede 방지)
    - `async _get_class_label_fallback(self, class_id, db_name, branch)` (line 2205): 락 획득 실패 시 fallback 조회 (성능보다 안정성 우선)
    - `get_cache_efficiency_metrics(self)` (line 2241): 캐시 효율성 및 락 경합 메트릭 반환
    - `log_cache_metrics(self)` (line 2294): 캐시 메트릭을 로그로 출력
    - `async _cache_class_label(self, class_id, label, db_name, branch)` (line 2313): 클래스 라벨을 Redis에 캐싱
    - `_normalize_properties(self, properties)` (line 2328): 속성을 검색 최적화된 형태로 정규화
    - `_is_transient_infra_error(error)` (line 2340): Return True for errors that are expected to recover via retry (e.g. ES outage).
    - `async _handle_retry(self, msg, error)` (line 2363): 재시도 처리
    - `async _send_to_dlq(self, msg, error)` (line 2422): 실패한 메시지를 DLQ로 전송
    - `async _shutdown(self)` (line 2477): 워커 종료

## quick_performance_test.py

### `backend/quick_performance_test.py`
- **Functions**
  - `async test_event_sourcing_complete()` (line 14): Test the complete Event Sourcing flow with the fixed field name

## quick_production_test.py

### `backend/quick_production_test.py`
- **Functions**
  - `async quick_production_test()` (line 11): Quick test to verify production system works

## run_coverage_report.py

### `backend/run_coverage_report.py`
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

## run_message_relay_local.py

### `backend/run_message_relay_local.py`
- **Functions**
  - `async main()` (line 25): EventPublisher 로컬 실행

## scripts

### `backend/scripts/backfill_lineage.py`
- **Functions**
  - `_parse_dt(value)` (line 22): no docstring
  - `async _run_queue(limit, db_name)` (line 32): no docstring
  - `async _run_replay(from_dt, to_dt, limit)` (line 68): no docstring
  - `async main()` (line 103): no docstring

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

### `backend/scripts/processed_event_registry_smoke.py`
- **Functions**
  - `async _main()` (line 18): no docstring

### `backend/scripts/single_source_of_truth_audit.py`
- **Functions**
  - `parse_requirements_txt(file_path)` (line 30): Parse requirements.txt file and extract dependencies with versions
  - `parse_pyproject_toml(file_path)` (line 55): Parse pyproject.toml dependencies
  - `check_duplicate_dependencies()` (line 94): Check for duplicate dependency declarations across files
  - `check_version_consistency()` (line 140): Check for version inconsistencies across files
  - `check_single_source_compliance()` (line 176): Verify that all services use only -e ../shared in requirements.txt
  - `main()` (line 212): Main audit execution

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
  - `async main()` (line 351): no docstring
- **Classes**
  - `SearchProjectionWorker` (line 33): no docstring
    - `__init__(self)` (line 34): no docstring
    - `async initialize(self)` (line 53): no docstring
    - `async close(self)` (line 92): no docstring
    - `async run(self)` (line 109): no docstring
    - `async _index_event(self, envelope)` (line 241): no docstring
    - `async _heartbeat_loop(self, handler, event_id)` (line 259): no docstring
    - `async _send_to_dlq(self, envelope, raw_payload, error, attempt_count)` (line 272): no docstring
    - `_is_retryable_error(exc)` (line 330): no docstring

## shared

### `backend/shared/__init__.py`

### `backend/shared/config/__init__.py`
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
  - `AppConfig` (line 10): SPICE HARVESTER 애플리케이션 전체 설정 중앙 관리 클래스
    - `get_instance_command_key(db_name, command_id)` (line 49): 인스턴스 Command S3 키 생성
    - `get_instance_latest_key(db_name, instance_id)` (line 54): 인스턴스 최신 상태 S3 키 생성 (deprecated - 순수 append-only로 변경됨)
    - `get_command_status_key(command_id)` (line 62): Command 상태 Redis 키 생성
    - `get_command_result_key(command_id)` (line 67): Command 결과 Redis 키 생성
    - `get_command_status_pattern()` (line 72): 모든 Command 상태 키 패턴
    - `get_class_label_key(db_name, class_id, branch)` (line 77): 클래스 라벨 캐시 Redis 키 생성 (branch-aware).
    - `get_user_session_key(user_id)` (line 84): 사용자 세션 Redis 키 생성
    - `get_websocket_connection_key(client_id)` (line 89): WebSocket 연결 Redis 키 생성
    - `get_instances_index_name(db_name, version)` (line 98): 인스턴스 Elasticsearch 인덱스 이름 생성
    - `get_ontologies_index_name(db_name, version)` (line 104): 온톨로지 Elasticsearch 인덱스 이름 생성
    - `get_oms_url()` (line 113): OMS 서비스 URL
    - `get_bff_url()` (line 119): BFF 서비스 URL
    - `get_funnel_url()` (line 125): Funnel 서비스 URL
    - `get_ontology_writeback_branch(cls, db_name)` (line 152): no docstring
    - `validate_config(cls)` (line 193): 설정값들의 유효성 검증
    - `get_all_topics(cls)` (line 222): 모든 Kafka 토픽 목록 반환
    - `get_config_summary(cls)` (line 240): 현재 설정 요약 반환 (디버깅용)

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
  - `sanitize_index_name(name)` (line 12): Elasticsearch 인덱스 이름 규칙에 맞게 문자열을 정제합니다.
  - `_branch_overlay_token(branch)` (line 60): Stable, collision-resistant token for ES branch overlay indices.
  - `get_instances_index_name(db_name, version, branch)` (line 75): 인스턴스 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
  - `get_ontologies_index_name(db_name, version, branch)` (line 108): 온톨로지 데이터를 위한 Elasticsearch 인덱스 이름을 생성합니다.
  - `get_index_alias_name(index_name)` (line 142): 인덱스의 별칭(alias) 이름을 생성합니다.
  - `_coerce_int_env(name, default, min_value)` (line 168): no docstring
  - `get_default_index_settings()` (line 181): Return default ES index settings with dev-safe replica defaults.

### `backend/shared/config/service_config.py`
- **Functions**
  - `get_oms_url()` (line 617): Get OMS URL - convenience function.
  - `get_bff_url()` (line 622): Get BFF URL - convenience function.
  - `get_funnel_url()` (line 627): Get Funnel URL - convenience function.
- **Classes**
  - `ServiceConfig` (line 18): Centralized service configuration management.
    - `get_oms_port()` (line 34): Get OMS (Ontology Management Service) port from environment or default.
    - `get_bff_port()` (line 39): Get BFF (Backend for Frontend) port from environment or default.
    - `get_funnel_port()` (line 44): Get Funnel service port from environment or default.
    - `get_oms_host()` (line 49): Get OMS host from environment or default.
    - `get_bff_host()` (line 55): Get BFF host from environment or default.
    - `get_funnel_host()` (line 61): Get Funnel host from environment or default.
    - `get_oms_url()` (line 67): Get complete OMS URL from environment or construct from host/port.
    - `get_bff_url()` (line 85): Get complete BFF URL from environment or construct from host/port.
    - `get_funnel_url()` (line 103): Get complete Funnel URL from environment or construct from host/port.
    - `get_terminus_url()` (line 121): Get TerminusDB URL from environment or default.
    - `get_postgres_url()` (line 131): Get PostgreSQL connection URL from environment or default.
    - `get_kafka_bootstrap_servers()` (line 146): Get Kafka bootstrap servers from environment or default.
    - `get_redis_host()` (line 160): Get Redis host from environment or default.
    - `get_redis_port()` (line 166): Get Redis port from environment or default.
    - `get_redis_url()` (line 171): Get Redis connection URL from environment or construct from host/port.
    - `get_elasticsearch_host()` (line 186): Get Elasticsearch host from environment or default.
    - `get_elasticsearch_port()` (line 194): Get Elasticsearch port from environment or default.
    - `get_elasticsearch_url()` (line 199): Get Elasticsearch base URL from environment or construct from host/port.
    - `is_docker_environment()` (line 208): Check if running in Docker environment.
    - `get_minio_endpoint()` (line 221): Get MinIO/S3 endpoint URL.
    - `get_minio_access_key()` (line 229): Get MinIO/S3 access key.
    - `get_minio_secret_key()` (line 234): Get MinIO/S3 secret key.
    - `get_lakefs_api_url()` (line 239): Get lakeFS API base URL.
    - `get_lakefs_s3_endpoint()` (line 255): Get lakeFS S3 Gateway endpoint URL.
    - `get_service_url(service_name)` (line 266): Get URL for a specific service by name.
    - `get_all_service_urls()` (line 292): Get all service URLs as a dictionary.
    - `validate_configuration()` (line 302): Validate that all required configuration is present.
    - `use_https()` (line 325): Check if HTTPS should be used for service communication.
    - `is_production()` (line 335): Check if running in production environment.
    - `is_debug_endpoints_enabled()` (line 340): Enable opt-in debug endpoints (never on by default).
    - `get_ssl_cert_path()` (line 345): Get SSL certificate path from environment.
    - `get_ssl_key_path()` (line 356): Get SSL key path from environment.
    - `get_ssl_ca_path()` (line 367): Get SSL CA certificate path from environment.
    - `verify_ssl()` (line 378): Check if SSL certificate verification should be enabled.
    - `get_protocol()` (line 393): Get the protocol to use for service communication.
    - `get_ssl_config()` (line 403): Get complete SSL configuration as a dictionary.
    - `get_client_ssl_config()` (line 430): Get SSL configuration for HTTP clients (requests, httpx).
    - `get_cors_origins()` (line 449): Get CORS allowed origins from environment variables.
    - `_get_environment_default_origins()` (line 504): Get environment-based default CORS origins.
    - `_get_dev_cors_origins()` (line 523): Get development CORS origins for common frontend ports.
    - `get_cors_config()` (line 550): Get complete CORS configuration for FastAPI middleware.
    - `is_cors_enabled()` (line 590): Check if CORS should be enabled.
    - `get_cors_debug_info()` (line 600): Get CORS configuration debug information.

### `backend/shared/config/settings.py`
- **Functions**
  - `get_settings()` (line 510): Get the global settings instance
  - `reload_settings()` (line 522): Reload settings from environment (useful for testing)
- **Classes**
  - `Environment` (line 21): Application environment types
  - `DatabaseSettings` (line 28): Database configuration settings
    - `get_terminus_url(cls, v)` (line 58): no docstring
    - `get_terminus_user(cls, v)` (line 63): no docstring
    - `get_terminus_password(cls, v)` (line 68): no docstring
    - `get_terminus_account(cls, v)` (line 73): no docstring
    - `postgres_url(self)` (line 169): Construct PostgreSQL connection URL
    - `kafka_servers(self)` (line 174): Get Kafka bootstrap servers
    - `elasticsearch_url(self)` (line 181): Construct Elasticsearch URL with authentication
    - `redis_url(self)` (line 188): Construct Redis URL
  - `ServiceSettings` (line 195): Service configuration settings
    - `oms_base_url(self)` (line 264): Construct OMS base URL
    - `bff_base_url(self)` (line 270): Construct BFF base URL
    - `funnel_base_url(self)` (line 276): Construct Funnel base URL
    - `cors_origins_list(self)` (line 282): Parse CORS origins from JSON string
  - `StorageSettings` (line 290): Storage configuration settings
    - `use_ssl(self)` (line 335): Determine if SSL should be used based on endpoint URL
  - `CacheSettings` (line 340): Cache and TTL configuration settings
  - `SecuritySettings` (line 365): Security configuration settings
  - `PerformanceSettings` (line 390): Performance and optimization settings
  - `TestSettings` (line 421): Test environment configuration
  - `GoogleSheetsSettings` (line 441): Google Sheets integration settings
  - `ApplicationSettings` (line 461): Main application settings - aggregates all other settings
    - `is_development(self)` (line 492): Check if running in development mode
    - `is_production(self)` (line 497): Check if running in production mode
    - `is_test(self)` (line 502): Check if running in test mode

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
  - `async get_settings_dependency()` (line 32): FastAPI dependency to get application settings
  - `async get_storage_service(container)` (line 43): FastAPI dependency to get StorageService instance
  - `async get_lakefs_storage_service(container)` (line 66): FastAPI dependency to get LakeFSStorageService instance (S3 gateway via lakeFS).
  - `async get_redis_service(container)` (line 80): FastAPI dependency to get RedisService instance
  - `async get_elasticsearch_service(container)` (line 99): FastAPI dependency to get ElasticsearchService instance
  - `async get_lineage_store(container)` (line 117): FastAPI dependency to get LineageStore instance.
  - `async get_audit_log_store(container)` (line 126): FastAPI dependency to get AuditLogStore instance.
  - `async get_llm_gateway(container)` (line 134): FastAPI dependency to get LLMGateway instance.
  - `register_core_services(container)` (line 155): Register all core services with the container
  - `async health_check_core_services(container)` (line 179): Perform health check on all core services

### `backend/shared/dependencies/type_inference.py`
- **Functions**
  - `configure_type_inference_service(service)` (line 14): Configure the type inference service implementation.
  - `get_type_inference_service()` (line 28): Get the configured type inference service.
  - `type_inference_dependency()` (line 49): FastAPI dependency function for type inference service.
  - `reset_type_inference_service()` (line 63): Reset the type inference service (mainly for testing).

### `backend/shared/errors/enterprise_catalog.py`
- **Functions**
  - `_normalize_subsystem(service_name)` (line 129): no docstring
  - `is_external_code(value)` (line 1215): no docstring
  - `_resolve_http_status(spec, status_code, prefer_status_code)` (line 1221): no docstring
  - `_resolve_retryable(spec, retryable_hint)` (line 1234): no docstring
  - `_resolve_action(spec)` (line 1246): no docstring
  - `_resolve_owner(spec)` (line 1250): no docstring
  - `resolve_enterprise_error(service_name, code, category, status_code, external_code, retryable_hint, prefer_status_code)` (line 1254): no docstring
  - `resolve_objectify_error(error)` (line 1295): no docstring
  - `_normalize_objectify_error_key(error)` (line 1322): no docstring
- **Classes**
  - `EnterpriseSeverity` (line 12): no docstring
  - `EnterpriseDomain` (line 19): no docstring
  - `EnterpriseClass` (line 35): no docstring
  - `EnterpriseAction` (line 50): no docstring
  - `EnterpriseOwner` (line 63): no docstring
  - `EnterpriseSubsystem` (line 69): no docstring
  - `EnterpriseErrorSpec` (line 81): no docstring
  - `EnterpriseError` (line 94): no docstring
    - `to_dict(self)` (line 108): no docstring

### `backend/shared/errors/error_envelope.py`
- **Functions**
  - `_normalize_origin(service_name, origin)` (line 31): no docstring
  - `_derive_category_code(enterprise)` (line 44): no docstring
  - `build_error_envelope(service_name, message, detail, code, category, status_code, errors, context, external_code, objectify_error, enterprise, origin, request_id, trace_id, prefer_status_code)` (line 50): no docstring

### `backend/shared/errors/error_response.py`
- **Functions**
  - `_get_request_id(request)` (line 41): no docstring
  - `_get_origin(request, service_name)` (line 45): no docstring
  - `_normalize_message(detail)` (line 58): no docstring
  - `_extract_upstream_metadata(body)` (line 70): no docstring
  - `_extract_external_code(detail)` (line 85): no docstring
  - `_classify_upstream_url(url, status_code)` (line 100): no docstring
  - `_classify_db_error(exc)` (line 122): no docstring
  - `_build_payload(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 143): no docstring
  - `_build_response(request, service_name, code, category, status_code, message, detail, errors, context, external_code)` (line 172): no docstring
  - `_resolve_validation_error(exc)` (line 200): no docstring
  - `install_error_handlers(app, service_name, validation_status)` (line 207): no docstring

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
  - `rate_limit(requests, window, strategy, cost)` (line 380): Rate limiting decorator for FastAPI endpoints
  - `install_rate_limit_headers_middleware(app)` (line 533): no docstring
  - `async get_rate_limiter()` (line 572): Get or create global rate limiter instance
- **Classes**
  - `TokenBucket` (line 22): Token Bucket algorithm implementation for rate limiting
    - `__init__(self, redis_client, capacity, refill_rate, key_prefix, fail_open)` (line 32): Initialize Token Bucket
    - `async consume(self, key, tokens)` (line 55): Try to consume tokens from the bucket
  - `LocalTokenBucket` (line 150): In-memory token bucket for degraded mode when Redis is unavailable.
    - `__init__(self, capacity, refill_rate, max_entries)` (line 153): no docstring
    - `_evict_if_needed(self)` (line 160): no docstring
    - `async consume(self, key, tokens)` (line 168): no docstring
  - `RateLimiter` (line 202): Rate limiting middleware for FastAPI
    - `__init__(self, redis_url)` (line 208): Initialize rate limiter
    - `async initialize(self)` (line 225): Initialize Redis connection
    - `async close(self)` (line 253): Close Redis connection
    - `get_bucket(self, bucket_type, capacity, refill_rate)` (line 258): Get or create a token bucket
    - `get_local_bucket(self, bucket_type, capacity, refill_rate)` (line 283): no docstring
    - `get_client_id(self, request, strategy)` (line 293): Get client identifier based on strategy
    - `async check_rate_limit(self, request, capacity, refill_rate, strategy, tokens)` (line 328): Check if request should be rate limited
  - `RateLimitPresets` (line 546): Common rate limit configurations

### `backend/shared/models/__init__.py`

### `backend/shared/models/ai.py`
- **Classes**
  - `AIQueryMode` (line 19): no docstring
  - `AIQueryTool` (line 25): no docstring
  - `AIQueryPlan` (line 31): LLM-produced query plan.
    - `_validate_shape(self)` (line 48): no docstring
  - `AIQueryRequest` (line 58): no docstring
  - `AIAnswer` (line 67): no docstring
  - `AIQueryResponse` (line 74): no docstring

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
  - `CommandStatus` (line 50): 명령 상태
  - `BaseCommand` (line 60): 기본 명령 모델
  - `OntologyCommand` (line 79): 온톨로지 관련 명령
    - `__init__(self, **data)` (line 84): no docstring
  - `PropertyCommand` (line 90): 속성 관련 명령
    - `__init__(self, **data)` (line 95): no docstring
  - `RelationshipCommand` (line 101): 관계 관련 명령
    - `__init__(self, **data)` (line 107): no docstring
  - `DatabaseCommand` (line 113): 데이터베이스 관련 명령
    - `__init__(self, **data)` (line 115): no docstring
  - `BranchCommand` (line 121): 브랜치 관련 명령
    - `__init__(self, **data)` (line 125): no docstring
  - `InstanceCommand` (line 131): 인스턴스 관련 명령
    - `__init__(self, **data)` (line 138): no docstring
  - `CommandResult` (line 151): 명령 실행 결과

### `backend/shared/models/common.py`
- **Classes**
  - `DataType` (line 13): Data type enumeration
    - `from_python_type(cls, py_type)` (line 56): Convert Python type to DataType
    - `is_numeric(cls, data_type)` (line 67): Check if data type is numeric
    - `is_temporal(cls, data_type)` (line 85): Check if data type is temporal
    - `validate_value(self, value)` (line 100): Validate if value matches this data type
    - `is_complex_type(cls, data_type)` (line 124): Check if data type is complex
    - `get_base_type(cls, data_type)` (line 142): Get base type for complex types
  - `Cardinality` (line 193): Cardinality enumeration
    - `is_valid(cls, value)` (line 204): Check if value is a valid cardinality
  - `QueryOperator` (line 210): Query operator definition
    - `can_apply_to(self, data_type)` (line 218): Check if operator can apply to data type

### `backend/shared/models/config.py`
- **Classes**
  - `ConnectionConfig` (line 11): Database connection configuration
    - `__post_init__(self)` (line 25): Post-initialization validation
    - `from_env(cls)` (line 44): Create ConnectionConfig from environment variables
    - `to_dict(self)` (line 71): Convert to dictionary
  - `AsyncConnectionInfo` (line 88): Async connection information
    - `__post_init__(self)` (line 97): Post-initialization setup
    - `mark_used(self)` (line 104): Mark connection as used
    - `can_create_connection(self)` (line 108): Check if new connection can be created
    - `to_dict(self)` (line 112): Convert to dictionary

### `backend/shared/models/event_envelope.py`
- **Classes**
  - `EventEnvelope` (line 22): Canonical event envelope.
    - `_normalize_datetime(value)` (line 50): no docstring
    - `from_command(cls, command, actor, event_type, kafka_topic, metadata)` (line 56): no docstring
    - `from_base_event(cls, event, kafka_topic, metadata)` (line 92): no docstring
    - `from_connector_update(cls, source_type, source_id, cursor, previous_cursor, sequence_number, occurred_at, event_type, actor, kafka_topic, data, metadata)` (line 127): Build a canonical connector update envelope.
    - `as_kafka_key(self)` (line 199): no docstring
    - `as_json(self)` (line 203): no docstring

### `backend/shared/models/events.py`
- **Classes**
  - `EventType` (line 14): 이벤트 유형
  - `BaseEvent` (line 55): 기본 이벤트 모델
  - `OntologyEvent` (line 73): 온톨로지 관련 이벤트
    - `__init__(self, **data)` (line 79): no docstring
  - `PropertyEvent` (line 88): 속성 관련 이벤트
    - `__init__(self, **data)` (line 94): no docstring
  - `RelationshipEvent` (line 102): 관계 관련 이벤트
    - `__init__(self, **data)` (line 109): no docstring
  - `DatabaseEvent` (line 117): 데이터베이스 관련 이벤트
    - `__init__(self, **data)` (line 121): no docstring
  - `BranchEvent` (line 129): 브랜치 관련 이벤트
    - `__init__(self, **data)` (line 134): no docstring
  - `InstanceEvent` (line 142): 인스턴스 관련 이벤트
    - `__init__(self, **data)` (line 151): no docstring
  - `CommandFailedEvent` (line 163): 명령 실패 이벤트
    - `__init__(self, **data)` (line 170): no docstring

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
  - `GraphQueryRequest` (line 23): Request model for multi-hop graph queries.
  - `GraphNode` (line 44): Graph node with ES document reference.
  - `GraphEdge` (line 59): Graph edge between nodes.
  - `GraphQueryResponse` (line 68): Response model for graph queries.
  - `SimpleGraphQueryRequest` (line 81): Request for simple single-class queries.

### `backend/shared/models/i18n.py`

### `backend/shared/models/lineage.py`
- **Classes**
  - `LineageNode` (line 20): no docstring
  - `LineageEdge` (line 31): no docstring
  - `LineageGraph` (line 43): no docstring

### `backend/shared/models/objectify_job.py`
- **Classes**
  - `ObjectifyJob` (line 13): no docstring
    - `_validate_inputs(self)` (line 37): no docstring

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
    - `validate_name(cls, v)` (line 210): Validate property name
    - `validate_type(cls, v)` (line 218): Validate property type
    - `validate_label(cls, v)` (line 226): no docstring
    - `validate_description(cls, v)` (line 231): no docstring
    - `validate_value(self, value)` (line 234): Validate property value
    - `is_class_reference(self)` (line 262): Check if this property is a class reference (ObjectProperty)
    - `to_relationship(self)` (line 299): Convert property to relationship format
  - `OntologyCreateRequest` (line 328): Request model for creating ontology
    - `validate_id(cls, v)` (line 344): Validate ID format (optional).
    - `validate_label(cls, v)` (line 354): Validate label is not empty (string or language map).
    - `validate_properties(cls, v)` (line 368): Validate properties don't have duplicate names
    - `validate_relationships(cls, v)` (line 378): Validate relationships don't have duplicate predicates
  - `OntologyUpdateRequest` (line 387): Request model for updating ontology
    - `validate_label(cls, v)` (line 400): no docstring
    - `validate_description(cls, v)` (line 405): no docstring
    - `validate_properties(cls, v)` (line 410): Validate properties don't have duplicate names
    - `validate_relationships(cls, v)` (line 420): Validate relationships don't have duplicate predicates
    - `has_changes(self)` (line 428): Check if request has any changes
  - `OntologyResponse` (line 443): Response model for ontology operations
    - `validate_structure(self)` (line 454): Validate ontology structure
  - `QueryFilter` (line 478): Query filter model
    - `validate_field(cls, v)` (line 487): Validate field name
    - `validate_operator(cls, v)` (line 495): Validate operator
  - `QueryInput` (line 515): Query input model
    - `validate_limit(cls, v)` (line 529): Validate limit
    - `validate_offset(cls, v)` (line 537): Validate offset
    - `validate_order_direction(cls, v)` (line 545): Validate order direction
    - `validate_class_identifier(self)` (line 552): Validate that either class_label or class_id is provided

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
  - `GroupDefinition` (line 74): Grouping / module metadata for ontology resources.
  - `FunctionDefinition` (line 80): Derived field/function definition.
  - `ActionTypeDefinition` (line 89): Action template definition.
  - `OntologyResourceRecord` (line 98): Standardized API shape for ontology resources.

### `backend/shared/models/pipeline_job.py`
- **Classes**
  - `PipelineJob` (line 15): no docstring

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
- **Classes**
  - `TypeInferenceResult` (line 13): Type inference result with confidence and reasoning
  - `ColumnAnalysisResult` (line 22): Analysis result for a single column
  - `DatasetAnalysisRequest` (line 62): Request for dataset type analysis
  - `DatasetAnalysisResponse` (line 71): Response for dataset type analysis
  - `SchemaGenerationRequest` (line 79): Request for schema generation based on analysis
  - `SchemaGenerationResponse` (line 89): Generated schema based on type analysis
  - `FunnelPreviewRequest` (line 98): Request for data preview with type inference
  - `FunnelPreviewResponse` (line 107): Preview response with inferred types
  - `TypeMappingRequest` (line 118): Request for mapping inferred types to target schema
  - `TypeMappingResponse` (line 126): Response with mapped types for target system

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
  - `_to_text(value)` (line 45): no docstring
  - `carrier_from_kafka_headers(kafka_headers)` (line 61): Extract a W3C carrier dict from confluent_kafka headers.
  - `carrier_from_envelope_metadata(payload_or_metadata)` (line 80): Extract a W3C carrier dict from an EventEnvelope.metadata-like dict.
  - `kafka_headers_from_carrier(carrier)` (line 105): no docstring
  - `kafka_headers_from_envelope_metadata(payload_or_metadata)` (line 118): no docstring
  - `kafka_headers_from_current_context()` (line 123): Build Kafka headers (W3C Trace Context + baggage) from the current OTel context.
  - `enrich_metadata_with_current_trace(metadata)` (line 137): Mutate a metadata/payload dict by adding W3C trace context keys.
  - `attach_context_from_carrier(carrier, service_name)` (line 159): Attach an extracted context for the duration of the `with` block.
  - `attach_context_from_kafka(kafka_headers, fallback_metadata, service_name)` (line 195): Attach trace context from Kafka headers (preferred) or fallback metadata.

### `backend/shared/observability/metrics.py`
- **Functions**
  - `_prom_counter(name, description, labelnames)` (line 27): no docstring
  - `_prom_histogram(name, description, labelnames)` (line 35): no docstring
  - `_prom_gauge(name, description, labelnames)` (line 43): no docstring
  - `measure_time(metric_name, collector)` (line 531): Decorator for measuring function execution time
  - `prometheus_latest()` (line 644): Render Prometheus metrics for `/metrics`.
  - `get_metrics_collector(service_name)` (line 658): Get or create global metrics collector
- **Classes**
  - `MetricsCollector` (line 51): Centralized metrics collection based on Context7 patterns
    - `__init__(self, service_name)` (line 56): Initialize metrics collector
    - `_initialize_metrics(self)` (line 69): Initialize all metrics
    - `record_request(self, method, endpoint, status_code, duration, request_size, response_size)` (line 280): Record HTTP request metrics
    - `record_db_query(self, operation, table, duration, success)` (line 329): Record database query metrics
    - `record_cache_access(self, hit, cache_name)` (line 368): Record cache access
    - `record_event(self, event_type, action, duration)` (line 393): Record event sourcing metrics
    - `record_rate_limit(self, endpoint, rejected, strategy)` (line 431): Record rate limiting metrics
    - `record_business_metric(self, metric_name, value, attributes)` (line 468): Record custom business metrics
    - `timer(self, metric_name, attributes)` (line 504): Context manager for timing operations
  - `RequestMetricsMiddleware` (line 585): FastAPI middleware for automatic request metrics collection
    - `__init__(self, metrics_collector)` (line 590): Initialize middleware
    - `async __call__(self, request, call_next)` (line 600): Process request and collect metrics

### `backend/shared/observability/tracing.py`
- **Functions**
  - `get_tracing_service(service_name)` (line 379): no docstring
  - `trace_endpoint(name)` (line 388): no docstring
  - `trace_db_operation(name)` (line 395): no docstring
  - `trace_external_call(name)` (line 403): no docstring
- **Classes**
  - `OpenTelemetryConfig` (line 100): no docstring
  - `TracingService` (line 126): Distributed tracing facade.
    - `__init__(self, service_name)` (line 134): no docstring
    - `_log_no_op_once(self, reason)` (line 143): no docstring
    - `initialize(self)` (line 149): no docstring
    - `instrument_fastapi(self, app)` (line 229): no docstring
    - `instrument_clients(self)` (line 245): no docstring
    - `span(self, name, kind, attributes)` (line 286): no docstring
    - `trace(self, name, kind, attributes)` (line 301): no docstring
    - `get_current_span(self)` (line 321): no docstring
    - `record_exception(self, exception)` (line 326): no docstring
    - `set_span_attribute(self, key, value)` (line 333): no docstring
    - `get_trace_id(self)` (line 339): no docstring
    - `get_span_id(self)` (line 348): no docstring
    - `inject_trace_context(self, headers)` (line 357): no docstring
    - `extract_trace_context(self, headers)` (line 366): no docstring

### `backend/shared/routers/__init__.py`

### `backend/shared/routers/config_monitoring.py`
- **Functions**
  - `async get_settings()` (line 33): Get application settings
  - `async get_config_monitor(settings)` (line 39): Get or create configuration monitor
  - `async get_current_configuration(include_validation, monitor)` (line 61): Get current application configuration
  - `async get_configuration_changes(limit, severity, change_type, since, monitor)` (line 94): Get configuration change history
  - `async validate_configuration(monitor)` (line 179): Validate current configuration
  - `async perform_security_audit(monitor)` (line 222): Perform security audit of configuration
  - `async get_configuration_report(monitor)` (line 266): Get comprehensive configuration report
  - `async check_configuration_changes(background_tasks, monitor)` (line 305): Manually trigger configuration change detection
  - `async analyze_environment_drift(compare_environment, monitor)` (line 334): Analyze configuration drift between environments
  - `async analyze_configuration_health_impact(monitor)` (line 439): Analyze configuration health impact
  - `async get_monitoring_status(monitor)` (line 531): Get configuration monitoring system status

### `backend/shared/routers/monitoring.py`
- **Functions**
  - `async get_settings()` (line 31): Get application settings for monitoring
  - `async _check_service_instance(instance)` (line 37): Best-effort, runtime-validated service health check.
  - `async basic_health_check()` (line 82): Basic health check endpoint
  - `async detailed_health_check(include_metrics, settings, container)` (line 98): Detailed health check with comprehensive service information
  - `async readiness_probe(container)` (line 162): Kubernetes readiness probe
  - `async liveness_probe(container)` (line 184): Kubernetes liveness probe
  - `async get_service_metrics(service_name)` (line 218): Get comprehensive service metrics
  - `async get_service_status(container)` (line 236): Get current status of all services
  - `async get_configuration_overview(include_sensitive, settings)` (line 263): Get current application configuration
  - `async restart_service(service_name, _)` (line 333): Restart a specific service
  - `async get_service_dependencies(_)` (line 353): Get service dependency information
  - `async get_background_task_metrics(container)` (line 371): Get background task execution metrics
  - `async get_active_background_tasks(limit, container)` (line 432): Get list of all active background tasks
  - `async get_background_task_health(container)` (line 504): Get health status of background task processing system

### `backend/shared/security/__init__.py`

### `backend/shared/security/auth_utils.py`
- **Functions**
  - `get_expected_token(env_keys)` (line 17): no docstring
  - `extract_presented_token(headers)` (line 25): no docstring
  - `auth_disable_allowed(allow_disable_env_keys)` (line 35): no docstring
  - `auth_required(require_env_key, token_env_keys, default_required, allow_pytest, pytest_env_key)` (line 42): no docstring
  - `get_exempt_paths(env_key, defaults)` (line 60): no docstring
  - `is_exempt_path(path, exempt_paths)` (line 67): no docstring
  - `get_db_scope(headers)` (line 71): no docstring
  - `enforce_db_scope(headers, db_name, require_env_key)` (line 79): no docstring

### `backend/shared/security/input_sanitizer.py`
- **Functions**
  - `sanitize_input(data)` (line 564): 전역 입력 정화 함수
  - `sanitize_label_input(data)` (line 576): Sanitize a label-keyed payload (BFF).
  - `validate_db_name(db_name)` (line 595): 데이터베이스 이름 검증 함수
  - `validate_class_id(class_id)` (line 600): 클래스 ID 검증 함수
  - `validate_branch_name(branch_name)` (line 605): 브랜치 이름 검증 함수
  - `validate_instance_id(instance_id)` (line 610): 인스턴스 ID 검증 함수
  - `sanitize_es_query(query)` (line 615): Elasticsearch 쿼리 문자열 정제
- **Classes**
  - `SecurityViolationError` (line 17): 보안 위반 시 발생하는 예외
  - `InputSanitizer` (line 23): 포괄적인 입력 데이터 보안 검증 및 정화 클래스
    - `_read_int_env(name, default)` (line 130): no docstring
    - `__init__(self)` (line 141): no docstring
    - `detect_sql_injection(self, value)` (line 167): SQL Injection 패턴 탐지
    - `detect_xss(self, value)` (line 174): XSS 패턴 탐지
    - `detect_path_traversal(self, value)` (line 181): Path Traversal 패턴 탐지
    - `detect_command_injection(self, value, is_shell_context)` (line 188): Command Injection 패턴 탐지
    - `detect_nosql_injection(self, value)` (line 208): NoSQL Injection 패턴 탐지
    - `detect_ldap_injection(self, value)` (line 215): LDAP Injection 패턴 탐지
    - `sanitize_string(self, value, max_length)` (line 222): 문자열 정화 처리
    - `sanitize_field_name(self, value)` (line 270): 필드명 정화 (id, name 등 일반적인 필드명 허용)
    - `sanitize_label_key(self, value)` (line 290): Label-key sanitizer for "label-based" payloads (BFF).
    - `sanitize_label_dict(self, data, max_depth, current_depth)` (line 312): Sanitize a dict whose keys are *labels* (human-facing), not internal field names.
    - `sanitize_description(self, value)` (line 343): 설명 텍스트 정화 (command injection 체크 안함)
    - `sanitize_shell_command(self, value)` (line 367): Shell 명령어 컨텍스트의 문자열 정화 (모든 보안 체크 적용)
    - `sanitize_dict(self, data, max_depth, current_depth)` (line 381): 딕셔너리 재귀적 정화 처리
    - `sanitize_list(self, data, max_depth, current_depth)` (line 421): 리스트 정화 처리
    - `sanitize_any(self, value, max_depth, current_depth)` (line 443): 모든 타입의 데이터 정화 처리
    - `validate_database_name(self, db_name)` (line 471): 데이터베이스 이름 검증 - 엄격한 규칙 적용
    - `validate_class_id(self, class_id)` (line 508): 클래스 ID 검증
    - `validate_branch_name(self, branch_name)` (line 522): 브랜치 이름 검증
    - `validate_instance_id(self, instance_id)` (line 541): 인스턴스 ID 검증

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

### `backend/shared/services/aggregate_sequence_allocator.py`
- **Classes**
  - `OptimisticConcurrencyError` (line 23): Raised when the aggregate's current sequence doesn't match the caller's expectation.
    - `__init__(self, handler, aggregate_id, expected_last_sequence, actual_last_sequence)` (line 26): no docstring
  - `AggregateSequenceAllocator` (line 44): Atomic per-aggregate sequence allocator.
    - `__init__(self, dsn, schema, handler_prefix)` (line 57): no docstring
    - `async connect(self)` (line 69): no docstring
    - `async close(self)` (line 81): no docstring
    - `async ensure_schema(self)` (line 86): no docstring
    - `handler_for(self, aggregate_type)` (line 104): no docstring
    - `async try_reserve_next_sequence(self, handler, aggregate_id)` (line 109): Fast path: reserve the next seq if the allocator row already exists.
    - `async reserve_next_sequence(self, handler, aggregate_id, seed_last_sequence)` (line 140): Reserve the next seq, initializing/catching-up using `seed_last_sequence`.
    - `async try_reserve_next_sequence_if_expected(self, handler, aggregate_id, expected_last_sequence)` (line 178): OCC fast path: reserve the next seq only if current last_sequence matches `expected_last_sequence`.
    - `async reserve_next_sequence_if_expected(self, handler, aggregate_id, seed_last_sequence, expected_last_sequence)` (line 242): OCC reserve with seeding: ensure allocator is at least `seed_last_sequence`, then reserve next seq

### `backend/shared/services/async_terminus.py`
- **Classes**
  - `AsyncTerminusService` (line 14): Lightweight TerminusDB service for BFF
    - `__init__(self, connection_info)` (line 17): no docstring
    - `async connect(self)` (line 21): Establish connection to TerminusDB
    - `async ping(self)` (line 30): Check if TerminusDB is accessible
    - `async close(self)` (line 41): Close the connection

### `backend/shared/services/audit_log_store.py`
- **Functions**
  - `create_audit_log_store(settings)` (line 443): no docstring
- **Classes**
  - `AuditLogStore` (line 23): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 24): no docstring
    - `async initialize(self)` (line 38): no docstring
    - `async connect(self)` (line 41): no docstring
    - `async shutdown(self)` (line 52): no docstring
    - `async close(self)` (line 55): no docstring
    - `async health_check(self)` (line 60): no docstring
    - `async ensure_schema(self)` (line 70): no docstring
    - `_canonical_json(value)` (line 131): no docstring
    - `_compute_hash(cls, prev_hash, payload)` (line 135): no docstring
    - `async append(self, entry, partition_key)` (line 140): no docstring
    - `async log(self, partition_key, actor, action, status, resource_type, resource_id, event_id, command_id, trace_id, correlation_id, metadata, error, occurred_at)` (line 243): no docstring
    - `async list_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until, limit, offset)` (line 276): no docstring
    - `async count_logs(self, partition_key, action, status, resource_type, resource_id, event_id, command_id, actor, since, until)` (line 364): no docstring
    - `async get_chain_head(self, partition_key)` (line 418): no docstring

### `backend/shared/services/background_task_manager.py`
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

### `backend/shared/services/command_status_service.py`
- **Classes**
  - `CommandStatus` (line 20): Command execution status enumeration.
  - `CommandStatusService` (line 30): Service for tracking command execution status.
    - `__init__(self, redis_service)` (line 42): no docstring
    - `async create_command_status(self, command_id, command_type, aggregate_id, payload, user_id)` (line 59): Create initial command status entry.
    - `async update_status(self, command_id, status, message, error, progress)` (line 101): Update command status.
    - `async start_processing(self, command_id, worker_id)` (line 159): Mark command as being processed.
    - `async complete_command(self, command_id, result, message)` (line 184): Mark command as completed with result.
    - `async fail_command(self, command_id, error, retry_count)` (line 212): Mark command as failed.
    - `async cancel_command(self, command_id, reason)` (line 240): Cancel a pending or processing command.
    - `async get_command_details(self, command_id)` (line 275): Get complete command details including status and result.
    - `async list_user_commands(self, user_id, status_filter, limit)` (line 308): List commands for a specific user.
    - `async cleanup_old_commands(self, days)` (line 355): Clean up commands older than specified days.
    - `async set_command_status(self, command_id, status, metadata)` (line 371): Legacy compatibility method for setting command status.
    - `async get_command_status(self, command_id)` (line 402): Legacy compatibility method for getting command status.
    - `async get_command_result(self, command_id)` (line 429): Legacy compatibility method for getting command result.

### `backend/shared/services/connector_registry.py`
- **Classes**
  - `ConnectorSource` (line 32): no docstring
  - `ConnectorMapping` (line 42): no docstring
  - `SyncState` (line 57): no docstring
  - `OutboxItem` (line 74): no docstring
  - `ConnectorRegistry` (line 88): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 91): no docstring
    - `async initialize(self)` (line 105): no docstring
    - `async connect(self)` (line 108): no docstring
    - `async close(self)` (line 119): no docstring
    - `async ensure_schema(self)` (line 124): no docstring
    - `async upsert_source(self, source_type, source_id, config_json, enabled)` (line 226): no docstring
    - `async set_source_enabled(self, source_type, source_id, enabled)` (line 272): no docstring
    - `async get_source(self, source_type, source_id)` (line 290): no docstring
    - `async list_sources(self, source_type, enabled, limit)` (line 316): no docstring
    - `_deterministic_mapping_id(self, source_type, source_id)` (line 355): no docstring
    - `async upsert_mapping(self, source_type, source_id, enabled, status, target_db_name, target_branch, target_class_label, field_mappings)` (line 360): no docstring
    - `async get_mapping(self, source_type, source_id)` (line 431): no docstring
    - `async record_poll_result(self, source_type, source_id, current_cursor, kafka_topic)` (line 467): Record a poll result, and enqueue a connector update event when the cursor changed.
    - `async claim_outbox_batch(self, limit)` (line 580): no docstring
    - `async mark_outbox_published(self, outbox_id)` (line 632): no docstring
    - `async mark_outbox_failed(self, outbox_id, error)` (line 647): no docstring
    - `async record_sync_outcome(self, source_type, source_id, success, command_id, error, next_retry_at, rate_limit_until)` (line 665): no docstring
    - `async get_sync_state(self, source_type, source_id)` (line 735): no docstring

### `backend/shared/services/consistency_checker.py`
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

### `backend/shared/services/consistency_token.py`
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

### `backend/shared/services/dataset_ingest_outbox.py`
- **Functions**
  - `async flush_dataset_ingest_outbox(dataset_registry, lineage_store, batch_size)` (line 255): no docstring
  - `async run_dataset_ingest_outbox_worker(dataset_registry, lineage_store, poll_interval_seconds, batch_size, stop_event)` (line 275): no docstring
  - `build_dataset_event_payload(event_id, event_type, aggregate_type, aggregate_id, command_type, actor, data)` (line 304): no docstring
- **Classes**
  - `DatasetIngestOutboxPublisher` (line 29): no docstring
    - `__init__(self, dataset_registry, lineage_store, batch_size)` (line 30): no docstring
    - `async close(self)` (line 100): no docstring
    - `_next_attempt_at(self, attempts)` (line 107): no docstring
    - `async _send_to_dlq(self, item, error, attempts)` (line 111): no docstring
    - `async _handle_failure(self, item, error)` (line 152): no docstring
    - `async _publish_item(self, item)` (line 171): no docstring
    - `async flush_once(self)` (line 221): no docstring
    - `async maybe_purge(self)` (line 237): no docstring

### `backend/shared/services/dataset_ingest_reconciler.py`
- **Functions**
  - `async run_dataset_ingest_reconciler(dataset_registry, poll_interval_seconds, stale_after_seconds, stop_event)` (line 12): no docstring

### `backend/shared/services/dataset_registry.py`
- **Functions**
  - `_inject_dataset_version(outbox_entries, dataset_version_id)` (line 107): Ensure dataset_version_id is propagated into outbox payloads that depend on it.
- **Classes**
  - `DatasetRecord` (line 28): no docstring
  - `DatasetVersionRecord` (line 42): no docstring
  - `DatasetIngestRequestRecord` (line 55): no docstring
  - `DatasetIngestTransactionRecord` (line 76): no docstring
  - `DatasetIngestOutboxItem` (line 90): no docstring
  - `DatasetRegistry` (line 141): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 142): no docstring
    - `async initialize(self)` (line 156): no docstring
    - `async connect(self)` (line 159): no docstring
    - `async close(self)` (line 170): no docstring
    - `async ensure_schema(self)` (line 175): no docstring
    - `async create_dataset(self, db_name, name, description, source_type, source_ref, schema_json, branch, dataset_id)` (line 466): no docstring
    - `async list_datasets(self, db_name, branch)` (line 518): no docstring
    - `async get_dataset(self, dataset_id)` (line 572): no docstring
    - `async get_dataset_by_name(self, db_name, name, branch)` (line 600): no docstring
    - `async get_dataset_by_source_ref(self, db_name, source_type, source_ref, branch)` (line 636): no docstring
    - `async add_version(self, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, version_id, ingest_request_id, promoted_from_artifact_id)` (line 674): no docstring
    - `async get_latest_version(self, dataset_id)` (line 748): no docstring
    - `async get_version(self, version_id)` (line 779): no docstring
    - `async get_version_by_ingest_request(self, ingest_request_id)` (line 808): no docstring
    - `async get_ingest_request_by_key(self, idempotency_key)` (line 841): no docstring
    - `async create_ingest_request(self, dataset_id, db_name, branch, idempotency_key, request_fingerprint, schema_json, sample_json, row_count, source_metadata)` (line 881): no docstring
    - `async get_ingest_transaction(self, ingest_request_id)` (line 952): no docstring
    - `async create_ingest_transaction(self, ingest_request_id, status)` (line 984): no docstring
    - `async mark_ingest_transaction_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 1023): no docstring
    - `async mark_ingest_transaction_aborted(self, ingest_request_id, error)` (line 1064): no docstring
    - `async mark_ingest_committed(self, ingest_request_id, lakefs_commit_id, artifact_key)` (line 1102): no docstring
    - `async mark_ingest_failed(self, ingest_request_id, error)` (line 1162): no docstring
    - `async update_ingest_request_payload(self, ingest_request_id, schema_json, sample_json, row_count, source_metadata)` (line 1195): no docstring
    - `async publish_ingest_request(self, ingest_request_id, dataset_id, lakefs_commit_id, artifact_key, row_count, sample_json, schema_json, outbox_entries)` (line 1227): no docstring
    - `async claim_ingest_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 1495): no docstring
    - `async mark_ingest_outbox_published(self, outbox_id)` (line 1572): no docstring
    - `async mark_ingest_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 1591): no docstring
    - `async mark_ingest_outbox_dead(self, outbox_id, error)` (line 1618): no docstring
    - `async purge_ingest_outbox(self, retention_days, limit)` (line 1638): no docstring
    - `async get_ingest_outbox_metrics(self)` (line 1668): no docstring
    - `async reconcile_ingest_state(self, stale_after_seconds, limit, use_lock, lock_key)` (line 1709): Best-effort reconciliation for ingest atomicity.

### `backend/shared/services/dlq_handler_fixed.py`
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

### `backend/shared/services/elasticsearch_service.py`
- **Functions**
  - `create_elasticsearch_service(settings)` (line 599): Elasticsearch 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_elasticsearch_service_legacy(host, port, username, password)` (line 622): 레거시 Elasticsearch 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `ElasticsearchService` (line 26): Async Elasticsearch client service with connection pooling and error handling.
    - `__init__(self, host, port, username, password, use_ssl, verify_certs, request_timeout, max_retries, retry_on_timeout)` (line 40): no docstring
    - `async connect(self)` (line 75): Initialize Elasticsearch connection.
    - `async disconnect(self)` (line 89): Close Elasticsearch connection.
    - `client(self)` (line 99): Get Elasticsearch client instance.
    - `async get_cluster_health(self)` (line 105): Get Elasticsearch cluster health status.
    - `async create_index(self, index, mappings, settings, aliases)` (line 117): Create an index with optional mappings, settings, and aliases.
    - `async delete_index(self, index)` (line 155): Delete an index.
    - `async index_exists(self, index)` (line 176): Check if index exists.
    - `async update_mapping(self, index, properties)` (line 184): Update index mapping.
    - `async index_document(self, index, document, doc_id, refresh, version, version_type, op_type)` (line 212): Index a single document.
    - `async get_document(self, index, doc_id, source_includes, source_excludes)` (line 255): Get a document by ID.
    - `async update_document(self, index, doc_id, doc, script, upsert, refresh)` (line 288): Update a document.
    - `async delete_document(self, index, doc_id, refresh, version, version_type)` (line 336): Delete a document.
    - `async bulk_index(self, index, documents, chunk_size, refresh)` (line 373): Bulk index documents.
    - `async search(self, index, query, size, from_, sort, source_includes, source_excludes, aggregations)` (line 422): Search documents.
    - `async count(self, index, query)` (line 475): Count documents matching query.
    - `async create_alias(self, index, alias, filter)` (line 503): Create an alias for an index with optional filter.
    - `async delete_alias(self, index, alias)` (line 536): Delete an alias.
    - `async update_aliases(self, actions)` (line 558): Perform multiple alias operations atomically.
    - `async refresh_index(self, index)` (line 581): Force refresh an index to make changes searchable.
    - `async ping(self)` (line 590): Check Elasticsearch connection.

### `backend/shared/services/event_replay.py`
- **Functions**
  - `async demo_replay()` (line 333): Demo the replay functionality
- **Classes**
  - `EventReplayService` (line 19): Deterministic event replay from S3 storage
    - `__init__(self, s3_endpoint, s3_access_key, s3_secret_key, bucket_name)` (line 25): no docstring
    - `async replay_aggregate(self, db_name, class_id, aggregate_id, up_to_sequence, up_to_timestamp)` (line 42): Replay all events for a specific aggregate
    - `_apply_event(self, state, event)` (line 148): Apply a single event to the current state
    - `async replay_all_aggregates(self, db_name, class_id, limit)` (line 184): Replay all aggregates of a specific class
    - `async point_in_time_replay(self, db_name, class_id, aggregate_id, target_time)` (line 235): Replay aggregate state at a specific point in time
    - `async verify_replay_determinism(self, db_name, class_id, aggregate_id)` (line 256): Verify that replaying produces deterministic results
    - `async get_aggregate_history(self, db_name, class_id, aggregate_id)` (line 285): Get complete event history for an aggregate

### `backend/shared/services/event_store.py`
- **Functions**
  - `async get_event_store()` (line 1161): Dependency to get the Event Store instance
- **Classes**
  - `EventStore` (line 46): The REAL Event Store using S3/MinIO as Single Source of Truth.
    - `__init__(self)` (line 61): no docstring
    - `_s3_client_kwargs(self)` (line 76): no docstring
    - `async connect(self)` (line 122): Initialize S3/MinIO connection
    - `async _initialize_lineage_and_audit(self)` (line 161): no docstring
    - `_partition_key_for_envelope(envelope)` (line 181): no docstring
    - `async _record_lineage_and_audit(self, envelope, s3_key, audit_action)` (line 190): no docstring
    - `async _record_audit_failure(self, envelope, error)` (line 261): no docstring
    - `async _get_sequence_allocator(self)` (line 298): no docstring
    - `async _ensure_sequence_number(self, event)` (line 310): Ensure `event.sequence_number` is set using an atomic write-side allocator.
    - `async append_event(self, event)` (line 439): Append an immutable event to S3/MinIO.
    - `_enforce_idempotency_contract(self, existing, incoming, source)` (line 620): Detect event_id reuse with conflicting contents.
    - `_is_command_envelope(env)` (line 655): no docstring
    - `_normalize_for_idempotency_compare(env)` (line 661): no docstring
    - `_stable_hash(payload)` (line 676): no docstring
    - `async _get_existing_key_by_event_id(self, s3, event_id)` (line 680): no docstring
    - `async _get_existing_key_by_aggregate_index(self, s3, aggregate_type, aggregate_id, event_id)` (line 693): no docstring
    - `async _read_event_object(self, s3, key)` (line 715): no docstring
    - `async get_event_object_key(self, event_id)` (line 720): Resolve an event_id to its S3 object key using the by-event-id index.
    - `async read_event_by_key(self, key)` (line 732): Read an event envelope from S3/MinIO by object key.
    - `async get_events(self, aggregate_type, aggregate_id, from_version, to_version)` (line 740): Retrieve all events for an aggregate from S3/MinIO.
    - `_dedup_events(events)` (line 830): Best-effort dedup to hide historical duplicates during migration.
    - `_dedup_key(event)` (line 843): no docstring
    - `async replay_events(self, from_timestamp, to_timestamp, event_types)` (line 854): Replay events from S3/MinIO for a time range.
    - `async get_aggregate_version(self, aggregate_type, aggregate_id)` (line 946): Get the current version of an aggregate (max sequence_number).
    - `async _update_indexes(self, event, key, s3)` (line 984): Update various indexes for efficient querying.
    - `async _write_index_entries(self, s3, aggregate_index_key, date_index_key, event_id_index_key, payload)` (line 1050): no docstring
    - `async get_snapshot(self, aggregate_type, aggregate_id, version)` (line 1079): Get a snapshot of an aggregate at a specific version.
    - `async save_snapshot(self, aggregate_type, aggregate_id, version, state)` (line 1107): Save a snapshot for performance optimization.

### `backend/shared/services/graph_federation_service_woql.py`
- **Classes**
  - `GraphFederationServiceWOQL` (line 25): Production-ready Graph Federation using REAL WOQL
    - `__init__(self, terminus_service, es_host, es_port, es_username, es_password)` (line 33): no docstring
    - `_terminus_branch_path(branch)` (line 50): Build `/local/branch/{...}` descriptor segment for TerminusDB APIs (WOQL/Document).
    - `_extract_binding_literal(value)` (line 58): no docstring
    - `_parse_timestamp(value)` (line 74): no docstring
    - `async _fetch_schema_class_doc(self, db_name, class_id, branch)` (line 97): no docstring
    - `_schema_range_for_predicate(schema_doc, predicate)` (line 115): no docstring
    - `async _validate_hop_semantics(self, db_name, branch, start_class, hops)` (line 128): no docstring
    - `async multi_hop_query(self, db_name, start_class, hops, branch, filters, limit, offset, max_nodes, max_edges, include_paths, max_paths, no_cycles, include_documents, include_audit)` (line 157): Execute multi-hop graph query with ES federation using REAL WOQL
    - `async simple_graph_query(self, db_name, class_name, branch, filters, include_documents, include_audit)` (line 441): Simple single-class query - PALANTIR STYLE
    - `_build_simple_woql(self, class_name, filters)` (line 522): Build WOQL query for simple class query
    - `_build_multi_hop_woql(self, start_class, hops, filters)` (line 562): Build WOQL query for multi-hop traversal
    - `_extract_es_doc_id(self, instance_id)` (line 673): Extract ES document ID from instance ID
    - `async find_relationship_paths(self, db_name, source_class, target_class, branch, max_depth)` (line 680): Find all relationship paths between two classes using REAL WOQL schema queries
    - `_get_known_paths(self, source_class, target_class)` (line 768): Fallback to known relationship paths
    - `_get_known_multi_hop_paths(self, source_class, target_class, max_depth)` (line 785): Get known multi-hop paths (would be replaced by recursive WOQL search)
    - `async _fetch_es_documents(self, db_name, doc_ids, branch)` (line 795): Fetch documents from Elasticsearch for the given graph node IDs.
    - `async _fetch_audit_records(self, db_name, doc_ids)` (line 899): Fetch audit records from Elasticsearch audit index

### `backend/shared/services/health_check.py`
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

### `backend/shared/services/idempotency_service.py`
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

### `backend/shared/services/lakefs_client.py`
- **Functions**
  - `_extract_commit_id(payload)` (line 58): no docstring
  - `_normalize_metadata(metadata)` (line 67): lakeFS commit/merge metadata is stored as string key-value pairs.
- **Classes**
  - `LakeFSError` (line 20): no docstring
  - `LakeFSAuthError` (line 24): no docstring
  - `LakeFSNotFoundError` (line 28): no docstring
  - `LakeFSConflictError` (line 32): no docstring
  - `LakeFSConfig` (line 37): no docstring
    - `from_env()` (line 43): no docstring
  - `LakeFSClient` (line 93): Minimal async lakeFS REST client.
    - `__init__(self, config, timeout_seconds)` (line 102): no docstring
    - `_client(self)` (line 106): no docstring
    - `async create_branch(self, repository, name, source)` (line 113): no docstring
    - `async delete_branch(self, repository, name)` (line 135): no docstring
    - `async commit(self, repository, branch, message, metadata)` (line 153): no docstring
    - `async get_branch_head_commit_id(self, repository, branch)` (line 186): no docstring
    - `async list_diff_objects(self, repository, ref, since, prefix, amount)` (line 208): no docstring
    - `async merge(self, repository, source_ref, destination_branch, message, metadata, allow_empty)` (line 283): no docstring

### `backend/shared/services/lakefs_storage_service.py`
- **Functions**
  - `create_lakefs_storage_service(settings)` (line 29): no docstring
- **Classes**
  - `LakeFSStorageService` (line 19): no docstring
    - `async create_bucket(self, bucket_name)` (line 20): no docstring
    - `async bucket_exists(self, bucket_name)` (line 24): no docstring

### `backend/shared/services/lineage_store.py`
- **Functions**
  - `create_lineage_store(settings)` (line 1174): no docstring
- **Classes**
  - `LineageStore` (line 25): Postgres-backed lineage store.
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 35): no docstring
    - `async initialize(self)` (line 49): no docstring
    - `async connect(self)` (line 52): no docstring
    - `async shutdown(self)` (line 63): no docstring
    - `async close(self)` (line 66): no docstring
    - `async health_check(self)` (line 71): no docstring
    - `async ensure_schema(self)` (line 81): no docstring
    - `node_event(event_id)` (line 290): no docstring
    - `node_aggregate(aggregate_type, aggregate_id)` (line 294): no docstring
    - `node_artifact(kind, *parts)` (line 298): no docstring
    - `_infer_node_type(node_id)` (line 303): no docstring
    - `_parse_node_id(node_id)` (line 315): Decompose node_id into queryable columns (best-effort).
    - `_env_first(*keys)` (line 375): no docstring
    - `_run_context(cls)` (line 383): no docstring
    - `_deterministic_edge_id(*parts)` (line 390): no docstring
    - `_coerce_metadata(value)` (line 395): Coerce a Postgres JSONB value into a Python dict.
    - `async upsert_node(self, node_id, node_type, label, metadata, created_at, recorded_at, db_name, run_id, code_sha, schema_version)` (line 427): no docstring
    - `async insert_edge(self, from_node_id, to_node_id, edge_type, occurred_at, metadata, projection_name, recorded_at, db_name, run_id, code_sha, schema_version, edge_id)` (line 492): no docstring
    - `async enqueue_backfill(self, envelope, s3_bucket, s3_key, error)` (line 556): Best-effort enqueue for eventual lineage recovery.
    - `async mark_backfill_done(self, event_id)` (line 605): no docstring
    - `async mark_backfill_failed(self, event_id, error)` (line 619): no docstring
    - `async claim_backfill_batch(self, limit, db_name)` (line 635): Claim a batch of pending backfill rows (best-effort).
    - `async get_backfill_metrics(self, db_name)` (line 691): no docstring
    - `async count_edges(self, edge_type, db_name, since, until)` (line 728): no docstring
    - `async get_latest_edges_to(self, to_node_ids, edge_type, projection_name, db_name)` (line 764): Fetch the latest edge (by occurred_at) for each `to_node_id`.
    - `async record_link(self, from_node_id, to_node_id, edge_type, occurred_at, edge_metadata, from_label, to_label, from_type, to_type, from_metadata, to_metadata, db_name, projection_name, run_id, code_sha, schema_version, edge_id)` (line 842): no docstring
    - `async record_event_envelope(self, envelope, s3_bucket, s3_key)` (line 918): Record the core lineage relationships for an EventEnvelope:
    - `normalize_root(root)` (line 1015): no docstring
    - `async get_graph(self, root, direction, max_depth, max_nodes, max_edges, db_name)` (line 1028): no docstring

### `backend/shared/services/llm_gateway.py`
- **Functions**
  - `_env_bool(name, default)` (line 54): no docstring
  - `_extract_json_object(text)` (line 61): Best-effort JSON object extraction.
  - `create_llm_gateway(_settings)` (line 291): no docstring
- **Classes**
  - `LLMUnavailableError` (line 34): no docstring
  - `LLMRequestError` (line 38): no docstring
  - `LLMOutputValidationError` (line 42): no docstring
  - `LLMCallMeta` (line 47): no docstring
  - `LLMGateway` (line 103): A thin, safe wrapper around an LLM provider.
    - `__init__(self)` (line 112): no docstring
    - `is_enabled(self)` (line 129): no docstring
    - `async complete_json(self, task, system_prompt, user_prompt, response_model, redis_service, audit_store, audit_partition_key, audit_actor, audit_resource_id, audit_metadata, temperature, max_tokens)` (line 138): no docstring

### `backend/shared/services/objectify_job_queue.py`
- **Classes**
  - `ObjectifyJobQueue` (line 16): no docstring
    - `__init__(self, objectify_registry)` (line 17): no docstring
    - `async _get_registry(self)` (line 21): no docstring
    - `async close(self)` (line 28): no docstring
    - `async publish(self, job, require_delivery)` (line 34): no docstring

### `backend/shared/services/objectify_outbox.py`
- **Functions**
  - `async run_objectify_outbox_worker(objectify_registry, poll_interval_seconds, batch_size, stop_event)` (line 193): no docstring
- **Classes**
  - `ObjectifyOutboxPublisher` (line 25): no docstring
    - `__init__(self, objectify_registry, topic, batch_size)` (line 26): no docstring
    - `async close(self)` (line 97): no docstring
    - `_next_attempt_at(self, attempts)` (line 103): no docstring
    - `async _publish_batch(self, batch)` (line 107): no docstring
    - `async flush_once(self)` (line 165): no docstring
    - `async maybe_purge(self)` (line 175): no docstring

### `backend/shared/services/objectify_reconciler.py`
- **Functions**
  - `_match_output_name(output, name)` (line 18): no docstring
  - `async _build_objectify_payload(job, dataset_registry, objectify_registry, pipeline_registry)` (line 29): no docstring
  - `async reconcile_objectify_jobs(objectify_registry, dataset_registry, pipeline_registry, stale_after_seconds, enqueued_stale_seconds, limit, use_lock, lock_key)` (line 104): no docstring
  - `async run_objectify_reconciler(objectify_registry, dataset_registry, pipeline_registry, poll_interval_seconds, stale_after_seconds, enqueued_stale_seconds, stop_event)` (line 211): no docstring

### `backend/shared/services/objectify_registry.py`
- **Functions**
  - `_coerce_json_list(value)` (line 26): no docstring
- **Classes**
  - `OntologyMappingSpecRecord` (line 53): no docstring
  - `ObjectifyJobRecord` (line 71): no docstring
  - `ObjectifyOutboxItem` (line 92): no docstring
  - `ObjectifyRegistry` (line 106): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 107): no docstring
    - `async initialize(self)` (line 121): no docstring
    - `async connect(self)` (line 124): no docstring
    - `async close(self)` (line 135): no docstring
    - `async ensure_schema(self)` (line 140): no docstring
    - `_normalize_optional(value)` (line 337): no docstring
    - `build_dedupe_key(dataset_id, dataset_branch, mapping_spec_id, mapping_spec_version, dataset_version_id, artifact_id, artifact_output_name)` (line 344): no docstring
    - `_validate_objectify_inputs(self, dataset_version_id, artifact_id, artifact_output_name)` (line 368): no docstring
    - `async create_mapping_spec(self, dataset_id, dataset_branch, artifact_output_name, schema_hash, target_class_id, mappings, target_field_types, status, auto_sync, options)` (line 382): no docstring
    - `async get_mapping_spec(self, mapping_spec_id)` (line 497): no docstring
    - `async list_mapping_specs(self, dataset_id, include_inactive)` (line 530): no docstring
    - `async get_active_mapping_spec(self, dataset_id, dataset_branch, target_class_id, artifact_output_name, schema_hash)` (line 581): no docstring
    - `async create_objectify_job(self, job_id, mapping_spec_id, mapping_spec_version, dataset_id, dataset_version_id, artifact_id, artifact_output_name, dataset_branch, target_class_id, status, outbox_payload, dedupe_key)` (line 639): no docstring
    - `async get_objectify_metrics(self)` (line 749): no docstring
    - `async enqueue_objectify_job(self, job)` (line 794): no docstring
    - `async enqueue_outbox_for_job(self, job_id, payload)` (line 822): no docstring
    - `async has_outbox_for_job(self, job_id, statuses)` (line 844): no docstring
    - `async claim_objectify_outbox_batch(self, limit, claimed_by, claim_timeout_seconds)` (line 869): no docstring
    - `async mark_objectify_outbox_published(self, outbox_id, job_id)` (line 943): no docstring
    - `async mark_objectify_outbox_failed(self, outbox_id, error, next_attempt_at)` (line 972): no docstring
    - `async purge_objectify_outbox(self, retention_days, limit)` (line 998): no docstring
    - `async list_objectify_jobs(self, statuses, older_than, limit)` (line 1028): no docstring
    - `async get_objectify_job(self, job_id)` (line 1081): no docstring
    - `async get_objectify_job_by_dedupe_key(self, dedupe_key)` (line 1118): no docstring
    - `async find_objectify_job(self, dataset_version_id, mapping_spec_id, mapping_spec_version, statuses)` (line 1160): no docstring
    - `async find_objectify_job_for_artifact(self, artifact_id, artifact_output_name, mapping_spec_id, mapping_spec_version, statuses)` (line 1214): no docstring
    - `async update_objectify_job_status(self, job_id, status, command_id, error, report, completed_at)` (line 1273): no docstring

### `backend/shared/services/ontology_linter.py`
- **Functions**
  - `_is_snake_case(value)` (line 66): no docstring
  - `_tokenize(value)` (line 70): no docstring
  - `_event_like_triggers(value)` (line 77): Conservative "event/state/log-like" hint detector.
  - `_issue(severity, rule_id, message, path, suggestion, rationale, metadata)` (line 123): no docstring
  - `compute_risk_score(errors, warnings, infos)` (line 144): no docstring
  - `risk_level(score)` (line 156): no docstring
  - `lint_ontology_create(class_id, label, abstract, properties, relationships, config)` (line 166): Lint a create payload (no IO).
  - `lint_ontology_update(existing_properties, existing_relationships, updated_properties, updated_relationships, config)` (line 344): Lint an update as a diff (no IO).
- **Classes**
  - `OntologyLinterConfig` (line 26): Controls strictness (domain-neutral).
    - `from_env(cls, branch)` (line 35): no docstring

### `backend/shared/services/pipeline_artifact_store.py`
- **Classes**
  - `PipelineArtifactStore` (line 17): no docstring
    - `__init__(self, storage_service, bucket)` (line 18): no docstring
    - `async save_table(self, dataset_name, columns, rows, db_name, pipeline_id)` (line 22): no docstring

### `backend/shared/services/pipeline_control_plane_events.py`
- **Functions**
  - `pipeline_control_plane_events_enabled()` (line 15): no docstring
  - `sanitize_event_id(value)` (line 24): no docstring
  - `async emit_pipeline_control_plane_event(event_type, pipeline_id, event_id, data, actor, kind)` (line 28): no docstring

### `backend/shared/services/pipeline_executor.py`
- **Functions**
  - `_extract_schema_columns(schema)` (line 455): no docstring
  - `_extract_sample_rows(sample)` (line 473): no docstring
  - `_fallback_columns(node)` (line 492): no docstring
  - `_build_sample_rows(columns, count)` (line 502): no docstring
  - `_group_by_table(table, group_by, aggregates)` (line 512): no docstring
  - `_pivot_table(table, pivot_meta)` (line 578): no docstring
  - `_window_table(table, window_meta)` (line 621): no docstring
  - `_select_columns(table, columns)` (line 643): no docstring
  - `_drop_columns(table, columns)` (line 649): no docstring
  - `_rename_columns(table, rename_map)` (line 656): no docstring
  - `_cast_columns(table, casts)` (line 668): no docstring
  - `_dedupe_table(table, columns)` (line 702): no docstring
  - `_sort_table(table, columns)` (line 715): no docstring
  - `_union_tables(left, right, union_mode)` (line 723): no docstring
  - `_join_tables(left, right, join_type, left_key, right_key, join_key, allow_cross_join)` (line 761): no docstring
  - `_merge_rows(left, right, right_column_map)` (line 813): no docstring
  - `_filter_table(table, expression, parameters)` (line 830): no docstring
  - `_parse_filter(expression, parameters)` (line 846): no docstring
  - `_compare(left, op, right)` (line 862): no docstring
  - `_compute_table(table, expression, parameters)` (line 886): no docstring
  - `_explode_table(table, column)` (line 903): no docstring
  - `_parse_assignment(expression)` (line 926): no docstring
  - `_safe_eval(expression, row, parameters)` (line 933): no docstring
  - `_is_safe_ast(node)` (line 954): no docstring
  - `_eval_ast(node, variables)` (line 964): no docstring
  - `_parse_literal(raw)` (line 996): no docstring
  - `_parse_csv_bytes(raw_bytes)` (line 1009): no docstring
  - `_parse_excel_bytes(raw_bytes)` (line 1034): no docstring
  - `_parse_json_bytes(raw_bytes)` (line 1045): no docstring
  - `_infer_column_types(table)` (line 1078): no docstring
  - `_infer_type_from_values(values)` (line 1087): no docstring
  - `_validate_schema_checks(table, checks)` (line 1145): no docstring
  - `_split_expectation_columns(column)` (line 1200): no docstring
  - `_normalize_pk_semantics(value)` (line 1204): no docstring
  - `_is_truthy(value)` (line 1221): no docstring
  - `_resolve_execution_semantics(definition)` (line 1231): no docstring
  - `_resolve_incremental_config(definition)` (line 1257): no docstring
  - `_resolve_pk_semantics(execution_semantics, definition, output_metadata)` (line 1262): no docstring
  - `_resolve_delete_column(definition, output_metadata)` (line 1296): no docstring
  - `_validate_pk_semantics(table, pk_semantics, pk_columns, delete_column)` (line 1310): no docstring
  - `_coerce_pk_columns(value)` (line 1330): no docstring
  - `_collect_pk_columns(*candidates)` (line 1371): no docstring
  - `_match_output_declaration(output, node_id, output_name)` (line 1382): no docstring
  - `_resolve_pk_columns(definition, output_metadata, output_name, output_node_id, declared_outputs)` (line 1405): no docstring
  - `_build_expectations_with_pk(definition, output_metadata, output_name, output_node_id, declared_outputs, pk_semantics, delete_column, pk_columns, available_columns)` (line 1449): no docstring
  - `_validate_expectations(table, expectations)` (line 1503): no docstring
  - `_validate_schema_contract(table, contract)` (line 1570): no docstring
- **Classes**
  - `PipelineExpectationError` (line 40): no docstring
  - `PipelineTable` (line 45): no docstring
    - `limited_rows(self, limit)` (line 49): no docstring
  - `PipelineRunResult` (line 56): no docstring
  - `PipelineArtifactStore` (line 61): no docstring
    - `__init__(self, base_path)` (line 62): no docstring
    - `save_table(self, table, dataset_name)` (line 67): no docstring
  - `PipelineExecutor` (line 79): no docstring
    - `__init__(self, dataset_registry, pipeline_registry, artifact_store, storage_service)` (line 80): no docstring
    - `async preview(self, definition, db_name, node_id, limit, input_overrides)` (line 92): no docstring
    - `async deploy(self, definition, db_name, node_id, dataset_name, store_local, input_overrides)` (line 105): no docstring
    - `async run(self, definition, db_name, input_overrides)` (line 125): no docstring
    - `async _load_input(self, node, db_name, branch)` (line 224): no docstring
    - `async _load_rows_from_artifact(self, artifact_key)` (line 284): no docstring
    - `async _apply_transform(self, metadata, inputs, parameters)` (line 327): no docstring
    - `async _apply_udf_transform(self, table, metadata)` (line 392): no docstring
    - `_table_to_sample(self, table, limit)` (line 434): no docstring
    - `_select_table(self, result, node_id)` (line 445): no docstring

### `backend/shared/services/pipeline_graph_utils.py`
- **Functions**
  - `normalize_nodes(nodes_raw)` (line 6): no docstring
  - `normalize_edges(edges_raw)` (line 20): no docstring
  - `build_incoming(edges)` (line 35): no docstring
  - `topological_sort(nodes, edges, include_unordered)` (line 42): no docstring

### `backend/shared/services/pipeline_job_queue.py`
- **Classes**
  - `PipelineJobQueue` (line 24): no docstring
    - `__init__(self)` (line 25): no docstring
    - `_producer_instance(self)` (line 30): no docstring
    - `async publish(self, job, require_delivery)` (line 45): no docstring

### `backend/shared/services/pipeline_parameter_utils.py`
- **Functions**
  - `normalize_parameters(parameters_raw)` (line 6): no docstring
  - `apply_parameters(expression, parameters)` (line 22): no docstring

### `backend/shared/services/pipeline_profiler.py`
- **Functions**
  - `_safe_stringify(value)` (line 15): no docstring
  - `_coerce_float(value)` (line 28): no docstring
  - `_compute_histogram(values, bins)` (line 46): no docstring
  - `_normalize_columns(columns)` (line 79): no docstring
  - `compute_column_stats(rows, columns, max_top_values)` (line 93): Returns a dict payload that is safe to JSON serialize:

### `backend/shared/services/pipeline_registry.py`
- **Functions**
  - `_ensure_json_string(value)` (line 45): no docstring
  - `_is_production_env()` (line 65): no docstring
  - `_lakefs_credentials_source()` (line 70): no docstring
  - `_lakefs_service_principal()` (line 79): no docstring
  - `_lakefs_fernet()` (line 83): no docstring
  - `_encrypt_secret(secret_access_key)` (line 96): no docstring
  - `_decrypt_secret(encrypted)` (line 103): no docstring
  - `_normalize_pipeline_role(value)` (line 121): no docstring
  - `_normalize_principal_type(value)` (line 130): no docstring
  - `_role_allows(required, assigned)` (line 137): no docstring
  - `_definition_object_key(db_name, pipeline_name)` (line 143): no docstring
  - `_row_to_pipeline_record(row)` (line 161): no docstring
  - `_row_to_pipeline_artifact(row)` (line 312): no docstring
- **Classes**
  - `PipelineMergeNotSupportedError` (line 33): no docstring
  - `PipelineAlreadyExistsError` (line 37): no docstring
    - `__init__(self, db_name, name, branch)` (line 38): no docstring
  - `LakeFSCredentials` (line 151): no docstring
  - `PipelineRecord` (line 200): no docstring
  - `PipelineVersionRecord` (line 237): no docstring
  - `PipelineUdfRecord` (line 247): no docstring
  - `PipelineUdfVersionRecord` (line 258): no docstring
  - `PipelineArtifactRecord` (line 267): no docstring
  - `PromotionManifestRecord` (line 291): no docstring
  - `PipelineRegistry` (line 337): no docstring
    - `__init__(self, dsn, schema, pool_min, pool_max)` (line 338): no docstring
    - `async _get_lakefs_credentials(self, principal_type, principal_id)` (line 352): no docstring
    - `async upsert_lakefs_credentials(self, principal_type, principal_id, access_key_id, secret_access_key, created_by)` (line 390): no docstring
    - `async list_lakefs_credentials(self)` (line 435): no docstring
    - `async resolve_lakefs_credentials(self, user_id)` (line 458): no docstring
    - `async get_lakefs_client(self, user_id)` (line 494): no docstring
    - `async get_lakefs_storage(self, user_id)` (line 505): no docstring
    - `_resolve_repository(self, pipeline)` (line 515): no docstring
    - `async initialize(self)` (line 522): no docstring
    - `async connect(self)` (line 525): no docstring
    - `async close(self)` (line 536): no docstring
    - `async ensure_schema(self)` (line 541): no docstring
    - `async list_dependencies(self, pipeline_id)` (line 1074): no docstring
    - `async replace_dependencies(self, pipeline_id, dependencies)` (line 1093): no docstring
    - `async grant_permission(self, pipeline_id, principal_type, principal_id, role)` (line 1131): no docstring
    - `async revoke_permission(self, pipeline_id, principal_type, principal_id)` (line 1162): no docstring
    - `async list_permissions(self, pipeline_id)` (line 1187): no docstring
    - `async has_any_permissions(self, pipeline_id)` (line 1212): no docstring
    - `async get_permission_role(self, pipeline_id, principal_type, principal_id)` (line 1228): no docstring
    - `async has_permission(self, pipeline_id, principal_type, principal_id, required_role)` (line 1257): no docstring
    - `async create_pipeline(self, db_name, name, description, pipeline_type, location, status, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle, schedule_interval_seconds, schedule_cron, pipeline_id)` (line 1274): no docstring
    - `async list_pipelines(self, db_name, branch)` (line 1361): no docstring
    - `async list_proposals(self, db_name, branch, status)` (line 1464): no docstring
    - `async submit_proposal(self, pipeline_id, title, description, proposal_bundle)` (line 1517): no docstring
    - `async review_proposal(self, pipeline_id, status, review_comment)` (line 1561): no docstring
    - `async merge_branch(self, pipeline_id, from_branch, to_branch, user_id)` (line 1599): no docstring
    - `async get_pipeline(self, pipeline_id)` (line 1695): no docstring
    - `async get_pipeline_by_name(self, db_name, name, branch)` (line 1720): no docstring
    - `async update_pipeline(self, pipeline_id, name, description, location, status, schedule_interval_seconds, schedule_cron, branch, lakefs_repository, proposal_status, proposal_title, proposal_description, proposal_submitted_at, proposal_reviewed_at, proposal_review_comment, proposal_bundle)` (line 1753): no docstring
    - `async add_version(self, pipeline_id, branch, definition_json, version_id, user_id)` (line 1845): no docstring
    - `async get_latest_version(self, pipeline_id, branch)` (line 1959): no docstring
    - `async get_version(self, pipeline_id, lakefs_commit_id, branch)` (line 1997): no docstring
    - `async record_preview(self, pipeline_id, status, row_count, sample_json, job_id, node_id)` (line 2034): no docstring
    - `async record_run(self, pipeline_id, job_id, mode, status, node_id, row_count, sample_json, output_json, pipeline_spec_commit_id, pipeline_spec_hash, input_lakefs_commits, output_lakefs_commit_id, spark_conf, code_version, started_at, finished_at)` (line 2078): no docstring
    - `async upsert_artifact(self, pipeline_id, job_id, mode, status, run_id, artifact_id, definition_hash, definition_commit_id, pipeline_spec_hash, pipeline_spec_commit_id, inputs, lakefs_repository, lakefs_branch, lakefs_commit_id, outputs, declared_outputs, sampling_strategy, error)` (line 2187): no docstring
    - `async get_artifact(self, artifact_id)` (line 2305): no docstring
    - `async get_artifact_by_job(self, pipeline_id, job_id, mode)` (line 2326): no docstring
    - `async list_artifacts(self, pipeline_id, limit, mode)` (line 2362): no docstring
    - `async list_runs(self, pipeline_id, limit)` (line 2396): no docstring
    - `async get_run(self, pipeline_id, job_id)` (line 2445): no docstring
    - `async get_watermarks(self, pipeline_id, branch)` (line 2491): no docstring
    - `async upsert_watermarks(self, pipeline_id, branch, watermarks)` (line 2511): no docstring
    - `async record_build(self, pipeline_id, status, output_json, deployed_commit_id)` (line 2546): no docstring
    - `async record_promotion_manifest(self, pipeline_id, db_name, build_job_id, artifact_id, definition_hash, lakefs_repository, lakefs_commit_id, ontology_commit_id, mapping_spec_id, mapping_spec_version, mapping_spec_target_class_id, promoted_dataset_version_id, promoted_dataset_name, target_branch, promoted_by, metadata, manifest_id, promoted_at)` (line 2578): no docstring
    - `async list_scheduled_pipelines(self)` (line 2699): no docstring
    - `async record_schedule_tick(self, pipeline_id, scheduled_at)` (line 2753): no docstring
    - `async list_pipeline_branches(self, db_name)` (line 2768): no docstring
    - `async get_pipeline_branch(self, db_name, branch)` (line 2795): no docstring
    - `async archive_pipeline_branch(self, db_name, branch)` (line 2821): no docstring
    - `async restore_pipeline_branch(self, db_name, branch)` (line 2858): no docstring
    - `async create_branch(self, pipeline_id, new_branch, user_id)` (line 2895): no docstring
    - `async create_udf(self, db_name, name, code, description)` (line 2979): no docstring
    - `async create_udf_version(self, udf_id, code)` (line 3038): no docstring
    - `async get_udf(self, udf_id)` (line 3099): no docstring
    - `async get_udf_version(self, udf_id, version)` (line 3119): no docstring
    - `async get_udf_latest_version(self, udf_id)` (line 3146): no docstring

### `backend/shared/services/pipeline_scheduler.py`
- **Functions**
  - `_should_run_schedule(now, last_run, interval, cron)` (line 360): no docstring
  - `_cron_matches(now, expression)` (line 377): no docstring
  - `_cron_field_matches(field, value)` (line 392): no docstring
  - `_normalize_dependencies(raw)` (line 426): no docstring
  - `async _dependencies_satisfied(registry, dependencies)` (line 446): no docstring
  - `async _evaluate_dependencies(registry, dependencies)` (line 454): no docstring
  - `_is_valid_cron_expression(expression)` (line 489): no docstring
  - `_is_valid_cron_field(field)` (line 496): no docstring
- **Classes**
  - `ScheduledPipeline` (line 27): no docstring
  - `DependencyEvaluation` (line 36): no docstring
  - `PipelineScheduler` (line 42): no docstring
    - `__init__(self, registry, queue, poll_seconds, tracing)` (line 43): no docstring
    - `async run(self)` (line 57): no docstring
    - `async stop(self)` (line 77): no docstring
    - `async _tick(self)` (line 80): no docstring
    - `async _record_scheduler_config_error(self, pipeline_id, now, error_key, detail, extra)` (line 269): no docstring
    - `async _record_scheduler_ignored(self, pipeline_id, now, reason, detail, extra)` (line 313): no docstring

### `backend/shared/services/pipeline_schema_utils.py`
- **Functions**
  - `normalize_schema_type(value)` (line 28): no docstring
  - `normalize_schema_checks(checks)` (line 56): no docstring
  - `normalize_expectations(expectations)` (line 72): no docstring
  - `normalize_schema_contract(contract)` (line 93): no docstring
  - `normalize_value_list(value)` (line 110): no docstring
  - `normalize_number(value)` (line 120): no docstring
- **Classes**
  - `SchemaCheckSpec` (line 8): no docstring
  - `ExpectationSpec` (line 15): no docstring
  - `SchemaContractSpec` (line 22): no docstring

### `backend/shared/services/pipeline_transform_spec.py`
- **Functions**
  - `normalize_operation(value)` (line 35): no docstring
  - `resolve_join_spec(metadata)` (line 39): no docstring
  - `normalize_union_mode(metadata)` (line 52): no docstring
- **Classes**
  - `JoinSpec` (line 28): no docstring

### `backend/shared/services/pipeline_udf_runtime.py`
- **Functions**
  - `compile_row_udf(code)` (line 29): Compile a Python UDF for row-level transforms.
- **Classes**
  - `PipelineUdfError` (line 7): no docstring

### `backend/shared/services/pipeline_unit_test_runner.py`
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

### `backend/shared/services/processed_event_registry.py`
- **Functions**
  - `validate_lease_settings()` (line 517): no docstring
  - `validate_registry_enabled()` (line 542): no docstring
- **Classes**
  - `ClaimDecision` (line 24): no docstring
  - `ClaimResult` (line 32): no docstring
  - `ProcessedEventRegistry` (line 38): Postgres-backed idempotency + ordering guard.
    - `__init__(self, dsn, schema, lease_timeout_seconds)` (line 47): no docstring
    - `async connect(self)` (line 65): no docstring
    - `async initialize(self)` (line 77): no docstring
    - `async close(self)` (line 80): no docstring
    - `async shutdown(self)` (line 85): no docstring
    - `async ensure_schema(self)` (line 88): no docstring
    - `async claim(self, handler, event_id, aggregate_id, sequence_number)` (line 161): Try to claim an event for processing.
    - `async heartbeat(self, handler, event_id)` (line 336): Extend processing lease for a claimed event (owner-scoped).
    - `async get_event_record(self, event_id)` (line 358): no docstring
    - `async mark_done(self, handler, event_id, aggregate_id, sequence_number)` (line 397): no docstring
    - `async mark_failed(self, handler, event_id, error)` (line 465): no docstring

### `backend/shared/services/projection_manager.py`
- **Classes**
  - `ProjectionManager` (line 23): 팔란티어 스타일 프로젝션 관리
    - `__init__(self, graph_service, es_service, redis_service)` (line 33): no docstring
    - `async initialize(self)` (line 52): 프로젝션 매니저 초기화
    - `async register_projection(self, db_name, view_name, start_class, hops, filters, refresh_interval)` (line 80): 새로운 프로젝션 등록
    - `async materialize_view(self, projection_id, force_refresh)` (line 154): 프로젝션 뷰 실체화 (WOQL 실행 → ES 저장)
    - `async query_projection(self, view_name, filters, limit)` (line 271): 프로젝션 뷰 조회 (캐시된 데이터)
    - `async _projection_refresh_loop(self, projection_id)` (line 323): 프로젝션 자동 갱신 루프
    - `async _load_existing_projections(self)` (line 345): 기존 프로젝션 메타데이터 로드
    - `_generate_query_hash(self, db_name, woql_config)` (line 371): 쿼리 설정의 해시 생성
    - `async stop_projection(self, projection_id)` (line 376): 프로젝션 중지
    - `async shutdown(self)` (line 398): 프로젝션 매니저 종료

### `backend/shared/services/redis_service.py`
- **Functions**
  - `create_redis_service(settings)` (line 430): Redis 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_redis_service_legacy(host, port, password)` (line 451): 레거시 Redis 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `RedisService` (line 21): Async Redis client service with connection pooling and error handling.
    - `__init__(self, host, port, password, db, decode_responses, max_connections, socket_timeout, connection_timeout, retry_on_timeout)` (line 33): no docstring
    - `async connect(self)` (line 67): Initialize Redis connection.
    - `async initialize(self)` (line 77): ServiceContainer-compatible initialization method.
    - `async disconnect(self)` (line 81): Close Redis connection and pool.
    - `client(self)` (line 97): Get Redis client instance.
    - `async set_command_status(self, command_id, status, data, ttl)` (line 105): Set command status with optional data.
    - `async get_command_status(self, command_id)` (line 138): Get command status and data.
    - `async update_command_progress(self, command_id, progress, message)` (line 156): Update command execution progress.
    - `async set_command_result(self, command_id, result, ttl)` (line 182): Store command execution result.
    - `async get_command_result(self, command_id)` (line 204): Get command execution result.
    - `async publish_command_update(self, command_id, data)` (line 224): Publish command status update to subscribers.
    - `async subscribe_command_updates(self, command_id, callback, task_manager)` (line 239): Subscribe to command status updates with proper task tracking.
    - `async _listen_for_updates(self, pubsub, callback, command_id)` (line 288): Listen for pub/sub updates with improved error handling.
    - `_handle_listener_done(self, task)` (line 325): Handle completion of a listener task.
    - `async set_json(self, key, value, ttl)` (line 336): Set JSON value with optional TTL.
    - `async get_json(self, key)` (line 348): Get JSON value.
    - `async set(self, key, value, ttl)` (line 355): Set key-value pair with optional TTL.
    - `async get(self, key)` (line 361): Get value for key.
    - `async delete(self, key)` (line 365): Delete key.
    - `async exists(self, key)` (line 369): Check if key exists.
    - `async expire(self, key, seconds)` (line 373): Set expiration on key.
    - `async keys(self, pattern)` (line 377): Get keys matching pattern.
    - `async ping(self)` (line 381): Check Redis connection.
    - `async cleanup_listeners(self)` (line 388): Clean up all active pub/sub listeners.
    - `async scan_keys(self, pattern, count)` (line 406): Scan keys matching pattern without blocking.

### `backend/shared/services/schema_versioning.py`
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

### `backend/shared/services/sequence_service.py`
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

### `backend/shared/services/service_factory.py`
- **Functions**
  - `create_fastapi_service(service_info, custom_lifespan, include_health_check, include_logging_middleware, custom_tags)` (line 49): Create a standardized FastAPI application with common configurations.
  - `_install_openapi_language_contract(app)` (line 132): Add `?lang=en|ko` and `Accept-Language` to OpenAPI so clients discover i18n support.
  - `_configure_cors(app)` (line 182): Configure CORS middleware based on environment variables
  - `_add_logging_middleware(app)` (line 196): Add request logging middleware
  - `_add_health_check(app, service_info)` (line 209): Add standardized health check endpoints
  - `_add_debug_endpoints(app)` (line 233): Add debug endpoints for development environment
  - `_install_observability(app, service_info)` (line 242): Install tracing + metrics in a way that is:
  - `create_uvicorn_config(service_info, reload)` (line 285): Create standardized uvicorn configuration.
  - `_get_logging_config(service_name)` (line 318): Get standardized logging configuration for uvicorn
  - `run_service(app, service_info, app_module_path, reload)` (line 356): Run the service with standardized uvicorn configuration.
- **Classes**
  - `ServiceInfo` (line 27): Service configuration container
    - `__init__(self, name, title, description, version, port, host, tags)` (line 30): no docstring

### `backend/shared/services/sheet_grid_parser.py`
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

### `backend/shared/services/sheet_import_service.py`
- **Classes**
  - `FieldMapping` (line 26): no docstring
  - `SheetImportService` (line 31): Pure helpers; no network/IO.
    - `build_column_index(columns)` (line 35): no docstring
    - `_is_blank(value)` (line 48): no docstring
    - `_strip_numeric_affixes(raw)` (line 54): Remove common affixes around numeric strings (currency symbols/units/codes, percent).
    - `coerce_value(cls, value, target_type)` (line 112): Coerce a cell value into a JSON-serializable value compatible with target_type.
    - `build_instances(cls, columns, rows, mappings, target_field_types, max_rows)` (line 221): Apply mappings and type coercion to build target instances.

### `backend/shared/services/storage_service.py`
- **Functions**
  - `create_storage_service(settings)` (line 792): 스토리지 서비스 팩토리 함수 (Anti-pattern 13 해결)
  - `create_storage_service_legacy(endpoint_url, access_key, secret_key)` (line 818): 레거시 스토리지 서비스 팩토리 함수 (하위 호환성)
- **Classes**
  - `StorageService` (line 30): S3/MinIO 스토리지 서비스 - Event Sourcing 지원
    - `__init__(self, endpoint_url, access_key, secret_key, region, use_ssl)` (line 54): 스토리지 서비스 초기화
    - `async create_bucket(self, bucket_name)` (line 99): 버킷 생성
    - `async bucket_exists(self, bucket_name)` (line 120): 버킷 존재 여부 확인
    - `async save_json(self, bucket, key, data, metadata)` (line 139): JSON 데이터를 S3에 저장하고 체크섬 반환
    - `async save_bytes(self, bucket, key, data, content_type, metadata)` (line 186): Raw bytes를 S3에 저장하고 체크섬 반환
    - `async save_fileobj(self, bucket, key, fileobj, content_type, metadata, checksum)` (line 233): Stream a file-like object into S3 and return a checksum.
    - `async load_json(self, bucket, key)` (line 289): S3에서 JSON 데이터 로드
    - `async load_bytes(self, bucket, key)` (line 310): S3에서 Raw bytes 로드
    - `async verify_checksum(self, bucket, key, expected_checksum)` (line 329): 저장된 파일의 체크섬 검증
    - `async delete_object(self, bucket, key)` (line 354): S3 객체 삭제
    - `async delete_prefix(self, bucket, prefix)` (line 371): Delete all objects under a prefix.
    - `async list_objects(self, bucket, prefix, max_keys)` (line 413): 버킷의 객체 목록 조회
    - `async list_objects_paginated(self, bucket, prefix, max_keys, continuation_token)` (line 441): Paginated object listing (returns next continuation token if more).
    - `async iter_objects(self, bucket, prefix, max_keys)` (line 462): Async iterator over all objects under prefix (pagination-aware).
    - `async get_object_metadata(self, bucket, key)` (line 485): 객체 메타데이터 조회
    - `generate_instance_path(self, db_name, class_id, instance_id, command_id)` (line 513): 인스턴스 이벤트 저장 경로 생성
    - `async get_all_commands_for_instance(self, bucket, db_name, class_id, instance_id)` (line 534): 특정 인스턴스의 모든 Command 파일 목록 조회
    - `async replay_instance_state(self, bucket, command_files)` (line 597): Command 파일들을 순차적으로 읽어 인스턴스의 최종 상태 재구성
    - `is_instance_deleted(self, instance_state)` (line 754): 인스턴스가 삭제된 상태인지 확인
    - `get_deletion_info(self, instance_state)` (line 768): 삭제된 인스턴스의 삭제 정보 반환

### `backend/shared/services/sync_wrapper_service.py`
- **Classes**
  - `SyncWrapperService` (line 20): 비동기 Command API를 동기적으로 래핑하는 서비스.
    - `__init__(self, command_status_service)` (line 27): no docstring
    - `async wait_for_command(self, command_id, options)` (line 30): Command가 완료될 때까지 기다리고 결과를 반환합니다.
    - `async _poll_until_complete(self, command_id, options, progress_history, start_time)` (line 113): Command가 완료될 때까지 주기적으로 상태를 확인합니다.
    - `async execute_sync(self, async_func, request_data, options)` (line 177): 비동기 함수를 실행하고 결과를 기다립니다.
    - `async get_command_progress(self, command_id)` (line 217): Command의 현재 진행 상태를 조회합니다.

### `backend/shared/services/watermark_monitor.py`
- **Functions**
  - `async create_watermark_monitor(kafka_config, redis_url, consumer_groups, topics)` (line 422): Create and start a watermark monitor
- **Classes**
  - `PartitionWatermark` (line 23): Watermark information for a single partition
    - `progress_percentage(self)` (line 34): Calculate progress as percentage
  - `GlobalWatermark` (line 44): Aggregated watermark across all partitions
    - `is_healthy(self)` (line 57): Check if lag is within acceptable limits
    - `estimated_catch_up_time_ms(self)` (line 63): Estimate time to catch up based on processing rate
  - `WatermarkMonitor` (line 73): Monitor Kafka consumer lag and watermarks across all partitions
    - `__init__(self, kafka_config, redis_client, consumer_groups, topics, alert_threshold_ms)` (line 85): Initialize watermark monitor
    - `async start_monitoring(self, interval_seconds)` (line 123): Start monitoring watermarks
    - `async stop_monitoring(self)` (line 140): Stop monitoring watermarks
    - `async _monitor_loop(self, interval_seconds)` (line 157): Main monitoring loop
    - `async update_watermarks(self, consumer_group)` (line 184): Update watermarks for a consumer group
    - `calculate_global_watermark(self)` (line 237): Calculate global watermark across all partitions
    - `async store_metrics(self)` (line 275): Store metrics in Redis for historical tracking
    - `async check_alerts(self)` (line 319): Check for lag alerts and trigger notifications
    - `async export_prometheus_metrics(self)` (line 345): Export metrics in Prometheus format
    - `async get_current_lag(self)` (line 371): Get current lag information
    - `async get_partition_details(self, topic)` (line 395): Get detailed lag information for a specific topic

### `backend/shared/services/websocket_service.py`
- **Functions**
  - `utc_now()` (line 20): no docstring
  - `get_connection_manager()` (line 408): WebSocket 연결 관리자 싱글톤 인스턴스 반환
  - `get_notification_service(redis_service)` (line 416): WebSocket 알림 서비스 싱글톤 인스턴스 반환
- **Classes**
  - `WebSocketConnection` (line 25): WebSocket 연결 정보
  - `WebSocketConnectionManager` (line 35): WebSocket 연결 관리자
    - `__init__(self)` (line 44): no docstring
    - `async connect(self, websocket, client_id, user_id)` (line 52): 새로운 WebSocket 연결 수락
    - `async disconnect(self, client_id)` (line 77): WebSocket 연결 해제
    - `async subscribe_command(self, client_id, command_id)` (line 99): 특정 Command에 대한 업데이트 구독
    - `async unsubscribe_command(self, client_id, command_id)` (line 114): Command 구독 해제
    - `async send_to_client(self, client_id, message)` (line 130): 특정 클라이언트에게 메시지 전송
    - `async broadcast_command_update(self, command_id, update_data)` (line 148): Command 업데이트를 구독 중인 클라이언트들에게 브로드캐스트
    - `async send_to_user(self, user_id, message)` (line 181): 특정 사용자의 모든 연결에 메시지 전송
    - `async broadcast_to_all(self, message)` (line 198): Broadcast a message to all connected clients.
    - `async ping_all_clients(self)` (line 218): 모든 클라이언트에 ping 전송 (연결 상태 확인)
    - `get_connection_stats(self)` (line 241): 연결 통계 반환
  - `WebSocketNotificationService` (line 254): WebSocket 알림 서비스 with proper task tracking
    - `__init__(self, redis_service, connection_manager, task_manager)` (line 262): no docstring
    - `async start(self)` (line 275): 알림 서비스 시작 with proper task tracking
    - `async stop(self)` (line 297): 알림 서비스 중지 with proper cleanup
    - `_handle_pubsub_task_done(self, task)` (line 316): Handle completion of pubsub task.
    - `async _restart_pubsub_listener(self)` (line 329): Restart the pubsub listener after a failure.
    - `async _listen_redis_updates(self)` (line 336): Redis Pub/Sub 채널을 수신하여 WebSocket으로 전달 with improved error handling
    - `async notify_task_update(self, update_data)` (line 390): Send task update notification to all connected clients.

### `backend/shared/setup.py`

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

### `backend/shared/utils/__init__.py`

### `backend/shared/utils/app_logger.py`
- **Functions**
  - `get_logger(name, level)` (line 11): Get a configured logger instance.
  - `configure_logging(level)` (line 52): Configure global logging settings.
  - `get_funnel_logger(name)` (line 74): Get Funnel service logger.
  - `get_bff_logger(name)` (line 79): Get BFF service logger.
  - `get_oms_logger(name)` (line 84): Get OMS service logger.

### `backend/shared/utils/branch_utils.py`
- **Functions**
  - `protected_branch_write_message()` (line 12): no docstring
  - `get_protected_branches(env_key, defaults)` (line 16): no docstring

### `backend/shared/utils/chaos.py`
- **Functions**
  - `chaos_enabled()` (line 26): no docstring
  - `_sanitize_marker(point)` (line 30): no docstring
  - `maybe_crash(point, logger)` (line 36): Crash the current process if CHAOS_CRASH_POINT matches.

### `backend/shared/utils/commit_utils.py`
- **Functions**
  - `coerce_commit_id(value)` (line 6): no docstring

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
  - `normalize_import_target_type(type_value)` (line 12): Normalize a target field type for import.

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

### `backend/shared/utils/label_mapper.py`
- **Classes**
  - `LabelMapper` (line 25): 레이블과 ID 간의 매핑을 관리하는 클래스
    - `_resolve_database_path(db_path)` (line 32): 데이터베이스 파일 경로를 안전하게 해결합니다.
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
    - `async register_property(self, db_name, class_id, property_id, label)` (line 620): 속성 레이블 매핑 등록
    - `async register_relationship(self, db_name, predicate, label)` (line 656): 관계 레이블 매핑 등록
    - `async get_class_id(self, db_name, label, lang)` (line 688): 레이블로 클래스 ID 조회
    - `async get_class_label(self, db_name, class_id, lang)` (line 718): 클래스 ID로 레이블 조회
    - `async get_property_id(self, db_name, class_id, label, lang)` (line 748): 레이블로 속성 ID 조회
    - `async get_predicate(self, db_name, label, lang)` (line 781): 레이블로 관계 술어 조회
    - `async convert_query_to_internal(self, db_name, query, lang)` (line 811): 레이블 기반 쿼리를 내부 ID 기반으로 변환
    - `async convert_to_display(self, db_name, data, lang)` (line 903): 내부 ID 기반 데이터를 레이블 기반으로 변환
    - `async get_property_label(self, db_name, class_id, property_id, lang)` (line 924): 속성 ID로 레이블 조회 (공개 메서드)
    - `async _get_property_label(self, db_name, class_id, property_id, lang)` (line 941): 속성 ID로 레이블 조회 (내부 메서드)
    - `async _get_relationship_label(self, db_name, predicate, lang)` (line 963): 관계 술어로 레이블 조회
    - `_extract_labels(self, label)` (line 985): 레이블에서 언어별 텍스트 추출
    - `async update_mappings(self, db_name, ontology_data)` (line 1021): 온톨로지 데이터로부터 모든 매핑 업데이트
    - `async remove_class(self, db_name, class_id)` (line 1050): 클래스 관련 모든 매핑 제거
    - `async export_mappings(self, db_name)` (line 1081): 특정 데이터베이스의 모든 매핑 내보내기
    - `async import_mappings(self, data)` (line 1151): 매핑 데이터 가져오기

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
  - `sha256_hex(value)` (line 23): no docstring
  - `stable_json_dumps(obj)` (line 28): no docstring
  - `digest_for_audit(obj)` (line 32): no docstring
  - `truncate_text(text, max_chars)` (line 36): no docstring
  - `_mask_email(text)` (line 44): no docstring
  - `_mask_long_digits(text)` (line 53): no docstring
  - `mask_pii_text(text, max_chars)` (line 65): no docstring
  - `mask_pii(obj, max_string_chars)` (line 74): Recursively mask likely PII in a JSON-like structure.
  - `sample_items(items, max_items)` (line 97): no docstring

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

### `backend/shared/utils/pythonpath_setup.py`
- **Functions**
  - `detect_backend_directory()` (line 12): Dynamically detect the backend directory by looking for characteristic files/directories.
  - `setup_pythonpath(backend_dir)` (line 45): Setup PYTHONPATH to include the backend directory.
  - `validate_pythonpath()` (line 95): Validate that PYTHONPATH is correctly configured by testing imports.
  - `configure_python_environment(backend_dir, verbose)` (line 113): Complete Python environment configuration including PYTHONPATH setup and validation.
  - `ensure_backend_in_path()` (line 162): Convenience function to ensure backend directory is in Python path.

### `backend/shared/utils/s3_uri.py`
- **Functions**
  - `is_s3_uri(value)` (line 11): no docstring
  - `build_s3_uri(bucket, key)` (line 15): no docstring
  - `parse_s3_uri(uri)` (line 25): no docstring
  - `normalize_s3_uri(value, bucket)` (line 36): no docstring

### `backend/shared/utils/schema_hash.py`
- **Functions**
  - `compute_schema_hash(columns)` (line 12): Produce a stable hash for a list of column definitions.

### `backend/shared/utils/terminus_branch.py`
- **Functions**
  - `encode_branch_name(branch_name)` (line 21): no docstring
  - `decode_branch_name(branch_name)` (line 30): no docstring

### `backend/shared/utils/time_utils.py`
- **Functions**
  - `utcnow()` (line 6): no docstring

### `backend/shared/validators/__init__.py`
- **Functions**
  - `get_validator(data_type)` (line 54): Get validator instance for a specific data type
  - `register_validator(data_type, validator_class)` (line 70): Register a new validator
  - `get_composite_validator()` (line 81): Get a composite validator with all registered validators

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
    - `normalize(self, value)` (line 65): Normalize array value
    - `get_supported_types(self)` (line 79): Get supported types
    - `_validate_item_types(self, array, item_type)` (line 83): Validate types of array items
    - `_validate_basic_type(self, value, type_name)` (line 116): Validate basic types

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
    - `validate(cls, value, data_type, constraints)` (line 194): Validate a value against a complex data type with constraints.
    - `_validate_string_type(cls, value)` (line 242): Validate string type.
    - `_validate_integer_type(cls, value, data_type)` (line 249): Validate integer types (int, long, short, byte).
    - `_validate_unsigned_integer_type(cls, value, data_type)` (line 256): Validate unsigned integer types.
    - `_validate_float_type(cls, value)` (line 265): Validate float/double types.
    - `_validate_boolean_type(cls, value)` (line 272): Validate boolean type.
    - `_validate_decimal_type(cls, value)` (line 279): Validate decimal type.
    - `_validate_date_type(cls, value)` (line 287): Validate date type.
    - `_validate_datetime_type(cls, value)` (line 300): Validate datetime type.
    - `_validate_uri_type(cls, value)` (line 313): Validate URI type.
    - `_get_type_validator_map(cls)` (line 325): Get mapping of data types to their validator functions.
    - `_validate_xsd_type(cls, value, data_type, constraints)` (line 347): Validate XSD data types using type-specific validators.
    - `get_supported_types(cls)` (line 360): Get list of supported complex types.
    - `is_supported_type(cls, data_type)` (line 393): Check if data type is supported.

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

## simple_schema_test.py

### `backend/simple_schema_test.py`
- **Functions**
  - `_admin_headers()` (line 16): no docstring
  - `async create_simple_schema(db_name)` (line 20): Create schema using OMS ontology endpoints.
  - `async test_create_instance(db_name)` (line 96): Test creating a lightweight instance
  - `db_name()` (line 137): no docstring
  - `async main()` (line 140): Main function

## start_services.py

### `backend/start_services.py`
- **Functions**
  - `start_service(name, path, command, port, health_path)` (line 20): Start a service and verify it's running
  - `stop_services(processes)` (line 75): Stop all services
  - `main()` (line 89): no docstring

## test_full_api_integration_ultra.py

### `backend/test_full_api_integration_ultra.py`
- **Functions**
  - `async _wait_for_command_completed(session, command_id, db_name, timeout_seconds, poll_interval_seconds)` (line 31): no docstring
  - `async test_full_api_integration()` (line 64): no docstring

## test_lightweight_nodes_ultra.py

### `backend/test_lightweight_nodes_ultra.py`
- **Functions**
  - `async test_lightweight_architecture()` (line 25): no docstring

## test_ontology_issue.py

### `backend/test_ontology_issue.py`
- **Functions**
  - `async test_ontology_creation()` (line 13): no docstring

## test_palantir_architecture_verification.py

### `backend/test_palantir_architecture_verification.py`
- **Functions**
  - `async test_complete_user_flow()` (line 24): no docstring

## test_palantir_federation.py

### `backend/test_palantir_federation.py`
- **Functions**
  - `async test_palantir_federation()` (line 28): no docstring

## test_real_production_flow.py

### `backend/test_real_production_flow.py`
- **Functions**
  - `async test_real_production_flow()` (line 527): no docstring
  - `async main()` (line 534): Run the production test
- **Classes**
  - `ProductionFlowTest` (line 52): REAL production flow test - no mocks
    - `__init__(self)` (line 55): no docstring
    - `async verify_infrastructure(self)` (line 60): Verify ALL services are running and accessible
    - `async create_test_database(self)` (line 176): Create a test database via OMS API
    - `async create_test_ontology(self, db_name)` (line 209): Create test ontology
    - `async create_test_instance(self, db_name, class_id)` (line 257): Create test instance
    - `async verify_s3_events(self)` (line 290): Verify events in S3
    - `async verify_postgresql_registry(self)` (line 331): Verify processed-event registry entries
    - `async verify_kafka_messages(self)` (line 369): Verify Kafka message flow
    - `async run_complete_test(self)` (line 447): Run the complete production test

## tests

### `backend/tests/__init__.py`

### `backend/tests/chaos_lite.py`
- **Functions**
  - `_read_env_file(path)` (line 35): no docstring
  - `_run(cmd, cwd, env)` (line 70): no docstring
  - `_run_input(cmd, cwd, input_text, env)` (line 86): no docstring
  - `_docker_compose(args, extra_env)` (line 102): no docstring
  - `_http_json(method, url, payload, timeout_s)` (line 109): no docstring
  - `_wait_until(name, fn, timeout_s, interval_s)` (line 123): no docstring
  - `_wait_http_ok(url, timeout_s)` (line 135): no docstring
  - `_wait_db_exists(db_name, expected, timeout_s)` (line 143): no docstring
  - `_wait_ontology(db_name, class_id, timeout_s)` (line 156): no docstring
  - `_wait_command_completed(command_id, timeout_s)` (line 169): no docstring
  - `_instances_index(db_name)` (line 184): no docstring
  - `_wait_es_doc(index, doc_id, timeout_s)` (line 189): no docstring
  - `_graph_query(db_name, product_id, include_provenance)` (line 203): no docstring
  - `_assert_graph_full(result)` (line 223): no docstring
  - `_domain_event_id(command_id, event_type, aggregate_id)` (line 233): no docstring
  - `_check_s3_has_event_id(event_id)` (line 237): no docstring
  - `_wait_s3_has_event_id(event_id, timeout_s)` (line 257): no docstring
  - `_read_s3_event_envelope(event_id)` (line 265): no docstring
  - `_psql_scalar(sql)` (line 287): no docstring
  - `_write_side_last_sequence(aggregate_type, aggregate_id)` (line 308): no docstring
  - `_assert_registry_done(handler, event_id)` (line 319): no docstring
  - `_assert_registry_status(handler, event_id, expected_status)` (line 334): no docstring
  - `_wait_registry_done(handler, event_id, timeout_s)` (line 346): no docstring
  - `_wait_registry_status(handler, event_id, expected_status, timeout_s)` (line 354): no docstring
  - `_kafka_produce_json(topic, key, payload)` (line 364): no docstring
  - `_setup_db_and_ontologies()` (line 388): no docstring
  - `_create_customer_and_product(db_name, customer_id, product_id, wait_command)` (line 430): no docstring
  - `_assert_converged(db_name, customer_id, product_id, customer_command_id, product_command_id, retry_expected)` (line 466): no docstring
  - `scenario_kafka_down_then_recover()` (line 524): no docstring
  - `scenario_redis_down_then_recover()` (line 556): no docstring
  - `scenario_es_down_then_recover()` (line 578): no docstring
  - `scenario_terminus_down_then_recover()` (line 610): no docstring
  - `scenario_instance_worker_crash_after_claim()` (line 642): no docstring
  - `scenario_out_of_order_delivery()` (line 680): no docstring
  - `scenario_soak_random_failures(duration_s, seed)` (line 764): Soak test with real infra + random partial failures (no mocks).
  - `main()` (line 855): no docstring
- **Classes**
  - `Endpoints` (line 53): no docstring

### `backend/tests/conftest.py`
- **Functions**
  - `pytest_configure()` (line 6): Host-run integration defaults for the `backend/tests` suite.

### `backend/tests/connectors/__init__.py`

### `backend/tests/integration/test_pipeline_branch_lifecycle.py`
- **Functions**
  - `async test_pipeline_branch_lifecycle(monkeypatch)` (line 12): no docstring

### `backend/tests/test_auth_hardening_e2e.py`
- **Functions**
  - `_strip_auth_env(env)` (line 25): no docstring
  - `_run_auth_check(module, func, env)` (line 42): no docstring
  - `test_auth_disabled_requires_explicit_allow()` (line 51): no docstring
  - `async test_oms_write_requires_auth()` (line 65): no docstring

### `backend/tests/test_branch_virtualization_e2e.py`
- **Functions**
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 53): Best-effort: fetch current write-side sequence for OCC cleanup (returns None if Postgres unavailable).
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 108): no docstring
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 128): no docstring
  - `async _wait_for_ontology_present(session, db_name, ontology_id, timeout_seconds, poll_interval_seconds)` (line 153): no docstring
  - `async _wait_for_graph_node(session, db_name, branch, class_id, primary_key_value, expected_name, require_overlay_index, timeout_seconds, poll_interval_seconds)` (line 174): Poll BFF graph query until a single node is returned and ES enrichment is ready.
  - `async test_branch_virtualization_overlay_copy_on_write()` (line 252): no docstring

### `backend/tests/test_command_status_ttl_e2e.py`
- **Functions**
  - `_set_env(**updates)` (line 22): no docstring
  - `_redis_params()` (line 39): no docstring
  - `async test_command_status_ttl_configurable()` (line 58): no docstring

### `backend/tests/test_core_functionality.py`
- **Functions**
  - `_get_postgres_url_candidates()` (line 30): Return Postgres DSN candidates (env override first, then common local ports).
  - `async _resolve_bff_path(session, candidates)` (line 55): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 74): Fetch the current write-side sequence for an aggregate from Postgres.
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 138): no docstring
  - `async _wait_for_command_terminal_state(session, command_id, timeout_seconds, poll_interval_seconds)` (line 159): Wait until an async (202 Accepted) command reaches a terminal state.
  - `async _wait_for_ontology_present(session, db_name, ontology_id, timeout_seconds, poll_interval_seconds)` (line 194): no docstring
  - `async _wait_for_es_doc(session, index_name, doc_id, timeout_seconds, poll_interval_seconds)` (line 216): no docstring
- **Classes**
  - `TestCoreOntologyManagement` (line 240): Test suite for Ontology Management Service
    - `async test_database_lifecycle(self)` (line 245): Test complete database lifecycle with Event Sourcing
    - `async test_ontology_creation(self)` (line 286): Test ontology creation with complex types
    - `async test_ontology_i18n_label_projection(self)` (line 352): Ensure i18n labels are normalized for ES and preserved in label_i18n.
    - `async test_ontology_creation_advanced_relationships(self)` (line 457): Test advanced ontology creation path is truly event-sourced and functional.
  - `TestBFFGraphFederation` (line 530): Test suite for BFF Graph Federation capabilities
    - `async test_schema_suggestion(self)` (line 535): Test ML-driven schema suggestion
    - `async test_graph_query_federation(self)` (line 578): Test federated graph queries with Elasticsearch
  - `TestEventSourcingInfrastructure` (line 591): Test Event Sourcing and CQRS infrastructure
    - `async test_s3_event_storage(self)` (line 599): Verify S3/MinIO event storage is working
    - `async test_postgresql_processed_event_registry(self)` (line 646): Verify Postgres processed_events registry is available (idempotency contract)
    - `async test_kafka_message_flow(self)` (line 710): Verify Kafka message flow is operational
  - `TestComplexTypes` (line 733): Test complex type validation and handling
    - `test_email_validation(self)` (line 737): Test email type validation
    - `test_phone_validation(self)` (line 753): Test phone number validation
    - `test_json_validation(self)` (line 764): Test JSON type validation
  - `TestHealthEndpoints` (line 781): Test all service health endpoints
    - `async test_oms_health(self)` (line 786): Test OMS health endpoint
    - `async test_bff_health(self)` (line 796): Test BFF health endpoint
    - `async test_funnel_health(self)` (line 806): Test Funnel health endpoint

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
  - `async _assert_command_event_has_ontology_stamp(event_id)` (line 41): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 57): Best-effort: fetch current write-side sequence for OCC cleanup (returns None if Postgres unavailable).
  - `async _wait_for_db_exists(session, db_name, expected, timeout_seconds, poll_interval_seconds)` (line 112): no docstring
  - `async _wait_for_command_completed(session, command_id, db_name, timeout_seconds, poll_interval_seconds)` (line 132): no docstring
  - `async _wait_for_ontology_present(session, db_name, ontology_id, timeout_seconds, poll_interval_seconds)` (line 164): no docstring
  - `async _wait_for_instance_count(session, db_name, class_id, expected_count, timeout_seconds, poll_interval_seconds)` (line 185): no docstring
  - `async test_oms_end_to_end_smoke()` (line 210): no docstring

### `backend/tests/test_openapi_contract_smoke.py`
- **Functions**
  - `_load_repo_dotenv()` (line 54): Best-effort loader for the repo root `.env` used by docker-compose port overrides.
  - `_get_postgres_url_candidates()` (line 83): no docstring
  - `async _get_write_side_last_sequence(aggregate_type, aggregate_id)` (line 110): Fetch current write-side aggregate sequence (OCC expected_seq) from Postgres.
  - `async _wait_for_command_completed(session, command_id, timeout_seconds, poll_interval_seconds)` (line 162): no docstring
  - `_xlsx_bytes(header, rows)` (line 192): no docstring
  - `_csv_bytes(header, rows)` (line 210): no docstring
  - `_is_wip(op)` (line 230): no docstring
  - `_is_ops_only(op)` (line 240): no docstring
  - `_safe_pipeline_ref(value)` (line 281): no docstring
  - `async _request(session, plan)` (line 311): no docstring
  - `_format_path(template, ctx, overrides)` (line 339): no docstring
  - `_normalize_db_path(path)` (line 362): no docstring
  - `_pick_spec_path(paths, *candidates)` (line 370): no docstring
  - `_ontology_payload(class_id, label_en, label_ko)` (line 378): no docstring
  - `_mapping_file_bytes(ctx)` (line 403): no docstring
  - `_target_schema_json(ctx)` (line 425): no docstring
  - `_mappings_json(ctx)` (line 434): no docstring
  - `async _build_plan(op, ctx)` (line 442): Return a runnable RequestPlan for every non-WIP/non-ops operation.
  - `async test_openapi_stable_contract_smoke()` (line 1207): no docstring
- **Classes**
  - `Operation` (line 223): no docstring
  - `SmokeContext` (line 249): no docstring
    - `ontology_aggregate_id(self)` (line 262): no docstring
    - `advanced_ontology_aggregate_id(self)` (line 266): no docstring
    - `instance_aggregate_id(self)` (line 270): no docstring
    - `pipeline_name(self)` (line 274): no docstring
    - `dataset_name(self)` (line 278): no docstring
  - `RequestPlan` (line 298): no docstring

### `backend/tests/test_pipeline_execution_semantics_e2e.py`
- **Functions**
  - `_parse_s3_uri(uri)` (line 29): no docstring
  - `_lakefs_s3_client()` (line 37): no docstring
  - `_list_relative_object_keys(bucket, commit_id, artifact_prefix)` (line 64): List the object keys for a dataset artifact under a specific lakeFS ref (commit id),
  - `_load_rows_from_artifact(bucket, commit_id, artifact_prefix)` (line 100): no docstring
  - `_load_partitioned_rows_from_artifact(bucket, commit_id, artifact_prefix)` (line 150): no docstring
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 187): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 212): no docstring
  - `_artifact_for_output(run, node_id)` (line 236): no docstring
  - `_commit_and_prefix_from_artifact(artifact_key)` (line 251): no docstring
  - `async test_snapshot_overwrites_outputs_across_runs()` (line 261): Checklist CL-015:
  - `async test_incremental_appends_outputs_and_preserves_previous_parts()` (line 388): Checklist CL-016:
  - `async test_incremental_watermark_boundary_includes_equal_timestamp_rows()` (line 536): Boundary check: rows with watermark == previous max should still appear in output (no gaps).
  - `async test_incremental_empty_diff_noop()` (line 665): Checklist CL-020:
  - `async test_incremental_removed_files_noop()` (line 809): Checklist CL-021:
  - `async test_run_branch_conflict_fallback_and_cleanup()` (line 1005): Retry safety: pre-existing run branch should not block deploy; fallback branch must be used and cleaned up.
  - `async test_partition_column_special_chars_roundtrip()` (line 1135): Checklist CL-021:
  - `async test_pk_semantics_append_log_allows_duplicate_ids()` (line 1248): P0-3: append_log should not enforce unique PK.
  - `async test_pk_semantics_append_state_blocks_duplicate_ids()` (line 1379): P0-3: append_state must enforce unique PK and fail on duplicates.
  - `async test_pk_semantics_remove_requires_delete_column()` (line 1511): P0-3: remove semantics must enforce deleteColumn.
  - `async test_schema_contract_breach_blocks_deploy()` (line 1611): Schema contract should fail when required columns or types mismatch.
  - `async test_executor_vs_worker_validation_consistency()` (line 1703): Compare in-memory executor vs spark worker validation errors for PK expectations.
  - `async test_incremental_small_files_compaction_metrics()` (line 1851): Perf check: measure small file growth across repeated incremental runs.
  - `async test_composite_pk_unique_perf()` (line 1998): Perf check: composite PK unique validation should complete without OOM/shuffle failures.

### `backend/tests/test_pipeline_streaming_semantics_e2e.py`
- **Functions**
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 20): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 51): no docstring
  - `async test_streaming_build_deploy_promotes_all_outputs()` (line 77): Checklist CL-017:
  - `async test_streaming_build_fails_as_job_group_on_contract_mismatch()` (line 207): Checklist CL-017:

### `backend/tests/test_pipeline_type_mismatch_guard_e2e.py`
- **Functions**
  - `async _wait_for_command(client, command_id, timeout_seconds, db_name)` (line 18): no docstring
  - `async _wait_for_run_terminal(client, pipeline_id, job_id, timeout_seconds)` (line 49): no docstring
  - `async test_preview_rejects_type_mismatch_in_compute_expression()` (line 75): Checklist CL-006:

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

### `backend/tests/unit/middleware/test_middleware_fixes.py`
- **Functions**
  - `_set_env(**updates)` (line 14): no docstring
  - `test_i18n_large_json_not_truncated()` (line 32): no docstring
  - `test_i18n_translates_description_field()` (line 49): no docstring
  - `test_rate_limit_headers_attach_for_dict_response()` (line 68): no docstring
  - `test_bff_auth_middleware_blocks_unsafe_methods()` (line 87): no docstring

### `backend/tests/unit/observability/test_config_monitor.py`
- **Functions**
  - `test_config_monitor_current_endpoint_ok()` (line 9): no docstring

### `backend/tests/unit/observability/test_context_propagation.py`
- **Functions**
  - `test_kafka_headers_roundtrip_via_attached_context()` (line 11): no docstring
  - `test_kafka_headers_from_envelope_metadata_only_emits_known_keys()` (line 33): no docstring

### `backend/tests/unit/observability/test_tracing_config.py`
- **Functions**
  - `_set_env(**updates)` (line 11): no docstring
  - `test_otlp_export_disabled_when_no_endpoint()` (line 29): no docstring

### `backend/tests/unit/openapi/test_wip_hidden.py`
- **Functions**
  - `test_wip_projection_endpoints_hidden_from_openapi()` (line 8): no docstring

### `backend/tests/unit/security/__init__.py`

### `backend/tests/unit/services/__init__.py`

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

### `backend/tests/unit/services/test_ontology_linter_pk_branching.py`
- **Functions**
  - `_reset_env(monkeypatch)` (line 9): no docstring
  - `_make_properties()` (line 22): no docstring
  - `test_linter_allows_implicit_pk_on_dev_branch(monkeypatch)` (line 32): no docstring
  - `test_linter_blocks_implicit_pk_on_protected_branch(monkeypatch)` (line 52): no docstring

### `backend/tests/unit/services/test_pipeline_control_plane_events.py`
- **Functions**
  - `async test_control_plane_events_always_on(monkeypatch)` (line 9): no docstring
  - `async test_control_plane_event_emits_with_topic(monkeypatch)` (line 34): no docstring

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

### `backend/tests/unit/services/test_pipeline_executor_preview.py`
- **Functions**
  - `async test_executor_preview_supports_node_level_preview_and_row_count()` (line 47): no docstring
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
  - `test_expectations_unique_detects_duplicate_primary_key()` (line 7): no docstring
  - `test_expectations_unique_detects_duplicate_composite_key()` (line 15): no docstring
  - `test_expectations_row_count_bounds()` (line 26): no docstring
  - `test_schema_contract_missing_required_column_is_reported()` (line 37): no docstring
  - `test_schema_contract_type_mismatch_is_reported()` (line 45): no docstring

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

### `backend/tests/unit/utils/__init__.py`

### `backend/tests/unit/utils/test_label_mapper_i18n.py`
- **Functions**
  - `async test_label_mapper_detects_language_for_string_and_falls_back(tmp_path)` (line 7): no docstring
  - `async test_label_mapper_supports_language_map_and_reverse_lookup(tmp_path)` (line 22): no docstring
  - `async test_label_mapper_batch_fallback_returns_best_available(tmp_path)` (line 42): no docstring

### `backend/tests/unit/utils/test_utils_core.py`
- **Functions**
  - `test_parse_bool_env_and_int_env(monkeypatch)` (line 6): no docstring
  - `test_safe_path_helpers()` (line 20): no docstring
  - `test_s3_uri_helpers()` (line 27): no docstring
  - `test_branch_utils_defaults(monkeypatch)` (line 35): no docstring

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

### `backend/tests/unit/workers/test_instance_worker_s3.py`
- **Functions**
  - `async test_s3_call_does_not_block_event_loop()` (line 11): no docstring

### `backend/tests/utils/__init__.py`

### `backend/tests/utils/auth.py`
- **Functions**
  - `require_token(env_keys)` (line 7): no docstring
  - `bff_auth_headers()` (line 15): no docstring
  - `oms_auth_headers()` (line 32): no docstring

## update_imports.py

### `backend/update_imports.py`
- **Functions**
  - `update_imports_in_file(file_path)` (line 11): Update imports in a single file
  - `find_python_files(root_dir)` (line 35): Find all Python files excluding backup directories
  - `main()` (line 47): no docstring

## validate_environment.py

### `backend/validate_environment.py`
- **Functions**
  - `async main()` (line 398): no docstring
- **Classes**
  - `EnvironmentValidator` (line 25): no docstring
    - `__init__(self)` (line 26): no docstring
    - `print_header(self, title)` (line 30): no docstring
    - `check_result(self, name, success, message)` (line 35): no docstring
    - `validate_env_variables(self)` (line 44): Validate all required environment variables
    - `async check_postgresql(self)` (line 103): Check PostgreSQL connection and processed-event registry tables
    - `async check_redis(self)` (line 164): Check Redis connection
    - `async check_elasticsearch(self)` (line 189): Check Elasticsearch connection
    - `check_kafka(self)` (line 215): Check Kafka connection
    - `async check_terminus(self)` (line 248): Check TerminusDB connection
    - `async check_services(self)` (line 286): Check all microservices
    - `check_docker_config(self)` (line 309): Check Docker configuration
    - `async check_workers(self)` (line 333): Check background workers
    - `async run_all_checks(self)` (line 361): Run all validation checks
