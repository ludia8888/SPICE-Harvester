# Foundry Checklist Verification Report

- Checklist: `docs/PipelineBuilder_checklist.md`
- Matrix: `docs/foundry_checklist/FOUNDARY_CHECKLIST_MATRIX.yml`
- Evidence dir: `docs/foundry_checklist/evidence/`

## Current Status
- P0: 41/45 PASS
- P1: 0/28 PASS
- P2: 0/4 PASS

## How to Reproduce
```bash
./scripts/verify_foundry_checklist.sh
```

## Per-Item Results

### CL-001 (P0) — PASS

- Original: •	☐ 그래프 캔버스에서 파이프라인을 작성할 수 있다 (입력→변환→출력의 노드 연결).
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`

### CL-002 (P0) — PASS

- Original: •	☐ 폼(설정 패널)에서 변환 파라미터를 구성할 수 있다.
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`

### CL-003 (P0) — PASS

- Original: •	☐ 작성 중 즉시 피드백(실시간 검증) 제공: 예) 조인 키 후보, 컬럼 캐스팅 제안.
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`

### CL-004 (P0) — PASS

- Original: •	☐ 대규모 파이프라인에서도 편집/탐색이 유지된다(관리 기능 포함).
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-large-graph.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-large-graph.spec.ts 2>&1 | tee ../docs/foundry_checklist/evidence/e2e_large_graph.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_large_graph.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-large-f46a1-eline-via-search-focus-mode/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-large-f46a1-eline-via-search-focus-mode/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_large_graph.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-large-f46a1-eline-via-search-focus-mode/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-large-f46a1-eline-via-search-focus-mode/trace.zip`

### CL-005 (P0) — PASS

- Original: •	☐ 스키마(컬럼 타입) 추론/전파가 변환 노드를 통과하며 유지된다.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_preview.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_executor_preview.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl005_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl005_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl005_test.log`

### CL-006 (P0) — PASS

- Original: •	☐ 타입 불일치 연산을 즉시 차단/경고한다(작성 단계).
- Verification type: `integration`
- Tests:
  - `backend/tests/test_pipeline_type_mismatch_guard_e2e.py`
- Commands:
  - `PYTHONPATH=backend ADMIN_TOKEN=test-token pytest -q backend/tests/test_pipeline_type_mismatch_guard_e2e.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl006_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl006_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl006_test.log`

### CL-007 (P0) — PASS

- Original: •	☐ 함수 카테고리(행 단위/집계/생성기) 수준의 표현식 체계가 있다.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_function_categories.py`
- Commands:
  - `PYTHONPATH=backend pytest -q backend/tests/unit/services/test_pipeline_executor_function_categories.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl007_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl007_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl007_test.log`

### CL-008 (P0) — PASS

- Original: •	☑ Join 변환 제공(최소 inner/left/right/full 중 일부라도 명확).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-009 (P0) — PASS

- Original: •	☑ Union 변환 제공 + 스키마 동일성 강제.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_executor_transform_safety.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-010 (P0) — PASS

- Original: •	☐ 변환 표현을 두 가지 방식으로 보기 제공(예: 보드 렌더링 vs 의사코드 렌더링).
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--d9848-t-with-immediate-validation/trace.zip`

### CL-011 (P0) — PASS

- Original: •	☐ 커스텀 로직 확장: UDF(사용자 정의 함수)로 “필요 시 코드 실행”이 가능하고 버전 업그레이드가 가능.
- Verification type: `integration`
- Tests:
  - `backend/tests/unit/services/test_pipeline_udf_versioning.py`
- Commands:
  - `pytest -q backend/tests/unit/services/test_pipeline_udf_versioning.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl011_test_before.log`
  - `docs/foundry_checklist/evidence/backend_cl011_test_after.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl011_test_before.log`
  - `docs/foundry_checklist/evidence/backend_cl011_test_after.log`

### CL-012 (P0) — PASS

- Original: •	☐ 구조화 데이터 입력: 스키마(컬럼 메타데이터) 기반 테이블 입력.
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts -g "csv upload input"`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/trace.zip`

### CL-013 (P0) — PASS

- Original: •	☐ 반구조화 입력(스키마 없음) + 파싱 변환이 있다.
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts -g "csv upload input"`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--01cc2-hema-aware-join-suggestions/trace.zip`

### CL-014 (P0) — PASS

- Original: •	☐ **비구조화 입력(문서/미디어 등)**을 파이프라인에서 취급할 수 있다(최소 “미디어 출력”/“미디어 변환” 흐름).
- Verification type: `e2e`
- Tests:
  - `frontend/tests/e2e/pipeline-builder-live.spec.ts`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `cd frontend && PLAYWRIGHT_OUTPUT_DIR=../docs/foundry_checklist/evidence/e2e_results PLAYWRIGHT_SCREENSHOT=on PLAYWRIGHT_TRACE=on npx playwright test tests/e2e/pipeline-builder-live.spec.ts -g "media upload input"`
- Artifacts:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--f7b7a-ructured-dataset-references/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--f7b7a-ructured-dataset-references/trace.zip`
- Evidence links:
  - `docs/foundry_checklist/evidence/e2e_test.log`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--f7b7a-ructured-dataset-references/test-finished-1.png`
  - `docs/foundry_checklist/evidence/e2e_results/e2e-pipeline-builder-live--f7b7a-ructured-dataset-references/trace.zip`

### CL-015 (P0) — PASS

- Original: •	☐ Batch Snapshot 모드: 전체 입력을 재계산하며 출력이 전체 교체.
- Verification type: `integration`
- Tests:
  - `backend/tests/test_pipeline_execution_semantics_e2e.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `PYTHONPATH=backend ADMIN_TOKEN=test-token BFF_ADMIN_TOKEN=test-token KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:39092 pytest -q backend/tests/test_pipeline_execution_semantics_e2e.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl015016_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl015016_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl015016_test.log`

### CL-016 (P0) — PASS

- Original: •	☐ Batch Incremental 모드: “마지막 빌드 이후 추가된 데이터만” 처리.
- Verification type: `integration`
- Tests:
  - `backend/tests/test_pipeline_execution_semantics_e2e.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `PYTHONPATH=backend ADMIN_TOKEN=test-token BFF_ADMIN_TOKEN=test-token KAFKA_BOOTSTRAP_SERVERS=127.0.0.1:39092 pytest -q backend/tests/test_pipeline_execution_semantics_e2e.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl015016_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl015016_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl015016_test.log`

### CL-017 (P0) — PASS

- Original: •	☐ Streaming 파이프라인: 스트림 실행 단위/잡 구성·실패 의미가 명확하다.
- Verification type: `integration`
- Tests:
  - `backend/tests/test_pipeline_streaming_semantics_e2e.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `PYTHONPATH=backend ADMIN_TOKEN=test-token BFF_ADMIN_TOKEN=test-token pytest -q backend/tests/test_pipeline_streaming_semantics_e2e.py 2>&1 | tee docs/foundry_checklist/evidence/backend_cl017_test.log`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl017_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl017_test.log`

### CL-018 (P0) — PASS

- Original: •	☑ Deploy와 Build를 명확히 분리한다.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-019 (P0) — PASS

- Original: •	☑ **Deploy 전에 검증(Validation checks)**를 수행하고 통과해야 배포 가능.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-020 (P0) — PASS

- Original: •	☑ 출력 계약(스키마/컬럼/타입/제약)을 “기대값”으로 정의할 수 있다.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-021 (P0) — PASS

- Original: •	☑ Breaking change 자동 감지가 있다(특히 incremental/streaming에서).
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-022 (P0) — PASS

- Original: •	☑ Replay(재처리) 옵션: optional/required 정책이 있다.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-023 (P0) — PASS

- Original: •	☑ Primary key expectation을 output 및 중간 transform에 설정 가능.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-024 (P0) — PASS

- Original: •	☑ Row count expectation 지원.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_expectations_and_contracts.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-025 (P0) — PASS

- Original: •	☐ 테스트 구성요소: test inputs / transform nodes / expected outputs 3요소로 테스트를 정의.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `PYTHONPATH=backend pytest -q backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-026 (P0) — PASS

- Original: •	☐ 테스트는 breaking change 탐지/디버깅에 유효하다.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `PYTHONPATH=backend pytest -q backend/tests/unit/services/test_pipeline_unit_test_runner.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-027 (P1) — FAIL

- Original: •	☐ Job-level status check(output job 성공 여부).
- Verification type: `unit`

### CL-028 (P1) — FAIL

- Original: •	☐ Build-level duration check(예상 시간 내 완료).
- Verification type: `unit`

### CL-029 (P1) — FAIL

- Original: •	☐ Freshness check(데이터가 최신인지).
- Verification type: `unit`

### CL-030 (P0) — PASS

- Original: •	☑ 노드 단위 Preview 실행(해당 노드까지의 파이프라인 실행).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_preview.py`
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_executor_preview.py`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-031 (P0) — PASS

- Original: •	☑ 컬럼 통계 제공(string 히스토그램/공백/대소문자/null, numeric 분포·min/max/mean 등).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_profiler.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_profiler.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-032 (P0) — PASS

- Original: •	☑ Row count 계산이 preview에서 가능.
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_executor_preview.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_executor_preview.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-033 (P0) — PASS

- Original: •	☑ Preview/Build 아티팩트는 prod prefix와 분리되고, Deploy는 prod로 “발행”된다.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-034 (P0) — FAIL

- Original: •	☐ 차트 노드/시각화 기능(bar/histogram 지원).
- Verification type: `unit`

### CL-035 (P1) — FAIL

- Original: •	☐ 파라미터 타입 제공(최소 Column 파라미터, Expression 파라미터).
- Verification type: `unit`

### CL-036 (P1) — FAIL

- Original: •	☐ 커스텀 함수 정의 시 입력 인자 타입을 강제한다.
- Verification type: `unit`

### CL-037 (P1) — FAIL

- Original: •	☐ 선택 노드 숨김/나머지 숨김 같은 스코프 축소 기능.
- Verification type: `unit`

### CL-038 (P1) — FAIL

- Original: •	☐ 파이프라인 검색 패널: 노드명/텍스트/컬럼/파라미터/상수/스키마 등으로 검색.
- Verification type: `unit`

### CL-039 (P1) — FAIL

- Original: •	☐ 컬럼명 일괄 치환(Replace columns) 지원.
- Verification type: `unit`

### CL-040 (P1) — FAIL

- Original: •	☐ 그래프에 Markdown 텍스트 노드 추가(레이아웃 영향 X, 특정 노드에 종속 X).
- Verification type: `unit`

### CL-041 (P1) — FAIL

- Original: •	☐ Input sampling은 Preview만 가속하고 Build에는 영향이 없어야 한다.
- Verification type: `unit`

### CL-042 (P1) — FAIL

- Original: •	☐ Checkpoints(배치 전용): 공유 upstream 로직을 1회만 계산하도록 중간 결과 저장.
- Verification type: `unit`

### CL-043 (P1) — FAIL

- Original: •	☐ Checkpoint 전략(단기 디스크/메모리/출력데이터로 저장) 같은 옵션이 있다.
- Verification type: `unit`

### CL-044 (P1) — FAIL

- Original: •	☐ Job groups: 배치에서는 기본적으로 output별 job 분리, 스트리밍은 묶임 등 실행 단위 규칙이 있다.
- Verification type: `unit`

### CL-045 (P0) — PASS

- Original: •	☑ 시간 기반 스케줄(매일 02:00 등).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
  - `backend/tests/unit/services/test_pipeline_scheduler_validation.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-046 (P0) — PASS

- Original: •	☑ 주기 기반 스케줄(매 15분/매시간 등).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-047 (P0) — PASS

- Original: •	☑ 업스트림 트리거 스케줄(업스트림 pipeline build/deploy 업데이트 시 실행).
- Verification type: `unit`
- Tests:
  - `backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
  - `backend/tests/unit/services/test_pipeline_scheduler_validation.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-048 (P0) — PASS

- Original: •	☐ 브랜치 생성(Main + 작업 브랜치) 및 브랜치 목록/아카이브/복원.
- Verification type: `integration`
- Tests:
  - `backend/tests/integration/test_pipeline_branch_lifecycle.py::test_pipeline_branch_lifecycle`
- Commands:
  - `pytest backend/tests/integration/test_pipeline_branch_lifecycle.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`

### CL-049 (P0) — PASS

- Original: •	☐ Propose(메인으로 변경 제안): 저장 후 제안 생성 + 설명 첨부.
- Verification type: `integration`
- Tests:
  - `backend/bff/tests/test_pipeline_proposal_governance.py::test_pipeline_proposal_submit_and_approve_flow`
- Commands:
  - `pytest backend/bff/tests/test_pipeline_proposal_governance.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`

### CL-050 (P0) — PASS

- Original: •	☐ Approve(승인): Edit 권한자 등 규칙에 따라 승인 가능, 오류 확인 가능.
- Verification type: `integration`
- Tests:
  - `backend/bff/tests/test_pipeline_proposal_governance.py::test_pipeline_proposal_requires_approve_role`
- Commands:
  - `pytest backend/bff/tests/test_pipeline_proposal_governance.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`

### CL-051 (P0) — PASS

- Original: •	☐ Branch protection: 보호 브랜치에서는 제안+승인 없이는 merge 불가.
- Verification type: `integration`
- Tests:
  - `backend/bff/tests/test_pipeline_proposal_governance.py::test_pipeline_proposal_requires_pending_status`
- Commands:
  - `pytest backend/bff/tests/test_pipeline_proposal_governance.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl048_cl049_cl050_cl051_tests.log`

### CL-052 (P1) — FAIL

- Original: •	☐ **컴퓨트 프로파일/리소스 설정(배치·스트리밍)**을 UI에서 변경 가능.
- Verification type: `unit`

### CL-053 (P1) — FAIL

- Original: •	☐ 저지연(특히 15분 이하) 워크로드 최적화 옵션(Foundry의 faster pipelines 포지션에 대응).
- Verification type: `unit`

### CL-054 (P0) — PASS

- Original: •	☐ Dataset output 생성.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-055 (P0) — FAIL

- Original: •	☐ Virtual table output 생성(가능하다면).
- Verification type: `unit`

### CL-056 (P0) — FAIL

- Original: •	☐ Ontology output(객체/링크/타임시리즈) 생성.
- Verification type: `unit`

### CL-057 (P0) — FAIL

- Original: •	☐ Media set output 생성(미디어 타입/포맷 설정, 배포 전 수정 가능, 배포 후 output 생성).
- Verification type: `unit`

### CL-058 (P1) — FAIL

- Original: •	☐ 파이프라인 설명/메타데이터 편집.
- Verification type: `unit`

### CL-059 (P1) — FAIL

- Original: •	☐ 권한/역할/액세스 요구사항 표시(최소 read/edit/admin 구분).
- Verification type: `unit`

### CL-060 (P1) — FAIL

- Original: •	☐ 댓글/협업자(누가 참여 중인지) 표시.
- Verification type: `unit`

### CL-061 (P1) — FAIL

- Original: •	☐ 공유(Share) 플로우.
- Verification type: `unit`

### CL-062 (P1) — FAIL

- Original: •	☐ 파이프라인 정의로부터 라인리지 그래프가 자동 생성된다.
- Verification type: `unit`

### CL-063 (P1) — FAIL

- Original: •	☐ 라인리지에서 스키마/마지막 빌드/생성 로직(코드/정의) 드릴다운이 가능.
- Verification type: `unit`

### CL-064 (P1) — FAIL

- Original: •	☐ 라인리지 스냅샷 저장/공유가 가능.
- Verification type: `unit`

### CL-065 (P1) — FAIL

- Original: •	☐ **외부 시스템 연결(네트워크 제약 환경 포함)**을 지원하는 아키텍처가 있다.
- Verification type: `unit`

### CL-066 (P1) — FAIL

- Original: •	☐ 자격증명(credential) 로테이션/업데이트가 코드 변경 없이 가능하다.
- Verification type: `unit`

### CL-067 (P1) — FAIL

- Original: •	☐ 연결 설정을 재사용(공유)할 수 있다(프로젝트/리포지토리 간).
- Verification type: `unit`

### CL-068 (P1) — FAIL

- Original: •	☐ 라인리지에 외부 연결까지 시각화된다.
- Verification type: `unit`

### CL-069 (P1) — FAIL

- Original: •	☐ 노코드/로우코드 정의를 코드 리포로 export할 수 있다.
- Verification type: `unit`

### CL-070 (P1) — FAIL

- Original: •	☐ **비가역성/제약(동일 출력 보장 X, 일부 변환 미지원 등)**을 제품 정책과 UI에서 명확히 고지한다.
- Verification type: `unit`

### CL-071 (P0) — PASS

- Original: •	☐ **모든 변경(노드 추가/삭제/설정 변경/배포/리플레이)**이 감사 로그로 남는다.
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_audit_logging.py`
- Commands:
  - `pytest backend/bff/tests/test_pipeline_audit_logging.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl071_cl072_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl071_cl072_tests.log`

### CL-072 (P0) — PASS

- Original: •	☐ 권한 모델이 파이프라인 수준에서 실제로 강제된다(읽기/편집/승인 등).
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_permissions_enforced.py`
- Commands:
  - `pytest backend/bff/tests/test_pipeline_permissions_enforced.py -q`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl071_cl072_tests.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl071_cl072_tests.log`

### CL-073 (P0) — PASS

- Original: •	☐ 데이터 건강성 체크/기대조건이 ‘배포 게이트’로 동작한다(통과하지 않으면 프로덕션 배포 불가).
- Verification type: `unit`
- Tests:
  - `backend/bff/tests/test_pipeline_promotion_semantics.py`
- Commands:
  - `./scripts/verify_foundry_checklist.sh`
  - `pytest -q backend/bff/tests/test_pipeline_promotion_semantics.py -k expectations_failed`
- Artifacts:
  - `docs/foundry_checklist/evidence/backend_cl073_test.log`
  - `docs/foundry_checklist/evidence/backend_test.log`
- Evidence links:
  - `docs/foundry_checklist/evidence/backend_cl073_test.log`
  - `docs/foundry_checklist/evidence/backend_test.log`

### CL-074 (P2) — FAIL

- Original: •	☐ (P2) AIP “Generate”: 프롬프트로 변환 로직을 생성하고, 생성된 변환이 일반 변환처럼 파이프라인에 저장된다.
- Verification type: `unit`

### CL-075 (P2) — FAIL

- Original: •	☐ (P2) AIP “Explain”: 선택한 변환 경로를 설명하고 이해를 돕는다.
- Verification type: `unit`

### CL-076 (P2) — FAIL

- Original: •	☐ (P2) Use LLM node: 파이프라인 중간에 LLM 처리를 삽입(무코드), 템플릿 프롬프트 제공, 소량 행 trial 가능.
- Verification type: `unit`

### CL-077 (P2) — FAIL

- Original: •	☐ (P2) 권한 게이트: LLM 기능은 관리자 권한 부여가 필요.
- Verification type: `unit`
