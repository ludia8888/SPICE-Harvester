> 상태: 체크리스트/설계 문서입니다. 실제 구현/검증 상태는 코드와 테스트 기준으로 확인하세요.

0) 체크리스트 사용 규칙
	•	각 항목은 반드시 ‘재현 가능한 테스트’로 증명해야 합니다. (스크린샷/로그/실행 결과/권한·감사 이벤트)
	•	우선순위:
	•	P0 = Foundry급을 주장하려면 필수
	•	P1 = 운영/확장/협업에서 체감 격차를 만드는 핵심
	•	P2 = 최신/부가(특히 AIP/LLM) — 있으면 경쟁력, 없다고 즉시 탈락은 아님

⸻

1) 파이프라인 작성 UX: 그래프 + 폼 기반 빌더 (P0)

Pipeline Builder는 그래프(노드·엣지) + 폼 기반 설정을 핵심 UX로 내세우고, 작성 중 피드백(조인 키/캐스팅 제안 등)을 제공합니다.  ￼
	•	☐ 그래프 캔버스에서 파이프라인을 작성할 수 있다 (입력→변환→출력의 노드 연결).
	•	검증: 신규 파이프라인 생성 후 입력 2개 추가 → Join → Output까지 완성.
	•	합격: 코드 없이 end-to-end 파이프라인 정의가 가능.
	•	☐ 폼(설정 패널)에서 변환 파라미터를 구성할 수 있다.
	•	검증: Join/Filter/GroupBy 등 변환 노드 클릭 시 설정 패널에서 키/조건/집계 등을 입력.
	•	합격: 모든 핵심 변환은 UI에서 완전 설정 가능(“일부는 코드로만” 금지).
	•	☐ 작성 중 즉시 피드백(실시간 검증) 제공: 예) 조인 키 후보, 컬럼 캐스팅 제안.
	•	검증: 타입이 맞지 않는 조인 키/연산 입력 시 즉시 오류 또는 제안 표시.
	•	합격: “빌드 눌렀더니 실패”가 아니라, 작성 단계에서 대부분 잡힘.  ￼
	•	☐ 대규모 파이프라인에서도 편집/탐색이 유지된다(관리 기능 포함).
	•	검증: 노드 200개 이상 생성 후 탐색/접기/부분 표시.
	•	합격: 최소한 폴더/컬러 그룹/숨김 기능이 있고, 성능이 실사용 가능.  ￼

⸻

2) 타입-세이프(Type-safe) 표현식/함수 시스템 (P0)

문서에서 Pipeline Builder는 강타입 함수를 강조하며, “빌드 시점이 아니라 즉시 오류를 표시”한다고 명시합니다.  ￼
	•	☐ 스키마(컬럼 타입) 추론/전파가 변환 노드를 통과하며 유지된다.
	•	검증: 입력 스키마 → 변환 후 스키마가 UI에 반영(미리보기/스키마 패널).
	•	합격: downstream 노드에서 타입 불일치가 즉시 드러남.
	•	☐ 타입 불일치 연산을 즉시 차단/경고한다(작성 단계).
	•	검증: String + Int 같은 연산/비교, 날짜 캐스팅 오류 입력.
	•	합격: 실행 전(Deploy/Build 전) 오류로 확정되거나 교정 가이드 제공.  ￼
	•	☐ 함수 카테고리(행 단위/집계/생성기) 수준의 표현식 체계가 있다.
	•	검증: row-level(예 add), aggregation(예 sum), generator(예 explode) 계열 함수 분류/동작.
	•	합격: UI·엔진·검증 로직에서 이 구분이 실제로 작동.  ￼

⸻

3) 변환(Transforms) 핵심 동작: Join/Union 등 (P0)

Pipeline Builder는 Join/Union 같은 변환을 제공하며, Union은 스키마 동일성을 요구하고 불일치 시 누락 컬럼 목록을 포함한 에러를 보여줍니다.  ￼
	•	☑ Join 변환 제공(최소 inner/left/right/full 중 일부라도 명확).
	•	검증: 두 테이블 조인 후 결과 프리뷰 및 스키마 확인.
	•	합격: 설정/실행/프리뷰/오류 처리까지 일관.
	•	증거:
		- backend/pipeline_worker/main.py (join 설정 검증 + runtime guard)
		- backend/tests/unit/services/test_pipeline_executor_transform_safety.py

	•	☑ Union 변환 제공 + 스키마 동일성 강제.
	•	검증: 컬럼 1개 누락된 입력을 union 시도.
	•	합격: union이 실패하며 어떤 컬럼이 누락됐는지 사용자에게 명확히 보여줌.  ￼
	•	증거:
		- backend/pipeline_worker/main.py (unionMode 검증 + runtime guard)
		- backend/tests/unit/services/test_pipeline_executor_transform_safety.py
	•	☐ 변환 표현을 두 가지 방식으로 보기 제공(예: 보드 렌더링 vs 의사코드 렌더링).
	•	검증: 동일 변환 경로에서 보기 모드 전환.
	•	합격: 렌더링만 바뀌고, 로직 자체는 변경되지 않음(의사코드 편집은 불가라는 점까지 Foundry와 유사).  ￼
	•	☐ 커스텀 로직 확장: UDF(사용자 정의 함수)로 “필요 시 코드 실행”이 가능하고 버전 업그레이드가 가능.
	•	검증: Python UDF 생성 → 파이프라인에서 재사용 → 버전 변경/업그레이드 시 영향 확인.
	•	합격: UDF는 예외적 확장 수단이며, 기본 변환 보드가 우선이라는 포지셔닝까지 유지.  ￼

⸻

4) 입력 데이터(Inputs) 범위: 구조화/반구조화/비구조화 (P0~P1)

Pipeline Builder 입력은 structured/semi-structured/unstructured를 지원하며, semi-structured는 XML/JSON/CSV 등 “스키마 없는 파일”을 파싱해 테이블로 변환할 수 있다고 설명합니다.  ￼
	•	☐ 구조화 데이터 입력: 스키마(컬럼 메타데이터) 기반 테이블 입력.
	•	검증: 업로드/동기화/가상 테이블 등 최소 1~2개 경로로 입력 가능.
	•	합격: 스키마가 저장되고 타입-세이프 검증에 사용됨.  ￼
	•	☐ 반구조화 입력(스키마 없음) + 파싱 변환이 있다.
	•	검증: JSON/CSV/XML을 입력 → 파싱 변환 → 테이블화 → 스키마 생성/검증.
	•	합격: 파싱 후부터 스키마 안전성(type safety) 혜택이 적용.  ￼
	•	☐ **비구조화 입력(문서/미디어 등)**을 파이프라인에서 취급할 수 있다(최소 “미디어 출력”/“미디어 변환” 흐름).
	•	검증: 미디어를 입력/변환 후 media set output 생성.
	•	합격: 출력 구성·배포 후 동일 폴더에 output 생성 등 운영 흐름이 존재.  ￼

⸻

5) 배치 vs 증분(Incremental) vs 스트리밍(Streaming) 실행 정합성 (P0)

배치 입력에서 snapshot vs incremental 계산 모드를 선택할 수 있으며, incremental은 APPEND/UPDATE(기존 파일 수정 없는 경우) 같은 제약이 있다고 명시합니다.  ￼
또한 스트리밍 파이프라인에서는 Flink 기반으로 묶여 실행된다는 설명(잡 그룹)도 존재합니다.  ￼
	•	☐ Batch Snapshot 모드: 전체 입력을 재계산하며 출력이 전체 교체.
	•	검증: 입력 변경 없이 재빌드 시 동일 결과, 입력 전체 스캔 발생 확인.
	•	합격: “스냅샷”이 실제로 전체 재처리 의미를 갖는다.  ￼
	•	☐ Batch Incremental 모드: “마지막 빌드 이후 추가된 데이터만” 처리.
	•	검증: APPEND로 1일치만 추가 → incremental build → 처리 범위/비용이 감소.
	•	합격: 문서 수준 제약(append/update 조건)과 유사한 규칙이 존재.  ￼
	•	☐ Streaming 파이프라인: 스트림 실행 단위/잡 구성·실패 의미가 명확하다.
	•	검증: 스트리밍 output 2개 생성 → 실행/장애 시 동작 확인(함께 실패/복구).
	•	합격: “스트리밍은 output이 하나의 실행 단위로 묶인다” 같은 규칙이 구현되어 있음.  ￼

⸻

6) Deploy vs Build 분리 및 릴리즈 플로우 (P0)

Deploy는 로직을 업데이트, Build는 로직을 실행해 결과를 물리화라고 구분하며, 비용 때문에 “deploy만 하고 build는 나중”이 가능하다고 설명합니다.  ￼
	•	☑ Deploy와 Build를 명확히 분리한다.
	•	검증: 로직 변경 → build로 스테이징 아티팩트 생성 → deploy(promote_build)로 동일 아티팩트 승격/발행.
	•	합격: (현 구현) build로 “스테이징 아티팩트 생성” → deploy(promote_build)로 “발행/승격”이 분리되어 존재.  ￼
	•	증거:
		- backend/bff/routers/pipeline.py (POST /pipelines/{id}/build + POST /pipelines/{id}/deploy promote_build)
		- backend/pipeline_worker/main.py (build: pipelines-staging/, deploy: pipelines/)
		- frontend/src/pages/PipelineBuilderPage.tsx (deploy=buildPipeline → deployPipeline(promote_build))
		- backend/bff/tests/test_pipeline_promotion_semantics.py
		- scripts/run_pipeline_artifact_e2e.sh
		- docs/foundry_checklist/PIPELINE_ARTIFACT_E2E.md
	•	☑ **Deploy 전에 검증(Validation checks)**를 수행하고 통과해야 배포 가능.
	•	검증: 타입 오류/출력 체크 실패 상태에서 deploy 시도.
	•	합격: deploy가 차단되고 어떤 검증이 실패했는지 표시.  ￼
	•	증거:
		- backend/pipeline_worker/main.py (schemaContract/expectations fail 시 preview/build/deploy FAILED)
		- frontend/src/pages/PipelineBuilderPage.tsx (preview panel에 errors/expectations 경고 표시)

⸻

7) “엄격한 출력 체크(Strict output checks)”와 Breaking Change 제어 (P0)

expected output checks를 만족하지 않으면 빌드를 막아 downstream break를 방지한다고 명시합니다.  ￼
또한 incremental 파이프라인에서 breaking change를 자동 감지하고 deploy 시 replay를 강제할 수 있다고 안내합니다.  ￼
	•	☑ 출력 계약(스키마/컬럼/타입/제약)을 “기대값”으로 정의할 수 있다.
	•	검증: output 스키마 기대값 저장 → 로직 변경으로 스키마 변형 → 체크 동작 확인.
	•	합격: 기대값 불일치 시 build/deploy가 차단되거나 명시적 승인/절차가 필요.  ￼
	•	☑ Breaking change 자동 감지가 있다(특히 incremental/streaming에서).
	•	검증: 잡 그룹 변경/상태 구조 변화 등 “리플레이가 필요한 변경”을 수행.
	•	합격: 시스템이 이를 감지하고 “리플레이 필요”를 강제/권고.  ￼
	•	☑ Replay(재처리) 옵션: optional/required 정책이 있다.
	•	검증: 변경 유형별로 replay가 선택/강제되는지 확인.
	•	합격: 최소 “breaking change면 required replay”는 존재.  ￼
	•	증거:
		- backend/shared/services/pipeline_executor.py (_validate_schema_contract, type/required guard)
		- backend/pipeline_worker/main.py (schemaContract + expectations enforced on preview/build/deploy)
		- backend/bff/routers/pipeline.py (promote_build replay gate: REPLAY_REQUIRED)
		- frontend/src/pages/PipelineBuilderPage.tsx (Deploy settings: replayOnDeploy toggle)
		- backend/tests/unit/services/test_pipeline_expectations_and_contracts.py
		- backend/bff/tests/test_pipeline_promotion_semantics.py

⸻

8) 데이터 품질: Data expectations + Unit tests + Health checks (P0)

Pipeline Builder는:
	•	Data expectations(기대 조건): 현재 primary key / row count 2종을 지원하고 실패 시 build 실패  ￼
	•	Unit tests: predefined input으로 expected output 비교  ￼
	•	Data health checks: job status / build duration / freshness  ￼

8-1) Data expectations (P0)
	•	☑ Primary key expectation을 output 및 중간 transform에 설정 가능.
	•	검증: PK 중복 데이터 입력 → expectation fail.
	•	합격: build가 실패하고 어떤 expectation이 실패했는지 UI에 표시.  ￼
	•	☑ Row count expectation 지원.
	•	검증: 기대 row count 범위를 벗어나게 입력.
	•	합격: build fail + 결과 패널에서 pass/fail 표시.  ￼
	•	증거:
		- backend/shared/services/pipeline_executor.py (_validate_expectations)
		- backend/pipeline_worker/main.py (preview/build에서 expectations fail 처리)
		- frontend/src/pages/PipelineBuilderPage.tsx (expectations 설정 + preview warnings)
		- backend/tests/unit/services/test_pipeline_expectations_and_contracts.py

8-2) Unit tests (P0)
	•	☐ 테스트 구성요소: test inputs / transform nodes / expected outputs 3요소로 테스트를 정의.
	•	검증: 수동 입력 테이블로 test input·expected output 작성.
	•	합격: 실제 결과와 expected의 diff를 확인 가능.  ￼
	•	☐ 테스트는 breaking change 탐지/디버깅에 유효하다.
	•	검증: 로직 변경 후 테스트 재실행 → 실패 확인.
	•	합격: “어떤 행/컬럼이 달라졌는지” 확인 가능.  ￼

8-3) Data health checks (P1)
	•	☐ Job-level status check(output job 성공 여부).
	•	☐ Build-level duration check(예상 시간 내 완료).
	•	☐ Freshness check(데이터가 최신인지).
	•	검증: 각 체크를 설정하고 실패 조건을 인위적으로 만들기.
	•	합격: 체크 실패가 표준화된 방식으로 표시/알림/차단으로 연결.  ￼

⸻

9) 미리보기(Preview) + 컬럼 통계(Profiling) + 차트(Chart) (P0~P1)

문서:
	•	Preview는 “선택 노드까지 raw datasets부터 로직 실행” + 컬럼 통계 제공  ￼
	•	차트(막대/히스토그램)로 중간 데이터를 워크스페이스 내에서 검증 가능  ￼
	•	☑ 노드 단위 Preview 실행(해당 노드까지의 파이프라인 실행).
	•	검증: 중간 transform 선택 → preview → 결과 테이블 확인.
	•	합격: upstream부터 선택 노드까지 실행 경로가 자동 구성.  ￼
	•	증거:
		- backend/bff/routers/pipeline.py (POST /pipelines/{id}/preview)
		- backend/pipeline_worker/main.py (preview 실행 + sample_json 저장)
		- frontend/src/pages/PipelineBuilderPage.tsx (preview panel + polling)

	•	☑ 컬럼 통계 제공(string 히스토그램/공백/대소문자/null, numeric 분포·min/max/mean 등).
	•	검증: 문자열/숫자 컬럼 각각 stats 확인.
	•	합격: 최소한 문서에 언급된 통계 항목 상당수가 구현.  ￼
	•	증거:
		- backend/shared/services/pipeline_profiler.py
		- backend/pipeline_worker/main.py (column_stats, row_count, sample_row_count 포함)
		- frontend/src/features/pipeline/PipelinePreview.tsx (컬럼 tooltip + rowCount 표시)
		- frontend/src/pages/PipelineBuilderPage.tsx (rowCount/sampleRowCount/columnStats 전달)
		- backend/tests/unit/services/test_pipeline_profiler.py

	•	☑ Row count 계산이 preview에서 가능.
	•	검증: preview 패널에서 row count 계산.
	•	합격: 결과가 사용자에게 명확히 제공.  ￼

	•	☑ Preview/Build 아티팩트는 prod prefix와 분리되고, Deploy는 prod로 “발행”된다.
	•	검증: build -> deploy(promote_build) 시 `artifact_key`가 `pipelines-staging/` -> `pipelines/`로 승격되며, stage_output preview는 거부된다.
	•	증거:
		- backend/pipeline_worker/main.py (staging prefix: pipelines-staging/, prod prefix: pipelines/)
		- backend/bff/routers/pipeline.py (stage_output preview deprecate + promote_build/promote_preview copy)
		- backend/shared/services/storage_service.py (copy_prefix)
		- backend/bff/tests/test_pipeline_promotion_semantics.py
	•	☐ 차트 노드/시각화 기능(bar/histogram 지원).
	•	검증: transform node에서 차트 추가 → 집계 선택(row count, sum 등) → 렌더링 확인.
	•	합격: 중간 데이터 검증을 “도구 이동 없이” 수행 가능.  ￼

⸻

10) 대형 파이프라인 관리 기능 (P1)

Foundry 문서의 Pipeline management:
	•	파라미터 & 커스텀 함수로 로직 재사용  ￼
	•	폴더/컬러 그룹/노드 숨김 등으로 그래프 가독성 유지  ￼
	•	체크포인트/잡 그룹/인풋 샘플링 등 성능·운영 최적화  ￼

10-1) 재사용: Parameters / Custom functions (P1)
	•	☐ 파라미터 타입 제공(최소 Column 파라미터, Expression 파라미터).
	•	검증: 컬럼/표현식 파라미터 생성 → 여러 변환에서 참조.
	•	합격: 존재 검증(컬럼 존재 확인 등)이 각 transform에서 수행됨.  ￼
	•	☐ 커스텀 함수 정의 시 입력 인자 타입을 강제한다.
	•	검증: 인자 타입을 Expression<String>, Expression<Date> 등으로 설정(개념적으로라도).
	•	합격: 타입이 맞지 않으면 함수 적용이 거부됨.  ￼

10-2) 그래프 관리: Show/Hide, 폴더, 컬러 그룹 (P1)
	•	☐ 선택 노드 숨김/나머지 숨김 같은 스코프 축소 기능.
	•	검증: 특정 영역만 남기고 숨김 → 연결 표시(점선 등)로 흐름 유지.
	•	합격: 큰 그래프에서 “문제 구간만 집중”이 가능.  ￼

10-3) 탐색·리팩토링: Find & Replace (P1)
	•	☐ 파이프라인 검색 패널: 노드명/텍스트/컬럼/파라미터/상수/스키마 등으로 검색.
	•	☐ 컬럼명 일괄 치환(Replace columns) 지원.
	•	검증: 컬럼 리네임 후 전체 변환에서 참조가 일괄 변경.
	•	합격: 리팩토링 비용이 “수작업 편집”으로 폭발하지 않음.  ￼

10-4) 문서화: Text nodes (P1)
	•	☐ 그래프에 Markdown 텍스트 노드 추가(레이아웃 영향 X, 특정 노드에 종속 X).
	•	검증: 텍스트 노드 작성/색상 변경/리사이즈.
	•	합격: 파이프라인 자체에 운영 메모/주의사항을 내장 가능.  ￼

10-5) 성능 최적화: Input sampling / Checkpoints / Job groups (P1)
	•	☐ Input sampling은 Preview만 가속하고 Build에는 영향이 없어야 한다.
	•	검증: 샘플링 적용 후 preview row/time 감소 + 실제 build 결과 동일성 확인.
	•	합격: 프로토타이핑은 빠르게, 배포는 전체 데이터로.  ￼
	•	☐ Checkpoints(배치 전용): 공유 upstream 로직을 1회만 계산하도록 중간 결과 저장.
	•	검증: output 2개가 공유하는 upstream node에 checkpoint 지정 → build 시 계산 횟수/시간 감소 확인.
	•	합격: “배치에서만 가능”, “같은 job group에 있어야 효율” 같은 제약까지 모델링.  ￼
	•	☐ Checkpoint 전략(단기 디스크/메모리/출력데이터로 저장) 같은 옵션이 있다.
	•	검증: 전략 변경 시 성능·비용·안정성 차이 확인.
	•	합격: 사용자가 트레이드오프를 선택 가능.  ￼
	•	☐ Job groups: 배치에서는 기본적으로 output별 job 분리, 스트리밍은 묶임 등 실행 단위 규칙이 있다.
	•	검증: 배치에서 output별 독립 성공/실패, 스트리밍에서 묶음 성공/실패 확인.
	•	합격: 문서 수준의 실행 모델이 시스템에 구현.  ￼

⸻

11) 스케줄링(Recurring runs / Upstream-triggered) (P0~P1)

Pipeline Builder에서 스케줄은 dataset builds를 시간/주기/상위 리소스 상태(업스트림 업데이트 시) 등으로 실행 가능하다고 설명합니다.  ￼
	•	☑ 시간 기반 스케줄(매일 02:00 등).
	•	☑ 주기 기반 스케줄(매 15분/매시간 등).
	•	☑ 업스트림 트리거 스케줄(업스트림 pipeline build/deploy 업데이트 시 실행).
	•	검증: 각 트리거 방식으로 실행되며 실행 이력에 남는지 확인.
	•	합격: “데이터 흐름 유지” 목적의 스케줄이 운영 수준으로 작동.  ￼
	•	증거:
		- backend/shared/services/pipeline_scheduler.py (cron/interval/dependencies 평가 + IGNORED 기록)
		- backend/tests/unit/services/test_pipeline_scheduler_validation.py
		- backend/tests/unit/services/test_pipeline_scheduler_ignored_runs.py

⸻

12) 브랜치/제안/승인 기반 Change management (P0)

Pipeline Builder의 브랜치, 제안(Propose), 승인(Approve), 보호 브랜치(Branch protection) 같은 git-style 운영 모델이 구체적으로 존재합니다.  ￼
	•	☐ 브랜치 생성(Main + 작업 브랜치) 및 브랜치 목록/아카이브/복원.
	•	검증: 브랜치 생성 → 아카이브 → 복원.
	•	합격: 아카이브된 브랜치는 편집/사용 불가 등 상태 모델 존재.  ￼
	•	☐ Propose(메인으로 변경 제안): 저장 후 제안 생성 + 설명 첨부.
	•	검증: 변경 후 Propose → 제안 뷰에서 내용/오류 확인.
	•	합격: 승인자에게 전달 가능한 “릴리즈 단위”가 만들어짐.  ￼
	•	☐ Approve(승인): Edit 권한자 등 규칙에 따라 승인 가능, 오류 확인 가능.
	•	검증: 승인 플로우에서 에러가 있으면 차단되는지 확인.
	•	합격: 최소 1인 이상 승인 요구 같은 보호 규칙 연동 가능.  ￼
	•	☐ Branch protection: 보호 브랜치에서는 제안+승인 없이는 merge 불가.
	•	검증: 보호 설정 후 직접 변경/반영 시도 → 차단.
	•	합격: “보호 브랜치=무결성”이 실제로 강제됨.  ￼

⸻

13) 실행 성능/엔진 옵션: Faster pipelines, Compute profiles (P1)

Pipeline Builder는 DataFusion(오픈소스, Rust) 기반 “faster pipelines” 옵션으로 배치/증분 실행 시간을 줄일 수 있다고 설명합니다.  ￼
또한 Build settings에서 배치·스트리밍 파이프라인의 성능 설정/컴퓨트 프로파일을 조정할 수 있습니다.  ￼
	•	☐ **컴퓨트 프로파일/리소스 설정(배치·스트리밍)**을 UI에서 변경 가능.
	•	검증: Default/Medium/Large 등 프로파일 전환 → 실행 시간/비용 변화 관찰.
	•	합격: 사용자에게 “성능 vs 비용” 선택지가 제공.  ￼
	•	☐ 저지연(특히 15분 이하) 워크로드 최적화 옵션(Foundry의 faster pipelines 포지션에 대응).
	•	검증: 동일 파이프라인을 기본 엔진 vs 빠른 엔진으로 비교.
	•	합격: 짧은 작업에서 체감 가능한 시간 단축.  ￼

⸻

14) 출력(Output) 타입: Datasets / Virtual tables / Ontology / Media sets (P0~P1)

Pipeline Builder output은 **datasets, virtual tables, Ontology 컴포넌트(object types/links/time series)**가 가능하다고 명시합니다.  ￼
또한 Ontology output은 배치/스트리밍 기반 생성 지원과 일부 제약도 명시합니다.  ￼
	•	☐ Dataset output 생성.
	•	☐ Virtual table output 생성(가능하다면).
	•	☐ Ontology output(객체/링크/타임시리즈) 생성.
	•	검증: output 패널에서 object type 생성, 속성 매핑, 배포 후 Ontology에 반영.
	•	합격: “출력 가이드가 compute checks를 줄이고 end-to-end 워크플로우를 돕는다” 수준의 UX 제공.  ￼
	•	☐ Media set output 생성(미디어 타입/포맷 설정, 배포 전 수정 가능, 배포 후 output 생성).
	•	검증: New media set 구성 → deploy → output 생성 확인.
	•	합격: 문서에 준하는 구성 파라미터/동작.  ￼

⸻

15) 메타데이터/협업 기능: 댓글, 팔로우, 공유, 권한/역할 표시 (P1)

문서에는 파이프라인 상세 사이드바에 메타데이터/액세스 요구사항/역할, 그리고 views/followers/comments/collaborators 같은 협업 지표가 있는 것으로 설명합니다.  ￼
	•	☐ 파이프라인 설명/메타데이터 편집.
	•	☐ 권한/역할/액세스 요구사항 표시(최소 read/edit/admin 구분).
	•	☐ 댓글/협업자(누가 참여 중인지) 표시.
	•	☐ 공유(Share) 플로우.
	•	검증: 사용자 2~3명으로 코멘트/팔로우/권한 변경 시 즉시 반영 여부 확인.
	•	합격: “혼자 쓰는 도구”가 아니라 조직 협업 도구로서 기능.  ￼

⸻

16) 데이터 라인리지(Lineage) 연동 (P1)

Data Lineage를 별도 앱으로 제공하며, 데이터가 Foundry 내에서 어떻게 흐르는지(조상/자손 확장, 스키마/마지막 빌드/코드 드릴다운, out-of-date 표시 등) 확인할 수 있고 스냅샷 공유도 가능합니다.  ￼
	•	☐ 파이프라인 정의로부터 라인리지 그래프가 자동 생성된다.
	•	검증: 입력→변환→출력의 upstream/downstream 탐색.
	•	합격: 조상/자손 확장/숨김이 가능.  ￼
	•	☐ 라인리지에서 스키마/마지막 빌드/생성 로직(코드/정의) 드릴다운이 가능.
	•	검증: 특정 output 클릭 → 스키마/last built/로직 정보 확인.
	•	합격: 운영자가 “왜 이 값이 이렇게 나왔나”를 추적 가능.  ￼
	•	☐ 라인리지 스냅샷 저장/공유가 가능.
	•	검증: 스냅샷 생성 → 다른 사용자에게 공유.
	•	합격: 운영 커뮤니케이션 비용이 감소.  ￼

⸻

17) 외부 연결/자격증명 거버넌스 (P1)

문서(External transforms):
	•	인터넷 접근 불가 시스템 연결,
	•	코드 변경 없이 자격 증명 로테이션,
	•	연결 설정 공유,
	•	거버넌스 워크플로우,
	•	Data Lineage 시각화,
	•	Virtual Tables 호환 등을 언급합니다.  ￼
	•	☐ **외부 시스템 연결(네트워크 제약 환경 포함)**을 지원하는 아키텍처가 있다.
	•	☐ 자격증명(credential) 로테이션/업데이트가 코드 변경 없이 가능하다.
	•	☐ 연결 설정을 재사용(공유)할 수 있다(프로젝트/리포지토리 간).
	•	☐ 라인리지에 외부 연결까지 시각화된다.
	•	검증: credential 갱신 후 파이프라인 재실행, 외부 연결 메타데이터/감사 로그 확인.
	•	합격: 운영/보안 측면에서 “연결은 자산”으로 관리됨.  ￼

⸻

18) 코드 리포지토리 연동/내보내기(Export) (P1)

Pipeline Builder에서 Java transforms repository로 export 가능하며, 파이프라인이 Java transform code로 변환되어 푸시됩니다. 단, 기존 파일 삭제/출력 비동일 가능/비가역/일부 변환 미지원 같은 제약도 명시합니다.  ￼
	•	☐ 노코드/로우코드 정의를 코드 리포로 export할 수 있다.
	•	검증: export 실행 → 대상 브랜치에 코드 생성 확인.
	•	합격: 최소한 “export 가능한 subset”이 명확히 정의되고 안내됨.  ￼
	•	☐ **비가역성/제약(동일 출력 보장 X, 일부 변환 미지원 등)**을 제품 정책과 UI에서 명확히 고지한다.
	•	검증: export UI에서 경고/동의 과정.
	•	합격: 사용자가 실수로 되돌릴 수 없는 작업을 하지 않게 방지.  ￼

⸻

19) 보안/감사(Audit)·거버넌스(Foundry급 주장에 사실상 필수) (P0~P1)

Pipeline Builder가 “no-code/low-code 모두 동일한 git-style change management, data health checks, multi-modal security, fine-grain auditing”을 사용한다고 명시합니다.  ￼
또한 공식 마케팅/문서에서도 “robustness, security, governed collaboration”을 강조합니다.  ￼
	•	☐ **모든 변경(노드 추가/삭제/설정 변경/배포/리플레이)**이 감사 로그로 남는다.
	•	검증: 변경 이벤트별 audit 이벤트 생성 여부 확인.
	•	합격: 누가/언제/무엇을/왜(제안 설명)까지 추적 가능.  ￼
	•	☐ 권한 모델이 파이프라인 수준에서 실제로 강제된다(읽기/편집/승인 등).
	•	검증: 권한 없는 사용자가 propose/approve/deploy 시도.
	•	합격: 차단 + 필요한 권한 안내.  ￼
	•	☐ 데이터 건강성 체크/기대조건이 ‘배포 게이트’로 동작한다(통과하지 않으면 프로덕션 배포 불가).
	•	검증: expectation fail 상태에서 deploy.
	•	합격: “불완전 데이터는 배포되지 않는다”가 기술적으로 강제.  ￼

⸻

20) AI 보조 기능: AIP / Use LLM node (P2)
최신 문서에 따르면 Pipeline Builder에는 AIP 보조 기능(로직 생성/설명/이름 추천/정규식/타임스탬프 캐스팅 등)과, 별도의 **Use LLM 노드(템플릿 프롬프트·샘플 행으로 빠른 trial)**가 있습니다.  ￼
	•	☐ (P2) AIP “Generate”: 프롬프트로 변환 로직을 생성하고, 생성된 변환이 일반 변환처럼 파이프라인에 저장된다.
	•	검증: 동일 입력 노드 선택 후 Generate → 변환 노드 생성 → 저장.
	•	합격: 생성 로직이 재현 가능(버전 관리/승인/배포 흐름 포함).  ￼
	•	☐ (P2) AIP “Explain”: 선택한 변환 경로를 설명하고 이해를 돕는다.
	•	검증: 변환 경로 선택 → Explain 결과 확인.
	•	합격: 단순 요약이 아니라 파이프라인 메타데이터 기반 설명.  ￼
	•	☐ (P2) Use LLM node: 파이프라인 중간에 LLM 처리를 삽입(무코드), 템플릿 프롬프트 제공, 소량 행 trial 가능.
	•	검증: 입력 데이터 몇 행 trial → 프롬프트 수정 → 전체 실행.
	•	합격: “샘플-반복-확대” 개발 루프가 빠르게 돌아감.  ￼
	•	☐ (P2) 권한 게이트: LLM 기능은 관리자 권한 부여가 필요.
	•	검증: 권한 없는 사용자 접근 차단.
	•	합격: AI 기능이 보안/거버넌스 체계 내에 포함.  ￼

⸻

마지막 점검: 합격 기준(현실적인 최소 바)

최소한 아래는 데모가 아니라 운영 수준으로 통과해야 합니다.
	•	작성 경험: 그래프+폼, 실시간 피드백, 타입-세이프 오류 차단  ￼
	•	릴리즈/협업: 브랜치→제안→승인→보호 브랜치  ￼
	•	안전장치: strict output checks, breaking change 감지/리플레이  ￼
	•	품질 체계: expectations(최소 PK/rowcount), unit tests, health checks  ￼
	•	운영/관측: preview+컬럼 통계, lineage(추적/드릴다운), audit  ￼
