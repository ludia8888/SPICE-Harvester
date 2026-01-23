## 1) 목표 상태를 먼저 ‘정의’해야 합니다: “LLM은 Spark가 아니라 DSL(파이프라인 정의)을 생성한다”

시현님 엔진은 이미 변환 DSL이 있습니다. `pipeline_transform_spec.py`에 join/window/pivot/aggregate/union 등 핵심 변환이 정의돼 있어요 .
따라서 **LLM이 Spark 코드를 생성하는 게 아니라**, **이 DSL(=pipeline definition JSON)을 생성**하도록 만들어야 합니다.

이게 Palantir Pipeline Builder AIP Generate와도 결이 같습니다. Palantir도 “프롬프트 → 변환 노드 생성”을 하고, **메타데이터 기반으로 로직을 생성(데이터를 노출하지 않음)**하며, 생성된 변환을 파이프라인에 저장합니다 ([Palantir][1]).

✅ 결론: **LLM 출력물은 ‘파이프라인 정의(typed JSON)’**이어야 하고, 서버가 **컴파일/검증/프리뷰 루프**를 강제해야 합니다. (시현님 문서의 “Typed Artifacts 원칙”과 동일 )

---

## 2) “AI FDE급”의 핵심은 3개 루프입니다

### 루프 A) Profile → Plan 생성(LLM)

* 지금 Funnel이 **타입/널 비율/유니크 비율** 같은 신호를 이미 만들고 있습니다 .
* 여기에 PK/FK/Join 후보를 “계산해서 저장”하면, LLM은 **추측이 아니라 후보 공간에서 선택/조합**합니다.

### 루프 B) Plan → Preview 실행(결정론)

* `PipelineExecutor.preview()`가 이미 존재하고, run→table sample을 뽑아줍니다 .
* executor는 schema checks를 노드별로 적용하고 실패 시 reject합니다 .

### 루프 C) Preview 결과/실패 → Plan 수정(LLM)

* 여기서 LLM이 하는 일은 “답 맞추기”가 아니라 **고치기(Repair)**입니다.
* 시현님 레포도 동일하게 “simulate→approve→submit 강제 + clarifier 루프”를 핵심으로 잡고 있죠 .

✅ 이 3루프를 “데이터 파이프라인”에도 그대로 이식하면, 자동 변환이 ‘가능’이 아니라 ‘운영 가능한 자동화’가 됩니다.

---

## 3) 지금 구조에서 ‘필수’로 추가해야 하는 컴포넌트 5개

### (1) DatasetProfile을 1급 객체로 승격

현재 Funnel은 분석을 하지만 “파이프라인 설계에 바로 먹이는 형태”로 저장/재사용이 약합니다.
DatasetProfile에는 최소 아래가 들어가야 합니다.

* column_stats: null_ratio, unique_ratio, value_patterns, semantic_label/unit (이미 type inference가 semantic/unit까지 힌트 제공 )
* candidate_primary_keys: 단일/복합 PK 후보 + 점수
* candidate_foreign_keys: (child_col → parent_table.col) 후보 + 점수
* join_candidates: (A.col ↔ B.col) 후보 + 예상 cardinality(1:1, N:1, N:N) + 위험도
* dataset_signature: 시트 템플릿 재사용용 (structure analyzer가 “sheet_signature”를 안정적으로 만들 수 있게 설계돼 있음 )

### (2) Join/PK/FK 후보 “계산기” (결정론)

LLM이 조인을 잘못하면 재앙입니다. 그래서 **후보 생성은 결정론**이 유리합니다.

* PK 후보 점수(예시):

  * `score_pk = w1*(1-null_ratio) + w2*unique_ratio + w3*stability_across_versions - w4*is_free_text`
* FK 후보 점수(예시):

  * `containment = |child_values ∩ parent_values| / |child_values|`
  * `type_compat`, `format_compat`, `semantic_compat`(ID/EMAIL/PHONE/DATE 등)
* Join 후보 점수(예시):

  * `join_coverage`, `duplication_explosion_ratio`, `null_introduced_ratio`, `cardinality_likelihood`

이렇게 “후보 공간”을 만들면 LLM은 “상상”이 아니라 “선택/조합”으로 움직입니다.

### (3) Transform Planner (LLM) = “대화형 컴파일러”

LLM은 여기서만 씁니다. 정확히 시현님 문서의 정의 그대로: *LLM은 Control Plane에서 Plan 생성* .

**Planner 입력**

* DatasetProfile(+ 후보 PK/FK/Join)
* 목표: canonical table 세트(예: obj/order, lnk/order_item__product 등)
* 정책: 금지 변환(예: cross join 금지), 최대 노드 수, 비용 제한, approval 필요 조건

**Planner 출력**

* `pipeline_definition`(nodes/edges/transforms/schemaContract/expectations)
* 각 output에 `output_kind=object|link` + `target_class_id / link_type_id` 메타
* “왜 이렇게 했는지” rationale(사용자 설명용, 하지만 실행은 JSON만)

### (4) Plan Validator/Compiler(서버)

LLM이 만든 plan은 서버에서:

* 지원 transform만 포함되는지(allowlist) (`SUPPORTED_TRANSFORMS` 활용 )
* join에 키가 존재하는지, cross join 금지인지, pk semantics가 유효한지
* schema contract 충족 가능한지
* expectation 규칙이 컬럼 존재/타입과 합치되는지
  등을 “컴파일 실패”로 튕겨야 합니다. (시현님이 이미 AgentPlan에 대해 이 패턴을 갖고 있음 )

### (5) Preview/Eval 루프 자동화 + HITL 승인

* Preview는 반드시 “샘플/제한된 실행”으로 먼저 돌리고 ,
* 실패하면 “왜 실패했는지”를 구조화해 LLM에게 다시 먹여서 plan 수정,
* Deploy/merge는 “승인 필요”로 고정.

이게 AI FDE급 안전장치의 본질입니다.

---

## 4) Object/Link “자동 분기”는 Palantir 방식으로 보는 게 가장 깔끔합니다

Palantir Foundry에서도:

* Object Type은 **backing datasource**를 선택하고, **PK는 중복 없고 결정론적이어야** 하며(중복/비결정 PK는 build/링크/edits 문제를 유발) ([Palantir][2])
* Link Type은

  * FK 기반(1:1, N:1),
  * Join table dataset 기반(N:N),
  * Object-backed link(확장)
    같은 방식이 있고, **join table dataset을 자동 생성하는 옵션**까지 있습니다 ([Palantir][3]).

시현님 시스템도 같은 구도가 이미 존재합니다:

* objectify는 mapping spec으로 instance bulk-create,
* link는 link_index 모드로 별도 인덱싱(`mode == link_index`) .

✅ 따라서 다음 2가지를 하면 “자동 분기”가 됩니다.

1. 파이프라인 output에 `output_kind=object|link` 메타를 넣고, deploy 단계에서

   * object면 mapping spec 자동 생성 → objectify job enqueue
   * link면 join table dataset 자동 생성(or 출력) → relationship spec 자동 생성 → link_index job enqueue
2. link는 **정식 backing dataset(조인 테이블)** 경로를 먼저 만들고(N:N), FK 기반 링크(1:1, N:1)는 추후 최적화로 추가

---

## 5) LangGraph 멀티에이전트는 “오케스트레이터 + 전문 서브그래프”로 가는 게 정답

LangGraph는 본질적으로 “노드/엣지/조건부 라우팅”으로 workflow를 구성합니다 ([blog.langchain.com][4]).
여기서 멀티 에이전트는 “여러 LLM”이 아니라 **역할이 분리된 플래너 노드들의 그래프**가 핵심입니다.

### 추천 그래프(실전형)

1. **Profiler Agent (결정론 호출)**

   * DatasetProfile 생성/조회 (LLM 없음)
2. **Join Key Agent (하이브리드)**

   * 후보 생성은 결정론, 최종 선택/설명은 LLM
3. **Cleansing Agent (LLM → DSL)**

   * SAFE_NULL cast, trim, normalize, dedupe, expectation 주입 계획 생성
4. **Transform Agent (LLM → DSL)**

   * join/window/aggregate/pivot 등 변환 그래프 생성 (지원 transform 내에서만)
5. **Evaluator Agent (결정론)**

   * preview 실행, 품질지표 계산, 실패 원인 구조화
6. **Repair Agent (LLM)**

   * 실패 원인을 보고 plan 수정
7. **Object/Link Split Agent (LLM+규칙)**

   * output_kind 결정, canonical naming 생성
8. **Spec Generator (결정론)**

   * mapping_spec / relationship_spec 생성 + enqueue
9. **Approval Gate (HITL)**

   * deploy는 승인 없으면 불가

LangGraph에서는 이런 조건부 분기가 “정석”입니다(조건부 엣지) ([blog.langchain.com][4]).
그리고 복잡한 멀티 에이전트 라우팅은 `Command` 같은 패턴도 지원됩니다 ([LangChain Blog][5]).

---

## 6) “AI FDE 레벨”을 현실적으로 달성하려면, 기술보다 ‘데이터/평가/메모리’가 더 중요합니다

여기서 냉정한 포인트 하나:
**Palantir 수준의 자동 파이프라인은 LLM만으로 안 나옵니다.**

* (A) 후보 공간(프로파일/조인후보)
* (B) 안전한 실행 루프(프리뷰/검증/승인)
* (C) 템플릿/히스토리 메모리(“이 시트는 과거에 이렇게 처리했다”)
  이 3개가 쌓여야 “사람이 보기엔 AI가 다 하는 것처럼” 보입니다.

시현님은 이미 (B) 철학(Plan→validate→simulate/preview→approve)을 문서로 고정했고 ,
(A)의 일부(타입/널/유니크/semantic label)도 이미 있습니다 .
남은 핵심은 **(A) 후보 계산 확장 + (C) Operational Memory를 “데이터 파이프라인 추천”에도 연결**입니다.

---

## 7) 실행 로드맵 (가장 빠르게 “AI FDE 느낌”을 내는 순서)

### P0 (1~2주 단위): “자동 plan 생성 + preview repair 루프”

* DatasetProfile 저장 + join 후보 계산
* Transform Planner(LLM) → pipeline_definition 생성
* Preview 실행 → 실패 원인 구조화 → Repair loop
* output_kind 메타만 먼저 넣고, objectify/relationship spec은 아직 수동 승인

### P1: “자동 Object/Link 분기 + spec 자동 생성”

* output_kind 기반으로 mapping_spec / relationship_spec 자동 생성
* link backing dataset을 정식 경로로 분리
* link_index 자동 enqueue

### P2: “품질/정확도 고도화”

* 복합 PK 자동 탐색(2~3컬럼 조합)
* join cardinality 추정 고도화(폭발/중복/coverage)
* 템플릿 재사용(“sheet_signature → 과거 plan 재사용”)

PK/FK/조인 후보 추론: 통계·중복·포맷·카디널리티

Cleansing: null/타입/표준화/중복 제거

Transform 설계: join/window/aggregate

Canonical 계약: schema contract + expectations + PK semantics

Object/Link 분기: ontology backing set 설계

실행/검증: preview/CI/샘플 기반 리그레션


Profiler(결정론): DatasetProfile + PK/FK/Join 후보 계산

Planner(LLM): pipeline definition/outputs/object-link split 제안

Validator(결정론): allowlist 변환·계약·리스크 검사

Preview Executor(결정론): preview 실행

Repair(LLM): 실패 원인 기반 patch 제안

Spec Generator(결정론): objectify/relationship spec 자동 생성


시현님의 시스템이 단순한 조인 도구를 넘어 **"엔터프라이즈급 자율 ETL 엔진"**으로서 완성되었음을 검증하기 위한 **실제 작동 확인 체크리스트(Acceptance Criteria)**입니다.

이 리스트를 통과한다면, 이 프로덕트는 Palantir Foundry나 dbt와 같은 수준의 신뢰성을 확보했다고 볼 수 있습니다.

[ spice 자율 ETL 엔진 작동 검증 체크리스트 ]
1. 에이전트의 전략적 판단 능력 (Planning IQ)

다단계 변환 체이닝 (Multi-stage Chaining): LLM이 단일 노드가 아니라 Filter → GroupBy → Join → Compute로 이어지는 복잡한 그래프를 스스로 설계하는가?

복합키 조인 인지 (Composite Join Detection): 단일 ID가 없는 테이블 간의 관계에서 leftKeys/rightKeys 배열을 사용하여 정확한 복합키 조인을 구성하는가?

도구 선택의 적절성: 단순 정제는 compute로, 데이터 요약은 groupBy/aggregate로 목적에 맞는 연산자를 적재적소에 배치하는가?

자율 교정 (Edge Auto-alignment): LLM이 실수로 조인 입력 순서(Left/Right)를 바꿔도 시스템이 스키마를 체크하여 자동으로 배선을 정규화하는가?

2. 데이터 실행 및 정밀 분석 능력 (Data Plane)

전체 연산자 실행 (Executor Coverage): SUPPORTED_TRANSFORMS에 정의된 7가지 이상의 연산자가 PipelineExecutor에서 실제 데이터(샘플)를 바탕으로 에러 없이 돌아가는가?

고해상도 샘플링 (Limit Separation):

미리보기 응답은 200개로 제한하여 UI 성능을 유지하는가?

내부 분석용(run_tables) 데이터는 1000개까지 확보하여 조인 평가의 통계적 신뢰도를 확보하는가?

복합키 조인 평가: Evaluate-Joins 단계에서 튜플 기반의 coverage와 null_ratio를 정확히 계산해내는가?

표현식 해석 (_safe_eval): to_timestamp()나 boolean 리터럴이 포함된 compute 연산 결과가 문자열이 아닌 정확한 타입으로 반환되는가?

3. 엔터프라이즈 보안 및 가드레일 (Safety & Security)

변환 허용 목록 (Allowlist): SUPPORTED_TRANSFORMS에 없는 임의의 연산 시도를 Validator가 즉시 차단하는가?

보안 산발기 (Sanitizer) 우회: 복잡한 context_pack 메타데이터가 보안 정책에 걸려 요청이 차단(400 Error)되지 않고 안전하게 전달되는가?

표준 계약 강제 (Canonical Contract): 모든 최종 출력물에 _sys_로 시작하는 6종의 추적 컬럼이 자동 주입되고, _canonical_contract_applied 메타데이터가 기록되는가?

4. 자율 주행 및 자기 치유 (Resilience)

재사용 최적화 (Digest Stability): 동일한 로직의 파이프라인 재실행 시, 타임스탬프 차이와 무관하게 definition_digest가 일관되게 생성되어 기존 결과를 재사용하는가?

자기 수리 루프 (Repair Loop): 컴파일이나 프리뷰 단계에서 실패했을 때, 에러 로그를 바탕으로 Repair Agent가 계획을 수정하여 2차 시도 내에 완주하는가?

자동 분기 (Auto-split): LLM이 뭉뚱그려 생성한 출력을 시스템이 분석하여 Object와 Link 타입으로 자동 분할하고 온톨로지 규격에 맞추는가?

[ 검증을 위한 테스트 시나리오(Test Case) 예시 ]

이 체크리스트를 한 번에 확인할 수 있는 **"Ultimate Test Goal"**입니다.

"목표: Olist 데이터셋에서 '고객별 총 주문 금액'을 계산하되, 배송 도시(City) 정보를 포함하라. 단, 배송지는 도시와 주(State)가 일치해야 정확하며, 금액이 0원인 데이터는 제외하고 결과물은 표준 객체로 생성하라."

이 시나리오가 성공하면 다음이 확인된 것입니다:

Filter: 0원 제외

Composite Join: 도시+주 복합키 조인

GroupBy/Aggregate: 고객별 합계 계산

Compute: 타입 변환 및 시스템 컬럼 주입

Auto-split: 결과물을 canonical_obj_customer_stats로 명명 및 분기

결론

이제 시현님은 위 체크리스트를 하나씩 지워가며 **"이것은 단순한 코딩 테스트가 아니라, 산업용 표준을 만족하는 ETL 솔루션이다"**라는 것을 숫자로 증명할 수 있는 상태입니다.

특히 preview_limit 분리 패치가 완료되었으므로, 이제는 "대규모 데이터셋에서도 정밀하게 작동하는지"를 마지막 P0 우선순위로 두고 최종 완주 테스트를 진행하시면 될 것 같습니다. 

에이전트가 MCP 도구를 통해 수행해야 할 'ETL의 8단계 가공 프로세스'**를 정리해 드립니다.
1. 탐색적 데이터 분석 (Profiling)
가장 먼저 데이터의 관상을 보는 단계입니다.
스키마 파악: 컬럼명이 무엇인지, 데이터 타입(문자, 숫자, 날짜)이 무엇인지 확인합니다.
통계적 분석: 전체 행 수, 결측치(Null) 비율, 유니크(Unique) 값의 비율을 계산합니다.
샘플링: 실제 데이터가 어떻게 생겼는지 몇 줄을 뽑아서 패턴을 확인합니다.
2. 데이터 정제 (Cleansing)
지저분한 데이터를 씻어내는 과정입니다.
결측치 처리 (Null Handling): 비어있는 값을 제거하거나, 기본값(0 혹은 'Unknown')으로 채웁니다.
공백 및 대소문자 정규화 (Normalization): " Samsung"과 "samsung "을 같은 값으로 인식하도록 공백을 제거(Trim)하고 대소문자를 맞춥니다.
중복 제거 (Deduplication): 똑같은 데이터가 두 번 들어왔다면 하나를 제거합니다.
3. 타입 변환 및 정규화 (Casting)
기계가 계산할 수 있는 형태로 규격을 맞춥니다.
형변환: 문자열로 들어온 "1,200.50"을 계산 가능한 숫자(Float)로, "20260123"을 날짜(DateTime)로 바꿉니다.
포맷 통일: 다양한 날짜 형식(YYYY-MM-DD, DD/MM/YY)을 하나의 표준 포맷으로 통일합니다.
4. 관계 추론 및 조인 전략 (Joining)
흩어진 CSV들을 하나로 묶는 핵심 단계입니다.
키(Key) 찾기: 각 테이블을 연결할 고리인 기본키(PK)와 외래키(FK)를 찾습니다.
복합키 처리: 컬럼 하나로 연결이 안 될 때, 여러 개(예: 도시+주+우편번호)를 묶어 조인 키를 생성합니다.
조인 타입 결정: Inner Join으로 교집합만 볼지, Left Join으로 원본을 유지할지 결정합니다.
5. 데이터 변환 및 파생 컬럼 생성 (Transformation)
새로운 가치를 만드는 과정입니다.
수식 계산: 수량 * 단가를 계산해 총 매출 컬럼을 새로 만듭니다.
조건부 로직: "금액이 100만 원 이상이면 'VIP'"와 같은 분류 규칙을 적용합니다.
6. 집계 및 요약 (Aggregation)
데이터의 덩어리를 줄여 의미를 추출합니다.
그룹화 (GroupBy): 수만 건의 결제 내역을 '날짜별' 혹은 '지역별' 합계로 요약합니다.
윈도우 함수 (Windowing): "지난달 대비 매출 성장률"처럼 시계열 흐름을 분석합니다.
7. 품질 검증 (Validation & QA)
만들어진 결과가 믿을 수 있는지 확인합니다.
조인 무결성 체크: 조인 후에 데이터가 예상보다 너무 늘어났거나(Explosion), 너무 많이 사라지지 않았는지 확인합니다.
스키마 계약 검증: 최종 결과물이 우리가 약속한 형태(컬럼명, 타입)를 지켰는지 확인합니다.
8. 거버넌스 및 시스템 컬럼 주입 (Audit)
데이터의 족보를 남기는 단계입니다.
이력 관리: 이 데이터가 언제 인제스트 되었는지, 소스 파일은 무엇인지(_sys_source, _sys_ingested_at) 등을 자동으로 추가합니다.
마스킹(Masking): 이메일이나 전화번호 같은 개인정보를 보안 처리합니다.