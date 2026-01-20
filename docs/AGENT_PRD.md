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
