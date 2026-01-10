알겠습니다, 시현님. 아래는 제가 직전에 드린 영문 답변 전체를 한국어로 완전 번역한 내용입니다(구조/의미 유지, 프론트엔드 설명은 원문에서도 최소였고 그대로 유지했습니다).

⸻

Palantir Foundry Action Types – 백엔드 아키텍처 딥 다이브

Foundry의 Action type은 온톨로지(Ontology)에 대한 “write-back(되쓰기)” 작업을 선언적으로 정의하는 단위입니다. 각 action type은 하나의 액션이 수행할 **객체 편집 묶음(생성/수정/삭제/링크 등)**을 정의하며, 이 변경 묶음은 원자적으로(atomic) 적용됩니다. 백엔드 관점에서 action 정의(규칙, 파라미터, submission criteria 등)는 온톨로지 메타데이터의 일부로 OMS(Ontology Metadata Service)에 저장됩니다.

런타임에서는 Actions 서비스가 OMS에서 action 정의를 로드하고, 사용자 권한/제출 조건(submission criteria)을 검증한 뒤, 최종적으로 적용할 편집 내용을 계산해 실행합니다. 인제스트(ingestion) 파이프라인이 외부 소스 데이터를 대량으로 적재해 객체를 만드는 흐름이라면, action은 사용자 입력(또는 자동화 트리거)을 받아 **“운영/의사결정 결과를 온톨로지에 기록”**하는 흐름입니다. 즉, ingestion이 “데이터가 들어오는 과정”이라면, action은 “결정이 데이터로 다시 기록되는 과정”에 가깝습니다.

⸻

내부 구조와 실행 흐름

Action type 구성요소는 보통 Parameters(입력 폼 스키마), Rules/Functions(변경 로직), Submission criteria(제출 가능 조건), 그리고 선택적으로 **Side-effects(알림/웹훅 등)**을 포함합니다. 이들은 OMS의 온톨로지 메타데이터로 저장됩니다.
	•	Rule 기반 액션: 각 rule이 하나의 “온톨로지 편집”으로 매핑됩니다(예: 특정 property 세팅, 링크 생성/삭제 등).
	•	Function 기반 액션: 액션이 특정 Ontology Function을 참조하며, 함수가 실제 편집 목록을 계산합니다.
	•	Submission criteria: 객체 상태/사용자 속성 등을 바탕으로 “이 액션을 제출 가능한가”를 판정하는 논리식 조건이며, Actions 서버가 실제 적용 전에 이를 평가합니다.

액션이 호출되면(REST API 또는 UI 경유), Actions 서비스가 전체 과정을 오케스트레이션합니다. 내부 실행 흐름은 다음과 같습니다.

1) 메타데이터와 객체 로드
	•	OMS에서 action type 정의(파라미터 스키마, rules, 함수 참조, submission criteria 등)를 가져옵니다.
	•	Object Set Service 등을 통해 현재 객체 인스턴스 상태를 로드합니다.

2) 권한 및 제출 조건(Submission criteria) 검증
	•	사용자는 **편집될 모든 데이터에 대한 “view 권한”**이 있어야 하며,
	•	submission criteria를 모두 만족해야 합니다.
조건을 만족하지 못하면 액션은 거부됩니다.

3) 편집 내용 계산(Compute edits)
	•	Rule 기반: rules를 순서대로 평가합니다. 백엔드에서 여러 rule을 객체별 단일 편집으로 “컴파일”하며, 충돌 시 rule 순서에 따라 후순위 rule이 우선합니다(예: 먼저 A로 세팅, 나중 rule이 B로 세팅하면 B가 최종).
또한 “삭제 후 생성” 같은 일부 조합은 금지됩니다.
	•	Function 기반: 지정된 function을 실행해 편집 목록을 생성합니다. 함수는 객체를 읽고 임의의 수정 목록을 출력할 수 있습니다. 함수 실행이 실패하면 액션 전체가 실패하며 부분 적용은 되지 않습니다.

4) Funnel로 편집 적용 요청 전송
	•	Actions 서비스는 계산된 편집 내용을 “modification instruction”으로 패키징해 Object Data Funnel 서비스로 보냅니다.
	•	Funnel은 객체 데이터스토어(OSv2)의 실제 write를 수행합니다. OSv2에서는 편집이 객체 DB/인덱스에 즉시 반영되며, 큐 오프셋 등을 통해 read-after-write 일관성이 보장된다고 설명됩니다.

5) Writeback dataset에 저장(Writeback persist)
	•	동시에 Funnel은 편집 내용을 해당 Object type의 writeback dataset에도 기록합니다.
	•	이 dataset은 각 객체의 “최신 사용자 수정본”을 담는 계층이며, 원본 데이터(backing data)는 그대로 유지됩니다.
즉, 객체 DB는 “원본(backing) + writeback”의 합성 결과(최신 상태)를 보게 됩니다.

6) Action Log 기록(감사/추적)
	•	Actions 서비스는 Action Log를 생성합니다.
	•	Foundry는 action type마다 [LOG] 프리픽스의 Action Log object type을 자동 생성한다고 설명합니다.
	•	액션 1회 제출마다 로그 객체 1개가 만들어지며, 그 로그 객체는 해당 액션이 편집한 모든 객체에 링크됩니다.
	•	로그에는 action ID, type, version, timestamp, user ID, 파라미터 값, 편집된 객체 PK 등 메타데이터가 담깁니다.
따라서 “액션 실행” 자체가 온톨로지의 데이터(사실)로 남아 분석/감사가 가능합니다.

⸻

실제 사용 패턴(Real-world usage patterns)

현업에서 action type은 “업무 흐름”을 온톨로지 편집으로 구현하는 데 쓰입니다.
	•	공급망(control tower): “Shipment 지연”, “Stockout 확인” 같은 액션이 주문/재고/알림 객체를 동시에 갱신
	•	제조 품질(Quality): “불량 조치 완료” 액션이 defect 객체를 닫고 검사/수리 객체를 함께 갱신

이 경우 백엔드는 액션을 “단일 원자 편집”으로 처리합니다. rule/function이 전체 변경 목록을 계산하고, 한 번에 적용합니다.

고객 사례/글들에서는 Actions가 “원클릭 비즈니스 프로세스”로 소개되곤 합니다. 예를 들어 운영 앱에서 “Delay Flight” 버튼을 누르면 항공편/승무원/정비 객체를 원자적으로 갱신하고 알림을 보내며 스케줄을 재계산하는 식입니다. (핵심은 같은 액션 로직/검증이 여러 앱과 통합에서 재사용된다는 점입니다.)

⸻

거버넌스, 권한, 감사(Audit) 메커니즘

Foundry의 action 프레임워크는 “권한/거버넌스”를 기본으로 내장합니다.

권한/제출조건
	•	사용자는 편집 대상 데이터에 대해 view 권한이 있어야 하고,
	•	submission criteria를 충족해야 제출 가능합니다.
여러 소스가 합쳐진 객체 타입에서도 과도한 제한을 피하기 위한 규칙이 존재한다고 설명됩니다.

Action Log 기반 감사
	•	성공한 액션은 Action Log 객체로 남습니다.
	•	이 로그 객체들은 action RID, 시간, 사용자, 파라미터를 포함합니다.
	•	또한 편집된 모든 객체의 PK를 foreign-key 링크로 연결합니다.
결과적으로 “누가/언제/무엇을/왜 바꿨는지”가 온톨로지 데이터로 조회 가능합니다.

Action Log object type도 일반 object type과 동일하게 권한 모델을 따르며, 필요 시 기본 필드 외 추가 필드를 확장할 수도 있다고 합니다.

⸻

복원력(Resilience), 멱등성(Idempotency), 재시도(Retry)

Foundry action 실행은 **원자적(성공하면 전부 적용, 실패하면 전부 미적용)**이지만, 서버 차원에서 기본적으로 중복 제거(멱등성)를 자동 보장해주진 않습니다.
	•	성공 시 OSv2에서는 즉시 반영되며, 이후 쿼리에서 바로 보입니다(read-after-write).
	•	동일 요청을 클라이언트가 두 번 보내면, 액션 로직이 자체적으로 방어하지 않는 한 중복 변경이 발생할 수 있습니다.
따라서 automation(자동화)에서는 “at-least-once” 실행 모델을 고려해 효과를 멱등하게 설계하라는 권고가 있습니다. 예: 고정 ID로 리소스를 만들면 두 번째 호출은 실패/무시되도록 구성.

Automate 기반 재시도/재실행

Automate(스케줄/트리거 기반)에서는 액션 효과가 실패할 때 재시도를 구성할 수 있고, 운영자가 실패 배치를 수동으로 재실행할 수 있는 흐름이 존재합니다.
액션이 원자적이므로 “절반만 적용되는” 형태의 불일치는 만들지 않고, 실패는 실패로 끝난 뒤 재실행합니다.

⸻

비교: Actions vs CRUD API / 워크플로 엔진(Camunda/Temporal 등)

백엔드 시스템 설계 관점에서 Foundry Actions는 전통적인 CRUD API와 다릅니다.

1) CRUD 엔드포인트 대신 “모델에 Action을 선언”

Foundry는 각 object type 생성 시 기본 “Create/Modify/Delete” action type을 자동 생성한다고 알려져 있습니다.
즉, 단순 생성/수정조차도 “action 호출”이라는 동일한 거버넌스/로그/권한 프레임워크로 처리합니다.

2) 폼/커스텀 API보다 상위 레벨(메타데이터 주도)

일반 앱이라면 여러 테이블 업데이트, 이벤트, 감사로그를 백엔드 코드로 구현해야 합니다.
Foundry에서는 “action type 정의(규칙/함수/검증)”를 온톨로지 메타데이터로 만들고, 플랫폼이 실행/감사/권한을 공통 처리합니다.

3) 워크플로 엔진과의 차이: Action은 원칙적으로 단일 단계/원자 트랜잭션

Camunda/Temporal 같은 엔진은 장기 실행 프로세스/다단계 상태 머신이 핵심입니다.
반면 Foundry Action은 기본적으로 “하나의 원자적 편집 트랜잭션”이고, 멀티스텝 흐름은 Automate나 외부에서 액션을 체인으로 엮는 방식입니다.
또한 워크플로 엔진은 데이터 저장소를 외부로 취급하지만, Foundry는 Ontology라는 의미 레이어와 액션 실행이 강하게 결합되어, “연결된 여러 객체를 한 번에 갱신”하는 방식이 자연스럽습니다.

⸻

요약

Foundry Action type은 온톨로지 중심의 선언적 데이터 수정 API입니다.
	•	스키마/검증/권한/감사를 메타데이터로 정의
	•	Actions 서비스가 실행하고, Funnel이 객체 데이터스토어와 writeback dataset에 반영
	•	Action Log가 자동 생성되어 감사 추적이 데이터로 남음
	•	자동화(Automate)에서는 at-least-once 특성을 고려해 멱등성을 설계해야 함

참고 출처(원문에서 사용한 근거): Palantir 공식 문서(온톨로지/액션/권한/Automate/OSv2 관련)와 커뮤니티 예시, Workshop 관련 서드파티 글 등을 인용해 구성했습니다.
