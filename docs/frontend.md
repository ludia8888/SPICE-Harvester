시현님, 아래 문서는 BFF v1 레퍼런스(2025‑12‑19) 계약을 그대로 준수하면서, **Blueprint.js(팔란티어 스타일)**로 “프론트엔드 개발자가 즉시 구현 가능한 수준”SPICE Harvester UI/UX 기획서 최종본 (BFF v1 Strict)
	•	버전: UI Spec v1.0
	•	기준 API: BFF /api/v1 only
	•	디자인: Blueprint.js + Palantir 3‑pane (Left Nav / Main / Right Inspector)
	•	목적: 사용자가 raw → 스키마/매핑 → 임포트 → 그래프+검색 페더레이션 조회까지 “제품 안에서” 완주

⸻

목차
	1.	제품 범위와 핵심 제약(= UI 설계의 법칙)
	2.	정보 구조(IA) & 라우팅
	3.	전역 UX 패턴(인증/레이트리밋/비동기 커맨드/브랜치/데이터 상태)
	4.	프론트 상태 모델(스토어/캐시/로컬 추적)
	5.	재사용 컴포넌트 명세(구현 단위)
	6.	화면별 상세 기획(와이어프레임 ASCII + API 시퀀스 + 상태/에러 + 완료조건)
	7.	에러/상태 매트릭스(필수 UX)
	8.	구현 순서(개발 플랜)

⸻

구현 현황 (현재 브랜치 기준)

✅ 개발 완료
	•	App Shell: ContextNavbar + Sidebar + InspectorPanel + CommandTrackerDrawer
	•	주요 화면: Databases, Overview(요약), Branches, Ontology, Mappings, Sheets Hub,
		Import Sheets/Excel, Instances, Graph Explorer, Query Builder, Merge, Audit, Lineage,
		Operations(Tasks/Admin)
	•	Core UX: Command Tracker 로컬 추적, 429 카운트다운(AI/Import/Connector),
		409(actual_seq 재시도), unknown_label_keys 표+Mappings CTA
	•	Graph: Cytoscape 기반 GraphCanvas + data_status 표시/콜아웃
	•	SettingsDialog 강제 오픈 + 실패 요청 1회 재시도(503/401/403)
	•	WebSocket 기반 Command 구독(선택 기능)
	•	Inspector Drawer 탭형 상세 패널(Summary/JSON/Audit/Lineage)
	•	AsyncCommandButton/ApiErrorCallout 공용 컴포넌트화
	•	Overview “Next Steps” 카드
	•	Schema Suggestion 카드형 UI + Validate 단계 + 테이블 선택 UX
	•	Graph Explorer 결과 export + Re-run 버튼
	•	Query Builder raw query UI
	•	Audit Reset 버튼 + Command Tracker 연동 CTA
	•	Lineage 화면 내 “Use Selected Graph Node” 버튼
	•	Branches 페이지 “SwitchTo” 버튼(전환은 현재 상단 ContextNavbar에서 처리)

⸻

1) 제품 범위와 핵심 제약

아래는 API 레퍼런스에 의해 “UI가 절대 가정하면 안 되는 것”과 “UI가 반드시 해야 하는 것”입니다.

1.1 절대 가정 금지
	•	커맨드 전역 목록 조회 API 없음
→ GET /api/v1/commands/{command_id}/status만 존재
→ “서버 전체 커맨드 히스토리” UI는 불가.
→ UI는 클라이언트가 알고 있는 command_id만 추적해야 함.
	•	Google Sheets/Excel Commit은 단일 202가 아니라, 200으로 배치 커맨드 리스트를 반환
→ 응답의 write.commands[] 각각을 추적.
	•	Sheets/Excel Commit은 현재 branch=main 고정
→ 사용자가 feature 브랜치 컨텍스트에 있어도 “main에 써짐”.
→ UI는 반드시 경고 + 확인(동의) UX 필요.
	•	Instances 읽기 API는 branch를 받지 않거나 무시
→ Instances 화면에서 branch 기반 what‑if를 제공하면 거짓 UX가 됨.
→ branch는 “ignored”로 명시.
	•	Audit는 partition_key(예: db:<db_name>)가 필수/권장
→ DB 스코프 화면으로 고정하고 자동 주입.
	•	Lineage는 root 필수
→ root 입력 전에는 로드하지 않는다.
	•	Graph Query는 class_id/predicate_id 기반
→ UI는 label로 보여주되 실제 값은 ID를 써야 한다.
→ 결과 노드는 data_status=FULL|PARTIAL|MISSING를 표시해야 한다.

1.2 UI가 반드시 해야 하는 것
	•	모든 202 쓰기는 **Command Tracker(클라 추적형)**에 자동 등록
	•	429 + Retry‑After는 자동 백오프/재시도(제한적으로) + 사용자에게 “몇 초 후 가능”을 명확히 표시
	•	409(OCC)에서는 actual_seq 기반 재시도 UX
	•	unknown_label_keys(400)에서는 Mappings로 유도 + 누락 라벨 리스트 표시
	•	data_status가 PARTIAL/MISSING일 때 **“정상 상태일 수 있음(프로젝션 지연)”**을 UI에서 안내

⸻

2) 정보 구조(IA) & 라우팅

2.1 App Shell 구조(전 화면 공통)
	•	상단 Navbar: DB/Branch/Command/Settings
	•	좌측 Nav: 작업 흐름 중심
	•	우측 Inspector Drawer: 선택 항목 상세(노드/인스턴스/클래스/감사/커맨드)

2.2 Routes (권장)
	•	/ Databases
	•	/db/:db/overview?branch=...
	•	/db/:db/branches
	•	/db/:db/ontology?branch=...
	•	/db/:db/mappings
	•	/db/:db/data/sheets  (preview/grid/register)
	•	/db/:db/data/import/sheets (dry‑run/commit)
	•	/db/:db/data/import/excel
	•	/db/:db/data/schema-suggestion
	•	/db/:db/instances (branch ignored)
	•	/db/:db/explore/graph?branch=...
	•	/db/:db/explore/query?branch=... (label 기반 Query Builder)
	•	/db/:db/merge?branch=...
	•	/db/:db/audit
	•	/db/:db/lineage
	•	/operations/tasks
	•	/operations/admin (admin 토큰 필요)

“/commands” 전역 페이지는 가능하나, 소스는 서버가 아니라 로컬 추적 목록입니다.

⸻

3) 전역 UX 패턴

3.1 인증(503/401/403) 처리

구현 상태: ✅ 개발 완료 (SettingsDialog 강제 오픈 + 마지막 실패 요청 1회 재시도)

	•	모든 요청은 토큰 필요(Authorization: Bearer 또는 X-Admin-Token). 토큰 없으면 503이 나올 수 있음.
	•	전역 인터셉터 규칙:
	•	503 + 토큰 미설정 추정 → SettingsDialog 강제 오픈 + “토큰 필요”
	•	401/403 → “권한/토큰 오류” Callout + Settings 유도
	•	설정 저장 후: 마지막 실패 요청 한 번만 재시도(루프 방지)

3.2 레이트리밋(429) 처리

구현 상태: ✅ 개발 완료 (AI/Import/Connector 1회 재시도 + 카운트다운)

	•	Retry‑After를 읽어:
	•	Toaster: “레이트리밋. N초 후 재시도 가능”
	•	AI/커넥터/Import는 자동 재시도 0~1회만(폭주 방지)
	•	사용자가 버튼을 다시 누를 수 있게 버튼에 카운트다운 표시

3.3 비동기 커맨드(202) 처리 — Command Tracker가 핵심

구현 상태: ✅ 개발 완료 (로컬 추적 + 폴링)

	•	202 응답이면 command_id를 즉시 로컬 추적 저장
	•	폴링: GET /api/v1/commands/{id}/status
	•	상태:
	•	PENDING/PROCESSING/RETRYING → 진행중
	•	COMPLETED → 성공
	•	FAILED/CANCELLED → 실패/중단(에러 노출)
	•	404 → TTL 만료/알 수 없음 → EXPIRED/UNKNOWN로 표시

3.4 WebSocket 커맨드 구독(선택, UX 향상)

구현 상태: ✅ 개발 완료 (WS 구독 + 폴링 폴백)

	•	상세 화면(커맨드 Drawer 열렸을 때)에 한해:
	•	WS /api/v1/ws/commands/{command_id}?token=...
	•	이벤트:
	•	command_update 수신 시 UI 즉시 갱신
	•	WS 실패 시 폴링으로 자동 폴백

3.5 브랜치 컨텍스트 규칙

구현 상태: ✅ 개발 완료

	•	branch 유효: Ontology / Graph Query / Instance 쓰기(create/update/delete/bulk‑create) / Query / Suggest
	•	branch 무시/미지원: Instances 읽기(리스트/샘플/단건)
→ 해당 페이지 상단에 “Branch Ignored” 고정 배지

3.6 Graph 데이터 상태(data_status)

구현 상태: ✅ 개발 완료

	•	FULL: ES 문서 join 완료
	•	PARTIAL: 일부 누락/지연
	•	MISSING: 문서 없음(지연/미인덱스)
	•	UI 표시 규칙:
	•	노드 카드/인스펙터에 Tag로 표시
	•	PARTIAL/MISSING은 오류가 아니라 상태임을 Callout로 안내
	•	“Re‑run” 제공(동일 쿼리 재실행)

⸻

4) 프론트 상태 모델(스토어/캐시/로컬 추적)

4.1 LocalStorage(필수)

구현 상태: ✅ 개발 완료

	•	spice.authToken (Authorization: Bearer 권장)
	•	spice.adminToken (X-Admin-Token)
	•	spice.language (ko|en)
	•	spice.rememberToken
	•	spice.project / spice.branch / spice.theme (호환 캐시)
	•	commandTracker.items[]
	•	{ command_id, source, createdAt, lastStatus?, lastFetchedAt?, expired? }
	•	spice.recentContext
	•	{ lastDb, lastBranchByDb: { [db]: branch } }

4.2 Runtime Store(예: Zustand)

구현 상태: ✅ 개발 완료

	•	context.db, context.branch, context.lang
	•	registry.classesById
	•	온톨로지/클래스/관계 선택을 위한 캐시(아래 4.3)
	•	commandTracker.map + pollingJobs
	•	ui.drawer.inspectorContext
	•	ui.toasts

4.3 Class/Predicate Registry(중요)

구현 상태: ✅ 개발 완료

Graph Query는 ID 기반이므로, UI는 최소한 이 캐시가 필요합니다.
	•	소스:
	•	GET /api/v1/database/{db}/ontology/list?branch=...
	•	보조: GET /api/v1/databases/{db}/classes
	•	구성(권장):
	•	ClassRegistryItem
	•	class_id
	•	label(LocalizedText)
	•	properties[](name/type/label/required/pk)
	•	relationships[](predicate/target/label/cardinality)
	•	사용처:
	•	Graph Builder의 class/predicate dropdown
	•	Import target_schema 구성(= properties → ImportTargetField[])
	•	Query Builder의 필터 필드 dropdown(라벨 기반)

⸻

5) 재사용 컴포넌트 명세(구현 단위)

아래는 “페이지를 구성하는 레고”입니다. 이 단위로 컴포넌트를 만들면 개발이 빠릅니다.

5.1 ContextNavbar

구현 상태: ✅ 개발 완료

	•	Blueprint: Navbar, Popover, Menu, Tag, Button
	•	Props:
	•	db, branch, lang
	•	onDbChange, onBranchChange, onLangChange
	•	commandActiveCount
	•	동작:
	•	DB 바꾸면 해당 DB의 lastBranch를 복원(없으면 main)

5.2 SettingsDialog

구현 상태: ✅ 개발 완료 (SettingsDialog + 강제 오픈/재시도)

	•	Props:
	•	isOpen, onClose, onSave
	•	Fields:
	•	Token(Bearer)
	•	Admin Token
	•	Language
	•	저장 시:
	•	메모리/스토리지 동기화
	•	선택적으로 마지막 실패 요청 1회 재시도

5.3 CommandTrackerDrawer

구현 상태: ✅ 개발 완료

	•	핵심 컴포넌트(서버 전역 목록이 아니라 로컬 추적)
	•	UI:
	•	Tabs(Active/Completed/Failed/Expired)
	•	InputGroup(command_id 붙여넣기)
	•	Table
	•	Callout(“전역 리스트 없음” 고정)
	•	기능:
	•	Add / Remove / Clear Expired
	•	Detail view(선택 command_id → status 호출)

5.4 AsyncCommandButton

구현 상태: ✅ 개발 완료 (공용 컴포넌트화)

	•	어떤 “쓰기 액션”이든 이 패턴을 사용
	•	입력:
	•	submit(): Promise<{command_id}> 또는 배치 {commands:[...]}
	•	출력:
	•	성공 시 Tracker 등록 + Toast + 버튼 상태 변경

5.5 ClassSelector / PredicateSelector

구현 상태: ✅ 개발 완료 (페이지 내 셀렉트로 구현)

	•	값: 내부 ID
	•	표시: label(ko/en) + (id: ...)
	•	Registry 기반

5.6 ApiErrorCallout

구현 상태: ✅ 개발 완료 (공용 Callout + 404 NonIdealState)

	•	에러 JSON 패턴 처리:
	•	{detail: string}
	•	{detail:{error:"unknown_label_keys", labels:[]}}
	•	{detail:{error:"optimistic_concurrency_conflict", expected_seq, actual_seq}}
	•	CTA:
	•	unknown_label_keys → “Open Mappings”
	•	OCC → “Use actual_seq and retry”
	•	429 → “Retry after N sec”

5.7 GraphCanvas

구현 상태: ✅ 개발 완료 (Cytoscape)

	•	Cytoscape/ForceGraph wrapper
	•	node click/edge click → InspectorContext 설정
	•	node badge: data_status 표시

5.8 InspectorDrawer

구현 상태: ✅ 개발 완료 (탭형 Summary/JSON/Audit/Lineage)

	•	Tabs:
	•	Summary / JSON / Audit(링크) / Lineage(링크)
	•	Context 타입:
	•	Class / Instance / GraphNode / GraphEdge / AuditItem / Command

⸻

6) 화면별 상세 기획 (와이어프레임 + API 시퀀스 + 상태/에러 + 완료조건)

아래는 “바로 구현 가능한” 수준으로 내려갑니다.

⸻

6.1 Databases /

구현 상태: ✅ 개발 완료

목적

DB 생성/삭제, 진입점

레이아웃(ASCII, Blueprint 단위)

┌──────────────────────────────[Main] Databases───────────────────────────────┐
│ [Card] Create Database                                                       │
│  name [InputGroup]  description [InputGroup]  [Button:Create (202)]          │
│  [Callout intent=warning] 202 → Command Tracker에서 완료 확인                 │
│                                                                              │
│ [Table] Databases                                                            │
│  db_name | description | [Button:Open] | [Button:Delete]                     │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	목록: GET /api/v1/databases
	•	생성(202): POST /api/v1/databases → data.command_id
	•	삭제(202, OCC):
	1.	GET /api/v1/databases/{db}/expected-seq
	2.	DELETE /api/v1/databases/{db}?expected_seq=...

상태/에러
	•	503/401/403 → SettingsDialog
	•	409(DB 삭제 OCC) → expected_seq 재조회 후 재시도
	•	성공: Tracker 등록 + Toast

완료 조건(수용 기준)
	•	Create → command COMPLETED → DB 목록에 노출

⸻

6.2 Overview /db/:db/overview?branch=...

구현 상태: ✅ 개발 완료 (Summary + Next Steps)

목적

현재 컨텍스트 요약 + “다음 행동” 가이드

레이아웃

┌──────────────────────────────[Main] Overview────────────────────────────────┐
│ [Card] Summary (GET /summary)                                                │
│  - context: db, branch                                                       │
│  - policy: protected_branches, is_protected_branch                            │
│  - services: redis/es health                                                  │
│                                                                              │
│ [Card] Next Steps                                                            │
│  1) Ontology 만들기 →                                                       │
│  2) Sheets preview/grid →                                                    │
│  3) Suggest schema/mappings →                                                │
│  4) Dry-run → Commit →                                                       │
│  5) Graph Explorer로 검증 →                                                  │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	GET /api/v1/summary?db=<db>&branch=<branch>

완료 조건
	•	summary 로드 / policy 표시

⸻

6.3 Branches /db/:db/branches

구현 상태: ✅ 개발 완료 (SwitchTo 버튼 + ContextNavbar 전환)

목적

스키마/데이터 실험 브랜치 생성/관리

레이아웃

┌──────────────────────────────[Main] Branches────────────────────────────────┐
│ [Button:Create Branch]                                                       │
│ [Table] branch_name | from | [SwitchTo] | [Delete]                           │
└──────────────────────────────────────────────────────────────────────────────┘

[Dialog] Create Branch
 name [InputGroup]  from_branch [Select default=main]  [Create]

API
	•	GET /api/v1/databases/{db}/branches
	•	POST /api/v1/databases/{db}/branches body {name, from_branch}
	•	DELETE /api/v1/databases/{db}/branches/{branch_name:path}

주의
	•	브랜치명에 / 가능 → URL 인코딩 필수

⸻

6.4 Ontology /db/:db/ontology?branch=...

구현 상태: ✅ 개발 완료

목적

클래스/속성/관계 정의 + validate + apply(202) + 보호브랜치 안전장치

레이아웃

┌──────────────────────────────[Main] Ontology────────────────────────────────┐
│ ┌───────────────┬────────────────────────────────────────────┬─────────────┐ │
│ │ [Card] Classes│ [Card] Editor                               │ [Card] Policy│ │
│ │ search [Input]│  Header: class_id | label | expected_seq     │ protected... │ │
│ │ [Tree]        │  [Tabs: Properties | Relationships | Validate| Export]      │ │
│ │ [Button:+New] │  [Button:Validate] [Button:Apply(202)] [Delete(202)]       │ │
│ └───────────────┴────────────────────────────────────────────┴─────────────┘ │
└──────────────────────────────────────────────────────────────────────────────┘

API (BFF v1 계약)
	•	목록: GET /api/v1/database/{db}/ontology/list?branch=...
	•	단건: GET /api/v1/database/{db}/ontology/{class_label}?branch=...
	•	생성 validate: POST /api/v1/database/{db}/ontology/validate?branch=...
	•	생성(202): POST /api/v1/database/{db}/ontology?branch=...
	•	업데이트 validate: POST /api/v1/database/{db}/ontology/{class_label}/validate?branch=...
	•	업데이트(202, OCC): PUT ...?branch=...&expected_seq=...
	•	삭제(202, OCC): DELETE ...?branch=...&expected_seq=...
	•	스키마 export: GET /api/v1/database/{db}/ontology/{class_id}/schema?branch=...&format=json|jsonld|owl

보호 브랜치 가드(필수)
	•	summary.policy에서 is_protected_branch=true이고 “고위험 변경”이면:
	•	Apply/Delete 클릭 시 Dialog:
	•	reason 입력(X-Change-Reason)
	•	admin token 필요(없으면 confirm 비활성)
	•	요청 헤더:
	•	X-Change-Reason
	•	Authorization 또는 X-Admin-Token
	•	(선택) X-Admin-Actor

OCC 처리
	•	409 응답의 actual_seq가 오면:
	•	Callout: “최신 seq는 actual_seq”
	•	버튼: “Use actual_seq and retry”

완료 조건
	•	Apply → command COMPLETED → list 재조회 시 반영

⸻

6.5 Mappings /db/:db/mappings

구현 상태: ✅ 개발 완료

목적

라벨→property_id 매핑 관리(unknown_label_keys 해결의 핵심)

레이아웃

┌──────────────────────────────[Main] Mappings────────────────────────────────┐
│ [Button:Refresh] [Button:Export] [Button:Validate File] [Button:Import File]│
│                                                                              │
│ [Table] label_key | property_id | class_id | status                           │
│                                                                              │
│ [Callout] unknown_label_keys 발생 시 여기서 해결                             │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	요약: GET /api/v1/database/{db}/mappings/
	•	export: POST /api/v1/database/{db}/mappings/export → 파일 다운로드
	•	validate(import 전): POST /api/v1/database/{db}/mappings/validate
	•	(레퍼런스 기준) multipart/form-data file
	•	import: POST /api/v1/database/{db}/mappings/import
	•	multipart/form-data file

파일 다운로드/업로드 구현 포인트
	•	export 응답의 Content-Disposition에서 파일명 추출해 저장
	•	import/validate는 FormData 사용

완료 조건
	•	import 성공 후 GET mappings/에 반영

⸻

6.6 Sheets Hub /db/:db/data/sheets

구현 상태: ✅ 개발 완료

목적

Sheets 소스 확인(Preview/Grid/Register)

레이아웃

┌──────────────────────────────[Main] Google Sheets───────────────────────────┐
│ [Tabs: Preview | Grid Detect | Registered]                                   │
│                                                                              │
│ Preview: sheet_url [InputGroup] worksheet [InputGroup] [Button:Preview]      │
│  → [Table] sample rows                                                       │
│                                                                              │
│ Grid: sheet_url ... [Button:Detect Grid]                                     │
│  → [Table] detected tables (table_id, bbox) [Button:Use in Import]           │
│                                                                              │
│ Registered: [Button:Register] [Table] sheet_id | db | branch | class_label... │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	Preview: POST /api/v1/data-connectors/google-sheets/preview
	•	Grid: POST /api/v1/data-connectors/google-sheets/grid
	•	Register: POST /api/v1/data-connectors/google-sheets/register
	•	Registered list: GET /api/v1/data-connectors/google-sheets/registered?database_name=...
	•	Registered preview: GET /api/v1/data-connectors/google-sheets/{sheet_id}/preview
	•	Unregister: DELETE /api/v1/data-connectors/google-sheets/{sheet_id}

Register UX 주의
	•	register 바디에 branch가 들어가지만, “Commit import”와는 다른 흐름임을 UI에서 구분:
	•	Register는 “모니터링 등록”
	•	Import Commit은 “배치 커맨드 제출(현재 main 고정)”

⸻

6.7 Import Wizard (Sheets) /db/:db/data/import/sheets

구현 상태: ✅ 개발 완료

목적

Prepare → Suggest Mappings → Dry‑run → Commit
(ImportFromGoogleSheetsRequest를 정확히 구성)

레이아웃(필수 4단계)

┌──────────────────────────────[Main] Import: Google Sheets────────────────────┐
│ [Stepper: 1 Prepare → 2 Suggest → 3 Dry‑run → 4 Commit]                      │
│                                                                              │
│ Step 1 Prepare                                                               │
│  sheet_url [Input] worksheet [Input] [Button:Preview]                        │
│  [Button:Detect Grid] → tables [Select table_id] bbox auto                    │
│  target_class_id [Select]  [Button:Load Target Schema]                       │
│  target_schema (read-only)                                                   │
│  [Button:Next] (enabled when sheet_url+table+schema ready)                   │
│                                                                              │
│ Step 2 Suggest Mappings                                                      │
│  [Button:Suggest] → mappings table (source_field→target_field)               │
│  [Button:Save mapping metadata]                                              │
│                                                                              │
│ Step 3 Dry‑run                                                               │
│  [Button:Run Dry‑run] → stats + errors table                                 │
│                                                                              │
│ Step 4 Commit                                                                │
│  [Callout WARNING] Commit은 현재 branch가 아니라 main에 반영됩니다.           │
│  [Button:Commit to main]                                                     │
│  → response.write.commands[] table + [Track All]                             │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	suggest mappings:
	•	POST /api/v1/database/{db}/suggest-mappings-from-google-sheets
	•	dry-run:
	•	POST /api/v1/database/{db}/import-from-google-sheets/dry-run
	•	commit(배치):
	•	POST /api/v1/database/{db}/import-from-google-sheets/commit
	•	응답: write.commands[] (각 항목 command_id, status_url)

Prepare 단계 구현 디테일(중요)
	•	target_schema 생성:
	•	1순위: GET /api/v1/database/{db}/ontology/{class_id}/schema?branch=...&format=json에서 파싱
	•	2순위(더 단순): ontology list/get의 properties를 {name,type}로 변환
	•	table_id/bbox:
	•	grid 응답에서 사용자가 table 선택하면 자동 채움

Commit 경고 UX(필수)
	•	현재 UI 컨텍스트 branch가 main이 아니면:
	•	Confirm Dialog:
	•	“이 커밋은 main에 반영됩니다”
	•	체크박스 동의 없으면 진행 불가

결과 처리
	•	write.commands[]를 모두 Command Tracker에 등록
	•	“Track All” 버튼 누르면 즉시 폴링 시작
	•	각 커맨드가 COMPLETED/FAILED일 때 토스트

주요 에러 처리
	•	400 변환/매핑 오류: errors table에서 row/column 제공
	•	429: Retry‑After 카운트다운, 버튼 disable
	•	5xx: 재시도 안내(자동 재시도는 1회 이하)

완료 조건
	•	commands[] 전부 COMPLETED(또는 일부 FAILED 시 사용자에게 실패 배치 명시)
	•	이후 Graph Explorer에서 노드가 조회되면 성공 판정

⸻

6.8 Import Wizard (Excel) /db/:db/data/import/excel

구현 상태: ✅ 개발 완료

목적

Excel 파일 업로드 기반 Dry‑run/Commit (multipart)

레이아웃(핵심만)

┌──────────────────────────────[Main] Import: Excel────────────────────────────┐
│ file [FileInput] sheet_name [Input]                                          │
│ target_class_id [Select] [Load Target Schema]                                │
│ mappings (json editor or upload)                                              │
│ table bbox (optional)                                                        │
│ [Button:Dry‑run]  [Button:Commit]                                             │
│ Commit 결과: write.commands[]                                                 │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	dry-run: POST /api/v1/database/{db}/import-from-excel/dry-run (multipart)
	•	commit: POST /api/v1/database/{db}/import-from-excel/commit (multipart)

주의
	•	Sheets와 동일하게 배치 commands[] 추적
	•	commit branch 제약은 Sheets와 동일하게 취급(문서에 “현재 main 고정” 명시)

⸻

6.9 Schema Suggestion /db/:db/data/schema-suggestion

구현 상태: ✅ 개발 완료 (카드형 UI + Validate 단계 + 테이블 선택)

목적

샘플 데이터 → 스키마 후보 생성 → Ontology 생성으로 연결

레이아웃

┌──────────────────────────────[Main] Schema Suggestion────────────────────────┐
│ source [Select: sheets | paste]                                              │
│ (sheets) sheet_url + table select + [Suggest Schema]                         │
│ (paste) columns + rows json + [Suggest Schema]                               │
│                                                                              │
│ Suggested classes [Cards]                                                    │
│  - class_id suggestion + properties + relationships                           │
│  [Validate Ontology] [Apply (202)]                                           │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	sheets: POST /api/v1/database/{db}/suggest-schema-from-google-sheets
	•	paste: POST /api/v1/database/{db}/suggest-schema-from-data
	•	적용:
	•	각 클래스별 POST /api/v1/database/{db}/ontology?branch=... (202)
	•	생성 커맨드들을 Tracker에 등록(“Batch group” 표시)

⸻

6.10 Instances /db/:db/instances (읽기 중심, branch ignored)

구현 상태: ✅ 개발 완료

목적

적재된 인스턴스 “빠른 확인”(하지만 branch 기반 검증은 Graph Explorer로)

레이아웃

┌──────────────────────────────[Main] Instances────────────────────────────────┐
│ [Tag WARNING] Branch ignored (instances read uses ES w/o branch)              │
│ class_id [Select] search [InputGroup] limit [Numeric] [Refresh]              │
│ [Table] instance_id | version | event_timestamp | summary... | [Open]         │
└──────────────────────────────────────────────────────────────────────────────┘

[Drawer:R] Instance Detail
 Header: instance_id + version(as expected_seq candidate)
 [Tabs: View(JSON) | Edit | Audit Links | Lineage Links]

API
	•	list: GET /api/v1/database/{db}/class/{class_id}/instances?limit&offset&search
	•	one: GET /api/v1/database/{db}/class/{class_id}/instance/{instance_id}
	•	sample-values: GET /api/v1/database/{db}/class/{class_id}/sample-values

Update/Delete(비동기, OCC) 지원 방식
	•	API:
	•	update: PUT /api/v1/database/{db}/instances/{class_label}/{instance_id}/update?branch=...&expected_seq=...
	•	delete: DELETE /api/v1/database/{db}/instances/{class_label}/{instance_id}/delete?branch=...&expected_seq=...
	•	expected_seq 확보 규칙(레퍼런스 근거):
	•	최신 조회 응답의 version을 expected_seq로 사용
	•	409 발생 시 actual_seq를 받아 “Use actual_seq” 재시도 버튼 제공

unknown_label_keys 처리
	•	인스턴스 쓰기(라벨 키)에서 400이 오면:
	•	labels[]를 표로 보여주고
	•	“Open Mappings” CTA

⸻

6.11 Explore: Graph Explorer /db/:db/explore/graph?branch=...

구현 상태: ✅ 개발 완료 (결과 export + Re‑run 포함)

목적(제품의 메인)

Graph traversal( Term) + ES 문서 join을 한 화면에서

레이아웃(최종)

┌──────────────────────────────[Main] Graph Explorer───────────────────────────┐
│ [Card] AI Assist (rate-limited)                                              │
│ question [InputGroup] [Generate Plan] [Ask&Run(optional)]                     │
│ [Callout] AI는 실행 전 Plan 검토를 권장                                      │
│                                                                              │
│ ┌──────────────┬───────────────────────────────┬───────────────────────────┐ │
│ │ [Card] Builder│ [Card] Graph Canvas           │ [Card] Inspector           │ │
│ │ start_class_id│ (Graph view)                   │ node/edge details          │ │
│ │ hops[]        │                                │ data_status Tag            │ │
│ │ filters       │                                │ ES payload(json)           │ │
│ │ branch        │                                │ provenance/audit (toggle)  │ │
│ │ safety        │                                │ [Copy] [Open Audit]        │ │
│ │ [SuggestPaths]│                                │ [Set as Lineage root]      │ │
│ │ [Run]         │                                │                             │ │
│ └──────────────┴───────────────────────────────┴───────────────────────────┘ │
│ [Collapse] Results Table (nodes/edges export)                                 │
└──────────────────────────────────────────────────────────────────────────────┘

Builder 필드(구현 고정)
	•	start_class (value: class_id)
	•	hops[]:
	•	predicate (value: predicate_id)
	•	target_class (value: class_id)
	•	filters (고급, 기본은 빈 값)
	•	주의: filters는 내부 property_id 기반일 수 있어 오해 위험
	•	v1에서는 “필터는 최소 기능”으로 두고, 주 사용은 limit/paths
	•	branch(필수, 기본 main)
	•	safety 기본값(제품 기본):
	•	limit=10
	•	max_nodes=200
	•	max_edges=500
	•	no_cycles=true
	•	include_documents=true
	•	include_paths=false(기본 OFF)
	•	include_provenance=false(기본 OFF)
	•	include_audit=false(기본 OFF)

API
	•	실행: POST /api/v1/graph-query/{db}?branch=...
	•	경로 추천: GET /api/v1/graph-query/{db}/paths?source_class=...&target_class=...&max_depth=...&branch=...
	•	헬스: GET /api/v1/graph-query/health

결과 렌더링 규칙
	•	nodes[]:
	•	data_status를 Tag로 표시
	•	FULL이면 data(json) 탭에 문서 표시
	•	PARTIAL/MISSING이면 Callout: “프로젝션 지연 가능”
	•	edges[]:
	•	predicate 표시(가능하면 registry에서 label로 렌더)

AI Assist(자연어) — “안전한 방식” 기준
	•	“Generate Plan(실행 없음)”:
	•	POST /api/v1/ai/translate/query-plan/{db}
	•	응답의 plan을 Builder에 적용(Apply)
	•	“Ask&Run(선택)”:
	•	POST /api/v1/ai/query/{db}
	•	응답의 answer + warnings + plan + execution을 별도 패널에 표시
	•	단, 결과 그래프 렌더는 가능하면 plan을 기반으로 graph-query를 다시 실행해 일관성 유지(선택)

429 대비
	•	AI는 레이트리밋 대상 → Retry‑After 카운트다운으로 버튼 disable

완료 조건
	•	Run → 응답 nodes/edges 렌더
	•	노드 클릭 → Inspector 표시
	•	data_status 상태를 사용자에게 명확히 전달

⸻

6.12 Explore: Query Builder(라벨 기반) /db/:db/explore/query?branch=...

구현 상태: ✅ 개발 완료 (raw query UI 포함)

목적

그래프가 아니라 “테이블형 조회”가 필요한 사용자용(라벨 기반 Query API)

레이아웃

┌──────────────────────────────[Main] Query Builder────────────────────────────┐
│ class_label [Select(label)]  limit [Numeric]                                 │
│ filters [Repeater] field(label) [Select] op [Select] value [Input] [Remove]  │
│ select fields [MultiSelect] order_by [Select] dir [Select]                   │
│ [Button:Run Query]                                                           │
│ [Table] results                                                              │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	builder meta: GET /api/v1/database/{db}/query/builder
	•	run: POST /api/v1/database/{db}/query
	•	raw(run 제한): POST /api/v1/database/{db}/query/raw

구현 디테일
	•	operator UI(=, >=, like …)는 API operator 키(eq, ge, like …)로 매핑
	•	field 선택은 “클래스 properties label”을 사용(라벨 기반 계약)
	•	unknown_label_keys 발생 시 mappings로 유도

⸻

6.13 Merge /db/:db/merge?branch=...

구현 상태: ✅ 개발 완료

목적

브랜치 병합 충돌 시뮬레이션/해결

레이아웃

┌──────────────────────────────[Main] Merge────────────────────────────────────┐
│ source_branch [Select] target_branch [Select] [Simulate]                     │
│ conflicts [Table] [Open]                                                     │
│ conflict detail + resolution (ours/theirs/custom json)                        │
│ [Resolve] (필요시 protected guard)                                            │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	simulate: POST /api/v1/database/{db}/merge/simulate
	•	resolve: POST /api/v1/database/{db}/merge/resolve

⸻

6.14 Audit /db/:db/audit

구현 상태: ✅ 개발 완료 (Reset 버튼 + Command Tracker 연동)

목적

감사 로그 조회(DB partition 고정)

레이아웃

┌──────────────────────────────[Main] Audit Logs───────────────────────────────┐
│ [Tag] partition_key = db:<db> (fixed)                                        │
│ filters: since/until actor resource_type action command_id event_id search    │
│ [Apply] [Reset] [Chain‑Head Verify]                                           │
│ [Table] time | actor | action | status | resource | command_id | [Open]       │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	logs: GET /api/v1/audit/logs?partition_key=db:<db>&...
	•	chain-head: GET /api/v1/audit/chain-head?partition_key=db:<db>

Inspector 연계
	•	command_id 클릭 → Command Tracker에서 열기(또는 수동 추가)

⸻

6.15 Lineage /db/:db/lineage

구현 상태: ✅ 개발 완료 (Use Selected Graph Node 포함)

목적

root 기반 라인리지 그래프/영향 분석

레이아웃

┌──────────────────────────────[Main] Lineage──────────────────────────────────┐
│ root [InputGroup] [Load] [Use Selected Graph Node]                            │
│ (no root) → [NonIdealState]                                                  │
│ [Tabs: Graph | Impact | Metrics]                                             │
│ Graph: lineage canvas                                                        │
└──────────────────────────────────────────────────────────────────────────────┘

API
	•	graph: GET /api/v1/lineage/graph?root=...&db_name=...
	•	impact: GET /api/v1/lineage/impact?root=...&db_name=...
	•	metrics: GET /api/v1/lineage/metrics?db_name=...&window_minutes=...

⸻

6.16 Operations: Tasks /operations/tasks

구현 상태: ✅ 개발 완료

목적

백그라운드 작업 모니터링/취소

API
	•	list: GET /api/v1/tasks/
	•	detail: GET /api/v1/tasks/{task_id}
	•	result: GET /api/v1/tasks/{task_id}/result
	•	cancel: DELETE /api/v1/tasks/{task_id}
	•	metrics: GET /api/v1/tasks/metrics/summary

⸻

6.17 Operations: Admin /operations/admin (Admin Token 필요)

구현 상태: ✅ 개발 완료

목적

replay/recompute 등 운영자 작업 트리거/결과 확인

API
	•	replay: POST /api/v1/admin/replay-instance-state → task_id
	•	recompute: POST /api/v1/admin/recompute-projection → task_id
	•	cleanup: POST /api/v1/admin/cleanup-old-replays
	•	system-health: GET /api/v1/admin/system-health

UX
	•	admin token 없으면 Callout + 버튼 disabled
	•	실행 결과는 Tasks 화면으로 링크

⸻

7) 에러/상태 매트릭스 (필수 UX)

구현 상태: ✅ 개발 완료 (SettingsDialog 강제 오픈 + 404 NonIdealState)

7.1 HTTP 코드별 전역 동작
	•	400
	•	detail.error가 unknown_label_keys면:
	•	labels 표 출력 + “Open Mappings”
	•	그 외: detail 문자열 그대로 Callout
	•	401/403
	•	SettingsDialog 열기 + “권한/토큰”
	•	404
	•	command status에서 404면 “EXPIRED/UNKNOWN”(TTL)
	•	일반 리소스 404면 NonIdealState
	•	409(OCC)
	•	detail.actual_seq 노출
	•	“Use actual_seq and retry”
	•	429
	•	Retry‑After 기반 카운트다운 + 버튼 disable
	•	5xx
	•	재시도 안내(자동 1회 이하), 이후 사용자가 재시도

7.2 커맨드 상태별 UI
	•	PENDING/PROCESSING: Spinner + “진행 중”
	•	RETRYING: “재시도 중(백오프)” + 계속 폴링
	•	COMPLETED: 성공 토스트 + 관련 화면 CTA(예: Ontology list refresh)
	•	FAILED: error 표시 + “원인 수정 후 재제출” 안내
	•	CANCELLED: 중단 표시
	•	EXPIRED/UNKNOWN(404): 만료 표시 + “목록에서 제거” 제공

⸻

8) 구현 순서(가장 안전한 개발 플랜)

구현 상태: ✅ 개발 완료 (체크리스트 기준 전부 구현됨)
	1.	API Client + 전역 인터셉터(503/401/403/429/409)
	2.	SettingsDialog(토큰 저장) + AppShell
	3.	CommandTrackerDrawer(로컬 저장 + 폴링 + WS 옵션)
	4.	Databases + Branches
	5.	Ontology(list/get/validate/apply + protected guard + registry 구축)
	6.	Sheets Hub(preview/grid/register)
	7.	Import Wizard(Sheets) — Prepare/Mapping/Dry‑run/Commit + 배치 커맨드 추적
	8.	Graph Explorer(ID 기반 Builder + paths + data_status)
	9.	Mappings(export/import/validate file)
	10.	Instances(read + update/delete with version/actual_seq)
	11.	Audit(partition_key 강제) + Lineage(root 필수)
	12.	Merge(simulate/resolve)
	13.	Operations(Tasks/Admin)

⸻
