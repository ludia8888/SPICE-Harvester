# 실전 시나리오 - 우리 플랫폼으로 이런 것까지 된다고?

> 비개발자도 "아, 이게 이렇게 되는 거구나!"라고 무릎을 탁 칠 수 있도록, 실제 비즈니스 상황에 빗대어 Spice OS의 핵심 기능을 풀어드릴게요. 모든 예시는 **우리 아키텍처에서 실제로 동작하는 코드**입니다.

---

## 🕵️ 시나리오 1: 보이스피싱 자금 추적 (멀티홉 그래프 쿼리)

### 자금 추적이 필요한 상황

수사관이 전화번호 `010-1234-5678`을 쓰는 용의자가 **누구에게 최종적으로 돈을 보냈는지** 추적해야 합니다.

일반 데이터베이스라면? SQL JOIN을 4~5번 중첩해서 몇 분씩 기다려야 해요. 하지만 우리 플랫폼의 **멀티홉 그래프 쿼리**는 징검다리를 건너듯 한 번에 찾아냅니다.

### 수사 명령서 (실제 코드)

```json
{
  "start_class": "Person",
  "filters": { "phone": "010-1234-5678" },
  "hops": [
    {
      "predicate": "owner",
      "target_class": "BankAccount",
      "reverse": true
    },
    {
      "predicate": "from_account",
      "target_class": "Transaction",
      "reverse": true
    },
    {
      "predicate": "to_account",
      "target_class": "BankAccount"
    },
    {
      "predicate": "owner",
      "target_class": "Person"
    }
  ],
  "include_paths": true,
  "max_paths": 25
}
```

### 수사 명령서 해부하기

| 단계 | 코드 | 수사관의 말로 풀면 |
|------|------|--------------------|
| 📍 출발 | `"start_class": "Person"` + `"filters"` | "전화번호가 010-1234-5678인 **사람**을 찾아라!" |
| 🦘 1홉 | `predicate: "owner"`, `reverse: true` → `BankAccount` | "그 용의자가 소유한 **은행 계좌**를 전부 찾아라!" |
| 🦘 2홉 | `predicate: "from_account"`, `reverse: true` → `Transaction` | "그 계좌에서 돈이 빠져나간 **거래 내역**을 싹 다 긁어와라!" |
| 🦘 3홉 | `predicate: "to_account"` → `BankAccount` | "그 돈이 최종적으로 입금된 **도착지 계좌**가 어디냐!" |
| 🦘 4홉 | `predicate: "owner"` → `Person` | "그 계좌의 명의자, **최종 수취인**이 누구냐!" |
| 📸 보고서 | `"include_paths": true, "max_paths": 25` | "경로를 전부 포함해서 25개만 추려서 보고해라!" |

> 💡 **`reverse: true`가 뭔가요?**
> 데이터의 화살표는 보통 `[계좌] → (주인이 누구?) → [사람]` 방향이에요.
> 우리는 사람에서 출발해서 계좌를 찾아야 하니까, 화살표를 **역방향**으로 거슬러 올라가라는 뜻이에요.

### 그래프 쿼리의 아키텍처 흐름

```
클라이언트 요청
  → BFF (:8002, 라우터: graph.py)
    → OMS (:8000, 그래프 엔진)
      → Elasticsearch (노드/엣지 저장소)
        → 결과: 노드 + 엣지 + 경로 반환
```

- **BFF**가 요청을 받아서 **OMS(Object Management System)**에 전달해요
- OMS의 그래프 엔진이 Elasticsearch에서 **노드(사람, 계좌)와 엣지(관계)**를 탐색해요
- `include_paths: true` 덕분에 프론트엔드에서 **수사 드라마의 실타래 보드**처럼 시각화할 수 있어요

### 이 기능으로 할 수 있는 것들

| 분야 | 활용 예시 | 홉 수 |
|------|-----------|-------|
| 🏦 금융 사기 탐지 | 용의자 → 계좌 → 거래 → 도착 계좌 → 수취인 | 4홉 |
| 🦠 역학 조사 | 확진자 → 방문 장소 → 동시간대 방문자 | 3홉 |
| 🛒 이커머스 추천 | 나 → 내가 산 상품 → 그 상품을 산 다른 사람 → 그 사람이 산 다른 상품 | 3홉 |
| 🏭 공급망 리콜 | 불량 부품 → 엔진 → 차량 → 소유주 | 3홉 |
| 👥 조직 분석 | 직원 → 부서 → 부서장 → 부서장의 프로젝트 | 3홉 |

---

## 🔍 시나리오 2: "검색의 달인" — 23개 무기로 뭐든 찾는다

### 복합 검색이 필요한 상황

마케팅팀이 이런 조건의 고객을 찾아야 해요:
> "서울에서 반경 5km 이내에 살고, 지난 30일 이내에 가입했고, 이름에 '김'이 들어가며, VIP 등급인 활성 고객"

일반 데이터베이스로는 복잡한 SQL을 짜야 하지만, 우리 검색 API는 **레고 블록 쌓듯이** 조건을 조합하면 돼요.

### 검색 명령서 (실제 코드)

```json
{
  "where": {
    "type": "and",
    "value": [
      {
        "type": "withinDistanceOf",
        "field": "location",
        "value": {
          "center": { "latitude": 37.5665, "longitude": 126.9780 },
          "distance": { "value": 5, "unit": "km" }
        }
      },
      {
        "type": "relativeDateRange",
        "field": "joinDate",
        "startDuration": "P30D"
      },
      {
        "type": "containsAnyTerm",
        "field": "fullName",
        "value": "김"
      },
      {
        "type": "eq",
        "field": "tier",
        "value": "VIP"
      },
      {
        "type": "eq",
        "field": "status",
        "value": "ACTIVE"
      }
    ]
  },
  "orderBy": {
    "fields": [{ "field": "joinDate", "direction": "desc" }]
  },
  "pageSize": 50
}
```

### 23개 검색 무기 총정리

형사가 수사할 때 다양한 도구를 쓰듯, 우리 검색 API에는 **23개 연산자**가 있어요.

#### 🎯 비교 연산자 (5개) — "딱 맞는 거 찾기"

| 연산자 | 비유 | 예시 |
|--------|------|------|
| `eq` | "이거랑 **똑같은** 거" | 부서 = "Engineering" |
| `gt` | "이것보다 **큰** 거" | 연봉 > 5000만 |
| `lt` | "이것보다 **작은** 거" | 나이 < 30 |
| `gte` | "이것 **이상**인 거" | 평점 ≥ 4.0 |
| `lte` | "이것 **이하**인 거" | 가격 ≤ 10000 |

#### 📝 텍스트 검색 (7개) — "글자 속에서 찾기"

| 연산자 | 비유 | 예시 |
|--------|------|------|
| `containsAnyTerm` | "이 단어 중 **하나라도** 포함" | "서울 OR 부산" |
| `containsAllTerms` | "이 단어가 **전부** 포함" | "클라우드 AND 보안" |
| `containsAllTermsInOrder` | "**정확한 문구** 검색" | "인공지능 플랫폼" (순서 일치) |
| `containsAllTermsInOrderPrefixLastTerm` | "**자동완성** 검색" | "김철" → 김철수, 김철민 |
| `startsWith` | "이걸로 **시작**하는 거" | 주문번호가 "ORD-2024"로 시작 |
| `wildcard` | "**패턴** 검색" | "customer_*" (와일드카드) |
| `regex` | "**정규식** 검색" | 이메일 형식 확인 |

#### 🗺️ 지리공간 검색 (7개) — "지도 위에서 찾기"

| 연산자 | 비유 | 예시 |
|--------|------|------|
| `withinDistanceOf` | "반경 N km 이내" | 강남역에서 3km 이내 매장 |
| `withinBoundingBox` | "사각형 영역 안" | 서울시 경계 안의 고객 |
| `withinPolygon` | "다각형 영역 안" | 특정 배달 구역 안의 주문 |
| `intersectsBoundingBox` | "사각형과 겹치는 영역" | 지도 화면과 겹치는 배달 구역 |
| `intersectsPolygon` | "다각형과 겹치는 영역" | 공사 구역과 겹치는 배송 경로 |
| `doesNotIntersectBoundingBox` | "사각형 **밖** 영역" | 홍수 지역 밖의 창고 |
| `doesNotIntersectPolygon` | "다각형 **밖** 영역" | 규제 구역 밖의 사업장 |

#### 🔗 논리 연산자 (3개) — "조건 조합하기"

| 연산자 | 비유 | 예시 |
|--------|------|------|
| `and` | "**모든** 조건을 만족" | VIP **이면서** 서울 거주 |
| `or` | "**하나라도** 만족" | 서울 **또는** 부산 거주 |
| `not` | "이건 **아닌** 것" | 탈퇴 회원 **제외** |

#### 📦 기타 (1개)

| 연산자 | 비유 | 예시 |
|--------|------|------|
| `isNull` | "값이 **있는지/없는지**" | 이메일이 없는 고객 찾기 |

### 검색 API의 아키텍처 흐름

```
검색 요청
  → BFF (:8002)
    → OMS (:8000, query.py)
      → SearchJsonQueryV2 → Elasticsearch bool/range/match 쿼리로 변환
        → Elasticsearch (:9200) 실행
          → 밀리초 단위로 결과 반환
```

---

## 📊 시나리오 3: CEO의 실시간 경영 대시보드 (집계 쿼리)

### 실시간 분석이 필요한 상황

CEO가 이사회에서 발표할 자료가 필요합니다:
> "지역별 매출 합계, 고객 평균 생애가치, 상위 5% 고객의 평점은?"

엑셀로 10만 건을 돌리면 컴퓨터가 뻗지만, 우리 집계 API는 **서버에서 계산해서 결과만 딱** 보내줘요.

### 대시보드 명령서 (실제 코드)

```json
{
  "aggregation": [
    { "type": "sum", "field": "revenue", "name": "총매출" },
    { "type": "avg", "field": "customerLifetimeValue", "name": "평균생애가치" },
    {
      "type": "approximatePercentile",
      "field": "rating",
      "approximatePercentile": 0.95,
      "name": "상위5%평점"
    }
  ],
  "groupBy": [
    { "field": "region" },
    { "field": "productCategory" }
  ]
}
```

### 집계 코드 해부하기

| 코드 | CEO에게 설명하면 |
|------|-----------------|
| `"type": "sum", "field": "revenue"` | "매출을 전부 **더해라**" |
| `"type": "avg", "field": "customerLifetimeValue"` | "고객 생애가치의 **평균**을 내라" |
| `"type": "approximatePercentile", 0.95` | "평점 **상위 5%** 기준점을 알려달라" |
| `"groupBy": ["region", "productCategory"]` | "지역별, 상품 카테고리별로 **나눠서** 보여달라" |

### 지원하는 집계 유형 8가지

| 집계 | 비유 | 용도 |
|------|------|------|
| `count` | "**몇 개**야?" | 부서별 직원 수 |
| `sum` | "**전부 더하면** 얼마?" | 지역별 총 매출 |
| `avg` | "**평균**이 얼마?" | 평균 고객 만족도 |
| `min` | "**제일 작은** 게 뭐야?" | 최저 주문 금액 |
| `max` | "**제일 큰** 게 뭐야?" | 최고 연봉 |
| `exactDistinct` | "**종류가 몇 가지**야?" | 고유 고객 수 (정확) |
| `approximateDistinct` | "**대략 몇 종류**야?" | 방문 IP 수 (빠른 추정) |
| `approximatePercentile` | "**상위 몇 %**선은?" | P95 응답시간 |

### 결과 예시

```json
{
  "buckets": [
    {
      "key": { "region": "수도권", "productCategory": "소프트웨어" },
      "metrics": { "총매출": 50000000, "평균생애가치": 1200000, "상위5%평점": 4.8 }
    },
    {
      "key": { "region": "영남권", "productCategory": "하드웨어" },
      "metrics": { "총매출": 30000000, "평균생애가치": 800000, "상위5%평점": 4.5 }
    }
  ]
}
```

---

## 🏦 시나리오 4: 절대 지워지지 않는 장부 (이벤트 소싱)

### 감사 추적이 필요한 상황

금융감독원 감사가 나왔습니다:
> "6개월 전에 고객 정보를 누가, 언제, 왜 변경했는지 전부 보여주세요."

일반 시스템은 "현재 상태"만 저장하니까 과거 기록이 없어요. 하지만 우리 플랫폼은 **모든 변경을 이벤트로 기록**하기 때문에, 6개월 전이든 1년 전이든 완벽하게 추적할 수 있어요.

### 작동 원리

은행 장부를 생각해 보세요. 잔액을 지우고 새로 쓰는 게 아니라, **입금/출금 내역을 계속 쌓아나가는** 거예요.

```
[일반 시스템]                    [우리 시스템 - 이벤트 소싱]
┌──────────────────┐            ┌──────────────────────────┐
│ 고객: 김철수      │            │ 이벤트 #1: 고객 생성      │
│ 부서: 영업팀      │ ← 최신값만 │   김철수, 개발팀           │
│ 등급: VIP        │   덮어씀   │ 이벤트 #2: 부서 변경      │
└──────────────────┘            │   개발팀 → 마케팅팀        │
                                │   변경자: 박대리, 3월 15일  │
                                │ 이벤트 #3: 부서 변경      │
                                │   마케팅팀 → 영업팀        │
                                │   변경자: 이과장, 6월 1일   │
                                │ 이벤트 #4: 등급 변경      │
                                │   일반 → VIP              │
                                │   변경자: 최팀장, 9월 10일  │
                                └──────────────────────────┘
```

### 이벤트 봉투 (EventEnvelope) — 실제 구조

모든 변경은 이 **"봉투"**에 담겨서 기록돼요:

```json
{
  "event_id": "evt-20240601-001",
  "event_type": "EXECUTE_ACTION_REQUESTED",
  "aggregate_type": "action_log",
  "aggregate_id": "EMP-042",
  "occurred_at": "2024-06-01T09:30:00Z",
  "actor": "이과장",
  "data": {
    "objectType": "Employee",
    "primaryKey": "EMP-042",
    "changes": {
      "department": { "before": "마케팅팀", "after": "영업팀" }
    }
  },
  "metadata": {
    "correlation_id": "req-abc123",
    "code_version": "v2.1.0"
  },
  "sequence_number": 3
}
```

| 필드 | 감사관에게 설명하면 |
|------|-------------------|
| `event_id` | 이 변경의 **고유 번호** (영수증 번호) |
| `actor` | **누가** 했나 |
| `occurred_at` | **언제** 했나 |
| `data.changes` | **뭘** 바꿨나 (이전값 → 이후값) |
| `metadata.correlation_id` | 이 변경이 **어떤 요청**에서 시작됐나 (추적 번호) |
| `sequence_number` | 이 객체의 **몇 번째 변경**인가 (순서 보장) |

### 이벤트 소싱의 아키텍처 흐름

```
사용자 액션 요청
  → BFF → Kafka (이벤트 큐)
    → Action Worker (비동기 실행)
      → S3/MinIO (이벤트 영구 저장 — 절대 삭제 불가!)
      → Postgres (현재 상태 업데이트)
      → Elasticsearch (검색 인덱스 업데이트)
      → Redis (실행 상태 추적)
```

**핵심**: S3에 저장된 이벤트는 **불변(Immutable)**이에요. 한번 기록되면 수정도, 삭제도 불가능해요. 그래서 완벽한 감사 추적이 가능한 거예요.

---

## 🗺️ 시나리오 5: "내 주변 맛집" — 지리공간 검색

### 위치 기반 검색이 필요한 상황

배달 앱에서 사용자 위치 기반으로 가게를 찾아야 해요:
> "강남역에서 반경 2km 이내, 평점 4.0 이상, 현재 영업 중인 음식점"

### 검색 명령서

```json
{
  "where": {
    "type": "and",
    "value": [
      {
        "type": "withinDistanceOf",
        "field": "location",
        "value": {
          "center": { "latitude": 37.4979, "longitude": 127.0276 },
          "distance": { "value": 2, "unit": "km" }
        }
      },
      { "type": "gte", "field": "rating", "value": 4.0 },
      { "type": "eq", "field": "isOpen", "value": true }
    ]
  },
  "orderBy": {
    "fields": [{ "field": "rating", "direction": "desc" }]
  },
  "pageSize": 20
}
```

### 지도 위의 다양한 검색

```
📍 withinDistanceOf          📦 withinBoundingBox         🔷 withinPolygon
   반경 원 안에서 찾기           사각형 안에서 찾기           다각형 안에서 찾기

      ╭──╮                    ┌────────────┐              ╱╲
     ╱ 📍 ╲                   │  🏪  🏪    │             ╱  ╲
    │ 🏪  🏪│                  │      🏪    │            │ 🏪 🏪│
     ╲ 🏪 ╱                   │  🏪       │            │  🏪  │
      ╰──╯                    └────────────┘             ╲    ╱
                                                          ╲╱
   "3km 이내 매장"            "지도 화면 안 매장"         "배달 구역 안 매장"
```

---

## 🔄 시나리오 6: 엑셀을 지식 그래프로 변환 (데이터 파이프라인)

### 데이터 변환이 필요한 상황

고객팀이 3만 건짜리 CSV 파일을 들고 왔어요:
> "이 고객 데이터를 시스템에 넣어서 검색하고 분석하고 싶어요."

### 변환 과정

```
📄 customers.csv (원시 데이터)
│
│  customer_id, name,     company_rid,    revenue,   join_date
│  CUST-001,   김영희,   COMP-100,       5000000,   2024-01-15
│  CUST-002,   이철수,   COMP-200,       3000000,   2024-03-22
│
▼ 1단계: 매핑 (Map)
│  CSV 컬럼 → 온톨로지 속성 연결
│  customer_id → id (string, 기본 키)
│  name → displayName (string)
│  company_rid → company (참조, Company 타입으로)
│  revenue → revenue (number)
│  join_date → joinDate (date, 날짜 파싱)
│
▼ 2단계: 검증 (Validate)
│  ✅ id가 고유한가?
│  ✅ company_rid가 실제 존재하는 Company인가?
│  ✅ revenue가 양수인가?
│  ✅ join_date가 유효한 날짜 형식인가?
│
▼ 3단계: 객체화 (Objectify)
│  CSV 행 → 객체 인스턴스 생성
│  관계(relationship) 자동 연결: Customer → Company
│
▼ 4단계: 인덱싱 (Index)
│  → Elasticsearch에 저장
│  → 즉시 검색 가능!
│
▼ 결과
   {
     "instance_id": "cust-001",
     "class_id": "Customer",
     "properties": {
       "id": "CUST-001",
       "displayName": "김영희",
       "revenue": 5000000,
       "joinDate": "2024-01-15"
     },
     "relationships": {
       "company": "ri.spice.main.object.comp-100"
     }
   }
```

### 파이프라인의 아키텍처 흐름

```
CSV 업로드
  → BFF (pipeline_datasets_ops_objectify.py)
    → LakeFSClient (데이터셋 버전 관리)
      → Kafka (변환 작업 큐)
        → Objectify Worker (매핑 + 검증 + 변환)
          → OMS (객체 생성)
            → Projection Worker → Elasticsearch (검색 가능!)
```

**핵심**: 한 번 파이프라인을 설정하면, 같은 형식의 CSV를 넣을 때마다 **자동으로 동일한 변환**이 적용돼요. 38개 이상의 변환 규칙을 조합할 수 있어요.

---

## 📦 시나리오 7: 불량 부품 긴급 리콜 (멀티홉 + 검색 조합)

### 긴급 리콜이 필요한 상황

자동차 공장에서 불량 부품이 발견됐습니다:
> "부품번호 PART-X99가 들어간 차량의 소유주를 **즉시** 찾아서 리콜 안내를 보내야 합니다!"

### 1단계: 멀티홉으로 소유주 추적

```json
{
  "start_class": "Part",
  "filters": { "partNumber": "PART-X99" },
  "hops": [
    { "predicate": "installed_in", "target_class": "Engine" },
    { "predicate": "mounted_on", "target_class": "Vehicle" },
    { "predicate": "owned_by", "target_class": "Person" }
  ],
  "include_paths": true
}
```

### 2단계: 찾은 소유주 중 특정 조건 검색

```json
{
  "where": {
    "type": "and",
    "value": [
      { "type": "eq", "field": "vehicleStatus", "value": "ACTIVE" },
      {
        "type": "withinPolygon",
        "field": "address",
        "value": {
          "coordinates": [[37.4, 126.8], [37.6, 126.8], [37.6, 127.1], [37.4, 127.1]]
        }
      }
    ]
  }
}
```

> 이렇게 **그래프 쿼리 + 검색 API를 조합**하면, "서울에 사는 활성 차량 소유주" 같은 복합 조건도 한 번에 해결할 수 있어요.

---

## 🔀 시나리오 8: 데이터의 "되돌리기" — 데이터셋 버전 관리

### 데이터 롤백이 필요한 상황

데이터 엔지니어가 실수로 잘못된 데이터를 올렸어요:
> "어제 올린 고객 데이터에 오류가 있었어요. 이전 버전으로 돌아갈 수 있나요?"

구글 독스에 "버전 기록"이 있듯이, 우리 데이터셋에도 **브랜치와 트랜잭션**이 있어요!

### 작동 방식

```
main 브랜치 (정식 데이터)
│
├── v1: 초기 고객 데이터 (1월 1일)
├── v2: 고객 100명 추가 (2월 1일) ← 여기로 되돌리기!
├── v3: 잘못된 데이터 업로드 (3월 1일) ❌
│
└── feature 브랜치 (실험용)
    ├── 새 필드 추가 테스트
    └── 아직 main에 합치지 않음
```

### 실제 API 흐름

```bash
# 1. 데이터셋 정보 확인
GET /api/v2/datasets/{datasetRid}

# 2. 브랜치 목록 확인
GET /api/v2/datasets/{datasetRid}/branches
# → ["main", "feature/new-fields"]

# 3. 트랜잭션 시작 (원자적 작업 보장)
POST /api/v2/datasets/{datasetRid}/transactions
# → { "rid": "ri.spice.main.transaction.txn-345" }

# 4. 데이터 업로드
POST /api/v2/datasets/{datasetRid}/files/customers.csv/upload

# 5-A. 문제 없으면 → 커밋
POST /api/v2/datasets/{datasetRid}/transactions/txn-345/commit

# 5-B. 문제 있으면 → 롤백!
POST /api/v2/datasets/{datasetRid}/transactions/txn-345/abort
```

### 데이터셋의 아키텍처 흐름

```
데이터셋 요청
  → BFF (foundry_datasets_v2.py)
    → LakeFSClient (Git 같은 버전 관리)
      → S3/MinIO (실제 파일 저장)
      → Postgres - DatasetRegistry (메타데이터)
```

- **LakeFSClient**가 데이터를 Git처럼 관리해요
- **브랜치**로 실험하고, **트랜잭션**으로 원자성을 보장하고, 문제 있으면 **abort**로 롤백해요

---

## 🔌 시나리오 9: 어디서든 데이터 가져오기 (데이터 커넥터)

### 분산된 데이터를 통합해야 하는 상황

회사에 데이터가 여기저기 흩어져 있어요:
> "Google Sheets에 고객 목록, Snowflake에 매출 데이터, PostgreSQL에 주문 데이터..."

### 지원하는 데이터 소스 6가지

```
┌─────────────────────────────────────────────────────┐
│                    Spice OS                          │
│                                                      │
│   📊 Google Sheets  ──┐                              │
│   ❄️  Snowflake      ──┤                              │
│   🐘 PostgreSQL     ──┼──→ 데이터 커넥터 ──→ 데이터셋  │
│   🐬 MySQL          ──┤      (자동 변환)    (통합 저장) │
│   🔷 SQL Server     ──┤                              │
│   🔶 Oracle         ──┘                              │
│                                                      │
│   데이터셋 ──→ Objectify ──→ 검색/분석/그래프 쿼리     │
└─────────────────────────────────────────────────────┘
```

### 연결부터 분석까지 전체 흐름

```bash
# 1단계: 데이터 소스 연결 등록
POST /api/v2/connectivity/connections
{ "type": "POSTGRES", "host": "db.company.com", "database": "orders" }

# 2단계: 테이블 임포트 설정
POST /api/v2/connectivity/tableImports
{ "connectionRid": "...", "query": "SELECT * FROM orders WHERE year = 2024" }

# 3단계: 데이터 미리보기 (진짜 넣기 전에 확인)
GET /api/v2/connectivity/tableImports/{importRid}/preview

# 4단계: 임포트 실행 → 데이터셋 생성
POST /api/v2/connectivity/tableImports/{importRid}/execute

# 5단계: 온톨로지 매핑 → 객체화 → 검색 가능!
```

---

## 🎯 정리: 한눈에 보는 "우리 플랫폼으로 할 수 있는 것들"

| 기능 | 비유 | 핵심 서비스 | 활용 시나리오 |
|------|------|-------------|--------------|
| **멀티홉 그래프** | 징검다리 건너기 | OMS + Elasticsearch | 자금 추적, 역학 조사, 추천 |
| **23개 검색 연산자** | 형사의 수사 도구 | OMS + Elasticsearch | 고객 세그먼트, 재고 검색 |
| **지리공간 검색** | 배달앱 "내 주변" | Elasticsearch | 매장 찾기, 배달 구역 관리 |
| **집계 쿼리** | CEO 대시보드 | Elasticsearch | 매출 분석, KPI 모니터링 |
| **이벤트 소싱** | 은행 장부 | Kafka + S3 + Action Worker | 감사 추적, 규정 준수 |
| **데이터 파이프라인** | 요리 레시피 | Objectify Worker + LakeFSClient | CSV→객체 변환, ETL |
| **버전 관리** | 구글 독스 히스토리 | LakeFSClient + S3 | 데이터 롤백, 실험 |
| **데이터 커넥터** | 만능 어댑터 | Data Connector | 외부 DB 연동 |

---

## 다음 단계

더 깊이 알고 싶다면:

- **[아키텍처 이해하기](05-ARCHITECTURE-EXPLAINED.md)** — 서비스 간 관계 상세 설명
- **[데이터 흐름](06-DATA-FLOW-WALKTHROUGH.md)** — 데이터가 시스템을 흐르는 과정
