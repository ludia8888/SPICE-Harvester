# 테스트 가이드 - 테스트 실행 및 작성

> 이 문서는 Spice OS의 테스트 구조, 실행 방법, 새 테스트 작성 패턴을 설명해요. 테스트가 처음이어도 괜찮습니다. 차근차근 따라가 볼까요?

---

## 테스트 개요

> 💡 한 줄 요약: 백엔드 단위 테스트는 Docker 없이 30초면 돌아가요. E2E 테스트는 전체 스택이 필요합니다.

| 종류 | 개수 | Docker 필요 | 소요 시간 |
|------|------|------------|----------|
| **백엔드 단위 테스트** | 1,556+ | 불필요 | ~30초 |
| **백엔드 E2E 테스트** | 91+ | 전체 스택 필요 | 5~20분 |
| **프론트엔드 단위 테스트** | - | 불필요 | ~10초 |
| **프론트엔드 E2E 테스트** | - | BFF+OMS 필요 | ~2분 |

**실행 명령:**

- 백엔드 단위: `make backend-unit`
- 백엔드 E2E: `make backend-prod-full`
- 프론트엔드 단위: `cd frontend && npm test`
- 프론트엔드 E2E: `cd frontend && npx playwright test`

---

## 백엔드 단위 테스트

> 💡 한 줄 요약: 가장 자주 쓰는 명령어는 `make backend-unit`이에요. Docker 없이도 실행 가능합니다.

### 실행

```bash
# 전체 단위 테스트 실행 (가장 많이 사용)
make backend-unit
```

**예상 출력:**

```
backend/shared/tools/error_taxonomy_audit.py ...  OK
... passed, ... warnings in 30.XXs
```

✅ 이런 출력이 나오면 성공이에요!

### 커버리지 리포트와 함께 실행

```bash
make backend-coverage
```

**예상 출력:**

```
... passed
TOTAL    ...    30.0%+
Required test coverage of 30.0% reached. Total coverage: XX.X%
```

### 특정 테스트만 실행

개발 중에는 전체 테스트를 돌리기보다, 내가 수정한 부분만 빠르게 확인하고 싶을 때가 많아요.

```bash
# 특정 파일
PYTHONPATH=backend python3 -m pytest backend/tests/unit/test_something.py -v

# 특정 테스트 함수
PYTHONPATH=backend python3 -m pytest backend/tests/unit/test_something.py::TestClass::test_method -v

# 키워드로 필터링
PYTHONPATH=backend python3 -m pytest backend/tests/unit/ -k "keyword" -v
```

### 테스트 파일 위치

```
backend/
├── tests/
│   ├── unit/                    # 단위 테스트 (Docker 불필요)
│   │   ├── test_ontology_*.py   #   온톨로지 관련
│   │   ├── test_pipeline_*.py   #   파이프라인 관련
│   │   ├── test_action_*.py     #   액션 관련
│   │   └── ...
│   ├── test_*.py                # E2E 테스트 (전체 스택 필요)
│   ├── conftest.py              # 공통 fixture + 인프라 감지
│   ├── pytest.ini               # pytest 설정
│   └── utils/                   # 테스트 유틸리티
│       ├── auth.py              #   인증 헬퍼
│       ├── qa_helpers.py        #   QA 공통 함수
│       └── pipelines_v2_adapter.py  # 파이프라인 테스트 어댑터
├── bff/tests/                   # BFF 전용 단위 테스트
└── funnel/tests/                # Funnel 전용 단위 테스트
```

### pytest.ini 설정

```ini
# backend/tests/pytest.ini
[pytest]
pythonpath = ..
```

> ⚠️ **주의:** `pytest.ini`는 `--timeout` 플래그를 지원하지 **않아요**. 타임아웃이 필요하면 코드 내에서 `asyncio.wait_for()`를 사용하세요.

---

## 백엔드 E2E 테스트

> 💡 한 줄 요약: E2E 테스트는 전체 스택이 떠 있어야 돌릴 수 있어요. 시간이 오래 걸리니 필요할 때만 실행하세요.

### 사전 준비

E2E 테스트를 실행하기 전에 전체 스택이 실행 중이어야 해요.

```bash
docker compose -f docker-compose.full.yml up -d
```

### 실행

```bash
# 전체 E2E 테스트
make backend-prod-full

# Postgres 기반 빠른 테스트만
make backend-prod-quick
```

### `@requires_infra` 마커

E2E 테스트에는 `@requires_infra` 마커가 붙어있어요. 이 마커 덕분에 Docker 없이도 단위 테스트만 실행할 수 있습니다.

```python
@pytest.mark.requires_infra
async def test_create_and_search_objects():
    """전체 스택이 필요한 E2E 테스트"""
    ...
```

- `make backend-unit`은 `@requires_infra` 테스트를 **건너뛰어요** (Docker 없이도 실행 가능)
- `make backend-prod-full`은 **모든 테스트**를 실행해요

### 주요 E2E 테스트 파일

| 파일 | 설명 | 소요 시간 |
|------|------|----------|
| `test_foundry_e2e_qa.py` | Foundry API 전체 워크플로 | ~3분 |
| `test_financial_investigation_workflow_e2e.py` | 금융 조사 워크플로 | **4분+** |
| `test_pipeline_execution_semantics_e2e.py` | 파이프라인 (Pipeline) 실행 시맨틱 | ~5분 |
| `test_openapi_contract_smoke.py` | OpenAPI 스펙 계약 검증 | ~1분 |
| `test_action_writeback_e2e_smoke.py` | 액션 (Action) Writeback 검증 | ~2분 |
| `test_consistency_e2e_smoke.py` | 데이터 일관성 검증 | ~3분 |

> ⚠️ **참고:** `test_financial_investigation_workflow_e2e.py`는 objectify_worker와 projection_worker가 **반드시** 실행 중이어야 해요. 이 테스트는 정상적으로도 4분 이상 걸리니, 실패가 아니라 단순히 느린 것일 수 있어요.

---

## 프론트엔드 테스트

### 단위 테스트 (Vitest)

```bash
cd frontend
npm test              # 실행
npm run test:watch    # 변경 감지 모드
```

> 💡 `test:watch` 모드는 파일을 수정할 때마다 관련 테스트를 자동으로 다시 돌려줘서, 개발 중에 특히 편리해요.

### E2E 테스트 (Playwright)

```bash
cd frontend
npx playwright install    # 브라우저 설치 (최초 1회)
npx playwright test       # 실행
npx playwright test --ui  # UI 모드 (디버깅에 유용)
```

---

## 새 테스트 작성하기

> 💡 한 줄 요약: Arrange → Act → Assert 패턴을 따르면 깔끔한 테스트를 작성할 수 있어요.

### 단위 테스트 패턴

```python
# backend/tests/unit/test_my_feature.py
import pytest
from unittest.mock import AsyncMock, MagicMock

class TestMyFeature:
    """기능 X에 대한 단위 테스트"""

    @pytest.fixture
    def mock_registry(self):
        """테스트용 Mock 레지스트리"""
        registry = AsyncMock()
        registry.get_by_id.return_value = {"id": "test-1", "name": "Test"}
        return registry

    async def test_should_return_data_when_exists(self, mock_registry):
        """존재하는 데이터를 조회하면 데이터를 반환한다"""
        # Arrange
        service = MyService(registry=mock_registry)

        # Act
        result = await service.get("test-1")

        # Assert
        assert result["name"] == "Test"
        mock_registry.get_by_id.assert_called_once_with("test-1")

    async def test_should_raise_when_not_found(self, mock_registry):
        """존재하지 않는 데이터를 조회하면 에러가 발생한다"""
        # Arrange
        mock_registry.get_by_id.return_value = None

        # Act & Assert
        with pytest.raises(NotFoundError):
            await MyService(registry=mock_registry).get("nonexistent")
```

### E2E 테스트 패턴

```python
# backend/tests/test_my_workflow_e2e.py
import pytest
import httpx

@pytest.mark.requires_infra
class TestMyWorkflowE2E:
    """E2E: 전체 워크플로 검증"""

    @pytest.fixture
    def client(self):
        return httpx.AsyncClient(
            base_url="http://localhost:8002",
            headers={"Authorization": f"Bearer {ADMIN_TOKEN}"},
            timeout=180.0,
        )

    async def test_create_and_retrieve(self, client):
        # 1. 생성
        resp = await client.post("/api/v1/...", json={...})
        assert resp.status_code == 200

        # 2. 조회로 확인
        resp = await client.get(f"/api/v1/.../{id}")
        assert resp.status_code == 200
        assert resp.json()["data"]["name"] == "expected"
```

### 테스트 이름 규칙

테스트 이름은 **"어떤 행동을 하면 어떤 결과가 나오는지"**를 설명해야 해요. 구현 세부사항이 아니라 기대하는 동작을 담아주세요.

```python
# ✅ 좋은 예: 행동을 설명
async def test_should_return_empty_list_when_no_data():
async def test_should_raise_validation_error_for_invalid_type():
async def test_search_returns_filtered_results_by_department():

# ❌ 나쁜 예: 구현을 설명
async def test_query_elasticsearch():
async def test_call_registry():
```

---

## 테스트 디버깅 팁

> 💡 한 줄 요약: 테스트가 실패하면 `-v -s` 옵션으로 상세 출력을 확인하고, E2E라면 서비스 상태부터 점검하세요.

### 단일 테스트 디버깅 실행

```bash
# 상세 출력 (-v) + print 출력 (-s) + 특정 테스트
PYTHONPATH=backend python3 -m pytest \
  backend/tests/unit/test_something.py::TestClass::test_method \
  -v -s
```

### E2E 테스트가 실패할 때

순서대로 확인해 보세요.

**1단계: 서비스가 실행 중인지 확인**

```bash
docker compose -f docker-compose.full.yml ps
```

**2단계: 서비스 로그 확인**

```bash
docker compose -f docker-compose.full.yml logs bff --tail 50
docker compose -f docker-compose.full.yml logs oms --tail 50
```

**3단계: Elasticsearch 인덱스 확인**

```bash
curl http://localhost:9200/_cat/indices?v
```

---

## CI 파이프라인

> 💡 한 줄 요약: `make ci` 하나로 CI에서 돌아가는 전체 검증을 로컬에서도 실행할 수 있어요.

```bash
# CI에서 실행하는 전체 검증
make ci
```

이 명령은 다음 순서로 실행돼요:

1. `backend-coverage` — 백엔드 단위 테스트 + 커버리지
2. `frontend-check` — 프론트엔드 린트 + 빌드
3. `frontend-coverage` — 프론트엔드 테스트 + 커버리지

> 💡 PR을 올리기 전에 `make ci`를 한 번 돌려보면, CI에서 실패할 일을 미리 방지할 수 있어요.

---

## 다음으로 읽을 문서

- [트러블슈팅 FAQ](10-TROUBLESHOOTING-FAQ.md) - 테스트 실패 등 자주 겪는 문제
- [30일 학습 로드맵](LEARNING-ROADMAP.md) - 학습 일정 가이드
