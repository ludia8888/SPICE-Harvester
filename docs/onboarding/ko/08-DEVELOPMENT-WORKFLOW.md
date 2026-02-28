# 개발 워크플로 - 코드 수정 가이드

> 이 문서는 Spice OS 코드베이스에서 **첫 코드 변경**을 하기 위해 알아야 할 것들을 정리했어요. 코드 구조 패턴, 실제 예시, Git 규칙, 빌드 프로세스를 다룹니다.

---

## 코드 구조 패턴: Router → Service → Model

> 💡 한 줄 요약: 백엔드 코드는 "라우터 → 서비스 → 모델"이라는 3계층 패턴을 따릅니다.

```
Router (라우터)           HTTP 엔드포인트 정의, 요청/응답 변환
  │                      파일 위치: backend/bff/routers/ 또는 backend/oms/routers/
  ▼
Service (서비스)          비즈니스 로직, 유효성 검증
  │                      파일 위치: backend/bff/services/ 또는 backend/shared/services/
  ▼
Model (모델)              데이터 구조 정의 (Pydantic)
                          파일 위치: backend/shared/models/
```

**지켜야 할 규칙:**

- **라우터**는 HTTP 관련 코드만 담당해요 (요청 파싱, 응답 포맷팅)
- **서비스**는 비즈니스 로직을 담당해요 (검증, 계산, DB 호출)
- **모델**은 데이터 구조를 정의해요 (Pydantic 모델)
- ⚠️ 라우터가 직접 DB를 호출하면 **안 돼요** — 반드시 서비스를 거쳐야 합니다

> 💡 **왜 이렇게 나눌까요?** 각 계층의 역할이 분리되어 있으면 테스트가 쉬워지고, 코드를 수정할 때 영향 범위가 명확해집니다. 예를 들어 DB 쿼리를 바꿔도 라우터 코드는 건드릴 필요가 없거든요.

---

## 예시 워크스루: "BFF에 새 API 엔드포인트 추가하기"

> 💡 한 줄 요약: "데이터베이스 통계 API"를 추가하는 과정을 6단계로 따라가 봅시다.

"데이터베이스의 통계 정보를 반환하는 엔드포인트"를 추가한다고 가정해 볼게요.

### Step 1: 모델 정의

`backend/shared/models/`에 응답 모델을 정의합니다.

```python
# backend/shared/models/responses.py (기존 파일에 추가)
from pydantic import BaseModel

class DatabaseStats(BaseModel):
    object_type_count: int
    instance_count: int
    dataset_count: int
```

### Step 2: 서비스 로직 작성

`backend/bff/services/`에 비즈니스 로직을 작성합니다.

```python
# backend/bff/services/database_stats_service.py (새 파일)
from shared.services.registries.dataset_registry import DatasetRegistry

async def get_database_stats(db_name: str, dataset_registry: DatasetRegistry):
    # DB에서 통계 조회
    datasets = await dataset_registry.list_by_database(db_name)
    return {
        "object_type_count": ...,
        "instance_count": ...,
        "dataset_count": len(datasets),
    }
```

### Step 3: 라우터에 엔드포인트 추가

기존 라우터 파일에 엔드포인트를 추가합니다.

```python
# backend/bff/routers/ 의 적절한 파일에 추가
@router.get("/api/v1/databases/{db_name}/stats")
async def get_database_stats_endpoint(
    db_name: str,
    container = Depends(get_container),
):
    stats = await get_database_stats(
        db_name,
        container.dataset_registry
    )
    return {"status": "success", "data": stats}
```

### Step 4: 테스트 작성

```python
# backend/tests/unit/test_database_stats.py
import pytest

class TestDatabaseStats:
    async def test_returns_correct_counts(self):
        # Arrange
        ...
        # Act
        result = await get_database_stats("test-db", mock_registry)
        # Assert
        assert result["dataset_count"] == 5
```

### Step 5: 테스트 실행

```bash
# 단위 테스트만 빠르게 실행
make backend-unit

# 특정 테스트 파일만 실행
PYTHONPATH=backend python3 -m pytest backend/tests/unit/test_database_stats.py -v
```

### Step 6: Docker 리빌드 (필요한 경우)

Docker에서 실행 중인 서비스에 반영하려면 리빌드가 필요해요.

```bash
# BFF 리빌드
docker compose -f docker-compose.full.yml build bff

# BFF만 재시작
docker compose -f docker-compose.full.yml restart bff
```

---

## 파일을 찾는 방법

> 💡 한 줄 요약: "이 기능의 코드가 어디 있지?" 싶을 때 쓸 수 있는 3가지 방법이에요.

### API 엔드포인트로 찾기

1. 프론트엔드에서 Network 탭을 열어 API URL을 확인해요
2. 해당 URL 패턴으로 라우터를 검색합니다

```bash
# 예: "/api/v2/ontologies" 관련 코드 찾기
grep -r "ontologies" backend/bff/routers/ --include="*.py" -l

# 예: 특정 엔드포인트 찾기
grep -r "objectTypes" backend/bff/routers/ --include="*.py" -l
```

### 페이지에서 찾기

1. 프론트엔드 URL 경로를 확인해요 (예: `/ontology`)
2. `AppRouter.tsx`에서 해당 경로에 매핑된 페이지 컴포넌트를 찾아요
3. 페이지 컴포넌트에서 호출하는 API 함수를 추적합니다

### 에러 메시지로 찾기

```bash
# 에러 메시지 텍스트로 검색
grep -r "에러 메시지 텍스트" backend/ --include="*.py" -l
```

---

## 의존성 주입 (DI) 패턴

> 💡 한 줄 요약: 서비스를 직접 만들지 않고, **컨테이너에서 주입받아** 사용해요.

Spice OS는 **컨테이너 기반 의존성 주입**을 사용합니다.

```python
# ❌ 나쁜 예: 서비스를 직접 생성
async def my_endpoint():
    registry = DatasetRegistry(postgres_pool)  # 직접 생성하면 안 됨
    ...

# ✅ 좋은 예: 컨테이너에서 주입받기
async def my_endpoint(container = Depends(get_container)):
    registry = container.dataset_registry  # 컨테이너에서 가져옴
    ...
```

**왜 이렇게 할까요?**

- 테스트에서 Mock으로 쉽게 교체할 수 있어요
- 서비스 생명주기를 중앙에서 관리할 수 있어요
- 순환 의존성을 방지할 수 있거든요

---

## 환경 설정 패턴

> 💡 한 줄 요약: 환경 변수는 **Pydantic Settings**로 중앙 관리하고, `os.getenv()` 직접 호출은 금지예요.

```python
# backend/shared/config/settings.py
from pydantic_settings import BaseSettings

class ApplicationSettings(BaseSettings):
    postgres_host: str = "localhost"
    postgres_port: int = 5432
    # ...

# 사용할 때
settings = get_settings()
print(settings.postgres_host)
```

⚠️ **`os.getenv()` 직접 호출은 금지!** 반드시 `settings`를 통해 접근해야 합니다.

> 💡 **왜?** 환경 변수를 한 곳에서 관리하면 타입 검증이 되고, 어떤 설정이 있는지 한눈에 파악할 수 있어요. `os.getenv()`가 여기저기 흩어져 있으면 나중에 관리가 힘들어집니다.

---

## Git 브랜치 규칙

> 💡 한 줄 요약: `main`에서 분기하고, 용도에 맞는 접두사를 붙여요.

```
main              # 안정 브랜치 (배포 기준)
  └── feature/*   # 기능 개발 (예: feature/add-stats-endpoint)
  └── fix/*       # 버그 수정 (예: fix/search-pagination)
  └── docs/*      # 문서 변경 (예: docs/update-readme)
  └── codex/*     # Codex 관련 변경
```

### 커밋 메시지 컨벤션

```
feat: 새 기능 추가
fix: 버그 수정
docs: 문서 변경
test: 테스트 추가/수정
refactor: 리팩토링 (기능 변경 없음)
chore: 빌드/설정 변경
```

**예시:**

```
feat(bff): add database stats endpoint
fix(oms): correct pagination token in search response
test(e2e): add objectify workflow test
docs(onboarding): update local setup guide
```

### PR (Pull Request) 워크플로

1. `main`에서 feature 브랜치를 생성해요
2. 코드 변경 + 테스트를 추가해요
3. `make backend-unit` 통과를 확인해요
4. PR 생성 → 코드 리뷰 → 머지

---

## 코드 스타일

### 백엔드 (Python)

| 도구 | 용도 | 실행 명령 |
|------|------|----------|
| **Ruff** | 린터 + 포매터 | `ruff check backend/` |
| **mypy** | 타입 체크 | `mypy backend/` |

### 프론트엔드 (TypeScript)

| 도구 | 용도 | 실행 명령 |
|------|------|----------|
| **ESLint** | 린터 | `cd frontend && npm run lint` |
| **Prettier** | 포매터 | 에디터 자동 적용 |

---

## Docker 리빌드 참고

> 💡 한 줄 요약: 코드를 수정한 뒤 Docker에 반영하려면 리빌드가 필요해요.

```bash
# 특정 서비스만 리빌드
docker compose -f docker-compose.full.yml build <서비스이름>

# 리빌드 + 재시작
docker compose -f docker-compose.full.yml up -d --build <서비스이름>

# 모든 서비스 리빌드 (시간 오래 걸림)
docker compose -f docker-compose.full.yml build

# BFF의 Dockerfile 위치: backend/bff/Dockerfile
# 빌드 컨텍스트: backend/
```

---

## 다음으로 읽을 문서

- [테스트 가이드](09-TESTING-GUIDE.md) - 테스트 실행 및 작성 방법
- [트러블슈팅 FAQ](10-TROUBLESHOOTING-FAQ.md) - 개발 중 자주 겪는 문제
