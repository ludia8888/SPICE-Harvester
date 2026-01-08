# SPICE HARVESTER 배포 가이드 (코드 기준)

> Updated: 2026-01-08  \
> Canonical ops doc: `docs/OPERATIONS.md`

## 1) 빠른 시작 (로컬/개발)

```bash
# repo root 기준
docker compose -f docker-compose.full.yml up -d
```

헬스 체크:

```bash
curl -fsS http://localhost:8000/health
curl -fsS http://localhost:8002/api/v1/health
curl -fsS http://localhost:8003/health
```

## 2) 환경 변수

- `.env.example`를 `.env`로 복사 후 수정
- 상세 목록: `backend/ENVIRONMENT_VARIABLES.md`

## 3) 서비스 포트

- OMS: 8000
- BFF: 8002
- Funnel: 8003
- TerminusDB: 6363
- Postgres: 5433
- Kafka: 39092
- MinIO: 9000/9001
- lakeFS: 48080

## 4) 프로덕션 고려사항

- TLS 종료는 리버스 프록시/LB에서 처리
- BFF/OMS 토큰 인증 강제
- Postgres + MinIO + lakeFS 메타데이터 정기 백업
- Kafka retention/DLQ 정책 정의
- 모니터링: `/metrics`, `/api/v1/monitoring/*`
