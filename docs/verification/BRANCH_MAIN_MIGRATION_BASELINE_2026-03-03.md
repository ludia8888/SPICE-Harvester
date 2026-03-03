---
orphan: true
---

# Branch `master` → `main` Migration Baseline (2026-03-03)

## Scope
- Repo: `/Users/isihyeon/Desktop/SPICE-Harvester`
- Snapshot taken before runtime/test rewrite for main-only cutover.

## Commands Used
```bash
rg --fixed-strings '"master"' backend --glob '!backend/tests/**' --glob '!backend/bff/tests/**' --glob '!backend/docs/**' --glob '!backend/**/migrations/**' | wc -l
rg --fixed-strings '"master"' backend/tests backend/bff/tests | wc -l
rg --fixed-strings 'master' docs README.md README.ko.md .env.example backend/.env.example | wc -l
```

## Baseline Counts
- Runtime code (`backend/**`, tests/docs/migrations 제외): **182**
- Test code (`backend/tests/**`, `backend/bff/tests/**`): **244**
- Docs/env examples (`docs/**`, `README*`, `.env.example`, `backend/.env.example`): **13**

## High-Concentration Runtime Files (pre-migration)
- `backend/bff/routers/foundry_ontology_v2.py`
- `backend/bff/routers/foundry_datasets_v2.py`
- `backend/oms/routers/ontology.py`
- `backend/oms/routers/instance_async.py`
- `backend/shared/config/settings.py`
- `backend/shared/services/pipeline/*`

## Notes
- `DEV_MASTER_*` 계열(개발용 auth bypass 문맥)은 브랜치 의미와 분리해 검토.
- 본 문서는 마이그레이션 전 기준선 기록용이며, Gate A/B 완료 후 후속 검증 리포트에서 잔존 건수를 재기록한다.
