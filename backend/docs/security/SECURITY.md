# SPICE HARVESTER 보안 설정 가이드 (코드 기준)

> 최종 업데이트: 2026-01-08  \
> 메인 보안 문서: `docs/SECURITY.md`

## 1) 인증

- BFF/OMS는 **공유 토큰 방식**을 사용합니다.
- 헤더: `X-Admin-Token` 또는 `Authorization: Bearer <token>`.
- 기본값은 **인증 필수**이며, 로컬에서만 명시적으로 끌 수 있습니다.

환경 변수:
- BFF: `BFF_REQUIRE_AUTH`, `BFF_ADMIN_TOKEN`/`BFF_WRITE_TOKEN`
- OMS: `OMS_REQUIRE_AUTH`, `OMS_ADMIN_TOKEN`/`OMS_WRITE_TOKEN`
- 비활성화(로컬 전용): `ALLOW_INSECURE_BFF_AUTH_DISABLE=true`, `ALLOW_INSECURE_OMS_AUTH_DISABLE=true`

## 2) 입력 보안

- 입력 정규화/검증: `backend/shared/security/input_sanitizer.py`
- DB 이름, 클래스 ID, 파일명, URL 등은 강제 검증됩니다.

## 3) CORS

- 환경 변수 `CORS_ORIGINS`로 허용 origin을 제어합니다.
- 자세한 설정은 `backend/docs/development/CORS_CONFIGURATION_GUIDE.md`를 참고하세요.

## 4) 레이트 리미터

- Redis 기반 토큰 버킷(장애 시 로컬 fallback).
- 헤더: `X-RateLimit-*`.

## 5) TLS

- 앱 자체는 TLS를 종료하지 않습니다.
- 운영 환경에서는 리버스 프록시/LB에서 TLS 종료를 구성하세요.
