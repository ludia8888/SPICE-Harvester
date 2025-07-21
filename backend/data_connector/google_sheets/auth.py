"""
Google Sheets Connector - Authentication Module (for future OAuth2 support)
"""

import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, Optional

import httpx

logger = logging.getLogger(__name__)


class GoogleOAuth2Client:
    """
    Google OAuth2 인증 클라이언트 (향후 확장용)

    현재는 API Key 기반 인증만 지원하지만,
    향후 OAuth2 flow를 위한 기본 구조 제공
    """

    # OAuth2 endpoints
    AUTH_URL = "https://accounts.google.com/o/oauth2/v2/auth"
    TOKEN_URL = "https://oauth2.googleapis.com/token"
    REVOKE_URL = "https://oauth2.googleapis.com/revoke"

    # Required scopes for Google Sheets
    SCOPES = [
        "https://www.googleapis.com/auth/spreadsheets.readonly",
        "https://www.googleapis.com/auth/drive.readonly",
    ]

    def __init__(
        self,
        client_id: Optional[str] = None,
        client_secret: Optional[str] = None,
        redirect_uri: Optional[str] = None,
    ):
        """
        초기화

        Args:
            client_id: Google OAuth2 Client ID
            client_secret: Google OAuth2 Client Secret
            redirect_uri: OAuth2 redirect URI
        """
        self.client_id = client_id or os.getenv("GOOGLE_CLIENT_ID")
        self.client_secret = client_secret or os.getenv("GOOGLE_CLIENT_SECRET")
        self.redirect_uri = redirect_uri or os.getenv(
            "GOOGLE_REDIRECT_URI", "http://localhost:8002/api/v1/connectors/google/oauth/callback"
        )

        # Token storage (실제로는 DB나 Redis 사용)
        self._tokens: Dict[str, Dict[str, Any]] = {}

    def get_authorization_url(self, state: str) -> str:
        """
        OAuth2 인증 URL 생성

        Args:
            state: CSRF 방지용 state 파라미터

        Returns:
            인증 URL
        """
        params = {
            "client_id": self.client_id,
            "redirect_uri": self.redirect_uri,
            "response_type": "code",
            "scope": " ".join(self.SCOPES),
            "access_type": "offline",  # Refresh token 받기 위함
            "prompt": "consent",  # 항상 동의 화면 표시
            "state": state,
        }

        # URL encode parameters
        param_str = "&".join([f"{k}={v}" for k, v in params.items()])
        return f"{self.AUTH_URL}?{param_str}"

    async def exchange_code_for_token(self, code: str) -> Dict[str, Any]:
        """
        Authorization code를 access token으로 교환

        Args:
            code: Authorization code

        Returns:
            Token 정보
        """
        async with httpx.AsyncClient() as client:
            data = {
                "code": code,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "redirect_uri": self.redirect_uri,
                "grant_type": "authorization_code",
            }

            response = await client.post(self.TOKEN_URL, data=data)
            response.raise_for_status()

            token_data = response.json()

            # Calculate expiry time
            expires_in = token_data.get("expires_in", 3600)
            token_data["expires_at"] = (
                datetime.utcnow() + timedelta(seconds=expires_in)
            ).isoformat()

            return token_data

    async def refresh_access_token(self, refresh_token: str) -> Dict[str, Any]:
        """
        Refresh token으로 새 access token 획득

        Args:
            refresh_token: Refresh token

        Returns:
            새 token 정보
        """
        async with httpx.AsyncClient() as client:
            data = {
                "refresh_token": refresh_token,
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "grant_type": "refresh_token",
            }

            response = await client.post(self.TOKEN_URL, data=data)
            response.raise_for_status()

            token_data = response.json()

            # Calculate new expiry time
            expires_in = token_data.get("expires_in", 3600)
            token_data["expires_at"] = (
                datetime.utcnow() + timedelta(seconds=expires_in)
            ).isoformat()

            # Refresh token은 새로 발급되지 않을 수 있음
            if "refresh_token" not in token_data:
                token_data["refresh_token"] = refresh_token

            return token_data

    async def revoke_token(self, token: str) -> bool:
        """
        Token 취소

        Args:
            token: Access token 또는 refresh token

        Returns:
            성공 여부
        """
        async with httpx.AsyncClient() as client:
            params = {"token": token}

            response = await client.post(self.REVOKE_URL, params=params)
            return response.status_code == 200

    def store_user_token(self, user_id: str, token_data: Dict[str, Any]):
        """
        사용자 토큰 저장

        Args:
            user_id: 사용자 ID
            token_data: Token 정보
        """
        self._tokens[user_id] = token_data
        logger.info(f"Stored token for user {user_id}")

    def get_user_token(self, user_id: str) -> Optional[Dict[str, Any]]:
        """
        사용자 토큰 조회

        Args:
            user_id: 사용자 ID

        Returns:
            Token 정보 또는 None
        """
        return self._tokens.get(user_id)

    async def get_valid_access_token(self, user_id: str) -> Optional[str]:
        """
        유효한 access token 조회 (필요시 refresh)

        Args:
            user_id: 사용자 ID

        Returns:
            Valid access token 또는 None
        """
        token_data = self.get_user_token(user_id)
        if not token_data:
            return None

        # Check if token is expired
        expires_at = datetime.fromisoformat(token_data["expires_at"])
        if expires_at <= datetime.utcnow():
            # Token expired, try to refresh
            refresh_token = token_data.get("refresh_token")
            if not refresh_token:
                logger.warning(f"No refresh token for user {user_id}")
                return None

            try:
                new_token_data = await self.refresh_access_token(refresh_token)
                self.store_user_token(user_id, new_token_data)
                return new_token_data["access_token"]
            except Exception as e:
                logger.error(f"Failed to refresh token for user {user_id}: {e}")
                return None

        return token_data["access_token"]

    def remove_user_token(self, user_id: str) -> bool:
        """
        사용자 토큰 삭제

        Args:
            user_id: 사용자 ID

        Returns:
            삭제 성공 여부
        """
        if user_id in self._tokens:
            del self._tokens[user_id]
            logger.info(f"Removed token for user {user_id}")
            return True
        return False


class APIKeyAuth:
    """
    API Key 기반 인증 (현재 사용 중)
    """

    def __init__(self, api_key: Optional[str] = None):
        """
        초기화

        Args:
            api_key: Google API Key
        """
        self.api_key = api_key or os.getenv("GOOGLE_API_KEY", "")

    def get_auth_params(self) -> Dict[str, str]:
        """
        API 요청용 인증 파라미터 반환

        Returns:
            인증 파라미터 딕셔너리
        """
        if self.api_key:
            return {"key": self.api_key}
        return {}

    def is_configured(self) -> bool:
        """
        API Key 설정 여부 확인

        Returns:
            설정 여부
        """
        return bool(self.api_key)
