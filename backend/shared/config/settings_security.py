from __future__ import annotations

import os
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

from shared.config.settings_support import _ENV_FILE


class SecuritySettings(BaseSettings):
    """Security configuration settings"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        description="JWT secret key",
    )
    algorithm: str = Field(
        default="HS256",
        description="JWT algorithm",
    )
    access_token_expire_minutes: int = Field(
        default=30,
        description="JWT access token expiry in minutes",
    )
    data_encryption_keys: str = Field(
        default="",
        description="Comma-separated base64/hex keys for encrypting stored data at rest (DATA_ENCRYPTION_KEYS)",
    )
    allow_plaintext_connector_secrets: bool = Field(
        default=False,
        description=(
            "Allow connector secrets plaintext fallback when DATA_ENCRYPTION_KEYS is unset "
            "(ALLOW_PLAINTEXT_CONNECTOR_SECRETS). Use only for local debugging."
        ),
    )
    input_sanitizer_max_dict_keys: int = Field(
        default=100,
        description="Max dict keys to sanitize (INPUT_SANITIZER_MAX_DICT_KEYS)",
    )
    input_sanitizer_max_list_items: int = Field(
        default=1000,
        description="Max list items to sanitize (INPUT_SANITIZER_MAX_LIST_ITEMS)",
    )
    max_search_query_length: int = Field(
        default=100,
        description="Maximum search query length (MAX_SEARCH_QUERY_LENGTH)",
    )
    max_db_name_length: int = Field(
        default=50,
        description="Maximum database name length (MAX_DB_NAME_LENGTH)",
    )
    max_class_id_length: int = Field(
        default=100,
        description="Maximum class id length (MAX_CLASS_ID_LENGTH)",
    )
    max_instance_id_length: int = Field(
        default=255,
        description="Maximum instance id length (MAX_INSTANCE_ID_LENGTH)",
    )
    max_client_id_length: int = Field(
        default=50,
        description="Maximum websocket client id length (MAX_CLIENT_ID_LENGTH)",
    )
    max_user_id_length: int = Field(
        default=50,
        description="Maximum user id length (MAX_USER_ID_LENGTH)",
    )


class TestSettings(BaseSettings):
    """Test environment configuration"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    test_database_url: str = Field(
        default="sqlite:///./test.db",
        description="Test database URL",
    )
    test_timeout: int = Field(
        default=30,
        description="Test timeout in seconds",
    )
    oms_query_wait_seconds: float = Field(
        default=60.0,
        description="OMS query wait seconds (OMS_QUERY_WAIT_SECONDS)",
    )


class GoogleSheetsSettings(BaseSettings):
    """Google Sheets integration settings"""

    model_config = SettingsConfigDict(
        env_file=_ENV_FILE,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    google_sheets_api_key: Optional[str] = Field(
        default=None,
        description="Google Sheets API key",
    )
    google_sheets_credentials_path: Optional[str] = Field(
        default=None,
        description="Google Sheets service account credentials path",
    )
    oauth_client_id: Optional[str] = Field(
        default=None,
        validation_alias="GOOGLE_CLIENT_ID",
        description="Google OAuth client id (GOOGLE_CLIENT_ID)",
    )
    oauth_client_secret: Optional[str] = Field(
        default=None,
        validation_alias="GOOGLE_CLIENT_SECRET",
        description="Google OAuth client secret (GOOGLE_CLIENT_SECRET)",
    )
    oauth_redirect_uri: str = Field(
        default="http://localhost:8002/oauth/callback",
        validation_alias="GOOGLE_REDIRECT_URI",
        description="Google OAuth redirect URI (GOOGLE_REDIRECT_URI)",
    )

    @field_validator("google_sheets_api_key", mode="before")
    @classmethod
    def fallback_google_api_key(cls, v):  # noqa: ANN001
        if os.getenv("GOOGLE_SHEETS_API_KEY") not in (None, ""):
            return v
        fallback = (os.getenv("GOOGLE_API_KEY") or "").strip()
        return fallback or v
