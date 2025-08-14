"""
Centralized Configuration System for SPICE HARVESTER

This module provides a type-safe, centralized configuration system using Pydantic Settings
to replace scattered environment variable loading and eliminate anti-pattern 13.

Features:
- Type-safe configuration with validation
- Environment variable binding with defaults
- Hierarchical configuration structure
- Single source of truth for all settings
- Test-friendly configuration isolation
"""

import os
from typing import Optional, List, Dict, Any
from pydantic import Field, validator, root_validator, ConfigDict
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum

class Environment(str, Enum):
    """Application environment types"""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TEST = "test"

class DatabaseSettings(BaseSettings):
    """Database configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # TerminusDB Configuration
    terminus_url: str = Field(
        default="http://localhost:6363",
        description="TerminusDB server URL"
    )
    terminus_user: str = Field(
        default="anonymous",
        description="TerminusDB username"
    )
    terminus_password: str = Field(
        default="admin",
        description="TerminusDB password/key"
    )
    terminus_account: str = Field(
        default="admin",
        description="TerminusDB account"
    )
    
    @validator('terminus_url', pre=True, always=True)
    def get_terminus_url(cls, v):
        return os.getenv("TERMINUS_SERVER_URL", v or "http://localhost:6363")
    
    @validator('terminus_user', pre=True, always=True)
    def get_terminus_user(cls, v):
        return os.getenv("TERMINUS_USER", v or "anonymous")
        
    @validator('terminus_password', pre=True, always=True)
    def get_terminus_password(cls, v):
        return os.getenv("TERMINUS_KEY", v or "admin")
        
    @validator('terminus_account', pre=True, always=True)
    def get_terminus_account(cls, v):
        return os.getenv("TERMINUS_ACCOUNT", v or "admin")
    terminus_timeout: int = Field(
        default=30,
        env="TERMINUS_TIMEOUT",
        description="TerminusDB connection timeout in seconds"
    )
    terminus_retry_attempts: int = Field(
        default=3,
        env="TERMINUS_RETRY_ATTEMPTS",
        description="TerminusDB retry attempts"
    )
    terminus_retry_delay: float = Field(
        default=1.0,
        env="TERMINUS_RETRY_DELAY",
        description="TerminusDB retry delay in seconds"
    )
    terminus_ssl_verify: bool = Field(
        default=True,
        env="TERMINUS_SSL_VERIFY",
        description="Verify SSL certificates for TerminusDB"
    )
    terminus_use_ssl: bool = Field(
        default=False,
        env="TERMINUS_USE_SSL",
        description="Use SSL for TerminusDB connections"
    )
    
    # PostgreSQL Configuration
    postgres_host: str = Field(
        default="localhost",
        env="POSTGRES_HOST",
        description="PostgreSQL host"
    )
    postgres_port: int = Field(
        default=5432,
        env="POSTGRES_PORT",
        description="PostgreSQL port"
    )
    postgres_user: str = Field(
        default="spiceadmin",
        env="POSTGRES_USER",
        description="PostgreSQL username"
    )
    postgres_password: str = Field(
        default="spicepass123",
        env="POSTGRES_PASSWORD",
        description="PostgreSQL password"
    )
    postgres_db: str = Field(
        default="spicedb",
        env="POSTGRES_DB",
        description="PostgreSQL database name"
    )
    
    # Redis Configuration
    redis_host: str = Field(
        default="localhost",
        env="REDIS_HOST",
        description="Redis host"
    )
    redis_port: int = Field(
        default=6379,
        env="REDIS_PORT",
        description="Redis port"
    )
    redis_password: Optional[str] = Field(
        default="spice123!",
        env="REDIS_PASSWORD",
        description="Redis password"
    )
    
    # Elasticsearch Configuration
    elasticsearch_host: str = Field(
        default="localhost",
        env="ELASTICSEARCH_HOST",
        description="Elasticsearch host"
    )
    elasticsearch_port: int = Field(
        default=9200,
        env="ELASTICSEARCH_PORT",
        description="Elasticsearch port"
    )
    elasticsearch_username: Optional[str] = Field(
        default="elastic",
        env="ELASTICSEARCH_USERNAME",
        description="Elasticsearch username"
    )
    elasticsearch_password: Optional[str] = Field(
        default="spice123!",
        env="ELASTICSEARCH_PASSWORD",
        description="Elasticsearch password"
    )
    
    # Kafka Configuration
    kafka_host: str = Field(
        default="localhost",
        env="KAFKA_HOST",
        description="Kafka host"
    )
    kafka_port: int = Field(
        default=9092,
        env="KAFKA_PORT",
        description="Kafka port"
    )
    kafka_bootstrap_servers: Optional[str] = Field(
        default=None,
        env="KAFKA_BOOTSTRAP_SERVERS",
        description="Kafka bootstrap servers (overrides host:port)"
    )
    
    @property
    def postgres_url(self) -> str:
        """Construct PostgreSQL connection URL"""
        return f"postgresql://{self.postgres_user}:{self.postgres_password}@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
    
    @property
    def kafka_servers(self) -> str:
        """Get Kafka bootstrap servers"""
        if self.kafka_bootstrap_servers:
            return self.kafka_bootstrap_servers
        return f"{self.kafka_host}:{self.kafka_port}"
    
    @property
    def elasticsearch_url(self) -> str:
        """Construct Elasticsearch URL with authentication"""
        if self.elasticsearch_username and self.elasticsearch_password:
            return f"http://{self.elasticsearch_username}:{self.elasticsearch_password}@{self.elasticsearch_host}:{self.elasticsearch_port}"
        return f"http://{self.elasticsearch_host}:{self.elasticsearch_port}"
    
    @property
    def redis_url(self) -> str:
        """Construct Redis URL"""
        # If Redis password is empty, don't include auth
        if not self.redis_password:
            return f"redis://{self.redis_host}:{self.redis_port}"
        return f"redis://:{self.redis_password}@{self.redis_host}:{self.redis_port}"

class ServiceSettings(BaseSettings):
    """Service configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Service Hosts and Ports
    oms_host: str = Field(
        default="localhost",
        env="OMS_HOST",
        description="OMS service host"
    )
    oms_port: int = Field(
        default=8000,
        env="OMS_PORT",
        description="OMS service port"
    )
    bff_host: str = Field(
        default="localhost",
        env="BFF_HOST",
        description="BFF service host"
    )
    bff_port: int = Field(
        default=8002,
        env="BFF_PORT",
        description="BFF service port"
    )
    funnel_host: str = Field(
        default="localhost",
        env="FUNNEL_HOST",
        description="Funnel service host"
    )
    funnel_port: int = Field(
        default=8003,
        env="FUNNEL_PORT",
        description="Funnel service port"
    )
    
    # SSL Configuration
    use_https: bool = Field(
        default=False,
        env="USE_HTTPS",
        description="Use HTTPS for service communication"
    )
    ssl_cert_path: str = Field(
        default="./ssl/common/server.crt",
        env="SSL_CERT_PATH",
        description="SSL certificate path"
    )
    ssl_key_path: str = Field(
        default="./ssl/common/server.key",
        env="SSL_KEY_PATH",
        description="SSL private key path"
    )
    ssl_ca_path: str = Field(
        default="./ssl/ca.crt",
        env="SSL_CA_PATH",
        description="SSL CA certificate path"
    )
    verify_ssl: bool = Field(
        default=False,
        env="VERIFY_SSL",
        description="Verify SSL certificates"
    )
    
    # CORS Configuration
    cors_enabled: bool = Field(
        default=True,
        env="CORS_ENABLED",
        description="Enable CORS"
    )
    cors_origins: str = Field(
        default='["http://localhost:3000", "http://localhost:3001", "http://localhost:8080"]',
        env="CORS_ORIGINS",
        description="CORS allowed origins (JSON array string)"
    )
    
    @property
    def oms_base_url(self) -> str:
        """Construct OMS base URL"""
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.oms_host}:{self.oms_port}"
    
    @property
    def bff_base_url(self) -> str:
        """Construct BFF base URL"""
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.bff_host}:{self.bff_port}"
    
    @property
    def funnel_base_url(self) -> str:
        """Construct Funnel base URL"""
        protocol = "https" if self.use_https else "http"
        return f"{protocol}://{self.funnel_host}:{self.funnel_port}"
    
    @property
    def cors_origins_list(self) -> List[str]:
        """Parse CORS origins from JSON string"""
        try:
            import json
            return json.loads(self.cors_origins)
        except (json.JSONDecodeError, TypeError):
            return ["*"]  # Fallback to allow all origins

class StorageSettings(BaseSettings):
    """Storage configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # MinIO/S3 Configuration
    minio_endpoint_url: str = Field(
        default="http://localhost:9000",
        env="MINIO_ENDPOINT_URL",
        description="MinIO/S3 endpoint URL"
    )
    minio_access_key: str = Field(
        default="admin",
        env="MINIO_ACCESS_KEY",
        description="MinIO/S3 access key"
    )
    minio_secret_key: str = Field(
        default="spice123!",
        env="MINIO_SECRET_KEY",
        description="MinIO/S3 secret key"
    )
    
    # S3 Buckets
    instance_bucket: str = Field(
        default="instance-events",
        env="INSTANCE_BUCKET",
        description="S3 bucket for instance events"
    )
    
    @property
    def use_ssl(self) -> bool:
        """Determine if SSL should be used based on endpoint URL"""
        return self.minio_endpoint_url.startswith("https://")


class CacheSettings(BaseSettings):
    """Cache and TTL configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Cache TTL Settings (in seconds)
    class_label_cache_ttl: int = Field(
        default=3600,
        env="CLASS_LABEL_CACHE_TTL",
        description="Class label cache TTL in seconds"
    )
    command_status_cache_ttl: int = Field(
        default=300,
        env="COMMAND_STATUS_CACHE_TTL",
        description="Command status cache TTL in seconds"
    )
    mapping_cache_ttl: int = Field(
        default=1800,
        env="MAPPING_CACHE_TTL",
        description="Mapping cache TTL in seconds"
    )


class SecuritySettings(BaseSettings):
    """Security configuration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # JWT Configuration
    secret_key: str = Field(
        default="your-secret-key-change-this-in-production",
        env="SECRET_KEY",
        description="JWT secret key"
    )
    algorithm: str = Field(
        default="HS256",
        env="JWT_ALGORITHM",
        description="JWT algorithm"
    )
    access_token_expire_minutes: int = Field(
        default=30,
        env="ACCESS_TOKEN_EXPIRE_MINUTES", 
        description="JWT access token expiry in minutes"
    )


class PerformanceSettings(BaseSettings):
    """Performance and optimization settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Connection Pool Settings
    database_pool_size: int = Field(
        default=20,
        env="DATABASE_POOL_SIZE",
        description="Database connection pool size"
    )
    database_max_overflow: int = Field(
        default=30,
        env="DATABASE_MAX_OVERFLOW",
        description="Database max overflow connections"
    )
    
    # Rate Limiting
    enable_rate_limiting: bool = Field(
        default=True,
        env="ENABLE_RATE_LIMITING",
        description="Enable API rate limiting"
    )
    requests_per_minute: int = Field(
        default=60,
        env="REQUESTS_PER_MINUTE",
        description="Requests per minute limit"
    )


class TestSettings(BaseSettings):
    """Test environment configuration"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    test_database_url: str = Field(
        default="sqlite:///./test.db",
        env="TEST_DATABASE_URL",
        description="Test database URL"
    )
    test_timeout: int = Field(
        default=30,
        env="TEST_TIMEOUT",
        description="Test timeout in seconds"
    )


class GoogleSheetsSettings(BaseSettings):
    """Google Sheets integration settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    google_sheets_api_key: Optional[str] = Field(
        default=None,
        env="GOOGLE_SHEETS_API_KEY",
        description="Google Sheets API key"
    )
    google_sheets_credentials_path: Optional[str] = Field(
        default=None,
        env="GOOGLE_SHEETS_CREDENTIALS_PATH",
        description="Google Sheets service account credentials path"
    )


class ApplicationSettings(BaseSettings):
    """Main application settings - aggregates all other settings"""
    
    model_config = SettingsConfigDict(
        env_file=".env" if not os.getenv("DOCKER_CONTAINER") else None,
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Environment and basic settings
    environment: Environment = Field(
        default=Environment.DEVELOPMENT,
        env="ENVIRONMENT",
        description="Application environment"
    )
    debug: bool = Field(
        default=True,
        env="DEBUG",
        description="Enable debug mode"
    )
    
    # Nested settings
    database: DatabaseSettings = DatabaseSettings()
    services: ServiceSettings = ServiceSettings()
    storage: StorageSettings = StorageSettings()
    cache: CacheSettings = CacheSettings()
    security: SecuritySettings = SecuritySettings()
    performance: PerformanceSettings = PerformanceSettings()
    test: TestSettings = TestSettings()
    google_sheets: GoogleSheetsSettings = GoogleSheetsSettings()
    
    @property
    def is_development(self) -> bool:
        """Check if running in development mode"""
        return self.environment == Environment.DEVELOPMENT
    
    @property
    def is_production(self) -> bool:
        """Check if running in production mode"""
        return self.environment == Environment.PRODUCTION
    
    @property
    def is_test(self) -> bool:
        """Check if running in test mode"""
        return self.environment == Environment.TEST


# This replaces all scattered ServiceConfig() and AppConfig() instantiations
settings = ApplicationSettings()

def get_settings() -> ApplicationSettings:
    """
    Get the global settings instance
    
    This function provides access to the centralized configuration
    and can be used with FastAPI's Depends() for dependency injection.
    
    Returns:
        ApplicationSettings: The global settings instance
    """
    return settings

def reload_settings() -> ApplicationSettings:
    """
    Reload settings from environment (useful for testing)
    
    Returns:
        ApplicationSettings: New settings instance with reloaded values
    """
    global settings
    settings = ApplicationSettings()
    return settings