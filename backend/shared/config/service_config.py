"""
🔥 THINK ULTRA! Service Configuration
Centralized configuration for all SPICE HARVESTER services

This module provides a single source of truth for service ports and URLs,
allowing flexible configuration through environment variables while
maintaining sensible defaults.
"""

import json
import logging
import os
from typing import List, Optional, Tuple

logger = logging.getLogger(__name__)


class ServiceConfig:
    """
    Centralized service configuration management.

    All port and URL configurations should be accessed through this class
    to ensure consistency across the entire SPICE HARVESTER system.
    """

    # Default ports - these match the expected values in tests and documentation
    DEFAULT_OMS_PORT = 8000
    DEFAULT_BFF_PORT = 8002
    DEFAULT_FUNNEL_PORT = 8003
    DEFAULT_AGENT_PORT = 8004
    DEFAULT_ELASTICSEARCH_PORT = 9200
    DEFAULT_LAKEFS_PORT = 8000

    @staticmethod
    def get_oms_port() -> int:
        """Get OMS (Ontology Management Service) port from environment or default."""
        return int(os.getenv("OMS_PORT", str(ServiceConfig.DEFAULT_OMS_PORT)))

    @staticmethod
    def get_bff_port() -> int:
        """Get BFF (Backend for Frontend) port from environment or default."""
        return int(os.getenv("BFF_PORT", str(ServiceConfig.DEFAULT_BFF_PORT)))

    @staticmethod
    def get_funnel_port() -> int:
        """Get Funnel service port from environment or default."""
        return int(os.getenv("FUNNEL_PORT", str(ServiceConfig.DEFAULT_FUNNEL_PORT)))

    @staticmethod
    def get_agent_port() -> int:
        """Get Agent service port from environment or default."""
        return int(os.getenv("AGENT_PORT", str(ServiceConfig.DEFAULT_AGENT_PORT)))

    @staticmethod
    def get_oms_host() -> str:
        """Get OMS host from environment or default."""
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        return os.getenv("OMS_HOST", "127.0.0.1")

    @staticmethod
    def get_bff_host() -> str:
        """Get BFF host from environment or default."""
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        return os.getenv("BFF_HOST", "127.0.0.1")

    @staticmethod
    def get_funnel_host() -> str:
        """Get Funnel host from environment or default."""
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        return os.getenv("FUNNEL_HOST", "127.0.0.1")

    @staticmethod
    def get_agent_host() -> str:
        """Get Agent host from environment or default."""
        return os.getenv("AGENT_HOST", "127.0.0.1")

    @staticmethod
    def get_oms_url() -> str:
        """
        Get complete OMS URL from environment or construct from host/port.

        Priority:
        1. OMS_BASE_URL environment variable (if set)
        2. Constructed from OMS_HOST and OMS_PORT
        3. Default: http://localhost:8000
        """
        if base_url := os.getenv("OMS_BASE_URL"):
            return base_url.rstrip("/")

        host = ServiceConfig.get_oms_host()
        port = ServiceConfig.get_oms_port()
        protocol = ServiceConfig.get_protocol()
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def get_bff_url() -> str:
        """
        Get complete BFF URL from environment or construct from host/port.

        Priority:
        1. BFF_BASE_URL environment variable (if set)
        2. Constructed from BFF_HOST and BFF_PORT
        3. Default: http://localhost:8002
        """
        if base_url := os.getenv("BFF_BASE_URL"):
            return base_url.rstrip("/")

        host = ServiceConfig.get_bff_host()
        port = ServiceConfig.get_bff_port()
        protocol = ServiceConfig.get_protocol()
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def get_funnel_url() -> str:
        """
        Get complete Funnel URL from environment or construct from host/port.

        Priority:
        1. FUNNEL_BASE_URL environment variable (if set)
        2. Constructed from FUNNEL_HOST and FUNNEL_PORT
        3. Default: http://localhost:8003
        """
        if base_url := os.getenv("FUNNEL_BASE_URL"):
            return base_url.rstrip("/")

        host = ServiceConfig.get_funnel_host()
        port = ServiceConfig.get_funnel_port()
        protocol = ServiceConfig.get_protocol()
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def get_agent_url() -> str:
        """
        Get complete Agent URL from environment or construct from host/port.

        Priority:
        1. AGENT_BASE_URL environment variable (if set)
        2. Constructed from AGENT_HOST and AGENT_PORT
        3. Default: http://localhost:8004
        """
        if base_url := os.getenv("AGENT_BASE_URL"):
            return base_url.rstrip("/")

        host = ServiceConfig.get_agent_host()
        port = ServiceConfig.get_agent_port()
        protocol = ServiceConfig.get_protocol()
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def get_terminus_url() -> str:
        """Get TerminusDB URL from environment or default."""
        if url := os.getenv("TERMINUS_SERVER_URL"):
            return url.rstrip("/")
        protocol = ServiceConfig.get_protocol()
        # Use Docker service names when running in container
        host = "terminusdb" if ServiceConfig.is_docker_environment() else "127.0.0.1"
        return f"{protocol}://{host}:6363"
    
    @staticmethod
    def get_postgres_url() -> str:
        """Get PostgreSQL connection URL from environment or default."""
        if url := os.getenv("POSTGRES_URL"):
            return url
        
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        host = os.getenv("POSTGRES_HOST", "postgres" if ServiceConfig.is_docker_environment() else "127.0.0.1")
        port = os.getenv("POSTGRES_PORT") or ("5432" if ServiceConfig.is_docker_environment() else "5433")
        user = os.getenv("POSTGRES_USER", "spiceadmin")
        password = os.getenv("POSTGRES_PASSWORD", "spicepass123")
        database = os.getenv("POSTGRES_DB", "spicedb")
        
        return f"postgresql://{user}:{password}@{host}:{port}/{database}"
    
    @staticmethod
    def get_kafka_bootstrap_servers() -> str:
        """Get Kafka bootstrap servers from environment or default."""
        if servers := os.getenv("KAFKA_BOOTSTRAP_SERVERS"):
            return servers
        
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        host = os.getenv("KAFKA_HOST", "kafka" if ServiceConfig.is_docker_environment() else "127.0.0.1")
        port = os.getenv(
            "KAFKA_PORT",
            "29092" if ServiceConfig.is_docker_environment() else "9092",
        )
        return f"{host}:{port}"
    
    @staticmethod
    def get_redis_host() -> str:
        """Get Redis host from environment or default."""
        # FIXED: Use 127.0.0.1 instead of localhost to avoid IPv6 issues
        return os.getenv("REDIS_HOST", "redis" if ServiceConfig.is_docker_environment() else "127.0.0.1")
    
    @staticmethod
    def get_redis_port() -> int:
        """Get Redis port from environment or default."""
        return int(os.getenv("REDIS_PORT", "6379"))
    
    @staticmethod
    def get_redis_url() -> str:
        """Get Redis connection URL from environment or construct from host/port."""
        if url := os.getenv("REDIS_URL"):
            return url
        host = ServiceConfig.get_redis_host()
        port = ServiceConfig.get_redis_port()
        password = os.getenv("REDIS_PASSWORD")
        if password is None and ServiceConfig.is_docker_environment():
            password = "spicepass123"
        password = (password or "").strip()
        if not password:
            return f"redis://{host}:{port}"
        return f"redis://:{password}@{host}:{port}"

    @staticmethod
    def get_elasticsearch_host() -> str:
        """Get Elasticsearch host from environment or default."""
        return os.getenv(
            "ELASTICSEARCH_HOST",
            "elasticsearch" if ServiceConfig.is_docker_environment() else "127.0.0.1",
        )

    @staticmethod
    def get_elasticsearch_port() -> int:
        """Get Elasticsearch port from environment or default."""
        return int(os.getenv("ELASTICSEARCH_PORT", str(ServiceConfig.DEFAULT_ELASTICSEARCH_PORT)))

    @staticmethod
    def get_elasticsearch_url() -> str:
        """Get Elasticsearch base URL from environment or construct from host/port."""
        if base_url := os.getenv("ELASTICSEARCH_URL"):
            return base_url.rstrip("/")
        host = ServiceConfig.get_elasticsearch_host()
        port = ServiceConfig.get_elasticsearch_port()
        return f"http://{host}:{port}"

    @staticmethod
    def is_docker_environment() -> bool:
        """
        Check if running in Docker environment.

        In Docker, services communicate using service names instead of localhost.
        """
        # FIXED: Check environment variable more carefully
        docker_env = os.getenv("DOCKER_CONTAINER", "").lower()
        if docker_env in ("false", "0", "no", "off"):
            return False
        return os.path.exists("/.dockerenv") or docker_env == "true"
    
    @staticmethod
    def get_minio_endpoint() -> str:
        """Get MinIO/S3 endpoint URL."""
        if url := os.getenv("MINIO_ENDPOINT_URL"):
            return url
        host = "spice-minio" if ServiceConfig.is_docker_environment() else "127.0.0.1"
        return f"http://{host}:9000"
    
    @staticmethod
    def get_minio_access_key() -> str:
        """Get MinIO/S3 access key."""
        return os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    
    @staticmethod
    def get_minio_secret_key() -> str:
        """Get MinIO/S3 secret key."""
        return os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    @staticmethod
    def get_lakefs_api_url() -> str:
        """
        Get lakeFS API base URL.

        Notes:
        - lakeFS commonly serves both the REST API and the S3 Gateway on the same port.
        - This returns the base server URL (without /api/v1).
        """
        if base_url := os.getenv("LAKEFS_API_URL"):
            return base_url.rstrip("/")
        host = "lakefs" if ServiceConfig.is_docker_environment() else "127.0.0.1"
        port = int(os.getenv("LAKEFS_API_PORT", str(ServiceConfig.DEFAULT_LAKEFS_PORT)))
        protocol = "http"
        return f"{protocol}://{host}:{port}"

    @staticmethod
    def get_lakefs_s3_endpoint() -> str:
        """
        Get lakeFS S3 Gateway endpoint URL.

        By default, this matches the lakeFS API host/port unless overridden.
        """
        if url := os.getenv("LAKEFS_S3_ENDPOINT_URL"):
            return url.rstrip("/")
        return ServiceConfig.get_lakefs_api_url()

    @staticmethod
    def get_service_url(service_name: str) -> str:
        """
        Get URL for a specific service by name.

        Args:
            service_name: Name of the service (oms, bff, funnel, agent)

        Returns:
            Service URL

        Raises:
            ValueError: If service name is not recognized
        """
        service_map = {
            "oms": ServiceConfig.get_oms_url,
            "bff": ServiceConfig.get_bff_url,
            "funnel": ServiceConfig.get_funnel_url,
            "agent": ServiceConfig.get_agent_url,
            "terminus": ServiceConfig.get_terminus_url,
        }

        if service_name.lower() not in service_map:
            raise ValueError(f"Unknown service: {service_name}")

        return service_map[service_name.lower()]()

    @staticmethod
    def get_all_service_urls() -> dict:
        """Get all service URLs as a dictionary."""
        return {
            "oms": ServiceConfig.get_oms_url(),
            "bff": ServiceConfig.get_bff_url(),
            "funnel": ServiceConfig.get_funnel_url(),
            "agent": ServiceConfig.get_agent_url(),
            "terminus": ServiceConfig.get_terminus_url(),
        }

    @staticmethod
    def validate_configuration() -> bool:
        """
        Validate that all required configuration is present.

        Returns:
            True if configuration is valid, False otherwise
        """
        required_vars = []

        # In production, you might want to require certain environment variables
        # For now, we accept defaults

        missing = [var for var in required_vars if not os.getenv(var)]

        if missing:
            print(f"Missing required environment variables: {', '.join(missing)}")
            return False

        return True

    # HTTPS/SSL Configuration Methods

    @staticmethod
    def use_https() -> bool:
        """
        Check if HTTPS should be used for service communication.

        Returns:
            True if HTTPS is enabled, False otherwise
        """
        return os.getenv("USE_HTTPS", "false").lower() in ("true", "1", "yes", "on")

    @staticmethod
    def is_production() -> bool:
        """Check if running in production environment."""
        return os.getenv("ENVIRONMENT", "development").lower() in ("production", "prod")

    @staticmethod
    def is_debug_endpoints_enabled() -> bool:
        """Enable opt-in debug endpoints (never on by default)."""
        return os.getenv("ENABLE_DEBUG_ENDPOINTS", "false").lower() in ("true", "1", "yes", "on")

    @staticmethod
    def get_ssl_cert_path() -> Optional[str]:
        """
        Get SSL certificate path from environment.

        Returns:
            Path to SSL certificate file or None if not configured
        """
        default_path = "./ssl/common/server.crt" if ServiceConfig.use_https() else None
        return os.getenv("SSL_CERT_PATH", default_path)

    @staticmethod
    def get_ssl_key_path() -> Optional[str]:
        """
        Get SSL key path from environment.

        Returns:
            Path to SSL key file or None if not configured
        """
        default_path = "./ssl/common/server.key" if ServiceConfig.use_https() else None
        return os.getenv("SSL_KEY_PATH", default_path)

    @staticmethod
    def get_ssl_ca_path() -> Optional[str]:
        """
        Get SSL CA certificate path from environment.

        Returns:
            Path to CA certificate file or None if not configured
        """
        default_path = "./ssl/ca.crt" if ServiceConfig.use_https() else None
        return os.getenv("SSL_CA_PATH", default_path)

    @staticmethod
    def verify_ssl() -> bool:
        """
        Check if SSL certificate verification should be enabled.

        In development, this is usually disabled for self-signed certificates.
        In production, this should always be True.

        Returns:
            True if SSL verification is enabled, False otherwise
        """
        if ServiceConfig.is_production():
            return True
        return os.getenv("VERIFY_SSL", "false").lower() in ("true", "1", "yes", "on")

    @staticmethod
    def get_protocol() -> str:
        """
        Get the protocol to use for service communication.

        Returns:
            "https" if HTTPS is enabled, "http" otherwise
        """
        return "https" if ServiceConfig.use_https() else "http"

    @staticmethod
    def get_ssl_config() -> dict:
        """
        Get complete SSL configuration as a dictionary.

        Returns:
            Dictionary with SSL configuration suitable for uvicorn
        """
        if not ServiceConfig.use_https():
            return {}

        config = {}

        if cert_path := ServiceConfig.get_ssl_cert_path():
            if os.path.exists(cert_path):
                config["ssl_certfile"] = cert_path
            else:
                logger.warning(f"SSL certificate not found at {cert_path}")

        if key_path := ServiceConfig.get_ssl_key_path():
            if os.path.exists(key_path):
                config["ssl_keyfile"] = key_path
            else:
                logger.warning(f"SSL key not found at {key_path}")

        return config

    @staticmethod
    def get_client_ssl_config() -> dict:
        """
        Get SSL configuration for HTTP clients (requests, httpx).

        Returns:
            Dictionary with SSL configuration for clients
        """
        config = {"verify": ServiceConfig.verify_ssl()}

        # In production with custom CA, specify the CA certificate
        if ServiceConfig.verify_ssl() and (ca_path := ServiceConfig.get_ssl_ca_path()):
            if os.path.exists(ca_path):
                config["verify"] = ca_path

        return config

    # CORS Configuration Methods

    @staticmethod
    def get_cors_origins() -> List[str]:
        """
        Get CORS allowed origins from environment variables.

        Priority:
        1. CORS_ORIGINS environment variable (JSON array)
        2. Development defaults if in development
        3. Production defaults if in production

        Returns:
            List of allowed origins
        """
        cors_origins_env = os.getenv("CORS_ORIGINS")

        if cors_origins_env:
            try:
                origins = json.loads(cors_origins_env)
                if isinstance(origins, list):
                    # 🔥 BUG FIX: 빈 리스트가 설정된 경우 처리
                    if len(origins) == 0:
                        logger.warning("CORS_ORIGINS is empty. Using environment defaults.")
                        # 빈 리스트라도 환경별 기본값 사용
                        return ServiceConfig._get_environment_default_origins()

                    # 🔥 SECURITY FIX: 프로덕션 환경에서 와일드카드 차단
                    if ServiceConfig.is_production():
                        filtered_origins = []
                        for origin in origins:
                            if origin == "*":
                                logger.error(
                                    "SECURITY WARNING: Wildcard (*) CORS origin is not allowed in production!"
                                )
                                logger.error("Using production defaults instead.")
                                continue
                            filtered_origins.append(origin)

                        # 와일드카드가 필터링된 후 빈 리스트가 되면 기본값 사용
                        if len(filtered_origins) == 0:
                            logger.warning(
                                "All CORS origins were invalid in production. Using defaults."
                            )
                            return ServiceConfig._get_environment_default_origins()

                        return filtered_origins

                    return origins
                else:
                    logger.warning(f"CORS_ORIGINS must be a JSON array, got: {type(origins)}")
            except json.JSONDecodeError as e:
                logger.warning(f"Invalid CORS_ORIGINS JSON format: {e}")

        # Environment-based defaults
        return ServiceConfig._get_environment_default_origins()

    @staticmethod
    def _get_environment_default_origins() -> List[str]:
        """
        Get environment-based default CORS origins.

        Returns:
            List of default origins based on environment
        """
        if ServiceConfig.is_production():
            # Production: Only allow specific domains
            return [
                "https://app.spice-harvester.com",
                "https://www.spice-harvester.com",
                "https://spice-harvester.com",
            ]
        else:
            # Development: Allow common frontend development ports
            return ServiceConfig._get_dev_cors_origins()

    @staticmethod
    def _get_dev_cors_origins() -> List[str]:
        """
        Get development CORS origins for common frontend ports.

        Returns:
            List of development origins
        """
        # Common frontend development ports
        common_ports = [3000, 3001, 3002, 5173, 5174, 8080, 8081, 8082, 4200, 4201]
        origins = []

        for port in common_ports:
            origins.extend(
                [
                    f"http://localhost:{port}",
                    f"http://127.0.0.1:{port}",
                    f"https://localhost:{port}",
                    f"https://127.0.0.1:{port}",
                ]
            )

        # Add wildcard for maximum flexibility in development
        origins.append("*")

        return origins

    @staticmethod
    def get_cors_config() -> dict:
        """
        Get complete CORS configuration for FastAPI middleware.

        Returns:
            Dictionary with CORS configuration
        """
        origins = ServiceConfig.get_cors_origins()

        # Base configuration
        config = {
            "allow_origins": origins,
            "allow_methods": ["GET", "POST", "PUT", "DELETE", "OPTIONS", "PATCH"],
            "allow_headers": ["*"],
            "expose_headers": ["*"],
            "allow_credentials": True,
            "max_age": 3600,  # 1 hour
        }

        # Production-specific adjustments
        if ServiceConfig.is_production():
            # More restrictive headers in production
            config["allow_headers"] = [
                "Accept",
                "Accept-Language",
                "Authorization",
                "Content-Type",
                "DNT",
                "Origin",
                "User-Agent",
                "X-Requested-With",
            ]
            # Specific exposed headers
            config["expose_headers"] = ["Content-Length", "Content-Type", "X-Request-ID"]
            # Longer cache for production
            config["max_age"] = 86400  # 24 hours

        return config

    @staticmethod
    def is_cors_enabled() -> bool:
        """
        Check if CORS should be enabled.

        Returns:
            True if CORS is enabled, False otherwise
        """
        return os.getenv("CORS_ENABLED", "true").lower() in ("true", "1", "yes", "on")

    @staticmethod
    def get_cors_debug_info() -> dict:
        """
        Get CORS configuration debug information.

        Returns:
            Dictionary with debug information
        """
        return {
            "enabled": ServiceConfig.is_cors_enabled(),
            "origins": ServiceConfig.get_cors_origins(),
            "environment": os.getenv("ENVIRONMENT", "development"),
            "is_production": ServiceConfig.is_production(),
            "config": ServiceConfig.get_cors_config(),
        }


# Convenience functions for backward compatibility
def get_oms_url() -> str:
    """Get OMS URL - convenience function."""
    return ServiceConfig.get_oms_url()


def get_bff_url() -> str:
    """Get BFF URL - convenience function."""
    return ServiceConfig.get_bff_url()


def get_funnel_url() -> str:
    """Get Funnel URL - convenience function."""
    return ServiceConfig.get_funnel_url()


def get_agent_url() -> str:
    """Get Agent URL - convenience function."""
    return ServiceConfig.get_agent_url()


if __name__ == "__main__":
    # Print current configuration when run directly
    print("🔥 SPICE HARVESTER Service Configuration")
    print("=" * 50)
    print(f"Environment: {os.getenv('ENVIRONMENT', 'development')}")
    print(f"Protocol: {ServiceConfig.get_protocol()}")
    print(f"HTTPS Enabled: {ServiceConfig.use_https()}")
    print(f"SSL Verification: {ServiceConfig.verify_ssl()}")
    print("=" * 50)
    print(f"OMS Port: {ServiceConfig.get_oms_port()}")
    print(f"OMS URL: {ServiceConfig.get_oms_url()}")
    print(f"BFF Port: {ServiceConfig.get_bff_port()}")
    print(f"BFF URL: {ServiceConfig.get_bff_url()}")
    print(f"Funnel Port: {ServiceConfig.get_funnel_port()}")
    print(f"Funnel URL: {ServiceConfig.get_funnel_url()}")
    print(f"Agent Port: {ServiceConfig.get_agent_port()}")
    print(f"Agent URL: {ServiceConfig.get_agent_url()}")
    print(f"TerminusDB URL: {ServiceConfig.get_terminus_url()}")
    print(f"Docker Environment: {ServiceConfig.is_docker_environment()}")
    print("=" * 50)
    if ServiceConfig.use_https():
        print("SSL Configuration:")
        print(f"  Certificate: {ServiceConfig.get_ssl_cert_path()}")
        print(f"  Key: {ServiceConfig.get_ssl_key_path()}")
        print(f"  CA: {ServiceConfig.get_ssl_ca_path()}")
        print("=" * 50)
    print("Configuration Valid:", ServiceConfig.validate_configuration())
