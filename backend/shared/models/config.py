"""
Configuration models for SPICE HARVESTER
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Dict, Optional


@dataclass
class ConnectionConfig:
    """Database connection configuration"""

    server_url: str
    user: str
    key: str
    account: str
    timeout: int = 30
    database: Optional[str] = None
    retries: int = 3
    retry_delay: float = 1.0
    ssl_verify: bool = True
    headers: Dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        """Post-initialization validation"""
        if not self.server_url:
            raise ValueError("server_url is required")
        if not self.user:
            raise ValueError("user is required")
        if not self.key:
            raise ValueError("key is required")
        if not self.account:
            raise ValueError("account is required")
        if self.timeout <= 0:
            raise ValueError("timeout must be positive")
        if self.retries < 0:
            raise ValueError("retries must be non-negative")
        if self.retry_delay < 0:
            raise ValueError("retry_delay must be non-negative")

    @classmethod
    def from_env(cls) -> "ConnectionConfig":
        """Create ConnectionConfig from environment variables"""
        import os

        # Get environment variables with defaults
        server_url = os.getenv("TERMINUS_SERVER_URL", "http://terminusdb:6363")
        user = os.getenv("TERMINUS_USER", "admin")
        key = os.getenv("TERMINUS_KEY", os.getenv("TERMINUSDB_ADMIN_PASS", "admin123"))
        account = os.getenv("TERMINUS_ACCOUNT", "admin")
        timeout = int(os.getenv("TERMINUS_TIMEOUT", "30"))
        database = os.getenv("TERMINUS_DATABASE")
        retries = int(os.getenv("TERMINUS_RETRY_ATTEMPTS", "3"))
        retry_delay = float(os.getenv("TERMINUS_RETRY_DELAY", "1.0"))
        ssl_verify = os.getenv("TERMINUS_SSL_VERIFY", "true").lower() == "true"

        return cls(
            server_url=server_url,
            user=user,
            key=key,
            account=account,
            timeout=timeout,
            database=database,
            retries=retries,
            retry_delay=retry_delay,
            ssl_verify=ssl_verify,
        )

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "server_url": self.server_url,
            "user": self.user,
            "key": self.key,
            "account": self.account,
            "timeout": self.timeout,
            "database": self.database,
            "retries": self.retries,
            "retry_delay": self.retry_delay,
            "ssl_verify": self.ssl_verify,
            "headers": self.headers,
        }


@dataclass
class AsyncConnectionInfo:
    """Async connection information"""

    config: ConnectionConfig
    connected_at: Optional[datetime] = None
    last_used: Optional[datetime] = None
    active_connections: int = 0
    max_connections: int = 10

    def __post_init__(self) -> None:
        """Post-initialization setup"""
        if self.connected_at is None:
            self.connected_at = datetime.now()
        if self.last_used is None:
            self.last_used = datetime.now()

    def mark_used(self) -> None:
        """Mark connection as used"""
        self.last_used = datetime.now()

    def can_create_connection(self) -> bool:
        """Check if new connection can be created"""
        return self.active_connections < self.max_connections

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "config": self.config.to_dict(),
            "connected_at": self.connected_at.isoformat() if self.connected_at else None,
            "last_used": self.last_used.isoformat() if self.last_used else None,
            "active_connections": self.active_connections,
            "max_connections": self.max_connections,
        }
