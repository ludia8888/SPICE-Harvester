"""
Rate Limiting Configuration
Based on Context7 recommendations for API security

This module defines rate limiting rules for different endpoints and user types.
"""

from typing import Dict, Any, Optional
from enum import Enum
from pydantic import BaseModel, Field


class RateLimitStrategy(str, Enum):
    """Rate limiting identification strategy"""
    IP = "ip"
    USER = "user"
    API_KEY = "api_key"
    COMBINED = "combined"  # IP + User


class EndpointCategory(str, Enum):
    """Categories of endpoints for rate limiting"""
    PUBLIC_READ = "public_read"
    PUBLIC_WRITE = "public_write"
    AUTHENTICATED_READ = "authenticated_read"
    AUTHENTICATED_WRITE = "authenticated_write"
    ADMIN = "admin"
    SEARCH = "search"
    BULK = "bulk"
    WEBHOOK = "webhook"


class RateLimitRule(BaseModel):
    """Rate limit rule configuration"""
    requests: int = Field(description="Number of requests allowed")
    window: int = Field(description="Time window in seconds")
    strategy: RateLimitStrategy = Field(default=RateLimitStrategy.IP)
    cost: int = Field(default=1, description="Request cost in tokens")
    burst_allowed: bool = Field(default=False)
    error_message: Optional[str] = None


class RateLimitConfig:
    """
    Centralized rate limiting configuration
    Based on Context7 analysis and best practices
    """
    
    # Default limits by endpoint category
    CATEGORY_LIMITS: Dict[EndpointCategory, RateLimitRule] = {
        EndpointCategory.PUBLIC_READ: RateLimitRule(
            requests=100,
            window=60,
            strategy=RateLimitStrategy.IP,
            error_message="Public read rate limit exceeded"
        ),
        EndpointCategory.PUBLIC_WRITE: RateLimitRule(
            requests=10,
            window=60,
            strategy=RateLimitStrategy.IP,
            cost=5,
            error_message="Public write rate limit exceeded"
        ),
        EndpointCategory.AUTHENTICATED_READ: RateLimitRule(
            requests=300,
            window=60,
            strategy=RateLimitStrategy.USER,
            error_message="Authenticated read rate limit exceeded"
        ),
        EndpointCategory.AUTHENTICATED_WRITE: RateLimitRule(
            requests=60,
            window=60,
            strategy=RateLimitStrategy.USER,
            cost=3,
            error_message="Authenticated write rate limit exceeded"
        ),
        EndpointCategory.ADMIN: RateLimitRule(
            requests=1000,
            window=60,
            strategy=RateLimitStrategy.API_KEY,
            error_message="Admin rate limit exceeded"
        ),
        EndpointCategory.SEARCH: RateLimitRule(
            requests=30,
            window=60,
            strategy=RateLimitStrategy.IP,
            cost=2,
            error_message="Search rate limit exceeded"
        ),
        EndpointCategory.BULK: RateLimitRule(
            requests=5,
            window=60,
            strategy=RateLimitStrategy.USER,
            cost=10,
            error_message="Bulk operation rate limit exceeded"
        ),
        EndpointCategory.WEBHOOK: RateLimitRule(
            requests=100,
            window=1,
            strategy=RateLimitStrategy.IP,
            burst_allowed=True,
            error_message="Webhook rate limit exceeded"
        ),
    }
    
    # Specific endpoint overrides
    ENDPOINT_LIMITS: Dict[str, RateLimitRule] = {
        # OMS endpoints
        "/api/v1/database/*/ontology/*": RateLimitRule(
            requests=200,
            window=60,
            strategy=RateLimitStrategy.IP
        ),
        "/api/v1/database/*/ontologies": RateLimitRule(
            requests=100,
            window=60,
            strategy=RateLimitStrategy.IP
        ),
        "/api/v1/database/*/create": RateLimitRule(
            requests=5,
            window=60,
            strategy=RateLimitStrategy.USER,
            cost=10
        ),
        "/api/v1/database/*/delete": RateLimitRule(
            requests=3,
            window=60,
            strategy=RateLimitStrategy.USER,
            cost=20
        ),
        
        # BFF endpoints
        "/api/v1/suggest-schema": RateLimitRule(
            requests=20,
            window=60,
            strategy=RateLimitStrategy.IP,
            cost=5
        ),
        "/api/v1/query": RateLimitRule(
            requests=50,
            window=60,
            strategy=RateLimitStrategy.IP,
            cost=2
        ),
        "/api/v1/instances/bulk": RateLimitRule(
            requests=10,
            window=60,
            strategy=RateLimitStrategy.USER,
            cost=10
        ),
        
        # Health endpoints (more relaxed)
        "/health": RateLimitRule(
            requests=600,
            window=60,
            strategy=RateLimitStrategy.IP
        ),
        "/api/v1/health/*": RateLimitRule(
            requests=600,
            window=60,
            strategy=RateLimitStrategy.IP
        ),
    }
    
    # User tier multipliers
    USER_TIER_MULTIPLIERS: Dict[str, float] = {
        "free": 1.0,
        "basic": 2.0,
        "pro": 5.0,
        "enterprise": 10.0,
        "unlimited": float('inf')
    }
    
    # IP whitelist (no rate limiting)
    IP_WHITELIST = [
        "127.0.0.1",
        "localhost",
        "::1",
        # Add trusted IPs here
    ]
    
    # API key tiers
    API_KEY_TIERS: Dict[str, Dict[str, Any]] = {
        "internal": {
            "multiplier": 100.0,
            "description": "Internal service communication"
        },
        "partner": {
            "multiplier": 5.0,
            "description": "Partner API access"
        },
        "standard": {
            "multiplier": 1.0,
            "description": "Standard API access"
        },
    }
    
    @classmethod
    def get_endpoint_rule(cls, endpoint: str, method: str = "GET") -> Optional[RateLimitRule]:
        """
        Get rate limit rule for a specific endpoint
        
        Args:
            endpoint: The API endpoint path
            method: HTTP method
            
        Returns:
            Rate limit rule or None if no specific rule
        """
        # Check exact match first
        if endpoint in cls.ENDPOINT_LIMITS:
            return cls.ENDPOINT_LIMITS[endpoint]
        
        # Check pattern matches
        for pattern, rule in cls.ENDPOINT_LIMITS.items():
            if cls._match_pattern(endpoint, pattern):
                return rule
        
        # Fall back to category defaults based on method
        if method in ["GET", "HEAD", "OPTIONS"]:
            return cls.CATEGORY_LIMITS[EndpointCategory.PUBLIC_READ]
        elif method in ["POST", "PUT", "PATCH", "DELETE"]:
            return cls.CATEGORY_LIMITS[EndpointCategory.PUBLIC_WRITE]
        
        return None
    
    @staticmethod
    def _match_pattern(endpoint: str, pattern: str) -> bool:
        """
        Check if endpoint matches a pattern with wildcards
        
        Args:
            endpoint: The actual endpoint
            pattern: Pattern with * wildcards
            
        Returns:
            True if matches
        """
        import re
        # Convert pattern to regex
        regex_pattern = pattern.replace("*", ".*")
        regex_pattern = f"^{regex_pattern}$"
        return bool(re.match(regex_pattern, endpoint))
    
    @classmethod
    def get_user_limit(cls, base_rule: RateLimitRule, user_tier: str) -> RateLimitRule:
        """
        Adjust rate limit based on user tier
        
        Args:
            base_rule: Base rate limit rule
            user_tier: User's subscription tier
            
        Returns:
            Adjusted rate limit rule
        """
        multiplier = cls.USER_TIER_MULTIPLIERS.get(user_tier, 1.0)
        
        if multiplier == float('inf'):
            # Unlimited tier - very high limits
            return RateLimitRule(
                requests=1000000,
                window=base_rule.window,
                strategy=base_rule.strategy,
                cost=1  # Minimal cost for unlimited users
            )
        
        return RateLimitRule(
            requests=int(base_rule.requests * multiplier),
            window=base_rule.window,
            strategy=base_rule.strategy,
            cost=base_rule.cost,
            burst_allowed=base_rule.burst_allowed,
            error_message=base_rule.error_message
        )
    
    @classmethod
    def is_whitelisted(cls, ip: str) -> bool:
        """
        Check if IP is whitelisted
        
        Args:
            ip: IP address to check
            
        Returns:
            True if whitelisted
        """
        return ip in cls.IP_WHITELIST


# Export for easy access
rate_limit_config = RateLimitConfig()
