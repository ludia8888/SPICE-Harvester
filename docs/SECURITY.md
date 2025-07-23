# SPICE HARVESTER Security Documentation

## Table of Contents

1. [Security Overview](#security-overview)
2. [Threat Model](#threat-model)
3. [Authentication and Authorization](#authentication-and-authorization)
4. [Input Validation and Sanitization](#input-validation-and-sanitization)
5. [Data Protection](#data-protection)
6. [Network Security](#network-security)
7. [Application Security](#application-security)
8. [Security Monitoring and Logging](#security-monitoring-and-logging)
9. [Incident Response](#incident-response)
10. [Compliance and Standards](#compliance-and-standards)
11. [Security Testing](#security-testing)
12. [Security Best Practices](#security-best-practices)

## Security Overview

### Security Architecture

SPICE HARVESTER implements a defense-in-depth security strategy with multiple layers of protection:

1. **Network Layer**: Firewall rules, DDoS protection, SSL/TLS encryption
2. **Application Layer**: Input validation, authentication, authorization
3. **Data Layer**: Encryption at rest, access controls, audit logging
4. **Infrastructure Layer**: Container security, OS hardening, patch management

### Security Principles

1. **Least Privilege**: Users and services have minimum required permissions
2. **Zero Trust**: No implicit trust; verify all requests
3. **Defense in Depth**: Multiple security layers
4. **Fail Secure**: System fails to a secure state
5. **Security by Design**: Security built into architecture

## Threat Model

### Identified Threats

#### 1. Injection Attacks

**Threat**: SQL/NoSQL injection, command injection, XSS
**Impact**: Data breach, unauthorized access, system compromise
**Likelihood**: High
**Mitigation**:
- Parameterized queries
- Input sanitization
- Output encoding
- Content Security Policy

#### 2. Authentication Bypass

**Threat**: Weak authentication, session hijacking, credential stuffing
**Impact**: Unauthorized access, data manipulation
**Likelihood**: Medium
**Mitigation**:
- Strong password policies
- Multi-factor authentication
- Session management
- Rate limiting

#### 3. Data Exposure

**Threat**: Sensitive data exposure, information leakage
**Impact**: Privacy violation, regulatory penalties
**Likelihood**: Medium
**Mitigation**:
- Encryption at rest and in transit
- Data classification
- Access controls
- Data minimization

#### 4. Denial of Service

**Threat**: Resource exhaustion, amplification attacks
**Impact**: Service unavailability, revenue loss
**Likelihood**: Medium
**Mitigation**:
- Rate limiting
- Resource quotas
- DDoS protection
- Circuit breakers

### Risk Matrix

| Threat | Likelihood | Impact | Risk Level | Priority |
|--------|------------|--------|------------|----------|
| Injection attacks | High | Critical | High | P1 |
| Data breach | Medium | Critical | High | P1 |
| DoS attacks | Medium | High | Medium | P2 |
| Authentication bypass | Low | High | Medium | P2 |
| Insider threat | Low | Critical | Medium | P2 |

## Authentication and Authorization

### Authentication Mechanisms

#### API Key Authentication

```python
# shared/security/api_key_auth.py
import hashlib
import secrets
from datetime import datetime, timedelta

class ApiKeyManager:
    """Secure API key management"""
    
    def generate_api_key(self, user_id: str) -> tuple[str, str]:
        """Generate secure API key"""
        # Generate random key
        api_key = secrets.token_urlsafe(32)
        
        # Hash for storage
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        # Store with metadata
        self.store_api_key(user_id, key_hash, {
            "created_at": datetime.utcnow(),
            "expires_at": datetime.utcnow() + timedelta(days=365),
            "permissions": ["read", "write"],
            "rate_limit": 1000
        })
        
        return api_key, key_hash
    
    def validate_api_key(self, api_key: str) -> dict:
        """Validate API key and return permissions"""
        key_hash = hashlib.sha256(api_key.encode()).hexdigest()
        
        # Lookup key metadata
        metadata = self.get_key_metadata(key_hash)
        
        if not metadata:
            raise AuthenticationError("Invalid API key")
        
        if metadata["expires_at"] < datetime.utcnow():
            raise AuthenticationError("API key expired")
        
        return metadata
```

#### JWT Authentication (Future)

```python
# shared/security/jwt_auth.py
import jwt
from datetime import datetime, timedelta

class JWTManager:
    """JWT token management"""
    
    def __init__(self, secret_key: str, algorithm: str = "HS256"):
        self.secret_key = secret_key
        self.algorithm = algorithm
    
    def generate_token(self, user_id: str, permissions: list) -> str:
        """Generate JWT token"""
        payload = {
            "user_id": user_id,
            "permissions": permissions,
            "exp": datetime.utcnow() + timedelta(hours=24),
            "iat": datetime.utcnow(),
            "iss": "spice-harvester"
        }
        
        return jwt.encode(payload, self.secret_key, algorithm=self.algorithm)
    
    def verify_token(self, token: str) -> dict:
        """Verify and decode JWT token"""
        try:
            payload = jwt.decode(
                token, 
                self.secret_key, 
                algorithms=[self.algorithm],
                options={"verify_exp": True}
            )
            return payload
        except jwt.ExpiredSignatureError:
            raise AuthenticationError("Token expired")
        except jwt.InvalidTokenError:
            raise AuthenticationError("Invalid token")
```

### Authorization Framework

```python
# shared/security/authorization.py
from enum import Enum
from typing import List, Set

class Permission(Enum):
    """System permissions"""
    # Database permissions
    DATABASE_CREATE = "database:create"
    DATABASE_READ = "database:read"
    DATABASE_UPDATE = "database:update"
    DATABASE_DELETE = "database:delete"
    
    # Ontology permissions
    ONTOLOGY_CREATE = "ontology:create"
    ONTOLOGY_READ = "ontology:read"
    ONTOLOGY_UPDATE = "ontology:update"
    ONTOLOGY_DELETE = "ontology:delete"
    
    # Instance permissions
    INSTANCE_CREATE = "instance:create"
    INSTANCE_READ = "instance:read"
    INSTANCE_UPDATE = "instance:update"
    INSTANCE_DELETE = "instance:delete"

class Role(Enum):
    """System roles"""
    ADMIN = "admin"
    DEVELOPER = "developer"
    ANALYST = "analyst"
    VIEWER = "viewer"

# Role-permission mapping
ROLE_PERMISSIONS = {
    Role.ADMIN: set(Permission),  # All permissions
    Role.DEVELOPER: {
        Permission.DATABASE_READ,
        Permission.ONTOLOGY_CREATE,
        Permission.ONTOLOGY_READ,
        Permission.ONTOLOGY_UPDATE,
        Permission.INSTANCE_CREATE,
        Permission.INSTANCE_READ,
        Permission.INSTANCE_UPDATE,
    },
    Role.ANALYST: {
        Permission.DATABASE_READ,
        Permission.ONTOLOGY_READ,
        Permission.INSTANCE_READ,
    },
    Role.VIEWER: {
        Permission.DATABASE_READ,
        Permission.ONTOLOGY_READ,
    }
}

class AuthorizationManager:
    """Authorization management"""
    
    def check_permission(
        self, 
        user_roles: List[Role], 
        required_permission: Permission
    ) -> bool:
        """Check if user has required permission"""
        user_permissions: Set[Permission] = set()
        
        for role in user_roles:
            user_permissions.update(ROLE_PERMISSIONS.get(role, set()))
        
        return required_permission in user_permissions
    
    def require_permission(self, permission: Permission):
        """Decorator for permission checking"""
        def decorator(func):
            async def wrapper(*args, **kwargs):
                # Extract user context from request
                user = kwargs.get("current_user")
                
                if not user:
                    raise AuthorizationError("No user context")
                
                if not self.check_permission(user.roles, permission):
                    raise AuthorizationError(
                        f"Permission denied: {permission.value}"
                    )
                
                return await func(*args, **kwargs)
            return wrapper
        return decorator
```

## Input Validation and Sanitization

### Input Sanitization Implementation

```python
# shared/security/input_sanitizer.py
import re
import html
from typing import Any, Dict, List, Optional
import bleach

class InputSanitizer:
    """Comprehensive input sanitization"""
    
    # Updated SQL injection patterns
    SQL_INJECTION_PATTERNS = [
        # Specific patterns with context
        r"(\bUNION\b\s+\bSELECT\b)",
        r"(\bOR\b\s+\d+\s*=\s*\d+)",
        r"(\bAND\b\s+\d+\s*=\s*\d+)",
        r"(--\s*$)",  # SQL comments at end of line
        r"(/\*.*\*/)",  # SQL block comments
        r"(\bDROP\s+TABLE\b)",
        r"(\bINSERT\s+INTO\b)",
        r"(\bUPDATE\s+\w+\s+SET\b)",
        r"(\bDELETE\s+FROM\b)",
        r"(xp_cmdshell)",
        r"(exec\s*\()",
    ]
    
    # Updated NoSQL injection patterns
    NOSQL_INJECTION_PATTERNS = [
        r"(?<![a-zA-Z@])\$where\b",
        r"(?<![a-zA-Z@])\$ne\b",
        r"(?<![a-zA-Z@])\$gt\b",
        r"(?<![a-zA-Z@])\$gte\b",
        r"(?<![a-zA-Z@])\$lt\b",
        r"(?<![a-zA-Z@])\$lte\b",
        r"(?<![a-zA-Z@])\$in\b",
        r"(?<![a-zA-Z@])\$nin\b",
        r"(?<![a-zA-Z@])\$exists\b",
        r"(?<![a-zA-Z@])\$type\b",
        r"(?<![a-zA-Z@])\$regex\b",
        r"(?<![a-zA-Z@])\$expr\b",
    ]
    
    # XSS patterns
    XSS_PATTERNS = [
        r"<script[^>]*>.*?</script>",
        r"javascript:",
        r"onerror\s*=",
        r"onload\s*=",
        r"onclick\s*=",
        r"<iframe[^>]*>",
        r"<object[^>]*>",
        r"<embed[^>]*>",
    ]
    
    # Path traversal patterns
    PATH_TRAVERSAL_PATTERNS = [
        r"\.\./",
        r"\.\.[/\\]",
        r"%2e%2e[/\\]",
        r"\.\.%2f",
        r"\.\.%5c",
    ]
    
    def sanitize_string(self, value: str, context: str = "general") -> str:
        """Sanitize string based on context"""
        if not isinstance(value, str):
            return str(value)
        
        # Apply context-specific sanitization
        if context == "html":
            return self._sanitize_html(value)
        elif context == "sql":
            return self._sanitize_sql(value)
        elif context == "path":
            return self._sanitize_path(value)
        elif context == "script":
            return self._sanitize_script(value)
        else:
            return self._sanitize_general(value)
    
    def _sanitize_html(self, value: str) -> str:
        """Sanitize HTML content"""
        # Use bleach for HTML sanitization
        allowed_tags = ['p', 'br', 'span', 'div', 'h1', 'h2', 'h3', 
                       'h4', 'h5', 'h6', 'strong', 'em', 'u', 'a']
        allowed_attributes = {'a': ['href', 'title']}
        
        return bleach.clean(
            value,
            tags=allowed_tags,
            attributes=allowed_attributes,
            strip=True
        )
    
    def _sanitize_sql(self, value: str) -> str:
        """Sanitize for SQL contexts"""
        # Escape special characters
        value = value.replace("'", "''")
        value = value.replace("\\", "\\\\")
        value = value.replace("\0", "")
        
        # Check for injection patterns
        for pattern in self.SQL_INJECTION_PATTERNS:
            if re.search(pattern, value, re.IGNORECASE):
                raise ValidationError(f"Potential SQL injection detected")
        
        return value
    
    def _sanitize_path(self, value: str) -> str:
        """Sanitize file paths"""
        # Check for path traversal
        for pattern in self.PATH_TRAVERSAL_PATTERNS:
            if re.search(pattern, value, re.IGNORECASE):
                raise ValidationError("Path traversal detected")
        
        # Remove dangerous characters
        value = re.sub(r'[<>:"|?*]', '', value)
        
        return value
    
    def _sanitize_script(self, value: str) -> str:
        """Sanitize for script contexts"""
        # Escape for JavaScript
        value = value.replace("\\", "\\\\")
        value = value.replace("'", "\\'")
        value = value.replace('"', '\\"')
        value = value.replace("\n", "\\n")
        value = value.replace("\r", "\\r")
        value = value.replace("</", "<\\/")
        
        return value
    
    def _sanitize_general(self, value: str) -> str:
        """General sanitization"""
        # Remove null bytes
        value = value.replace("\0", "")
        
        # Limit length
        max_length = 10000
        if len(value) > max_length:
            value = value[:max_length]
        
        # Check all injection patterns
        all_patterns = (
            self.SQL_INJECTION_PATTERNS +
            self.NOSQL_INJECTION_PATTERNS +
            self.XSS_PATTERNS +
            self.PATH_TRAVERSAL_PATTERNS
        )
        
        for pattern in all_patterns:
            if re.search(pattern, value, re.IGNORECASE | re.DOTALL):
                # Log potential attack
                logger.warning(f"Potential injection detected: {pattern}")
                # Don't raise error for general sanitization
                # Just remove the pattern
                value = re.sub(pattern, "", value, flags=re.IGNORECASE | re.DOTALL)
        
        return value
    
    def validate_email(self, email: str) -> bool:
        """Validate email address"""
        pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        return bool(re.match(pattern, email))
    
    def validate_url(self, url: str) -> bool:
        """Validate URL"""
        pattern = r'^https?://[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        return bool(re.match(pattern, url))
    
    def validate_phone(self, phone: str) -> bool:
        """Validate phone number"""
        # Remove common separators
        cleaned = re.sub(r'[\s\-\(\)]+', '', phone)
        # Check if it's a valid phone number
        pattern = r'^\+?[1-9]\d{7,14}$'
        return bool(re.match(pattern, cleaned))
```

### Request Validation Middleware

```python
# shared/middleware/request_validator.py
from fastapi import Request, HTTPException
from typing import Dict, Any
import json

class RequestValidator:
    """Request validation middleware"""
    
    def __init__(self, sanitizer: InputSanitizer):
        self.sanitizer = sanitizer
    
    async def validate_request(self, request: Request) -> Dict[str, Any]:
        """Validate incoming request"""
        # Check content type
        content_type = request.headers.get("content-type", "")
        
        if "application/json" not in content_type:
            raise HTTPException(
                status_code=400,
                detail="Content-Type must be application/json"
            )
        
        # Parse body
        try:
            body = await request.json()
        except json.JSONDecodeError:
            raise HTTPException(
                status_code=400,
                detail="Invalid JSON in request body"
            )
        
        # Sanitize all string values
        sanitized_body = self._sanitize_dict(body)
        
        # Validate required fields based on endpoint
        endpoint = request.url.path
        self._validate_required_fields(endpoint, sanitized_body)
        
        return sanitized_body
    
    def _sanitize_dict(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively sanitize dictionary"""
        sanitized = {}
        
        for key, value in data.items():
            # Sanitize key
            safe_key = self.sanitizer.sanitize_string(str(key), "general")
            
            # Sanitize value based on type
            if isinstance(value, str):
                sanitized[safe_key] = self.sanitizer.sanitize_string(value, "general")
            elif isinstance(value, dict):
                sanitized[safe_key] = self._sanitize_dict(value)
            elif isinstance(value, list):
                sanitized[safe_key] = [
                    self._sanitize_dict(item) if isinstance(item, dict)
                    else self.sanitizer.sanitize_string(str(item), "general")
                    if isinstance(item, str)
                    else item
                    for item in value
                ]
            else:
                sanitized[safe_key] = value
        
        return sanitized
    
    def _validate_required_fields(
        self, 
        endpoint: str, 
        data: Dict[str, Any]
    ) -> None:
        """Validate required fields for endpoint"""
        # Define required fields per endpoint
        required_fields = {
            "/api/v1/database/create": ["name"],
            "/api/v1/ontology/create": ["label", "properties"],
            "/api/v1/instance/create": ["class_id", "data"],
        }
        
        # Check if endpoint has requirements
        for pattern, fields in required_fields.items():
            if pattern in endpoint:
                for field in fields:
                    if field not in data:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Missing required field: {field}"
                        )
```

## Data Protection

### Encryption at Rest

```python
# shared/security/encryption.py
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import base64
import os

class DataEncryption:
    """Data encryption utilities"""
    
    def __init__(self, master_key: str):
        """Initialize with master key"""
        # Derive encryption key from master key
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=b'spice-harvester-salt',  # Use proper salt in production
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(
            kdf.derive(master_key.encode())
        )
        self.cipher = Fernet(key)
    
    def encrypt_field(self, value: str) -> str:
        """Encrypt sensitive field"""
        if not value:
            return value
        
        encrypted = self.cipher.encrypt(value.encode())
        return base64.urlsafe_b64encode(encrypted).decode()
    
    def decrypt_field(self, encrypted_value: str) -> str:
        """Decrypt sensitive field"""
        if not encrypted_value:
            return encrypted_value
        
        try:
            encrypted = base64.urlsafe_b64decode(encrypted_value.encode())
            decrypted = self.cipher.decrypt(encrypted)
            return decrypted.decode()
        except Exception as e:
            logger.error(f"Decryption failed: {e}")
            raise SecurityError("Failed to decrypt data")
    
    def encrypt_file(self, file_path: str, output_path: str) -> None:
        """Encrypt file"""
        with open(file_path, 'rb') as infile:
            data = infile.read()
            encrypted_data = self.cipher.encrypt(data)
            
        with open(output_path, 'wb') as outfile:
            outfile.write(encrypted_data)
    
    def decrypt_file(self, encrypted_path: str, output_path: str) -> None:
        """Decrypt file"""
        with open(encrypted_path, 'rb') as infile:
            encrypted_data = infile.read()
            decrypted_data = self.cipher.decrypt(encrypted_data)
            
        with open(output_path, 'wb') as outfile:
            outfile.write(decrypted_data)
```

### Data Classification

```python
# shared/security/data_classification.py
from enum import Enum
from typing import List, Dict, Any

class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"

class FieldClassification:
    """Field-level data classification"""
    
    # Define sensitive fields and their classification
    FIELD_CLASSIFICATIONS = {
        # Personal Information
        "email": DataClassification.CONFIDENTIAL,
        "phone": DataClassification.CONFIDENTIAL,
        "ssn": DataClassification.RESTRICTED,
        "credit_card": DataClassification.RESTRICTED,
        "bank_account": DataClassification.RESTRICTED,
        
        # Business Data
        "revenue": DataClassification.CONFIDENTIAL,
        "salary": DataClassification.CONFIDENTIAL,
        "price": DataClassification.INTERNAL,
        
        # System Data
        "password": DataClassification.RESTRICTED,
        "api_key": DataClassification.RESTRICTED,
        "secret": DataClassification.RESTRICTED,
    }
    
    @classmethod
    def classify_field(cls, field_name: str) -> DataClassification:
        """Classify field based on name"""
        field_lower = field_name.lower()
        
        # Check exact matches
        if field_lower in cls.FIELD_CLASSIFICATIONS:
            return cls.FIELD_CLASSIFICATIONS[field_lower]
        
        # Check patterns
        if any(sensitive in field_lower for sensitive in 
               ["password", "secret", "token", "key"]):
            return DataClassification.RESTRICTED
        
        if any(sensitive in field_lower for sensitive in 
               ["email", "phone", "address", "birth"]):
            return DataClassification.CONFIDENTIAL
        
        # Default classification
        return DataClassification.INTERNAL
    
    @classmethod
    def apply_protection(
        cls, 
        data: Dict[str, Any], 
        user_clearance: DataClassification
    ) -> Dict[str, Any]:
        """Apply data protection based on classification"""
        protected_data = {}
        
        for field, value in data.items():
            classification = cls.classify_field(field)
            
            # Check if user has clearance
            if cls._has_clearance(user_clearance, classification):
                protected_data[field] = value
            else:
                # Mask sensitive data
                protected_data[field] = cls._mask_value(value, classification)
        
        return protected_data
    
    @staticmethod
    def _has_clearance(
        user_clearance: DataClassification, 
        data_classification: DataClassification
    ) -> bool:
        """Check if user has clearance for data"""
        clearance_levels = {
            DataClassification.PUBLIC: 0,
            DataClassification.INTERNAL: 1,
            DataClassification.CONFIDENTIAL: 2,
            DataClassification.RESTRICTED: 3,
        }
        
        return (clearance_levels[user_clearance] >= 
                clearance_levels[data_classification])
    
    @staticmethod
    def _mask_value(value: Any, classification: DataClassification) -> str:
        """Mask sensitive value"""
        if classification == DataClassification.RESTRICTED:
            return "[RESTRICTED]"
        elif classification == DataClassification.CONFIDENTIAL:
            return "[CONFIDENTIAL]"
        else:
            return "[PROTECTED]"
```

## Network Security

### TLS Configuration

```nginx
# nginx/ssl.conf
# Modern SSL configuration

# SSL certificates
ssl_certificate /etc/ssl/certs/spice-harvester.crt;
ssl_certificate_key /etc/ssl/private/spice-harvester.key;

# Protocols - TLS 1.2 and 1.3 only
ssl_protocols TLSv1.2 TLSv1.3;

# Ciphers - Modern cipher suite
ssl_ciphers ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256:ECDHE-ECDSA-AES256-GCM-SHA384:ECDHE-RSA-AES256-GCM-SHA384:ECDHE-ECDSA-CHACHA20-POLY1305:ECDHE-RSA-CHACHA20-POLY1305:DHE-RSA-AES128-GCM-SHA256:DHE-RSA-AES256-GCM-SHA384;

# Prefer server ciphers
ssl_prefer_server_ciphers off;

# HSTS
add_header Strict-Transport-Security "max-age=63072000" always;

# SSL session caching
ssl_session_cache shared:SSL:10m;
ssl_session_timeout 1d;
ssl_session_tickets off;

# OCSP stapling
ssl_stapling on;
ssl_stapling_verify on;
ssl_trusted_certificate /etc/ssl/certs/chain.pem;

# Security headers
add_header X-Frame-Options "SAMEORIGIN" always;
add_header X-Content-Type-Options "nosniff" always;
add_header X-XSS-Protection "1; mode=block" always;
add_header Referrer-Policy "strict-origin-when-cross-origin" always;
add_header Content-Security-Policy "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data: https:; font-src 'self'; connect-src 'self' https://api.spiceharvester.com; frame-ancestors 'none';" always;
```

### Firewall Rules

```bash
#!/bin/bash
# scripts/setup_firewall.sh

# Reset firewall
sudo ufw --force reset

# Default policies
sudo ufw default deny incoming
sudo ufw default allow outgoing

# Allow SSH (restrict source IPs in production)
sudo ufw allow from 10.0.0.0/8 to any port 22

# Allow HTTP/HTTPS
sudo ufw allow 80/tcp
sudo ufw allow 443/tcp

# Allow internal services (only from internal network)
sudo ufw allow from 10.0.0.0/8 to any port 8000  # OMS
sudo ufw allow from 10.0.0.0/8 to any port 8002  # BFF
sudo ufw allow from 10.0.0.0/8 to any port 8003  # Funnel
sudo ufw allow from 10.0.0.0/8 to any port 6363  # TerminusDB

# Rate limiting for API endpoints
sudo ufw limit 443/tcp

# Enable firewall
sudo ufw --force enable

# Show status
sudo ufw status verbose
```

### DDoS Protection

```python
# shared/middleware/ddos_protection.py
from collections import defaultdict
from datetime import datetime, timedelta
from typing import Dict, Tuple
import asyncio

class DDoSProtection:
    """DDoS protection middleware"""
    
    def __init__(
        self,
        rate_limit: int = 100,
        window_seconds: int = 60,
        block_duration_seconds: int = 300
    ):
        self.rate_limit = rate_limit
        self.window_seconds = window_seconds
        self.block_duration_seconds = block_duration_seconds
        
        # Request tracking
        self.request_counts: Dict[str, List[datetime]] = defaultdict(list)
        self.blocked_ips: Dict[str, datetime] = {}
        
        # Start cleanup task
        asyncio.create_task(self._cleanup_task())
    
    async def check_request(self, client_ip: str) -> Tuple[bool, str]:
        """Check if request should be allowed"""
        # Check if IP is blocked
        if client_ip in self.blocked_ips:
            block_until = self.blocked_ips[client_ip]
            if datetime.utcnow() < block_until:
                return False, f"IP blocked until {block_until}"
            else:
                del self.blocked_ips[client_ip]
        
        # Track request
        now = datetime.utcnow()
        self.request_counts[client_ip].append(now)
        
        # Remove old requests
        cutoff = now - timedelta(seconds=self.window_seconds)
        self.request_counts[client_ip] = [
            req_time for req_time in self.request_counts[client_ip]
            if req_time > cutoff
        ]
        
        # Check rate limit
        request_count = len(self.request_counts[client_ip])
        if request_count > self.rate_limit:
            # Block IP
            self.blocked_ips[client_ip] = (
                now + timedelta(seconds=self.block_duration_seconds)
            )
            return False, f"Rate limit exceeded: {request_count} requests"
        
        return True, "OK"
    
    async def _cleanup_task(self):
        """Periodic cleanup of old data"""
        while True:
            await asyncio.sleep(300)  # Run every 5 minutes
            
            now = datetime.utcnow()
            cutoff = now - timedelta(seconds=self.window_seconds * 2)
            
            # Clean request counts
            for ip in list(self.request_counts.keys()):
                self.request_counts[ip] = [
                    req_time for req_time in self.request_counts[ip]
                    if req_time > cutoff
                ]
                if not self.request_counts[ip]:
                    del self.request_counts[ip]
            
            # Clean expired blocks
            for ip in list(self.blocked_ips.keys()):
                if self.blocked_ips[ip] < now:
                    del self.blocked_ips[ip]
```

## Application Security

### Secure Coding Guidelines

#### 1. Input Handling

```python
# DO: Validate and sanitize all inputs
@router.post("/create")
async def create_resource(
    request: CreateResourceRequest,
    sanitizer: InputSanitizer = Depends()
):
    # Validate request model with Pydantic
    validated_data = request.dict()
    
    # Additional sanitization
    sanitized_name = sanitizer.sanitize_string(
        validated_data["name"], 
        context="general"
    )
    
    # Process request
    return await process_creation(sanitized_name)

# DON'T: Trust user input
@router.post("/create")
async def create_resource(data: dict):
    # UNSAFE: Direct use of user input
    name = data["name"]
    query = f"INSERT INTO resources (name) VALUES ('{name}')"
```

#### 2. Authentication Checks

```python
# DO: Check authentication on every protected endpoint
@router.get("/protected")
@require_authentication
async def protected_endpoint(
    current_user: User = Depends(get_current_user)
):
    return {"user": current_user.id}

# DON'T: Assume authentication from previous requests
@router.get("/protected")
async def protected_endpoint(request: Request):
    # UNSAFE: No authentication check
    return {"data": "sensitive information"}
```

#### 3. Error Handling

```python
# DO: Handle errors securely
try:
    result = await dangerous_operation()
except ValidationError as e:
    # Log full error internally
    logger.error(f"Validation error: {e}", exc_info=True)
    # Return safe error to user
    raise HTTPException(
        status_code=400,
        detail="Invalid input provided"
    )
except Exception as e:
    # Log full error internally
    logger.exception("Unexpected error occurred")
    # Return generic error to user
    raise HTTPException(
        status_code=500,
        detail="An error occurred processing your request"
    )

# DON'T: Expose internal errors
except Exception as e:
    # UNSAFE: Exposes internal details
    return {"error": str(e), "traceback": traceback.format_exc()}
```

### Dependency Management

```python
# scripts/check_dependencies.py
import subprocess
import json
from typing import List, Dict

class DependencyChecker:
    """Check dependencies for vulnerabilities"""
    
    def check_python_dependencies(self) -> List[Dict[str, Any]]:
        """Check Python dependencies with safety"""
        try:
            result = subprocess.run(
                ["safety", "check", "--json"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                vulnerabilities = json.loads(result.stdout)
                return self._format_vulnerabilities(vulnerabilities)
            
            return []
        except Exception as e:
            logger.error(f"Failed to check dependencies: {e}")
            return []
    
    def _format_vulnerabilities(
        self, 
        vulnerabilities: List[Dict]
    ) -> List[Dict[str, Any]]:
        """Format vulnerability information"""
        formatted = []
        
        for vuln in vulnerabilities:
            formatted.append({
                "package": vuln["package"],
                "installed_version": vuln["installed_version"],
                "vulnerability": vuln["vulnerability"],
                "severity": vuln.get("severity", "unknown"),
                "description": vuln["description"],
                "fixed_version": vuln.get("fixed_version", "No fix available")
            })
        
        return formatted
    
    def update_dependencies(self, dry_run: bool = True) -> None:
        """Update dependencies to fix vulnerabilities"""
        vulnerable_packages = self.check_python_dependencies()
        
        for package in vulnerable_packages:
            if package["fixed_version"] != "No fix available":
                update_cmd = f"pip install {package['package']}>={package['fixed_version']}"
                
                if dry_run:
                    print(f"Would run: {update_cmd}")
                else:
                    subprocess.run(update_cmd.split())
```

## Security Monitoring and Logging

### Security Event Logging

```python
# shared/security/security_logger.py
import logging
import json
from datetime import datetime
from typing import Dict, Any, Optional

class SecurityLogger:
    """Specialized security event logger"""
    
    def __init__(self, logger_name: str = "security"):
        self.logger = logging.getLogger(logger_name)
        
        # Configure security log handler
        handler = logging.FileHandler("logs/security.log")
        handler.setFormatter(logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        ))
        self.logger.addHandler(handler)
        self.logger.setLevel(logging.INFO)
    
    def log_authentication_attempt(
        self,
        success: bool,
        user_id: Optional[str],
        client_ip: str,
        method: str,
        details: Optional[Dict[str, Any]] = None
    ):
        """Log authentication attempt"""
        event = {
            "event_type": "authentication",
            "timestamp": datetime.utcnow().isoformat(),
            "success": success,
            "user_id": user_id,
            "client_ip": client_ip,
            "method": method,
            "details": details or {}
        }
        
        if success:
            self.logger.info(json.dumps(event))
        else:
            self.logger.warning(json.dumps(event))
    
    def log_authorization_failure(
        self,
        user_id: str,
        resource: str,
        action: str,
        client_ip: str
    ):
        """Log authorization failure"""
        event = {
            "event_type": "authorization_failure",
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "resource": resource,
            "action": action,
            "client_ip": client_ip
        }
        
        self.logger.warning(json.dumps(event))
    
    def log_security_violation(
        self,
        violation_type: str,
        client_ip: str,
        details: Dict[str, Any]
    ):
        """Log security violation"""
        event = {
            "event_type": "security_violation",
            "timestamp": datetime.utcnow().isoformat(),
            "violation_type": violation_type,
            "client_ip": client_ip,
            "details": details
        }
        
        self.logger.error(json.dumps(event))
    
    def log_data_access(
        self,
        user_id: str,
        resource_type: str,
        resource_id: str,
        action: str,
        data_classification: str
    ):
        """Log data access for audit trail"""
        event = {
            "event_type": "data_access",
            "timestamp": datetime.utcnow().isoformat(),
            "user_id": user_id,
            "resource_type": resource_type,
            "resource_id": resource_id,
            "action": action,
            "data_classification": data_classification
        }
        
        self.logger.info(json.dumps(event))
```

### Real-time Security Monitoring

```python
# scripts/security_monitor.py
import asyncio
import re
from collections import defaultdict
from datetime import datetime, timedelta

class SecurityMonitor:
    """Real-time security monitoring"""
    
    def __init__(self):
        self.alert_thresholds = {
            "failed_auth": 5,  # Failed auth attempts
            "auth_spike": 100,  # Auth attempts per minute
            "injection_attempts": 3,  # Injection attempts
            "rate_limit": 10,  # Rate limit violations
        }
        
        self.event_counts = defaultdict(lambda: defaultdict(list))
    
    async def monitor_logs(self, log_file: str):
        """Monitor security logs in real-time"""
        with open(log_file, 'r') as f:
            # Go to end of file
            f.seek(0, 2)
            
            while True:
                line = f.readline()
                if line:
                    await self.process_log_entry(line)
                else:
                    await asyncio.sleep(0.1)
    
    async def process_log_entry(self, log_line: str):
        """Process single log entry"""
        try:
            # Parse JSON log entry
            import json
            event = json.loads(log_line)
            
            # Check different event types
            if event["event_type"] == "authentication":
                await self.check_authentication_anomalies(event)
            elif event["event_type"] == "security_violation":
                await self.check_security_violations(event)
            elif event["event_type"] == "rate_limit":
                await self.check_rate_limits(event)
                
        except Exception as e:
            logger.error(f"Failed to process log entry: {e}")
    
    async def check_authentication_anomalies(self, event: dict):
        """Check for authentication anomalies"""
        client_ip = event["client_ip"]
        
        # Track failed attempts
        if not event["success"]:
            self.event_counts["failed_auth"][client_ip].append(
                datetime.utcnow()
            )
            
            # Check threshold
            recent_failures = [
                t for t in self.event_counts["failed_auth"][client_ip]
                if t > datetime.utcnow() - timedelta(minutes=5)
            ]
            
            if len(recent_failures) >= self.alert_thresholds["failed_auth"]:
                await self.send_alert(
                    "AUTHENTICATION_ATTACK",
                    f"Multiple failed auth attempts from {client_ip}",
                    {"ip": client_ip, "count": len(recent_failures)}
                )
    
    async def send_alert(
        self, 
        alert_type: str, 
        message: str, 
        details: dict
    ):
        """Send security alert"""
        alert = {
            "type": alert_type,
            "timestamp": datetime.utcnow().isoformat(),
            "message": message,
            "details": details,
            "severity": self.get_severity(alert_type)
        }
        
        # Log alert
        logger.critical(f"SECURITY ALERT: {json.dumps(alert)}")
        
        # Send notifications (implement based on your needs)
        # await send_email_alert(alert)
        # await send_slack_alert(alert)
        # await send_pagerduty_alert(alert)
    
    def get_severity(self, alert_type: str) -> str:
        """Determine alert severity"""
        severity_map = {
            "AUTHENTICATION_ATTACK": "HIGH",
            "INJECTION_ATTEMPT": "CRITICAL",
            "DATA_BREACH": "CRITICAL",
            "RATE_LIMIT_ABUSE": "MEDIUM",
        }
        return severity_map.get(alert_type, "LOW")
```

## Incident Response

### Incident Response Plan

#### 1. Detection and Analysis

```python
# scripts/incident_detector.py
class IncidentDetector:
    """Automated incident detection"""
    
    def __init__(self):
        self.indicators = {
            "data_exfiltration": [
                "Large data transfer",
                "Unusual access patterns",
                "After-hours activity"
            ],
            "account_compromise": [
                "Login from new location",
                "Privilege escalation",
                "Unusual API usage"
            ],
            "system_compromise": [
                "New processes",
                "File modifications",
                "Network connections"
            ]
        }
    
    async def detect_incidents(self):
        """Detect potential security incidents"""
        checks = [
            self.check_data_exfiltration(),
            self.check_account_compromise(),
            self.check_system_compromise(),
        ]
        
        results = await asyncio.gather(*checks)
        
        for incident in results:
            if incident:
                await self.handle_incident(incident)
```

#### 2. Containment Procedures

```bash
#!/bin/bash
# scripts/incident_containment.sh

INCIDENT_TYPE=$1
TARGET=$2

case $INCIDENT_TYPE in
  "account_compromise")
    echo "Containing compromised account: $TARGET"
    # Disable user account
    python scripts/disable_user.py --user $TARGET
    # Revoke all sessions
    python scripts/revoke_sessions.py --user $TARGET
    # Force password reset
    python scripts/force_password_reset.py --user $TARGET
    ;;
    
  "data_breach")
    echo "Containing data breach"
    # Enable read-only mode
    python scripts/enable_readonly.py
    # Backup current state
    ./scripts/emergency_backup.sh
    # Block external access
    sudo ufw deny 443/tcp
    ;;
    
  "system_compromise")
    echo "Containing system compromise"
    # Isolate affected system
    sudo iptables -A INPUT -s $TARGET -j DROP
    sudo iptables -A OUTPUT -d $TARGET -j DROP
    # Kill suspicious processes
    python scripts/kill_suspicious_processes.py
    ;;
esac
```

#### 3. Post-Incident Analysis

```python
# scripts/post_incident_analysis.py
class PostIncidentAnalyzer:
    """Post-incident analysis and reporting"""
    
    def generate_incident_report(
        self,
        incident_id: str,
        incident_type: str,
        timeline: List[Dict[str, Any]],
        impact: Dict[str, Any],
        response_actions: List[str]
    ) -> Dict[str, Any]:
        """Generate comprehensive incident report"""
        
        report = {
            "incident_id": incident_id,
            "report_date": datetime.utcnow().isoformat(),
            "incident_type": incident_type,
            "severity": self.assess_severity(impact),
            "timeline": timeline,
            "impact_assessment": impact,
            "response_actions": response_actions,
            "root_cause": self.determine_root_cause(timeline),
            "lessons_learned": self.extract_lessons(timeline, response_actions),
            "recommendations": self.generate_recommendations(incident_type)
        }
        
        return report
    
    def assess_severity(self, impact: Dict[str, Any]) -> str:
        """Assess incident severity"""
        if impact.get("data_loss") or impact.get("system_compromise"):
            return "CRITICAL"
        elif impact.get("service_disruption"):
            return "HIGH"
        elif impact.get("policy_violation"):
            return "MEDIUM"
        else:
            return "LOW"
```

## Compliance and Standards

### Compliance Framework

#### GDPR Compliance

```python
# shared/compliance/gdpr.py
class GDPRCompliance:
    """GDPR compliance utilities"""
    
    def anonymize_personal_data(
        self, 
        data: Dict[str, Any],
        fields_to_anonymize: List[str]
    ) -> Dict[str, Any]:
        """Anonymize personal data fields"""
        anonymized = data.copy()
        
        for field in fields_to_anonymize:
            if field in anonymized:
                anonymized[field] = self._anonymize_value(
                    field, 
                    anonymized[field]
                )
        
        return anonymized
    
    def _anonymize_value(self, field_name: str, value: Any) -> str:
        """Anonymize single value"""
        if "email" in field_name:
            # Keep domain for analytics
            parts = value.split("@")
            if len(parts) == 2:
                return f"***@{parts[1]}"
        elif "phone" in field_name:
            # Keep country code
            return re.sub(r'\d', '*', value[3:])
        elif "name" in field_name:
            # Keep first letter
            return f"{value[0]}***"
        else:
            # Complete anonymization
            return "***"
    
    def handle_data_request(
        self,
        request_type: str,
        user_id: str
    ) -> Dict[str, Any]:
        """Handle GDPR data requests"""
        if request_type == "access":
            return self.export_user_data(user_id)
        elif request_type == "deletion":
            return self.delete_user_data(user_id)
        elif request_type == "portability":
            return self.export_portable_data(user_id)
        else:
            raise ValueError(f"Unknown request type: {request_type}")
```

#### Security Standards

```python
# shared/compliance/security_standards.py
class SecurityStandards:
    """Security standards compliance checks"""
    
    def check_owasp_compliance(self) -> Dict[str, bool]:
        """Check OWASP Top 10 compliance"""
        return {
            "injection": self.check_injection_prevention(),
            "broken_auth": self.check_authentication_security(),
            "sensitive_data": self.check_data_protection(),
            "xxe": self.check_xxe_prevention(),
            "broken_access": self.check_access_control(),
            "misconfig": self.check_security_configuration(),
            "xss": self.check_xss_prevention(),
            "deserialization": self.check_deserialization_security(),
            "components": self.check_component_security(),
            "logging": self.check_logging_monitoring()
        }
    
    def generate_compliance_report(self) -> Dict[str, Any]:
        """Generate compliance report"""
        return {
            "report_date": datetime.utcnow().isoformat(),
            "standards": {
                "owasp_top_10": self.check_owasp_compliance(),
                "pci_dss": self.check_pci_compliance(),
                "iso_27001": self.check_iso_compliance(),
                "gdpr": self.check_gdpr_compliance()
            },
            "overall_score": self.calculate_compliance_score(),
            "recommendations": self.get_compliance_recommendations()
        }
```

## Security Testing

### Automated Security Testing

```python
# tests/security/test_security.py
import pytest
from httpx import AsyncClient

class TestSecurityFeatures:
    """Automated security tests"""
    
    @pytest.mark.asyncio
    async def test_sql_injection_prevention(self, client: AsyncClient):
        """Test SQL injection prevention"""
        injection_payloads = [
            "'; DROP TABLE users; --",
            "1' OR '1'='1",
            "1'; INSERT INTO admins VALUES ('hacker', 'password'); --",
            "1' UNION SELECT * FROM passwords; --"
        ]
        
        for payload in injection_payloads:
            response = await client.post(
                "/api/v1/search",
                json={"query": payload}
            )
            
            # Should be rejected or sanitized
            assert response.status_code in [400, 200]
            if response.status_code == 200:
                # Verify payload was sanitized
                assert "DROP TABLE" not in str(response.json())
    
    @pytest.mark.asyncio
    async def test_xss_prevention(self, client: AsyncClient):
        """Test XSS prevention"""
        xss_payloads = [
            "<script>alert('xss')</script>",
            "<img src=x onerror=alert('xss')>",
            "javascript:alert('xss')",
            "<iframe src='javascript:alert(\"xss\")'></iframe>"
        ]
        
        for payload in xss_payloads:
            response = await client.post(
                "/api/v1/comment",
                json={"text": payload}
            )
            
            if response.status_code == 200:
                # Verify script tags are escaped
                result = response.json()
                assert "<script>" not in result.get("text", "")
                assert "javascript:" not in result.get("text", "")
    
    @pytest.mark.asyncio
    async def test_authentication_required(self, client: AsyncClient):
        """Test authentication enforcement"""
        protected_endpoints = [
            "/api/v1/admin/users",
            "/api/v1/database/create",
            "/api/v1/settings/update"
        ]
        
        for endpoint in protected_endpoints:
            # Request without authentication
            response = await client.get(endpoint)
            
            # Should require authentication
            assert response.status_code in [401, 403]
    
    @pytest.mark.asyncio
    async def test_rate_limiting(self, client: AsyncClient):
        """Test rate limiting"""
        # Make many requests quickly
        responses = []
        for _ in range(150):
            response = await client.get("/api/v1/status")
            responses.append(response.status_code)
        
        # Should hit rate limit
        assert 429 in responses
```

### Penetration Testing Checklist

```markdown
## Web Application Penetration Testing Checklist

### 1. Information Gathering
- [ ] Identify technologies used
- [ ] Map application endpoints
- [ ] Discover hidden files/directories
- [ ] Analyze client-side code
- [ ] Review API documentation

### 2. Authentication Testing
- [ ] Test password policies
- [ ] Check account lockout
- [ ] Test password reset
- [ ] Verify session management
- [ ] Test multi-factor authentication

### 3. Authorization Testing
- [ ] Test vertical privilege escalation
- [ ] Test horizontal privilege escalation
- [ ] Verify object-level authorization
- [ ] Check function-level authorization
- [ ] Test JWT token manipulation

### 4. Input Validation Testing
- [ ] SQL injection
- [ ] NoSQL injection
- [ ] Command injection
- [ ] XSS (reflected, stored, DOM-based)
- [ ] XML injection
- [ ] LDAP injection
- [ ] Path traversal

### 5. Business Logic Testing
- [ ] Test workflow bypass
- [ ] Check race conditions
- [ ] Verify transaction integrity
- [ ] Test negative numbers
- [ ] Check boundary conditions

### 6. Session Management Testing
- [ ] Session fixation
- [ ] Session timeout
- [ ] Cookie security flags
- [ ] Token entropy
- [ ] Concurrent sessions

### 7. API Security Testing
- [ ] Test API authentication
- [ ] Check API rate limiting
- [ ] Verify API input validation
- [ ] Test API versioning
- [ ] Check CORS configuration
```

## Security Best Practices

### Development Security Practices

1. **Secure Development Lifecycle**
   - Security requirements in design phase
   - Threat modeling for new features
   - Security code reviews
   - Automated security testing
   - Security training for developers

2. **Code Security**
   - Use parameterized queries
   - Validate all inputs
   - Encode all outputs
   - Use secure random generators
   - Implement proper error handling

3. **Dependency Management**
   - Regular dependency updates
   - Vulnerability scanning
   - License compliance
   - Minimal dependency principle
   - Dependency pinning

4. **Secret Management**
   - Never commit secrets
   - Use environment variables
   - Rotate secrets regularly
   - Use secret management tools
   - Audit secret access

### Operational Security Practices

1. **Access Control**
   - Principle of least privilege
   - Regular access reviews
   - Multi-factor authentication
   - Automated deprovisioning
   - Audit trail maintenance

2. **Monitoring and Alerting**
   - Real-time security monitoring
   - Automated alert response
   - Regular log review
   - Anomaly detection
   - Incident tracking

3. **Backup and Recovery**
   - Encrypted backups
   - Offsite backup storage
   - Regular recovery testing
   - Documented procedures
   - Access restrictions

4. **Security Updates**
   - Patch management process
   - Security bulletin monitoring
   - Testing before deployment
   - Rollback procedures
   - Update documentation

### Security Training Topics

1. **For Developers**
   - Secure coding practices
   - OWASP Top 10
   - Authentication/Authorization
   - Cryptography basics
   - Security testing

2. **For Operations**
   - Incident response
   - Log analysis
   - Security monitoring
   - Compliance requirements
   - Disaster recovery

3. **For All Staff**
   - Security awareness
   - Phishing recognition
   - Password security
   - Data handling
   - Incident reporting

## Conclusion

Security is an ongoing process that requires constant vigilance and improvement. This documentation provides a comprehensive foundation for securing SPICE HARVESTER, but security practices must evolve with emerging threats and technologies.

Regular security assessments, updates to this documentation, and continuous training are essential for maintaining a strong security posture.