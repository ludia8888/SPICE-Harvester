"""
Configuration Monitoring and Observability

This module provides advanced configuration monitoring, change detection,
and observability features for the modernized architecture.

Key features:
1. ✅ Configuration change detection and alerting
2. ✅ Environment drift monitoring
3. ✅ Configuration validation and recommendations
4. ✅ Security configuration auditing
5. ✅ Performance impact analysis of configuration changes
"""

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone, timedelta
from dataclasses import dataclass, field
from enum import Enum
from typing import Dict, List, Optional, Any, Set, Callable, Union
from pathlib import Path

from shared.config.settings import ApplicationSettings, Environment

logger = logging.getLogger(__name__)


class ConfigChangeType(Enum):
    """Types of configuration changes"""
    ADDED = "added"
    MODIFIED = "modified"
    REMOVED = "removed"
    SECURITY_SENSITIVE = "security_sensitive"
    PERFORMANCE_IMPACT = "performance_impact"


class ConfigSeverity(Enum):
    """Severity levels for configuration issues"""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class ConfigChange:
    """Represents a configuration change"""
    change_type: ConfigChangeType
    key_path: str
    old_value: Any
    new_value: Any
    timestamp: datetime
    severity: ConfigSeverity = ConfigSeverity.INFO
    description: str = ""
    security_implications: List[str] = field(default_factory=list)
    performance_implications: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "change_type": self.change_type.value,
            "key_path": self.key_path,
            "old_value": self._sanitize_value(self.old_value),
            "new_value": self._sanitize_value(self.new_value),
            "timestamp": self.timestamp.isoformat(),
            "severity": self.severity.value,
            "description": self.description,
            "security_implications": self.security_implications,
            "performance_implications": self.performance_implications
        }
    
    def _sanitize_value(self, value: Any) -> Any:
        """Sanitize sensitive values for logging"""
        if self._is_sensitive_key(self.key_path):
            return "*****" if value else None
        return value
    
    def _is_sensitive_key(self, key_path: str) -> bool:
        """Check if a key path contains sensitive information"""
        sensitive_keywords = [
            "password", "key", "secret", "token", "credential",
            "private", "auth", "api_key", "access_key"
        ]
        return any(keyword in key_path.lower() for keyword in sensitive_keywords)


@dataclass
class ConfigValidationRule:
    """Configuration validation rule"""
    name: str
    description: str
    key_pattern: str
    validator: Callable[[Any], bool]
    severity: ConfigSeverity
    recommendation: str = ""


@dataclass
class ConfigSecurityAudit:
    """Security audit result for configuration"""
    timestamp: datetime
    total_settings: int
    security_issues: List[Dict[str, Any]]
    recommendations: List[str]
    risk_score: float  # 0-100
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization"""
        return {
            "timestamp": self.timestamp.isoformat(),
            "total_settings": self.total_settings,
            "security_issues": self.security_issues,
            "recommendations": self.recommendations,
            "risk_score": self.risk_score
        }


class ConfigurationMonitor:
    """
    Monitors configuration changes and provides observability
    
    Tracks configuration state, detects changes, validates settings,
    and provides security and performance insights.
    """
    
    def __init__(self, settings: ApplicationSettings):
        self.settings = settings
        self.last_config_hash: Optional[str] = None
        self.last_config_snapshot: Optional[Dict[str, Any]] = None
        self.change_history: List[ConfigChange] = []
        self.validation_rules: List[ConfigValidationRule] = []
        self.change_callbacks: List[Callable[[ConfigChange], None]] = []
        self.monitoring_enabled = True
        self.max_history_entries = 1000
        
        # Initialize default validation rules
        self._initialize_default_validation_rules()
    
    def _initialize_default_validation_rules(self):
        """Initialize default configuration validation rules"""
        
        # Database connection validation
        self.validation_rules.extend([
            ConfigValidationRule(
                name="database_port_range",
                description="Database port should be in valid range",
                key_pattern="database.port",
                validator=lambda x: isinstance(x, int) and 1024 <= x <= 65535,
                severity=ConfigSeverity.WARNING,
                recommendation="Use ports between 1024-65535 for database connections"
            ),
            ConfigValidationRule(
                name="database_host_not_localhost_in_prod",
                description="Database host should not be localhost in production",
                key_pattern="database.host",
                validator=lambda x: not (self.settings.is_production and x in ["localhost", "127.0.0.1"]),
                severity=ConfigSeverity.ERROR,
                recommendation="Use proper database host in production environment"
            ),
            ConfigValidationRule(
                name="strong_database_password",
                description="Database password should be strong in production",
                key_pattern="database.password",
                validator=lambda x: not self.settings.is_production or (isinstance(x, str) and len(x) >= 12),
                severity=ConfigSeverity.CRITICAL,
                recommendation="Use strong passwords (12+ characters) in production"
            )
        ])
        
        # Security validation
        self.validation_rules.extend([
            ConfigValidationRule(
                name="debug_disabled_in_prod",
                description="Debug mode should be disabled in production",
                key_pattern="debug",
                validator=lambda x: not (self.settings.is_production and x),
                severity=ConfigSeverity.CRITICAL,
                recommendation="Disable debug mode in production for security"
            ),
            ConfigValidationRule(
                name="terminus_secure_connection",
                description="TerminusDB should use HTTPS in production",
                key_pattern="services.terminus_url",
                validator=lambda x: not self.settings.is_production or (isinstance(x, str) and x.startswith("https://")),
                severity=ConfigSeverity.ERROR,
                recommendation="Use HTTPS for TerminusDB connections in production"
            ),
            ConfigValidationRule(
                name="redis_auth_in_prod",
                description="Redis should have authentication in production",
                key_pattern="services.redis_password",
                validator=lambda x: not self.settings.is_production or (x is not None and len(str(x)) > 0),
                severity=ConfigSeverity.WARNING,
                recommendation="Enable Redis authentication in production"
            )
        ])
        
        # Performance validation
        self.validation_rules.extend([
            ConfigValidationRule(
                name="redis_connection_pooling",
                description="Redis connection pool should be configured properly",
                key_pattern="services.redis_max_connections",
                validator=lambda x: isinstance(x, int) and x >= 10,
                severity=ConfigSeverity.INFO,
                recommendation="Configure adequate Redis connection pool size"
            )
        ])
    
    def add_validation_rule(self, rule: ConfigValidationRule):
        """Add custom validation rule"""
        self.validation_rules.append(rule)
        logger.info(f"Added validation rule: {rule.name}")
    
    def add_change_callback(self, callback: Callable[[ConfigChange], None]):
        """Add callback for configuration changes"""
        self.change_callbacks.append(callback)
    
    def get_config_snapshot(self) -> Dict[str, Any]:
        """Get current configuration snapshot"""
        # Convert settings to dictionary for comparison
        config_dict = {}
        
        # Basic settings
        config_dict["environment"] = self.settings.environment.value
        config_dict["debug"] = self.settings.debug
        
        # Database settings (prefer postgres_* fields from DatabaseSettings)
        db_settings = self.settings.database
        config_dict["database"] = {
            "host": getattr(db_settings, "host", None) or getattr(db_settings, "postgres_host", None),
            "port": getattr(db_settings, "port", None) or getattr(db_settings, "postgres_port", None),
            "name": getattr(db_settings, "name", None) or getattr(db_settings, "postgres_db", None),
            "user": getattr(db_settings, "user", None) or getattr(db_settings, "postgres_user", None),
            # Don't include password in snapshot for security
        }
        
        # Service settings
        service_settings = self.settings.services
        config_dict["services"] = {
            "terminus_url": getattr(self.settings.database, "terminus_url", None),
            "oms_base_url": getattr(service_settings, "oms_base_url", None),
            "bff_base_url": getattr(service_settings, "bff_base_url", None),
            "redis_host": getattr(self.settings.database, "redis_host", None),
            "redis_port": getattr(self.settings.database, "redis_port", None),
            "redis_db": getattr(self.settings.database, "redis_db", None),
            "elasticsearch_host": getattr(self.settings.database, "elasticsearch_host", None),
            "elasticsearch_port": getattr(self.settings.database, "elasticsearch_port", None),
            "elasticsearch_username": getattr(self.settings.database, "elasticsearch_username", None),
            # Don't include passwords/keys in snapshot for security
        }
        
        return config_dict
    
    def calculate_config_hash(self, config_dict: Dict[str, Any]) -> str:
        """Calculate hash of configuration for change detection"""
        config_json = json.dumps(config_dict, sort_keys=True, default=str)
        return hashlib.sha256(config_json.encode()).hexdigest()
    
    def detect_changes(self) -> List[ConfigChange]:
        """Detect configuration changes since last check"""
        current_snapshot = self.get_config_snapshot()
        current_hash = self.calculate_config_hash(current_snapshot)
        
        changes = []
        
        if self.last_config_hash and self.last_config_hash != current_hash:
            changes = self._compare_snapshots(self.last_config_snapshot, current_snapshot)
            
            # Add changes to history
            self.change_history.extend(changes)
            
            # Trim history if too long
            if len(self.change_history) > self.max_history_entries:
                self.change_history = self.change_history[-self.max_history_entries:]
            
            # Notify callbacks
            for change in changes:
                for callback in self.change_callbacks:
                    try:
                        callback(change)
                    except Exception as e:
                        logger.error(f"Change callback failed: {e}")
        
        # Update tracking
        self.last_config_hash = current_hash
        self.last_config_snapshot = current_snapshot
        
        return changes
    
    def _compare_snapshots(self, old_snapshot: Dict[str, Any], new_snapshot: Dict[str, Any]) -> List[ConfigChange]:
        """Compare two configuration snapshots and return changes"""
        changes = []
        
        def compare_recursive(old_dict: Dict, new_dict: Dict, path: str = ""):
            # Check for added/modified keys
            for key, new_value in new_dict.items():
                current_path = f"{path}.{key}" if path else key
                
                if key not in old_dict:
                    # Added key
                    change = ConfigChange(
                        change_type=ConfigChangeType.ADDED,
                        key_path=current_path,
                        old_value=None,
                        new_value=new_value,
                        timestamp=datetime.now(timezone.utc),
                        description=f"Configuration key '{current_path}' was added"
                    )
                    self._analyze_change_impact(change)
                    changes.append(change)
                    
                elif old_dict[key] != new_value:
                    if isinstance(old_dict[key], dict) and isinstance(new_value, dict):
                        # Recurse into nested dictionaries
                        compare_recursive(old_dict[key], new_value, current_path)
                    else:
                        # Modified key
                        change = ConfigChange(
                            change_type=ConfigChangeType.MODIFIED,
                            key_path=current_path,
                            old_value=old_dict[key],
                            new_value=new_value,
                            timestamp=datetime.now(timezone.utc),
                            description=f"Configuration key '{current_path}' was modified"
                        )
                        self._analyze_change_impact(change)
                        changes.append(change)
            
            # Check for removed keys
            for key, old_value in old_dict.items():
                current_path = f"{path}.{key}" if path else key
                
                if key not in new_dict:
                    change = ConfigChange(
                        change_type=ConfigChangeType.REMOVED,
                        key_path=current_path,
                        old_value=old_value,
                        new_value=None,
                        timestamp=datetime.now(timezone.utc),
                        description=f"Configuration key '{current_path}' was removed"
                    )
                    self._analyze_change_impact(change)
                    changes.append(change)
        
        if old_snapshot and new_snapshot:
            compare_recursive(old_snapshot, new_snapshot)
        
        return changes
    
    def _analyze_change_impact(self, change: ConfigChange):
        """Analyze the impact of a configuration change"""
        key_path = change.key_path.lower()
        
        # Security impact analysis
        security_keywords = ["password", "key", "secret", "auth", "token", "credential"]
        if any(keyword in key_path for keyword in security_keywords):
            change.change_type = ConfigChangeType.SECURITY_SENSITIVE
            change.severity = ConfigSeverity.WARNING
            change.security_implications.append("Sensitive security configuration changed")
            
            if change.key_path.endswith("password") and not change.new_value:
                change.severity = ConfigSeverity.CRITICAL
                change.security_implications.append("Password was removed or set to empty")
        
        # Performance impact analysis
        performance_keywords = ["pool", "timeout", "cache", "connection", "thread", "worker"]
        if any(keyword in key_path for keyword in performance_keywords):
            change.change_type = ConfigChangeType.PERFORMANCE_IMPACT
            change.performance_implications.append("Configuration change may affect performance")
        
        # Environment-specific analysis
        if "debug" in key_path and self.settings.is_production:
            if change.new_value:
                change.severity = ConfigSeverity.CRITICAL
                change.security_implications.append("Debug mode enabled in production environment")
        
        # Database connection changes
        if key_path.startswith("database"):
            change.performance_implications.append("Database configuration change may require service restart")
            if "host" in key_path or "port" in key_path:
                change.severity = ConfigSeverity.WARNING
    
    def validate_configuration(self) -> List[Dict[str, Any]]:
        """Validate current configuration against rules"""
        violations = []
        config_snapshot = self.get_config_snapshot()
        
        for rule in self.validation_rules:
            try:
                # Extract value using key pattern
                value = self._get_nested_value(config_snapshot, rule.key_pattern)
                
                if not rule.validator(value):
                    violations.append({
                        "rule_name": rule.name,
                        "description": rule.description,
                        "key_path": rule.key_pattern,
                        "current_value": self._sanitize_sensitive_value(rule.key_pattern, value),
                        "severity": rule.severity.value,
                        "recommendation": rule.recommendation,
                        "timestamp": datetime.now(timezone.utc).isoformat()
                    })
                    
            except Exception as e:
                logger.warning(f"Validation rule '{rule.name}' failed: {e}")
        
        return violations
    
    def perform_security_audit(self) -> ConfigSecurityAudit:
        """Perform comprehensive security audit of configuration"""
        timestamp = datetime.now(timezone.utc)
        config_snapshot = self.get_config_snapshot()
        
        security_issues = []
        recommendations = []
        risk_score = 0.0
        
        # Check for common security issues
        security_checks = [
            self._check_debug_mode_in_production,
            self._check_default_passwords,
            self._check_insecure_connections,
            self._check_exposed_sensitive_data,
            self._check_weak_authentication,
        ]
        
        for check in security_checks:
            try:
                issues, recs, score = check(config_snapshot)
                security_issues.extend(issues)
                recommendations.extend(recs)
                risk_score += score
            except Exception as e:
                logger.error(f"Security check failed: {e}")
        
        # Normalize risk score (0-100)
        risk_score = min(100.0, max(0.0, risk_score))
        
        return ConfigSecurityAudit(
            timestamp=timestamp,
            total_settings=self._count_total_settings(config_snapshot),
            security_issues=security_issues,
            recommendations=recommendations,
            risk_score=risk_score
        )
    
    def _check_debug_mode_in_production(self, config: Dict[str, Any]) -> tuple:
        """Check if debug mode is enabled in production"""
        issues = []
        recommendations = []
        score = 0.0
        
        if self.settings.is_production and config.get("debug", False):
            issues.append({
                "type": "debug_mode_production",
                "severity": "critical",
                "description": "Debug mode is enabled in production environment",
                "key_path": "debug",
                "risk_level": "high"
            })
            recommendations.append("Disable debug mode in production environment")
            score += 25.0
        
        return issues, recommendations, score
    
    def _check_default_passwords(self, config: Dict[str, Any]) -> tuple:
        """Check for default or weak passwords"""
        issues = []
        recommendations = []
        score = 0.0
        
        # Check database password strength
        if self.settings.is_production:
            db_password = getattr(self.settings.database, 'password', None)
            if db_password is None:
                db_password = getattr(self.settings.database, 'postgres_password', '')
            if not db_password or len(db_password) < 12:
                issues.append({
                    "type": "weak_database_password",
                    "severity": "critical",
                    "description": "Database password is weak or missing in production",
                    "key_path": "database.password",
                    "risk_level": "high"
                })
                recommendations.append("Use strong database passwords (12+ characters) in production")
                score += 30.0
        
        return issues, recommendations, score
    
    def _check_insecure_connections(self, config: Dict[str, Any]) -> tuple:
        """Check for insecure connection configurations"""
        issues = []
        recommendations = []
        score = 0.0
        
        if self.settings.is_production:
            # Check TerminusDB connection
            terminus_url = config.get("services", {}).get("terminus_url", "")
            if terminus_url and not terminus_url.startswith("https://"):
                issues.append({
                    "type": "insecure_terminus_connection",
                    "severity": "error",
                    "description": "TerminusDB connection is not using HTTPS in production",
                    "key_path": "services.terminus_url",
                    "risk_level": "medium"
                })
                recommendations.append("Use HTTPS for TerminusDB connections in production")
                score += 15.0
        
        return issues, recommendations, score
    
    def _check_exposed_sensitive_data(self, config: Dict[str, Any]) -> tuple:
        """Check for potentially exposed sensitive data"""
        issues = []
        recommendations = []
        score = 0.0
        
        # This is a placeholder - in real implementation, you'd check logs, 
        # environment variables, etc. for exposed secrets
        
        return issues, recommendations, score
    
    def _check_weak_authentication(self, config: Dict[str, Any]) -> tuple:
        """Check for weak authentication configurations"""
        issues = []
        recommendations = []
        score = 0.0
        
        # Check if Redis authentication is configured in production
        if self.settings.is_production:
            redis_password = getattr(self.settings.database, 'redis_password', None)
            if not redis_password:
                issues.append({
                    "type": "redis_no_auth",
                    "severity": "warning",
                    "description": "Redis authentication is not configured in production",
                    "key_path": "services.redis_password",
                    "risk_level": "medium"
                })
                recommendations.append("Enable Redis authentication in production")
                score += 10.0
        
        return issues, recommendations, score
    
    def _get_nested_value(self, data: Dict[str, Any], key_path: str) -> Any:
        """Get nested value from dictionary using dot notation"""
        keys = key_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
        
        return value
    
    def _sanitize_sensitive_value(self, key_path: str, value: Any) -> Any:
        """Sanitize sensitive values for display"""
        sensitive_keywords = ["password", "key", "secret", "token", "credential"]
        if any(keyword in key_path.lower() for keyword in sensitive_keywords):
            return "*****" if value else None
        return value
    
    def _count_total_settings(self, config: Dict[str, Any]) -> int:
        """Count total number of configuration settings"""
        def count_recursive(data):
            if isinstance(data, dict):
                return sum(count_recursive(v) for v in data.values())
            else:
                return 1
        
        return count_recursive(config)
    
    def get_change_history(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent configuration change history"""
        recent_changes = self.change_history[-limit:] if limit else self.change_history
        return [change.to_dict() for change in recent_changes]
    
    def get_configuration_report(self) -> Dict[str, Any]:
        """Get comprehensive configuration report"""
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "environment": self.settings.environment.value,
            "current_configuration": self.get_config_snapshot(),
            "validation_results": self.validate_configuration(),
            "security_audit": self.perform_security_audit().to_dict(),
            "recent_changes": self.get_change_history(20),
            "monitoring_status": {
                "enabled": self.monitoring_enabled,
                "last_check": self.last_config_hash is not None,
                "total_validation_rules": len(self.validation_rules),
                "change_callbacks": len(self.change_callbacks)
            }
        }
