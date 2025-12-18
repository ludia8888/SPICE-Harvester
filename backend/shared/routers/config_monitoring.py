"""
Configuration Monitoring Router

This router provides comprehensive configuration monitoring and observability
endpoints, completing the modernized architecture's monitoring capabilities.

Key features:
1. ✅ Real-time configuration monitoring
2. ✅ Configuration change alerts and history
3. ✅ Security audit endpoints
4. ✅ Performance impact analysis
5. ✅ Environment drift detection
"""

import asyncio
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Any
from fastapi import APIRouter, Depends, HTTPException, status, Query, BackgroundTasks
from fastapi.responses import JSONResponse

from shared.config.settings import ApplicationSettings
from shared.observability.config_monitor import (
    ConfigurationMonitor, ConfigChange, ConfigSeverity, 
    ConfigChangeType, ConfigSecurityAudit
)

router = APIRouter(tags=["Config Monitoring"])

# Global configuration monitor instance
_config_monitor: Optional[ConfigurationMonitor] = None


async def get_settings() -> ApplicationSettings:
    """Get application settings"""
    from shared.config.settings import settings
    return settings


async def get_config_monitor(settings: ApplicationSettings = Depends(get_settings)) -> ConfigurationMonitor:
    """Get or create configuration monitor"""
    global _config_monitor
    
    if _config_monitor is None:
        _config_monitor = ConfigurationMonitor(settings)
        
        # Add some useful change callbacks
        def log_critical_changes(change: ConfigChange):
            if change.severity in [ConfigSeverity.CRITICAL, ConfigSeverity.ERROR]:
                import logging
                logger = logging.getLogger(__name__)
                logger.warning(f"Critical config change: {change.key_path} = {change.new_value}")
        
        _config_monitor.add_change_callback(log_critical_changes)
    
    return _config_monitor


@router.get("/config/current",
           summary="Current Configuration",
           description="Get current application configuration with sensitive values masked")
async def get_current_configuration(
    include_validation: bool = Query(False, description="Include validation results"),
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Get current application configuration
    
    Returns the current configuration snapshot with validation results
    if requested. Sensitive values are automatically masked.
    """
    try:
        config_data = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "configuration": monitor.get_config_snapshot(),
            "config_hash": monitor.calculate_config_hash(monitor.get_config_snapshot()),
            "environment": monitor.settings.environment.value
        }
        
        if include_validation:
            config_data["validation_results"] = monitor.validate_configuration()
        
        return config_data
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve configuration: {str(e)}"
        )


@router.get("/config/changes",
           summary="Configuration Changes", 
           description="Get configuration change history and detect new changes")
async def get_configuration_changes(
    limit: int = Query(50, ge=1, le=500, description="Maximum number of changes to return"),
    severity: Optional[str] = Query(None, description="Filter by severity (info, warning, error, critical)"),
    change_type: Optional[str] = Query(None, description="Filter by change type"),
    since: Optional[str] = Query(None, description="Get changes since timestamp (ISO format)"),
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Get configuration change history
    
    Returns recent configuration changes with optional filtering by
    severity, type, and time period.
    """
    try:
        # Detect any new changes first
        new_changes = monitor.detect_changes()
        
        # Get change history
        all_changes = monitor.get_change_history(limit * 2)  # Get more to allow for filtering
        
        # Apply filters
        filtered_changes = all_changes
        
        if severity:
            try:
                severity_enum = ConfigSeverity(severity.lower())
                filtered_changes = [
                    change for change in filtered_changes 
                    if ConfigSeverity(change["severity"]) == severity_enum
                ]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid severity: {severity}"
                )
        
        if change_type:
            try:
                type_enum = ConfigChangeType(change_type.lower())
                filtered_changes = [
                    change for change in filtered_changes
                    if ConfigChangeType(change["change_type"]) == type_enum
                ]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid change type: {change_type}"
                )
        
        if since:
            try:
                since_dt = datetime.fromisoformat(since.replace('Z', '+00:00'))
                filtered_changes = [
                    change for change in filtered_changes
                    if datetime.fromisoformat(change["timestamp"].replace('Z', '+00:00')) >= since_dt
                ]
            except ValueError:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Invalid timestamp format: {since}"
                )
        
        # Apply limit after filtering
        filtered_changes = filtered_changes[-limit:]
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_changes": len(all_changes),
            "filtered_changes": len(filtered_changes),
            "new_changes_detected": len(new_changes),
            "changes": filtered_changes
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve configuration changes: {str(e)}"
        )


@router.get("/config/validation",
           summary="Configuration Validation",
           description="Validate configuration against security and best practice rules")
async def validate_configuration(
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Validate current configuration
    
    Runs all configured validation rules against the current configuration
    and returns any violations or recommendations.
    """
    try:
        violations = monitor.validate_configuration()
        
        # Categorize violations by severity
        violations_by_severity = {}
        for violation in violations:
            severity = violation["severity"]
            if severity not in violations_by_severity:
                violations_by_severity[severity] = []
            violations_by_severity[severity].append(violation)
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "total_violations": len(violations),
            "violations_by_severity": violations_by_severity,
            "all_violations": violations,
            "validation_summary": {
                "critical": len(violations_by_severity.get("critical", [])),
                "error": len(violations_by_severity.get("error", [])),
                "warning": len(violations_by_severity.get("warning", [])),
                "info": len(violations_by_severity.get("info", []))
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Configuration validation failed: {str(e)}"
        )


@router.get("/config/security-audit",
           summary="Security Audit",
           description="Perform comprehensive security audit of configuration")
async def perform_security_audit(
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Perform security audit of configuration
    
    Analyzes configuration for security issues, weak authentication,
    insecure connections, and other security concerns.
    """
    try:
        audit_result = monitor.perform_security_audit()
        
        # Add risk assessment
        risk_assessment = "low"
        if audit_result.risk_score >= 75:
            risk_assessment = "critical"
        elif audit_result.risk_score >= 50:
            risk_assessment = "high"
        elif audit_result.risk_score >= 25:
            risk_assessment = "medium"
        
        response_data = audit_result.to_dict()
        response_data["risk_assessment"] = risk_assessment
        
        # Set appropriate HTTP status based on risk
        if audit_result.risk_score >= 75:
            status_code = status.HTTP_400_BAD_REQUEST  # Critical security issues
        elif audit_result.risk_score >= 50:
            status_code = status.HTTP_200_OK  # High risk but operational
        else:
            status_code = status.HTTP_200_OK  # Acceptable risk
        
        return JSONResponse(content=response_data, status_code=status_code)
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Security audit failed: {str(e)}"
        )


@router.get("/config/report",
           summary="Configuration Report",
           description="Get comprehensive configuration report with all monitoring data")
async def get_configuration_report(
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Get comprehensive configuration report
    
    Returns a complete report including current configuration,
    validation results, security audit, change history, and
    monitoring status.
    """
    try:
        # Detect any new changes first
        monitor.detect_changes()
        
        report = monitor.get_configuration_report()
        
        # Add summary statistics
        report["summary"] = {
            "environment": monitor.settings.environment.value,
            "monitoring_enabled": monitor.monitoring_enabled,
            "total_validation_rules": len(monitor.validation_rules),
            "total_change_callbacks": len(monitor.change_callbacks),
            "recent_changes_count": len(report.get("recent_changes", [])),
            "validation_violations": len(report.get("validation_results", [])),
            "security_risk_score": report.get("security_audit", {}).get("risk_score", 0)
        }
        
        return report
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to generate configuration report: {str(e)}"
        )


@router.post("/config/check-changes",
            summary="Check for Changes",
            description="Manually trigger configuration change detection")
async def check_configuration_changes(
    background_tasks: BackgroundTasks,
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Manually trigger configuration change detection
    
    Forces a check for configuration changes and returns any
    detected changes immediately.
    """
    try:
        changes = monitor.detect_changes()
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "changes_detected": len(changes),
            "changes": [change.to_dict() for change in changes]
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check for configuration changes: {str(e)}"
        )


@router.get("/config/drift-analysis",
           summary="Environment Drift Analysis",
           description="Analyze configuration drift between environments")
async def analyze_environment_drift(
    compare_environment: str = Query(..., description="Environment to compare against (development, staging, production)"),
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Analyze configuration drift between environments
    
    Compares current configuration with expected configuration
    for different environments to detect drift.
    """
    try:
        current_config = monitor.get_config_snapshot()
        current_env = monitor.settings.environment.value
        
        # This is a simplified example - in a real implementation,
        # you'd load expected configuration from a configuration store
        expected_configs = {
            "development": {
                "debug": True,
                "database": {"host": "localhost"},
                "services": {"terminus_url": "http://localhost:6363"}
            },
            "staging": {
                "debug": False,
                "database": {"host": "staging-db"},
                "services": {"terminus_url": "https://staging-terminus.example.com"}
            },
            "production": {
                "debug": False,
                "database": {"host": "prod-db"},
                "services": {"terminus_url": "https://terminus.example.com"}
            }
        }
        
        if compare_environment not in expected_configs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Unknown environment: {compare_environment}"
            )
        
        expected_config = expected_configs[compare_environment]
        
        # Find differences
        differences = []
        
        def compare_recursive(current_dict: Dict, expected_dict: Dict, path: str = ""):
            for key, expected_value in expected_dict.items():
                current_path = f"{path}.{key}" if path else key
                
                if key not in current_dict:
                    differences.append({
                        "type": "missing",
                        "key_path": current_path,
                        "expected_value": expected_value,
                        "current_value": None,
                        "drift_severity": "error"
                    })
                elif isinstance(expected_value, dict) and isinstance(current_dict[key], dict):
                    compare_recursive(current_dict[key], expected_value, current_path)
                elif current_dict[key] != expected_value:
                    differences.append({
                        "type": "different",
                        "key_path": current_path,
                        "expected_value": expected_value,
                        "current_value": current_dict[key],
                        "drift_severity": "warning"
                    })
        
        compare_recursive(current_config, expected_config)
        
        # Calculate drift score
        drift_score = len(differences) * 10  # Simple scoring
        drift_level = "low"
        if drift_score >= 50:
            drift_level = "high"
        elif drift_score >= 20:
            drift_level = "medium"
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "current_environment": current_env,
            "compared_environment": compare_environment,
            "drift_detected": len(differences) > 0,
            "drift_score": drift_score,
            "drift_level": drift_level,
            "differences": differences,
            "summary": {
                "total_differences": len(differences),
                "missing_keys": len([d for d in differences if d["type"] == "missing"]),
                "different_values": len([d for d in differences if d["type"] == "different"])
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Environment drift analysis failed: {str(e)}"
        )


@router.get("/config/health-impact",
           summary="Configuration Health Impact",
           description="Analyze how configuration affects system health")
async def analyze_configuration_health_impact(
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Analyze configuration health impact
    
    Correlates configuration settings with system health and performance
    to identify potentially problematic configurations.
    """
    try:
        current_config = monitor.get_config_snapshot()
        
        # Analyze potential health impacts
        health_impacts = []
        
        # Database configuration impacts
        if current_config.get("database", {}).get("host") == "localhost":
            if monitor.settings.is_production:
                health_impacts.append({
                    "category": "database",
                    "impact_type": "availability",
                    "severity": "high",
                    "description": "Database host is localhost in production - single point of failure",
                    "recommendation": "Use external database host for production",
                    "affected_services": ["all"]
                })
        
        # Debug mode impacts
        if current_config.get("debug", False):
            health_impacts.append({
                "category": "performance",
                "impact_type": "performance",
                "severity": "medium" if monitor.settings.is_production else "low",
                "description": "Debug mode enabled - may impact performance and security",
                "recommendation": "Disable debug mode in production",
                "affected_services": ["all"]
            })
        
        # Redis configuration impacts
        redis_config = current_config.get("services", {})
        if redis_config.get("redis_host") == "localhost":
            health_impacts.append({
                "category": "caching",
                "impact_type": "scalability",
                "severity": "medium",
                "description": "Redis host is localhost - limits scalability",
                "recommendation": "Use external Redis service for better scalability",
                "affected_services": ["caching", "session_management"]
            })
        
        # Calculate overall health impact score
        impact_score = sum({
            "low": 5,
            "medium": 15,
            "high": 30,
            "critical": 50
        }.get(impact["severity"], 0) for impact in health_impacts)
        
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "overall_impact_score": impact_score,
            "impact_level": (
                "critical" if impact_score >= 75 else
                "high" if impact_score >= 50 else
                "medium" if impact_score >= 25 else
                "low"
            ),
            "health_impacts": health_impacts,
            "recommendations": list(set(impact["recommendation"] for impact in health_impacts)),
            "summary": {
                "total_impacts": len(health_impacts),
                "by_severity": {
                    severity: len([i for i in health_impacts if i["severity"] == severity])
                    for severity in ["low", "medium", "high", "critical"]
                },
                "by_category": {
                    category: len([i for i in health_impacts if i["category"] == category])
                    for category in set(impact["category"] for impact in health_impacts)
                }
            }
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Health impact analysis failed: {str(e)}"
        )


@router.get("/config/monitoring-status",
           summary="Configuration Monitoring Status",
           description="Get status of configuration monitoring system")
async def get_monitoring_status(
    monitor: ConfigurationMonitor = Depends(get_config_monitor)
):
    """
    Get configuration monitoring system status
    
    Returns the current status and health of the configuration
    monitoring system itself.
    """
    try:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "monitoring_enabled": monitor.monitoring_enabled,
            "last_config_hash": monitor.last_config_hash is not None,
            "total_validation_rules": len(monitor.validation_rules),
            "change_callbacks": len(monitor.change_callbacks),
            "change_history_size": len(monitor.change_history),
            "max_history_entries": monitor.max_history_entries,
            "environment": monitor.settings.environment.value,
            "settings_debug": monitor.settings.debug,
            "system_status": "healthy"
        }
        
    except Exception as e:
        return {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "system_status": "error",
            "error": str(e)
        }
