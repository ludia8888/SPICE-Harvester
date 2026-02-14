"""
Schema Change Monitor Service

Provides periodic monitoring of schema changes across active mapping specs
and broadcasts notifications when drift is detected.

Features:
- Periodic schema drift checks for active mapping specs
- WebSocket notifications for detected changes
- Configurable check intervals and severity thresholds
- Integration with BackgroundTaskManager for proper lifecycle management
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any, Callable, Dict, List, Optional
from dataclasses import dataclass

from shared.errors.error_envelope import build_error_envelope
from shared.errors.error_types import ErrorCategory, ErrorCode

from shared.services.core.schema_drift_detector import (
    SchemaDriftDetector,
    SchemaDrift,
    ImpactedMapping,
)

logger = logging.getLogger(__name__)


@dataclass
class MonitorConfig:
    """Configuration for schema change monitoring"""
    # Check interval in seconds
    check_interval_seconds: int = 300  # 5 minutes

    # Minimum severity level to notify ("info", "warning", "breaking")
    min_notify_severity: str = "warning"

    # Whether to notify on first check (when no previous state exists)
    notify_on_first_check: bool = False

    # Maximum number of mapping specs to check per cycle
    batch_size: int = 100

    # Cooldown period for repeated notifications (seconds)
    notification_cooldown: int = 3600  # 1 hour

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "MonitorConfig":
        return cls(
            check_interval_seconds=config_dict.get("check_interval_seconds", 300),
            min_notify_severity=config_dict.get("min_notify_severity", "warning"),
            notify_on_first_check=config_dict.get("notify_on_first_check", False),
            batch_size=config_dict.get("batch_size", 100),
            notification_cooldown=config_dict.get("notification_cooldown", 3600),
        )


class SchemaChangeMonitor:
    """
    Monitors schema changes for active mapping specs and broadcasts notifications.

    Design:
    - Runs as a background task managed by BackgroundTaskManager
    - Periodically checks all active mapping specs against their source datasets
    - Broadcasts WebSocket notifications when breaking changes detected
    - Tracks notification history to avoid spam
    """

    def __init__(
        self,
        drift_detector: SchemaDriftDetector,
        dataset_registry: Any,  # DatasetRegistry
        objectify_registry: Any,  # ObjectifyRegistry
        websocket_service: Optional[Any] = None,  # WebSocketNotificationService
        config: Optional[MonitorConfig] = None,
    ):
        """
        Args:
            drift_detector: Schema drift detector service
            dataset_registry: Registry for dataset metadata
            objectify_registry: Registry for mapping specs
            websocket_service: Optional WebSocket service for notifications
            config: Monitor configuration
        """
        self.drift_detector = drift_detector
        self.dataset_registry = dataset_registry
        self.objectify_registry = objectify_registry
        self.websocket_service = websocket_service
        self.config = config or MonitorConfig()

        # Runtime state
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._last_check_time: Optional[datetime] = None
        self._notification_history: Dict[str, datetime] = {}  # subject_id -> last_notified
        self._known_hashes: Dict[str, str] = {}  # subject_id -> last_known_hash

        # Callbacks for custom handling
        self._on_drift_callbacks: List[Callable[[SchemaDrift, List[ImpactedMapping]], None]] = []

    def add_drift_callback(
        self,
        callback: Callable[[SchemaDrift, List[ImpactedMapping]], None],
    ) -> None:
        """Add a callback to be invoked when drift is detected"""
        self._on_drift_callbacks.append(callback)

    async def start(self) -> str:
        """
        Start the schema change monitor.

        Returns:
            Task ID for tracking
        """
        if self._running:
            logger.warning("Schema change monitor is already running")
            return "already_running"

        self._running = True
        self._task = asyncio.create_task(self._monitoring_loop())
        self._task.add_done_callback(self._on_task_done)

        logger.info(
            f"Schema change monitor started with {self.config.check_interval_seconds}s interval"
        )
        return "started"

    async def stop(self) -> None:
        """Stop the schema change monitor"""
        self._running = False

        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

        logger.info("Schema change monitor stopped")

    def _on_task_done(self, task: asyncio.Task) -> None:
        """Handle task completion"""
        try:
            exc = task.exception()
            if exc:
                logger.error(f"Schema change monitor task failed: {exc}")
        except asyncio.CancelledError:
            pass

    async def _monitoring_loop(self) -> None:
        """Main monitoring loop"""
        while self._running:
            try:
                await self._check_all_active_mappings()
                self._last_check_time = datetime.now(timezone.utc)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in schema monitoring loop: {e}")
                # Continue monitoring despite errors

            await asyncio.sleep(self.config.check_interval_seconds)

    async def _check_all_active_mappings(self) -> None:
        """Check all active mapping specs for schema drift"""
        try:
            # Get all active mapping specs
            mapping_specs = await self.objectify_registry.list_mapping_specs(
                include_inactive=False,
                limit=self.config.batch_size,
            )

            if not mapping_specs:
                logger.debug("No active mapping specs to monitor")
                return

            logger.debug(f"Checking {len(mapping_specs)} mapping specs for schema drift")

            for spec in mapping_specs:
                try:
                    await self._check_mapping_spec(spec)
                except Exception as e:
                    logger.error(f"Error checking mapping spec {spec.mapping_spec_id}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Error fetching mapping specs: {e}")

    async def _check_mapping_spec(self, spec: Any) -> None:
        """Check a single mapping spec for schema drift"""
        spec_id = str(spec.mapping_spec_id)
        dataset_id = str(spec.dataset_id)
        db_name = spec.db_name

        # Get current dataset schema
        try:
            dataset = await self.dataset_registry.get_dataset(dataset_id)
            if not dataset:
                return

            latest_version = await self.dataset_registry.get_latest_version(dataset_id)
            if not latest_version:
                return

            current_schema = latest_version.schema_json or []
            current_hash = latest_version.schema_hash

        except Exception as e:
            logger.debug(f"Could not fetch dataset for {spec_id}: {e}")
            return

        # Get previous known hash
        previous_hash = self._known_hashes.get(spec_id)

        # Quick check: if hash unchanged, skip detailed check
        if previous_hash == current_hash:
            return

        # Get previous schema if available
        previous_schema = None
        if hasattr(spec, "expected_schema_hash") and spec.expected_schema_hash:
            # Could fetch previous version here if needed
            pass

        # Detect drift
        drift = self.drift_detector.detect_drift(
            subject_type="dataset",
            subject_id=dataset_id,
            db_name=db_name,
            current_schema=current_schema,
            previous_schema=previous_schema,
            previous_hash=previous_hash,
        )

        # Update known hash
        self._known_hashes[spec_id] = current_hash

        if drift is None:
            return

        # Skip if this is first check and notify_on_first_check is False
        if previous_hash is None and not self.config.notify_on_first_check:
            logger.debug(f"First check for {spec_id}, skipping notification")
            return

        # Check if severity meets threshold
        if not self._should_notify(drift, spec_id):
            return

        # Analyze impact on this mapping spec
        impacted_mappings = await self.drift_detector.analyze_impact(
            drift,
            [{"mapping_spec_id": spec_id, "name": spec.name, "field_mappings": spec.field_mappings or []}],
        )

        # Broadcast notification
        await self._notify_drift(drift, impacted_mappings)

        # Invoke callbacks
        for callback in self._on_drift_callbacks:
            try:
                callback(drift, impacted_mappings)
            except Exception as e:
                logger.error(f"Drift callback error: {e}")

    def _should_notify(self, drift: SchemaDrift, subject_id: str) -> bool:
        """Determine if notification should be sent"""
        # Check severity threshold
        severity_order = {"info": 0, "warning": 1, "breaking": 2}
        min_level = severity_order.get(self.config.min_notify_severity, 1)
        drift_level = severity_order.get(drift.severity, 0)

        if drift_level < min_level:
            return False

        # Check notification cooldown
        last_notified = self._notification_history.get(subject_id)
        if last_notified:
            elapsed = (datetime.now(timezone.utc) - last_notified).total_seconds()
            if elapsed < self.config.notification_cooldown:
                return False

        return True

    async def _notify_drift(
        self,
        drift: SchemaDrift,
        impacted_mappings: List[ImpactedMapping],
    ) -> None:
        """Send drift notification"""
        # Update notification history
        self._notification_history[drift.subject_id] = datetime.now(timezone.utc)

        # Prepare payload
        payload = self.drift_detector.to_notification_payload(drift, impacted_mappings)

        logger.info(
            f"Schema drift detected for {drift.subject_type}/{drift.subject_id}: "
            f"severity={drift.severity}, changes={drift.change_summary}"
        )

        # Broadcast via WebSocket if available
        if self.websocket_service:
            try:
                await self.websocket_service.broadcast_to_channel(
                    channel=f"schema_changes:{drift.db_name}",
                    message=payload,
                )
            except Exception as e:
                logger.error(f"Failed to broadcast schema drift notification: {e}")

    async def check_mapping_spec_compatibility(
        self,
        mapping_spec_id: str,
    ) -> Dict[str, Any]:
        """
        On-demand compatibility check for a specific mapping spec.

        Returns:
            Compatibility status with details
        """
        try:
            spec = await self.objectify_registry.get_mapping_spec(mapping_spec_id)
            if not spec:
                payload = build_error_envelope(
                    service_name="schema_change_monitor",
                    message="Mapping spec not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                    category=ErrorCategory.RESOURCE,
                    status_code=404,
                    context={"mapping_spec_id": mapping_spec_id},
                    external_code="MAPPING_SPEC_NOT_FOUND",
                    prefer_status_code=True,
                )
                payload["error"] = "Mapping spec not found"
                return payload

            dataset = await self.dataset_registry.get_dataset(str(spec.dataset_id))
            if not dataset:
                payload = build_error_envelope(
                    service_name="schema_change_monitor",
                    message="Dataset not found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                    category=ErrorCategory.RESOURCE,
                    status_code=404,
                    context={"dataset_id": str(spec.dataset_id)},
                    external_code="DATASET_NOT_FOUND",
                    prefer_status_code=True,
                )
                payload["error"] = "Dataset not found"
                return payload

            latest_version = await self.dataset_registry.get_latest_version(str(spec.dataset_id))
            if not latest_version:
                payload = build_error_envelope(
                    service_name="schema_change_monitor",
                    message="No dataset version found",
                    code=ErrorCode.RESOURCE_NOT_FOUND,
                    category=ErrorCategory.RESOURCE,
                    status_code=404,
                    context={"dataset_id": str(spec.dataset_id)},
                    external_code="DATASET_VERSION_MISSING",
                    prefer_status_code=True,
                )
                payload["error"] = "No dataset version found"
                return payload

            current_schema = latest_version.schema_json or []
            expected_hash = getattr(spec, "expected_schema_hash", None)

            drift = self.drift_detector.detect_drift(
                subject_type="dataset",
                subject_id=str(spec.dataset_id),
                db_name=spec.db_name,
                current_schema=current_schema,
                previous_hash=expected_hash,
            )

            if drift is None:
                return {
                    "status": "compatible",
                    "message": "Schema matches expected state",
                    "current_hash": latest_version.schema_hash,
                }

            impacted = await self.drift_detector.analyze_impact(
                drift,
                [{"mapping_spec_id": mapping_spec_id, "field_mappings": spec.field_mappings or []}],
            )

            return {
                "status": "incompatible" if drift.is_breaking else "warning",
                "drift": self.drift_detector.to_notification_payload(drift, impacted),
                "recommendations": impacted[0].recommendations if impacted else [],
            }

        except Exception as e:
            logger.error(f"Compatibility check failed: {e}")
            payload = build_error_envelope(
                service_name="schema_change_monitor",
                message="Compatibility check failed",
                detail=str(e),
                code=ErrorCode.INTERNAL_ERROR,
                category=ErrorCategory.INTERNAL,
                status_code=500,
                context={"mapping_spec_id": mapping_spec_id},
                prefer_status_code=True,
            )
            payload["error"] = str(e)
            return payload

    def get_status(self) -> Dict[str, Any]:
        """Get monitor status"""
        return {
            "running": self._running,
            "last_check_time": self._last_check_time.isoformat() if self._last_check_time else None,
            "known_subjects_count": len(self._known_hashes),
            "notification_history_count": len(self._notification_history),
            "config": {
                "check_interval_seconds": self.config.check_interval_seconds,
                "min_notify_severity": self.config.min_notify_severity,
            },
        }
