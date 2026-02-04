"""
Schema changes domain logic (BFF).

Extracted from `bff.routers.schema_changes` to keep routers thin and to
centralize SQL + formatting logic behind a small service facade.
"""

import logging
from datetime import datetime
from typing import Any, Dict, List, Optional
from uuid import uuid4

from fastapi import HTTPException, status

from shared.models.requests import ApiResponse

logger = logging.getLogger(__name__)


async def list_schema_changes(
    *,
    pool: Any,
    db_name: str,
    subject_type: Optional[str],
    subject_id: Optional[str],
    severity: Optional[str],
    since: Optional[datetime],
    limit: int,
    offset: int,
) -> Dict[str, Any]:
    try:
        query = """
            SELECT drift_id, subject_type, subject_id, db_name,
                   previous_hash, current_hash, drift_type, severity,
                   changes, detected_at, acknowledged_at, acknowledged_by
            FROM schema_drift_history
            WHERE db_name = $1
        """
        params: List[Any] = [db_name]
        param_idx = 2

        if subject_type:
            query += f" AND subject_type = ${param_idx}"
            params.append(subject_type)
            param_idx += 1

        if subject_id:
            query += f" AND subject_id = ${param_idx}"
            params.append(subject_id)
            param_idx += 1

        if severity:
            query += f" AND severity = ${param_idx}"
            params.append(severity)
            param_idx += 1

        if since:
            query += f" AND detected_at >= ${param_idx}"
            params.append(since)
            param_idx += 1

        query += f" ORDER BY detected_at DESC LIMIT ${param_idx} OFFSET ${param_idx + 1}"
        params.extend([limit, offset])

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        changes = [
            {
                "drift_id": str(row["drift_id"]),
                "subject_type": row["subject_type"],
                "subject_id": row["subject_id"],
                "db_name": row["db_name"],
                "previous_hash": row["previous_hash"],
                "current_hash": row["current_hash"],
                "drift_type": row["drift_type"],
                "severity": row["severity"],
                "changes": row["changes"] or [],
                "detected_at": row["detected_at"].isoformat() if row["detected_at"] else None,
                "acknowledged_at": row["acknowledged_at"].isoformat() if row["acknowledged_at"] else None,
                "acknowledged_by": row["acknowledged_by"],
            }
            for row in rows
        ]

        return ApiResponse.success(
            message="Schema changes fetched",
            data={"items": changes, "count": len(changes)},
        ).to_dict()

    except Exception as exc:
        if "does not exist" in str(exc):
            return ApiResponse.success(
                message="No schema change history available",
                data={"items": [], "count": 0},
            ).to_dict()
        logger.exception("Failed to list schema changes")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def acknowledge_drift(
    *,
    pool: Any,
    drift_id: str,
    acknowledged_by: str,
) -> Dict[str, Any]:
    try:
        query = """
            UPDATE schema_drift_history
            SET acknowledged_at = NOW(), acknowledged_by = $2
            WHERE drift_id = $1::uuid
            RETURNING drift_id
        """

        async with pool.acquire() as conn:
            result = await conn.fetchrow(query, drift_id, acknowledged_by)

        if not result:
            raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Drift {drift_id} not found")

        return ApiResponse.success(
            message="Drift acknowledged",
            data={"drift_id": drift_id, "acknowledged_by": acknowledged_by},
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to acknowledge drift")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def list_subscriptions(
    *,
    pool: Any,
    user_id: str,
    db_name: Optional[str],
    status_filter: Optional[str],
    limit: int,
) -> Dict[str, Any]:
    try:
        query = """
            SELECT subscription_id, user_id, subject_type, subject_id, db_name,
                   severity_filter, notification_channels, status, created_at, updated_at
            FROM schema_subscriptions
            WHERE user_id = $1
        """
        params: List[Any] = [user_id]
        param_idx = 2

        if db_name:
            query += f" AND db_name = ${param_idx}"
            params.append(db_name)
            param_idx += 1

        if status_filter:
            query += f" AND status = ${param_idx}"
            params.append(status_filter)
            param_idx += 1

        query += f" ORDER BY created_at DESC LIMIT ${param_idx}"
        params.append(limit)

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, *params)

        subscriptions = [
            {
                "subscription_id": str(row["subscription_id"]),
                "user_id": row["user_id"],
                "subject_type": row["subject_type"],
                "subject_id": row["subject_id"],
                "db_name": row["db_name"],
                "severity_filter": row["severity_filter"],
                "notification_channels": row["notification_channels"],
                "status": row["status"],
                "created_at": row["created_at"].isoformat() if row["created_at"] else None,
            }
            for row in rows
        ]

        return ApiResponse.success(
            message="Subscriptions fetched",
            data={"items": subscriptions, "count": len(subscriptions)},
        ).to_dict()

    except Exception as exc:
        if "does not exist" in str(exc):
            return ApiResponse.success(
                message="No subscriptions available",
                data={"items": [], "count": 0},
            ).to_dict()
        logger.exception("Failed to list subscriptions")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def create_subscription(
    *,
    pool: Any,
    user_id: str,
    subject_type: str,
    subject_id: str,
    db_name: str,
    severity_filter: List[str],
    notification_channels: List[str],
) -> Dict[str, Any]:
    try:
        subscription_id = str(uuid4())

        query = """
            INSERT INTO schema_subscriptions (
                subscription_id, user_id, subject_type, subject_id, db_name,
                severity_filter, notification_channels, status
            )
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7, 'ACTIVE')
            ON CONFLICT (user_id, subject_type, subject_id)
            DO UPDATE SET
                severity_filter = EXCLUDED.severity_filter,
                notification_channels = EXCLUDED.notification_channels,
                status = 'ACTIVE',
                updated_at = NOW()
            RETURNING subscription_id, created_at
        """

        async with pool.acquire() as conn:
            result = await conn.fetchrow(
                query,
                subscription_id,
                user_id,
                subject_type,
                subject_id,
                db_name,
                severity_filter,
                notification_channels,
            )

        return ApiResponse.success(
            message="Subscription created",
            data={
                "subscription_id": str(result["subscription_id"]),
                "user_id": user_id,
                "subject_type": subject_type,
                "subject_id": subject_id,
                "db_name": db_name,
                "severity_filter": severity_filter,
                "notification_channels": notification_channels,
                "status": "ACTIVE",
            },
        ).to_dict()

    except Exception as exc:
        logger.exception("Failed to create subscription")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def delete_subscription(
    *,
    pool: Any,
    user_id: str,
    subscription_id: str,
) -> Dict[str, Any]:
    try:
        query = """
            UPDATE schema_subscriptions
            SET status = 'DELETED', updated_at = NOW()
            WHERE subscription_id = $1::uuid AND user_id = $2
            RETURNING subscription_id
        """

        async with pool.acquire() as conn:
            result = await conn.fetchrow(query, subscription_id, user_id)

        if not result:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Subscription {subscription_id} not found",
            )

        return ApiResponse.success(
            message="Subscription deleted",
            data={"subscription_id": subscription_id},
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to delete subscription")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def check_mapping_compatibility(
    *,
    mapping_spec_id: str,
    db_name: str,
    dataset_version_id: Optional[str],
    dataset_registry: Any,
    objectify_registry: Any,
    detector: Any,
) -> Dict[str, Any]:
    try:
        mapping_spec = await objectify_registry.get_mapping_spec(mapping_spec_id=mapping_spec_id)
        if not mapping_spec:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Mapping spec {mapping_spec_id} not found",
            )

        dataset_id = mapping_spec.dataset_id
        if dataset_version_id:
            version = await dataset_registry.get_version(version_id=dataset_version_id)
            if not version:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Dataset version {dataset_version_id} not found",
                )
            sample = version.sample_json or {}
            if isinstance(sample, dict) and isinstance(sample.get("columns"), list):
                current_schema = sample.get("columns") or []
            else:
                schema_json = sample.get("schema_json") if isinstance(sample, dict) else None
                current_schema = (schema_json or {}).get("columns") if isinstance(schema_json, dict) else []
        else:
            version = await dataset_registry.get_latest_version(dataset_id=str(dataset_id))
            if version:
                sample = version.sample_json or {}
                if isinstance(sample, dict) and isinstance(sample.get("columns"), list):
                    current_schema = sample.get("columns") or []
                else:
                    schema_json = sample.get("schema_json") if isinstance(sample, dict) else None
                    current_schema = (schema_json or {}).get("columns") if isinstance(schema_json, dict) else []
            else:
                dataset = await dataset_registry.get_dataset(dataset_id=str(dataset_id))
                if not dataset:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail=f"Dataset {dataset_id} not found",
                    )
                schema_json = dataset.schema_json or {}
                current_schema = schema_json.get("columns") or schema_json.get("fields") or []

        if not isinstance(current_schema, list):
            current_schema = []
        current_schema = [c for c in current_schema if isinstance(c, dict)]

        expected_hash = getattr(mapping_spec, "expected_schema_hash", None) or mapping_spec.schema_hash

        drift = detector.detect_drift(
            subject_type="mapping_spec",
            subject_id=mapping_spec_id,
            db_name=db_name,
            current_schema=current_schema,
            previous_hash=expected_hash,
        )

        if drift is None:
            return ApiResponse.success(
                message="Mapping spec is compatible",
                data={
                    "is_compatible": True,
                    "has_drift": False,
                    "mapping_spec_id": mapping_spec_id,
                    "dataset_id": dataset_id,
                },
            ).to_dict()

        recommendations = []
        if drift.is_breaking:
            recommendations.append("Review and update the mapping spec to handle schema changes")
            for change in drift.changes:
                if change.change_type == "column_removed":
                    recommendations.append(f"Column '{change.column_name}' was removed - update mappings")
                elif change.change_type == "type_changed":
                    recommendations.append(
                        f"Column '{change.column_name}' type changed from {change.old_value} to {change.new_value}"
                    )

        return ApiResponse.success(
            message="Schema drift detected",
            data={
                "is_compatible": not drift.is_breaking,
                "has_drift": True,
                "drift_type": drift.drift_type,
                "severity": drift.severity,
                "changes": [
                    {
                        "change_type": c.change_type,
                        "column_name": c.column_name,
                        "old_value": c.old_value,
                        "new_value": c.new_value,
                        "impact": c.impact,
                    }
                    for c in drift.changes
                ],
                "recommendations": recommendations,
                "mapping_spec_id": mapping_spec_id,
                "dataset_id": dataset_id,
            },
        ).to_dict()

    except HTTPException:
        raise
    except Exception as exc:
        logger.exception("Failed to check mapping compatibility")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc


async def get_schema_change_stats(
    *,
    pool: Any,
    db_name: str,
    days: int,
) -> Dict[str, Any]:
    try:
        query = """
            SELECT
                severity,
                subject_type,
                COUNT(*) as count,
                COUNT(*) FILTER (WHERE acknowledged_at IS NULL) as unacknowledged
            FROM schema_drift_history
            WHERE db_name = $1 AND detected_at >= NOW() - ($2 || ' days')::interval
            GROUP BY severity, subject_type
            ORDER BY severity, subject_type
        """

        async with pool.acquire() as conn:
            rows = await conn.fetch(query, db_name, str(days))

        stats = {
            "by_severity": {},
            "by_subject_type": {},
            "total": 0,
            "total_unacknowledged": 0,
        }

        for row in rows:
            severity = row["severity"]
            subject_type = row["subject_type"]
            count = row["count"]
            unack = row["unacknowledged"]

            stats["total"] += count
            stats["total_unacknowledged"] += unack

            if severity not in stats["by_severity"]:
                stats["by_severity"][severity] = {"count": 0, "unacknowledged": 0}
            stats["by_severity"][severity]["count"] += count
            stats["by_severity"][severity]["unacknowledged"] += unack

            if subject_type not in stats["by_subject_type"]:
                stats["by_subject_type"][subject_type] = {"count": 0, "unacknowledged": 0}
            stats["by_subject_type"][subject_type]["count"] += count
            stats["by_subject_type"][subject_type]["unacknowledged"] += unack

        return ApiResponse.success(
            message="Schema change stats fetched",
            data={"db_name": db_name, "days": days, "stats": stats},
        ).to_dict()

    except Exception as exc:
        if "does not exist" in str(exc):
            return ApiResponse.success(
                message="No schema change history available",
                data={
                    "db_name": db_name,
                    "days": days,
                    "stats": {
                        "by_severity": {},
                        "by_subject_type": {},
                        "total": 0,
                        "total_unacknowledged": 0,
                    },
                },
            ).to_dict()
        logger.exception("Failed to get schema change stats")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)) from exc

