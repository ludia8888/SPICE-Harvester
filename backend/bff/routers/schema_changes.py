"""
Schema Changes API Router

Provides REST endpoints for:
- Querying schema drift history
- Managing schema change subscriptions
- Checking mapping spec compatibility
"""

from __future__ import annotations
from shared.observability.tracing import trace_endpoint

import logging
from datetime import datetime
from typing import Optional

from fastapi import APIRouter, Query, Request

from bff.routers.registry_deps import get_dataset_registry, get_objectify_registry
from bff.schemas.schema_changes_requests import (
    AcknowledgeRequest,
    CompatibilityCheckRequest,
    CompatibilityCheckResponse,
    SchemaChangeItem,
    SubscriptionCreateRequest,
    SubscriptionResponse,
)
from bff.services import schema_changes_service
from shared.services.core.schema_drift_detector import SchemaDriftDetector

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/schema-changes", tags=["Schema Changes"])


# ----- Endpoints -----


@router.get("/history")
@trace_endpoint("bff.schema_changes.list_schema_changes")
async def list_schema_changes(
    db_name: str = Query(..., description="Database name"),
    subject_type: Optional[str] = Query(None, description="Filter by subject type (dataset, mapping_spec)"),
    subject_id: Optional[str] = Query(None, description="Filter by subject ID"),
    severity: Optional[str] = Query(None, description="Filter by severity (info, warning, breaking)"),
    since: Optional[datetime] = Query(None, description="Only return changes after this time"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
):
    """
    List schema drift history for a database.

    Returns all detected schema changes, optionally filtered by subject and severity.
    """
    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.list_schema_changes(
        pool=pool,
        db_name=db_name,
        subject_type=subject_type,
        subject_id=subject_id,
        severity=severity,
        since=since,
        limit=limit,
        offset=offset,
    )


@router.put("/drifts/{drift_id}/acknowledge")
@trace_endpoint("bff.schema_changes.acknowledge_drift")
async def acknowledge_drift(
    drift_id: str,
    request: AcknowledgeRequest,
):
    """
    Acknowledge a schema drift.

    Marks the drift as reviewed by the specified user.
    """
    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.acknowledge_drift(
        pool=pool,
        drift_id=drift_id,
        acknowledged_by=request.acknowledged_by,
    )


@router.get("/subscriptions")
@trace_endpoint("bff.schema_changes.list_subscriptions")
async def list_subscriptions(
    request: Request,
    db_name: Optional[str] = Query(None, description="Filter by database name"),
    status_filter: Optional[str] = Query(None, alias="status", description="Filter by status (ACTIVE, PAUSED)"),
    limit: int = Query(50, ge=1, le=500),
):
    """
    List schema change subscriptions for the current user.
    """
    user_id = request.headers.get("X-User-Id", "anonymous")

    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.list_subscriptions(
        pool=pool,
        user_id=user_id,
        db_name=db_name,
        status_filter=status_filter,
        limit=limit,
    )


@router.post("/subscriptions")
@trace_endpoint("bff.schema_changes.create_subscription")
async def create_subscription(
    request: Request,
    body: SubscriptionCreateRequest,
):
    """
    Create a new schema change subscription.

    Subscribes to schema drift notifications for a specific dataset or mapping spec.
    """
    user_id = request.headers.get("X-User-Id", "anonymous")

    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.create_subscription(
        pool=pool,
        user_id=user_id,
        subject_type=body.subject_type,
        subject_id=body.subject_id,
        db_name=body.db_name,
        severity_filter=body.severity_filter,
        notification_channels=body.notification_channels,
    )


@router.delete("/subscriptions/{subscription_id}")
@trace_endpoint("bff.schema_changes.delete_subscription")
async def delete_subscription(
    request: Request,
    subscription_id: str,
):
    """
    Delete a schema change subscription.
    """
    user_id = request.headers.get("X-User-Id", "anonymous")

    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.delete_subscription(
        pool=pool,
        user_id=user_id,
        subscription_id=subscription_id,
    )


@router.get("/mappings/{mapping_spec_id}/compatibility")
@trace_endpoint("bff.schema_changes.check_mapping_compatibility")
async def check_mapping_compatibility(
    mapping_spec_id: str,
    db_name: str = Query(..., description="Database name"),
    dataset_version_id: Optional[str] = Query(None, description="Specific dataset version to check (default: latest)"),
):
    """
    Check if a mapping spec is compatible with the current dataset schema.

    Returns drift information if the schema has changed since the mapping was created.
    """
    dataset_registry = await get_dataset_registry()
    objectify_registry = await get_objectify_registry()
    detector = SchemaDriftDetector()
    return await schema_changes_service.check_mapping_compatibility(
        mapping_spec_id=mapping_spec_id,
        db_name=db_name,
        dataset_version_id=dataset_version_id,
        dataset_registry=dataset_registry,
        objectify_registry=objectify_registry,
        detector=detector,
    )


@router.get("/stats")
@trace_endpoint("bff.schema_changes.get_schema_change_stats")
async def get_schema_change_stats(
    db_name: str = Query(..., description="Database name"),
    days: int = Query(30, ge=1, le=365, description="Number of days to analyze"),
):
    """
    Get schema change statistics for a database.

    Returns aggregate counts by severity and subject type.
    """
    dataset_registry = await get_dataset_registry()
    pool = dataset_registry._pool
    return await schema_changes_service.get_schema_change_stats(pool=pool, db_name=db_name, days=days)
