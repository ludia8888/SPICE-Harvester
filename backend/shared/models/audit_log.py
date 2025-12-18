"""
Audit log models.

Audit logs are:
- Structured (schema), queryable (API), and durable (Postgres).
- Append-only at the application layer (no update/delete operations exposed).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, Literal, Optional

from pydantic import BaseModel, ConfigDict, Field


AuditStatus = Literal["success", "failure"]


class AuditLogEntry(BaseModel):
    audit_id: Optional[str] = Field(default=None, description="Audit entry id (uuid)")
    occurred_at: Optional[datetime] = Field(default=None, description="UTC timestamp")
    actor: Optional[str] = Field(default=None, description="Actor/user/service")

    action: str = Field(..., description="Action name (e.g. EVENT_APPENDED, ES_INDEX)")
    status: AuditStatus = Field(..., description="Result status")

    resource_type: Optional[str] = Field(default=None, description="Target resource type")
    resource_id: Optional[str] = Field(default=None, description="Target resource id")

    event_id: Optional[str] = Field(default=None, description="Related EventEnvelope.event_id")
    command_id: Optional[str] = Field(default=None, description="Related command id (if any)")
    trace_id: Optional[str] = Field(default=None, description="Trace id (if available)")
    correlation_id: Optional[str] = Field(default=None, description="Correlation id (if available)")

    metadata: Dict[str, Any] = Field(default_factory=dict)
    error: Optional[str] = None

    # Tamper-evident hash chain (optional, but enabled by default in the store).
    prev_hash: Optional[str] = None
    entry_hash: Optional[str] = None

    model_config = ConfigDict(extra="ignore")

