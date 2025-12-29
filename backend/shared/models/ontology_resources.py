"""
Ontology resource models (shared properties, value types, interfaces, groups, functions, action types).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, field_validator, model_validator

from .i18n import LocalizedText
from .ontology import Property, Relationship


class OntologyResourceBase(BaseModel):
    """Base fields shared by ontology resource definitions."""

    id: str = Field(..., description="Resource identifier")
    label: LocalizedText = Field(..., description="UI label (string or language map)")
    description: Optional[LocalizedText] = Field(None, description="UI description (string or language map)")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Optional metadata")
    spec: Dict[str, Any] = Field(default_factory=dict, description="Resource-specific spec payload")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")

    @field_validator("id")
    @classmethod
    def _validate_id(cls, value: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError("Resource id must be a non-empty string")
        return value.strip()

    @model_validator(mode="before")
    @classmethod
    def _set_timestamps(cls, values: Any) -> Any:
        if isinstance(values, dict):
            now = datetime.now(timezone.utc)
            created_at = values.get("created_at")
            updated_at = values.get("updated_at")
            if isinstance(created_at, datetime) and created_at.tzinfo is None:
                values["created_at"] = created_at.replace(tzinfo=timezone.utc)
            if isinstance(updated_at, datetime) and updated_at.tzinfo is None:
                values["updated_at"] = updated_at.replace(tzinfo=timezone.utc)
            if not values.get("created_at"):
                values["created_at"] = now
            if not values.get("updated_at"):
                values["updated_at"] = now
        return values


class SharedPropertyDefinition(OntologyResourceBase):
    """Reusable property templates for object types."""

    properties: List[Property] = Field(default_factory=list, description="Reusable property definitions")


class ValueTypeDefinition(OntologyResourceBase):
    """Semantic value type definition (e.g., Money, GeoPoint)."""

    base_type: Optional[str] = Field(None, description="Underlying primitive type")
    format: Optional[str] = Field(None, description="Display/validation format")
    unit: Optional[str] = Field(None, description="Unit (if applicable)")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Validation constraints")


class InterfaceDefinition(OntologyResourceBase):
    """Interface-style contract shared across object types."""

    required_properties: List[Property] = Field(default_factory=list)
    required_relationships: List[Relationship] = Field(default_factory=list)


class GroupDefinition(OntologyResourceBase):
    """Grouping / module metadata for ontology resources."""

    members: List[str] = Field(default_factory=list, description="Member resource ids")


class FunctionDefinition(OntologyResourceBase):
    """Derived field/function definition."""

    expression: str = Field(..., description="Expression/DSL body")
    language: Optional[str] = Field("expression", description="Expression language/engine")
    dependencies: List[str] = Field(default_factory=list, description="Dependent resource ids")
    deterministic: bool = Field(True, description="Whether the function is deterministic")


class ActionTypeDefinition(OntologyResourceBase):
    """Action template definition."""

    input_schema: Dict[str, Any] = Field(default_factory=dict)
    output_schema: Dict[str, Any] = Field(default_factory=dict)
    permission_policy: Dict[str, Any] = Field(default_factory=dict)
    side_effects: List[str] = Field(default_factory=list)


class OntologyResourceRecord(BaseModel):
    """Standardized API shape for ontology resources."""

    resource_type: str = Field(..., description="Resource type discriminator")
    resource: OntologyResourceBase = Field(..., description="Resource payload")
