"""
Ontology models for SPICE HARVESTER
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class Cardinality(Enum):
    """Cardinality enumeration"""

    ONE_TO_ONE = "1:1"
    ONE_TO_MANY = "1:n"
    MANY_TO_ONE = "n:1"
    MANY_TO_MANY = "n:m"
    ONE = "one"
    MANY = "many"


# MultiLingualText removed - using simple strings for better stability


class QueryOperator(BaseModel):
    """Query operator definition"""

    name: str
    symbol: str
    description: str
    applies_to: List[str] = Field(default_factory=list)

    def can_apply_to(self, data_type: str) -> bool:
        """Check if operator can apply to data type"""
        return data_type in self.applies_to


class OntologyBase(BaseModel):
    """Base ontology model"""

    id: str = Field(..., description="Ontology identifier")
    label: str = Field(..., description="English label")
    description: Optional[str] = Field(None, description="English description")
    created_at: Optional[datetime] = Field(None, description="Creation timestamp")
    updated_at: Optional[datetime] = Field(None, description="Last update timestamp")
    parent_class: Optional[str] = Field(None, description="Parent class identifier")
    abstract: bool = Field(default=False, description="Whether class is abstract")
    properties: List['Property'] = Field(default_factory=list, description="Class properties")
    relationships: List['Relationship'] = Field(default_factory=list, description="Class relationships")
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    @field_validator("id")
    @classmethod
    def validate_id(cls, v) -> str:
        """Validate ID format"""
        if not v or not isinstance(v, str):
            raise ValueError("ID must be a non-empty string")
        return v

    @field_validator("label")
    @classmethod
    def validate_label(cls, v) -> str:
        """Validate label is not empty"""
        if not v or not isinstance(v, str):
            raise ValueError("Label must be a non-empty string")
        return v

    @model_validator(mode="before")
    @classmethod
    def set_timestamps(cls, values) -> Any:
        """Set timestamps if not provided"""
        if isinstance(values, dict):
            now = datetime.now()
            if "created_at" not in values or values["created_at"] is None:
                values["created_at"] = now
            if "updated_at" not in values or values["updated_at"] is None:
                values["updated_at"] = now
        return values


class Relationship(BaseModel):
    """Relationship model"""

    predicate: str = Field(..., description="Relationship predicate")
    target: str = Field(..., description="Target class")
    label: str = Field(..., description="English label")
    cardinality: str = Field(default="1:n", description="Relationship cardinality")
    description: Optional[str] = Field(None, description="English description")
    inverse_predicate: Optional[str] = Field(None, description="Inverse relationship predicate")
    inverse_label: Optional[str] = Field(None, description="Inverse relationship label")

    @field_validator("cardinality")
    @classmethod
    def validate_cardinality(cls, v) -> str:
        """Validate cardinality format"""
        valid_cardinalities = ["1:1", "1:n", "n:1", "n:m", "n:n"]  # 🔥 THINK ULTRA! Added n:n for OMS compatibility
        if v not in valid_cardinalities:
            raise ValueError(f"Invalid cardinality: {v}. Must be one of {valid_cardinalities}")
        # Normalize n:n to n:m for consistency
        if v == "n:n":
            return "n:m"
        return v

    def is_valid_cardinality(self) -> bool:
        """Check if cardinality is valid"""
        valid_cardinalities = ["1:1", "1:n", "n:1", "n:m"]
        return self.cardinality in valid_cardinalities


class Property(BaseModel):
    """Property model with class reference support"""

    name: str = Field(..., description="Property name")
    type: str = Field(..., description="Property data type or class reference")
    label: str = Field(..., description="English label")
    required: bool = Field(default=False, description="Whether property is required")
    default: Optional[Any] = Field(None, description="Default value")
    description: Optional[str] = Field(None, description="English description")
    constraints: Dict[str, Any] = Field(default_factory=dict, description="Property constraints")
    
    # 🔥 THINK ULTRA! Class reference support for ObjectProperty conversion
    target: Optional[str] = Field(None, description="Target class for link properties (BFF input)")
    linkTarget: Optional[str] = Field(None, description="Target class for object reference (when type is a class)")
    isRelationship: Optional[bool] = Field(None, description="Whether this property represents a relationship")
    cardinality: Optional[str] = Field(None, description="Relationship cardinality (1:1, 1:n, n:1, n:m)")
    
    # 🔥 THINK ULTRA! Array relationship support
    items: Optional[Dict[str, Any]] = Field(None, description="Array items definition for array type relationships")

    @field_validator("name")
    @classmethod
    def validate_name(cls, v) -> str:
        """Validate property name"""
        if not v or not isinstance(v, str):
            raise ValueError("Property name must be a non-empty string")
        return v

    @field_validator("type")
    @classmethod
    def validate_type(cls, v) -> str:
        """Validate property type"""
        if not v or not isinstance(v, str):
            raise ValueError("Property type must be a non-empty string")
        return v

    def validate_value(self, value: Any) -> List[str]:
        """Validate property value"""
        errors = []

        if self.required and value is None:
            errors.append(f"Property '{self.name}' is required")

        if value is not None:
            if self.type == "xsd:string" and not isinstance(value, str):
                errors.append(f"Property '{self.name}' must be a string")
            elif self.type == "xsd:integer" and not isinstance(value, int):
                errors.append(f"Property '{self.name}' must be an integer")
            elif self.type == "xsd:boolean" and not isinstance(value, bool):
                errors.append(f"Property '{self.name}' must be a boolean")

        if self.constraints and value is not None:
            if "min" in self.constraints and value < self.constraints["min"]:
                errors.append(f"Property '{self.name}' must be >= {self.constraints['min']}")
            if "max" in self.constraints and value > self.constraints["max"]:
                errors.append(f"Property '{self.name}' must be <= {self.constraints['max']}")
            if "pattern" in self.constraints:
                import re

                if not re.match(self.constraints["pattern"], str(value)):
                    errors.append(f"Property '{self.name}' does not match pattern")

        return errors
    
    def is_class_reference(self) -> bool:
        """Check if this property is a class reference (ObjectProperty)"""
        # 🔥 THINK ULTRA! type="link" 명시적 지원
        if self.type == "link":
            return True
            
        # 🔥 THINK ULTRA! Array relationship 지원
        if self.type == "array" and self.items:
            items_type = self.items.get("type")
            if items_type == "link" and (self.items.get("linkTarget") or self.items.get("target")):
                return True
            
        # 명시적으로 target, linkTarget이 설정되었거나 isRelationship가 True인 경우
        if self.target or self.linkTarget or self.isRelationship:
            return True
        
        # 🔥 ULTRA! Handle parameterized types like list<string>, set<integer>, etc.
        type_lower = self.type.lower()
        if type_lower.startswith(("list<", "set<", "array<", "optional<", "union<")) and type_lower.endswith(">"):
            # These are parameterized basic types, not class references
            return False
            
        # type이 기본 데이터 타입이 아닌 경우 (클래스명일 가능성)
        basic_types = {
            "STRING", "INTEGER", "DECIMAL", "BOOLEAN", "DATE", "DATETIME", "TIME", "FLOAT", "DOUBLE", "LONG", "TEXT",
            "ARRAY", "OBJECT", "ENUM", "EMAIL", "PHONE", "URL", "MONEY", "IP", "UUID",
            "COORDINATE", "ADDRESS", "NAME", "IMAGE", "FILE", "LINK", "SET", "LIST",  # LINK 추가
            "JSON", "GEOPOINT", "UNION", "OPTIONAL"  # 🔥 ULTRA! Added missing basic types
        }
        
        # xsd: 프리픽스가 있는 경우는 기본 타입
        if self.type.startswith("xsd:"):
            return False
            
        # 대문자로 변환하여 기본 타입 목록과 비교
        return self.type.upper() not in basic_types
    
    def to_relationship(self) -> Dict[str, Any]:
        """Convert property to relationship format"""
        # 🔥 THINK ULTRA! type="link"일 때 target 또는 linkTarget 사용
        if self.type == "link":
            if not self.target and not self.linkTarget:
                raise ValueError(f"Property '{self.name}' with type='link' must have target or linkTarget")
            target = self.target or self.linkTarget
            cardinality = self.cardinality or "n:1"  # link는 보통 n:1
        # 🔥 THINK ULTRA! Array relationship 지원
        elif self.type == "array" and self.items:
            items_type = self.items.get("type")
            if items_type == "link" and (self.items.get("linkTarget") or self.items.get("target")):
                target = self.items.get("target") or self.items["linkTarget"]
                cardinality = self.cardinality or "1:n"  # array는 보통 1:n
            else:
                raise ValueError(f"Property '{self.name}' with type='array' must have items.type='link' and items.linkTarget or items.target")
        else:
            target = self.target or self.linkTarget or self.type
            cardinality = self.cardinality or "1:n"
            
        return {
            "predicate": self.name,
            "target": target,
            "label": self.label,
            "cardinality": cardinality,
            "description": self.description
        }


class OntologyCreateRequest(BaseModel):
    """Request model for creating ontology"""

    id: Optional[str] = Field(None, description="Ontology identifier (auto-generated if not provided)")
    label: str = Field(..., description="English label")
    description: Optional[str] = Field(None, description="English description")
    parent_class: Optional[str] = Field(None, description="Parent class identifier")
    abstract: bool = Field(default=False, description="Whether class is abstract")
    properties: List[Property] = Field(default_factory=list, description="Class properties")
    relationships: List[Relationship] = Field(
        default_factory=list, description="Class relationships"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    @field_validator("id")
    @classmethod
    def validate_id(cls, v) -> str:
        """Validate ID format"""
        if not v or not isinstance(v, str):
            raise ValueError("ID must be a non-empty string")
        return v

    @field_validator("label")
    @classmethod
    def validate_label(cls, v) -> str:
        """Validate label is not empty"""
        if not v or not isinstance(v, str):
            raise ValueError("Label must be a non-empty string")
        return v

    @field_validator("properties")
    @classmethod
    def validate_properties(cls, v) -> List[Any]:
        """Validate properties don't have duplicate names"""
        if v:
            names = [p.name for p in v]
            if len(names) != len(set(names)):
                raise ValueError("Properties must have unique names")
        return v

    @field_validator("relationships")
    @classmethod
    def validate_relationships(cls, v) -> List[Any]:
        """Validate relationships don't have duplicate predicates"""
        if v:
            predicates = [r.predicate for r in v]
            if len(predicates) != len(set(predicates)):
                raise ValueError("Relationships must have unique predicates")
        return v


class OntologyUpdateRequest(BaseModel):
    """Request model for updating ontology"""

    label: Optional[str] = Field(None, description="English label")
    description: Optional[str] = Field(None, description="English description")
    parent_class: Optional[str] = Field(None, description="Parent class identifier")
    abstract: Optional[bool] = Field(None, description="Whether class is abstract")
    properties: Optional[List[Property]] = Field(None, description="Class properties")
    relationships: Optional[List[Relationship]] = Field(None, description="Class relationships")
    metadata: Optional[Dict[str, Any]] = Field(None, description="Additional metadata")

    @field_validator("properties")
    @classmethod
    def validate_properties(cls, v) -> List[Any]:
        """Validate properties don't have duplicate names"""
        if v:
            names = [p.name for p in v]
            if len(names) != len(set(names)):
                raise ValueError("Properties must have unique names")
        return v

    @field_validator("relationships")
    @classmethod
    def validate_relationships(cls, v) -> List[Any]:
        """Validate relationships don't have duplicate predicates"""
        if v:
            predicates = [r.predicate for r in v]
            if len(predicates) != len(set(predicates)):
                raise ValueError("Relationships must have unique predicates")
        return v

    def has_changes(self) -> bool:
        """Check if request has any changes"""
        return any(
            [
                self.label is not None,
                self.description is not None,
                self.parent_class is not None,
                self.abstract is not None,
                self.properties is not None,
                self.relationships is not None,
                self.metadata is not None,
            ]
        )


class OntologyResponse(OntologyBase):
    """Response model for ontology operations"""

    parent_class: Optional[str] = Field(None, description="Parent class identifier")
    abstract: bool = Field(default=False, description="Whether class is abstract")
    properties: List[Property] = Field(default_factory=list, description="Class properties")
    relationships: List[Relationship] = Field(
        default_factory=list, description="Class relationships"
    )
    metadata: Dict[str, Any] = Field(default_factory=dict, description="Additional metadata")

    def validate_structure(self) -> List[str]:
        """Validate ontology structure"""
        errors = []

        # Check property name uniqueness
        property_names = [p.name for p in self.properties]
        if len(property_names) != len(set(property_names)):
            errors.append("Duplicate property names found")

        # Check relationship predicate uniqueness
        predicates = [r.predicate for r in self.relationships]
        if len(predicates) != len(set(predicates)):
            errors.append("Duplicate relationship predicates found")

        # Validate all relationships have valid cardinalities
        for rel in self.relationships:
            if not rel.is_valid_cardinality():
                errors.append(
                    f"Invalid cardinality for relationship '{rel.predicate}': {rel.cardinality}"
                )

        return errors


class QueryFilter(BaseModel):
    """Query filter model"""

    field: str = Field(..., description="Field to filter on")
    operator: str = Field(..., description="Filter operator")
    value: Any = Field(..., description="Filter value")

    @field_validator("field")
    @classmethod
    def validate_field(cls, v) -> str:
        """Validate field name"""
        if not v or not isinstance(v, str):
            raise ValueError("Field must be a non-empty string")
        return v

    @field_validator("operator")
    @classmethod
    def validate_operator(cls, v) -> str:
        """Validate operator"""
        valid_operators = [
            "eq",
            "ne",
            "gt",
            "ge",
            "lt",
            "le",
            "like",
            "in",
            "not_in",
            "is_null",
            "is_not_null",
        ]
        if v not in valid_operators:
            raise ValueError(f"Invalid operator: {v}. Must be one of {valid_operators}")
        return v


class QueryInput(BaseModel):
    """Query input model"""

    class_label: Optional[str] = Field(None, description="Class label to query")
    class_id: Optional[str] = Field(None, description="Class ID to query")
    filters: List[QueryFilter] = Field(default_factory=list, description="Query filters")
    select: Optional[List[str]] = Field(None, description="Fields to select")
    limit: Optional[int] = Field(None, description="Maximum number of results")
    offset: Optional[int] = Field(None, description="Number of results to skip")
    order_by: Optional[str] = Field(None, description="Field to order by")
    order_direction: Optional[str] = Field(default="asc", description="Order direction")

    @field_validator("limit")
    @classmethod
    def validate_limit(cls, v) -> int:
        """Validate limit"""
        if v is not None and (not isinstance(v, int) or v < 0):
            raise ValueError("Limit must be a non-negative integer")
        return v

    @field_validator("offset")
    @classmethod
    def validate_offset(cls, v) -> int:
        """Validate offset"""
        if v is not None and (not isinstance(v, int) or v < 0):
            raise ValueError("Offset must be a non-negative integer")
        return v

    @field_validator("order_direction")
    @classmethod
    def validate_order_direction(cls, v) -> str:
        """Validate order direction"""
        if v not in ["asc", "desc"]:
            raise ValueError("Order direction must be 'asc' or 'desc'")
        return v

    @model_validator(mode="after")
    def validate_class_identifier(self) -> str:
        """Validate that either class_label or class_id is provided"""
        if not self.class_label and not self.class_id:
            raise ValueError("Either class_label or class_id must be provided")
        return self


# BFF aliases - to avoid code duplication
OntologyCreateRequestBFF = OntologyCreateRequest
OntologyUpdateInput = OntologyUpdateRequest
QueryRequest = QueryInput
QueryRequestInternal = QueryInput  # Internal query request
QueryResponse = dict  # Generic dict for query responses
