"""
Schema Drift Detector Service

Detects and analyzes schema changes between versions:
- Column additions, removals, renames
- Type changes
- Impact analysis on dependent mapping specs
"""

import logging
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field
from datetime import datetime
from difflib import SequenceMatcher

from shared.utils.schema_hash import compute_schema_hash

logger = logging.getLogger(__name__)


@dataclass
class SchemaChange:
    """Individual schema change detail"""
    change_type: str  # "column_added", "column_removed", "type_changed", "column_renamed"
    column_name: str
    old_value: Optional[Any] = None  # Old type, or old column def for removed
    new_value: Optional[Any] = None  # New type, or new column def for added
    impact: str = "none"  # "none", "mapping_invalid", "type_mismatch", "data_loss"


@dataclass
class SchemaDrift:
    """Detected schema drift between two versions"""
    subject_type: str  # "dataset", "mapping_spec", "object_type"
    subject_id: str
    db_name: str
    previous_hash: Optional[str]
    current_hash: str
    drift_type: str  # Primary drift type: "column_added", "column_removed", "type_changed", "mixed"
    changes: List[SchemaChange] = field(default_factory=list)
    severity: str = "info"  # "info", "warning", "breaking"
    detected_at: datetime = field(default_factory=datetime.utcnow)

    @property
    def is_breaking(self) -> bool:
        return self.severity == "breaking"

    @property
    def change_summary(self) -> Dict[str, int]:
        """Count of changes by type"""
        summary: Dict[str, int] = {}
        for change in self.changes:
            summary[change.change_type] = summary.get(change.change_type, 0) + 1
        return summary


@dataclass
class ImpactedMapping:
    """Mapping spec impacted by schema drift"""
    mapping_spec_id: str
    mapping_spec_name: str
    impacted_fields: List[str]
    impact_severity: str  # "none", "warning", "breaking"
    recommendations: List[str] = field(default_factory=list)


@dataclass
class SchemaDriftConfig:
    """Configuration for schema drift detection"""
    # Column similarity threshold for rename detection
    rename_similarity_threshold: float = 0.8

    # Breaking change patterns (column names that if removed, are breaking)
    critical_column_patterns: Set[str] = field(default_factory=lambda: {
        r"^id$", r"_id$", r"^pk_", r"_pk$",  # Primary keys
    })

    # Type compatibility matrix (old_type -> allowed new_types)
    type_compatibility: Dict[str, Set[str]] = field(default_factory=lambda: {
        "xsd:integer": {"xsd:integer", "xsd:decimal", "xsd:string"},
        "xsd:decimal": {"xsd:decimal", "xsd:string"},
        "xsd:string": {"xsd:string"},
        "xsd:boolean": {"xsd:boolean", "xsd:string"},
        "xsd:date": {"xsd:date", "xsd:dateTime", "xsd:string"},
        "xsd:dateTime": {"xsd:dateTime", "xsd:string"},
    })

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SchemaDriftConfig":
        return cls(
            rename_similarity_threshold=config_dict.get("rename_similarity_threshold", 0.8),
            critical_column_patterns=set(config_dict.get("critical_column_patterns", [])),
            type_compatibility=config_dict.get("type_compatibility", cls.__dataclass_fields__["type_compatibility"].default_factory()),
        )


class SchemaDriftDetector:
    """
    Detects schema drift and analyzes impact on dependent resources.

    Features:
    - Detects column additions, removals, renames, type changes
    - Classifies severity (info, warning, breaking)
    - Analyzes impact on mapping specs
    - Generates recommendations for remediation
    """

    def __init__(
        self,
        config: Optional[SchemaDriftConfig] = None,
    ):
        self.config = config or SchemaDriftConfig()

    def detect_drift(
        self,
        subject_type: str,
        subject_id: str,
        db_name: str,
        current_schema: List[Dict[str, Any]],
        previous_schema: Optional[List[Dict[str, Any]]] = None,
        previous_hash: Optional[str] = None,
    ) -> Optional[SchemaDrift]:
        """
        Detect schema drift between previous and current schema.

        Args:
            subject_type: Type of subject ("dataset", "mapping_spec", etc.)
            subject_id: ID of the subject
            db_name: Database name
            current_schema: Current schema (list of column defs)
            previous_schema: Previous schema (if available)
            previous_hash: Previous schema hash (for quick comparison)

        Returns:
            SchemaDrift if drift detected, None otherwise
        """
        current_hash = compute_schema_hash(current_schema)

        # Quick check: if hashes match, no drift
        if previous_hash and current_hash == previous_hash:
            return None

        # If no previous schema provided, we can't detect specific changes
        if previous_schema is None:
            if previous_hash and previous_hash != current_hash:
                # Hash changed but no schema to compare
                return SchemaDrift(
                    subject_type=subject_type,
                    subject_id=subject_id,
                    db_name=db_name,
                    previous_hash=previous_hash,
                    current_hash=current_hash,
                    drift_type="unknown",
                    changes=[],
                    severity="warning",
                )
            return None

        # Detect specific changes
        changes = self._detect_changes(previous_schema, current_schema)

        if not changes:
            return None

        # Classify drift type and severity
        drift_type = self._classify_drift_type(changes)
        severity = self._classify_severity(changes)

        return SchemaDrift(
            subject_type=subject_type,
            subject_id=subject_id,
            db_name=db_name,
            previous_hash=previous_hash,
            current_hash=current_hash,
            drift_type=drift_type,
            changes=changes,
            severity=severity,
        )

    def _detect_changes(
        self,
        old_schema: List[Dict[str, Any]],
        new_schema: List[Dict[str, Any]],
    ) -> List[SchemaChange]:
        """Detect individual schema changes"""
        changes: List[SchemaChange] = []

        old_columns = {col.get("name"): col for col in old_schema}
        new_columns = {col.get("name"): col for col in new_schema}

        old_names = set(old_columns.keys())
        new_names = set(new_columns.keys())

        # Removed columns
        removed = old_names - new_names
        # Added columns
        added = new_names - old_names
        # Common columns (check for type changes)
        common = old_names & new_names

        # Check for renames (removed + added with similar names/types)
        renames = self._detect_renames(
            [old_columns[name] for name in removed],
            [new_columns[name] for name in added],
        )

        renamed_old = {r[0] for r in renames}
        renamed_new = {r[1] for r in renames}

        # Process renames
        for old_name, new_name in renames:
            changes.append(SchemaChange(
                change_type="column_renamed",
                column_name=old_name,
                old_value=old_name,
                new_value=new_name,
                impact="mapping_invalid" if self._is_critical_column(old_name) else "warning",
            ))

        # Process pure removals (not renames)
        for name in removed - renamed_old:
            col = old_columns[name]
            impact = "data_loss" if self._is_critical_column(name) else "mapping_invalid"
            changes.append(SchemaChange(
                change_type="column_removed",
                column_name=name,
                old_value=col,
                new_value=None,
                impact=impact,
            ))

        # Process pure additions (not renames)
        for name in added - renamed_new:
            col = new_columns[name]
            changes.append(SchemaChange(
                change_type="column_added",
                column_name=name,
                old_value=None,
                new_value=col,
                impact="none",
            ))

        # Check type changes in common columns
        for name in common:
            old_col = old_columns[name]
            new_col = new_columns[name]

            old_type = old_col.get("type", "xsd:string")
            new_type = new_col.get("type", "xsd:string")

            if old_type != new_type:
                impact = self._assess_type_change_impact(old_type, new_type)
                changes.append(SchemaChange(
                    change_type="type_changed",
                    column_name=name,
                    old_value=old_type,
                    new_value=new_type,
                    impact=impact,
                ))

        return changes

    def _detect_renames(
        self,
        removed_cols: List[Dict[str, Any]],
        added_cols: List[Dict[str, Any]],
    ) -> List[Tuple[str, str]]:
        """Detect potential column renames"""
        renames: List[Tuple[str, str]] = []

        if not removed_cols or not added_cols:
            return renames

        for removed in removed_cols:
            removed_name = removed.get("name", "")
            removed_type = removed.get("type", "")

            best_match: Optional[Tuple[float, str]] = None

            for added in added_cols:
                added_name = added.get("name", "")
                added_type = added.get("type", "")

                # Types must match or be compatible
                if removed_type != added_type:
                    continue

                # Calculate name similarity
                similarity = SequenceMatcher(
                    None,
                    removed_name.lower(),
                    added_name.lower()
                ).ratio()

                if similarity >= self.config.rename_similarity_threshold:
                    if best_match is None or similarity > best_match[0]:
                        best_match = (similarity, added_name)

            if best_match:
                renames.append((removed_name, best_match[1]))

        return renames

    def _is_critical_column(self, column_name: str) -> bool:
        """Check if column is critical (e.g., primary key)"""
        import re
        name_lower = column_name.lower()
        for pattern in self.config.critical_column_patterns:
            if re.search(pattern, name_lower):
                return True
        return False

    def _assess_type_change_impact(self, old_type: str, new_type: str) -> str:
        """Assess impact of type change"""
        compatible_types = self.config.type_compatibility.get(old_type, set())

        if new_type in compatible_types:
            return "none"  # Widening conversion, safe
        elif old_type in self.config.type_compatibility.get(new_type, set()):
            return "type_mismatch"  # Narrowing conversion, potential data loss
        else:
            return "data_loss"  # Incompatible types

    def _classify_drift_type(self, changes: List[SchemaChange]) -> str:
        """Classify the primary drift type"""
        types = set(c.change_type for c in changes)

        if len(types) > 1:
            return "mixed"
        elif types:
            return types.pop()
        else:
            return "unknown"

    def _classify_severity(self, changes: List[SchemaChange]) -> str:
        """Classify overall severity of drift"""
        has_breaking = any(c.impact in ("data_loss", "mapping_invalid") for c in changes)
        has_warning = any(c.impact in ("type_mismatch", "warning") for c in changes)

        # Breaking: column removals, type narrowing, critical column changes
        if has_breaking:
            return "breaking"
        elif has_warning:
            return "warning"
        else:
            return "info"

    async def analyze_impact(
        self,
        drift: SchemaDrift,
        mapping_specs: List[Dict[str, Any]],
    ) -> List[ImpactedMapping]:
        """
        Analyze impact of schema drift on mapping specs.

        Args:
            drift: Detected schema drift
            mapping_specs: List of mapping specs that reference the drifted subject

        Returns:
            List of impacted mappings with recommendations
        """
        impacted: List[ImpactedMapping] = []

        # Get affected column names
        removed_columns = {
            c.column_name for c in drift.changes
            if c.change_type in ("column_removed", "column_renamed")
        }
        renamed_columns = {
            c.column_name: c.new_value for c in drift.changes
            if c.change_type == "column_renamed"
        }
        type_changed_columns = {
            c.column_name: (c.old_value, c.new_value) for c in drift.changes
            if c.change_type == "type_changed"
        }

        for spec in mapping_specs:
            spec_id = spec.get("mapping_spec_id", spec.get("id", "unknown"))
            spec_name = spec.get("name", spec_id)

            # Get fields used in this mapping spec
            field_mappings = spec.get("field_mappings", [])
            used_columns = {
                fm.get("source_field", fm.get("column_name"))
                for fm in field_mappings
            }

            # Check for impacted fields
            impacted_by_removal = used_columns & removed_columns
            impacted_by_type_change = used_columns & set(type_changed_columns.keys())

            if not impacted_by_removal and not impacted_by_type_change:
                continue

            recommendations: List[str] = []
            severity = "none"

            if impacted_by_removal:
                severity = "breaking"
                for col in impacted_by_removal:
                    if col in renamed_columns:
                        recommendations.append(
                            f"Update field mapping: '{col}' renamed to '{renamed_columns[col]}'"
                        )
                    else:
                        recommendations.append(
                            f"Remove or replace mapping for deleted column: '{col}'"
                        )

            if impacted_by_type_change:
                if severity != "breaking":
                    severity = "warning"
                for col in impacted_by_type_change:
                    old_type, new_type = type_changed_columns[col]
                    recommendations.append(
                        f"Verify type compatibility for '{col}': {old_type} -> {new_type}"
                    )

            impacted.append(ImpactedMapping(
                mapping_spec_id=spec_id,
                mapping_spec_name=spec_name,
                impacted_fields=list(impacted_by_removal | impacted_by_type_change),
                impact_severity=severity,
                recommendations=recommendations,
            ))

        return impacted

    def to_notification_payload(
        self,
        drift: SchemaDrift,
        impacted_mappings: Optional[List[ImpactedMapping]] = None,
    ) -> Dict[str, Any]:
        """Convert drift to notification payload for WebSocket broadcast"""
        return {
            "event_type": "schema_drift_detected",
            "subject_type": drift.subject_type,
            "subject_id": drift.subject_id,
            "db_name": drift.db_name,
            "severity": drift.severity,
            "drift_type": drift.drift_type,
            "change_summary": drift.change_summary,
            "changes": [
                {
                    "change_type": c.change_type,
                    "column_name": c.column_name,
                    "old_value": str(c.old_value) if c.old_value else None,
                    "new_value": str(c.new_value) if c.new_value else None,
                    "impact": c.impact,
                }
                for c in drift.changes
            ],
            "impacted_mappings": [
                {
                    "mapping_spec_id": m.mapping_spec_id,
                    "mapping_spec_name": m.mapping_spec_name,
                    "impacted_fields": m.impacted_fields,
                    "impact_severity": m.impact_severity,
                    "recommendations": m.recommendations,
                }
                for m in (impacted_mappings or [])
            ],
            "detected_at": drift.detected_at.isoformat(),
            "current_hash": drift.current_hash,
            "previous_hash": drift.previous_hash,
        }
