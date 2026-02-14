"""
Foreign Key Pattern Detector Service

Detects potential foreign key relationships between datasets based on:
- Column naming conventions (configurable patterns)
- Value distribution overlap analysis
- Primary key reference detection
"""

import logging
import re
from typing import Any, Dict, List, Optional, Set, Tuple
from dataclasses import dataclass, field

logger = logging.getLogger(__name__)


@dataclass
class ForeignKeyPattern:
    """Detected foreign key pattern"""
    source_dataset_id: str
    source_column: str
    target_dataset_id: Optional[str]  # None if not yet resolved
    target_object_type: Optional[str]  # Ontology class reference if known
    target_pk_field: Optional[str]  # Primary key field in target
    confidence: float  # 0.0 ~ 1.0
    detection_method: str  # "naming_convention", "value_overlap", "explicit_constraint"
    reasons: List[str] = field(default_factory=list)


@dataclass
class TargetCandidate:
    """Potential FK target (dataset or object_type)"""
    candidate_type: str  # "dataset" or "object_type"
    candidate_id: str
    candidate_name: str
    pk_columns: List[str]  # Primary key column(s)
    sample_values: Optional[List[Any]] = None  # Sample PK values for overlap detection
    schema: Optional[List[Dict[str, Any]]] = None


@dataclass
class FKDetectionConfig:
    """Configuration for FK pattern detection - no hardcoding"""
    # Naming patterns with confidence scores (regex -> confidence)
    naming_rules: Dict[str, float] = field(default_factory=lambda: {
        r"_id$": 0.75,           # column_name_id
        r"_fk$": 0.95,           # column_name_fk
        r"_key$": 0.70,          # column_name_key
        r"^fk_": 0.95,           # fk_column_name
        r"_ref$": 0.85,          # column_name_ref
        r"_code$": 0.60,         # column_name_code
        r"Id$": 0.70,            # columnNameId (camelCase)
        r"Fk$": 0.90,            # columnNameFk (camelCase)
    })

    # Value overlap threshold for FK detection
    value_overlap_threshold: float = 0.7

    # Minimum sample size for statistical analysis
    sample_size: int = 1000

    # Minimum confidence to report a pattern
    min_confidence: float = 0.5

    # Column types that can be FK (exclude dates, booleans, etc.)
    fk_compatible_types: Set[str] = field(default_factory=lambda: {
        "xsd:string", "xsd:integer", "xsd:decimal",
        "text", "varchar", "int", "bigint", "uuid",
        "string", "integer", "number",
    })

    # Patterns to exclude from FK detection
    exclude_patterns: Set[str] = field(default_factory=lambda: {
        r"created_at", r"updated_at", r"deleted_at",
        r"^is_", r"^has_", r"^can_",  # boolean prefixes
        r"_at$", r"_date$", r"_time$",  # timestamp suffixes
        r"^count_", r"_count$",  # count fields
        r"^total_", r"_total$",  # aggregate fields
    })

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "FKDetectionConfig":
        """Create config from dictionary (for settings injection)"""
        return cls(
            naming_rules=config_dict.get("naming_rules", cls.__dataclass_fields__["naming_rules"].default_factory()),
            value_overlap_threshold=config_dict.get("value_overlap_threshold", 0.7),
            sample_size=config_dict.get("sample_size", 1000),
            min_confidence=config_dict.get("min_confidence", 0.5),
            fk_compatible_types=set(config_dict.get("fk_compatible_types", cls.__dataclass_fields__["fk_compatible_types"].default_factory())),
            exclude_patterns=set(config_dict.get("exclude_patterns", cls.__dataclass_fields__["exclude_patterns"].default_factory())),
        )


class ForeignKeyPatternDetector:
    """
    Detects foreign key patterns in dataset schemas using configurable rules.

    Design principles:
    - No hardcoding: all rules are injected via FKDetectionConfig
    - Multiple detection methods: naming, value overlap, explicit constraints
    - Confidence scoring: combines multiple signals
    - Enterprise-ready: handles large datasets with sampling
    """

    def __init__(self, config: Optional[FKDetectionConfig] = None):
        self.config = config or FKDetectionConfig()
        self._compiled_naming_rules: List[Tuple[re.Pattern, float]] = []
        self._compiled_exclude_patterns: List[re.Pattern] = []
        self._compile_patterns()

    def _compile_patterns(self) -> None:
        """Pre-compile regex patterns for performance"""
        self._compiled_naming_rules = [
            (re.compile(pattern, re.IGNORECASE), confidence)
            for pattern, confidence in self.config.naming_rules.items()
        ]
        self._compiled_exclude_patterns = [
            re.compile(pattern, re.IGNORECASE)
            for pattern in self.config.exclude_patterns
        ]

    def detect_patterns(
        self,
        source_dataset_id: str,
        source_schema: List[Dict[str, Any]],
        source_sample: Optional[List[Dict[str, Any]]] = None,
        target_candidates: Optional[List[TargetCandidate]] = None,
    ) -> List[ForeignKeyPattern]:
        """
        Detect potential FK relationships in a dataset.

        Args:
            source_dataset_id: ID of the source dataset
            source_schema: Schema of the source dataset
            source_sample: Sample data from source (for value overlap analysis)
            target_candidates: Potential FK targets (other datasets/object_types in same DB)

        Returns:
            List of detected FK patterns sorted by confidence (descending)
        """
        patterns: List[ForeignKeyPattern] = []
        target_candidates = target_candidates or []

        for column in source_schema:
            column_name = column.get("name", "")
            column_type = column.get("type", "xsd:string")

            # Skip excluded columns
            if self._is_excluded(column_name):
                continue

            # Skip incompatible types
            if not self._is_fk_compatible_type(column_type):
                continue

            # 1. Naming convention detection
            naming_pattern = self._detect_by_naming(
                source_dataset_id, column_name, target_candidates
            )
            if naming_pattern:
                patterns.append(naming_pattern)

            # 2. Value overlap detection (if sample data provided)
            if source_sample and target_candidates:
                overlap_pattern = self._detect_by_value_overlap(
                    source_dataset_id,
                    column_name,
                    source_sample,
                    target_candidates,
                )
                if overlap_pattern:
                    # Merge with naming pattern if exists
                    existing = next(
                        (p for p in patterns
                         if p.source_column == column_name and p.target_dataset_id == overlap_pattern.target_dataset_id),
                        None
                    )
                    if existing:
                        # Combine confidences
                        existing.confidence = min(1.0, existing.confidence * 0.5 + overlap_pattern.confidence * 0.5)
                        existing.reasons.extend(overlap_pattern.reasons)
                        existing.detection_method = "combined"
                    else:
                        patterns.append(overlap_pattern)

        # Filter by minimum confidence and sort
        filtered = [p for p in patterns if p.confidence >= self.config.min_confidence]
        return sorted(filtered, key=lambda p: p.confidence, reverse=True)

    def _is_excluded(self, column_name: str) -> bool:
        """Check if column should be excluded from FK detection"""
        return any(pattern.search(column_name) for pattern in self._compiled_exclude_patterns)

    def _is_fk_compatible_type(self, column_type: str) -> bool:
        """Check if column type is compatible with FK references"""
        normalized = column_type.lower().replace("xsd:", "")
        return any(
            compat_type.lower() in normalized or normalized in compat_type.lower()
            for compat_type in self.config.fk_compatible_types
        )

    def _detect_by_naming(
        self,
        source_dataset_id: str,
        column_name: str,
        target_candidates: List[TargetCandidate],
    ) -> Optional[ForeignKeyPattern]:
        """Detect FK by naming convention patterns"""
        best_match: Optional[Tuple[float, str, Optional[TargetCandidate]]] = None

        for pattern, base_confidence in self._compiled_naming_rules:
            if not pattern.search(column_name):
                continue

            # Extract potential target name from column
            # e.g., "customer_id" -> "customer", "order_fk" -> "order"
            potential_target = self._extract_target_name(column_name, pattern)

            # Try to match with target candidates
            matched_target = self._find_matching_target(potential_target, target_candidates)

            if matched_target:
                confidence = base_confidence * 1.2  # Boost if target found
            else:
                confidence = base_confidence * 0.8  # Lower if no target match

            confidence = min(1.0, confidence)

            if best_match is None or confidence > best_match[0]:
                best_match = (confidence, potential_target, matched_target)

        if best_match is None:
            return None

        confidence, potential_target, matched_target = best_match

        reasons = [f"Column name matches FK pattern"]
        if matched_target:
            reasons.append(f"Target '{matched_target.candidate_name}' found in database")
        else:
            reasons.append(f"Potential target: '{potential_target}' (not yet resolved)")

        return ForeignKeyPattern(
            source_dataset_id=source_dataset_id,
            source_column=column_name,
            target_dataset_id=matched_target.candidate_id if matched_target else None,
            target_object_type=matched_target.candidate_name if matched_target and matched_target.candidate_type == "object_type" else None,
            target_pk_field=matched_target.pk_columns[0] if matched_target and matched_target.pk_columns else None,
            confidence=confidence,
            detection_method="naming_convention",
            reasons=reasons,
        )

    def _extract_target_name(self, column_name: str, pattern: re.Pattern) -> str:
        """Extract potential target name from FK column name"""
        # Remove FK suffix/prefix
        name = pattern.sub("", column_name)
        # Normalize: snake_case to lowercase, remove underscores
        name = name.lower().replace("_", "")
        return name

    def _find_matching_target(
        self,
        potential_name: str,
        candidates: List[TargetCandidate],
    ) -> Optional[TargetCandidate]:
        """Find a target candidate that matches the potential name"""
        potential_normalized = potential_name.lower().replace("_", "").replace("-", "")

        for candidate in candidates:
            candidate_normalized = candidate.candidate_name.lower().replace("_", "").replace("-", "")

            # Exact match
            if potential_normalized == candidate_normalized:
                return candidate

            # Plural/singular match (simple)
            if potential_normalized + "s" == candidate_normalized:
                return candidate
            if potential_normalized == candidate_normalized + "s":
                return candidate

            # Partial match (name contains potential or vice versa)
            if potential_normalized in candidate_normalized or candidate_normalized in potential_normalized:
                return candidate

        return None

    def _detect_by_value_overlap(
        self,
        source_dataset_id: str,
        column_name: str,
        source_sample: List[Dict[str, Any]],
        target_candidates: List[TargetCandidate],
    ) -> Optional[ForeignKeyPattern]:
        """Detect FK by value overlap with target primary keys"""
        # Get source column values
        source_values = self._extract_column_values(source_sample, column_name)

        if not source_values:
            return None

        best_match: Optional[Tuple[float, TargetCandidate, str]] = None

        for candidate in target_candidates:
            if not candidate.sample_values:
                continue

            # Convert to sets for overlap calculation
            source_set = set(str(v) for v in source_values if v is not None)
            target_set = set(str(v) for v in candidate.sample_values if v is not None)

            if not source_set or not target_set:
                continue

            # Calculate overlap metrics
            overlap = self._calculate_overlap_confidence(source_set, target_set)

            if overlap > self.config.value_overlap_threshold:
                pk_field = candidate.pk_columns[0] if candidate.pk_columns else "unknown"
                if best_match is None or overlap > best_match[0]:
                    best_match = (overlap, candidate, pk_field)

        if best_match is None:
            return None

        overlap, candidate, pk_field = best_match

        return ForeignKeyPattern(
            source_dataset_id=source_dataset_id,
            source_column=column_name,
            target_dataset_id=candidate.candidate_id,
            target_object_type=candidate.candidate_name if candidate.candidate_type == "object_type" else None,
            target_pk_field=pk_field,
            confidence=min(1.0, overlap),
            detection_method="value_overlap",
            reasons=[
                f"High value overlap ({overlap:.1%}) with '{candidate.candidate_name}.{pk_field}'",
            ],
        )

    def _extract_column_values(
        self,
        sample: List[Dict[str, Any]],
        column_name: str,
    ) -> List[Any]:
        """Extract column values from sample data"""
        values = []
        for row in sample[:self.config.sample_size]:
            value = row.get(column_name)
            if value is not None:
                values.append(value)
        return values

    def _calculate_overlap_confidence(
        self,
        source_set: Set[str],
        target_set: Set[str],
    ) -> float:
        """
        Calculate FK confidence based on value overlap.

        A good FK should have:
        - High % of source values found in target (referential integrity)
        - Reasonable cardinality difference (many source -> fewer target)
        """
        intersection = source_set & target_set

        if not intersection:
            return 0.0

        # What % of source values exist in target?
        # This is the primary indicator of FK relationship
        source_coverage = len(intersection) / len(source_set)

        # Cardinality ratio (expect more source values than unique references)
        cardinality_ratio = len(target_set) / max(len(source_set), 1)

        # FK typically has many-to-one relationship
        # If cardinality_ratio < 1, it's a good sign (many source rows reference fewer target rows)
        cardinality_factor = 1.0 if cardinality_ratio <= 1.0 else max(0.5, 1.0 - (cardinality_ratio - 1.0) * 0.1)

        # Combine factors
        confidence = source_coverage * 0.7 + cardinality_factor * 0.3

        return min(1.0, confidence)

    def suggest_link_type(
        self,
        fk_pattern: ForeignKeyPattern,
        source_object_type: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Generate a link_type suggestion from a detected FK pattern.

        Returns a structure compatible with link_type creation API.
        """
        # Generate predicate from column name
        # e.g., "customer_id" -> "hasCustomer", "order_fk" -> "hasOrder"
        predicate = self._generate_predicate(fk_pattern.source_column)

        # Infer cardinality
        # FK usually implies many-to-one (n:1)
        cardinality = "n:1"

        return {
            "suggested_link_type": {
                "predicate": predicate,
                "source_class": source_object_type or f"<from:{fk_pattern.source_dataset_id}>",
                "target_class": fk_pattern.target_object_type or f"<unknown>",
                "cardinality": cardinality,
                "description": f"Auto-detected from FK pattern: {fk_pattern.source_column}",
            },
            "relationship_spec": {
                "spec_type": "foreign_key",
                "source_column": fk_pattern.source_column,
                "target_pk_field": fk_pattern.target_pk_field or "id",
            },
            "confidence": fk_pattern.confidence,
            "detection_method": fk_pattern.detection_method,
            "reasons": fk_pattern.reasons,
        }

    def _generate_predicate(self, column_name: str) -> str:
        """Generate a predicate name from FK column name"""
        # Remove common FK suffixes
        name = re.sub(r"(_id|_fk|_key|_ref|_code|Id|Fk|Key)$", "", column_name)

        # Convert to camelCase predicate
        # e.g., "customer" -> "hasCustomer"
        words = re.split(r"[_\s]+", name)
        if not words:
            return "hasRelation"

        predicate = "has" + "".join(word.capitalize() for word in words)
        return predicate
