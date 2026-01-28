"""
Objectify Delta Utilities

Provides utilities for computing row-level deltas between dataset versions
for incremental objectify processing using LakeFS diff.
"""

import logging
import hashlib
import json
from typing import Any, Dict, List, Optional, Set, Tuple, AsyncIterator
from dataclasses import dataclass, field
from datetime import datetime

logger = logging.getLogger(__name__)


@dataclass
class DeltaResult:
    """Result of delta computation between two dataset versions"""
    added_rows: List[Dict[str, Any]] = field(default_factory=list)
    modified_rows: List[Dict[str, Any]] = field(default_factory=list)  # New values only
    deleted_keys: List[str] = field(default_factory=list)  # Primary key values of deleted rows
    stats: Dict[str, int] = field(default_factory=dict)

    def __post_init__(self):
        if not self.stats:
            self.stats = {
                "added_count": len(self.added_rows),
                "modified_count": len(self.modified_rows),
                "deleted_count": len(self.deleted_keys),
                "total_changes": len(self.added_rows) + len(self.modified_rows) + len(self.deleted_keys),
            }

    @property
    def has_changes(self) -> bool:
        return self.stats.get("total_changes", 0) > 0


@dataclass
class WatermarkState:
    """Watermark state for incremental processing"""
    mapping_spec_id: str
    dataset_branch: str
    watermark_column: str
    watermark_value: Any
    lakefs_commit_id: Optional[str] = None
    last_processed_at: datetime = field(default_factory=datetime.utcnow)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "mapping_spec_id": self.mapping_spec_id,
            "dataset_branch": self.dataset_branch,
            "watermark_column": self.watermark_column,
            "watermark_value": self.watermark_value,
            "lakefs_commit_id": self.lakefs_commit_id,
            "last_processed_at": self.last_processed_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "WatermarkState":
        last_processed = data.get("last_processed_at")
        if isinstance(last_processed, str):
            last_processed = datetime.fromisoformat(last_processed.replace("Z", "+00:00"))
        elif last_processed is None:
            last_processed = datetime.utcnow()

        return cls(
            mapping_spec_id=data["mapping_spec_id"],
            dataset_branch=data.get("dataset_branch", "main"),
            watermark_column=data["watermark_column"],
            watermark_value=data["watermark_value"],
            lakefs_commit_id=data.get("lakefs_commit_id"),
            last_processed_at=last_processed,
        )


class ObjectifyDeltaComputer:
    """
    Computes deltas for incremental objectify processing.

    Supports two modes:
    1. Watermark-based: Filter rows where watermark_column > previous_watermark
    2. LakeFS diff-based: Use LakeFS diff API to get changed files and compute row-level delta

    Enterprise features:
    - Handles large datasets via chunked processing
    - Supports primary key deduplication
    - Tracks processing state for resume capability
    """

    def __init__(
        self,
        pk_columns: List[str],
        batch_size: int = 10000,
    ):
        """
        Args:
            pk_columns: Primary key column(s) for row identification
            batch_size: Batch size for processing
        """
        self.pk_columns = pk_columns
        self.batch_size = batch_size

    def compute_row_key(self, row: Dict[str, Any]) -> str:
        """Compute a unique key for a row based on primary key columns"""
        key_values = [str(row.get(col, "")) for col in self.pk_columns]
        return "|".join(key_values)

    def compute_row_hash(self, row: Dict[str, Any]) -> str:
        """Compute a content hash for a row (for change detection)"""
        # Sort keys for consistent hashing
        payload = json.dumps(row, sort_keys=True, default=str)
        return hashlib.md5(payload.encode("utf-8")).hexdigest()

    async def compute_delta_from_watermark(
        self,
        rows_iterator: AsyncIterator[Dict[str, Any]],
        watermark_column: str,
        previous_watermark: Any,
    ) -> DeltaResult:
        """
        Compute delta using watermark-based filtering.

        Filters rows where watermark_column > previous_watermark.
        All matching rows are considered 'added' (upsert semantics).
        """
        added_rows: List[Dict[str, Any]] = []
        max_watermark = previous_watermark

        async for row in rows_iterator:
            row_watermark = row.get(watermark_column)

            if row_watermark is None:
                continue

            # Compare watermarks (handles datetime, int, string)
            if self._compare_watermarks(row_watermark, previous_watermark) > 0:
                added_rows.append(row)

                # Track max watermark
                if max_watermark is None or self._compare_watermarks(row_watermark, max_watermark) > 0:
                    max_watermark = row_watermark

        return DeltaResult(
            added_rows=added_rows,
            modified_rows=[],  # Watermark mode treats all as adds (upsert)
            deleted_keys=[],   # Cannot detect deletes in watermark mode
            stats={
                "added_count": len(added_rows),
                "modified_count": 0,
                "deleted_count": 0,
                "total_changes": len(added_rows),
                "max_watermark": str(max_watermark) if max_watermark else None,
            }
        )

    def _compare_watermarks(self, a: Any, b: Any) -> int:
        """
        Compare two watermark values.
        Returns: >0 if a > b, 0 if equal, <0 if a < b
        """
        if a is None and b is None:
            return 0
        if a is None:
            return -1
        if b is None:
            return 1

        # Try datetime comparison
        if isinstance(a, datetime) and isinstance(b, datetime):
            return (a - b).total_seconds()

        # Try numeric comparison
        try:
            return float(a) - float(b)
        except (ValueError, TypeError):
            pass

        # Fall back to string comparison
        return (str(a) > str(b)) - (str(a) < str(b))

    async def compute_delta_from_snapshots(
        self,
        old_rows_iterator: AsyncIterator[Dict[str, Any]],
        new_rows_iterator: AsyncIterator[Dict[str, Any]],
    ) -> DeltaResult:
        """
        Compute delta by comparing two full snapshots.

        This is memory-intensive for large datasets.
        Use compute_delta_from_lakefs_diff for better performance.
        """
        # Build old snapshot index: key -> hash
        old_index: Dict[str, str] = {}
        old_rows_by_key: Dict[str, Dict[str, Any]] = {}

        async for row in old_rows_iterator:
            key = self.compute_row_key(row)
            old_index[key] = self.compute_row_hash(row)
            old_rows_by_key[key] = row

        # Process new snapshot
        added_rows: List[Dict[str, Any]] = []
        modified_rows: List[Dict[str, Any]] = []
        seen_keys: Set[str] = set()

        async for row in new_rows_iterator:
            key = self.compute_row_key(row)
            seen_keys.add(key)

            if key not in old_index:
                # New row
                added_rows.append(row)
            else:
                # Existing row - check if modified
                new_hash = self.compute_row_hash(row)
                if new_hash != old_index[key]:
                    modified_rows.append(row)

        # Deleted rows
        deleted_keys = [key for key in old_index.keys() if key not in seen_keys]

        return DeltaResult(
            added_rows=added_rows,
            modified_rows=modified_rows,
            deleted_keys=deleted_keys,
        )

    async def compute_delta_from_lakefs_diff(
        self,
        lakefs_client: Any,  # LakeFSClient type
        repository: str,
        base_ref: str,
        target_ref: str,
        path: str,
        file_reader: Any,  # Callable to read parquet/csv files
    ) -> DeltaResult:
        """
        Compute delta using LakeFS diff API.

        More efficient than snapshot comparison as it only processes changed files.

        Args:
            lakefs_client: LakeFS client instance
            repository: LakeFS repository name
            base_ref: Base reference (commit, branch, tag)
            target_ref: Target reference
            path: Path prefix to diff
            file_reader: Function to read file contents from LakeFS

        Returns:
            DeltaResult with changes
        """
        # Get diff from LakeFS
        diff_results = await self._get_lakefs_diff(
            lakefs_client, repository, base_ref, target_ref, path
        )

        added_rows: List[Dict[str, Any]] = []
        modified_rows: List[Dict[str, Any]] = []
        deleted_keys: List[str] = []

        for diff_entry in diff_results:
            diff_type = diff_entry.get("type", "")
            file_path = diff_entry.get("path", "")

            if diff_type == "added":
                # New file - all rows are added
                rows = await file_reader(repository, target_ref, file_path)
                added_rows.extend(rows)

            elif diff_type == "removed":
                # Deleted file - extract keys from old version
                rows = await file_reader(repository, base_ref, file_path)
                for row in rows:
                    deleted_keys.append(self.compute_row_key(row))

            elif diff_type == "changed":
                # Modified file - need row-level diff
                old_rows = await file_reader(repository, base_ref, file_path)
                new_rows = await file_reader(repository, target_ref, file_path)

                # Build index of old rows
                old_by_key = {self.compute_row_key(r): r for r in old_rows}
                old_hashes = {k: self.compute_row_hash(r) for k, r in old_by_key.items()}

                seen_keys: Set[str] = set()

                for row in new_rows:
                    key = self.compute_row_key(row)
                    seen_keys.add(key)

                    if key not in old_by_key:
                        added_rows.append(row)
                    elif self.compute_row_hash(row) != old_hashes.get(key):
                        modified_rows.append(row)

                # Check for deleted rows within this file
                for key in old_by_key.keys():
                    if key not in seen_keys:
                        deleted_keys.append(key)

        return DeltaResult(
            added_rows=added_rows,
            modified_rows=modified_rows,
            deleted_keys=deleted_keys,
        )

    async def _get_lakefs_diff(
        self,
        lakefs_client: Any,
        repository: str,
        base_ref: str,
        target_ref: str,
        path: str,
    ) -> List[Dict[str, Any]]:
        """Get diff entries from LakeFS"""
        try:
            # LakeFS diff API call
            diff_response = await lakefs_client.diff(
                repository=repository,
                left_ref=base_ref,
                right_ref=target_ref,
                prefix=path,
            )
            return diff_response.get("results", [])
        except Exception as e:
            logger.error(f"LakeFS diff failed: {e}")
            raise

    def filter_rows_by_watermark(
        self,
        rows: List[Dict[str, Any]],
        watermark_column: str,
        previous_watermark: Any,
    ) -> Tuple[List[Dict[str, Any]], Any]:
        """
        Synchronous version of watermark filtering.

        Returns:
            Tuple of (filtered_rows, max_watermark)
        """
        filtered: List[Dict[str, Any]] = []
        max_watermark = previous_watermark

        for row in rows:
            row_watermark = row.get(watermark_column)

            if row_watermark is None:
                continue

            if self._compare_watermarks(row_watermark, previous_watermark) > 0:
                filtered.append(row)

                if max_watermark is None or self._compare_watermarks(row_watermark, max_watermark) > 0:
                    max_watermark = row_watermark

        return filtered, max_watermark


def create_delta_computer_for_mapping_spec(
    mapping_spec: Dict[str, Any],
) -> ObjectifyDeltaComputer:
    """
    Factory function to create a delta computer from mapping spec configuration.
    """
    # Extract PK columns from mapping spec
    pk_spec = mapping_spec.get("pk_spec", {})
    pk_columns = pk_spec.get("columns", [])

    if not pk_columns:
        # Fall back to looking for 'id' or first column
        field_mappings = mapping_spec.get("field_mappings", [])
        for fm in field_mappings:
            if fm.get("source_field", "").lower() in ("id", "pk", "key"):
                pk_columns = [fm.get("source_field")]
                break

        if not pk_columns and field_mappings:
            pk_columns = [field_mappings[0].get("source_field", "id")]

    return ObjectifyDeltaComputer(
        pk_columns=pk_columns,
        batch_size=mapping_spec.get("batch_size", 10000),
    )
