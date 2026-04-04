from __future__ import annotations

import logging
import sys
from typing import Any, Dict, List, Optional

from shared.models.objectify_job import ObjectifyJob
from shared.services.core.relationship_extractor import (
    extract_relationships as _extract_instance_relationships_raw,
)
from shared.services.pipeline.objectify_delta_utils import (
    DeltaResult,
    ObjectifyDeltaComputer,
)
from shared.services.storage.lakefs_client import LakeFSClient, LakeFSConfig

logger = logging.getLogger("objectify_worker.main")

# Well-known watermark column names in priority order
_WATERMARK_CANDIDATES = (
    "updated_at", "modified_at", "last_modified", "last_updated",
    "created_at", "timestamp", "event_time", "ingested_at",
)


class ObjectifyNonRetryableError(RuntimeError):
    """Raised for objectify failures that should not be retried."""


def _auto_detect_watermark_column(
    *,
    columns: Optional[List[str]],
    options: Optional[Dict[str, Any]] = None,
) -> Optional[str]:
    """Auto-detect a watermark column from dataset schema or options."""
    if columns:
        lower_map = {c.lower(): c for c in columns}
        for candidate in _WATERMARK_CANDIDATES:
            if candidate in lower_map:
                return lower_map[candidate]

    if options and isinstance(options, dict):
        hint = options.get("watermark_column_hint")
        if hint and isinstance(hint, str):
            return hint.strip() or None

    return None


async def _compute_lakefs_delta(
    *,
    job: ObjectifyJob,
    delta_computer: ObjectifyDeltaComputer,
    storage: Any,
) -> Optional[DeltaResult]:
    """Compute row-level delta between two LakeFS commits using the diff API."""
    base_commit = job.base_commit_id
    if not base_commit:
        return None

    repository = ""
    target_ref = ""
    diff_prefix = ""
    if job.artifact_key:
        parts = job.artifact_key.replace("s3://", "").split("/", 2)
        if len(parts) >= 2:
            repository = parts[0]
            target_ref = parts[1]
            raw_path = parts[2] if len(parts) >= 3 else ""
            if raw_path:
                if "/staging/" in raw_path:
                    diff_prefix = raw_path.split("/staging/")[0]
                else:
                    diff_prefix = raw_path.rsplit("/", 1)[0] if "/" in raw_path else raw_path

    if not repository or not target_ref:
        return None

    try:
        lakefs_config = LakeFSConfig.from_env()
        lakefs_client = LakeFSClient(config=lakefs_config)

        async def _read_file_rows(repo: str, ref: str, path: str) -> List[Dict[str, Any]]:
            import csv
            import io

            bucket = repo
            key = f"{ref}/{path}" if not path.startswith(ref) else path
            try:
                raw = await storage.load_bytes(bucket, key)
                text = raw.decode("utf-8", errors="replace")
                reader = csv.DictReader(io.StringIO(text))
                return list(reader)
            except Exception as exc:
                logger.warning("Failed to read %s/%s at ref %s: %s", repo, path, ref, exc)
                return []

        result = await delta_computer.compute_delta_from_lakefs_diff(
            lakefs_client=lakefs_client,
            repository=repository,
            base_ref=base_commit,
            target_ref=target_ref,
            path=diff_prefix,
            file_reader=_read_file_rows,
        )
        return result
    except Exception as exc:
        logger.error("LakeFS delta computation failed: %s", exc, exc_info=True)
        return None


def _extract_instance_relationships(
    instance: Dict[str, Any],
    *,
    rel_map: Dict[str, Dict[str, Any]],
) -> Dict[str, Any]:
    """Thin wrapper: extract relationships for a single instance using pre-parsed rel_map."""
    main_module = sys.modules.get("objectify_worker.main")
    extractor = getattr(main_module, "_extract_instance_relationships_raw", _extract_instance_relationships_raw)
    try:
        return extractor(
            instance, rel_map=rel_map, allow_pattern_fallback=True,
        )
    except Exception as exc:
        raise ObjectifyNonRetryableError(
            f"relationship extraction failed for instance {instance.get('instance_id') or '<unknown>'}: {exc}"
        ) from exc
