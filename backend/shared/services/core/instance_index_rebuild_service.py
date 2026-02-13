"""Instance index rebuild service.

Foundry Ephemeral-Index pattern: rebuild an ES instances index from scratch,
then atomically swap aliases (Blue-Green) for zero-downtime cutover.

Two modes:
- **reindex**: Copy existing ES index documents into a fresh index with
  up-to-date mappings.  Fast, safe, good for mapping schema upgrades.
- **full_rebuild** (future): Re-read every dataset artifact from lakeFS and
  re-run the objectify transformation pipeline.  Needed only when the
  source-of-truth data format changes.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from objectify_worker.write_paths import DatasetPrimaryIndexWritePath
from shared.config.search_config import get_default_index_settings, get_instances_index_name
from shared.services.storage.elasticsearch_service import (
    ElasticsearchService,
    promote_alias_to_index,
)

logger = logging.getLogger(__name__)


@dataclass
class RebuildIndexRequest:
    """Parameters for an index rebuild operation."""

    db_name: str
    branch: str = "main"
    allow_delete_base_index: bool = True


@dataclass
class RebuildClassResult:
    class_id: str
    instances_indexed: int
    error: Optional[str] = None


@dataclass
class RebuildIndexResult:
    task_id: str
    status: str  # COMPLETED | FAILED
    new_index: str
    old_indices: List[str]
    total_instances_indexed: int
    classes_processed: List[RebuildClassResult] = field(default_factory=list)
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


async def rebuild_instance_index(
    *,
    request: RebuildIndexRequest,
    elasticsearch_service: ElasticsearchService,
    task_id: Optional[str] = None,
) -> RebuildIndexResult:
    """Rebuild the instances index via ES reindex + Blue-Green alias swap.

    Steps:
      1. Create a new versioned index with current ``_INSTANCE_MAPPING``.
      2. ES ``_reindex`` all documents from the current index into the new one.
      3. Atomic alias swap: ``{db}_instances_{branch}`` → new versioned index.
      4. (Optional) Delete old indices.

    Returns:
        ``RebuildIndexResult`` with statistics and status.
    """
    task_id = task_id or str(uuid4())
    started_at = datetime.now(timezone.utc)
    base_index = get_instances_index_name(request.db_name, branch=request.branch)
    timestamp = int(started_at.timestamp())
    new_index = f"{base_index}_v{timestamp}"

    try:
        # 1. Create new index with up-to-date mappings
        await elasticsearch_service.create_index(
            index=new_index,
            mappings=DatasetPrimaryIndexWritePath._INSTANCE_MAPPING,
            settings=get_default_index_settings(),
        )
        logger.info("Created rebuild target index: %s", new_index)

        # 2. Determine source index (resolve alias if needed)
        source_indices = await _resolve_alias_targets(
            elasticsearch_service, base_index
        )
        if not source_indices:
            # No existing data — source might be a concrete index or not exist
            source_exists = await elasticsearch_service.index_exists(base_index)
            if source_exists:
                source_indices = [base_index]

        total_reindexed = 0
        classes_processed: List[RebuildClassResult] = []

        if source_indices:
            # 3. Reindex from source(s) to new index
            for source_idx in source_indices:
                if source_idx == new_index:
                    continue  # Skip self
                try:
                    count = await _reindex_from_source(
                        elasticsearch_service,
                        source_index=source_idx,
                        dest_index=new_index,
                    )
                    total_reindexed += count
                    logger.info(
                        "Reindexed %d documents from %s → %s",
                        count, source_idx, new_index,
                    )
                except Exception as e:
                    logger.error("Reindex from %s failed: %s", source_idx, e)
                    classes_processed.append(
                        RebuildClassResult(
                            class_id=source_idx,
                            instances_indexed=0,
                            error=str(e),
                        )
                    )

        # 4. Gather per-class stats from new index
        class_counts = await _get_class_counts(elasticsearch_service, new_index)
        for class_id, count in class_counts.items():
            classes_processed.append(
                RebuildClassResult(class_id=class_id, instances_indexed=count)
            )

        # 5. Blue-Green alias swap
        success, error = await promote_alias_to_index(
            elasticsearch_service=elasticsearch_service,
            base_index=base_index,
            new_index=new_index,
            allow_delete_base_index=request.allow_delete_base_index,
        )
        if not success:
            raise RuntimeError(f"Alias swap failed: {error}")

        logger.info(
            "Rebuild complete: %s → %s (%d instances)",
            base_index, new_index, total_reindexed,
        )

        return RebuildIndexResult(
            task_id=task_id,
            status="COMPLETED",
            new_index=new_index,
            old_indices=source_indices,
            total_instances_indexed=total_reindexed,
            classes_processed=classes_processed,
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
        )

    except Exception as exc:
        logger.error("Rebuild failed for %s: %s", base_index, exc)
        # Cleanup: delete partially built new index
        try:
            await elasticsearch_service.delete_index(new_index)
        except Exception:
            logging.getLogger(__name__).warning("Broad exception fallback at shared/services/core/instance_index_rebuild_service.py:170", exc_info=True)
            pass
        return RebuildIndexResult(
            task_id=task_id,
            status="FAILED",
            new_index=new_index,
            old_indices=[],
            total_instances_indexed=0,
            error=str(exc),
            started_at=started_at,
            completed_at=datetime.now(timezone.utc),
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

async def _resolve_alias_targets(
    es: ElasticsearchService,
    alias_name: str,
) -> List[str]:
    """Return concrete index names behind *alias_name*, or empty list."""
    try:
        exists = await es.client.indices.exists_alias(name=alias_name)
        if not exists:
            return []
        result = await es.client.indices.get_alias(name=alias_name)
        return list(result.keys())
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/core/instance_index_rebuild_service.py:199", exc_info=True)
        return []


async def _reindex_from_source(
    es: ElasticsearchService,
    *,
    source_index: str,
    dest_index: str,
) -> int:
    """Run ES ``_reindex`` API and return the number of documents reindexed."""
    body: Dict[str, Any] = {
        "source": {"index": source_index},
        "dest": {"index": dest_index},
        "conflicts": "proceed",
    }
    response = await es.client.reindex(
        body=body,
        request_timeout=600,  # 10min for large indices
    )
    payload = getattr(response, "body", response)
    if isinstance(payload, dict):
        return int(payload.get("total", 0))
    return 0


async def _get_class_counts(
    es: ElasticsearchService,
    index_name: str,
) -> Dict[str, int]:
    """Return ``{class_id: count}`` from a terms aggregation."""
    try:
        body: Dict[str, Any] = {
            "size": 0,
            "aggs": {
                "classes": {
                    "terms": {"field": "class_id", "size": 1000}
                }
            },
        }
        response = await es.client.search(index=index_name, body=body)
        payload = getattr(response, "body", response)
        if not isinstance(payload, dict):
            return {}
        aggs = payload.get("aggregations", {})
        buckets = aggs.get("classes", {}).get("buckets", [])
        return {b["key"]: b["doc_count"] for b in buckets if "key" in b}
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at shared/services/core/instance_index_rebuild_service.py:246", exc_info=True)
        return {}
