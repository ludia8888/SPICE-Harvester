"""
Relationship Reconciler Service.

Automatically populates ``relationships`` fields on ES instances by detecting
FK (foreign-key) references between ontology classes.

Algorithm:
  1. Load all class definitions + relationship declarations from OMS.
  2. For each declared relationship (source_class → target_class via predicate):
     a. Determine the FK field in target class instances that references
        source class PK (e.g. Order.customer_id → Customer).
     b. Scan target class ES instances to group FK values.
     c. Bulk update source class instances with discovered relationship refs.
  3. Return a summary of discovered and written relationships.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional, Set

from bff.services.oms_client import OMSClient
from shared.config.search_config import get_instances_index_name
from shared.services.registries.objectify_registry import ObjectifyRegistry
from shared.services.storage.elasticsearch_service import ElasticsearchService

logger = logging.getLogger(__name__)

# Page size for ES scroll operations
_ES_SCAN_SIZE = 500


async def reconcile_relationships(
    *,
    db_name: str,
    branch: str = "main",
    oms_client: OMSClient,
    es_service: ElasticsearchService,
    objectify_registry: Optional[ObjectifyRegistry] = None,
) -> Dict[str, Any]:
    """
    Reconcile all FK-based relationships for a database.

    Returns a summary dict with per-relationship stats.
    """
    # Ensure ES client is connected
    if getattr(es_service, '_client', None) is None:
        await es_service.connect()

    index_name = get_instances_index_name(db_name, branch=branch)

    # ------------------------------------------------------------------
    # Step 1: Load all classes + relationships from OMS
    # ------------------------------------------------------------------
    class_defs = await _load_class_defs(oms_client, db_name, branch=branch)
    if not class_defs:
        return {"status": "no_classes", "relationships_processed": 0}

    # Collect all relationship declarations across classes
    all_rels = _collect_relationships(class_defs)
    if not all_rels:
        return {"status": "no_relationships", "relationships_processed": 0}

    logger.info(
        "Reconciling %d relationship(s) across %d class(es) in %s/%s",
        len(all_rels), len(class_defs), db_name, branch,
    )

    # ------------------------------------------------------------------
    # Step 2-3: For each relationship, detect FK → group → update
    # ------------------------------------------------------------------
    results: List[Dict[str, Any]] = []
    total_updated = 0

    for rel in all_rels:
        source_class = rel["source_class"]
        target_class = rel["target_class"]
        predicate = rel["predicate"]
        cardinality = rel.get("cardinality", "1:n")

        logger.info(
            "Processing relationship: %s.%s → %s (cardinality=%s)",
            source_class, predicate, target_class, cardinality,
        )

        # Detect FK field in target class
        fk_field = await _detect_fk_field(
            source_class=source_class,
            target_class=target_class,
            index_name=index_name,
            es_service=es_service,
            objectify_registry=objectify_registry,
            db_name=db_name,
        )
        if not fk_field:
            results.append({
                "source_class": source_class,
                "predicate": predicate,
                "target_class": target_class,
                "status": "fk_not_found",
                "updated": 0,
            })
            continue

        logger.info(
            "Detected FK field: %s.%s → %s PK",
            target_class, fk_field, source_class,
        )

        # Scan target instances and group by FK value
        fk_groups = await _scan_and_group_fk(
            index_name=index_name,
            target_class=target_class,
            fk_field=fk_field,
            es_service=es_service,
        )
        if not fk_groups:
            results.append({
                "source_class": source_class,
                "predicate": predicate,
                "target_class": target_class,
                "fk_field": fk_field,
                "status": "no_fk_values",
                "updated": 0,
            })
            continue

        # Bulk update source instances
        updated = await _bulk_update_relationships(
            index_name=index_name,
            source_class=source_class,
            predicate=predicate,
            target_class=target_class,
            fk_groups=fk_groups,
            es_service=es_service,
        )
        total_updated += updated

        results.append({
            "source_class": source_class,
            "predicate": predicate,
            "target_class": target_class,
            "fk_field": fk_field,
            "status": "ok",
            "fk_groups": len(fk_groups),
            "updated": updated,
        })

    return {
        "status": "ok",
        "db_name": db_name,
        "branch": branch,
        "relationships_processed": len(results),
        "total_instances_updated": total_updated,
        "details": results,
    }


# ==========================================================================
# Internal helpers
# ==========================================================================


async def _load_class_defs(
    oms_client: OMSClient, db_name: str, *, branch: str,
) -> Dict[str, Dict[str, Any]]:
    """Load all class definitions from OMS keyed by class_id."""
    try:
        payload = await oms_client.list_ontologies(db_name, branch=branch)
    except Exception:
        logger.exception("Failed to list ontologies for %s", db_name)
        return {}

    data = payload.get("data") if isinstance(payload, dict) else None
    ontologies = (data or {}).get("ontologies") if isinstance(data, dict) else None
    if not isinstance(ontologies, list):
        return {}

    result: Dict[str, Dict[str, Any]] = {}
    for ont in ontologies:
        if not isinstance(ont, dict):
            continue
        class_id = str(ont.get("id") or ont.get("class_id") or "").strip()
        if not class_id:
            continue
        result[class_id] = ont

    # If the list endpoint only returns summaries, try fetching full definitions
    for class_id in list(result.keys()):
        cls = result[class_id]
        if "relationships" not in cls:
            try:
                full = await oms_client.get_ontology(db_name, class_id, branch=branch)
                full_data = full.get("data") if isinstance(full, dict) else full
                if isinstance(full_data, dict):
                    result[class_id] = full_data
            except Exception:
                logger.debug("Could not fetch full class def for %s", class_id)

    return result


def _collect_relationships(
    class_defs: Dict[str, Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Collect all relationship declarations from class definitions."""
    rels: List[Dict[str, Any]] = []
    seen: Set[str] = set()

    for class_id, cls in class_defs.items():
        relationships = cls.get("relationships")
        if not isinstance(relationships, list):
            continue
        for rel in relationships:
            if not isinstance(rel, dict):
                continue
            predicate = str(rel.get("predicate") or rel.get("name") or "").strip()
            target = str(rel.get("target_class") or rel.get("target") or "").strip()
            if not predicate or not target:
                continue
            key = f"{class_id}:{predicate}:{target}"
            if key in seen:
                continue
            seen.add(key)
            rels.append({
                "source_class": class_id,
                "predicate": predicate,
                "target_class": target,
                "cardinality": rel.get("cardinality"),
            })

    return rels


async def _detect_fk_field(
    *,
    source_class: str,
    target_class: str,
    index_name: str,
    es_service: ElasticsearchService,
    objectify_registry: Optional[ObjectifyRegistry],
    db_name: str,
) -> Optional[str]:
    """
    Detect the FK field in target class instances that references the source class PK.

    Strategy (priority order):
    1. Mapping spec: look for a target_field in the target class mapping whose name
       matches the source class PK pattern (e.g. ``customer_id``).
    2. Pattern matching: ``{source_class.lower()}_id``.
    3. Data sampling: scan a target instance's ``data`` and look for a field
       whose value matches any source instance ID.
    """
    source_lower = source_class.lower()
    candidate_patterns = [
        f"{source_lower}_id",
        f"{source_lower}id",
        f"{source_lower}_key",
    ]

    # Strategy 1: Check mapping specs
    if objectify_registry:
        try:
            specs = await objectify_registry.list_mapping_specs(include_inactive=False)
            for spec in specs:
                if spec.target_class_id != target_class:
                    continue
                target_fields = {
                    str(m.get("target_field") or m.get("target") or "").strip()
                    for m in (spec.mappings or [])
                    if isinstance(m, dict)
                }
                for pat in candidate_patterns:
                    if pat in target_fields:
                        return pat
        except Exception:
            logger.debug("Mapping spec FK detection skipped")

    # Strategy 2: Pattern matching via data sampling
    sample = await _sample_instance(
        index_name=index_name,
        class_id=target_class,
        es_service=es_service,
    )
    if sample:
        # Unwrap _source and then data sub-object
        source = sample.get("_source") or sample
        data = source.get("data") if isinstance(source, dict) else None
        if not isinstance(data, dict):
            data = source  # fallback to flat source
        if isinstance(data, dict):
            for pat in candidate_patterns:
                if pat in data:
                    return pat

    # Strategy 3: Broad data sampling — look for any field whose value format
    # matches source instance IDs (e.g. "CUST001")
    if sample:
        source_ids = await _get_class_instance_ids(
            index_name=index_name,
            class_id=source_class,
            es_service=es_service,
            limit=10,
        )
        if source_ids:
            source_doc = sample.get("_source") or sample
            data = source_doc.get("data") if isinstance(source_doc, dict) else None
            if not isinstance(data, dict):
                data = source_doc
            if isinstance(data, dict):
                for key, value in data.items():
                    if key in ("instance_id", "class_id", "db_name", "branch"):
                        continue
                    if isinstance(value, str) and value.strip() in source_ids:
                        return key

    return None


async def _sample_instance(
    *, index_name: str, class_id: str, es_service: ElasticsearchService,
) -> Optional[Dict[str, Any]]:
    """Get a single instance document for a class."""
    try:
        result = await es_service.search(
            index=index_name,
            query={"term": {"class_id": class_id}},
            size=1,
            source_includes=["data", "instance_id"],
        )
        hits = result.get("hits") or []
        if hits:
            return hits[0]
    except Exception:
        logger.debug("Sample instance fetch failed for %s", class_id)
    return None


async def _get_class_instance_ids(
    *, index_name: str, class_id: str, es_service: ElasticsearchService, limit: int = 10,
) -> Set[str]:
    """Get a sample of instance IDs for a class."""
    ids: Set[str] = set()
    try:
        result = await es_service.search(
            index=index_name,
            query={"term": {"class_id": class_id}},
            size=limit,
            source_includes=["instance_id"],
        )
        for hit in (result.get("hits") or []):
            iid = hit.get("instance_id") or (hit.get("_source") or {}).get("instance_id")
            if iid:
                ids.add(str(iid).strip())
    except Exception:
        pass
    return ids


async def _scan_and_group_fk(
    *,
    index_name: str,
    target_class: str,
    fk_field: str,
    es_service: ElasticsearchService,
) -> Dict[str, List[str]]:
    """
    Scan all instances of target_class, grouping by their FK field value.

    Returns:
        Dict mapping FK value (source instance ID) → list of target instance IDs.
        e.g. {"CUST001": ["ORD001", "ORD002"], "CUST002": ["ORD003"]}
    """
    groups: Dict[str, List[str]] = defaultdict(list)
    from_ = 0

    while True:
        result = await es_service.search(
            index=index_name,
            query={"term": {"class_id": target_class}},
            size=_ES_SCAN_SIZE,
            from_=from_,
            sort=[{"instance_id": {"order": "asc"}}],
            source_includes=["instance_id", f"data.{fk_field}"],
        )
        hits = result.get("hits") or []
        if not hits:
            break

        for hit in hits:
            source = hit.get("_source") or hit
            instance_id = str(source.get("instance_id") or hit.get("_id") or "").strip()
            if not instance_id:
                continue

            # FK value lives in data sub-object
            data = source.get("data") or {}
            fk_value = data.get(fk_field) if isinstance(data, dict) else None
            if fk_value is None:
                # Also try flat source (in case data is flattened)
                fk_value = source.get(fk_field)
            if fk_value is None:
                continue
            fk_str = str(fk_value).strip()
            if fk_str:
                groups[fk_str].append(instance_id)

        from_ += len(hits)
        if len(hits) < _ES_SCAN_SIZE:
            break

    return dict(groups)


async def _bulk_update_relationships(
    *,
    index_name: str,
    source_class: str,
    predicate: str,
    target_class: str,
    fk_groups: Dict[str, List[str]],
    es_service: ElasticsearchService,
) -> int:
    """
    Bulk update source class instances with relationship references.

    For each FK group entry (source_instance_id → [target_instance_ids]):
    - Sets ``relationships.{predicate}`` = ["{target_class}/{id}", ...]
    """
    updated = 0

    for source_id, target_ids in fk_groups.items():
        # Build relationship refs in TargetClass/instance_id format
        refs = [f"{target_class}/{tid}" for tid in target_ids]
        # De-duplicate
        seen: Set[str] = set()
        unique_refs = [r for r in refs if not (r in seen or seen.add(r))]  # type: ignore[func-returns-value]

        try:
            ok = await es_service.update_document(
                index=index_name,
                doc_id=source_id,
                doc={"relationships": {predicate: unique_refs}},
                refresh=False,
            )
            if ok:
                updated += 1
        except Exception:
            logger.warning(
                "Failed to update relationships for %s/%s",
                source_class, source_id,
                exc_info=True,
            )

    # Refresh index after all updates
    try:
        await es_service.refresh_index(index_name)
    except Exception:
        logger.debug("Index refresh after reconcile failed")

    return updated
