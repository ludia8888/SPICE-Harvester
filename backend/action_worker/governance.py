from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.principal_policy import build_principal_tags, policy_allows
from shared.utils.writeback_governance import extract_backing_dataset_id, policies_aligned

logger = logging.getLogger(__name__)


async def check_writeback_dataset_acl_alignment(
    *,
    dataset_registry: DatasetRegistry | None,
    db_name: str,
    submitted_by: str,
    submitted_by_type: str,
    actor_role: Optional[str],
    ontology_commit_id: str,
    resources: OntologyResourceService,
    class_ids: set[str],
) -> Optional[Dict[str, Any]]:
    if not AppConfig.WRITEBACK_ENFORCE_GOVERNANCE:
        return None
    if not class_ids:
        return None
    if not dataset_registry:
        return {
            "error": "writeback_governance_unavailable",
            "message": "DatasetRegistry not initialized (WRITEBACK_ENFORCE_GOVERNANCE=true)",
        }

    scope = AppConfig.WRITEBACK_DATASET_ACL_SCOPE
    writeback_dataset_id = AppConfig.ONTOLOGY_WRITEBACK_DATASET_ID
    if not writeback_dataset_id:
        return {
            "error": "writeback_acl_unverifiable",
            "message": "ONTOLOGY_WRITEBACK_DATASET_ID is required when WRITEBACK_ENFORCE_GOVERNANCE=true",
            "scope": scope,
        }

    try:
        writeback_dataset = await dataset_registry.get_dataset(dataset_id=writeback_dataset_id)
    except Exception as exc:
        logger.warning("Failed to load writeback dataset %s: %s", writeback_dataset_id, exc, exc_info=True)
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Failed to load writeback dataset",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
            "detail": str(exc),
        }
    if not writeback_dataset:
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Writeback dataset not found",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
        }
    if writeback_dataset.db_name != db_name:
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Writeback dataset db_name mismatch",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
            "writeback_db_name": writeback_dataset.db_name,
            "expected_db_name": db_name,
        }

    try:
        writeback_acl = await dataset_registry.get_access_policy(
            db_name=db_name,
            scope=scope,
            subject_type="dataset",
            subject_id=writeback_dataset_id,
        )
    except Exception as exc:
        logger.warning(
            "Failed to load writeback dataset ACL policy for %s: %s",
            writeback_dataset_id,
            exc,
            exc_info=True,
        )
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Failed to load writeback dataset ACL policy",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
            "detail": str(exc),
        }
    if not writeback_acl:
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Writeback dataset ACL policy missing",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
        }
    if not isinstance(writeback_acl.policy, dict) or not writeback_acl.policy:
        return {
            "error": "writeback_acl_unverifiable",
            "message": "Writeback dataset ACL policy is empty",
            "scope": scope,
            "writeback_dataset_id": writeback_dataset_id,
        }

    actor_id = str(submitted_by or "").strip()
    if actor_id and actor_id != "system":
        tags = build_principal_tags(
            principal_type=submitted_by_type,
            principal_id=actor_id,
            role=actor_role,
        )
        if not policy_allows(policy=writeback_acl.policy, principal_tags=tags):
            return {
                "error": "writeback_acl_denied",
                "message": "Actor is not allowed by writeback dataset ACL",
                "scope": scope,
                "writeback_dataset_id": writeback_dataset_id,
                "submitted_by": actor_id,
                "role": actor_role,
            }

    for class_id in sorted({cid for cid in class_ids if cid}):
        try:
            object_resource = await resources.get_resource(
                db_name,
                branch=ontology_commit_id,
                resource_type="object_type",
                resource_id=class_id,
            )
        except Exception as exc:
            logger.warning("Failed to load object_type resource for class %s: %s", class_id, exc, exc_info=True)
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Failed to load object_type resource for writeback governance checks",
                "scope": scope,
                "class_id": class_id,
                "detail": str(exc),
            }
        obj_spec = object_resource.get("spec") if isinstance(object_resource, dict) else None
        backing_dataset_id = extract_backing_dataset_id(obj_spec)
        if not backing_dataset_id:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "object_type.spec.backing_source.dataset_id is required for writeback governance checks",
                "scope": scope,
                "class_id": class_id,
            }

        try:
            backing_dataset = await dataset_registry.get_dataset(dataset_id=backing_dataset_id)
        except Exception as exc:
            logger.warning(
                "Failed to load backing dataset %s for class %s: %s",
                backing_dataset_id,
                class_id,
                exc,
                exc_info=True,
            )
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Failed to load backing dataset for writeback governance checks",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
                "detail": str(exc),
            }
        if not backing_dataset:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Backing dataset not found",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
            }
        if backing_dataset.db_name != db_name:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Backing dataset db_name mismatch",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
                "backing_db_name": backing_dataset.db_name,
                "expected_db_name": db_name,
            }

        try:
            backing_acl = await dataset_registry.get_access_policy(
                db_name=db_name,
                scope=scope,
                subject_type="dataset",
                subject_id=backing_dataset_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to load backing dataset ACL policy for %s (class=%s): %s",
                backing_dataset_id,
                class_id,
                exc,
                exc_info=True,
            )
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Failed to load backing dataset ACL policy",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
                "detail": str(exc),
            }
        if not backing_acl:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Backing dataset ACL policy missing",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
            }
        if not isinstance(backing_acl.policy, dict) or not backing_acl.policy:
            return {
                "error": "writeback_acl_unverifiable",
                "message": "Backing dataset ACL policy is empty",
                "scope": scope,
                "class_id": class_id,
                "backing_dataset_id": backing_dataset_id,
            }

        if not policies_aligned(backing_acl.policy, writeback_acl.policy):
            return {
                "error": "writeback_acl_misaligned",
                "message": "Writeback dataset ACL must match backing dataset ACL",
                "scope": scope,
                "class_id": class_id,
                "writeback_dataset_id": writeback_dataset_id,
                "backing_dataset_id": backing_dataset_id,
            }

    return None
