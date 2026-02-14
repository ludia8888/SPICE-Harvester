from __future__ import annotations

import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Protocol, Sequence

from shared.utils.access_policy import apply_access_policy

logger = logging.getLogger(__name__)


class _DatasetRegistryLike(Protocol):
    async def get_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
    ) -> Any: ...


@dataclass(frozen=True)
class ActionTargetDataAccessReport:
    denied: List[Dict[str, str]]
    unverifiable: List[Dict[str, str]]


def _target_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None:
        return ""
    text = str(value).strip()
    return text


async def evaluate_action_target_data_access(
    *,
    dataset_registry: _DatasetRegistryLike,
    db_name: str,
    targets: Sequence[Mapping[str, Any]],
    scope: str = "data_access",
) -> ActionTargetDataAccessReport:
    denied: List[Dict[str, str]] = []
    unverifiable: List[Dict[str, str]] = []

    for target in targets:
        class_id = _target_text(target, "class_id")
        instance_id = _target_text(target, "instance_id")
        base_state = target.get("base_state")
        if not class_id or not instance_id or not isinstance(base_state, dict):
            continue

        try:
            access_policy = await dataset_registry.get_access_policy(
                db_name=db_name,
                scope=scope,
                subject_type="object_type",
                subject_id=class_id,
            )
        except Exception as exc:
            logger.warning(
                "Failed to load object_type access policy for class %s: %s",
                class_id,
                exc,
                exc_info=True,
            )
            unverifiable.append({"class_id": class_id, "instance_id": instance_id})
            continue

        policy = access_policy.policy if access_policy is not None else None
        if not isinstance(policy, dict) or not policy:
            continue

        filtered, _info = apply_access_policy([base_state], policy=policy)
        if not filtered:
            denied.append({"class_id": class_id, "instance_id": instance_id})

    return ActionTargetDataAccessReport(denied=denied, unverifiable=unverifiable)
