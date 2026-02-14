from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Mapping, Optional, Protocol, Sequence, Set, Tuple

from shared.utils.access_policy import apply_access_policy
from shared.utils.ontology_type_normalization import normalize_ontology_base_type
from shared.utils.principal_policy import policy_allows

logger = logging.getLogger(__name__)

_ATTACHMENT_BASE_TYPES = {"attachment", "media", "file", "document"}
_OBJECT_SET_BASE_TYPES = {"object_set", "objectset"}


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
    edit_denied: List[Dict[str, str]] = field(default_factory=list)
    edit_unverifiable: List[Dict[str, str]] = field(default_factory=list)


def _target_text(payload: Mapping[str, Any], key: str) -> str:
    value = payload.get(key)
    if value is None:
        return ""
    text = str(value).strip()
    return text


def _extract_changes(target: Mapping[str, Any]) -> Dict[str, Any]:
    changes = target.get("changes")
    return changes if isinstance(changes, dict) else {}


def _extract_changed_fields(changes: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    set_ops = changes.get("set")
    if isinstance(set_ops, dict):
        for key in set_ops.keys():
            text = str(key or "").strip()
            if text:
                out.add(text)
    unset_ops = changes.get("unset")
    if isinstance(unset_ops, list):
        for key in unset_ops:
            text = str(key or "").strip()
            if text:
                out.add(text)
    return out


def _has_link_changes(changes: Dict[str, Any]) -> bool:
    link_add = changes.get("link_add")
    link_remove = changes.get("link_remove")
    return bool((isinstance(link_add, list) and link_add) or (isinstance(link_remove, list) and link_remove))


def _changed_attachment_fields(
    *,
    changed_fields: Set[str],
    field_types: Mapping[str, Any],
) -> List[str]:
    out: List[str] = []
    for field_name in sorted(changed_fields):
        normalized_type = normalize_ontology_base_type(field_types.get(field_name))
        if normalized_type in _ATTACHMENT_BASE_TYPES:
            out.append(field_name)
    return out


def _changed_object_set_fields(
    *,
    changed_fields: Set[str],
    field_types: Mapping[str, Any],
    link_changed: bool,
) -> List[str]:
    out: Set[str] = set()
    for field_name in changed_fields:
        normalized_type = normalize_ontology_base_type(field_types.get(field_name))
        if normalized_type in _OBJECT_SET_BASE_TYPES:
            out.add(field_name)
    if link_changed:
        out.add("__links__")
    return sorted(out)


async def _load_object_type_policy(
    *,
    dataset_registry: _DatasetRegistryLike,
    db_name: str,
    scope: str,
    class_id: str,
    cache: Dict[Tuple[str, str], Tuple[Optional[Dict[str, Any]], Optional[str], bool]],
) -> Tuple[Optional[Dict[str, Any]], Optional[str], bool]:
    key = (scope, class_id)
    cached = cache.get(key)
    if cached is not None:
        return cached
    try:
        rec = await dataset_registry.get_access_policy(
            db_name=db_name,
            scope=scope,
            subject_type="object_type",
            subject_id=class_id,
        )
    except Exception as exc:
        logger.warning(
            "Failed to load object_type policy (scope=%s class_id=%s): %s",
            scope,
            class_id,
            exc,
            exc_info=True,
        )
        error_text = str(exc)
        out = (None, error_text, False)
        cache[key] = out
        return out

    policy = rec.policy if rec is not None else None
    if not isinstance(policy, dict) or not policy:
        out = (None, None, True)
        cache[key] = out
        return out
    out = (policy, None, False)
    cache[key] = out
    return out


async def evaluate_action_target_data_access(
    *,
    dataset_registry: _DatasetRegistryLike,
    db_name: str,
    targets: Sequence[Mapping[str, Any]],
    scope: str = "data_access",
    enforce_data_access_policy: bool = True,
    principal_tags: Optional[Set[str]] = None,
    enforce_object_edit_policy: bool = False,
    enforce_attachment_edit_policy: bool = False,
    enforce_object_set_edit_policy: bool = False,
    fail_on_missing_edit_policy: bool = True,
    object_edit_scope: str = "object_edit",
    attachment_edit_scope: str = "attachment_edit",
    object_set_edit_scope: str = "object_set_edit",
) -> ActionTargetDataAccessReport:
    denied: List[Dict[str, str]] = []
    unverifiable: List[Dict[str, str]] = []
    edit_denied: List[Dict[str, str]] = []
    edit_unverifiable: List[Dict[str, str]] = []
    policy_cache: Dict[Tuple[str, str], Tuple[Optional[Dict[str, Any]], Optional[str], bool]] = {}

    for target in targets:
        class_id = _target_text(target, "class_id")
        instance_id = _target_text(target, "instance_id")
        base_state = target.get("base_state")
        if not class_id or not instance_id or not isinstance(base_state, dict):
            continue

        if enforce_data_access_policy:
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
            if isinstance(policy, dict) and policy:
                filtered, _info = apply_access_policy([base_state], policy=policy)
                if not filtered:
                    denied.append({"class_id": class_id, "instance_id": instance_id})

        if not principal_tags:
            continue

        changes = _extract_changes(target)
        changed_fields = _extract_changed_fields(changes)
        link_changed = _has_link_changes(changes)
        field_types = target.get("field_types")
        field_types_map: Mapping[str, Any] = field_types if isinstance(field_types, Mapping) else {}
        attachment_fields = _changed_attachment_fields(changed_fields=changed_fields, field_types=field_types_map)
        object_set_fields = _changed_object_set_fields(
            changed_fields=changed_fields,
            field_types=field_types_map,
            link_changed=link_changed,
        )
        touched_any = bool(changed_fields or link_changed)

        async def _evaluate_edit_scope(
            *,
            enabled: bool,
            required: bool,
            scope_name: str,
            touched_fields: Optional[List[str]] = None,
        ) -> None:
            if not enabled:
                return
            if not required:
                return
            policy_obj, load_error, missing = await _load_object_type_policy(
                dataset_registry=dataset_registry,
                db_name=db_name,
                scope=scope_name,
                class_id=class_id,
                cache=policy_cache,
            )
            base_payload: Dict[str, str] = {
                "class_id": class_id,
                "instance_id": instance_id,
                "scope": scope_name,
            }
            if touched_fields:
                base_payload["fields"] = ",".join(sorted({str(v).strip() for v in touched_fields if str(v).strip()}))

            if load_error:
                logger.warning(
                    "Failed to load object_type edit policy for class %s (scope=%s): %s",
                    class_id,
                    scope_name,
                    load_error,
                    exc_info=True,
                )
                edit_unverifiable.append(base_payload)
                return
            if missing:
                if fail_on_missing_edit_policy:
                    edit_unverifiable.append(base_payload)
                return
            if not isinstance(policy_obj, dict):
                if fail_on_missing_edit_policy:
                    edit_unverifiable.append(base_payload)
                return
            if not policy_allows(policy=policy_obj, principal_tags=principal_tags):
                edit_denied.append(base_payload)

        await _evaluate_edit_scope(
            enabled=enforce_object_edit_policy,
            required=touched_any,
            scope_name=object_edit_scope,
            touched_fields=sorted(changed_fields),
        )
        await _evaluate_edit_scope(
            enabled=enforce_attachment_edit_policy,
            required=bool(attachment_fields),
            scope_name=attachment_edit_scope,
            touched_fields=attachment_fields,
        )
        await _evaluate_edit_scope(
            enabled=enforce_object_set_edit_policy,
            required=bool(object_set_fields),
            scope_name=object_set_edit_scope,
            touched_fields=object_set_fields,
        )

    return ActionTargetDataAccessReport(
        denied=denied,
        unverifiable=unverifiable,
        edit_denied=edit_denied,
        edit_unverifiable=edit_unverifiable,
    )
