from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List

from oms.services.ontology_resources import OntologyResourceService
from shared.config.app_config import AppConfig
from shared.errors.infra_errors import StorageUnavailableError
from shared.services.storage.storage_service import StorageService
from shared.utils.action_runtime_contracts import load_action_target_runtime_contract


@dataclass(frozen=True)
class LoadedActionTargetState:
    class_id: str
    instance_id: str
    base_state: Dict[str, Any]
    interfaces: List[str]
    field_types: Dict[str, str]


class ActionTargetStateLoadError(RuntimeError):
    def __init__(self, kind: str, message: str, **details: Any) -> None:
        super().__init__(message)
        self.kind = kind
        self.details = details


async def load_action_target_states(
    *,
    db_name: str,
    base_branch: str,
    compiled_targets: Iterable[Any],
    resources: OntologyResourceService,
    storage: StorageService,
    required_interfaces: set[str] | None = None,
    allow_missing_base_state: bool = False,
    skip_invalid_targets: bool = False,
    contract_branch: str | None = None,
    load_runtime_contract_fn: Any = load_action_target_runtime_contract,
) -> list[LoadedActionTargetState]:
    required_interface_set = set(required_interfaces or ())
    effective_contract_branch = str(contract_branch or base_branch)
    contract_cache: dict[str, tuple[list[str], dict[str, str]]] = {}
    state_cache: dict[tuple[str, str], LoadedActionTargetState] = {}
    loaded: list[LoadedActionTargetState] = []

    for item in compiled_targets:
        class_id = str(getattr(item, "class_id", "") or "").strip()
        instance_id = str(getattr(item, "instance_id", "") or "").strip()
        if not class_id or not instance_id:
            if skip_invalid_targets:
                continue
            raise ActionTargetStateLoadError(
                "invalid_target",
                "each compiled target requires class_id and instance_id",
                class_id=class_id,
                instance_id=instance_id,
            )

        if class_id not in contract_cache:
            contract = await load_runtime_contract_fn(
                db_name=db_name,
                class_id=class_id,
                branch=effective_contract_branch,
                resources=resources,
            )
            if contract is None:
                raise ActionTargetStateLoadError(
                    "target_class_not_found",
                    "Target class not found at ontology commit",
                    class_id=class_id,
                    ontology_commit_id=effective_contract_branch,
                )
            contract_cache[class_id] = (list(contract.interfaces), dict(contract.field_types or {}))

        interfaces, field_types = contract_cache[class_id]
        if required_interface_set:
            implemented = set(interfaces)
            missing = sorted(required_interface_set - implemented)
            if missing:
                raise ActionTargetStateLoadError(
                    "required_interfaces_missing",
                    "Action target class does not satisfy required interfaces",
                    class_id=class_id,
                    required_interfaces=sorted(required_interface_set),
                    implemented_interfaces=sorted(implemented),
                    missing_interfaces=missing,
                )

        key = (class_id, instance_id)
        cached = state_cache.get(key)
        if cached is None:
            prefix = f"{db_name}/{base_branch}/{class_id}/{instance_id}/"
            try:
                command_files = await storage.list_command_files(bucket=AppConfig.INSTANCE_BUCKET, prefix=prefix)
                base_state = await storage.replay_instance_state(
                    bucket=AppConfig.INSTANCE_BUCKET,
                    command_files=command_files,
                    strict=True,
                )
            except Exception as exc:
                storage_error = StorageUnavailableError(
                    "Unable to load target base state",
                    operation="load_action_target_states",
                    bucket=AppConfig.INSTANCE_BUCKET,
                    path=prefix,
                    cause=exc,
                )
                raise ActionTargetStateLoadError(
                    "storage_unavailable",
                    "Unable to load target base state",
                    class_id=class_id,
                    instance_id=instance_id,
                    base_branch=base_branch,
                ) from storage_error

            if not isinstance(base_state, dict):
                base_state = {}
            if not allow_missing_base_state and not base_state:
                raise ActionTargetStateLoadError(
                    "base_state_not_found",
                    "Base instance state not found",
                    class_id=class_id,
                    instance_id=instance_id,
                    base_branch=base_branch,
                )

            cached = LoadedActionTargetState(
                class_id=class_id,
                instance_id=instance_id,
                base_state=base_state,
                interfaces=list(interfaces),
                field_types=dict(field_types),
            )
            state_cache[key] = cached

        loaded.append(cached)

    return loaded
