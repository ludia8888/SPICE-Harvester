from __future__ import annotations

from typing import Any, Dict, List, Optional

import asyncpg

from shared.services.registries import dataset_registry_governance as _governance_registry
from shared.services.registries.dataset_registry_models import (
    AccessPolicyRecord,
    GatePolicyRecord,
    GateResultRecord,
    KeySpecRecord,
)
from shared.services.registries.dataset_registry_rows import (
    key_spec_scope_lock_key,
    row_to_access_policy,
    row_to_gate_policy,
    row_to_gate_result,
    row_to_key_spec,
)


class DatasetRegistryGovernanceMixin:
    @staticmethod
    def _row_to_key_spec(row: asyncpg.Record) -> KeySpecRecord:
        return row_to_key_spec(row)

    @staticmethod
    def _key_spec_scope_lock_key(*, dataset_id: str, dataset_version_id: Optional[str]) -> str:
        return key_spec_scope_lock_key(dataset_id=dataset_id, dataset_version_id=dataset_version_id)

    @staticmethod
    def _row_to_gate_policy(row: asyncpg.Record) -> GatePolicyRecord:
        return row_to_gate_policy(row)

    @staticmethod
    def _row_to_gate_result(row: asyncpg.Record) -> GateResultRecord:
        return row_to_gate_result(row)

    @staticmethod
    def _row_to_access_policy(row: asyncpg.Record) -> AccessPolicyRecord:
        return row_to_access_policy(row)

    async def create_key_spec(
        self,
        *,
        dataset_id: str,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        key_spec_id: Optional[str] = None,
    ) -> KeySpecRecord:
        return await _governance_registry.create_key_spec(
            self,
            dataset_id=dataset_id,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            key_spec_id=key_spec_id,
        )

    async def _get_key_spec_for_scope(
        self,
        conn: asyncpg.Connection,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[asyncpg.Record]:
        return await _governance_registry.get_key_spec_for_scope(
            self,
            conn,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )

    async def get_or_create_key_spec(
        self,
        *,
        dataset_id: str,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        key_spec_id: Optional[str] = None,
    ) -> tuple[KeySpecRecord, bool]:
        return await _governance_registry.get_or_create_key_spec(
            self,
            dataset_id=dataset_id,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            key_spec_id=key_spec_id,
        )

    async def get_key_spec(self, *, key_spec_id: str) -> Optional[KeySpecRecord]:
        return await _governance_registry.get_key_spec(self, key_spec_id=key_spec_id)

    async def get_key_spec_for_dataset(
        self,
        *,
        dataset_id: str,
        dataset_version_id: Optional[str] = None,
    ) -> Optional[KeySpecRecord]:
        return await _governance_registry.get_key_spec_for_dataset(
            self,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
        )

    async def list_key_specs(
        self,
        *,
        dataset_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[KeySpecRecord]:
        return await _governance_registry.list_key_specs(
            self,
            dataset_id=dataset_id,
            limit=limit,
        )

    async def upsert_gate_policy(
        self,
        *,
        scope: str,
        name: str,
        description: Optional[str] = None,
        rules: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> GatePolicyRecord:
        return await _governance_registry.upsert_gate_policy(
            self,
            scope=scope,
            name=name,
            description=description,
            rules=rules,
            status=status,
        )

    async def get_gate_policy(
        self,
        *,
        scope: str,
        name: str,
    ) -> Optional[GatePolicyRecord]:
        return await _governance_registry.get_gate_policy(
            self,
            scope=scope,
            name=name,
        )

    async def list_gate_policies(
        self,
        *,
        scope: Optional[str] = None,
        limit: int = 200,
    ) -> List[GatePolicyRecord]:
        return await _governance_registry.list_gate_policies(
            self,
            scope=scope,
            limit=limit,
        )

    async def record_gate_result(
        self,
        *,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: str,
        details: Optional[Dict[str, Any]] = None,
        policy_name: str = "default",
    ) -> GateResultRecord:
        return await _governance_registry.record_gate_result(
            self,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            details=details,
            policy_name=policy_name,
        )

    async def list_gate_results(
        self,
        *,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[GateResultRecord]:
        return await _governance_registry.list_gate_results(
            self,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            limit=limit,
        )

    async def upsert_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        policy: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
    ) -> AccessPolicyRecord:
        return await _governance_registry.upsert_access_policy(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            policy=policy,
            status=status,
        )

    async def get_access_policy(
        self,
        *,
        db_name: str,
        scope: str,
        subject_type: str,
        subject_id: str,
        status: Optional[str] = "ACTIVE",
    ) -> Optional[AccessPolicyRecord]:
        return await _governance_registry.get_access_policy(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
        )

    async def list_access_policies(
        self,
        *,
        db_name: Optional[str] = None,
        scope: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[AccessPolicyRecord]:
        return await _governance_registry.list_access_policies(
            self,
            db_name=db_name,
            scope=scope,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            limit=limit,
        )
