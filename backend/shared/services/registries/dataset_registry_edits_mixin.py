from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

import asyncpg

from shared.services.registries import dataset_registry_edits as _edits_registry
from shared.services.registries import dataset_registry_relationships as _relationship_registry
from shared.services.registries.dataset_registry_models import (
    InstanceEditRecord,
    LinkEditRecord,
    RelationshipIndexResultRecord,
    RelationshipSpecRecord,
    SchemaMigrationPlanRecord,
)
from shared.services.registries.dataset_registry_rows import (
    row_to_instance_edit,
    row_to_link_edit,
    row_to_relationship_index_result,
    row_to_relationship_spec,
    row_to_schema_migration_plan,
)


class DatasetRegistryEditsMixin:
    @staticmethod
    def _row_to_instance_edit(row: asyncpg.Record) -> InstanceEditRecord:
        return row_to_instance_edit(row)

    @staticmethod
    def _row_to_relationship_spec(row: asyncpg.Record) -> RelationshipSpecRecord:
        return row_to_relationship_spec(row)

    @staticmethod
    def _row_to_relationship_index_result(row: asyncpg.Record) -> RelationshipIndexResultRecord:
        return row_to_relationship_index_result(row)

    @staticmethod
    def _row_to_link_edit(row: asyncpg.Record) -> LinkEditRecord:
        return row_to_link_edit(row)

    @staticmethod
    def _row_to_schema_migration_plan(row: asyncpg.Record) -> SchemaMigrationPlanRecord:
        return row_to_schema_migration_plan(row)

    async def record_instance_edit(
        self,
        *,
        db_name: str,
        class_id: str,
        instance_id: str,
        edit_type: str,
        metadata: Optional[Dict[str, Any]] = None,
        status: str = "ACTIVE",
        fields: Optional[List[str]] = None,
    ) -> InstanceEditRecord:
        return await _edits_registry.record_instance_edit(
            self,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            edit_type=edit_type,
            metadata=metadata,
            status=status,
            fields=fields,
        )

    async def count_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        status: Optional[str] = None,
    ) -> int:
        return await _edits_registry.count_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            status=status,
        )

    async def list_instance_edits(
        self,
        *,
        db_name: str,
        class_id: Optional[str] = None,
        instance_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[InstanceEditRecord]:
        return await _edits_registry.list_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            instance_id=instance_id,
            status=status,
            limit=limit,
        )

    async def clear_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
    ) -> int:
        return await _edits_registry.clear_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
        )

    async def remap_instance_edits(
        self,
        *,
        db_name: str,
        class_id: str,
        id_map: Dict[str, str],
        status: Optional[str] = None,
    ) -> int:
        return await _edits_registry.remap_instance_edits(
            self,
            db_name=db_name,
            class_id=class_id,
            id_map=id_map,
            status=status,
        )

    async def get_instance_edit_field_stats(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        status: Optional[str] = "ACTIVE",
    ) -> Dict[str, Any]:
        return await _edits_registry.get_instance_edit_field_stats(
            self,
            db_name=db_name,
            class_id=class_id,
            fields=fields,
            status=status,
        )

    async def apply_instance_edit_field_moves(
        self,
        *,
        db_name: str,
        class_id: str,
        field_moves: Dict[str, str],
        status: Optional[str] = "ACTIVE",
    ) -> int:
        return await _edits_registry.apply_instance_edit_field_moves(
            self,
            db_name=db_name,
            class_id=class_id,
            field_moves=field_moves,
            status=status,
        )

    async def update_instance_edit_status_by_fields(
        self,
        *,
        db_name: str,
        class_id: str,
        fields: List[str],
        new_status: str,
        status: Optional[str] = "ACTIVE",
        metadata_note: Optional[str] = None,
    ) -> int:
        return await _edits_registry.update_instance_edit_status_by_fields(
            self,
            db_name=db_name,
            class_id=class_id,
            fields=fields,
            new_status=new_status,
            status=status,
            metadata_note=metadata_note,
        )

    async def create_relationship_spec(
        self,
        *,
        link_type_id: str,
        db_name: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        spec_type: str,
        dataset_id: str,
        mapping_spec_id: str,
        mapping_spec_version: int,
        spec: Dict[str, Any],
        dataset_version_id: Optional[str] = None,
        status: str = "ACTIVE",
        auto_sync: bool = True,
        relationship_spec_id: Optional[str] = None,
    ) -> RelationshipSpecRecord:
        return await _relationship_registry.create_relationship_spec(
            self,
            link_type_id=link_type_id,
            db_name=db_name,
            source_object_type=source_object_type,
            target_object_type=target_object_type,
            predicate=predicate,
            spec_type=spec_type,
            dataset_id=dataset_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
            spec=spec,
            dataset_version_id=dataset_version_id,
            status=status,
            auto_sync=auto_sync,
            relationship_spec_id=relationship_spec_id,
        )

    async def update_relationship_spec(
        self,
        *,
        relationship_spec_id: str,
        status: Optional[str] = None,
        spec: Optional[Dict[str, Any]] = None,
        auto_sync: Optional[bool] = None,
        dataset_id: Optional[str] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
    ) -> Optional[RelationshipSpecRecord]:
        return await _relationship_registry.update_relationship_spec(
            self,
            relationship_spec_id=relationship_spec_id,
            status=status,
            spec=spec,
            auto_sync=auto_sync,
            dataset_id=dataset_id,
            dataset_version_id=dataset_version_id,
            mapping_spec_id=mapping_spec_id,
            mapping_spec_version=mapping_spec_version,
        )

    async def record_relationship_index_result(
        self,
        *,
        relationship_spec_id: str,
        status: str,
        stats: Optional[Dict[str, Any]] = None,
        errors: Optional[List[Dict[str, Any]]] = None,
        dataset_version_id: Optional[str] = None,
        mapping_spec_version: Optional[int] = None,
        lineage: Optional[Dict[str, Any]] = None,
        indexed_at: Optional[datetime] = None,
    ) -> Optional[RelationshipIndexResultRecord]:
        return await _relationship_registry.record_relationship_index_result(
            self,
            relationship_spec_id=relationship_spec_id,
            status=status,
            stats=stats,
            errors=errors,
            dataset_version_id=dataset_version_id,
            mapping_spec_version=mapping_spec_version,
            lineage=lineage,
            indexed_at=indexed_at,
        )

    async def get_relationship_spec(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
    ) -> Optional[RelationshipSpecRecord]:
        return await _relationship_registry.get_relationship_spec(
            self,
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
        )

    async def list_relationship_specs(
        self,
        *,
        db_name: Optional[str] = None,
        dataset_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        return await _relationship_registry.list_relationship_specs(
            self,
            db_name=db_name,
            dataset_id=dataset_id,
            status=status,
            limit=limit,
        )

    async def list_relationship_specs_by_relationship_object_type(
        self,
        *,
        db_name: str,
        relationship_object_type: str,
        status: Optional[str] = "ACTIVE",
        limit: int = 200,
    ) -> List[RelationshipSpecRecord]:
        return await _relationship_registry.list_relationship_specs_by_relationship_object_type(
            self,
            db_name=db_name,
            relationship_object_type=relationship_object_type,
            status=status,
            limit=limit,
        )

    async def list_relationship_index_results(
        self,
        *,
        relationship_spec_id: Optional[str] = None,
        link_type_id: Optional[str] = None,
        db_name: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[RelationshipIndexResultRecord]:
        return await _edits_registry.list_relationship_index_results(
            self,
            relationship_spec_id=relationship_spec_id,
            link_type_id=link_type_id,
            db_name=db_name,
            status=status,
            limit=limit,
        )

    async def record_link_edit(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: str,
        source_object_type: str,
        target_object_type: str,
        predicate: str,
        source_instance_id: str,
        target_instance_id: str,
        edit_type: str,
        status: str = "ACTIVE",
        metadata: Optional[Dict[str, Any]] = None,
        edit_id: Optional[str] = None,
    ) -> LinkEditRecord:
        return await _edits_registry.record_link_edit(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            source_object_type=source_object_type,
            target_object_type=target_object_type,
            predicate=predicate,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            edit_type=edit_type,
            status=status,
            metadata=metadata,
            edit_id=edit_id,
        )

    async def list_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: Optional[str] = None,
        branch: Optional[str] = None,
        status: Optional[str] = None,
        source_instance_id: Optional[str] = None,
        target_instance_id: Optional[str] = None,
        limit: int = 200,
    ) -> List[LinkEditRecord]:
        return await _edits_registry.list_link_edits(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
            status=status,
            source_instance_id=source_instance_id,
            target_instance_id=target_instance_id,
            limit=limit,
        )

    async def clear_link_edits(
        self,
        *,
        db_name: str,
        link_type_id: str,
        branch: Optional[str] = None,
    ) -> int:
        return await _edits_registry.clear_link_edits(
            self,
            db_name=db_name,
            link_type_id=link_type_id,
            branch=branch,
        )

    async def create_schema_migration_plan(
        self,
        *,
        db_name: str,
        subject_type: str,
        subject_id: str,
        plan: Dict[str, Any],
        status: str = "PENDING",
        plan_id: Optional[str] = None,
    ) -> SchemaMigrationPlanRecord:
        return await _edits_registry.create_schema_migration_plan(
            self,
            db_name=db_name,
            subject_type=subject_type,
            subject_id=subject_id,
            plan=plan,
            status=status,
            plan_id=plan_id,
        )

    async def list_schema_migration_plans(
        self,
        *,
        db_name: Optional[str] = None,
        subject_type: Optional[str] = None,
        subject_id: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 200,
    ) -> List[SchemaMigrationPlanRecord]:
        return await _edits_registry.list_schema_migration_plans(
            self,
            db_name=db_name,
            subject_type=subject_type,
            subject_id=subject_id,
            status=status,
            limit=limit,
        )
