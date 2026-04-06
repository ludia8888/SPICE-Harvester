"""
Dataset Registry - durable dataset metadata in Postgres.

Stores dataset metadata + versions (artifact references + samples).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional
from uuid import uuid4

import asyncpg

from shared.config.settings import get_settings
from shared.observability.context_propagation import enrich_metadata_with_current_trace
from shared.services.registries import dataset_registry_catalog as _catalog_registry
from shared.services.registries import dataset_registry_edits as _edits_registry
from shared.services.registries import dataset_registry_governance as _governance_registry
from shared.services.registries import dataset_registry_ingest as _ingest_registry
from shared.services.registries import dataset_registry_publish as _publish_registry
from shared.services.registries import dataset_registry_schema as _schema_registry
from shared.services.registries.dataset_registry_models import (
    AccessPolicyRecord,
    BackingDatasourceRecord,
    BackingDatasourceVersionRecord,
    DatasetIngestOutboxItem,
    DatasetIngestRequestRecord,
    DatasetIngestTransactionRecord,
    DatasetRecord,
    DatasetVersionRecord,
    GatePolicyRecord,
    GateResultRecord,
    InstanceEditRecord,
    KeySpecRecord,
    LinkEditRecord,
    RelationshipIndexResultRecord,
    RelationshipSpecRecord,
    SchemaMigrationPlanRecord,
)
from shared.services.registries.dataset_registry_rows import (
    key_spec_scope_lock_key,
    row_to_access_policy,
    row_to_backing,
    row_to_backing_version,
    row_to_gate_policy,
    row_to_gate_result,
    row_to_instance_edit,
    row_to_key_spec,
    row_to_link_edit,
    row_to_relationship_index_result,
    row_to_relationship_spec,
    row_to_schema_migration_plan,
)
from shared.services.registries import dataset_registry_relationships as _relationship_registry
from shared.services.registries.postgres_schema_registry import PostgresSchemaRegistry
from shared.services.registries.dataset_registry_catalog_mixin import DatasetRegistryCatalogMixin
from shared.services.registries.dataset_registry_governance_mixin import DatasetRegistryGovernanceMixin
from shared.services.registries.dataset_registry_edits_mixin import DatasetRegistryEditsMixin
from shared.services.registries.dataset_registry_ingest_mixin import DatasetRegistryIngestMixin
from shared.utils.s3_uri import is_s3_uri, parse_s3_uri
from shared.utils.json_utils import coerce_json_dataset, normalize_json_payload
from shared.utils.schema_hash import compute_schema_hash
from shared.utils.time_utils import utcnow

logger = logging.getLogger(__name__)


class DatasetRegistry(
    DatasetRegistryCatalogMixin,
    DatasetRegistryGovernanceMixin,
    DatasetRegistryEditsMixin,
    DatasetRegistryIngestMixin,
    PostgresSchemaRegistry,
):
    _REQUIRED_TABLES = (
        "datasets",
        "dataset_versions",
        "dataset_ingest_requests",
        "dataset_ingest_transactions",
        "dataset_ingest_outbox",
        "backing_datasources",
        "backing_datasource_versions",
        "key_specs",
        "gate_policies",
        "gate_results",
        "access_policies",
        "instance_edits",
        "relationship_specs",
        "relationship_index_results",
        "link_edits",
        "schema_migration_plans",
    )

    def __init__(
        self,
        *,
        dsn: Optional[str] = None,
        schema: str = "spice_datasets",
        pool_min: Optional[int] = None,
        pool_max: Optional[int] = None,
        allow_runtime_ddl_bootstrap: Optional[bool] = None,
    ):
        perf = get_settings().performance
        pool_min_value = int(pool_min) if pool_min is not None else int(perf.dataset_registry_pg_pool_min)
        pool_max_value = int(pool_max) if pool_max is not None else int(perf.dataset_registry_pg_pool_max)
        super().__init__(
            dsn=dsn,
            schema=schema,
            pool_min=pool_min_value,
            pool_max=pool_max_value,
            command_timeout=int(perf.dataset_registry_pg_command_timeout_seconds),
            allow_runtime_ddl_bootstrap=allow_runtime_ddl_bootstrap,
        )

    async def _ensure_tables(self, conn: asyncpg.Connection) -> None:
        await _schema_registry.ensure_tables(self, conn)
