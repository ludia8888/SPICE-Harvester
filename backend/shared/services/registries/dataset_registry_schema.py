from __future__ import annotations

from typing import Any

import asyncpg


async def ensure_tables(registry: Any, conn: asyncpg.Connection) -> None:
    self = registry
    async with conn.transaction():
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.datasets (
                dataset_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                source_type TEXT NOT NULL,
                source_ref TEXT,
                branch TEXT NOT NULL DEFAULT 'main',
                schema_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (db_name, name, branch)
            )
            """
        )

        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.datasets
                ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main'
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.datasets
                DROP CONSTRAINT IF EXISTS datasets_db_name_name_key
            """
        )
        await conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS datasets_db_name_name_branch_key
            ON {self._schema}.datasets(db_name, name, branch)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.dataset_versions (
                version_id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                lakefs_commit_id TEXT NOT NULL,
                artifact_key TEXT,
                row_count INTEGER,
                sample_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                ingest_request_id UUID,
                promoted_from_artifact_id UUID,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (dataset_id, lakefs_commit_id),
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE
            )
            """
        )

        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_versions
                ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT
            """
        )
        await conn.execute(
            f"""
            UPDATE {self._schema}.dataset_versions
            SET lakefs_commit_id = COALESCE(
                NULLIF(lakefs_commit_id, ''),
                NULLIF(
                    CASE
                        WHEN split_part(replace(COALESCE(artifact_key, ''), 's3://', ''), '/', 2)
                             IN ('', 'datasets', 'pipelines', 'pipelines-staging', 'checkpoints', 'events', 'indexes')
                        THEN NULL
                        ELSE split_part(replace(COALESCE(artifact_key, ''), 's3://', ''), '/', 2)
                    END,
                    ''
                ),
                'compat-' || version_id::text
            )
            WHERE lakefs_commit_id IS NULL OR lakefs_commit_id = ''
            """
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.dataset_versions DROP CONSTRAINT IF EXISTS dataset_versions_dataset_id_version_key"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.dataset_versions DROP COLUMN IF EXISTS version"
        )
        await conn.execute(
            f"ALTER TABLE {self._schema}.dataset_versions ALTER COLUMN lakefs_commit_id SET NOT NULL"
        )
        await conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS dataset_versions_dataset_id_lakefs_commit_id_key
            ON {self._schema}.dataset_versions(dataset_id, lakefs_commit_id)
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_versions
                ADD COLUMN IF NOT EXISTS ingest_request_id UUID
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_versions
                ADD COLUMN IF NOT EXISTS promoted_from_artifact_id UUID
            """
        )
        await conn.execute(
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS dataset_versions_ingest_request_id_key
            ON {self._schema}.dataset_versions(ingest_request_id)
            WHERE ingest_request_id IS NOT NULL
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_ingest_request_id
            ON {self._schema}.dataset_versions(ingest_request_id)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_versions_promoted_artifact
            ON {self._schema}.dataset_versions(promoted_from_artifact_id)
            """
        )

        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_datasets_db_name ON {self._schema}.datasets(db_name)"
        )
        await conn.execute(
            f"CREATE INDEX IF NOT EXISTS idx_dataset_versions_dataset_id ON {self._schema}.dataset_versions(dataset_id)"
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_requests (
                ingest_request_id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                db_name TEXT NOT NULL,
                branch TEXT NOT NULL,
                idempotency_key TEXT NOT NULL,
                request_fingerprint TEXT,
                status TEXT NOT NULL,
                lakefs_commit_id TEXT,
                artifact_key TEXT,
                schema_json JSONB,
                schema_status TEXT NOT NULL DEFAULT 'PENDING',
                schema_approved_at TIMESTAMPTZ,
                schema_approved_by TEXT,
                sample_json JSONB,
                row_count INTEGER,
                source_metadata JSONB,
                error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                published_at TIMESTAMPTZ,
                UNIQUE (idempotency_key),
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_ingest_requests_status
            ON {self._schema}.dataset_ingest_requests(status, created_at)
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS schema_json JSONB
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS schema_status TEXT NOT NULL DEFAULT 'PENDING'
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS schema_approved_at TIMESTAMPTZ
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS schema_approved_by TEXT
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS sample_json JSONB
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS row_count INTEGER
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_requests
                ADD COLUMN IF NOT EXISTS source_metadata JSONB
            """
        )
        await conn.execute(
            f"""
            UPDATE {self._schema}.dataset_ingest_requests
            SET schema_status = 'PENDING'
            WHERE schema_status IS NULL
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_transactions (
                transaction_id UUID PRIMARY KEY,
                ingest_request_id UUID NOT NULL,
                status TEXT NOT NULL,
                lakefs_commit_id TEXT,
                artifact_key TEXT,
                error TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                committed_at TIMESTAMPTZ,
                aborted_at TIMESTAMPTZ,
                UNIQUE (ingest_request_id),
                FOREIGN KEY (ingest_request_id)
                    REFERENCES {self._schema}.dataset_ingest_requests(ingest_request_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_ingest_transactions_status
            ON {self._schema}.dataset_ingest_transactions(status, created_at)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.dataset_ingest_outbox (
                outbox_id UUID PRIMARY KEY,
                ingest_request_id UUID NOT NULL,
                kind TEXT NOT NULL,
                payload JSONB NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending',
                publish_attempts INTEGER NOT NULL DEFAULT 0,
                retry_count INTEGER NOT NULL DEFAULT 0,
                error TEXT,
                last_error TEXT,
                claimed_by TEXT,
                claimed_at TIMESTAMPTZ,
                next_attempt_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (ingest_request_id)
                    REFERENCES {self._schema}.dataset_ingest_requests(ingest_request_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.dataset_ingest_outbox
                ADD COLUMN IF NOT EXISTS claimed_by TEXT,
                ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS next_attempt_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0,
                ADD COLUMN IF NOT EXISTS last_error TEXT
            """
        )
        await conn.execute(
            f"""
            UPDATE {self._schema}.dataset_ingest_outbox
            SET retry_count = GREATEST(retry_count, publish_attempts),
                last_error = COALESCE(last_error, error)
            WHERE retry_count = 0 AND publish_attempts > 0
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_status
            ON {self._schema}.dataset_ingest_outbox(status, created_at)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_status_next
            ON {self._schema}.dataset_ingest_outbox(status, next_attempt_at, created_at)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_dataset_ingest_outbox_claimed
            ON {self._schema}.dataset_ingest_outbox(status, claimed_at)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.backing_datasources (
                backing_id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                db_name TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                source_type TEXT NOT NULL DEFAULT 'dataset',
                source_ref TEXT,
                branch TEXT NOT NULL DEFAULT 'main',
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (dataset_id, branch),
                UNIQUE (db_name, name, branch),
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_backing_datasources_dataset
            ON {self._schema}.backing_datasources(dataset_id, branch)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.backing_datasource_versions (
                backing_version_id UUID PRIMARY KEY,
                backing_id UUID NOT NULL,
                dataset_version_id UUID NOT NULL,
                schema_hash TEXT NOT NULL,
                artifact_key TEXT,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (backing_id, dataset_version_id),
                FOREIGN KEY (backing_id)
                    REFERENCES {self._schema}.backing_datasources(backing_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (dataset_version_id)
                    REFERENCES {self._schema}.dataset_versions(version_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_backing_versions_dataset_version
            ON {self._schema}.backing_datasource_versions(dataset_version_id)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.key_specs (
                key_spec_id UUID PRIMARY KEY,
                dataset_id UUID NOT NULL,
                dataset_version_id UUID,
                spec JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (dataset_id, dataset_version_id),
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (dataset_version_id)
                    REFERENCES {self._schema}.dataset_versions(version_id)
                    ON DELETE SET NULL
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_key_specs_dataset
            ON {self._schema}.key_specs(dataset_id)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.gate_policies (
                policy_id UUID PRIMARY KEY,
                scope TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                rules JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (scope, name)
            )
            """
        )
        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.gate_results (
                result_id UUID PRIMARY KEY,
                policy_id UUID NOT NULL,
                scope TEXT NOT NULL,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                status TEXT NOT NULL,
                details JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (policy_id)
                    REFERENCES {self._schema}.gate_policies(policy_id)
                    ON DELETE CASCADE
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_gate_results_subject
            ON {self._schema}.gate_results(scope, subject_type, subject_id)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.access_policies (
                policy_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                scope TEXT NOT NULL,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                policy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (db_name, scope, subject_type, subject_id)
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_access_policies_subject
            ON {self._schema}.access_policies(db_name, subject_type, subject_id)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.instance_edits (
                edit_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                class_id TEXT NOT NULL,
                instance_id TEXT NOT NULL,
                edit_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                fields JSONB NOT NULL DEFAULT '[]'::jsonb,
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_instance_edits_class
            ON {self._schema}.instance_edits(db_name, class_id)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_instance_edits_instance
            ON {self._schema}.instance_edits(db_name, class_id, instance_id)
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.instance_edits
                ADD COLUMN IF NOT EXISTS status TEXT NOT NULL DEFAULT 'ACTIVE'
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.instance_edits
                ADD COLUMN IF NOT EXISTS fields JSONB NOT NULL DEFAULT '[]'::jsonb
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_instance_edits_status
            ON {self._schema}.instance_edits(db_name, class_id, status)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_instance_edits_fields
            ON {self._schema}.instance_edits
            USING GIN (fields)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.relationship_specs (
                relationship_spec_id UUID PRIMARY KEY,
                link_type_id TEXT NOT NULL,
                db_name TEXT NOT NULL,
                source_object_type TEXT NOT NULL,
                target_object_type TEXT NOT NULL,
                predicate TEXT NOT NULL,
                spec_type TEXT NOT NULL,
                dataset_id UUID NOT NULL,
                dataset_version_id UUID,
                mapping_spec_id UUID NOT NULL,
                mapping_spec_version INTEGER NOT NULL DEFAULT 1,
                spec JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                auto_sync BOOLEAN NOT NULL DEFAULT TRUE,
                last_index_status TEXT,
                last_indexed_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (link_type_id),
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (dataset_version_id)
                    REFERENCES {self._schema}.dataset_versions(version_id)
                    ON DELETE SET NULL
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_relationship_specs_dataset
            ON {self._schema}.relationship_specs(dataset_id, status)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_relationship_specs_db
            ON {self._schema}.relationship_specs(db_name, status)
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.relationship_specs
                ADD COLUMN IF NOT EXISTS last_index_result_id UUID
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.relationship_specs
                ADD COLUMN IF NOT EXISTS last_index_stats JSONB NOT NULL DEFAULT '{{}}'::jsonb
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.relationship_specs
                ADD COLUMN IF NOT EXISTS last_index_dataset_version_id UUID
            """
        )
        await conn.execute(
            f"""
            ALTER TABLE {self._schema}.relationship_specs
                ADD COLUMN IF NOT EXISTS last_index_mapping_spec_version INTEGER
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.relationship_index_results (
                result_id UUID PRIMARY KEY,
                relationship_spec_id UUID NOT NULL,
                link_type_id TEXT NOT NULL,
                db_name TEXT NOT NULL,
                dataset_id UUID NOT NULL,
                dataset_version_id UUID,
                mapping_spec_id UUID NOT NULL,
                mapping_spec_version INTEGER NOT NULL,
                status TEXT NOT NULL,
                stats JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                errors JSONB NOT NULL DEFAULT '[]'::jsonb,
                lineage JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                FOREIGN KEY (relationship_spec_id)
                    REFERENCES {self._schema}.relationship_specs(relationship_spec_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (dataset_id)
                    REFERENCES {self._schema}.datasets(dataset_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (dataset_version_id)
                    REFERENCES {self._schema}.dataset_versions(version_id)
                    ON DELETE SET NULL
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_relationship_index_results_spec
            ON {self._schema}.relationship_index_results(relationship_spec_id, status)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_relationship_index_results_link
            ON {self._schema}.relationship_index_results(link_type_id, status)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_relationship_index_results_db
            ON {self._schema}.relationship_index_results(db_name, status)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.link_edits (
                edit_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                link_type_id TEXT NOT NULL,
                branch TEXT NOT NULL DEFAULT 'main',
                source_object_type TEXT NOT NULL,
                target_object_type TEXT NOT NULL,
                predicate TEXT NOT NULL,
                source_instance_id TEXT NOT NULL,
                target_instance_id TEXT NOT NULL,
                edit_type TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'ACTIVE',
                metadata JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_link_edits_link
            ON {self._schema}.link_edits(link_type_id, status)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_link_edits_source
            ON {self._schema}.link_edits(db_name, source_object_type, source_instance_id)
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_link_edits_target
            ON {self._schema}.link_edits(db_name, target_object_type, target_instance_id)
            """
        )

        await conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self._schema}.schema_migration_plans (
                plan_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                subject_type TEXT NOT NULL,
                subject_id TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'PENDING',
                plan JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
            """
        )
        await conn.execute(
            f"""
            CREATE INDEX IF NOT EXISTS idx_schema_migration_plans_subject
            ON {self._schema}.schema_migration_plans(db_name, subject_type, subject_id)
            """
        )
