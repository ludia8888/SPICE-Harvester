from __future__ import annotations

import asyncpg
from contextlib import asynccontextmanager


async def _execute_statements(conn: asyncpg.Connection, statements: tuple[str, ...]) -> None:
    for sql in statements:
        await conn.execute(sql)


@asynccontextmanager
async def _transaction_if_available(conn: asyncpg.Connection):
    transaction_factory = getattr(conn, "transaction", None)
    if callable(transaction_factory):
        async with transaction_factory():
            yield
        return
    yield


async def ensure_pipeline_registry_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    async with _transaction_if_available(conn):
        await _ensure_lakefs_credentials_schema(conn, schema=schema)
        await _ensure_pipeline_catalog_schema(conn, schema=schema)
        await _ensure_pipeline_execution_schema(conn, schema=schema)
        await _ensure_pipeline_aux_schema(conn, schema=schema)


async def _ensure_lakefs_credentials_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    await _execute_statements(
        conn,
        (
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.lakefs_credentials (
                principal_type TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                access_key_id TEXT NOT NULL,
                secret_access_key_enc TEXT NOT NULL,
                created_by TEXT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (principal_type, principal_id)
            )
            """,
            f"""
            DO $$
            BEGIN
                ALTER TABLE {schema}.lakefs_credentials
                    ADD CONSTRAINT lakefs_credentials_principal_type_check
                    CHECK (principal_type IN ('user', 'service'));
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """,
        ),
    )


async def _ensure_pipeline_catalog_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    await _execute_statements(
        conn,
        (
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipelines (
                pipeline_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                pipeline_type TEXT NOT NULL,
                location TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'draft',
                branch TEXT NOT NULL DEFAULT 'main',
                lakefs_repository TEXT,
                proposal_status TEXT,
                proposal_title TEXT,
                proposal_description TEXT,
                proposal_submitted_at TIMESTAMPTZ,
                proposal_reviewed_at TIMESTAMPTZ,
                proposal_review_comment TEXT,
                proposal_bundle JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                last_preview_status TEXT,
                last_preview_at TIMESTAMPTZ,
                last_preview_rows INTEGER,
                last_preview_job_id TEXT,
                last_preview_node_id TEXT,
                last_preview_sample JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                last_preview_nodes JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                last_build_status TEXT,
                last_build_at TIMESTAMPTZ,
                last_build_output JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                deployed_at TIMESTAMPTZ,
                deployed_commit_id TEXT,
                schedule_interval_seconds INTEGER,
                schedule_cron TEXT,
                last_scheduled_at TIMESTAMPTZ,
                version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (db_name, name, branch)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_branches (
                db_name TEXT NOT NULL,
                branch TEXT NOT NULL,
                archived BOOLEAN NOT NULL DEFAULT FALSE,
                archived_at TIMESTAMPTZ,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (db_name, branch)
            )
            """,
            f"""
            ALTER TABLE {schema}.pipelines
                ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
                ADD COLUMN IF NOT EXISTS lakefs_repository TEXT,
                ADD COLUMN IF NOT EXISTS proposal_status TEXT,
                ADD COLUMN IF NOT EXISTS proposal_title TEXT,
                ADD COLUMN IF NOT EXISTS proposal_description TEXT,
                ADD COLUMN IF NOT EXISTS proposal_submitted_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS proposal_reviewed_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS proposal_review_comment TEXT,
                ADD COLUMN IF NOT EXISTS proposal_bundle JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                ADD COLUMN IF NOT EXISTS last_preview_job_id TEXT,
                ADD COLUMN IF NOT EXISTS last_preview_node_id TEXT,
                ADD COLUMN IF NOT EXISTS last_preview_nodes JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                ADD COLUMN IF NOT EXISTS deployed_commit_id TEXT,
                ADD COLUMN IF NOT EXISTS schedule_interval_seconds INTEGER,
                ADD COLUMN IF NOT EXISTS schedule_cron TEXT,
                ADD COLUMN IF NOT EXISTS last_scheduled_at TIMESTAMPTZ,
                ADD COLUMN IF NOT EXISTS version INTEGER NOT NULL DEFAULT 1
            """,
            f"""
            INSERT INTO {schema}.pipeline_branches (db_name, branch, archived)
            SELECT DISTINCT db_name, branch, FALSE
            FROM {schema}.pipelines
            ON CONFLICT (db_name, branch) DO NOTHING
            """,
            f"""
            ALTER TABLE {schema}.pipelines
                DROP COLUMN IF EXISTS branch_base_branch,
                DROP COLUMN IF EXISTS branch_base_version,
                DROP COLUMN IF EXISTS deployed_version
            """,
            f"""
            ALTER TABLE {schema}.pipelines
                DROP CONSTRAINT IF EXISTS pipelines_db_name_name_key
            """,
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS pipelines_db_name_name_branch_key
            ON {schema}.pipelines(db_name, name, branch)
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_versions (
                version_id UUID PRIMARY KEY,
                pipeline_id UUID NOT NULL,
                branch TEXT NOT NULL DEFAULT 'main',
                lakefs_commit_id TEXT NOT NULL,
                definition_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (pipeline_id, branch, lakefs_commit_id),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE
            )
            """,
            f"""
            ALTER TABLE {schema}.pipeline_versions
                ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main',
                ADD COLUMN IF NOT EXISTS lakefs_commit_id TEXT
            """,
            f"""
            ALTER TABLE {schema}.pipeline_versions
                DROP COLUMN IF EXISTS version
            """,
            f"""
            ALTER TABLE {schema}.pipeline_versions
                DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_branch_version_key
            """,
            f"""
            CREATE UNIQUE INDEX IF NOT EXISTS pipeline_versions_pipeline_id_branch_commit_key
            ON {schema}.pipeline_versions(pipeline_id, branch, lakefs_commit_id)
            """,
        ),
    )


async def _ensure_pipeline_execution_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    await _execute_statements(
        conn,
        (
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_runs (
                run_id UUID PRIMARY KEY,
                pipeline_id UUID NOT NULL,
                job_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                node_id TEXT,
                row_count INTEGER,
                sample_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                output_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                pipeline_spec_commit_id TEXT,
                pipeline_spec_hash TEXT,
                input_lakefs_commits JSONB,
                output_lakefs_commit_id TEXT,
                spark_conf JSONB,
                code_version TEXT,
                started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                finished_at TIMESTAMPTZ,
                UNIQUE (pipeline_id, job_id),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_artifacts (
                artifact_id UUID PRIMARY KEY,
                pipeline_id UUID NOT NULL,
                job_id TEXT NOT NULL,
                run_id UUID,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                definition_hash TEXT,
                definition_commit_id TEXT,
                pipeline_spec_hash TEXT,
                pipeline_spec_commit_id TEXT,
                inputs JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                lakefs_repository TEXT,
                lakefs_branch TEXT,
                lakefs_commit_id TEXT,
                outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
                declared_outputs JSONB NOT NULL DEFAULT '[]'::jsonb,
                sampling_strategy JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                error JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (pipeline_id, job_id, mode),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE
            )
            """,
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'pipeline_artifacts_outputs_array'
                      AND conrelid = '{schema}.pipeline_artifacts'::regclass
                ) THEN
                    ALTER TABLE {schema}.pipeline_artifacts
                    ADD CONSTRAINT pipeline_artifacts_outputs_array
                    CHECK (jsonb_typeof(outputs) = 'array') NOT VALID;
                END IF;
            END $$;
            """,
            f"""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint
                    WHERE conname = 'pipeline_artifacts_declared_outputs_array'
                      AND conrelid = '{schema}.pipeline_artifacts'::regclass
                ) THEN
                    ALTER TABLE {schema}.pipeline_artifacts
                    ADD CONSTRAINT pipeline_artifacts_declared_outputs_array
                    CHECK (jsonb_typeof(declared_outputs) = 'array') NOT VALID;
                END IF;
            END $$;
            """,
            f"""
            UPDATE {schema}.pipeline_artifacts
            SET outputs = jsonb_build_array(outputs)
            WHERE jsonb_typeof(outputs) = 'object'
            """,
            f"""
            UPDATE {schema}.pipeline_artifacts
            SET outputs = '[]'::jsonb
            WHERE outputs IS NULL OR jsonb_typeof(outputs) <> 'array'
            """,
            f"""
            UPDATE {schema}.pipeline_artifacts
            SET declared_outputs = jsonb_build_array(declared_outputs)
            WHERE jsonb_typeof(declared_outputs) = 'object'
            """,
            f"""
            UPDATE {schema}.pipeline_artifacts
            SET declared_outputs = '[]'::jsonb
            WHERE declared_outputs IS NULL OR jsonb_typeof(declared_outputs) <> 'array'
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.promotion_manifests (
                manifest_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                pipeline_id UUID NOT NULL,
                db_name TEXT NOT NULL,
                build_job_id TEXT NOT NULL,
                artifact_id UUID,
                definition_hash TEXT,
                lakefs_repository TEXT NOT NULL,
                lakefs_commit_id TEXT NOT NULL,
                ontology_commit_id TEXT NOT NULL,
                mapping_spec_id TEXT,
                mapping_spec_version INTEGER,
                mapping_spec_target_class_id TEXT,
                promoted_dataset_version_id UUID NOT NULL,
                promoted_dataset_name TEXT,
                target_branch TEXT NOT NULL,
                promoted_by TEXT,
                promoted_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                metadata JSONB
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_pipeline_id ON {schema}.promotion_manifests(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_build_job ON {schema}.promotion_manifests(build_job_id)",
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_artifact_id ON {schema}.promotion_manifests(artifact_id)",
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_dataset_version ON {schema}.promotion_manifests(promoted_dataset_version_id)",
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_lakefs_commit ON {schema}.promotion_manifests(lakefs_commit_id)",
            f"CREATE INDEX IF NOT EXISTS idx_promotion_manifests_ontology_commit ON {schema}.promotion_manifests(ontology_commit_id)",
            f"""
            ALTER TABLE {schema}.pipeline_runs
                ADD COLUMN IF NOT EXISTS pipeline_spec_commit_id TEXT,
                ADD COLUMN IF NOT EXISTS pipeline_spec_hash TEXT,
                ADD COLUMN IF NOT EXISTS input_lakefs_commits JSONB,
                ADD COLUMN IF NOT EXISTS output_lakefs_commit_id TEXT,
                ADD COLUMN IF NOT EXISTS spark_conf JSONB,
                ADD COLUMN IF NOT EXISTS code_version TEXT
            """,
        ),
    )


async def _ensure_pipeline_aux_schema(conn: asyncpg.Connection, *, schema: str) -> None:
    await _execute_statements(
        conn,
        (
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_watermarks (
                pipeline_id UUID NOT NULL,
                branch TEXT NOT NULL,
                watermarks_json JSONB NOT NULL DEFAULT '{{}}'::jsonb,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (pipeline_id, branch),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_dependencies (
                pipeline_id UUID NOT NULL,
                depends_on_pipeline_id UUID NOT NULL,
                required_status TEXT NOT NULL DEFAULT 'DEPLOYED',
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (pipeline_id, depends_on_pipeline_id),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE,
                FOREIGN KEY (depends_on_pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE RESTRICT
            )
            """,
            f"""
            DO $$
            BEGIN
                ALTER TABLE {schema}.pipeline_dependencies
                    ADD CONSTRAINT pipeline_dependencies_required_status_check
                    CHECK (required_status IN ('DEPLOYED', 'SUCCESS'));
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_permissions (
                pipeline_id UUID NOT NULL,
                principal_type TEXT NOT NULL,
                principal_id TEXT NOT NULL,
                role TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                PRIMARY KEY (pipeline_id, principal_type, principal_id),
                FOREIGN KEY (pipeline_id)
                    REFERENCES {schema}.pipelines(pipeline_id)
                    ON DELETE CASCADE
            )
            """,
            f"""
            DO $$
            BEGIN
                ALTER TABLE {schema}.pipeline_permissions
                    ADD CONSTRAINT pipeline_permissions_role_check
                    CHECK (role IN ('read', 'edit', 'approve', 'admin'));
            EXCEPTION
                WHEN duplicate_object THEN NULL;
            END $$;
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_udfs (
                udf_id UUID PRIMARY KEY,
                db_name TEXT NOT NULL,
                name TEXT NOT NULL,
                description TEXT,
                latest_version INTEGER NOT NULL DEFAULT 1,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (db_name, name)
            )
            """,
            f"""
            CREATE TABLE IF NOT EXISTS {schema}.pipeline_udf_versions (
                version_id UUID PRIMARY KEY,
                udf_id UUID NOT NULL,
                version INTEGER NOT NULL,
                code TEXT NOT NULL,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                UNIQUE (udf_id, version),
                FOREIGN KEY (udf_id)
                    REFERENCES {schema}.pipeline_udfs(udf_id)
                    ON DELETE CASCADE
            )
            """,
            f"CREATE INDEX IF NOT EXISTS idx_pipelines_db_name ON {schema}.pipelines(db_name)",
            f"CREATE INDEX IF NOT EXISTS idx_pipelines_version ON {schema}.pipelines(pipeline_id, version)",
            f"CREATE INDEX IF NOT EXISTS idx_pipelines_schedule ON {schema}.pipelines(schedule_interval_seconds)",
            f"CREATE INDEX IF NOT EXISTS idx_pipelines_schedule_cron ON {schema}.pipelines(schedule_cron)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_versions_pipeline_id ON {schema}.pipeline_versions(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_versions_branch ON {schema}.pipeline_versions(branch)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_pipeline_id ON {schema}.pipeline_artifacts(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_artifacts_job_id ON {schema}.pipeline_artifacts(job_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_pipeline_id ON {schema}.pipeline_dependencies(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_dependencies_depends_on ON {schema}.pipeline_dependencies(depends_on_pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_pipeline_id ON {schema}.pipeline_permissions(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_permissions_principal ON {schema}.pipeline_permissions(principal_type, principal_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_watermarks_pipeline_id ON {schema}.pipeline_watermarks(pipeline_id)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_udfs_db_name ON {schema}.pipeline_udfs(db_name)",
            f"CREATE INDEX IF NOT EXISTS idx_pipeline_udf_versions_udf_id ON {schema}.pipeline_udf_versions(udf_id)",
            f"""
            ALTER TABLE {schema}.pipeline_versions
                ADD COLUMN IF NOT EXISTS branch TEXT NOT NULL DEFAULT 'main'
            """,
            f"""
            ALTER TABLE {schema}.pipeline_versions
                DROP CONSTRAINT IF EXISTS pipeline_versions_pipeline_id_version_key
            """,
            f"""
            DROP INDEX IF EXISTS {schema}.pipeline_versions_pipeline_id_branch_version_key
            """,
            f"""
            UPDATE {schema}.pipeline_versions v
            SET branch = p.branch
            FROM {schema}.pipelines p
            WHERE v.pipeline_id = p.pipeline_id AND (v.branch IS NULL OR v.branch = '')
            """,
        ),
    )
