"""Pipeline datasets helpers (BFF).

Composition root for dataset upload/ingest helper modules.

Keeps a stable import surface for dataset routers/tests while delegating
implementation details to smaller helpers (Composite + Facade).
"""

from __future__ import annotations

from bff.routers.pipeline_datasets_ops_funnel import (
    FUNNEL_RISK_POLICY,
    _build_funnel_analysis_payload,
    _build_schema_columns,
    _columns_from_schema,
    _compute_funnel_analysis_from_sample,
    _extract_sample_columns,
    _extract_sample_rows,
    _normalize_inferred_type,
    _rows_from_preview,
    _select_sample_row,
)
from bff.routers.pipeline_datasets_ops_ingest import (
    _build_ingest_request_fingerprint,
    _dataset_artifact_prefix,
    _ensure_ingest_transaction,
    _ingest_staging_prefix,
    _sanitize_s3_metadata,
)
from bff.routers.pipeline_datasets_ops_lakefs import (
    _acquire_lakefs_commit_lock,
    _commit_lakefs_with_predicate_fallback,
    _ensure_lakefs_branch_exists,
    _extract_lakefs_ref_from_artifact_key,
    _lakefs_commit_retry_delay,
    _release_lakefs_commit_lock,
    _resolve_lakefs_commit_from_head,
    _resolve_lakefs_raw_repository,
)
from bff.routers.pipeline_datasets_ops_objectify import _maybe_enqueue_objectify_job
from bff.routers.pipeline_datasets_ops_parsing import (
    _convert_xls_to_xlsx_bytes,
    _default_dataset_name,
    _detect_csv_delimiter,
    _normalize_table_bbox,
    _parse_csv_content,
    _parse_csv_file,
)
from shared.services.events.dataset_ingest_outbox import build_dataset_event_payload, flush_dataset_ingest_outbox
from shared.services.storage.event_store import event_store

__all__ = [
    "FUNNEL_RISK_POLICY",
    "_acquire_lakefs_commit_lock",
    "_build_funnel_analysis_payload",
    "_build_ingest_request_fingerprint",
    "_build_schema_columns",
    "_columns_from_schema",
    "_commit_lakefs_with_predicate_fallback",
    "_compute_funnel_analysis_from_sample",
    "_convert_xls_to_xlsx_bytes",
    "_dataset_artifact_prefix",
    "_default_dataset_name",
    "_detect_csv_delimiter",
    "_ensure_ingest_transaction",
    "_ensure_lakefs_branch_exists",
    "_extract_lakefs_ref_from_artifact_key",
    "_extract_sample_columns",
    "_extract_sample_rows",
    "_ingest_staging_prefix",
    "_lakefs_commit_retry_delay",
    "_maybe_enqueue_objectify_job",
    "_normalize_inferred_type",
    "_normalize_table_bbox",
    "_parse_csv_content",
    "_parse_csv_file",
    "_release_lakefs_commit_lock",
    "_resolve_lakefs_commit_from_head",
    "_resolve_lakefs_raw_repository",
    "_rows_from_preview",
    "_sanitize_s3_metadata",
    "_select_sample_row",
    "build_dataset_event_payload",
    "event_store",
    "flush_dataset_ingest_outbox",
]
