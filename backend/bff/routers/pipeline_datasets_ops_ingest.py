"""Pipeline dataset ingest helpers.

Small, stable helpers extracted from `bff.routers.pipeline_datasets_ops`.
"""


import hashlib
import json
from typing import Any, Dict, Optional
from urllib.parse import quote

from shared.services.registries.dataset_registry import DatasetRegistry
from shared.utils.path_utils import safe_path_segment
import logging


async def _ensure_ingest_transaction(
    dataset_registry: DatasetRegistry,
    *,
    ingest_request_id: str,
):
    transaction = await dataset_registry.get_ingest_transaction(
        ingest_request_id=ingest_request_id,
    )
    if transaction:
        return transaction
    return await dataset_registry.create_ingest_transaction(
        ingest_request_id=ingest_request_id,
    )


def _build_ingest_request_fingerprint(payload: Dict[str, Any]) -> str:
    try:
        serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    except Exception:
        logging.getLogger(__name__).warning("Broad exception fallback at bff/routers/pipeline_datasets_ops_ingest.py:35", exc_info=True)
        serialized = json.dumps(str(payload))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _ingest_staging_prefix(prefix: str, ingest_request_id: str) -> str:
    safe_request_id = safe_path_segment(ingest_request_id)
    return f"{prefix}/staging/{safe_request_id}"


def _sanitize_s3_metadata(metadata: Optional[Dict[str, Any]]) -> Dict[str, str]:
    if not metadata:
        return {}
    sanitized: Dict[str, str] = {}
    for key, value in metadata.items():
        if value is None:
            continue
        raw = str(value)
        try:
            raw.encode("ascii")
            sanitized[key] = raw
        except UnicodeEncodeError:
            sanitized[key] = quote(raw, safe="")
    return sanitized


def _dataset_artifact_prefix(*, db_name: str, dataset_id: str, dataset_name: str) -> str:
    safe_name = safe_path_segment(dataset_name)
    return f"datasets/{db_name}/{dataset_id}/{safe_name}"

