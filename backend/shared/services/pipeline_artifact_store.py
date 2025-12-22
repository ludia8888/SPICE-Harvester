"""
Pipeline artifact store for S3/MinIO.

Stores pipeline output snapshots as JSON in object storage and returns the S3 key.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Dict, List

from shared.services.storage_service import StorageService
from shared.utils.s3_uri import build_s3_uri


class PipelineArtifactStore:
    def __init__(self, storage_service: StorageService, bucket: str) -> None:
        self._storage = storage_service
        self._bucket = bucket

    async def save_table(
        self,
        *,
        dataset_name: str,
        columns: List[str],
        rows: List[Dict[str, Any]],
        db_name: str,
        pipeline_id: str,
    ) -> str:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        safe_name = dataset_name.replace(" ", "_")
        key = f"pipelines/{db_name}/{pipeline_id}/{safe_name}/{timestamp}.json"
        payload = {
            "columns": [{"name": name, "type": "String"} for name in columns],
            "rows": rows,
        }
        await self._storage.create_bucket(self._bucket)
        await self._storage.save_json(self._bucket, key, payload)
        return build_s3_uri(self._bucket, key)
