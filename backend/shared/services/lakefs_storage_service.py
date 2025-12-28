"""
StorageService wrapper configured for lakeFS S3 Gateway.

Important difference vs raw S3/MinIO:
- lakeFS repositories are not created via S3 CreateBucket; they are created via the lakeFS REST API.
  To avoid accidental bucket creation attempts, this service no-ops create_bucket/bucket_exists.
"""

from __future__ import annotations

import os
from typing import Optional

from shared.config.service_config import ServiceConfig
from shared.config.settings import ApplicationSettings
from shared.services.storage_service import HAS_BOTO3, StorageService


class LakeFSStorageService(StorageService):
    async def create_bucket(self, bucket_name: str) -> bool:  # pragma: no cover
        # lakeFS repositories must exist already (created via REST / infra).
        return True

    async def bucket_exists(self, bucket_name: str) -> bool:  # pragma: no cover
        # Best-effort; repository existence is enforced by the gateway at request time.
        return True


def create_lakefs_storage_service(settings: ApplicationSettings) -> Optional[LakeFSStorageService]:
    if not HAS_BOTO3:
        return None

    endpoint = (
        (settings.storage.lakefs_s3_endpoint_url or os.getenv("LAKEFS_S3_ENDPOINT_URL") or "").strip()
        or ServiceConfig.get_lakefs_s3_endpoint()
    )
    access = (settings.storage.lakefs_access_key_id or os.getenv("LAKEFS_ACCESS_KEY_ID") or "").strip()
    secret = (settings.storage.lakefs_secret_access_key or os.getenv("LAKEFS_SECRET_ACCESS_KEY") or "").strip()
    if not access or not secret:
        raise RuntimeError("LakeFSStorageService requires LAKEFS_ACCESS_KEY_ID and LAKEFS_SECRET_ACCESS_KEY")

    return LakeFSStorageService(
        endpoint_url=endpoint,
        access_key=access,
        secret_key=secret,
        use_ssl=endpoint.startswith("https"),
    )

