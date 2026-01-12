from datetime import datetime, timezone

import pytest

from shared.services.storage_service import StorageService


class _FakeS3Client:
    def __init__(self, pages):
        self._pages = pages

    def list_objects_v2(self, **kwargs):
        token = kwargs.get("ContinuationToken")
        return self._pages.get(token, {"IsTruncated": False, "Contents": []})


@pytest.mark.unit
@pytest.mark.asyncio
async def test_list_command_files_paginates_filters_and_sorts():
    prefix = "demo/main/Ticket/ticket-1/"
    t1 = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)
    t2 = datetime(2026, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    t3 = datetime(2026, 1, 1, 0, 0, 2, tzinfo=timezone.utc)

    pages = {
        None: {
            "IsTruncated": True,
            "NextContinuationToken": "p2",
            "Contents": [
                {"Key": f"{prefix}b.json", "LastModified": t2},
                {"Key": f"{prefix}a.json", "LastModified": t1},
                {"Key": f"{prefix}note.txt", "LastModified": t3},
            ],
        },
        "p2": {
            "IsTruncated": False,
            "Contents": [
                {"Key": f"{prefix}c.json", "LastModified": t3},
            ],
        },
    }

    service = StorageService.__new__(StorageService)
    service.client = _FakeS3Client(pages)

    keys = await StorageService.list_command_files(service, bucket="bucket", prefix=prefix.rstrip("/"))
    assert keys == [f"{prefix}a.json", f"{prefix}b.json", f"{prefix}c.json"]

