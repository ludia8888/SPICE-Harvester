from types import SimpleNamespace

import pytest
from starlette.requests import Request

from bff.routers import link_types as link_types_router


class _FakeDatasetRegistry:
    def __init__(self) -> None:
        self.created = []
        self.added_versions = []
        self._datasets = {}
        self._versions = {}

    async def get_dataset(self, *, dataset_id: str):
        return self._datasets.get(dataset_id)

    async def get_dataset_by_name(self, *, db_name: str, name: str, branch: str):
        return None

    async def create_dataset(
        self,
        *,
        db_name: str,
        name: str,
        description: str | None,
        source_type: str,
        source_ref: str | None,
        schema_json: dict,
        branch: str,
    ):
        dataset = SimpleNamespace(
            dataset_id="join-ds-1",
            db_name=db_name,
            name=name,
            description=description,
            source_type=source_type,
            source_ref=source_ref,
            schema_json=schema_json,
            branch=branch,
        )
        self._datasets[dataset.dataset_id] = dataset
        self.created.append(dataset)
        return dataset

    async def get_version(self, *, version_id: str):
        for version in self._versions.values():
            if version.version_id == version_id:
                return version
        return None

    async def get_latest_version(self, *, dataset_id: str):
        return self._versions.get(dataset_id)

    async def add_version(
        self,
        *,
        dataset_id: str,
        lakefs_commit_id: str,
        artifact_key: str | None,
        row_count: int,
        sample_json: dict,
        schema_json: dict,
    ):
        version = SimpleNamespace(
            version_id="join-ver-1",
            dataset_id=dataset_id,
            sample_json=sample_json,
            schema_json=schema_json,
            artifact_key=artifact_key,
        )
        self._versions[dataset_id] = version
        self.added_versions.append(version)
        return version


@pytest.mark.asyncio
async def test_ensure_join_dataset_auto_creates_dataset_and_version() -> None:
    registry = _FakeDatasetRegistry()
    request = Request({"type": "http", "headers": []})

    dataset, version, schema_hash = await link_types_router._ensure_join_dataset(
        dataset_registry=registry,
        request=request,
        db_name="test_db",
        join_dataset_id=None,
        join_dataset_version_id=None,
        join_dataset_name=None,
        join_dataset_branch="staging",
        auto_create=True,
        default_name="Account_owned_by_User_join",
        source_key_column="account_id",
        target_key_column="user_id",
        source_key_type="xsd:string",
        target_key_type="xsd:integer",
    )

    assert dataset.name == "Account_owned_by_User_join"
    assert dataset.branch == "staging"
    assert registry.created
    assert registry.added_versions

    columns = version.sample_json["columns"]
    assert columns[0]["name"] == "account_id"
    assert columns[0]["type"] == "xsd:string"
    assert columns[1]["name"] == "user_id"
    assert columns[1]["type"] == "xsd:integer"
    assert schema_hash == link_types_router._compute_schema_hash(version.sample_json)
