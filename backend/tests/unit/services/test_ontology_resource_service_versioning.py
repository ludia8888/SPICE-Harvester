from __future__ import annotations

import pytest

from oms.exceptions import DatabaseError
from oms.services.ontology_resources import OntologyResourceService


class _AcquireCtx:
    def __init__(self, conn):
        self._conn = conn

    async def __aenter__(self):
        return self._conn

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _Pool:
    def __init__(self, conn):
        self._conn = conn

    def acquire(self):
        return _AcquireCtx(self._conn)


class _Conn:
    def __init__(self, *, existing_relations: set[str] | None = None) -> None:
        self._existing_relations = set(existing_relations or set())
        self.executed: list[str] = []

    async def fetchval(self, sql: str, *args):
        if sql == "SELECT to_regclass($1) IS NOT NULL":
            return args[0] in self._existing_relations
        return None

    async def execute(self, sql: str, *_args):
        self.executed.append(sql)
        if "CREATE TABLE IF NOT EXISTS ontology_resources" in sql:
            self._existing_relations.add("ontology_resources")
        if "CREATE TABLE IF NOT EXISTS ontology_resource_versions" in sql:
            self._existing_relations.add("ontology_resource_versions")
        return None

    def transaction(self):
        return _AcquireCtx(self)

    async def fetchrow(self, sql: str, *_args):
        self.executed.append(sql)
        return None


@pytest.mark.unit
def test_payload_to_document_sets_initial_version_and_rev() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    doc = service._payload_to_document(  # noqa: SLF001
        "object_type",
        "Ticket",
        {"label": "Ticket", "spec": {"properties": []}},
        doc_id="__ontology_resource/object_type:Ticket",
        is_create=True,
    )

    assert doc["version"] == 1
    assert doc["metadata"]["rev"] == 1


@pytest.mark.unit
def test_payload_to_document_increments_version_and_rev_from_existing() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    existing = {
        "resource_type": "object_type",
        "resource_id": "Ticket",
        "label": "Ticket",
        "metadata": {"rev": 3, "owner": "ops"},
        "version": 3,
        "created_at": "2026-01-01T00:00:00+00:00",
    }

    doc = service._payload_to_document(  # noqa: SLF001
        "object_type",
        "Ticket",
        {"metadata": {"owner": "platform"}},
        doc_id="__ontology_resource/object_type:Ticket",
        is_create=False,
        existing=existing,
    )

    assert doc["version"] == 4
    assert doc["metadata"]["rev"] == 4
    assert doc["metadata"]["owner"] == "platform"


@pytest.mark.unit
def test_normalize_branch_for_write_strips_branch_prefix() -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")

    assert service._normalize_branch_for_write("branch:main") == "main"  # noqa: SLF001
    assert service._normalize_branch_for_write("main") == "main"  # noqa: SLF001
    assert service._normalize_branch_for_write("   ") == "main"  # noqa: SLF001


@pytest.mark.unit
@pytest.mark.asyncio
async def test_get_resource_falls_back_to_deployed_target_branch(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    looked_up: list[str] = []

    async def _fake_ensure() -> None:
        return None

    async def _fake_resolve_target(*, db_name: str, ontology_commit_id: str) -> str | None:
        _ = db_name
        if ontology_commit_id == "commit-1":
            return "main"
        return None

    async def _fake_get(
        *,
        db_name: str,  # noqa: ARG001
        branch: str,
        resource_type: str,  # noqa: ARG001
        resource_id: str,  # noqa: ARG001
    ) -> dict | None:
        looked_up.append(branch)
        if branch != "main":
            return None
        return {
            "db_name": "demo",
            "branch": "main",
            "resource_type": "action_type",
            "resource_id": "ApproveTicket",
            "label": "Approve Ticket",
            "description": None,
            "label_i18n": None,
            "description_i18n": None,
            "spec": {},
            "metadata": {},
            "version": 1,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }

    monkeypatch.setattr(service, "_ensure_postgres_schema", _fake_ensure)
    monkeypatch.setattr(service, "_resolve_deployed_target_branch", _fake_resolve_target)
    monkeypatch.setattr(service, "_get_resource_document_postgres", _fake_get)

    resolved = await service.get_resource(
        "demo",
        branch="commit-1",
        resource_type="action_type",
        resource_id="ApproveTicket",
    )

    assert resolved is not None
    assert resolved.get("id") == "ApproveTicket"
    assert looked_up == ["commit-1", "main"]


@pytest.mark.unit
@pytest.mark.asyncio
async def test_materialize_commit_snapshot_delegates_to_promote(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    captured: dict[str, str] = {}

    async def _fake_promote(
        db_name: str,
        *,
        source_branch: str,
        target_branch: str,
    ) -> int:
        captured["db_name"] = db_name
        captured["source_branch"] = source_branch
        captured["target_branch"] = target_branch
        return 3

    monkeypatch.setattr(service, "promote_branch_resources", _fake_promote)
    copied = await service.materialize_commit_snapshot(
        "demo",
        source_branch="main",
        ontology_commit_id="commit-xyz",
    )

    assert copied == 3
    assert captured == {
        "db_name": "demo",
        "source_branch": "main",
        "target_branch": "commit-xyz",
    }


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_postgres_schema_raises_when_bootstrap_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    conn = _Conn()

    async def _fake_pool():
        return _Pool(conn)

    monkeypatch.setattr(service, "_ensure_pool", _fake_pool)
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "false")

    with pytest.raises(DatabaseError, match="missing required schema objects"):
        await service._ensure_postgres_schema()  # noqa: SLF001


@pytest.mark.unit
@pytest.mark.asyncio
async def test_ensure_postgres_schema_bootstraps_when_enabled(monkeypatch: pytest.MonkeyPatch) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    conn = _Conn()

    async def _fake_pool():
        return _Pool(conn)

    monkeypatch.setattr(service, "_ensure_pool", _fake_pool)
    monkeypatch.setenv("ALLOW_RUNTIME_DDL_BOOTSTRAP", "true")

    await service._ensure_postgres_schema()  # noqa: SLF001

    assert any("CREATE TABLE IF NOT EXISTS ontology_resources" in sql for sql in conn.executed)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_update_resource_passes_expected_version_to_postgres_update(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    captured: dict[str, int] = {}

    async def _fake_ensure() -> None:
        return None

    async def _fake_get(**kwargs):  # noqa: ANN003, ANN201
        _ = kwargs
        return {
            "resource_type": "object_type",
            "resource_id": "Ticket",
            "label": "Ticket",
            "description": None,
            "label_i18n": None,
            "description_i18n": None,
            "spec": {"id": "Ticket"},
            "metadata": {"rev": 7},
            "version": 7,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-01T00:00:00+00:00",
        }

    async def _fake_update(*, expected_version: int, **kwargs):  # noqa: ANN003, ANN201
        captured["expected_version"] = expected_version
        _ = kwargs
        return {
            "resource_type": "object_type",
            "resource_id": "Ticket",
            "label": "Ticket",
            "description": "updated",
            "label_i18n": None,
            "description_i18n": None,
            "spec": {"id": "Ticket"},
            "metadata": {"rev": 8},
            "version": 8,
            "created_at": "2026-01-01T00:00:00+00:00",
            "updated_at": "2026-01-02T00:00:00+00:00",
        }

    monkeypatch.setattr(service, "_ensure_postgres_schema", _fake_ensure)
    monkeypatch.setattr(service, "_get_resource_document_postgres", _fake_get)
    monkeypatch.setattr(service, "_upsert_resource_document_postgres", _fake_update)

    payload = await service.update_resource(
        "demo",
        branch="main",
        resource_type="object_type",
        resource_id="Ticket",
        payload={"description": "updated"},
    )

    assert captured["expected_version"] == 7
    assert payload["version"] == 8


@pytest.mark.unit
@pytest.mark.asyncio
async def test_upsert_resource_document_raises_conflict_on_version_mismatch(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    service = OntologyResourceService(postgres_url="postgresql://localhost/test")
    conn = _Conn(existing_relations={"ontology_resources", "ontology_resource_versions"})

    async def _fake_fetchval(sql: str, *args):  # noqa: ANN001, ANN201
        conn.executed.append(sql)
        if "SELECT 1" in sql:
            return 1
        return await _Conn.fetchval(conn, sql, *args)

    conn.fetchval = _fake_fetchval  # type: ignore[method-assign]

    async def _fake_pool():
        return _Pool(conn)

    monkeypatch.setattr(service, "_ensure_pool", _fake_pool)

    with pytest.raises(DatabaseError, match="modified concurrently"):
        await service._upsert_resource_document_postgres(  # noqa: SLF001
            db_name="demo",
            branch="main",
            doc={
                "resource_type": "object_type",
                "resource_id": "Ticket",
                "label": "Ticket",
                "description": None,
                "spec": {"id": "Ticket"},
                "metadata": {"rev": 2},
                "version": 2,
                "created_at": "2026-01-01T00:00:00+00:00",
                "updated_at": "2026-01-02T00:00:00+00:00",
            },
            expected_version=1,
        )

    assert any("AND version = $14::bigint" in sql for sql in conn.executed)
