import pytest

from shared.services.core.object_type_meta_resolver import build_object_type_meta_resolver


class _FakeResources:
    def __init__(self, response=None, *, should_raise: bool = False) -> None:
        self.response = response
        self.should_raise = should_raise
        self.calls: list[tuple[str, str, str, str]] = []

    async def get_resource(
        self,
        db_name: str,
        *,
        branch: str,
        resource_type: str,
        resource_id: str,
    ):
        self.calls.append((db_name, branch, resource_type, resource_id))
        if self.should_raise:
            raise RuntimeError("boom")
        return self.response


@pytest.mark.asyncio
async def test_object_type_meta_resolver_parses_and_caches() -> None:
    resources = _FakeResources(response={"spec": {"conflict_policy": "base_wins"}, "metadata": {"rev": "3"}})
    resolver = build_object_type_meta_resolver(resources=resources, db_name="demo", branch="commit-1")

    first = await resolver("Ticket")
    second = await resolver("Ticket")

    assert first == {"conflict_policy": "BASE_WINS", "rev": 3}
    assert second == first
    assert resources.calls == [("demo", "commit-1", "object_type", "Ticket")]


@pytest.mark.asyncio
async def test_object_type_meta_resolver_blank_id_uses_default() -> None:
    resources = _FakeResources(response={"spec": {"conflict_policy": "FAIL"}, "metadata": {"rev": 9}})
    resolver = build_object_type_meta_resolver(resources=resources, db_name="demo", branch="commit-2")

    meta = await resolver("   ")

    assert meta == {"conflict_policy": None, "rev": 1}
    assert resources.calls == []


@pytest.mark.asyncio
async def test_object_type_meta_resolver_on_error_returns_default_and_caches() -> None:
    resources = _FakeResources(should_raise=True)
    resolver = build_object_type_meta_resolver(resources=resources, db_name="demo", branch="commit-3")

    first = await resolver("Order")
    second = await resolver("Order")

    assert first == {"conflict_policy": None, "rev": 1}
    assert second == first
    assert resources.calls == [("demo", "commit-3", "object_type", "Order")]
