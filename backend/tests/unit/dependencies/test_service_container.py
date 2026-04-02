from __future__ import annotations

import pytest

from shared.config.settings import ApplicationSettings
from shared.dependencies.container import ServiceContainer, ServiceToken


@pytest.mark.unit
@pytest.mark.asyncio
async def test_service_container_distinguishes_types_with_same_name() -> None:
    ServiceA = type("DuplicateName", (), {})
    ServiceB = type("DuplicateName", (), {})
    settings = ApplicationSettings()
    container = ServiceContainer(settings)

    container.register_instance(ServiceA, ServiceA())
    container.register_instance(ServiceB, ServiceB())

    assert isinstance(await container.get(ServiceA), ServiceA)
    assert isinstance(await container.get(ServiceB), ServiceB)


@pytest.mark.unit
def test_service_container_duplicate_registration_fails_fast() -> None:
    class ExampleService:
        pass

    settings = ApplicationSettings()
    container = ServiceContainer(settings)
    container.register_instance(ExampleService, ExampleService())

    with pytest.raises(ValueError, match="already registered"):
        container.register_instance(ExampleService, ExampleService())


@pytest.mark.unit
@pytest.mark.asyncio
async def test_service_container_token_registration_is_explicit() -> None:
    class ExampleService:
        def __init__(self, value: str) -> None:
            self.value = value

    settings = ApplicationSettings()
    container = ServiceContainer(settings)
    primary_token = ServiceToken[ExampleService]("primary-example", ExampleService)
    secondary_token = ServiceToken[ExampleService]("secondary-example", ExampleService)

    container.register_singleton_token(primary_token, lambda settings: ExampleService("one"))
    container.register_singleton_token(secondary_token, lambda settings: ExampleService("two"))

    assert (await container.get_token(primary_token)).value == "one"
    assert (await container.get_token(secondary_token)).value == "two"
