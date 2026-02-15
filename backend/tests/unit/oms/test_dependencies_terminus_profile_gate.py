from __future__ import annotations

from oms.dependencies import OMSDependencyProvider


def test_legacy_terminus_dependency_accessors_are_removed() -> None:
    assert not hasattr(OMSDependencyProvider, "get_terminus_service")
    assert not hasattr(OMSDependencyProvider, "get_optional_terminus_service")
