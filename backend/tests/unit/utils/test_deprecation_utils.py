from __future__ import annotations

import pytest

from oms.utils import deprecation


def test_deprecated_decorator_sync() -> None:
    @deprecation.deprecated(
        reason="Use new_fn",
        version="1.0",
        alternative="new_fn",
        removal_version="2.0",
    )
    def old_fn() -> str:
        return "ok"

    with pytest.warns(DeprecationWarning):
        assert old_fn() == "ok"
    assert getattr(old_fn, "__deprecated__", False) is True
    assert "deprecated" in (old_fn.__doc__ or "")


def test_legacy_and_experimental_decorators() -> None:
    @deprecation.legacy_api("legacy reason")
    def legacy_fn() -> str:
        return "legacy"

    assert getattr(legacy_fn, "__legacy__", False) is True
    assert "Legacy API" in (legacy_fn.__doc__ or "")

    @deprecation.experimental("feature-x")
    async def exp_fn() -> str:
        return "exp"

    assert getattr(exp_fn, "__experimental__", False) is True
