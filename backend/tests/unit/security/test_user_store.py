from __future__ import annotations

from types import SimpleNamespace

import pytest

import shared.security.user_store as user_store_module
from shared.security.user_store import UserStore


@pytest.mark.unit
def test_user_store_disables_default_admin_outside_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        user_store_module,
        "get_settings",
        lambda: SimpleNamespace(
            auth=SimpleNamespace(auth_users=None),
            is_development=False,
            is_test=False,
            is_pytest=False,
        ),
    )

    store = UserStore(auth_users_json="")

    assert store.authenticate("admin", "admin") is None


@pytest.mark.unit
def test_user_store_keeps_default_admin_in_dev(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        user_store_module,
        "get_settings",
        lambda: SimpleNamespace(
            auth=SimpleNamespace(auth_users=None),
            is_development=True,
            is_test=False,
            is_pytest=False,
        ),
    )

    store = UserStore(auth_users_json="")

    assert store.authenticate("admin", "admin") is not None
