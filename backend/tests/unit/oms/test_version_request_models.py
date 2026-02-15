"""Unit tests for OMS version router request models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from oms.routers.version import RollbackRequest


def test_rollback_request_accepts_target_commit() -> None:
    model = RollbackRequest.model_validate({"target_commit": "HEAD~1"})
    assert model.target_commit == "HEAD~1"


def test_rollback_request_rejects_legacy_target_alias() -> None:
    with pytest.raises(ValidationError):
        RollbackRequest.model_validate({"target": "HEAD~1"})
