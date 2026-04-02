from __future__ import annotations

import pytest

import objectify_worker.main as objectify_main
from objectify_worker.main import ObjectifyNonRetryableError, _extract_instance_relationships


def test_extract_instance_relationships_raises_non_retryable_error_on_extraction_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _boom(instance, *, rel_map, allow_pattern_fallback):  # noqa: ANN001, ARG001
        raise ValueError("bad relationship mapping")

    monkeypatch.setattr(objectify_main, "_extract_instance_relationships_raw", _boom)

    with pytest.raises(ObjectifyNonRetryableError, match="relationship extraction failed"):
        _extract_instance_relationships({"instance_id": "inst-1"}, rel_map={"a": {}})


