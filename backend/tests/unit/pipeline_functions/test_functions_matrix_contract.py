from __future__ import annotations

import pytest

from shared.tools.foundry_functions_compat import (
    default_snapshot_path,
    load_foundry_functions_snapshot,
)

_ALLOWED = {"supported", "partial", "unsupported"}


@pytest.mark.unit
def test_functions_snapshot_entries_have_valid_classification() -> None:
    entries = load_foundry_functions_snapshot(default_snapshot_path())
    assert entries, "functions snapshot must not be empty"

    for entry in entries:
        assert entry.preview in _ALLOWED, f"invalid preview status for {entry.name}"
        assert entry.spark in _ALLOWED, f"invalid spark status for {entry.name}"
        assert entry.category, f"missing category for {entry.name}"


@pytest.mark.unit
def test_functions_snapshot_has_no_unclassified_rows() -> None:
    entries = load_foundry_functions_snapshot(default_snapshot_path())
    unclassified = [
        entry.name
        for entry in entries
        if entry.preview not in _ALLOWED or entry.spark not in _ALLOWED
    ]
    assert not unclassified, f"unclassified functions: {unclassified}"
