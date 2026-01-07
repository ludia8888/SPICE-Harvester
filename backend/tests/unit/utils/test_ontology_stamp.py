from __future__ import annotations

from oms.utils.ontology_stamp import merge_ontology_stamp


def test_merge_ontology_stamp_prefers_existing() -> None:
    existing = {"ref": "main", "commit": "abc"}
    resolved = {"ref": "dev", "commit": "def"}

    merged = merge_ontology_stamp(existing, resolved)
    assert merged["ref"] == "main"
    assert merged["commit"] == "abc"


def test_merge_ontology_stamp_fills_missing() -> None:
    existing = {"ref": "main"}
    resolved = {"ref": "dev", "commit": "def"}

    merged = merge_ontology_stamp(existing, resolved)
    assert merged["ref"] == "main"
    assert merged["commit"] == "def"
