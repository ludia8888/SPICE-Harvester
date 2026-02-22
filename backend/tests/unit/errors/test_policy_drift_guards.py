from __future__ import annotations

import json
from pathlib import Path

from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.utils.canonical_json import sha256_canonical_json_prefixed


EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT = (
    "sha256:b5c7e092647078a95d03679f8eeb14bbbcc57f9b39c8ea88ea0ab452238ba2fd"
)
EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH = (
    "sha256:e11c5b1408a99ed7bb94284ae49f5fa18c1973afa0281edd8be73ac7c23fbdd2"
)


def test_enterprise_catalog_fingerprint_is_pinned() -> None:
    assert enterprise_catalog_fingerprint() == EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT


def test_agent_tool_allowlist_bundle_hash_is_pinned() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    bundle_path = repo_backend / "shared" / "policies" / "agent_tool_allowlist.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_sorted = sorted(bundle, key=lambda item: str(item.get("tool_id") or ""))
    assert sha256_canonical_json_prefixed(bundle_sorted) == EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH
