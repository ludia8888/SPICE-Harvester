from __future__ import annotations

import json
from pathlib import Path

from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.utils.canonical_json import sha256_canonical_json_prefixed


EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT = (
    "sha256:dea077b81d01428cf9b84eb2330157b355f6f9db0fe797e5e151f8124acb7c74"
)
EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH = (
    "sha256:d75092a37d10083978306373887f3ba77ca52a1a8a9bc0d4ef4a103c7f02ea1a"
)


def test_enterprise_catalog_fingerprint_is_pinned() -> None:
    assert enterprise_catalog_fingerprint() == EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT


def test_agent_tool_allowlist_bundle_hash_is_pinned() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    bundle_path = repo_backend / "shared" / "policies" / "agent_tool_allowlist.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_sorted = sorted(bundle, key=lambda item: str(item.get("tool_id") or ""))
    assert sha256_canonical_json_prefixed(bundle_sorted) == EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH
