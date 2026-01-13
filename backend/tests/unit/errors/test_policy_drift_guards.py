from __future__ import annotations

import json
from pathlib import Path

from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.utils.canonical_json import sha256_canonical_json_prefixed


EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT = (
    "sha256:ab9df32ed6bcb4c49bef300f273f3bf9e57acdb7363c9a375c1f388f07f1c67c"
)
EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH = (
    "sha256:6534e4a3a3dceddd99b94b854496b1d274c94114e3a72da99a2b6b456a1d3384"
)


def test_enterprise_catalog_fingerprint_is_pinned() -> None:
    assert enterprise_catalog_fingerprint() == EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT


def test_agent_tool_allowlist_bundle_hash_is_pinned() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    bundle_path = repo_backend / "shared" / "policies" / "agent_tool_allowlist.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_sorted = sorted(bundle, key=lambda item: str(item.get("tool_id") or ""))
    assert sha256_canonical_json_prefixed(bundle_sorted) == EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH

