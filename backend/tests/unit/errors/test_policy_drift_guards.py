from __future__ import annotations

import json
from pathlib import Path

from shared.errors.enterprise_catalog import enterprise_catalog_fingerprint
from shared.utils.canonical_json import sha256_canonical_json_prefixed


EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT = (
    "sha256:447d7b671d2a1cc69bb96cddc8034725bfb2aa47e6712414039d2c7acc7baead"
)
EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH = (
    "sha256:d70f959b4c9f8a546d4a8a7b689861ca2dc3d1c853a19c26219028710350c3b1"
)


def test_enterprise_catalog_fingerprint_is_pinned() -> None:
    assert enterprise_catalog_fingerprint() == EXPECTED_ENTERPRISE_CATALOG_FINGERPRINT


def test_agent_tool_allowlist_bundle_hash_is_pinned() -> None:
    repo_backend = Path(__file__).resolve().parents[3]
    bundle_path = repo_backend / "shared" / "policies" / "agent_tool_allowlist.json"
    bundle = json.loads(bundle_path.read_text(encoding="utf-8"))
    bundle_sorted = sorted(bundle, key=lambda item: str(item.get("tool_id") or ""))
    assert sha256_canonical_json_prefixed(bundle_sorted) == EXPECTED_AGENT_TOOL_ALLOWLIST_BUNDLE_HASH
