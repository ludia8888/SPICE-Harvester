"""
TerminusDB branch name encoding helpers.

Why:
- TerminusDB branch resource descriptors cannot contain slashes.
- Our API allows Git-like branch names (e.g. "feature/foo").
- We therefore encode/decode branch names at the TerminusDB boundary only.

Encoding format:
- If the branch contains "/", encode with urlsafe base64 and prefix with `spiceb64_`.
- Otherwise return the original branch name.
"""

from __future__ import annotations

import base64

_BRANCH_B64_PREFIX = "spiceb64_"


def encode_branch_name(branch_name: str) -> str:
    if not isinstance(branch_name, str):
        raise TypeError("branch_name must be a string")
    if "/" not in branch_name:
        return branch_name
    token = base64.urlsafe_b64encode(branch_name.encode("utf-8")).decode("ascii").rstrip("=")
    return f"{_BRANCH_B64_PREFIX}{token}"


def decode_branch_name(branch_name: str) -> str:
    if not isinstance(branch_name, str):
        return branch_name
    if not branch_name.startswith(_BRANCH_B64_PREFIX):
        return branch_name
    token = branch_name[len(_BRANCH_B64_PREFIX) :]
    padded = token + ("=" * (-len(token) % 4))
    try:
        return base64.urlsafe_b64decode(padded.encode("ascii")).decode("utf-8")
    except Exception:
        return branch_name

