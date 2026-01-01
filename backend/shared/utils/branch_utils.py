from __future__ import annotations

import os
from typing import Iterable, Set


PROTECTED_BRANCH_WRITE_MESSAGE = (
    "Protected branch. Use proposal workflow for changes."
)


def protected_branch_write_message() -> str:
    return PROTECTED_BRANCH_WRITE_MESSAGE


def get_protected_branches(
    *,
    env_key: str = "ONTOLOGY_PROTECTED_BRANCHES",
    defaults: Iterable[str] = ("main", "master", "production", "prod"),
) -> Set[str]:
    raw = (os.getenv(env_key) or "").strip()
    if not raw:
        return set(defaults)
    branches = {b.strip() for b in raw.split(",") if b.strip()}
    return branches or set(defaults)
