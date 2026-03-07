from __future__ import annotations

from typing import Any, List

from shared.utils.string_list_utils import normalize_string_list


def normalize_join_key_list(value: Any) -> List[str]:
    return normalize_string_list(value, split_commas=False)
