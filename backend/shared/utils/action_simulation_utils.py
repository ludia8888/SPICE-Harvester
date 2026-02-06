from __future__ import annotations

from typing import Any


def reject_simulation_delete_flag(value: Any) -> bool:
    if bool(value):
        raise ValueError("delete is not supported for simulation assumptions")
    return False
