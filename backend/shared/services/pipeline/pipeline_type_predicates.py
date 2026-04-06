from __future__ import annotations

from typing import Any

from shared.services.pipeline.pipeline_value_predicates import (
    is_bool_like,
    is_datetime_like,
    is_decimal_like,
    is_int_like,
)


def inference_is_bool(value: Any) -> bool:
    return is_bool_like(value)


def inference_is_datetime(value: Any) -> bool:
    return is_datetime_like(value, allow_ambiguous=False)


def inference_is_int(value: Any) -> bool:
    return is_int_like(value)


def inference_is_decimal(value: Any) -> bool:
    return is_decimal_like(value, include_int=True)


def preview_is_bool(value: Any) -> bool:
    return is_bool_like(value)


def preview_is_int(value: Any) -> bool:
    return is_int_like(value)


def preview_is_decimal(value: Any) -> bool:
    return is_decimal_like(value, include_int=False)


def preview_is_datetime(value: Any) -> bool:
    return is_datetime_like(value, iso_only=True)
