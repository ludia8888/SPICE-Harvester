from __future__ import annotations

import pytest

from bff.services.funnel_client import FunnelClient


@pytest.mark.unit
def test_estimate_total_rows_normalizes_string_bbox_values() -> None:
    table = {
        "mode": "table",
        "bbox": {"top": "1", "left": "1", "bottom": "10", "right": "4"},
        "header_rows": "1",
    }

    assert FunnelClient._estimate_total_rows(table) == 9


@pytest.mark.unit
def test_estimate_total_rows_returns_zero_for_invalid_bbox() -> None:
    table = {
        "mode": "table",
        "bbox": {"top": "x", "left": "1", "bottom": "10", "right": "4"},
        "header_rows": 1,
    }

    assert FunnelClient._estimate_total_rows(table) == 0
