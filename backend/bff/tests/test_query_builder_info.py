from __future__ import annotations

import pytest

from bff.routers.query import query_builder_info


@pytest.mark.asyncio
async def test_query_builder_info_exposes_canonical_text_operators_only() -> None:
    payload = await query_builder_info()

    operators = payload.get("operators") or []
    assert "startsWith" not in operators
    assert "containsAnyTerm" in operators
    assert "containsAllTerms" in operators
    assert "containsAllTermsInOrder" in operators
    assert "containsAllTermsInOrderPrefixLastTerm" in operators

    assert payload.get("aliases") == {
        "startsWith": "containsAllTermsInOrderPrefixLastTerm",
    }
