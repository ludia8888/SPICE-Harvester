import pytest

from instance_worker.main import StrictPalantirInstanceWorker


def test_primary_key_required_when_generation_disabled():
    worker = StrictPalantirInstanceWorker()

    with pytest.raises(ValueError):
        worker.get_primary_key_value("Product", {"name": "Widget"}, allow_generate=False)


@pytest.mark.asyncio
async def test_relationship_fallback_can_be_disabled():
    worker = StrictPalantirInstanceWorker()
    worker.terminus_service = None
    payload = {"linked_to": "Target/abc123"}

    relationships = await worker.extract_relationships(
        "db",
        "Thing",
        payload,
        allow_pattern_fallback=False,
    )
    assert relationships == {}

    relationships = await worker.extract_relationships(
        "db",
        "Thing",
        payload,
        allow_pattern_fallback=True,
    )
    assert relationships == {"linked_to": "Target/abc123"}
