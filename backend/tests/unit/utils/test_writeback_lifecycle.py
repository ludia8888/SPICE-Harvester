import pytest

from shared.utils.writeback_lifecycle import DEFAULT_LIFECYCLE_ID, derive_lifecycle_id, overlay_doc_id


def test_derive_lifecycle_id_prefers_top_level_value():
    assert derive_lifecycle_id({"lifecycle_id": "lc-123"}) == "lc-123"


def test_derive_lifecycle_id_reads_metadata_value():
    assert derive_lifecycle_id({"_metadata": {"lifecycle_id": "lc-9"}}) == "lc-9"


def test_derive_lifecycle_id_uses_last_create_command_id():
    state = {
        "_metadata": {
            "command_history": [
                {"command_type": "CREATE_INSTANCE", "command_id": "c1"},
                {"command_type": "UPDATE_INSTANCE", "command_id": "u1"},
                {"command_type": "CREATE_INSTANCE", "command_id": "c2"},
            ]
        }
    }
    assert derive_lifecycle_id(state) == "c2"


def test_derive_lifecycle_id_defaults_when_missing():
    assert derive_lifecycle_id({}) == DEFAULT_LIFECYCLE_ID


def test_overlay_doc_id_composes_instance_and_lifecycle():
    assert overlay_doc_id(instance_id="ticket:1", lifecycle_id="lc-0") == "ticket:1|lc-0"


def test_overlay_doc_id_rejects_delimiter_collision():
    with pytest.raises(ValueError):
        overlay_doc_id(instance_id="bad|id", lifecycle_id="lc-0")

