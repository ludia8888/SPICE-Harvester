from shared.utils.resource_rid import format_resource_rid, parse_metadata_rev, strip_rid_revision


def test_parse_metadata_rev_defaults_to_one():
    assert parse_metadata_rev(None) == 1
    assert parse_metadata_rev({}) == 1
    assert parse_metadata_rev({"rev": "x"}) == 1


def test_parse_metadata_rev_parses_int():
    assert parse_metadata_rev({"rev": 3}) == 3
    assert parse_metadata_rev({"rev": "4"}) == 4
    assert parse_metadata_rev({"rev": 0}) == 1


def test_format_resource_rid_always_includes_revision():
    assert format_resource_rid(resource_type="object_type", resource_id="Ticket", rev=None) == "object_type:Ticket@1"
    assert format_resource_rid(resource_type="object_type", resource_id="Ticket", rev=5) == "object_type:Ticket@5"


def test_strip_rid_revision_handles_prefixed_and_unprefixed():
    assert strip_rid_revision("object_type:Ticket@5") == "Ticket"
    assert strip_rid_revision("Ticket@5") == "Ticket"
    assert strip_rid_revision("Ticket") == "Ticket"

