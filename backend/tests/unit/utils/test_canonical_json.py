from datetime import datetime, timezone

from shared.utils.canonical_json import canonical_json_dumps, sha256_canonical_json_prefixed


def test_canonical_json_dumps_sorts_keys_and_is_compact():
    assert canonical_json_dumps({"b": 1, "a": 2}) == '{"a":2,"b":1}'


def test_canonical_json_dumps_normalizes_datetime_to_utc():
    value = {"ts": datetime(2026, 1, 1, 0, 0, 0)}  # naive
    dumped = canonical_json_dumps(value)
    assert dumped == '{"ts":"2026-01-01T00:00:00+00:00"}'


def test_sha256_prefixed_has_expected_prefix():
    digest = sha256_canonical_json_prefixed({"a": 1})
    assert digest.startswith("sha256:")
    assert len(digest) == len("sha256:") + 64

