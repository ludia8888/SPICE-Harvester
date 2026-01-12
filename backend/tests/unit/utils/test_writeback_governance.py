from shared.utils.writeback_governance import extract_backing_dataset_id, policies_aligned


def test_extract_backing_dataset_id_reads_object_type_spec():
    spec = {"backing_source": {"dataset_id": "ds-1"}}
    assert extract_backing_dataset_id(spec) == "ds-1"


def test_extract_backing_dataset_id_returns_none_for_missing_shape():
    assert extract_backing_dataset_id(None) is None
    assert extract_backing_dataset_id({}) is None
    assert extract_backing_dataset_id({"backing_source": {}}) is None


def test_policies_aligned_requires_dicts_and_exact_match():
    assert policies_aligned({"a": 1}, {"a": 1}) is True
    assert policies_aligned({"a": 1}, {"a": 2}) is False
    assert policies_aligned(None, {"a": 1}) is False
    assert policies_aligned({"a": 1}, None) is False
