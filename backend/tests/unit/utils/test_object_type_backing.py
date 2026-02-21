from shared.utils.object_type_backing import list_backing_sources, select_primary_backing_source


def test_list_backing_sources_prefers_backing_sources_list():
    spec = {
        "backing_source": {"dataset_id": "legacy-ds"},
        "backing_sources": [{"dataset_id": "ds-1"}, {"dataset_id": "ds-2"}],
    }
    sources = list_backing_sources(spec)
    assert [source["dataset_id"] for source in sources] == ["ds-1", "ds-2"]


def test_list_backing_sources_falls_back_to_backing_source():
    spec = {"backing_source": {"dataset_id": "legacy-ds"}}
    sources = list_backing_sources(spec)
    assert len(sources) == 1
    assert sources[0]["dataset_id"] == "legacy-ds"


def test_select_primary_backing_source_returns_first_source():
    spec = {"backing_sources": [{"dataset_id": "ds-1"}, {"dataset_id": "ds-2"}]}
    selected = select_primary_backing_source(spec)
    assert selected["dataset_id"] == "ds-1"


def test_select_primary_backing_source_returns_empty_dict_for_missing_shape():
    assert select_primary_backing_source(None) == {}
    assert select_primary_backing_source({}) == {}

