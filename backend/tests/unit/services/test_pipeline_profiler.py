import pytest

from shared.services.pipeline_profiler import compute_column_stats


@pytest.mark.unit
def test_compute_column_stats_string_column_counts_null_empty_whitespace_and_top_values():
    rows = [
        {"name": "a"},
        {"name": "a"},
        {"name": "b"},
        {"name": ""},
        {"name": "   "},
        {"name": None},
    ]
    columns = [{"name": "name", "type": "xsd:string"}]

    stats = compute_column_stats(rows=rows, columns=columns, max_top_values=10)
    assert stats["sample_row_count"] == 6
    col = stats["columns"]["name"]
    assert col["null_count"] == 1
    assert col["empty_count"] == 1
    assert col["whitespace_count"] == 1
    assert col["distinct_count"] == 3
    assert col["top_values"][0] == {"value": "a", "count": 2}


@pytest.mark.unit
def test_compute_column_stats_numeric_min_max_mean_from_mixed_values():
    rows = [
        {"n": 1},
        {"n": "2"},
        {"n": 3.0},
        {"n": None},
        {"n": ""},
    ]
    columns = [{"name": "n", "type": "xsd:decimal"}]

    stats = compute_column_stats(rows=rows, columns=columns, max_top_values=5)
    col = stats["columns"]["n"]
    assert col["null_count"] == 1
    assert col["empty_count"] == 1
    assert col["distinct_count"] == 3
    assert col["numeric"]["min"] == 1.0
    assert col["numeric"]["max"] == 3.0
    assert col["numeric"]["mean"] == 2.0
    histogram = col["numeric"]["histogram"]
    assert isinstance(histogram, list)
    assert sum(bucket["count"] for bucket in histogram) == 3
