from shared.services.pipeline.pipeline_definition_utils import normalize_expectation_columns


def test_normalize_expectation_columns_with_csv() -> None:
    assert normalize_expectation_columns(" id , email ,, name ") == ["id", "email", "name"]


def test_normalize_expectation_columns_with_list() -> None:
    assert normalize_expectation_columns([" id ", "", "email", None]) == ["id", "email", "None"]


def test_normalize_expectation_columns_with_none() -> None:
    assert normalize_expectation_columns(None) == []
