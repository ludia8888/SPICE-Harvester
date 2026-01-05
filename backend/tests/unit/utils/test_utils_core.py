import os

from shared.utils import branch_utils, env_utils, path_utils, s3_uri


def test_parse_bool_env_and_int_env(monkeypatch):
    monkeypatch.setenv("FLAG_TRUE", "yes")
    monkeypatch.setenv("FLAG_FALSE", "0")
    monkeypatch.setenv("FLAG_BAD", "maybe")
    monkeypatch.setenv("COUNT", "42")
    monkeypatch.setenv("COUNT_BAD", "nope")

    assert env_utils.parse_bool_env("FLAG_TRUE") is True
    assert env_utils.parse_bool_env("FLAG_FALSE") is False
    assert env_utils.parse_bool_env("FLAG_BAD", default=True) is True
    assert env_utils.parse_int_env("COUNT", 0, min_value=1, max_value=100) == 42
    assert env_utils.parse_int_env("COUNT_BAD", 7) == 7


def test_safe_path_helpers():
    assert path_utils.safe_lakefs_ref("") == "main"
    assert path_utils.safe_lakefs_ref("feature/branch") == "feature-branch"
    assert path_utils.safe_path_segment("Hello world!") == "Hello_world"
    assert path_utils.safe_path_segment("$$$") == "untitled"


def test_s3_uri_helpers():
    assert s3_uri.is_s3_uri("s3://bucket/key")
    assert not s3_uri.is_s3_uri("http://bucket/key")
    assert s3_uri.build_s3_uri("bucket", "/key") == "s3://bucket/key"
    assert s3_uri.parse_s3_uri("s3://bucket/path/to") == ("bucket", "path/to")
    assert s3_uri.normalize_s3_uri("path/to", bucket="bucket") == "s3://bucket/path/to"


def test_branch_utils_defaults(monkeypatch):
    monkeypatch.delenv("ONTOLOGY_PROTECTED_BRANCHES", raising=False)
    defaults = branch_utils.get_protected_branches()
    assert "main" in defaults

    monkeypatch.setenv("ONTOLOGY_PROTECTED_BRANCHES", "dev, staging")
    assert branch_utils.get_protected_branches() == {"dev", "staging"}
