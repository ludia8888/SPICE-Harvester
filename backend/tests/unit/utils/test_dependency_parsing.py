from pathlib import Path

from scripts.dependency_parsing import parse_pyproject_toml, parse_requirements_txt


def test_parse_requirements_txt_extracts_versions(tmp_path: Path) -> None:
    requirements = tmp_path / "requirements.txt"
    requirements.write_text(
        "\n".join(
            [
                "# comment",
                "-e ../shared",
                "fastapi==0.115.0",
                "pydantic>=2.0.0",
                "urllib3<=2.2.3",
                "redis[hiredis]",
            ]
        )
    )
    parsed = parse_requirements_txt(requirements)
    assert parsed["fastapi"] == "0.115.0"
    assert parsed["pydantic"] == ">=2.0.0"
    assert parsed["urllib3"] == "<=2.2.3"
    assert parsed["redis"] is None
    assert "-e ../shared" not in parsed


def test_parse_pyproject_toml_extracts_dependencies(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    pyproject.write_text(
        "\n".join(
            [
                "[project]",
                'dependencies = ["fastapi==0.115.0", "pydantic>=2.0.0", "redis[hiredis]", "-e ../shared"]',
            ]
        )
    )
    parsed = parse_pyproject_toml(pyproject)
    assert parsed["fastapi"] == "0.115.0"
    assert parsed["pydantic"] == ">=2.0.0"
    assert parsed["redis"] is None
    assert "-e ../shared" not in parsed

