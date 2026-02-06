from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional, Tuple

try:
    import tomllib  # Python 3.11+
except ImportError:  # pragma: no cover
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ImportError:
        import toml as tomllib  # type: ignore[no-redef]


def _normalize_package_name(raw_name: str) -> str:
    name = str(raw_name or "").strip()
    if "[" in name:
        name = name.split("[", 1)[0]
    return name.strip()


def _split_dependency_spec(spec: str) -> Tuple[str, Optional[str]]:
    raw = str(spec or "").strip()
    if not raw or raw.startswith("-e "):
        return "", None
    for operator in ("==", ">=", "<="):
        if operator in raw:
            name, version = raw.split(operator, 1)
            normalized_name = _normalize_package_name(name)
            if not normalized_name:
                return "", None
            cleaned_version = version.strip()
            if operator == "==":
                return normalized_name, cleaned_version or None
            return normalized_name, f"{operator}{cleaned_version}" if cleaned_version else operator
    normalized_name = _normalize_package_name(raw)
    return normalized_name, None


def parse_requirements_txt(file_path: Path) -> Dict[str, Optional[str]]:
    deps: Dict[str, Optional[str]] = {}
    if not file_path.exists():
        return deps
    with open(file_path, "r") as file:
        for line in file:
            raw = line.strip()
            if not raw or raw.startswith("#"):
                continue
            name, version = _split_dependency_spec(raw)
            if name:
                deps[name] = version
    return deps


def parse_pyproject_toml(file_path: Path) -> Dict[str, Optional[str]]:
    deps: Dict[str, Optional[str]] = {}
    if not file_path.exists():
        return deps
    try:
        with open(file_path, "rb") as file:
            data = tomllib.load(file)
    except Exception:
        return deps
    project_deps = data.get("project", {}).get("dependencies", [])
    for dep in project_deps:
        name, version = _split_dependency_spec(str(dep))
        if name:
            deps[name] = version
    return deps

