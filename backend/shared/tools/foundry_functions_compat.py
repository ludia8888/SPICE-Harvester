from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal

try:
    import yaml as _yaml
except ModuleNotFoundError:  # pragma: no cover - exercised via fallback parser tests
    _yaml = None

CompatibilityStatus = Literal["supported", "partial", "unsupported"]

_ALLOWED_STATUS = frozenset({"supported", "partial", "unsupported"})
_ALLOWED_ENGINES = frozenset({"preview", "spark"})


def _parse_scalar(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if (text.startswith('"') and text.endswith('"')) or (text.startswith("'") and text.endswith("'")):
        return text[1:-1]
    return text


def _load_yaml_payload(path: Path) -> Any:
    if _yaml is not None:
        return _yaml.safe_load(path.read_text(encoding="utf-8")) or {}

    # Fallback parser for the controlled snapshot shape:
    # top-level keys + "functions" list with nested "engines".
    payload: dict[str, Any] = {}
    functions: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    in_engines = False

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        stripped = raw_line.strip()
        if not stripped or stripped.startswith("#"):
            continue

        if stripped == "functions:":
            continue

        if stripped.startswith("- name:"):
            if current is not None:
                functions.append(current)
            current = {"name": _parse_scalar(stripped.split(":", 1)[1])}
            in_engines = False
            continue

        if current is None:
            if ":" not in stripped:
                continue
            key, value = stripped.split(":", 1)
            payload[key.strip()] = _parse_scalar(value)
            continue

        if stripped == "engines:":
            current["engines"] = {}
            in_engines = True
            continue

        if ":" not in stripped:
            continue

        key, value = stripped.split(":", 1)
        key = key.strip()
        parsed = _parse_scalar(value)
        if in_engines and key in _ALLOWED_ENGINES:
            engines = current.setdefault("engines", {})
            if isinstance(engines, dict):
                engines[key] = parsed
            continue

        in_engines = False
        current[key] = parsed

    if current is not None:
        functions.append(current)
    payload["functions"] = functions
    return payload


@dataclass(frozen=True)
class FunctionCompatibility:
    name: str
    category: str
    preview: CompatibilityStatus
    spark: CompatibilityStatus
    notes: str = ""

    def status_for_engine(self, engine: str) -> CompatibilityStatus:
        normalized = str(engine or "").strip().lower()
        if normalized == "preview":
            return self.preview
        if normalized == "spark":
            return self.spark
        raise ValueError(f"Unsupported engine: {engine}")


def default_snapshot_path() -> Path:
    return Path(__file__).resolve().parents[2] / "tests" / "fixtures" / "foundry_functions_index_snapshot_2026_02_13.yaml"


def _parse_status(raw: Any, *, field: str, fn_name: str) -> CompatibilityStatus:
    status = str(raw or "").strip().lower()
    if status not in _ALLOWED_STATUS:
        raise ValueError(
            f"Invalid compatibility status '{raw}' for {fn_name}.{field}; "
            f"expected one of {sorted(_ALLOWED_STATUS)}"
        )
    return status  # type: ignore[return-value]


def load_foundry_functions_snapshot(snapshot_path: str | Path) -> list[FunctionCompatibility]:
    path = Path(snapshot_path)
    payload = _load_yaml_payload(path) or {}
    if not isinstance(payload, dict):
        raise ValueError("snapshot must be a mapping")

    functions = payload.get("functions")
    if not isinstance(functions, list) or not functions:
        raise ValueError("snapshot.functions must be a non-empty list")

    entries: list[FunctionCompatibility] = []
    seen: set[str] = set()

    for raw in functions:
        if not isinstance(raw, dict):
            raise ValueError("each function entry must be a mapping")

        name = str(raw.get("name") or "").strip().lower()
        if not name:
            raise ValueError("function entry missing name")
        if name in seen:
            raise ValueError(f"duplicate function name in snapshot: {name}")
        seen.add(name)

        category = str(raw.get("category") or "").strip().lower() or "uncategorized"
        engines = raw.get("engines")
        if not isinstance(engines, dict):
            raise ValueError(f"function '{name}' missing engines mapping")

        unknown_engines = set(engines.keys()) - _ALLOWED_ENGINES
        if unknown_engines:
            raise ValueError(f"function '{name}' has unsupported engines: {sorted(unknown_engines)}")

        preview = _parse_status(engines.get("preview"), field="preview", fn_name=name)
        spark = _parse_status(engines.get("spark"), field="spark", fn_name=name)

        entries.append(
            FunctionCompatibility(
                name=name,
                category=category,
                preview=preview,
                spark=spark,
                notes=str(raw.get("notes") or "").strip(),
            )
        )

    entries.sort(key=lambda item: item.name)
    return entries


def load_default_foundry_functions_snapshot() -> list[FunctionCompatibility]:
    return load_foundry_functions_snapshot(default_snapshot_path())


def filter_functions(
    entries: Iterable[FunctionCompatibility],
    *,
    engine: str,
    status: CompatibilityStatus,
) -> list[FunctionCompatibility]:
    normalized_engine = str(engine or "").strip().lower()
    if normalized_engine not in _ALLOWED_ENGINES:
        raise ValueError(f"Unsupported engine: {engine}")
    if status not in _ALLOWED_STATUS:
        raise ValueError(f"Unsupported status: {status}")

    filtered = [item for item in entries if item.status_for_engine(normalized_engine) == status]
    filtered.sort(key=lambda item: item.name)
    return filtered
