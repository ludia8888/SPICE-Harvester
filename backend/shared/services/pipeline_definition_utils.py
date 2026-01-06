from __future__ import annotations

from typing import Any, Dict, List, Optional

from shared.services.pipeline_schema_utils import normalize_expectations


def split_expectation_columns(column: str) -> List[str]:
    return [part.strip() for part in str(column or "").split(",") if part.strip()]


def resolve_execution_semantics(
    definition: Dict[str, Any],
    *,
    pipeline_type: Optional[str] = None,
) -> str:
    raw_mode = ""
    for key in ("execution_mode", "executionMode", "run_mode", "runMode", "batch_mode", "batchMode"):
        value = definition.get(key)
        if isinstance(value, str) and value.strip():
            raw_mode = value.strip().lower()
            break
    if not raw_mode:
        settings = definition.get("settings")
        if isinstance(settings, dict):
            engine = settings.get("engine")
            if isinstance(engine, str) and engine.strip():
                raw_mode = engine.strip().lower()

    if raw_mode in {"incremental", "increment", "append"}:
        return "incremental"
    if raw_mode in {"stream", "streaming"}:
        return "streaming"

    resolved_type = (
        pipeline_type
        or definition.get("pipeline_type")
        or definition.get("pipelineType")
        or ""
    )
    resolved_type = str(resolved_type or "").strip().lower()
    if resolved_type in {"stream", "streaming"}:
        return "streaming"
    if resolved_type in {"incremental", "increment", "inc"}:
        return "incremental"
    return "snapshot"


def resolve_incremental_config(definition: Dict[str, Any]) -> Dict[str, Any]:
    raw = definition.get("incremental") or definition.get("incremental_config") or definition.get("incrementalConfig")
    return dict(raw) if isinstance(raw, dict) else {}


def is_truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return False


def normalize_pk_semantics(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    raw = str(value).strip().lower()
    if not raw:
        return None
    if raw in {"snapshot", "overwrite", "full", "full_refresh", "replace"}:
        return "snapshot"
    if raw in {"append", "append_log", "log", "event", "history", "audit"}:
        return "append_log"
    if raw in {"state", "append_state", "latest", "upsert", "merge"}:
        return "append_state"
    if raw in {"remove", "delete", "tombstone"}:
        return "remove"
    return raw


def resolve_pk_semantics(
    *,
    execution_semantics: str,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
) -> str:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    incremental = resolve_incremental_config(definition)
    raw: Optional[str] = None
    for key in ("pkSemantics", "pk_semantics", "pkMode", "pk_mode", "loadSemantics", "load_semantics"):
        value = output_metadata.get(key) or incremental.get(key) or settings.get(key) or definition.get(key)
        if isinstance(value, str) and value.strip():
            raw = value
            break
    if raw is None:
        remove_flag = (
            output_metadata.get("remove")
            or output_metadata.get("delete")
            or incremental.get("remove")
            or incremental.get("delete")
        )
        if is_truthy(remove_flag):
            raw = "remove"

    normalized = normalize_pk_semantics(raw)
    if execution_semantics == "snapshot":
        return "snapshot"
    if normalized:
        return normalized
    if execution_semantics in {"incremental", "streaming"}:
        return "append_state"
    return "snapshot"


def resolve_delete_column(
    *,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
) -> Optional[str]:
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    incremental = resolve_incremental_config(definition)
    for key in ("deleteColumn", "delete_column", "removeColumn", "remove_column", "is_deleted", "isDeleted"):
        value = output_metadata.get(key) or incremental.get(key) or settings.get(key) or definition.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    return None


def coerce_pk_columns(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    if isinstance(value, list):
        columns: List[str] = []
        for item in value:
            if isinstance(item, dict):
                columns.extend(
                    coerce_pk_columns(
                        item.get("column")
                        or item.get("name")
                        or item.get("key")
                        or item.get("field")
                    )
                )
            else:
                columns.extend(coerce_pk_columns(item))
        return columns
    if isinstance(value, dict):
        columns: List[str] = []
        for key in (
            "columns",
            "column",
            "keys",
            "key",
            "fields",
            "field",
            "propertyKeys",
            "property_keys",
            "primaryKey",
            "primary_key",
            "primaryKeys",
            "primary_keys",
        ):
            columns.extend(coerce_pk_columns(value.get(key)))
        return columns
    return []


def collect_pk_columns(*candidates: Any) -> List[str]:
    seen: set[str] = set()
    output: List[str] = []
    for candidate in candidates:
        for column in coerce_pk_columns(candidate):
            if column and column not in seen:
                seen.add(column)
                output.append(column)
    return output


def match_output_declaration(
    output: Dict[str, Any],
    *,
    node_id: Optional[str],
    output_name: Optional[str],
) -> bool:
    if node_id:
        declared_node_id = str(output.get("node_id") or output.get("nodeId") or "").strip()
        if declared_node_id and declared_node_id == node_id:
            return True
    if output_name:
        declared_name = (
            output.get("output_name")
            or output.get("outputName")
            or output.get("dataset_name")
            or output.get("datasetName")
            or output.get("name")
        )
        if declared_name and str(declared_name).strip() == output_name:
            return True
    return False


def resolve_pk_columns(
    *,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
    output_name: Optional[str],
    output_node_id: Optional[str],
    declared_outputs: List[Dict[str, Any]],
) -> List[str]:
    definition_pk = collect_pk_columns(
        definition.get("pk_spec") or definition.get("pkSpec"),
        definition.get("primary_key")
        or definition.get("primaryKey")
        or definition.get("primary_keys")
        or definition.get("primaryKeys"),
    )
    output_pk = collect_pk_columns(
        output_metadata.get("pk_spec") or output_metadata.get("pkSpec"),
        output_metadata.get("primary_key")
        or output_metadata.get("primaryKey")
        or output_metadata.get("primary_keys")
        or output_metadata.get("primaryKeys")
        or output_metadata.get("pkColumns")
        or output_metadata.get("pk_columns"),
    )
    declared_pk: List[str] = []
    for item in declared_outputs:
        if not isinstance(item, dict):
            continue
        if not match_output_declaration(item, node_id=output_node_id, output_name=output_name):
            continue
        declared_pk.extend(
            collect_pk_columns(
                item.get("pk_spec") or item.get("pkSpec"),
                item.get("primary_key")
                or item.get("primaryKey")
                or item.get("primary_keys")
                or item.get("primaryKeys")
                or item.get("pkColumns")
                or item.get("pk_columns"),
            )
        )
    return collect_pk_columns(definition_pk, output_pk, declared_pk)


def build_expectations_with_pk(
    *,
    definition: Dict[str, Any],
    output_metadata: Dict[str, Any],
    output_name: Optional[str],
    output_node_id: Optional[str],
    declared_outputs: List[Dict[str, Any]],
    pk_semantics: str,
    delete_column: Optional[str],
    pk_columns: Optional[List[str]] = None,
    available_columns: Optional[set[str]] = None,
) -> List[Dict[str, Any]]:
    base_expectations = definition.get("expectations") if isinstance(definition.get("expectations"), list) else []
    pk_columns = pk_columns or resolve_pk_columns(
        definition=definition,
        output_metadata=output_metadata,
        output_name=output_name,
        output_node_id=output_node_id,
        declared_outputs=declared_outputs,
    )
    if available_columns is not None:
        pk_columns = [col for col in pk_columns if col in available_columns]
    if not pk_columns and pk_semantics != "remove":
        return list(base_expectations)

    existing_not_null: set[str] = set()
    existing_unique: set[str] = set()
    for exp in normalize_expectations(base_expectations):
        if exp.rule in {"not_null", "non_null"}:
            for col in split_expectation_columns(exp.column):
                if col:
                    existing_not_null.add(col)
        if exp.rule == "unique":
            cols = split_expectation_columns(exp.column)
            if cols:
                existing_unique.add(",".join(cols))

    injected: List[Dict[str, Any]] = []
    enforce_unique = pk_semantics in {"snapshot", "append_state"}
    for col in pk_columns:
        if col not in existing_not_null:
            injected.append({"rule": "not_null", "column": col})
    if enforce_unique and pk_columns:
        composite_key = ",".join(pk_columns)
        if composite_key not in existing_unique:
            injected.append({"rule": "unique", "column": composite_key})
    if pk_semantics == "remove" and delete_column:
        if available_columns is None or delete_column in available_columns:
            if delete_column not in existing_not_null:
                injected.append({"rule": "not_null", "column": delete_column})

    return list(base_expectations) + injected


def validate_pk_semantics(
    *,
    available_columns: set[str],
    pk_semantics: str,
    pk_columns: List[str],
    delete_column: Optional[str],
) -> List[str]:
    errors: List[str] = []
    missing_pk = [col for col in pk_columns if col not in available_columns]
    if missing_pk:
        errors.append(f"pk columns missing: {', '.join(missing_pk)}")
    if pk_semantics == "remove":
        if not delete_column:
            errors.append("remove semantics requires deleteColumn (e.g. is_deleted)")
        elif delete_column not in available_columns:
            errors.append(f"delete column missing: {delete_column}")
    return errors
