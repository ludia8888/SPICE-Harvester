from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from shared.services.pipeline.pipeline_definition_utils import collect_pk_columns, resolve_execution_semantics


class DatasetWriteMode(str, Enum):
    DEFAULT = "default"
    ALWAYS_APPEND = "always_append"
    APPEND_ONLY_NEW_ROWS = "append_only_new_rows"
    CHANGELOG = "changelog"
    SNAPSHOT_DIFFERENCE = "snapshot_difference"
    SNAPSHOT_REPLACE = "snapshot_replace"
    SNAPSHOT_REPLACE_AND_REMOVE = "snapshot_replace_and_remove"


SUPPORTED_DATASET_WRITE_MODES: frozenset[str] = frozenset(mode.value for mode in DatasetWriteMode)
SUPPORTED_DATASET_OUTPUT_FORMATS: frozenset[str] = frozenset({"parquet", "json", "csv", "avro", "orc"})


_DATASET_WRITE_MODE_ALIASES: Dict[str, str] = {
    "default": DatasetWriteMode.DEFAULT.value,
    "append": DatasetWriteMode.ALWAYS_APPEND.value,
    "always_append": DatasetWriteMode.ALWAYS_APPEND.value,
    "alwaysappend": DatasetWriteMode.ALWAYS_APPEND.value,
    "append_all": DatasetWriteMode.ALWAYS_APPEND.value,
    "append_only_new_rows": DatasetWriteMode.APPEND_ONLY_NEW_ROWS.value,
    "append_state": DatasetWriteMode.APPEND_ONLY_NEW_ROWS.value,
    "upsert": DatasetWriteMode.APPEND_ONLY_NEW_ROWS.value,
    "merge": DatasetWriteMode.APPEND_ONLY_NEW_ROWS.value,
    "state": DatasetWriteMode.APPEND_ONLY_NEW_ROWS.value,
    "changelog": DatasetWriteMode.CHANGELOG.value,
    "append_log": DatasetWriteMode.CHANGELOG.value,
    "log": DatasetWriteMode.CHANGELOG.value,
    "history": DatasetWriteMode.CHANGELOG.value,
    "event": DatasetWriteMode.CHANGELOG.value,
    "snapshot_difference": DatasetWriteMode.SNAPSHOT_DIFFERENCE.value,
    "snapshot_diff": DatasetWriteMode.SNAPSHOT_DIFFERENCE.value,
    "diff": DatasetWriteMode.SNAPSHOT_DIFFERENCE.value,
    "delta": DatasetWriteMode.SNAPSHOT_DIFFERENCE.value,
    "snapshot_replace": DatasetWriteMode.SNAPSHOT_REPLACE.value,
    "overwrite": DatasetWriteMode.SNAPSHOT_REPLACE.value,
    "snapshot": DatasetWriteMode.SNAPSHOT_REPLACE.value,
    "replace": DatasetWriteMode.SNAPSHOT_REPLACE.value,
    "full_refresh": DatasetWriteMode.SNAPSHOT_REPLACE.value,
    "snapshot_replace_and_remove": DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE.value,
    "remove": DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE.value,
    "delete": DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE.value,
    "tombstone": DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE.value,
}

_WRITE_MODE_KEYS: Tuple[str, ...] = (
    "write_mode",
    "writeMode",
    "pkSemantics",
    "pk_semantics",
    "pkMode",
    "pk_mode",
    "loadSemantics",
    "load_semantics",
)

_PRIMARY_KEY_KEYS: Tuple[str, ...] = (
    "primary_key_columns",
    "primaryKeyColumns",
    "pk_columns",
    "pkColumns",
    "primary_key",
    "primaryKey",
    "primary_keys",
    "primaryKeys",
    "pk_spec",
    "pkSpec",
)

_POST_FILTERING_KEYS: Tuple[str, ...] = (
    "post_filtering_column",
    "postFilteringColumn",
    "delete_column",
    "deleteColumn",
    "removeColumn",
    "remove_column",
    "is_deleted",
    "isDeleted",
)

_OUTPUT_FORMAT_KEYS: Tuple[str, ...] = (
    "output_format",
    "outputFormat",
    "format",
)

_PARTITION_KEYS: Tuple[str, ...] = (
    "partition_by",
    "partitionBy",
)


@dataclass(frozen=True)
class NormalizedDatasetOutputMetadata:
    metadata: Dict[str, Any]
    warnings: List[str] = field(default_factory=list)
    explicit_write_mode: bool = False


@dataclass(frozen=True)
class ResolvedDatasetWritePolicy:
    requested_write_mode: str
    resolved_write_mode: DatasetWriteMode
    runtime_write_mode: str
    primary_key_columns: List[str]
    post_filtering_column: Optional[str]
    output_format: str
    partition_by: List[str]
    execution_semantics: str
    explicit_write_mode: bool
    normalized_metadata: Dict[str, Any]
    has_incremental_input: bool = False
    incremental_inputs_have_additive_updates: Optional[bool] = None
    warnings: List[str] = field(default_factory=list)
    policy_hash: str = ""

    @property
    def requires_primary_key(self) -> bool:
        return self.resolved_write_mode in {
            DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
            DatasetWriteMode.CHANGELOG,
            DatasetWriteMode.SNAPSHOT_DIFFERENCE,
            DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
        }

    @property
    def append_mode(self) -> bool:
        return self.runtime_write_mode == "append"


def _text(value: Any) -> str:
    return str(value or "").strip()


def _first_text(
    *,
    output_metadata: Mapping[str, Any],
    settings: Mapping[str, Any],
    definition: Mapping[str, Any],
    keys: Iterable[str],
) -> Tuple[str, Optional[str]]:
    for source in (output_metadata, settings, definition):
        for key in keys:
            raw = source.get(key)
            if isinstance(raw, str) and raw.strip():
                return raw.strip(), key
    return "", None


def _first_raw(
    *,
    output_metadata: Mapping[str, Any],
    settings: Mapping[str, Any],
    definition: Mapping[str, Any],
    keys: Iterable[str],
) -> Any:
    for source in (output_metadata, settings, definition):
        for key in keys:
            if key in source and source.get(key) is not None:
                return source.get(key)
    return None


def _normalize_partition_by(value: Any) -> List[str]:
    if value is None:
        return []
    if isinstance(value, str):
        tokens = [part.strip() for part in value.split(",") if part.strip()]
    elif isinstance(value, list):
        tokens = [str(item).strip() for item in value if str(item).strip()]
    else:
        return []
    deduped: List[str] = []
    seen: set[str] = set()
    for token in tokens:
        if token in seen:
            continue
        seen.add(token)
        deduped.append(token)
    return deduped


def _normalize_primary_key_columns(value: Any) -> List[str]:
    values = collect_pk_columns(value)
    deduped: List[str] = []
    seen: set[str] = set()
    for item in values:
        key = _text(item)
        if not key:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(key)
    return deduped


def _normalize_write_mode(value: str) -> str:
    raw = _text(value).lower()
    if not raw:
        return DatasetWriteMode.DEFAULT.value
    return _DATASET_WRITE_MODE_ALIASES.get(raw, raw)


def _mode_requires_primary_key(mode: DatasetWriteMode) -> bool:
    return mode in {
        DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
        DatasetWriteMode.CHANGELOG,
        DatasetWriteMode.SNAPSHOT_DIFFERENCE,
        DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE,
    }


def normalize_dataset_output_metadata(
    *,
    definition: Mapping[str, Any],
    output_metadata: Mapping[str, Any],
) -> NormalizedDatasetOutputMetadata:
    metadata = dict(output_metadata or {})
    settings = definition.get("settings") if isinstance(definition.get("settings"), dict) else {}
    warnings: List[str] = []

    write_mode_raw, write_mode_key = _first_text(
        output_metadata=output_metadata,
        settings=settings,
        definition=definition,
        keys=_WRITE_MODE_KEYS,
    )
    explicit_write_mode = bool(write_mode_raw)
    normalized_write_mode = _normalize_write_mode(write_mode_raw)
    if write_mode_key and write_mode_key not in {"write_mode", "writeMode"}:
        warnings.append(
            f"legacy write-mode key '{write_mode_key}' normalized to 'write_mode={normalized_write_mode}'"
        )

    primary_key_raw = _first_raw(
        output_metadata=output_metadata,
        settings=settings,
        definition=definition,
        keys=_PRIMARY_KEY_KEYS,
    )
    primary_key_columns = _normalize_primary_key_columns(primary_key_raw)

    post_filtering_value, post_filtering_key = _first_text(
        output_metadata=output_metadata,
        settings=settings,
        definition=definition,
        keys=_POST_FILTERING_KEYS,
    )
    if post_filtering_key and post_filtering_key not in {"post_filtering_column", "postFilteringColumn"}:
        warnings.append(
            "legacy post-filtering key "
            f"'{post_filtering_key}' normalized to 'post_filtering_column={post_filtering_value}'"
        )

    output_format_value, output_format_key = _first_text(
        output_metadata=output_metadata,
        settings=settings,
        definition=definition,
        keys=_OUTPUT_FORMAT_KEYS,
    )
    output_format = _text(output_format_value).lower() or "parquet"
    if output_format_key and output_format_key not in {"output_format", "outputFormat"}:
        warnings.append(
            f"legacy output format key '{output_format_key}' normalized to 'output_format={output_format}'"
        )

    partition_value = _first_raw(
        output_metadata=output_metadata,
        settings=settings,
        definition=definition,
        keys=_PARTITION_KEYS,
    )
    partition_by = _normalize_partition_by(partition_value)

    if "write_mode" not in metadata:
        metadata["write_mode"] = normalized_write_mode
    metadata["primary_key_columns"] = list(primary_key_columns)
    metadata["post_filtering_column"] = post_filtering_value or None
    metadata["output_format"] = output_format
    metadata["partition_by"] = list(partition_by)

    return NormalizedDatasetOutputMetadata(
        metadata=metadata,
        warnings=warnings,
        explicit_write_mode=explicit_write_mode,
    )


def _resolve_default_mode(
    *,
    execution_semantics: str,
    has_incremental_input: bool,
    primary_key_columns: List[str],
    incremental_inputs_have_additive_updates: Optional[bool],
    warnings: List[str],
) -> DatasetWriteMode:
    _ = execution_semantics
    if not has_incremental_input:
        return DatasetWriteMode.SNAPSHOT_REPLACE
    if incremental_inputs_have_additive_updates is False:
        return DatasetWriteMode.SNAPSHOT_REPLACE
    if incremental_inputs_have_additive_updates is None:
        warnings.append(
            "write_mode default resolved to snapshot_replace because additive incremental update signal is unavailable"
        )
        return DatasetWriteMode.SNAPSHOT_REPLACE
    if primary_key_columns:
        return DatasetWriteMode.APPEND_ONLY_NEW_ROWS
    warnings.append(
        "write_mode default resolved to always_append because primary_key_columns are missing"
    )
    return DatasetWriteMode.ALWAYS_APPEND


def _normalize_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    text = _text(value).lower()
    if text in {"1", "true", "yes", "y", "on"}:
        return True
    if text in {"0", "false", "no", "n", "off"}:
        return False
    return None


def _runtime_write_mode(mode: DatasetWriteMode) -> str:
    if mode in {
        DatasetWriteMode.ALWAYS_APPEND,
        DatasetWriteMode.APPEND_ONLY_NEW_ROWS,
        DatasetWriteMode.CHANGELOG,
    }:
        return "append"
    return "overwrite"


def _policy_hash_payload(policy: ResolvedDatasetWritePolicy) -> Dict[str, Any]:
    return {
        "requested_write_mode": policy.requested_write_mode,
        "resolved_write_mode": policy.resolved_write_mode.value,
        "runtime_write_mode": policy.runtime_write_mode,
        "primary_key_columns": list(policy.primary_key_columns),
        "post_filtering_column": policy.post_filtering_column,
        "output_format": policy.output_format,
        "partition_by": list(policy.partition_by),
        "execution_semantics": policy.execution_semantics,
        "has_incremental_input": policy.has_incremental_input,
        "incremental_inputs_have_additive_updates": policy.incremental_inputs_have_additive_updates,
    }


def _compute_policy_hash(payload: Mapping[str, Any]) -> str:
    encoded = json.dumps(dict(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def resolve_dataset_write_policy(
    *,
    definition: Mapping[str, Any],
    output_metadata: Mapping[str, Any],
    execution_semantics: Optional[str] = None,
    has_incremental_input: Optional[bool] = None,
    incremental_inputs_have_additive_updates: Optional[bool] = None,
) -> ResolvedDatasetWritePolicy:
    normalized = normalize_dataset_output_metadata(definition=definition, output_metadata=output_metadata)
    warnings = list(normalized.warnings)

    resolved_execution = _text(execution_semantics) or resolve_execution_semantics(dict(definition or {}))
    inferred_has_incremental_input = resolved_execution in {"incremental", "streaming"}
    effective_has_incremental_input = (
        bool(has_incremental_input) if has_incremental_input is not None else inferred_has_incremental_input
    )
    effective_additive_updates = _normalize_bool(incremental_inputs_have_additive_updates)
    primary_key_columns = _normalize_primary_key_columns(normalized.metadata.get("primary_key_columns"))
    requested_write_mode = _text(normalized.metadata.get("write_mode") or DatasetWriteMode.DEFAULT.value).lower()
    if not requested_write_mode:
        requested_write_mode = DatasetWriteMode.DEFAULT.value
    effective_write_mode = requested_write_mode

    if effective_write_mode not in SUPPORTED_DATASET_WRITE_MODES:
        warnings.append(f"unknown write_mode '{effective_write_mode}' normalized to default")
        effective_write_mode = DatasetWriteMode.DEFAULT.value

    if effective_write_mode == DatasetWriteMode.DEFAULT.value:
        resolved_mode = _resolve_default_mode(
            execution_semantics=resolved_execution,
            has_incremental_input=effective_has_incremental_input,
            primary_key_columns=primary_key_columns,
            incremental_inputs_have_additive_updates=effective_additive_updates,
            warnings=warnings,
        )
    else:
        resolved_mode = DatasetWriteMode(effective_write_mode)

    output_format = _text(normalized.metadata.get("output_format") or "parquet").lower() or "parquet"
    partition_by = _normalize_partition_by(normalized.metadata.get("partition_by"))
    post_filtering_column = _text(normalized.metadata.get("post_filtering_column")) or None

    policy = ResolvedDatasetWritePolicy(
        requested_write_mode=requested_write_mode,
        resolved_write_mode=resolved_mode,
        runtime_write_mode=_runtime_write_mode(resolved_mode),
        primary_key_columns=primary_key_columns,
        post_filtering_column=post_filtering_column,
        output_format=output_format,
        partition_by=partition_by,
        execution_semantics=resolved_execution,
        explicit_write_mode=normalized.explicit_write_mode,
        has_incremental_input=effective_has_incremental_input,
        incremental_inputs_have_additive_updates=effective_additive_updates,
        normalized_metadata=dict(normalized.metadata),
        warnings=warnings,
        policy_hash="",
    )
    policy_hash = _compute_policy_hash(_policy_hash_payload(policy))
    return ResolvedDatasetWritePolicy(
        requested_write_mode=policy.requested_write_mode,
        resolved_write_mode=policy.resolved_write_mode,
        runtime_write_mode=policy.runtime_write_mode,
        primary_key_columns=list(policy.primary_key_columns),
        post_filtering_column=policy.post_filtering_column,
        output_format=policy.output_format,
        partition_by=list(policy.partition_by),
        execution_semantics=policy.execution_semantics,
        explicit_write_mode=policy.explicit_write_mode,
        has_incremental_input=policy.has_incremental_input,
        incremental_inputs_have_additive_updates=policy.incremental_inputs_have_additive_updates,
        normalized_metadata=dict(policy.normalized_metadata),
        warnings=list(policy.warnings),
        policy_hash=policy_hash,
    )


def validate_dataset_output_metadata(
    *,
    definition: Mapping[str, Any],
    output_metadata: Mapping[str, Any],
    execution_semantics: Optional[str] = None,
    has_incremental_input: Optional[bool] = None,
    incremental_inputs_have_additive_updates: Optional[bool] = None,
    available_columns: Optional[Iterable[str]] = None,
) -> List[str]:
    policy = resolve_dataset_write_policy(
        definition=definition,
        output_metadata=output_metadata,
        execution_semantics=execution_semantics,
        has_incremental_input=has_incremental_input,
        incremental_inputs_have_additive_updates=incremental_inputs_have_additive_updates,
    )
    errors: List[str] = []

    requested = policy.requested_write_mode
    if requested not in SUPPORTED_DATASET_WRITE_MODES:
        errors.append(
            "write_mode must be one of: " + "|".join(sorted(SUPPORTED_DATASET_WRITE_MODES))
        )

    if policy.output_format not in SUPPORTED_DATASET_OUTPUT_FORMATS:
        errors.append(
            "output_format must be one of: " + "|".join(sorted(SUPPORTED_DATASET_OUTPUT_FORMATS))
        )

    if _mode_requires_primary_key(policy.resolved_write_mode) and not policy.primary_key_columns:
        errors.append(
            f"write_mode={policy.resolved_write_mode.value} requires primary_key_columns"
        )

    if policy.resolved_write_mode == DatasetWriteMode.SNAPSHOT_REPLACE_AND_REMOVE and not policy.post_filtering_column:
        errors.append("write_mode=snapshot_replace_and_remove requires post_filtering_column")

    if policy.primary_key_columns and len(set(policy.primary_key_columns)) != len(policy.primary_key_columns):
        errors.append("primary_key_columns must not contain duplicates")

    if available_columns is not None:
        available = {str(col).strip() for col in available_columns if str(col).strip()}
        missing_pk = [col for col in policy.primary_key_columns if col not in available]
        if missing_pk:
            errors.append("primary_key_columns missing in output schema: " + ", ".join(missing_pk))
        if policy.post_filtering_column and policy.post_filtering_column not in available:
            errors.append(
                f"post_filtering_column missing in output schema: {policy.post_filtering_column}"
            )
        missing_partitions = [col for col in policy.partition_by if col not in available]
        if missing_partitions:
            errors.append("partition_by columns missing in output schema: " + ", ".join(missing_partitions))

    return errors
