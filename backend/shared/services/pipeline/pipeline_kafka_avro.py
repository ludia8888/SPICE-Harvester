from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Optional
from urllib.parse import quote

import httpx

from shared.utils.mapping_text import (
    first_mapping_text as _first_text,
    normalized_text as _text,
)


@dataclass(frozen=True)
class KafkaAvroSchemaRegistryReference:
    url: str
    subject: str
    version: int

    @property
    def cache_key(self) -> str:
        return f"{self.url}|{self.subject}|{self.version}"

    @property
    def request_url(self) -> str:
        safe_subject = quote(self.subject, safe="")
        return f"{self.url.rstrip('/')}/subjects/{safe_subject}/versions/{self.version}"
def _mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    return {}


def _parse_schema_registry_version(*, version_raw: str, prefix: str) -> tuple[Optional[int], list[str]]:
    if not version_raw:
        return None, [f"{prefix} schema registry requires version"]

    normalized = version_raw.strip().lower()
    if normalized == "latest":
        return None, [
            f"{prefix} schema registry version=latest is not allowed; pin an integer version >= 1"
        ]

    try:
        parsed_version = int(version_raw)
    except (TypeError, ValueError):
        return None, [f"{prefix} schema registry version must be an integer >= 1"]
    if parsed_version <= 0:
        return None, [f"{prefix} schema registry version must be an integer >= 1"]
    return parsed_version, []


def resolve_inline_avro_schema(*, read_config: Mapping[str, Any]) -> str:
    return _first_text(
        keys=("avro_schema", "avroSchema", "value_schema", "valueSchema"),
        sources=(read_config,),
    )


def _extract_schema_registry_reference_parts(*, read_config: Mapping[str, Any]) -> tuple[str, str, str]:
    nested = _mapping(read_config.get("schema_registry") or read_config.get("schemaRegistry"))
    options = _mapping(read_config.get("options"))
    url = _first_text(
        keys=(
            "url",
            "avro_schema_registry_url",
            "avroSchemaRegistryUrl",
            "schema_registry_url",
            "schemaRegistryUrl",
            "schema.registry.url",
            "registry_url",
        ),
        sources=(read_config, nested, options),
    )
    subject = _first_text(
        keys=(
            "avro_schema_subject",
            "avroSchemaSubject",
            "schema_subject",
            "schemaSubject",
            "subject",
        ),
        sources=(read_config, nested, options),
    )
    version = _first_text(
        keys=(
            "avro_schema_version",
            "avroSchemaVersion",
            "schema_version",
            "schemaVersion",
            "version",
        ),
        sources=(read_config, nested, options),
    )
    return url, subject, version


def validate_kafka_avro_schema_config(
    *,
    read_config: Mapping[str, Any],
    node_id: str,
) -> list[str]:
    prefix = f"input node {node_id} kafka value_format=avro"
    inline_schema = resolve_inline_avro_schema(read_config=read_config)
    if inline_schema:
        return []

    url, subject, version_raw = _extract_schema_registry_reference_parts(read_config=read_config)
    if not url and not subject and not version_raw:
        return [
            f"{prefix} requires read.avro_schema or read.schema_registry(url+subject+version)"
        ]

    errors: list[str] = []
    if not url:
        errors.append(f"{prefix} schema registry requires url")
    elif not (url.startswith("http://") or url.startswith("https://")):
        errors.append(f"{prefix} schema registry url must start with http:// or https://")

    if not subject:
        errors.append(f"{prefix} schema registry requires subject")
    _, version_errors = _parse_schema_registry_version(version_raw=version_raw, prefix=prefix)
    errors.extend(version_errors)

    return errors


def resolve_kafka_avro_schema_registry_reference(
    *,
    read_config: Mapping[str, Any],
    node_id: str,
) -> KafkaAvroSchemaRegistryReference:
    errors = validate_kafka_avro_schema_config(read_config=read_config, node_id=node_id)
    if errors:
        raise ValueError("; ".join(errors))

    url, subject, version_raw = _extract_schema_registry_reference_parts(read_config=read_config)
    prefix = f"input node {node_id} kafka value_format=avro"
    parsed_version, version_errors = _parse_schema_registry_version(
        version_raw=version_raw,
        prefix=prefix,
    )
    if version_errors or parsed_version is None:
        raise ValueError("; ".join(version_errors or [f"{prefix} schema registry requires version"]))
    return KafkaAvroSchemaRegistryReference(
        url=url,
        subject=subject,
        version=parsed_version,
    )


def parse_kafka_avro_schema_registry_response(*, payload: Any) -> str:
    if not isinstance(payload, Mapping):
        raise ValueError("schema registry response must be an object")
    schema = _text(payload.get("schema"))
    if not schema:
        raise ValueError("schema registry response missing schema")
    schema_type = _text(payload.get("schemaType") or payload.get("schema_type")).upper()
    if schema_type and schema_type != "AVRO":
        raise ValueError(f"schema registry schemaType must be AVRO (got: {schema_type})")
    return schema


def fetch_kafka_avro_schema_from_registry(
    *,
    reference: KafkaAvroSchemaRegistryReference,
    timeout_seconds: float,
    headers: Optional[Mapping[str, str]] = None,
) -> str:
    response = httpx.get(
        reference.request_url,
        timeout=float(timeout_seconds),
        headers=dict(headers or {}),
    )
    response.raise_for_status()
    return parse_kafka_avro_schema_registry_response(payload=response.json())
