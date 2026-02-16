"""
Shared Elasticsearch mapping contract for the instances index.

This module is intentionally placed in `shared` so workers and services can
reuse the same mapping definition without cross-package imports.
"""

from __future__ import annotations

from typing import Any, Dict


INSTANCE_INDEX_MAPPING: Dict[str, Any] = {
    "properties": {
        "instance_id": {"type": "keyword"},
        "class_id": {"type": "keyword"},
        "class_label": {
            "type": "text",
            "fields": {"keyword": {"type": "keyword"}},
        },
        "properties": {
            "type": "nested",
            "properties": {
                "name": {"type": "keyword"},
                "value": {
                    "type": "text",
                    "fields": {
                        "keyword": {"type": "keyword"},
                        "numeric": {"type": "double", "ignore_malformed": True},
                    },
                },
                "type": {"type": "keyword"},
            },
        },
        "data": {"enabled": False},
        "lifecycle_id": {"type": "keyword"},
        "event_id": {"type": "keyword"},
        "event_sequence": {"type": "long"},
        "event_timestamp": {"type": "date"},
        "version": {"type": "long"},
        "db_name": {"type": "keyword"},
        "branch": {"type": "keyword"},
        "ontology_ref": {"type": "keyword"},
        "ontology_commit": {"type": "keyword"},
        "created_at": {"type": "date"},
        "updated_at": {"type": "date"},
        "overlay_tombstone": {"type": "boolean"},
        "patchset_commit_id": {"type": "keyword"},
        "action_log_id": {"type": "keyword"},
        "conflict_status": {"type": "keyword"},
        "base_token": {"type": "object", "enabled": True},
        "relationships": {"type": "object", "enabled": True},
        "backing_dataset": {
            "type": "object",
            "properties": {
                "dataset_id": {"type": "keyword"},
                "dataset_version_id": {"type": "keyword"},
                "artifact_id": {"type": "keyword"},
                "mapping_spec_id": {"type": "keyword"},
                "mapping_spec_version": {"type": "long"},
                "objectify_job_id": {"type": "keyword"},
                "indexed_at": {"type": "date"},
            },
        },
    }
}

