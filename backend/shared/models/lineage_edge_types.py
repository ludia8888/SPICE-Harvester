"""
Canonical lineage edge type constants.

This module centralizes runtime edge type identifiers so workers/services use a
single source of truth and avoid drift from typo/alias variants.
"""

EDGE_AGGREGATE_EMITTED_EVENT = "aggregate_emitted_event"
EDGE_COMMAND_CAUSED_DOMAIN_EVENT = "command_caused_domain_event"
EDGE_EVENT_STORED_IN_OBJECT_STORE = "event_stored_in_object_store"
EDGE_EVENT_MATERIALIZED_ES_DOCUMENT = "event_materialized_es_document"
EDGE_EVENT_DELETED_ES_DOCUMENT = "event_deleted_es_document"
EDGE_EVENT_WROTE_GRAPH_DOCUMENT = "event_wrote_graph_document"
EDGE_EVENT_DELETED_GRAPH_DOCUMENT = "event_deleted_graph_document"
EDGE_PIPELINE_OUTPUT_STORED = "pipeline_output_stored"
EDGE_DATASET_VERSION_OBJECTIFIED = "dataset_version_objectified"

LINEAGE_CANONICAL_EDGE_TYPES: frozenset[str] = frozenset(
    {
        EDGE_AGGREGATE_EMITTED_EVENT,
        EDGE_COMMAND_CAUSED_DOMAIN_EVENT,
        EDGE_EVENT_STORED_IN_OBJECT_STORE,
        EDGE_EVENT_MATERIALIZED_ES_DOCUMENT,
        EDGE_EVENT_DELETED_ES_DOCUMENT,
        EDGE_EVENT_WROTE_GRAPH_DOCUMENT,
        EDGE_EVENT_DELETED_GRAPH_DOCUMENT,
        EDGE_PIPELINE_OUTPUT_STORED,
        EDGE_DATASET_VERSION_OBJECTIFIED,
    }
)
