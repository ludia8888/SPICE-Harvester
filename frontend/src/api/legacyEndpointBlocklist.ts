const LEGACY_ENDPOINT_BLOCKLIST_REGEX_SOURCES = [
  '^/api/v1/databases/[^/]+/merge(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-mappings(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-mappings-from-excel(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-mappings-from-google-sheets(?:/|$)',
  '^/api/v1/databases/[^/]+/import-from-excel(?:/|$)',
  '^/api/v1/databases/[^/]+/import-from-google-sheets(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-schema-from-data(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-schema-from-excel(?:/|$)',
  '^/api/v1/databases/[^/]+/suggest-schema-from-google-sheets(?:/|$)',
  '^/api/v1/databases/[^/]+/branches(?:/|$)',
  '^/api/v1/pipelines/datasets/media-upload(?:/|$)',
  '^/api/v1/pipelines/datasets/ingest-requests/[^/]+/schema/approve(?:/|$)',
  '^/api/v1/pipeline-plans/[^/]+/preview(?:/|$)',
  '^/api/v1/pipelines/udfs(?:/|$)',
  // Phase 1 v2 migration: dataset endpoints replaced by /api/v2/datasets/*
  '^/api/v1/pipelines/datasets/csv-upload(?:/|$)',
  '^/api/v1/pipelines/datasets/excel-upload(?:/|$)',
  '^/api/v1/pipelines/datasets/[^/]+/versions(?:/|$)',
  // Phase 1 v2 migration: ontology create replaced by /api/v2/ontologies/*/objectTypes
  '^/api/v1/databases/[^/]+/ontology/?$',
  // Phase 1 v2 migration: pipeline preview replaced by /api/v2/orchestration/builds/create
  '^/api/v1/pipelines/[^/]+/preview(?:/|$)',
  '^/api/v1/agent/pipeline-runs(?:/|$)',
] as const

export const LEGACY_ENDPOINT_BLOCKLIST_PATTERNS: ReadonlyArray<RegExp> =
  LEGACY_ENDPOINT_BLOCKLIST_REGEX_SOURCES.map((source) => new RegExp(source))
