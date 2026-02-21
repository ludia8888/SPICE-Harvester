export type FoundryQaApiContractRow = {
  phase: string
  bffMethod: string
  endpoint: string
  backendSurface: 'bff_v1' | 'foundry_v2'
  notes?: string
}

export const FOUNDRY_QA_API_CONTRACT: FoundryQaApiContractRow[] = [
  {
    phase: 'Ingest (Foundry v2 datasets)',
    bffMethod: 'createDatasetV2 / createTransactionV2 / uploadFileV2 / commitTransactionV2 / readTableV2',
    endpoint:
      'POST /api/v2/datasets, POST /api/v2/datasets/{datasetRid}/transactions, POST /api/v2/datasets/{datasetRid}/files:upload, POST /api/v2/datasets/{datasetRid}/transactions/{transactionRid}/commit, POST /api/v2/datasets/{datasetRid}/readTable',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Connectivity (Foundry v2)',
    bffMethod:
      'createConnectionV2 / listConnectionsV2 / testConnectionV2 / updateConnectionExportSettingsV2 / createConnectionExportRunV2|getConnectionExportRunV2|listConnectionExportRunsV2 / createTableImportV2|createFileImportV2|createVirtualTableV2 / execute*ImportV2',
    endpoint:
      'POST|GET /api/v2/connectivity/connections, POST /api/v2/connectivity/connections/{connectionRid}/test, POST /api/v2/connectivity/connections/{connectionRid}/updateExportSettings, POST|GET /api/v2/connectivity/connections/{connectionRid}/exportRuns, POST /api/v2/connectivity/connections/{connectionRid}/tableImports|fileImports|virtualTables, POST /api/v2/connectivity/connections/{connectionRid}/{resource}/{resourceRid}/execute',
    backendSurface: 'foundry_v2',
    notes:
      'All connectivity endpoints are preview-gated (`preview=true`) and must be called through FE/BFF contract only. Non-dry-run exportRuns now require connection markings unless explicitly bypassed by export settings.',
  },
  {
    phase: 'Transform',
    bffMethod: 'createPipeline / buildPipeline / deployPipeline',
    endpoint: 'POST /api/v1/pipelines, POST /api/v1/pipelines/{pipelineId}/build, POST /api/v1/pipelines/{pipelineId}/deploy',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Ontology object types',
    bffMethod: 'createObjectTypeV2 / updateObjectTypeV2 / listObjectTypesV2 / getObjectTypeFullMetadataV2',
    endpoint: 'POST /api/v2/ontologies/{ontology}/objectTypes, PATCH /api/v2/ontologies/{ontology}/objectTypes/{objectType}, GET /api/v2/ontologies/{ontology}/objectTypes, GET /api/v2/ontologies/{ontology}/objectTypes/{objectType}/fullMetadata',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Objectify',
    bffMethod: 'createObjectifyMappingSpec / runObjectifyDataset',
    endpoint: 'POST /api/v1/objectify/mapping-specs, POST /api/v1/objectify/datasets/{datasetId}/run',
    backendSurface: 'bff_v1',
    notes: 'runObjectifyDataset is version-pinned and must include dataset_version_id (artifact mode 제외).',
  },
  {
    phase: 'Projection and multihop',
    bffMethod: 'runGraphQueryCtx / recomputeProjection',
    endpoint: 'POST /api/v1/graph-query/{db}, POST /api/v1/admin/recompute-projection',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Action',
    bffMethod: 'createOntologyResourceV1 / recordOntologyDeploymentV1 / listActionTypesV2 / applyActionV2',
    endpoint: 'POST /api/v1/databases/{db}/ontology/resources/action-types, POST /api/v1/databases/{db}/ontology/records/deployments, GET /api/v2/ontologies/{ontology}/actionTypes, POST /api/v2/ontologies/{ontology}/actions/{action}/apply',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Dynamic security',
    bffMethod: 'upsertAccessPolicyV1 / listAccessPoliciesV1 / getActionTypeV2',
    endpoint:
      'POST /api/v1/access-policies, GET /api/v1/access-policies, GET /api/v2/ontologies/{ontology}/actionTypes/{actionType}',
    backendSurface: 'bff_v1',
    notes: 'Action permission profile + project-policy inheritance must be visible on FE/BFF contract and verifiable via policy CRUD.',
  },
  {
    phase: 'Kinetic query types',
    bffMethod: 'createOntologyResourceV1(functions) / listQueryTypesV2 / getQueryTypeV2 / executeQueryTypeV2',
    endpoint:
      'POST /api/v1/databases/{db}/ontology/resources/functions, GET /api/v2/ontologies/{ontology}/queryTypes, GET /api/v2/ontologies/{ontology}/queryTypes/{queryApiName}, POST /api/v2/ontologies/{ontology}/queries/{queryApiName}/execute',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Closed loop + audit',
    bffMethod: 'createInstance / getObjectV2 / searchObjectsV2 / listAuditLogs',
    endpoint: 'POST /api/v1/databases/{db}/instances/{class}/create, GET /api/v2/ontologies/{ontology}/objects/{objectType}/{primaryKey}, POST /api/v2/ontologies/{ontology}/objects/{objectType}/search, GET /api/v1/audit/logs',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Lineage',
    bffMethod:
      'getLineageGraphCtx / getLineagePathCtx / getLineageImpactCtx / getLineageDiffCtx / getLineageMetrics / getLineageRunsCtx / getLineageRunImpactCtx / getLineageTimelineCtx / getLineageColumnLineageCtx / getLineageOutOfDateCtx',
    endpoint:
      'GET /api/v1/lineage/graph, GET /api/v1/lineage/path, GET /api/v1/lineage/impact, GET /api/v1/lineage/diff, GET /api/v1/lineage/metrics, GET /api/v1/lineage/runs, GET /api/v1/lineage/run-impact, GET /api/v1/lineage/timeline, GET /api/v1/lineage/column-lineage, GET /api/v1/lineage/out-of-date',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Control plane',
    bffMethod: 'listDatabasesCtx / createDatabaseCtx / getSummary / getCommandStatus',
    endpoint: 'GET|POST /api/v1/databases, GET /api/v1/summary, GET /api/v1/commands/{commandId}/status',
    backendSurface: 'bff_v1',
    notes: 'Foundry QA allowlist includes control-plane endpoints that mandatory UI screens call.',
  },
]
