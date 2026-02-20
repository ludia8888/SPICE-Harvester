export type FoundryQaApiContractRow = {
  phase: string
  bffMethod: string
  endpoint: string
  backendSurface: 'bff_v1' | 'foundry_v2'
  notes?: string
}

export const FOUNDRY_QA_API_CONTRACT: FoundryQaApiContractRow[] = [
  {
    phase: 'Ingest',
    bffMethod: 'uploadDataset / approveDatasetIngestSchema',
    endpoint: 'POST /api/v1/pipelines/datasets/csv-upload, POST /api/v1/pipelines/datasets/ingest-requests/{id}/schema/approve',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Transform',
    bffMethod: 'createPipeline / buildPipeline / deployPipeline',
    endpoint: 'POST /api/v1/pipelines, POST /api/v1/pipelines/{pipelineId}/build, POST /api/v1/pipelines/{pipelineId}/deploy',
    backendSurface: 'bff_v1',
  },
  {
    phase: 'Ontology object types',
    bffMethod: 'createOntology / listObjectTypesV2',
    endpoint: 'POST /api/v1/databases/{db}/ontology, GET /api/v2/ontologies/{ontology}/objectTypes',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Objectify',
    bffMethod: 'createObjectifyMappingSpec / runObjectifyDataset',
    endpoint: 'POST /api/v1/objectify/mapping-specs, POST /api/v1/objectify/datasets/{datasetId}/run',
    backendSurface: 'bff_v1',
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
    phase: 'Closed loop + audit',
    bffMethod: 'createInstance / searchObjectsV2 / listAuditLogs',
    endpoint: 'POST /api/v1/databases/{db}/instances/{class}/create, POST /api/v2/ontologies/{ontology}/objects/{objectType}/search, GET /api/v1/audit/logs',
    backendSurface: 'foundry_v2',
  },
  {
    phase: 'Lineage',
    bffMethod: 'getLineageMetrics',
    endpoint: 'GET /api/v1/lineage/metrics',
    backendSurface: 'bff_v1',
  },
]
