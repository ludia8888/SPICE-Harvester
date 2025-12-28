export type PipelineMode = 'edit' | 'proposals' | 'history'

export type PipelineTool = 'tools' | 'select' | 'remove'

export type PipelineNodeType = 'input' | 'transform' | 'output'

export type PipelineNodeStatus = 'success' | 'warning' | 'error'

export type PipelineSchemaCheck = {
  column: string
  rule: string
  value?: string | number | boolean
}

export type PipelineNodeMetadata = {
  datasetId?: string
  datasetName?: string
  datasetBranch?: string
  outputId?: string
  outputName?: string
  operation?: string
  expression?: string
  columns?: string[]
  rename?: Record<string, string>
  casts?: Array<{ column: string; type: string }>
  groupBy?: string[]
  aggregates?: Array<{ column: string; op: string; alias?: string }>
  pivot?: {
    index: string[]
    columns: string
    values: string
    agg?: string
  }
  window?: {
    partitionBy?: string[]
    orderBy?: string[]
    frame?: string
  }
  schemaChecks?: PipelineSchemaCheck[]
  joinKey?: string
  leftKey?: string
  rightKey?: string
  joinType?: string
  allowCrossJoin?: boolean
  unionMode?: string
  description?: string
}

export type PipelineNode = {
  id: string
  title: string
  type: PipelineNodeType
  icon: string
  x: number
  y: number
  subtitle?: string
  columns?: string[]
  status?: PipelineNodeStatus
  metadata?: PipelineNodeMetadata
}

export type PipelineEdge = {
  id: string
  from: string
  to: string
}

export type PipelineParameter = {
  id: string
  name: string
  value: string
}

export type PipelineOutput = {
  id: string
  name: string
  datasetName: string
  description?: string
}

export type PipelineSettings = {
  compute?: string
  memory?: string
  schedule?: string
  engine?: string
  branch?: string
  proposalStatus?: string
  proposalTitle?: string
  proposalDescription?: string
  expectationsJson?: string
  schemaContractJson?: string
}

export type PipelineExpectation = {
  rule: string
  column?: string
  value?: string | number | boolean
}

export type PipelineSchemaContract = {
  column: string
  type?: string
  required?: boolean
}

export type PipelineDependency = {
  pipelineId: string
  status?: string
}

export type PipelineDefinition = {
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  parameters: PipelineParameter[]
  outputs: PipelineOutput[]
  settings?: PipelineSettings
  expectations?: PipelineExpectation[]
  schemaContract?: PipelineSchemaContract[]
  dependencies?: PipelineDependency[]
}

export type PreviewColumn = {
  key: string
  type: string
}

export type PreviewRow = Record<string, string | number | null>

export const createId = (prefix = 'id') => {
  if (globalThis.crypto?.randomUUID) {
    return `${prefix}-${globalThis.crypto.randomUUID()}`
  }
  return `${prefix}-${Date.now()}-${Math.random().toString(16).slice(2)}`
}

export const createDefaultDefinition = (): PipelineDefinition => {
  const nodes: PipelineNode[] = []

  const edges: PipelineEdge[] = []

  return {
    nodes,
    edges,
    parameters: [],
    outputs: [],
    expectations: [],
    schemaContract: [],
    dependencies: [],
    settings: {
      compute: 'Medium',
      memory: '4 GB',
      schedule: 'Manual',
      engine: 'Batch',
    },
  }
}
