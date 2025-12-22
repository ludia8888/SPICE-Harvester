export type PipelineMode = 'edit' | 'proposals' | 'history'

export type PipelineTool = 'tools' | 'select' | 'remove'

export type PipelineNodeType = 'input' | 'transform' | 'output'

export type PipelineNodeStatus = 'success' | 'warning' | 'error'

export type PipelineNodeMetadata = {
  datasetId?: string
  operation?: string
  expression?: string
  joinType?: string
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
}

export type PipelineDefinition = {
  nodes: PipelineNode[]
  edges: PipelineEdge[]
  parameters: PipelineParameter[]
  outputs: PipelineOutput[]
  settings?: PipelineSettings
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
    settings: {
      compute: 'Medium',
      memory: '4 GB',
      schedule: 'Manual',
      engine: 'Batch',
    },
  }
}
