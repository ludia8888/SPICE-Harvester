import { useCallback, useEffect, useMemo, useRef, useState } from 'react'
import { Button, Icon, Menu, MenuDivider, MenuItem, Popover, Spinner, Tag } from '@blueprintjs/core'
import type { IconName } from '@blueprintjs/icons'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import ReactFlow, {
  addEdge,
  MiniMap,
  type Connection,
  type Edge,
  type EdgeChange,
  type Node as FlowNode,
  type NodeChange,
  Position,
  type ReactFlowInstance,
  useEdgesState,
  useNodesState,
} from 'reactflow'
import { useAppStore } from '../state/store'
import {
  NaturalLanguagePipelineInput,
  NodePalette,
  TransformPresets,
  PipelineWizardDialog,
  CreatePipelineDialog,
  UdfManageDialog,
  DatasetSelectDialog,
  DatasetNode,
  TransformNode,
  type PaletteNode,
  type CreatePipelineDialogValues,
  type TransformPreset,
  type TransformType,
} from '../components/pipeline'
import { UploadFilesDialog } from '../components/UploadFilesDialog'
import {
  createPipeline,
  buildPipeline,
  deployPipeline,
  getPipeline,
  getPipelineReadiness,
  getTransformPreview,
  listDatabases,
  listDatasets,
  listPipelineArtifacts,
  listPipelines,
  previewPipelinePlan,
  runPipelineAgentStreaming,
  submitPipelineProposal,
  updatePipeline,
  type AgentStreamEvent,
  type DatabaseRecord,
  type DatasetRecord,
  type PipelineArtifactRecord,
  type PipelineDetailRecord,
  type PipelineRecord,
  type PipelineReadiness,
  type TransformPreviewResponse,
} from '../api/bff'

type ToolMode = 'pan' | 'pointer' | 'select' | 'remove'

type PreviewColumn = {
  key: string
  label: string
  type: string
  icon: IconName
  width: number
}
type PreviewRow = Record<string, string>

type SchemaColumn = {
  name: string
  type?: string
}

type DefinitionNode = {
  id?: string
  type?: string
  operation?: string
  metadata?: Record<string, unknown>
  meta?: Record<string, unknown>
}

type DefinitionEdge = {
  id?: string
  from?: string
  to?: string
  source?: string
  target?: string
}

// Pipeline Agent 응답의 plan 구조
type AgentPlanDefinition = {
  nodes?: DefinitionNode[]
  edges?: DefinitionEdge[]
}

type AgentPlan = {
  definition_json?: AgentPlanDefinition
  outputs?: Array<{ output_name?: string; output_kind?: string }>
}

// 백엔드 노드 타입 → TransformType 매핑
const mapBackendOperationToTransformType = (type?: string, operation?: string): TransformType | null => {
  const op = (operation || type || '').toLowerCase()

  // input 타입은 dataset 노드로 처리
  if (op === 'input') return null

  // 명시적 operation 매핑
  const operationMap: Record<string, TransformType> = {
    // 조합
    join: 'join',
    union: 'union',
    merge: 'join',
    // 필터링
    filter: 'filter',
    where: 'filter',
    dedupe: 'dedupe',
    dedup: 'dedupe',
    distinct: 'dedupe',
    // 컬럼 변환
    select: 'select',
    project: 'select',
    drop: 'drop',
    rename: 'rename',
    cast: 'cast',
    normalize: 'normalize',
    clean: 'normalize',
    regex: 'regexReplace',
    'regex-replace': 'regexReplace',
    // 계산
    compute: 'compute',
    transform: 'compute',
    calculate: 'compute',
    explode: 'explode',
    flatten: 'explode',
    // 집계
    groupby: 'groupBy',
    'group-by': 'groupBy',
    group_by: 'groupBy',
    aggregate: 'aggregate',
    agg: 'aggregate',
    sum: 'aggregate',
    pivot: 'pivot',
    window: 'window',
    rank: 'window',
    // 정렬
    sort: 'sort',
    order: 'sort',
    orderby: 'sort',
    // 출력
    output: 'output',
    sink: 'output',
    write: 'output',
    // UDF
    udf: 'udf',
    custom: 'udf',
  }

  return operationMap[op] || 'compute'
}

const normalizeBackendOperation = (value: unknown): string => {
  const raw = typeof value === 'string' ? value.trim() : String(value ?? '').trim()
  if (!raw) {
    return ''
  }
  const lowered = raw.toLowerCase()
  if (['group_by', 'groupby'].includes(lowered)) {
    return 'groupBy'
  }
  if (['regex_replace', 'regexreplace', 'regex-replace', 'regex'].includes(lowered)) {
    return 'regexReplace'
  }
  // Canonical operations are case-sensitive for groupBy/regexReplace; everything else is lower-case.
  return lowered
}

// Agent 플랜 응답을 ReactFlow 노드/엣지로 변환
const convertAgentPlanToFlow = (
  plan: AgentPlan,
  existingNodes: FlowNode[],
  existingEdges: Edge[],
  datasets: DatasetRecord[],
): { nodes: FlowNode[]; edges: Edge[] } => {
  const definition = plan.definition_json
  if (!definition) {
    return { nodes: existingNodes, edges: existingEdges }
  }

  const planNodes = definition.nodes || []
  const planEdges = definition.edges || []

  if (planNodes.length === 0) {
    return { nodes: existingNodes, edges: existingEdges }
  }

  // 기존 노드 ID 셋
  const existingNodeIds = new Set(existingNodes.map(n => n.id))

  // 새 노드 생성
  const newNodes: FlowNode[] = [...existingNodes]
  const newEdges: Edge[] = [...existingEdges]

  // 데이터셋 매핑 (ID → DatasetRecord)
  const datasetById = new Map<string, DatasetRecord>()
  const datasetByName = new Map<string, DatasetRecord>()
  datasets.forEach(d => {
    datasetById.set(d.dataset_id, d)
    datasetByName.set(d.name.toLowerCase(), d)
  })

  planNodes.forEach((node, index) => {
    const nodeId = node.id || `agent-node-${index}`

    // 이미 존재하는 노드는 건너뛰기
    if (existingNodeIds.has(nodeId)) return

    const nodeType = (node.type || '').toLowerCase()
    const meta = node.metadata || node.meta || {}
    // 백엔드는 operation을 metadata 안에 저장함
    const operation = (node.operation || String(meta.operation || '')).toLowerCase()

    // 임시 위치 (나중에 레이아웃으로 재배치)
    const position = { x: 200 * (index % 6), y: 150 * Math.floor(index / 6) }

    if (nodeType === 'input') {
      // Dataset 노드 생성
      const datasetId = String(meta.datasetId || meta.dataset_id || '')
      const datasetName = String(meta.datasetName || meta.dataset_name || '')

      // 데이터셋 찾기
      const dataset = datasetById.get(datasetId) || datasetByName.get(datasetName.toLowerCase())

      newNodes.push({
        id: nodeId,
        type: 'datasetInput',
        position,
        data: {
          label: dataset?.name || datasetName || nodeId,
          columnCount: dataset ? extractSchemaColumns(dataset.schema_json).length : undefined,
          sourceType: dataset?.source_type,
          stage: 'raw',
          metadata: { datasetId: dataset?.dataset_id || datasetId, datasetName: dataset?.name || datasetName },
        },
      })
    } else {
      // Transform 노드 생성
      const transformType = mapBackendOperationToTransformType(nodeType, operation)
      if (!transformType) return

      // 라벨과 설명 생성 (백엔드 메타데이터 기반)
      let label = ''
      let description = ''

      // operation별 라벨/설명 추출
      switch (operation) {
        case 'filter':
          label = 'Filter'
          description = String(meta.expression || '').slice(0, 40)
          break
        case 'join':
          label = `${String(meta.joinType || 'inner').toUpperCase()} Join`
          const leftKeys = Array.isArray(meta.leftKeys) ? meta.leftKeys.join(', ') : ''
          const rightKeys = Array.isArray(meta.rightKeys) ? meta.rightKeys.join(', ') : ''
          if (leftKeys && rightKeys) {
            description = `${leftKeys} = ${rightKeys}`
          }
          break
        case 'union':
          label = 'Union'
          break
        case 'select':
          label = 'Select'
          const cols = Array.isArray(meta.columns) ? meta.columns : []
          if (cols.length > 0) {
            description = cols.slice(0, 3).join(', ') + (cols.length > 3 ? '...' : '')
          }
          break
        case 'cast':
          label = 'Cast Types'
          const castRules = Array.isArray(meta.rules) ? meta.rules : []
          if (castRules.length > 0) {
            description = castRules.slice(0, 2).map((r: Record<string, unknown>) =>
              `${r.column || ''}→${r.targetType || r.type || ''}`
            ).filter(Boolean).join(', ')
          }
          break
        case 'normalize':
          label = 'Normalize'
          const normRules = Array.isArray(meta.rules) ? meta.rules : []
          if (normRules.length > 0) {
            description = normRules.slice(0, 2).map((r: Record<string, unknown>) =>
              `${r.column || ''}: ${r.op || r.operation || ''}`
            ).filter(Boolean).join(', ')
          }
          break
        case 'dedupe':
        case 'distinct':
          label = 'Dedupe'
          const dedupeKeys = Array.isArray(meta.keys) ? meta.keys : []
          if (dedupeKeys.length > 0) {
            description = `by ${dedupeKeys.join(', ')}`
          }
          break
        case 'sort':
        case 'orderby':
          label = 'Sort'
          const sortCols = Array.isArray(meta.columns) ? meta.columns : []
          if (sortCols.length > 0) {
            description = sortCols.slice(0, 2).map((c: unknown) =>
              typeof c === 'string' ? c : String((c as Record<string, unknown>)?.column || '')
            ).join(', ')
          }
          break
        case 'groupby':
        case 'aggregate':
          label = operation === 'groupby' ? 'Group By' : 'Aggregate'
          const groupKeysRaw = meta.groupBy || meta.keys
          const groupKeys: string[] = Array.isArray(groupKeysRaw) ? groupKeysRaw.map(k => String(k)) : []
          if (groupKeys.length > 0) {
            description = `by ${groupKeys.slice(0, 2).join(', ')}`
          }
          break
        case 'compute':
          label = 'Compute'
          const targetCol = String(meta.targetColumn || meta.target_column || '')
          const formula = String(meta.formula || meta.expression || '')
          if (targetCol) {
            description = `${targetCol} = ${formula.slice(0, 20)}`
          } else if (formula) {
            description = formula.slice(0, 30)
          }
          break
        case 'output':
          label = String(meta.outputName || meta.output_name || 'Output')
          break
        default:
          label = operation || nodeType || 'Transform'
          // 일반적인 rules 처리
          const rules = meta.rules as Array<{ column?: string; op?: string }> | undefined
          if (rules && Array.isArray(rules) && rules.length > 0) {
            description = rules.slice(0, 2).map(r =>
              `${r.column || ''}${r.op ? `→${r.op}` : ''}`
            ).filter(Boolean).join(', ')
          }
      }

      // 라벨 첫 글자 대문자
      label = label.charAt(0).toUpperCase() + label.slice(1)

      newNodes.push({
        id: nodeId,
        type: 'transform',
        position,
        data: {
          label,
          transformType,
          description: description || undefined,
          config: meta,
        },
      })
    }

    existingNodeIds.add(nodeId)
  })

  // 엣지 생성
  const existingEdgeIds = new Set(existingEdges.map(e => e.id))

  planEdges.forEach((edge, index) => {
    const source = edge.from || edge.source || ''
    const target = edge.to || edge.target || ''
    if (!source || !target) return

    const edgeId = edge.id || `agent-edge-${source}-${target}`
    if (existingEdgeIds.has(edgeId)) return

    newEdges.push({
      id: edgeId,
      source,
      target,
      type: 'straight',
      style: { stroke: '#404854', strokeWidth: 2 },
    })

    existingEdgeIds.add(edgeId)
  })

  return { nodes: newNodes, edges: newEdges }
}

type FolderOption = {
  id: string
  name: string
}

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value)

const safeText = (value: unknown) => {
  if (typeof value === 'string') {
    return value
  }
  if (value == null) {
    return ''
  }
  return String(value)
}

const normalizeOutputKey = (value: unknown) => safeText(value).trim().toLowerCase()

const parseTimestamp = (value?: string) => {
  if (!value) {
    return 0
  }
  const parsed = Date.parse(value)
  return Number.isNaN(parsed) ? 0 : parsed
}

const formatCellValue = (value: unknown) => {
  if (value === null || value === undefined) {
    return ''
  }
  if (typeof value === 'string') {
    return value
  }
  if (typeof value === 'number' || typeof value === 'boolean') {
    return String(value)
  }
  if (value instanceof Date) {
    return value.toISOString()
  }
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

const extractSchemaColumns = (schemaJson?: Record<string, unknown>): SchemaColumn[] => {
  if (!schemaJson) {
    return []
  }
  const rawColumns = schemaJson.columns
  if (!Array.isArray(rawColumns)) {
    return []
  }
  return rawColumns
    .map((column) => {
      if (typeof column === 'string') {
        return { name: column }
      }
      if (isRecord(column)) {
        const name =
          typeof column.name === 'string'
            ? column.name
            : typeof column.column === 'string'
              ? column.column
              : ''
        const type = typeof column.type === 'string' ? column.type : undefined
        return { name, type }
      }
      return null
    })
    .filter((column): column is SchemaColumn => Boolean(column && column.name))
}

const extractSampleRows = (sampleJson?: Record<string, unknown>): Record<string, unknown>[] => {
  if (!sampleJson) {
    return []
  }
  const rows = sampleJson.rows
  if (Array.isArray(rows)) {
    return rows.filter(isRecord)
  }
  return []
}

const extractSampleColumns = (
  sampleJson: Record<string, unknown> | undefined,
  sampleRows: Record<string, unknown>[],
): SchemaColumn[] => {
  if (sampleJson && Array.isArray(sampleJson.columns)) {
    return sampleJson.columns
      .map((column) => {
        if (typeof column === 'string') {
          return { name: column }
        }
        if (isRecord(column) && typeof column.name === 'string') {
          const type = typeof column.type === 'string' ? column.type : undefined
          return { name: column.name, type }
        }
        return null
      })
      .filter((column): column is SchemaColumn => Boolean(column && column.name))
  }

  if (sampleRows.length === 0) {
    return [] as SchemaColumn[]
  }

  const keys = new Set<string>()
  sampleRows.forEach((row) => {
    Object.keys(row).forEach((key) => keys.add(key))
  })
  return Array.from(keys).map((name) => ({ name }))
}

const normalizeTypeLabel = (value: string) => {
  const lower = value.toLowerCase()
  // ID 타입
  if (lower === 'id') {
    return 'ID'
  }
  // Foreign Key 타입
  if (lower === 'foreign_key' || lower === 'fk') {
    return 'FK'
  }
  // Enum 타입
  if (lower === 'enum') {
    return 'Enum'
  }
  // Email 타입
  if (lower === 'email') {
    return 'Email'
  }
  // Phone 타입
  if (lower === 'phone') {
    return 'Phone'
  }
  // Float 타입
  if (lower === 'float' || lower === 'decimal' || lower === 'double') {
    return 'Float'
  }
  // Integer 타입
  if (lower === 'integer' || lower === 'int') {
    return 'Integer'
  }
  // Number 타입
  if (lower === 'number') {
    return 'Number'
  }
  // Boolean 타입
  if (lower === 'boolean' || lower === 'bool') {
    return 'Boolean'
  }
  // Datetime 타입
  if (lower === 'datetime' || lower === 'timestamp') {
    return 'Datetime'
  }
  // Date 타입
  if (lower === 'date') {
    return 'Date'
  }
  // String 타입
  if (lower === 'string' || lower === 'text' || lower === 'char' || lower === 'varchar') {
    return 'String'
  }
  // 그 외는 첫 글자 대문자로 반환
  return value.charAt(0).toUpperCase() + value.slice(1)
}

const inferColumnType = (key: string, rows: Record<string, unknown>[]) => {
  // 컬럼 이름으로 타입 힌트
  const keyLower = key.toLowerCase()
  if (keyLower.includes('date') || keyLower.includes('time') || keyLower.includes('created') || keyLower.includes('updated')) {
    return 'Datetime'
  }
  if (keyLower.includes('flag') || keyLower.includes('is_') || keyLower === 'active' || keyLower === 'enabled') {
    return 'Boolean'
  }
  if (keyLower.includes('amount') || keyLower.includes('balance') || keyLower.includes('price') || keyLower.includes('count') || keyLower.includes('score') || keyLower.includes('limit')) {
    return 'Number'
  }
  if (keyLower.includes('id') && !keyLower.includes('valid')) {
    return 'ID'
  }
  if (keyLower.includes('email')) {
    return 'Email'
  }
  if (keyLower.includes('phone')) {
    return 'Phone'
  }

  // 샘플 값으로 타입 추론
  for (const row of rows) {
    const value = row[key]
    if (value === null || value === undefined || value === '') {
      continue
    }
    if (typeof value === 'number') {
      return Number.isInteger(value) ? 'Integer' : 'Number'
    }
    if (typeof value === 'boolean') {
      return 'Boolean'
    }
    if (Array.isArray(value)) {
      return 'Array'
    }
    if (typeof value === 'object') {
      return 'Object'
    }
    if (typeof value === 'string') {
      // 문자열 패턴으로 타입 추론
      const str = value.trim()
      // Boolean 패턴
      if (str === 'true' || str === 'false' || str === 'TRUE' || str === 'FALSE') {
        return 'Boolean'
      }
      // 날짜 패턴 (ISO, 슬래시, 하이픈)
      if (/^\d{4}-\d{2}-\d{2}/.test(str) || /^\d{2}\/\d{2}\/\d{4}/.test(str) || /^\d{4}\/\d{2}\/\d{2}/.test(str)) {
        return 'Datetime'
      }
      // 숫자 패턴 (정수, 소수, 음수, 통화)
      if (/^-?\d+$/.test(str)) {
        return 'Integer'
      }
      if (/^-?\d+\.\d+$/.test(str) || /^-?\d{1,3}(,\d{3})*(\.\d+)?$/.test(str)) {
        return 'Number'
      }
      // 이메일 패턴
      if (/^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(str)) {
        return 'Email'
      }
      // 전화번호 패턴
      if (/^[\d\s\-+()]{7,}$/.test(str)) {
        return 'Phone'
      }
      return 'String'
    }
  }
  return 'Unknown'
}

const selectColumnIcon = (type: string): IconName => {
  const lower = type.toLowerCase()
  // ID 타입
  if (lower === 'id') {
    return 'key'
  }
  // Foreign Key 타입
  if (lower === 'fk' || lower === 'foreign_key') {
    return 'link'
  }
  // Enum 타입
  if (lower === 'enum') {
    return 'property'
  }
  // 숫자 타입 (Integer, Float, Number)
  if (lower === 'integer' || lower === 'int' || lower === 'float' || lower === 'number' || lower === 'decimal' || lower === 'double') {
    return 'numerical'
  }
  // Boolean 타입
  if (lower === 'boolean' || lower === 'bool') {
    return 'tick-circle'
  }
  // 날짜/시간 타입
  if (lower === 'date' || lower === 'datetime' || lower === 'timestamp' || lower === 'time') {
    return 'calendar'
  }
  // 이메일 타입
  if (lower === 'email') {
    return 'envelope'
  }
  // 전화번호 타입
  if (lower === 'phone') {
    return 'phone'
  }
  // 배열 타입
  if (lower === 'array' || lower === 'list') {
    return 'array'
  }
  // 객체 타입
  if (lower === 'object' || lower === 'json') {
    return 'code-block'
  }
  // 문자열 (기본값)
  return 'font'
}

const estimateColumnWidth = (label: string) => {
  const width = Math.max(120, label.length * 12)
  return Math.min(320, width)
}

const buildPreviewFromDataset = (dataset: DatasetRecord | null) => {
  if (!dataset) {
    return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
  }
  const sampleJson = dataset.sample_json ?? {}
  const schemaJson = dataset.schema_json ?? {}
  const sampleRows = extractSampleRows(sampleJson)
  const schemaColumns = extractSchemaColumns(schemaJson)
  const sampleColumns = extractSampleColumns(sampleJson, sampleRows)
  const mergedColumns = schemaColumns.map((column) => ({ ...column }))
  const columnIndex = new Map(mergedColumns.map((column, index) => [column.name, index]))
  sampleColumns.forEach((column) => {
    const index = columnIndex.get(column.name)
    if (index === undefined) {
      columnIndex.set(column.name, mergedColumns.length)
      mergedColumns.push(column)
      return
    }
    if (!mergedColumns[index].type && column.type) {
      mergedColumns[index].type = column.type
    }
  })
  const columns = mergedColumns.map((column) => {
    const typeLabel = column.type ? normalizeTypeLabel(column.type) : inferColumnType(column.name, sampleRows)
    return {
      key: column.name,
      label: column.name,
      type: typeLabel,
      icon: selectColumnIcon(typeLabel),
      width: estimateColumnWidth(column.name),
    }
  })
  const rows = sampleRows.slice(0, 25).map((row) => {
    const formatted: PreviewRow = {}
    columns.forEach((column) => {
      formatted[column.key] = formatCellValue(row[column.key])
    })
    return formatted
  })
  return { columns, rows }
}

const buildPreviewFromPipeline = (preview: Record<string, unknown> | null) => {
  if (!preview) {
    return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
  }
  const rawColumns = Array.isArray(preview.columns) ? preview.columns : []
  const rawRows = Array.isArray(preview.rows) ? preview.rows : []
  const rowFallback = rawRows.length > 0 && isRecord(rawRows[0]) ? Object.keys(rawRows[0] as Record<string, unknown>) : []
  const columns = (rawColumns.length > 0 ? rawColumns : rowFallback)
    .map((column) => {
      if (typeof column === 'string') {
        return { name: column, type: '' }
      }
      if (isRecord(column)) {
        return {
          name: String(column.name ?? column.key ?? ''),
          type: typeof column.type === 'string' ? column.type : '',
        }
      }
      return null
    })
    .filter((column): column is { name: string; type: string } => Boolean(column && column.name))
    .map((column) => {
      const typeLabel = column.type ? normalizeTypeLabel(column.type) : inferColumnType(column.name, rawRows)
      return {
        key: column.name,
        label: column.name,
        type: typeLabel,
        icon: selectColumnIcon(typeLabel),
        width: estimateColumnWidth(column.name),
      }
    })
  const rows = rawRows.slice(0, 25).map((row) => {
    const formatted: PreviewRow = {}
    columns.forEach((column) => {
      formatted[column.key] = formatCellValue(isRecord(row) ? row[column.key] : '')
    })
    return formatted
  })
  return { columns, rows }
}

const buildPreviewFromTransform = (transformData: TransformPreviewResponse | null) => {
  if (!transformData) {
    return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
  }
  const schemaColumns = transformData.schema_json?.columns ?? []
  const sampleRows = transformData.sample_json?.rows ?? []

  const columns = schemaColumns.map((column) => {
    const typeLabel = column.type ? normalizeTypeLabel(column.type) : inferColumnType(column.name, sampleRows)
    return {
      key: column.name,
      label: column.name,
      type: typeLabel,
      icon: selectColumnIcon(typeLabel),
      width: estimateColumnWidth(column.name),
    }
  })

  const rows = sampleRows.slice(0, 25).map((row) => {
    const formatted: PreviewRow = {}
    columns.forEach((column) => {
      formatted[column.key] = formatCellValue(isRecord(row) ? row[column.key] : row[column.key as keyof typeof row])
    })
    return formatted
  })

  return { columns, rows }
}

// ReactFlow 커스텀 노드 타입 정의 (컴포넌트 외부에서 정의해야 함)
const nodeTypes = {
  datasetInput: DatasetNode,
  transform: TransformNode,
}

// 엣지 스타일 헬퍼
const edgeStyle = { stroke: '#404854', strokeWidth: 2 }
const createEdge = (id: string, source: string, target: string, targetHandle?: string): Edge => ({
  id,
  source,
  target,
  type: 'straight',
  style: edgeStyle,
  ...(targetHandle ? { targetHandle } : {}),
})

// Enterprise Analytics 완전한 ETL 파이프라인 빌더
const buildDatasetInputNodes = (datasets: DatasetRecord[]): { nodes: FlowNode[]; edges: Edge[] } => {
  const rawDatasets = datasets.filter(d => d.stage === 'raw' || !d.stage)
  const sortedDatasets = [...rawDatasets].sort((left, right) => left.name.localeCompare(right.name))
  const nodes: FlowNode[] = []
  const edges: Edge[] = []

  // 데이터셋 ID 매핑 (이름 기반으로 찾기)
  const findDataset = (namePattern: string) =>
    sortedDatasets.find(d => d.name.toLowerCase().includes(namePattern.toLowerCase()))

  const accountsDs = findDataset('accounts')
  const customersDs = findDataset('customers')
  const productsDs = findDataset('products')
  const ordersDs = findDataset('orders')
  const transactionsDs = findDataset('transactions')

  // 레이아웃 상수
  const ROW_HEIGHT = 140
  const COL_WIDTH = 220

  // ===== 1. ACCOUNTS PIPELINE =====
  // Raw → Normalize → Cast → Filter → Select → Canonical Accounts
  if (accountsDs) {
    const rawId = accountsDs.dataset_id
    const row = 0
    const schemaColumns = extractSchemaColumns(accountsDs.schema_json)

    nodes.push({
      id: rawId,
      type: 'datasetInput',
      data: {
        label: accountsDs.name,
        columnCount: schemaColumns.length || accountsDs.row_count,
        sourceType: accountsDs.source_type,
        stage: 'raw',
        metadata: { datasetId: rawId, datasetName: accountsDs.name },
      },
      position: { x: 0, y: row * ROW_HEIGHT },
    })

    const normalizeId = `${rawId}-normalize`
    const castId = `${rawId}-cast`
    const filterId = `${rawId}-filter`
    const selectId = `${rawId}-select`
    const outputId = `${rawId}-canonical`

    nodes.push(
      { id: normalizeId, type: 'transform', data: { label: 'Normalize', transformType: 'normalize', description: 'trim, emptyToNull' }, position: { x: COL_WIDTH, y: row * ROW_HEIGHT } },
      { id: castId, type: 'transform', data: { label: 'Cast Types', transformType: 'cast', description: 'balance→decimal, opened_date→date' }, position: { x: COL_WIDTH * 2, y: row * ROW_HEIGHT } },
      { id: filterId, type: 'transform', data: { label: 'Active Only', transformType: 'filter', description: "status = 'Active'" }, position: { x: COL_WIDTH * 3, y: row * ROW_HEIGHT } },
      { id: selectId, type: 'transform', data: { label: 'Select', transformType: 'select', description: 'account_id, customer_id, type, balance, status' }, position: { x: COL_WIDTH * 4, y: row * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Canonical Accounts', transformType: 'output' }, position: { x: COL_WIDTH * 5, y: row * ROW_HEIGHT } },
    )

    edges.push(
      createEdge(`e-${rawId}-norm`, rawId, normalizeId),
      createEdge(`e-${normalizeId}-cast`, normalizeId, castId),
      createEdge(`e-${castId}-filter`, castId, filterId),
      createEdge(`e-${filterId}-select`, filterId, selectId),
      createEdge(`e-${selectId}-out`, selectId, outputId),
    )
  }

  // ===== 2. CUSTOMERS PIPELINE =====
  // Raw → Normalize → Dedupe → Cast → Select → Canonical Customers
  if (customersDs) {
    const rawId = customersDs.dataset_id
    const row = 1
    const schemaColumns = extractSchemaColumns(customersDs.schema_json)

    nodes.push({
      id: rawId,
      type: 'datasetInput',
      data: {
        label: customersDs.name,
        columnCount: schemaColumns.length || customersDs.row_count,
        sourceType: customersDs.source_type,
        stage: 'raw',
        metadata: { datasetId: rawId, datasetName: customersDs.name },
      },
      position: { x: 0, y: row * ROW_HEIGHT },
    })

    const normalizeId = `${rawId}-normalize`
    const dedupeId = `${rawId}-dedupe`
    const castId = `${rawId}-cast`
    const selectId = `${rawId}-select`
    const outputId = `${rawId}-canonical`

    nodes.push(
      { id: normalizeId, type: 'transform', data: { label: 'Normalize', transformType: 'normalize', description: 'email→lowercase, phone→E.164' }, position: { x: COL_WIDTH, y: row * ROW_HEIGHT } },
      { id: dedupeId, type: 'transform', data: { label: 'Dedupe', transformType: 'dedupe', description: 'by customer_id, keep latest' }, position: { x: COL_WIDTH * 2, y: row * ROW_HEIGHT } },
      { id: castId, type: 'transform', data: { label: 'Cast Types', transformType: 'cast', description: 'tier→enum, created_at→timestamp' }, position: { x: COL_WIDTH * 3, y: row * ROW_HEIGHT } },
      { id: selectId, type: 'transform', data: { label: 'Select', transformType: 'select', description: 'customer_id, name, email, phone, tier' }, position: { x: COL_WIDTH * 4, y: row * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Canonical Customers', transformType: 'output' }, position: { x: COL_WIDTH * 5, y: row * ROW_HEIGHT } },
    )

    edges.push(
      createEdge(`e-${rawId}-norm`, rawId, normalizeId),
      createEdge(`e-${normalizeId}-dedupe`, normalizeId, dedupeId),
      createEdge(`e-${dedupeId}-cast`, dedupeId, castId),
      createEdge(`e-${castId}-select`, castId, selectId),
      createEdge(`e-${selectId}-out`, selectId, outputId),
    )
  }

  // ===== 3. PRODUCTS PIPELINE =====
  // Raw → Normalize → Cast → Filter → Select → Canonical Products
  if (productsDs) {
    const rawId = productsDs.dataset_id
    const row = 2
    const schemaColumns = extractSchemaColumns(productsDs.schema_json)

    nodes.push({
      id: rawId,
      type: 'datasetInput',
      data: {
        label: productsDs.name,
        columnCount: schemaColumns.length || productsDs.row_count,
        sourceType: productsDs.source_type,
        stage: 'raw',
        metadata: { datasetId: rawId, datasetName: productsDs.name },
      },
      position: { x: 0, y: row * ROW_HEIGHT },
    })

    const normalizeId = `${rawId}-normalize`
    const castId = `${rawId}-cast`
    const filterId = `${rawId}-filter`
    const selectId = `${rawId}-select`
    const outputId = `${rawId}-canonical`

    nodes.push(
      { id: normalizeId, type: 'transform', data: { label: 'Normalize', transformType: 'normalize', description: 'category→titleCase, sku→uppercase' }, position: { x: COL_WIDTH, y: row * ROW_HEIGHT } },
      { id: castId, type: 'transform', data: { label: 'Cast Types', transformType: 'cast', description: 'price→decimal(10,2), stock→integer' }, position: { x: COL_WIDTH * 2, y: row * ROW_HEIGHT } },
      { id: filterId, type: 'transform', data: { label: 'In Stock', transformType: 'filter', description: 'stock > 0' }, position: { x: COL_WIDTH * 3, y: row * ROW_HEIGHT } },
      { id: selectId, type: 'transform', data: { label: 'Select', transformType: 'select', description: 'product_id, name, category, price, stock' }, position: { x: COL_WIDTH * 4, y: row * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Canonical Products', transformType: 'output' }, position: { x: COL_WIDTH * 5, y: row * ROW_HEIGHT } },
    )

    edges.push(
      createEdge(`e-${rawId}-norm`, rawId, normalizeId),
      createEdge(`e-${normalizeId}-cast`, normalizeId, castId),
      createEdge(`e-${castId}-filter`, castId, filterId),
      createEdge(`e-${filterId}-select`, filterId, selectId),
      createEdge(`e-${selectId}-out`, selectId, outputId),
    )
  }

  // ===== 4. ORDERS PIPELINE =====
  // Raw → Filter → Cast → Compute → Select → Canonical Orders
  if (ordersDs) {
    const rawId = ordersDs.dataset_id
    const row = 3
    const schemaColumns = extractSchemaColumns(ordersDs.schema_json)

    nodes.push({
      id: rawId,
      type: 'datasetInput',
      data: {
        label: ordersDs.name,
        columnCount: schemaColumns.length || ordersDs.row_count,
        sourceType: ordersDs.source_type,
        stage: 'raw',
        metadata: { datasetId: rawId, datasetName: ordersDs.name },
      },
      position: { x: 0, y: row * ROW_HEIGHT },
    })

    const filterId = `${rawId}-filter`
    const castId = `${rawId}-cast`
    const computeId = `${rawId}-compute`
    const selectId = `${rawId}-select`
    const outputId = `${rawId}-canonical`

    nodes.push(
      { id: filterId, type: 'transform', data: { label: 'Valid Orders', transformType: 'filter', description: "status != 'Cancelled'" }, position: { x: COL_WIDTH, y: row * ROW_HEIGHT } },
      { id: castId, type: 'transform', data: { label: 'Cast Types', transformType: 'cast', description: 'order_date→date, total→decimal' }, position: { x: COL_WIDTH * 2, y: row * ROW_HEIGHT } },
      { id: computeId, type: 'transform', data: { label: 'Compute Total', transformType: 'compute', description: 'line_total = quantity × unit_price' }, position: { x: COL_WIDTH * 3, y: row * ROW_HEIGHT } },
      { id: selectId, type: 'transform', data: { label: 'Select', transformType: 'select', description: 'order_id, customer_id, product_id, total, status' }, position: { x: COL_WIDTH * 4, y: row * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Canonical Orders', transformType: 'output' }, position: { x: COL_WIDTH * 5, y: row * ROW_HEIGHT } },
    )

    edges.push(
      createEdge(`e-${rawId}-filter`, rawId, filterId),
      createEdge(`e-${filterId}-cast`, filterId, castId),
      createEdge(`e-${castId}-compute`, castId, computeId),
      createEdge(`e-${computeId}-select`, computeId, selectId),
      createEdge(`e-${selectId}-out`, selectId, outputId),
    )
  }

  // ===== 5. TRANSACTIONS PIPELINE =====
  // Raw → Filter → Normalize → Cast → Select → Canonical Transactions
  if (transactionsDs) {
    const rawId = transactionsDs.dataset_id
    const row = 4
    const schemaColumns = extractSchemaColumns(transactionsDs.schema_json)

    nodes.push({
      id: rawId,
      type: 'datasetInput',
      data: {
        label: transactionsDs.name,
        columnCount: schemaColumns.length || transactionsDs.row_count,
        sourceType: transactionsDs.source_type,
        stage: 'raw',
        metadata: { datasetId: rawId, datasetName: transactionsDs.name },
      },
      position: { x: 0, y: row * ROW_HEIGHT },
    })

    const filterId = `${rawId}-filter`
    const normalizeId = `${rawId}-normalize`
    const castId = `${rawId}-cast`
    const selectId = `${rawId}-select`
    const outputId = `${rawId}-canonical`

    nodes.push(
      { id: filterId, type: 'transform', data: { label: 'Completed Only', transformType: 'filter', description: "status = 'Completed'" }, position: { x: COL_WIDTH, y: row * ROW_HEIGHT } },
      { id: normalizeId, type: 'transform', data: { label: 'Normalize', transformType: 'normalize', description: 'currency→ISO4217, ref→uppercase' }, position: { x: COL_WIDTH * 2, y: row * ROW_HEIGHT } },
      { id: castId, type: 'transform', data: { label: 'Cast Types', transformType: 'cast', description: 'amount→decimal, timestamp→datetime' }, position: { x: COL_WIDTH * 3, y: row * ROW_HEIGHT } },
      { id: selectId, type: 'transform', data: { label: 'Select', transformType: 'select', description: 'txn_id, account_id, amount, type, timestamp' }, position: { x: COL_WIDTH * 4, y: row * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Canonical Transactions', transformType: 'output' }, position: { x: COL_WIDTH * 5, y: row * ROW_HEIGHT } },
    )

    edges.push(
      createEdge(`e-${rawId}-filter`, rawId, filterId),
      createEdge(`e-${filterId}-norm`, filterId, normalizeId),
      createEdge(`e-${normalizeId}-cast`, normalizeId, castId),
      createEdge(`e-${castId}-select`, castId, selectId),
      createEdge(`e-${selectId}-out`, selectId, outputId),
    )
  }

  // ===== 6. JOIN PIPELINE: CUSTOMER 360 =====
  // Canonical Customers + Canonical Accounts → Join → Aggregate → Customer 360
  if (customersDs && accountsDs) {
    const row = 5.5
    const joinId = 'join-customer-360'
    const aggregateId = 'agg-customer-360'
    const outputId = 'out-customer-360'

    nodes.push(
      { id: joinId, type: 'transform', data: { label: 'Join Customer+Account', transformType: 'join', description: 'LEFT JOIN on customer_id' }, position: { x: COL_WIDTH * 6, y: 0.5 * ROW_HEIGHT } },
      { id: aggregateId, type: 'transform', data: { label: 'Aggregate', transformType: 'aggregate', description: 'total_balance, account_count per customer' }, position: { x: COL_WIDTH * 7, y: 0.5 * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Customer 360', transformType: 'output' }, position: { x: COL_WIDTH * 8, y: 0.5 * ROW_HEIGHT } },
    )

    edges.push(
      createEdge('e-cust-canonical-join', `${customersDs.dataset_id}-canonical`, joinId, 'input-1'),
      createEdge('e-acct-canonical-join', `${accountsDs.dataset_id}-canonical`, joinId, 'input-2'),
      createEdge('e-join-agg', joinId, aggregateId),
      createEdge('e-agg-360', aggregateId, outputId),
    )
  }

  // ===== 7. JOIN PIPELINE: ORDER DETAILS =====
  // Canonical Orders + Canonical Products → Join → Compute → Order Details
  if (ordersDs && productsDs) {
    const joinId = 'join-order-details'
    const computeId = 'compute-order-details'
    const outputId = 'out-order-details'

    nodes.push(
      { id: joinId, type: 'transform', data: { label: 'Join Order+Product', transformType: 'join', description: 'INNER JOIN on product_id' }, position: { x: COL_WIDTH * 6, y: 2.5 * ROW_HEIGHT } },
      { id: computeId, type: 'transform', data: { label: 'Enrich', transformType: 'compute', description: 'product_name, category, subtotal' }, position: { x: COL_WIDTH * 7, y: 2.5 * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Order Details', transformType: 'output' }, position: { x: COL_WIDTH * 8, y: 2.5 * ROW_HEIGHT } },
    )

    edges.push(
      createEdge('e-order-canonical-join', `${ordersDs.dataset_id}-canonical`, joinId, 'input-1'),
      createEdge('e-prod-canonical-join', `${productsDs.dataset_id}-canonical`, joinId, 'input-2'),
      createEdge('e-join-compute', joinId, computeId),
      createEdge('e-compute-order', computeId, outputId),
    )
  }

  // ===== 8. JOIN PIPELINE: TRANSACTION LEDGER =====
  // Canonical Transactions + Canonical Accounts → Join → Aggregate → Transaction Ledger
  if (transactionsDs && accountsDs) {
    const joinId = 'join-txn-ledger'
    const aggregateId = 'agg-txn-ledger'
    const outputId = 'out-txn-ledger'

    nodes.push(
      { id: joinId, type: 'transform', data: { label: 'Join Txn+Account', transformType: 'join', description: 'INNER JOIN on account_id' }, position: { x: COL_WIDTH * 6, y: 4.5 * ROW_HEIGHT } },
      { id: aggregateId, type: 'transform', data: { label: 'Aggregate', transformType: 'aggregate', description: 'daily_volume, txn_count by account' }, position: { x: COL_WIDTH * 7, y: 4.5 * ROW_HEIGHT } },
      { id: outputId, type: 'transform', data: { label: 'Transaction Ledger', transformType: 'output' }, position: { x: COL_WIDTH * 8, y: 4.5 * ROW_HEIGHT } },
    )

    edges.push(
      createEdge('e-txn-canonical-join', `${transactionsDs.dataset_id}-canonical`, joinId, 'input-1'),
      createEdge('e-acct-canonical-join2', `${accountsDs.dataset_id}-canonical`, joinId, 'input-2'),
      createEdge('e-join-agg-txn', joinId, aggregateId),
      createEdge('e-agg-ledger', aggregateId, outputId),
    )
  }

  return { nodes, edges }
}

const mergeDatasetNodeData = (existingData: unknown, datasetData: unknown) => {
  if (!isRecord(existingData)) {
    return isRecord(datasetData) ? datasetData : existingData
  }
  if (!isRecord(datasetData)) {
    return existingData
  }
  const next = { ...existingData }
  if (typeof next.label !== 'string' || !next.label) {
    if (typeof datasetData.label === 'string' && datasetData.label) {
      next.label = datasetData.label
    }
  }
  const existingMeta = isRecord(next.metadata) ? next.metadata : null
  const datasetMeta = isRecord(datasetData.metadata) ? datasetData.metadata : null
  if (existingMeta || datasetMeta) {
    next.metadata = { ...(datasetMeta ?? {}), ...(existingMeta ?? {}) }
  }
  return next
}

const syncDatasetNodes = (currentNodes: FlowNode[], datasetNodes: FlowNode[], edges: Edge[]) => {
  if (datasetNodes.length === 0) {
    return []
  }
  if (currentNodes.length === 0) {
    return layoutFlowNodes(datasetNodes, edges)
  }

  const datasetById = new Map(datasetNodes.map((node) => [node.id, node]))
  const nextNodes: FlowNode[] = []
  const existingIds = new Set<string>()

  currentNodes.forEach((node) => {
    const datasetNode = datasetById.get(node.id)
    if (!datasetNode) {
      return
    }
    nextNodes.push({
      ...node,
      type: datasetNode.type,
      data: mergeDatasetNodeData(node.data, datasetNode.data),
      sourcePosition: datasetNode.sourcePosition,
      targetPosition: datasetNode.targetPosition,
    })
    existingIds.add(node.id)
  })

  datasetNodes.forEach((node) => {
    if (existingIds.has(node.id)) {
      return
    }
    nextNodes.push(node)
  })

  // Always apply layout to ensure proper positioning
  return layoutFlowNodes(nextNodes, edges)
}

const hydrateFlowNodesWithDatasets = (flowNodes: FlowNode[], datasets: DatasetRecord[]) => {
  if (datasets.length === 0 || flowNodes.length === 0) {
    return flowNodes
  }
  return flowNodes.map((node) => {
    if (node.type !== 'input' && node.type !== 'datasetInput') {
      return node
    }
    const dataset = resolveDatasetForNode(node, datasets)
    if (!dataset) {
      return node
    }
    const data = isRecord(node.data) ? { ...node.data } : {}
    const metadata = isRecord(data.metadata) ? { ...data.metadata } : {}
    metadata.datasetId = dataset.dataset_id
    metadata.datasetName = dataset.name
    metadata.dataset_name = dataset.name
    data.metadata = metadata
    if (!data.label || data.label === node.id) {
      data.label = dataset.name
    }
    if (typeof data.columnCount !== 'number') {
      const schemaColumns = extractSchemaColumns(dataset.schema_json)
      if (schemaColumns.length > 0) {
        data.columnCount = schemaColumns.length
      }
    }
    if (!data.sourceType) {
      data.sourceType = dataset.source_type
    }
    if (!data.stage) {
      data.stage = dataset.stage || 'raw'
    }
    return { ...node, type: 'datasetInput', data }
  })
}

const resolveDatasetForNode = (node: FlowNode, datasets: DatasetRecord[]) => {
  if (datasets.length === 0) {
    return null
  }
  const data = isRecord(node.data) ? node.data : {}
  const metadata = isRecord(data.metadata) ? data.metadata : {}
  const candidateIds: string[] = []
  const candidateNames: string[] = []
  const pushUnique = (list: string[], value: unknown) => {
    if (typeof value === 'string' && value && !list.includes(value)) {
      list.push(value)
    }
  }

  pushUnique(candidateIds, metadata.datasetId)
  pushUnique(candidateIds, metadata.dataset_id)
  pushUnique(candidateIds, node.id)
  pushUnique(candidateNames, metadata.datasetName)
  pushUnique(candidateNames, metadata.dataset_name)
  pushUnique(candidateNames, data.label)

  for (const id of candidateIds) {
    const match = datasets.find((dataset) => dataset.dataset_id === id)
    if (match) {
      return match
    }
  }
  for (const name of candidateNames) {
    const match = datasets.find((dataset) => dataset.name === name)
    if (match) {
      return match
    }
  }
  return null
}

const buildNodeLabel = (node: DefinitionNode) => {
  const metadata = node.metadata
  if (metadata && typeof metadata === 'object' && !Array.isArray(metadata)) {
    if (typeof metadata.datasetName === 'string') {
      return metadata.datasetName
    }
    if (typeof metadata.dataset_name === 'string') {
      return metadata.dataset_name
    }
    if (typeof metadata.outputName === 'string') {
      return metadata.outputName
    }
    if (typeof metadata.output_dataset_name === 'string') {
      return metadata.output_dataset_name
    }
    if (typeof metadata.name === 'string') {
      return metadata.name
    }
  }
  if (node.type) {
    return node.type
  }
  return node.id ?? 'node'
}

const buildLayoutPositions = (nodeIds: string[], edges: Edge[]) => {
  const incoming = new Map(nodeIds.map((id) => [id, 0]))
  const adjacency = new Map(nodeIds.map((id) => [id, [] as string[]]))

  edges.forEach((edge) => {
    if (!adjacency.has(edge.source)) {
      adjacency.set(edge.source, [])
    }
    adjacency.get(edge.source)?.push(edge.target)
    incoming.set(edge.target, (incoming.get(edge.target) ?? 0) + 1)
  })

  const queue = nodeIds.filter((id) => (incoming.get(id) ?? 0) === 0)
  const levels = new Map<string, number>()

  while (queue.length > 0) {
    const current = queue.shift()
    if (!current) {
      continue
    }
    const currentLevel = levels.get(current) ?? 0
    const neighbors = adjacency.get(current) ?? []
    neighbors.forEach((next) => {
      const nextLevel = Math.max(levels.get(next) ?? 0, currentLevel + 1)
      levels.set(next, nextLevel)
      incoming.set(next, (incoming.get(next) ?? 0) - 1)
      if ((incoming.get(next) ?? 0) <= 0) {
        queue.push(next)
      }
    })
  }

  const grouped = new Map<number, string[]>()
  nodeIds.forEach((id) => {
    const level = levels.get(id) ?? 0
    if (!grouped.has(level)) {
      grouped.set(level, [])
    }
    grouped.get(level)?.push(id)
  })

  const xGap = 260
  const yGap = 120
  const positions = new Map<string, { x: number; y: number }>()

  nodeIds.forEach((id) => {
    const level = levels.get(id) ?? 0
    const group = grouped.get(level) ?? []
    const index = group.indexOf(id)
    positions.set(id, { x: level * xGap, y: index * yGap })
  })

  return positions
}

const layoutFlowNodes = (nodes: FlowNode[], edges: Edge[]) => {
  if (nodes.length === 0) {
    return nodes
  }
  const nodeIds = nodes.map((node) => node.id)
  const positions = buildLayoutPositions(nodeIds, edges)
  return nodes.map((node) => ({
    ...node,
    position: positions.get(node.id) ?? node.position,
  }))
}

const buildFlowFromDefinition = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return { nodes: [] as FlowNode[], edges: [] as Edge[] }
  }
  const rawNodes = Array.isArray(definition.nodes) ? definition.nodes : []
  const rawEdges = Array.isArray(definition.edges) ? definition.edges : []

  const nodes = rawNodes
    .map((node, index): FlowNode | null => {
      if (!isRecord(node) || typeof node.id !== 'string') {
        return null
      }
      const typedNode = node as DefinitionNode
      const meta = isRecord(typedNode.metadata) ? typedNode.metadata : {}
      const nodeType = String(typedNode.type || '').trim()
      const nodeTypeLower = nodeType.toLowerCase()

      const position = { x: 0, y: index * 120 }

      if (nodeTypeLower === 'input' || nodeTypeLower === 'read_dataset') {
        return {
          id: node.id,
          type: 'datasetInput',
          position,
          data: {
            label: buildNodeLabel(typedNode),
            metadata: meta,
            stage: 'raw',
          },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        }
      }

      if (nodeTypeLower === 'output') {
        const outputName = String(meta.outputName || meta.output_name || buildNodeLabel(typedNode) || node.id)
        return {
          id: node.id,
          type: 'transform',
          position,
          data: {
            label: outputName,
            transformType: 'output',
            config: meta,
          },
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
        }
      }

      const operation = normalizeBackendOperation(meta.operation || typedNode.operation || '')
      const transformType = mapBackendOperationToTransformType(nodeTypeLower || 'transform', operation) || 'compute'
      const label = (() => {
        if (transformType === 'groupBy') {
          return 'Group By'
        }
        if (transformType === 'regexReplace') {
          return 'Regex Replace'
        }
        return transformType.charAt(0).toUpperCase() + transformType.slice(1)
      })()

      return {
        id: node.id,
        type: 'transform',
        position,
        data: {
          label,
          transformType,
          config: meta,
        },
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
      }
    })
    .filter((node): node is FlowNode => node !== null)

  const nodeIds = new Set(nodes.map((node) => node.id))
  const edges = rawEdges
    .map((edge, index) => {
      if (!isRecord(edge)) {
        return null
      }
      const typedEdge = edge as DefinitionEdge
      const source = typedEdge.from ?? typedEdge.source
      const target = typedEdge.to ?? typedEdge.target
      if (!source || !target || !nodeIds.has(source) || !nodeIds.has(target)) {
        return null
      }
      return {
        id: typedEdge.id ?? `${source}-${target}-${index}`,
        source,
        target,
      } satisfies Edge
    })
    .filter((edge): edge is Edge => Boolean(edge))

  return { nodes, edges }
}

const normalizeFolder = (record: DatabaseRecord | string): FolderOption | null => {
  if (typeof record === 'string') {
    return { id: record, name: record }
  }
  const id = record.db_name || record.name || record.id
  if (!id) {
    return null
  }
  const displayName = record.display_name || record.label || record.name || id
  return {
    id,
    name: displayName,
  }
}

const extractDefinitionNodes = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return [] as Array<Record<string, unknown>>
  }
  const nodes = (definition as Record<string, unknown>).nodes
  if (!Array.isArray(nodes)) {
    return [] as Array<Record<string, unknown>>
  }
  return nodes.filter((node): node is Record<string, unknown> => isRecord(node))
}

const extractDefinitionEdges = (definition: PipelineDetailRecord['definition_json']) => {
  if (!definition || typeof definition !== 'object') {
    return [] as Array<Record<string, unknown>>
  }
  const edges = (definition as Record<string, unknown>).edges
  if (!Array.isArray(edges)) {
    return [] as Array<Record<string, unknown>>
  }
  return edges.filter((edge): edge is Record<string, unknown> => isRecord(edge))
}

const getDefinitionEdgeKey = (from: string, to: string) => `${from}::${to}`

const resolveEdgeEndpoints = (edge: Record<string, unknown>) => {
  const from =
    typeof edge.from === 'string'
      ? edge.from
      : typeof edge.source === 'string'
        ? edge.source
        : ''
  const to =
    typeof edge.to === 'string'
      ? edge.to
      : typeof edge.target === 'string'
        ? edge.target
        : ''
  return { from, to }
}

const buildDefinitionDraft = (
  definition: PipelineDetailRecord['definition_json'],
  nodes: FlowNode[],
  edges: Edge[],
) => {
  const base = isRecord(definition) ? { ...definition } : {}
  const baseNodes = extractDefinitionNodes(definition)
  const baseEdges = extractDefinitionEdges(definition)
  const baseNodesById = new Map<string, Record<string, unknown>>()
  baseNodes.forEach((node) => {
    const id = typeof node.id === 'string' ? node.id : ''
    if (id) {
      baseNodesById.set(id, node)
    }
  })
  const baseEdgesByKey = new Map<string, Record<string, unknown>>()
  baseEdges.forEach((edge) => {
    const { from, to } = resolveEdgeEndpoints(edge)
    if (from && to) {
      baseEdgesByKey.set(getDefinitionEdgeKey(from, to), edge)
    }
  })

  const nextNodes = nodes.map((node) => {
    const existing = baseNodesById.get(node.id)
    const data = isRecord(node.data) ? node.data : {}

    const uiType = String(node.type || '').trim()
    const uiTypeLower = uiType.toLowerCase()

    let backendType = 'transform'
    let metadata: Record<string, unknown> | null = null

    if (uiType === 'datasetInput' || uiTypeLower === 'input') {
      backendType = 'input'
      metadata = isRecord(data.metadata) ? { ...data.metadata } : {}
    } else if (uiType === 'transform') {
      const transformType = typeof data.transformType === 'string' ? data.transformType : ''
      backendType = transformType === 'output' ? 'output' : 'transform'
      const metaCandidate = isRecord(data.config) ? data.config : isRecord(data.metadata) ? data.metadata : {}
      metadata = { ...metaCandidate }

      // Ensure backend gets a canonical operation string when required.
      const opCandidate = (metadata as Record<string, unknown>).operation ?? transformType
      const normalizedOperation = normalizeBackendOperation(opCandidate)
      if (normalizedOperation) {
        ;(metadata as Record<string, unknown>).operation = normalizedOperation
      }
    } else if (uiTypeLower === 'output') {
      backendType = 'output'
      metadata = isRecord(data.metadata) ? { ...data.metadata } : {}
    } else {
      backendType = 'transform'
      metadata = isRecord(data.metadata) ? { ...data.metadata } : isRecord(data.config) ? { ...data.config } : {}
      const normalizedOperation = normalizeBackendOperation((metadata as Record<string, unknown>).operation ?? uiType)
      if (normalizedOperation) {
        ;(metadata as Record<string, unknown>).operation = normalizedOperation
      }
    }

    const metaPayload = metadata ?? {}
    const hasMetadata = Object.keys(metaPayload).length > 0

    if (existing) {
      const merged: Record<string, unknown> = { ...existing }
      merged.type = backendType
      if (hasMetadata) {
        const existingMeta = isRecord(existing.metadata) ? existing.metadata : {}
        merged.metadata = { ...existingMeta, ...metaPayload }
      }
      return merged
    }

    const fresh: Record<string, unknown> = { id: node.id, type: backendType }
    if (hasMetadata) {
      fresh.metadata = metaPayload
    }
    return fresh
  })

  const nextEdges = edges.map((edge) => {
    const key = getDefinitionEdgeKey(edge.source, edge.target)
    const existing = baseEdgesByKey.get(key)
    if (existing) {
      return existing
    }
    return { from: edge.source, to: edge.target }
  })

  return { ...base, nodes: nextNodes, edges: nextEdges }
}

const extractOutputNodeIds = (definition: PipelineDetailRecord['definition_json']) => {
  const nodes = extractDefinitionNodes(definition)
  return nodes
    .filter((node) => String(node.type || '').toLowerCase() === 'output')
    .map((node) => String(node.id || '').trim())
    .filter((id) => id)
}

export const GraphPage = () => {
  const queryClient = useQueryClient()
  const pipelineContext = useAppStore((state) => state.pipelineContext)
  const setPipelineContext = useAppStore((state) => state.setPipelineContext)
  const pipelineAgentRun = useAppStore((state) => state.pipelineAgentRun)
  const [activeTab, setActiveTab] = useState<'edit' | 'proposals' | 'history'>('edit')
  const [toolMode, setToolMode] = useState<ToolMode>('pointer')
  const [isRightPanelOpen, setRightPanelOpen] = useState(false)
  const [isBottomPanelOpen, setBottomPanelOpen] = useState(false)
  const [previewDatasetId, setPreviewDatasetId] = useState<string>('')
  const [transformPreviewData, setTransformPreviewData] = useState<TransformPreviewResponse | null>(null)
  const [transformPreviewNodeId, setTransformPreviewNodeId] = useState<string>('')
  const [columnSearch, setColumnSearch] = useState('')
  const [previewColumns, setPreviewColumns] = useState<PreviewColumn[]>([])
  const [previewRows, setPreviewRows] = useState<PreviewRow[]>([])
  const [activeColumn, setActiveColumn] = useState<string>('')
  const [pipelinePreviewOverride, setPipelinePreviewOverride] = useState<Record<string, unknown> | null>(null)
  const [pipelinePreviewLabel, setPipelinePreviewLabel] = useState<string>('')
  const [draggedColumnKey, setDraggedColumnKey] = useState<string | null>(null)
  const [dragOverColumnKey, setDragOverColumnKey] = useState<string | null>(null)
  const [activeOutputTab, setActiveOutputTab] = useState<'datasets' | 'objectTypes' | 'linkTypes'>('datasets')
  const [previewSource, setPreviewSource] = useState<'dataset' | 'pipeline' | 'transform'>('dataset')
  const [reactFlowInstance, setReactFlowInstance] = useState<ReactFlowInstance | null>(null)
  const [isNLProcessing, setIsNLProcessing] = useState(false)
  const [isAgentOpen, setIsAgentOpen] = useState(false)
  const [agentMessages, setAgentMessages] = useState<Array<{ id: string; role: 'user' | 'assistant'; text: string }>>([])
  const [agentInput, setAgentInput] = useState('')
  // Agent가 생성한 노드별 preview 데이터 저장
  const [agentNodePreviews, setAgentNodePreviews] = useState<
    Map<
      string,
      {
        columns: Array<{ name: string; type?: string }>
        rows: Array<Record<string, unknown>>
        previewStatus?: string
        previewPolicy?: Record<string, unknown> | null
        hint?: string
      }
    >
  >(new Map())
  const [isWizardOpen, setIsWizardOpen] = useState(false)
  const [isUdfDialogOpen, setIsUdfDialogOpen] = useState(false)
  const [isDatasetSelectOpen, setIsDatasetSelectOpen] = useState(false)
  const [isFileUploadOpen, setIsFileUploadOpen] = useState(false)
  const [selectedBranch, setSelectedBranch] = useState<string>('dev')
  const [isCreatePipelineDialogOpen, setCreatePipelineDialogOpen] = useState(false)
  const [createPipelineDefaults, setCreatePipelineDefaults] = useState<{ name: string; branch: string }>({
    name: '',
    branch: 'dev',
  })
  const [isCreatingPipeline, setIsCreatingPipeline] = useState(false)
  const [createPipelineError, setCreatePipelineError] = useState<string | null>(null)
  const [nodes, setNodes, onNodesChange] = useNodesState<FlowNode[]>([])
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge[]>([])
  const nodesRef = useRef<FlowNode[]>([])
  const edgesRef = useRef<Edge[]>([])
  const [definitionDirty, setDefinitionDirty] = useState(false)
  const [isSaving, setIsSaving] = useState(false)
  const [isProposing, setIsProposing] = useState(false)
  const [isDeploying, setIsDeploying] = useState(false)
  const [isBuilding, setIsBuilding] = useState(false)
  const [isCheckingReadiness, setIsCheckingReadiness] = useState(false)
  const lastAppliedAgentRunId = useRef<string | null>(null)
  const agentAbortRef = useRef<(() => void) | null>(null)

  useEffect(() => {
    nodesRef.current = nodes
  }, [nodes])

  useEffect(() => {
    edgesRef.current = edges
  }, [edges])

  useEffect(() => {
    return () => {
      agentAbortRef.current?.()
    }
  }, [])

  const activeDbName = pipelineContext?.folderId ?? ''
  const { data: databases = [] } = useQuery({
    queryKey: ['databases'],
    queryFn: listDatabases,
  })
  const { data: datasets = [] } = useQuery({
    queryKey: ['datasets', activeDbName],
    queryFn: () => listDatasets(activeDbName),
    enabled: Boolean(activeDbName),
  })
  const { data: pipelines = [] } = useQuery({
    queryKey: ['pipelines', activeDbName],
    queryFn: () => listPipelines(activeDbName),
    enabled: Boolean(activeDbName),
  })
  const branchOptions = useMemo(() => {
    const branches = new Set<string>()
    pipelines.forEach((pipeline) => {
      const branch = String(pipeline.branch || 'main').trim() || 'main'
      branches.add(branch)
    })
    branches.add('main')
    // Encourage a working branch even when no pipelines exist yet.
    if (!branches.has('dev')) {
      branches.add('dev')
    }
    const list = Array.from(branches)
    list.sort((left, right) => {
      if (left === right) return 0
      if (left === 'main') return -1
      if (right === 'main') return 1
      return left.localeCompare(right)
    })
    return list
  }, [pipelines])

  useEffect(() => {
    if (!activeDbName) {
      setSelectedBranch('dev')
      return
    }
    setSelectedBranch((current) => {
      if (current && branchOptions.includes(current)) {
        return current
      }
      if (branchOptions.includes('dev')) {
        return 'dev'
      }
      return branchOptions[0] ?? 'main'
    })
  }, [activeDbName, branchOptions])

  const pipelinesForSelectedBranch = useMemo(() => {
    if (!selectedBranch) {
      return pipelines
    }
    return pipelines.filter((pipeline) => String(pipeline.branch || 'main').trim() === selectedBranch)
  }, [pipelines, selectedBranch])

  const primaryPipeline = useMemo<PipelineRecord | null>(() => {
    if (pipelinesForSelectedBranch.length === 0) {
      return null
    }
    const sorted = [...pipelinesForSelectedBranch].sort(
      (left, right) => parseTimestamp(right.updated_at) - parseTimestamp(left.updated_at),
    )
    return sorted[0] ?? null
  }, [pipelinesForSelectedBranch])
  const { data: pipelineDetail = null } = useQuery({
    queryKey: ['pipeline-detail', primaryPipeline?.pipeline_id],
    queryFn: () => getPipeline(primaryPipeline?.pipeline_id ?? '', { dbName: activeDbName }),
    enabled: Boolean(primaryPipeline?.pipeline_id),
    refetchInterval: definitionDirty ? false : 2000,
  })
  const { data: pipelineArtifacts = [] } = useQuery({
    queryKey: ['pipeline-artifacts', primaryPipeline?.pipeline_id, 'build'],
    queryFn: () =>
      listPipelineArtifacts(primaryPipeline?.pipeline_id ?? '', { mode: 'build', limit: 50, dbName: activeDbName }),
    enabled: Boolean(primaryPipeline?.pipeline_id),
    // Build/Deploy 중이거나 실행 중인 Job이 있으면 2초 간격 polling
    refetchInterval: (query) => {
      if (isBuilding || isDeploying) return 2000
      // 실행 중인 artifact가 있으면 계속 polling
      const artifacts = query.state.data ?? []
      const hasRunningJob = artifacts.some(
        (a) => ['RUNNING', 'PENDING'].includes(String(a.status || '').toUpperCase())
      )
      return hasRunningJob ? 2000 : false
    },
  })
  const pipelineFolderName = pipelineContext?.folderName || primaryPipeline?.db_name || pipelineDetail?.db_name
  const pipelineFolderLabel = pipelineFolderName || 'No project selected'
  const pipelineDisplayName = primaryPipeline?.name || 'Select a pipeline'
  const pipelineContextName = primaryPipeline?.name || pipelineDetail?.name || ''
  const folderOptions = useMemo(
    () => {
      const options = (databases ?? []).map(normalizeFolder).filter(Boolean) as FolderOption[]
      return options.sort((left, right) => left.name.localeCompare(right.name))
    },
    [databases],
  )
  const pipelineType = (primaryPipeline?.pipeline_type || 'batch').toLowerCase()
  const pipelineTypeLabel = pipelineType.includes('stream') ? 'Streaming' : 'Batch'
  const pipelineBranchName = selectedBranch || pipelineDetail?.branch || primaryPipeline?.branch || 'main'
  const pipelineBranchLabel = pipelineBranchName === 'main' ? 'Main' : pipelineBranchName
  const isStreamingPipeline = pipelineType.includes('stream')
  const latestBuildArtifact = useMemo<PipelineArtifactRecord | null>(() => {
    const successful = pipelineArtifacts.filter(
      (artifact) => String(artifact.status || '').toUpperCase() === 'SUCCESS',
    )
    if (successful.length === 0) {
      return null
    }
    const sorted = [...successful].sort(
      (left, right) => parseTimestamp(right.created_at) - parseTimestamp(left.created_at),
    )
    return sorted[0] ?? null
  }, [pipelineArtifacts])

  // 실행 중인 Job 상태 추적
  const runningArtifact = useMemo<PipelineArtifactRecord | null>(() => {
    const running = pipelineArtifacts.filter(
      (artifact) => ['RUNNING', 'PENDING'].includes(String(artifact.status || '').toUpperCase()),
    )
    if (running.length === 0) {
      return null
    }
    const sorted = [...running].sort(
      (left, right) => parseTimestamp(right.created_at) - parseTimestamp(left.created_at),
    )
    return sorted[0] ?? null
  }, [pipelineArtifacts])

  // 최근 실패한 Job
  const latestFailedArtifact = useMemo<PipelineArtifactRecord | null>(() => {
    const failed = pipelineArtifacts.filter(
      (artifact) => String(artifact.status || '').toUpperCase() === 'FAILED',
    )
    if (failed.length === 0) {
      return null
    }
    const sorted = [...failed].sort(
      (left, right) => parseTimestamp(right.created_at) - parseTimestamp(left.created_at),
    )
    // 최근 성공보다 더 최근인 경우에만 표시
    if (latestBuildArtifact) {
      const failedTime = parseTimestamp(sorted[0]?.created_at)
      const successTime = parseTimestamp(latestBuildArtifact.created_at)
      if (failedTime < successTime) {
        return null
      }
    }
    return sorted[0] ?? null
  }, [pipelineArtifacts, latestBuildArtifact])
  const outputNodeIds = useMemo(
    () => extractOutputNodeIds(pipelineDetail?.definition_json),
    [pipelineDetail?.definition_json],
  )
  const defaultOutputNodeId = outputNodeIds[0] ?? null
  const canSave = Boolean(primaryPipeline && pipelineDetail && definitionDirty) && !isSaving
  const canPropose = Boolean(primaryPipeline) && !isProposing
  const canBuild =
    Boolean(primaryPipeline) &&
    Boolean(activeDbName) &&
    !definitionDirty &&
    (isStreamingPipeline || Boolean(defaultOutputNodeId)) &&
    !isBuilding
  const canDeploy =
    Boolean(primaryPipeline && latestBuildArtifact) &&
    Boolean(activeDbName) &&
    !definitionDirty &&
    (isStreamingPipeline || Boolean(defaultOutputNodeId)) &&
    !isDeploying
  const pipelineCount = primaryPipeline ? nodes.length : pipelines.length
  const previewDataset = useMemo(() => {
    if (previewDatasetId) {
      const selected = datasets.find((dataset) => dataset.dataset_id === previewDatasetId)
      if (selected) {
        return selected
      }
    }
    if (datasets.length === 0) {
      return null
    }
    const withSample = datasets.find((dataset) => extractSampleRows(dataset.sample_json ?? {}).length > 0)
    if (withSample) {
      return withSample
    }
    const withSchema = datasets.find((dataset) => extractSchemaColumns(dataset.schema_json).length > 0)
    return withSchema ?? datasets[0]
  }, [datasets, previewDatasetId])
  const previewDatasetName = previewDataset?.name ?? 'No datasets available'
  const agentRunDbName = useMemo(() => {
    if (!isRecord(pipelineAgentRun)) {
      return ''
    }
    const plan = isRecord(pipelineAgentRun.plan) ? (pipelineAgentRun.plan as Record<string, unknown>) : null
    const planScope = plan && isRecord(plan.data_scope) ? (plan.data_scope as Record<string, unknown>) : null
    const planDbName = planScope && typeof planScope.db_name === 'string' ? planScope.db_name : ''
    if (planDbName) {
      return planDbName
    }
    const scope = isRecord(pipelineAgentRun.data_scope) ? (pipelineAgentRun.data_scope as Record<string, unknown>) : null
    return scope && typeof scope.db_name === 'string' ? scope.db_name : ''
  }, [pipelineAgentRun])
  const agentRunMarker = useMemo(() => {
    if (!isRecord(pipelineAgentRun)) {
      return ''
    }
    const runId = typeof pipelineAgentRun.run_id === 'string' ? pipelineAgentRun.run_id : ''
    const planId = typeof pipelineAgentRun.plan_id === 'string' ? pipelineAgentRun.plan_id : ''
    return runId || planId
  }, [pipelineAgentRun])
  const pipelineAgentPlanId = useMemo(() => {
    if (!isRecord(pipelineAgentRun)) {
      return ''
    }
    return typeof pipelineAgentRun.plan_id === 'string' ? pipelineAgentRun.plan_id : ''
  }, [pipelineAgentRun])
  const pipelinePreviewPayload = useMemo(() => {
    const override = pipelinePreviewOverride
    const previewSourcePayload = override
      ? override
      : isRecord(pipelineAgentRun) && isRecord(pipelineAgentRun.preview)
        ? (pipelineAgentRun.preview as Record<string, unknown>)
        : null
    if (!isRecord(previewSourcePayload)) {
      return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
    }
    if (agentRunDbName && agentRunDbName !== activeDbName) {
      return { columns: [] as PreviewColumn[], rows: [] as PreviewRow[] }
    }
    return buildPreviewFromPipeline(previewSourcePayload as Record<string, unknown>)
  }, [pipelinePreviewOverride, pipelineAgentRun, agentRunDbName, activeDbName])
  const hasPipelinePreview = pipelinePreviewPayload.columns.length > 0
  const transformPreviewPayload = useMemo(
    () => buildPreviewFromTransform(transformPreviewData),
    [transformPreviewData],
  )
  const previewPayload = useMemo(() => {
    if (previewSource === 'transform' && transformPreviewData) {
      return transformPreviewPayload
    }
    if (previewSource === 'pipeline' && hasPipelinePreview) {
      return pipelinePreviewPayload
    }
    return buildPreviewFromDataset(previewDataset)
  }, [previewSource, transformPreviewData, transformPreviewPayload, hasPipelinePreview, pipelinePreviewPayload, previewDataset])
  const transformNodeLabel = useMemo(() => {
    if (!transformPreviewNodeId) return ''
    const node = nodes.find(n => n.id === transformPreviewNodeId)
    if (node && isRecord(node.data) && typeof node.data.label === 'string') {
      return node.data.label
    }
    return transformPreviewNodeId
  }, [transformPreviewNodeId, nodes])
  const previewLabel =
    previewSource === 'transform' && transformPreviewData
      ? `Transform: ${transformNodeLabel}`
      : previewSource === 'pipeline' && hasPipelinePreview
        ? pipelinePreviewLabel || 'Pipeline preview'
        : previewDatasetName
  const transformNodes = useMemo(() => {
    return nodes.filter((node) => node.type === 'transform')
  }, [nodes])

  const pipelinePreviewNotice = useMemo(() => {
    if (previewSource !== 'pipeline') {
      return null
    }
    if (agentRunDbName && agentRunDbName !== activeDbName && !pipelinePreviewOverride) {
      return null
    }
    const override = pipelinePreviewOverride
    const payload =
      override && isRecord(override)
        ? override
        : isRecord(pipelineAgentRun) && isRecord(pipelineAgentRun.preview)
          ? (pipelineAgentRun.preview as Record<string, unknown>)
          : null
    if (!isRecord(payload)) {
      return null
    }
    const statusRaw =
      (payload.__preview_status as string | undefined) ||
      (payload.preview_status as string | undefined) ||
      ''
    const status = String(statusRaw || '').toLowerCase()
    const hintRaw =
      (payload.__hint as string | undefined) ||
      (payload.hint as string | undefined) ||
      ''
    const hint = String(hintRaw || '').trim()
    const policyRaw =
      (payload.__preview_policy as Record<string, unknown> | undefined) ||
      (payload.preview_policy as Record<string, unknown> | undefined)
    const policy = policyRaw && isRecord(policyRaw) ? policyRaw : null
    const policyLevel = policy ? String(policy.level ?? '').toLowerCase() : ''
    const issues = policy && Array.isArray(policy.issues) ? (policy.issues as Array<Record<string, unknown>>) : []
    const issueMessages = issues
      .map((issue) => String(issue.message || '').trim())
      .filter(Boolean)
      .slice(0, 3)

    if (status === 'requires_spark_preview' || policyLevel === 'require_spark') {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then click Build (Spark) to verify.'
          : 'Click Build (Spark) to verify.'
        : 'Save the pipeline first, then click Build (Spark) to verify.'
      return {
        level: 'require_spark',
        title: 'Spark preview required',
        message: `${hint || 'This pipeline preview requires Spark execution for accurate results.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    if (status === 'preview_denied' || policyLevel === 'deny') {
      return {
        level: 'deny',
        title: 'Preview denied by policy',
        message: hint || 'This pipeline contains operations not supported for production execution. Fix the plan first.',
        issues: issueMessages,
      }
    }
    if (status === 'preview_failed') {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then use Build (Spark) to validate.'
          : 'Consider using Build (Spark) for validation.'
        : 'Save the pipeline first, then use Build (Spark) for validation.'
      return {
        level: 'warn',
        title: 'Preview failed',
        message: `${hint || 'Pipeline preview failed.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    if (policyLevel === 'warn' && (issueMessages.length > 0 || hint)) {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then validate with Build (Spark) when ready.'
          : 'Validate with Build (Spark) when ready.'
        : 'Save the pipeline first, then validate with Build (Spark) when ready.'
      return {
        level: 'warn',
        title: 'Best-effort preview',
        message: `${hint || 'Preview may diverge from full Spark execution.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    return null
  }, [previewSource, pipelinePreviewOverride, pipelineAgentRun, agentRunDbName, activeDbName, primaryPipeline, definitionDirty])

  const transformPreviewNotice = useMemo(() => {
    if (previewSource !== 'transform' || !transformPreviewNodeId) {
      return null
    }
    const agentPreview = agentNodePreviews.get(transformPreviewNodeId)
    if (!agentPreview) {
      return null
    }
    const status = (agentPreview.previewStatus || '').toLowerCase()
    const policy = agentPreview.previewPolicy && isRecord(agentPreview.previewPolicy) ? agentPreview.previewPolicy : null
    const policyLevel = policy ? String(policy.level ?? '').toLowerCase() : ''
    const issues = policy && Array.isArray(policy.issues) ? (policy.issues as Array<Record<string, unknown>>) : []

    const issueMessages = issues
      .map((issue) => String(issue.message || '').trim())
      .filter(Boolean)
      .slice(0, 3)

    if (status === 'requires_spark_preview' || policyLevel === 'require_spark') {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then click Build (Spark) to verify.'
          : 'Click Build (Spark) to verify.'
        : 'Save the pipeline first, then click Build (Spark) to verify.'
      return {
        level: 'require_spark',
        title: 'Spark preview required',
        message:
          `${agentPreview.hint || 'This transform uses Spark semantics that plan_preview cannot validate safely.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    if (status === 'preview_denied' || policyLevel === 'deny') {
      return {
        level: 'deny',
        title: 'Preview denied by policy',
        message:
          agentPreview.hint ||
          'This transform contains operations that are not supported for production execution. Fix the plan first.',
        issues: issueMessages,
      }
    }
    if (status === 'preview_failed') {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then use Build (Spark) for validation.'
          : 'Consider using Build (Spark) for validation.'
        : 'Save the pipeline first, then use Build (Spark) for validation.'
      return {
        level: 'warn',
        title: 'Preview failed',
        message: `${agentPreview.hint || 'Plan preview failed.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    if (policyLevel === 'warn' && (issueMessages.length > 0 || agentPreview.hint)) {
      const actionHint = primaryPipeline
        ? definitionDirty
          ? 'Save the pipeline first, then validate with Build (Spark) when ready.'
          : 'Validate with Build (Spark) when ready.'
        : 'Save the pipeline first, then validate with Build (Spark) when ready.'
      return {
        level: 'warn',
        title: 'Best-effort preview',
        message: `${agentPreview.hint || 'Preview may diverge from full Spark execution.'} ${actionHint}`.trim(),
        issues: issueMessages,
      }
    }
    return null
  }, [previewSource, transformPreviewNodeId, agentNodePreviews, primaryPipeline, definitionDirty])

  const previewMenu = useMemo(() => {
    const hasDatasets = datasets.length > 0
    const hasTransforms = transformNodes.length > 0
    const currentIcon = previewSource === 'transform' ? 'function' : previewSource === 'pipeline' ? 'flow-branch' : 'database'
    return (
      <Menu>
        <MenuItem icon={currentIcon} text={`Current: ${previewLabel}`} disabled />
        <MenuDivider />
        {hasPipelinePreview ? (
          <MenuItem
            icon={previewSource === 'pipeline' ? 'small-tick' : 'flow-branch'}
            text="Pipeline preview"
            disabled={previewSource === 'pipeline'}
            onClick={() => setPreviewSource('pipeline')}
          />
        ) : null}
        {hasDatasets && (
          <>
            <MenuDivider title="Datasets" />
            {datasets.map((dataset) => {
              const isActive = previewSource === 'dataset' && dataset.dataset_id === previewDataset?.dataset_id
              return (
                <MenuItem
                  key={dataset.dataset_id}
                  icon={isActive ? 'small-tick' : 'database'}
                  text={dataset.name}
                  disabled={isActive}
                  onClick={() => {
                    setPreviewDatasetId(dataset.dataset_id)
                    setTransformPreviewNodeId('')
                    setTransformPreviewData(null)
                    setPreviewSource('dataset')
                    setColumnSearch('')
                  }}
                />
              )
            })}
          </>
        )}
        {hasTransforms && (
          <>
            <MenuDivider title="Transforms" />
            {transformNodes.map((node) => {
              const nodeLabel = isRecord(node.data) && typeof node.data.label === 'string' ? node.data.label : node.id
              const isActive = previewSource === 'transform' && transformPreviewNodeId === node.id
              return (
                <MenuItem
                  key={node.id}
                  icon={isActive ? 'small-tick' : 'function'}
                  text={nodeLabel}
                  disabled={isActive}
                  onClick={async () => {
                    setPreviewDatasetId('')
                    setTransformPreviewNodeId(node.id)
                    setPreviewSource('transform')
                    setColumnSearch('')
                    const preview = await getTransformPreview(activeDbName, node.id)
                    if (preview) {
                      setTransformPreviewData(preview)
                    }
                  }}
                />
              )
            })}
          </>
        )}
        {!hasDatasets && !hasTransforms && (
          <MenuItem text="No data available" disabled />
        )}
      </Menu>
    )
  }, [
    datasets,
    previewDataset,
    previewLabel,
    hasPipelinePreview,
    previewSource,
    setPreviewDatasetId,
    setColumnSearch,
    transformNodes,
    transformPreviewNodeId,
    activeDbName,
  ])
  const datasetOutputs = useMemo(() => {
    // Raw 데이터셋만 표시 (stage가 없거나 'raw'인 경우)
    return datasets
      .filter(d => !d.stage || d.stage === 'raw')
      .map((dataset) => {
        const sampleRows = extractSampleRows(dataset.sample_json ?? {})
        const schemaColumns = extractSchemaColumns(dataset.schema_json)
        const sampleColumns = extractSampleColumns(dataset.sample_json ?? {}, sampleRows)
        const schemaNames = new Set(schemaColumns.map((column) => column.name))
        const sampleNames = sampleColumns.map((column) => column.name)
        const extraColumns = schemaNames.size > 0 ? sampleNames.filter((name) => !schemaNames.has(name)) : []
        const columnCount = schemaColumns.length || sampleColumns.length
        const rowCount = dataset.row_count ?? sampleRows.length
        return {
          dataset,
          columnCount,
          rowCount,
          extraColumnsCount: extraColumns.length,
        }
      })
  }, [datasets])

  // Canonical (Output) 노드 추출 - Objectify 직전 데이터셋
  const canonicalOutputs = useMemo(() => {
    const emptyData: Record<string, unknown> = {}
    return nodes
      .filter(node => {
        if (node.type !== 'transform') return false
        const data = isRecord(node.data) ? node.data : emptyData
        return data['transformType'] === 'output'
      })
      .map(node => {
        const data = isRecord(node.data) ? node.data : emptyData
        const label = typeof data['label'] === 'string' ? data['label'] : node.id
        return {
          nodeId: node.id,
          label,
          description: typeof data['description'] === 'string' ? data['description'] : '',
        }
      })
  }, [nodes])
  const pipelineOutputs = useMemo(() => {
    const grouped = {
      object: [] as Array<Record<string, unknown>>,
      link: [] as Array<Record<string, unknown>>,
      unknown: [] as Array<Record<string, unknown>>,
    }
    if (!isRecord(pipelineAgentRun) || !isRecord(pipelineAgentRun.plan)) {
      return grouped
    }
    if (agentRunDbName && agentRunDbName !== activeDbName) {
      return grouped
    }
    const plan = pipelineAgentRun.plan as Record<string, unknown>
    const outputs = Array.isArray(plan.outputs) ? plan.outputs : []
    outputs.forEach((output) => {
      if (!isRecord(output)) {
        return
      }
      const kind = String(output.output_kind || 'unknown').toLowerCase()
      if (kind === 'object') {
        grouped.object.push(output)
      } else if (kind === 'link') {
        grouped.link.push(output)
      } else {
        grouped.unknown.push(output)
      }
    })
    return grouped
  }, [pipelineAgentRun, agentRunDbName, activeDbName])
  const { nodes: datasetNodes, edges: datasetEdges } = useMemo(
    () => buildDatasetInputNodes(datasets),
    [datasets],
  )
  const aiAgentNodes = useMemo(
    () =>
      nodes.map((node) => {
        const data = isRecord(node.data) ? node.data : null
        const label = data && typeof data['label'] === 'string' && data['label'] ? data['label'] : node.id
        return { id: node.id, label, type: node.type }
      }),
    [nodes],
  )
  const agentPlanDefinition = useMemo(() => {
    if (!isRecord(pipelineAgentRun) || !isRecord(pipelineAgentRun.plan)) {
      return null
    }
    const plan = pipelineAgentRun.plan as Record<string, unknown>
    return isRecord(plan.definition_json) ? (plan.definition_json as Record<string, unknown>) : null
  }, [pipelineAgentRun])
  const outputNodeIdByName = useMemo(() => {
    const definition = agentPlanDefinition ?? pipelineDetail?.definition_json
    const nodes = extractDefinitionNodes(definition)
    const map = new Map<string, string>()
    nodes.forEach((node) => {
      const nodeType = safeText(node.type).trim().toLowerCase()
      if (nodeType !== 'output') {
        return
      }
      const nodeId = safeText(node.id).trim()
      if (!nodeId) {
        return
      }
      const metadata = isRecord(node.metadata) ? node.metadata : {}
      const candidates = [
        metadata.outputName,
        metadata.output_dataset_name,
        metadata.datasetName,
        metadata.dataset_name,
        metadata.name,
        nodeId,
      ]
      candidates.forEach((candidate) => {
        const key = normalizeOutputKey(candidate)
        if (key) {
          map.set(key, nodeId)
        }
      })
    })
    return map
  }, [agentPlanDefinition, pipelineDetail?.definition_json])
  const resolveOutputNodeId = useCallback(
    (outputName: string) => {
      const key = normalizeOutputKey(outputName)
      return key ? outputNodeIdByName.get(key) ?? '' : ''
    },
    [outputNodeIdByName],
  )
  const handlePreviewOutput = useCallback(
    async (outputName: string) => {
      if (!pipelineAgentPlanId) {
        return
      }
      if (agentRunDbName && agentRunDbName !== activeDbName) {
        return
      }
      const nodeId = resolveOutputNodeId(outputName)
      try {
        const response = await previewPipelinePlan(pipelineAgentPlanId, {
          node_id: nodeId || undefined,
          limit: 200,
        })
        const preview = isRecord(response.preview) ? (response.preview as Record<string, unknown>) : null
        const previewStatus = typeof response.preview_status === 'string' ? response.preview_status : ''
        const previewPolicy = isRecord(response.preview_policy) ? (response.preview_policy as Record<string, unknown>) : null
        const hint = typeof response.hint === 'string' ? response.hint : ''
        if (preview) {
          setPipelinePreviewOverride({
            ...preview,
            __preview_status: previewStatus,
            __preview_policy: previewPolicy ?? undefined,
            __hint: hint,
          })
          setPipelinePreviewLabel(outputName)
          setPreviewSource('pipeline')
          setBottomPanelOpen(true)
        }
      } catch {
        return
      }
    },
    [
      pipelineAgentPlanId,
      agentRunDbName,
      activeDbName,
      resolveOutputNodeId,
      setBottomPanelOpen,
      setPreviewSource,
    ],
  )

  useEffect(() => {
    setPreviewDatasetId('')
  }, [activeDbName])

  useEffect(() => {
    if (hasPipelinePreview && previewSource !== 'pipeline') {
      setPreviewSource('pipeline')
    }
  }, [hasPipelinePreview, previewSource])

  useEffect(() => {
    setPipelinePreviewOverride(null)
    setPipelinePreviewLabel('')
  }, [agentRunMarker, activeDbName])

  useEffect(() => {
    if (previewDatasetId && !datasets.some((dataset) => dataset.dataset_id === previewDatasetId)) {
      setPreviewDatasetId('')
    }
  }, [datasets, previewDatasetId])

  const isPanMode = toolMode === 'pan'
  const isPointerMode = toolMode === 'pointer'
  const isSelectMode = toolMode === 'select'
  const isRemoveMode = toolMode === 'remove'
  const isSelectableMode = isPointerMode || isSelectMode
  const isViewportReady = Boolean(reactFlowInstance)
  const filteredColumns = useMemo(() => {
    const query = columnSearch.trim().toLowerCase()
    if (!query) {
      return previewColumns
    }
    return previewColumns.filter((column) => column.label.toLowerCase().includes(query))
  }, [columnSearch, previewColumns])
  const tableGridTemplate = useMemo(() => {
    return ['48px', ...previewColumns.map((column) => `minmax(${column.width}px, 1fr)`)].join(' ')
  }, [previewColumns])
  const tableMinWidth = useMemo(() => {
    const baseWidth = previewColumns.reduce((sum, column) => sum + column.width, 48)
    const gaps = previewColumns.length * 8
    return baseWidth + gaps
  }, [previewColumns])

  const canvasClassName = useMemo(() => {
    if (isPanMode) {
      return 'pipeline-reactflow is-pan'
    }
    if (isSelectMode) {
      return 'pipeline-reactflow is-select'
    }
    if (isRemoveMode) {
      return 'pipeline-reactflow is-remove'
    }
    return 'pipeline-reactflow'
  }, [isPanMode, isSelectMode, isRemoveMode])

  const handleConnect = useCallback((connection: Connection) => {
    setEdges((current) => addEdge({ ...connection, type: 'straight', style: edgeStyle }, current))
    setDefinitionDirty(true)
  }, [setEdges])

  const handleNodesChange = useCallback((changes: NodeChange[]) => {
    onNodesChange(changes)
    if (changes.some((change) => change.type === 'remove')) {
      setDefinitionDirty(true)
    }
  }, [onNodesChange])

  const handleEdgesChange = useCallback((changes: EdgeChange[]) => {
    onEdgesChange(changes)
    if (changes.some((change) => change.type === 'remove' || change.type === 'add')) {
      setDefinitionDirty(true)
    }
  }, [onEdgesChange])

  const handleNodeClick = useCallback(async (_: unknown, node: FlowNode) => {
    if (!isRemoveMode) {
      // Transform 노드인 경우
      if (node.type === 'transform') {
        setPreviewDatasetId('')
        setTransformPreviewNodeId(node.id)
        setColumnSearch('')
        setPreviewSource('transform')
        setBottomPanelOpen(true)

        // Agent가 생성한 노드인 경우 저장된 프리뷰 데이터 사용
        const agentPreview = agentNodePreviews.get(node.id)
        if (agentPreview) {
          setTransformPreviewData({
            node_id: node.id,
            schema_json: { columns: agentPreview.columns },
            sample_json: { rows: agentPreview.rows },
            row_count: agentPreview.rows.length,
          })
          return
        }

        // 기존 노드인 경우 API로 프리뷰 데이터 가져오기
        const preview = await getTransformPreview(activeDbName, node.id)
        if (preview) {
          setTransformPreviewData(preview)
        }
        return
      }
      // Dataset 노드인 경우
      const dataset = resolveDatasetForNode(node, datasets)
      if (dataset) {
        setPreviewDatasetId(dataset.dataset_id)
        setTransformPreviewNodeId('')
        setTransformPreviewData(null)
        setColumnSearch('')
        setPreviewSource('dataset')
      }
      setBottomPanelOpen(true)
      return
    }
    setNodes((current) => current.filter((item) => item.id !== node.id))
    setEdges((current) => current.filter((item) => item.source !== node.id && item.target !== node.id))
    setDefinitionDirty(true)
  }, [isRemoveMode, datasets, activeDbName, agentNodePreviews, setPreviewDatasetId, setBottomPanelOpen, setColumnSearch, setNodes, setEdges])

  const handleEdgeClick = useCallback((_: unknown, edge: Edge) => {
    if (!isRemoveMode) {
      return
    }
    setEdges((current) => current.filter((item) => item.id !== edge.id))
    setDefinitionDirty(true)
  }, [isRemoveMode, setEdges])

  const handleLayout = useCallback(() => {
    if (nodes.length === 0) {
      return
    }
    setNodes((current) => layoutFlowNodes(current, edges))
  }, [nodes, edges, setNodes])

  const reorderColumns = useCallback((sourceKey: string, targetKey: string) => {
    if (sourceKey === targetKey) {
      return
    }
    setPreviewColumns((current) => {
      const sourceIndex = current.findIndex((column) => column.key === sourceKey)
      const targetIndex = current.findIndex((column) => column.key === targetKey)
      if (sourceIndex < 0 || targetIndex < 0) {
        return current
      }
      const next = [...current]
      const [moved] = next.splice(sourceIndex, 1)
      next.splice(targetIndex, 0, moved)
      return next
    })
  }, [])

  const handleColumnDragStart = useCallback((event: React.DragEvent<HTMLButtonElement>, key: string) => {
    event.dataTransfer.effectAllowed = 'move'
    event.dataTransfer.setData('text/plain', key)
    setDraggedColumnKey(key)
  }, [])

  const handleColumnDragEnd = useCallback(() => {
    setDraggedColumnKey(null)
    setDragOverColumnKey(null)
  }, [])

  const handleColumnDragOver = useCallback((event: React.DragEvent<HTMLButtonElement>) => {
    event.preventDefault()
    event.dataTransfer.dropEffect = 'move'
  }, [])

  const handleColumnDragEnter = useCallback((key: string) => {
    if (draggedColumnKey === key) {
      return
    }
    setDragOverColumnKey(key)
  }, [draggedColumnKey])

  const handleColumnDragLeave = useCallback((event: React.DragEvent<HTMLButtonElement>) => {
    const nextTarget = event.relatedTarget as Element | null
    if (nextTarget && event.currentTarget.contains(nextTarget)) {
      return
    }
    setDragOverColumnKey(null)
  }, [])

  const handleColumnDrop = useCallback((event: React.DragEvent<HTMLButtonElement>, targetKey: string) => {
    event.preventDefault()
    const sourceKey = event.dataTransfer.getData('text/plain')
    if (!sourceKey) {
      return
    }
    reorderColumns(sourceKey, targetKey)
    setDraggedColumnKey(null)
    setDragOverColumnKey(null)
  }, [reorderColumns])

  const handleZoomIn = useCallback(() => {
    reactFlowInstance?.zoomIn()
  }, [reactFlowInstance])

  const handleZoomOut = useCallback(() => {
    reactFlowInstance?.zoomOut()
  }, [reactFlowInstance])

  const handleFitView = useCallback(() => {
    reactFlowInstance?.fitView({ padding: 0.1, minZoom: 0.1 })
  }, [reactFlowInstance])

  const handleCloseBottomPanel = useCallback(() => {
    setBottomPanelOpen(false)
  }, [])

  // Agent 채팅 메시지 전송 핸들러 (SSE 스트리밍 버전)
  const handleAgentSend = useCallback(() => {
    const text = agentInput.trim()
    if (!text || isNLProcessing) return

    if (!activeDbName) {
      setAgentMessages((prev) => [
        ...prev,
        { id: `msg-${Date.now()}`, role: 'assistant' as const, text: '먼저 프로젝트를 선택해주세요.' },
      ])
      return
    }

    const userMsg = { id: `msg-${Date.now()}`, role: 'user' as const, text }
    setAgentMessages((prev) => [...prev, userMsg])
    setAgentInput('')
    setIsNLProcessing(true)

    // 실시간 상태 추적용
    let totalNodesCreated = 0
    let currentTool = ''

    const datasetIds = datasets.map((d) => d.dataset_id).filter(Boolean)

    // SSE 스트리밍 시작
    const { abort } = runPipelineAgentStreaming(
      {
        goal: text,
        data_scope: {
          db_name: activeDbName,
          dataset_ids: datasetIds,
          branch: 'main',
        },
        max_transform: 5,
        max_cleansing: 3,
        max_repairs: 3,
      },
      {
        // 에이전트 시작
        onStart: (data) => {
          setAgentMessages((prev) => [
            ...prev,
            { id: `msg-${Date.now()}`, role: 'assistant' as const, text: '🤖 파이프라인 생성을 시작합니다...' },
          ])
        },

        // 도구 호출 시작 - 실시간 피드백
        onToolStart: (data) => {
          currentTool = data.tool || ''
          const toolLabel = getToolLabel(currentTool)
          setAgentMessages((prev) => {
            const last = prev[prev.length - 1]
            if (last?.role === 'assistant' && last.text.startsWith('🤖')) {
              // 기존 메시지 업데이트
              return [
                ...prev.slice(0, -1),
                { ...last, text: `🤖 ${toolLabel} 실행 중...` },
              ]
            }
            return prev
          })
        },

        // 도구 호출 완료
        onToolEnd: (data) => {
          if (!data.success && data.error) {
            setAgentMessages((prev) => [
              ...prev,
              { id: `msg-${Date.now()}`, role: 'assistant' as const, text: `⚠️ ${data.tool}: ${data.error}` },
            ])
          }
        },

        // 플랜 업데이트 - 노드 실시간 추가 ✨
        onPlanUpdate: (data) => {
          const plan = data.plan as AgentPlan | undefined
          if (!plan?.definition_json) return

          const currentNodes = nodesRef.current
          const currentEdges = edgesRef.current
          const { nodes: nextNodes, edges: nextEdges } = convertAgentPlanToFlow(plan, currentNodes, currentEdges, datasets)

          const addedCount = nextNodes.length - currentNodes.length
          const edgeCountDelta = nextEdges.length - currentEdges.length
          if (addedCount <= 0 && edgeCountDelta <= 0) {
            return
          }

          if (addedCount > 0) {
            totalNodesCreated += addedCount
          }

          const laidOutNodes = layoutFlowNodes(nextNodes, nextEdges)
          nodesRef.current = laidOutNodes
          edgesRef.current = nextEdges
          setNodes(laidOutNodes)
          setEdges(nextEdges)
          setDefinitionDirty(true)

          // 새 노드로 뷰 이동 (ReactFlow 내부 상태 반영을 위해 다음 tick에서 수행)
          setTimeout(() => {
            reactFlowInstance?.fitView({ padding: 0.2, minZoom: 0.5, duration: 300 })
          }, 0)

          // 노드 추가 메시지
          if (addedCount > 0) {
            const toolLabel = getToolLabel(data.tool || '')
            setAgentMessages((prev) => {
              const last = prev[prev.length - 1]
              if (last?.role === 'assistant' && last.text.startsWith('🤖')) {
                return [
                  ...prev.slice(0, -1),
                  { ...last, text: `✅ ${toolLabel} 완료 (+${addedCount} 노드)` },
                ]
              }
              return [
                ...prev,
                {
                  id: `msg-${Date.now()}`,
                  role: 'assistant' as const,
                  text: `✅ ${toolLabel} 완료 (+${addedCount} 노드)`,
                },
              ]
            })
          }
        },

        // 추가 정보 필요
        onClarification: (data) => {
          setIsNLProcessing(false)
          const questions = data.questions as Array<Record<string, unknown>> | undefined
          if (questions && questions.length > 0) {
            const questionTexts = questions.map((q) => String(q?.question || q?.text || '')).filter(Boolean)
            setAgentMessages((prev) => [
              ...prev,
              { id: `msg-${Date.now()}`, role: 'assistant' as const, text: `❓ 추가 정보가 필요합니다:\n\n${questionTexts.map((q) => `• ${q}`).join('\n')}` },
            ])
          }
        },

        // 에러 발생
        onError: (data) => {
          setIsNLProcessing(false)
          setAgentMessages((prev) => [
            ...prev,
            { id: `msg-${Date.now()}`, role: 'assistant' as const, text: `❌ 오류: ${data.error || '알 수 없는 오류'}` },
          ])
        },

        // 프리뷰 데이터 업데이트 ✨
        onPreviewUpdate: (data) => {
          const nodeId = data.node_id as string | undefined
          const preview = data.preview as { columns?: Array<{ name: string; type?: string }>; rows?: Array<Record<string, unknown>> } | undefined
          if (nodeId && preview) {
            const columns = preview.columns || []
            const rows = preview.rows || []
            const previewStatus = data.preview_status as string | undefined
            const previewPolicy = data.preview_policy as Record<string, unknown> | undefined
            const hint = data.hint as string | undefined
            setAgentNodePreviews((prev) => {
              const next = new Map(prev)
              next.set(nodeId, {
                columns,
                rows,
                previewStatus,
                previewPolicy: previewPolicy ?? null,
                hint,
              })
              return next
            })
          }
        },

        // 완료
        onComplete: (data) => {
          setIsNLProcessing(false)

          const status = data.status || 'unknown'
          const planId = data.plan_id || ''
          const errors = data.validation_errors as string[] | undefined

          let finalMessage = ''

          if (status === 'success') {
            finalMessage = `🎉 파이프라인이 성공적으로 생성되었습니다!`
            if (totalNodesCreated > 0) {
              finalMessage += `\n\n총 ${totalNodesCreated}개의 노드가 추가되었습니다.`
            }
            if (planId) {
              finalMessage += `\n\nPlan ID: ${planId}`
            }
          } else if (status === 'partial') {
            finalMessage = `⚠️ 파이프라인이 부분적으로 완료되었습니다.`
            if (totalNodesCreated > 0) {
              finalMessage += `\n\n${totalNodesCreated}개의 노드가 추가되었습니다.`
            }
            if (errors && errors.length > 0) {
              finalMessage += `\n\n경고:\n${errors.slice(0, 3).map((e) => `• ${e}`).join('\n')}`
            }
          } else {
            finalMessage = `❌ 파이프라인 생성에 실패했습니다.`
            if (data.error) {
              finalMessage += `\n\n${data.error}`
            }
          }

          setAgentMessages((prev) => [
            ...prev,
            { id: `msg-${Date.now()}`, role: 'assistant' as const, text: finalMessage },
          ])
        },
      },
    )

    agentAbortRef.current = abort
  }, [agentInput, isNLProcessing, activeDbName, datasets, setNodes, setEdges, reactFlowInstance])

  // 도구 이름을 한글 라벨로 변환
  const getToolLabel = (tool: string): string => {
    const labels: Record<string, string> = {
      plan_new: '플랜 생성',
      plan_add_input: '입력 노드 추가',
      plan_add_transform: '변환 노드 추가',
      plan_add_filter: '필터 노드 추가',
      plan_add_join: '조인 노드 추가',
      plan_add_union: '유니온 노드 추가',
      plan_add_compute_column: '계산 컬럼 추가',
      plan_add_compute_assignments: '계산 할당 추가',
      plan_add_select_expr: 'Select 표현식 추가',
      plan_add_group_by: '그룹화 추가',
      plan_add_group_by_expr: '그룹화 표현식 추가',
      plan_add_window_expr: '윈도우 함수 추가',
      plan_add_sort: '정렬 추가',
      plan_add_explode: 'Explode 추가',
      plan_add_pivot: '피봇 추가',
      plan_add_output: '출력 노드 추가',
      plan_preview: '미리보기',
      plan_validate: '검증',
      dataset_profile: '데이터셋 프로파일링',
      dataset_null_check: 'Null 체크',
      dataset_key_inference: '키 추론',
      dataset_type_inference: '타입 추론',
    }
    return labels[tool] || tool
  }

  // 노드 팔레트에서 노드 추가 핸들러
  const handleAddPaletteNode = useCallback((paletteNode: PaletteNode) => {
    // 프로젝트가 선택되지 않은 경우
    if (!activeDbName) {
      window.alert('먼저 프로젝트를 선택해주세요.')
      return
    }

    // 데이터셋 선택 다이얼로그 열기
    if (paletteNode.id === 'dataset') {
      setIsDatasetSelectOpen(true)
      return
    }

    // 파일 업로드 다이얼로그 열기
    if (paletteNode.id === 'file') {
      setIsFileUploadOpen(true)
      return
    }

    // 그 외 노드는 기존 방식으로 추가
    const newNodeId = `${paletteNode.id}-${Date.now()}`
    const viewport = reactFlowInstance?.getViewport()
    const centerX = viewport ? -viewport.x / viewport.zoom + 400 : 400
    const centerY = viewport ? -viewport.y / viewport.zoom + 200 : 200

    const position = { x: centerX, y: centerY }
    const category = paletteNode.category
    const transformOperation = normalizeBackendOperation(paletteNode.transformType || paletteNode.id)

    const newNode: FlowNode = (() => {
      if (category === 'output') {
        return {
          id: newNodeId,
          type: 'transform',
          position,
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
          data: {
            label: paletteNode.label || 'Output',
            transformType: 'output',
            config: {
              outputName: paletteNode.label || 'Output',
              operation: 'output',
            },
          },
        }
      }

      if (category === 'transform') {
        const transformType =
          mapBackendOperationToTransformType('transform', transformOperation) || ('compute' as TransformType)
        return {
          id: newNodeId,
          type: 'transform',
          position,
          sourcePosition: Position.Right,
          targetPosition: Position.Left,
          data: {
            label: paletteNode.label,
            transformType,
            description: paletteNode.description,
            config: {
              operation: transformOperation || transformType,
            },
          },
        }
      }

      return {
        id: newNodeId,
        type: 'datasetInput',
        position,
        sourcePosition: Position.Right,
        targetPosition: Position.Left,
        data: {
          label: paletteNode.label,
          stage: 'raw',
          metadata: { inputType: paletteNode.id },
        },
      }
    })()

    setNodes((nds) => [...nds, newNode])
    setDefinitionDirty(true)
  }, [reactFlowInstance, setNodes, activeDbName])

  // 데이터셋 선택 시 input 노드 추가
  const handleSelectDataset = useCallback((dataset: DatasetRecord) => {
    const newNodeId = `input-${dataset.dataset_id}-${Date.now()}`
    const viewport = reactFlowInstance?.getViewport()
    const centerX = viewport ? -viewport.x / viewport.zoom + 400 : 400
    const centerY = viewport ? -viewport.y / viewport.zoom + 200 : 200

    const schemaColumns = extractSchemaColumns(dataset.schema_json)
    const newNode: FlowNode = {
      id: newNodeId,
      type: 'datasetInput',
      position: { x: centerX, y: centerY },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        label: dataset.name,
        columnCount: schemaColumns.length || undefined,
        rowCount: dataset.row_count,
        sourceType: dataset.source_type,
        stage: dataset.stage || 'raw',
        metadata: { datasetId: dataset.dataset_id, datasetName: dataset.name },
      },
    }

    setNodes((nds) => [...nds, newNode])
    setDefinitionDirty(true)
  }, [reactFlowInstance, setNodes])

  // 변환 프리셋 선택 핸들러
  const handleSelectPreset = useCallback((preset: TransformPreset) => {
    const newNodeId = `transform-${preset.id}-${Date.now()}`
    const viewport = reactFlowInstance?.getViewport()
    const centerX = viewport ? -viewport.x / viewport.zoom + 400 : 400
    const centerY = viewport ? -viewport.y / viewport.zoom + 200 : 200

    const operation = normalizeBackendOperation(preset.transformType)
    const transformType = mapBackendOperationToTransformType('transform', operation) || ('compute' as TransformType)

    const newNode: FlowNode = {
      id: newNodeId,
      type: 'transform',
      position: { x: centerX, y: centerY },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        label: preset.label,
        transformType,
        description: preset.description,
        config: {
          operation: operation || transformType,
        },
      },
    }

    setNodes((nds) => [...nds, newNode])
    setDefinitionDirty(true)
  }, [reactFlowInstance, setNodes])

  const openCreatePipelineDialog = useCallback(
    (overrides?: Partial<{ name: string; branch: string }>) => {
      if (!activeDbName) {
        return
      }
      const dateTag = new Date().toISOString().slice(0, 10).replace(/-/g, '')
      const slugBase = activeDbName.replace(/[^a-zA-Z0-9_-]+/g, '_').replace(/^_+|_+$/g, '')
      const defaultName = overrides?.name || `${slugBase || 'pipeline'}_${dateTag}`
      const defaultBranch =
        (overrides?.branch || (selectedBranch && selectedBranch !== 'main' ? selectedBranch : 'dev')).trim() || 'dev'
      setCreatePipelineDefaults({ name: defaultName, branch: defaultBranch })
      setCreatePipelineError(null)
      setCreatePipelineDialogOpen(true)
    },
    [activeDbName, selectedBranch],
  )

  // 마법사 완료 핸들러
  const handleWizardComplete = useCallback((config: {
    inputType: 'dataset' | 'file' | 'api' | null
    selectedDataset: { id: string; name: string } | null
    transformGoal: string
    outputType: 'object-type' | 'export' | null
    outputName: string
  }) => {
    // 마법사 설정에 따라 파이프라인 노드 생성
    const newNodes: FlowNode[] = []
    const newEdges: Edge[] = []
    const baseTime = Date.now()

    // 입력 노드
    const inputNodeId = `input-${baseTime}`
    newNodes.push({
      id: inputNodeId,
      type: 'datasetInput',
      position: { x: 100, y: 200 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        label: config.selectedDataset?.name || (config.inputType === 'file' ? '파일 업로드' : 'API 연결'),
        stage: 'raw',
        metadata: {
          inputType: config.inputType,
          datasetId: config.selectedDataset?.id,
          datasetName: config.selectedDataset?.name,
        },
      },
    })

    // 변환 노드
    const transformNodeId = `transform-${baseTime}`
    newNodes.push({
      id: transformNodeId,
      type: 'transform',
      position: { x: 400, y: 200 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        label: config.transformGoal,
        transformType: 'compute',
        description: 'Draft transform',
        config: { operation: 'compute', goal: config.transformGoal },
      },
    })

    // 출력 노드
    const outputNodeId = `output-${baseTime}`
    newNodes.push({
      id: outputNodeId,
      type: 'transform',
      position: { x: 700, y: 200 },
      sourcePosition: Position.Right,
      targetPosition: Position.Left,
      data: {
        label: config.outputName,
        transformType: 'output',
        config: { operation: 'output', outputType: config.outputType, outputName: config.outputName },
      },
    })

    // 엣지 연결
    newEdges.push(createEdge(`edge-${inputNodeId}-${transformNodeId}`, inputNodeId, transformNodeId))
    newEdges.push(createEdge(`edge-${transformNodeId}-${outputNodeId}`, transformNodeId, outputNodeId))

    setNodes((nds) => [...nds, ...newNodes])
    setEdges((eds) => [...eds, ...newEdges])
    setDefinitionDirty(true)

    // 마법사 닫기
    setIsWizardOpen(false)
  }, [setNodes, setEdges])

  const savePipelineDefinition = useCallback(async () => {
    if (!primaryPipeline) {
      return false
    }
    const draft = buildDefinitionDraft(pipelineDetail?.definition_json, nodes, edges)
    setIsSaving(true)
    try {
      await updatePipeline(primaryPipeline.pipeline_id, {
        definition_json: draft,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['pipeline-detail', primaryPipeline.pipeline_id] })
      await queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
      setDefinitionDirty(false)
      return true
    } catch (error) {
      console.error('Failed to save pipeline definition', error)
      window.alert('Failed to save pipeline definition.')
      return false
    } finally {
      setIsSaving(false)
    }
  }, [primaryPipeline, pipelineDetail, nodes, edges, activeDbName, queryClient])

  const handleSave = useCallback(() => {
    if (!activeDbName) {
      window.alert('먼저 프로젝트를 선택해주세요.')
      return
    }
    if (primaryPipeline) {
      void savePipelineDefinition()
      return
    }
    if (definitionDirty) {
      openCreatePipelineDialog()
    }
  }, [activeDbName, primaryPipeline, definitionDirty, openCreatePipelineDialog, savePipelineDefinition])

  const handleCreatePipeline = useCallback(
    async (values: CreatePipelineDialogValues) => {
      if (!activeDbName) {
        return
      }
      setIsCreatingPipeline(true)
      setCreatePipelineError(null)
      try {
        const draft = buildDefinitionDraft(undefined, nodesRef.current, edgesRef.current)
        await createPipeline({
          dbName: activeDbName,
          name: values.name,
          pipelineType,
          branch: values.branch,
          description: 'Created from Pipeline Builder',
          definitionJson: draft,
        })
        await queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
        setSelectedBranch(values.branch)
        setDefinitionDirty(false)
        setCreatePipelineDialogOpen(false)
      } catch (error) {
        console.error('Failed to create pipeline', error)
        setCreatePipelineError(error instanceof Error ? error.message : String(error))
      } finally {
        setIsCreatingPipeline(false)
      }
    },
    [activeDbName, pipelineType, queryClient],
  )

  const handlePropose = useCallback(async () => {
    if (!primaryPipeline) {
      return
    }
    setIsProposing(true)
    try {
      if (definitionDirty) {
        const saved = await savePipelineDefinition()
        if (!saved) {
          return
        }
      }
      const timestamp = new Date().toISOString().slice(0, 19)
      const title = `${pipelineDisplayName} proposal ${timestamp}`
      await submitPipelineProposal(primaryPipeline.pipeline_id, {
        title,
        description: 'Submitted from Pipeline Builder',
        buildJobId: latestBuildArtifact?.job_id,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
    } catch (error) {
      console.error('Failed to submit proposal', error)
    } finally {
      setIsProposing(false)
    }
  }, [
    primaryPipeline,
    definitionDirty,
    savePipelineDefinition,
    pipelineDisplayName,
    latestBuildArtifact,
    activeDbName,
    queryClient,
  ])

  const handleBuild = useCallback(async () => {
    if (!primaryPipeline) {
      return
    }
    if (!activeDbName) {
      window.alert('먼저 프로젝트를 선택해주세요.')
      return
    }
    if (definitionDirty) {
      window.alert('Build 전에 Save로 파이프라인 정의를 먼저 저장해주세요.')
      return
    }
    if (!isStreamingPipeline && !defaultOutputNodeId) {
      window.alert('Build 전에 Output 노드를 추가해주세요.')
      return
    }
    setIsBuilding(true)
    try {
      await buildPipeline(primaryPipeline.pipeline_id, {
        nodeId: defaultOutputNodeId ?? undefined,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['pipeline-artifacts', primaryPipeline.pipeline_id, 'build'] })
    } catch (error) {
      console.error('Failed to build pipeline', error)
    } finally {
      setIsBuilding(false)
    }
  }, [
    primaryPipeline,
    definitionDirty,
    defaultOutputNodeId,
    activeDbName,
    queryClient,
    isStreamingPipeline,
  ])

  const handleDeploy = useCallback(async () => {
    if (!primaryPipeline || !latestBuildArtifact) {
      return
    }
    if (!activeDbName) {
      window.alert('먼저 프로젝트를 선택해주세요.')
      return
    }
    if (definitionDirty) {
      window.alert('Deploy 전에 Save로 파이프라인 정의를 먼저 저장하고, Build를 다시 수행해주세요.')
      return
    }
    if (!isStreamingPipeline && !defaultOutputNodeId) {
      return
    }
    setIsDeploying(true)
    try {
      await deployPipeline(primaryPipeline.pipeline_id, {
        promoteBuild: true,
        buildJobId: latestBuildArtifact.job_id,
        artifactId: latestBuildArtifact.artifact_id,
        nodeId: isStreamingPipeline ? undefined : defaultOutputNodeId ?? undefined,
        replayOnDeploy: false,
        dbName: activeDbName,
      })
      await queryClient.invalidateQueries({ queryKey: ['datasets', activeDbName] })
      await queryClient.invalidateQueries({ queryKey: ['pipeline-artifacts', primaryPipeline.pipeline_id, 'build'] })
    } catch (error) {
      console.error('Failed to deploy pipeline', error)
    } finally {
      setIsDeploying(false)
    }
  }, [
    primaryPipeline,
    latestBuildArtifact,
    isStreamingPipeline,
    defaultOutputNodeId,
    definitionDirty,
    activeDbName,
    queryClient,
  ])

  const handleSelectFolder = useCallback(
    (folder: FolderOption) => {
      setPipelineContext({ folderId: folder.id, folderName: folder.name })
    },
    [setPipelineContext],
  )

  const handleRefresh = useCallback(() => {
    if (activeDbName) {
      void queryClient.invalidateQueries({ queryKey: ['datasets', activeDbName] })
      void queryClient.invalidateQueries({ queryKey: ['pipelines', activeDbName] })
    }
    if (primaryPipeline?.pipeline_id) {
      void queryClient.invalidateQueries({ queryKey: ['pipeline-detail', primaryPipeline.pipeline_id] })
      void queryClient.invalidateQueries({ queryKey: ['pipeline-artifacts', primaryPipeline.pipeline_id, 'build'] })
    }
  }, [activeDbName, primaryPipeline, queryClient])

  const summarizeReadiness = useCallback((readiness: PipelineReadiness) => {
    const status = readiness.status ?? 'UNKNOWN'
    const inputs = readiness.inputs ?? []
    const blocked = inputs.filter((input) => String(input.status || '').toUpperCase() !== 'READY')
    if (inputs.length === 0) {
      return `Readiness: ${status}\nNo inputs configured yet.`
    }
    return `Readiness: ${status}\nInputs: ${inputs.length}\nNot ready: ${blocked.length}`
  }, [])

  const handleSettings = useCallback(async () => {
    if (!primaryPipeline) {
      return
    }
    setIsCheckingReadiness(true)
    try {
      const readiness = await getPipelineReadiness(primaryPipeline.pipeline_id, { dbName: activeDbName })
      window.alert(summarizeReadiness(readiness))
      handleRefresh()
    } catch (error) {
      console.error('Failed to load pipeline readiness', error)
      window.alert('Failed to load pipeline readiness.')
    } finally {
      setIsCheckingReadiness(false)
    }
  }, [primaryPipeline, activeDbName, summarizeReadiness, handleRefresh])

  const handleHelp = useCallback(() => {
    const helpUrl =
      (import.meta.env.VITE_PIPELINE_HELP_URL as string | undefined) ??
      '/docs/PipelineBuilder_checklist.md'
    window.open(helpUrl, '_blank', 'noopener,noreferrer')
  }, [])

  const fileMenu = useMemo(() => {
    const hasFolders = folderOptions.length > 0
    return (
      <Menu>
        <MenuItem icon="folder-close" text={`Current: ${pipelineFolderLabel}`} disabled />
        <MenuDivider />
        {hasFolders
          ? folderOptions.map((folder) => {
              const isActive = folder.id === activeDbName
              return (
                <MenuItem
                  key={folder.id}
                  icon={isActive ? 'small-tick' : 'folder-close'}
                  text={folder.name}
                  disabled={isActive}
                  onClick={() => handleSelectFolder(folder)}
                />
              )
            })
          : <MenuItem text="No folders found" disabled />}
      </Menu>
    )
  }, [
    folderOptions,
    pipelineFolderLabel,
    activeDbName,
    handleSelectFolder,
  ])

  useEffect(() => {
    setPreviewColumns(previewPayload.columns)
    setPreviewRows(previewPayload.rows)
    setActiveColumn((current) => {
      if (previewPayload.columns.some((column) => column.key === current)) {
        return current
      }
      return previewPayload.columns[0]?.key ?? ''
    })
  }, [previewPayload])

  useEffect(() => {
    if (!agentPlanDefinition) {
      return
    }
    if (agentRunDbName && agentRunDbName !== activeDbName) {
      return
    }
    if (agentRunMarker && lastAppliedAgentRunId.current === agentRunMarker) {
      return
    }
    const { nodes: flowNodes, edges: flowEdges } = buildFlowFromDefinition(
      agentPlanDefinition as PipelineDetailRecord['definition_json'],
    )
    if (flowNodes.length === 0) {
      return
    }
    const hydratedNodes = hydrateFlowNodesWithDatasets(flowNodes, datasets)
    const laidOutNodes = layoutFlowNodes(hydratedNodes, flowEdges)
    setNodes(laidOutNodes)
    setEdges(flowEdges)
    setDefinitionDirty(true)
    setRightPanelOpen(true)
    setBottomPanelOpen(true)
    lastAppliedAgentRunId.current = agentRunMarker || `agent-${Date.now()}`
  }, [
    agentPlanDefinition,
    agentRunMarker,
    agentRunDbName,
    activeDbName,
    datasets,
    setNodes,
    setEdges,
    setDefinitionDirty,
    setRightPanelOpen,
    setBottomPanelOpen,
  ])

  useEffect(() => {
    if (definitionDirty) {
      return
    }
    if (agentPlanDefinition && (!agentRunDbName || agentRunDbName === activeDbName)) {
      return
    }
    const { nodes: flowNodes, edges: flowEdges } = buildFlowFromDefinition(pipelineDetail?.definition_json)
    if (flowNodes.length > 0) {
      const hydratedNodes = hydrateFlowNodesWithDatasets(flowNodes, datasets)
      const laidOutNodes = layoutFlowNodes(hydratedNodes, flowEdges)
      setNodes(laidOutNodes)
      setEdges(flowEdges)
      return
    }
    setEdges(datasetEdges)
    setNodes((current) => syncDatasetNodes(current, datasetNodes, datasetEdges))
  }, [
    pipelineDetail?.definition_json,
    datasetNodes,
    datasetEdges,
    datasets,
    definitionDirty,
    agentPlanDefinition,
    agentRunDbName,
    activeDbName,
    setNodes,
    setEdges,
  ])

  const topbar = (
    <div className="pipeline-topbar">
      <div className="pipeline-topbar-left">
        <div className="pipeline-header-details">
          <div className="pipeline-title-row">
            <div className="pipeline-breadcrumb">
              <Icon icon="flow-branch" size={14} className="pipeline-breadcrumb-icon" />
              <span className="pipeline-breadcrumb-text">{pipelineFolderLabel}</span>
            </div>
          </div>

          <div className="pipeline-menu-row">
            <div className="pipeline-menu">
              <Popover content={fileMenu} position="bottom-left" usePortal={false}>
                <Button minimal text="File" rightIcon="caret-down" small />
              </Popover>
              <Button
                minimal
                text="Settings"
                rightIcon="caret-down"
                small
                onClick={handleSettings}
                loading={isCheckingReadiness}
                disabled={!primaryPipeline}
              />
              <Button
                minimal
                text="Help"
                rightIcon="caret-down"
                small
                onClick={handleHelp}
              />
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-info-item">
              <Icon icon="office" size={12} />
              <span>{pipelineCount}</span>
            </div>
            <div className="pipeline-divider" />
            <div className="pipeline-batch-badge">
              <span>{pipelineTypeLabel}</span>
            </div>
          </div>
        </div>

        <div className="pipeline-divider" />

        <div className="pipeline-tabs">
          <Button
            minimal
            className={activeTab === 'edit' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('edit')}
            text="Edit"
          />
          <Button
            minimal
            className={activeTab === 'proposals' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('proposals')}
            text="Proposals"
          />
          <Button
            minimal
            className={activeTab === 'history' ? 'pipeline-tab-active' : ''}
            onClick={() => setActiveTab('history')}
            text="History"
          />
        </div>
      </div>
      <div className="pipeline-topbar-right">
        <div className="pipeline-history-controls">
          <Button minimal icon="undo" disabled small />
          <Button minimal icon="redo" disabled small />
        </div>

        <div className="pipeline-divider" />

        <Popover
          content={
            <Menu>
              {branchOptions.map((branch) => {
                const isActive = branch === selectedBranch
                const label = branch === 'main' ? 'default' : undefined
                return (
                  <MenuItem
                    key={branch}
                    icon={isActive ? 'small-tick' : 'git-branch'}
                    text={branch}
                    label={label}
                    disabled={isActive}
                    onClick={() => {
                      if (definitionDirty) {
                        const confirmed = window.confirm(
                          '저장되지 않은 변경사항이 있습니다. 브랜치를 전환하면 변경사항이 사라집니다. 계속할까요?',
                        )
                        if (!confirmed) {
                          return
                        }
                        setDefinitionDirty(false)
                      }
                      setSelectedBranch(branch)
                    }}
                  />
                )
              })}
              <MenuDivider />
              <MenuItem
                icon="add"
                text="새 파이프라인 생성..."
                onClick={() => openCreatePipelineDialog({ branch: selectedBranch === 'main' ? 'dev' : selectedBranch })}
              />
            </Menu>
          }
          placement="bottom"
          minimal
        >
          <button type="button" className="pipeline-branch-selector">
            <Icon icon="git-merge" size={12} className="pipeline-branch-icon" />
            <span className="pipeline-branch-name">{pipelineBranchLabel}</span>
            <Icon icon="caret-down" size={12} />
          </button>
        </Popover>

        <div className="pipeline-divider" />

        {/* Save 버튼 */}
        <Button
          icon="cloud-upload"
          text="Save"
          outlined
          intent="success"
          onClick={handleSave}
          loading={isSaving}
          disabled={!definitionDirty || !activeDbName}
          className="pipeline-save-button"
        />

        {/* Build 버튼 - Spark 빌드 */}
        <Button
          icon="build"
          text="Build"
          outlined
          intent="warning"
          onClick={handleBuild}
          loading={isBuilding}
          disabled={!canBuild}
          className="pipeline-build-button"
        />

        {/* Deploy 버튼 - Spark 실행 */}
        <Button
          icon="rocket-slant"
          text="Deploy"
          intent="primary"
          onClick={handleDeploy}
          loading={isDeploying}
          disabled={!canDeploy}
          className="pipeline-deploy-button"
        />

        {/* Job 실행 상태 표시 */}
        {runningArtifact && (
          <Tag
            intent="primary"
            icon={<Spinner size={12} />}
            className="pipeline-job-status"
          >
            {String(runningArtifact.status || '').toUpperCase() === 'PENDING' ? '대기 중' : '실행 중'}
          </Tag>
        )}
        {!runningArtifact && latestFailedArtifact && (
          <Tag
            intent="danger"
            icon="error"
            className="pipeline-job-status"
          >
            실패
          </Tag>
        )}
        {!runningArtifact && !latestFailedArtifact && latestBuildArtifact && (
          <Tag
            intent="success"
            icon="tick"
            className="pipeline-job-status"
          >
            빌드 완료
          </Tag>
        )}

        <div className="pipeline-divider" />

        <Button
          minimal
          icon="grid-view"
          small
          className={`pipeline-bottom-panel-toggle ${isBottomPanelOpen ? 'is-active' : ''}`}
          onClick={() => setBottomPanelOpen((open) => !open)}
          aria-pressed={isBottomPanelOpen}
        />
        <Button
          minimal
          icon="panel-table"
          small
          className={`pipeline-panel-toggle ${isRightPanelOpen ? 'is-active' : ''}`}
          onClick={() => setRightPanelOpen((open) => !open)}
          aria-pressed={isRightPanelOpen}
        />
      </div>
    </div>
  )

  return (
    <div className="page pipeline-page">
      {topbar}
      <div className="pipeline-canvas">
        <div className={`pipeline-canvas-inner ${isAgentOpen ? 'is-agent-open' : ''}`}>
          {/* Agent Chat Panel - Left Side */}
          <div className={`pipeline-agent-panel ${isAgentOpen ? 'is-open' : ''}`}>
            <div className="pipeline-agent-panel-header">
              <Icon icon="chat" size={16} />
              <span>Pipeline Agent</span>
              <button
                type="button"
                className="pipeline-agent-close"
                onClick={() => setIsAgentOpen(false)}
                aria-label="Close agent"
              >
                <Icon icon="cross" size={16} />
              </button>
            </div>
            <div className="pipeline-agent-messages">
              {agentMessages.length === 0 ? (
                <div className="pipeline-agent-empty">
                  <Icon icon="chat" size={32} />
                  <p>자연어로 파이프라인을 만들어보세요</p>
                  <span>예: "이메일에서 도메인 추출해줘"</span>
                </div>
              ) : (
                agentMessages.map((msg) => (
                  <div key={msg.id} className={`pipeline-agent-message pipeline-agent-message--${msg.role}`}>
                    <div className="pipeline-agent-message-text">{msg.text}</div>
                  </div>
                ))
              )}
              {isNLProcessing && (
                <div className="pipeline-agent-message pipeline-agent-message--assistant">
                  <div className="pipeline-agent-message-text">처리 중...</div>
                </div>
              )}
            </div>
            <div className="pipeline-agent-input">
              <input
                type="text"
                value={agentInput}
                onChange={(e) => setAgentInput(e.target.value)}
                onKeyDown={(e) => {
                  if (e.key === 'Enter' && !e.shiftKey) {
                    e.preventDefault()
                    handleAgentSend()
                  }
                }}
                placeholder="메시지를 입력하세요..."
                disabled={isNLProcessing}
              />
              <Button
                minimal
                icon="send-message"
                onClick={handleAgentSend}
                disabled={!agentInput.trim() || isNLProcessing}
              />
            </div>
          </div>
          <div className="pipeline-canvas-stage">
            <div className={canvasClassName}>
              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                onNodesChange={handleNodesChange}
                onEdgesChange={handleEdgesChange}
                onConnect={handleConnect}
                onNodeClick={handleNodeClick}
                onEdgeClick={handleEdgeClick}
                onInit={setReactFlowInstance}
                panOnDrag={isPanMode}
                nodesDraggable={isPointerMode}
                nodesConnectable={isPointerMode}
                elementsSelectable={isSelectableMode}
                selectionOnDrag={isSelectMode || isPointerMode}
                selectNodesOnDrag={isSelectMode}
                selectionKeyCode={null}
                multiSelectionKeyCode={isSelectMode ? ['Meta', 'Control'] : null}
                deleteKeyCode={['Backspace', 'Delete']}
                fitView
                minZoom={0.1}
                proOptions={{ hideAttribution: true }}
              >
                <MiniMap
                  className="pipeline-minimap"
                  position="bottom-right"
                  style={{ right: 60, bottom: 8 }}
                  maskColor="rgba(11, 18, 26, 0.6)"
                />
              </ReactFlow>
              <div className="pipeline-toolbox">
                <div className="pipeline-toolbox-group">
                  <div className="pipeline-toolbox-button-group">
                    <Button
                      minimal
                      icon="move"
                      active={isPanMode}
                      onClick={() => setToolMode('pan')}
                      className="pipeline-tool-button"
                    />
                    <Button
                      minimal
                      icon="select"
                      active={isPointerMode}
                      onClick={() => setToolMode('pointer')}
                      className="pipeline-tool-button"
                    />
                  </div>
                  <span className="pipeline-toolbox-label">Tools</span>
                </div>
                <div className="pipeline-toolbox-group">
                  <Button
                    minimal
                    icon="locate"
                    active={isSelectMode}
                    onClick={() => setToolMode('select')}
                    className="pipeline-tool-button"
                  />
                  <span className="pipeline-toolbox-label">Select</span>
                </div>
                <div className="pipeline-toolbox-group">
                  <Button
                    minimal
                    icon="small-cross"
                    active={isRemoveMode}
                    onClick={() => setToolMode('remove')}
                    className="pipeline-tool-button"
                  />
                  <span className="pipeline-toolbox-label">Remove</span>
                </div>
                <div className="pipeline-toolbox-group">
                  <Button
                    minimal
                    icon="layout-hierarchy"
                    onClick={handleLayout}
                    className="pipeline-tool-button pipeline-layout-button"
                  />
                  <span className="pipeline-toolbox-label">Layout</span>
                </div>

                <div className="pipeline-toolbox-divider" />

                {/* Add datasets */}
                <NodePalette onAddNode={handleAddPaletteNode} />

                {/* Parameters (UDF) */}
                <div className="pipeline-toolbox-group">
                  <button
                    type="button"
                    className="pipeline-tool-button-outlined"
                    onClick={() => setIsUdfDialogOpen(true)}
                  >
                    <Icon icon="variable" size={14} />
                    <span>Parameters</span>
                  </button>
                </div>

                <div className="pipeline-toolbox-divider" />

                {/* Transform */}
                <TransformPresets onSelectPreset={handleSelectPreset} />

                {/* Edit */}
                <div className="pipeline-toolbox-group">
                  <Button
                    minimal
                    icon="edit"
                    className="pipeline-tool-button"
                  />
                  <span className="pipeline-toolbox-label">Edit</span>
                </div>
              </div>
              <div className="pipeline-viewport-controls">
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleZoomIn}
                  disabled={!isViewportReady}
                  aria-label="Zoom in"
                >
                  <Icon icon="zoom-in" size={18} />
                </button>
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleZoomOut}
                  disabled={!isViewportReady}
                  aria-label="Zoom out"
                >
                  <Icon icon="zoom-out" size={18} />
                </button>
                <button
                  type="button"
                  className="pipeline-viewport-button"
                  onClick={handleFitView}
                  disabled={!isViewportReady}
                  aria-label="Fit view"
                >
                  <Icon icon="zoom-to-fit" size={18} />
                </button>
              </div>
              {/* Agent Button - Bottom Left */}
              <div className="pipeline-agent-trigger">
                <button
                  type="button"
                  className={`pipeline-agent-button ${isAgentOpen ? 'is-active' : ''}`}
                  onClick={() => setIsAgentOpen(!isAgentOpen)}
                  aria-label="Toggle agent"
                >
                  <Icon icon="chat" size={18} />
                  <span>Agent</span>
                </button>
              </div>
            </div>
            <div className={`pipeline-bottom-panel ${isBottomPanelOpen ? 'is-open' : ''}`}>
              <div className="pipeline-bottom-panel-header">
                <div className="pipeline-bottom-panel-title">
                  <Icon icon="th" size={14} />
                  <span>Data preview</span>
                </div>
                <button
                  type="button"
                  className="pipeline-bottom-panel-collapse"
                  onClick={handleCloseBottomPanel}
                  aria-label="Close preview"
                >
                  <Icon icon="chevron-down" size={16} />
                </button>
              </div>
              <div className="pipeline-bottom-panel-body">
                <div className="pipeline-preview">
                  <div className="pipeline-preview-sidebar">
                    <Popover content={previewMenu} position="bottom-left" usePortal={false}>
                      <button type="button" className="pipeline-preview-selector">
                        <div className="pipeline-preview-selector-left">
                          <Icon icon={previewSource === 'transform' ? 'function' : previewSource === 'pipeline' ? 'flow-branch' : 'database'} size={14} />
                          <span>{previewLabel}</span>
                        </div>
                        <Icon icon="chevron-down" size={14} />
                      </button>
                    </Popover>
                    <label className="pipeline-preview-search">
                      <Icon icon="search" size={14} />
                      <input
                        className="pipeline-preview-input"
                        value={columnSearch}
                        onChange={(event) => setColumnSearch(event.target.value)}
                        placeholder={`Search ${previewColumns.length} columns...`}
                      />
                    </label>
                    <div className="pipeline-preview-columns">
                      {filteredColumns.length === 0 ? (
                        <div className="pipeline-preview-empty">No matching columns.</div>
                      ) : (
                        filteredColumns.map((column) => (
                          <button
                            key={column.key}
                            type="button"
                            className={`pipeline-preview-column${activeColumn === column.key ? ' is-active' : ''}${
                              draggedColumnKey === column.key ? ' is-dragging' : ''
                            }${dragOverColumnKey === column.key ? ' is-dragover' : ''}`}
                            onClick={() => setActiveColumn(column.key)}
                            draggable
                            onDragStart={(event) => handleColumnDragStart(event, column.key)}
                            onDragEnd={handleColumnDragEnd}
                            onDragOver={handleColumnDragOver}
                            onDragEnter={() => handleColumnDragEnter(column.key)}
                            onDragLeave={handleColumnDragLeave}
                            onDrop={(event) => handleColumnDrop(event, column.key)}
                          >
                            <div className="pipeline-preview-column-left">
                              <Icon icon={column.icon} size={14} className="pipeline-preview-column-icon" />
                              <span>{column.label}</span>
                            </div>
                            <Icon icon="drag-handle-vertical" size={14} className="pipeline-preview-column-menu" />
                          </button>
                        ))
                      )}
                    </div>
                  </div>
                  <div className="pipeline-preview-table">
                    <div className="pipeline-preview-table-scroll">
                      {(transformPreviewNotice || pipelinePreviewNotice) ? (
                        <div className={`pipeline-preview-notice is-${(transformPreviewNotice || pipelinePreviewNotice)?.level}`}>
                          <div className="pipeline-preview-notice-header">
                            <Icon
                              icon={
                                (transformPreviewNotice || pipelinePreviewNotice)?.level === 'deny'
                                  ? 'error'
                                  : (transformPreviewNotice || pipelinePreviewNotice)?.level === 'require_spark'
                                    ? 'warning-sign'
                                    : 'info-sign'
                              }
                              size={14}
                            />
                            <span className="pipeline-preview-notice-title">{(transformPreviewNotice || pipelinePreviewNotice)?.title}</span>
                          </div>
                          <div className="pipeline-preview-notice-message">{(transformPreviewNotice || pipelinePreviewNotice)?.message}</div>
                          {(transformPreviewNotice || pipelinePreviewNotice)?.level === 'require_spark' ? (
                            <div className="pipeline-preview-notice-actions">
                              {primaryPipeline && !definitionDirty ? (
                                <Button
                                  small
                                  icon="build"
                                  text="Build (Spark Preview)"
                                  intent="warning"
                                  onClick={handleBuild}
                                  disabled={!canBuild}
                                />
                              ) : (
                                <Button
                                  small
                                  icon="cloud-upload"
                                  text="Save"
                                  intent="success"
                                  onClick={handleSave}
                                  disabled={!definitionDirty || !activeDbName || isSaving}
                                />
                              )}
                            </div>
                          ) : null}
                          {(transformPreviewNotice || pipelinePreviewNotice)?.issues.length ? (
                            <ul className="pipeline-preview-notice-issues">
                              {(transformPreviewNotice || pipelinePreviewNotice)?.issues.map((issue, idx) => (
                                <li key={`${idx}`}>{issue}</li>
                              ))}
                            </ul>
                          ) : null}
                        </div>
                      ) : null}
                      <div
                        className="pipeline-preview-table-header"
                        style={{ gridTemplateColumns: tableGridTemplate, minWidth: tableMinWidth }}
                      >
                        <div className="pipeline-preview-table-cell is-index">#</div>
                        {previewColumns.map((column) => (
                          <div
                            key={column.key}
                            className={`pipeline-preview-table-cell is-header${
                              activeColumn === column.key ? ' is-active' : ''
                            }`}
                          >
                            <div className="pipeline-preview-header-main">
                              <span>{column.label}</span>
                              <Icon icon="caret-down" size={12} />
                            </div>
                            <span className="pipeline-preview-header-type">{column.type}</span>
                          </div>
                        ))}
                      </div>
                      {previewRows.map((row, index) => (
                        <div
                          key={`${index + 1}`}
                          className="pipeline-preview-table-row"
                          style={{ gridTemplateColumns: tableGridTemplate, minWidth: tableMinWidth }}
                        >
                          <div className="pipeline-preview-table-cell is-index">{index + 1}</div>
                          {previewColumns.map((column) => (
                            <div
                              key={column.key}
                              className={`pipeline-preview-table-cell${
                                activeColumn === column.key ? ' is-active' : ''
                              }`}
                            >
                              {row[column.key] ?? ''}
                            </div>
                          ))}
                        </div>
                      ))}
                    </div>
                  </div>
                </div>
              </div>
            </div>
          </div>
          <aside className={`pipeline-right-panel ${isRightPanelOpen ? 'is-open' : ''}`}>
            <div className="pipeline-right-panel-header">
              <div className="pipeline-right-panel-tabs">
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'datasets' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('datasets')}
                >
                  <Icon icon="database" size={12} />
                  <span>Datasets</span>
                </button>
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'objectTypes' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('objectTypes')}
                >
                  <Icon icon="cube" size={12} />
                  <span>Object types</span>
                </button>
                <button
                  type="button"
                  className={`pipeline-right-panel-tab ${activeOutputTab === 'linkTypes' ? 'is-active' : ''}`}
                  onClick={() => setActiveOutputTab('linkTypes')}
                >
                  <Icon icon="link" size={12} />
                  <span>Link types</span>
                </button>
              </div>
              <button
                type="button"
                className="pipeline-right-panel-close"
                onClick={() => setRightPanelOpen(false)}
                aria-label="Close panel"
              >
                <Icon icon="chevron-right" size={12} />
              </button>
            </div>
            <div className="pipeline-right-panel-body">
              {activeOutputTab === 'datasets' ? (
                <div className="pipeline-output-panel">
                  <div className="pipeline-output-list">
                    {/* Canonical Datasets Section - Objectify Ready */}
                    {canonicalOutputs.length > 0 && (
                      <>
                        <div className="pipeline-output-section-header">
                          <Icon icon="endorsed" size={12} style={{ color: '#72ca9b' }} />
                          <span>Canonical Datasets</span>
                          <span className="pipeline-output-section-badge">{canonicalOutputs.length}</span>
                        </div>
                        {canonicalOutputs.map(({ nodeId, label }) => (
                          <div
                            className="pipeline-output-card is-canonical"
                            key={nodeId}
                            role="button"
                            tabIndex={0}
                            onClick={async () => {
                              setTransformPreviewNodeId(nodeId)
                              setPreviewSource('transform')
                              setBottomPanelOpen(true)
                              const preview = await getTransformPreview(activeDbName, nodeId)
                              if (preview) {
                                setTransformPreviewData(preview)
                              }
                            }}
                          >
                            <div className="pipeline-output-header">
                              <div className="pipeline-output-title">
                                <Icon icon="data-lineage" size={14} style={{ color: '#72ca9b' }} />
                                <span>{label}</span>
                              </div>
                              <div className="pipeline-output-badge is-ready">
                                Ready for Objectify
                              </div>
                            </div>
                            <div className="pipeline-output-path">
                              <Icon icon="flow-end" size={12} />
                              <span>Pipeline Output → Ontology Mapping</span>
                            </div>
                            <div className="pipeline-output-row">
                              <div className="pipeline-output-status is-success">
                                <Icon icon="tick-circle" size={12} />
                                <span>Fully typed & cleansed</span>
                              </div>
                              <button type="button" className="pipeline-output-action">
                                <Icon icon="cube-add" size={12} />
                                <span>Map to Ontology</span>
                              </button>
                            </div>
                          </div>
                        ))}
                        <div className="pipeline-output-section-divider" />
                      </>
                    )}

                    {/* Raw Datasets Section */}
                    {datasetOutputs.length > 0 && (
                      <div className="pipeline-output-section-header">
                        <Icon icon="database" size={12} style={{ color: '#8abbff' }} />
                        <span>Raw Datasets</span>
                        <span className="pipeline-output-section-badge">{datasetOutputs.length}</span>
                      </div>
                    )}
                    {datasetOutputs.length === 0 && canonicalOutputs.length === 0 && pipelineOutputs.unknown.length === 0 ? (
                      <div className="pipeline-output-empty">
                        <Icon icon="info-sign" size={14} />
                        <span>No datasets available yet.</span>
                      </div>
                    ) : (
                      <>
                        {pipelineOutputs.unknown.map((output) => {
                          const name = safeText(output.output_name || output.outputName).trim() || 'Pipeline output'
                          return (
                            <div
                              className="pipeline-output-card"
                              key={`pipeline-output-${name}`}
                              role="button"
                              tabIndex={0}
                              onClick={() => handlePreviewOutput(name)}
                            >
                              <div className="pipeline-output-header">
                                <div className="pipeline-output-title">
                                  <Icon icon="th" size={14} />
                                  <span>{name}</span>
                                </div>
                              </div>
                              <div className="pipeline-output-path">
                                <Icon icon="lab-test" size={12} />
                                <span>Pipeline output</span>
                              </div>
                              <div className="pipeline-output-row">
                                <div className="pipeline-output-status is-success">
                                  <Icon icon="tick-circle" size={12} />
                                  <span>Click to preview</span>
                                </div>
                              </div>
                            </div>
                          )
                        })}
                        {datasetOutputs.map(({ dataset, columnCount, rowCount, extraColumnsCount }) => {
                          const statusLabel = columnCount > 0 ? `${columnCount} columns` : 'No schema yet'
                          const rowLabel = rowCount > 0 ? `${rowCount} rows` : ''
                          const statusText = rowLabel ? `${statusLabel} · ${rowLabel}` : statusLabel
                          return (
                            <div className="pipeline-output-card" key={dataset.dataset_id}>
                              <div className="pipeline-output-header">
                                <div className="pipeline-output-title">
                                  <Icon icon="th" size={14} />
                                  <span>{dataset.name}</span>
                                </div>
                                <button type="button" className="pipeline-output-menu" aria-label="Output actions">
                                  <Icon icon="more" size={12} />
                                </button>
                              </div>
                              <div className="pipeline-output-path">
                                <Icon icon="folder-close" size={12} />
                                <span>/{dataset.db_name || pipelineDisplayName}</span>
                              </div>
                              <div className="pipeline-output-row">
                                <div className="pipeline-output-status is-success">
                                  <Icon icon="tick-circle" size={12} />
                                  <span>{statusText}</span>
                                </div>
                                <button type="button" className="pipeline-output-action">
                                  <Icon icon="edit" size={12} />
                                  <span>Edit schema</span>
                                </button>
                              </div>
                              {extraColumnsCount > 0 ? (
                                <div className="pipeline-output-alert">
                                  <Icon icon="warning-sign" size={12} />
                                  <span>{extraColumnsCount} column(s) not in schema</span>
                                </div>
                              ) : null}
                            </div>
                          )
                        })}
                      </>
                    )}
                  </div>
                  <div className="pipeline-output-footer">
                    <button type="button" className="pipeline-output-add">
                      <Icon icon="add" size={12} />
                      <span>Add output</span>
                    </button>
                  </div>
                </div>
              ) : activeOutputTab === 'objectTypes' ? (
                <div className="pipeline-output-panel">
                  <div className="pipeline-output-list">
                    {pipelineOutputs.object.length === 0 ? (
                      <div className="pipeline-output-empty">
                        <Icon icon="info-sign" size={14} />
                        <span>No object outputs available yet.</span>
                      </div>
                    ) : (
                      pipelineOutputs.object.map((output) => {
                        const name = safeText(output.output_name || output.outputName).trim() || 'Object output'
                        const target = safeText(output.target_class_id || output.targetClassId).trim()
                        return (
                          <div
                            className="pipeline-output-card"
                            key={name}
                            role="button"
                            tabIndex={0}
                            onClick={() => handlePreviewOutput(name)}
                          >
                            <div className="pipeline-output-header">
                              <div className="pipeline-output-title">
                                <Icon icon="cube" size={14} />
                                <span>{name}</span>
                              </div>
                            </div>
                            <div className="pipeline-output-row">
                              <div className="pipeline-output-status is-success">
                                <Icon icon="tick-circle" size={12} />
                                <span>{target ? `Target: ${target}` : 'Target class pending'}</span>
                              </div>
                            </div>
                          </div>
                        )
                      })
                    )}
                  </div>
                </div>
              ) : (
                <div className="pipeline-output-panel">
                  <div className="pipeline-output-list">
                    {pipelineOutputs.link.length === 0 ? (
                      <div className="pipeline-output-empty">
                        <Icon icon="info-sign" size={14} />
                        <span>No link outputs available yet.</span>
                      </div>
                    ) : (
                      pipelineOutputs.link.map((output) => {
                        const name = safeText(output.output_name || output.outputName).trim() || 'Link output'
                        const source = safeText(output.source_class_id || output.sourceClassId).trim()
                        const target = safeText(output.target_class_id || output.targetClassId).trim()
                        const predicate = safeText(output.predicate).trim()
                        const title = predicate ? `${name} · ${predicate}` : name
                        const relation = [source, target].filter(Boolean).join(' -> ')
                        return (
                          <div
                            className="pipeline-output-card"
                            key={name}
                            role="button"
                            tabIndex={0}
                            onClick={() => handlePreviewOutput(name)}
                          >
                            <div className="pipeline-output-header">
                              <div className="pipeline-output-title">
                                <Icon icon="link" size={14} />
                                <span>{title}</span>
                              </div>
                            </div>
                            <div className="pipeline-output-row">
                              <div className="pipeline-output-status is-success">
                                <Icon icon="tick-circle" size={12} />
                                <span>{relation || 'Link metadata pending'}</span>
                              </div>
                            </div>
                          </div>
                        )
                      })
                    )}
                  </div>
                </div>
              )}
            </div>
          </aside>
        </div>
      </div>

      <CreatePipelineDialog
        isOpen={isCreatePipelineDialogOpen}
        defaultName={createPipelineDefaults.name}
        defaultBranch={createPipelineDefaults.branch}
        isSubmitting={isCreatingPipeline}
        error={createPipelineError}
        onClose={() => {
          if (isCreatingPipeline) {
            return
          }
          setCreatePipelineDialogOpen(false)
          setCreatePipelineError(null)
        }}
        onSubmit={handleCreatePipeline}
      />

      {/* 파이프라인 마법사 다이얼로그 */}
      <PipelineWizardDialog
        isOpen={isWizardOpen}
        onClose={() => setIsWizardOpen(false)}
        onComplete={handleWizardComplete}
        datasets={datasets.map((d) => ({ id: d.dataset_id, name: d.name }))}
      />
      <UdfManageDialog
        isOpen={isUdfDialogOpen}
        onClose={() => setIsUdfDialogOpen(false)}
        dbName={activeDbName}
      />
      <DatasetSelectDialog
        isOpen={isDatasetSelectOpen}
        onClose={() => setIsDatasetSelectOpen(false)}
        onSelect={handleSelectDataset}
        datasets={datasets}
        isLoading={false}
      />
      <UploadFilesDialog
        isOpen={isFileUploadOpen}
        onClose={() => setIsFileUploadOpen(false)}
        activeFolderId={activeDbName}
      />
    </div>
  )
}
