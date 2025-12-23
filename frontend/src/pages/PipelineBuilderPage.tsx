import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Button,
  Card,
  Dialog,
  FileInput,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  Tag,
  Text,
  TextArea,
} from '@blueprintjs/core'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  createDataset,
  createDatasetVersion,
  createPipeline,
  deployPipeline,
  getPipeline,
  listBranches,
  listDatasets,
  listRegisteredSheets,
  listPipelines,
  previewPipeline,
  startPipeliningSheet,
  uploadCsvDataset,
  uploadExcelDataset,
  updatePipeline,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'
import { PipelineCanvas } from '../features/pipeline/PipelineCanvas'
import { PipelineHeader } from '../features/pipeline/PipelineHeader'
import { PipelinePreview } from '../features/pipeline/PipelinePreview'
import { PipelineToolbar } from '../features/pipeline/PipelineToolbar'
import {
  createDefaultDefinition,
  createId,
  type PipelineDefinition,
  type PipelineMode,
  type PipelineNode,
  type PipelineParameter,
  type PipelineTool,
  type PreviewColumn,
  type PreviewRow,
} from '../features/pipeline/pipelineTypes'
import '../features/pipeline/pipeline.css'

type PipelineRecord = Record<string, unknown>
type DatasetRecord = Record<string, unknown>
type RegisteredSheetRecord = Record<string, unknown>
const extractList = <T,>(payload: unknown, key: string): T[] => {
  if (!payload || typeof payload !== 'object') return []
  const data = payload as { data?: Record<string, unknown> }
  const nested = data.data?.[key]
  if (Array.isArray(nested)) return nested as T[]
  const direct = (payload as Record<string, unknown>)[key]
  if (Array.isArray(direct)) return direct as T[]
  return []
}

const extractPipeline = (payload: unknown): PipelineRecord | null => {
  if (!payload || typeof payload !== 'object') return null
  const data = payload as { data?: { pipeline?: PipelineRecord } }
  return data.data?.pipeline ?? (payload as { pipeline?: PipelineRecord }).pipeline ?? null
}

const ensureDefinition = (raw: unknown): PipelineDefinition => {
  const fallback = createDefaultDefinition()
  if (!raw || typeof raw !== 'object') return fallback
  const value = raw as Partial<PipelineDefinition>
  const nodes = Array.isArray(value.nodes) ? value.nodes : fallback.nodes
  const normalizedNodes = nodes.map((node, index) => {
    const safeNode = node as PipelineNode
    return {
      id: typeof safeNode.id === 'string' ? safeNode.id : createId('node'),
      title: safeNode.title ?? `Node ${index + 1}`,
      type: safeNode.type ?? 'transform',
      icon: safeNode.icon ?? (safeNode.type === 'output' ? 'export' : 'th'),
      x: Number.isFinite(safeNode.x) ? Number(safeNode.x) : 80,
      y: Number.isFinite(safeNode.y) ? Number(safeNode.y) : 80 + index * 140,
      subtitle: safeNode.subtitle,
      columns: Array.isArray(safeNode.columns) ? safeNode.columns : undefined,
      status: safeNode.status ?? 'success',
      metadata: safeNode.metadata ?? {},
    }
  })
  const nodeIds = new Set(normalizedNodes.map((node) => node.id))
  const edges = (Array.isArray(value.edges) ? value.edges : fallback.edges)
    .map((edge) => ({
      id: typeof edge.id === 'string' ? edge.id : createId('edge'),
      from: edge.from ?? '',
      to: edge.to ?? '',
    }))
    .filter((edge) => nodeIds.has(edge.from) && nodeIds.has(edge.to))
  const parameters = Array.isArray(value.parameters) ? value.parameters : fallback.parameters
  const outputs = Array.isArray(value.outputs) ? value.outputs : fallback.outputs
  return {
    nodes: normalizedNodes,
    edges,
    parameters: parameters.map((param) => ({
      id: typeof param.id === 'string' ? param.id : createId('param'),
      name: param.name ?? 'param',
      value: param.value ?? '',
    })),
    outputs: outputs.map((output) => ({
      id: typeof output.id === 'string' ? output.id : createId('output'),
      name: output.name ?? 'Output',
      datasetName: output.datasetName ?? 'output_dataset',
      description: output.description,
    })),
    settings: value.settings ?? fallback.settings,
  }
}

const extractColumns = (schema: unknown, fallbackType = 'String'): PreviewColumn[] => {
  if (!schema || typeof schema !== 'object') return []
  const raw = schema as Record<string, unknown>
  if (Array.isArray(raw.columns)) {
    return raw.columns
      .map((column) => {
        const col = column as Record<string, unknown>
        const key = String(col.name ?? col.key ?? '')
        if (!key) return null
        return { key, type: String(col.type ?? fallbackType) }
      })
      .filter((col): col is PreviewColumn => Boolean(col))
  }
  if (Array.isArray(raw.fields)) {
    return raw.fields
      .map((column) => {
        const col = column as Record<string, unknown>
        const key = String(col.name ?? col.key ?? '')
        if (!key) return null
        return { key, type: String(col.type ?? fallbackType) }
      })
      .filter((col): col is PreviewColumn => Boolean(col))
  }
  if (raw.properties && typeof raw.properties === 'object') {
    return Object.entries(raw.properties as Record<string, unknown>).map(([key, value]) => ({
      key,
      type: typeof value === 'object' && value ? String((value as Record<string, unknown>).type ?? fallbackType) : fallbackType,
    }))
  }
  return []
}

const extractRows = (sample: unknown): PreviewRow[] => {
  if (!sample || typeof sample !== 'object') return []
  const raw = sample as Record<string, unknown>
  if (Array.isArray(raw.rows)) {
    return raw.rows.filter((row) => row && typeof row === 'object') as PreviewRow[]
  }
  if (Array.isArray(raw.data)) {
    return raw.data.filter((row) => row && typeof row === 'object') as PreviewRow[]
  }
  return []
}

const extractColumnNames = (schema: unknown, fallbackType: string) =>
  extractColumns(schema, fallbackType).map((col) => col.key)

const buildColumnsFromManual = (raw: string): Array<{ name: string; type: string }> =>
  raw
    .split(',')
    .map((col) => col.trim())
    .filter(Boolean)
    .map((name) => ({ name, type: 'String' }))

const buildManualRows = (raw: string, columns: string[] = []): PreviewRow[] => {
  if (!raw.trim()) return []
  const lines = raw
    .split('\n')
    .map((line) => line.trim())
    .filter(Boolean)
  if (lines.length === 0) return []
  const delimiter = lines[0].includes('\t') ? '\t' : ','
  const rows: PreviewRow[] = []
  if (columns.length > 0) {
    lines.forEach((line) => {
      const cells = line.split(delimiter)
      const row: PreviewRow = {}
      columns.forEach((key, index) => {
        row[key] = cells[index]?.trim() ?? ''
      })
      rows.push(row)
    })
    return rows
  }
  const header = lines[0].split(delimiter).map((cell) => cell.trim())
  lines.slice(1).forEach((line) => {
    const cells = line.split(delimiter)
    const row: PreviewRow = {}
    header.forEach((key, index) => {
      row[key] = cells[index]?.trim() ?? ''
    })
    rows.push(row)
  })
  return rows
}

const buildDatasetNameFromFile = (fileName: string): string => {
  const trimmed = fileName.trim()
  if (!trimmed) return 'excel_dataset'
  const base = trimmed.replace(/\.[^.]+$/, '')
  return base || 'excel_dataset'
}

export const PipelineBuilderPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const queryClient = useQueryClient()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const setBranch = useAppStore((state) => state.setBranch)
  const setCommandDrawerOpen = useAppStore((state) => state.setCommandDrawerOpen)
  const commands = useAppStore((state) => state.commands)

  const uiCopy = useMemo(
    () =>
      language === 'ko'
        ? {
            header: {
              file: '파일',
              help: '도움말',
              batch: '배치',
              undo: '되돌리기',
              redo: '다시 실행',
              deploySettings: '배포 설정',
              tabs: { edit: '편집', proposals: '제안', history: '기록' },
              commands: '커맨드',
              save: '저장',
              deploy: '배포',
              projectFallback: '프로젝트',
              pipelineFallback: '파이프라인',
              newPipeline: '새 파이프라인',
              openPipeline: '파이프라인 열기',
              saveMenu: '저장',
              noPipelines: '파이프라인 없음',
              noBranches: '브랜치 없음',
            },
            toolbar: {
              tools: '도구',
              select: '선택',
              remove: '삭제',
              layout: '정렬',
              addDatasets: '데이터 추가',
              parameters: '파라미터',
              transform: '변환',
              edit: '편집',
            },
            sidebar: {
              folders: '폴더',
              inputs: '입력',
              transforms: '변환',
              outputs: '출력',
              addData: '데이터 추가',
              details: '상세 정보',
              nameLabel: '이름',
              typeLabel: '유형',
              operationLabel: '작업',
              expressionLabel: '수식',
              outputsTitle: '출력',
              noOutputs: '출력 없음',
              editNode: '노드 편집',
              selectNode: '노드를 선택하세요.',
            },
            preview: {
              title: '미리보기',
              dataPreview: '데이터 미리보기',
              noNodes: '노드 없음',
              searchPlaceholder: (count: number) => `${count}개 컬럼 검색...`,
              formatType: (type: string) => {
                const normalized = type.toLowerCase()
                if (normalized.includes('date') || normalized.includes('time')) return '날짜'
                if (normalized.includes('bool')) return '불리언'
                if (
                  normalized.includes('int') ||
                  normalized.includes('float') ||
                  normalized.includes('double') ||
                  normalized.includes('decimal') ||
                  normalized.includes('number')
                )
                  return '숫자'
                return '문자열'
              },
              showPreview: '미리보기 열기',
            },
            dialogs: {
              warnings: {
                selectTransform: '변환할 노드를 선택하세요.',
                selectEdit: '편집할 노드를 선택하세요.',
              },
              pipeline: {
                title: '새 파이프라인',
                name: '파이프라인 이름',
                location: '저장 위치',
                description: '설명',
                create: '생성',
                cancel: '취소',
              },
              dataset: {
                title: '데이터 추가',
                tabs: {
                  datasets: '데이터셋',
                  excel: 'Excel 업로드',
                  csv: 'CSV 업로드',
                  manual: '수동 입력',
                },
                callout: '추가 커넥터 관리는 데이터 연결에서 진행할 수 있습니다.',
                openConnections: '커넥터 연결하기',
                connectionsTitle: '연결된 커넥터',
                noConnections: '등록된 커넥터가 없습니다.',
                startPipelining: '파이프라인 시작',
                search: '데이터셋 검색',
                noDatasets: '데이터셋이 없습니다.',
                columns: '컬럼',
                manual: '수동 데이터셋 생성',
                datasetName: '데이터셋 이름',
                columnsPlaceholder: '컬럼 (쉼표로 구분)',
                sampleRows: '샘플 행 (선택, CSV 라인)',
                createDataset: '데이터셋 생성',
                addToGraph: '그래프에 추가',
                cancel: '취소',
                excel: {
                  title: 'Excel 업로드',
                  helper: 'Excel 파일을 업로드해 canonical dataset으로 저장합니다.',
                  file: 'Excel 파일',
                  datasetName: '데이터셋 이름',
                  sheetName: '시트 이름 (선택)',
                  preview: '미리보기',
                  previewEmpty: '파일을 업로드하면 미리보기가 표시됩니다.',
                  upload: '업로드 및 그래프 추가',
                  reset: '초기화',
                  loading: '업로드 중...',
                },
                csv: {
                  title: 'CSV 업로드',
                  helper: 'CSV 파일을 업로드해 canonical dataset으로 저장합니다.',
                  file: 'CSV 파일',
                  datasetName: '데이터셋 이름',
                  delimiter: '구분자 (선택)',
                  header: '첫 행을 헤더로 사용',
                  preview: '미리보기',
                  previewEmpty: '파일을 업로드하면 미리보기가 표시됩니다.',
                  upload: '업로드 및 그래프 추가',
                  reset: '초기화',
                },
              },
              parameters: {
                title: '파라미터',
                add: '파라미터 추가',
                namePlaceholder: '이름',
                valuePlaceholder: '값',
                cancel: '취소',
                save: '저장',
              },
              transform: {
                title: '변환 편집',
                nodeName: '노드 이름',
                operation: '작업',
                expression: '수식',
                cancel: '취소',
                save: '저장',
              },
              join: {
                title: '조인 생성',
                leftDataset: '왼쪽 데이터셋',
                rightDataset: '오른쪽 데이터셋',
                leftKey: '왼쪽 키',
                rightKey: '오른쪽 키',
                joinType: '조인 유형',
                selectNode: '노드 선택',
                selectKey: '키 선택',
                inner: '내부',
                left: '왼쪽',
                right: '오른쪽',
                full: '전체',
                cancel: '취소',
                create: '조인 생성',
              },
              visualize: {
                title: '시각화',
                body: '미리보기 데이터가 로드되면 시각화를 사용할 수 있습니다.',
                close: '닫기',
              },
              deploy: {
                title: '파이프라인 배포',
                outputDataset: '출력 데이터셋 이름',
                rowCount: '행 수',
                artifactKey: '아티팩트 키',
                cancel: '취소',
                deploy: '배포',
              },
              deploySettings: {
                title: '배포 설정',
                compute: '컴퓨트 티어',
                memory: '메모리',
                schedule: '스케줄',
                engine: '엔진',
                cancel: '취소',
                save: '저장',
              },
              output: {
                title: '출력 추가',
                outputName: '출력 이름',
                datasetName: '데이터셋 이름',
                description: '설명',
                cancel: '취소',
                add: '출력 추가',
              },
              help: {
                title: '파이프라인 빌더 도움말',
                body: '데이터셋을 추가하고 그래프에서 변환을 만든 뒤 배포해 정제된 데이터셋을 생성합니다.',
                close: '닫기',
              },
            },
            toast: {
              pipelineCreated: '파이프라인이 생성되었습니다.',
              pipelineSaved: '파이프라인이 저장되었습니다.',
              pipelineDeployed: '파이프라인이 배포되었습니다.',
              datasetCreated: '데이터셋이 생성되었습니다.',
              pipeliningStarted: '커넥터 데이터가 파이프라인에 추가되었습니다.',
            },
            labels: {
              columnsSuffix: '컬럼',
              join: '조인',
              output: '출력',
              outputDatasetFallback: 'output_dataset',
              pipelineFallback: '파이프라인',
            },
            operations: {
              filter: '필터',
              compute: '계산',
              join: '조인',
            },
            nodeTypes: {
              input: '입력',
              transform: '변환',
              output: '출력',
            },
            canvas: {
              join: '조인',
              filter: '필터',
              compute: '계산',
              visualize: '시각화',
              edit: '편집',
            },
          }
        : {
            header: {
              file: 'File',
              help: 'Help',
              batch: 'Batch',
              undo: 'Undo',
              redo: 'Redo',
              deploySettings: 'Deployment settings',
              tabs: { edit: 'Edit', proposals: 'Proposals', history: 'History' },
              commands: 'Commands',
              save: 'Save',
              deploy: 'Deploy',
              projectFallback: 'Project',
              pipelineFallback: 'Pipeline',
              newPipeline: 'New pipeline',
              openPipeline: 'Open pipeline',
              saveMenu: 'Save',
              noPipelines: 'No pipelines',
              noBranches: 'No branches',
            },
            toolbar: {
              tools: 'Tools',
              select: 'Select',
              remove: 'Remove',
              layout: 'Layout',
              addDatasets: 'Add datasets',
              parameters: 'Parameters',
              transform: 'Transform',
              edit: 'Edit',
            },
            sidebar: {
              folders: 'Folders',
              inputs: 'Inputs',
              transforms: 'Transforms',
              outputs: 'Outputs',
              addData: 'Add data',
              details: 'Details',
              nameLabel: 'Name',
              typeLabel: 'Type',
              operationLabel: 'Operation',
              expressionLabel: 'Expression',
              outputsTitle: 'Outputs',
              noOutputs: 'No outputs defined yet.',
              editNode: 'Edit node',
              selectNode: 'Select a node to see details.',
            },
            preview: {
              title: 'Preview',
              dataPreview: 'Data preview',
              noNodes: 'No nodes',
              searchPlaceholder: (count: number) => `Search ${count} columns...`,
              formatType: (type: string) => type,
              showPreview: 'Show preview',
            },
            dialogs: {
              warnings: {
                selectTransform: 'Select a node to edit a transform.',
                selectEdit: 'Select a node to edit.',
              },
              pipeline: {
                title: 'New pipeline',
                name: 'Pipeline name',
                location: 'Location',
                description: 'Description',
                create: 'Create',
                cancel: 'Cancel',
              },
                dataset: {
                  title: 'Add dataset',
                  tabs: {
                    datasets: 'Datasets',
                    excel: 'Excel upload',
                    csv: 'CSV upload',
                    manual: 'Manual',
                  },
                callout: 'Manage additional connectors in Data Connections.',
                openConnections: 'Connect a connector',
                connectionsTitle: 'Connected connectors',
                noConnections: 'No registered connectors yet.',
                startPipelining: 'Start pipelining',
                search: 'Search datasets',
                noDatasets: 'No datasets yet.',
                columns: 'columns',
                manual: 'Create manual dataset',
                datasetName: 'Dataset name',
                columnsPlaceholder: 'Columns (comma separated)',
                sampleRows: 'Sample rows (optional, CSV lines)',
                createDataset: 'Create dataset',
                addToGraph: 'Add to graph',
                cancel: 'Cancel',
                  excel: {
                    title: 'Excel upload',
                    helper: 'Upload an Excel file and save it as a canonical dataset.',
                    file: 'Excel file',
                    datasetName: 'Dataset name',
                    sheetName: 'Sheet name (optional)',
                    preview: 'Preview',
                    previewEmpty: 'Upload a file to see a preview.',
                    upload: 'Upload and add to graph',
                    reset: 'Reset',
                    loading: 'Uploading...',
                  },
                  csv: {
                    title: 'CSV upload',
                    helper: 'Upload a CSV file and save it as a canonical dataset.',
                    file: 'CSV file',
                    datasetName: 'Dataset name',
                    delimiter: 'Delimiter (optional)',
                    header: 'Use first row as header',
                    preview: 'Preview',
                    previewEmpty: 'Upload a file to see a preview.',
                    upload: 'Upload and add to graph',
                    reset: 'Reset',
                  },
                },
              parameters: {
                title: 'Parameters',
                add: 'Add parameter',
                namePlaceholder: 'Name',
                valuePlaceholder: 'Value',
                cancel: 'Cancel',
                save: 'Save',
              },
              transform: {
                title: 'Edit transform',
                nodeName: 'Node name',
                operation: 'Operation',
                expression: 'Expression',
                cancel: 'Cancel',
                save: 'Save',
              },
              join: {
                title: 'Create join',
                leftDataset: 'Left dataset',
                rightDataset: 'Right dataset',
                leftKey: 'Left key',
                rightKey: 'Right key',
                joinType: 'Join type',
                selectNode: 'Select node',
                selectKey: 'Select key',
                inner: 'Inner',
                left: 'Left',
                right: 'Right',
                full: 'Full',
                cancel: 'Cancel',
                create: 'Create join',
              },
              visualize: {
                title: 'Visualize',
                body: 'Visualization will be available once preview data is loaded.',
                close: 'Close',
              },
              deploy: {
                title: 'Deploy pipeline',
                outputDataset: 'Output dataset name',
                rowCount: 'Row count',
                artifactKey: 'Artifact key',
                cancel: 'Cancel',
                deploy: 'Deploy',
              },
              deploySettings: {
                title: 'Deployment settings',
                compute: 'Compute tier',
                memory: 'Memory',
                schedule: 'Schedule',
                engine: 'Engine',
                cancel: 'Cancel',
                save: 'Save',
              },
              output: {
                title: 'Add output',
                outputName: 'Output name',
                datasetName: 'Dataset name',
                description: 'Description',
                cancel: 'Cancel',
                add: 'Add output',
              },
              help: {
                title: 'Pipeline Builder help',
                body: 'Add datasets, build transforms on the graph, and deploy outputs to create canonical datasets.',
                close: 'Close',
              },
            },
            toast: {
              pipelineCreated: 'Pipeline created.',
              pipelineSaved: 'Pipeline saved.',
              pipelineDeployed: 'Pipeline deployed.',
              datasetCreated: 'Dataset created.',
              pipeliningStarted: 'Connector dataset added to the pipeline.',
            },
            labels: {
              columnsSuffix: 'columns',
              join: 'Join',
              output: 'Output',
              outputDatasetFallback: 'output_dataset',
              pipelineFallback: 'Pipeline',
            },
            operations: {
              filter: 'Filter',
              compute: 'Compute',
              join: 'Join',
            },
            nodeTypes: {
              input: 'Input',
              transform: 'Transform',
              output: 'Output',
            },
            canvas: {
              join: 'Join',
              filter: 'Filter',
              compute: 'Compute',
              visualize: 'Visualize',
              edit: 'Edit',
            },
          },
    [language],
  )

  const [mode, setMode] = useState<PipelineMode>('edit')
  const [activeTool, setActiveTool] = useState<PipelineTool>('tools')
  const [definition, setDefinition] = useState<PipelineDefinition>(() => createDefaultDefinition())
  const [pipelineId, setPipelineId] = useState<string | null>(null)
  const [pipelineName, setPipelineName] = useState('')
  const [pipelineLocation, setPipelineLocation] = useState(`/projects/${dbName}/pipelines`)
  const [history, setHistory] = useState<PipelineDefinition[]>([])
  const [future, setFuture] = useState<PipelineDefinition[]>([])
  const [isDirty, setIsDirty] = useState(false)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null)
  const [canvasZoom, setCanvasZoom] = useState(1)
  const [inspectorOpen, setInspectorOpen] = useState(false)
  const [previewCollapsed, setPreviewCollapsed] = useState(false)
  const [previewFullscreen, setPreviewFullscreen] = useState(false)
  const [previewSample, setPreviewSample] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)

  const [pipelineDialogOpen, setPipelineDialogOpen] = useState(false)
  const [datasetDialogOpen, setDatasetDialogOpen] = useState(false)
  const [parametersOpen, setParametersOpen] = useState(false)
  const [transformOpen, setTransformOpen] = useState(false)
  const [joinOpen, setJoinOpen] = useState(false)
  const [visualizeOpen, setVisualizeOpen] = useState(false)
  const [deployOpen, setDeployOpen] = useState(false)
  const [deploySettingsOpen, setDeploySettingsOpen] = useState(false)
  const [helpOpen, setHelpOpen] = useState(false)
  const [outputOpen, setOutputOpen] = useState(false)

  const [newPipelineName, setNewPipelineName] = useState('')
  const [newPipelineLocation, setNewPipelineLocation] = useState('')
  const [newPipelineDescription, setNewPipelineDescription] = useState('')

  const [datasetSearch, setDatasetSearch] = useState('')
  const [selectedDatasetId, setSelectedDatasetId] = useState<string | null>(null)
  const [datasetTab, setDatasetTab] = useState('datasets')
  const [manualDatasetName, setManualDatasetName] = useState('')
  const [manualColumns, setManualColumns] = useState('')
  const [manualRows, setManualRows] = useState('')
  const [pipeliningSheetId, setPipeliningSheetId] = useState<string | null>(null)
  const [excelFile, setExcelFile] = useState<File | null>(null)
  const [excelDatasetName, setExcelDatasetName] = useState('')
  const [excelSheetName, setExcelSheetName] = useState('')
  const [excelPreview, setExcelPreview] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)
  const [csvFile, setCsvFile] = useState<File | null>(null)
  const [csvDatasetName, setCsvDatasetName] = useState('')
  const [csvDelimiter, setCsvDelimiter] = useState('')
  const [csvHasHeader, setCsvHasHeader] = useState(true)
  const [csvPreview, setCsvPreview] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)

  const [parameterDrafts, setParameterDrafts] = useState<PipelineParameter[]>([])
  const [transformDraft, setTransformDraft] = useState({ title: '', operation: '', expression: '' })
  const [joinLeft, setJoinLeft] = useState('')
  const [joinRight, setJoinRight] = useState('')
  const [joinType, setJoinType] = useState('inner')
  const [joinLeftKey, setJoinLeftKey] = useState('')
  const [joinRightKey, setJoinRightKey] = useState('')
  const [outputDraft, setOutputDraft] = useState({ name: '', datasetName: '', description: '' })
  const [deployDraft, setDeployDraft] = useState({ datasetName: '', rowCount: '500', artifactKey: '' })
  const [deploySettings, setDeploySettings] = useState({ compute: 'Medium', memory: '4 GB', schedule: 'Manual', engine: 'Batch' })

  const autoPrompted = useRef(false)
  const loadedPipeline = useRef<string | null>(null)

  const pipelinesQuery = useQuery({
    queryKey: qk.pipelines(dbName, requestContext.language),
    queryFn: () => listPipelines(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const branchesQuery = useQuery({
    queryKey: qk.branches(dbName, requestContext.language),
    queryFn: () => listBranches(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const datasetsQuery = useQuery({
    queryKey: qk.datasets(dbName, requestContext.language),
    queryFn: () => listDatasets(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const registeredSheetsQuery = useQuery({
    queryKey: qk.registeredSheets(dbName, requestContext.language),
    queryFn: () => listRegisteredSheets(requestContext, dbName),
    enabled: datasetDialogOpen,
  })


  const pipelineQuery = useQuery({
    queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language),
    queryFn: () => getPipeline(requestContext, pipelineId ?? ''),
    enabled: Boolean(pipelineId),
  })

  const createPipelineMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => createPipeline(requestContext, payload),
    onSuccess: (payload) => {
      const pipeline = extractPipeline(payload)
      if (!pipeline) return
      setPipelineId(String(pipeline.pipeline_id ?? ''))
      setPipelineName(String(pipeline.name ?? ''))
      setPipelineLocation(String(pipeline.location ?? pipelineLocation))
      setDefinition(ensureDefinition(pipeline.definition_json))
      setHistory([])
      setFuture([])
      setIsDirty(false)
      setPipelineDialogOpen(false)
      void queryClient.invalidateQueries({ queryKey: qk.pipelines(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineCreated })
    },
    onError: (error) => toastApiError(error, language),
  })

  const updatePipelineMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => updatePipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      setIsDirty(false)
      void queryClient.invalidateQueries({ queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineSaved })
    },
    onError: (error) => toastApiError(error, language),
  })

  const previewMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => previewPipeline(requestContext, pipelineId ?? '', payload),
    onError: (error) => toastApiError(error, language),
  })

  const deployMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => deployPipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineDeployed })
    },
    onError: (error) => toastApiError(error, language),
  })

  const createDatasetMutation = useMutation({
    mutationFn: (payload: {
      request: Record<string, unknown>
      sample?: { columns: Array<{ name: string; type: string }>; rows: PreviewRow[] }
      autoAdd?: boolean
      source?: 'manual'
    }) => createDataset(requestContext, payload.request),
    onSuccess: (payload, variables) => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      const dataset = (payload as { data?: { dataset?: Record<string, unknown> } })?.data?.dataset
      const datasetId = typeof dataset?.dataset_id === 'string' ? dataset.dataset_id : ''
      const sampleColumns = variables?.sample?.columns ?? []
      const sampleRows = variables?.sample?.rows ?? []
      if (datasetId && sampleRows.length > 0) {
        void createDatasetVersion(requestContext, datasetId, {
          sample_json: { columns: sampleColumns, rows: sampleRows },
          schema_json: { columns: sampleColumns },
          row_count: sampleRows.length,
        })
      }
      if (variables?.autoAdd && dataset) {
        handleAddDatasetNode(dataset)
      }
      if (variables?.source === 'manual') {
        setManualDatasetName('')
        setManualColumns('')
        setManualRows('')
      }
    },
    onError: (error) => toastApiError(error, language),
  })

  const uploadExcelMutation = useMutation({
    mutationFn: (payload: { file: File; datasetName: string; sheetName?: string }) =>
      uploadExcelDataset(requestContext, dbName, {
        file: payload.file,
        datasetName: payload.datasetName,
        sheetName: payload.sheetName || undefined,
      }),
    onSuccess: (payload) => {
      const dataset = (payload as { data?: { dataset?: DatasetRecord } })?.data?.dataset
      const preview = (payload as { data?: { preview?: { columns?: Array<Record<string, unknown>>; rows?: PreviewRow[] } } })
        ?.data?.preview
      if (dataset) {
        handleAddDatasetNode(dataset)
      }
      if (preview?.columns && preview?.rows) {
        const normalizedColumns = preview.columns
          .map((col) => {
            const record = col as Record<string, unknown>
            const key = String(record.key ?? record.name ?? '').trim()
            if (!key) return null
            return {
              key,
              type: String(record.type ?? 'String'),
            }
          })
          .filter((col): col is PreviewColumn => Boolean(col))
        setExcelPreview({ columns: normalizedColumns, rows: preview.rows })
        setPreviewSample({ columns: normalizedColumns, rows: preview.rows })
      }
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      setExcelFile(null)
      setExcelDatasetName('')
      setExcelSheetName('')
    },
    onError: (error) => toastApiError(error, language),
  })

  const uploadCsvMutation = useMutation({
    mutationFn: (payload: { file: File; datasetName: string; delimiter?: string; hasHeader: boolean }) =>
      uploadCsvDataset(requestContext, dbName, {
        file: payload.file,
        datasetName: payload.datasetName,
        delimiter: payload.delimiter || undefined,
        hasHeader: payload.hasHeader,
      }),
    onSuccess: (payload) => {
      const dataset = (payload as { data?: { dataset?: DatasetRecord } })?.data?.dataset
      const preview = (payload as { data?: { preview?: { columns?: Array<Record<string, unknown>>; rows?: PreviewRow[] } } })
        ?.data?.preview
      if (dataset) {
        handleAddDatasetNode(dataset)
      }
      if (preview?.columns && preview?.rows) {
        const normalizedColumns = preview.columns
          .map((col) => {
            const record = col as Record<string, unknown>
            const key = String(record.key ?? record.name ?? '').trim()
            if (!key) return null
            return {
              key,
              type: String(record.type ?? 'xsd:string'),
            }
          })
          .filter((col): col is PreviewColumn => Boolean(col))
        setCsvPreview({ columns: normalizedColumns, rows: preview.rows })
        setPreviewSample({ columns: normalizedColumns, rows: preview.rows })
      }
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      setCsvFile(null)
      setCsvDatasetName('')
      setCsvDelimiter('')
      setCsvHasHeader(true)
    },
    onError: (error) => toastApiError(error, language),
  })

  const startPipeliningMutation = useMutation({
    mutationFn: (payload: { sheetId: string; worksheetName?: string }) =>
      startPipeliningSheet(requestContext, payload.sheetId, {
        db_name: dbName,
        worksheet_name: payload.worksheetName,
      }),
    onSuccess: (payload) => {
      const dataset = (payload as { data?: { dataset?: DatasetRecord } })?.data?.dataset
      if (dataset) {
        handleAddDatasetNode(dataset)
      }
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipeliningStarted })
    },
    onError: (error) => toastApiError(error, language),
    onSettled: () => setPipeliningSheetId(null),
  })

  const pipelines = useMemo(() => extractList<PipelineRecord>(pipelinesQuery.data, 'pipelines'), [pipelinesQuery.data])
  const pipelineOptions = useMemo(
    () =>
      pipelines.map((pipeline) => ({
        id: String(pipeline.pipeline_id ?? ''),
        name: String(pipeline.name ?? 'Pipeline'),
      })).filter((pipeline) => pipeline.id),
    [pipelines],
  )
  const branches = useMemo(() => {
    const raw = extractList<Record<string, unknown>>(branchesQuery.data, 'branches')
    return raw
      .map((branchItem) => String(branchItem.name ?? branchItem.branch_name ?? branchItem.id ?? ''))
      .filter(Boolean)
  }, [branchesQuery.data])
  const datasets = useMemo(() => extractList<DatasetRecord>(datasetsQuery.data, 'datasets'), [datasetsQuery.data])
  const registeredSheets = useMemo(
    () => extractList<RegisteredSheetRecord>(registeredSheetsQuery.data, 'sheets'),
    [registeredSheetsQuery.data],
  )
  useEffect(() => {
    if (!pipelinesQuery.data || pipelineId) return
    if (pipelines.length > 0) {
      const first = pipelines[0]
      setPipelineId(String(first.pipeline_id ?? ''))
      setPipelineName(String(first.name ?? ''))
      setPipelineLocation(String(first.location ?? pipelineLocation))
      return
    }
    if (autoPrompted.current) return
    autoPrompted.current = true
    setPipelineDialogOpen(true)
  }, [pipelines, pipelinesQuery.data, pipelineId, pipelineLocation])

  useEffect(() => {
    if (!pipelineQuery.data || !pipelineId) return
    if (loadedPipeline.current === pipelineId && isDirty) return
    const pipeline = extractPipeline(pipelineQuery.data)
    if (!pipeline) return
    loadedPipeline.current = pipelineId
    setPipelineName(String(pipeline.name ?? ''))
    setPipelineLocation(String(pipeline.location ?? pipelineLocation))
    setDefinition(ensureDefinition(pipeline.definition_json))
    setHistory([])
    setFuture([])
    setIsDirty(false)
  }, [pipelineQuery.data, pipelineId, pipelineLocation, isDirty])

  useEffect(() => {
    if (!datasetDialogOpen) return
    setDatasetTab('datasets')
    setSelectedDatasetId(null)
    setDatasetSearch('')
    setExcelFile(null)
    setExcelDatasetName('')
    setExcelSheetName('')
    setExcelPreview(null)
    setCsvFile(null)
    setCsvDatasetName('')
    setCsvDelimiter('')
    setCsvHasHeader(true)
    setCsvPreview(null)
  }, [datasetDialogOpen])

  useEffect(() => {
    setNewPipelineLocation(pipelineLocation)
  }, [pipelineLocation])

  useEffect(() => {
    if (!parametersOpen) return
    setParameterDrafts(definition.parameters)
  }, [parametersOpen, definition.parameters])

  useEffect(() => {
    if (!excelFile) {
      setExcelPreview(null)
      return
    }
    if (!excelDatasetName.trim()) {
      setExcelDatasetName(buildDatasetNameFromFile(excelFile.name))
    }
    setExcelPreview(null)
  }, [excelFile, excelDatasetName])

  useEffect(() => {
    if (!csvFile) {
      setCsvPreview(null)
      return
    }
    if (!csvDatasetName.trim()) {
      setCsvDatasetName(buildDatasetNameFromFile(csvFile.name))
    }
    setCsvPreview(null)
  }, [csvFile, csvDatasetName])

  useEffect(() => {
    if (!transformOpen) return
    const node = definition.nodes.find((item) => item.id === selectedNodeId)
    if (!node) return
    setTransformDraft({
      title: node.title,
      operation: node.metadata?.operation ?? '',
      expression: node.metadata?.expression ?? '',
    })
  }, [transformOpen, definition.nodes, selectedNodeId])

  useEffect(() => {
    if (!joinOpen) return
    setJoinLeft(selectedNodeId ?? '')
    setJoinRight('')
    setJoinType('inner')
    setJoinLeftKey('')
    setJoinRightKey('')
  }, [joinOpen, selectedNodeId])

  useEffect(() => {
    if (!outputOpen) return
    setOutputDraft({ name: '', datasetName: '', description: '' })
  }, [outputOpen])

  useEffect(() => {
    if (!deploySettingsOpen) return
    setDeploySettings((current) => ({
      compute: definition.settings?.compute ?? current.compute,
      memory: definition.settings?.memory ?? current.memory,
      schedule: definition.settings?.schedule ?? current.schedule,
      engine: definition.settings?.engine ?? current.engine,
    }))
  }, [deploySettingsOpen, definition.settings])

  const selectedNode = useMemo(
    () => definition.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [definition.nodes, selectedNodeId],
  )

  const previewNode = useMemo(() => {
    const candidate = definition.nodes.find((node) => node.id === previewNodeId)
    return candidate ?? definition.nodes[0] ?? null
  }, [definition.nodes, previewNodeId])

  useEffect(() => {
    setPreviewSample(null)
  }, [previewNode?.id])

  const previewSchema = useMemo(() => {
    if (previewSample?.columns?.length) return previewSample.columns
    if (!previewNode) return [] as PreviewColumn[]
    const datasetId = previewNode.metadata?.datasetId
    if (datasetId) {
      const dataset = datasets.find((item) => String(item.dataset_id ?? '') === datasetId)
      if (dataset) {
        const columns = extractColumns(dataset.schema_json, language === 'ko' ? '문자열' : 'String')
        if (columns.length) return columns
        const sampleColumns = extractColumns(dataset.sample_json, language === 'ko' ? '문자열' : 'String')
        if (sampleColumns.length) return sampleColumns
        const latestColumns = extractColumns(dataset.latest_sample_json, language === 'ko' ? '문자열' : 'String')
        if (latestColumns.length) return latestColumns
      }
    }
    const fallbackNames = previewNode.columns ?? []
    if (fallbackNames.length) {
      return fallbackNames.map((name, index) => ({
        key: name.replace(/\s+/g, '_').toLowerCase() || `col_${index + 1}`,
        type: language === 'ko' ? '문자열' : 'String',
      }))
    }
    return []
  }, [previewNode, datasets, previewSample, language])

  const getNodeColumnNames = (nodeId: string) => {
    const node = definition.nodes.find((item) => item.id === nodeId)
    if (!node) return [] as string[]
    if (node.columns?.length) return node.columns
    const datasetId = node.metadata?.datasetId
    if (!datasetId) return [] as string[]
    const dataset = datasets.find((item) => String(item.dataset_id ?? '') === String(datasetId))
    if (!dataset) return [] as string[]
    const schemaColumns = extractColumnNames(dataset.schema_json, language === 'ko' ? '문자열' : 'String')
    if (schemaColumns.length) return schemaColumns
    const sampleColumns = extractColumnNames(dataset.sample_json, language === 'ko' ? '문자열' : 'String')
    if (sampleColumns.length) return sampleColumns
    const latestColumns = extractColumnNames(dataset.latest_sample_json, language === 'ko' ? '문자열' : 'String')
    if (latestColumns.length) return latestColumns
    return []
  }

  const previewRows = useMemo(() => {
    if (previewSample?.rows?.length) return previewSample.rows
    if (!previewNode) return []
    const datasetId = previewNode.metadata?.datasetId
    if (datasetId) {
      const dataset = datasets.find((item) => String(item.dataset_id ?? '') === datasetId)
      const rows = extractRows(dataset?.sample_json)
      if (rows.length) return rows
      const latestRows = extractRows(dataset?.latest_sample_json)
      if (latestRows.length) return latestRows
    }
    return []
  }, [previewNode, datasets, previewSample])

  useEffect(() => {
    if (!pipelineId || !previewNode) return
    if (previewMutation.isPending) return
    previewMutation.mutate({
      db_name: dbName,
      definition_json: definition,
      node_id: previewNode.id,
      limit: 200,
    })
  }, [pipelineId, previewNode, definition, dbName, previewMutation])

  useEffect(() => {
    if (!previewMutation.data) return
    const sample = (previewMutation.data as { data?: { sample?: Record<string, unknown> } })?.data?.sample
    if (!sample || typeof sample !== 'object') return
    setPreviewSample({
      columns: extractColumns(sample),
      rows: extractRows(sample),
    })
  }, [previewMutation.data])

  useEffect(() => {
    if (!previewFullscreen) return
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setPreviewFullscreen(false)
      }
    }
    window.addEventListener('keydown', handleKey)
    return () => window.removeEventListener('keydown', handleKey)
  }, [previewFullscreen])

  const updateDefinition = (updater: (current: PipelineDefinition) => PipelineDefinition) => {
    setDefinition((current) => {
      const next = updater(current)
      setHistory((prev) => [...prev, current])
      setFuture([])
      setIsDirty(true)
      return next
    })
  }

  const removeNode = (nodeId: string) => {
    updateDefinition((current) => ({
      ...current,
      nodes: current.nodes.filter((node) => node.id !== nodeId),
      edges: current.edges.filter((edge) => edge.from !== nodeId && edge.to !== nodeId),
    }))
    setSelectedNodeId(null)
  }

  const handleSelectNode = (nodeId: string | null) => {
    if (!nodeId) {
      setSelectedNodeId(null)
      setInspectorOpen(false)
      return
    }
    if (activeTool === 'remove') {
      removeNode(nodeId)
      return
    }
    setSelectedNodeId(nodeId)
    setPreviewNodeId(nodeId)
    setInspectorOpen(true)
    setPreviewCollapsed(false)
  }

  const handleLayout = () => {
    updateDefinition((current) => {
      const grouped = {
        input: current.nodes.filter((node) => node.type === 'input'),
        transform: current.nodes.filter((node) => node.type === 'transform'),
        output: current.nodes.filter((node) => node.type === 'output'),
      }
      const spaced = (nodes: PipelineNode[], x: number) =>
        nodes
          .slice()
          .sort((a, b) => a.y - b.y)
          .map((node, index) => ({ ...node, x, y: 80 + index * 140 }))
      return {
        ...current,
        nodes: [...spaced(grouped.input, 80), ...spaced(grouped.transform, 380), ...spaced(grouped.output, 920)],
      }
    })
  }

  const handleSave = () => {
    if (!pipelineId) return
    updatePipelineMutation.mutate({
      name: pipelineName,
      location: pipelineLocation,
      pipeline_type: 'batch',
      definition_json: definition,
    })
  }

  const handleUndo = () => {
    setHistory((prev) => {
      if (prev.length === 0) return prev
      const last = prev[prev.length - 1]
      setFuture((next) => [definition, ...next])
      setDefinition(last)
      setIsDirty(true)
      return prev.slice(0, -1)
    })
  }

  const handleRedo = () => {
    setFuture((prev) => {
      if (prev.length === 0) return prev
      const [next, ...rest] = prev
      setHistory((historyPrev) => [...historyPrev, definition])
      setDefinition(next)
      setIsDirty(true)
      return rest
    })
  }

  const handleAddDatasetNode = (dataset: DatasetRecord) => {
    const datasetId = String(dataset.dataset_id ?? '')
    const columnCount = extractColumns(dataset.schema_json, language === 'ko' ? '문자열' : 'String').length
    const nodeId = createId('node')
    updateDefinition((current) => {
      const inputs = current.nodes.filter((node) => node.type === 'input')
      const nextY = inputs.length > 0 ? Math.max(...inputs.map((node) => node.y)) + 140 : 80
      const node: PipelineNode = {
        id: nodeId,
        title: String(dataset.name ?? 'dataset'),
        type: 'input',
        icon: 'th',
        x: 80,
        y: nextY,
        subtitle: columnCount ? `${columnCount} ${uiCopy.labels.columnsSuffix}` : undefined,
        status: 'success',
        metadata: { datasetId },
      }
      return { ...current, nodes: [...current.nodes, node] }
    })
    setSelectedNodeId(nodeId)
    setPreviewNodeId(nodeId)
    setInspectorOpen(true)
    setPreviewCollapsed(false)
    setDatasetDialogOpen(false)
  }


  const handleSimpleTransform = (operation: string, icon: PipelineNode['icon']) => {
    if (!selectedNode) return
    updateDefinition((current) => {
      const source = current.nodes.find((node) => node.id === selectedNode.id)
      if (!source) return current
      const node: PipelineNode = {
        id: createId('node'),
        title: `${source.title} · ${operation}`,
        type: 'transform',
        icon,
        x: source.x + 260,
        y: source.y,
        subtitle: source.subtitle,
        status: 'success',
        metadata: { operation },
      }
      return {
        ...current,
        nodes: [...current.nodes, node],
        edges: [...current.edges, { id: createId('edge'), from: source.id, to: node.id }],
      }
    })
  }

  const handleJoin = () => {
    if (!joinLeft || !joinRight) return
    updateDefinition((current) => {
      const leftNode = current.nodes.find((node) => node.id === joinLeft)
      const rightNode = current.nodes.find((node) => node.id === joinRight)
      if (!leftNode || !rightNode) return current
      const node: PipelineNode = {
        id: createId('node'),
        title: uiCopy.labels.join,
        type: 'transform',
        icon: 'inner-join',
        x: Math.max(leftNode.x, rightNode.x) + 260,
        y: Math.round((leftNode.y + rightNode.y) / 2),
        status: 'success',
        metadata: {
          operation: 'join',
          joinType,
          leftKey: joinLeftKey || undefined,
          rightKey: joinRightKey || undefined,
          joinKey: joinLeftKey && joinLeftKey === joinRightKey ? joinLeftKey : undefined,
        },
      }
      return {
        ...current,
        nodes: [...current.nodes, node],
        edges: [
          ...current.edges,
          { id: createId('edge'), from: leftNode.id, to: node.id },
          { id: createId('edge'), from: rightNode.id, to: node.id },
        ],
      }
    })
    setJoinOpen(false)
  }

  const handleTransformSave = () => {
    if (!selectedNode) return
    updateDefinition((current) => ({
      ...current,
      nodes: current.nodes.map((node) =>
        node.id === selectedNode.id
          ? {
              ...node,
              title: transformDraft.title || node.title,
              metadata: { ...node.metadata, operation: transformDraft.operation, expression: transformDraft.expression },
            }
          : node,
      ),
    }))
    setTransformOpen(false)
  }

  const parseScheduleInterval = (schedule: string) => {
    const normalized = schedule.trim().toLowerCase()
    if (!normalized || normalized === 'manual') return null
    const minuteMatch = normalized.match(/^(\d+)\s*m(in)?$/)
    if (minuteMatch) return Number(minuteMatch[1]) * 60
    const hourMatch = normalized.match(/^(\d+)\s*h(our)?s?$/)
    if (hourMatch) return Number(hourMatch[1]) * 3600
    const dayMatch = normalized.match(/^(\d+)\s*d(ay)?s?$/)
    if (dayMatch) return Number(dayMatch[1]) * 86400
    const numeric = Number(normalized)
    if (Number.isFinite(numeric)) return numeric
    return null
  }

  const handleDeploy = () => {
    if (!pipelineId) return
    const outputDatasetName = deployDraft.datasetName || definition.outputs[0]?.datasetName || uiCopy.labels.outputDatasetFallback
    const outputNodeId = definition.nodes.find((node) => node.type === 'output')?.id
    const scheduleInterval = parseScheduleInterval(deploySettings.schedule || '')
    deployMutation.mutate({
      db_name: dbName,
      definition_json: definition,
      node_id: outputNodeId,
      output: {
        db_name: dbName,
        dataset_name: outputDatasetName,
      },
      schedule: scheduleInterval ? { interval_seconds: scheduleInterval } : undefined,
    })
    setDeployOpen(false)
  }

  const handleSaveParameters = () => {
    updateDefinition((current) => ({ ...current, parameters: parameterDrafts }))
    setParametersOpen(false)
  }

  const handleSaveSettings = () => {
    updateDefinition((current) => ({ ...current, settings: { ...current.settings, ...deploySettings } }))
    setDeploySettingsOpen(false)
  }

  const handleAddOutput = () => {
    updateDefinition((current) => {
      const outputId = createId('output')
      const outputName = outputDraft.name || uiCopy.labels.output
      const outputDataset = outputDraft.datasetName || uiCopy.labels.outputDatasetFallback
      const outputs = [
        ...current.outputs,
        {
          id: outputId,
          name: outputName,
          datasetName: outputDataset,
          description: outputDraft.description || undefined,
        },
      ]
      const outputNodes = current.nodes.filter((node) => node.type === 'output')
      const nextY = outputNodes.length > 0 ? Math.max(...outputNodes.map((node) => node.y)) + 140 : 80
      const outputNode: PipelineNode = {
        id: createId('node'),
        title: outputName,
        type: 'output',
        icon: 'export',
        x: 920,
        y: nextY,
        subtitle: outputDataset,
        status: 'success',
      }
      const edges = selectedNode
        ? [...current.edges, { id: createId('edge'), from: selectedNode.id, to: outputNode.id }]
        : current.edges
      return { ...current, outputs, nodes: [...current.nodes, outputNode], edges }
    })
    setOutputOpen(false)
  }

  const handleCreateManualDataset = () => {
    if (!manualDatasetName.trim()) return
    const columns = buildColumnsFromManual(manualColumns)
    const columnNames = columns.map((col) => col.name)
    const sampleRows = buildManualRows(manualRows, columnNames)
    createDatasetMutation.mutate({
      request: {
        db_name: dbName,
        name: manualDatasetName.trim(),
        source_type: 'manual',
        schema_json: { columns },
      },
      sample: { columns, rows: sampleRows },
      autoAdd: true,
      source: 'manual',
    })
  }

  const handleNodeAction = (action: 'join' | 'filter' | 'compute' | 'visualize' | 'edit') => {
    if (!selectedNode) return
    if (action === 'join') {
      setJoinOpen(true)
      return
    }
    if (action === 'filter') {
      handleSimpleTransform('filter', 'filter')
      return
    }
    if (action === 'compute') {
      handleSimpleTransform('compute', 'function')
      return
    }
    if (action === 'visualize') {
      setVisualizeOpen(true)
      return
    }
    setTransformOpen(true)
  }

  const datasetOptions = useMemo(() => {
    const normalized = datasetSearch.trim().toLowerCase()
    return datasets.filter((dataset) =>
      String(dataset.name ?? '').toLowerCase().includes(normalized),
    )
  }, [datasets, datasetSearch])

  const nodeOptions = definition.nodes.map((node) => ({ id: node.id, title: node.title }))
  const canUndo = history.length > 0
  const canRedo = future.length > 0
  const activeCommandCount = useMemo(
    () =>
      Object.values(commands).filter((cmd) => {
        if (cmd.expired) return false
        if (cmd.writePhase === 'FAILED' || cmd.writePhase === 'CANCELLED') return false
        if (cmd.writePhase === 'WRITE_DONE' && cmd.indexPhase === 'VISIBLE_IN_SEARCH') return false
        return true
      }).length,
    [commands],
  )


  const datasetsPanel = (
    <div className="dataset-tab">
      <FormGroup label={uiCopy.dialogs.dataset.search}>
        <InputGroup value={datasetSearch} onChange={(event) => setDatasetSearch(event.currentTarget.value)} />
      </FormGroup>
      <div className="dataset-connector-section">
        <Text className="dataset-connector-callout">{uiCopy.dialogs.dataset.callout}</Text>
        <Button
          className="dataset-connector-cta"
          intent={Intent.PRIMARY}
          icon="link"
          rightIcon="arrow-right"
          onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/data/sheets`)}
        >
          {uiCopy.dialogs.dataset.openConnections}
        </Button>
        <div className="dataset-connector-list">
          <Text className="dataset-connector-title">{uiCopy.dialogs.dataset.connectionsTitle}</Text>
          {registeredSheets.length === 0 ? (
            <Text className="muted">{uiCopy.dialogs.dataset.noConnections}</Text>
          ) : (
            registeredSheets.map((sheet) => {
              const sheetId = String((sheet as Record<string, unknown>).sheet_id ?? '')
              const sheetTitle = String((sheet as Record<string, unknown>).sheet_title ?? '')
              const worksheet = String((sheet as Record<string, unknown>).worksheet_name ?? '')
              const sheetUrl = String((sheet as Record<string, unknown>).sheet_url ?? '')
              const label = sheetTitle || sheetUrl || worksheet || sheetId
              return (
                <div key={sheetId} className="dataset-connector-row">
                  <div>
                    <Text>{label}</Text>
                    <Text className="muted">{sheetId}</Text>
                  </div>
                  <Button
                    small
                    intent={Intent.PRIMARY}
                    disabled={!sheetId || pipeliningSheetId === sheetId || startPipeliningMutation.isPending}
                    loading={pipeliningSheetId === sheetId && startPipeliningMutation.isPending}
                    onClick={() => {
                      setPipeliningSheetId(sheetId)
                      startPipeliningMutation.mutate({ sheetId, worksheetName: worksheet || undefined })
                    }}
                  >
                    {uiCopy.dialogs.dataset.startPipelining}
                  </Button>
                </div>
              )
            })
          )}
        </div>
      </div>
      <div className="dialog-scroll">
        {datasetOptions.length === 0 ? (
          <Text className="muted">{uiCopy.dialogs.dataset.noDatasets}</Text>
        ) : (
          datasetOptions.map((dataset) => {
            const datasetId = String(dataset.dataset_id ?? '')
            const columnCount = extractColumns(dataset.schema_json).length
            return (
              <Card
                key={datasetId}
                className={`dataset-card ${datasetId === selectedDatasetId ? 'is-selected' : ''}`}
                onClick={() => setSelectedDatasetId(datasetId)}
              >
                <Text>{String(dataset.name ?? datasetId)}</Text>
                <Text className="muted">{columnCount} {uiCopy.dialogs.dataset.columns}</Text>
              </Card>
            )
          })
        )}
      </div>
    </div>
  )

  const manualPanel = (
    <div className="dataset-tab">
      <FormGroup label={uiCopy.dialogs.dataset.manual}>
        <InputGroup
          placeholder={uiCopy.dialogs.dataset.datasetName}
          value={manualDatasetName}
          onChange={(event) => setManualDatasetName(event.currentTarget.value)}
        />
        <TextArea
          placeholder={uiCopy.dialogs.dataset.columnsPlaceholder}
          value={manualColumns}
          onChange={(event) => setManualColumns(event.currentTarget.value)}
        />
        <TextArea
          placeholder={uiCopy.dialogs.dataset.sampleRows}
          value={manualRows}
          onChange={(event) => setManualRows(event.currentTarget.value)}
        />
        <Button
          icon="add"
          minimal
          onClick={handleCreateManualDataset}
          disabled={!manualDatasetName.trim() || createDatasetMutation.isPending}
          loading={createDatasetMutation.isPending}
        >
          {uiCopy.dialogs.dataset.createDataset}
        </Button>
      </FormGroup>
    </div>
  )

  const excelPanel = (
    <div className="dataset-tab">
      <div className="dataset-preview-panel">
        <Text className="dataset-section-title">{uiCopy.dialogs.dataset.excel.title}</Text>
        <Text className="muted">{uiCopy.dialogs.dataset.excel.helper}</Text>
      </div>
      <FormGroup label={uiCopy.dialogs.dataset.excel.file}>
        <FileInput
          text={excelFile?.name ?? uiCopy.dialogs.dataset.excel.file}
          inputProps={{ accept: '.xlsx,.xlsm' }}
          onInputChange={(event) => setExcelFile(event.currentTarget.files?.[0] ?? null)}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.excel.datasetName}>
        <InputGroup
          value={excelDatasetName}
          onChange={(event) => setExcelDatasetName(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.excel.sheetName}>
        <InputGroup
          value={excelSheetName}
          onChange={(event) => setExcelSheetName(event.currentTarget.value)}
        />
      </FormGroup>
      <div className="dataset-card-actions">
        <Button
          intent={Intent.PRIMARY}
          icon="upload"
          onClick={() => {
            if (!excelFile) return
            uploadExcelMutation.mutate({
              file: excelFile,
              datasetName: excelDatasetName.trim() || buildDatasetNameFromFile(excelFile.name),
              sheetName: excelSheetName.trim() || undefined,
            })
          }}
          disabled={!excelFile || !excelDatasetName.trim() || uploadExcelMutation.isPending}
          loading={uploadExcelMutation.isPending}
        >
          {uiCopy.dialogs.dataset.excel.upload}
        </Button>
        <Button
          minimal
          icon="refresh"
          onClick={() => {
            setExcelFile(null)
            setExcelDatasetName('')
            setExcelSheetName('')
            setExcelPreview(null)
          }}
        >
          {uiCopy.dialogs.dataset.excel.reset}
        </Button>
      </div>
      <div className="dataset-preview-panel">
        <div className="dataset-preview-header">
          <Text>{uiCopy.dialogs.dataset.excel.preview}</Text>
        </div>
        {excelPreview?.columns?.length ? (
          <div className="dataset-preview-table">
            <div className="preview-sample-card">
              <div className="preview-sample-row preview-sample-header">
                {excelPreview.columns.map((column) => (
                  <span key={column.key}>{column.key}</span>
                ))}
              </div>
              {(excelPreview.rows ?? []).slice(0, 6).map((row, index) => (
                <div key={index} className="preview-sample-row">
                  {excelPreview.columns.map((column) => (
                    <span key={column.key} title={String(row?.[column.key] ?? '')}>
                      {String(row?.[column.key] ?? '')}
                    </span>
                  ))}
                </div>
              ))}
            </div>
          </div>
        ) : (
          <Text className="muted">{uiCopy.dialogs.dataset.excel.previewEmpty}</Text>
        )}
      </div>
    </div>
  )

  const csvPanel = (
    <div className="dataset-tab">
      <div className="dataset-preview-panel">
        <Text className="dataset-section-title">{uiCopy.dialogs.dataset.csv.title}</Text>
        <Text className="muted">{uiCopy.dialogs.dataset.csv.helper}</Text>
      </div>
      <FormGroup label={uiCopy.dialogs.dataset.csv.file}>
        <FileInput
          text={csvFile?.name ?? uiCopy.dialogs.dataset.csv.file}
          inputProps={{ accept: '.csv' }}
          onInputChange={(event) => setCsvFile(event.currentTarget.files?.[0] ?? null)}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.csv.datasetName}>
        <InputGroup
          value={csvDatasetName}
          onChange={(event) => setCsvDatasetName(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.csv.delimiter}>
        <InputGroup
          value={csvDelimiter}
          onChange={(event) => setCsvDelimiter(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup>
        <label className="bp5-control bp5-switch">
          <input
            type="checkbox"
            checked={csvHasHeader}
            onChange={(event) => setCsvHasHeader(event.currentTarget.checked)}
          />
          <span className="bp5-control-indicator" />
          {uiCopy.dialogs.dataset.csv.header}
        </label>
      </FormGroup>
      <div className="dataset-card-actions">
        <Button
          intent={Intent.PRIMARY}
          icon="upload"
          onClick={() => {
            if (!csvFile) return
            uploadCsvMutation.mutate({
              file: csvFile,
              datasetName: csvDatasetName.trim() || buildDatasetNameFromFile(csvFile.name),
              delimiter: csvDelimiter.trim() || undefined,
              hasHeader: csvHasHeader,
            })
          }}
          disabled={!csvFile || !csvDatasetName.trim() || uploadCsvMutation.isPending}
          loading={uploadCsvMutation.isPending}
        >
          {uiCopy.dialogs.dataset.csv.upload}
        </Button>
        <Button
          minimal
          icon="refresh"
          onClick={() => {
            setCsvFile(null)
            setCsvDatasetName('')
            setCsvDelimiter('')
            setCsvHasHeader(true)
            setCsvPreview(null)
          }}
        >
          {uiCopy.dialogs.dataset.csv.reset}
        </Button>
      </div>
      <div className="dataset-preview-panel">
        <div className="dataset-preview-header">
          <Text>{uiCopy.dialogs.dataset.csv.preview}</Text>
        </div>
        {csvPreview?.columns?.length ? (
          <div className="dataset-preview-table">
            <div className="preview-sample-card">
              <div className="preview-sample-row preview-sample-header">
                {csvPreview.columns.map((column) => (
                  <span key={column.key}>{column.key}</span>
                ))}
              </div>
              {(csvPreview.rows ?? []).slice(0, 6).map((row, index) => (
                <div key={index} className="preview-sample-row">
                  {csvPreview.columns.map((column) => (
                    <span key={column.key} title={String(row?.[column.key] ?? '')}>
                      {String(row?.[column.key] ?? '')}
                    </span>
                  ))}
                </div>
              ))}
            </div>
          </div>
        ) : (
          <Text className="muted">{uiCopy.dialogs.dataset.csv.previewEmpty}</Text>
        )}
      </div>
    </div>
  )


  return (
    <div className="pipeline-builder-container">
      <PipelineHeader
        dbName={dbName}
        pipelineName={pipelineName || uiCopy.labels.pipelineFallback}
        pipelines={pipelineOptions}
        mode={mode}
        branch={branch}
        branches={branches}
        activeCommandCount={activeCommandCount}
        copy={uiCopy.header}
        canUndo={canUndo}
        canRedo={canRedo}
        isDirty={isDirty}
        onModeChange={setMode}
        onUndo={handleUndo}
        onRedo={handleRedo}
        onBranchSelect={setBranch}
        onPipelineSelect={(id) => {
          setPipelineId(id)
          setPipelineName('')
          setPipelineLocation(pipelineLocation)
        }}
        onSave={handleSave}
        onDeploy={() => {
          setDeployDraft({
            datasetName: definition.outputs[0]?.datasetName ?? '',
            rowCount: '500',
            artifactKey: '',
          })
          setDeployOpen(true)
        }}
        onOpenDeploySettings={() => setDeploySettingsOpen(true)}
        onOpenHelp={() => setHelpOpen(true)}
        onCreatePipeline={() => setPipelineDialogOpen(true)}
        onOpenCommandDrawer={() => setCommandDrawerOpen(true)}
      />

      <PipelineToolbar
        activeTool={activeTool}
        copy={uiCopy.toolbar}
        onToolChange={setActiveTool}
        onLayout={handleLayout}
        onRemove={() => {
          if (selectedNodeId) {
            removeNode(selectedNodeId)
            return
          }
          setActiveTool('remove')
        }}
        onAddDatasets={() => setDatasetDialogOpen(true)}
        onParameters={() => setParametersOpen(true)}
        onTransform={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          setTransformOpen(true)
        }}
        onEdit={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectEdit })
            return
          }
          setTransformOpen(true)
        }}
      />

      <div className={`pipeline-body ${inspectorOpen ? 'has-inspector' : 'no-inspector'}`}>
        <aside className="pipeline-sidebar pipeline-sidebar-left">
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.folders}</div>
            <div className="pipeline-sidebar-item">
              <Tag minimal icon="folder-close">{pipelineLocation}</Tag>
            </div>
          </div>
          <div className="pipeline-divider" />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.inputs}</div>
            {definition.nodes.filter((node) => node.type === 'input').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
          <div className="pipeline-divider" />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.transforms}</div>
            {definition.nodes.filter((node) => node.type === 'transform').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
          <div className="pipeline-divider" />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.outputs}</div>
            {definition.nodes.filter((node) => node.type === 'output').map((node) => (
              <button
                key={node.id}
                type="button"
                className={`pipeline-sidebar-item ${node.id === selectedNodeId ? 'is-active' : ''}`}
                onClick={() => handleSelectNode(node.id)}
              >
                {node.title}
              </button>
            ))}
          </div>
        </aside>

        <div className="pipeline-canvas-area">
          <div className="canvas-zoom-controls">
            <Button icon="zoom-in" minimal onClick={() => setCanvasZoom((current) => Math.min(1.4, current + 0.1))} />
            <Button icon="zoom-out" minimal onClick={() => setCanvasZoom((current) => Math.max(0.6, current - 0.1))} />
            <Button icon="zoom-to-fit" minimal onClick={() => setCanvasZoom(1)} />
          </div>
          <PipelineCanvas
            nodes={definition.nodes}
            edges={definition.edges}
            selectedNodeId={selectedNodeId}
            copy={uiCopy.canvas}
            zoom={canvasZoom}
            onSelectNode={handleSelectNode}
            onNodeAction={(action) => handleNodeAction(action)}
          />
        </div>

        <aside className={`pipeline-sidebar pipeline-sidebar-right ${inspectorOpen ? 'is-open' : 'is-collapsed'}`}>
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-header">
              <div className="pipeline-sidebar-title">{uiCopy.sidebar.details}</div>
              <Button
                icon="chevron-right"
                minimal
                small
                aria-label={language === 'ko' ? '인스펙터 접기' : 'Collapse inspector'}
                onClick={() => setInspectorOpen(false)}
              />
            </div>
            {selectedNode ? (
              <Card className="pipeline-detail-card">
                <Text className="pipeline-detail-label">{uiCopy.sidebar.nameLabel}</Text>
                <Text>{selectedNode.title}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.typeLabel}</Text>
                <Text>{uiCopy.nodeTypes[selectedNode.type] ?? selectedNode.type}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.operationLabel}</Text>
                <Text>{selectedNode.metadata?.operation ? uiCopy.operations[selectedNode.metadata.operation as keyof typeof uiCopy.operations] ?? selectedNode.metadata.operation : '—'}</Text>
                <Text className="pipeline-detail-label">{uiCopy.sidebar.expressionLabel}</Text>
                <Text>{selectedNode.metadata?.expression ?? '—'}</Text>
                <Button icon="edit" minimal small onClick={() => setTransformOpen(true)}>{uiCopy.sidebar.editNode}</Button>
              </Card>
            ) : (
              <Text className="muted">{uiCopy.sidebar.selectNode}</Text>
            )}
          </div>
          <div className="pipeline-divider" />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.outputsTitle}</div>
            {definition.outputs.length === 0 ? (
              <Text className="muted">{uiCopy.sidebar.noOutputs}</Text>
            ) : (
              definition.outputs.map((output) => (
                <Card key={output.id} className="pipeline-output-card">
                  <Text>{output.name}</Text>
                  <Text className="muted">{output.datasetName}</Text>
                </Card>
              ))
            )}
            <Button icon="add" minimal small onClick={() => setOutputOpen(true)}>{uiCopy.dialogs.output.add}</Button>
          </div>
        </aside>
      </div>

      {previewCollapsed ? (
        <div className="preview-collapsed">
          <Button
            icon="chevron-up"
            minimal
            small
            aria-label={uiCopy.preview.showPreview}
            onClick={() => setPreviewCollapsed(false)}
          >
            {uiCopy.preview.showPreview}
          </Button>
        </div>
      ) : (
        <PipelinePreview
          nodes={nodeOptions}
          selectedNodeId={previewNode?.id ?? null}
          columns={previewSchema}
          rows={previewRows}
          copy={uiCopy.preview}
          onSelectNode={(nodeId) => setPreviewNodeId(nodeId)}
          onToggleFullscreen={() => {
            setPreviewFullscreen((current) => !current)
          }}
          onCollapse={() => {
            setPreviewCollapsed(true)
            setPreviewFullscreen(false)
          }}
          fullscreen={previewFullscreen}
        />
      )}

      <Dialog isOpen={pipelineDialogOpen} onClose={() => setPipelineDialogOpen(false)} title={uiCopy.dialogs.pipeline.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.pipeline.name}>
            <InputGroup value={newPipelineName} onChange={(event) => setNewPipelineName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.location}>
            <InputGroup value={newPipelineLocation} onChange={(event) => setNewPipelineLocation(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.description}>
            <TextArea value={newPipelineDescription} onChange={(event) => setNewPipelineDescription(event.currentTarget.value)} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setPipelineDialogOpen(false)}>{uiCopy.dialogs.pipeline.cancel}</Button>
          <Button
            intent={Intent.PRIMARY}
            onClick={() =>
              createPipelineMutation.mutate({
                db_name: dbName,
                name: newPipelineName.trim(),
                pipeline_type: 'batch',
                location: newPipelineLocation.trim(),
                description: newPipelineDescription.trim() || undefined,
                definition_json: definition,
              })
            }
            disabled={!newPipelineName.trim() || !newPipelineLocation.trim()}
            loading={createPipelineMutation.isPending}
          >
            {uiCopy.dialogs.pipeline.create}
          </Button>
        </div>
      </Dialog>

      <Dialog isOpen={datasetDialogOpen} onClose={() => setDatasetDialogOpen(false)} title={uiCopy.dialogs.dataset.title}>
        <div className="dialog-body">
          <Tabs id="dataset-tabs" selectedTabId={datasetTab} onChange={(tabId) => setDatasetTab(String(tabId))}>
            <Tab id="datasets" title={uiCopy.dialogs.dataset.tabs.datasets} panel={datasetsPanel} />
            <Tab id="excel" title={uiCopy.dialogs.dataset.tabs.excel} panel={excelPanel} />
            <Tab id="csv" title={uiCopy.dialogs.dataset.tabs.csv} panel={csvPanel} />
            <Tab id="manual" title={uiCopy.dialogs.dataset.tabs.manual} panel={manualPanel} />
          </Tabs>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDatasetDialogOpen(false)}>{uiCopy.dialogs.dataset.cancel}</Button>
          {datasetTab === 'datasets' ? (
            <Button
              intent={Intent.PRIMARY}
              onClick={() => {
                const dataset = datasets.find((item) => String(item.dataset_id ?? '') === selectedDatasetId)
                if (dataset) {
                  handleAddDatasetNode(dataset)
                }
              }}
              disabled={!selectedDatasetId}
            >
              {uiCopy.dialogs.dataset.addToGraph}
            </Button>
          ) : null}
        </div>
      </Dialog>

      <Dialog isOpen={parametersOpen} onClose={() => setParametersOpen(false)} title={uiCopy.dialogs.parameters.title}>
        <div className="dialog-body">
          {parameterDrafts.map((param, index) => (
            <div key={param.id} className="parameter-row">
              <InputGroup
                placeholder={uiCopy.dialogs.parameters.namePlaceholder}
                value={param.name}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setParameterDrafts((current) => current.map((item, idx) => (idx === index ? { ...item, name: value } : item)))
                }}
              />
              <InputGroup
                placeholder={uiCopy.dialogs.parameters.valuePlaceholder}
                value={param.value}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setParameterDrafts((current) => current.map((item, idx) => (idx === index ? { ...item, value } : item)))
                }}
              />
              <Button
                icon="trash"
                minimal
                intent={Intent.DANGER}
                onClick={() => setParameterDrafts((current) => current.filter((item) => item.id !== param.id))}
              />
            </div>
          ))}
          <Button
            icon="add"
            minimal
            onClick={() => setParameterDrafts((current) => [...current, { id: createId('param'), name: '', value: '' }])}
          >
            {uiCopy.dialogs.parameters.add}
          </Button>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setParametersOpen(false)}>{uiCopy.dialogs.parameters.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleSaveParameters}>{uiCopy.dialogs.parameters.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={transformOpen} onClose={() => setTransformOpen(false)} title={uiCopy.dialogs.transform.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.transform.nodeName}>
            <InputGroup value={transformDraft.title} onChange={(event) => setTransformDraft((prev) => ({ ...prev, title: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.operation}>
            <InputGroup value={transformDraft.operation} onChange={(event) => setTransformDraft((prev) => ({ ...prev, operation: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.expression}>
            <TextArea value={transformDraft.expression} onChange={(event) => setTransformDraft((prev) => ({ ...prev, expression: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setTransformOpen(false)}>{uiCopy.dialogs.transform.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleTransformSave} disabled={!selectedNode}>{uiCopy.dialogs.transform.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={joinOpen} onClose={() => setJoinOpen(false)} title={uiCopy.dialogs.join.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.join.leftDataset}>
            <HTMLSelect value={joinLeft} onChange={(event) => setJoinLeft(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.leftKey}>
            <HTMLSelect
              value={joinLeftKey}
              onChange={(event) => setJoinLeftKey(event.currentTarget.value)}
              disabled={!joinLeft}
            >
              <option value="">{uiCopy.dialogs.join.selectKey}</option>
              {getNodeColumnNames(joinLeft).map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.rightDataset}>
            <HTMLSelect value={joinRight} onChange={(event) => setJoinRight(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.rightKey}>
            <HTMLSelect
              value={joinRightKey}
              onChange={(event) => setJoinRightKey(event.currentTarget.value)}
              disabled={!joinRight}
            >
              <option value="">{uiCopy.dialogs.join.selectKey}</option>
              {getNodeColumnNames(joinRight).map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.joinType}>
            <HTMLSelect value={joinType} onChange={(event) => setJoinType(event.currentTarget.value)}>
              <option value="inner">{uiCopy.dialogs.join.inner}</option>
              <option value="left">{uiCopy.dialogs.join.left}</option>
              <option value="right">{uiCopy.dialogs.join.right}</option>
              <option value="full">{uiCopy.dialogs.join.full}</option>
            </HTMLSelect>
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setJoinOpen(false)}>{uiCopy.dialogs.join.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleJoin} disabled={!joinLeft || !joinRight}>{uiCopy.dialogs.join.create}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={visualizeOpen} onClose={() => setVisualizeOpen(false)} title={uiCopy.dialogs.visualize.title}>
        <div className="dialog-body">
          <Text className="muted">{uiCopy.dialogs.visualize.body}</Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setVisualizeOpen(false)}>{uiCopy.dialogs.visualize.close}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={deployOpen} onClose={() => setDeployOpen(false)} title={uiCopy.dialogs.deploy.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.deploy.outputDataset}>
            <InputGroup value={deployDraft.datasetName} onChange={(event) => setDeployDraft((prev) => ({ ...prev, datasetName: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploy.rowCount}>
            <InputGroup value={deployDraft.rowCount} onChange={(event) => setDeployDraft((prev) => ({ ...prev, rowCount: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploy.artifactKey}>
            <InputGroup value={deployDraft.artifactKey} onChange={(event) => setDeployDraft((prev) => ({ ...prev, artifactKey: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeployOpen(false)}>{uiCopy.dialogs.deploy.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleDeploy} loading={deployMutation.isPending}>{uiCopy.dialogs.deploy.deploy}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={deploySettingsOpen} onClose={() => setDeploySettingsOpen(false)} title={uiCopy.dialogs.deploySettings.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.deploySettings.compute}>
            <InputGroup value={deploySettings.compute} onChange={(event) => setDeploySettings((prev) => ({ ...prev, compute: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.memory}>
            <InputGroup value={deploySettings.memory} onChange={(event) => setDeploySettings((prev) => ({ ...prev, memory: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.schedule}>
            <InputGroup value={deploySettings.schedule} onChange={(event) => setDeploySettings((prev) => ({ ...prev, schedule: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.engine}>
            <InputGroup value={deploySettings.engine} onChange={(event) => setDeploySettings((prev) => ({ ...prev, engine: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeploySettingsOpen(false)}>{uiCopy.dialogs.deploySettings.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleSaveSettings}>{uiCopy.dialogs.deploySettings.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={outputOpen} onClose={() => setOutputOpen(false)} title={uiCopy.dialogs.output.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.output.outputName}>
            <InputGroup value={outputDraft.name} onChange={(event) => setOutputDraft((prev) => ({ ...prev, name: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.datasetName}>
            <InputGroup value={outputDraft.datasetName} onChange={(event) => setOutputDraft((prev) => ({ ...prev, datasetName: event.currentTarget.value }))} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.description}>
            <TextArea value={outputDraft.description} onChange={(event) => setOutputDraft((prev) => ({ ...prev, description: event.currentTarget.value }))} />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setOutputOpen(false)}>{uiCopy.dialogs.output.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleAddOutput} disabled={!outputDraft.datasetName.trim()}>{uiCopy.dialogs.output.add}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={helpOpen} onClose={() => setHelpOpen(false)} title={uiCopy.dialogs.help.title}>
        <div className="dialog-body">
          <Text className="muted">
            {uiCopy.dialogs.help.body}
          </Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setHelpOpen(false)}>{uiCopy.dialogs.help.close}</Button>
        </div>
      </Dialog>


    </div>
  )
}
