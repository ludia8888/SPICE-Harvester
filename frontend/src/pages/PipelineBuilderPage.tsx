import { useEffect, useMemo, useRef, useState } from 'react'
import {
  Button,
  Card,
  Checkbox,
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
  archivePipelineBranch,
  approvePipelineProposal,
  createDataset,
  createDatasetVersion,
  createPipeline,
  createPipelineBranch,
  deployPipeline,
  getPipeline,
  getPipelinePreviewStatus,
  listDatasets,
  listPipelineBranches,
  listPipelineProposals,
  listRegisteredSheets,
  listPipelines,
  listPipelineRuns,
  previewPipeline,
  buildPipeline,
  rejectPipelineProposal,
  restorePipelineBranch,
  startPipeliningSheet,
  submitPipelineProposal,
  uploadCsvDataset,
  uploadExcelDataset,
  uploadMediaDataset,
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
  type PipelineDependency,
  type PipelineMode,
  type PipelineNode,
  type PipelineParameter,
  type PipelineTool,
  type PreviewColumn,
  type PreviewRow,
  type PipelineExpectation,
  type PipelineSchemaContract,
} from '../features/pipeline/pipelineTypes'
import '../features/pipeline/pipeline.css'

type PipelineRecord = Record<string, unknown>
type DatasetRecord = Record<string, unknown>
type RegisteredSheetRecord = Record<string, unknown>
type ProposalRecord = Record<string, unknown>
type ExpectationItem = { id: string; rule: string; column: string; value: string }
type SchemaContractItem = { id: string; column: string; type: string; required: boolean }
type DependencyItem = { id: string; pipelineId: string; status: string }
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

const extractProposals = (payload: unknown): ProposalRecord[] => extractList<ProposalRecord>(payload, 'proposals')

const isCronExpression = (input: string) => {
  const parts = input.trim().split(/\s+/)
  if (parts.length !== 5) return false
  const fieldPattern = /^([*]|\d+|\d+-\d+|\*\/\d+|\d+-\d+\/\d+)(,([*]|\d+|\d+-\d+|\*\/\d+|\d+-\d+\/\d+))*$/
  return parts.every((part) => fieldPattern.test(part))
}

const parseScheduleInput = (schedule: string) => {
  const normalized = schedule.trim()
  if (!normalized || normalized.toLowerCase() === 'manual') {
    return { intervalSeconds: null, cron: null }
  }
  if (isCronExpression(normalized)) {
    return { intervalSeconds: null, cron: normalized }
  }
  const minuteMatch = normalized.match(/^(\d+)\s*m(in)?$/i)
  if (minuteMatch) return { intervalSeconds: Number(minuteMatch[1]) * 60, cron: null }
  const hourMatch = normalized.match(/^(\d+)\s*h(our)?s?$/i)
  if (hourMatch) return { intervalSeconds: Number(hourMatch[1]) * 3600, cron: null }
  const dayMatch = normalized.match(/^(\d+)\s*d(ay)?s?$/i)
  if (dayMatch) return { intervalSeconds: Number(dayMatch[1]) * 86400, cron: null }
  const numeric = Number(normalized)
  if (Number.isFinite(numeric)) return { intervalSeconds: numeric, cron: null }
  return { intervalSeconds: null, cron: null }
}

const expectationDefaults = (): ExpectationItem => ({
  id: createId('expectation'),
  rule: 'row_count_min',
  column: '',
  value: '',
})

const schemaContractDefaults = (): SchemaContractItem => ({
  id: createId('schema'),
  column: '',
  type: 'xsd:string',
  required: true,
})

const dependencyDefaults = (): DependencyItem => ({
  id: createId('dependency'),
  pipelineId: '',
  status: 'DEPLOYED',
})

type PipelineDefinitionView = 'graph' | 'pseudocode'

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
  const dependencies = Array.isArray(value.dependencies) ? value.dependencies : (fallback.dependencies ?? [])
  const schemaContract = Array.isArray((value as { schemaContract?: unknown }).schemaContract)
    ? ((value as { schemaContract?: PipelineSchemaContract[] }).schemaContract as PipelineSchemaContract[])
    : []
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
    expectations: Array.isArray(value.expectations) ? (value.expectations as PipelineExpectation[]) : [],
    schemaContract,
    dependencies: dependencies
      .map((dep) => {
        const record = dep as PipelineDependency & { pipeline_id?: string }
        const pipelineId = record.pipelineId ?? record.pipeline_id ?? ''
        return {
          pipelineId,
          status: record.status ?? 'DEPLOYED',
        }
      })
      .filter((dep) => dep.pipelineId),
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

const extractNumber = (value: unknown): number | null => {
  if (typeof value === 'number' && Number.isFinite(value)) return value
  if (typeof value === 'string' && value.trim()) {
    const numeric = Number(value)
    if (Number.isFinite(numeric)) return numeric
  }
  return null
}

const extractRowCount = (payload: unknown): number | null => {
  if (!payload || typeof payload !== 'object') return null
  const raw = payload as Record<string, unknown>
  return extractNumber(raw.row_count ?? raw.rowCount)
}

const extractSampleRowCount = (payload: unknown): number | null => {
  if (!payload || typeof payload !== 'object') return null
  const raw = payload as Record<string, unknown>
  return extractNumber(raw.sample_row_count ?? raw.sampleRowCount)
}

const extractColumnStats = (payload: unknown): Record<string, unknown> | null => {
  if (!payload || typeof payload !== 'object') return null
  const raw = payload as Record<string, unknown>
  const stats = raw.column_stats ?? raw.columnStats
  if (!stats || typeof stats !== 'object') return null
  return stats as Record<string, unknown>
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
              createBranch: '브랜치 생성',
              archiveBranch: '브랜치 보관',
              restoreBranch: '브랜치 복원',
              archivedLabel: '보관됨',
            },
	            toolbar: {
	              tools: '도구',
	              select: '선택',
	              remove: '삭제',
	              layout: '정렬',
	              focus: '포커스',
	              showAll: '전체 보기',
	              addDatasets: '데이터 추가',
	              parameters: '파라미터',
	              transform: '변환',
	              edit: '편집',
	              graphView: '그래프',
	              pseudocodeView: '의사코드',
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
              rowCountLabel: '전체 행',
              sampleRowCountLabel: '샘플',
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
              validationTitle: '검증 경고',
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
                  media: '미디어 업로드',
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
                media: {
                  title: '미디어 업로드',
                  helper: '이미지/문서 등 파일을 업로드해 media dataset으로 저장합니다.',
                  file: '파일',
                  datasetName: '데이터셋 이름',
                  preview: '미리보기',
                  previewEmpty: '파일을 선택하면 미리보기가 표시됩니다.',
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
                categories: {
                  rowLevel: '행 단위',
                  aggregation: '집계',
                  generator: '생성기',
                },
                expression: '수식',
                groupBy: '그룹 기준 컬럼',
                aggregates: '집계 정의 (col:op as alias)',
                pivotIndex: '피벗 인덱스',
                pivotColumns: '피벗 컬럼',
                pivotValues: '피벗 값',
                pivotAgg: '피벗 집계',
                windowPartition: '윈도우 파티션',
                windowOrder: '윈도우 정렬',
                columns: '컬럼 목록',
                renameMap: '이름 변경 매핑 (JSON)',
                castMap: '타입 변환 목록 (JSON)',
                schemaChecks: '스키마 검증(JSON)',
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
	                cross: '크로스(위험)',
	                allowCrossJoin: '키 없이 조인(크로스 조인) 허용',
	                allowCrossJoinHelp: '키 없이 조인하면 결과가 폭증할 수 있습니다. 정말 필요한 경우에만 사용하세요.',
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
                helper: '배포는 먼저 Build(스테이징)로 아티팩트를 생성한 뒤, 동일 아티팩트를 승격(promote)합니다.',
                cancel: '취소',
                deploy: '배포',
              },
              deploySettings: {
                title: '배포 설정',
                compute: '컴퓨트 티어',
                memory: '메모리',
                schedule: '스케줄',
                engine: '엔진',
                branch: '브랜치',
                replayOnDeploy: 'Breaking change 재처리(Replay) 허용',
                replayOnDeployHelp:
                  '스키마 breaking change(컬럼 삭제/타입 변경 등)가 감지되면 기본적으로 배포가 차단됩니다. 배포를 진행하려면 체크하세요.',
                proposalStatus: '제안 상태',
                proposalTitle: '제안 제목',
                proposalDescription: '제안 설명',
                expectations: '데이터 품질 기대치',
                schemaContract: '스키마 계약',
                dependencies: '실행 의존성',
                dependencySelectPipeline: '파이프라인 선택',
                dependencyStatusDeployed: 'DEPLOYED(배포됨) 이상',
                dependencyStatusSuccess: 'SUCCESS(성공) 이상',
                dependencyAdd: '의존성 추가',
                dependenciesHelp: '의존 파이프라인이 지정 상태에 도달하고(그리고 최신이면) 자동 실행됩니다.',
                expectationAdd: '기대치 추가',
                expectationColumn: '컬럼',
                expectationValue: '값',
                schemaAdd: '스키마 컬럼 추가',
                schemaColumn: '컬럼',
                schemaRequired: '필수',
                cancel: '취소',
                save: '저장',
              },
              branch: {
                title: '브랜치 생성',
                name: '브랜치 이름',
                helper: '현재 파이프라인 버전을 새 브랜치로 복사합니다.',
                cancel: '취소',
                create: '생성',
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
              deployMissingOutput: '배포할 출력 노드를 추가하세요.',
              deployPreflight: '배포 전 검증(Build) 실행 중…',
              deployPromote: '스테이징 빌드 결과를 배포로 반영 중…',
              deployPreflightFailed: '배포 전 검증(Build)이 실패했습니다.',
              deployPreflightTimeout: '배포 전 검증(Build) 시간이 초과되었습니다.',
              proposalSubmitted: '제안이 제출되었습니다.',
              proposalApproved: '제안이 승인되었습니다.',
              proposalRejected: '제안이 거절되었습니다.',
              branchCreated: '브랜치가 생성되었습니다.',
              branchArchived: '브랜치가 보관되었습니다.',
              branchRestored: '브랜치가 복원되었습니다.',
	              joinMissingKeys: '조인 키를 선택하세요. (또는 위험한 크로스 조인을 명시적으로 허용하세요.)',
	              transformMissingExpression: '수식을 입력하세요.',
              transformMissingPivot: '피벗 설정을 완성하세요.',
              transformMissingAggregate: '집계 정의를 입력하세요.',
              transformMissingWindow: '윈도우 정렬 컬럼을 입력하세요.',
              transformMissingColumns: '대상 컬럼을 입력하세요.',
              transformMissingRename: '이름 변경 매핑을 입력하세요.',
              transformMissingCasts: '타입 변환 설정을 입력하세요.',
              schemaChecksInvalid: '스키마 체크(JSON) 형식을 확인하세요.',
              expectationsInvalid: '데이터 품질 기대치 설정을 확인하세요.',
              schemaContractInvalid: '스키마 계약 항목을 확인하세요.',
              dependenciesInvalid: '의존성(JSON) 형식을 확인하세요.',
              dependenciesSelf: '자기 자신을 의존성으로 설정할 수 없습니다.',
              scheduleInvalid: '스케줄 입력 형식을 확인하세요.',
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
              explode: 'Explode',
              join: '조인',
              groupBy: '그룹',
              aggregate: '집계',
              pivot: '피벗',
              window: '윈도우',
              select: '컬럼 선택',
              drop: '컬럼 제거',
              rename: '컬럼 이름 변경',
              cast: '타입 변환',
              dedupe: '중복 제거',
              sort: '정렬',
              union: '유니온',
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
            proposals: {
              title: '제안',
              subtitle: '승인 전 변경사항을 검토하세요.',
              create: '제안 만들기',
              empty: '제안이 없습니다.',
              approve: '승인',
              reject: '거절',
              createTitle: '제안 제출',
              submit: '제안 제출',
              fields: {
                title: '제안 제목',
                description: '제안 설명',
              },
            },
	            history: {
	              title: '기록',
	              empty: '히스토리가 없습니다.',
	              filterAll: '전체',
	              filterSchedule: '스케줄',
	              wholePipeline: '전체 파이프라인',
	              scheduleReason: '사유',
	              scheduleError: '오류',
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
              createBranch: 'Create branch',
              archiveBranch: 'Archive branch',
              restoreBranch: 'Restore branch',
              archivedLabel: 'Archived',
            },
	            toolbar: {
	              tools: 'Tools',
	              select: 'Select',
	              remove: 'Remove',
	              layout: 'Layout',
	              focus: 'Focus',
	              showAll: 'Show all',
	              addDatasets: 'Add datasets',
	              parameters: 'Parameters',
	              transform: 'Transform',
	              edit: 'Edit',
	              graphView: 'Graph',
	              pseudocodeView: 'Pseudocode',
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
              rowCountLabel: 'Rows',
              sampleRowCountLabel: 'Sample',
              noNodes: 'No nodes',
              searchPlaceholder: (count: number) => `Search ${count} columns...`,
              formatType: (type: string) => type,
              validationTitle: 'Validation alerts',
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
                    media: 'Media upload',
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
                  media: {
                    title: 'Media upload',
                    helper: 'Upload media files and save them as a media dataset.',
                    file: 'Files',
                    datasetName: 'Dataset name',
                    preview: 'Preview',
                    previewEmpty: 'Select files to see a preview.',
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
                categories: {
                  rowLevel: 'Row-level',
                  aggregation: 'Aggregation',
                  generator: 'Generator',
                },
                expression: 'Expression',
                groupBy: 'Group by columns',
                aggregates: 'Aggregations (col:op as alias)',
                pivotIndex: 'Pivot index',
                pivotColumns: 'Pivot columns',
                pivotValues: 'Pivot values',
                pivotAgg: 'Pivot agg',
                windowPartition: 'Window partition',
                windowOrder: 'Window order',
                columns: 'Columns',
                renameMap: 'Rename map (JSON)',
                castMap: 'Cast list (JSON)',
                schemaChecks: 'Schema checks (JSON)',
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
	                cross: 'Cross (dangerous)',
	                allowCrossJoin: 'Allow keyless join (cross join)',
	                allowCrossJoinHelp: 'Joins without keys can explode row counts. Use only when absolutely necessary.',
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
                helper: 'Deploy runs a staged build first, then promotes the exact same artifact.',
                cancel: 'Cancel',
                deploy: 'Deploy',
              },
              deploySettings: {
                title: 'Deployment settings',
                compute: 'Compute tier',
                memory: 'Memory',
                schedule: 'Schedule',
                engine: 'Engine',
                branch: 'Branch',
                replayOnDeploy: 'Allow replay on deploy (breaking schema change)',
                replayOnDeployHelp:
                  'If a breaking schema change is detected (column removed/type changed), deploy is blocked unless you enable this.',
                proposalStatus: 'Proposal status',
                proposalTitle: 'Proposal title',
                proposalDescription: 'Proposal description',
                expectations: 'Data expectations',
                schemaContract: 'Schema contract',
                dependencies: 'Run dependencies',
                dependencySelectPipeline: 'Select pipeline',
                dependencyStatusDeployed: 'DEPLOYED or higher',
                dependencyStatusSuccess: 'SUCCESS or higher',
                dependencyAdd: 'Add dependency',
                dependenciesHelp: 'Runs when dependencies reach the required status (and are newer).',
                expectationAdd: 'Add expectation',
                expectationColumn: 'Column',
                expectationValue: 'Value',
                schemaAdd: 'Add schema column',
                schemaColumn: 'Column',
                schemaRequired: 'Required',
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
              branch: {
                title: 'Create branch',
                name: 'Branch name',
                helper: 'Copies the current pipeline version into a new branch.',
                cancel: 'Cancel',
                create: 'Create',
              },
            },
		            toast: {
		              pipelineCreated: 'Pipeline created.',
		              pipelineSaved: 'Pipeline saved.',
		              pipelineDeployed: 'Pipeline deployed.',
		              datasetCreated: 'Dataset created.',
		              pipeliningStarted: 'Connector dataset added to the pipeline.',
		              deployMissingOutput: 'Add an output node before deploying.',
		              deployPreflight: 'Running preflight build…',
		              deployPromote: 'Promoting staged build artifact to deploy…',
		              deployPreflightFailed: 'Preflight build failed.',
		              deployPreflightTimeout: 'Preflight build timed out.',
	              proposalSubmitted: 'Proposal submitted.',
	              proposalApproved: 'Proposal approved.',
	              proposalRejected: 'Proposal rejected.',
	              branchCreated: 'Branch created.',
                branchArchived: 'Branch archived.',
                branchRestored: 'Branch restored.',
	              joinMissingKeys: 'Select join keys (or explicitly allow dangerous cross join).',
	              transformMissingExpression: 'Add an expression to continue.',
              transformMissingPivot: 'Complete pivot settings to continue.',
              transformMissingAggregate: 'Add at least one aggregate.',
              transformMissingWindow: 'Add window order columns to continue.',
              transformMissingColumns: 'Add target columns.',
              transformMissingRename: 'Provide rename mapping.',
              transformMissingCasts: 'Provide cast definitions.',
	              schemaChecksInvalid: 'Schema checks JSON is invalid.',
	              expectationsInvalid: 'Review data expectation settings.',
	              schemaContractInvalid: 'Review schema contract settings.',
	              dependenciesInvalid: 'Dependencies JSON is invalid.',
	              dependenciesSelf: 'A pipeline cannot depend on itself.',
	              scheduleInvalid: 'Schedule input is invalid.',
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
              explode: 'Explode',
              join: 'Join',
              groupBy: 'Group by',
              aggregate: 'Aggregate',
              pivot: 'Pivot',
              window: 'Window',
              select: 'Select columns',
              drop: 'Drop columns',
              rename: 'Rename columns',
              cast: 'Cast types',
              dedupe: 'Remove duplicates',
              sort: 'Sort',
              union: 'Union',
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
            proposals: {
              title: 'Proposals',
              subtitle: 'Review changes before merging into production.',
              create: 'Create proposal',
              empty: 'No proposals yet.',
              approve: 'Approve',
              reject: 'Reject',
              createTitle: 'Submit proposal',
              submit: 'Submit proposal',
              fields: {
                title: 'Proposal title',
                description: 'Proposal description',
              },
            },
	            history: {
	              title: 'History',
	              empty: 'No history yet.',
	              filterAll: 'All runs',
	              filterSchedule: 'Schedule runs',
	              wholePipeline: 'Entire pipeline',
	              scheduleReason: 'Reason',
	              scheduleError: 'Error',
	            },
	          },
	    [language],
	  )

		  const [mode, setMode] = useState<PipelineMode>('edit')
		  const [activeTool, setActiveTool] = useState<PipelineTool>('tools')
		  const [definitionView, setDefinitionView] = useState<PipelineDefinitionView>('graph')
		  const [historyFilter, setHistoryFilter] = useState<'all' | 'schedule'>('all')
		  const [definition, setDefinition] = useState<PipelineDefinition>(() => createDefaultDefinition())
	  const [pipelineId, setPipelineId] = useState<string | null>(null)
	  const [pipelineName, setPipelineName] = useState('')
	  const [pipelineLocation, setPipelineLocation] = useState(`/projects/${dbName}/pipelines`)
  const [history, setHistory] = useState<PipelineDefinition[]>([])
  const [future, setFuture] = useState<PipelineDefinition[]>([])
  const [isDirty, setIsDirty] = useState(false)
  const [selectedNodeId, setSelectedNodeId] = useState<string | null>(null)
  const [previewNodeId, setPreviewNodeId] = useState<string | null>(null)
  const [nodeSearch, setNodeSearch] = useState('')
  const [focusNodeId, setFocusNodeId] = useState<string | null>(null)
  const [canvasZoom, setCanvasZoom] = useState(1)
  const [inspectorOpen, setInspectorOpen] = useState(false)
	  const [previewCollapsed, setPreviewCollapsed] = useState(false)
	  const [previewFullscreen, setPreviewFullscreen] = useState(false)
	  const [previewSample, setPreviewSample] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)

	  const pseudocodeText = useMemo(() => {
	    const nodesById = new Map(definition.nodes.map((node) => [node.id, node]))
	    const outgoing = new Map<string, string[]>()
	    const indegree = new Map<string, number>()

	    definition.nodes.forEach((node) => indegree.set(node.id, 0))
	    definition.edges.forEach((edge) => {
	      if (!nodesById.has(edge.from) || !nodesById.has(edge.to)) return
	      outgoing.set(edge.from, [...(outgoing.get(edge.from) ?? []), edge.to])
	      indegree.set(edge.to, (indegree.get(edge.to) ?? 0) + 1)
	    })

	    const queue = Array.from(indegree.entries())
	      .filter(([, count]) => count === 0)
	      .map(([id]) => id)
	      .sort()

	    const ordered: string[] = []
	    while (queue.length) {
	      const current = queue.shift()
	      if (!current) break
	      ordered.push(current)
	      for (const next of outgoing.get(current) ?? []) {
	        const nextCount = (indegree.get(next) ?? 0) - 1
	        indegree.set(next, nextCount)
	        if (nextCount === 0) {
	          queue.push(next)
	          queue.sort()
	        }
	      }
	    }

	    const outputById = new Map(definition.outputs.map((output) => [output.id, output]))
	    const lines = ordered.map((nodeId, index) => {
	      const node = nodesById.get(nodeId)
	      if (!node) return null

	      if (node.type === 'input') {
	        const datasetName = String(node.metadata?.datasetName ?? '').trim()
	        const datasetId = String(node.metadata?.datasetId ?? '').trim()
	        const datasetRef = datasetName || datasetId || '—'
	        return `${index + 1}. input ${node.title}  (dataset: ${datasetRef})`
	      }

	      if (node.type === 'output') {
	        const outputId = String(node.metadata?.outputId ?? '').trim()
	        const outputDatasetName =
	          (outputId && outputById.get(outputId)?.datasetName) || String(node.metadata?.datasetName ?? '').trim()
	        return `${index + 1}. output ${node.title}  (dataset: ${outputDatasetName || '—'})`
	      }

	      const operation = String(node.metadata?.operation ?? '').trim()
	      const expression = String(node.metadata?.expression ?? '').trim()
	      const suffix = [operation && `op=${operation}`, expression && `expr=${expression}`].filter(Boolean).join(' ')
	      return `${index + 1}. transform ${node.title}${suffix ? `  (${suffix})` : ''}`
	    })

	    return lines.filter(Boolean).join('\\n')
	  }, [definition.edges, definition.nodes, definition.outputs])

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
  const [proposalDrawerOpen, setProposalDrawerOpen] = useState(false)
  const [proposalDraft, setProposalDraft] = useState({ title: '', description: '' })
  const [branchDialogOpen, setBranchDialogOpen] = useState(false)
  const [branchDraft, setBranchDraft] = useState('')
  const [expectationDrafts, setExpectationDrafts] = useState<ExpectationItem[]>([expectationDefaults()])
  const [schemaContractDrafts, setSchemaContractDrafts] = useState<SchemaContractItem[]>([schemaContractDefaults()])
  const [dependencyDrafts, setDependencyDrafts] = useState<DependencyItem[]>([])

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
  const [mediaFiles, setMediaFiles] = useState<File[]>([])
  const [mediaDatasetName, setMediaDatasetName] = useState('')
  const [mediaPreview, setMediaPreview] = useState<{ columns: PreviewColumn[]; rows: PreviewRow[] } | null>(null)

  const [parameterDrafts, setParameterDrafts] = useState<PipelineParameter[]>([])
  const [transformDraft, setTransformDraft] = useState({
    title: '',
    operation: 'filter',
    expression: '',
    groupBy: '',
    aggregates: '',
    pivotIndex: '',
    pivotColumns: '',
    pivotValues: '',
    pivotAgg: 'sum',
    windowPartition: '',
    windowOrder: '',
    schemaChecksJson: '',
    columns: '',
    renameMap: '',
    castMap: '',
  })
  const [joinLeft, setJoinLeft] = useState('')
  const [joinRight, setJoinRight] = useState('')
  const [joinType, setJoinType] = useState('inner')
  const [joinLeftKey, setJoinLeftKey] = useState('')
  const [joinRightKey, setJoinRightKey] = useState('')
  const [joinAllowCrossJoin, setJoinAllowCrossJoin] = useState(false)
  const [outputDraft, setOutputDraft] = useState({ name: '', datasetName: '', description: '' })
  const [deployDraft, setDeployDraft] = useState({ datasetName: '' })
  const [safeDeployPending, setSafeDeployPending] = useState(false)
  const [deploySettings, setDeploySettings] = useState({
    compute: 'Medium',
    memory: '4 GB',
    schedule: 'Manual',
    engine: 'Batch',
    branch,
    replayOnDeploy: false,
    proposalStatus: 'draft',
    proposalTitle: '',
    proposalDescription: '',
    expectationsJson: '',
    schemaContractJson: '[]',
  })

  const autoPrompted = useRef(false)
  const loadedPipeline = useRef<string | null>(null)

  const pipelinesQuery = useQuery({
    queryKey: qk.pipelines(dbName, requestContext.language, branch),
    queryFn: () => listPipelines(requestContext, dbName, branch),
    enabled: Boolean(dbName),
  })

  const proposalsQuery = useQuery({
    queryKey: qk.pipelineProposals(dbName, requestContext.language, branch),
    queryFn: () => listPipelineProposals(requestContext, dbName, branch),
    enabled: Boolean(dbName),
  })

  const branchesQuery = useQuery({
    queryKey: qk.pipelineBranches(dbName, requestContext.language),
    queryFn: () => listPipelineBranches(requestContext, dbName),
    enabled: Boolean(dbName),
  })

  const datasetsQuery = useQuery({
    queryKey: qk.datasets(dbName, requestContext.language, branch),
    queryFn: () => listDatasets(requestContext, dbName, branch),
    enabled: Boolean(dbName),
  })

  const registeredSheetsQuery = useQuery({
    queryKey: qk.registeredSheets(dbName, requestContext.language),
    queryFn: () => listRegisteredSheets(requestContext, dbName),
    enabled: datasetDialogOpen,
  })


  const pipelineQuery = useQuery({
    queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language, branch),
    queryFn: () => getPipeline(requestContext, pipelineId ?? '', branch),
    enabled: Boolean(pipelineId),
    refetchInterval: 2000,
  })

  const [previewJobId, setPreviewJobId] = useState<string | null>(null)

  const previewNode = useMemo(() => {
    const candidate = definition.nodes.find((node) => node.id === previewNodeId)
    return candidate ?? definition.nodes[0] ?? null
  }, [definition.nodes, previewNodeId])

  const previewStatusQuery = useQuery({
    queryKey: [
      'bff',
      'pipelines',
      'preview',
      pipelineId ?? 'pending',
      previewNode?.id ?? null,
      previewJobId ?? null,
      { branch: branch ?? null, lang: requestContext.language },
    ],
    queryFn: () => getPipelinePreviewStatus(requestContext, pipelineId ?? '', previewNode?.id ?? undefined),
    enabled: Boolean(pipelineId) && !previewCollapsed,
    refetchInterval: previewCollapsed ? false : 4000,
  })

  const pipelineRunsQuery = useQuery({
    queryKey: qk.pipelineRuns(pipelineId ?? 'pending', requestContext.language),
    queryFn: () => listPipelineRuns(requestContext, pipelineId ?? '', 50),
    enabled: Boolean(pipelineId) && mode === 'history',
    refetchInterval: mode === 'history' ? 4000 : false,
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
      void queryClient.invalidateQueries({ queryKey: qk.pipelines(dbName, requestContext.language, branch) })
      void queryClient.invalidateQueries({ queryKey: qk.pipelineProposals(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineCreated })
    },
    onError: (error) => toastApiError(error, language),
  })

  const createBranchMutation = useMutation({
    mutationFn: (branchName: string) => createPipelineBranch(requestContext, pipelineId ?? '', branchName),
    onSuccess: (payload, branchName) => {
      const created = (payload as { data?: { branch?: Record<string, unknown> } } | null | undefined)?.data?.branch
      const createdPipelineId = created?.pipeline_id ?? created?.pipelineId
      const createdBranch = String(created?.branch ?? created?.name ?? branchName).trim()
      if (createdPipelineId) {
        setPipelineId(String(createdPipelineId))
      }
      if (createdBranch) {
        setBranch(createdBranch)
      }
      setBranchDialogOpen(false)
      setBranchDraft('')
      void queryClient.invalidateQueries({ queryKey: qk.pipelineBranches(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.branchCreated })
    },
    onError: (error) => toastApiError(error, language),
  })

  const branchArchived = useMemo(() => {
    if (!branchesQuery.data) return false
    if (!branch || branch === 'main') return false
    const raw = extractList<Record<string, unknown>>(branchesQuery.data, 'branches')
    const match = raw.find((item) => String(item.branch ?? item.name ?? item.branch_name ?? '').trim() === branch)
    if (!match) return false
    const archived = match.archived
    if (typeof archived === 'boolean') return archived
    if (typeof archived === 'string') return archived.toLowerCase() === 'true'
    if (typeof archived === 'number') return archived > 0
    return false
  }, [branchesQuery.data, branch])

  const archiveBranchMutation = useMutation({
    mutationFn: () => archivePipelineBranch(requestContext, dbName, branch),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: qk.pipelineBranches(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.branchArchived })
    },
    onError: (error) => toastApiError(error, language),
  })

  const restoreBranchMutation = useMutation({
    mutationFn: () => restorePipelineBranch(requestContext, dbName, branch),
    onSuccess: async () => {
      await queryClient.invalidateQueries({ queryKey: qk.pipelineBranches(dbName, requestContext.language) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.branchRestored })
    },
    onError: (error) => toastApiError(error, language),
  })

  const updatePipelineMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => updatePipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      setIsDirty(false)
      void queryClient.invalidateQueries({ queryKey: qk.pipeline(pipelineId ?? 'pending', requestContext.language, branch) })
      void queryClient.invalidateQueries({ queryKey: qk.pipelines(dbName, requestContext.language, branch) })
      void queryClient.invalidateQueries({ queryKey: qk.pipelineProposals(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.pipelineSaved })
    },
    onError: (error) => toastApiError(error, language),
  })

  const previewMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => previewPipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: (payload) => {
      const jobId = (payload as { data?: { job_id?: string } })?.data?.job_id
      if (typeof jobId === 'string') {
        setPreviewJobId(jobId)
      }
    },
    onError: (error) => toastApiError(error, language),
  })

  const submitProposalMutation = useMutation({
    mutationFn: (payload: { title: string; description: string }) =>
      submitPipelineProposal(requestContext, pipelineId ?? '', {
        db_name: dbName,
        title: payload.title,
        description: payload.description,
        branch,
      }),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pipelineProposals(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.proposalSubmitted })
      setProposalDraft({ title: '', description: '' })
      setProposalDrawerOpen(false)
    },
    onError: (error) => toastApiError(error, language),
  })

  const approveProposalMutation = useMutation({
    mutationFn: (payload: { pipelineId: string; proposalId: string }) =>
      approvePipelineProposal(requestContext, payload.pipelineId, payload.proposalId, undefined, 'main'),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pipelineProposals(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.proposalApproved })
    },
    onError: (error) => toastApiError(error, language),
  })

  const rejectProposalMutation = useMutation({
    mutationFn: (payload: { pipelineId: string; proposalId: string }) =>
      rejectPipelineProposal(requestContext, payload.pipelineId, payload.proposalId),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.pipelineProposals(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.proposalRejected })
    },
    onError: (error) => toastApiError(error, language),
  })

  const deployMutation = useMutation({
    mutationFn: (payload: Record<string, unknown>) => deployPipeline(requestContext, pipelineId ?? '', payload),
    onSuccess: () => {
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
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
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
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
        branch,
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
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
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
        branch,
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
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      setCsvFile(null)
      setCsvDatasetName('')
      setCsvDelimiter('')
      setCsvHasHeader(true)
    },
    onError: (error) => toastApiError(error, language),
  })

  const uploadMediaMutation = useMutation({
    mutationFn: (payload: { files: File[]; datasetName: string }) =>
      uploadMediaDataset(requestContext, dbName, {
        files: payload.files,
        datasetName: payload.datasetName,
        branch,
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
        setMediaPreview({ columns: normalizedColumns, rows: preview.rows })
        setPreviewSample({ columns: normalizedColumns, rows: preview.rows })
      }
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: uiCopy.toast.datasetCreated })
      setMediaFiles([])
      setMediaDatasetName('')
      setMediaPreview(null)
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
  const proposals = useMemo(() => extractProposals(proposalsQuery.data), [proposalsQuery.data])
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
    const names = raw
      .map((branchItem) => String(branchItem.name ?? branchItem.branch ?? branchItem.branch_name ?? branchItem.id ?? ''))
      .filter(Boolean)
    const base = names.length ? names : [branch]
    const extras = ['__create__']
    return [...new Set([...base, ...extras])]
  }, [branchesQuery.data, branch])
  const datasets = useMemo(() => extractList<DatasetRecord>(datasetsQuery.data, 'datasets'), [datasetsQuery.data])
  const registeredSheets = useMemo(
    () => extractList<RegisteredSheetRecord>(registeredSheetsQuery.data, 'sheets'),
    [registeredSheetsQuery.data],
  )
  useEffect(() => {
    if (!pipelinesQuery.data) return
    if (pipelines.length > 0) {
      const first = pipelines[0]
      if (pipelineId === String(first.pipeline_id ?? '')) return
      setPipelineId(String(first.pipeline_id ?? ''))
      setPipelineName(String(first.name ?? ''))
      setPipelineLocation(String(first.location ?? pipelineLocation))
      return
    }
    if (pipelineId) {
      setPipelineId(null)
      setPipelineName('')
    }
    if (pipelinesQuery.isFetching) return
    if (autoPrompted.current) return
    autoPrompted.current = true
    setPipelineDialogOpen(true)
  }, [pipelines, pipelinesQuery.data, pipelineId, pipelineLocation, pipelinesQuery.isFetching])

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
    const aggregates = Array.isArray(node.metadata?.aggregates) ? node.metadata?.aggregates : []
    const groupBy = Array.isArray(node.metadata?.groupBy) ? node.metadata?.groupBy : []
    const pivot = node.metadata?.pivot as { index?: string[]; columns?: string; values?: string; agg?: string } | undefined
    const windowMeta = node.metadata?.window as { partitionBy?: string[]; orderBy?: string[] } | undefined
	    const schemaChecks = Array.isArray(node.metadata?.schemaChecks) ? node.metadata?.schemaChecks : []
	    const operation = String(node.metadata?.operation ?? 'filter')
	    setTransformDraft({
	      title: node.title,
	      operation,
	      expression: node.metadata?.expression ?? '',
	      columns: Array.isArray(node.metadata?.columns) ? node.metadata?.columns.join(', ') : '',
	      renameMap: node.metadata?.rename ? JSON.stringify(node.metadata.rename, null, 2) : '',
	      castMap: Array.isArray(node.metadata?.casts) ? JSON.stringify(node.metadata.casts, null, 2) : '',
	      groupBy: groupBy.join(', '),
	      aggregates: aggregates.map((agg) => {
	        const col = String(agg?.column ?? '')
	        const op = String(agg?.op ?? '')
	        const alias = agg?.alias ? ` as ${String(agg.alias)}` : ''
        return col && op ? `${col}:${op}${alias}` : ''
      }).filter(Boolean).join('\n'),
      pivotIndex: (pivot?.index ?? []).join(', '),
      pivotColumns: pivot?.columns ?? '',
      pivotValues: pivot?.values ?? '',
      pivotAgg: pivot?.agg ?? 'sum',
      windowPartition: (windowMeta?.partitionBy ?? []).join(', '),
      windowOrder: (windowMeta?.orderBy ?? []).join(', '),
      schemaChecksJson: schemaChecks.length ? JSON.stringify(schemaChecks, null, 2) : '',
    })
  }, [transformOpen, definition.nodes, selectedNodeId])

  useEffect(() => {
    if (!joinOpen) return
    setJoinLeft(selectedNodeId ?? '')
    setJoinRight('')
    setJoinType('inner')
    setJoinLeftKey('')
    setJoinRightKey('')
    setJoinAllowCrossJoin(false)
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
      branch: definition.settings?.branch ?? branch,
      replayOnDeploy: Boolean((definition.settings as Record<string, unknown> | undefined)?.replayOnDeploy ?? current.replayOnDeploy),
      proposalStatus: definition.settings?.proposalStatus ?? current.proposalStatus,
      proposalTitle: definition.settings?.proposalTitle ?? current.proposalTitle,
      proposalDescription: definition.settings?.proposalDescription ?? current.proposalDescription,
      expectationsJson: definition.settings?.expectationsJson ?? current.expectationsJson,
      schemaContractJson: definition.settings?.schemaContractJson ?? current.schemaContractJson,
    }))
    setExpectationDrafts(
      definition.expectations?.length
        ? definition.expectations.map((item) => ({
            id: createId('expectation'),
            rule: String(item?.rule ?? 'row_count_min'),
            column: String(item?.column ?? ''),
            value: item?.value != null ? String(item.value) : '',
          }))
        : [expectationDefaults()],
    )
    setSchemaContractDrafts(
      definition.schemaContract?.length
        ? definition.schemaContract.map((item) => ({
            id: createId('schema'),
            column: String(item?.column ?? ''),
            type: String(item?.type ?? 'xsd:string'),
            required: item?.required !== false,
          }))
        : [schemaContractDefaults()],
    )
    setDependencyDrafts(
      definition.dependencies?.length
        ? definition.dependencies.map((dep) => ({
            id: createId('dependency'),
            pipelineId: String(dep?.pipelineId ?? (dep as Record<string, unknown> | null | undefined)?.pipeline_id ?? ''),
            status: String(dep?.status ?? 'DEPLOYED'),
          }))
        : [],
    )
  }, [
    deploySettingsOpen,
    definition.settings,
    definition.dependencies,
    definition.expectations,
    definition.schemaContract,
    branch,
  ])

  const selectedNode = useMemo(
    () => definition.nodes.find((node) => node.id === selectedNodeId) ?? null,
    [definition.nodes, selectedNodeId],
  )

  const focusVisibleNodeIds = useMemo(() => {
    if (!focusNodeId) return null
    const visible = new Set<string>()
    visible.add(focusNodeId)
    for (const edge of definition.edges) {
      if (edge.from === focusNodeId) visible.add(edge.to)
      if (edge.to === focusNodeId) visible.add(edge.from)
    }
    return visible
  }, [definition.edges, focusNodeId])

  useEffect(() => {
    if (!focusNodeId) return
    const exists = definition.nodes.some((node) => node.id === focusNodeId)
    if (!exists) setFocusNodeId(null)
  }, [definition.nodes, focusNodeId])

  const displayedNodes = useMemo(() => {
    if (!focusVisibleNodeIds) return definition.nodes
    return definition.nodes.filter((node) => focusVisibleNodeIds.has(node.id))
  }, [definition.nodes, focusVisibleNodeIds])

  const displayedEdges = useMemo(() => {
    if (!focusVisibleNodeIds) return definition.edges
    return definition.edges.filter((edge) => focusVisibleNodeIds.has(edge.from) && focusVisibleNodeIds.has(edge.to))
  }, [definition.edges, focusVisibleNodeIds])

  const nodeSearchTerm = useMemo(() => nodeSearch.trim().toLowerCase(), [nodeSearch])

  useEffect(() => {
    setPreviewSample(null)
  }, [previewNode?.id])

  const previewSchema = useMemo(() => {
    if (previewSample?.columns?.length) return previewSample.columns
    const previewPayload = extractPipeline(previewStatusQuery.data)
    const previewSamplePayload = previewPayload?.last_preview_sample as Record<string, unknown> | undefined
    const previewJobFromServer = typeof previewPayload?.last_preview_job_id === 'string'
      ? previewPayload?.last_preview_job_id
      : null
    const isLatestPreview = !previewJobId || !previewJobFromServer || previewJobFromServer === previewJobId
    const previewColumns = isLatestPreview
      ? extractColumns(previewSamplePayload, language === 'ko' ? '문자열' : 'String')
      : []
    if (previewColumns.length) return previewColumns
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
  }, [previewNode, datasets, previewSample, language, previewStatusQuery.data, previewJobId])

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
    const previewPayload = extractPipeline(previewStatusQuery.data)
    const previewSamplePayload = previewPayload?.last_preview_sample as Record<string, unknown> | undefined
    const previewJobFromServer = typeof previewPayload?.last_preview_job_id === 'string'
      ? previewPayload?.last_preview_job_id
      : null
    const isLatestPreview = !previewJobId || !previewJobFromServer || previewJobFromServer === previewJobId
    const previewRowsSample = isLatestPreview ? extractRows(previewSamplePayload) : []
    if (previewRowsSample.length) return previewRowsSample
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
  }, [previewNode, datasets, previewSample, previewStatusQuery.data, previewJobId])

	  const previewErrors = useMemo(() => {
	    const previewPayload = extractPipeline(previewStatusQuery.data)
	    const previewSamplePayload = previewPayload?.last_preview_sample as Record<string, unknown> | undefined
	    const previewJobFromServer = typeof previewPayload?.last_preview_job_id === 'string'
	      ? previewPayload?.last_preview_job_id
	      : null
	    const isLatestPreview = !previewJobId || !previewJobFromServer || previewJobFromServer === previewJobId
	    if (!isLatestPreview) return []
	    const errors = Array.isArray(previewSamplePayload?.errors) ? (previewSamplePayload?.errors as string[]) : []
	    const expectations = Array.isArray(previewSamplePayload?.expectations)
	      ? (previewSamplePayload?.expectations as string[])
	      : []
	    return [...errors, ...expectations].filter(Boolean)
	  }, [previewStatusQuery.data, previewJobId])

	  const previewMeta = useMemo(() => {
	    const previewPayload = extractPipeline(previewStatusQuery.data)
	    const previewSamplePayload = previewPayload?.last_preview_sample as Record<string, unknown> | undefined
	    const previewJobFromServer = typeof previewPayload?.last_preview_job_id === 'string'
	      ? previewPayload?.last_preview_job_id
	      : null
	    const isLatestPreview = !previewJobId || !previewJobFromServer || previewJobFromServer === previewJobId
	    if (isLatestPreview) {
	      const rowCount = extractRowCount(previewSamplePayload)
	      const sampleRowCount = extractSampleRowCount(previewSamplePayload)
	      const columnStats = extractColumnStats(previewSamplePayload)
	      if (rowCount !== null || sampleRowCount !== null || columnStats) {
	        const rows = previewSamplePayload ? (previewSamplePayload as { rows?: unknown }).rows : undefined
	        const fallbackSampleCount = Array.isArray(rows) ? rows.length : null
	        return { rowCount, sampleRowCount: sampleRowCount ?? fallbackSampleCount, columnStats }
	      }
	    }

	    if (!previewNode) return { rowCount: null, sampleRowCount: null, columnStats: null }
	    const datasetId = previewNode.metadata?.datasetId
	    if (!datasetId) return { rowCount: null, sampleRowCount: null, columnStats: null }
	    const dataset = datasets.find((item) => String(item.dataset_id ?? '') === datasetId)
	    const latestSample = dataset?.latest_sample_json
	    const baseSample = dataset?.sample_json
	    const rowCount = extractRowCount(latestSample) ?? extractRowCount(baseSample)
	    const sampleRowCount =
	      extractSampleRowCount(latestSample) ??
	      extractSampleRowCount(baseSample) ??
	      (latestSample ? extractRows(latestSample).length : null) ??
	      (baseSample ? extractRows(baseSample).length : null)
	    const columnStats = extractColumnStats(latestSample) ?? extractColumnStats(baseSample)
	    return { rowCount, sampleRowCount, columnStats }
	  }, [previewStatusQuery.data, previewJobId, previewNode, datasets])

	  const proposalSummary = useMemo(() => {
	    const items = proposals.filter((proposal) => proposal.status !== 'approved' && proposal.status !== 'rejected')
	    return items
	  }, [proposals])

	  const pipelineRuns = useMemo(() => extractList<Record<string, unknown>>(pipelineRunsQuery.data, 'runs'), [pipelineRunsQuery.data])
	  const filteredPipelineRuns = useMemo(() => {
	    if (historyFilter === 'schedule') {
	      return pipelineRuns.filter((run) => String(run.mode ?? '').toLowerCase() === 'schedule')
	    }
	    return pipelineRuns
	  }, [pipelineRuns, historyFilter])

  const previewDebounceRef = useRef<number | null>(null)
  useEffect(() => {
    if (!pipelineId || !previewNode) return
    if (safeDeployPending) return
    if (previewMutation.isPending) return
    if (mode !== 'edit') return
    if (previewCollapsed) return
    if (previewDebounceRef.current) {
      window.clearTimeout(previewDebounceRef.current)
    }
    previewDebounceRef.current = window.setTimeout(() => {
      previewMutation.mutate({
        db_name: dbName,
        definition_json: definition,
        expectations: definition.expectations,
        schema_contract: definition.schemaContract,
        node_id: previewNode.id,
        limit: 200,
        branch,
      })
    }, 700)
    return () => {
      if (previewDebounceRef.current) {
        window.clearTimeout(previewDebounceRef.current)
        previewDebounceRef.current = null
      }
    }
  }, [pipelineId, previewNode, definition, dbName, previewMutation, branch, mode, previewCollapsed, safeDeployPending])

  useEffect(() => {
    if (!previewMutation.data) return
    const sample = (previewMutation.data as { data?: { sample?: Record<string, unknown> } })?.data?.sample
    if (!sample || typeof sample !== 'object') return
    if ((sample as { queued?: boolean }).queued) {
      setPreviewSample(null)
      return
    }
    const columns = extractColumns(sample)
    const rows = extractRows(sample)
    if (columns.length || rows.length) {
      setPreviewSample({ columns, rows })
      return
    }
    setPreviewSample(null)
  }, [previewMutation.data])

  useEffect(() => {
    setPreviewJobId(null)
  }, [previewNode?.id])

  useEffect(() => {
    const previewPayload = extractPipeline(previewStatusQuery.data)
    const previewJobFromServer = typeof previewPayload?.last_preview_job_id === 'string'
      ? previewPayload?.last_preview_job_id
      : null
    if (previewJobFromServer && previewJobFromServer === previewJobId) return
    if (previewJobFromServer) {
      setPreviewSample(null)
    }
  }, [previewStatusQuery.data, previewJobId])

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
    const { dependencies, ...definitionPayload } = definition
    const scheduleInput = (deploySettings.schedule || '').trim()
    const schedulePayload = parseScheduleInput(scheduleInput)
    const scheduleIsManual = !scheduleInput || scheduleInput.toLowerCase() === 'manual'
    const scheduleHasInterval = typeof schedulePayload.intervalSeconds === 'number' && schedulePayload.intervalSeconds > 0
    const scheduleHasCron = Boolean(schedulePayload.cron)
    if (!scheduleIsManual && !scheduleHasInterval && !scheduleHasCron) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.scheduleInvalid })
      return
    }
    updatePipelineMutation.mutate({
      name: pipelineName,
      location: pipelineLocation,
      pipeline_type: 'batch',
      definition_json: definitionPayload,
      dependencies,
      proposal_status: deploySettings.proposalStatus || undefined,
      proposal_title: deploySettings.proposalTitle || undefined,
      proposal_description: deploySettings.proposalDescription || undefined,
      schedule:
        schedulePayload.intervalSeconds || schedulePayload.cron
          ? { interval_seconds: schedulePayload.intervalSeconds ?? undefined, cron: schedulePayload.cron ?? undefined }
          : undefined,
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
    const datasetBranch = String(dataset.branch ?? branch ?? 'main')
    const datasetName = String(dataset.name ?? '')
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
        metadata: { datasetId, datasetBranch, datasetName },
      }
      return { ...current, nodes: [...current.nodes, node] }
    })
    setSelectedNodeId(nodeId)
    setPreviewNodeId(nodeId)
    setInspectorOpen(true)
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
    const hasKeys = Boolean(joinLeftKey && joinRightKey)
    if (!joinAllowCrossJoin && !hasKeys) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.joinMissingKeys })
      return
    }
    const resolvedJoinType = joinAllowCrossJoin ? 'cross' : joinType
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
          joinType: resolvedJoinType,
          allowCrossJoin: joinAllowCrossJoin || undefined,
          leftKey: joinAllowCrossJoin ? undefined : (joinLeftKey || undefined),
          rightKey: joinAllowCrossJoin ? undefined : (joinRightKey || undefined),
          joinKey: !joinAllowCrossJoin && joinLeftKey && joinLeftKey === joinRightKey ? joinLeftKey : undefined,
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
    const operation = transformDraft.operation.trim()
    if (!operation) return
    if (operation === 'filter' && !transformDraft.expression.trim()) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingExpression })
      return
    }
    if (operation === 'compute' && !transformDraft.expression.trim()) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingExpression })
      return
    }
    const metadata: PipelineNode['metadata'] = {
      ...selectedNode.metadata,
      operation,
      expression: transformDraft.expression.trim() || undefined,
    }
    if (operation === 'groupBy' || operation === 'aggregate') {
      metadata.groupBy = transformDraft.groupBy
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
      metadata.aggregates = transformDraft.aggregates
        .split('\n')
        .map((line) => line.trim())
        .filter(Boolean)
        .map((line) => {
          const [left, aliasPart] = line.split(/\s+as\s+/i)
          const [column, op] = (left ?? '').split(':').map((part) => part.trim())
          return {
            column,
            op,
            alias: aliasPart?.trim() || undefined,
          }
        })
        .filter((item) => item.column && item.op)
      if (!metadata.aggregates?.length) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingAggregate })
        return
      }
    }
    if (operation === 'pivot') {
      metadata.pivot = {
        index: transformDraft.pivotIndex.split(',').map((item) => item.trim()).filter(Boolean),
        columns: transformDraft.pivotColumns.trim(),
        values: transformDraft.pivotValues.trim(),
        agg: transformDraft.pivotAgg.trim() || 'sum',
      }
    }
    if (operation === 'window') {
      metadata.window = {
        partitionBy: transformDraft.windowPartition.split(',').map((item) => item.trim()).filter(Boolean),
        orderBy: transformDraft.windowOrder.split(',').map((item) => item.trim()).filter(Boolean),
      }
      if (!metadata.window.orderBy?.length) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingWindow })
        return
      }
    }
    if (['select', 'drop', 'sort', 'dedupe', 'explode'].includes(operation)) {
      metadata.columns = transformDraft.columns
        .split(',')
        .map((item) => item.trim())
        .filter(Boolean)
      if (!metadata.columns.length) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingColumns })
        return
      }
    }
    if (operation === 'rename') {
      try {
        const parsed = JSON.parse(transformDraft.renameMap || '{}')
        if (parsed && typeof parsed === 'object') {
          metadata.rename = parsed as Record<string, string>
        }
      } catch {
        // ignore
      }
      if (!metadata.rename || Object.keys(metadata.rename).length === 0) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingRename })
        return
      }
    }
    if (operation === 'cast') {
      try {
        const parsed = JSON.parse(transformDraft.castMap || '[]')
        if (Array.isArray(parsed)) {
          metadata.casts = parsed as Array<{ column: string; type: string }>
        }
      } catch {
        // ignore
      }
      if (!metadata.casts?.length) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingCasts })
        return
      }
    }
    if (transformDraft.schemaChecksJson.trim()) {
      try {
        const parsed = JSON.parse(transformDraft.schemaChecksJson)
        if (!Array.isArray(parsed)) {
          void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.schemaChecksInvalid })
          return
        }
        metadata.schemaChecks = parsed
      } catch {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.schemaChecksInvalid })
        return
      }
    }
    if (operation === 'pivot') {
      if (!metadata.pivot?.columns || !metadata.pivot?.values || !metadata.pivot?.index?.length) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.transformMissingPivot })
        return
      }
    }
    updateDefinition((current) => ({
      ...current,
      nodes: current.nodes.map((node) =>
        node.id === selectedNode.id
          ? {
              ...node,
              title: transformDraft.title || node.title,
              metadata,
            }
          : node,
      ),
    }))
    setTransformOpen(false)
  }

  const handleDeploy = () => {
    if (!pipelineId) return
    if (safeDeployPending) return

    const { dependencies, ...definitionPayload } = definition
    const outputDatasetName =
      deployDraft.datasetName || definition.outputs[0]?.datasetName || uiCopy.labels.outputDatasetFallback
    const outputNodeId = definition.nodes.find((node) => node.type === 'output')?.id
    if (!outputNodeId) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.deployMissingOutput })
      return
    }

    const scheduleInput = (deploySettings.schedule || '').trim()
    const schedulePayload = parseScheduleInput(scheduleInput)
    const scheduleIsManual = !scheduleInput || scheduleInput.toLowerCase() === 'manual'
    const scheduleHasInterval = typeof schedulePayload.intervalSeconds === 'number' && schedulePayload.intervalSeconds > 0
    const scheduleHasCron = Boolean(schedulePayload.cron)
    if (!scheduleIsManual && !scheduleHasInterval && !scheduleHasCron) {
      void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.scheduleInvalid })
      return
    }

    const deployBranch = deploySettings.branch || branch

    const runSafeDeploy = async () => {
      setSafeDeployPending(true)
      try {
        void showAppToast({ intent: Intent.PRIMARY, message: uiCopy.toast.deployPreflight })

        const buildResponse = await buildPipeline(requestContext, pipelineId, {
          db_name: dbName,
          definition_json: definitionPayload,
          expectations: definition.expectations,
          schema_contract: definition.schemaContract,
          node_id: outputNodeId,
          limit: 200,
          branch: deployBranch,
        })
        const buildJobId = (buildResponse as { data?: { job_id?: string } })?.data?.job_id
        if (!buildJobId) {
          throw new Error(uiCopy.toast.deployPreflightFailed)
        }

        const startedAt = Date.now()
        const timeoutMs = 10 * 60 * 1000
        while (true) {
          const runsResponse = await listPipelineRuns(requestContext, pipelineId, 100)
          const runs = extractList<Record<string, unknown>>(runsResponse, 'runs')
          const run = runs.find((item) => String(item.job_id ?? '') === buildJobId)
          const status = String(run?.status ?? '').toUpperCase()
          if (status === 'SUCCESS') break
          if (status === 'FAILED') {
            throw new Error(uiCopy.toast.deployPreflightFailed)
          }
          if (Date.now() - startedAt > timeoutMs) {
            throw new Error(uiCopy.toast.deployPreflightTimeout)
          }
          await new Promise((resolve) => setTimeout(resolve, 2000))
        }

        void showAppToast({ intent: Intent.PRIMARY, message: uiCopy.toast.deployPromote })
        await deployMutation.mutateAsync({
          db_name: dbName,
          definition_json: definitionPayload,
          dependencies,
          expectations: definition.expectations,
          schema_contract: definition.schemaContract,
          node_id: outputNodeId,
          outputs: definition.outputs,
          output: {
            db_name: dbName,
            dataset_name: outputDatasetName,
          },
          branch: deployBranch,
          proposal_status: deploySettings.proposalStatus || undefined,
          proposal_title: deploySettings.proposalTitle || undefined,
          proposal_description: deploySettings.proposalDescription || undefined,
          schedule:
            schedulePayload.intervalSeconds || schedulePayload.cron
              ? { interval_seconds: schedulePayload.intervalSeconds ?? undefined, cron: schedulePayload.cron ?? undefined }
              : undefined,
          replay: deploySettings.replayOnDeploy || undefined,
          promote_build: true,
          build_job_id: buildJobId,
        })
        setDeployOpen(false)
      } catch (error) {
        toastApiError(error, language)
      } finally {
        setSafeDeployPending(false)
      }
    }

    void runSafeDeploy()
  }

  const handleSaveParameters = () => {
    updateDefinition((current) => ({ ...current, parameters: parameterDrafts }))
    setParametersOpen(false)
  }

  const handleSaveSettings = () => {
    let hasError = false
    updateDefinition((current) => {
      if (expectationDrafts.some((item) => !item.rule.trim())) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.expectationsInvalid })
        hasError = true
        return current
      }
      if (schemaContractDrafts.some((item) => !item.column.trim())) {
        void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.schemaContractInvalid })
        hasError = true
        return current
      }
      let expectations: PipelineExpectation[] | undefined = expectationDrafts
        .map((item) => ({
          rule: item.rule.trim(),
          column: item.column.trim() || undefined,
          value: item.value.trim() || undefined,
        }))
        .filter((item) => item.rule)
      if (!expectations.length) {
        expectations = undefined
      }
      let schemaContract: PipelineSchemaContract[] | undefined = schemaContractDrafts
        .map((item) => ({
          column: item.column.trim(),
          type: item.type.trim() || undefined,
          required: item.required,
        }))
        .filter((item) => item.column)
      if (!schemaContract.length) {
        schemaContract = undefined
      }
      const selfPipelineId = pipelineId ?? ''
      const dependencyMap = new Map<string, PipelineDependency>()
      for (const dep of dependencyDrafts) {
        const depPipelineId = (dep.pipelineId ?? '').trim()
        if (!depPipelineId) continue
        if (selfPipelineId && depPipelineId === selfPipelineId) {
          void showAppToast({ intent: Intent.WARNING, message: uiCopy.toast.dependenciesSelf })
          hasError = true
          return current
        }
        dependencyMap.set(depPipelineId, {
          pipelineId: depPipelineId,
          status: (dep.status ?? 'DEPLOYED').toUpperCase(),
        })
      }
      const dependencies = dependencyMap.size ? Array.from(dependencyMap.values()) : undefined
      const nextSettings = { ...current.settings, ...deploySettings }
      nextSettings.expectationsJson = expectations ? JSON.stringify(expectations, null, 2) : ''
      nextSettings.schemaContractJson = schemaContract ? JSON.stringify(schemaContract, null, 2) : ''
      return {
        ...current,
        settings: nextSettings,
        expectations,
        schemaContract,
        dependencies,
      }
    })
    if (!hasError) {
      setDeploySettingsOpen(false)
    }
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
        metadata: { outputId, datasetName: outputDataset, outputName },
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
        branch,
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
          placeholder={uiCopy.dialogs.dataset.excel.datasetName}
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
          placeholder={uiCopy.dialogs.dataset.csv.datasetName}
          value={csvDatasetName}
          onChange={(event) => setCsvDatasetName(event.currentTarget.value)}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.csv.delimiter}>
        <InputGroup
          placeholder={uiCopy.dialogs.dataset.csv.delimiter}
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

  const mediaPanel = (
    <div className="dataset-tab">
      <div className="dataset-preview-panel">
        <Text className="dataset-section-title">{uiCopy.dialogs.dataset.media.title}</Text>
        <Text className="muted">{uiCopy.dialogs.dataset.media.helper}</Text>
      </div>
      <FormGroup label={uiCopy.dialogs.dataset.media.file}>
        <FileInput
          text={
            mediaFiles.length
              ? `${mediaFiles.length} ${language === 'ko' ? '개 파일' : 'files'}`
              : uiCopy.dialogs.dataset.media.file
          }
          inputProps={{ multiple: true }}
          onInputChange={(event) => {
            const files = Array.from(event.currentTarget.files ?? [])
            setMediaFiles(files)
          }}
        />
      </FormGroup>
      <FormGroup label={uiCopy.dialogs.dataset.media.datasetName}>
        <InputGroup
          placeholder={uiCopy.dialogs.dataset.media.datasetName}
          value={mediaDatasetName}
          onChange={(event) => setMediaDatasetName(event.currentTarget.value)}
        />
      </FormGroup>
      <div className="dataset-card-actions">
        <Button
          intent={Intent.PRIMARY}
          icon="upload"
          onClick={() => {
            if (!mediaFiles.length) return
            uploadMediaMutation.mutate({
              files: mediaFiles,
              datasetName: mediaDatasetName.trim(),
            })
          }}
          disabled={!mediaFiles.length || !mediaDatasetName.trim() || uploadMediaMutation.isPending}
          loading={uploadMediaMutation.isPending}
        >
          {uiCopy.dialogs.dataset.media.upload}
        </Button>
        <Button
          minimal
          icon="refresh"
          onClick={() => {
            setMediaFiles([])
            setMediaDatasetName('')
            setMediaPreview(null)
          }}
        >
          {uiCopy.dialogs.dataset.media.reset}
        </Button>
      </div>
      <div className="dataset-preview-panel">
        <div className="dataset-preview-header">
          <Text>{uiCopy.dialogs.dataset.media.preview}</Text>
        </div>
        {mediaPreview?.columns?.length ? (
          <div className="dataset-preview-table">
            <div className="preview-sample-card">
              <div className="preview-sample-row preview-sample-header">
                {mediaPreview.columns.map((column) => (
                  <span key={column.key}>{column.key}</span>
                ))}
              </div>
              {(mediaPreview.rows ?? []).slice(0, 6).map((row, index) => (
                <div key={index} className="preview-sample-row">
                  {mediaPreview.columns.map((column) => (
                    <span key={column.key} title={String(row?.[column.key] ?? '')}>
                      {String(row?.[column.key] ?? '')}
                    </span>
                  ))}
                </div>
              ))}
            </div>
          </div>
        ) : (
          <Text className="muted">{uiCopy.dialogs.dataset.media.previewEmpty}</Text>
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
        branchArchived={branchArchived}
        activeCommandCount={activeCommandCount}
        copy={uiCopy.header}
        canUndo={canUndo}
        canRedo={canRedo}
        isDirty={isDirty}
        onModeChange={setMode}
        onUndo={handleUndo}
        onRedo={handleRedo}
        onBranchSelect={(name) => {
          if (name === '__create__') {
            setBranchDialogOpen(true)
            return
          }
          setBranch(name)
        }}
        onPipelineSelect={(id) => {
          setPipelineId(id)
          setPipelineName('')
          setPipelineLocation(pipelineLocation)
        }}
        onSave={handleSave}
        onDeploy={() => {
          setDeployDraft({ datasetName: definition.outputs[0]?.datasetName ?? '' })
          setDeployOpen(true)
        }}
        onOpenDeploySettings={() => setDeploySettingsOpen(true)}
        onOpenHelp={() => setHelpOpen(true)}
        onCreatePipeline={() => setPipelineDialogOpen(true)}
        onOpenCommandDrawer={() => setCommandDrawerOpen(true)}
        onArchiveBranch={() => {
          if (!branch || branch === 'main') return
          if (archiveBranchMutation.isPending || restoreBranchMutation.isPending) return
          void archiveBranchMutation.mutateAsync()
        }}
        onRestoreBranch={() => {
          if (!branch || branch === 'main') return
          if (archiveBranchMutation.isPending || restoreBranchMutation.isPending) return
          void restoreBranchMutation.mutateAsync()
        }}
      />

	      <PipelineToolbar
	        activeTool={activeTool}
	        view={definitionView}
	        copy={uiCopy.toolbar}
	        onToolChange={setActiveTool}
	        onViewChange={setDefinitionView}
	        onLayout={handleLayout}
        onFocus={() => {
          if (!selectedNodeId) return
          setFocusNodeId(selectedNodeId)
        }}
        onShowAll={() => setFocusNodeId(null)}
        canFocus={Boolean(selectedNodeId)}
        focusActive={Boolean(focusNodeId)}
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
        onAddGroupBy={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          handleSimpleTransform('groupBy', 'grouped-bar-chart')
        }}
        onAddAggregate={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          handleSimpleTransform('aggregate', 'chart')
        }}
        onAddPivot={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          handleSimpleTransform('pivot', 'pivot')
        }}
        onAddWindow={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectTransform })
            return
          }
          handleSimpleTransform('window', 'timeline-line-chart')
        }}
        onEdit={() => {
          if (!selectedNodeId) {
            void showAppToast({ intent: Intent.WARNING, message: uiCopy.dialogs.warnings.selectEdit })
            return
          }
          setTransformOpen(true)
        }}
      />

      {mode === 'edit' ? (
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
            <FormGroup
              label={language === 'ko' ? '노드 검색' : 'Search nodes'}
              labelFor="pipeline-node-search"
            >
              <InputGroup
                id="pipeline-node-search"
                leftIcon="search"
                placeholder={language === 'ko' ? '노드 이름 검색...' : 'Search node names...'}
                value={nodeSearch}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setNodeSearch(value)
                }}
              />
            </FormGroup>
            {focusNodeId ? (
              <Tag minimal icon="eye-open">
                {(language === 'ko' ? '포커스 모드' : 'Focus mode') +
                  `: ${displayedNodes.length}/${definition.nodes.length}`}
              </Tag>
            ) : null}
          </div>
          <div className="pipeline-divider" />
          <div className="pipeline-sidebar-section">
            <div className="pipeline-sidebar-title">{uiCopy.sidebar.inputs}</div>
            {displayedNodes
              .filter((node) => node.type === 'input')
              .filter((node) => !nodeSearchTerm || node.title.toLowerCase().includes(nodeSearchTerm))
              .map((node) => (
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
            {displayedNodes
              .filter((node) => node.type === 'transform')
              .filter((node) => !nodeSearchTerm || node.title.toLowerCase().includes(nodeSearchTerm))
              .map((node) => (
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
            {displayedNodes
              .filter((node) => node.type === 'output')
              .filter((node) => !nodeSearchTerm || node.title.toLowerCase().includes(nodeSearchTerm))
              .map((node) => (
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
	          {definitionView === 'graph' ? (
	            <>
	              <div className="canvas-zoom-controls">
	                <Button icon="zoom-in" minimal onClick={() => setCanvasZoom((current) => Math.min(1.4, current + 0.1))} />
	                <Button icon="zoom-out" minimal onClick={() => setCanvasZoom((current) => Math.max(0.6, current - 0.1))} />
	                <Button icon="zoom-to-fit" minimal onClick={() => setCanvasZoom(1)} />
	              </div>
	              <PipelineCanvas
	                nodes={displayedNodes}
	                edges={displayedEdges}
	                selectedNodeId={selectedNodeId}
	                copy={uiCopy.canvas}
	                zoom={canvasZoom}
	                onSelectNode={handleSelectNode}
	                onNodeAction={(action) => handleNodeAction(action)}
	              />
	            </>
	          ) : (
	            <div className="pipeline-pseudocode-view" data-testid="pipeline-pseudocode">
	              <pre>{pseudocodeText}</pre>
	            </div>
	          )}
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
      ) : (
        <div className="pipeline-body pipeline-mode-panel">
          <div className="pipeline-mode-panel-content">
            <h2>{mode === 'proposals' ? uiCopy.proposals.title : uiCopy.history.title}</h2>
            {mode === 'proposals' ? (
              <>
                <p className="muted">{uiCopy.proposals.subtitle}</p>
                <Button intent={Intent.PRIMARY} icon="git-pull" onClick={() => setProposalDrawerOpen(true)}>
                  {uiCopy.proposals.create}
                </Button>
                <div className="proposal-list">
                  {proposalSummary.length === 0 ? (
                    <Text className="muted">{uiCopy.proposals.empty}</Text>
                  ) : (
                    proposalSummary.map((proposal) => (
                      <Card key={String(proposal.proposal_id ?? proposal.id ?? '')} className="proposal-card">
                        <div className="proposal-card-header">
                          <Text>{String(proposal.title ?? proposal.proposal_title ?? '')}</Text>
                          <Tag minimal>{String(proposal.status ?? '')}</Tag>
                        </div>
                        <Text className="muted">{String(proposal.description ?? proposal.proposal_description ?? '')}</Text>
                        <div className="proposal-card-actions">
                          <Button
                            small
                            intent={Intent.SUCCESS}
                            onClick={() =>
                              approveProposalMutation.mutate({
                                pipelineId: String(proposal.pipeline_id ?? pipelineId ?? ''),
                                proposalId: String(proposal.proposal_id ?? proposal.id ?? ''),
                              })
                            }
                          >
                            {uiCopy.proposals.approve}
                          </Button>
                          <Button
                            small
                            intent={Intent.DANGER}
                            onClick={() =>
                              rejectProposalMutation.mutate({
                                pipelineId: String(proposal.pipeline_id ?? pipelineId ?? ''),
                                proposalId: String(proposal.proposal_id ?? proposal.id ?? ''),
                              })
                            }
                          >
                            {uiCopy.proposals.reject}
                          </Button>
                        </div>
                      </Card>
                    ))
                  )}
                </div>
              </>
	            ) : (
	              <div className="pipeline-history-list">
	                <div className="pipeline-history-toolbar">
	                  <HTMLSelect value={historyFilter} onChange={(event) => setHistoryFilter(event.currentTarget.value as 'all' | 'schedule')}>
	                    <option value="all">{uiCopy.history.filterAll}</option>
	                    <option value="schedule">{uiCopy.history.filterSchedule}</option>
	                  </HTMLSelect>
	                </div>
	                {filteredPipelineRuns.length === 0 ? (
	                  <Text className="muted">{uiCopy.history.empty}</Text>
	                ) : (
	                  filteredPipelineRuns.map((run) => {
	                    const statusValue = String(run.status ?? '').toUpperCase()
	                    const modeValue = String(run.mode ?? '').toUpperCase()
	                    const outputJson =
	                      run.output_json && typeof run.output_json === 'object'
	                        ? (run.output_json as Record<string, unknown>)
	                        : null
	                    const reason = outputJson && typeof outputJson.reason === 'string' ? outputJson.reason : null
	                    const error = outputJson && typeof outputJson.error === 'string' ? outputJson.error : null
	                    const detail = outputJson && typeof outputJson.detail === 'string' ? outputJson.detail : null
	                    const scheduleNote = reason
	                      ? `${uiCopy.history.scheduleReason}: ${reason}${detail ? ` — ${detail}` : ''}`
	                      : error
	                      ? `${uiCopy.history.scheduleError}: ${error}${detail ? ` — ${detail}` : ''}`
	                      : detail
	                      ? detail
	                      : null
	                    const tagText = statusValue === 'IGNORED' && reason ? `${statusValue} · ${reason}` : statusValue
	                    const tagIntent =
	                      statusValue === 'FAILED'
	                        ? Intent.DANGER
	                        : statusValue === 'IGNORED'
	                        ? Intent.WARNING
	                        : statusValue === 'QUEUED'
	                        ? Intent.PRIMARY
	                        : Intent.SUCCESS
	                    return (
	                    <Card key={String(run.run_id ?? run.job_id ?? '')} className="pipeline-history-card">
	                      <div className="pipeline-history-header">
	                        <Text>{modeValue}</Text>
	                        <Tag minimal intent={tagIntent}>
	                          {tagText}
	                        </Tag>
	                      </div>
	                      <Text className="muted">{String(run.job_id ?? '')}</Text>
	                      <Text className="muted">
	                        {run.node_id ? `Node ${String(run.node_id)}` : uiCopy.history.wholePipeline}
	                      </Text>
	                      {scheduleNote ? <Text className="muted">{scheduleNote}</Text> : null}
	                    </Card>
	                    )
	                  })
	                )}
	              </div>
	            )}
          </div>
        </div>
      )}

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
        <>
	          <PipelinePreview
	            nodes={nodeOptions}
	            selectedNodeId={previewNode?.id ?? null}
	            columns={previewSchema}
	            rows={previewRows}
	            rowCount={previewMeta.rowCount}
	            sampleRowCount={previewMeta.sampleRowCount}
	            columnStats={previewMeta.columnStats}
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
          {previewErrors.length ? (
            <div className="preview-errors">
              <Tag minimal intent={Intent.WARNING}>{uiCopy.preview.validationTitle}</Tag>
              <ul>
                {previewErrors.map((error) => (
                  <li key={error}>{error}</li>
                ))}
              </ul>
            </div>
          ) : null}
        </>
      )}

      <Dialog isOpen={pipelineDialogOpen} onClose={() => setPipelineDialogOpen(false)} title={uiCopy.dialogs.pipeline.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.pipeline.name} labelFor="pipeline-create-name">
            <InputGroup id="pipeline-create-name" value={newPipelineName} onChange={(event) => setNewPipelineName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.location} labelFor="pipeline-create-location">
            <InputGroup id="pipeline-create-location" value={newPipelineLocation} onChange={(event) => setNewPipelineLocation(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.pipeline.description} labelFor="pipeline-create-description">
            <TextArea id="pipeline-create-description" value={newPipelineDescription} onChange={(event) => setNewPipelineDescription(event.currentTarget.value)} />
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
                branch,
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
            <Tab id="media" title={uiCopy.dialogs.dataset.tabs.media} panel={mediaPanel} />
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

      <Dialog isOpen={proposalDrawerOpen} onClose={() => setProposalDrawerOpen(false)} title={uiCopy.proposals.createTitle}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.proposals.fields.title}>
            <InputGroup
              value={proposalDraft.title}
              onChange={(event) => {
                const value = event.currentTarget.value
                setProposalDraft((prev) => ({ ...prev, title: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.proposals.fields.description}>
            <TextArea
              value={proposalDraft.description}
              onChange={(event) => {
                const value = event.currentTarget.value
                setProposalDraft((prev) => ({ ...prev, description: value }))
              }}
            />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setProposalDrawerOpen(false)}>{uiCopy.dialogs.pipeline.cancel}</Button>
          <Button
            intent={Intent.PRIMARY}
            disabled={!proposalDraft.title.trim() || !pipelineId}
            loading={submitProposalMutation.isPending}
            onClick={() => submitProposalMutation.mutate({ title: proposalDraft.title.trim(), description: proposalDraft.description.trim() })}
          >
            {uiCopy.proposals.submit}
          </Button>
        </div>
      </Dialog>

      <Dialog isOpen={branchDialogOpen} onClose={() => setBranchDialogOpen(false)} title={uiCopy.dialogs.branch.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.branch.name}>
            <InputGroup value={branchDraft} onChange={(event) => setBranchDraft(event.currentTarget.value)} />
          </FormGroup>
          <Text className="muted">{uiCopy.dialogs.branch.helper}</Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setBranchDialogOpen(false)}>{uiCopy.dialogs.branch.cancel}</Button>
          <Button
            intent={Intent.PRIMARY}
            disabled={!branchDraft.trim() || !pipelineId}
            loading={createBranchMutation.isPending}
            onClick={() => branchDraft.trim() && createBranchMutation.mutate(branchDraft)}
          >
            {uiCopy.dialogs.branch.create}
          </Button>
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
          <FormGroup label={uiCopy.dialogs.transform.nodeName} labelFor="pipeline-transform-node-name">
            <InputGroup
              id="pipeline-transform-node-name"
              value={transformDraft.title}
              onChange={(event) => {
                const value = event.currentTarget.value
                setTransformDraft((prev) => ({ ...prev, title: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.operation} labelFor="pipeline-transform-operation">
            <HTMLSelect
              id="pipeline-transform-operation"
              value={transformDraft.operation}
              onChange={(event) => {
                const value = event.currentTarget.value
                setTransformDraft((prev) => ({ ...prev, operation: value }))
              }}
            >
              <optgroup label={uiCopy.dialogs.transform.categories.rowLevel}>
                <option value="filter">{uiCopy.operations.filter}</option>
                <option value="compute">{uiCopy.operations.compute}</option>
                <option value="select">{uiCopy.operations.select}</option>
                <option value="drop">{uiCopy.operations.drop}</option>
                <option value="rename">{uiCopy.operations.rename}</option>
                <option value="cast">{uiCopy.operations.cast}</option>
                <option value="dedupe">{uiCopy.operations.dedupe}</option>
                <option value="sort">{uiCopy.operations.sort}</option>
                <option value="union">{uiCopy.operations.union}</option>
              </optgroup>
              <optgroup label={uiCopy.dialogs.transform.categories.aggregation}>
                <option value="groupBy">{uiCopy.operations.groupBy}</option>
                <option value="aggregate">{uiCopy.operations.aggregate}</option>
                <option value="pivot">{uiCopy.operations.pivot}</option>
              </optgroup>
              <optgroup label={uiCopy.dialogs.transform.categories.generator}>
                <option value="explode">{uiCopy.operations.explode}</option>
                <option value="window">{uiCopy.operations.window}</option>
              </optgroup>
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.transform.expression} labelFor="pipeline-transform-expression">
            <TextArea
              id="pipeline-transform-expression"
              value={transformDraft.expression}
              onChange={(event) => {
                const value = event.currentTarget.value
                setTransformDraft((prev) => ({ ...prev, expression: value }))
              }}
            />
          </FormGroup>
          {(transformDraft.operation === 'groupBy' || transformDraft.operation === 'aggregate') ? (
            <>
              <FormGroup label={uiCopy.dialogs.transform.groupBy}>
                <InputGroup
                  value={transformDraft.groupBy}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, groupBy: value }))
                  }}
                />
              </FormGroup>
              <FormGroup label={uiCopy.dialogs.transform.aggregates}>
                <TextArea
                  value={transformDraft.aggregates}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, aggregates: value }))
                  }}
                />
              </FormGroup>
            </>
          ) : null}
          {transformDraft.operation === 'pivot' ? (
            <>
              <FormGroup label={uiCopy.dialogs.transform.pivotIndex}>
                <InputGroup
                  value={transformDraft.pivotIndex}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, pivotIndex: value }))
                  }}
                />
              </FormGroup>
              <FormGroup label={uiCopy.dialogs.transform.pivotColumns}>
                <InputGroup
                  value={transformDraft.pivotColumns}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, pivotColumns: value }))
                  }}
                />
              </FormGroup>
              <FormGroup label={uiCopy.dialogs.transform.pivotValues}>
                <InputGroup
                  value={transformDraft.pivotValues}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, pivotValues: value }))
                  }}
                />
              </FormGroup>
              <FormGroup label={uiCopy.dialogs.transform.pivotAgg}>
                <InputGroup
                  value={transformDraft.pivotAgg}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, pivotAgg: value }))
                  }}
                />
              </FormGroup>
            </>
          ) : null}
          {transformDraft.operation === 'window' ? (
            <>
              <FormGroup label={uiCopy.dialogs.transform.windowPartition}>
                <InputGroup
                  value={transformDraft.windowPartition}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, windowPartition: value }))
                  }}
                />
              </FormGroup>
              <FormGroup label={uiCopy.dialogs.transform.windowOrder}>
                <InputGroup
                  value={transformDraft.windowOrder}
                  onChange={(event) => {
                    const value = event.currentTarget.value
                    setTransformDraft((prev) => ({ ...prev, windowOrder: value }))
                  }}
                />
              </FormGroup>
            </>
          ) : null}
          {['select', 'drop', 'sort', 'dedupe', 'explode'].includes(transformDraft.operation) ? (
            <FormGroup label={uiCopy.dialogs.transform.columns}>
              <InputGroup
                value={transformDraft.columns}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setTransformDraft((prev) => ({ ...prev, columns: value }))
                }}
                placeholder="col_a, col_b"
              />
            </FormGroup>
          ) : null}
          {transformDraft.operation === 'rename' ? (
            <FormGroup label={uiCopy.dialogs.transform.renameMap}>
              <TextArea
                value={transformDraft.renameMap}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setTransformDraft((prev) => ({ ...prev, renameMap: value }))
                }}
                placeholder='{"old_name": "new_name"}'
              />
            </FormGroup>
          ) : null}
          {transformDraft.operation === 'cast' ? (
            <FormGroup label={uiCopy.dialogs.transform.castMap}>
              <TextArea
                value={transformDraft.castMap}
                onChange={(event) => {
                  const value = event.currentTarget.value
                  setTransformDraft((prev) => ({ ...prev, castMap: value }))
                }}
                placeholder='[{"column": "amount", "type": "xsd:decimal"}]'
              />
            </FormGroup>
          ) : null}
          <FormGroup label={uiCopy.dialogs.transform.schemaChecks}>
            <TextArea
              value={transformDraft.schemaChecksJson}
              onChange={(event) => {
                const value = event.currentTarget.value
                setTransformDraft((prev) => ({ ...prev, schemaChecksJson: value }))
              }}
              placeholder='[{"column":"id","rule":"not_null"}]'
            />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setTransformOpen(false)}>{uiCopy.dialogs.transform.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleTransformSave} disabled={!selectedNode}>{uiCopy.dialogs.transform.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={joinOpen} onClose={() => setJoinOpen(false)} title={uiCopy.dialogs.join.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.join.leftDataset} labelFor="pipeline-join-left-dataset">
            <HTMLSelect id="pipeline-join-left-dataset" value={joinLeft} onChange={(event) => setJoinLeft(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.leftKey} labelFor="pipeline-join-left-key">
            <HTMLSelect
              id="pipeline-join-left-key"
              value={joinLeftKey}
              onChange={(event) => setJoinLeftKey(event.currentTarget.value)}
              disabled={!joinLeft || joinAllowCrossJoin}
            >
              <option value="">{uiCopy.dialogs.join.selectKey}</option>
              {getNodeColumnNames(joinLeft).map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.rightDataset} labelFor="pipeline-join-right-dataset">
            <HTMLSelect id="pipeline-join-right-dataset" value={joinRight} onChange={(event) => setJoinRight(event.currentTarget.value)}>
              <option value="">{uiCopy.dialogs.join.selectNode}</option>
              {definition.nodes.map((node) => (
                <option key={node.id} value={node.id}>{node.title}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.rightKey} labelFor="pipeline-join-right-key">
            <HTMLSelect
              id="pipeline-join-right-key"
              value={joinRightKey}
              onChange={(event) => setJoinRightKey(event.currentTarget.value)}
              disabled={!joinRight || joinAllowCrossJoin}
            >
              <option value="">{uiCopy.dialogs.join.selectKey}</option>
              {getNodeColumnNames(joinRight).map((col) => (
                <option key={col} value={col}>{col}</option>
              ))}
            </HTMLSelect>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.join.joinType} labelFor="pipeline-join-type">
            <HTMLSelect id="pipeline-join-type" value={joinType} onChange={(event) => setJoinType(event.currentTarget.value)} disabled={joinAllowCrossJoin}>
              <option value="inner">{uiCopy.dialogs.join.inner}</option>
              <option value="left">{uiCopy.dialogs.join.left}</option>
              <option value="right">{uiCopy.dialogs.join.right}</option>
              <option value="full">{uiCopy.dialogs.join.full}</option>
              {joinAllowCrossJoin ? <option value="cross">{uiCopy.dialogs.join.cross}</option> : null}
            </HTMLSelect>
          </FormGroup>
          <FormGroup>
            <Checkbox
              checked={joinAllowCrossJoin}
              label={uiCopy.dialogs.join.allowCrossJoin}
              onChange={(event) => {
                const checked = event.currentTarget.checked
                setJoinAllowCrossJoin(checked)
                if (checked) {
                  setJoinType('cross')
                  setJoinLeftKey('')
                  setJoinRightKey('')
                } else if (joinType === 'cross') {
                  setJoinType('inner')
                }
              }}
            />
            <Text className="muted">{uiCopy.dialogs.join.allowCrossJoinHelp}</Text>
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setJoinOpen(false)}>{uiCopy.dialogs.join.cancel}</Button>
          <Button
            intent={Intent.PRIMARY}
            onClick={handleJoin}
            disabled={!joinLeft || !joinRight || (!joinAllowCrossJoin && (!joinLeftKey || !joinRightKey))}
          >
            {uiCopy.dialogs.join.create}
          </Button>
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
            <InputGroup
              value={deployDraft.datasetName}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeployDraft((prev) => ({ ...prev, datasetName: value }))
              }}
            />
          </FormGroup>
          <Text className="muted">{uiCopy.dialogs.deploy.helper}</Text>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeployOpen(false)}>{uiCopy.dialogs.deploy.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleDeploy} loading={deployMutation.isPending || safeDeployPending}>{uiCopy.dialogs.deploy.deploy}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={deploySettingsOpen} onClose={() => setDeploySettingsOpen(false)} title={uiCopy.dialogs.deploySettings.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.deploySettings.compute}>
            <InputGroup
              value={deploySettings.compute}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, compute: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.memory}>
            <InputGroup
              value={deploySettings.memory}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, memory: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.schedule}>
            <InputGroup
              value={deploySettings.schedule}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, schedule: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.engine}>
            <InputGroup
              value={deploySettings.engine}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, engine: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.expectations}>
            <div className="dialog-scroll">
              {expectationDrafts.map((item) => (
                <div key={item.id} className="expectation-row">
                  <HTMLSelect
                    value={item.rule}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setExpectationDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, rule: value } : row)),
                      )
                    }}
                  >
                    <option value="row_count_min">row_count_min</option>
                    <option value="row_count_max">row_count_max</option>
                    <option value="not_null">not_null</option>
                    <option value="non_empty">non_empty</option>
                    <option value="unique">unique</option>
                    <option value="min">min</option>
                    <option value="max">max</option>
                    <option value="regex">regex</option>
                    <option value="in_set">in_set</option>
                  </HTMLSelect>
                  <InputGroup
                    placeholder={uiCopy.dialogs.deploySettings.expectationColumn}
                    value={item.column}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setExpectationDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, column: value } : row)),
                      )
                    }}
                  />
                  <InputGroup
                    placeholder={uiCopy.dialogs.deploySettings.expectationValue}
                    value={item.value}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setExpectationDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, value } : row)),
                      )
                    }}
                  />
                  <Button
                    icon="trash"
                    minimal
                    intent={Intent.DANGER}
                    onClick={() =>
                      setExpectationDrafts((current) => current.filter((row) => row.id !== item.id))
                    }
                  />
                </div>
              ))}
            </div>
            <Button
              icon="add"
              minimal
              onClick={() => setExpectationDrafts((current) => [...current, expectationDefaults()])}
            >
              {uiCopy.dialogs.deploySettings.expectationAdd}
            </Button>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.schemaContract}>
            <div className="dialog-scroll">
              {schemaContractDrafts.map((item) => (
                <div key={item.id} className="expectation-row">
                  <InputGroup
                    placeholder={uiCopy.dialogs.deploySettings.schemaColumn}
                    value={item.column}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setSchemaContractDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, column: value } : row)),
                      )
                    }}
                  />
                  <HTMLSelect
                    value={item.type}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setSchemaContractDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, type: value } : row)),
                      )
                    }}
                  >
                    <option value="xsd:string">xsd:string</option>
                    <option value="xsd:integer">xsd:integer</option>
                    <option value="xsd:decimal">xsd:decimal</option>
                    <option value="xsd:boolean">xsd:boolean</option>
                    <option value="xsd:dateTime">xsd:dateTime</option>
                  </HTMLSelect>
                  <label className="schema-toggle">
                    <input
                      type="checkbox"
                      checked={item.required}
                      onChange={(event) => {
                        const value = event.currentTarget.checked
                        setSchemaContractDrafts((current) =>
                          current.map((row) => (row.id === item.id ? { ...row, required: value } : row)),
                        )
                      }}
                    />
                    {uiCopy.dialogs.deploySettings.schemaRequired}
                  </label>
                  <Button
                    icon="trash"
                    minimal
                    intent={Intent.DANGER}
                    onClick={() =>
                      setSchemaContractDrafts((current) => current.filter((row) => row.id !== item.id))
                    }
                  />
                </div>
              ))}
            </div>
            <Button
              icon="add"
              minimal
              onClick={() => setSchemaContractDrafts((current) => [...current, schemaContractDefaults()])}
            >
              {uiCopy.dialogs.deploySettings.schemaAdd}
            </Button>
          </FormGroup>
          <FormGroup>
            <Checkbox
              checked={deploySettings.replayOnDeploy}
              onChange={(event) => {
                const checked = event.currentTarget.checked
                setDeploySettings((prev) => ({ ...prev, replayOnDeploy: checked }))
              }}
            >
              {uiCopy.dialogs.deploySettings.replayOnDeploy}
            </Checkbox>
            <Text className="muted">{uiCopy.dialogs.deploySettings.replayOnDeployHelp}</Text>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.dependencies}>
            <div className="dialog-scroll">
              {dependencyDrafts.map((item) => (
                <div key={item.id} className="expectation-row">
                  <HTMLSelect
                    value={item.pipelineId}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDependencyDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, pipelineId: value } : row)),
                      )
                    }}
                  >
                    <option value="">{uiCopy.dialogs.deploySettings.dependencySelectPipeline}</option>
                    {pipelines
                      .filter((record) => String(record.pipeline_id ?? '') && String(record.pipeline_id ?? '') !== String(pipelineId ?? ''))
                      .map((record) => {
                        const id = String(record.pipeline_id ?? '')
                        const name = String(record.name ?? id)
                        return (
                          <option key={id} value={id}>
                            {name}
                          </option>
                        )
                      })}
                  </HTMLSelect>
                  <HTMLSelect
                    value={item.status}
                    onChange={(event) => {
                      const value = event.currentTarget.value
                      setDependencyDrafts((current) =>
                        current.map((row) => (row.id === item.id ? { ...row, status: value } : row)),
                      )
                    }}
                  >
                    <option value="DEPLOYED">{uiCopy.dialogs.deploySettings.dependencyStatusDeployed}</option>
                    <option value="SUCCESS">{uiCopy.dialogs.deploySettings.dependencyStatusSuccess}</option>
                  </HTMLSelect>
                  <Button
                    icon="trash"
                    minimal
                    intent={Intent.DANGER}
                    onClick={() => setDependencyDrafts((current) => current.filter((row) => row.id !== item.id))}
                  />
                </div>
              ))}
            </div>
            <Button icon="add" minimal onClick={() => setDependencyDrafts((current) => [...current, dependencyDefaults()])}>
              {uiCopy.dialogs.deploySettings.dependencyAdd}
            </Button>
            <Text className="muted">{uiCopy.dialogs.deploySettings.dependenciesHelp}</Text>
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.branch}>
            <InputGroup
              value={deploySettings.branch}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, branch: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.proposalStatus}>
            <InputGroup
              value={deploySettings.proposalStatus}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, proposalStatus: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.proposalTitle}>
            <InputGroup
              value={deploySettings.proposalTitle}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, proposalTitle: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.deploySettings.proposalDescription}>
            <TextArea
              value={deploySettings.proposalDescription}
              onChange={(event) => {
                const value = event.currentTarget.value
                setDeploySettings((prev) => ({ ...prev, proposalDescription: value }))
              }}
            />
          </FormGroup>
        </div>
        <div className="dialog-footer">
          <Button minimal onClick={() => setDeploySettingsOpen(false)}>{uiCopy.dialogs.deploySettings.cancel}</Button>
          <Button intent={Intent.PRIMARY} onClick={handleSaveSettings}>{uiCopy.dialogs.deploySettings.save}</Button>
        </div>
      </Dialog>

      <Dialog isOpen={outputOpen} onClose={() => setOutputOpen(false)} title={uiCopy.dialogs.output.title}>
        <div className="dialog-body">
          <FormGroup label={uiCopy.dialogs.output.outputName} labelFor="pipeline-output-name">
            <InputGroup
              id="pipeline-output-name"
              value={outputDraft.name}
              onChange={(event) => {
                const value = event.currentTarget.value
                setOutputDraft((prev) => ({ ...prev, name: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.datasetName} labelFor="pipeline-output-dataset-name">
            <InputGroup
              id="pipeline-output-dataset-name"
              value={outputDraft.datasetName}
              onChange={(event) => {
                const value = event.currentTarget.value
                setOutputDraft((prev) => ({ ...prev, datasetName: value }))
              }}
            />
          </FormGroup>
          <FormGroup label={uiCopy.dialogs.output.description} labelFor="pipeline-output-description">
            <TextArea
              id="pipeline-output-description"
              value={outputDraft.description}
              onChange={(event) => {
                const value = event.currentTarget.value
                setOutputDraft((prev) => ({ ...prev, description: value }))
              }}
            />
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
