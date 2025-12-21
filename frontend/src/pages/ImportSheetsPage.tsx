import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Checkbox,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  ProgressBar,
} from '@blueprintjs/core'
import {
  extractWriteCommandIds,
  getOntologySchema,
  gridSheet,
  importFromSheetsCommit,
  importFromSheetsDryRun,
  previewSheet,
  saveMappingMetadata,
  suggestMappingsFromSheets,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry } from '../hooks/useOntologyRegistry'
import { useCooldown } from '../hooks/useCooldown'
import { HttpError } from '../api/bff'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

type ImportMapping = { source_field: string; target_field: string; confidence?: number }

type TargetField = { name: string; type: string }

export const ImportSheetsPage = () => {
  const { db } = useParams()
  const [searchParams] = useSearchParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const trackCommands = useAppStore((state) => state.trackCommands)

  const registry = useOntologyRegistry(db, context.branch)

  const readNumberParam = (key: string) => {
    const value = searchParams.get(key)
    if (!value) {
      return null
    }
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : null
  }

  const [step, setStep] = useState(0)
  const [sheetUrl, setSheetUrl] = useState(searchParams.get('sheet_url') ?? '')
  const [worksheetName, setWorksheetName] = useState(searchParams.get('worksheet_name') ?? '')
  const [apiKey, setApiKey] = useState(searchParams.get('api_key') ?? '')
  const [tableId, setTableId] = useState(searchParams.get('table_id') ?? '')
  const [bboxTop, setBboxTop] = useState<number | null>(readNumberParam('bbox_top'))
  const [bboxLeft, setBboxLeft] = useState<number | null>(readNumberParam('bbox_left'))
  const [bboxBottom, setBboxBottom] = useState<number | null>(readNumberParam('bbox_bottom'))
  const [bboxRight, setBboxRight] = useState<number | null>(readNumberParam('bbox_right'))
  const [targetClassId, setTargetClassId] = useState('')
  const [targetSchema, setTargetSchema] = useState<TargetField[]>([])
  const [mappings, setMappings] = useState<ImportMapping[]>([])
  const [previewResult, setPreviewResult] = useState<any>(null)
  const [gridResult, setGridResult] = useState<any>(null)
  const [suggestResult, setSuggestResult] = useState<any>(null)
  const [dryRunResult, setDryRunResult] = useState<any>(null)
  const [commitResult, setCommitResult] = useState<any>(null)
  const [confirmMain, setConfirmMain] = useState(false)
  const [mappingMetaResult, setMappingMetaResult] = useState<any>(null)

  const previewCooldown = useCooldown()
  const gridCooldown = useCooldown()
  const suggestCooldown = useCooldown()
  const dryRunCooldown = useCooldown()
  const commitCooldown = useCooldown()

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const buildSchemaFromRegistry = () => {
    const props = registry.propertyLabelsByClass.get(targetClassId) ?? []
    return props.map((prop) => ({ name: prop.id, type: prop.type ?? 'xsd:string' }))
  }

  const extractSchemaFromPayload = (payload: any): TargetField[] | null => {
    if (!payload || typeof payload !== 'object') {
      return null
    }
    const candidate =
      payload.properties ??
      payload.schema?.properties ??
      payload.data?.properties ??
      payload.schema?.data?.properties
    if (Array.isArray(candidate)) {
      return candidate.map((prop: any) => ({
        name: prop.name ?? prop.id ?? prop.property_id ?? '',
        type: prop.type ?? prop.datatype ?? 'xsd:string',
      }))
    }
    if (candidate && typeof candidate === 'object') {
      return Object.entries(candidate).map(([name, value]) => ({
        name,
        type: (value as any)?.type ?? (value as any)?.datatype ?? 'xsd:string',
      }))
    }
    return null
  }

  const schemaMutation = useMutation({
    mutationFn: async () => {
      if (!db || !targetClassId) {
        throw new Error('Missing class')
      }
      return getOntologySchema(requestContext, db, targetClassId, context.branch, 'json')
    },
    onSuccess: (result) => {
      const parsed = extractSchemaFromPayload(result)
      if (parsed && parsed.length) {
        setTargetSchema(parsed)
        return
      }
      setTargetSchema(buildSchemaFromRegistry())
    },
    onError: (error) => {
      setTargetSchema(buildSchemaFromRegistry())
      toastApiError(error, context.language)
    },
  })

  const bodyBase = () => ({
    sheet_url: sheetUrl,
    worksheet_name: worksheetName || undefined,
    api_key: apiKey || undefined,
    target_class_id: targetClassId,
    target_schema: targetSchema,
    mappings,
    table_id: tableId || undefined,
    table_bbox:
      bboxTop !== null && bboxLeft !== null && bboxBottom !== null && bboxRight !== null
        ? { top: bboxTop, left: bboxLeft, bottom: bboxBottom, right: bboxRight }
        : undefined,
  })

  const gridPreview = useMemo(() => {
    const grid = (gridResult as any)?.grid
    if (!Array.isArray(grid)) {
      return []
    }
    return grid.slice(0, 10).map((row: any[]) => row.slice(0, 10))
  }, [gridResult])

  const gridTables = useMemo(() => {
    const direct = (gridResult as any)?.tables ?? (gridResult as any)?.table_candidates
    const nested =
      (gridResult as any)?.structure?.tables ??
      (gridResult as any)?.data?.structure?.tables ??
      (gridResult as any)?.data?.tables
    const tables = Array.isArray(direct) ? direct : Array.isArray(nested) ? nested : []
    return tables.map((table: any, index: number) => ({
      id: table.table_id ?? table.id ?? table.tableId ?? `table-${index + 1}`,
      bbox: table.bbox ?? table.bounding_box ?? table.table_bbox ?? table.tableBBox ?? null,
    }))
  }, [gridResult])

  const applyTableSelection = (table: { id?: string; bbox?: any }) => {
    if (table.id) {
      setTableId(table.id)
    }
    if (table.bbox) {
      setBboxTop(typeof table.bbox.top === 'number' ? table.bbox.top : null)
      setBboxLeft(typeof table.bbox.left === 'number' ? table.bbox.left : null)
      setBboxBottom(typeof table.bbox.bottom === 'number' ? table.bbox.bottom : null)
      setBboxRight(typeof table.bbox.right === 'number' ? table.bbox.right : null)
    }
  }

  const previewMutation = useMutation({
    mutationFn: () =>
      previewSheet(
        requestContext,
        { sheet_url: sheetUrl, worksheet_name: worksheetName || undefined, api_key: apiKey || undefined },
        10,
      ),
    onSuccess: (result) => setPreviewResult(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        previewCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const gridMutation = useMutation({
    mutationFn: () =>
      gridSheet(requestContext, {
        sheet_url: sheetUrl,
        worksheet_name: worksheetName || undefined,
        api_key: apiKey || undefined,
      }),
    onSuccess: (result) => setGridResult(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        gridCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const suggestMutation = useMutation({
    mutationFn: () => suggestMappingsFromSheets(requestContext, db ?? '', bodyBase()),
    onSuccess: (result) => {
      setSuggestResult(result)
      setMappings((result as any)?.mappings ?? [])
    },
    onError: (error) => {
      if (error instanceof HttpError) {
        suggestCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const dryRunMutation = useMutation({
    mutationFn: () => importFromSheetsDryRun(requestContext, db ?? '', bodyBase()),
    onSuccess: (result) => setDryRunResult(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        dryRunCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const commitMutation = useMutation({
    mutationFn: () => importFromSheetsCommit(requestContext, db ?? '', bodyBase()),
    onSuccess: (result) => {
      setCommitResult(result)
      const commandIds = extractWriteCommandIds(result)
      if (commandIds.length > 0) {
        trackCommands(
          commandIds.map((id) => ({
            id,
            kind: 'IMPORT',
            title: 'Import from Sheets',
            targetDbName: db ?? '',
          })),
        )
      }
    },
    onError: (error) => {
      if (error instanceof HttpError) {
        commitCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const mappingMetaMutation = useMutation({
    mutationFn: async () => {
      if (!db || !targetClassId) {
        throw new Error('Missing class')
      }
      const confidenceValues = mappings
        .map((mapping) => mapping.confidence)
        .filter((value): value is number => typeof value === 'number')
      const avgConfidence = confidenceValues.length
        ? confidenceValues.reduce((sum, value) => sum + value, 0) / confidenceValues.length
        : 0
      return saveMappingMetadata(requestContext, db, targetClassId, {
        timestamp: new Date().toISOString(),
        sourceFile: sheetUrl || 'google-sheets',
        mappingsCount: mappings.length,
        averageConfidence: avgConfidence,
        mappingDetails: mappings.map((mapping) => ({
          source_field: mapping.source_field,
          target_field: mapping.target_field,
          confidence: mapping.confidence ?? null,
        })),
      })
    },
    onSuccess: (result) => {
      setMappingMetaResult(result)
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const dryRunErrors = useMemo(() => {
    const errors = (dryRunResult as any)?.errors ?? (dryRunResult as any)?.data?.errors
    return Array.isArray(errors) ? errors : []
  }, [dryRunResult])

  const commitCommands = useMemo(() => {
    const write = (commitResult as any)?.write
    const commands = write?.commands ?? (commitResult as any)?.data?.write?.commands ?? []
    return Array.isArray(commands) ? commands : []
  }, [commitResult])

  const hasTable =
    Boolean(tableId) ||
    (bboxTop !== null && bboxLeft !== null && bboxBottom !== null && bboxRight !== null)
  const canProceed = Boolean(sheetUrl && targetClassId && targetSchema.length > 0 && hasTable)

  return (
    <div>
      <PageHeader title="Import: Google Sheets" subtitle="Prepare → Suggest → Dry-run → Commit" />

      <Card elevation={1} className="section-card">
        <div className="stepper">
          {['Prepare', 'Suggest', 'Dry-run', 'Commit'].map((label, index) => (
            <div key={label} className={`stepper-item ${step === index ? 'active' : ''}`}>
              <div className="stepper-index">{index + 1}</div>
              <div>{label}</div>
            </div>
          ))}
        </div>
      </Card>

      {step === 0 ? (
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Step 1. Prepare</H5>
          </div>
          <div className="form-grid">
            <FormGroup label="Sheet URL">
              <InputGroup value={sheetUrl} onChange={(event) => setSheetUrl(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="Worksheet">
              <InputGroup value={worksheetName} onChange={(event) => setWorksheetName(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="API Key">
              <InputGroup value={apiKey} onChange={(event) => setApiKey(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="Target class">
              <HTMLSelect
                value={targetClassId}
                onChange={(event) => setTargetClassId(event.currentTarget.value)}
                options={[{ label: 'Select class', value: '' }, ...registry.classOptions]}
              />
            </FormGroup>
          </div>
          <div className="form-row">
            <InputGroup placeholder="table_id" value={tableId} onChange={(event) => setTableId(event.currentTarget.value)} />
            <NumericInput placeholder="bbox top" value={bboxTop ?? undefined} onValueChange={(value) => setBboxTop(Number.isNaN(value) ? null : value)} />
            <NumericInput placeholder="bbox left" value={bboxLeft ?? undefined} onValueChange={(value) => setBboxLeft(Number.isNaN(value) ? null : value)} />
            <NumericInput placeholder="bbox bottom" value={bboxBottom ?? undefined} onValueChange={(value) => setBboxBottom(Number.isNaN(value) ? null : value)} />
            <NumericInput placeholder="bbox right" value={bboxRight ?? undefined} onValueChange={(value) => setBboxRight(Number.isNaN(value) ? null : value)} />
          </div>
          <div className="form-row">
            <Button
              intent={Intent.PRIMARY}
              onClick={() => previewMutation.mutate()}
              disabled={!sheetUrl || previewCooldown.active}
              loading={previewMutation.isPending}
            >
              Preview {previewCooldown.active ? `(${previewCooldown.remainingSeconds}s)` : ''}
            </Button>
            <Button
              onClick={() => gridMutation.mutate()}
              disabled={!sheetUrl || gridCooldown.active}
              loading={gridMutation.isPending}
            >
              Detect Grid {gridCooldown.active ? `(${gridCooldown.remainingSeconds}s)` : ''}
            </Button>
            <Button icon="manual" onClick={() => schemaMutation.mutate()} disabled={!targetClassId} loading={schemaMutation.isPending}>
              Load Target Schema
            </Button>
          </div>
          <JsonView value={targetSchema} fallback="Target schema will appear here." />

          <div className="section-grid">
            <Card elevation={0} className="section-card">
              <div className="card-title">
                <H5>Preview</H5>
              </div>
              {previewResult?.columns?.length ? (
                <HTMLTable striped className="full-width">
                  <thead>
                    <tr>
                      {previewResult.columns.map((col: string) => (
                        <th key={col}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(previewResult.sample_rows ?? []).map((row: any[], index: number) => (
                      <tr key={`preview-${index}`}>
                        {row.map((cell: any, idx: number) => (
                          <td key={idx}>{cell}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </HTMLTable>
              ) : (
                <JsonView value={previewResult} fallback="프리뷰 결과가 표시됩니다." />
              )}
            </Card>
            <Card elevation={0} className="section-card">
              <div className="card-title">
                <H5>Grid Detect</H5>
              </div>
              {gridPreview.length ? (
                <HTMLTable striped className="full-width">
                  <tbody>
                    {gridPreview.map((row, rowIndex) => (
                      <tr key={`grid-${rowIndex}`}>
                        {row.map((cell: any, colIndex: number) => (
                          <td key={`grid-cell-${rowIndex}-${colIndex}`}>{cell ?? ''}</td>
                        ))}
                      </tr>
                    ))}
                  </tbody>
                </HTMLTable>
              ) : (
                <JsonView value={gridResult} fallback="그리드 감지 결과가 표시됩니다." />
              )}
              {gridTables.length ? (
                <HTMLTable striped className="full-width">
                  <thead>
                    <tr>
                      <th>Table ID</th>
                      <th>Bounding Box</th>
                      <th></th>
                    </tr>
                  </thead>
                  <tbody>
                    {gridTables.map((table) => (
                      <tr key={table.id}>
                        <td>{table.id}</td>
                        <td>
                          {table.bbox
                            ? `top:${table.bbox.top}, left:${table.bbox.left}, bottom:${table.bbox.bottom}, right:${table.bbox.right}`
                            : '-'}
                        </td>
                        <td>
                          <Button minimal onClick={() => applyTableSelection(table)}>
                            Use table
                          </Button>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </HTMLTable>
              ) : (
                <Callout intent={Intent.PRIMARY}>테이블 감지가 없으면 table_id/bbox를 수동으로 입력하세요.</Callout>
              )}
            </Card>
          </div>
          <Button intent={Intent.PRIMARY} onClick={() => setStep(1)} disabled={!canProceed}>
            Next
          </Button>
        </Card>
      ) : null}

      {step === 1 ? (
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Step 2. Suggest Mappings</H5>
            <Button
              intent={Intent.PRIMARY}
              onClick={() => suggestMutation.mutate()}
              loading={suggestMutation.isPending}
              disabled={!canProceed || suggestCooldown.active}
            >
              Suggest {suggestCooldown.active ? `(${suggestCooldown.remainingSeconds}s)` : ''}
            </Button>
            <Button
              onClick={() => mappingMetaMutation.mutate()}
              disabled={!targetClassId || mappings.length === 0}
              loading={mappingMetaMutation.isPending}
            >
              Save mapping metadata
            </Button>
          </div>
          <HTMLTable striped className="full-width">
            <thead>
              <tr>
                <th>Source</th>
                <th>Target</th>
                <th>Confidence</th>
              </tr>
            </thead>
            <tbody>
              {mappings.map((map, index) => (
                <tr key={`${map.source_field}-${index}`}>
                  <td>
                    <InputGroup
                      value={map.source_field}
                      onChange={(event) => {
                        const value = event.currentTarget.value
                        setMappings((prev) => {
                          const next = [...prev]
                          next[index] = { ...next[index], source_field: value }
                          return next
                        })
                      }}
                    />
                  </td>
                  <td>
                    <InputGroup
                      value={map.target_field}
                      onChange={(event) => {
                        const value = event.currentTarget.value
                        setMappings((prev) => {
                          const next = [...prev]
                          next[index] = { ...next[index], target_field: value }
                          return next
                        })
                      }}
                    />
                  </td>
                  <td>{map.confidence ? map.confidence.toFixed(2) : '-'}</td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
          <JsonView value={suggestResult} fallback="매핑 제안을 실행하세요." />
          <JsonView value={mappingMetaResult} fallback="매핑 메타데이터 저장 결과가 표시됩니다." />
          <div className="button-row">
            <Button onClick={() => setStep(0)}>Back</Button>
            <Button intent={Intent.PRIMARY} onClick={() => setStep(2)} disabled={mappings.length === 0}>
              Next
            </Button>
          </div>
        </Card>
      ) : null}

      {step === 2 ? (
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Step 3. Dry-run</H5>
            <Button
              intent={Intent.PRIMARY}
              onClick={() => dryRunMutation.mutate()}
              loading={dryRunMutation.isPending}
              disabled={!mappings.length || dryRunCooldown.active}
            >
              Run Dry-run {dryRunCooldown.active ? `(${dryRunCooldown.remainingSeconds}s)` : ''}
            </Button>
          </div>
          {dryRunMutation.isPending ? <ProgressBar /> : null}
          {dryRunErrors.length ? (
            <HTMLTable striped className="full-width">
              <thead>
                <tr>
                  <th>Row</th>
                  <th>Source</th>
                  <th>Target</th>
                  <th>Message</th>
                </tr>
              </thead>
              <tbody>
                {dryRunErrors.map((error: any, index: number) => (
                  <tr key={`error-${index}`}>
                    <td>{error.row_index ?? '-'}</td>
                    <td>{error.source_field ?? '-'}</td>
                    <td>{error.target_field ?? '-'}</td>
                    <td>{error.message ?? error.detail ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          ) : null}
          <JsonView value={dryRunResult} fallback="Dry-run 결과가 표시됩니다." />
          <div className="button-row">
            <Button onClick={() => setStep(1)}>Back</Button>
            <Button intent={Intent.PRIMARY} onClick={() => setStep(3)} disabled={!dryRunResult}>
              Next
            </Button>
          </div>
        </Card>
      ) : null}

      {step === 3 ? (
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Step 4. Commit</H5>
            <Button
              onClick={() => {
                const commandIds = extractWriteCommandIds(commitResult)
                if (commandIds.length) {
                  trackCommands(
                    commandIds.map((id) => ({
                      id,
                      kind: 'IMPORT',
                      title: 'Import from Sheets',
                      targetDbName: db ?? '',
                    })),
                  )
                }
              }}
              disabled={commitCommands.length === 0}
            >
              Track All
            </Button>
          </div>
          {context.branch !== 'main' ? (
            <Callout intent={Intent.WARNING} title="Commit will write to main">
              현재 브랜치({context.branch})와 무관하게 main에 반영됩니다.
              <Checkbox checked={confirmMain} onChange={(event) => setConfirmMain(event.currentTarget.checked)}>
                위 내용을 이해했습니다.
              </Checkbox>
            </Callout>
          ) : null}
          <Button
            intent={Intent.DANGER}
            onClick={() => commitMutation.mutate()}
            loading={commitMutation.isPending}
            disabled={(context.branch !== 'main' && !confirmMain) || commitCooldown.active}
          >
            Commit to main {commitCooldown.active ? `(${commitCooldown.remainingSeconds}s)` : ''}
          </Button>
          {commitCommands.length ? (
            <HTMLTable striped className="full-width">
              <thead>
                <tr>
                  <th>Command ID</th>
                  <th>Status URL</th>
                </tr>
              </thead>
              <tbody>
                {commitCommands.map((command: any, index: number) => (
                  <tr key={`${command.command_id ?? command.command?.command_id ?? index}`}>
                    <td>{command.command_id ?? command.command?.command_id ?? command.command?.id ?? '-'}</td>
                    <td>{command.status_url ?? command.command?.status_url ?? '-'}</td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          ) : null}
          <JsonView value={commitResult} fallback="커밋 결과가 표시됩니다." />
          <Button onClick={() => setStep(2)}>Back</Button>
        </Card>
      ) : null}
    </div>
  )
}
