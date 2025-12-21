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
import { asArray, asRecord, getNumber, getString, type UnknownRecord } from '../utils/typed'

type ImportMapping = { source_field: string; target_field: string; confidence?: number }

type TargetField = { name: string; type: string }

type TableCandidate = { id: string; bbox: UnknownRecord | null }

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
  const [previewResult, setPreviewResult] = useState<unknown>(null)
  const [gridResult, setGridResult] = useState<unknown>(null)
  const [suggestResult, setSuggestResult] = useState<unknown>(null)
  const [dryRunResult, setDryRunResult] = useState<unknown>(null)
  const [commitResult, setCommitResult] = useState<unknown>(null)
  const [confirmMain, setConfirmMain] = useState(false)
  const [mappingMetaResult, setMappingMetaResult] = useState<unknown>(null)

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

  const extractSchemaFromPayload = (payload: unknown): TargetField[] | null => {
    const root = asRecord(payload)
    const schema = asRecord(root.schema)
    const data = asRecord(root.data)
    const schemaData = asRecord(schema.data)
    const candidate =
      root.properties ?? schema.properties ?? data.properties ?? schemaData.properties
    if (Array.isArray(candidate)) {
      return candidate.map((prop) => {
        const record = asRecord(prop)
        return {
          name: getString(record.name) ?? getString(record.id) ?? getString(record.property_id) ?? '',
          type: getString(record.type) ?? getString(record.datatype) ?? 'xsd:string',
        }
      })
    }
    if (candidate && typeof candidate === 'object') {
      return Object.entries(candidate).map(([name, value]) => {
        const record = asRecord(value)
        return {
          name,
          type: getString(record.type) ?? getString(record.datatype) ?? 'xsd:string',
        }
      })
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
    const grid = asArray<unknown[]>(asRecord(gridResult).grid)
    if (!grid.length) {
      return []
    }
    return grid.slice(0, 10).map((row) => asArray<unknown>(row).slice(0, 10))
  }, [gridResult])

  const previewColumns = useMemo(
    () =>
      asArray<unknown>(asRecord(previewResult).columns).filter(
        (col): col is string => typeof col === 'string',
      ),
    [previewResult],
  )

  const previewRows = useMemo(
    () => asArray<unknown[]>(asRecord(previewResult).sample_rows),
    [previewResult],
  )

  const gridTables = useMemo<TableCandidate[]>(() => {
    const gridRecord = asRecord(gridResult)
    const direct = gridRecord.tables ?? gridRecord.table_candidates
    const nested =
      asRecord(gridRecord.structure).tables ??
      asRecord(asRecord(gridRecord.data).structure).tables ??
      asRecord(gridRecord.data).tables
    const tables = Array.isArray(direct) ? direct : Array.isArray(nested) ? nested : []
    return tables.map((table, index) => {
      const record = asRecord(table)
      const bbox = asRecord(
        record.bbox ?? record.bounding_box ?? record.table_bbox ?? record.tableBBox,
      )
      return {
        id:
          getString(record.table_id) ??
          getString(record.id) ??
          getString(record.tableId) ??
          `table-${index + 1}`,
        bbox: Object.keys(bbox).length ? bbox : null,
      }
    })
  }, [gridResult])

  const applyTableSelection = (table: TableCandidate) => {
    if (table.id) {
      setTableId(table.id)
    }
    if (table.bbox) {
      setBboxTop(getNumber(table.bbox.top) ?? null)
      setBboxLeft(getNumber(table.bbox.left) ?? null)
      setBboxBottom(getNumber(table.bbox.bottom) ?? null)
      setBboxRight(getNumber(table.bbox.right) ?? null)
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
      const resultRecord = asRecord(result)
      const nextMappings = asArray<ImportMapping>(resultRecord.mappings ?? asRecord(resultRecord.data).mappings)
      setMappings(nextMappings)
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
    const resultRecord = asRecord(dryRunResult)
    return asArray<UnknownRecord>(resultRecord.errors ?? asRecord(resultRecord.data).errors)
  }, [dryRunResult])

  const commitCommands = useMemo(() => {
    const resultRecord = asRecord(commitResult)
    const write = asRecord(resultRecord.write ?? asRecord(asRecord(resultRecord.data).write))
    return asArray<UnknownRecord>(write.commands)
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
              {previewColumns.length ? (
                <HTMLTable striped className="full-width">
                  <thead>
                    <tr>
                      {previewColumns.map((col) => (
                        <th key={col}>{col}</th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {previewRows.map((row, index) => (
                      <tr key={`preview-${index}`}>
                        {asArray<unknown>(row).map((cell, idx) => (
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
                        {row.map((cell, colIndex) => (
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
                    {gridTables.map((table) => {
                      const bbox = table.bbox
                      const top = bbox ? getNumber(bbox.top) : null
                      const left = bbox ? getNumber(bbox.left) : null
                      const bottom = bbox ? getNumber(bbox.bottom) : null
                      const right = bbox ? getNumber(bbox.right) : null
                      return (
                        <tr key={table.id}>
                          <td>{table.id}</td>
                          <td>
                            {bbox
                              ? `top:${top ?? '-'}, left:${left ?? '-'}, bottom:${bottom ?? '-'}, right:${right ?? '-'}`
                              : '-'}
                          </td>
                          <td>
                            <Button minimal onClick={() => applyTableSelection(table)}>
                              Use table
                            </Button>
                          </td>
                        </tr>
                      )
                    })}
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
                {dryRunErrors.map((error, index) => {
                  const rowIndex = getNumber(error.row_index)
                  const sourceField = getString(error.source_field) ?? '-'
                  const targetField = getString(error.target_field) ?? '-'
                  const message = getString(error.message) ?? getString(error.detail) ?? '-'
                  return (
                    <tr key={`error-${index}`}>
                      <td>{rowIndex ?? '-'}</td>
                      <td>{sourceField}</td>
                      <td>{targetField}</td>
                      <td>{message}</td>
                    </tr>
                  )
                })}
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
                {commitCommands.map((command, index) => {
                  const nested = asRecord(command.command)
                  const commandId =
                    getString(command.command_id) ?? getString(nested.command_id) ?? getString(nested.id) ?? '-'
                  const statusUrl =
                    getString(command.status_url) ?? getString(nested.status_url) ?? '-'
                  return (
                    <tr key={`${commandId}-${index}`}>
                      <td>{commandId}</td>
                      <td>{statusUrl}</td>
                    </tr>
                  )
                })}
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
