import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  Callout,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  Tag,
  TextArea,
} from '@blueprintjs/core'
import { createOntology, gridSheet, suggestSchemaFromData, suggestSchemaFromSheets, validateOntology } from '../api/bff'
import { AsyncCommandButton } from '../components/AsyncCommandButton'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

const extractSuggestedClasses = (payload: any): any[] => {
  const suggestion = payload?.suggested_schema ?? payload?.schema_suggestion ?? payload
  if (!suggestion) {
    return []
  }
  if (Array.isArray(suggestion)) {
    return suggestion
  }
  if (Array.isArray(suggestion?.classes)) {
    return suggestion.classes
  }
  if (Array.isArray(suggestion?.ontologies)) {
    return suggestion.ontologies
  }
  if (suggestion?.class_id || suggestion?.id) {
    return [suggestion]
  }
  return []
}

const buildOntologyPayload = (klass: any) => ({
  id: klass.id ?? klass.class_id ?? klass.name,
  label: klass.label ?? klass.name ?? klass.id,
  description: klass.description ?? undefined,
  properties: klass.properties ?? [],
  relationships: klass.relationships ?? [],
})

export const SchemaSuggestionPage = () => {
  const { db } = useParams()
  const [searchParams] = useSearchParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const readNumberParam = (key: string) => {
    const value = searchParams.get(key)
    if (!value) {
      return null
    }
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : null
  }

  const [source, setSource] = useState<'sheets' | 'paste'>('sheets')
  const [sheetUrl, setSheetUrl] = useState(searchParams.get('sheet_url') ?? '')
  const [worksheetName, setWorksheetName] = useState(searchParams.get('worksheet_name') ?? '')
  const [apiKey, setApiKey] = useState(searchParams.get('api_key') ?? '')
  const [tableId, setTableId] = useState(searchParams.get('table_id') ?? '')
  const [bboxTop, setBboxTop] = useState<number | null>(readNumberParam('bbox_top'))
  const [bboxLeft, setBboxLeft] = useState<number | null>(readNumberParam('bbox_left'))
  const [bboxBottom, setBboxBottom] = useState<number | null>(readNumberParam('bbox_bottom'))
  const [bboxRight, setBboxRight] = useState<number | null>(readNumberParam('bbox_right'))
  const [className, setClassName] = useState('')
  const [columnsJson, setColumnsJson] = useState('')
  const [rowsJson, setRowsJson] = useState('')
  const [gridResult, setGridResult] = useState<any>(null)
  const [result, setResult] = useState<any>(null)
  const [validationResult, setValidationResult] = useState<Record<string, unknown>>({})

  const suggestMutation = useMutation({
    mutationFn: async () => {
      if (!db) {
        throw new Error('Missing db')
      }
      if (source === 'sheets') {
        const table_bbox =
          bboxTop !== null && bboxLeft !== null && bboxBottom !== null && bboxRight !== null
            ? { top: bboxTop, left: bboxLeft, bottom: bboxBottom, right: bboxRight }
            : undefined
        return suggestSchemaFromSheets(requestContext, db, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          class_name: className || undefined,
          api_key: apiKey || undefined,
          table_id: tableId || undefined,
          table_bbox,
        })
      }
      const columns = JSON.parse(columnsJson || '[]')
      const data = JSON.parse(rowsJson || '[]')
      return suggestSchemaFromData(requestContext, db, {
        columns,
        data,
        class_name: className || undefined,
      })
    },
    onSuccess: (payload) => {
      setResult(payload)
      setValidationResult({})
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const gridMutation = useMutation({
    mutationFn: () =>
      gridSheet(requestContext, {
        sheet_url: sheetUrl,
        worksheet_name: worksheetName || undefined,
        api_key: apiKey || undefined,
      }),
    onSuccess: (payload) => setGridResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const validateMutation = useMutation({
    mutationFn: async () => {
      if (!db) {
        return {}
      }
      const classes = extractSuggestedClasses(result)
      const next: Record<string, unknown> = {}
      for (const klass of classes) {
        const payload = buildOntologyPayload(klass)
        const key = payload.id ?? payload.label ?? 'class'
        next[key] = await validateOntology(requestContext, db, context.branch, payload)
      }
      return next
    },
    onSuccess: (payload) => setValidationResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

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
    if (!table.id) {
      return
    }
    setTableId(table.id)
    if (table.bbox) {
      setBboxTop(typeof table.bbox.top === 'number' ? table.bbox.top : null)
      setBboxLeft(typeof table.bbox.left === 'number' ? table.bbox.left : null)
      setBboxBottom(typeof table.bbox.bottom === 'number' ? table.bbox.bottom : null)
      setBboxRight(typeof table.bbox.right === 'number' ? table.bbox.right : null)
    }
  }

  const suggestedClasses = extractSuggestedClasses(result)

  return (
    <div>
      <PageHeader title="Schema Suggestion" subtitle="샘플 데이터를 기반으로 스키마를 제안합니다." />

      <Card elevation={1} className="section-card">
        <div className="form-row">
          <HTMLSelect
            value={source}
            onChange={(event) => setSource(event.currentTarget.value as 'sheets' | 'paste')}
            options={[
              { label: 'Google Sheets', value: 'sheets' },
              { label: 'Paste JSON', value: 'paste' },
            ]}
          />
          <InputGroup
            placeholder="Class name (optional)"
            value={className}
            onChange={(event) => setClassName(event.currentTarget.value)}
          />
        </div>

        {source === 'sheets' ? (
          <>
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
            </div>
            <div className="form-grid">
              <FormGroup label="Table ID">
                <InputGroup value={tableId} onChange={(event) => setTableId(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="BBox Top">
                <NumericInput
                  value={bboxTop ?? undefined}
                  onValueChange={(value) => setBboxTop(Number.isNaN(value) ? null : value)}
                />
              </FormGroup>
              <FormGroup label="BBox Left">
                <NumericInput
                  value={bboxLeft ?? undefined}
                  onValueChange={(value) => setBboxLeft(Number.isNaN(value) ? null : value)}
                />
              </FormGroup>
              <FormGroup label="BBox Bottom">
                <NumericInput
                  value={bboxBottom ?? undefined}
                  onValueChange={(value) => setBboxBottom(Number.isNaN(value) ? null : value)}
                />
              </FormGroup>
              <FormGroup label="BBox Right">
                <NumericInput
                  value={bboxRight ?? undefined}
                  onValueChange={(value) => setBboxRight(Number.isNaN(value) ? null : value)}
                />
              </FormGroup>
            </div>
            <div className="button-row">
              <Button
                onClick={() => gridMutation.mutate()}
                disabled={!sheetUrl}
                loading={gridMutation.isPending}
              >
                Detect Tables
              </Button>
            </div>
            {gridTables.length ? (
              <HTMLTable striped className="full-width">
                <thead>
                  <tr>
                    <th>Table ID</th>
                    <th>Bounding Box</th>
                    <th>Action</th>
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
                          Use
                        </Button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            ) : (
              <Callout intent={Intent.PRIMARY}>테이블 감지가 없으면 Table ID/BBox를 수동으로 입력하세요.</Callout>
            )}
          </>
        ) : (
          <div className="form-grid">
            <FormGroup label="Columns (JSON array)">
              <TextArea
                rows={6}
                value={columnsJson}
                onChange={(event) => setColumnsJson(event.currentTarget.value)}
                placeholder='["col1","col2"]'
              />
            </FormGroup>
            <FormGroup label="Rows (JSON array)">
              <TextArea
                rows={6}
                value={rowsJson}
                onChange={(event) => setRowsJson(event.currentTarget.value)}
                placeholder='[["a","b"],["c","d"]]'
              />
            </FormGroup>
          </div>
        )}

        <Button intent={Intent.PRIMARY} onClick={() => suggestMutation.mutate()} loading={suggestMutation.isPending}>
          Suggest Schema
        </Button>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Suggested Schema</H5>
          <div className="button-row">
            <Button
              icon="endorsed"
              onClick={() => validateMutation.mutate()}
              disabled={suggestedClasses.length === 0}
              loading={validateMutation.isPending}
            >
              Validate Ontology
            </Button>
            <AsyncCommandButton
              intent={Intent.SUCCESS}
              icon="cloud-upload"
              disabled={suggestedClasses.length === 0}
              commandKind="ONTOLOGY_APPLY"
              commandTitle="Apply suggested schema"
              dbName={db ?? undefined}
              branch={context.branch}
              successMessage="Schema apply 요청을 전송했습니다."
              onSubmit={async () => {
                if (!db) {
                  return []
                }
                const responses = []
                for (const klass of suggestedClasses) {
                  const payload = buildOntologyPayload(klass)
                  responses.push(await createOntology(requestContext, db, context.branch, payload))
                }
                return responses
              }}
            >
              Apply (202)
            </AsyncCommandButton>
          </div>
        </div>
        {suggestedClasses.length ? (
          <div className="card-grid">
            {suggestedClasses.map((klass) => {
              const payload = buildOntologyPayload(klass)
              const key = payload.id ?? payload.label ?? 'class'
              const properties = Array.isArray(payload.properties) ? payload.properties : []
              const relationships = Array.isArray(payload.relationships) ? payload.relationships : []
              return (
                <Card key={key} elevation={0} className="schema-card">
                  <div className="card-title">
                    <H5>{payload.label ?? payload.id ?? 'Class'}</H5>
                    {payload.id ? <Tag minimal>id: {payload.id}</Tag> : null}
                  </div>
                  <div className="muted small">Properties</div>
                  {properties.length ? (
                    <HTMLTable striped className="full-width">
                      <thead>
                        <tr>
                          <th>Name</th>
                          <th>Type</th>
                          <th>Required</th>
                        </tr>
                      </thead>
                      <tbody>
                        {properties.map((prop: any, index: number) => (
                          <tr key={`${prop.name ?? prop.id ?? index}`}>
                            <td>{prop.name ?? prop.id ?? prop.property_id ?? '-'}</td>
                            <td>{prop.type ?? prop.datatype ?? '-'}</td>
                            <td>{prop.required ? 'yes' : 'no'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </HTMLTable>
                  ) : (
                    <div className="muted small">No properties</div>
                  )}
                  <div className="muted small" style={{ marginTop: 8 }}>
                    Relationships
                  </div>
                  {relationships.length ? (
                    <HTMLTable striped className="full-width">
                      <thead>
                        <tr>
                          <th>Predicate</th>
                          <th>Target</th>
                        </tr>
                      </thead>
                      <tbody>
                        {relationships.map((rel: any, index: number) => (
                          <tr key={`${rel.predicate ?? rel.target ?? index}`}>
                            <td>{rel.predicate ?? rel.name ?? '-'}</td>
                            <td>{rel.target ?? rel.to ?? '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </HTMLTable>
                  ) : (
                    <div className="muted small">No relationships</div>
                  )}
                  {validationResult[key] ? (
                    <div style={{ marginTop: 8 }}>
                      <Tag minimal intent={Intent.SUCCESS}>Validated</Tag>
                      <JsonView value={validationResult[key]} />
                    </div>
                  ) : null}
                </Card>
              )
            })}
          </div>
        ) : (
          <JsonView value={result} fallback="스키마 제안을 실행하세요." />
        )}
      </Card>
    </div>
  )
}
