import { useMemo, useState } from 'react'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  TextArea,
} from '@blueprintjs/core'
import {
  createOntology,
  getSummary,
  suggestSchemaFromData,
  suggestSchemaFromExcel,
  suggestSchemaFromGoogleSheets,
  validateOntologyCreate,
} from '../api/bff'
import { useRateLimitRetry } from '../api/useRateLimitRetry'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { showAppToast } from '../app/AppToaster'
import { useCommandRegistration } from '../commands/useCommandRegistration'
import { extractCommandId } from '../commands/extractCommandId'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

const parseJson = (raw: string) => JSON.parse(raw)

type SuggestedSchema = Record<string, unknown>

const parseBBoxNumber = (value: string) => {
  const parsed = Number(value)
  return Number.isFinite(parsed) ? parsed : null
}

const coerceLabel = (value: unknown, fallback: string) => {
  if (typeof value === 'string' && value.trim()) {
    return value.trim()
  }
  if (value && typeof value === 'object') {
    const localized = value as Record<string, unknown>
    const ko = typeof localized.ko === 'string' ? localized.ko.trim() : ''
    const en = typeof localized.en === 'string' ? localized.en.trim() : ''
    if (ko) return ko
    if (en) return en
    const first = Object.values(localized).find((item) => typeof item === 'string' && item.trim())
    if (typeof first === 'string') return first.trim()
  }
  return fallback
}

const extractSuggestedSchemas = (payload: unknown): SuggestedSchema[] => {
  if (!payload || typeof payload !== 'object') return []
  const data = payload as Record<string, unknown>
  const candidate =
    data.suggested_schema ||
    data.schema_suggestion ||
    (data.data as Record<string, unknown> | undefined)?.suggested_schema ||
    (data.data as Record<string, unknown> | undefined)?.schema_suggestion
  if (Array.isArray(candidate)) {
    return candidate.filter((item): item is SuggestedSchema => Boolean(item && typeof item === 'object'))
  }
  if (candidate && typeof candidate === 'object') {
    return [candidate as SuggestedSchema]
  }
  return []
}

const getSchemaId = (schema: SuggestedSchema) =>
  String(schema.id ?? schema['@id'] ?? schema.class_id ?? schema.name ?? '').trim()

const getSchemaLabel = (schema: SuggestedSchema) =>
  coerceLabel(schema.label ?? schema['@label'] ?? schema.display_label, getSchemaId(schema) || 'Unnamed')

export const SchemaSuggestionPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const registerCommand = useCommandRegistration()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const adminToken = useAppStore((state) => state.adminToken)
  const adminMode = useAppStore((state) => state.adminMode)
  const { cooldown: suggestCooldown, withRateLimitRetry } = useRateLimitRetry(1)

  const [sheetUrl, setSheetUrl] = useState('')
  const [worksheetName, setWorksheetName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [sheetTableId, setSheetTableId] = useState('')
  const [sheetTableTop, setSheetTableTop] = useState('')
  const [sheetTableLeft, setSheetTableLeft] = useState('')
  const [sheetTableBottom, setSheetTableBottom] = useState('')
  const [sheetTableRight, setSheetTableRight] = useState('')
  const [sheetTableError, setSheetTableError] = useState<string | null>(null)
  const [className, setClassName] = useState('')
  const [columnsJson, setColumnsJson] = useState('[]')
  const [dataJson, setDataJson] = useState('[]')
  const [excelFile, setExcelFile] = useState<File | null>(null)
  const [excelSheetName, setExcelSheetName] = useState('')
  const [excelTableId, setExcelTableId] = useState('')
  const [excelTableTop, setExcelTableTop] = useState('')
  const [excelTableLeft, setExcelTableLeft] = useState('')
  const [excelTableBottom, setExcelTableBottom] = useState('')
  const [excelTableRight, setExcelTableRight] = useState('')
  const [excelTableError, setExcelTableError] = useState<string | null>(null)

  const buildSheetTableBBox = () => {
    const values = [sheetTableTop, sheetTableLeft, sheetTableBottom, sheetTableRight]
    if (values.every((value) => value.trim() === '')) {
      setSheetTableError(null)
      return undefined
    }
    if (values.some((value) => value.trim() === '')) {
      setSheetTableError('Provide all table bbox values or leave them blank.')
      return undefined
    }
    const parsed = values.map(parseBBoxNumber)
    if (parsed.some((value) => value === null)) {
      setSheetTableError('Table bbox must be numeric values.')
      return undefined
    }
    setSheetTableError(null)
    return {
      top: parsed[0] ?? 0,
      left: parsed[1] ?? 0,
      bottom: parsed[2] ?? 0,
      right: parsed[3] ?? 0,
    }
  }

  const buildExcelTableBBox = () => {
    const values = [excelTableTop, excelTableLeft, excelTableBottom, excelTableRight]
    if (values.every((value) => value.trim() === '')) {
      setExcelTableError(null)
      return undefined
    }
    if (values.some((value) => value.trim() === '')) {
      setExcelTableError('Provide all table bbox values or leave them blank.')
      return undefined
    }
    const parsed = values.map(parseBBoxNumber)
    if (parsed.some((value) => value === null)) {
      setExcelTableError('Table bbox must be numeric values.')
      return undefined
    }
    setExcelTableError(null)
    return {
      top: parsed[0] ?? 0,
      left: parsed[1] ?? 0,
      bottom: parsed[2] ?? 0,
      right: parsed[3] ?? 0,
    }
  }

  const sheetsMutation = useMutation({
    mutationFn: () =>
      withRateLimitRetry(() =>
        suggestSchemaFromGoogleSheets(requestContext, dbName, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          api_key: apiKey || undefined,
          class_name: className || undefined,
          table_id: sheetTableId || undefined,
          table_bbox: buildSheetTableBBox(),
        }),
      ),
    onError: (error) => toastApiError(error, language),
  })

  const dataMutation = useMutation({
    mutationFn: () =>
      suggestSchemaFromData(requestContext, dbName, {
        columns: parseJson(columnsJson),
        data: parseJson(dataJson),
        class_name: className || undefined,
        include_complex_types: true,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const excelMutation = useMutation({
    mutationFn: () => {
      if (!excelFile) {
        throw new Error('Upload an Excel file first')
      }
      const tableBBox = buildExcelTableBBox()
      return withRateLimitRetry(() =>
        suggestSchemaFromExcel(requestContext, dbName, excelFile, {
          sheet_name: excelSheetName || undefined,
          class_name: className || undefined,
          table_id: excelTableId || undefined,
          table_top: tableBBox?.top,
          table_left: tableBBox?.left,
          table_bottom: tableBBox?.bottom,
          table_right: tableBBox?.right,
        }),
      )
    },
    onError: (error) => toastApiError(error, language),
  })

  const summaryQuery = useQuery({
    queryKey: qk.summary({ dbName, branch, language: requestContext.language }),
    queryFn: () => getSummary(requestContext, { dbName, branch }),
  })

  const isProtected = Boolean(
    (summaryQuery.data as { data?: { policy?: { is_protected_branch?: boolean } } } | undefined)?.data
      ?.policy?.is_protected_branch,
  )

  const validateMutation = useMutation({
    mutationFn: (schema: SuggestedSchema) =>
      validateOntologyCreate(requestContext, dbName, branch, schema),
    onError: (error) => toastApiError(error, language),
  })

  const applyMutation = useMutation({
    mutationFn: (schema: SuggestedSchema) => createOntology(requestContext, dbName, branch, schema),
    onSuccess: (payload, schema) => {
      const commandId = extractCommandId(payload)
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'CREATE_ONTOLOGY',
          targetClassId: getSchemaId(schema),
          title: `Create ontology: ${getSchemaLabel(schema)}`,
        })
      }
      void showAppToast({ intent: Intent.SUCCESS, message: 'Ontology create submitted.' })
    },
    onError: (error) => toastApiError(error, language),
  })

  const sheetSchemas = useMemo(() => extractSuggestedSchemas(sheetsMutation.data), [sheetsMutation.data])
  const dataSchemas = useMemo(() => extractSuggestedSchemas(dataMutation.data), [dataMutation.data])
  const excelSchemas = useMemo(() => extractSuggestedSchemas(excelMutation.data), [excelMutation.data])
  const canApply = !isProtected || (adminMode && adminToken)

  const renderSchemaCards = (schemas: SuggestedSchema[]) => {
    if (schemas.length === 0) {
      return <Callout intent={Intent.PRIMARY}>No suggestions yet.</Callout>
    }
    return (
      <div className="card-stack">
        {schemas.map((schema) => {
          const schemaId = getSchemaId(schema)
          const schemaLabel = getSchemaLabel(schema)
          const properties = Array.isArray(schema.properties) ? schema.properties : []
          const relationships = Array.isArray(schema.relationships) ? schema.relationships : []
          return (
            <Card key={schemaId || schemaLabel} className="card-stack">
              <div className="card-title">
                <div>
                  <div>{schemaLabel}</div>
                  <div className="muted small">{schemaId}</div>
                </div>
                <div className="form-row">
                  <Button
                    small
                    icon="tick-circle"
                    onClick={() => validateMutation.mutate(schema)}
                    loading={validateMutation.isPending}
                  >
                    Validate
                  </Button>
                  <Button
                    small
                    intent={Intent.PRIMARY}
                    icon="cloud-upload"
                    onClick={() => applyMutation.mutate(schema)}
                    disabled={!canApply}
                    loading={applyMutation.isPending}
                  >
                    Apply
                  </Button>
                </div>
              </div>
              <TextArea value={JSON.stringify(schema, null, 2)} readOnly rows={8} />
              <div className="form-row">
                <Callout intent={Intent.NONE}>Properties: {properties.length}</Callout>
                <Callout intent={Intent.NONE}>Relationships: {relationships.length}</Callout>
              </div>
            </Card>
          )
        })}
      </div>
    )
  }

  return (
    <div>
      <PageHeader title="Schema Suggestion" subtitle="Generate ontology suggestions from data." />

      {isProtected ? (
        <Callout intent={Intent.WARNING} style={{ marginBottom: 12 }}>
          Protected branch. Admin token + Admin mode required for apply.
        </Callout>
      ) : null}

      <Tabs id="schema-tabs" defaultSelectedTabId="sheets">
        <Tab
          id="sheets"
          title="Google Sheets"
          panel={
            <Card className="card-stack">
              <FormGroup label="Sheet URL">
                <InputGroup value={sheetUrl} onChange={(event) => setSheetUrl(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="Worksheet (optional)">
                <InputGroup value={worksheetName} onChange={(event) => setWorksheetName(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="API key (optional)">
                <InputGroup value={apiKey} onChange={(event) => setApiKey(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="Table ID (optional)">
                <InputGroup value={sheetTableId} onChange={(event) => setSheetTableId(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="Table bbox (optional)">
                <div className="form-row">
                  <InputGroup placeholder="top" value={sheetTableTop} onChange={(event) => setSheetTableTop(event.currentTarget.value)} />
                  <InputGroup placeholder="left" value={sheetTableLeft} onChange={(event) => setSheetTableLeft(event.currentTarget.value)} />
                  <InputGroup placeholder="bottom" value={sheetTableBottom} onChange={(event) => setSheetTableBottom(event.currentTarget.value)} />
                  <InputGroup placeholder="right" value={sheetTableRight} onChange={(event) => setSheetTableRight(event.currentTarget.value)} />
                </div>
              </FormGroup>
              <FormGroup label="Class name (optional)">
                <InputGroup value={className} onChange={(event) => setClassName(event.currentTarget.value)} />
              </FormGroup>
              <Button
                intent={Intent.PRIMARY}
                onClick={() => sheetsMutation.mutate()}
                disabled={!sheetUrl || Boolean(sheetTableError) || suggestCooldown > 0}
                loading={sheetsMutation.isPending}
              >
                {suggestCooldown > 0 ? `Retry in ${suggestCooldown}s` : 'Suggest schema'}
              </Button>
              {sheetTableError ? <Callout intent={Intent.WARNING}>{sheetTableError}</Callout> : null}
              <JsonViewer value={sheetsMutation.data} empty="Suggestion results will appear here." />
              {renderSchemaCards(sheetSchemas)}
            </Card>
          }
        />
        <Tab
          id="excel"
          title="Excel"
          panel={
            <Card className="card-stack">
              <FormGroup label="Upload Excel">
                <input
                  type="file"
                  accept=".xlsx,.xls"
                  onChange={(event) => setExcelFile(event.currentTarget.files?.[0] ?? null)}
                />
              </FormGroup>
              <FormGroup label="Sheet name (optional)">
                <InputGroup value={excelSheetName} onChange={(event) => setExcelSheetName(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="Table ID (optional)">
                <InputGroup value={excelTableId} onChange={(event) => setExcelTableId(event.currentTarget.value)} />
              </FormGroup>
              <FormGroup label="Table bbox (optional)">
                <div className="form-row">
                  <InputGroup placeholder="top" value={excelTableTop} onChange={(event) => setExcelTableTop(event.currentTarget.value)} />
                  <InputGroup placeholder="left" value={excelTableLeft} onChange={(event) => setExcelTableLeft(event.currentTarget.value)} />
                  <InputGroup placeholder="bottom" value={excelTableBottom} onChange={(event) => setExcelTableBottom(event.currentTarget.value)} />
                  <InputGroup placeholder="right" value={excelTableRight} onChange={(event) => setExcelTableRight(event.currentTarget.value)} />
                </div>
              </FormGroup>
              <FormGroup label="Class name (optional)">
                <InputGroup value={className} onChange={(event) => setClassName(event.currentTarget.value)} />
              </FormGroup>
              <Button
                intent={Intent.PRIMARY}
                onClick={() => excelMutation.mutate()}
                disabled={!excelFile || Boolean(excelTableError) || suggestCooldown > 0}
                loading={excelMutation.isPending}
              >
                {suggestCooldown > 0 ? `Retry in ${suggestCooldown}s` : 'Suggest schema'}
              </Button>
              {excelTableError ? <Callout intent={Intent.WARNING}>{excelTableError}</Callout> : null}
              <JsonViewer value={excelMutation.data} empty="Suggestion results will appear here." />
              {renderSchemaCards(excelSchemas)}
            </Card>
          }
        />
        <Tab
          id="paste"
          title="Paste Data"
          panel={
            <Card className="card-stack">
              <FormGroup label="Columns (JSON array)">
                <TextArea value={columnsJson} onChange={(event) => setColumnsJson(event.currentTarget.value)} rows={4} />
              </FormGroup>
              <FormGroup label="Rows (JSON array of arrays)">
                <TextArea value={dataJson} onChange={(event) => setDataJson(event.currentTarget.value)} rows={6} />
              </FormGroup>
              <FormGroup label="Class name (optional)">
                <InputGroup value={className} onChange={(event) => setClassName(event.currentTarget.value)} />
              </FormGroup>
              <Button intent={Intent.PRIMARY} onClick={() => dataMutation.mutate()} loading={dataMutation.isPending}>
                Suggest schema
              </Button>
              <JsonViewer value={dataMutation.data} empty="Suggestion results will appear here." />
              {renderSchemaCards(dataSchemas)}
            </Card>
          }
        />
      </Tabs>

      <Card className="card-stack" style={{ marginTop: 16 }}>
        <div className="card-title">Validation / Apply response</div>
        <JsonViewer value={validateMutation.data} empty="Validation results will appear here." />
        <JsonViewer value={applyMutation.data} empty="Apply responses will appear here." />
      </Card>
    </div>
  )
}
