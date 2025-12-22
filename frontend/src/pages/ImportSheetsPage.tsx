import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
  TextArea,
} from '@blueprintjs/core'
import {
  commitImportFromGoogleSheets,
  dryRunImportFromGoogleSheets,
  getOntology,
  previewGoogleSheet,
  suggestMappingsFromGoogleSheets,
} from '../api/bff'
import { useRateLimitRetry } from '../api/useRateLimitRetry'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { StepperBar } from '../components/StepperBar'
import { showAppToast } from '../app/AppToaster'
import { useCommandRegistration } from '../commands/useCommandRegistration'
import { extractCommandId } from '../commands/extractCommandId'
import { toastApiError } from '../errors/toastApiError'
import { useOntologyRegistry } from '../query/useOntologyRegistry'
import { useAppStore } from '../store/useAppStore'

type ImportTargetField = { name: string; type?: string }
type MappingItem = { source_field: string; target_field: string }

const parseJsonArray = <T,>(raw: string): T[] => {
  const parsed = JSON.parse(raw)
  if (!Array.isArray(parsed)) {
    throw new Error('Expected JSON array')
  }
  return parsed as T[]
}

export const ImportSheetsPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const registerCommand = useCommandRegistration()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const { cooldown: importCooldown, withRateLimitRetry } = useRateLimitRetry(1)
  const registry = useOntologyRegistry(dbName, branch)

  const [step, setStep] = useState(0)
  const [sheetUrl, setSheetUrl] = useState('')
  const [worksheetName, setWorksheetName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [connectionId, setConnectionId] = useState('')
  const [tableId, setTableId] = useState('')
  const [tableTop, setTableTop] = useState('')
  const [tableLeft, setTableLeft] = useState('')
  const [tableBottom, setTableBottom] = useState('')
  const [tableRight, setTableRight] = useState('')
  const [targetClassId, setTargetClassId] = useState('')
  const [targetSchemaJson, setTargetSchemaJson] = useState('[]')
  const [mappingsJson, setMappingsJson] = useState('[]')
  const [confirmMain, setConfirmMain] = useState(false)
  const [tableBBoxError, setTableBBoxError] = useState<string | null>(null)

  const classOptions = useMemo(
    () => [{ label: 'Select class', value: '' }, ...registry.classes.map((item) => ({
      label: item.label ? `${item.label} (${item.id})` : item.id,
      value: item.id,
    }))],
    [registry.classes],
  )

  const parseBBoxNumber = (value: string) => {
    const parsed = Number(value)
    return Number.isFinite(parsed) ? parsed : null
  }

  const buildTableBBox = () => {
    const values = [tableTop, tableLeft, tableBottom, tableRight]
    if (values.every((value) => value.trim() === '')) {
      setTableBBoxError(null)
      return undefined
    }
    if (values.some((value) => value.trim() === '')) {
      setTableBBoxError('Provide all table bbox values or leave them blank.')
      return undefined
    }
    const parsed = values.map(parseBBoxNumber)
    if (parsed.some((value) => value === null)) {
      setTableBBoxError('Table bbox must be numeric values.')
      return undefined
    }
    setTableBBoxError(null)
    return {
      top: parsed[0] ?? 0,
      left: parsed[1] ?? 0,
      bottom: parsed[2] ?? 0,
      right: parsed[3] ?? 0,
    }
  }

  const previewMutation = useMutation({
    mutationFn: () =>
      withRateLimitRetry(() =>
        previewGoogleSheet(requestContext, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          api_key: apiKey || undefined,
          connection_id: connectionId || undefined,
        }),
      ),
    onError: (error) => toastApiError(error, language),
  })

  const loadSchemaMutation = useMutation({
    mutationFn: async () => {
      if (!targetClassId) {
        throw new Error('Select target class first')
      }
      const ontology = await getOntology(requestContext, dbName, targetClassId, branch)
      const props = (ontology as { properties?: Array<{ name?: string; type?: string }> }).properties ?? []
      const schema = props
        .filter((prop) => prop.name)
        .map((prop) => ({ name: prop.name as string, type: prop.type ?? 'xsd:string' }))
      return schema
    },
    onSuccess: (schema) => setTargetSchemaJson(JSON.stringify(schema, null, 2)),
    onError: (error) => toastApiError(error, language),
  })

  const suggestMutation = useMutation({
    mutationFn: async () => {
      const targetSchema = parseJsonArray<ImportTargetField>(targetSchemaJson)
      return withRateLimitRetry(() =>
        suggestMappingsFromGoogleSheets(requestContext, dbName, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          api_key: apiKey || undefined,
          connection_id: connectionId || undefined,
          table_id: tableId || undefined,
          table_bbox: buildTableBBox(),
          target_class_id: targetClassId,
          target_schema: targetSchema,
        }),
      )
    },
    onSuccess: (result) => {
      const mappings = (result as { mappings?: MappingItem[] }).mappings ?? []
      setMappingsJson(JSON.stringify(mappings, null, 2))
    },
    onError: (error) => toastApiError(error, language),
  })

  const dryRunMutation = useMutation({
    mutationFn: async () => {
      const targetSchema = parseJsonArray<ImportTargetField>(targetSchemaJson)
      const mappings = parseJsonArray<MappingItem>(mappingsJson)
      return withRateLimitRetry(() =>
        dryRunImportFromGoogleSheets(requestContext, dbName, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          api_key: apiKey || undefined,
          connection_id: connectionId || undefined,
          table_id: tableId || undefined,
          table_bbox: buildTableBBox(),
          target_class_id: targetClassId,
          target_schema: targetSchema,
          mappings,
        }),
      )
    },
    onError: (error) => toastApiError(error, language),
  })

  const commitMutation = useMutation({
    mutationFn: async () => {
      const targetSchema = parseJsonArray<ImportTargetField>(targetSchemaJson)
      const mappings = parseJsonArray<MappingItem>(mappingsJson)
      return withRateLimitRetry(() =>
        commitImportFromGoogleSheets(requestContext, dbName, {
          sheet_url: sheetUrl,
          worksheet_name: worksheetName || undefined,
          api_key: apiKey || undefined,
          connection_id: connectionId || undefined,
          table_id: tableId || undefined,
          table_bbox: buildTableBBox(),
          target_class_id: targetClassId,
          target_schema: targetSchema,
          mappings,
          allow_partial: true,
          batch_size: 200,
        }),
      )
    },
    onSuccess: (payload) => {
      const commands = (payload as { write?: { commands?: Array<Record<string, unknown>> } })?.write?.commands ?? []
      commands.forEach((entry) => {
        const command = entry.command as Record<string, unknown> | undefined
        const commandId = extractCommandId(command ?? entry)
        if (commandId) {
          registerCommand({
            commandId,
            kind: 'IMPORT_SHEETS',
            targetClassId,
            title: `Import Sheets: ${targetClassId}`,
          })
        }
      })
      void showAppToast({ intent: Intent.SUCCESS, message: 'Import commit submitted.' })
    },
    onError: (error) => toastApiError(error, language),
  })

  const isMainBranch = branch === 'main'
  const canCommit = isMainBranch || confirmMain

  const stepPanels = useMemo(
    () => [
      {
        title: 'Prepare',
        content: (
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
              <InputGroup value={tableId} onChange={(event) => setTableId(event.currentTarget.value)} />
            </FormGroup>
            <FormGroup label="Table bbox (optional)">
              <div className="form-row">
                <InputGroup placeholder="top" value={tableTop} onChange={(event) => setTableTop(event.currentTarget.value)} />
                <InputGroup placeholder="left" value={tableLeft} onChange={(event) => setTableLeft(event.currentTarget.value)} />
                <InputGroup placeholder="bottom" value={tableBottom} onChange={(event) => setTableBottom(event.currentTarget.value)} />
                <InputGroup placeholder="right" value={tableRight} onChange={(event) => setTableRight(event.currentTarget.value)} />
              </div>
            </FormGroup>
            <FormGroup label="Target class id">
              <HTMLSelect
                options={classOptions}
                value={targetClassId}
                onChange={(event) => setTargetClassId(event.currentTarget.value)}
              />
            </FormGroup>
            <div className="form-row">
              <Button onClick={() => previewMutation.mutate()} disabled={!sheetUrl || importCooldown > 0} loading={previewMutation.isPending}>
                {importCooldown > 0 ? `Retry in ${importCooldown}s` : 'Preview'}
              </Button>
              <Button onClick={() => loadSchemaMutation.mutate()} disabled={!targetClassId} loading={loadSchemaMutation.isPending}>
                Load target schema
              </Button>
            </div>
            {tableBBoxError ? <Callout intent={Intent.WARNING}>{tableBBoxError}</Callout> : null}
            <JsonViewer value={previewMutation.data} empty="Preview sample rows." />
            <FormGroup label="Target schema (JSON array of {name,type})">
              <TextArea value={targetSchemaJson} onChange={(event) => setTargetSchemaJson(event.currentTarget.value)} rows={8} />
            </FormGroup>
          </Card>
        ),
      },
      {
        title: 'Suggest',
        content: (
          <Card className="card-stack">
            <div className="form-row">
              <Button
                intent={Intent.PRIMARY}
                onClick={() => suggestMutation.mutate()}
                disabled={!sheetUrl || !targetClassId || importCooldown > 0 || Boolean(tableBBoxError)}
                loading={suggestMutation.isPending}
              >
                {importCooldown > 0 ? `Retry in ${importCooldown}s` : 'Suggest mappings'}
              </Button>
            </div>
            <JsonViewer value={suggestMutation.data} empty="Run mapping suggestion." />
            <FormGroup label="Mappings (JSON array)">
              <TextArea value={mappingsJson} onChange={(event) => setMappingsJson(event.currentTarget.value)} rows={8} />
            </FormGroup>
          </Card>
        ),
      },
      {
        title: 'Dry-run',
        content: (
          <Card className="card-stack">
            <div className="form-row">
              <Button
                intent={Intent.PRIMARY}
                onClick={() => dryRunMutation.mutate()}
                disabled={!sheetUrl || !targetClassId || importCooldown > 0 || Boolean(tableBBoxError)}
                loading={dryRunMutation.isPending}
              >
                {importCooldown > 0 ? `Retry in ${importCooldown}s` : 'Run dry-run'}
              </Button>
            </div>
            <JsonViewer value={dryRunMutation.data} empty="Dry-run results will appear here." />
          </Card>
        ),
      },
      {
        title: 'Commit',
        content: (
          <Card className="card-stack">
            {!isMainBranch ? (
              <Callout intent={Intent.WARNING}>
                Commit always writes to main. Confirm to continue.
                <label style={{ display: 'block', marginTop: 8 }}>
                  <input
                    type="checkbox"
                    checked={confirmMain}
                    onChange={(event) => setConfirmMain(event.currentTarget.checked)}
                  />{' '}
                  I understand this will write to main.
                </label>
              </Callout>
            ) : null}
            <Button
              intent={Intent.PRIMARY}
              onClick={() => commitMutation.mutate()}
              disabled={!sheetUrl || !targetClassId || !canCommit || importCooldown > 0 || Boolean(tableBBoxError)}
              loading={commitMutation.isPending}
            >
              {importCooldown > 0 ? `Retry in ${importCooldown}s` : 'Commit import'}
            </Button>
            <JsonViewer value={commitMutation.data} empty="Commit response will appear here." />
          </Card>
        ),
      },
    ],
    [
      sheetUrl,
      worksheetName,
      apiKey,
      tableId,
      tableTop,
      tableLeft,
      tableBottom,
      tableRight,
      targetClassId,
      classOptions,
      targetSchemaJson,
      mappingsJson,
      previewMutation,
      loadSchemaMutation,
      suggestMutation,
      dryRunMutation,
      commitMutation,
      importCooldown,
      tableBBoxError,
      confirmMain,
      canCommit,
      isMainBranch,
    ],
  )

  const stepLabels = useMemo(() => stepPanels.map((panel) => panel.title), [stepPanels])

  return (
    <div>
      <PageHeader title="Import (Google Sheets)" subtitle={`Branch context: ${branch} (commit writes to main)`} />
      <StepperBar steps={stepLabels} activeStep={step} onStepChange={(next) => setStep(next)} />
      <div style={{ marginTop: 16 }}>{stepPanels[step]?.content}</div>
      <div className="form-row" style={{ marginTop: 16 }}>
        <Button onClick={() => setStep(Math.max(0, step - 1))} disabled={step === 0}>
          Back
        </Button>
        <Button onClick={() => setStep(Math.min(stepPanels.length - 1, step + 1))} disabled={step === stepPanels.length - 1}>
          Next
        </Button>
      </div>
    </div>
  )
}
