import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  Callout,
  Checkbox,
  FileInput,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  TextArea,
} from '@blueprintjs/core'
import {
  extractWriteCommandIds,
  importFromExcelCommit,
  importFromExcelDryRun,
  suggestMappingsFromExcel,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry } from '../hooks/useOntologyRegistry'
import { useCooldown } from '../hooks/useCooldown'
import { HttpError } from '../api/bff'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'
import { asArray, asRecord, getString, type UnknownRecord } from '../utils/typed'

export const ImportExcelPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const trackCommands = useAppStore((state) => state.trackCommands)

  const registry = useOntologyRegistry(db, context.branch)

  const [file, setFile] = useState<File | null>(null)
  const [sheetName, setSheetName] = useState('')
  const [targetClassId, setTargetClassId] = useState('')
  const [targetSchemaJson, setTargetSchemaJson] = useState('')
  const [mappingsJson, setMappingsJson] = useState('')
  const [tableId, setTableId] = useState('')
  const [tableTop, setTableTop] = useState<number | null>(null)
  const [tableLeft, setTableLeft] = useState<number | null>(null)
  const [tableBottom, setTableBottom] = useState<number | null>(null)
  const [tableRight, setTableRight] = useState<number | null>(null)
  const [dryRunResult, setDryRunResult] = useState<unknown>(null)
  const [commitResult, setCommitResult] = useState<unknown>(null)
  const [confirmMain, setConfirmMain] = useState(false)

  const dryRunCooldown = useCooldown()
  const commitCooldown = useCooldown()

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const loadSchema = () => {
    const props = registry.propertyLabelsByClass.get(targetClassId) ?? []
    const schema = props.map((prop) => ({ name: prop.id, type: prop.type ?? 'xsd:string' }))
    setTargetSchemaJson(JSON.stringify(schema, null, 2))
  }

  const suggestMutation = useMutation({
    mutationFn: async () => {
      if (!db || !file || !targetClassId) {
        throw new Error('Missing inputs')
      }
      return suggestMappingsFromExcel(requestContext, db, targetClassId, file, targetSchemaJson || undefined)
    },
    onSuccess: (result) => {
      if (!result) {
        return
      }
      const resultRecord = asRecord(result)
      const mappings = asArray<unknown>(resultRecord.mappings ?? asRecord(resultRecord.data).mappings)
      setMappingsJson(JSON.stringify(mappings, null, 2))
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const dryRunMutation = useMutation({
    mutationFn: async () => {
      if (!db || !file || !targetClassId || !mappingsJson) {
        throw new Error('Missing inputs')
      }
      return importFromExcelDryRun(requestContext, db, {
        file,
        target_class_id: targetClassId,
        target_schema_json: targetSchemaJson || undefined,
        mappings_json: mappingsJson,
        sheet_name: sheetName || undefined,
        table_id: tableId || undefined,
        table_top: tableTop ?? undefined,
        table_left: tableLeft ?? undefined,
        table_bottom: tableBottom ?? undefined,
        table_right: tableRight ?? undefined,
      })
    },
    onSuccess: (result) => setDryRunResult(result),
    onError: (error) => {
      if (error instanceof HttpError) {
        dryRunCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const commitMutation = useMutation({
    mutationFn: async () => {
      if (!db || !file || !targetClassId || !mappingsJson) {
        throw new Error('Missing inputs')
      }
      return importFromExcelCommit(requestContext, db, {
        file,
        target_class_id: targetClassId,
        target_schema_json: targetSchemaJson || undefined,
        mappings_json: mappingsJson,
        sheet_name: sheetName || undefined,
        table_id: tableId || undefined,
        table_top: tableTop ?? undefined,
        table_left: tableLeft ?? undefined,
        table_bottom: tableBottom ?? undefined,
        table_right: tableRight ?? undefined,
      })
    },
    onSuccess: (result) => {
      setCommitResult(result)
      const ids = extractWriteCommandIds(result)
      if (ids.length) {
        trackCommands(ids.map((id) => ({ id, kind: 'IMPORT', title: 'Import from Excel', targetDbName: db ?? '' })))
      }
    },
    onError: (error) => {
      if (error instanceof HttpError) {
        commitCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const commitCommands = useMemo(() => {
    const resultRecord = asRecord(commitResult)
    const write = asRecord(resultRecord.write ?? asRecord(asRecord(resultRecord.data).write))
    return asArray<UnknownRecord>(write.commands)
  }, [commitResult])

  return (
    <div>
      <PageHeader title="Import: Excel" subtitle="엑셀 파일 기반 Dry-run/Commit" />

      <Card elevation={1} className="section-card">
        <div className="form-grid">
          <FormGroup label="Excel file">
            <FileInput
              text={file ? file.name : 'Select file'}
              onInputChange={(event) => {
                const target = event.currentTarget as HTMLInputElement
                setFile(target.files?.[0] ?? null)
              }}
            />
          </FormGroup>
          <FormGroup label="Sheet name">
            <InputGroup value={sheetName} onChange={(event) => setSheetName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Target class">
            <HTMLSelect
              value={targetClassId}
              onChange={(event) => setTargetClassId(event.currentTarget.value)}
              options={[{ label: 'Select class', value: '' }, ...registry.classOptions]}
            />
          </FormGroup>
          <FormGroup label="Table ID">
            <InputGroup value={tableId} onChange={(event) => setTableId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Table bbox">
            <div className="form-row">
              <NumericInput placeholder="top" value={tableTop ?? undefined} onValueChange={(value) => setTableTop(Number.isNaN(value) ? null : value)} />
              <NumericInput placeholder="left" value={tableLeft ?? undefined} onValueChange={(value) => setTableLeft(Number.isNaN(value) ? null : value)} />
              <NumericInput placeholder="bottom" value={tableBottom ?? undefined} onValueChange={(value) => setTableBottom(Number.isNaN(value) ? null : value)} />
              <NumericInput placeholder="right" value={tableRight ?? undefined} onValueChange={(value) => setTableRight(Number.isNaN(value) ? null : value)} />
            </div>
          </FormGroup>
        </div>
        <Button icon="manual" onClick={loadSchema} disabled={!targetClassId}>
          Load Target Schema
        </Button>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Mappings</H5>
          <Button onClick={() => suggestMutation.mutate()} disabled={!file || !targetClassId}>
            Suggest from Excel
          </Button>
        </div>
        <TextArea
          rows={10}
          value={mappingsJson}
          onChange={(event) => setMappingsJson(event.currentTarget.value)}
          placeholder='[{"source_field":"A","target_field":"field"}]'
        />
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Target Schema</H5>
        <TextArea
          rows={10}
          value={targetSchemaJson}
          onChange={(event) => setTargetSchemaJson(event.currentTarget.value)}
          placeholder='[{"name":"field","type":"xsd:string"}]'
        />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Dry-run</H5>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => dryRunMutation.mutate()}
            loading={dryRunMutation.isPending}
            disabled={!file || !targetClassId || !mappingsJson || dryRunCooldown.active}
          >
            Dry-run {dryRunCooldown.active ? `(${dryRunCooldown.remainingSeconds}s)` : ''}
          </Button>
        </div>
        <JsonView value={dryRunResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Commit</H5>
          <Button
            intent={Intent.DANGER}
            onClick={() => commitMutation.mutate()}
            loading={commitMutation.isPending}
            disabled={!file || !targetClassId || !mappingsJson || commitCooldown.active || (context.branch !== 'main' && !confirmMain)}
          >
            Commit {commitCooldown.active ? `(${commitCooldown.remainingSeconds}s)` : ''}
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
        <JsonView value={commitResult} />
      </Card>
    </div>
  )
}
