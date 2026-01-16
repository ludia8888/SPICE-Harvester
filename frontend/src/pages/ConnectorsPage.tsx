import { useEffect, useMemo, useState } from 'react'
import { Button, Card, Dialog, FormGroup, H3, HTMLSelect, InputGroup, Spinner, Switch, Text } from '@blueprintjs/core'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { useAppStore } from '../state/store'
import {
  listDatabases,
  listRegisteredGoogleSheets,
  previewRegisteredGoogleSheet,
  registerGoogleSheet,
  startPipeliningGoogleSheet,
  unregisterGoogleSheet,
  type DatabaseRecord,
  type GoogleSheetPreview,
  type GoogleSheetRegisteredSheet,
} from '../api/bff'

type FolderOption = { id: string; name: string }

const normalizeFolder = (record: DatabaseRecord | string): FolderOption | null => {
  if (typeof record === 'string') {
    return { id: record, name: record }
  }
  const id = record.name || record.db_name || record.id
  if (!id) {
    return null
  }
  return {
    id,
    name: record.display_name || record.label || id,
  }
}

export const ConnectorsPage = () => {
  const queryClient = useQueryClient()
  const setPipelineContext = useAppStore((state) => state.setPipelineContext)
  const setActiveNav = useAppStore((state) => state.setActiveNav)

  const { data: databasesRaw = [], isLoading: isLoadingDatabases } = useQuery({
    queryKey: ['databases'],
    queryFn: listDatabases,
  })

  const databases = useMemo(
    () => databasesRaw.map(normalizeFolder).filter((item): item is FolderOption => Boolean(item)),
    [databasesRaw],
  )

  const [activeDbName, setActiveDbName] = useState<string>('')
  const [sheetUrl, setSheetUrl] = useState('')
  const [worksheetName, setWorksheetName] = useState('')
  const [pollingInterval, setPollingInterval] = useState('300')
  const [branch, setBranch] = useState('main')
  const [classLabel, setClassLabel] = useState('')
  const [autoImport, setAutoImport] = useState(false)
  const [isRegistering, setRegistering] = useState(false)
  const [registerError, setRegisterError] = useState<string | null>(null)
  const [registerResult, setRegisterResult] = useState<Record<string, unknown> | null>(null)

  const [previewTarget, setPreviewTarget] = useState<GoogleSheetRegisteredSheet | null>(null)
  const [previewData, setPreviewData] = useState<GoogleSheetPreview | null>(null)
  const [isPreviewOpen, setPreviewOpen] = useState(false)
  const [previewError, setPreviewError] = useState<string | null>(null)
  const [isPreviewLoading, setPreviewLoading] = useState(false)

  const [busySheetId, setBusySheetId] = useState<string | null>(null)
  const [actionError, setActionError] = useState<string | null>(null)

  useEffect(() => {
    if (!activeDbName && databases.length > 0) {
      setActiveDbName(databases[0].id)
    }
  }, [activeDbName, databases])

  const { data: registered = null, isLoading: isLoadingSheets } = useQuery({
    queryKey: ['connectors', 'google-sheets', 'registered', activeDbName],
    queryFn: () => listRegisteredGoogleSheets({ databaseName: activeDbName || undefined }),
    enabled: databases.length > 0,
  })

  const sheets = registered?.sheets ?? []

  const handleRegister = () => {
    if (!activeDbName) {
      setRegisterError('Select a project first.')
      return
    }
    const url = sheetUrl.trim()
    if (!url) {
      setRegisterError('Sheet URL is required.')
      return
    }

    const interval = Number.parseInt(pollingInterval, 10)
    const resolvedInterval = Number.isFinite(interval) && interval > 0 ? interval : 300

    const run = async () => {
      setRegistering(true)
      setRegisterError(null)
      setRegisterResult(null)
      try {
        const result = await registerGoogleSheet({
          sheet_url: url,
          worksheet_name: worksheetName.trim() || undefined,
          polling_interval: resolvedInterval,
          database_name: activeDbName,
          branch: branch.trim() || 'main',
          class_label: classLabel.trim() || undefined,
          auto_import: autoImport,
        })
        setRegisterResult(result)
        await queryClient.invalidateQueries({ queryKey: ['connectors', 'google-sheets', 'registered'] })
      } catch (error) {
        setRegisterError(error instanceof Error ? error.message : 'Failed to register Google Sheet')
      } finally {
        setRegistering(false)
      }
    }

    void run()
  }

  const openPreview = (sheet: GoogleSheetRegisteredSheet) => {
    const run = async () => {
      setPreviewTarget(sheet)
      setPreviewData(null)
      setPreviewError(null)
      setPreviewLoading(true)
      setPreviewOpen(true)
      try {
        const data = await previewRegisteredGoogleSheet(sheet.sheet_id, {
          worksheetName: sheet.worksheet_name || undefined,
          limit: 10,
        })
        setPreviewData(data)
      } catch (error) {
        setPreviewError(error instanceof Error ? error.message : 'Failed to load preview')
      } finally {
        setPreviewLoading(false)
      }
    }
    void run()
  }

  const handleStartPipelining = (sheet: GoogleSheetRegisteredSheet) => {
    if (!activeDbName) {
      setActionError('Select a project first.')
      return
    }
    const run = async () => {
      setBusySheetId(sheet.sheet_id)
      setActionError(null)
      try {
        await startPipeliningGoogleSheet(sheet.sheet_id, {
          db_name: activeDbName,
          worksheet_name: sheet.worksheet_name || undefined,
        })
        const folderName = databases.find((db) => db.id === activeDbName)?.name ?? activeDbName
        setPipelineContext({ folderId: activeDbName, folderName })
        setActiveNav('pipeline')
      } catch (error) {
        setActionError(error instanceof Error ? error.message : 'Failed to start pipelining')
      } finally {
        setBusySheetId(null)
      }
    }
    void run()
  }

  const handleUnregister = (sheet: GoogleSheetRegisteredSheet) => {
    const run = async () => {
      setBusySheetId(sheet.sheet_id)
      setActionError(null)
      try {
        await unregisterGoogleSheet(sheet.sheet_id)
        await queryClient.invalidateQueries({ queryKey: ['connectors', 'google-sheets', 'registered'] })
      } catch (error) {
        setActionError(error instanceof Error ? error.message : 'Failed to unregister sheet')
      } finally {
        setBusySheetId(null)
      }
    }
    void run()
  }

  const isBusy = isRegistering || Boolean(busySheetId)

  return (
    <div className="page">
      <H3>Connectors</H3>

      <Card className="card">
        <div className="card-title">Project scope</div>
        {isLoadingDatabases ? (
          <Spinner size={18} />
        ) : (
          <FormGroup label="Project">
            <HTMLSelect
              fill
              value={activeDbName}
              onChange={(event) => setActiveDbName(event.currentTarget.value)}
              options={databases.map((db) => ({ value: db.id, label: db.name }))}
            />
          </FormGroup>
        )}
        <Text className="card-meta">Google Sheets connector is available now. More connectors can be added via adapters.</Text>
      </Card>

      <Card className="card">
        <div className="card-title">Register Google Sheet</div>
        <FormGroup label="Sheet URL" labelFor="sheet-url">
          <InputGroup
            id="sheet-url"
            placeholder="https://docs.google.com/spreadsheets/d/..."
            value={sheetUrl}
            onChange={(event) => setSheetUrl(event.currentTarget.value)}
            disabled={isRegistering}
          />
        </FormGroup>
        <FormGroup label="Worksheet (optional)" labelFor="worksheet-name">
          <InputGroup
            id="worksheet-name"
            placeholder="Sheet1"
            value={worksheetName}
            onChange={(event) => setWorksheetName(event.currentTarget.value)}
            disabled={isRegistering}
          />
        </FormGroup>
        <div className="grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
          <FormGroup label="Polling interval (seconds)" labelFor="polling-interval">
            <InputGroup
              id="polling-interval"
              value={pollingInterval}
              onChange={(event) => setPollingInterval(event.currentTarget.value)}
              disabled={isRegistering}
            />
          </FormGroup>
          <FormGroup label="Branch" labelFor="branch">
            <InputGroup
              id="branch"
              value={branch}
              onChange={(event) => setBranch(event.currentTarget.value)}
              disabled={isRegistering}
            />
          </FormGroup>
          <FormGroup label="Target class label (optional)" labelFor="class-label">
            <InputGroup
              id="class-label"
              value={classLabel}
              onChange={(event) => setClassLabel(event.currentTarget.value)}
              disabled={isRegistering}
            />
          </FormGroup>
        </div>

        <Switch
          checked={autoImport}
          label="Auto-import into ontology (requires class label)"
          onChange={() => setAutoImport((value) => !value)}
          disabled={isRegistering}
        />

        {registerError ? <Text className="upload-error">{registerError}</Text> : null}
        {registerResult ? (
          <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{JSON.stringify(registerResult, null, 2)}</pre>
        ) : null}
        <div style={{ display: 'flex', gap: 8, marginTop: 12 }}>
          <Button intent="primary" text="Register" onClick={handleRegister} loading={isRegistering} disabled={isRegistering} />
        </div>
      </Card>

      <Card className="card">
        <div className="card-title">Registered Sheets</div>
        {isLoadingSheets ? (
          <Spinner size={18} />
        ) : sheets.length === 0 ? (
          <Text>No registered sheets yet.</Text>
        ) : (
          <div style={{ display: 'flex', flexDirection: 'column', gap: 12 }}>
            {sheets.map((sheet) => {
              const isSheetBusy = busySheetId === sheet.sheet_id
              return (
                <Card key={sheet.sheet_id} className="card">
                  <div style={{ display: 'flex', justifyContent: 'space-between', gap: 12, alignItems: 'center' }}>
                    <div style={{ display: 'flex', flexDirection: 'column', gap: 4 }}>
                      <Text className="card-title">{sheet.sheet_title || sheet.sheet_id}</Text>
                      <Text className="card-meta">{sheet.sheet_url}</Text>
                      <Text className="card-meta">
                        worksheet={sheet.worksheet_name || '-'} · interval={sheet.polling_interval}s · branch={sheet.branch || 'main'}
                      </Text>
                    </div>
                    <div style={{ display: 'flex', gap: 8, flexWrap: 'wrap', justifyContent: 'flex-end' }}>
                      <Button small text="Preview" onClick={() => openPreview(sheet)} disabled={isBusy} />
                      <Button
                        small
                        intent="success"
                        text="Start pipelining"
                        onClick={() => handleStartPipelining(sheet)}
                        loading={isSheetBusy && busySheetId === sheet.sheet_id}
                        disabled={isBusy}
                      />
                      <Button
                        small
                        intent="danger"
                        text="Unregister"
                        onClick={() => handleUnregister(sheet)}
                        loading={isSheetBusy && busySheetId === sheet.sheet_id}
                        disabled={isBusy}
                      />
                    </div>
                  </div>
                </Card>
              )
            })}
          </div>
        )}
        {actionError ? <Text className="upload-error">{actionError}</Text> : null}
      </Card>

      <Dialog
        isOpen={isPreviewOpen}
        onClose={() => setPreviewOpen(false)}
        title={previewTarget ? `Preview: ${previewTarget.sheet_title || previewTarget.sheet_id}` : 'Preview'}
        icon="th"
        className="bp5-dark"
      >
        <div style={{ padding: 20 }}>
          {isPreviewLoading ? (
            <Spinner />
          ) : previewError ? (
            <Text className="upload-error">{previewError}</Text>
          ) : previewData ? (
            <pre style={{ margin: 0, whiteSpace: 'pre-wrap' }}>{JSON.stringify(previewData, null, 2)}</pre>
          ) : (
            <Text>Preview is unavailable.</Text>
          )}
        </div>
      </Dialog>
    </div>
  )
}

