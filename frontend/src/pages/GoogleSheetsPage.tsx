import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
    Button,
    Card,
    Divider,
    FormGroup,
    HTMLTable,
    InputGroup,
    Intent,
    Tab,
    Tabs,
    Text,
} from '@blueprintjs/core'
import {
    gridGoogleSheet,
    listRegisteredSheets,
    previewGoogleSheet,
    previewRegisteredSheet,
    registerGoogleSheet,
    unregisterSheet,
} from '../api/bff'
import { useRateLimitRetry } from '../api/useRateLimitRetry'
import { useRequestContext } from '../api/useRequestContext'
import { JsonViewer } from '../components/JsonViewer'
import { PageHeader } from '../components/layout/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

type RegisteredSheet = Record<string, unknown>

export const GoogleSheetsPage = ({ dbName }: { dbName: string }) => {
    const queryClient = useQueryClient()
    const requestContext = useRequestContext()
    const language = useAppStore((state) => state.context.language)
    const branch = useAppStore((state) => state.context.branch)
    const { cooldown: connectorCooldown, withRateLimitRetry } = useRateLimitRetry(1)

    const [sheetUrl, setSheetUrl] = useState('')
    const [worksheetName, setWorksheetName] = useState('')
    const [apiKey, setApiKey] = useState('')
    const [selectedSheet, setSelectedSheet] = useState<string>('')

    const previewMutation = useMutation({
        mutationFn: () =>
            withRateLimitRetry(() =>
                previewGoogleSheet(requestContext, {
                    sheet_url: sheetUrl,
                    worksheet_name: worksheetName || undefined,
                    api_key: apiKey || undefined,
                }),
            ),
        onError: (error) => toastApiError(error, language),
    })

    const gridMutation = useMutation({
        mutationFn: () =>
            withRateLimitRetry(() =>
                gridGoogleSheet(requestContext, {
                    sheet_url: sheetUrl,
                    worksheet_name: worksheetName || undefined,
                    api_key: apiKey || undefined,
                    max_rows: 60,
                    max_cols: 30,
                    trim_trailing_empty: true,
                }),
            ),
        onError: (error) => toastApiError(error, language),
    })

    const registerMutation = useMutation({
        mutationFn: () =>
            withRateLimitRetry(() =>
                registerGoogleSheet(requestContext, {
                    sheet_url: sheetUrl,
                    worksheet_name: worksheetName || undefined,
                    database_name: dbName,
                    branch,
                    polling_interval: 300,
                    auto_import: false,
                    api_key: apiKey || undefined,
                }),
            ),
        onSuccess: () => {
            void showAppToast({ intent: Intent.SUCCESS, message: 'Sheet registered.' })
            void queryClient.invalidateQueries({ queryKey: qk.registeredSheets(dbName, requestContext.language) })
        },
        onError: (error) => toastApiError(error, language),
    })

    const registeredQuery = useQuery({
        queryKey: qk.registeredSheets(dbName, requestContext.language),
        queryFn: () => listRegisteredSheets(requestContext, dbName),
    })

    const registeredSheets = useMemo(() => {
        const payload = registeredQuery.data as { data?: { sheets?: RegisteredSheet[] } } | undefined
        return payload?.data?.sheets ?? []
    }, [registeredQuery.data])

    const previewRegisteredMutation = useMutation({
        mutationFn: () => withRateLimitRetry(() => previewRegisteredSheet(requestContext, selectedSheet, { limit: 10 })),
        onError: (error) => toastApiError(error, language),
    })

    const unregisterMutation = useMutation({
        mutationFn: (sheetId: string) => withRateLimitRetry(() => unregisterSheet(requestContext, sheetId)),
        onSuccess: () => {
            void showAppToast({ intent: Intent.WARNING, message: 'Sheet unregistered.' })
            void queryClient.invalidateQueries({ queryKey: qk.registeredSheets(dbName, requestContext.language) })
        },
        onError: (error) => toastApiError(error, language),
    })

    return (
        <div>
            <PageHeader title="Google Sheets" subtitle="Preview, detect grids, and manage registered sheets." />

            <Card style={{ marginBottom: 16 }}>
                <div className="form-row">
                    <FormGroup label="Sheet URL">
                        <InputGroup value={sheetUrl} onChange={(event) => setSheetUrl(event.currentTarget.value)} />
                    </FormGroup>
                    <FormGroup label="Worksheet (optional)">
                        <InputGroup value={worksheetName} onChange={(event) => setWorksheetName(event.currentTarget.value)} />
                    </FormGroup>
                    <FormGroup label="API key (optional)">
                        <InputGroup value={apiKey} onChange={(event) => setApiKey(event.currentTarget.value)} />
                    </FormGroup>
                </div>
            </Card>

            <Tabs id="sheets-tabs" defaultSelectedTabId="preview">
                <Tab
                    id="preview"
                    title="Preview"
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => previewMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={previewMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : 'Preview'}
                                </Button>
                            </div>
                            <JsonViewer value={previewMutation.data} empty="Run preview to see sample rows." />
                        </Card>
                    }
                />
                <Tab
                    id="grid"
                    title="Grid"
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => gridMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={gridMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : 'Detect grid'}
                                </Button>
                            </div>
                            <JsonViewer value={gridMutation.data} empty="Run grid detection to inspect merges." />
                        </Card>
                    }
                />
                <Tab
                    id="registered"
                    title="Registered"
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => registerMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={registerMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : 'Register'}
                                </Button>
                                <Button onClick={() => void queryClient.invalidateQueries({ queryKey: qk.registeredSheets(dbName, requestContext.language) })}>
                                    Refresh list
                                </Button>
                            </div>
                            {registeredSheets.length === 0 ? (
                                <Text className="muted">No registered sheets.</Text>
                            ) : (
                                <HTMLTable striped interactive className="command-table">
                                    <thead>
                                        <tr>
                                            <th>Sheet ID</th>
                                            <th>Worksheet</th>
                                            <th>DB</th>
                                            <th>Branch</th>
                                            <th>Actions</th>
                                        </tr>
                                    </thead>
                                    <tbody>
                                        {registeredSheets.map((sheet) => {
                                            const sheetId = String(sheet.sheet_id ?? '')
                                            return (
                                                <tr key={sheetId}>
                                                    <td>{sheetId}</td>
                                                    <td>{String(sheet.worksheet_name ?? '')}</td>
                                                    <td>{String(sheet.database_name ?? '')}</td>
                                                    <td>{String(sheet.branch ?? '')}</td>
                                                    <td>
                                                        <Button small onClick={() => { setSelectedSheet(sheetId); previewRegisteredMutation.mutate(); }} disabled={connectorCooldown > 0}>
                                                            {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : 'Preview'}
                                                        </Button>
                                                        <Button small intent={Intent.DANGER} style={{ marginLeft: 8 }} onClick={() => unregisterMutation.mutate(sheetId)} disabled={connectorCooldown > 0}>
                                                            {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : 'Unregister'}
                                                        </Button>
                                                    </td>
                                                </tr>
                                            )
                                        })}
                                    </tbody>
                                </HTMLTable>
                            )}
                            <Divider />
                            <JsonViewer value={previewRegisteredMutation.data} empty="Select a sheet to preview." />
                        </Card>
                    }
                />
            </Tabs>
        </div>
    )
}
