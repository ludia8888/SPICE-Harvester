import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
    Button,
    Card,
    Divider,
    FormGroup,
    H5,
    HTMLTable,
    InputGroup,
    Intent,
    Tab,
    Tabs,
    Text,
} from '@blueprintjs/core'
import {
    deleteGoogleSheetsConnection,
    gridGoogleSheet,
    listRegisteredSheets,
    listGoogleSheetsConnections,
    listGoogleSheetsSpreadsheets,
    listGoogleSheetsWorksheets,
    previewGoogleSheet,
    previewRegisteredSheet,
    registerGoogleSheet,
    startGoogleSheetsOAuth,
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
type GoogleSheetsConnection = {
    connection_id?: string
    label?: string
}
type GoogleSpreadsheet = {
    spreadsheet_id?: string
    name?: string
}
type GoogleWorksheet = {
    worksheet_id?: string | number
    title?: string
}

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
    const [connectionLabel, setConnectionLabel] = useState('')
    const [selectedConnection, setSelectedConnection] = useState('')
    const [sheetQuery, setSheetQuery] = useState('')
    const [selectedSpreadsheet, setSelectedSpreadsheet] = useState('')
    const [selectedWorksheet, setSelectedWorksheet] = useState('')

    useEffect(() => {
        const url = new URL(window.location.href)
        const connectionId = url.searchParams.get('connection_id')
        if (!connectionId) return
        url.searchParams.delete('connection_id')
        window.history.replaceState({}, '', url.toString())
        setTimeout(() => setSelectedConnection(connectionId), 0)
    }, [])

    const previewMutation = useMutation({
        mutationFn: () =>
            withRateLimitRetry(() =>
                previewGoogleSheet(requestContext, {
                    sheet_url: sheetUrl,
                    worksheet_name: worksheetName || undefined,
                    api_key: apiKey || undefined,
                    connection_id: selectedConnection || undefined,
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
                    connection_id: selectedConnection || undefined,
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
                    connection_id: selectedConnection || undefined,
                }),
            ),
        onSuccess: () => {
            void showAppToast({ intent: Intent.SUCCESS, message: 'Sheet registered.' })
            void queryClient.invalidateQueries({ queryKey: qk.registeredSheets(dbName, requestContext.language) })
        },
        onError: (error) => toastApiError(error, language),
    })

    const oauthMutation = useMutation({
        mutationFn: () =>
            withRateLimitRetry(() =>
                startGoogleSheetsOAuth(requestContext, {
                    redirect_uri: window.location.href.split('?')[0],
                    label: connectionLabel || undefined,
                    db_name: dbName,
                    branch,
                }),
            ),
        onSuccess: (payload) => {
            const url = (payload as { data?: { authorization_url?: string } })?.data?.authorization_url
            if (url) {
                window.location.href = url
            }
        },
        onError: (error) => toastApiError(error, language),
    })

    const connectionsQuery = useQuery({
        queryKey: qk.googleSheetsConnections(requestContext.language),
        queryFn: () => listGoogleSheetsConnections(requestContext),
    })

    const connections = useMemo(() => {
        const payload = connectionsQuery.data as { data?: { connections?: GoogleSheetsConnection[] } } | undefined
        return payload?.data?.connections ?? []
    }, [connectionsQuery.data])

    const spreadsheetsQuery = useQuery({
        queryKey: qk.googleSheetsSpreadsheets(selectedConnection, requestContext.language, sheetQuery),
        queryFn: () => listGoogleSheetsSpreadsheets(requestContext, selectedConnection, sheetQuery || undefined, 50),
        enabled: Boolean(selectedConnection),
    })

    const spreadsheets = useMemo(() => {
        const payload = spreadsheetsQuery.data as { data?: { spreadsheets?: GoogleSpreadsheet[] } } | undefined
        return payload?.data?.spreadsheets ?? []
    }, [spreadsheetsQuery.data])

    const worksheetsQuery = useQuery({
        queryKey: qk.googleSheetsWorksheets(selectedConnection, selectedSpreadsheet, requestContext.language),
        queryFn: () => listGoogleSheetsWorksheets(requestContext, selectedConnection, selectedSpreadsheet),
        enabled: Boolean(selectedConnection && selectedSpreadsheet),
    })

    const worksheets = useMemo(() => {
        const payload = worksheetsQuery.data as { data?: { worksheets?: GoogleWorksheet[] } } | undefined
        return payload?.data?.worksheets ?? []
    }, [worksheetsQuery.data])

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

    const deleteConnectionMutation = useMutation({
        mutationFn: (connectionId: string) =>
            withRateLimitRetry(() => deleteGoogleSheetsConnection(requestContext, connectionId)),
        onSuccess: () => {
            void showAppToast({ intent: Intent.SUCCESS, message: 'Connection removed.' })
            void queryClient.invalidateQueries({ queryKey: qk.googleSheetsConnections(requestContext.language) })
        },
        onError: (error) => toastApiError(error, language),
    })

    const copy = language === 'ko'
        ? {
            title: 'Google Sheets',
            subtitle: '연결을 관리하고 시트를 미리보기/등록합니다.',
            connections: {
                title: '연결',
                description: '한 번 연결하면 파이프라인에서 반복 사용합니다.',
                label: '연결 라벨',
                connect: 'Google 계정 연결',
                select: '연결 선택',
                remove: '연결 제거',
            },
            selection: {
                title: '스프레드시트 선택',
                search: '검색',
                searchPlaceholder: '시트 검색',
                spreadsheet: '스프레드시트',
                worksheet: '워크시트',
                sheetUrl: 'Sheet URL',
                worksheetOptional: '워크시트 (선택)',
                apiKey: 'API 키 (선택)',
                helper: '연결된 계정으로 스프레드시트를 탐색하거나 URL을 직접 붙여넣을 수 있습니다.',
            },
            tabs: {
                preview: '미리보기',
                grid: '그리드',
                registered: '등록됨',
            },
            actions: {
                preview: '미리보기',
                grid: '그리드 감지',
                register: '등록',
                refresh: '목록 새로고침',
                unregister: '등록 해제',
            },
            empty: {
                preview: '미리보기를 실행하면 샘플이 표시됩니다.',
                grid: '그리드 감지 결과를 확인하세요.',
                registered: '등록된 시트가 없습니다.',
                registeredPreview: '시트를 선택하면 미리보기가 표시됩니다.',
            },
            table: {
                sheetId: 'Sheet ID',
                worksheet: '워크시트',
                db: 'DB',
                branch: '브랜치',
                actions: '작업',
            },
        }
        : {
            title: 'Google Sheets',
            subtitle: 'Preview, detect grids, and manage registered sheets.',
            connections: {
                title: 'Connections',
                description: 'Connect once, then reuse the connection for every pipeline step.',
                label: 'Connection label',
                connect: 'Connect Google account',
                select: 'Select connection',
                remove: 'Remove connection',
            },
            selection: {
                title: 'Select spreadsheet',
                search: 'Search',
                searchPlaceholder: 'Search sheets',
                spreadsheet: 'Spreadsheet',
                worksheet: 'Worksheet',
                sheetUrl: 'Sheet URL',
                worksheetOptional: 'Worksheet (optional)',
                apiKey: 'API key (optional)',
                helper: 'Spreadsheet selection uses your connected Google account. You can also paste a sheet URL directly.',
            },
            tabs: {
                preview: 'Preview',
                grid: 'Grid',
                registered: 'Registered',
            },
            actions: {
                preview: 'Preview',
                grid: 'Detect grid',
                register: 'Register',
                refresh: 'Refresh list',
                unregister: 'Unregister',
            },
            empty: {
                preview: 'Run preview to see sample rows.',
                grid: 'Run grid detection to inspect merges.',
                registered: 'No registered sheets.',
                registeredPreview: 'Select a sheet to preview.',
            },
            table: {
                sheetId: 'Sheet ID',
                worksheet: 'Worksheet',
                db: 'DB',
                branch: 'Branch',
                actions: 'Actions',
            },
        }

    return (
        <div>
            <PageHeader title={copy.title} subtitle={copy.subtitle} />

            <Card style={{ marginBottom: 16 }}>
                <H5 style={{ marginTop: 0 }}>{copy.connections.title}</H5>
                <Text className="muted" style={{ marginBottom: 8 }}>
                    {copy.connections.description}
                </Text>
                <div className="form-row" style={{ alignItems: 'flex-end' }}>
                    <FormGroup label={copy.connections.label}>
                        <InputGroup value={connectionLabel} onChange={(event) => setConnectionLabel(event.currentTarget.value)} />
                    </FormGroup>
                    <Button
                        intent={Intent.PRIMARY}
                        onClick={() => oauthMutation.mutate()}
                        loading={oauthMutation.isPending}
                    >
                        {copy.connections.connect}
                    </Button>
                </div>
                <Divider style={{ margin: '12px 0' }} />
                <div className="form-row">
                    <FormGroup label={copy.connections.title}>
                        <div className="bp5-select">
                            <select
                                value={selectedConnection}
                                onChange={(event) => {
                                    setSelectedConnection(event.currentTarget.value)
                                    setSelectedSpreadsheet('')
                                    setSelectedWorksheet('')
                                }}
                            >
                                <option value="">{copy.connections.select}</option>
                                {connections.map((connection) => (
                                    <option key={String(connection.connection_id ?? '')} value={String(connection.connection_id ?? '')}>
                                        {String(connection.label ?? 'Google Sheets')}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </FormGroup>
                    <Button
                        intent={Intent.DANGER}
                        disabled={!selectedConnection}
                        onClick={() => deleteConnectionMutation.mutate(selectedConnection)}
                    >
                        {copy.connections.remove}
                    </Button>
                </div>
            </Card>

            <Card style={{ marginBottom: 16 }}>
                <H5 style={{ marginTop: 0 }}>{copy.selection.title}</H5>
                <div className="form-row">
                    <FormGroup label={copy.selection.search}>
                        <InputGroup
                            value={sheetQuery}
                            onChange={(event) => setSheetQuery(event.currentTarget.value)}
                            placeholder={copy.selection.searchPlaceholder}
                        />
                    </FormGroup>
                    <FormGroup label={copy.selection.spreadsheet}>
                        <div className="bp5-select">
                            <select
                                value={selectedSpreadsheet}
                                onChange={(event) => {
                                    setSelectedSpreadsheet(event.currentTarget.value)
                                    setSelectedWorksheet('')
                                    const match = spreadsheets.find(
                                        (item) => String(item.spreadsheet_id ?? '') === event.currentTarget.value,
                                    )
                                    if (match?.spreadsheet_id) {
                                        setSheetUrl(
                                            `https://docs.google.com/spreadsheets/d/${match.spreadsheet_id}/edit`,
                                        )
                                    }
                                }}
                                disabled={!selectedConnection}
                            >
                                <option value="">{copy.selection.spreadsheet}</option>
                                {spreadsheets.map((sheet) => (
                                    <option key={String(sheet.spreadsheet_id ?? '')} value={String(sheet.spreadsheet_id ?? '')}>
                                        {String(sheet.name ?? 'Untitled')}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </FormGroup>
                    <FormGroup label={copy.selection.worksheet}>
                        <div className="bp5-select">
                            <select
                                value={selectedWorksheet}
                                onChange={(event) => {
                                    setSelectedWorksheet(event.currentTarget.value)
                                    setWorksheetName(event.currentTarget.value)
                                }}
                                disabled={!selectedSpreadsheet}
                            >
                                <option value="">{copy.selection.worksheet}</option>
                                {worksheets.map((sheet) => (
                                    <option key={String(sheet.worksheet_id ?? '')} value={String(sheet.title ?? '')}>
                                        {String(sheet.title ?? '')}
                                    </option>
                                ))}
                            </select>
                        </div>
                    </FormGroup>
                </div>
                <div className="form-row">
                    <FormGroup label={copy.selection.sheetUrl}>
                        <InputGroup value={sheetUrl} onChange={(event) => setSheetUrl(event.currentTarget.value)} />
                    </FormGroup>
                    <FormGroup label={copy.selection.worksheetOptional}>
                        <InputGroup value={worksheetName} onChange={(event) => setWorksheetName(event.currentTarget.value)} />
                    </FormGroup>
                    <FormGroup label={copy.selection.apiKey}>
                        <InputGroup value={apiKey} onChange={(event) => setApiKey(event.currentTarget.value)} />
                    </FormGroup>
                </div>
                <Text className="muted" style={{ marginTop: 6 }}>
                    {copy.selection.helper}
                </Text>
            </Card>

            <Tabs id="sheets-tabs" defaultSelectedTabId="preview">
                <Tab
                    id="preview"
                    title={copy.tabs.preview}
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => previewMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={previewMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : copy.actions.preview}
                                </Button>
                            </div>
                            <JsonViewer value={previewMutation.data} empty={copy.empty.preview} />
                        </Card>
                    }
                />
                <Tab
                    id="grid"
                    title={copy.tabs.grid}
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => gridMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={gridMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : copy.actions.grid}
                                </Button>
                            </div>
                            <JsonViewer value={gridMutation.data} empty={copy.empty.grid} />
                        </Card>
                    }
                />
                <Tab
                    id="registered"
                    title={copy.tabs.registered}
                    panel={
                        <Card className="card-stack">
                            <div className="form-row">
                                <Button intent={Intent.PRIMARY} onClick={() => registerMutation.mutate()} disabled={!sheetUrl || connectorCooldown > 0} loading={registerMutation.isPending}>
                                    {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : copy.actions.register}
                                </Button>
                                <Button onClick={() => void queryClient.invalidateQueries({ queryKey: qk.registeredSheets(dbName, requestContext.language) })}>
                                    {copy.actions.refresh}
                                </Button>
                            </div>
                            {registeredSheets.length === 0 ? (
                                <Text className="muted">{copy.empty.registered}</Text>
                            ) : (
                                <HTMLTable striped interactive className="command-table">
                                    <thead>
                                        <tr>
                                            <th>{copy.table.sheetId}</th>
                                            <th>{copy.table.worksheet}</th>
                                            <th>{copy.table.db}</th>
                                            <th>{copy.table.branch}</th>
                                            <th>{copy.table.actions}</th>
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
                                                            {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : copy.actions.preview}
                                                        </Button>
                                                        <Button small intent={Intent.DANGER} style={{ marginLeft: 8 }} onClick={() => unregisterMutation.mutate(sheetId)} disabled={connectorCooldown > 0}>
                                                            {connectorCooldown > 0 ? `Retry in ${connectorCooldown}s` : copy.actions.unregister}
                                                        </Button>
                                                    </td>
                                                </tr>
                                            )
                                        })}
                                    </tbody>
                                </HTMLTable>
                            )}
                            <Divider />
                            <JsonViewer value={previewRegisteredMutation.data} empty={copy.empty.registeredPreview} />
                        </Card>
                    }
                />
            </Tabs>
        </div>
    )
}
