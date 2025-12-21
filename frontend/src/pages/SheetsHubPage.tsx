import { useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  Tab,
  Tabs,
  Callout,
} from '@blueprintjs/core'
import {
  deleteRegisteredSheet,
  gridSheet,
  listRegisteredSheets,
  previewRegisteredSheet,
  previewSheet,
  registerSheet,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useCooldown } from '../hooks/useCooldown'
import { showAppToast } from '../app/AppToaster'
import { HttpError } from '../api/bff'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

type TableCandidate = { id: string; bbox: UnknownRecord | null }
import { asArray, asRecord, getNumber, getString, type UnknownRecord } from '../utils/typed'

export const SheetsHubPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const [sheetUrl, setSheetUrl] = useState('')
  const [worksheetName, setWorksheetName] = useState('')
  const [apiKey, setApiKey] = useState('')
  const [previewLimit, setPreviewLimit] = useState(10)
  const [gridResult, setGridResult] = useState<unknown>(null)
  const [previewResult, setPreviewResult] = useState<unknown>(null)
  const [registeredPreview, setRegisteredPreview] = useState<unknown>(null)
  const [registerClassLabel, setRegisterClassLabel] = useState('')
  const [registerBranch, setRegisterBranch] = useState('main')

  const formatCellValue = (value: unknown) => {
    if (value === null || value === undefined) {
      return ''
    }
    if (typeof value === 'string' || typeof value === 'number' || typeof value === 'boolean') {
      return String(value)
    }
    try {
      return JSON.stringify(value)
    } catch {
      return String(value)
    }
  }

  const previewCooldown = useCooldown()
  const gridCooldown = useCooldown()
  const registerCooldown = useCooldown()

  const registeredQuery = useQuery({
    queryKey: qk.sheetsRegistered({ dbName: db ?? undefined, language: context.language }),
    queryFn: () => listRegisteredSheets(requestContext, db ?? undefined),
    enabled: Boolean(db),
  })

  const previewMutation = useMutation({
    mutationFn: () =>
      previewSheet(
        requestContext,
        { sheet_url: sheetUrl, worksheet_name: worksheetName || undefined, api_key: apiKey || undefined },
        previewLimit,
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

  const registerMutation = useMutation({
    mutationFn: () =>
      registerSheet(requestContext, {
        sheet_url: sheetUrl,
        worksheet_name: worksheetName || undefined,
        database_name: db,
        branch: registerBranch,
        class_label: registerClassLabel || undefined,
        api_key: apiKey || undefined,
      }),
    onSuccess: () => {
      void registeredQuery.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '등록 완료' })
    },
    onError: (error) => {
      if (error instanceof HttpError) {
        registerCooldown.startCooldown(error.retryAfterSeconds)
      }
      toastApiError(error, context.language)
    },
  })

  const previewRegisteredMutation = useMutation({
    mutationFn: (sheetId: string) => previewRegisteredSheet(requestContext, sheetId, undefined, previewLimit),
    onSuccess: (result) => setRegisteredPreview(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const deleteMutation = useMutation({
    mutationFn: (sheetId: string) => deleteRegisteredSheet(requestContext, sheetId),
    onSuccess: () => {
      void registeredQuery.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '등록 해제 완료' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const registeredSheets = useMemo(
    () =>
      asArray<UnknownRecord>(
        asRecord(asRecord(registeredQuery.data).data).sheets ??
          asRecord(asRecord(asRecord(registeredQuery.data).data).data).sheets,
      ),
    [registeredQuery.data],
  )

  const gridPreview = useMemo(() => {
    const grid = asArray<unknown[]>(asRecord(gridResult).grid)
    if (!grid.length) {
      return []
    }
    return grid.slice(0, 10).map((row) => asArray<unknown>(row).slice(0, 10))
  }, [gridResult])

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
      const bbox = asRecord(record.bbox ?? record.bounding_box ?? record.table_bbox ?? record.tableBBox)
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

  const buildImportUrl = (table?: TableCandidate) => {
    if (!db) {
      return '#'
    }
    const params = new URLSearchParams()
    if (sheetUrl) {
      params.set('sheet_url', sheetUrl)
    }
    if (worksheetName) {
      params.set('worksheet_name', worksheetName)
    }
    if (apiKey) {
      params.set('api_key', apiKey)
    }
    if (table?.id) {
      params.set('table_id', table.id)
    }
    if (table?.bbox) {
      const bbox = table.bbox
      const top = getNumber(bbox.top)
      const left = getNumber(bbox.left)
      const bottom = getNumber(bbox.bottom)
      const right = getNumber(bbox.right)
      if (typeof top === 'number') params.set('bbox_top', String(top))
      if (typeof left === 'number') params.set('bbox_left', String(left))
      if (typeof bottom === 'number') params.set('bbox_bottom', String(bottom))
      if (typeof right === 'number') params.set('bbox_right', String(right))
    }
    const query = params.toString()
    return `/db/${encodeURIComponent(db)}/data/import/sheets${query ? `?${query}` : ''}`
  }

  return (
    <div>
      <PageHeader title="Google Sheets" subtitle="시트 프리뷰, 그리드, 등록 상태를 확인합니다." />

      <Tabs id="sheets-tabs" defaultSelectedTabId="preview">
        <Tab id="preview" title="Preview" />
        <Tab id="grid" title="Grid Detect" />
        <Tab id="registered" title="Registered" />
      </Tabs>

      <Card elevation={1} className="section-card">
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
          <FormGroup label="Preview Limit">
            <NumericInput value={previewLimit} onValueChange={(value) => setPreviewLimit(value)} min={1} max={100} />
          </FormGroup>
        </div>
      </Card>

      <div className="section-grid">
        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Preview</H5>
            <Button
              intent={Intent.PRIMARY}
              onClick={() => previewMutation.mutate()}
              loading={previewMutation.isPending}
              disabled={!sheetUrl || previewCooldown.active}
            >
              Preview {previewCooldown.active ? `(${previewCooldown.remainingSeconds}s)` : ''}
            </Button>
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
                  <tr key={index}>
                    {asArray<unknown>(row).map((cell, idx) => (
                      <td key={idx}>{formatCellValue(cell)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          ) : (
            <JsonView value={previewResult} fallback="프리뷰를 실행하세요." />
          )}
        </Card>

        <Card elevation={1} className="section-card">
          <div className="card-title">
            <H5>Grid Detect</H5>
            <Button
              onClick={() => gridMutation.mutate()}
              loading={gridMutation.isPending}
              disabled={!sheetUrl || gridCooldown.active}
            >
              Detect {gridCooldown.active ? `(${gridCooldown.remainingSeconds}s)` : ''}
            </Button>
          </div>
          {gridPreview.length ? (
            <HTMLTable striped className="full-width">
              <tbody>
                {gridPreview.map((row, rowIndex) => (
                  <tr key={`row-${rowIndex}`}>
                    {row.map((cell, colIndex) => (
                      <td key={`cell-${rowIndex}-${colIndex}`}>{formatCellValue(cell)}</td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          ) : (
            <JsonView value={gridResult} fallback="그리드 감지를 실행하세요." />
          )}
          {gridTables.length ? (
            <HTMLTable striped className="full-width">
              <thead>
                <tr>
                  <th>Table ID</th>
                  <th>Bounding Box</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {gridTables.map((table) => (
                  <tr key={table.id}>
                    <td>{table.id}</td>
                    <td>
                      {table.bbox
                        ? `top:${getNumber(table.bbox.top) ?? '-'}, left:${getNumber(table.bbox.left) ?? '-'}, bottom:${getNumber(table.bbox.bottom) ?? '-'}, right:${getNumber(table.bbox.right) ?? '-'}`
                        : '-'}
                    </td>
                    <td>
                      <Link to={buildImportUrl(table)}>
                        <Button minimal>Use in Import</Button>
                      </Link>
                    </td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          ) : (
            <Callout intent={Intent.PRIMARY}>테이블 탐지 정보가 없으면 Import 화면에서 수동 입력하세요.</Callout>
          )}
        </Card>
      </div>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Register</H5>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => registerMutation.mutate()}
            disabled={!sheetUrl || registerCooldown.active}
            loading={registerMutation.isPending}
          >
            Register {registerCooldown.active ? `(${registerCooldown.remainingSeconds}s)` : ''}
          </Button>
        </div>
        <div className="form-row">
          <InputGroup
            placeholder="Target class label (optional)"
            value={registerClassLabel}
            onChange={(event) => setRegisterClassLabel(event.currentTarget.value)}
          />
          <InputGroup
            placeholder="Target branch"
            value={registerBranch}
            onChange={(event) => setRegisterBranch(event.currentTarget.value)}
          />
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Registered Sheets</H5>
          <Button minimal icon="refresh" onClick={() => registeredQuery.refetch()}>
            Refresh
          </Button>
        </div>
        <HTMLTable striped interactive className="full-width">
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
              const sheetId = getString(sheet.sheet_id) ?? getString(sheet.sheetId) ?? ''
              const worksheet = getString(sheet.worksheet_name) ?? getString(sheet.worksheet) ?? '-'
              const database = getString(sheet.database_name) ?? '-'
              const branch = getString(sheet.branch) ?? 'main'
              return (
                <tr key={sheetId}>
                  <td>{sheetId}</td>
                  <td>{worksheet}</td>
                  <td>{database}</td>
                  <td>{branch}</td>
                <td>
                  <Button minimal onClick={() => previewRegisteredMutation.mutate(sheetId)}>
                    Preview
                  </Button>
                  <Button minimal intent={Intent.DANGER} onClick={() => deleteMutation.mutate(sheetId)}>
                    Remove
                  </Button>
                </td>
              </tr>
            )})}
          </tbody>
        </HTMLTable>
        <JsonView value={registeredPreview} fallback="등록된 시트 프리뷰를 확인하세요." />
      </Card>
    </div>
  )
}
