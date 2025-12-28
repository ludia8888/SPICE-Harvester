import { useState } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Card,
  Dialog,
  FileInput,
  FormGroup,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Switch,
  Text,
} from '@blueprintjs/core'
import { createDataset, createDatasetVersion } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { navigate } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

type CsvPreview = { columns: string[]; rows: Record<string, string>[] }

const detectCsvDelimiter = (content: string) => {
  const firstLine = content.split(/\r?\n/).find((line) => line.trim()) ?? ''
  if (firstLine.includes('\t')) return '\t'
  if (firstLine.includes(';')) return ';'
  if (firstLine.includes('|')) return '|'
  return ','
}

const parseCsvLine = (line: string, delimiter: string) => {
  const cells: string[] = []
  let current = ''
  let inQuotes = false
  for (let index = 0; index < line.length; index += 1) {
    const char = line[index]
    if (char === '"') {
      const nextChar = line[index + 1]
      if (inQuotes && nextChar === '"') {
        current += '"'
        index += 1
      } else {
        inQuotes = !inQuotes
      }
      continue
    }
    if (char === delimiter && !inQuotes) {
      cells.push(current.trim())
      current = ''
      continue
    }
    current += char
  }
  cells.push(current.trim())
  return cells
}

const parseCsvContent = (content: string, delimiter: string, hasHeader: boolean, maxRows = 200): CsvPreview => {
  const lines = content
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
  if (lines.length === 0) {
    return { columns: [], rows: [] }
  }
  const resolvedDelimiter = delimiter || ','
  const headerCells = parseCsvLine(lines[0], resolvedDelimiter)
  const columns = hasHeader
    ? headerCells.map((cell, index) => cell || `column_${index + 1}`)
    : headerCells.map((_, index) => `column_${index + 1}`)
  const dataStart = hasHeader ? 1 : 0
  const rows: CsvPreview['rows'] = []
  lines.slice(dataStart, dataStart + maxRows).forEach((line) => {
    const cells = parseCsvLine(line, resolvedDelimiter)
    const row: Record<string, string> = {}
    columns.forEach((key, index) => {
      row[key] = cells[index] ?? ''
    })
    rows.push(row)
  })
  return { columns, rows }
}

export const SheetsHubPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const queryClient = useQueryClient()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const [csvDialogOpen, setCsvDialogOpen] = useState(false)
  const [csvFile, setCsvFile] = useState<File | null>(null)
  const [csvContent, setCsvContent] = useState('')
  const [csvHasHeader, setCsvHasHeader] = useState(true)
  const [csvDelimiter, setCsvDelimiter] = useState(',')
  const [csvDatasetName, setCsvDatasetName] = useState('')
  const [csvPreview, setCsvPreview] = useState<CsvPreview | null>(null)
  const [csvParsing, setCsvParsing] = useState(false)
  const [csvSaving, setCsvSaving] = useState(false)
  const copy = language === 'ko'
    ? {
        title: '커넥터',
        subtitle: '프로젝트에 데이터를 넣을 소스를 선택하세요.',
        googleLive: {
          title: 'Google Sheets (Live)',
          description: 'Google Sheets와 연결하여 데이터를 동기화합니다.',
          action: '연결하기',
        },
        googleImport: {
          title: 'Google Sheets (Import)',
          description: 'Google Sheets를 1회 가져오고 스키마를 매핑합니다.',
          action: '가져오기',
        },
        excelImport: {
          title: 'Excel Import',
          description: '.xlsx 파일을 업로드해 온톨로지 클래스로 매핑합니다.',
          action: '가져오기',
        },
        csvUpload: {
          title: 'CSV 업로드',
          description: '.csv 파일을 업로드해 데이터셋을 생성합니다.',
          action: '업로드',
        },
        postgres: {
          title: 'PostgreSQL',
          description: '원격 PostgreSQL 데이터베이스에 연결합니다.',
          action: '연결하기',
          toast: 'PostgreSQL 커넥터는 준비 중입니다.',
        },
        comingSoon: 'Coming Soon',
        snowflake: {
          title: 'Snowflake',
          description: 'Snowflake에서 대용량 데이터를 가져옵니다.',
        },
        salesforce: {
          title: 'Salesforce',
          description: 'CRM 데이터 엔티티를 동기화합니다.',
        },
        restApi: {
          title: 'REST API',
          description: '일반 JSON 데이터를 수집하는 소스입니다.',
        },
        csvDialog: {
          title: 'CSV 업로드',
          fileLabel: 'CSV 파일',
          datasetLabel: '데이터셋 이름',
          headerLabel: '첫 행을 헤더로 사용',
          delimiterLabel: '구분자',
          preview: '미리보기',
          create: '데이터셋 생성',
          previewEmpty: 'CSV 파일을 업로드하면 미리보기가 표시됩니다.',
          toast: 'CSV 데이터셋이 생성되었습니다.',
          back: '파이프라인 빌더로 돌아가기',
        },
      }
    : {
        title: 'Connectors',
        subtitle: 'Select a source to ingest data into your project.',
        googleLive: {
          title: 'Google Sheets (Live)',
          description: 'Connect and sync data directly from Google Sheets.',
          action: 'Connect',
        },
        googleImport: {
          title: 'Google Sheets (Import)',
          description: 'One-time import from Google Sheets with schema mapping.',
          action: 'Import',
        },
        excelImport: {
          title: 'Excel Import',
          description: 'Upload .xlsx files and map to ontology classes.',
          action: 'Import',
        },
        csvUpload: {
          title: 'CSV Upload',
          description: 'Upload .csv files and create datasets quickly.',
          action: 'Upload',
        },
        postgres: {
          title: 'PostgreSQL',
          description: 'Connect to a remote PostgreSQL database.',
          action: 'Connect',
          toast: 'PostgreSQL connector is coming soon.',
        },
        comingSoon: 'Coming Soon',
        snowflake: {
          title: 'Snowflake',
          description: 'Import large datasets from Snowflake.',
        },
        salesforce: {
          title: 'Salesforce',
          description: 'Sync CRM data entities.',
        },
        restApi: {
          title: 'REST API',
          description: 'Generic JSON data ingestion source.',
        },
        csvDialog: {
          title: 'CSV Upload',
          fileLabel: 'CSV file',
          datasetLabel: 'Dataset name',
          headerLabel: 'Use first row as header',
          delimiterLabel: 'Delimiter',
          preview: 'Preview',
          create: 'Create dataset',
          previewEmpty: 'Upload a CSV file to see a preview.',
          toast: 'CSV dataset created.',
          back: 'Back to Pipeline Builder',
        },
      }

  const handleCsvFile = async (file: File | null) => {
    setCsvFile(file)
    setCsvPreview(null)
    setCsvContent('')
    if (!file) return
    setCsvParsing(true)
    try {
      const content = await file.text()
      setCsvContent(content)
      const inferredDelimiter = detectCsvDelimiter(content)
      setCsvDelimiter(inferredDelimiter)
      const parsed = parseCsvContent(content, inferredDelimiter, csvHasHeader)
      setCsvPreview(parsed)
      if (!csvDatasetName.trim()) {
        setCsvDatasetName(file.name.replace(/\.csv$/i, ''))
      }
    } catch (error) {
      toastApiError(error, language)
    } finally {
      setCsvParsing(false)
    }
  }

  const handleCsvPreview = async () => {
    if (!csvFile) return
    setCsvParsing(true)
    try {
      const content = csvContent || (await csvFile.text())
      setCsvContent(content)
      const parsed = parseCsvContent(content, csvDelimiter, csvHasHeader)
      setCsvPreview(parsed)
    } catch (error) {
      toastApiError(error, language)
    } finally {
      setCsvParsing(false)
    }
  }

  const handleCreateCsvDataset = async () => {
    if (!csvFile || !csvDatasetName.trim()) return
    const parsed = csvPreview ?? parseCsvContent(csvContent || (await csvFile.text()), csvDelimiter, csvHasHeader)
    if (!parsed.columns.length) return
    const columns = parsed.columns.map((name) => ({ name, type: 'String' }))
    setCsvSaving(true)
    try {
      const payload = await createDataset(requestContext, {
        db_name: dbName,
        name: csvDatasetName.trim(),
        source_type: 'csv_upload',
        schema_json: { columns },
        branch,
      })
      const dataset = (payload as { data?: { dataset?: Record<string, unknown> } })?.data?.dataset
      const datasetId = typeof dataset?.dataset_id === 'string' ? dataset.dataset_id : ''
      if (datasetId) {
        await createDatasetVersion(requestContext, datasetId, {
          sample_json: { columns, rows: parsed.rows },
          schema_json: { columns },
          row_count: parsed.rows.length,
        })
      }
      void queryClient.invalidateQueries({ queryKey: qk.datasets(dbName, requestContext.language, branch) })
      void showAppToast({ intent: Intent.SUCCESS, message: copy.csvDialog.toast })
      setCsvDialogOpen(false)
      setCsvFile(null)
      setCsvContent('')
      setCsvDatasetName('')
      setCsvPreview(null)
      setCsvDelimiter(',')
      setCsvHasHeader(true)
    } catch (error) {
      toastApiError(error, language)
    } finally {
      setCsvSaving(false)
    }
  }

  return (
    <div>
      <PageHeader
        title={copy.title}
        subtitle={copy.subtitle}
        actions={
          <Button icon="arrow-left" minimal onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/pipeline`)}>
            {copy.csvDialog.back}
          </Button>
        }
      />

      <div className="db-grid" style={{ gridTemplateColumns: 'repeat(auto-fit, minmax(220px, 1fr))' }}>
        <Card className="connector-card" style={{ minHeight: '140px' }}>
          <H5 style={{ marginTop: 0 }}>{copy.googleLive.title}</H5>
          <Text className="muted" style={{ fontSize: '0.85rem' }}>{copy.googleLive.description}</Text>
          <Button intent={Intent.PRIMARY} onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/data/sheets/google`)}>
            {copy.googleLive.action}
          </Button>
        </Card>
        <Card className="connector-card" style={{ minHeight: '140px' }}>
          <H5 style={{ marginTop: 0 }}>{copy.googleImport.title}</H5>
          <Text className="muted" style={{ fontSize: '0.85rem' }}>{copy.googleImport.description}</Text>
          <Button onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/data/import/sheets`)}>
            {copy.googleImport.action}
          </Button>
        </Card>
        <Card className="connector-card" style={{ minHeight: '140px' }}>
          <H5 style={{ marginTop: 0 }}>{copy.excelImport.title}</H5>
          <Text className="muted" style={{ fontSize: '0.85rem' }}>{copy.excelImport.description}</Text>
          <Button onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/data/import/excel`)}>
            {copy.excelImport.action}
          </Button>
        </Card>
        <Card className="connector-card" style={{ minHeight: '140px' }}>
          <H5 style={{ marginTop: 0 }}>{copy.csvUpload.title}</H5>
          <Text className="muted" style={{ fontSize: '0.85rem' }}>{copy.csvUpload.description}</Text>
          <Button onClick={() => setCsvDialogOpen(true)}>
            {copy.csvUpload.action}
          </Button>
        </Card>
        <Card className="connector-card" style={{ minHeight: '140px' }}>
          <H5 style={{ marginTop: 0 }}>{copy.postgres.title}</H5>
          <Text className="muted" style={{ fontSize: '0.85rem' }}>{copy.postgres.description}</Text>
          <Button
            onClick={() =>
              showAppToast({
                intent: Intent.PRIMARY,
                message: copy.postgres.toast,
              })
            }
          >
            {copy.postgres.action}
          </Button>
        </Card>
        {[copy.snowflake, copy.salesforce, copy.restApi].map((connector) => (
          <Card key={connector.title} className="connector-card" style={{ minHeight: '140px' }}>
            <H5 style={{ marginTop: 0 }}>{connector.title}</H5>
            <Text className="muted" style={{ fontSize: '0.85rem' }}>{connector.description}</Text>
            <Text style={{ marginTop: 'auto', fontSize: '0.75rem', color: '#5C7080', fontStyle: 'italic' }}>
              {copy.comingSoon}
            </Text>
          </Card>
        ))}
      </div>

      <Dialog isOpen={csvDialogOpen} onClose={() => setCsvDialogOpen(false)} title={copy.csvDialog.title}>
        <div style={{ padding: '16px 20px', display: 'flex', flexDirection: 'column', gap: '12px' }}>
          <FormGroup label={copy.csvDialog.fileLabel}>
            <FileInput
              text={csvFile?.name ?? copy.csvDialog.fileLabel}
              inputProps={{ accept: '.csv' }}
              onInputChange={(event) => handleCsvFile(event.currentTarget.files?.[0] ?? null)}
            />
          </FormGroup>
          <FormGroup label={copy.csvDialog.datasetLabel}>
            <InputGroup value={csvDatasetName} onChange={(event) => setCsvDatasetName(event.currentTarget.value)} />
          </FormGroup>
          <div style={{ display: 'flex', gap: '12px', alignItems: 'center' }}>
            <Switch checked={csvHasHeader} label={copy.csvDialog.headerLabel} onChange={() => setCsvHasHeader((current) => !current)} />
            <FormGroup label={copy.csvDialog.delimiterLabel}>
              <HTMLSelect value={csvDelimiter} onChange={(event) => setCsvDelimiter(event.currentTarget.value)}>
                <option value=",">,</option>
                <option value=";">;</option>
                <option value="\t">Tab</option>
                <option value="|">|</option>
              </HTMLSelect>
            </FormGroup>
          </div>
          <div style={{ display: 'flex', gap: '8px' }}>
            <Button icon="eye-open" onClick={() => void handleCsvPreview()} disabled={!csvFile || csvParsing} loading={csvParsing}>
              {copy.csvDialog.preview}
            </Button>
            <Button intent={Intent.PRIMARY} icon="add" onClick={() => void handleCreateCsvDataset()} disabled={!csvFile || !csvDatasetName.trim() || csvSaving} loading={csvSaving}>
              {copy.csvDialog.create}
            </Button>
          </div>
          <div style={{ border: '1px solid rgba(255,255,255,0.08)', borderRadius: '6px', padding: '8px', maxHeight: '240px', overflow: 'auto' }}>
            {csvPreview?.columns?.length ? (
              <HTMLTable compact striped>
                <thead>
                  <tr>
                    {csvPreview.columns.map((column) => (
                      <th key={column}>{column}</th>
                    ))}
                  </tr>
                </thead>
                <tbody>
                  {csvPreview.rows.map((row, index) => (
                    <tr key={`csv-${index}`}>
                      {csvPreview.columns.map((column) => (
                        <td key={`${index}-${column}`}>{row[column] ?? ''}</td>
                      ))}
                    </tr>
                  ))}
                </tbody>
              </HTMLTable>
            ) : (
              <Text className="muted">{copy.csvDialog.previewEmpty}</Text>
            )}
          </div>
        </div>
      </Dialog>
    </div>
  )
}
