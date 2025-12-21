import { useMemo, useState } from 'react'
import { useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Card,
  FileInput,
  H5,
  HTMLTable,
  Intent,
} from '@blueprintjs/core'
import {
  exportMappings,
  getMappingsSummary,
  importMappings,
  validateMappings,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { asArray, asRecord, getString, type UnknownRecord } from '../utils/typed'

export const MappingsPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [file, setFile] = useState<File | null>(null)
  const [mappingData, setMappingData] = useState<UnknownRecord | null>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const summaryQuery = useQuery({
    queryKey: db ? qk.mappingsSummary({ dbName: db, language: context.language }) : ['bff', 'mappings', 'empty'],
    queryFn: () => getMappingsSummary(requestContext, db ?? ''),
    enabled: Boolean(db),
  })

  const exportMutation = useMutation({
    mutationFn: async () => {
      if (!db) {
        throw new Error('Missing db')
      }
      const { blob, filename } = await exportMappings(requestContext, db)
      const text = await blob.text()
      let json: unknown = null
      try {
        json = JSON.parse(text)
      } catch {
        json = null
      }
      return { json, filename, blob }
    },
    onSuccess: (result) => {
      if (!result) {
        return
      }
      setMappingData(result.json)
      const url = URL.createObjectURL(result.blob)
      const link = document.createElement('a')
      link.href = url
      link.download = result.filename ?? `${db ?? 'mappings'}_mappings.json`
      link.click()
      URL.revokeObjectURL(url)
      void showAppToast({ intent: Intent.SUCCESS, message: '매핑 파일을 다운로드했습니다.' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const validateMutation = useMutation({
    mutationFn: async () => {
      if (!db || !file) {
        throw new Error('Missing file')
      }
      return validateMappings(requestContext, db, file)
    },
    onSuccess: (result) => {
      setMappingData(result)
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const importMutation = useMutation({
    mutationFn: async () => {
      if (!db || !file) {
        throw new Error('Missing file')
      }
      return importMappings(requestContext, db, file)
    },
    onSuccess: () => {
      void summaryQuery.refetch()
      void showAppToast({ intent: Intent.SUCCESS, message: '매핑을 가져왔습니다.' })
    },
    onError: (error) => toastApiError(error, context.language),
  })

  const rows = useMemo(() => {
    if (!mappingData) {
      return []
    }
    const data = asRecord(mappingData)
    const classes = asArray<UnknownRecord>(data.classes).map((row) => ({
      kind: 'class',
      label:
        getString(row.label) ??
        getString(row.label_ko) ??
        getString(row.label_en) ??
        getString(row.label_key) ??
        getString(row.name) ??
        '-',
      class_id: getString(row.class_id) ?? getString(row.classId) ?? getString(row.class) ?? '-',
      property_id: '-',
      predicate: '-',
      status: getString(row.status) ?? getString(row.mapping_status) ?? getString(row.state) ?? '-',
    }))
    const props = asArray<UnknownRecord>(data.properties).map((row) => ({
      kind: 'property',
      label:
        getString(row.label) ??
        getString(row.label_ko) ??
        getString(row.label_en) ??
        getString(row.label_key) ??
        getString(row.name) ??
        '-',
      class_id: getString(row.class_id) ?? getString(row.classId) ?? getString(row.class) ?? '-',
      property_id:
        getString(row.property_id) ?? getString(row.propertyId) ?? getString(row.property) ?? '-',
      predicate: '-',
      status: getString(row.status) ?? getString(row.mapping_status) ?? getString(row.state) ?? '-',
    }))
    const rels = asArray<UnknownRecord>(data.relationships).map((row) => ({
      kind: 'relationship',
      label:
        getString(row.label) ??
        getString(row.label_ko) ??
        getString(row.label_en) ??
        getString(row.label_key) ??
        getString(row.name) ??
        '-',
      class_id:
        getString(row.class_id) ??
        getString(row.classId) ??
        getString(row.class) ??
        getString(row.source_class_id) ??
        '-',
      property_id: '-',
      predicate: getString(row.predicate) ?? getString(row.relationship_id) ?? '-',
      status: getString(row.status) ?? getString(row.mapping_status) ?? getString(row.state) ?? '-',
    }))
    return [...classes, ...props, ...rels]
  }, [mappingData])

  return (
    <div>
      <PageHeader title="Mappings" subtitle="라벨 매핑을 관리합니다." />

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Summary</H5>
        </div>
        <JsonView value={summaryQuery.data} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Actions</H5>
        </div>
        <div className="form-row">
          <Button icon="refresh" onClick={() => exportMutation.mutate()} loading={exportMutation.isPending}>
            Refresh
          </Button>
          <Button icon="cloud-download" onClick={() => exportMutation.mutate()} loading={exportMutation.isPending}>
            Export & Download
          </Button>
          <FileInput
            text={file ? file.name : 'Select mapping file'}
            onInputChange={(event) => {
              const target = event.currentTarget as HTMLInputElement
              const nextFile = target.files?.[0] ?? null
              setFile(nextFile)
            }}
          />
          <Button onClick={() => validateMutation.mutate()} disabled={!file}>
            Validate File
          </Button>
          <Button intent={Intent.PRIMARY} onClick={() => importMutation.mutate()} disabled={!file}>
            Import File
          </Button>
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Mappings</H5>
        </div>
        {rows.length === 0 ? (
          <JsonView value={mappingData} fallback="매핑 데이터를 불러오세요." />
        ) : (
          <HTMLTable striped interactive className="full-width">
            <thead>
              <tr>
                <th>Type</th>
                <th>Label</th>
                <th>Class ID</th>
                <th>Property/Predicate</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {rows.map((row, index) => (
                <tr key={`${row.label ?? row.property_id ?? row.class_id}-${index}`}>
                  <td>{row.kind}</td>
                  <td>{row.label ?? '-'}</td>
                  <td>{row.class_id ?? '-'}</td>
                  <td>{row.property_id !== '-' ? row.property_id : row.predicate ?? '-'}</td>
                  <td>{row.status ?? '-'}</td>
                </tr>
              ))}
            </tbody>
          </HTMLTable>
        )}
      </Card>
    </div>
  )
}
