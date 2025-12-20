import { useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { Button, Card, Callout, FormGroup, Intent, Text } from '@blueprintjs/core'
import { clearMappings, exportMappings, getMappingsSummary, importMappings, validateMappings } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { showAppToast } from '../app/AppToaster'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'

export const MappingsPage = ({ dbName }: { dbName: string }) => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)

  const [file, setFile] = useState<File | null>(null)
  const [validationResult, setValidationResult] = useState<unknown>(null)

  const summaryQuery = useQuery({
    queryKey: qk.mappingsSummary(dbName, requestContext.language),
    queryFn: () => getMappingsSummary(requestContext, dbName),
  })

  const exportMutation = useMutation({
    mutationFn: () => exportMappings(requestContext, dbName),
    onSuccess: ({ blob, disposition }) => {
      const url = URL.createObjectURL(blob)
      const anchor = document.createElement('a')
      const match = /filename=([^;]+)/i.exec(disposition)
      anchor.href = url
      anchor.download = match ? match[1].replace(/\"/g, '') : `${dbName}_mappings.json`
      anchor.click()
      URL.revokeObjectURL(url)
    },
    onError: (error) => toastApiError(error, language),
  })

  const validateMutation = useMutation({
    mutationFn: () => {
      if (!file) {
        throw new Error('Select a file first')
      }
      return validateMappings(requestContext, dbName, file)
    },
    onSuccess: (result) => setValidationResult(result),
    onError: (error) => toastApiError(error, language),
  })

  const importMutation = useMutation({
    mutationFn: () => {
      if (!file) {
        throw new Error('Select a file first')
      }
      return importMappings(requestContext, dbName, file)
    },
    onSuccess: () => {
      void showAppToast({ intent: Intent.SUCCESS, message: 'Mappings imported.' })
      void queryClient.invalidateQueries({ queryKey: qk.mappingsSummary(dbName, requestContext.language) })
    },
    onError: (error) => toastApiError(error, language),
  })

  const clearMutation = useMutation({
    mutationFn: () => clearMappings(requestContext, dbName),
    onSuccess: () => {
      void showAppToast({ intent: Intent.WARNING, message: 'Mappings cleared.' })
      void queryClient.invalidateQueries({ queryKey: qk.mappingsSummary(dbName, requestContext.language) })
    },
    onError: (error) => toastApiError(error, language),
  })

  const summary = useMemo(() => summaryQuery.data ?? null, [summaryQuery.data])

  return (
    <div>
      <PageHeader title="Mappings" subtitle="Manage label-to-property mappings." />

      <div className="card-stack">
        <Card>
          <div className="card-title">Actions</div>
          <div className="form-row">
            <Button icon="refresh" onClick={() => void queryClient.invalidateQueries({ queryKey: qk.mappingsSummary(dbName, requestContext.language) })}>
              Refresh
            </Button>
            <Button icon="download" onClick={() => exportMutation.mutate()} loading={exportMutation.isPending}>
              Export
            </Button>
            <Button icon="trash" intent={Intent.DANGER} onClick={() => clearMutation.mutate()} loading={clearMutation.isPending}>
              Clear
            </Button>
          </div>
          <FormGroup label="Import / Validate file" helperText="Upload JSON mapping bundle">
            <input
              type="file"
              accept=".json,.txt"
              onChange={(event) => setFile(event.currentTarget.files?.[0] ?? null)}
            />
          </FormGroup>
          <div className="form-row">
            <Button icon="tick-circle" onClick={() => validateMutation.mutate()} disabled={!file} loading={validateMutation.isPending}>
              Validate
            </Button>
            <Button icon="upload" intent={Intent.PRIMARY} onClick={() => importMutation.mutate()} disabled={!file} loading={importMutation.isPending}>
              Import
            </Button>
          </div>
        </Card>

        <Card>
          <div className="card-title">Summary</div>
          <JsonViewer value={summary} empty="No summary available." />
        </Card>

        <Card>
          <div className="card-title">Validation result</div>
          <JsonViewer value={validationResult} empty="Run validation to see details." />
        </Card>

        <Card>
          <div className="card-title">Notes</div>
          <Callout intent={Intent.PRIMARY}>
            Use mappings to resolve unknown label keys from imports and instance writes.
          </Callout>
        </Card>
      </div>
    </div>
  )
}
