import { useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  FormGroup,
  HTMLSelect,
  InputGroup,
  Intent,
  Switch,
} from '@blueprintjs/core'
import {
  cleanupOldReplays,
  getSystemHealth,
  recomputeProjection,
  replayInstanceState,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { navigateWithSearch } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

export const AdminPage = () => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const adminToken = useAppStore((state) => state.adminToken)

  const [dbName, setDbName] = useState('')
  const [classId, setClassId] = useState('')
  const [instanceId, setInstanceId] = useState('')

  const [projection, setProjection] = useState<'instances' | 'ontologies'>('instances')
  const [branch, setBranch] = useState('main')
  const [fromTs, setFromTs] = useState('')
  const [toTs, setToTs] = useState('')
  const [promote, setPromote] = useState(false)
  const [allowDelete, setAllowDelete] = useState(false)
  const [maxEvents, setMaxEvents] = useState('')

  const [cleanupHours, setCleanupHours] = useState('24')

  const canRun = Boolean(adminToken)

  const replayMutation = useMutation({
    mutationFn: () =>
      replayInstanceState(requestContext, {
        db_name: dbName,
        class_id: classId,
        instance_id: instanceId,
        store_result: true,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const recomputeMutation = useMutation({
    mutationFn: () =>
      recomputeProjection(requestContext, {
        db_name: dbName,
        projection,
        branch,
        from_ts: fromTs,
        to_ts: toTs || undefined,
        promote,
        allow_delete_base_index: allowDelete,
        max_events: maxEvents ? Number(maxEvents) : undefined,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const cleanupMutation = useMutation({
    mutationFn: () => cleanupOldReplays(requestContext, { older_than_hours: Number(cleanupHours) || 24 }),
    onError: (error) => toastApiError(error, language),
  })

  const healthMutation = useMutation({
    mutationFn: () => getSystemHealth(requestContext),
    onError: (error) => toastApiError(error, language),
  })

  return (
    <div>
      <PageHeader title="Admin Operations" subtitle="Admin token required." />
      {!canRun ? (
        <Callout intent={Intent.WARNING} style={{ marginBottom: 12 }}>
          Admin token is missing. Open Settings and set a token to enable admin operations.
        </Callout>
      ) : null}

      <div className="page-grid two-col">
        <Card className="card-stack">
          <div className="card-title">Replay instance state</div>
          <FormGroup label="Database">
            <InputGroup value={dbName} onChange={(event) => setDbName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Class ID">
            <InputGroup value={classId} onChange={(event) => setClassId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Instance ID">
            <InputGroup value={instanceId} onChange={(event) => setInstanceId(event.currentTarget.value)} />
          </FormGroup>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => replayMutation.mutate()}
            disabled={!dbName || !classId || !instanceId || !canRun}
            loading={replayMutation.isPending}
          >
            Replay
          </Button>
          {replayMutation.data && (replayMutation.data as { task_id?: string }).task_id ? (
            <Callout intent={Intent.PRIMARY}>
              Task ID: {(replayMutation.data as { task_id?: string }).task_id}
              <div style={{ marginTop: 6 }}>
                <Button
                  small
                  onClick={() =>
                    navigateWithSearch('/operations/tasks', {
                      task_id: (replayMutation.data as { task_id?: string }).task_id ?? '',
                    })
                  }
                >
                  Open in Tasks
                </Button>
              </div>
            </Callout>
          ) : null}
          <JsonViewer value={replayMutation.data} empty="Replay response will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Recompute projection</div>
          <FormGroup label="Database">
            <InputGroup value={dbName} onChange={(event) => setDbName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Projection">
            <HTMLSelect
              value={projection}
              options={[
                { label: 'instances', value: 'instances' },
                { label: 'ontologies', value: 'ontologies' },
              ]}
              onChange={(event) => setProjection(event.currentTarget.value as 'instances' | 'ontologies')}
            />
          </FormGroup>
          <FormGroup label="Branch">
            <InputGroup value={branch} onChange={(event) => setBranch(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="From timestamp (ISO8601)">
            <InputGroup value={fromTs} onChange={(event) => setFromTs(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="To timestamp (optional)">
            <InputGroup value={toTs} onChange={(event) => setToTs(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Max events (optional)">
            <InputGroup value={maxEvents} onChange={(event) => setMaxEvents(event.currentTarget.value)} />
          </FormGroup>
          <div className="form-row">
            <Switch checked={promote} label="Promote index" onChange={(event) => setPromote(event.currentTarget.checked)} />
            <Switch checked={allowDelete} label="Allow delete base index" onChange={(event) => setAllowDelete(event.currentTarget.checked)} />
          </div>
          <Button
            intent={Intent.PRIMARY}
            onClick={() => recomputeMutation.mutate()}
            disabled={!dbName || !fromTs || !canRun}
            loading={recomputeMutation.isPending}
          >
            Recompute
          </Button>
          {recomputeMutation.data && (recomputeMutation.data as { task_id?: string }).task_id ? (
            <Callout intent={Intent.PRIMARY}>
              Task ID: {(recomputeMutation.data as { task_id?: string }).task_id}
              <div style={{ marginTop: 6 }}>
                <Button
                  small
                  onClick={() =>
                    navigateWithSearch('/operations/tasks', {
                      task_id: (recomputeMutation.data as { task_id?: string }).task_id ?? '',
                    })
                  }
                >
                  Open in Tasks
                </Button>
              </div>
            </Callout>
          ) : null}
          <JsonViewer value={recomputeMutation.data} empty="Recompute response will appear here." />
        </Card>
      </div>

      <div className="page-grid two-col" style={{ marginTop: 16 }}>
        <Card className="card-stack">
          <div className="card-title">Cleanup old replay results</div>
          <FormGroup label="Older than (hours)">
            <InputGroup value={cleanupHours} onChange={(event) => setCleanupHours(event.currentTarget.value)} />
          </FormGroup>
          <Button intent={Intent.WARNING} onClick={() => cleanupMutation.mutate()} disabled={!canRun} loading={cleanupMutation.isPending}>
            Cleanup
          </Button>
          <JsonViewer value={cleanupMutation.data} empty="Cleanup response will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">System health</div>
          <Button onClick={() => healthMutation.mutate()} disabled={!canRun} loading={healthMutation.isPending}>
            Load health
          </Button>
          <JsonViewer value={healthMutation.data} empty="System health will appear here." />
        </Card>
      </div>
    </div>
  )
}
