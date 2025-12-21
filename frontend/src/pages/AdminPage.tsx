import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Checkbox,
  FormGroup,
  H5,
  HTMLSelect,
  InputGroup,
  Intent,
  NumericInput,
} from '@blueprintjs/core'
import {
  cleanupOldReplays,
  getSystemHealth,
  replayInstanceState,
  recomputeProjection,
} from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

export const AdminPage = () => {
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)

  const [dbName, setDbName] = useState('')
  const [classId, setClassId] = useState('')
  const [instanceId, setInstanceId] = useState('')
  const [storeResult, setStoreResult] = useState(true)
  const [resultTtl, setResultTtl] = useState(3600)
  const [replayResult, setReplayResult] = useState<any>(null)

  const [projection, setProjection] = useState('instances')
  const [branch, setBranch] = useState('main')
  const [fromTs, setFromTs] = useState('')
  const [toTs, setToTs] = useState('')
  const [promote, setPromote] = useState(false)
  const [allowDelete, setAllowDelete] = useState(false)
  const [maxEvents, setMaxEvents] = useState<number | null>(null)
  const [recomputeResult, setRecomputeResult] = useState<any>(null)

  const [cleanupHours, setCleanupHours] = useState(24)
  const [cleanupResult, setCleanupResult] = useState<any>(null)
  const [healthResult, setHealthResult] = useState<any>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const replayMutation = useMutation({
    mutationFn: () =>
      replayInstanceState(requestContext, {
        db_name: dbName,
        class_id: classId,
        instance_id: instanceId,
        store_result: storeResult,
        result_ttl: resultTtl,
      }),
    onSuccess: (payload) => setReplayResult(payload),
    onError: (error) => toastApiError(error, context.language),
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
        max_events: maxEvents || undefined,
      }),
    onSuccess: (payload) => setRecomputeResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const cleanupMutation = useMutation({
    mutationFn: () => cleanupOldReplays(requestContext, cleanupHours),
    onSuccess: (payload) => setCleanupResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const healthMutation = useMutation({
    mutationFn: () => getSystemHealth(requestContext),
    onSuccess: (payload) => setHealthResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  return (
    <div>
      <PageHeader title="Admin" subtitle="운영자 전용 작업" />
      {!adminToken ? (
        <Callout intent={Intent.WARNING} title="Admin token required">
          Admin 토큰을 설정해야 실행할 수 있습니다.
        </Callout>
      ) : null}

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Replay Instance State</H5>
          <Button intent={Intent.PRIMARY} onClick={() => replayMutation.mutate()} disabled={!adminToken}>
            Run
          </Button>
        </div>
        <div className="form-grid">
          <FormGroup label="DB">
            <InputGroup value={dbName} onChange={(event) => setDbName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Class ID">
            <InputGroup value={classId} onChange={(event) => setClassId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Instance ID">
            <InputGroup value={instanceId} onChange={(event) => setInstanceId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Result TTL">
            <NumericInput value={resultTtl} min={60} onValueChange={(value) => setResultTtl(value)} />
          </FormGroup>
        </div>
        <Checkbox checked={storeResult} onChange={(event) => setStoreResult(event.currentTarget.checked)}>
          Store result
        </Checkbox>
        <JsonView value={replayResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Recompute Projection</H5>
          <Button intent={Intent.PRIMARY} onClick={() => recomputeMutation.mutate()} disabled={!adminToken}>
            Run
          </Button>
        </div>
        <div className="form-grid">
          <FormGroup label="DB">
            <InputGroup value={dbName} onChange={(event) => setDbName(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Projection">
            <HTMLSelect value={projection} onChange={(event) => setProjection(event.currentTarget.value)} options={['instances', 'ontologies']} />
          </FormGroup>
          <FormGroup label="Branch">
            <InputGroup value={branch} onChange={(event) => setBranch(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="From">
            <InputGroup value={fromTs} onChange={(event) => setFromTs(event.currentTarget.value)} placeholder="2024-01-01T00:00:00Z" />
          </FormGroup>
          <FormGroup label="To">
            <InputGroup value={toTs} onChange={(event) => setToTs(event.currentTarget.value)} placeholder="optional" />
          </FormGroup>
          <FormGroup label="Max events">
            <NumericInput value={maxEvents ?? undefined} onValueChange={(value) => setMaxEvents(Number.isNaN(value) ? null : value)} />
          </FormGroup>
        </div>
        <Checkbox checked={promote} onChange={(event) => setPromote(event.currentTarget.checked)}>
          Promote
        </Checkbox>
        <Checkbox checked={allowDelete} onChange={(event) => setAllowDelete(event.currentTarget.checked)}>
          Allow delete base index
        </Checkbox>
        <JsonView value={recomputeResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Cleanup Old Replays</H5>
          <Button intent={Intent.DANGER} onClick={() => cleanupMutation.mutate()} disabled={!adminToken}>
            Cleanup
          </Button>
        </div>
        <NumericInput value={cleanupHours} min={1} max={168} onValueChange={(value) => setCleanupHours(value)} />
        <JsonView value={cleanupResult} />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>System Health</H5>
          <Button onClick={() => healthMutation.mutate()} disabled={!adminToken}>
            Fetch
          </Button>
        </div>
        <JsonView value={healthResult} />
      </Card>
    </div>
  )
}
