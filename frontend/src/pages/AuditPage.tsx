import { useMemo, useState } from 'react'
import { useParams, useSearchParams } from 'react-router-dom'
import { useMutation } from '@tanstack/react-query'
import {
  Button,
  Card,
  FormGroup,
  H5,
  HTMLTable,
  InputGroup,
  Intent,
} from '@blueprintjs/core'
import { getAuditChainHead, listAuditLogs } from '../api/bff'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'
import { asArray, asRecord, getString, type UnknownRecord } from '../utils/typed'

export const AuditPage = () => {
  const { db } = useParams()
  const [searchParams] = useSearchParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const commands = useAppStore((state) => state.commands)
  const trackCommand = useAppStore((state) => state.trackCommand)
  const openCommandDrawer = useAppStore((state) => state.openCommandDrawer)

  const [since, setSince] = useState('')
  const [until, setUntil] = useState('')
  const [actor, setActor] = useState('')
  const [resourceType, setResourceType] = useState('')
  const [action, setAction] = useState('')
  const [commandId, setCommandId] = useState(searchParams.get('command_id') ?? '')
  const [eventId, setEventId] = useState('')
  const [search, setSearch] = useState('')
  const [result, setResult] = useState<unknown>(null)
  const [chainHead, setChainHead] = useState<unknown>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const listMutation = useMutation({
    mutationFn: () =>
      listAuditLogs(requestContext, {
        partition_key: `db:${db}`,
        since: since || undefined,
        until: until || undefined,
        actor: actor || undefined,
        resource_type: resourceType || undefined,
        action: action || undefined,
        command_id: commandId || undefined,
        event_id: eventId || undefined,
        search: search || undefined,
      }),
    onSuccess: (payload) => setResult(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const chainMutation = useMutation({
    mutationFn: () => getAuditChainHead(requestContext, { partition_key: `db:${db}` }),
    onSuccess: (payload) => setChainHead(payload),
    onError: (error) => toastApiError(error, context.language),
  })

  const resultRecord = asRecord(result)
  const logs = asArray<UnknownRecord>(asRecord(resultRecord.data).logs ?? resultRecord.logs)

  const resetFilters = () => {
    setSince('')
    setUntil('')
    setActor('')
    setResourceType('')
    setAction('')
    setCommandId('')
    setEventId('')
    setSearch('')
    setResult(null)
    setChainHead(null)
  }

  const openCommandTracker = (commandIdValue: string) => {
    if (!commandIdValue) {
      return
    }
    if (!commands[commandIdValue]) {
      trackCommand({
        id: commandIdValue,
        kind: 'UNKNOWN',
        target: { dbName: db ?? '' },
        context: { project: db ?? null, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
    }
    openCommandDrawer(commandIdValue)
  }

  return (
    <div>
      <PageHeader title="Audit Logs" subtitle={`partition_key = db:${db}`} />

      <Card elevation={1} className="section-card">
        <div className="form-grid">
          <FormGroup label="Since">
            <InputGroup value={since} onChange={(event) => setSince(event.currentTarget.value)} placeholder="2024-01-01T00:00:00Z" />
          </FormGroup>
          <FormGroup label="Until">
            <InputGroup value={until} onChange={(event) => setUntil(event.currentTarget.value)} placeholder="2024-01-02T00:00:00Z" />
          </FormGroup>
          <FormGroup label="Actor">
            <InputGroup value={actor} onChange={(event) => setActor(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Resource type">
            <InputGroup value={resourceType} onChange={(event) => setResourceType(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Action">
            <InputGroup value={action} onChange={(event) => setAction(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Command ID">
            <InputGroup value={commandId} onChange={(event) => setCommandId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Event ID">
            <InputGroup value={eventId} onChange={(event) => setEventId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Search">
            <InputGroup value={search} onChange={(event) => setSearch(event.currentTarget.value)} />
          </FormGroup>
        </div>
        <div className="button-row">
          <Button intent={Intent.PRIMARY} onClick={() => listMutation.mutate()}>
            Apply
          </Button>
          <Button onClick={resetFilters}>Reset</Button>
          <Button
            disabled={!commandId.trim()}
            onClick={() => openCommandTracker(commandId.trim())}
          >
            Open Command Tracker
          </Button>
          <Button onClick={() => chainMutation.mutate()}>Chain-head Verify</Button>
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Logs</H5>
        {logs.length ? (
          <HTMLTable striped interactive className="full-width">
            <thead>
              <tr>
                <th>Time</th>
                <th>Actor</th>
                <th>Action</th>
                <th>Status</th>
                <th>Resource</th>
                <th>Command</th>
              </tr>
            </thead>
            <tbody>
              {logs.map((log, index) => {
                const eventId = getString(log.event_id) ?? String(index)
                const occurredAt = getString(log.occurred_at) ?? getString(log.timestamp) ?? '-'
                const actorValue = getString(log.actor) ?? '-'
                const actionValue = getString(log.action) ?? '-'
                const statusValue = getString(log.status) ?? '-'
                const resourceValue = getString(log.resource_type) ?? getString(log.resource_id) ?? '-'
                const cmdId = getString(log.command_id)
                return (
                  <tr key={eventId}>
                    <td>{occurredAt}</td>
                    <td>{actorValue}</td>
                    <td>{actionValue}</td>
                    <td>{statusValue}</td>
                    <td>{resourceValue}</td>
                    <td>
                      {cmdId ? (
                        <Button minimal onClick={() => openCommandTracker(cmdId)}>
                          {cmdId}
                        </Button>
                      ) : (
                        '-'
                      )}
                    </td>
                  </tr>
                )
              })}
            </tbody>
          </HTMLTable>
        ) : (
          <JsonView value={result} />
        )}
      </Card>

      <Card elevation={1} className="section-card">
        <H5>Chain Head</H5>
        <JsonView value={chainHead} />
      </Card>
    </div>
  )
}
