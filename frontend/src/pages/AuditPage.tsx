import { useMemo, useState } from 'react'
import { useMutation } from '@tanstack/react-query'
import { Button, Card, FormGroup, HTMLTable, InputGroup, Intent, Text } from '@blueprintjs/core'
import { getAuditChainHead, listAuditLogs } from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { toastApiError } from '../errors/toastApiError'
import { useAppStore } from '../store/useAppStore'

export const AuditPage = ({ dbName }: { dbName: string }) => {
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const setInspector = useAppStore((state) => state.setInspector)

  const searchParams = useMemo(() => {
    if (typeof window === 'undefined') {
      return new URLSearchParams()
    }
    return new URL(window.location.href).searchParams
  }, [])

  const readParam = (key: string, fallback: string) => searchParams.get(key) ?? fallback

  const [partitionKey, setPartitionKey] = useState(() => readParam('partition_key', `db:${dbName}`))
  const [limit, setLimit] = useState(() => readParam('limit', '50'))
  const [offset, setOffset] = useState(() => readParam('offset', '0'))
  const [action, setAction] = useState(() => readParam('action', ''))
  const [status, setStatus] = useState(() => readParam('status', ''))
  const [resourceType, setResourceType] = useState(() => readParam('resource_type', ''))
  const [resourceId, setResourceId] = useState(() => readParam('resource_id', ''))
  const [eventId, setEventId] = useState(() => readParam('event_id', ''))
  const [commandId, setCommandId] = useState(() => readParam('command_id', ''))
  const [actor, setActor] = useState(() => readParam('actor', ''))
  const [since, setSince] = useState(() => readParam('since', ''))
  const [until, setUntil] = useState(() => readParam('until', ''))

  const logsMutation = useMutation({
    mutationFn: () =>
      listAuditLogs(requestContext, {
        partition_key: partitionKey,
        limit: Number(limit) || 50,
        offset: Number(offset) || 0,
        action: action || undefined,
        status: status || undefined,
        resource_type: resourceType || undefined,
        resource_id: resourceId || undefined,
        event_id: eventId || undefined,
        command_id: commandId || undefined,
        actor: actor || undefined,
        since: since || undefined,
        until: until || undefined,
      }),
    onError: (error) => toastApiError(error, language),
  })

  const chainMutation = useMutation({
    mutationFn: () => getAuditChainHead(requestContext, partitionKey),
    onError: (error) => toastApiError(error, language),
  })

  const logItems = useMemo(() => {
    const payload = logsMutation.data as { data?: { items?: Array<Record<string, unknown>> } } | undefined
    return payload?.data?.items ?? []
  }, [logsMutation.data])

  return (
    <div>
      <PageHeader title="Audit Logs" subtitle="Inspect audit trail by partition key." />

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Partition key">
            <InputGroup value={partitionKey} onChange={(event) => setPartitionKey(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Action">
            <InputGroup value={action} onChange={(event) => setAction(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Status">
            <InputGroup value={status} onChange={(event) => setStatus(event.currentTarget.value)} placeholder="success|failure" />
          </FormGroup>
          <FormGroup label="Actor">
            <InputGroup value={actor} onChange={(event) => setActor(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Resource type">
            <InputGroup value={resourceType} onChange={(event) => setResourceType(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Resource id">
            <InputGroup value={resourceId} onChange={(event) => setResourceId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Command id">
            <InputGroup value={commandId} onChange={(event) => setCommandId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Event id">
            <InputGroup value={eventId} onChange={(event) => setEventId(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Since (ISO8601)">
            <InputGroup value={since} onChange={(event) => setSince(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Until (ISO8601)">
            <InputGroup value={until} onChange={(event) => setUntil(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Limit">
            <InputGroup value={limit} onChange={(event) => setLimit(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Offset">
            <InputGroup value={offset} onChange={(event) => setOffset(event.currentTarget.value)} />
          </FormGroup>
          <div className="form-row">
            <Button intent={Intent.PRIMARY} onClick={() => logsMutation.mutate()} loading={logsMutation.isPending}>
              Load logs
            </Button>
            <Button onClick={() => chainMutation.mutate()} loading={chainMutation.isPending}>
              Chain head
            </Button>
            <Button
              onClick={() => {
                setAction('')
                setStatus('')
                setResourceType('')
                setResourceId('')
                setEventId('')
                setCommandId('')
                setActor('')
                setSince('')
                setUntil('')
                setLimit('50')
                setOffset('0')
              }}
            >
              Reset
            </Button>
          </div>
          {logItems.length === 0 ? (
            <Text className="muted">Logs will appear here.</Text>
          ) : (
            <HTMLTable striped interactive className="command-table">
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
                {logItems.map((item, index) => (
                  <tr
                    key={`${item.id ?? item.event_id ?? index}`}
                    onClick={() =>
                      setInspector({
                        title: String(item.action ?? 'Audit log'),
                        kind: 'Audit log',
                        data: item,
                      })
                    }
                  >
                    <td>{String(item.occurred_at ?? item.timestamp ?? '')}</td>
                    <td>{String(item.actor ?? '')}</td>
                    <td>{String(item.action ?? '')}</td>
                    <td>{String(item.status ?? '')}</td>
                    <td>{String(item.resource_type ?? '')} {String(item.resource_id ?? '')}</td>
                    <td>{String(item.command_id ?? '')}</td>
                  </tr>
                ))}
              </tbody>
            </HTMLTable>
          )}
          <JsonViewer value={logsMutation.data} empty="Logs will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Chain head</div>
          <JsonViewer value={chainMutation.data} empty="Chain head will appear here." />
        </Card>
      </div>
    </div>
  )
}
