import { useEffect, useMemo, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Divider,
  Drawer,
  FormGroup,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  Tab,
  Tabs,
  Tag,
  Text,
  TextArea,
} from '@blueprintjs/core'
import {
  bulkCreateInstances,
  createInstance,
  deleteInstance,
  getInstance,
  getSampleValues,
  listDatabaseClasses,
  listInstances,
  updateInstance,
} from '../api/bff'
import { useRequestContext } from '../api/useRequestContext'
import { PageHeader } from '../components/layout/PageHeader'
import { JsonViewer } from '../components/JsonViewer'
import { showAppToast } from '../app/AppToaster'
import { useCommandRegistration } from '../commands/useCommandRegistration'
import { extractCommandId } from '../commands/extractCommandId'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { navigate, navigateWithSearch } from '../state/pathname'
import { useAppStore } from '../store/useAppStore'

type ClassItem = Record<string, unknown>
type InstanceItem = Record<string, unknown>

const getInstanceId = (item: InstanceItem) =>
  (item.instance_id as string | undefined) ||
  (item.id as string | undefined) ||
  (item['@id'] as string | undefined)

const getExpectedSeq = (item: InstanceItem | null) => {
  if (!item) return null
  const version = item.version
  if (typeof version === 'number' && Number.isFinite(version)) return version
  const indexStatus = item.index_status as { event_sequence?: number } | undefined
  if (indexStatus && typeof indexStatus.event_sequence === 'number') {
    return indexStatus.event_sequence
  }
  return null
}

const formatTimestamp = (value: unknown) => {
  if (typeof value !== 'string') return ''
  const date = new Date(value)
  if (Number.isNaN(date.valueOf())) return value
  return date.toLocaleString()
}

const getInstanceSummary = (item: InstanceItem) => {
  const display = item.display as { label?: string; name?: string } | undefined
  if (display?.label) return display.label
  if (display?.name) return display.name
  if (typeof item.label === 'string') return item.label
  if (typeof item.name === 'string') return item.name
  return ''
}

const coerceNumber = (value: unknown) =>
  typeof value === 'number' && Number.isFinite(value) ? value : null

const extractActualSeq = (error: unknown) => {
  if (!error || typeof error !== 'object') return null
  const detail = (error as { detail?: unknown }).detail ?? error
  if (!detail || typeof detail !== 'object') return null
  const payload = detail as { detail?: { actual_seq?: number }; actual_seq?: number }
  return coerceNumber(payload.detail?.actual_seq ?? payload.actual_seq)
}

const extractUnknownLabels = (error: unknown) => {
  if (!error || typeof error !== 'object') return []
  const detail = (error as { detail?: unknown }).detail ?? error
  if (!detail || typeof detail !== 'object') return []
  const payload = detail as { error?: string; labels?: unknown }
  if (payload.error !== 'unknown_label_keys') return []
  if (!Array.isArray(payload.labels)) return []
  return payload.labels.filter((item): item is string => typeof item === 'string' && item.trim().length > 0)
}

const parseJsonObject = (raw: string, label: string) => {
  const parsed = JSON.parse(raw)
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON object`)
  }
  return parsed as Record<string, unknown>
}

const parseJsonArray = (raw: string, label: string) => {
  const parsed = JSON.parse(raw)
  if (!Array.isArray(parsed)) {
    throw new Error(`${label} must be a JSON array`)
  }
  return parsed as Array<Record<string, unknown>>
}

export const InstancesPage = ({ dbName }: { dbName: string }) => {
  const queryClient = useQueryClient()
  const requestContext = useRequestContext()
  const language = useAppStore((state) => state.context.language)
  const branch = useAppStore((state) => state.context.branch)
  const registerCommand = useCommandRegistration()

  const [classId, setClassId] = useState('')
  const [search, setSearch] = useState('')
  const [limit, setLimit] = useState('100')
  const [selectedId, setSelectedId] = useState<string | null>(null)
  const [editJson, setEditJson] = useState('')
  const [drawerOpen, setDrawerOpen] = useState(false)
  const [activeTab, setActiveTab] = useState('view')
  const [occRetry, setOccRetry] = useState<{ action: 'update' | 'delete'; actualSeq: number } | null>(null)
  const [unknownLabels, setUnknownLabels] = useState<string[]>([])
  const [createJson, setCreateJson] = useState('{}')
  const [createMetadataJson, setCreateMetadataJson] = useState('{}')
  const [bulkJson, setBulkJson] = useState('[]')
  const [bulkMetadataJson, setBulkMetadataJson] = useState('{}')
  const [createJsonError, setCreateJsonError] = useState<string | null>(null)
  const [bulkJsonError, setBulkJsonError] = useState<string | null>(null)
  const [createUnknownLabels, setCreateUnknownLabels] = useState<string[]>([])
  const [bulkUnknownLabels, setBulkUnknownLabels] = useState<string[]>([])

  const classesQuery = useQuery({
    queryKey: qk.classes(dbName, requestContext.language),
    queryFn: () => listDatabaseClasses(requestContext, dbName),
  })

  const instancesQuery = useQuery({
    queryKey: qk.instances(dbName, classId, requestContext.language, { limit, search }),
    queryFn: () =>
      listInstances(requestContext, dbName, classId, {
        limit: Number(limit) || 100,
        offset: 0,
        search: search || undefined,
      }),
    enabled: Boolean(classId),
  })

  const detailQuery = useQuery({
    queryKey: selectedId ? qk.instance(dbName, classId, selectedId, requestContext.language) : ['instance', 'none'],
    queryFn: () => getInstance(requestContext, dbName, classId, selectedId ?? ''),
    enabled: Boolean(selectedId && classId),
  })

  const sampleQuery = useQuery({
    queryKey: qk.sampleValues(dbName, classId, requestContext.language),
    queryFn: () => getSampleValues(requestContext, dbName, classId),
    enabled: Boolean(classId),
  })

  useEffect(() => {
    if (detailQuery.data) {
      const payload = detailQuery.data as { data?: InstanceItem }
      const data = payload.data ?? payload
      setEditJson(JSON.stringify(data, null, 2))
      setOccRetry(null)
      setUnknownLabels([])
      setActiveTab('view')
    }
  }, [detailQuery.data])

  const selectedInstance = useMemo(() => {
    if (!detailQuery.data) return null
    const payload = detailQuery.data as { data?: InstanceItem }
    return payload.data ?? payload
  }, [detailQuery.data])

  const updateMutation = useMutation({
    mutationFn: async (vars?: { expectedSeqOverride?: number }) => {
      if (!selectedId) throw new Error('Select an instance')
      const parsed = JSON.parse(editJson)
      const expectedSeq =
        vars?.expectedSeqOverride ??
        getExpectedSeq(
          detailQuery.data
            ? (detailQuery.data as { data?: InstanceItem }).data ?? (detailQuery.data as InstanceItem)
            : null,
        )
      if (expectedSeq === null) {
        throw new Error('expected_seq missing')
      }
      return updateInstance(requestContext, dbName, classId, selectedId, branch, expectedSeq, {
        data: parsed,
        metadata: {},
      })
    },
    onSuccess: (payload) => {
      const commandId = payload.command_id
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'UPDATE_INSTANCE',
          targetClassId: classId,
          targetInstanceId: selectedId ?? undefined,
          title: `Update instance (${classId})`,
        })
      }
      setOccRetry(null)
      setUnknownLabels([])
      void showAppToast({ intent: Intent.SUCCESS, message: 'Update submitted.' })
    },
    onError: (error) => {
      const actual = extractActualSeq(error)
      if (actual !== null) {
        setOccRetry({ action: 'update', actualSeq: actual })
      }
      setUnknownLabels(extractUnknownLabels(error))
      toastApiError(error, language)
    },
  })

  const deleteMutation = useMutation({
    mutationFn: async (vars?: { expectedSeqOverride?: number }) => {
      if (!selectedId) throw new Error('Select an instance')
      const expectedSeq =
        vars?.expectedSeqOverride ??
        getExpectedSeq(
          detailQuery.data
            ? (detailQuery.data as { data?: InstanceItem }).data ?? (detailQuery.data as InstanceItem)
            : null,
        )
      if (expectedSeq === null) {
        throw new Error('expected_seq missing')
      }
      return deleteInstance(requestContext, dbName, classId, selectedId, branch, expectedSeq)
    },
    onSuccess: (payload) => {
      const commandId = payload.command_id
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'DELETE_INSTANCE',
          targetClassId: classId,
          targetInstanceId: selectedId ?? undefined,
          title: `Delete instance (${classId})`,
        })
      }
      setOccRetry(null)
      setUnknownLabels([])
      void showAppToast({ intent: Intent.WARNING, message: 'Delete submitted.' })
      void queryClient.invalidateQueries({ queryKey: qk.instances(dbName, classId, requestContext.language, { limit, search }) })
      setSelectedId(null)
      setDrawerOpen(false)
    },
    onError: (error) => {
      const actual = extractActualSeq(error)
      if (actual !== null) {
        setOccRetry({ action: 'delete', actualSeq: actual })
      }
      toastApiError(error, language)
    },
  })

  const createMutation = useMutation({
    mutationFn: (payload: { data: Record<string, unknown>; metadata: Record<string, unknown> }) => {
      if (!classId) {
        throw new Error('Select a class first')
      }
      return createInstance(requestContext, dbName, classId, branch, payload)
    },
    onSuccess: (payload) => {
      const commandId = payload.command_id ?? extractCommandId(payload)
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'CREATE_INSTANCE',
          targetClassId: classId,
          title: `Create instance (${classId})`,
        })
      }
      setCreateUnknownLabels([])
      void showAppToast({ intent: Intent.SUCCESS, message: 'Create submitted.' })
    },
    onError: (error) => {
      setCreateUnknownLabels(extractUnknownLabels(error))
      toastApiError(error, language)
    },
  })

  const bulkCreateMutation = useMutation({
    mutationFn: (payload: { instances: Array<Record<string, unknown>>; metadata: Record<string, unknown> }) => {
      if (!classId) {
        throw new Error('Select a class first')
      }
      return bulkCreateInstances(requestContext, dbName, classId, branch, payload)
    },
    onSuccess: (payload) => {
      const commandId = payload.command_id ?? extractCommandId(payload)
      if (commandId) {
        registerCommand({
          commandId,
          kind: 'BULK_CREATE_INSTANCE',
          targetClassId: classId,
          title: `Bulk create (${classId})`,
        })
      }
      setBulkUnknownLabels([])
      void showAppToast({ intent: Intent.SUCCESS, message: 'Bulk create submitted.' })
    },
    onError: (error) => {
      setBulkUnknownLabels(extractUnknownLabels(error))
      toastApiError(error, language)
    },
  })

  const handleCreate = () => {
    setCreateJsonError(null)
    try {
      const data = parseJsonObject(createJson, 'Create payload')
      const metadata = parseJsonObject(createMetadataJson, 'Metadata')
      createMutation.mutate({ data, metadata })
    } catch (error) {
      setCreateJsonError(error instanceof Error ? error.message : 'Invalid JSON')
    }
  }

  const handleBulkCreate = () => {
    setBulkJsonError(null)
    try {
      const instances = parseJsonArray(bulkJson, 'Instances payload')
      const metadata = parseJsonObject(bulkMetadataJson, 'Metadata')
      bulkCreateMutation.mutate({ instances, metadata })
    } catch (error) {
      setBulkJsonError(error instanceof Error ? error.message : 'Invalid JSON')
    }
  }

  const classOptions = useMemo(() => {
    const payload = classesQuery.data as { classes?: ClassItem[] } | undefined
    const list = payload?.classes ?? []
    return list.map((item) => {
      const id = (item.id as string | undefined) || (item['@id'] as string | undefined) || ''
      const label = (item.label as string | undefined) || id
      return { label: label ? `${label} (${id})` : id, value: id }
    })
  }, [classesQuery.data])

  const instances = useMemo(() => {
    const payload = instancesQuery.data as { instances?: InstanceItem[] } | undefined
    return payload?.instances ?? []
  }, [instancesQuery.data])

  return (
    <div>
      <PageHeader title="Instances" subtitle="Read model (branch ignored). Use Graph Explorer for branch-aware checks." />
      <Callout intent={Intent.WARNING} style={{ marginBottom: 12 }}>
        Branch ignored for instance reads.
      </Callout>

      <div className="page-grid two-col">
        <Card className="card-stack">
          <FormGroup label="Class">
            <HTMLSelect
              options={[{ label: 'Select class', value: '' }, ...classOptions]}
              value={classId}
              onChange={(event) => setClassId(event.currentTarget.value)}
            />
          </FormGroup>
          <FormGroup label="Search">
            <InputGroup value={search} onChange={(event) => setSearch(event.currentTarget.value)} />
          </FormGroup>
          <FormGroup label="Limit">
            <InputGroup value={limit} onChange={(event) => setLimit(event.currentTarget.value)} />
          </FormGroup>
          <Button onClick={() => void instancesQuery.refetch()} disabled={!classId} loading={instancesQuery.isFetching}>
            Refresh
          </Button>
          <Divider />
          {instances.length === 0 ? (
            <Text className="muted">No instances.</Text>
          ) : (
            <HTMLTable striped interactive className="command-table">
              <thead>
                <tr>
                  <th>ID</th>
                  <th>Version</th>
                  <th>Event timestamp</th>
                  <th>Summary</th>
                  <th>Actions</th>
                </tr>
              </thead>
              <tbody>
                {instances.map((item) => {
                  const id = getInstanceId(item) ?? 'unknown'
                  const version =
                    typeof item.version === 'number'
                      ? item.version
                      : (item.index_status as { event_sequence?: number } | undefined)?.event_sequence
                  const timestamp =
                    formatTimestamp(item.event_timestamp) ||
                    formatTimestamp(item.updated_at) ||
                    formatTimestamp(item.created_at)
                  const summary = getInstanceSummary(item)
                  return (
                    <tr key={id}>
                      <td>{id}</td>
                      <td>{version ?? ''}</td>
                      <td>{timestamp}</td>
                      <td>{summary}</td>
                      <td>
                        <Button
                          small
                          onClick={() => {
                            setSelectedId(id)
                            setDrawerOpen(true)
                          }}
                        >
                          Open
                        </Button>
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </HTMLTable>
          )}
          <Divider />
          <div className="card-title">Write instances</div>
          <div className="form-row">
            <Tag minimal intent={Intent.WARNING}>Branch: {branch}</Tag>
            {!classId ? <Text className="muted small">Select a class to enable writes.</Text> : null}
          </div>
          <Tabs id="instance-write-tabs">
            <Tab
              id="create"
              title="Create"
              panel={
                <div className="card-stack">
                  <FormGroup label="Create payload (JSON object, label keys)">
                    <TextArea
                      value={createJson}
                      onChange={(event) => setCreateJson(event.currentTarget.value)}
                      rows={6}
                    />
                  </FormGroup>
                  <FormGroup label="Metadata (JSON object)">
                    <TextArea
                      value={createMetadataJson}
                      onChange={(event) => setCreateMetadataJson(event.currentTarget.value)}
                      rows={4}
                    />
                  </FormGroup>
                  {createJsonError ? <Callout intent={Intent.DANGER}>{createJsonError}</Callout> : null}
                  {createUnknownLabels.length ? (
                    <Callout intent={Intent.WARNING}>
                      Unknown labels: {createUnknownLabels.join(', ')}
                      <div style={{ marginTop: 6 }}>
                        <Button small onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/mappings`)}>
                          Open mappings
                        </Button>
                      </div>
                    </Callout>
                  ) : null}
                  <Button
                    intent={Intent.PRIMARY}
                    onClick={handleCreate}
                    disabled={!classId}
                    loading={createMutation.isPending}
                  >
                    Create instance
                  </Button>
                </div>
              }
            />
            <Tab
              id="bulk"
              title="Bulk create"
              panel={
                <div className="card-stack">
                  <FormGroup label="Instances (JSON array of objects)">
                    <TextArea
                      value={bulkJson}
                      onChange={(event) => setBulkJson(event.currentTarget.value)}
                      rows={6}
                    />
                  </FormGroup>
                  <FormGroup label="Metadata (JSON object)">
                    <TextArea
                      value={bulkMetadataJson}
                      onChange={(event) => setBulkMetadataJson(event.currentTarget.value)}
                      rows={4}
                    />
                  </FormGroup>
                  {bulkJsonError ? <Callout intent={Intent.DANGER}>{bulkJsonError}</Callout> : null}
                  {bulkUnknownLabels.length ? (
                    <Callout intent={Intent.WARNING}>
                      Unknown labels: {bulkUnknownLabels.join(', ')}
                      <div style={{ marginTop: 6 }}>
                        <Button small onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/mappings`)}>
                          Open mappings
                        </Button>
                      </div>
                    </Callout>
                  ) : null}
                  <Button
                    intent={Intent.PRIMARY}
                    onClick={handleBulkCreate}
                    disabled={!classId}
                    loading={bulkCreateMutation.isPending}
                  >
                    Submit bulk create
                  </Button>
                </div>
              }
            />
          </Tabs>
          <Divider />
          <JsonViewer value={sampleQuery.data} empty="Sample values will appear here." />
        </Card>

        <Card className="card-stack">
          <div className="card-title">Detail</div>
          {selectedId ? (
            <>
              <Text className="muted small">Instance: {selectedId}</Text>
              <JsonViewer value={detailQuery.data} empty="No detail loaded." />
              <Button
                intent={Intent.PRIMARY}
                onClick={() => setDrawerOpen(true)}
                disabled={!selectedId}
              >
                Open drawer
              </Button>
            </>
          ) : (
            <Text className="muted">Select an instance.</Text>
          )}
        </Card>
      </div>

      <Drawer
        isOpen={drawerOpen && Boolean(selectedId)}
        onClose={() => setDrawerOpen(false)}
        position="right"
        title={selectedId ?? 'Instance'}
        className="command-drawer"
      >
        <div className="command-drawer-body">
          <div className="form-row">
            <Tag minimal intent={Intent.WARNING}>Branch ignored</Tag>
            {selectedInstance ? (
              <Tag minimal>
                expected_seq {getExpectedSeq(selectedInstance) ?? 'n/a'}
              </Tag>
            ) : null}
          </div>

          <Tabs id="instance-tabs" selectedTabId={activeTab} onChange={(value) => setActiveTab(value as string)}>
            <Tab
              id="view"
              title="View"
              panel={<JsonViewer value={detailQuery.data} empty="No detail loaded." />}
            />
            <Tab
              id="edit"
              title="Edit"
              panel={
                <div className="card-stack">
                  <FormGroup label="Edit payload (JSON)">
                    <TextArea value={editJson} onChange={(event) => setEditJson(event.currentTarget.value)} rows={10} />
                  </FormGroup>
                  {unknownLabels.length ? (
                    <Callout intent={Intent.WARNING}>
                      Unknown labels: {unknownLabels.join(', ')}
                      <div style={{ marginTop: 6 }}>
                        <Button small onClick={() => navigate(`/db/${encodeURIComponent(dbName)}/mappings`)}>
                          Open mappings
                        </Button>
                      </div>
                    </Callout>
                  ) : null}
                  {occRetry ? (
                    <Callout intent={Intent.WARNING}>
                      OCC conflict. actual_seq = {occRetry.actualSeq}
                      <div style={{ marginTop: 6 }}>
                        <Button
                          small
                          onClick={() => {
                            if (occRetry.action === 'update') {
                              updateMutation.mutate({ expectedSeqOverride: occRetry.actualSeq })
                            } else {
                              deleteMutation.mutate({ expectedSeqOverride: occRetry.actualSeq })
                            }
                          }}
                        >
                          Retry with actual_seq
                        </Button>
                      </div>
                    </Callout>
                  ) : null}
                  <div className="form-row">
                    <Button
                      intent={Intent.PRIMARY}
                      onClick={() => updateMutation.mutate(undefined)}
                      disabled={!selectedId}
                      loading={updateMutation.isPending}
                    >
                      Update
                    </Button>
                    <Button
                      intent={Intent.DANGER}
                      onClick={() => deleteMutation.mutate(undefined)}
                      disabled={!selectedId}
                      loading={deleteMutation.isPending}
                    >
                      Delete
                    </Button>
                  </div>
                </div>
              }
            />
            <Tab
              id="audit"
              title="Audit Links"
              panel={
                <div className="card-stack">
                  <Text className="muted">Open audit logs filtered for this instance.</Text>
                  <Button
                    onClick={() =>
                      navigateWithSearch(`/db/${encodeURIComponent(dbName)}/audit`, {
                        partition_key: `db:${dbName}`,
                        resource_id: selectedId ?? '',
                        resource_type: classId || undefined,
                      })
                    }
                    disabled={!selectedId}
                  >
                    Open Audit Logs
                  </Button>
                </div>
              }
            />
            <Tab
              id="lineage"
              title="Lineage Links"
              panel={
                <div className="card-stack">
                  <Text className="muted">Open lineage graph with this instance id as root.</Text>
                  <Button
                    onClick={() =>
                      navigateWithSearch(`/db/${encodeURIComponent(dbName)}/lineage`, {
                        root: selectedId ?? '',
                      })
                    }
                    disabled={!selectedId}
                  >
                    Open Lineage
                  </Button>
                </div>
              }
            />
          </Tabs>
        </div>
      </Drawer>
    </div>
  )
}
