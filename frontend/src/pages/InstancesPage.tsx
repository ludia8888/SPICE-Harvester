import { useMemo, useState } from 'react'
import { Link, useParams } from 'react-router-dom'
import { useMutation, useQuery } from '@tanstack/react-query'
import {
  Button,
  Callout,
  Card,
  Drawer,
  H5,
  HTMLSelect,
  HTMLTable,
  InputGroup,
  Intent,
  NumericInput,
  Tab,
  Tabs,
  TextArea,
} from '@blueprintjs/core'
import {
  bulkCreateInstances,
  createInstance,
  deleteInstance,
  getInstance,
  getSampleValues,
  listInstances,
  updateInstance,
  type CommandResult,
  type InstanceListResponse,
} from '../api/bff'
import { ApiErrorCallout } from '../components/ApiErrorCallout'
import { PageHeader } from '../components/PageHeader'
import { JsonView } from '../components/JsonView'
import { useOntologyRegistry } from '../hooks/useOntologyRegistry'
import { toastApiError } from '../errors/toastApiError'
import { qk } from '../query/queryKeys'
import { useAppStore } from '../store/useAppStore'
import { extractOccActualSeq } from '../utils/occ'
import { asArray, asRecord, getNumber, getString, type UnknownRecord } from '../utils/typed'

const getInstanceId = (item: unknown) => {
  const record = asRecord(item)
  return (
    getString(record.instance_id) ??
    getString(record.id) ??
    getString(record['@id']) ??
    getString(record.uuid) ??
    ''
  )
}

const getInstanceVersion = (item: unknown) => {
  const record = asRecord(item)
  return (
    getNumber(record.version) ??
    getNumber(record.expected_seq) ??
    getNumber(record.seq) ??
    getNumber(record.sequence) ??
    null
  )
}

export const InstancesPage = () => {
  const { db } = useParams()
  const context = useAppStore((state) => state.context)
  const authToken = useAppStore((state) => state.authToken)
  const adminToken = useAppStore((state) => state.adminToken)
  const trackCommand = useAppStore((state) => state.trackCommand)

  const registry = useOntologyRegistry(db, context.branch)

  const [classId, setClassId] = useState('')
  const [search, setSearch] = useState('')
  const [limit, setLimit] = useState(25)
  const [offset, setOffset] = useState(0)
  const [selectedInstance, setSelectedInstance] = useState<UnknownRecord | null>(null)
  const [drawerTab, setDrawerTab] = useState('view')
  const [editJson, setEditJson] = useState('')
  const [createJson, setCreateJson] = useState('')
  const [bulkJson, setBulkJson] = useState('')
  const [actualSeqHint, setActualSeqHint] = useState<number | null>(null)
  const [writeError, setWriteError] = useState<unknown>(null)
  const [sampleValues, setSampleValues] = useState<unknown>(null)

  const requestContext = useMemo(
    () => ({ language: context.language, authToken, adminToken }),
    [adminToken, authToken, context.language],
  )

  const listQuery = useQuery<InstanceListResponse>({
    queryKey: classId
      ? qk.instances({ dbName: db ?? '', classId, limit, offset, search, language: context.language })
      : ['bff', 'instances', 'empty'],
    queryFn: () => listInstances(requestContext, db ?? '', classId, { limit, offset, search }),
    enabled: Boolean(db && classId),
  })

  const detailQuery = useQuery({
    queryKey:
      db && classId && selectedInstance
        ? qk.instanceDetail({ dbName: db, classId, instanceId: getInstanceId(selectedInstance), language: context.language })
        : ['bff', 'instance', 'empty'],
    queryFn: () => getInstance(requestContext, db ?? '', classId, getInstanceId(selectedInstance)),
    enabled: Boolean(db && classId && selectedInstance),
    onSuccess: (data) => {
      setEditJson(JSON.stringify(data, null, 2))
    },
  })

  const createMutation = useMutation<CommandResult>({
    mutationFn: async () => {
      if (!db || !classId) {
        throw new Error('Missing class')
      }
      const data = JSON.parse(createJson || '{}')
      return createInstance(requestContext, db, classId, context.branch, { data })
    },
    onSuccess: (result) => {
      setWriteError(null)
      trackCommand({
        id: result.command_id,
        kind: 'INSTANCE_WRITE',
        title: `Create ${classId}`,
        target: { dbName: db ?? '' },
        context: { project: db ?? null, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
      setCreateJson('')
    },
    onError: (error) => {
      setWriteError(error)
      toastApiError(error, context.language)
    },
  })

  const bulkMutation = useMutation<CommandResult>({
    mutationFn: async () => {
      if (!db || !classId) {
        throw new Error('Missing class')
      }
      const instances = JSON.parse(bulkJson || '[]')
      return bulkCreateInstances(requestContext, db, classId, context.branch, { instances })
    },
    onSuccess: (result) => {
      setWriteError(null)
      trackCommand({
        id: result.command_id,
        kind: 'INSTANCE_WRITE',
        title: `Bulk create ${classId}`,
        target: { dbName: db ?? '' },
        context: { project: db ?? null, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
      setBulkJson('')
    },
    onError: (error) => {
      setWriteError(error)
      toastApiError(error, context.language)
    },
  })

  const updateMutation = useMutation<CommandResult, unknown, { expectedSeqOverride?: number } | undefined>({
    mutationFn: async (params?: { expectedSeqOverride?: number }) => {
      if (!db || !classId || !selectedInstance) {
        throw new Error('Missing instance')
      }
      const expectedSeq = params?.expectedSeqOverride ?? getInstanceVersion(selectedInstance) ?? actualSeqHint
      if (typeof expectedSeq !== 'number') {
        throw new Error('expected_seq missing')
      }
      const data = JSON.parse(editJson || '{}')
      return updateInstance(requestContext, db, classId, getInstanceId(selectedInstance), context.branch, expectedSeq, {
        data,
      })
    },
    onSuccess: (result) => {
      setActualSeqHint(null)
      trackCommand({
        id: result.command_id,
        kind: 'INSTANCE_WRITE',
        title: `Update ${classId}`,
        target: { dbName: db ?? '' },
        context: { project: db ?? null, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
    },
    onError: (error) => {
      const actual = extractOccActualSeq(error)
      if (actual !== null) {
        setActualSeqHint(actual)
      }
      toastApiError(error, context.language)
    },
  })

  const deleteMutation = useMutation<CommandResult, unknown, { expectedSeqOverride?: number } | undefined>({
    mutationFn: async (params?: { expectedSeqOverride?: number }) => {
      if (!db || !classId || !selectedInstance) {
        throw new Error('Missing instance')
      }
      const expectedSeq = params?.expectedSeqOverride ?? getInstanceVersion(selectedInstance) ?? actualSeqHint
      if (typeof expectedSeq !== 'number') {
        throw new Error('expected_seq missing')
      }
      return deleteInstance(requestContext, db, classId, getInstanceId(selectedInstance), context.branch, expectedSeq)
    },
    onSuccess: (result) => {
      setActualSeqHint(null)
      trackCommand({
        id: result.command_id,
        kind: 'INSTANCE_WRITE',
        title: `Delete ${classId}`,
        target: { dbName: db ?? '' },
        context: { project: db ?? null, branch: context.branch },
        submittedAt: new Date().toISOString(),
        writePhase: 'SUBMITTED',
        indexPhase: 'UNKNOWN',
      })
    },
    onError: (error) => {
      const actual = extractOccActualSeq(error)
      if (actual !== null) {
        setActualSeqHint(actual)
      }
      toastApiError(error, context.language)
    },
  })

  const sampleMutation = useMutation({
    mutationFn: async () => {
      if (!db || !classId) {
        throw new Error('Missing class')
      }
      return getSampleValues(requestContext, db, classId)
    },
    onSuccess: (result) => setSampleValues(result),
    onError: (error) => toastApiError(error, context.language),
  })

  const instances = useMemo(
    () => asArray<UnknownRecord>(listQuery.data?.instances ?? listQuery.data?.data?.instances),
    [listQuery.data],
  )

  const handleSelectInstance = (item: UnknownRecord) => {
    setSelectedInstance(item)
    setDrawerTab('view')
    setActualSeqHint(null)
  }

  return (
    <div>
      <PageHeader title="Instances" subtitle="Branch ignored. 빠른 인스턴스 확인용." />
      <Callout intent={Intent.WARNING} title="Branch ignored">
        Instances read API는 branch를 무시합니다. 검증은 Graph Explorer를 사용하세요.
      </Callout>

      {writeError ? (
        <ApiErrorCallout
          error={writeError}
          language={context.language}
          mappingsUrl={db ? `/db/${encodeURIComponent(db)}/mappings` : undefined}
        />
      ) : null}

      <Card elevation={1} className="section-card">
        <div className="form-row">
          <HTMLSelect
            value={classId}
            onChange={(event) => setClassId(event.currentTarget.value)}
            options={[{ label: 'Select class', value: '' }, ...registry.classOptions]}
          />
          <InputGroup placeholder="search" value={search} onChange={(event) => setSearch(event.currentTarget.value)} />
          <NumericInput value={limit} min={1} max={1000} onValueChange={(value) => setLimit(value)} />
          <NumericInput value={offset} min={0} onValueChange={(value) => setOffset(value)} />
          <Button onClick={() => listQuery.refetch()} icon="refresh">
            Refresh
          </Button>
        </div>
      </Card>

      <Card elevation={1} className="section-card">
        <HTMLTable striped interactive className="full-width">
          <thead>
            <tr>
              <th>Instance ID</th>
              <th>Version</th>
              <th>Timestamp</th>
            </tr>
          </thead>
          <tbody>
            {instances.map((item) => (
              <tr key={getInstanceId(item)} onClick={() => handleSelectInstance(item)}>
                <td>{getInstanceId(item)}</td>
                <td>{getInstanceVersion(item) ?? '-'}</td>
                <td>
                  {getString(item.event_timestamp) ?? getString(item.updated_at) ?? '-'}
                </td>
              </tr>
            ))}
          </tbody>
        </HTMLTable>
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Sample values</H5>
          <Button onClick={() => sampleMutation.mutate()} disabled={!classId} loading={sampleMutation.isPending}>
            Load sample
          </Button>
        </div>
        <JsonView value={sampleValues} fallback="샘플 값을 확인하세요." />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Create instance</H5>
          <Button intent={Intent.PRIMARY} onClick={() => createMutation.mutate()} disabled={!classId || !createJson}>
            Create (202)
          </Button>
        </div>
        <TextArea
          rows={6}
          value={createJson}
          onChange={(event) => setCreateJson(event.currentTarget.value)}
          placeholder='{"라벨": "값"}'
        />
      </Card>

      <Card elevation={1} className="section-card">
        <div className="card-title">
          <H5>Bulk create</H5>
          <Button intent={Intent.PRIMARY} onClick={() => bulkMutation.mutate()} disabled={!classId || !bulkJson}>
            Bulk Create (202)
          </Button>
        </div>
        <TextArea
          rows={6}
          value={bulkJson}
          onChange={(event) => setBulkJson(event.currentTarget.value)}
          placeholder='[{"라벨": "값"}]'
        />
      </Card>

      <Drawer
        isOpen={Boolean(selectedInstance)}
        onClose={() => {
          setSelectedInstance(null)
          setActualSeqHint(null)
          setEditJson('')
        }}
        title={selectedInstance ? getInstanceId(selectedInstance) : 'Instance'}
        position="right"
        size="50%"
      >
        <div className="drawer-body">
          <Tabs id="instance-tabs" selectedTabId={drawerTab} onChange={(tabId) => setDrawerTab(tabId as string)}>
            <Tab id="view" title="View" />
            <Tab id="edit" title="Edit" />
            <Tab id="audit" title="Audit" />
            <Tab id="lineage" title="Lineage" />
          </Tabs>
          {drawerTab === 'view' ? <JsonView value={detailQuery.data} /> : null}
          {drawerTab === 'edit' ? (
            <div>
              {actualSeqHint !== null ? (
                <Callout intent={Intent.WARNING}>
                  actual_seq: {actualSeqHint}
                  <div className="button-row">
                    <Button
                      minimal
                      icon="refresh"
                      onClick={() => updateMutation.mutate({ expectedSeqOverride: actualSeqHint })}
                    >
                      Use actual_seq and retry update
                    </Button>
                    <Button
                      minimal
                      icon="refresh"
                      intent={Intent.DANGER}
                      onClick={() => deleteMutation.mutate({ expectedSeqOverride: actualSeqHint })}
                    >
                      Use actual_seq and retry delete
                    </Button>
                  </div>
                </Callout>
              ) : null}
              <TextArea rows={10} value={editJson} onChange={(event) => setEditJson(event.currentTarget.value)} />
              <div className="button-row">
                <Button intent={Intent.PRIMARY} onClick={() => updateMutation.mutate(undefined)}>
                  Update (202)
                </Button>
                <Button intent={Intent.DANGER} onClick={() => deleteMutation.mutate(undefined)}>
                  Delete (202)
                </Button>
              </div>
            </div>
          ) : null}
          {drawerTab === 'audit' ? (
            <Link to={`/db/${encodeURIComponent(db ?? '')}/audit`}>Open Audit Logs</Link>
          ) : null}
          {drawerTab === 'lineage' ? (
            <Link to={`/db/${encodeURIComponent(db ?? '')}/lineage`}>Open Lineage</Link>
          ) : null}
        </div>
      </Drawer>
    </div>
  )
}
